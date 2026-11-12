//! Log format 3 on a device: one write-ahead log shared by every shard of a
//! data directory (format in [`fusee_core::wal`], acceptance rule in
//! `spec/FuseeLog.tla`).
//!
//! **Shape.** Each shard encodes its records into a section
//! ([`fusee_core::wal::SectionWriter`]) and stages it here. Whoever writes
//! next takes every staged section and writes them as one batch with one
//! flush: a step on one shard and a step on another share a write instead of
//! each paying for one.
//!
//! **Owner.** A shard that flushes inline and finds no write in progress
//! becomes the owner: it writes everything staged, publishes each shard's
//! durable position and wakes the shards it wrote for, then hands the next
//! write to a shard waiting with a staged section (see [`SharedWal`]). A
//! lone workflow therefore still costs one write and one flush on its own
//! thread. Shards that do not wait (busy ones) stage for the log thread and
//! its I/O threads.
//!
//! **Segments.** Segment files are `wal/<number>.wal`. The first is small
//! and sizes double up to the configured size, so a small embedded use opens
//! fast. A segment every shard's retained checkpoints cover is retired: its
//! header block is zeroed (its bytes can then never be read as part of the
//! chain) and the file is kept as a spare to be renamed and reused, so
//! steady state creates no files and zero-fills nothing.
//!
//! **Recovery** reads the chain from the oldest segment, keeps each shard's
//! raw sections, and decodes for each shard only the records after its
//! checkpoint. The next write resumes in place, right after the chain.

use crate::env::{Env, Notify};
use crate::local::{FlushMethod, LocalEnv, LocalOptions, flush_file, sync_dir, unix_ms};
use fusee_core::record::{Lsn, RecordBuf, RunId};
use fusee_core::segment::Batch;
use fusee_core::shard::BlobRef;
use fusee_core::store::Checkpoint;
use fusee_core::wal::{
    BATCH_HEADER, BLOB_ALIGN, BLOCK, End, Large, MAX_BATCH, OffloadRef, Progress, SECTION_HEADER,
    SectionRef, WAL_HEADER_BYTES, WalHeader, WalScanner, WalWriter, batch_body_len, batch_span,
    find_large, for_each_offload, section_records_resolved,
};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Instant;

/// Where a log's segment files live.
pub trait LogDevice {
    /// Segment numbers present, ascending.
    fn segments(&self) -> io::Result<Vec<u64>>;
    fn len(&self, segment: u64) -> io::Result<u64>;
    /// Up to `len` bytes at `offset`, fewer at the end of the file.
    fn read(&self, segment: u64, offset: u64, len: usize) -> io::Result<Vec<u8>>;
    /// Creates segment `segment` with `len` bytes, ready for overwrites,
    /// durably (file and directory entry).
    fn create(&self, segment: u64, len: u64) -> io::Result<()>;
    /// Writes at `offset`; durable only after [`sync`](Self::sync).
    fn write(&self, segment: u64, offset: u64, bytes: &[u8]) -> io::Result<()>;
    fn sync(&self, segment: u64) -> io::Result<()>;
    /// Renames a segment file, durably.
    fn rename(&self, from: u64, to: u64) -> io::Result<()>;
    fn remove(&self, segment: u64) -> io::Result<()>;
    /// Like [`create`](Self::create), possibly in the background; any later
    /// operation on the segment waits for it.
    fn prepare(&self, segment: u64, len: u64) -> io::Result<()> {
        self.create(segment, len)
    }
    /// Whether a [`prepare`](Self::prepare)d segment is ready: an
    /// operation on it now would not wait.
    fn is_ready(&self, _segment: u64) -> bool {
        true
    }
}

/// Segment files in a directory (or, with [`FileDevice::blobs`], the files
/// of a log's blob area).
pub struct FileDevice {
    dir: PathBuf,
    /// File name extension: `wal` for segments, `blob` for blob files.
    ext: &'static str,
    opts: LocalOptions,
    open: Mutex<BTreeMap<u64, Arc<File>>>,
    /// Linux with `LocalOptions::direct_io`, where the filesystem allows it.
    direct: Option<Direct>,
    /// An `O_DIRECT` write failed with `EINVAL` (a filesystem or device that
    /// accepted the open but not the write): every write since is buffered.
    refused: std::sync::atomic::AtomicBool,
    /// Tests: fail every `O_DIRECT` write with `EINVAL`.
    #[cfg(test)]
    reject_direct: std::sync::atomic::AtomicBool,
    /// Segments being prepared in the background.
    preparing: Mutex<BTreeMap<u64, Arc<Preparation>>>,
    /// The threads preparing them (joined when the device drops).
    preparers: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl FileDevice {
    pub fn open(dir: &Path, opts: LocalOptions) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let direct = Direct::probe(dir, &opts);
        Ok(Self {
            dir: dir.to_owned(),
            ext: "wal",
            opts,
            open: Mutex::new(BTreeMap::new()),
            direct,
            refused: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            reject_direct: std::sync::atomic::AtomicBool::new(false),
            preparing: Mutex::new(BTreeMap::new()),
            preparers: Mutex::new(Vec::new()),
        })
    }

    /// The blob area of a log (`<log dir>/blobs`, created with its first
    /// file): files that grow by appends, buffered writes made durable the
    /// way the log's are (`RWF_DSYNC`, `fdatasync` or `F_FULLFSYNC`).
    pub fn blobs(dir: &Path, opts: LocalOptions) -> Self {
        Self {
            dir: dir.to_owned(),
            ext: "blob",
            opts,
            open: Mutex::new(BTreeMap::new()),
            direct: None,
            refused: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            reject_direct: std::sync::atomic::AtomicBool::new(false),
            preparing: Mutex::new(BTreeMap::new()),
            preparers: Mutex::new(Vec::new()),
        }
    }

    /// Whether writes bypass the page cache.
    pub fn direct_io(&self) -> bool {
        self.direct.is_some() && !self.refused.load(Ordering::Relaxed)
    }

    /// The direct writer, unless an earlier `EINVAL` moved the log to
    /// buffered writes.
    fn direct(&self) -> Option<&Direct> {
        self.direct
            .as_ref()
            .filter(|_| !self.refused.load(Ordering::Relaxed))
    }

    /// For the log owner's ring: the file and aligned blocks an `O_DIRECT`
    /// write of `bytes` at `offset` takes (starting at `offset` rounded
    /// down to a block); `None` when the device writes buffered.
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    fn direct_buffer(
        &self,
        segment: u64,
        offset: u64,
        bytes: &[u8],
    ) -> io::Result<Option<(Arc<File>, AlignedBuf, DirectTail)>> {
        let Some(d) = self.direct() else {
            return Ok(None);
        };
        self.ready(segment)?;
        let path = self.path(segment);
        let (f, buf, _, tail) = d.buffer(&path, segment, offset, bytes, || self.file(segment))?;
        Ok(Some((f, buf, tail)))
    }

    /// The log owner's write completed: note its partial last block.
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    fn direct_wrote(&self, tail: DirectTail) {
        if let Some(d) = self.direct.as_ref() {
            d.wrote(tail);
        }
    }

    /// Waits for a background preparation of `segment`, if one was
    /// started. Every caller waits for it, not only the first: two writers
    /// of a new segment (a roll's batch and the next one) once found it
    /// half made, so one failed with `ENOENT` or had its bytes truncated by
    /// the fill.
    fn ready(&self, segment: u64) -> io::Result<()> {
        let job = self
            .preparing
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&segment)
            .cloned();
        let Some(p) = job else {
            return Ok(());
        };
        let mut r = p.result.lock().unwrap_or_else(|e| e.into_inner());
        while r.is_none() {
            r = p.done.wait(r).unwrap_or_else(|e| e.into_inner());
        }
        let result = r.clone().unwrap_or(Ok(()));
        drop(r);
        let mut map = self.preparing.lock().unwrap_or_else(|e| e.into_inner());
        if map.get(&segment).is_some_and(|q| Arc::ptr_eq(q, &p)) {
            map.remove(&segment);
        }
        result.map_err(|(kind, message)| io::Error::new(kind, message))
    }

    /// Zero-fills a new segment off the write path: through `O_DIRECT`
    /// where the filesystem allows it, in small chunks, paced to
    /// `prepare_bytes_per_sec`, one preparation at a time per process.
    fn fill(dir: &Path, path: &Path, len: u64, opts: &LocalOptions) -> io::Result<()> {
        static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());
        let _turn = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
        let mut o = OpenOptions::new();
        o.read(true).write(true).create(true).truncate(true);
        #[cfg(target_os = "linux")]
        let direct = {
            use std::os::unix::fs::OpenOptionsExt;
            let mut d = o.clone();
            d.custom_flags(libc::O_DIRECT);
            d.open(path).ok()
        };
        #[cfg(not(target_os = "linux"))]
        let direct: Option<File> = None;
        let mut direct_fill = direct.is_some();
        let mut file = match direct {
            Some(f) => f,
            None => o.open(path)?,
        };
        if matches!(
            opts.preallocate,
            crate::local::Preallocate::Zero | crate::local::Preallocate::ShortFirst
        ) {
            const CHUNK: usize = 256 << 10;
            let zeros = aligned_zeros(CHUNK);
            let started = Instant::now();
            let rate = opts.prepare_bytes_per_sec.max(1) as f64;
            let mut off = 0u64;
            while off < len {
                let n = (len - off).min(CHUNK as u64) as usize;
                match file.write_all_at(&zeros[..n], off) {
                    Ok(()) => {}
                    // A filesystem that took the O_DIRECT open but not the
                    // write: the rest goes buffered (it is all zeros).
                    Err(e) if direct_fill && e.raw_os_error() == Some(libc::EINVAL) => {
                        direct_fill = false;
                        file = OpenOptions::new().read(true).write(true).open(path)?;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
                off += n as u64;
                let due = std::time::Duration::from_secs_f64(off as f64 / rate);
                if let Some(ahead) = due.checked_sub(started.elapsed()) {
                    std::thread::sleep(ahead);
                }
            }
        } else {
            file.set_len(len)?;
        }
        flush_file(&file, opts.flush)?;
        sync_dir(dir, opts.flush)
    }

    fn path(&self, segment: u64) -> PathBuf {
        self.dir.join(format!("{segment:020}.{}", self.ext))
    }

    fn file(&self, segment: u64) -> io::Result<Arc<File>> {
        let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(f) = open.get(&segment) {
            return Ok(f.clone());
        }
        let f = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(self.path(segment))?,
        );
        open.insert(segment, f.clone());
        while open.len() > 4 {
            if let Some((&first, _)) = open.iter().next() {
                open.remove(&first);
            }
        }
        Ok(f)
    }

    fn forget(&self, segment: u64) {
        self.open
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&segment);
        if let Some(d) = &self.direct {
            d.forget(segment);
        }
    }
}

impl LogDevice for FileDevice {
    fn is_ready(&self, segment: u64) -> bool {
        let job = self
            .preparing
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&segment)
            .cloned();
        job.is_none_or(|p| p.result.lock().unwrap_or_else(|e| e.into_inner()).is_some())
    }

    fn segments(&self) -> io::Result<Vec<u64>> {
        let entries = match fs::read_dir(&self.dir) {
            Ok(d) => d,
            // A blob area is created with its first file.
            Err(e) if e.kind() == io::ErrorKind::NotFound && self.ext != "wal" => {
                return Ok(Vec::new());
            }
            Err(e) => return Err(e),
        };
        let mut v: Vec<u64> = entries
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                e.file_name()
                    .to_str()?
                    .strip_suffix(self.ext)?
                    .strip_suffix('.')?
                    .parse()
                    .ok()
            })
            .collect();
        v.sort_unstable();
        Ok(v)
    }

    fn len(&self, segment: u64) -> io::Result<u64> {
        self.ready(segment)?;
        Ok(fs::metadata(self.path(segment))?.len())
    }

    fn read(&self, segment: u64, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.ready(segment)?;
        let f = self.file(segment)?;
        let size = f.metadata()?.len();
        let end = offset.saturating_add(len as u64).min(size);
        let mut buf = vec![0; end.saturating_sub(offset) as usize];
        f.read_exact_at(&mut buf, offset.min(end))?;
        Ok(buf)
    }

    fn create(&self, segment: u64, len: u64) -> io::Result<()> {
        self.ready(segment)?;
        if !self.dir.exists() {
            fs::create_dir_all(&self.dir)?;
            if let Some(parent) = self.dir.parent() {
                sync_dir(parent, self.opts.flush)?;
            }
        }
        let path = self.path(segment);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        match self.opts.preallocate {
            crate::local::Preallocate::Zero | crate::local::Preallocate::ShortFirst => {
                let zeros = vec![0u8; 1 << 20];
                let mut off = 0;
                while off < len {
                    let n = (len - off).min(zeros.len() as u64) as usize;
                    file.write_all_at(&zeros[..n], off)?;
                    off += n as u64;
                }
            }
            _ => file.set_len(len)?,
        }
        flush_file(&file, self.opts.flush)?;
        sync_dir(&self.dir, self.opts.flush)?;
        self.forget(segment);
        Ok(())
    }

    fn write(&self, segment: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.ready(segment)?;
        if let Some(d) = self.direct() {
            let path = self.path(segment);
            #[cfg(test)]
            let rejected = self.reject_direct.load(Ordering::Relaxed);
            #[cfg(not(test))]
            let rejected = false;
            let written = if rejected {
                Err(io::Error::from_raw_os_error(libc::EINVAL))
            } else {
                d.write(
                    &path,
                    segment,
                    offset,
                    bytes,
                    self.opts.flush == FlushMethod::Dsync,
                    || self.file(segment),
                )
            };
            match written {
                // The kernel refused the write itself (it transferred nothing,
                // or a prefix of the same bytes): write them again buffered,
                // and keep to buffered writes from now on.
                Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
                    if !self.refused.swap(true, Ordering::Relaxed) {
                        eprintln!(
                            "fusee: an O_DIRECT write to {} (offset {offset}, {} bytes) failed: {e}; \
                             the log continues with buffered writes",
                            path.display(),
                            bytes.len()
                        );
                    }
                }
                other => return other,
            }
        }
        let f = self.file(segment)?;
        if self.opts.flush == FlushMethod::Dsync {
            return crate::local::pwrite_dsync(&f, bytes, offset);
        }
        f.write_all_at(bytes, offset)
    }

    fn sync(&self, segment: u64) -> io::Result<()> {
        if !self.opts.emulated_flush.is_zero() {
            std::thread::sleep(self.opts.emulated_flush);
        }
        if self.opts.flush == FlushMethod::Dsync {
            return Ok(());
        }
        if let Some(d) = self.direct() {
            return flush_file(&*d.file(&self.path(segment), segment)?, self.opts.flush);
        }
        flush_file(&*self.file(segment)?, self.opts.flush)
    }

    fn rename(&self, from: u64, to: u64) -> io::Result<()> {
        self.ready(from)?;
        self.ready(to)?;
        self.forget(from);
        self.forget(to);
        fs::rename(self.path(from), self.path(to))?;
        sync_dir(&self.dir, self.opts.flush)
    }

    fn remove(&self, segment: u64) -> io::Result<()> {
        self.ready(segment)?;
        self.forget(segment);
        fs::remove_file(self.path(segment))?;
        sync_dir(&self.dir, self.opts.flush)
    }

    fn prepare(&self, segment: u64, len: u64) -> io::Result<()> {
        self.ready(segment)?;
        let (dir, path, opts) = (self.dir.clone(), self.path(segment), self.opts.clone());
        // Registered before the fill starts, so nothing finds the segment
        // missing from the map while it is being made.
        let p = Arc::new(Preparation::default());
        self.preparing
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(segment, p.clone());
        let job = std::thread::Builder::new()
            .name("fusee-segment".into())
            .spawn(move || {
                let r = Self::fill(&dir, &path, len, &opts);
                *p.result.lock().unwrap_or_else(|e| e.into_inner()) =
                    Some(r.map_err(|e| (e.kind(), e.to_string())));
                p.done.notify_all();
            });
        let job = match job {
            Ok(j) => j,
            Err(e) => {
                self.preparing
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&segment);
                return Err(e);
            }
        };
        let mut threads = self.preparers.lock().unwrap_or_else(|e| e.into_inner());
        threads.retain(|t| !t.is_finished());
        threads.push(job);
        Ok(())
    }
}

impl Drop for FileDevice {
    fn drop(&mut self) {
        let jobs = std::mem::take(&mut *self.preparers.lock().unwrap_or_else(|e| e.into_inner()));
        for j in jobs {
            let _ = j.join();
        }
    }
}

/// A segment being made in the background ([`FileDevice::prepare`]): its
/// result once done, for every caller of [`FileDevice::ready`].
#[derive(Default)]
struct Preparation {
    result: Mutex<Option<Result<(), (io::ErrorKind, String)>>>,
    done: Condvar,
}

const DIRECT_ALIGN: usize = 4096;

/// A write's partial last block: `(segment, block offset, bytes)`.
#[cfg_attr(not(all(target_os = "linux", feature = "io-uring")), allow(dead_code))]
type DirectTail = Option<(u64, u64, Vec<u8>)>;

/// `O_DIRECT` writes for a [`FileDevice`]: whole aligned blocks from aligned
/// memory. A write that starts inside a block completes it from the bytes
/// before it. Only the previous write can have put them there, and it has
/// completed (a batch that shares a block with the next one is never in
/// flight together with it), so the cached tail or a reread is exact.
struct Direct {
    files: Mutex<BTreeMap<u64, Arc<File>>>,
    /// The last partial block written: `(segment, block offset, bytes)`.
    tail: Mutex<Option<(u64, u64, Vec<u8>)>>,
}

impl Direct {
    /// `Some` when direct I/O is asked for and the filesystem accepts it.
    fn probe(dir: &Path, opts: &LocalOptions) -> Option<Self> {
        if !opts.direct_io || !cfg!(target_os = "linux") {
            return None;
        }
        let probe = dir.join(".direct-probe");
        let ok = Self::open_direct(&probe).is_ok();
        let _ = fs::remove_file(&probe);
        if !ok {
            eprintln!(
                "fusee: {} does not support O_DIRECT; the log uses buffered writes",
                dir.display()
            );
            return None;
        }
        Some(Self {
            files: Mutex::new(BTreeMap::new()),
            tail: Mutex::new(None),
        })
    }

    #[cfg(target_os = "linux")]
    fn open_direct(path: &Path) -> io::Result<File> {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(libc::O_DIRECT)
            .open(path)
    }

    #[cfg(not(target_os = "linux"))]
    fn open_direct(_path: &Path) -> io::Result<File> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "O_DIRECT needs Linux",
        ))
    }

    fn file(&self, path: &Path, segment: u64) -> io::Result<Arc<File>> {
        let mut files = self.files.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(f) = files.get(&segment) {
            return Ok(f.clone());
        }
        let f = Arc::new(Self::open_direct(path)?);
        files.insert(segment, f.clone());
        while files.len() > 4 {
            if let Some((&first, _)) = files.iter().next() {
                files.remove(&first);
            }
        }
        Ok(f)
    }

    fn forget(&self, segment: u64) {
        self.files
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&segment);
        let mut tail = self.tail.lock().unwrap_or_else(|e| e.into_inner());
        if tail.as_ref().is_some_and(|t| t.0 == segment) {
            *tail = None;
        }
    }

    fn write(
        &self,
        path: &Path,
        segment: u64,
        offset: u64,
        bytes: &[u8],
        dsync: bool,
        buffered: impl FnOnce() -> io::Result<Arc<File>>,
    ) -> io::Result<()> {
        let (f, buf, block, tail) = self.buffer(path, segment, offset, bytes, buffered)?;
        if dsync {
            crate::local::pwrite_dsync(&f, buf.as_ref(), block)?;
        } else {
            f.write_all_at(buf.as_ref(), block)?;
        }
        self.wrote(tail);
        Ok(())
    }

    /// Notes a completed write's partial last block (see [`Self::buffer`]).
    fn wrote(&self, tail: Option<(u64, u64, Vec<u8>)>) {
        *self.tail.lock().unwrap_or_else(|e| e.into_inner()) = tail;
    }

    /// The aligned blocks a write of `bytes` at `offset` takes (completed
    /// from the bytes before it in its first block), the file, where they
    /// start, and the partial last block to note once the write completed
    /// ([`Self::wrote`]).
    #[allow(clippy::type_complexity)]
    fn buffer(
        &self,
        path: &Path,
        segment: u64,
        offset: u64,
        bytes: &[u8],
        buffered: impl FnOnce() -> io::Result<Arc<File>>,
    ) -> io::Result<(Arc<File>, AlignedBuf, u64, Option<(u64, u64, Vec<u8>)>)> {
        let align = DIRECT_ALIGN as u64;
        let block = offset / align * align;
        let prefix: Vec<u8> = if block == offset {
            Vec::new()
        } else {
            let cached = self.tail.lock().unwrap_or_else(|e| e.into_inner()).clone();
            match cached {
                Some((s, b, data))
                    if s == segment && b == block && data.len() as u64 == offset - block =>
                {
                    data
                }
                _ => {
                    let mut data = vec![0; (offset - block) as usize];
                    buffered()?.read_exact_at(&mut data, block)?;
                    data
                }
            }
        };
        let total = prefix.len() + bytes.len();
        let padded = total.div_ceil(DIRECT_ALIGN) * DIRECT_ALIGN;
        let mut buf = AlignedBuf::zeroed(padded);
        buf.as_mut()[..prefix.len()].copy_from_slice(&prefix);
        buf.as_mut()[prefix.len()..total].copy_from_slice(bytes);
        let f = self.file(path, segment)?;
        let end = offset + bytes.len() as u64;
        let end_block = end / align * align;
        let tail = (end != end_block).then(|| {
            (
                segment,
                end_block,
                buf.as_ref()[(end_block - block) as usize..total].to_vec(),
            )
        });
        Ok((f, buf, block, tail))
    }
}

/// Heap memory aligned for `O_DIRECT`.
struct AlignedBuf {
    ptr: std::ptr::NonNull<u8>,
    len: usize,
}

impl AlignedBuf {
    fn zeroed(len: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(len.max(DIRECT_ALIGN), DIRECT_ALIGN)
            .expect("aligned layout");
        // SAFETY: the layout has non-zero size.
        let raw = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr =
            std::ptr::NonNull::new(raw).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        Self { ptr, len }
    }

    fn as_mut(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` holds at least `len` initialized bytes owned by self.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    fn as_ref(&self) -> &[u8] {
        // SAFETY: as above.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        let layout = std::alloc::Layout::from_size_align(self.len.max(DIRECT_ALIGN), DIRECT_ALIGN)
            .expect("aligned layout");
        // SAFETY: allocated in `zeroed` with this layout.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), layout) }
    }
}

// SAFETY: the buffer is plain bytes owned by one value.
unsafe impl Send for AlignedBuf {}

/// A zeroed buffer aligned for `O_DIRECT`.
fn aligned_zeros(len: usize) -> &'static [u8] {
    #[repr(C, align(4096))]
    struct Block([u8; 4096]);
    static ZEROS: std::sync::OnceLock<&'static [u8]> = std::sync::OnceLock::new();
    let z = ZEROS.get_or_init(|| {
        let blocks: Vec<Block> = (0..(256 << 10) / 4096).map(|_| Block([0; 4096])).collect();
        let b: &'static mut [Block] = Box::leak(blocks.into_boxed_slice());
        // SAFETY: `Block` is plain bytes; the slice covers exactly the blocks.
        unsafe { std::slice::from_raw_parts(b.as_ptr().cast::<u8>(), b.len() * 4096) }
    });
    &z[..len.min(z.len())]
}

/// Sizes and identity of a log.
#[derive(Clone, Copy, Debug)]
pub struct WalOptions {
    pub group: u32,
    /// Largest segment size; segments grow from `first_segment_bytes` to it.
    pub segment_bytes: u64,
    pub first_segment_bytes: u64,
    /// Spare files kept for reuse; retired segments beyond this are deleted.
    pub spares: usize,
    /// Writes in flight at once. Batches of overlapping writes are padded
    /// to blocks of their own; positions are still published in order.
    pub in_flight: usize,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            group: 0,
            segment_bytes: fusee_core::segment::DEFAULT_SEGMENT_BYTES,
            first_segment_bytes: 1 << 20,
            spares: 2,
            in_flight: log_tuning().in_flight,
        }
    }
}

/// Settings of the shared log's write pipeline, read once from the
/// environment. `FUSEE_LOG_IN_FLIGHT` (1 to 8, default 2): writes in flight at
/// once. `FUSEE_LOG_PAD=always`: on a device whose flush takes at least
/// [`PAD_FOR_COMPANY_US`], pad every lead write, so another can start
/// beside it at once, instead of only when another shard already waits.
/// `FUSEE_LOG_SECOND=now`: start a write beside one in flight as soon as
/// something is staged, without first waiting a quarter of a flush. On an
/// i8g.2xlarge two `RWF_DSYNC` writes in flight take the same 13 µs as one
/// and four take 17 µs (`io-floors`), while the log reached 45 to 59 K
/// writes a second, under one write at a time.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LogTuning {
    pub in_flight: usize,
    pub pad_always: bool,
    pub second_now: bool,
}

pub(crate) fn log_tuning() -> LogTuning {
    static T: std::sync::OnceLock<LogTuning> = std::sync::OnceLock::new();
    *T.get_or_init(|| LogTuning {
        in_flight: std::env::var("FUSEE_LOG_IN_FLIGHT")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .map_or(2, |n| n.clamp(1, 8)),
        pad_always: std::env::var("FUSEE_LOG_PAD").is_ok_and(|v| v == "always"),
        second_now: std::env::var("FUSEE_LOG_SECOND").is_ok_and(|v| v == "now"),
    })
}

/// A log's blob area ([`Wal::open_with_blobs`]): where records longer than
/// `threshold` go instead of into a batch.
pub struct Blobs<D> {
    pub dev: D,
    /// Offload records whose canonical encoding is longer than this many
    /// bytes; 0: offload nothing (references already in the log are still
    /// read).
    pub threshold: u64,
}

/// One write of an offloaded record's body to the blob area. Write it with
/// [`BlobJob::perform`], sync its file, then report it with
/// [`Wal::blobs_written`].
pub struct BlobJob {
    pub id: u64,
    pub file: u64,
    pub offset: u64,
    /// The first write to a new file: create it first.
    pub create: bool,
    data: Arc<Vec<u8>>,
    range: std::ops::Range<usize>,
}

impl BlobJob {
    /// The record's canonical body.
    pub fn bytes(&self) -> &[u8] {
        &self.data[self.range.clone()]
    }

    /// Writes the body (creating its file first if it is new), without
    /// syncing.
    pub fn perform<D: LogDevice>(&self, dev: &D) -> io::Result<()> {
        if self.create {
            dev.create(self.file, 0)?;
        }
        dev.write(self.file, self.offset, self.bytes())
    }
}

impl std::fmt::Debug for BlobJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobJob")
            .field("id", &self.id)
            .field("file", &self.file)
            .field("offset", &self.offset)
            .field("len", &self.range.len())
            .finish()
    }
}

/// A section waiting for the blob writes its references name (`needs`: the
/// last of them), or queued behind one that does.
struct Held {
    batch: Batch,
    needs: u64,
    files: Vec<u64>,
}

/// A blob file: the last batch that references it, and references not yet
/// sealed into a batch.
#[derive(Clone, Copy, Debug, Default)]
struct BlobFile {
    last_ref: u64,
    pending: u64,
}

/// The state of a blob area. Blob files are append-only and never reused:
/// each open appends to a new one. A file is deleted once no batch the log
/// keeps references it.
struct BlobArea<D> {
    dev: D,
    threshold: u64,
    /// Roll to a new file past this size.
    file_bytes: u64,
    /// The file appends go to and its next offset.
    active: Option<(u64, u64)>,
    next_file: u64,
    files: BTreeMap<u64, BlobFile>,
    jobs: std::collections::VecDeque<BlobJob>,
    next_id: u64,
    /// Every job through this one is durable.
    written: u64,
    held: Vec<std::collections::VecDeque<Held>>,
    /// Records offloaded and bytes written to blob files, this open.
    records: u64,
    bytes: u64,
}

impl<D: LogDevice> BlobArea<D> {
    /// Space for a body of `len` bytes: `(file, offset, new file)`.
    fn alloc(&mut self, len: u64) -> (u64, u64, bool) {
        let span = len.div_ceil(BLOB_ALIGN) * BLOB_ALIGN;
        if let Some((file, at)) = self.active
            && at + span <= self.file_bytes
        {
            self.active = Some((file, at + span));
            return (file, at, false);
        }
        let file = self.next_file;
        self.next_file += 1;
        self.active = Some((file, span));
        self.files.insert(file, BlobFile::default());
        (file, 0, true)
    }

    /// Deletes every file that no batch at or after `first` references and
    /// no section waiting to be sealed does (the active one too: the next
    /// blob then starts a new file).
    fn retire(&mut self, first: u64) -> io::Result<()> {
        let dead: Vec<u64> = self
            .files
            .iter()
            .filter(|(_, f)| f.pending == 0 && f.last_ref < first)
            .map(|(n, _)| *n)
            .collect();
        for n in dead {
            self.files.remove(&n);
            if self.active.is_some_and(|a| a.0 == n) {
                self.active = None;
            }
            match self.dev.remove(n) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

/// What a log's blob area holds ([`Wal::blob_stats`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlobStats {
    /// Blob files kept.
    pub files: usize,
    /// Records offloaded and bytes written since the log was opened.
    pub records: u64,
    pub bytes: u64,
    /// Sections waiting for their blobs, and blob writes not yet started.
    pub held: usize,
    pub jobs: usize,
}

/// One shard's sections found by recovery: raw bytes, decoded on demand.
#[derive(Default)]
struct Recovered {
    sections: Vec<(Lsn, u32, Vec<u8>)>,
    last: Lsn,
}

/// A batch written and not yet durable.
#[derive(Debug)]
pub struct InFlight {
    segment: u64,
    seq: u64,
    shards: Vec<(u32, Lsn)>,
    /// Bytes not written yet (sealed with [`Wal::seal`]): a new segment's
    /// header, then the batch at `offset`.
    io: Option<(Option<Vec<u8>>, u64, Vec<u8>)>,
}

impl InFlight {
    /// The segment a sync must cover.
    pub fn segment(&self) -> u64 {
        self.segment
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }
}

/// A write in flight: whether it was padded, and once synced, the shards
/// and positions it made durable.
#[derive(Debug)]
struct Pending {
    seq: u64,
    padded: bool,
    done: Option<Vec<(u32, Lsn)>>,
}

#[derive(Clone, Copy, Debug)]
struct LiveSegment {
    number: u64,
    base_seq: u64,
    len: u64,
}

/// The log of one device: recovery, batching and segment lifecycle. Not
/// thread-safe; [`SharedWal`] adds the owner protocol for threads.
pub struct Wal<D: LogDevice> {
    dev: D,
    opts: WalOptions,
    writer: WalWriter,
    /// Highest batch sequence durable, with everything before it.
    acked: u64,
    staged: Vec<Option<Batch>>,
    durable: Vec<Lsn>,
    live: Vec<LiveSegment>,
    spares: Vec<u64>,
    /// The active segment whose successor a spare was found large enough
    /// for (see `prepare_ahead`): checked once per segment.
    spares_fit: Option<u64>,
    /// Writes not yet durable, oldest first.
    in_flight: std::collections::VecDeque<Pending>,
    /// A new segment being prepared ahead of the roll: `(number, size)`.
    prepared: Option<(u64, u64)>,
    next_number: u64,
    /// Per shard: the LSN its oldest retained checkpoint covers (`None`
    /// until it reports one: nothing is retired before then), and the
    /// batches holding its records after that, oldest first, with the last
    /// LSN each holds.
    retain_lsn: Vec<Option<Lsn>>,
    holding: Vec<std::collections::VecDeque<(u64, Lsn)>>,
    /// The first batch a continuous backup has not uploaded yet
    /// ([`crate::backup`]): segments holding it or later are not retired.
    /// `None` without a backup.
    backup_needs: Option<u64>,

    recovered: Vec<Recovered>,
    rng: u64,
    pub(crate) bytes_written: u64,
    /// Batches sealed, and segments rolled into.
    pub(crate) writes: u64,
    pub(crate) rolls: u64,
    /// When the active segment was rolled into (ms), `bytes_written` then,
    /// and the time of the latest write (ms): its fill rate.
    rolled_at: (u64, u64, u64),
    /// The blob area, for a log opened with one.
    blobs: Option<BlobArea<D>>,
    /// Per shard: the blob files the staged section's references name, one
    /// entry per reference.
    staged_refs: Vec<Vec<u64>>,
    /// The most bytes one batch carries ([`MAX_BATCH`], which recovery
    /// enforces; tests lower it).
    batch_limit: usize,
    /// The shard whose section the next batch takes first: the one after
    /// the shard whose section filled the last batch (0 while batches hold
    /// everything staged), so shards take turns when batches are full.
    seal_from: usize,
}

fn io_other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

/// Cuts the leading records of a staged section that fit in `room` bytes
/// off into a batch of their own; `b` keeps the rest, which follow them.
/// Returns them with how many of them are references to offloaded records,
/// or `None` when not even the section's first record fits.
fn split_section(b: &mut Batch, room: usize) -> io::Result<Option<(Batch, usize)>> {
    let total = b.section.len();
    let mut r = fusee_core::wire::Reader::new(&b.section);
    let (mut end, mut records, mut refs) = (0usize, 0u32, 0usize);
    while !r.is_empty() {
        let body = r
            .blob()
            .map_err(|e| io_other(format!("staged section: {e}")))?;
        let at = total - r.remaining();
        if at > room {
            break;
        }
        end = at;
        records += 1;
        refs += usize::from(fusee_core::wal::is_offloaded(body));
    }
    if records == 0 {
        return Ok(None);
    }
    if records > b.records {
        return Err(io_other("staged section holds more records than it says"));
    }
    let head = Batch {
        chunks: Vec::new(),
        first_lsn: b.first_lsn,
        last_lsn: b.first_lsn + u64::from(records) - 1,
        records,
        bytes: end as u64,
        section: b.section[..end].to_vec(),
    };
    b.section.drain(..end);
    b.first_lsn += u64::from(records);
    b.records -= records;
    b.bytes = b.bytes.saturating_sub(end as u64);
    Ok(Some((head, refs)))
}

impl<D: LogDevice> Wal<D> {
    /// Opens the log: follows the chain through the segments, keeps each
    /// shard's sections, makes everything accepted durable, and positions the
    /// next write right after the chain. A fresh device gets its first
    /// segment.
    pub fn open(dev: D, shards: u32, opts: WalOptions, life: u64, now_ms: u64) -> io::Result<Self> {
        Self::open_inner(dev, None, shards, opts, life, now_ms)
    }

    /// [`open`](Self::open) with a blob area: references to offloaded
    /// records are read from it, blob files no batch of the chain
    /// references are deleted, and records longer than `blobs.threshold`
    /// are offloaded from now on.
    pub fn open_with_blobs(
        dev: D,
        blobs: Blobs<D>,
        shards: u32,
        opts: WalOptions,
        life: u64,
        now_ms: u64,
    ) -> io::Result<Self> {
        Self::open_inner(dev, Some(blobs), shards, opts, life, now_ms)
    }

    fn open_inner(
        dev: D,
        blobs: Option<Blobs<D>>,
        shards: u32,
        opts: WalOptions,
        life: u64,
        now_ms: u64,
    ) -> io::Result<Self> {
        let mut rng = life | 1;
        // Blob files on disk; the chain's references to them are collected
        // while it is read (only when there are any, so a log that never
        // offloaded pays nothing for it).
        let blob_files = match &blobs {
            Some(b) => b.dev.segments()?,
            None => Vec::new(),
        };
        let scan_refs = !blob_files.is_empty();
        let mut blob_refs: BTreeMap<u64, u64> = BTreeMap::new();
        let mut recovered: Vec<Recovered> = (0..shards).map(|_| Recovered::default()).collect();
        let mut holding: Vec<std::collections::VecDeque<(u64, Lsn)>> =
            (0..shards).map(|_| Default::default()).collect();
        let mut live: Vec<LiveSegment> = Vec::new();
        let mut spares = Vec::new();
        let mut chain: Option<(u64, u64)> = None; // (last seq, checksum)
        let mut resume: Option<(u64, u64, u64)> = None; // (segment, salt, end)
        // Segments with a readable header that the chain does not pass
        // through: a roll whose earlier batch never became durable, so the
        // chain ended before it. Only chain segments may keep a valid header
        // (retirement and this recovery zero the others), so the first valid
        // header in number order always starts the chain.
        let mut stale: Vec<u64> = Vec::new();
        let numbers = dev.segments()?;
        let next_number = numbers.last().map_or(1, |n| n + 1);
        for number in numbers {
            let len = dev.len(number)?;
            let head = dev.read(number, 0, WAL_HEADER_BYTES as usize)?;
            let header = match WalHeader::decode(&head) {
                Ok(Some(h)) if h.segment == number => h,
                Err(e @ fusee_core::wal::WalError::UnsupportedFormat(_)) => {
                    return Err(io_other(e));
                }
                Ok(None) => {
                    // Never activated or retired.
                    spares.push(number);
                    continue;
                }
                // A header torn by the write that would have activated the
                // segment, or one from another file: nothing acknowledged.
                _ => {
                    stale.push(number);
                    continue;
                }
            };
            // The chain continues only into a segment whose header names the
            // batch it ended with; any other is skipped, in any order.
            if let Some((seq, checksum)) = chain
                && (header.base_seq != seq + 1 || header.base_prev != checksum)
            {
                stale.push(number);
                continue;
            }
            let mut first_read = 256 << 10;
            let mut bytes = dev.read(number, 0, first_read)?;
            let Some(mut scanner) =
                WalScanner::start(&bytes, bytes.len() as u64 >= len).map_err(io_other)?
            else {
                spares.push(number);
                continue;
            };
            let mut visit_err = None;
            let scanned = loop {
                let complete = bytes.len() as u64 >= len;
                let progress = scanner.advance(&bytes, complete, len as usize, &mut |b| {
                    for s in b.sections() {
                        let Some(r) = recovered.get_mut(s.shard as usize) else {
                            visit_err.get_or_insert_with(|| {
                                format!(
                                    "log batch {} names shard {} of {shards}",
                                    b.header.seq, s.shard
                                )
                            });
                            continue;
                        };
                        if r.last != 0 && s.first_lsn != r.last + 1 {
                            visit_err.get_or_insert_with(|| {
                                format!(
                                    "shard {} records {}..{} are missing from the log",
                                    s.shard,
                                    r.last + 1,
                                    s.first_lsn.saturating_sub(1)
                                )
                            });
                        }
                        r.last = s.first_lsn + u64::from(s.count) - 1;
                        r.sections.push((s.first_lsn, s.count, s.records.to_vec()));
                        holding[s.shard as usize].push_back((b.header.seq, r.last));
                        if scan_refs
                            && let Err(e) = for_each_offload(s.records, |_, r| {
                                blob_refs.insert(r.file, b.header.seq);
                            })
                        {
                            visit_err.get_or_insert_with(|| {
                                format!("log batch {}, shard {}: {e}", b.header.seq, s.shard)
                            });
                        }
                    }
                });
                match progress {
                    Progress::Done(s) => break s,
                    Progress::NeedMore => {
                        first_read = (first_read * 2).min(16 << 20);
                        let more = dev.read(number, bytes.len() as u64, first_read)?;
                        if more.is_empty() {
                            return Err(io_other(format!(
                                "reading log segment {number} made no progress"
                            )));
                        }
                        bytes.extend_from_slice(&more);
                    }
                }
            };
            if let Some(e) = visit_err {
                return Err(io_other(e));
            }
            if let End::Damaged { at, later_seq } = scanned.tail {
                return Err(io_other(format!(
                    "log segment {number} is damaged at offset {at} (batch {}), but batch \
                     {later_seq} after it says it had been acknowledged: acknowledged records \
                     were lost. Restore from a replica or backup.",
                    scanned.last_seq + 1
                )));
            }
            live.push(LiveSegment {
                number,
                base_seq: header.base_seq,
                len,
            });
            chain = Some((scanned.last_seq, scanned.last_checksum));
            resume = Some((number, header.salt, scanned.end as u64));
        }
        // A segment the chain skipped must not prove that the chain lost an
        // acknowledged batch: none of its batches may say a batch past the
        // chain's end had been acknowledged. Then its header is zeroed, so
        // it can never be taken for chain.
        let end = chain.map_or(0, |c| c.0);
        for &number in &stale {
            let len = dev.len(number)? as usize;
            let bytes = dev.read(number, 0, len)?;
            if let Ok(Some(mut scanner)) = WalScanner::start(&bytes, true) {
                let mut proof = None;
                let _ = scanner.advance(&bytes, true, len, &mut |b| {
                    if b.header.acked > end {
                        proof.get_or_insert(b.header.seq);
                    }
                });
                if let Some(seq) = proof {
                    return Err(io_other(format!(
                        "log segment {number} holds batch {seq}, written after batch {} had been \
                         acknowledged, but the log ends at batch {end}: acknowledged records were \
                         lost. Restore from a replica or backup.",
                        end + 1
                    )));
                }
            }
            dev.write(number, 0, &vec![0; WAL_HEADER_BYTES as usize])?;
            dev.sync(number)?;
            spares.push(number);
        }
        // Everything accepted may have been in the page cache only.
        for s in &live {
            dev.sync(s.number)?;
        }
        let mut next_number = next_number;
        let writer = match (resume, chain) {
            (Some((segment, salt, end)), Some((seq, checksum))) => {
                let len = live.last().map_or(0, |s| s.len);
                WalWriter::resume(opts.group, life, segment, salt, len, end, seq + 1, checksum)
            }
            _ => {
                // A new log: its first segment, header durable before use.
                let number = spares.pop().unwrap_or_else(|| {
                    next_number += 1;
                    next_number - 1
                });
                let len = opts.first_segment_bytes.min(opts.segment_bytes);
                dev.create(number, len)?;
                rng = xorshift(rng);
                let header = WalHeader {
                    group: opts.group,
                    segment: number,
                    salt: rng,
                    base_seq: 1,
                    base_prev: 0,
                    created_ms: now_ms,
                };
                dev.write(number, 0, &header.encode())?;
                dev.sync(number)?;
                live.push(LiveSegment {
                    number,
                    base_seq: 1,
                    len,
                });
                WalWriter::resume(
                    opts.group,
                    life,
                    number,
                    header.salt,
                    len,
                    WAL_HEADER_BYTES,
                    1,
                    0,
                )
            }
        };
        let acked = writer.next_seq() - 1;
        let durable = recovered.iter().map(|r| r.last).collect();
        let blobs = match blobs {
            None => None,
            Some(Blobs {
                dev: bdev,
                threshold,
            }) => {
                let mut files = BTreeMap::new();
                for &n in &blob_files {
                    match blob_refs.get(&n) {
                        Some(&last_ref) => {
                            files.insert(
                                n,
                                BlobFile {
                                    last_ref,
                                    pending: 0,
                                },
                            );
                        }
                        // Written for a batch that never became part of
                        // the chain, or referenced only by retired ones.
                        None => bdev.remove(n)?,
                    }
                }
                Some(BlobArea {
                    dev: bdev,
                    threshold,
                    file_bytes: opts.segment_bytes.max(1 << 20),
                    active: None,
                    next_file: blob_files.last().map_or(1, |n| n + 1),
                    files,
                    jobs: std::collections::VecDeque::new(),
                    next_id: 1,
                    written: 0,
                    held: (0..shards).map(|_| Default::default()).collect(),
                    records: 0,
                    bytes: 0,
                })
            }
        };
        Ok(Self {
            dev,
            opts,
            writer,
            acked,
            staged: (0..shards).map(|_| None).collect(),
            durable,
            live,
            spares,
            spares_fit: None,
            prepared: None,
            in_flight: std::collections::VecDeque::new(),
            next_number,
            retain_lsn: vec![None; shards as usize],
            holding,
            backup_needs: None,
            recovered,
            rng,
            bytes_written: 0,
            writes: 0,
            rolls: 0,
            rolled_at: (now_ms, 0, now_ms),
            blobs,
            staged_refs: (0..shards).map(|_| Vec::new()).collect(),
            batch_limit: MAX_BATCH,
            seal_from: 0,
        })
    }

    /// Lowers the most bytes a batch carries, so a test fills batches with
    /// small records.
    #[cfg(test)]
    fn limit_batches(&mut self, bytes: usize) {
        self.batch_limit = bytes.min(MAX_BATCH);
    }

    /// The shard's records after `after`, in order, and its last LSN in the
    /// log (`after` if it has none later). Fails if records right after
    /// `after` are missing.
    pub fn take_recovered(
        &mut self,
        shard: u32,
        after: Lsn,
    ) -> io::Result<(Vec<(Lsn, RecordBuf)>, Lsn)> {
        let r = self
            .recovered
            .get_mut(shard as usize)
            .ok_or_else(|| io_other(format!("no shard {shard}")))?;
        let mut out = Vec::new();
        let mut expected = after + 1;
        let blobs = self.blobs.as_ref().map(|b| &b.dev);
        for (first, count, bytes) in std::mem::take(&mut r.sections) {
            let last = first + u64::from(count) - 1;
            if last <= after {
                continue;
            }
            if first > expected {
                return Err(io_other(format!(
                    "shard {shard} records {expected}..{} are missing from the log",
                    first - 1
                )));
            }
            let s = SectionRef {
                shard,
                first_lsn: first,
                count,
                records: &bytes,
            };
            out.extend(resolve_records(&s, after, blobs)?);
            expected = last + 1;
        }
        let last = r.last.max(after);
        self.durable[shard as usize] = self.durable[shard as usize].max(last);
        Ok((out, last))
    }

    /// The sections recovery kept for `shard`, undecoded, taken: a second
    /// call returns none. For a log whose shards are recovered elsewhere
    /// (server mode ships them to the worker that runs the shard).
    pub fn take_recovered_sections(&mut self, shard: u32) -> Vec<RawSection> {
        self.recovered
            .get_mut(shard as usize)
            .map(|r| std::mem::take(&mut r.sections))
            .unwrap_or_default()
    }

    /// Stages a shard's section for the next write, offloading its records
    /// longer than the blob area's threshold (those and every later section
    /// of the shard wait until the blobs are durable: see
    /// [`blobs_written`](Self::blobs_written)).
    pub fn stage(&mut self, shard: u32, batch: Batch) -> io::Result<()> {
        let large = match &self.blobs {
            Some(b) if b.threshold > 0 && batch.bytes > b.threshold => {
                find_large(&batch.section, b.threshold as usize).map_err(io_other)?
            }
            _ => Vec::new(),
        };
        self.stage_found(shard, batch, large)
    }

    /// [`stage`](Self::stage) with the section's large records already
    /// found ([`find_large`], which the shard's own thread can run).
    pub fn stage_found(
        &mut self,
        shard: u32,
        mut batch: Batch,
        large: Vec<Large>,
    ) -> io::Result<()> {
        let s = shard as usize;
        if s >= self.staged.len() {
            return Err(io_other(format!("no shard {shard}")));
        }
        let Some(area) = self
            .blobs
            .as_mut()
            .filter(|a| !a.held[s].is_empty() || !large.is_empty())
        else {
            return self.stage_plain(shard, batch, Vec::new());
        };
        if large.is_empty() {
            // Behind a section that waits for its blobs.
            area.held[s].push_back(Held {
                batch,
                needs: 0,
                files: Vec::new(),
            });
            return Ok(());
        }
        let data = Arc::new(std::mem::take(&mut batch.section));
        let first = &large[0];
        let prefix = (first.index > 0).then(|| Batch {
            chunks: Vec::new(),
            first_lsn: batch.first_lsn,
            last_lsn: batch.first_lsn + u64::from(first.index) - 1,
            records: first.index,
            bytes: first.at as u64,
            section: data[..first.at].to_vec(),
        });
        let mut rest = Vec::with_capacity(
            data.len() - first.at - large.iter().map(|l| l.body.len()).sum::<usize>()
                + 32 * large.len(),
        );
        let mut pos = first.at;
        let mut files = Vec::with_capacity(large.len());
        let mut needs = 0;
        for l in &large {
            rest.extend_from_slice(&data[pos..l.at]);
            let len = l.body.len() as u64;
            let (file, offset, create) = area.alloc(len);
            let id = area.next_id;
            area.next_id += 1;
            area.jobs.push_back(BlobJob {
                id,
                file,
                offset,
                create,
                data: data.clone(),
                range: l.body.clone(),
            });
            OffloadRef {
                kind: data[l.body.start],
                file,
                offset,
                len,
                crc: l.crc,
            }
            .encode(&mut rest);
            if let Some(f) = area.files.get_mut(&file) {
                f.pending += 1;
            }
            files.push(file);
            area.records += 1;
            area.bytes += len;
            needs = id;
            pos = l.body.end;
        }
        rest.extend_from_slice(&data[pos..]);
        let held = Held {
            batch: Batch {
                chunks: Vec::new(),
                first_lsn: batch.first_lsn + u64::from(first.index),
                last_lsn: batch.last_lsn,
                records: batch.records - first.index,
                bytes: rest.len() as u64,
                section: rest,
            },
            needs,
            files,
        };
        let waiting = !area.held[s].is_empty();
        match prefix {
            Some(p) if waiting => area.held[s].push_back(Held {
                batch: p,
                needs: 0,
                files: Vec::new(),
            }),
            Some(p) => {
                area.held[s].push_back(held);
                return self.stage_plain(shard, p, Vec::new());
            }
            None => {}
        }
        area.held[s].push_back(held);
        Ok(())
    }

    /// Blob writes to perform, oldest first (taken: a second call returns
    /// only newer ones).
    pub fn take_blob_jobs(&mut self) -> Vec<BlobJob> {
        self.blobs
            .as_mut()
            .map(|b| b.jobs.drain(..).collect())
            .unwrap_or_default()
    }

    pub fn has_blob_jobs(&self) -> bool {
        self.blobs.as_ref().is_some_and(|b| !b.jobs.is_empty())
    }

    /// Every blob write through job `through` is durable (written and its
    /// file synced): the sections waiting for them are staged.
    pub fn blobs_written(&mut self, through: u64) -> io::Result<()> {
        let Some(area) = self.blobs.as_mut() else {
            return Ok(());
        };
        area.written = area.written.max(through);
        let written = area.written;
        let mut ready: Vec<(u32, Held)> = Vec::new();
        for (s, q) in area.held.iter_mut().enumerate() {
            while q.front().is_some_and(|h| h.needs <= written) {
                ready.push((s as u32, q.pop_front().expect("front exists")));
            }
        }
        for (s, h) in ready {
            self.stage_plain(s, h.batch, h.files)?;
        }
        Ok(())
    }

    /// Performs every blob write queued, syncs the files and stages the
    /// sections waiting for them, on this thread (for single-threaded
    /// users; [`SharedWal`] writes blobs on a thread of their own).
    pub fn flush_blobs(&mut self) -> io::Result<()> {
        let jobs = self.take_blob_jobs();
        let Some(last) = jobs.last().map(|j| j.id) else {
            return Ok(());
        };
        let dev = &self.blobs.as_ref().expect("jobs come from a blob area").dev;
        let mut files: Vec<u64> = Vec::new();
        for j in &jobs {
            j.perform(dev)?;
            if !files.contains(&j.file) {
                files.push(j.file);
            }
        }
        for f in files {
            dev.sync(f)?;
        }
        self.blobs_written(last)
    }

    /// The blob area's device, for a log opened with one.
    pub fn blob_device(&self) -> Option<&D> {
        self.blobs.as_ref().map(|b| &b.dev)
    }

    pub fn blob_stats(&self) -> BlobStats {
        self.blobs
            .as_ref()
            .map_or_else(BlobStats::default, |b| BlobStats {
                files: b.files.len(),
                records: b.records,
                bytes: b.bytes,
                held: b.held.iter().map(|q| q.len()).sum(),
                jobs: b.jobs.len(),
            })
    }

    /// Stages a section for the next write as it is. Sections of one shard
    /// must follow each other. `refs`: the blob files its references name.
    fn stage_plain(&mut self, shard: u32, batch: Batch, refs: Vec<u64>) -> io::Result<()> {
        let slot = self
            .staged
            .get_mut(shard as usize)
            .ok_or_else(|| io_other(format!("no shard {shard}")))?;
        self.staged_refs[shard as usize].extend(refs);
        match slot {
            Some(s) => {
                if batch.first_lsn != s.last_lsn + 1 {
                    return Err(io_other("staged sections are not consecutive"));
                }
                s.section.extend_from_slice(&batch.section);
                s.last_lsn = batch.last_lsn;
                s.records += batch.records;
                s.bytes += batch.bytes;
            }
            None => *slot = Some(batch),
        }
        Ok(())
    }

    pub fn has_staged(&self) -> bool {
        self.staged.iter().any(Option::is_some)
    }

    /// The shards with a section staged.
    pub fn staged_shards(&self) -> impl Iterator<Item = u32> + '_ {
        self.staged
            .iter()
            .enumerate()
            .filter(|(_, s)| s.is_some())
            .map(|(i, _)| i as u32)
    }

    /// Writes everything staged and makes it durable: one batch, or as many
    /// as what is staged fills (a batch holds at most [`MAX_BATCH`] bytes).
    /// Returns the shards it wrote for.
    pub fn write(&mut self, now_ms: u64) -> io::Result<Vec<u32>> {
        if !self.in_flight.is_empty() {
            return Err(io_other("a write is still in flight"));
        }
        let mut shards = Vec::new();
        while let Some(w) = self.begin_write(now_ms, false)? {
            for s in self.finish_write(w)? {
                if !shards.contains(&s) {
                    shards.push(s);
                }
            }
        }
        Ok(shards)
    }

    /// Whether another write may start now: fewer than the limit are in
    /// flight, and the newest one in flight was padded to a block of its own
    /// (a write that shares its last block must complete first).
    pub fn can_begin(&self) -> bool {
        self.in_flight.len() < self.opts.in_flight.max(1)
            && self.in_flight.back().is_none_or(|w| w.padded)
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    #[cfg(test)]
    fn complete_is_pending(&self) -> bool {
        !self.in_flight.is_empty()
    }

    /// Seals everything staged into one batch and writes it, without making
    /// it durable; complete it with [`finish_write`](Self::finish_write) or
    /// [`complete`](Self::complete) after a sync. `overlap`: the next write
    /// may start before this one completes (the log is busy), so the batch
    /// is padded to a block of its own; otherwise it packs, and the next
    /// write waits for it.
    pub fn begin_write(&mut self, now_ms: u64, overlap: bool) -> io::Result<Option<InFlight>> {
        let Some(mut w) = self.seal(now_ms, overlap)? else {
            return Ok(None);
        };
        Self::perform_write(&self.dev, &mut w)?;
        Ok(Some(w))
    }

    /// Writes a sealed batch (and its segment's header) without syncing.
    fn perform_write(dev: &D, w: &mut InFlight) -> io::Result<()> {
        if let Some((header, offset, bytes)) = w.io.take() {
            if let Some(h) = header {
                dev.write(w.segment, 0, &h)?;
            }
            dev.write(w.segment, offset, &bytes)?;
        }
        Ok(())
    }

    /// Writes a sealed batch and makes it durable. Needs only the device, so
    /// it can run outside whatever lock guards the log; then
    /// [`complete`](Self::complete) it.
    pub fn perform(dev: &D, w: &mut InFlight) -> io::Result<()> {
        Self::perform_write(dev, w)?;
        dev.sync(w.segment)
    }

    /// Like [`begin_write`](Self::begin_write), but leaves the writing to
    /// [`perform`](Self::perform): the batch is placed, positioned and
    /// counted in flight; its bytes travel in the returned value.
    pub fn seal(&mut self, now_ms: u64, overlap: bool) -> io::Result<Option<InFlight>> {
        if !self.can_begin() {
            return Ok(None);
        }
        let pipelined = overlap && self.opts.in_flight > 1;
        // A batch carries at most `batch_limit` bytes: recovery refuses a
        // longer one, so what was acknowledged in it would be lost. What is
        // staged beyond that (a burst of large records, or everything
        // staged during one slow flush) goes into the next batches. Shards
        // are taken in turn from `seal_from`: whole sections while they
        // fit; the first that does not gives its leading records, cut at a
        // record boundary, and closes the batch. The next batch starts
        // after that shard, so the others go before the rest of a long
        // section and no shard waits behind another for more than a round.
        let shards = self.staged.len();
        let mut room = self.batch_limit.saturating_sub(BATCH_HEADER);
        let mut staged: Vec<(u32, Batch)> = Vec::new();
        // The shard whose section was cut, with the references in the part
        // this batch carries.
        let mut cut: Option<(u32, usize)> = None;
        let mut next_from = 0;
        for i in (0..shards).map(|k| (self.seal_from + k) % shards) {
            let Some(b) = self.staged[i].as_mut() else {
                continue;
            };
            let need = SECTION_HEADER + b.section.len();
            if need <= room {
                room -= need;
                if let Some(b) = self.staged[i].take() {
                    staged.push((i as u32, b));
                }
                continue;
            }
            match split_section(b, room.saturating_sub(SECTION_HEADER))? {
                Some((head, refs)) => {
                    staged.push((i as u32, head));
                    cut = Some((i as u32, refs));
                    next_from = (i + 1) % shards;
                }
                // Not one record fits in what is left: this shard goes
                // first in the next batch.
                None if !staged.is_empty() => next_from = i,
                // Only a record longer than a batch, which no writer of
                // sections appends (`SectionWriter::append` refuses it).
                None => {
                    return Err(io_other(format!(
                        "shard {i} staged a record longer than a log batch ({} bytes)",
                        self.batch_limit
                    )));
                }
            }
            break;
        }
        if staged.is_empty() {
            return Ok(None);
        }
        self.seal_from = next_from;
        // Sections in shard order, as a batch has always had them.
        staged.sort_unstable_by_key(|(shard, _)| *shard);
        let sections: Vec<SectionRef<'_>> = staged
            .iter()
            .map(|(shard, b)| SectionRef {
                shard: *shard,
                first_lsn: b.first_lsn,
                count: b.records,
                records: &b.section,
            })
            .collect();
        // The same padding the writer gives the batch, here and, after a
        // roll, right after the new segment's header.
        let body = batch_body_len(&sections);
        let size = batch_span(self.writer.offset(), body, pipelined);
        let next = if self.writer.offset() + size > self.current_len() {
            Some(self.next_segment(batch_span(WAL_HEADER_BYTES, body, pipelined))?)
        } else {
            None
        };
        let placed = self
            .writer
            .place(&sections, self.acked, pipelined, now_ms, || {
                next.expect("a next segment was prepared")
            });
        debug_assert!(placed.bytes.len() <= MAX_BATCH, "recovery reads this batch");
        self.writes += 1;
        self.rolled_at.2 = now_ms;
        if let Some(h) = &placed.header {
            self.rolls += 1;
            self.rolled_at = (now_ms, self.bytes_written, now_ms);
            self.live.push(LiveSegment {
                number: placed.segment,
                base_seq: h.base_seq,
                len: next.map_or(0, |n| n.2),
            });
        }
        self.bytes_written += placed.bytes.len() as u64;
        self.prepare_ahead()?;
        self.in_flight.push_back(Pending {
            seq: placed.seq,
            // Whether the next write may start while this one is in flight:
            // only if it cannot share this batch's last block (a pipelined
            // batch too large to pad to its block's end can).
            padded: pipelined && (placed.offset + placed.bytes.len() as u64).is_multiple_of(BLOCK),
            done: None,
        });
        Ok(Some(InFlight {
            segment: placed.segment,
            seq: placed.seq,
            io: Some((
                placed.header.as_ref().map(WalHeader::encode),
                placed.offset,
                placed.bytes,
            )),
            shards: {
                for (s, b) in &staged {
                    self.holding[*s as usize].push_back((placed.seq, b.last_lsn));
                    let all = &mut self.staged_refs[*s as usize];
                    // A section that was cut: only the references in the
                    // records this batch carries (they are listed in the
                    // section's order).
                    let refs: Vec<u64> = match cut {
                        Some((shard, n)) if shard == *s => all.drain(..n.min(all.len())).collect(),
                        _ => std::mem::take(all),
                    };
                    if let Some(area) = self.blobs.as_mut() {
                        for f in refs {
                            if let Some(file) = area.files.get_mut(&f) {
                                file.last_ref = placed.seq;
                                file.pending = file.pending.saturating_sub(1);
                            }
                        }
                    }
                }
                staged.iter().map(|(s, b)| (*s, b.last_lsn)).collect()
            },
        }))
    }

    /// Makes a write durable and publishes the shards' positions. Returns
    /// the shards whose durable position advanced.
    pub fn finish_write(&mut self, w: InFlight) -> io::Result<Vec<u32>> {
        self.dev.sync(w.segment)?;
        Ok(self.complete(w))
    }

    /// Records that a write is durable (its segment was synced after it was
    /// written). Positions are published in batch order: a write that
    /// completes before an earlier one waits for it.
    pub fn complete(&mut self, w: InFlight) -> Vec<u32> {
        if let Some(p) = self.in_flight.iter_mut().find(|p| p.seq == w.seq) {
            p.done = Some(w.shards);
        }
        let mut shards = Vec::new();
        while self.in_flight.front().is_some_and(|p| p.done.is_some()) {
            let p = self.in_flight.pop_front().expect("front exists");
            self.acked = p.seq;
            for (shard, last) in p.done.unwrap_or_default() {
                self.durable[shard as usize] = last;
                if !shards.contains(&shard) {
                    shards.push(shard);
                }
            }
        }
        shards
    }

    fn current_len(&self) -> u64 {
        self.live.last().map_or(0, |s| s.len)
    }

    fn next_size(&self) -> u64 {
        (self.current_len().max(self.opts.first_segment_bytes) * 2).min(self.largest_segment())
    }

    /// The largest segment: [`WalOptions::segment_bytes`], or 256 MiB while
    /// the active segment filled at over 100 MB/s, so a fast log rolls (and
    /// asks shards for checkpoints) a quarter as often.
    fn largest_segment(&self) -> u64 {
        const FAST_BYTES_PER_MS: u64 = 100_000;
        const FAST_SEGMENT: u64 = 256 << 20;
        let max = self.opts.segment_bytes;
        // Logs configured with small segments (tests, the simulator) keep
        // them.
        if !(fusee_core::segment::DEFAULT_SEGMENT_BYTES..FAST_SEGMENT).contains(&max) {
            return max;
        }
        let written = self.bytes_written.saturating_sub(self.rolled_at.1);
        let ms = self.rolled_at.2.saturating_sub(self.rolled_at.0).max(1);
        if written >= 4 << 20 && written / ms > FAST_BYTES_PER_MS {
            FAST_SEGMENT
        } else {
            max
        }
    }

    /// Once the active segment is a quarter full, makes sure its successor
    /// exists before the roll needs it: a spare to rename, or a new file
    /// prepared in the background.
    fn prepare_ahead(&mut self) -> io::Result<()> {
        if self.prepared.is_some() || self.writer.offset() < self.current_len() / 4 {
            return Ok(());
        }
        let size = self.next_size();
        if !self.spares.is_empty() {
            // A spare as large as the next segment is renamed at the roll.
            // Without one, a segment of the next size is prepared in the
            // background even while smaller spares remain: renaming those
            // would keep a busy log cycling its first small files (1 to
            // 4 MiB) instead of growing, so it would roll, and ask every
            // shard for a checkpoint, dozens of times a second. The small
            // spares stay for a roll that comes before the
            // preparation is done (see `next_segment`).
            let active = self.live.last().map(|s| s.number);
            if self.spares_fit == active {
                return Ok(());
            }
            let mut fits = false;
            for &spare in &self.spares {
                fits |= self.dev.len(spare)? >= size;
            }
            if fits {
                self.spares_fit = active;
                return Ok(());
            }
        }
        let number = self.next_number;
        self.next_number += 1;
        self.dev.prepare(number, size)?;
        self.prepared = Some((number, size));
        Ok(())
    }

    /// The segment the next batch rolls into: `(number, salt, size)`.
    /// Reuses a spare by renaming it; creates a file only when none is left.
    fn next_segment(&mut self, need: u64) -> io::Result<(u64, u64, u64)> {
        let grown = self.next_size().max(need + WAL_HEADER_BYTES);
        if let Some((number, size)) = self.prepared.take() {
            if size < need + WAL_HEADER_BYTES {
                self.dev.remove(number)?;
            } else if self.dev.is_ready(number) || self.spares.is_empty() {
                self.rng = xorshift(self.rng);
                return Ok((number, self.rng, size));
            } else {
                // Still being made: a spare carries this segment instead of
                // the write waiting for the fill (a 256 MiB segment takes
                // about a second at the preparation's pace). The prepared
                // file serves a later roll.
                self.prepared = Some((number, size));
            }
        }
        let number = self.next_number;
        self.next_number += 1;
        let mut size = 0;
        // The largest spare first: the log grows where it can.
        let mut lens = Vec::with_capacity(self.spares.len());
        for &spare in &self.spares {
            lens.push((self.dev.len(spare)?, spare));
        }
        lens.sort_unstable();
        self.spares = lens.iter().map(|&(_, n)| n).collect();
        while let Some(spare) = self.spares.pop() {
            let len = self.dev.len(spare)?;
            if len >= need + WAL_HEADER_BYTES {
                self.dev.rename(spare, number)?;
                size = len;
                break;
            }
            self.dev.remove(spare)?;
        }
        if size == 0
            && let Some((prepared, len)) = self.prepared.take()
        {
            // No spare fits: the prepared segment after all (this waits for
            // its fill).
            self.next_number -= 1;
            self.rng = xorshift(self.rng);
            return Ok((prepared, self.rng, len));
        }
        if size == 0 {
            self.dev.create(number, grown)?;
            size = grown;
        }
        self.rng = xorshift(self.rng);
        Ok((number, self.rng, size))
    }

    pub fn durable(&self, shard: u32) -> Lsn {
        self.durable.get(shard as usize).copied().unwrap_or(0)
    }

    /// The sequence of the next batch: a checkpoint taken now covers every
    /// record in batches before it.
    pub fn position(&self) -> u64 {
        self.writer.next_seq()
    }

    pub fn active_segment(&self) -> u64 {
        self.writer.segment()
    }

    /// The oldest batch `shard` still needs: the first holding one of its
    /// records after its oldest retained checkpoint (`None`: none, so it
    /// needs nothing; 0: it has not reported a checkpoint yet).
    fn needed(&self, shard: usize) -> Option<u64> {
        match self.retain_lsn[shard] {
            None => Some(0),
            Some(_) => self.holding[shard].front().map(|(seq, _)| *seq),
        }
    }

    /// Whether `shard` is one of the shards holding back the oldest
    /// segment: the earliest batch anyone needs is its, in a segment other
    /// than the active one. One more checkpoint of it (with nothing new, its
    /// two retained checkpoints then cover the same records) lets that go.
    pub fn pins(&self, shard: u32) -> bool {
        let Some(mine) = self.needed(shard as usize) else {
            return false;
        };
        let min = (0..self.holding.len())
            .filter_map(|s| self.needed(s))
            .min()
            .unwrap_or(u64::MAX);
        // A backup that has not uploaded the segment holds it, not the
        // shard: another checkpoint would not let it go.
        mine == min
            && self.backup_needs.is_none_or(|b| b > mine)
            && self.live.len() >= 2
            && self.live[1].base_seq > mine
            && self.live.last().is_some_and(|s| mine < s.base_seq)
    }

    /// Whether the shard's oldest needed batch is in the oldest live
    /// segment, the one the log retires next: after a roll, only such a
    /// shard has a reason to checkpoint.
    pub fn holds_retiring(&self, shard: u32) -> bool {
        let Some(mine) = self.needed(shard as usize) else {
            return false;
        };
        self.live.len() >= 2 && mine < self.live[1].base_seq
    }

    /// Whether the shard still needs a segment before the active one.
    pub fn holds_old(&self, shard: u32) -> bool {
        let Some(mine) = self.needed(shard as usize) else {
            return false;
        };
        self.live.len() >= 2 && self.live.last().is_some_and(|s| mine < s.base_seq)
    }

    /// `lsn`: what the shard's oldest retained checkpoint covers. Retires
    /// every segment no shard needs any more.
    pub fn set_retain(&mut self, shard: u32, lsn: Lsn) -> io::Result<()> {
        let s = shard as usize;
        if s >= self.holding.len() {
            return Ok(());
        }
        self.retain_lsn[s] = Some(lsn);
        while self.holding[s]
            .front()
            .is_some_and(|(_, last)| *last <= lsn)
        {
            self.holding[s].pop_front();
        }
        self.retire()
    }

    /// `needs`: the first batch a continuous backup has not uploaded yet
    /// (`None`: no backup). Retires every segment nothing needs any more.
    pub fn set_backup(&mut self, needs: Option<u64>) -> io::Result<()> {
        self.backup_needs = needs;
        self.retire()
    }

    /// Retires every segment no shard (and no backup) needs any more.
    fn retire(&mut self) -> io::Result<()> {
        let min = (0..self.holding.len())
            .filter_map(|s| self.needed(s))
            .min()
            .unwrap_or(u64::MAX)
            .min(self.backup_needs.unwrap_or(u64::MAX))
            .min(self.position());
        // A segment is covered when the one after it starts at or before
        // `min`; the active segment always stays.
        while self.live.len() >= 2 && self.live[1].base_seq <= min {
            let old = self.live.remove(0);
            // Zero the header: the file can never be read as chain again.
            self.dev
                .write(old.number, 0, &vec![0; WAL_HEADER_BYTES as usize])?;
            self.dev.sync(old.number)?;
            if self.spares.len() < self.opts.spares {
                self.spares.push(old.number);
            } else {
                self.dev.remove(old.number)?;
            }
        }
        // Blob files only retired batches reference go with them: their
        // headers are zeroed (durably) first, so no batch that the chain can
        // accept references a deleted file.
        if let (Some(area), Some(first)) = (self.blobs.as_mut(), self.live.first()) {
            area.retire(first.base_seq)?;
        }
        Ok(())
    }

    /// For a clean close. When nothing in the log is needed any more (every
    /// shard's retained checkpoints cover its records, no backup waits for
    /// a batch, nothing is staged or being written), the chain moves to an
    /// empty segment and the segments before it are retired: the next open
    /// then reads a header where it would have read, checked and copied
    /// every batch of the active segment, which is as large as the log
    /// grew. Returns whether it did.
    ///
    /// The steps are a roll's and a retirement's, in their order: the new
    /// segment's header is durable before the old segment's is zeroed, so a
    /// crash between any two leaves a chain that recovery accepts. After an
    /// error the log must not be written again, as after a failed write.
    pub fn shorten(&mut self, now_ms: u64) -> io::Result<bool> {
        let quiet = self.in_flight.is_empty()
            && !self.has_staged()
            && self
                .blobs
                .as_ref()
                .is_none_or(|a| a.jobs.is_empty() && a.held.iter().all(|h| h.is_empty()));
        let unneeded = self.backup_needs.is_none()
            && (0..self.holding.len()).all(|s| self.needed(s).is_none());
        // An active segment without a batch is as short as the log gets.
        if !quiet || !unneeded || self.writer.offset() <= WAL_HEADER_BYTES {
            return Ok(false);
        }
        let (number, salt, len) = self.empty_segment()?;
        let header = self.writer.roll(number, salt, len, now_ms);
        self.dev.write(number, 0, &header.encode())?;
        self.dev.sync(number)?;
        self.rolls += 1;
        self.rolled_at = (now_ms, self.bytes_written, now_ms);
        self.live.push(LiveSegment {
            number,
            base_seq: header.base_seq,
            len,
        });
        self.retire()?;
        Ok(true)
    }

    /// A segment for [`shorten`](Self::shorten) to move to, as `(number,
    /// salt, size)`: the one prepared ahead if it is ready, else the largest
    /// spare (the log keeps the size it grew to), else a new file of a new
    /// log's first size. None of them waits for a segment to be filled.
    fn empty_segment(&mut self) -> io::Result<(u64, u64, u64)> {
        self.rng = xorshift(self.rng);
        if let Some((number, len)) = self.prepared
            && self.dev.is_ready(number)
        {
            self.prepared = None;
            return Ok((number, self.rng, len));
        }
        let number = self.next_number;
        self.next_number += 1;
        let mut largest: Option<(u64, usize)> = None;
        for (i, &spare) in self.spares.iter().enumerate() {
            let len = self.dev.len(spare)?;
            if largest.is_none_or(|(l, _)| len > l) {
                largest = Some((len, i));
            }
        }
        if let Some((len, i)) = largest.filter(|(len, _)| *len > WAL_HEADER_BYTES) {
            let spare = self.spares.remove(i);
            self.dev.rename(spare, number)?;
            return Ok((number, self.rng, len));
        }
        let len = self.opts.first_segment_bytes.min(self.opts.segment_bytes);
        self.dev.create(number, len)?;
        Ok((number, self.rng, len))
    }

    /// Segment files in the chain and spares (for tests and inspection).
    pub fn files(&self) -> (Vec<u64>, Vec<u64>) {
        (
            self.live.iter().map(|s| s.number).collect(),
            self.spares.clone(),
        )
    }

    pub fn device(&self) -> &D {
        &self.dev
    }

    /// Drops the section `shard` has staged and not yet sealed: its writer
    /// went away, and nothing staged was acknowledged. A log shared by
    /// several writers (server mode) calls this before another writer of
    /// the shard recovers it, so the old writer's records can never follow
    /// the new writer's. Returns whether anything was staged.
    pub fn discard_staged(&mut self, shard: u32) -> bool {
        let mut any = false;
        if let Some(refs) = self.staged_refs.get_mut(shard as usize) {
            let refs = std::mem::take(refs);
            if let Some(area) = self.blobs.as_mut() {
                for f in refs {
                    if let Some(file) = area.files.get_mut(&f) {
                        file.pending = file.pending.saturating_sub(1);
                    }
                }
            }
        }
        if let Some(q) = self
            .blobs
            .as_mut()
            .and_then(|a| a.held.get_mut(shard as usize))
        {
            any = !q.is_empty();
            let dropped: Vec<Held> = q.drain(..).collect();
            let area = self.blobs.as_mut().expect("held above");
            for h in dropped {
                for f in h.files {
                    if let Some(file) = area.files.get_mut(&f) {
                        file.pending = file.pending.saturating_sub(1);
                    }
                }
            }
        }
        self.staged
            .get_mut(shard as usize)
            .and_then(Option::take)
            .is_some()
            || any
    }

    /// The chain as it stands, for [`read_sections`]: its segments and the
    /// last batch that is durable together with every batch before it.
    pub fn chain(&self) -> Chain {
        Chain {
            segments: self
                .live
                .iter()
                .map(|s| (s.number, s.len, s.base_seq))
                .collect(),
            acked: self.acked,
        }
    }

    /// The highest batch sequence durable together with every earlier one.
    pub fn acked(&self) -> u64 {
        self.acked
    }
}

/// Decodes a section's records after `after`, reading offloaded ones back
/// from `blobs`.
fn resolve_records<D: LogDevice>(
    s: &SectionRef<'_>,
    after: Lsn,
    blobs: Option<&D>,
) -> io::Result<Vec<(Lsn, RecordBuf)>> {
    let mut fetch = |r: &OffloadRef| -> io::Result<Vec<u8>> {
        match blobs {
            Some(dev) => dev.read(r.file, r.offset, r.len as usize),
            None => Err(io_other("this log has no blob area")),
        }
    };
    section_records_resolved(s, after, &mut fetch).map_err(|e| match e {
        fusee_core::wal::ResolveError::Blob { .. } => io_other(format!(
            "shard {} {e}: an acknowledged record was lost or damaged. Restore from a replica \
             or backup.",
            s.shard
        )),
        e => io_other(format!("shard {}: {e}", s.shard)),
    })
}

/// The canonical bytes of a section that may hold references: each
/// reference replaced by the record it names, read from `blobs`.
pub fn resolve_section<D: LogDevice>(s: &SectionRef<'_>, blobs: Option<&D>) -> io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(s.records.len());
    let mut any = false;
    for (i, rec) in fusee_core::wal::section_spans(s.records).enumerate() {
        let rec = rec.map_err(io_other)?;
        if fusee_core::wal::is_offloaded(rec.body) {
            any = true;
            let r = OffloadRef::decode(rec.body).map_err(io_other)?;
            let lsn = s.first_lsn + i as u64;
            let bytes = match blobs {
                Some(dev) => dev.read(r.file, r.offset, r.len as usize)?,
                None => Vec::new(),
            };
            if !r.matches(&bytes) {
                return Err(io_other(format!(
                    "shard {} record {lsn} references bytes {}..{} of blob file {}, which are \
                     missing or damaged",
                    s.shard,
                    r.offset,
                    r.offset + r.len,
                    r.file
                )));
            }
            fusee_core::wire::put_bytes(&mut out, &bytes);
        } else {
            fusee_core::wire::put_bytes(&mut out, rec.body);
        }
    }
    if !any {
        out.clear();
        out.extend_from_slice(s.records);
    }
    Ok(out)
}

/// A snapshot of a log's chain ([`Wal::chain`]): `(number, length, first
/// batch)` of each segment, oldest first, and the last durable batch.
#[derive(Clone, Debug, Default)]
pub struct Chain {
    segments: Vec<(u64, u64, u64)>,
    acked: u64,
}

impl Chain {
    pub fn acked(&self) -> u64 {
        self.acked
    }

    /// `(number, length, first batch)` of each segment, oldest first; the
    /// last is the active one.
    pub fn segments(&self) -> &[(u64, u64, u64)] {
        &self.segments
    }
}

/// One shard's section read back from the log: `(first LSN, records,
/// canonical record bytes)`.
pub type RawSection = (Lsn, u32, Vec<u8>);

/// Reads a chain back from its device and returns, for each of `shards`,
/// its sections in every batch through the snapshot's last durable batch,
/// in order. Batches written after the snapshot are ignored, so the log may
/// keep writing meanwhile; its segments must not be retired or reused
/// until this returns. Fails if the chain on the device ends before the
/// snapshot's last durable batch.
pub fn read_sections<D: LogDevice>(
    dev: &D,
    chain: &Chain,
    shards: &[u32],
) -> io::Result<Vec<Vec<RawSection>>> {
    let mut out: Vec<Vec<RawSection>> = shards.iter().map(|_| Vec::new()).collect();
    if chain.acked == 0 {
        return Ok(out);
    }
    let mut expect: Option<(u64, u64)> = None;
    for &(number, len, base_seq) in &chain.segments {
        if base_seq > chain.acked {
            break;
        }
        let mut chunk = 256 << 10;
        let mut bytes = dev.read(number, 0, chunk)?;
        let Some(mut scanner) =
            WalScanner::start(&bytes, bytes.len() as u64 >= len).map_err(io_other)?
        else {
            return Err(io_other(format!(
                "log segment {number} lost its header while it was being read"
            )));
        };
        let h = *scanner.header();
        if h.segment != number
            || h.base_seq != base_seq
            || expect.is_some_and(|(seq, sum)| h.base_seq != seq + 1 || h.base_prev != sum)
        {
            return Err(io_other(format!(
                "log segment {number} does not continue the chain"
            )));
        }
        let mut reached = false;
        let scanned = loop {
            let complete = bytes.len() as u64 >= len;
            let progress = scanner.advance(&bytes, complete, len as usize, &mut |b| {
                if reached || b.header.seq > chain.acked {
                    return;
                }
                for s in b.sections() {
                    if let Some(i) = shards.iter().position(|x| *x == s.shard) {
                        out[i].push((s.first_lsn, s.count, s.records.to_vec()));
                    }
                }
                reached = b.header.seq == chain.acked;
            });
            if reached {
                return Ok(out);
            }
            match progress {
                Progress::Done(s) => break s,
                Progress::NeedMore => {
                    chunk = (chunk * 2).min(16 << 20);
                    let more = dev.read(number, bytes.len() as u64, chunk)?;
                    if more.is_empty() {
                        return Err(io_other(format!(
                            "reading log segment {number} made no progress"
                        )));
                    }
                    bytes.extend_from_slice(&more);
                }
            }
        };
        expect = Some((scanned.last_seq, scanned.last_checksum));
    }
    Err(io_other(format!(
        "the log ends at batch {} before durable batch {}",
        expect.map_or(0, |e| e.0),
        chain.acked
    )))
}

/// The records of `sections` after `after`, in order, and the last LSN they
/// hold (`after` if none is later). Fails if records right after `after`
/// are missing.
pub fn records_after(
    shard: u32,
    sections: &[RawSection],
    after: Lsn,
) -> io::Result<(Vec<(Lsn, RecordBuf)>, Lsn)> {
    let mut out = Vec::new();
    let mut expected = after + 1;
    let mut last = after;
    for (first, count, bytes) in sections {
        let end = first + u64::from(*count) - 1;
        if end <= after {
            continue;
        }
        if *first > expected {
            return Err(io_other(format!(
                "shard {shard} records {expected}..{} are missing from the log",
                first - 1
            )));
        }
        let s = SectionRef {
            shard,
            first_lsn: *first,
            count: *count,
            records: bytes,
        };
        for (lsn, rec) in fusee_core::wal::section_records(&s).map_err(io_other)? {
            if lsn > after {
                out.push((lsn, rec));
            }
        }
        expected = end + 1;
        last = end;
    }
    Ok((out, last))
}

fn xorshift(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

/// A random 64-bit value for a writer life.
pub fn new_life() -> u64 {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut x = t ^ (u64::from(std::process::id()) << 32) ^ (&t as *const u64 as u64);
    for _ in 0..4 {
        x = xorshift(x | 1);
    }
    x
}

/// Encodes log-format-3 sections without a batch header: the body of a
/// replication entry, whose op, view and chain checksum stand in for the
/// header (`fusee_server::replicated`). Each section has the layout it has
/// in a batch: `shard:u32 count:u32 first_lsn:u64 bytes:u32 reserved:u32
/// records`, after a `u32` section count.
pub fn encode_sections(out: &mut Vec<u8>, sections: &[SectionRef<'_>]) {
    out.reserve(
        4 + sections
            .iter()
            .map(|s| fusee_core::wal::SECTION_HEADER + s.records.len())
            .sum::<usize>(),
    );
    out.extend_from_slice(&(sections.len() as u32).to_le_bytes());
    for s in sections {
        out.extend_from_slice(&s.shard.to_le_bytes());
        out.extend_from_slice(&s.count.to_le_bytes());
        out.extend_from_slice(&s.first_lsn.to_le_bytes());
        out.extend_from_slice(&(s.records.len() as u32).to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(s.records);
    }
}

/// Reads what [`encode_sections`] wrote. The sections must fill `bytes`
/// exactly and each must hold at least one record.
pub fn decode_sections(bytes: &[u8]) -> io::Result<Vec<SectionRef<'_>>> {
    let bad = || io_other("malformed log sections");
    let n = u32::from_le_bytes(bytes.get(..4).ok_or_else(bad)?.try_into().unwrap());
    let mut out = Vec::with_capacity((n as usize).min(1024));
    let mut at = 4usize;
    for _ in 0..n {
        let h = bytes
            .get(at..at + fusee_core::wal::SECTION_HEADER)
            .ok_or_else(bad)?;
        let u32_at = |i: usize| u32::from_le_bytes(h[i..i + 4].try_into().unwrap());
        let len = u32_at(16) as usize;
        let start = at + fusee_core::wal::SECTION_HEADER;
        let records = bytes.get(start..start + len).ok_or_else(bad)?;
        let s = SectionRef {
            shard: u32_at(0),
            count: u32_at(4),
            first_lsn: u64::from_le_bytes(h[8..16].try_into().unwrap()),
            records,
        };
        if s.count == 0 || s.first_lsn == 0 {
            return Err(bad());
        }
        out.push(s);
        at = start + len;
    }
    if at != bytes.len() {
        return Err(bad());
    }
    Ok(out)
}

impl<T: LogDevice> LogDevice for Arc<T> {
    fn segments(&self) -> io::Result<Vec<u64>> {
        (**self).segments()
    }
    fn len(&self, segment: u64) -> io::Result<u64> {
        (**self).len(segment)
    }
    fn read(&self, segment: u64, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        (**self).read(segment, offset, len)
    }
    fn create(&self, segment: u64, len: u64) -> io::Result<()> {
        (**self).create(segment, len)
    }
    fn write(&self, segment: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        (**self).write(segment, offset, bytes)
    }
    fn sync(&self, segment: u64) -> io::Result<()> {
        (**self).sync(segment)
    }
    fn rename(&self, from: u64, to: u64) -> io::Result<()> {
        (**self).rename(from, to)
    }
    fn remove(&self, segment: u64) -> io::Result<()> {
        (**self).remove(segment)
    }
    fn prepare(&self, segment: u64, len: u64) -> io::Result<()> {
        (**self).prepare(segment, len)
    }
    fn is_ready(&self, segment: u64) -> bool {
        (**self).is_ready(segment)
    }
}

struct ShardSlot {
    durable: AtomicU64,
    notify: Mutex<Option<Notify>>,
    /// The last position the shard submitted for the log and I/O threads to
    /// write (0: none outstanding). The shard is told (`notify`) once, when
    /// that position is durable, not after every write that carried it.
    submitted: AtomicU64,
    /// The position a thread flushing inline waits for (0: none), and that
    /// thread: it is unparked when the position is durable, or when it is
    /// handed the next write.
    waiting: AtomicU64,
    waiter: Mutex<Option<std::thread::Thread>>,
}

/// What a writer wakes once it has let go of the log.
enum Wake {
    Thread(std::thread::Thread),
    Notify(Notify),
}

impl Wake {
    fn run(self) {
        match self {
            Wake::Thread(t) => t.unpark(),
            Wake::Notify(n) => n(),
        }
    }
}

/// No shard has been handed the next write ([`SharedWal::handed`]).
const NOBODY: u64 = u64::MAX;

/// A lead write is padded for a second beside it only where a flush takes
/// at least this long (see `SharedWal::expect_company`).
const PAD_FOR_COMPANY_US: u64 = 10;

/// The log thread's wake flag and whether it waits for it.
#[derive(Default)]
struct Woken {
    kicked: bool,
    waiting: bool,
}

/// The I/O threads' queue and how many of them wait for it.
#[derive(Default)]
struct Jobs {
    queue: std::collections::VecDeque<(InFlight, Instant)>,
    idle: usize,
}

/// A handoff not taken up within this long is taken over by another
/// waiting shard.
const HANDOFF_STALE_NS: u64 = 1_000_000;

/// A [`Wal`] shared by shard threads.
///
/// **Inline flushes lead in turn.** A shard flushing inline stages its
/// section and, when no write is in flight, writes everything staged on its
/// own thread. A shard that finds a write in flight waits on its own
/// position (spinning briefly, then parked), and the writer, once its write
/// is durable, hands the next write to one of the waiting shards whose
/// sections are staged: that shard was going to be woken anyway, so the
/// next write costs no thread hop, and each waiting shard is woken once,
/// when its position is durable or the write is its to make.
///
/// **Submitted flushes** (a busy shard does not wait) are staged for the
/// log thread, which is woken only when no write is in flight (a write in
/// flight starts the next when it completes). It seals batches while fewer
/// than [`WalOptions::in_flight`] writes are in flight; I/O threads sync
/// each write outside the log lock and publish positions in batch order.
pub struct SharedWal {
    // What the log and I/O threads wait on is shared with them apart from
    // the log, so that a waiting thread holds no strong reference: the log
    // drops (and stops its threads) when its last user lets go.
    wake: Arc<Condvar>,
    /// Set by a kick; and whether the log thread waits on `wake`. Both
    /// change under this mutex, so a kick notifies only a waiting thread:
    /// `Condvar::notify_one` is a system call even when nobody waits.
    woken: Arc<Mutex<Woken>>,
    /// Set by submissions; lets the log thread spin without the mutex.
    kicked: AtomicBool,
    /// Submissions start their writes on the submitting thread instead of
    /// the log thread's (a foreign host's pipelined flush, see
    /// `crate::foreign::Flush::Pipeline`), and the I/O threads spin for the
    /// next one at least `DIRECT_SPIN_US`.
    direct: AtomicBool,
    /// Jobs queued for the I/O threads (lock-free view for spinning).
    queued: AtomicU64,
    /// Spin before parking at most this long (0 on small hosts).
    spin_cap_us: u64,
    stop: Arc<AtomicBool>,
    threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
    dev: Arc<FileDevice>,
    wal: Mutex<Wal<Arc<FileDevice>>>,
    /// Writes to sync, for the I/O threads, and how many of them wait on
    /// `job_ready` (notified only then, as `woken`).
    jobs: Arc<Mutex<Jobs>>,
    job_ready: Arc<Condvar>,
    /// Writes sealed and not yet complete (`Wal::in_flight`), readable
    /// without the log lock.
    writes: AtomicU64,
    /// The shard handed the next write (its inline waiter writes it), or
    /// [`NOBODY`]. Set and cleared under the log lock; while set, nobody
    /// else starts a write.
    handed: AtomicU64,
    /// When the handoff was made ([`crate::spin::clock_ns`]).
    handed_at: AtomicU64,
    /// Sections waiting for the owner, with their shard and the records
    /// in them to offload (found on the shard's thread).
    staging: Mutex<Vec<(u32, Batch, Vec<Large>)>>,
    /// The blob area's files, written by the blob thread outside the log
    /// lock; records longer than `offload` bytes go there (0: none).
    blob_dev: Arc<FileDevice>,
    offload: u64,
    blob_ready: Arc<Condvar>,
    blob_kicked: Arc<Mutex<bool>>,
    shards: Vec<ShardSlot>,
    failed: Mutex<Option<String>>,
    has_failed: AtomicBool,
    last_flush_us: AtomicU64,
    /// Shards whose sections the last sealed batch carried (see `spin`).
    last_batch_shards: AtomicU64,
    /// [`WalOptions::in_flight`].
    in_flight_limit: usize,
    /// Linux: the log owner thread writes through an `io_uring`
    /// (`wal/owner.rs`), woken through this eventfd while it waits.
    owner: bool,
    /// The eventfd's number, open as long as the log (`owner_efd`).
    owner_fd: i32,
    /// The eventfd itself, shared with the owner thread: it closes when
    /// both are done with it, so neither writes to (or arms a read on) a
    /// number that may have been reused.
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    #[allow(dead_code)]
    owner_efd: Option<Arc<std::os::fd::OwnedFd>>,
    #[cfg_attr(not(all(target_os = "linux", feature = "io-uring")), allow(dead_code))]
    owner_waiting: AtomicBool,
    /// `Wal::writes`, `bytes_written` and `rolls`, readable without the lock.
    stats: [AtomicU64; 3],
    position: AtomicU64,
    segment: AtomicU64,
    /// The continuous backup shipping this log ([`crate::backup`]): it
    /// reads the chain, holds segments it has not uploaded, and makes a
    /// last upload when the log closes.
    backup: Mutex<Option<Arc<crate::backup::Shipper>>>,
}

impl SharedWal {
    /// Opens the log in `dir` (offloading nothing; references already in
    /// it are read from its blob area).
    pub fn open(
        dir: &Path,
        shards: u32,
        local: LocalOptions,
        opts: WalOptions,
    ) -> io::Result<Arc<Self>> {
        Self::open_with_offload(dir, shards, local, opts, 0)
    }

    /// [`open`](Self::open), offloading records whose canonical encoding
    /// is longer than `offload` bytes to the blob area (`<dir>/blobs`); 0:
    /// none.
    pub fn open_with_offload(
        dir: &Path,
        shards: u32,
        local: LocalOptions,
        opts: WalOptions,
        offload: u64,
    ) -> io::Result<Arc<Self>> {
        let blob_dev = Arc::new(FileDevice::blobs(&dir.join("blobs"), local.clone()));
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        let local_for_owner = local.clone();
        let dev = Arc::new(FileDevice::open(dir, local)?);
        let wal = Wal::open_with_blobs(
            dev.clone(),
            Blobs {
                dev: blob_dev.clone(),
                threshold: offload,
            },
            shards,
            opts,
            new_life(),
            unix_ms(),
        )?;
        let position = wal.position();
        let segment = wal.active_segment();
        let slots = (0..shards)
            .map(|s| ShardSlot {
                durable: AtomicU64::new(wal.durable(s)),
                notify: Mutex::new(None),
                submitted: AtomicU64::new(0),
                waiting: AtomicU64::new(0),
                waiter: Mutex::new(None),
            })
            .collect();
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        let (owner_ring, owner_efd) = match owner::wanted(&dev, &local_for_owner) {
            Some((ring, efd)) => (Some(ring), Some(Arc::new(efd))),
            None => (None, None),
        };
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        let owner_fd = owner_efd.as_ref().map_or(-1, |f| {
            use std::os::fd::AsRawFd;
            f.as_raw_fd()
        });
        #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
        let owner_fd = -1;
        let shared = Arc::new(Self {
            wake: Arc::new(Condvar::new()),
            woken: Arc::new(Mutex::new(Woken::default())),
            kicked: AtomicBool::new(false),
            direct: AtomicBool::new(false),
            queued: AtomicU64::new(0),
            // Plan principle 6: spend a core only where one is spare.
            spin_cap_us: if cores > 2 && !crate::spin::spin_off() {
                50
            } else {
                0
            },
            stop: Arc::new(AtomicBool::new(false)),
            threads: Mutex::new(Vec::new()),
            dev,
            wal: Mutex::new(wal),
            jobs: Arc::new(Mutex::new(Jobs::default())),
            job_ready: Arc::new(Condvar::new()),
            writes: AtomicU64::new(0),
            handed: AtomicU64::new(NOBODY),
            handed_at: AtomicU64::new(0),
            staging: Mutex::new(Vec::new()),
            blob_dev,
            offload,
            blob_ready: Arc::new(Condvar::new()),
            blob_kicked: Arc::new(Mutex::new(false)),
            shards: slots,
            failed: Mutex::new(None),
            has_failed: AtomicBool::new(false),
            last_flush_us: AtomicU64::new(0),
            last_batch_shards: AtomicU64::new(0),
            in_flight_limit: opts.in_flight.max(1),
            stats: [const { AtomicU64::new(0) }; 3],
            owner: owner_fd >= 0,
            owner_fd,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            owner_efd: owner_efd.clone(),
            owner_waiting: AtomicBool::new(false),
            position: AtomicU64::new(position),
            segment: AtomicU64::new(segment),
            backup: Mutex::new(None),
        });
        let mut threads = Vec::new();
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        if let (Some(ring), Some(efd)) = (owner_ring, owner_efd) {
            let weak = Arc::downgrade(&shared);
            threads.push(
                std::thread::Builder::new()
                    .name("fusee-wal-owner".into())
                    .spawn(move || owner::run(weak, ring, efd))?,
            );
        }
        if !shared.owner {
            let weak = Arc::downgrade(&shared);
            threads.push(
                std::thread::Builder::new()
                    .name("fusee-wal".into())
                    .spawn(move || log_thread(weak))?,
            );
            for i in 0..opts.in_flight.max(1) {
                let weak = Arc::downgrade(&shared);
                threads.push(
                    std::thread::Builder::new()
                        .name(format!("fusee-wal-io-{i}"))
                        .spawn(move || io_thread(weak))?,
                );
            }
        }
        if offload > 0 {
            let weak = Arc::downgrade(&shared);
            threads.push(
                std::thread::Builder::new()
                    .name("fusee-wal-blob".into())
                    .spawn(move || blob_thread(weak))?,
            );
        }
        *shared.threads.lock().unwrap_or_else(|e| e.into_inner()) = threads;
        Ok(shared)
    }

    /// Wakes the log thread to write what is staged.
    fn kick(&self) {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        if self.owner {
            self.kicked.store(true, Ordering::SeqCst);
            owner::wake(self);
            return;
        }
        // `woken` first: a log thread spinning on `kicked` must find it set,
        // or it parks on the condition variable and pays a wake-up.
        let waiting = {
            let mut w = self.woken.lock().unwrap_or_else(|e| e.into_inner());
            w.kicked = true;
            w.waiting
        };
        self.kicked.store(true, Ordering::Release);
        if waiting {
            self.wake.notify_one();
        }
    }

    /// See `direct`.
    pub fn set_direct_submit(&self, on: bool) {
        self.direct.store(on, Ordering::Relaxed);
    }

    /// How long a waiter spins before parking: the recent flush time (the
    /// handoff it waits for is at most that far away), capped, and zero
    /// without a spare core.
    fn spin_budget(&self) -> std::time::Duration {
        // Several shards writing together keep the log busy without help:
        // spin only for a lone writer, whose next submission is a flush away.
        // At most two shards at work, and a core spare for the spinner,
        // for at most a flush: spinning wherever a core is spare would cost
        // CPU and median latency under load.
        if self.last_batch_shards.load(Ordering::Relaxed) > 1
            || !crate::spin::few_busy()
            || !crate::spin::spare_core()
        {
            return std::time::Duration::ZERO;
        }
        let flush = self.last_flush_us.load(Ordering::Relaxed);
        std::time::Duration::from_micros(self.spin_cap_us.min(flush))
    }

    /// Spins until `ready` or the budget runs out.
    fn spin(&self, ready: impl Fn() -> bool) -> bool {
        let budget = self.spin_budget();
        if budget.is_zero() {
            return ready();
        }
        let until = Instant::now() + budget;
        loop {
            if ready() {
                return true;
            }
            if Instant::now() >= until {
                return false;
            }
            std::hint::spin_loop();
        }
    }

    fn fail_message(&self) -> Option<String> {
        if !self.has_failed.load(Ordering::Acquire) {
            return None;
        }
        self.failed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn fail(&self, e: io::Error) {
        // The log's state on disk is uncertain: every shard stops.
        *self.failed.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(format!("log write failed: {e}"));
        self.has_failed.store(true, Ordering::Release);
        for slot in &self.shards {
            if let Some(t) = slot
                .waiter
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
            {
                t.unpark();
            }
            if let Some(n) = slot
                .notify
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
            {
                n();
            }
        }
    }

    fn stage(&self, shard: u32, batch: Batch) {
        // Below the threshold this is one comparison. Above it the records
        // to offload are found and checksummed here, on the shard's thread,
        // not under the log lock.
        let large = if self.offload > 0 && batch.bytes > self.offload {
            match find_large(&batch.section, self.offload as usize) {
                Ok(l) => l,
                Err(e) => return self.fail(io_other(e)),
            }
        } else {
            Vec::new()
        };
        self.staging
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((shard, batch, large));
    }

    /// Moves staged sections into the log, and wakes the blob thread for
    /// records they offload. Called with the log locked.
    fn take_staged(&self, wal: &mut Wal<Arc<FileDevice>>) -> io::Result<()> {
        let staged = std::mem::take(&mut *self.staging.lock().unwrap_or_else(|e| e.into_inner()));
        for (shard, b, large) in staged {
            wal.stage_found(shard, b, large)?;
        }
        if wal.has_blob_jobs() {
            *self.blob_kicked.lock().unwrap_or_else(|e| e.into_inner()) = true;
            self.blob_ready.notify_one();
        }
        Ok(())
    }

    /// Tests: every later `O_DIRECT` write of the log fails with `EINVAL`.
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn reject_direct_writes(&self) {
        self.dev.reject_direct.store(true, Ordering::Relaxed);
    }

    /// Whether the log's writes still bypass the page cache.
    pub fn direct_io(&self) -> bool {
        self.dev.direct_io()
    }

    /// What the blob area holds.
    pub fn blob_stats(&self) -> BlobStats {
        self.wal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .blob_stats()
    }

    /// Publishes positions a completed write made durable, and notes who to
    /// wake for them: a thread flushing inline whose position is durable,
    /// and a shard whose submitted position is. `me` (the thread that
    /// wrote) needs no wake. Called with the log locked; wake after
    /// unlocking.
    fn publish(
        &self,
        wal: &Wal<Arc<FileDevice>>,
        shards: &[u32],
        me: Option<u32>,
        wakes: &mut Vec<Wake>,
    ) {
        self.position.store(wal.position(), Ordering::Release);
        self.segment.store(wal.active_segment(), Ordering::Release);
        self.writes.store(wal.in_flight() as u64, Ordering::SeqCst);
        for (a, v) in self
            .stats
            .iter()
            .zip([wal.writes, wal.bytes_written, wal.rolls])
        {
            a.store(v, Ordering::Relaxed);
        }
        for &s in shards {
            let slot = &self.shards[s as usize];
            let durable = wal.durable(s);
            slot.durable.store(durable, Ordering::Release);
            if me == Some(s) {
                continue;
            }
            let waiting = slot.waiting.load(Ordering::Acquire);
            if waiting != 0
                && durable >= waiting
                && let Some(t) = slot
                    .waiter
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
            {
                wakes.push(Wake::Thread(t.clone()));
            }
            // A shard with a submission outstanding is told of every write
            // that advanced it: with several of its sections in flight, an
            // earlier one's runs need not wait for the later.
            loop {
                let want = slot.submitted.load(Ordering::Acquire);
                if want == 0 {
                    break;
                }
                if durable < want
                    || slot
                        .submitted
                        .compare_exchange(want, 0, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                {
                    if let Some(n) = slot
                        .notify
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .as_ref()
                    {
                        wakes.push(Wake::Notify(n.clone()));
                    }
                    break;
                }
            }
        }
    }

    /// After a write completed: starts the next if anything is staged. When
    /// nothing is in flight and a shard waiting inline has a section staged,
    /// that shard is handed the write (it was going to be woken anyway, and
    /// writes on its own thread); otherwise the I/O threads take it. Called
    /// with the log locked; wake after unlocking.
    fn next_write(&self, wal: &mut Wal<Arc<FileDevice>>, me: Option<u32>, wakes: &mut Vec<Wake>) {
        if self.has_failed.load(Ordering::Acquire) || self.handed.load(Ordering::Acquire) != NOBODY
        {
            return;
        }
        if let Err(e) = self.take_staged(wal) {
            return self.fail(e);
        }
        if !wal.has_staged() {
            return;
        }
        if wal.in_flight() == 0 {
            let lead = wal.staged_shards().find(|&s| {
                Some(s) != me && self.shards[s as usize].waiting.load(Ordering::Acquire) != 0
            });
            if let Some(s) = lead
                && let Some(t) = self.shards[s as usize]
                    .waiter
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
            {
                self.handed_at
                    .store(crate::spin::clock_ns(), Ordering::Release);
                self.handed.store(u64::from(s), Ordering::Release);
                wakes.push(Wake::Thread(t.clone()));
                return;
            }
        }
        self.start_writes(wal);
    }

    /// Starts writes of what is staged while the log allows another in
    /// flight; the I/O threads sync them. Called with the log locked.
    fn start_writes(&self, wal: &mut Wal<Arc<FileDevice>>) {
        if self.has_failed.load(Ordering::Acquire) || self.handed.load(Ordering::Acquire) != NOBODY
        {
            return;
        }
        if let Err(e) = self.take_staged(wal) {
            return self.fail(e);
        }
        while wal.has_staged() && wal.can_begin() {
            match wal.seal(unix_ms(), true) {
                Ok(Some(w)) => {
                    self.writes.store(wal.in_flight() as u64, Ordering::SeqCst);
                    self.last_batch_shards
                        .store(w.shards.len() as u64, Ordering::Relaxed);
                    let idle = {
                        let mut j = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
                        j.queue.push_back((w, Instant::now()));
                        j.idle
                    };
                    self.queued.fetch_add(1, Ordering::SeqCst);
                    #[cfg(all(target_os = "linux", feature = "io-uring"))]
                    if self.owner {
                        owner::wake(self);
                        continue;
                    }
                    // A spinning I/O thread sees `queued`; only a waiting one
                    // needs the condition variable.
                    if idle > 0 {
                        self.job_ready.notify_one();
                    }
                }
                Ok(None) => break,
                Err(e) => return self.fail(e),
            }
        }
    }

    /// Stages a section for the log thread and returns; the shard is
    /// notified once it is durable. The log thread is woken only when no
    /// write is in flight: one in flight starts the next when it completes
    /// ([`next_write`](Self::next_write)), and sees this section, which is
    /// staged before the count of writes in flight is read.
    fn submit(&self, shard: u32, batch: Batch) {
        self.shards[shard as usize]
            .submitted
            .store(batch.last_lsn, Ordering::Release);
        self.stage(shard, batch);
        if self.direct.load(Ordering::Relaxed) {
            // Sealing is short; the I/O threads write.
            let mut wal = self.wal.lock().unwrap_or_else(|e| e.into_inner());
            self.start_writes(&mut wal);
        } else {
            let writes = self.writes.load(Ordering::SeqCst);
            // With `FUSEE_LOG_SECOND=now`, a write may start beside those in
            // flight (`Wal::can_begin` still requires the newest padded).
            if writes == 0 || (log_tuning().second_now && writes < self.in_flight_limit as u64) {
                self.kick();
            }
        }
    }

    /// Writes this shard's section and everything staged with it, on this
    /// thread when no write is in flight; otherwise waits for the write in
    /// flight to carry it or to hand this thread the next write. Returns
    /// once the section is durable.
    fn write_now(&self, shard: u32, batch: Batch) -> io::Result<Lsn> {
        let last = batch.last_lsn;
        let slot = &self.shards[shard as usize];
        {
            let mut w = slot.waiter.lock().unwrap_or_else(|e| e.into_inner());
            let me = std::thread::current();
            if w.as_ref().is_none_or(|t| t.id() != me.id()) {
                *w = Some(me);
            }
        }
        // Waiting before staged: a writer that finds the section knows
        // this thread can be handed a write.
        slot.waiting.store(last, Ordering::Release);
        self.stage(shard, batch);
        let r = self.wait_inline(shard, last, Instant::now());
        slot.waiting.store(0, Ordering::Release);
        if self.handed.load(Ordering::Acquire) == u64::from(shard) {
            // Handed a write this thread no longer needs (the log failed):
            // let the log thread have it.
            let mut wal = self.wal.lock().unwrap_or_else(|e| e.into_inner());
            if self.handed.load(Ordering::Acquire) == u64::from(shard) {
                self.handed.store(NOBODY, Ordering::Release);
                self.start_writes(&mut wal);
            }
        }
        r
    }

    /// Whether a write sealed now should leave room for a second next to
    /// it (padded to a block of its own): the disk takes long enough that
    /// another shard would rather write beside it than wait, and another
    /// shard is waiting inline now. Padding every write where flushes are
    /// fast cost 1.5–2 µs of CPU a step at 100 K steps/s on tmpfs (a block
    /// of log per write: 87 device bytes per record byte), for no latency.
    fn expect_company(&self, me: u32) -> bool {
        self.in_flight_limit > 1
            && self.last_flush_us.load(Ordering::Relaxed) >= PAD_FOR_COMPANY_US
            && (log_tuning().pad_always
                || self
                    .shards
                    .iter()
                    .enumerate()
                    .any(|(i, s)| i != me as usize && s.waiting.load(Ordering::Acquire) != 0))
    }

    fn wait_inline(&self, shard: u32, last: Lsn, staged_at: Instant) -> io::Result<Lsn> {
        let slot = &self.shards[shard as usize];
        let mine = u64::from(shard);
        loop {
            if let Some(m) = self.fail_message() {
                return Err(io_other(m));
            }
            let durable = slot.durable.load(Ordering::Acquire);
            if durable >= last {
                return Ok(durable);
            }
            let pending_handoff = {
                let wal = self.wal.lock().unwrap_or_else(|e| e.into_inner());
                let handed = self.handed.load(Ordering::Acquire);
                // A handoff its thread has not taken up for a while (the
                // thread is descheduled) is taken over, so one slow thread
                // does not hold up every shard's writes.
                let stale = handed != NOBODY
                    && crate::spin::clock_ns()
                        .saturating_sub(self.handed_at.load(Ordering::Acquire))
                        > HANDOFF_STALE_NS;
                if wal.in_flight() == 0 && (handed == NOBODY || handed == mine || stale) {
                    self.handed.store(NOBODY, Ordering::Release);
                    let overlap = self.expect_company(shard);
                    self.lead(wal, shard, overlap);
                    continue;
                }
                // A second write next to one in flight (padded, so it needs
                // the one in flight to be padded too), once what is staged
                // has waited a quarter of a flush: sooner, the write in
                // flight would rather hand over the next one with more in
                // it when it completes; later, waiting for it costs the rest
                // of its write and a wake.
                let quarter = std::time::Duration::from_micros(
                    self.last_flush_us.load(Ordering::Relaxed) / 4,
                );
                // Only while something is staged: this shard's section may
                // be in the write in flight already, with nothing to add.
                let second = handed == NOBODY
                    && wal.can_begin()
                    && (wal.has_staged()
                        || !self
                            .staging
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .is_empty());
                if second && (staged_at.elapsed() >= quarter || log_tuning().second_now) {
                    self.lead(wal, shard, true);
                    continue;
                }
                (
                    handed != NOBODY,
                    second.then(|| quarter.saturating_sub(staged_at.elapsed())),
                )
            };
            let (pending_handoff, second_in) = pending_handoff;
            // The write in flight is about a flush away: spin briefly, then
            // park until this position is durable or the next write is ours.
            let ready = || {
                slot.durable.load(Ordering::Acquire) >= last
                    || self.handed.load(Ordering::Acquire) == mine
                    || self.has_failed.load(Ordering::Acquire)
            };
            if !self.spin(ready) {
                // While another thread holds a handoff, look again soon
                // enough to take it over if it goes stale; when a second
                // write may start, look again when it is due.
                let mut wait = if pending_handoff {
                    std::time::Duration::from_nanos(HANDOFF_STALE_NS)
                } else {
                    std::time::Duration::from_millis(10)
                };
                if let Some(due) = second_in {
                    wait = wait.min(due.max(std::time::Duration::from_micros(1)));
                }
                std::thread::park_timeout(wait);
            }
        }
    }

    /// Writes everything staged as one batch on this thread, outside the
    /// log lock; then publishes it and starts the next. `overlap`: padded
    /// to a block of its own, so that another write may start next to it;
    /// otherwise packed, and the next write waits for this one.
    fn lead(
        &self,
        mut wal: std::sync::MutexGuard<'_, Wal<Arc<FileDevice>>>,
        me: u32,
        overlap: bool,
    ) {
        let started = Instant::now();
        let sealed = self
            .take_staged(&mut wal)
            .and_then(|()| wal.seal(unix_ms(), overlap));
        let mut w = match sealed {
            Ok(Some(w)) => w,
            // Someone else wrote it meanwhile.
            Ok(None) => return,
            Err(e) => return self.fail(e),
        };
        self.writes.store(wal.in_flight() as u64, Ordering::SeqCst);
        self.last_batch_shards
            .store(w.shards.len() as u64, Ordering::Relaxed);
        // Shards that flush meanwhile stage for the next batch instead of
        // waiting for the lock and writing one batch each.
        drop(wal);
        let r = Wal::perform(&self.dev, &mut w);
        self.last_flush_us
            .store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        let mut wakes = Vec::new();
        {
            let mut wal = self.wal.lock().unwrap_or_else(|e| e.into_inner());
            match r {
                Ok(()) => {
                    let shards = wal.complete(w);
                    self.publish(&wal, &shards, Some(me), &mut wakes);
                    // What others staged meanwhile (they could not start it
                    // while an unpadded write was in flight).
                    self.next_write(&mut wal, Some(me), &mut wakes);
                }
                Err(e) => self.fail(e),
            }
        }
        for w in wakes {
            w.run();
        }
    }

    fn take_recovered(&self, shard: u32, after: Lsn) -> io::Result<(Vec<(Lsn, RecordBuf)>, Lsn)> {
        let mut wal = self.wal.lock().unwrap_or_else(|e| e.into_inner());
        let r = wal.take_recovered(shard, after)?;
        self.shards[shard as usize]
            .durable
            .store(wal.durable(shard), Ordering::Release);
        Ok(r)
    }

    fn set_retain(&self, shard: u32, lsn: Lsn) -> io::Result<()> {
        self.wal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .set_retain(shard, lsn)
    }

    fn pins(&self, shard: u32) -> bool {
        self.wal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pins(shard)
    }

    fn holds_old(&self, shard: u32) -> bool {
        self.wal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .holds_old(shard)
    }

    /// Chain segments and spares.
    pub fn files(&self) -> (Vec<u64>, Vec<u64>) {
        self.wal.lock().unwrap_or_else(|e| e.into_inner()).files()
    }

    /// The directory holding the segment files.
    pub fn dir(&self) -> &Path {
        &self.dev.dir
    }

    /// The chain and the time it was read, together: every batch through
    /// [`Chain::acked`] was durable at that time and no later one was (the
    /// acknowledged position only moves under the log lock).
    pub(crate) fn backup_snapshot(&self) -> (Chain, u64) {
        let wal = self.wal.lock().unwrap_or_else(|e| e.into_inner());
        (wal.chain(), unix_ms())
    }

    /// The first batch the backup still needs; segments holding it or later
    /// stay until it moves past them.
    pub(crate) fn set_backup_needs(&self, needs: Option<u64>) -> io::Result<()> {
        self.wal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .set_backup(needs)
    }

    pub(crate) fn attach_backup(&self, shipper: Arc<crate::backup::Shipper>) {
        *self.backup.lock().unwrap_or_else(|e| e.into_inner()) = Some(shipper);
    }

    /// Where the shards report completed checkpoints for the backup.
    pub(crate) fn backup_inbox(&self) -> Option<Arc<crate::backup::Inbox>> {
        self.backup
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|b| b.inbox())
    }
}

/// How long an I/O thread spins for the next write at least, with direct
/// submissions on a host with spare cores.
const DIRECT_SPIN_US: u64 = 50;

/// Starts writes of whatever shards submit. Ends when the log is dropped.
/// It holds the log only while it spins or works, never while it waits.
fn log_thread(log: Weak<SharedWal>) {
    let Some((wake, woken, stop)) = log
        .upgrade()
        .map(|s| (s.wake.clone(), s.woken.clone(), s.stop.clone()))
    else {
        return;
    };
    loop {
        {
            let Some(shared) = log.upgrade() else { return };
            // A submission is usually close behind a write: spin first.
            shared.spin(|| shared.kicked.load(Ordering::Acquire));
            shared.kicked.store(false, Ordering::Release);
        }
        {
            let mut w = woken.lock().unwrap_or_else(|e| e.into_inner());
            if !w.kicked && !stop.load(Ordering::Acquire) {
                w.waiting = true;
                w = wake
                    .wait_timeout(w, std::time::Duration::from_millis(100))
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
                w.waiting = false;
            }
            if stop.load(Ordering::Acquire) {
                return;
            }
            if !w.kicked {
                continue;
            }
            w.kicked = false;
        }
        let Some(shared) = log.upgrade() else { return };
        let mut wal = shared.wal.lock().unwrap_or_else(|e| e.into_inner());
        shared.start_writes(&mut wal);
    }
}

/// Writes offloaded records' bodies to the blob area outside the log lock,
/// syncs their files, then lets the sections that reference them be
/// written: a batch never references a blob that is not durable.
fn blob_thread(log: Weak<SharedWal>) {
    let Some((ready, kicked, stop)) = log
        .upgrade()
        .map(|s| (s.blob_ready.clone(), s.blob_kicked.clone(), s.stop.clone()))
    else {
        return;
    };
    loop {
        {
            let mut k = kicked.lock().unwrap_or_else(|e| e.into_inner());
            if !*k && !stop.load(Ordering::Acquire) {
                k = ready
                    .wait_timeout(k, std::time::Duration::from_millis(100))
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
            if stop.load(Ordering::Acquire) {
                return;
            }
            if !*k {
                continue;
            }
            *k = false;
        }
        let Some(shared) = log.upgrade() else { return };
        loop {
            let jobs = shared
                .wal
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take_blob_jobs();
            let Some(last) = jobs.last().map(|j| j.id) else {
                break;
            };
            let mut files: Vec<u64> = Vec::new();
            let mut written = Ok(());
            for j in &jobs {
                written = j.perform(&*shared.blob_dev);
                if written.is_err() {
                    break;
                }
                if !files.contains(&j.file) {
                    files.push(j.file);
                }
            }
            if written.is_ok() {
                written = files.iter().try_for_each(|f| shared.blob_dev.sync(*f));
            }
            let mut wal = shared.wal.lock().unwrap_or_else(|e| e.into_inner());
            match written.and_then(|()| wal.blobs_written(last)) {
                Ok(()) => shared.start_writes(&mut wal),
                Err(e) => {
                    shared.fail(e);
                    return;
                }
            }
        }
    }
}

/// Syncs started writes outside the log lock, then publishes them. Like
/// the log thread, it holds the log only while it spins or works.
fn io_thread(log: Weak<SharedWal>) {
    let Some((jobs, job_ready, stop)) = log
        .upgrade()
        .map(|s| (s.jobs.clone(), s.job_ready.clone(), s.stop.clone()))
    else {
        return;
    };
    loop {
        {
            let Some(shared) = log.upgrade() else { return };
            if shared.direct.load(Ordering::Relaxed) {
                // The next write follows the host's code, not a flush.
                let budget = match shared.spin_cap_us {
                    0 => std::time::Duration::ZERO,
                    _ => shared
                        .spin_budget()
                        .max(std::time::Duration::from_micros(DIRECT_SPIN_US)),
                };
                crate::spin::spin_until(budget, || shared.queued.load(Ordering::Acquire) > 0);
            } else {
                shared.spin(|| shared.queued.load(Ordering::Acquire) > 0);
            }
        }
        let job = {
            let mut j = jobs.lock().unwrap_or_else(|e| e.into_inner());
            if j.queue.is_empty() && !stop.load(Ordering::Acquire) {
                j.idle += 1;
                j = job_ready
                    .wait_timeout(j, std::time::Duration::from_millis(100))
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
                j.idle -= 1;
            }
            if stop.load(Ordering::Acquire) {
                return;
            }
            j.queue.pop_front()
        };
        let Some((mut w, started)) = job else {
            continue;
        };
        let Some(shared) = log.upgrade() else { return };
        shared.queued.fetch_sub(1, Ordering::AcqRel);
        // The write and its sync, outside the log lock: with O_DIRECT and
        // RWF_DSYNC the write itself is the slow part.
        let synced = Wal::perform(&shared.dev, &mut w);
        shared
            .last_flush_us
            .store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        let mut wakes = Vec::new();
        {
            let mut wal = shared.wal.lock().unwrap_or_else(|e| e.into_inner());
            match synced {
                Ok(()) => {
                    let shards = wal.complete(w);
                    shared.publish(&wal, &shards, None, &mut wakes);
                    // A slot is free: start what was staged meanwhile.
                    shared.next_write(&mut wal, None, &mut wakes);
                }
                Err(e) => shared.fail(e),
            }
        }
        for w in wakes {
            w.run();
        }
    }
}

impl Drop for SharedWal {
    fn drop(&mut self) {
        // Every shard has closed (their shutdown checkpoints are reported):
        // the backup uploads what is left, while the log can still be read.
        let backup = self
            .backup
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let backed_up = backup.is_some();
        if let Some(b) = backup {
            b.finish(self);
        }
        // After a clean close no shard needs a batch of the log: leave it
        // short for the next open ([`Wal::shorten`], which does nothing
        // after any other close). A log with a backup keeps its segments
        // as the backup knows them. A failure here costs only the next
        // open its scan: every step leaves a chain recovery accepts.
        if !backed_up
            && !self.has_failed.load(Ordering::Acquire)
            && self
                .staging
                .get_mut()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
        {
            let wal = self.wal.get_mut().unwrap_or_else(|e| e.into_inner());
            if let Err(e) = wal.shorten(unix_ms()) {
                crate::log::warn(format!("the log was not shortened at close: {e}"));
            }
        }
        self.stop.store(true, Ordering::Release);
        if self.owner {
            // The owner reads it to wake, then closes it.
            let one: u64 = 1;
            // SAFETY: writes 8 bytes from a live u64 to the log's eventfd.
            unsafe { libc::write(self.owner_fd, (&raw const one).cast(), 8) };
        }
        self.wake.notify_all();
        self.job_ready.notify_all();
        self.blob_ready.notify_all();
        // The threads hold only weak references; they exit on their own
        // (one of them may be dropping the last strong reference).
        let _ = self.threads.lock().map(|mut t| t.clear());
    }
}

/// A shard's storage in log format 3: its records go to the shared log; its
/// run store and checkpoints stay in its own directory ([`LocalEnv`]).
pub struct WalEnv {
    inner: LocalEnv,
    wal: Arc<SharedWal>,
    shard: u32,
    shard_dir: PathBuf,
    /// The last position submitted and not yet durable.
    in_flight: Option<Lsn>,
    /// The durable position last reported to the runtime.
    reported: Lsn,
    /// Checkpoints submitted and not yet reported.
    checkpoints: Vec<Lsn>,
    /// What the two newest durable checkpoints cover, newest last.
    retained: Vec<Lsn>,
}

impl WalEnv {
    pub fn open(
        shard_dir: &Path,
        shard: u32,
        opts: LocalOptions,
        wal: Arc<SharedWal>,
    ) -> io::Result<Self> {
        let mut inner = LocalEnv::open(shard_dir, opts)?;
        // `retained` counts two checkpoints of one position as two: they
        // are two files.
        inner.keep_twin_checkpoints();
        if let Some(inbox) = wal.backup_inbox() {
            inner.set_backup(shard, inbox);
        }
        let reported = wal.shards[shard as usize].durable.load(Ordering::Acquire);
        Ok(Self {
            reported,
            inner,
            wal,
            shard,
            shard_dir: shard_dir.to_owned(),
            in_flight: None,
            checkpoints: Vec::new(),
            retained: Vec::new(),
        })
    }

    fn report_retained(&mut self) -> io::Result<()> {
        if let Some(min) = self.retained.iter().copied().min() {
            self.wal.set_retain(self.shard, min)?;
        }
        Ok(())
    }

    /// What the shard's two newest checkpoint files cover.
    fn retained_on_disk(&self) -> io::Result<Vec<Lsn>> {
        let mut cps: Vec<PathBuf> = fs::read_dir(&self.shard_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "fck"))
            .collect();
        cps.sort();
        let mut out = Vec::new();
        for path in cps.iter().rev() {
            if out.len() == 2 {
                break;
            }
            if let Ok(cp) = Checkpoint::decode(&fs::read(path)?) {
                out.push(cp.lsn);
            }
        }
        out.reverse();
        Ok(out)
    }

    /// At a clean close: when the newest checkpoint covers every record of
    /// the shard, a second copy of it takes the place of the checkpoint
    /// before it. Both retained checkpoints then cover everything, the log
    /// lets the shard's batches go, and the last shard to close leaves the
    /// log short ([`Wal::shorten`]). Two files still stand for every record
    /// the log drops. A failure leaves the log holding what it held.
    fn cover_twice(&mut self) {
        let Some(&newest) = self.retained.last() else {
            return;
        };
        let covered = self.retained.len() == 2 && self.retained[0] == newest;
        let durable = self.wal.shards[self.shard as usize]
            .durable
            .load(Ordering::Acquire);
        if covered || newest == 0 || newest < durable || !self.checkpoints.is_empty() {
            return;
        }
        let twinned = self.inner.twin_checkpoint(newest).and_then(|made| {
            if made {
                self.retained = vec![newest, newest];
                self.report_retained()?;
            }
            Ok(())
        });
        if let Err(e) = twinned {
            crate::log::warn(format!(
                "shard {}: no second copy of its last checkpoint at close: {e}",
                self.shard
            ));
        }
    }
}

impl Env for WalEnv {
    fn now_ms(&self) -> u64 {
        self.inner.now_ms()
    }
    fn monotonic_us(&self) -> u64 {
        self.inner.monotonic_us()
    }
    fn random(&mut self) -> u64 {
        self.inner.random()
    }
    fn set_notify(&mut self, notify: Notify) {
        *self.wal.shards[self.shard as usize]
            .notify
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(notify.clone());
        self.inner.set_notify(notify);
    }
    fn submit(&mut self, batch: Batch) {
        self.in_flight = Some(batch.last_lsn);
        self.wal.submit(self.shard, batch);
    }
    fn max_in_flight(&self) -> usize {
        // A shard's sections follow each other into the log's writes; the
        // log publishes positions in batch order.
        self.wal.in_flight_limit
    }
    fn log_stats(&self) -> crate::env::LogStats {
        if self.shard != 0 {
            return crate::env::LogStats::default();
        }
        let [w, b, r] = &self.wal.stats;
        crate::env::LogStats {
            writes: w.load(Ordering::Relaxed),
            device_bytes: b.load(Ordering::Relaxed),
            rolls: r.load(Ordering::Relaxed),
            owner: self.wal.owner,
        }
    }
    fn supports_inline(&self) -> bool {
        self.inner.options().emulated_flush.is_zero()
    }
    fn flush_inline(&mut self, batch: Batch) -> io::Result<Lsn> {
        self.wal.write_now(self.shard, batch)
    }
    fn last_flush_us(&self) -> u64 {
        self.wal.last_flush_us.load(Ordering::Relaxed)
    }
    fn poll_flushed(&mut self) -> Option<io::Result<Lsn>> {
        let want = self.in_flight?;
        if let Some(m) = self.wal.fail_message() {
            self.in_flight = None;
            return Some(Err(io_other(m)));
        }
        let durable = self.wal.shards[self.shard as usize]
            .durable
            .load(Ordering::Acquire);
        if durable >= want {
            self.in_flight = None;
            self.reported = durable;
            return Some(Ok(durable));
        }
        // An earlier batch of several in flight.
        if durable > self.reported {
            self.reported = durable;
            return Some(Ok(durable));
        }
        None
    }
    fn store_append(&mut self, frame: &[u8]) -> io::Result<u64> {
        self.inner.store_append(frame)
    }
    fn store_append_history(&mut self, frame: &[u8], run: RunId, lsn: Lsn) -> io::Result<u64> {
        self.inner.store_append_history(frame, run, lsn)
    }
    fn store_read(&mut self, offset: u64, len: u32) -> io::Result<Vec<u8>> {
        self.inner.store_read(offset, len)
    }
    fn store_len(&self) -> u64 {
        self.inner.store_len()
    }
    fn store_generation(&self) -> u64 {
        self.inner.store_generation()
    }
    fn compact_store(&mut self, live: &[(RunId, BlobRef)]) -> io::Result<bool> {
        self.inner.compact_store(live)
    }
    fn visibility_candidates(
        &mut self,
        filter: &crate::visibility::ListFilter,
        limit: usize,
    ) -> Option<crate::visibility::Candidates> {
        self.inner.visibility_candidates(filter, limit)
    }
    fn checkpoint(&mut self, checkpoint: Checkpoint, _delete_through_segment: Option<u64>) {
        self.checkpoints.push(checkpoint.lsn);
        // Segments of the shared log are retired by what every shard's
        // retained checkpoints cover, not here.
        self.inner.checkpoint(checkpoint, None);
    }
    fn poll_checkpoint(&mut self) -> Option<io::Result<Lsn>> {
        let r = self.inner.poll_checkpoint()?;
        if let Ok(lsn) = &r
            && let Some(i) = self.checkpoints.iter().position(|l| l == lsn)
        {
            self.checkpoints.remove(i);
            self.retained.push(*lsn);
            if self.retained.len() > 2 {
                self.retained.remove(0);
            }
            if let Err(e) = self.report_retained() {
                return Some(Err(e));
            }
        }
        Some(r)
    }
    fn load_checkpoint(&mut self) -> io::Result<Option<Checkpoint>> {
        let cp = self.inner.load_checkpoint()?;
        self.retained = self.retained_on_disk()?;
        if cp.is_none() {
            // Nothing checkpointed yet: the shard needs all its records.
            self.retained = vec![0];
        }
        self.report_retained()?;
        Ok(cp)
    }
    fn load_store(&mut self, generation: u64, len: u64) -> io::Result<Vec<u8>> {
        self.inner.load_store(generation, len)
    }
    fn recover_store(
        &mut self,
        generation: u64,
        len: u64,
        visit: &mut dyn FnMut(u64, u32, fusee_core::store::StoreEntry),
    ) -> io::Result<()> {
        self.inner.recover_store(generation, len, visit)
    }
    fn segments(&mut self) -> io::Result<Vec<u64>> {
        Ok(Vec::new())
    }
    fn read_segment(&mut self, _segment: u64) -> io::Result<Vec<u8>> {
        Err(io_other("a shared log has no per-shard segments"))
    }
    fn sync_recovered(&mut self) -> io::Result<()> {
        // The shared log syncs what it accepted when it opens.
        Ok(())
    }
    fn discard_segment(&mut self, _segment: u64) -> io::Result<()> {
        Ok(())
    }
    fn open_log(&mut self, _segment: u64) -> io::Result<()> {
        Ok(())
    }
    fn exclusive_log(&self) -> bool {
        true
    }
    fn shared_log(&self) -> bool {
        true
    }
    fn recover_log(&mut self, after: Lsn) -> io::Result<(Vec<(Lsn, RecordBuf)>, Lsn)> {
        self.wal.take_recovered(self.shard, after)
    }
    fn log_position(&self) -> u64 {
        self.wal.position.load(Ordering::Acquire)
    }
    fn log_segment(&self) -> u64 {
        self.wal.segment.load(Ordering::Acquire)
    }
    fn checkpoint_wanted(&self) -> bool {
        self.wal.pins(self.shard)
    }
    fn checkpoint_on_roll(&self) -> bool {
        self.wal
            .wal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .holds_retiring(self.shard)
    }
    fn checkpoint_wanted_on_close(&self) -> bool {
        self.wal.holds_old(self.shard)
    }
    fn report_due(&mut self, due: crate::runtime::Due, closed: bool) {
        self.inner.report_due(due, closed);
        if closed {
            self.cover_twice();
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod owner;
