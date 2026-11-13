//! Real I/O: segment files, the flusher thread, the run store and
//! checkpoints. Also [`MemoryEnv`] for the `memory` durability mode.
//!
//! **Flushing.** One flush is in flight per shard log. The runtime hands a
//! batch to the flusher thread (or flushes inline when it is otherwise idle);
//! the flusher writes every chunk and then calls the platform's durable flush
//! once for the whole batch. On Linux the default is `O_DIRECT` aligned
//! writes that each carry `RWF_DSYNC`, so no separate flush call is made; a
//! directory whose filesystem refuses `O_DIRECT` uses buffered writes and
//! `fdatasync` instead. `fdatasync` and `io_uring` (with the `io-uring`
//! feature) are options. On macOS it is `F_FULLFSYNC`, because plain `fsync`
//! there does not flush the drive's cache.
//!
//! **Segments** are created ahead of time by a background thread and filled
//! with zeros, so a flush never waits for file creation or extent allocation
//! on the hot path.

use crate::env::{Env, Notify};
use fusee_core::segment::{Batch, Chunk};
use fusee_core::shard::BlobRef;
use fusee_core::store::{Checkpoint, store_header};
use fusee_core::{Lsn, RunId};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Verify the footer and every journal frame in a retained history segment.
pub fn verify_history_segment(path: &Path) -> io::Result<()> {
    crate::archive::visit(path, |_, _, _| {})
}

/// Holds exclusive writer ownership until the returned descriptor is closed.
/// Lock the directory inode itself so a stale lock file cannot survive a crash.
pub fn lock_directory(dir: &Path) -> io::Result<File> {
    use std::os::fd::AsRawFd;
    fs::create_dir_all(dir)?;
    let file = File::open(dir)?;
    // SAFETY: flock uses the open directory descriptor and no user-space pointers.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            let holder = fs::read_to_string(dir.join(WRITER_PID))
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok())
                .map(|pid| format!(" (pid {pid})"))
                .unwrap_or_default();
            return Err(io::Error::new(
                err.kind(),
                format!(
                    "{} is open in another process{holder}. One process writes a directory; \
                     use server mode for several",
                    dir.display()
                ),
            ));
        }
        return Err(err);
    }
    // Advisory, for the message above: who holds the lock. The lock itself
    // is the directory's flock, which a crash releases.
    let _ = fs::write(dir.join(WRITER_PID), format!("{}\n", std::process::id()));
    Ok(file)
}

/// The file naming the process that holds a directory's writer lock.
pub const WRITER_PID: &str = "fusee.pid";

/// How a batch is made durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlushMethod {
    /// `fdatasync` after the writes.
    Fdatasync,
    /// `fcntl(F_FULLFSYNC)` after the writes (macOS default). The only call
    /// macOS guarantees reaches stable media.
    FullFsync,
    /// Plain `fsync`. On macOS this does **not** flush the drive cache: use
    /// it only for development figures, never for durability claims.
    Fsync,
    /// Linux: each write carries `RWF_DSYNC`; no separate flush call (Linux
    /// default, together with [`LocalOptions::direct_io`]). Where `O_DIRECT`
    /// is refused, the log uses buffered writes and `fdatasync` instead.
    Dsync,
    /// Linux with the `io-uring` feature: writes and `fdatasync` linked in
    /// one submission. Writes are buffered: `direct_io` does not apply.
    IoUring,
}

impl FlushMethod {
    pub fn platform_default() -> Self {
        if cfg!(target_vendor = "apple") {
            Self::FullFsync
        } else if cfg!(target_os = "linux") {
            Self::Dsync
        } else {
            Self::Fdatasync
        }
    }
}

/// How new segment files are prepared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Preallocate {
    /// Write zeros through the whole file and flush it, so later writes are
    /// overwrites and `fdatasync` has no metadata to persist.
    Zero,
    /// Zero-fill at most one MiB when opening a shard. Prepare full-size
    /// successor segments ahead of rollover.
    ShortFirst,
    /// Only set the file length.
    Size,
    /// Grow the file as records are written.
    None,
}

#[derive(Clone, Debug)]
pub struct LocalOptions {
    pub flush: FlushMethod,
    /// Linux only (default there): open segments with `O_DIRECT` and write
    /// aligned 4 KiB blocks. The last partial block is kept in memory and
    /// written again, with the same bytes, by the next write. If the
    /// filesystem refuses `O_DIRECT` with `EINVAL`, at an open or at a write
    /// (ramfs, tmpfs before Linux 6.6, devices whose blocks are larger than
    /// 4 KiB), that log directory falls back to buffered writes, and
    /// [`FlushMethod::Dsync`] to `fdatasync`, for the life of the process;
    /// one line on stderr says so. Not used with [`FlushMethod::IoUring`] or
    /// by a replica's [`BatchWriter`]. The shared log (`crate::wal`) applies
    /// the same option with its own probe.
    pub direct_io: bool,
    pub preallocate: Preallocate,
    pub segment_bytes: u64,
    /// How long the flusher thread spins for new work before sleeping. Spinning
    /// trades a busy core for lower handoff latency under moderate load.
    pub flusher_spin: Duration,
    /// Adds this much to every flush (benchmarks that emulate slower disks).
    pub emulated_flush: Duration,
    /// Pace of background zero-filling of successor segments, so preparing
    /// one does not take the device away from the logs. Ignored once a log
    /// is waiting for the segment.
    pub prepare_bytes_per_sec: u64,
}

impl Default for LocalOptions {
    fn default() -> Self {
        Self {
            flush: FlushMethod::platform_default(),
            direct_io: cfg!(target_os = "linux"),
            preallocate: if cfg!(target_os = "linux") {
                Preallocate::ShortFirst
            } else {
                Preallocate::Size
            },
            segment_bytes: fusee_core::segment::DEFAULT_SEGMENT_BYTES,
            flusher_spin: Duration::ZERO,
            emulated_flush: Duration::ZERO,
            prepare_bytes_per_sec: 256 << 20,
        }
    }
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Monotonic microseconds for measuring waits in real storage environments.
pub fn monotonic_us() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_micros() as u64
}

fn segment_path(dir: &Path, segment: u64) -> PathBuf {
    dir.join(format!("{segment:020}.seg"))
}

/// Durable flush of one file using `method`.
pub fn flush_file(file: &File, method: FlushMethod) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let fd = file.as_raw_fd();
    // SAFETY: `fd` is owned by `file`, which outlives the call.
    let rc = unsafe {
        match method {
            #[cfg(target_vendor = "apple")]
            FlushMethod::FullFsync => libc::fcntl(fd, libc::F_FULLFSYNC),
            #[cfg(target_os = "linux")]
            FlushMethod::Fdatasync | FlushMethod::Dsync | FlushMethod::IoUring => {
                libc::fdatasync(fd)
            }
            _ => libc::fsync(fd),
        }
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Persists directory entries (created, renamed or removed files).
pub fn sync_dir(dir: &Path, method: FlushMethod) -> io::Result<()> {
    let handle = File::open(dir)?;
    flush_file(&handle, method)
}

const ALIGN: usize = 4096;

/// A heap buffer aligned for `O_DIRECT` (also used by a replica's protocol
/// log in `fusee-server`).
pub struct AlignedBuf {
    ptr: std::ptr::NonNull<u8>,
    cap: usize,
}

impl AlignedBuf {
    /// Zeroed, at least `cap` bytes rounded up to whole 4 KiB blocks.
    pub fn new(cap: usize) -> Self {
        let cap = cap.max(ALIGN).div_ceil(ALIGN) * ALIGN;
        let layout = std::alloc::Layout::from_size_align(cap, ALIGN)
            .unwrap_or_else(|_| std::alloc::handle_alloc_error(std::alloc::Layout::new::<u8>()));
        // SAFETY: the layout has non-zero size.
        let raw = unsafe { std::alloc::alloc_zeroed(layout) };
        let Some(ptr) = std::ptr::NonNull::new(raw) else {
            std::alloc::handle_alloc_error(layout)
        };
        Self { ptr, cap }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` points to `cap` initialized bytes owned by self.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.cap) }
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: as above.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.cap) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        if let Ok(layout) = std::alloc::Layout::from_size_align(self.cap, ALIGN) {
            // SAFETY: allocated in `new` with this layout.
            unsafe { std::alloc::dealloc(self.ptr.as_ptr(), layout) }
        }
    }
}

// SAFETY: the buffer is plain bytes owned by one value.
unsafe impl Send for AlignedBuf {}

struct SegFile {
    file: File,
    /// Opened with `O_DIRECT`: writes go through the aligned path below.
    direct: bool,
    /// O_DIRECT: the last partial block and its aligned offset.
    tail: Vec<u8>,
    tail_offset: u64,
}

/// Whether segments are opened with `O_DIRECT`. The io_uring path submits
/// the batch's own unaligned buffers, so it always writes buffered.
fn wants_direct(opts: &LocalOptions) -> bool {
    cfg!(target_os = "linux") && opts.direct_io && opts.flush != FlushMethod::IoUring
}

/// Whether `file` was opened with `O_DIRECT`.
#[cfg(target_os = "linux")]
fn opened_direct(file: &File) -> bool {
    use std::os::fd::AsRawFd;
    // SAFETY: F_GETFL reads the descriptor's status flags; no pointers.
    let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
    flags >= 0 && flags & libc::O_DIRECT != 0
}

#[cfg(not(target_os = "linux"))]
fn opened_direct(_file: &File) -> bool {
    false
}

/// Opens with `O_DIRECT` unless `refused` says this directory's filesystem
/// already declined it. The first `EINVAL` (the kernel's answer for a
/// filesystem without direct I/O) sets `refused` for good and opens buffered.
fn open_direct_or_buffered(
    dir: &Path,
    refused: &AtomicBool,
    direct: impl FnOnce() -> io::Result<File>,
    buffered: impl FnOnce() -> io::Result<File>,
) -> io::Result<File> {
    if !refused.load(Ordering::Acquire) {
        match direct() {
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => refuse_direct(dir, refused),
            other => return other,
        }
    }
    buffered()
}

/// Moves `dir` to buffered writes and `fdatasync` for good.
fn refuse_direct(dir: &Path, refused: &AtomicBool) {
    if !refused.swap(true, Ordering::AcqRel) {
        note_direct_refused(dir);
    }
}

/// One stderr line per filesystem per process, naming the first directory.
pub fn note_direct_refused(dir: &Path) {
    use std::os::unix::fs::MetadataExt;
    static NOTED: Mutex<Vec<u64>> = Mutex::new(Vec::new());
    let device = fs::metadata(dir).map_or(u64::MAX, |m| m.dev());
    let mut noted = NOTED.lock().unwrap_or_else(|e| e.into_inner());
    if !noted.contains(&device) {
        noted.push(device);
        crate::log::warn(format!(
            "the filesystem under {} refuses O_DIRECT (EINVAL); log segments there use buffered \
             writes and fdatasync",
            dir.display()
        ));
    }
}

enum PreparedSegment {
    Ready(File),
    /// Being prepared on a background thread. Setting the flag tells a paced
    /// preparation that the log is waiting for it, so it stops pacing.
    InProgress(std::thread::JoinHandle<io::Result<File>>, Arc<AtomicBool>),
}

impl PreparedSegment {
    fn spawn(
        dir: PathBuf,
        segment: u64,
        opts: LocalOptions,
        refused: Arc<AtomicBool>,
        paced: bool,
    ) -> io::Result<Self> {
        let urgent = Arc::new(AtomicBool::new(!paced));
        let flag = urgent.clone();
        let thread = std::thread::Builder::new()
            .name("fusee-segment".into())
            .spawn(move || {
                if paced {
                    prepare_successor(&dir, segment, &opts, &refused, &flag)
                } else {
                    prepare_segment(&dir, segment, &opts, &refused)
                }
            })?;
        Ok(Self::InProgress(thread, urgent))
    }

    fn finish(self) -> io::Result<File> {
        match self {
            Self::Ready(file) => Ok(file),
            Self::InProgress(thread, urgent) => {
                urgent.store(true, Ordering::Release);
                thread
                    .join()
                    .map_err(|_| io::Error::other("segment preparation panicked"))?
            }
        }
    }

    fn preparing(&self) -> bool {
        matches!(self, Self::InProgress(thread, _) if !thread.is_finished())
    }
}

/// Background preparations run one at a time per process: zero-filling
/// competes with the logs for the device.
static PREPARING: Mutex<()> = Mutex::new(());

/// Zero-fills a successor segment in the background without disturbing the
/// log: on Linux through `O_DIRECT` (no page-cache burst for the final
/// `fdatasync` to write back at once), in small chunks, paced to
/// `opts.prepare_bytes_per_sec` until `urgent` says the log needs the file.
fn prepare_successor(
    dir: &Path,
    segment: u64,
    opts: &LocalOptions,
    refused: &AtomicBool,
    urgent: &AtomicBool,
) -> io::Result<File> {
    if !matches!(
        opts.preallocate,
        Preallocate::Zero | Preallocate::ShortFirst
    ) {
        return prepare_segment(dir, segment, opts, refused);
    }
    let _turn = PREPARING.lock().unwrap_or_else(|e| e.into_inner());
    let path = segment_path(dir, segment);
    let mut o = OpenOptions::new();
    o.read(true).write(true).create(true).truncate(false);
    #[cfg(target_os = "linux")]
    let direct = if refused.load(Ordering::Acquire) {
        None
    } else {
        with_direct(&o).open(&path).ok()
    };
    #[cfg(not(target_os = "linux"))]
    let direct: Option<File> = None;
    let (file, aligned) = match direct {
        Some(f) => (f, true),
        None => (o.open(&path)?, false),
    };
    const CHUNK: usize = 256 << 10;
    let mut zeros = AlignedBuf::new(CHUNK);
    let zeros = zeros.as_mut_slice();
    let started = Instant::now();
    let rate = opts.prepare_bytes_per_sec.max(1);
    let mut off = 0u64;
    while off < opts.segment_bytes {
        let n = (opts.segment_bytes - off).min(CHUNK as u64) as usize;
        // O_DIRECT needs whole blocks; segment sizes are multiples of 4 KiB.
        let n = if aligned {
            n.div_ceil(ALIGN) * ALIGN
        } else {
            n
        };
        file.write_all_at(&zeros[..n], off)?;
        off += n as u64;
        if !urgent.load(Ordering::Acquire) {
            let due = Duration::from_secs_f64(off as f64 / rate as f64);
            if let Some(ahead) = due.checked_sub(started.elapsed()) {
                // Sleep in short slices so a waiting log is noticed quickly.
                let until = Instant::now() + ahead;
                while !urgent.load(Ordering::Acquire) && Instant::now() < until {
                    std::thread::sleep(ahead.min(Duration::from_millis(2)));
                }
            }
        }
    }
    flush_file(&file, opts.flush)?;
    sync_dir(dir, opts.flush)?;
    drop(file);
    open_segment(dir, segment, opts, refused)
}

struct Files {
    dir: PathBuf,
    opts: LocalOptions,
    /// Set once this directory's filesystem refuses `O_DIRECT` (at an open
    /// or at a write), for the life of the process; shared with the threads
    /// that prepare its segments. From then on segments open buffered and
    /// `Dsync` flushes with `fdatasync`.
    refused: Arc<AtomicBool>,
    open: BTreeMap<u64, SegFile>,
    prepared: Arc<Mutex<BTreeMap<u64, PreparedSegment>>>,
    scratch: Option<AlignedBuf>,
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    ring: Option<io_uring::IoUring>,
    /// Tests: fail every `O_DIRECT` write with `EINVAL`.
    #[cfg(test)]
    reject_direct_writes: bool,
}

/// Opens an existing segment for the log: with `O_DIRECT` when the options
/// ask for it and the directory has not refused it.
fn open_segment(
    dir: &Path,
    segment: u64,
    opts: &LocalOptions,
    refused: &AtomicBool,
) -> io::Result<File> {
    let path = segment_path(dir, segment);
    let mut o = OpenOptions::new();
    o.read(true).write(true);
    if !wants_direct(opts) {
        return o.open(&path);
    }
    open_direct_or_buffered(
        dir,
        refused,
        || with_direct(&o).open(&path),
        || o.open(&path),
    )
}

/// `o` with `O_DIRECT` (Linux; elsewhere `o` unchanged).
#[cfg(target_os = "linux")]
pub fn with_direct(o: &OpenOptions) -> OpenOptions {
    use std::os::unix::fs::OpenOptionsExt;
    let mut d = o.clone();
    d.custom_flags(libc::O_DIRECT);
    d
}

#[cfg(not(target_os = "linux"))]
pub fn with_direct(o: &OpenOptions) -> OpenOptions {
    o.clone()
}

/// Creates and preallocates segment `segment`, durable (file and directory).
fn prepare_segment(
    dir: &Path,
    segment: u64,
    opts: &LocalOptions,
    refused: &AtomicBool,
) -> io::Result<File> {
    let path = segment_path(dir, segment);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;
    match opts.preallocate {
        Preallocate::Zero | Preallocate::ShortFirst => {
            let zeros = vec![0u8; 1 << 20];
            let mut off = 0;
            while off < opts.segment_bytes {
                let n = (opts.segment_bytes - off).min(zeros.len() as u64) as usize;
                file.write_all_at(&zeros[..n], off)?;
                off += n as u64;
            }
        }
        Preallocate::Size => file.set_len(opts.segment_bytes)?,
        Preallocate::None => {}
    }
    flush_file(&file, opts.flush)?;
    sync_dir(dir, opts.flush)?;
    drop(file);
    open_segment(dir, segment, opts, refused)
}

impl Files {
    fn new(
        dir: &Path,
        opts: LocalOptions,
        prepared: Arc<Mutex<BTreeMap<u64, PreparedSegment>>>,
    ) -> Self {
        Self {
            dir: dir.to_owned(),
            opts,
            refused: Arc::new(AtomicBool::new(false)),
            open: BTreeMap::new(),
            prepared,
            scratch: None,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            ring: None,
            #[cfg(test)]
            reject_direct_writes: false,
        }
    }

    /// The flush this directory uses: `Dsync` becomes `fdatasync` once the
    /// filesystem has refused `O_DIRECT`.
    fn method(&self) -> FlushMethod {
        if self.opts.flush == FlushMethod::Dsync && self.refused.load(Ordering::Acquire) {
            FlushMethod::Fdatasync
        } else {
            self.opts.flush
        }
    }

    fn file(&mut self, segment: u64, starts: bool) -> io::Result<&mut SegFile> {
        if !self.open.contains_key(&segment) {
            let prepared = self
                .prepared
                .lock()
                .map_err(|_| io::Error::other("poisoned"))?
                .remove(&segment);
            let file = match prepared {
                Some(p) => p.finish()?,
                None if starts => prepare_segment(&self.dir, segment, &self.opts, &self.refused)?,
                None => open_segment(&self.dir, segment, &self.opts, &self.refused)?,
            };
            self.open.insert(
                segment,
                SegFile {
                    direct: opened_direct(&file),
                    file,
                    tail: Vec::new(),
                    tail_offset: 0,
                },
            );
            // Keep at most two segments open, never closing the one asked
            // for: a batch that spans three segments reopens the oldest to
            // flush it after the newest was opened.
            while self.open.len() > 2 {
                let Some(&oldest) = self.open.keys().find(|n| **n != segment) else {
                    break;
                };
                self.open.remove(&oldest);
            }
        }
        self.open
            .get_mut(&segment)
            .ok_or_else(|| io::Error::other("segment vanished"))
    }

    /// Writes one chunk. `dsync` makes the write itself durable
    /// (`RWF_DSYNC`); otherwise the caller flushes the segment afterwards.
    ///
    /// An `O_DIRECT` write the kernel rejects with `EINVAL` (a filesystem
    /// that accepts the open but not the write, or wants larger alignment)
    /// moves the directory to the buffered fallback: the segment is reopened
    /// buffered and the chunk written again. The rejected write transferred
    /// nothing, or a prefix of the same bytes, so rewriting is exact; the
    /// caller then sees [`method`](Self::method) is no longer `Dsync` and
    /// flushes every segment the batch touched.
    fn write_chunk(&mut self, chunk: &Chunk, dsync: bool) -> io::Result<()> {
        let mut scratch = self.scratch.take();
        let result = self.write_chunk_once(chunk, dsync, &mut scratch);
        self.scratch = scratch;
        match result {
            Err(e)
                if e.raw_os_error() == Some(libc::EINVAL)
                    && self.open.get(&chunk.segment).is_some_and(|s| s.direct) =>
            {
                refuse_direct(&self.dir, &self.refused);
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(segment_path(&self.dir, chunk.segment))?;
                let seg = self
                    .open
                    .get_mut(&chunk.segment)
                    .ok_or_else(|| io::Error::other("segment vanished"))?;
                *seg = SegFile {
                    file,
                    direct: false,
                    tail: Vec::new(),
                    tail_offset: 0,
                };
                seg.file.write_all_at(&chunk.bytes, chunk.offset)
            }
            other => other,
        }
    }

    fn write_chunk_once(
        &mut self,
        chunk: &Chunk,
        dsync: bool,
        scratch: &mut Option<AlignedBuf>,
    ) -> io::Result<()> {
        #[cfg(test)]
        let reject_direct = self.reject_direct_writes;
        let seg = self.file(chunk.segment, chunk.starts_segment)?;
        if !seg.direct {
            if dsync {
                return pwrite_dsync(&seg.file, &chunk.bytes, chunk.offset);
            }
            return seg.file.write_all_at(&chunk.bytes, chunk.offset);
        }
        if chunk.starts_segment {
            seg.tail.clear();
            seg.tail_offset = 0;
        }
        // The log only appends, and a segment's first chunk starts it, so
        // the kept block is always the one this chunk continues.
        let start = seg.tail_offset;
        if chunk.offset != start + seg.tail.len() as u64 {
            return Err(io::Error::other("non-sequential direct write"));
        }
        // The kept partial block, then the chunk, then zeros to the end of
        // the block: every write starts and ends on a block boundary and
        // comes from block-aligned memory.
        let total = seg.tail.len() + chunk.bytes.len();
        let padded = total.div_ceil(ALIGN) * ALIGN;
        let buf = match scratch {
            Some(b) if b.cap >= padded => b,
            _ => scratch.insert(AlignedBuf::new(padded.max(1 << 20))),
        };
        let slice = buf.as_mut_slice();
        slice[..seg.tail.len()].copy_from_slice(&seg.tail);
        slice[seg.tail.len()..total].copy_from_slice(&chunk.bytes);
        slice[total..padded].fill(0);
        #[cfg(test)]
        if reject_direct {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        if dsync {
            pwrite_dsync(&seg.file, &slice[..padded], start)?;
        } else {
            seg.file.write_all_at(&slice[..padded], start)?;
        }
        // Only a completed write moves the kept block forward.
        let end = start + total as u64;
        let new_tail = end & !(ALIGN as u64 - 1);
        seg.tail = slice[(new_tail - start) as usize..total].to_vec();
        seg.tail_offset = new_tail;
        Ok(())
    }

    fn flush_batch(&mut self, batch: &Batch) -> io::Result<()> {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        if self.opts.flush == FlushMethod::IoUring && batch.chunks.len() <= 2 {
            return self.flush_uring(batch);
        }
        let mut touched: Vec<u64> = Vec::with_capacity(2);
        for chunk in &batch.chunks {
            // Read per chunk: preparing the next segment can be what finds
            // that the filesystem refuses O_DIRECT.
            self.write_chunk(chunk, self.method() == FlushMethod::Dsync)?;
            if !touched.contains(&chunk.segment) {
                touched.push(chunk.segment);
            }
        }
        // A batch whose chunks were all written with RWF_DSYNC is durable.
        let method = self.method();
        if method != FlushMethod::Dsync {
            for seg in touched {
                let f = self.file(seg, false)?;
                flush_file(&f.file, method)?;
            }
        }
        Ok(())
    }

    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    fn flush_uring(&mut self, batch: &Batch) -> io::Result<()> {
        use io_uring::{opcode, squeue, types};
        use std::os::fd::AsRawFd;
        if self.ring.is_none() {
            self.ring = Some(io_uring::IoUring::new(64)?);
        }
        let mut fds = Vec::new();
        for chunk in &batch.chunks {
            let fd = self
                .file(chunk.segment, chunk.starts_segment)?
                .file
                .as_raw_fd();
            fds.push(fd);
        }
        let ring = self
            .ring
            .as_mut()
            .ok_or_else(|| io::Error::other("no ring"))?;
        let mut touched: Vec<i32> = Vec::new();
        let mut n = 0u32;
        for (chunk, fd) in batch.chunks.iter().zip(&fds) {
            let sqe = opcode::Write::new(
                types::Fd(*fd),
                chunk.bytes.as_ptr(),
                chunk.bytes.len() as u32,
            )
            .offset(chunk.offset)
            .build()
            .flags(squeue::Flags::IO_LINK)
            .user_data(chunk.bytes.len() as u64);
            // SAFETY: the buffers live in `batch` until the submission completes below.
            unsafe { ring.submission().push(&sqe) }.map_err(|_| io::Error::other("sq full"))?;
            n += 1;
            if !touched.contains(fd) {
                touched.push(*fd);
            }
        }
        for (i, fd) in touched.iter().enumerate() {
            let mut sqe = opcode::Fsync::new(types::Fd(*fd))
                .flags(types::FsyncFlags::DATASYNC)
                .build()
                .user_data(0);
            if i + 1 < touched.len() {
                sqe = sqe.flags(squeue::Flags::IO_LINK);
            }
            // SAFETY: no buffers are referenced by fsync.
            unsafe { ring.submission().push(&sqe) }.map_err(|_| io::Error::other("sq full"))?;
            n += 1;
        }
        ring.submit_and_wait(n as usize)?;
        let mut done = 0;
        let mut error = None;
        while done < n {
            for cqe in ring.completion() {
                done += 1;
                if cqe.result() < 0 {
                    error.get_or_insert(io::Error::from_raw_os_error(-cqe.result()));
                } else if cqe.result() as u64 != cqe.user_data() {
                    error.get_or_insert(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "short io_uring write",
                    ));
                }
            }
            if done < n {
                ring.submit_and_wait(1)?;
            }
        }
        error.map_or(Ok(()), Err)
    }
}

/// Writes flush batches into a log directory's segment files and flushes
/// them: how a replica applies committed batches to its own log.
pub struct BatchWriter {
    files: Files,
    dirty: BTreeMap<u64, File>,
}

impl BatchWriter {
    /// Always writes buffered: a replica re-applies batches that start in
    /// the middle of a segment after a restart, and its engine's recovery
    /// touches the same files, so the direct path's in-memory tail block
    /// would not match the file.
    pub fn new(log_dir: &Path, mut opts: LocalOptions) -> io::Result<Self> {
        fs::create_dir_all(log_dir)?;
        opts.direct_io = false;
        Ok(Self {
            dirty: BTreeMap::new(),
            files: Files::new(log_dir, opts, Arc::new(Mutex::new(BTreeMap::new()))),
        })
    }

    /// Materialize bytes already durable in the quorum log. Pin every
    /// touched descriptor until a later superblock/checkpoint barrier.
    pub fn stage(&mut self, batch: &Batch) -> io::Result<()> {
        for chunk in &batch.chunks {
            // Not durable per write: `sync_staged` flushes at the barrier.
            self.files.write_chunk(chunk, false)?;
            if let std::collections::btree_map::Entry::Vacant(slot) =
                self.dirty.entry(chunk.segment)
            {
                slot.insert(self.files.file(chunk.segment, false)?.file.try_clone()?);
            }
        }
        Ok(())
    }

    pub fn sync_staged(&mut self) -> io::Result<()> {
        for file in self.dirty.values() {
            flush_file(file, self.files.opts.flush)?;
        }
        self.dirty.clear();
        Ok(())
    }

    /// Writes the batch and makes it durable.
    pub fn write(&mut self, batch: &Batch) -> io::Result<()> {
        self.files.flush_batch(batch)
    }
}

#[cfg(target_os = "linux")]
/// Write the entire slice using the Linux per-write data synchronization flag.
pub fn pwrite_dsync(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let mut done = 0usize;
    while done < bytes.len() {
        let iov = libc::iovec {
            iov_base: bytes[done..].as_ptr() as *mut libc::c_void,
            iov_len: bytes.len() - done,
        };
        let pos = offset + done as u64;
        // The kernel ABI splits the offset into low and high native words.
        // SAFETY: iov and its byte slice remain valid for the entire syscall.
        let n = unsafe {
            libc::syscall(
                libc::SYS_pwritev2,
                file.as_raw_fd(),
                &iov,
                1 as libc::c_ulong,
                pos as libc::c_ulong,
                pos.checked_shr(libc::c_ulong::BITS).unwrap_or(0) as libc::c_ulong,
                libc::RWF_DSYNC,
            )
        };
        if n < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short RWF_DSYNC write",
            ));
        }
        done += n as usize;
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn pwrite_dsync(_file: &File, _bytes: &[u8], _offset: u64) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "RWF_DSYNC requires Linux",
    ))
}

struct FlushSlot {
    job: Option<Batch>,
    result: Option<io::Result<Lsn>>,
    stop: bool,
}

struct Flusher {
    slot: Mutex<FlushSlot>,
    cv: Condvar,
    pending: AtomicBool,
}

struct CheckpointSlot {
    result: Option<io::Result<Lsn>>,
}

pub struct LocalEnv {
    shard_dir: PathBuf,
    log_dir: PathBuf,
    opts: LocalOptions,
    files: Arc<Mutex<Files>>,
    prepared: Arc<Mutex<BTreeMap<u64, PreparedSegment>>>,
    /// The log directory's `O_DIRECT` refusal, shared with `files`.
    direct_refused: Arc<AtomicBool>,
    flusher: Arc<Flusher>,
    flusher_thread: Option<std::thread::JoinHandle<()>>,
    notify: Arc<Mutex<Option<Notify>>>,
    store: Option<Arc<File>>,
    archive_pending: Vec<crate::archive::Pending>,
    visible_prefix: Arc<Mutex<Option<crate::visibility::index::Source>>>,
    visibility: Option<Arc<crate::visibility::index::Index>>,
    store_len: u64,
    store_gen: u64,
    /// The store prefix of the last loaded checkpoint, if that checkpoint
    /// names a persisted run index: recovery reads the index instead of
    /// scanning (once; see [`Env::recover_store`]).
    indexed_prefix: Option<(u64, u64)>,
    rewrite: Option<StoreRewrite>,
    ckpt: Arc<Mutex<CheckpointSlot>>,
    checkpoint_thread: Option<std::thread::JoinHandle<()>>,
    recovered_segments: Vec<u64>,
    prepared_through: u64,
    first_segment: Option<u64>,
    rng: u64,
    last_flush_us: Arc<AtomicU64>,
    /// The shard number and where its completed checkpoints are reported
    /// for a continuous backup ([`crate::backup`]).
    backup: Option<(u32, Arc<crate::backup::Inbox>)>,
    /// A checkpoint of the position the newest one covers is written
    /// beside it, not in its place ([`checkpoint_twin`]): the shared log
    /// drops a shard's records once two checkpoints cover them, and those
    /// must then be two files.
    twin_checkpoints: bool,
}

/// A shard's checkpoint of everything through `lsn`.
fn checkpoint_file(dir: &Path, lsn: Lsn) -> PathBuf {
    dir.join(format!("checkpoint-{lsn:020}.fck"))
}

/// A second checkpoint of the same position. It sorts right after the
/// first and before any later position's, so the two newest files by name
/// are still the two newest checkpoints.
fn checkpoint_twin(dir: &Path, lsn: Lsn) -> PathBuf {
    dir.join(format!("checkpoint-{lsn:020}b.fck"))
}

/// A private candidate generation. Its name becomes authoritative only
/// when a durable checkpoint references it.
struct StoreRewrite {
    file: File,
    generation: u64,
    /// Bytes of the new generation, `pending` included.
    length: u64,
    /// Copied frames not yet written: they go out [`REWRITE_WRITE`] bytes
    /// at a time instead of one write per frame.
    pending: Vec<u8>,
}

/// Bytes a store rewrite writes at once, and the most it reads at once
/// for frames next to each other.
const REWRITE_WRITE: usize = 1 << 20;
const REWRITE_READ: u64 = 1 << 20;

impl StoreRewrite {
    fn write_pending(&mut self) -> io::Result<()> {
        if !self.pending.is_empty() {
            let at = self.length - self.pending.len() as u64;
            self.file.write_all_at(&self.pending, at)?;
            self.pending.clear();
        }
        Ok(())
    }
}

impl LocalEnv {
    pub fn open(shard_dir: &Path, opts: LocalOptions) -> io::Result<Self> {
        let log_dir = shard_dir.join("log");
        fs::create_dir_all(&log_dir)?;
        let prepared = Arc::new(Mutex::new(BTreeMap::new()));
        let files = Files::new(&log_dir, opts.clone(), prepared.clone());
        let direct_refused = files.refused.clone();
        let files = Arc::new(Mutex::new(files));
        let flusher = Arc::new(Flusher {
            slot: Mutex::new(FlushSlot {
                job: None,
                result: None,
                stop: false,
            }),
            cv: Condvar::new(),
            pending: AtomicBool::new(false),
        });
        let notify: Arc<Mutex<Option<Notify>>> = Arc::new(Mutex::new(None));
        let last_flush_us = Arc::new(AtomicU64::new(0));
        let thread = {
            let flusher = flusher.clone();
            let files = files.clone();
            let notify = notify.clone();
            let spin = opts.flusher_spin;
            let emulated = opts.emulated_flush;
            let last = last_flush_us.clone();
            std::thread::Builder::new()
                .name("fusee-flush".into())
                .spawn(move || flusher_loop(&flusher, &files, &notify, spin, emulated, &last))?
        };
        let seed = unix_ms() ^ (std::process::id() as u64).rotate_left(32);
        Ok(Self {
            shard_dir: shard_dir.to_owned(),
            log_dir,
            opts,
            files,
            prepared,
            direct_refused,
            flusher,
            flusher_thread: Some(thread),
            notify,
            store: None,
            archive_pending: Vec::new(),
            visible_prefix: Arc::new(Mutex::new(None)),
            visibility: None,
            store_len: 0,
            store_gen: 0,
            indexed_prefix: None,
            rewrite: None,
            ckpt: Arc::new(Mutex::new(CheckpointSlot { result: None })),
            checkpoint_thread: None,
            recovered_segments: Vec::new(),
            prepared_through: 0,
            first_segment: None,
            rng: seed | 1,
            last_flush_us,
            backup: None,
            twin_checkpoints: false,
        })
    }

    /// Reports every checkpoint this shard completes, with the run store it
    /// names, to a continuous backup.
    pub(crate) fn set_backup(&mut self, shard: u32, inbox: Arc<crate::backup::Inbox>) {
        self.backup = Some((shard, inbox));
    }

    /// Keeps a second checkpoint of a position beside the first instead of
    /// replacing it (see `twin_checkpoints`).
    pub(crate) fn keep_twin_checkpoints(&mut self) {
        self.twin_checkpoints = true;
    }

    /// Makes the checkpoint of `lsn` two files, durably: whichever of the
    /// two names holds a readable one is copied to the other, and the
    /// checkpoints before them are removed. Two files then cover everything
    /// through `lsn`, and no older one is left to be loaded without the
    /// records that followed it. False, with nothing changed, if neither
    /// file is a readable checkpoint of `lsn`. No checkpoint may be in
    /// progress.
    pub(crate) fn twin_checkpoint(&self, lsn: Lsn) -> io::Result<bool> {
        let dir = &self.shard_dir;
        let method = self.opts.flush;
        let (first, twin) = (checkpoint_file(dir, lsn), checkpoint_twin(dir, lsn));
        let read = |path: &Path| -> io::Result<Option<Vec<u8>>> {
            match fs::read(path) {
                Ok(b) => Ok(Checkpoint::decode(&b)
                    .is_ok_and(|cp| cp.lsn == lsn)
                    .then_some(b)),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            }
        };
        let copy = match (read(&first)?, read(&twin)?) {
            (Some(_), Some(_)) => None,
            (Some(bytes), None) => Some((bytes, &twin)),
            (None, Some(bytes)) => Some((bytes, &first)),
            (None, None) => return Ok(false),
        };
        if let Some((bytes, to)) = copy {
            let tmp = dir.join(format!("checkpoint-{lsn:020}.tmp"));
            {
                let f = File::create(&tmp)?;
                f.write_all_at(&bytes, 0)?;
                flush_file(&f, method)?;
            }
            fs::rename(&tmp, to)?;
            sync_dir(dir, method)?;
        }
        let mut removed = false;
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|x| x == "fck") && path < first {
                fs::remove_file(&path)?;
                removed = true;
            }
        }
        if removed {
            sync_dir(dir, method)?;
        }
        Ok(true)
    }

    /// Return the exact file length and the end of its last nonzero byte.
    /// Remote recovery can transmit the prefix and reconstruct all padding
    /// losslessly. Inspect the whole tail so nonzero corruption stays visible.
    pub fn recovery_segment_extent(&mut self, segment: u64) -> io::Result<(u64, u64)> {
        let file = File::open(segment_path(&self.log_dir, segment))?;
        let length = file.metadata()?.len();
        let mut end = length;
        let mut buf = vec![0; 256 << 10];
        const ZERO: [u8; 4096] = [0; 4096];
        while end != 0 {
            let size = end.min(buf.len() as u64) as usize;
            let start = end - size as u64;
            file.read_exact_at(&mut buf[..size], start)?;
            for (block, bytes) in buf[..size].chunks(ZERO.len()).enumerate().rev() {
                if bytes != &ZERO[..bytes.len()] {
                    let last = bytes.iter().rposition(|b| *b != 0).expect("nonzero block");
                    let prefix = start + (block * ZERO.len() + last + 1) as u64;
                    self.recovered_segments.push(segment);
                    return Ok((length, prefix));
                }
            }
            end = start;
        }
        self.recovered_segments.push(segment);
        Ok((length, 0))
    }

    /// Read an exact range of an already-opened recovery segment.
    pub fn read_recovery_range(
        &self,
        segment: u64,
        offset: u64,
        size: usize,
    ) -> io::Result<Vec<u8>> {
        let file = File::open(segment_path(&self.log_dir, segment))?;
        let mut data = vec![0; size];
        file.read_exact_at(&mut data, offset)?;
        Ok(data)
    }

    pub fn options(&self) -> &LocalOptions {
        &self.opts
    }

    fn store_path(&self, generation: u64) -> PathBuf {
        self.shard_dir.join(format!("runs-{generation:010}.dat"))
    }

    fn new_store(&mut self, generation: u64) -> io::Result<()> {
        let path = self.store_path(generation);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        let header = store_header();
        file.write_all_at(&header, 0)?;
        self.store = Some(Arc::new(file));
        self.store_len = header.len() as u64;
        self.store_gen = generation;
        // Older stores are superseded once a checkpoint names this one.
        Ok(())
    }

    /// Select the prefix named by a checkpoint before scanning its records.
    pub fn open_store_prefix(&mut self, generation: u64, len: u64) -> io::Result<()> {
        if generation == 0 {
            return self.new_store(1);
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.store_path(generation))?;
        if len < fusee_core::store::STORE_HEADER || len > file.metadata()?.len() {
            return Err(io::Error::other(
                "run-store checkpoint length is outside the file",
            ));
        }
        // Appends overwrite any uncommitted tail only after successful recovery.
        let file = Arc::new(file);
        *self
            .visible_prefix
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(crate::visibility::index::Source {
            file: file.clone(),
            generation,
            length: len,
        });
        self.store = Some(file);
        self.store_len = len;
        self.store_gen = generation;
        Ok(())
    }

    pub fn begin_store_rewrite(&mut self) -> io::Result<()> {
        let mut generation = self.store_gen;
        let file = loop {
            generation = generation
                .checked_add(1)
                .ok_or_else(|| io::Error::other("store generation overflow"))?;
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(self.store_path(generation))
            {
                Ok(file) => break file,
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        };
        let header = store_header();
        file.write_all_at(&header, 0)?;
        self.rewrite = Some(StoreRewrite {
            file,
            generation,
            length: header.len() as u64,
            pending: Vec::new(),
        });
        Ok(())
    }

    pub fn copy_store_frame(&mut self, run: RunId, blob: BlobRef) -> io::Result<()> {
        if blob
            .offset
            .checked_add(u64::from(blob.len))
            .is_none_or(|end| end > self.store_len)
        {
            return Err(io::Error::other("live frame outside selected generation"));
        }
        let frame = self.store_read(blob.offset, blob.len)?;
        self.copy_frame_bytes(run, &frame)
    }

    /// Copies live frames in store order, reading frames that lie next to
    /// each other with one read.
    fn copy_store_frames(&mut self, live: &[(RunId, BlobRef)]) -> io::Result<()> {
        let mut i = 0;
        while i < live.len() {
            let start = live[i].1.offset;
            let mut end = start.saturating_add(u64::from(live[i].1.len));
            let mut j = i + 1;
            while j < live.len() {
                let b = live[j].1;
                let b_end = b.offset.saturating_add(u64::from(b.len));
                if b.offset != end || b_end - start > REWRITE_READ {
                    break;
                }
                end = b_end;
                j += 1;
            }
            if j - i == 1 || end > self.store_len {
                self.copy_store_frame(live[i].0, live[i].1)?;
                i += 1;
                continue;
            }
            let span = self.store_read(start, (end - start) as u32)?;
            for &(run, b) in &live[i..j] {
                let from = (b.offset - start) as usize;
                self.copy_frame_bytes(run, &span[from..from + b.len as usize])?;
            }
            i = j;
        }
        Ok(())
    }

    fn copy_frame_bytes(&mut self, run: RunId, frame: &[u8]) -> io::Result<()> {
        crate::env::check_live_frame(frame, run)?;
        let candidate = self
            .rewrite
            .as_mut()
            .ok_or_else(|| io::Error::other("no candidate run store"))?;
        candidate.pending.extend_from_slice(frame);
        candidate.length += frame.len() as u64;
        if candidate.pending.len() >= REWRITE_WRITE {
            candidate.write_pending()?;
        }
        Ok(())
    }

    pub fn finish_store_rewrite(&mut self) -> io::Result<()> {
        let mut candidate = self
            .rewrite
            .take()
            .ok_or_else(|| io::Error::other("no candidate run store"))?;
        candidate.write_pending()?;
        self.store = Some(Arc::new(candidate.file));
        self.store_len = candidate.length;
        self.store_gen = candidate.generation;
        Ok(())
    }

    // Start preparing the successor only after this shard has consumed part
    // of its current segment. An idle shard should not zero a second 64 MiB
    // file while the first workflow is waiting for its durability gate.
    fn prepare_successor(&mut self, batch: &Batch) {
        if let Some(chunk) = batch.chunks.last()
            && chunk.segment >= self.prepared_through
            && chunk.offset + chunk.bytes.len() as u64
                >= (if self.first_segment == Some(chunk.segment) {
                    self.first_segment_bytes(self.opts.segment_bytes)
                } else {
                    self.opts.segment_bytes
                }) / 4
        {
            self.prepared_through = chunk.segment + 1;
            self.prepare_next(self.prepared_through);
        }
    }

    fn prepare_next(&self, segment: u64) {
        let dir = self.log_dir.clone();
        let opts = self.opts.clone();
        let mut prepared = self.prepared.lock().unwrap_or_else(|e| e.into_inner());
        if prepared.contains_key(&segment) || segment_path(&dir, segment).exists() {
            return;
        }
        if let Ok(p) = PreparedSegment::spawn(dir, segment, opts, self.direct_refused.clone(), true)
        {
            prepared.insert(segment, p);
        }
    }

    /// Waits for segment files still being prepared in the background.
    pub fn wait_for_preparation(&self) -> io::Result<()> {
        let mut prepared = self.prepared.lock().unwrap_or_else(|e| e.into_inner());
        let pending: Vec<u64> = prepared
            .iter()
            .filter(|(_, p)| matches!(p, PreparedSegment::InProgress(..)))
            .map(|(n, _)| *n)
            .collect();
        for n in pending {
            if let Some(p) = prepared.remove(&n) {
                prepared.insert(n, PreparedSegment::Ready(p.finish()?));
            }
        }
        Ok(())
    }

    fn notify_now(&self) {
        if let Ok(n) = self.notify.lock()
            && let Some(n) = n.as_ref()
        {
            n();
        }
    }
}

fn flusher_loop(
    flusher: &Flusher,
    files: &Mutex<Files>,
    notify: &Mutex<Option<Notify>>,
    spin: Duration,
    emulated: Duration,
    last: &AtomicU64,
) {
    loop {
        if !spin.is_zero() {
            let until = Instant::now() + spin;
            while !flusher.pending.load(Ordering::Acquire) && Instant::now() < until {
                std::hint::spin_loop();
            }
        }
        let job = {
            let Ok(mut slot) = flusher.slot.lock() else {
                return;
            };
            loop {
                if slot.stop {
                    return;
                }
                if let Some(job) = slot.job.take() {
                    break job;
                }
                slot = match flusher.cv.wait(slot) {
                    Ok(s) => s,
                    Err(_) => return,
                };
            }
        };
        flusher.pending.store(false, Ordering::Release);
        let started = Instant::now();
        let result = match files.lock() {
            Ok(mut f) => f.flush_batch(&job).map(|()| job.last_lsn),
            Err(_) => Err(io::Error::other("poisoned")),
        };
        if !emulated.is_zero() {
            std::thread::sleep(emulated);
        }
        last.store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        drop(job);
        if let Ok(mut slot) = flusher.slot.lock() {
            slot.result = Some(result);
        }
        if let Ok(n) = notify.lock()
            && let Some(n) = n.as_ref()
        {
            n();
        }
    }
}

impl Drop for LocalEnv {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.flusher.slot.lock() {
            slot.stop = true;
        }
        self.flusher.cv.notify_all();
        if let Some(t) = self.flusher_thread.take() {
            let _ = t.join();
        }
        for (_, segment) in
            std::mem::take(&mut *self.prepared.lock().unwrap_or_else(|e| e.into_inner()))
        {
            let _ = segment.finish();
        }
        if let Some(thread) = self.checkpoint_thread.take() {
            let _ = thread.join();
        }
    }
}

impl Env for LocalEnv {
    fn now_ms(&self) -> u64 {
        unix_ms()
    }
    fn monotonic_us(&self) -> u64 {
        monotonic_us()
    }

    fn random(&mut self) -> u64 {
        // xorshift64*, seeded per process; ctx.random() values are recorded,
        // so this need not be cryptographic.
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn set_notify(&mut self, notify: Notify) {
        if let Ok(mut n) = self.notify.lock() {
            *n = Some(notify);
        }
    }

    fn submit(&mut self, batch: Batch) {
        self.prepare_successor(&batch);
        if let Ok(mut slot) = self.flusher.slot.lock() {
            slot.job = Some(batch);
        }
        self.flusher.pending.store(true, Ordering::Release);
        self.flusher.cv.notify_one();
    }

    fn supports_inline(&self) -> bool {
        self.opts.emulated_flush.is_zero()
    }

    fn flush_inline(&mut self, batch: Batch) -> io::Result<Lsn> {
        self.prepare_successor(&batch);
        let started = Instant::now();
        let mut files = self
            .files
            .lock()
            .map_err(|_| io::Error::other("poisoned"))?;
        files.flush_batch(&batch)?;
        self.last_flush_us
            .store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        Ok(batch.last_lsn)
    }

    fn poll_flushed(&mut self) -> Option<io::Result<Lsn>> {
        self.flusher.slot.lock().ok()?.result.take()
    }

    fn last_flush_us(&self) -> u64 {
        self.last_flush_us.load(Ordering::Relaxed)
    }

    fn store_append(&mut self, frame: &[u8]) -> io::Result<u64> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| io::Error::other("run store not open"))?;
        let off = self.store_len;
        store.write_all_at(frame, off)?;
        self.store_len += frame.len() as u64;
        Ok(off)
    }

    fn store_append_history(&mut self, frame: &[u8], run: RunId, lsn: Lsn) -> io::Result<u64> {
        let len = u32::try_from(frame.len()).map_err(io::Error::other)?;
        let offset = self.store_append(frame)?;
        self.archive_pending.push(crate::archive::Pending {
            source: self
                .store
                .as_ref()
                .ok_or_else(|| io::Error::other("no active run store"))?
                .clone(),
            run,
            lsn,
            blob: BlobRef { offset, len },
        });
        Ok(offset)
    }
    fn store_read(&mut self, offset: u64, len: u32) -> io::Result<Vec<u8>> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| io::Error::other("run store not open"))?;
        let mut buf = vec![0u8; len as usize];
        match store.read_exact_at(&mut buf, offset) {
            Ok(()) => Ok(buf),
            // A FUSE filesystem shrinks the kernel's cached file size when
            // the daemon answers a read short, and later reads past that
            // point end early until the size is refreshed. Seen under
            // LazyFS: the bytes were intact and a second read after `fstat`
            // returned them. Local filesystems never get here.
            Err(e)
                if e.kind() == io::ErrorKind::UnexpectedEof
                    && store.metadata()?.len() >= offset + u64::from(len) =>
            {
                store.read_exact_at(&mut buf, offset)?;
                Ok(buf)
            }
            Err(e) => Err(e),
        }
    }

    fn store_len(&self) -> u64 {
        self.store_len
    }

    fn store_generation(&self) -> u64 {
        self.store_gen
    }

    fn compact_store(&mut self, live: &[(RunId, BlobRef)]) -> io::Result<bool> {
        self.begin_store_rewrite()?;
        self.copy_store_frames(live)?;
        self.finish_store_rewrite()?;
        Ok(true)
    }

    fn visibility_candidates(
        &mut self,
        filter: &crate::visibility::ListFilter,
        limit: usize,
    ) -> Option<crate::visibility::Candidates> {
        if self.visibility.is_none() {
            self.visibility = Some(Arc::new(
                crate::visibility::index::Index::start(self.shard_dir.join("visibility.redb"))
                    .ok()?,
            ));
        }
        let index = self.visibility.as_ref()?;
        let src = self.visible_prefix.lock().ok()?.clone()?;
        index.schedule(src);
        index.candidates(filter, limit)
    }

    fn checkpoint(&mut self, checkpoint: Checkpoint, delete_through_segment: Option<u64>) {
        let store = self.store.clone();
        let visible_prefix = self.visible_prefix.clone();
        let visibility = self.visibility.clone();
        let (archive, later): (Vec<_>, Vec<_>) = std::mem::take(&mut self.archive_pending)
            .into_iter()
            .partition(|p| p.lsn <= checkpoint.lsn);
        self.archive_pending = later;
        let dir = self.shard_dir.clone();
        let log_dir = self.log_dir.clone();
        let method = self.opts.flush;
        let slot = self.ckpt.clone();
        let notify = self.notify.clone();
        let store_gen = self.store_gen;
        let backup = self.backup.clone();
        let twin = self.twin_checkpoints;
        let work = move || {
            let lsn = checkpoint.lsn;
            let result = (|| -> io::Result<Lsn> {
                crate::archive::persist(&dir, lsn, method, &archive)?;
                if let Some(s) = &store {
                    flush_file(s, method)?;
                }
                let bytes = checkpoint.encode();
                let tmp = dir.join(format!("checkpoint-{lsn:020}.tmp"));
                let mut fin = checkpoint_file(&dir, lsn);
                if twin && fin.exists() {
                    // Nothing was recorded since the newest checkpoint:
                    // this one goes beside it, and the one before both
                    // goes below. Replacing the newest would leave one
                    // file for what two checkpoints are taken to cover.
                    fin = checkpoint_twin(&dir, lsn);
                }
                {
                    let f = File::create(&tmp)?;
                    f.write_all_at(&bytes, 0)?;
                    flush_file(&f, method)?;
                }
                fs::rename(&tmp, &fin)?;
                sync_dir(&dir, method)?;
                if let Some((shard, inbox)) = &backup {
                    // An open handle: the store prefix stays readable after
                    // a later compaction removes the file.
                    inbox.checkpoint(*shard, &checkpoint, bytes, store.clone());
                }
                if let Some(file) = store {
                    let src = crate::visibility::index::Source {
                        file,
                        generation: checkpoint.store_generation,
                        length: checkpoint.store_len,
                    };
                    *visible_prefix.lock().unwrap_or_else(|e| e.into_inner()) = Some(src.clone());
                    if let Some(index) = visibility {
                        index.schedule(src);
                    }
                }
                // Keep the two newest checkpoints and the stores they name.
                let mut cps: Vec<PathBuf> = fs::read_dir(&dir)?
                    .filter_map(|e| e.ok().map(|e| e.path()))
                    .filter(|p| p.extension().is_some_and(|x| x == "fck"))
                    .collect();
                cps.sort();
                let mut retained = Vec::new();
                for path in cps.iter().rev() {
                    if retained.len() < 2
                        && let Ok(saved) = Checkpoint::decode(&fs::read(path)?)
                    {
                        retained.push((saved.lsn, saved.store_generation));
                    } else {
                        fs::remove_file(path)?;
                    }
                }
                let oldest = retained
                    .last()
                    .ok_or_else(|| io::Error::other("no readable checkpoint after install"))?
                    .0;
                // Persist checkpoint removal before unlinking anything it could need.
                sync_dir(&dir, method)?;
                for entry in fs::read_dir(&dir)? {
                    let entry = entry?;
                    let name = entry.file_name();
                    if let Some(generation) = name
                        .to_string_lossy()
                        .strip_prefix("runs-")
                        .and_then(|n| n.strip_suffix(".dat"))
                        .and_then(|n| n.parse::<u64>().ok())
                        && generation < store_gen
                        && !retained.iter().any(|(_, g)| *g == generation)
                    {
                        fs::remove_file(entry.path())?;
                    }
                }
                if let Some(through) = delete_through_segment {
                    // The next activated segment's base LSN proves the preceding
                    // segment is wholly covered by *both* retained checkpoints.
                    let mut numbers = Vec::new();
                    for entry in fs::read_dir(&log_dir)? {
                        if let Some(n) = entry?
                            .file_name()
                            .to_string_lossy()
                            .strip_suffix(".seg")
                            .and_then(|n| n.parse::<u64>().ok())
                        {
                            numbers.push(n);
                        }
                    }
                    numbers.sort_unstable();
                    // A never-activated segment may lie between two real
                    // segments after a restart. Always remove a single prefix:
                    // leaving one old segment behind a gap breaks recovery.
                    let mut boundary = None;
                    for &n in numbers
                        .iter()
                        .rev()
                        .filter(|n| **n <= through.saturating_add(1))
                    {
                        let mut bytes = [0; 44];
                        let count =
                            File::open(segment_path(&log_dir, n))?.read_at(&mut bytes, 0)?;
                        if let Ok(Some(header)) =
                            fusee_core::segment::SegmentHeader::decode(&bytes[..count])
                            && header.segment == n
                            && header.base_lsn <= oldest.saturating_add(1)
                        {
                            boundary = Some(n);
                            break;
                        }
                    }
                    if let Some(boundary) = boundary {
                        for n in numbers.into_iter().take_while(|n| *n < boundary) {
                            fs::remove_file(segment_path(&log_dir, n))?;
                        }
                    }
                    sync_dir(&log_dir, method)?;
                }
                Ok(lsn)
            })();
            if let Ok(mut s) = slot.lock() {
                s.result = Some(result);
            }
            if let Ok(n) = notify.lock()
                && let Some(n) = n.as_ref()
            {
                n();
            }
        };
        match std::thread::Builder::new()
            .name("fusee-checkpoint".into())
            .spawn(work)
        {
            Ok(thread) => self.checkpoint_thread = Some(thread),
            Err(_) => {
                if let Ok(mut s) = self.ckpt.lock() {
                    s.result = Some(Err(io::Error::other("could not start checkpoint thread")));
                }
                self.notify_now();
            }
        }
    }

    fn poll_checkpoint(&mut self) -> Option<io::Result<Lsn>> {
        self.ckpt.lock().ok()?.result.take()
    }

    fn load_checkpoint(&mut self) -> io::Result<Option<Checkpoint>> {
        let mut cps: Vec<PathBuf> = fs::read_dir(&self.shard_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "fck"))
            .collect();
        cps.sort();
        self.indexed_prefix = None;
        for path in cps.iter().rev() {
            let mut bytes = Vec::new();
            File::open(path)?.read_to_end(&mut bytes)?;
            if let Ok(cp) = Checkpoint::decode(&bytes) {
                if cp.index.is_some() {
                    self.indexed_prefix = Some((cp.store_generation, cp.store_len));
                }
                return Ok(Some(cp));
            }
        }
        if !cps.is_empty() {
            return Err(io::Error::other("all saved checkpoints are damaged"));
        }
        if let Some(first) = self.segments()?.first() {
            let f = File::open(segment_path(&self.log_dir, *first))?;
            let mut head = [0; 44];
            let n = f.read_at(&mut head, 0)?;
            if fusee_core::segment::SegmentHeader::decode(&head[..n])
                .ok()
                .flatten()
                .is_some_and(|h| h.base_lsn > 1)
            {
                return Err(io::Error::other(
                    "log begins after LSN 1 and its checkpoint is missing",
                ));
            }
        }
        Ok(None)
    }

    fn load_store(&mut self, generation: u64, len: u64) -> io::Result<Vec<u8>> {
        self.open_store_prefix(generation, len)?;
        let length = usize::try_from(self.store_len).map_err(io::Error::other)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(io::Error::other)?;
        bytes.resize(length, 0);
        self.store.as_ref().unwrap().read_exact_at(&mut bytes, 0)?;
        Ok(bytes)
    }

    fn recover_store(
        &mut self,
        generation: u64,
        len: u64,
        visit: &mut dyn FnMut(u64, u32, fusee_core::store::StoreEntry),
    ) -> io::Result<()> {
        self.open_store_prefix(generation, len)?;
        if generation != 0 && self.indexed_prefix.take() == Some((generation, len)) {
            // The runtime reads the checkpoint's index frames instead.
            return Ok(());
        }
        let len = self.store_len;
        crate::env::scan_store_pages(self, len, visit)
    }

    fn segments(&mut self) -> io::Result<Vec<u64>> {
        let mut v: Vec<u64> = fs::read_dir(&self.log_dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                e.file_name()
                    .to_string_lossy()
                    .strip_suffix(".seg")
                    .and_then(|n| n.parse().ok())
            })
            .collect();
        v.sort_unstable();
        Ok(v)
    }

    fn read_segment(&mut self, segment: u64) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        File::open(segment_path(&self.log_dir, segment))?.read_to_end(&mut bytes)?;
        self.recovered_segments.push(segment);
        Ok(bytes)
    }

    fn read_segment_from(
        &mut self,
        segment: u64,
        offset: u64,
        len: usize,
    ) -> io::Result<(Vec<u8>, bool)> {
        let file = File::open(segment_path(&self.log_dir, segment))?;
        let length = file.metadata()?.len();
        if offset == 0 {
            self.recovered_segments.push(segment);
        }
        let end = offset.saturating_add(len as u64).min(length);
        let mut bytes = vec![0; end.saturating_sub(offset) as usize];
        file.read_exact_at(&mut bytes, offset.min(end))?;
        Ok((bytes, end >= length))
    }

    fn read_segment_header(&mut self, segment: u64) -> io::Result<Vec<u8>> {
        let file = File::open(segment_path(&self.log_dir, segment))?;
        let mut bytes = vec![0; fusee_core::segment::HEADER_BYTES as usize];
        let mut n = 0;
        while n < bytes.len() {
            match file.read_at(&mut bytes[n..], n as u64)? {
                0 => break,
                read => n += read,
            }
        }
        bytes.truncate(n);
        Ok(bytes)
    }

    fn sync_recovered(&mut self) -> io::Result<()> {
        for s in std::mem::take(&mut self.recovered_segments) {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(segment_path(&self.log_dir, s))?;
            flush_file(&f, self.opts.flush)?;
        }
        Ok(())
    }

    fn discard_segment(&mut self, segment: u64) -> io::Result<()> {
        if let Ok(mut f) = self.files.lock() {
            f.open.remove(&segment);
        }
        self.recovered_segments.retain(|s| *s != segment);
        fs::remove_file(segment_path(&self.log_dir, segment))?;
        sync_dir(&self.log_dir, self.opts.flush)
    }

    fn exclusive_log(&self) -> bool {
        true
    }

    fn preparing_segment(&self) -> bool {
        self.prepared
            .lock()
            .map(|p| p.values().any(PreparedSegment::preparing))
            .unwrap_or(false)
    }

    fn first_segment_bytes(&self, maximum: u64) -> u64 {
        if self.opts.preallocate == Preallocate::ShortFirst {
            maximum.min(1 << 20)
        } else {
            maximum
        }
    }

    fn open_log(&mut self, segment: u64) -> io::Result<()> {
        self.first_segment = Some(segment);
        self.prepared_through = segment;
        let mut options = self.opts.clone();
        options.segment_bytes = self.first_segment_bytes(options.segment_bytes);
        // Open does not wait for the zero-fill: the first flush that needs
        // the file joins the preparation (it is usually done by then).
        let prepared = match PreparedSegment::spawn(
            self.log_dir.clone(),
            segment,
            options.clone(),
            self.direct_refused.clone(),
            false,
        ) {
            Ok(p) => p,
            Err(_) => PreparedSegment::Ready(prepare_segment(
                &self.log_dir,
                segment,
                &options,
                &self.direct_refused,
            )?),
        };
        if let Ok(mut p) = self.prepared.lock() {
            p.insert(segment, prepared);
        }
        Ok(())
    }
}

/// The `memory` durability mode: nothing is written, everything is
/// "durable" at once. For tests.
pub struct MemoryEnv {
    rng: u64,
    done: Option<Lsn>,
    store: Vec<u8>,
    ckpt: Option<Lsn>,
    /// The test kit's virtual time: milliseconds added to the wall clock.
    #[cfg(feature = "testing")]
    pub(crate) offset: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl Default for MemoryEnv {
    fn default() -> Self {
        Self {
            rng: 0x9e37_79b9_7f4a_7c15,
            done: None,
            store: store_header(),
            ckpt: None,
            #[cfg(feature = "testing")]
            offset: None,
        }
    }
}

/// The test kits of the SDKs (whose engines' code runs in the SDK): the
/// virtual clock of every `memory` engine without its own, ms ahead of the
/// wall clock. Process-wide.
#[cfg(feature = "testing")]
pub static TEST_CLOCK_MS: AtomicU64 = AtomicU64::new(0);

impl MemoryEnv {
    #[cfg(feature = "testing")]
    fn offset_ms(&self) -> u64 {
        match &self.offset {
            Some(o) => o.load(std::sync::atomic::Ordering::Acquire),
            None => TEST_CLOCK_MS.load(std::sync::atomic::Ordering::Acquire),
        }
    }

    #[cfg(not(feature = "testing"))]
    #[inline(always)]
    fn offset_ms(&self) -> u64 {
        0
    }
}

impl Env for MemoryEnv {
    fn now_ms(&self) -> u64 {
        unix_ms() + self.offset_ms()
    }
    fn monotonic_us(&self) -> u64 {
        monotonic_us() + self.offset_ms() * 1000
    }

    fn random(&mut self) -> u64 {
        self.rng = self
            .rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.rng ^ (self.rng >> 29)
    }

    fn set_notify(&mut self, _notify: Notify) {}

    fn submit(&mut self, batch: Batch) {
        self.done = Some(batch.last_lsn);
    }

    fn durable_on_append(&self) -> bool {
        true
    }

    fn supports_inline(&self) -> bool {
        true
    }

    fn flush_inline(&mut self, batch: Batch) -> io::Result<Lsn> {
        Ok(batch.last_lsn)
    }

    fn poll_flushed(&mut self) -> Option<io::Result<Lsn>> {
        self.done.take().map(Ok)
    }

    fn store_append(&mut self, frame: &[u8]) -> io::Result<u64> {
        let off = self.store.len() as u64;
        self.store.extend_from_slice(frame);
        Ok(off)
    }

    fn store_read(&mut self, offset: u64, len: u32) -> io::Result<Vec<u8>> {
        self.store
            .get(offset as usize..offset as usize + len as usize)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| io::Error::other("out of range"))
    }

    fn store_len(&self) -> u64 {
        self.store.len() as u64
    }

    fn store_generation(&self) -> u64 {
        1
    }

    fn checkpoint(&mut self, checkpoint: Checkpoint, _delete: Option<u64>) {
        self.ckpt = Some(checkpoint.lsn);
    }

    fn poll_checkpoint(&mut self) -> Option<io::Result<Lsn>> {
        self.ckpt.take().map(Ok)
    }

    fn load_checkpoint(&mut self) -> io::Result<Option<Checkpoint>> {
        Ok(None)
    }

    fn recovers(&self) -> bool {
        false
    }

    fn load_store(&mut self, _generation: u64, _len: u64) -> io::Result<Vec<u8>> {
        Ok(self.store.clone())
    }

    fn segments(&mut self) -> io::Result<Vec<u64>> {
        Ok(Vec::new())
    }

    fn read_segment(&mut self, _segment: u64) -> io::Result<Vec<u8>> {
        Err(io::Error::other("memory mode has no segments"))
    }

    fn sync_recovered(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn discard_segment(&mut self, _segment: u64) -> io::Result<()> {
        Ok(())
    }

    fn open_log(&mut self, _segment: u64) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod segment_preparation_tests {
    use super::*;

    #[test]
    fn opening_a_short_first_segment_changes_only_the_new_file() {
        let dir = std::env::temp_dir().join(format!("fusee-first-segment-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let opts = LocalOptions {
            flush: FlushMethod::Fsync,
            segment_bytes: 4 << 20,
            preallocate: Preallocate::ShortFirst,
            ..LocalOptions::default()
        };
        let mut env = LocalEnv::open(&dir, opts.clone()).unwrap();
        env.open_log(1).unwrap();
        env.wait_for_preparation().unwrap();
        let first = segment_path(&dir.join("log"), 1);
        assert_eq!(fs::metadata(&first).unwrap().len(), 1 << 20);
        drop(env);
        let marker = b"previously durable segment data";
        OpenOptions::new()
            .write(true)
            .open(&first)
            .unwrap()
            .write_all_at(marker, 8192)
            .unwrap();
        let mut env = LocalEnv::open(&dir, opts.clone()).unwrap();
        env.open_log(2).unwrap();
        env.wait_for_preparation().unwrap();
        assert_eq!(
            &env.read_segment(1).unwrap()[8192..8192 + marker.len()],
            marker
        );
        assert_eq!(
            fs::metadata(segment_path(&dir.join("log"), 2))
                .unwrap()
                .len(),
            1 << 20
        );
        let successor =
            prepare_segment(&dir.join("log"), 3, &opts, &AtomicBool::new(false)).unwrap();
        assert_eq!(successor.metadata().unwrap().len(), 4 << 20);
        drop(successor);
        drop(env);
        let mut full = LocalEnv::open(
            &dir,
            LocalOptions {
                preallocate: Preallocate::Zero,
                ..opts
            },
        )
        .unwrap();
        full.open_log(4).unwrap();
        full.wait_for_preparation().unwrap();
        assert_eq!(
            fs::metadata(segment_path(&dir.join("log"), 4))
                .unwrap()
                .len(),
            4 << 20
        );
        drop(full);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_refused_direct_open_falls_back_once_and_other_errors_propagate() {
        // Named in the stderr line only; it does not exist.
        let dir = std::env::temp_dir().join("fusee-test-injected-einval");
        let refused = AtomicBool::new(false);
        let einval = || Err(io::Error::from_raw_os_error(libc::EINVAL));
        let buffered = || File::open(std::env::temp_dir());
        assert!(open_direct_or_buffered(&dir, &refused, einval, buffered).is_ok());
        assert!(refused.load(Ordering::Acquire));
        // Decided for the directory: O_DIRECT is not tried again.
        let unreachable = || -> io::Result<File> { panic!("O_DIRECT retried") };
        assert!(open_direct_or_buffered(&dir, &refused, unreachable, buffered).is_ok());
        // Anything other than EINVAL is an error, not a reason to fall back.
        let fresh = AtomicBool::new(false);
        let missing = || Err(io::Error::from_raw_os_error(libc::ENOENT));
        let unused = || -> io::Result<File> { panic!("fell back on ENOENT") };
        let err = open_direct_or_buffered(&dir, &fresh, missing, unused).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
        assert!(!fresh.load(Ordering::Acquire));
    }

    /// Whether the filesystem under `dir` opens files with `O_DIRECT`.
    fn accepts_direct(dir: &Path) -> bool {
        let probe = dir.join("direct-probe");
        let ok = with_direct(OpenOptions::new().write(true).create(true))
            .open(&probe)
            .is_ok_and(|f| opened_direct(&f));
        let _ = fs::remove_file(probe);
        ok
    }

    /// What one log directory looked like after [`write_two_lives`].
    struct Written {
        log: PathBuf,
        /// Segments opened with `O_DIRECT` in the first life, at its end.
        direct_open: usize,
        refused: bool,
        method: FlushMethod,
    }

    /// Writes the same batches, as a shard would, over two process lives
    /// (the first ends without a shutdown; the second opens the next
    /// segment, as recovery does). Batch ends fall inside blocks, so each
    /// direct write rewrites the previous partial block; some batches span
    /// three segments. `reject_after` fails `O_DIRECT` writes with `EINVAL`
    /// from that batch of the first life on.
    fn write_two_lives(dir: &Path, opts: LocalOptions, reject_after: Option<usize>) -> Written {
        use fusee_core::record::{Record, RecordRef, Status};
        use fusee_core::segment::LogWriter;
        let segment_bytes = opts.segment_bytes;
        let shapes = [(1, 10), (3, 700), (1, 3000), (14, 3000), (2, 5), (5, 1500)];
        let (mut segment, mut lsn, mut seq) = (1, 1, 0u32);
        let mut first = None;
        for life in 0..2 {
            let mut env = LocalEnv::open(dir, opts.clone()).unwrap();
            env.open_log(segment).unwrap();
            let mut log = LogWriter::new(0, segment, lsn, segment_bytes, 7);
            log.limit_first_segment(env.first_segment_bytes(segment_bytes));
            for (n, (records, size)) in shapes.into_iter().cycle().take(18).enumerate() {
                if life == 0 && reject_after == Some(n) {
                    env.files.lock().unwrap().reject_direct_writes = true;
                }
                for _ in 0..records {
                    seq += 1;
                    let payload = vec![seq as u8; size + seq as usize % 13];
                    let rec: RecordRef<'_> = Record::Step {
                        run: 1,
                        seq,
                        name: "step",
                        attempt: 1,
                        status: Status::Ok,
                        payload: &payload,
                        time: None,
                    };
                    log.append(&rec, 7).unwrap();
                }
                let batch = log.take().unwrap();
                assert_eq!(env.flush_inline(batch).unwrap(), log.last_lsn());
            }
            assert!(log.segment() >= segment + 6, "the batches should roll");
            env.wait_for_preparation().unwrap();
            if life == 0 {
                let files = env.files.lock().unwrap();
                first = Some(Written {
                    log: dir.join("log"),
                    direct_open: files.open.values().filter(|s| s.direct).count(),
                    refused: files.refused.load(Ordering::Acquire),
                    method: files.method(),
                });
            }
            segment = log.segment() + 1;
            lsn = log.last_lsn() + 1;
            // Dropped without a shutdown: a crash that keeps the page cache.
        }
        first.unwrap()
    }

    fn assert_same_files(a: &Path, b: &Path) {
        let names = |d: &Path| {
            let mut v: Vec<_> = fs::read_dir(d)
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect();
            v.sort();
            v
        };
        let names_a = names(a);
        assert_eq!(names_a, names(b));
        for name in names_a {
            let x = fs::read(a.join(&name)).unwrap();
            let y = fs::read(b.join(&name)).unwrap();
            assert!(x == y, "{name:?} differs between {a:?} and {b:?}");
        }
    }

    /// The default write path (on Linux `O_DIRECT` with `RWF_DSYNC`),
    /// `O_DIRECT` with `fdatasync`, and a fall back to buffered writes in
    /// the middle of a segment all leave exactly the bytes buffered writes
    /// leave, in every segment.
    #[test]
    fn direct_writes_leave_the_bytes_buffered_writes_leave() {
        let base = std::env::temp_dir().join(format!(
            "fusee-direct-bytes-{}-{}",
            std::process::id(),
            unix_ms()
        ));
        fs::create_dir_all(&base).unwrap();
        let direct = cfg!(target_os = "linux") && accepts_direct(&base);
        let defaults = LocalOptions {
            preallocate: Preallocate::Zero,
            segment_bytes: 16 << 10,
            ..LocalOptions::default()
        };
        if cfg!(target_os = "linux") {
            assert!(defaults.direct_io);
            assert_eq!(defaults.flush, FlushMethod::Dsync);
        } else {
            assert!(!defaults.direct_io);
        }
        // macOS: plain fsync, where F_FULLFSYNC is slow.
        let fast = if cfg!(target_os = "linux") {
            defaults.clone()
        } else {
            LocalOptions {
                flush: FlushMethod::Fsync,
                ..defaults.clone()
            }
        };
        let buffered = write_two_lives(
            &base.join("buffered"),
            LocalOptions {
                flush: FlushMethod::Fsync,
                direct_io: false,
                ..defaults.clone()
            },
            None,
        );
        assert_eq!(buffered.direct_open, 0);

        // A filesystem that refuses O_DIRECT (ramfs) falls back at the
        // first open instead.
        let refuses = cfg!(target_os = "linux") && !direct;
        let default = write_two_lives(&base.join("default"), fast.clone(), None);
        assert_eq!(default.direct_open > 0, direct);
        assert_eq!(default.refused, refuses);
        if refuses {
            assert_eq!(default.method, FlushMethod::Fdatasync);
        }
        assert_same_files(&default.log, &buffered.log);

        let fdatasync = write_two_lives(
            &base.join("fdatasync"),
            LocalOptions {
                flush: FlushMethod::Fdatasync,
                ..fast.clone()
            },
            None,
        );
        assert_eq!(fdatasync.direct_open > 0, direct);
        assert_same_files(&fdatasync.log, &buffered.log);

        // Rejected from the eighth batch on: in the middle of a segment,
        // with a partial block kept.
        let fallback = write_two_lives(&base.join("fallback"), fast, Some(7));
        assert_eq!(fallback.refused, cfg!(target_os = "linux"));
        assert_eq!(fallback.direct_open, 0);
        if cfg!(target_os = "linux") {
            assert_eq!(fallback.method, FlushMethod::Fdatasync);
        }
        if direct {
            eprintln!("segments were written with O_DIRECT");
        }
        assert_same_files(&fallback.log, &buffered.log);
        fs::remove_dir_all(base).unwrap();
    }
}
