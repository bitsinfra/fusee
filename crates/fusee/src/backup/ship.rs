//! The shipper: one background thread per backed-up directory that uploads
//! the shared log and the shards' checkpoints, records restore points,
//! applies retention and writes the manifest.
//!
//! **What it reads.** Segment files of the shared log through the batch
//! the log has acknowledged (bytes that are durable and never rewritten
//! while the backup holds the segment), checking that every batch continues
//! the chain before uploading it; checkpoints as the checkpoint thread
//! wrote them (handed over in memory, [`Inbox`]), with an open handle on
//! the run store they name; history archives, which are immutable.
//!
//! **Order of effects.** Objects are uploaded, then the manifest naming
//! them replaces the previous one, then objects no manifest names any more
//! are deleted, and only then may the log retire the segments uploaded.
//! An error anywhere makes the next pass reload the manifest from the
//! target and continue from what it names.

use super::manifest::{
    ArchiveEntry, BlobPack, CheckpointEntry, Current, Event, FORMAT, LogChunk, MAX_OBJECT, Point,
    Root, State, StoreChunk, keys, merge_plan, store_end,
};
use super::{BackupOptions, BackupStatus, RestorePoint, Target};
use crate::wal::{Chain, SharedWal};
use fusee_core::store::Checkpoint;
use fusee_core::wal::{BATCH_HEADER, MAX_BATCH, WAL_HEADER_BYTES, WalHeader, batch_checksum};
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Manifest events kept after a snapshot before the next one.
const SNAPSHOT_EVENTS: usize = 128;
/// Retention runs at most this often.
const RETAIN_EVERY_MS: u64 = 60_000;

fn other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

/// Wakes the shipper thread.
struct Wake {
    flag: Mutex<bool>,
    cv: Condvar,
}

impl Wake {
    fn kick(&self) {
        *self.flag.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.cv.notify_one();
    }

    fn wait(&self, timeout: Duration) {
        let mut f = self.flag.lock().unwrap_or_else(|e| e.into_inner());
        if !*f {
            f = self
                .cv
                .wait_timeout(f, timeout)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        *f = false;
    }
}

/// A checkpoint a shard completed, waiting for upload.
pub(crate) struct PendingCheckpoint {
    shard: u32,
    lsn: u64,
    log_position: u64,
    created_ms: u64,
    store_generation: u64,
    store_len: u64,
    bytes: Vec<u8>,
    store: Option<Arc<File>>,
}

/// Where shards report completed checkpoints ([`crate::local::LocalEnv`]'s
/// checkpoint thread). Only the newest checkpoint of each shard waits: an
/// older one not uploaded yet is superseded.
pub(crate) struct Inbox {
    pending: Mutex<Vec<Option<PendingCheckpoint>>>,
    wake: Arc<Wake>,
}

impl Inbox {
    /// Called by the checkpoint thread once `cp` is installed: `bytes` is
    /// its encoding, `store` an open handle on the run store it names.
    pub(crate) fn checkpoint(
        &self,
        shard: u32,
        cp: &Checkpoint,
        bytes: Vec<u8>,
        store: Option<Arc<File>>,
    ) {
        let p = PendingCheckpoint {
            shard,
            lsn: cp.lsn,
            log_position: cp.log_position,
            created_ms: cp.created_ms,
            store_generation: cp.store_generation,
            store_len: cp.store_len,
            bytes,
            store,
        };
        if let Some(slot) = self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(shard as usize)
        {
            *slot = Some(p);
        }
        self.wake.kick();
    }

    fn take(&self) -> Vec<PendingCheckpoint> {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter_mut()
            .filter_map(Option::take)
            .collect()
    }
}

/// A position in the chain: the end offset of a batch in its segment, its
/// sequence and chain checksum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Walk {
    pub end: u64,
    pub seq: u64,
    pub checksum: u64,
}

/// Follows batches in `buf` (bytes of one segment from offset `base`) from
/// `at`, checking each one's length, salted checksum, sequence and link to
/// the one before, until batch `until` or offset `stop`. Stops early, with
/// no error, at a batch `buf` does not hold whole yet.
pub(crate) fn walk(
    buf: &[u8],
    base: u64,
    salt: u64,
    mut at: Walk,
    until: u64,
    stop: u64,
) -> io::Result<Walk> {
    while at.seq < until && at.end < stop {
        let pos = (at.end - base) as usize;
        let Some(h) = buf.get(pos..pos + BATCH_HEADER) else {
            break;
        };
        let u32_at = |i: usize| u32::from_le_bytes(h[i..i + 4].try_into().unwrap());
        let u64_at = |i: usize| u64::from_le_bytes(h[i..i + 8].try_into().unwrap());
        let (len, crc, body) = (u32_at(0) as usize, u32_at(4), u32_at(8) as usize);
        if !len.is_multiple_of(8) || body < BATCH_HEADER || body > len || len > MAX_BATCH {
            return Err(other(format!(
                "log batch {} at offset {} is malformed",
                at.seq + 1,
                at.end
            )));
        }
        let Some(batch) = buf.get(pos..pos + len) else {
            break;
        };
        let covered = &batch[8..body];
        let (seq, prev) = (u64_at(16), u64_at(24));
        if crc32c::crc32c_append(crc32c::crc32c(&salt.to_le_bytes()), covered) != crc
            || seq != at.seq + 1
            || prev != at.checksum
        {
            return Err(other(format!(
                "the log does not continue with batch {} at offset {}",
                at.seq + 1,
                at.end
            )));
        }
        at = Walk {
            end: at.end + len as u64,
            seq,
            checksum: batch_checksum(covered),
        };
    }
    Ok(at)
}

/// Reads batches of a segment file from `from` until batch `until` (or
/// about `max` bytes), checking the chain; returns where it got and, if
/// `keep`, the bytes read.
pub(crate) fn read_batches(
    file: &File,
    salt: u64,
    from: Walk,
    until: u64,
    max: u64,
    keep: bool,
) -> io::Result<(Walk, Vec<u8>)> {
    let mut buf: Vec<u8> = Vec::new();
    let mut base = from.end;
    let mut read_at = from.end;
    let mut at = from;
    let stop = from.end.saturating_add(max);
    while at.seq < until && at.end < stop {
        let want = (1usize << 20).max(if buf.len() >= BATCH_HEADER {
            // A batch larger than what is buffered.
            u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize
        } else {
            0
        });
        let mut more = vec![0u8; want];
        let n = file.read_at(&mut more, read_at)?;
        if n == 0 {
            return Err(other(format!(
                "the log segment ends at offset {read_at}, before batch {until}"
            )));
        }
        more.truncate(n);
        buf.extend_from_slice(&more);
        read_at += n as u64;
        at = walk(&buf, base, salt, at, until, stop)?;
        if !keep {
            let used = (at.end - base) as usize;
            buf.drain(..used);
            base = at.end;
        }
    }
    if keep {
        buf.truncate((at.end - from.end) as usize);
    }
    Ok((at, buf))
}

/// The references to offloaded records in `buf`: whole batches, already
/// checked by [`walk`].
pub(crate) fn batch_references(buf: &[u8]) -> io::Result<Vec<fusee_core::wal::OffloadRef>> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    let bad = || other("a log batch read for the backup is malformed");
    while pos + BATCH_HEADER <= buf.len() {
        let h = &buf[pos..pos + BATCH_HEADER];
        let u32_at = |i: usize| u32::from_le_bytes(h[i..i + 4].try_into().unwrap()) as usize;
        let (len, body, sections) = (u32_at(0), u32_at(8), u32_at(48));
        if len == 0 {
            break;
        }
        let batch = buf.get(pos..pos + body).ok_or_else(bad)?;
        let mut at = BATCH_HEADER;
        for _ in 0..sections {
            let sh = batch
                .get(at..at + fusee_core::wal::SECTION_HEADER)
                .ok_or_else(bad)?;
            let n = u32::from_le_bytes(sh[16..20].try_into().unwrap()) as usize;
            let start = at + fusee_core::wal::SECTION_HEADER;
            let records = batch.get(start..start + n).ok_or_else(bad)?;
            fusee_core::wal::for_each_offload(records, |_, r| out.push(r)).map_err(other)?;
            at = start + n;
        }
        pos += len;
    }
    Ok(out)
}

/// The blobs `refs` name, read from the blob area under `wal_dir`, as one
/// pack object (see [`BlobPack`]).
pub(crate) fn pack_blobs(
    wal_dir: &Path,
    refs: &[fusee_core::wal::OffloadRef],
) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    for r in refs {
        let path = wal_dir.join("blobs").join(format!("{:020}.blob", r.file));
        let bytes = read_range(&File::open(&path)?, r.offset, r.len)?;
        if !r.matches(&bytes) {
            return Err(other(format!(
                "blob file {} does not hold the record a log batch references at {}",
                r.file, r.offset
            )));
        }
        out.extend_from_slice(&r.file.to_le_bytes());
        out.extend_from_slice(&r.offset.to_le_bytes());
        out.extend_from_slice(&r.len.to_le_bytes());
        out.extend_from_slice(&bytes);
    }
    Ok(out)
}

/// The blobs of a pack: `(file, offset, bytes)`.
pub(crate) fn unpack_blobs(pack: &[u8]) -> io::Result<Vec<(u64, u64, &[u8])>> {
    let mut out = Vec::new();
    let mut at = 0usize;
    let bad = || other("a backup blob object is malformed");
    while at < pack.len() {
        let h = pack.get(at..at + 24).ok_or_else(bad)?;
        let u = |i: usize| u64::from_le_bytes(h[i..i + 8].try_into().unwrap());
        let (file, offset, len) = (u(0), u(8), u(16) as usize);
        let bytes = pack.get(at + 24..at + 24 + len).ok_or_else(bad)?;
        out.push((file, offset, bytes));
        at += 24 + len;
    }
    Ok(out)
}

fn read_header(file: &File) -> io::Result<WalHeader> {
    let mut head = vec![0u8; WAL_HEADER_BYTES as usize];
    file.read_exact_at(&mut head, 0)?;
    WalHeader::decode(&head)
        .map_err(other)?
        .ok_or_else(|| other("the log segment has no header"))
}

fn read_range(file: &File, offset: u64, len: u64) -> io::Result<Vec<u8>> {
    let mut b = vec![0u8; len as usize];
    file.read_exact_at(&mut b, offset)?;
    Ok(b)
}

/// The next batch to upload: in segment `number`, after `at`.
#[derive(Clone, Copy, Debug)]
struct Cursor {
    number: u64,
    salt: Option<u64>,
    at: Walk,
}

/// The run store generation a shard's checkpoints name now, and how much
/// of it is uploaded.
struct StoreSource {
    generation: u64,
    epoch: u64,
    file: Arc<File>,
    uploaded: u64,
    /// The uploaded prefix was compared with the file in this process.
    verified: bool,
}

struct Work {
    state: State,
    version: u64,
    snapshot: Option<String>,
    /// Committed events after the snapshot.
    events: Vec<Event>,
    /// Applied to `state`, not yet in a manifest.
    staged: Vec<Event>,
    /// To delete once the next manifest is in place.
    deletes: Vec<String>,
    snapshot_due: bool,
    tl: String,
    cursor: Cursor,
    stores: BTreeMap<u32, StoreSource>,
    checkpoints: Vec<Option<PendingCheckpoint>>,
    /// Uploaded, not yet named by a manifest: back to `checkpoints` if the
    /// manifest write fails.
    shipped: Vec<PendingCheckpoint>,
    /// `(time, acknowledged batch)` read together, not yet uploaded.
    samples: VecDeque<Point>,
    last_sample: Point,
    last_active_upload: Option<Instant>,
    last_retain_ms: u64,
    /// Reload from the target before the next pass.
    reset: bool,
    /// Stopped for good (another writer); the log is no longer held.
    dead: bool,
    failures: u32,
    retry_at: Option<Instant>,
    last_message: Option<String>,
}

/// The backup of one directory's shared log.
pub(crate) struct Shipper {
    target: Arc<dyn Target>,
    opts: BackupOptions,
    dir: PathBuf,
    wal_dir: PathBuf,
    shards: u32,
    inbox: Arc<Inbox>,
    wake: Arc<Wake>,
    work: Mutex<Work>,
    status: Mutex<BackupStatus>,
    requests: Mutex<Vec<oneshot::Sender<io::Result<RestorePoint>>>>,
    stop: AtomicBool,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

fn new_timeline_id() -> String {
    format!("{:016x}", crate::wal::new_life())
}

#[derive(Serialize)]
struct SnapshotRef<'a> {
    format: u32,
    version: u64,
    state: &'a State,
}

impl Shipper {
    /// Reads the target, decides where the backup continues, takes the
    /// shards' newest checkpoints from disk, holds the log from there and
    /// starts the thread.
    pub(crate) fn start(
        wal: &Arc<SharedWal>,
        dir: &Path,
        shards: u32,
        target: Arc<dyn Target>,
        opts: BackupOptions,
    ) -> io::Result<Arc<Self>> {
        let wake = Arc::new(Wake {
            flag: Mutex::new(false),
            cv: Condvar::new(),
        });
        let inbox = Arc::new(Inbox {
            pending: Mutex::new((0..shards).map(|_| None).collect()),
            wake: wake.clone(),
        });
        let shipper = Arc::new(Self {
            target,
            opts,
            dir: dir.to_owned(),
            wal_dir: wal.dir().to_owned(),
            shards,
            inbox,
            wake,
            work: Mutex::new(Work {
                state: State {
                    shards,
                    timelines: Vec::new(),
                },
                version: 0,
                snapshot: None,
                events: Vec::new(),
                staged: Vec::new(),
                deletes: Vec::new(),
                snapshot_due: false,
                tl: String::new(),
                cursor: Cursor {
                    number: 0,
                    salt: None,
                    at: Walk {
                        end: 0,
                        seq: 0,
                        checksum: 0,
                    },
                },
                stores: BTreeMap::new(),
                checkpoints: (0..shards).map(|_| None).collect(),
                shipped: Vec::new(),
                samples: VecDeque::new(),
                last_sample: Point(0, 0),
                last_active_upload: None,
                last_retain_ms: 0,
                reset: false,
                dead: false,
                failures: 0,
                retry_at: None,
                last_message: None,
            }),
            status: Mutex::new(BackupStatus::default()),
            requests: Mutex::new(Vec::new()),
            stop: AtomicBool::new(false),
            thread: Mutex::new(None),
        });
        {
            let mut w = shipper.work.lock().unwrap_or_else(|e| e.into_inner());
            shipper.reconcile(&mut w, wal)?;
            // Before any shard runs: the files cannot change under us.
            shipper.capture_disk_checkpoints(&mut w)?;
            wal.set_backup_needs(Some(w.cursor.at.seq + 1))?;
            shipper.update_status(&w, wal.backup_snapshot().0.acked());
        }
        wal.attach_backup(shipper.clone());
        let weak = Arc::downgrade(wal);
        let me = shipper.clone();
        let t = std::thread::Builder::new()
            .name("fusee-backup".into())
            .spawn(move || me.run(weak))?;
        *shipper.thread.lock().unwrap_or_else(|e| e.into_inner()) = Some(t);
        Ok(shipper)
    }

    pub(crate) fn inbox(&self) -> Arc<Inbox> {
        self.inbox.clone()
    }

    pub(crate) fn status(&self) -> BackupStatus {
        self.status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub(crate) fn request(&self) -> oneshot::Receiver<io::Result<RestorePoint>> {
        let (tx, rx) = oneshot::channel();
        self.requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(tx);
        self.wake.kick();
        rx
    }

    fn tick_interval(&self) -> Duration {
        let r = self.opts.resolution.max(Duration::from_millis(10));
        self.opts
            .upload_interval
            .map_or(r, |i| i.max(Duration::from_millis(10)).min(r))
    }

    fn run(self: Arc<Self>, wal: Weak<SharedWal>) {
        let tick = self.tick_interval();
        loop {
            self.wake.wait(tick);
            if self.stop.load(Ordering::Acquire) {
                return;
            }
            let Some(log) = wal.upgrade() else { return };
            self.tick(&log, false);
            // Dropping the last reference here closes the log, which runs
            // `finish` on this thread.
            drop(log);
        }
    }

    /// The log is closing (every shard has stopped): one last upload of
    /// everything, on the closing thread.
    pub(crate) fn finish(&self, log: &SharedWal) {
        self.stop.store(true, Ordering::Release);
        self.wake.kick();
        let t = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(t) = t
            && t.thread().id() != std::thread::current().id()
        {
            let _ = t.join();
        }
        self.tick(log, true);
        for r in std::mem::take(&mut *self.requests.lock().unwrap_or_else(|e| e.into_inner())) {
            let _ = r.send(Err(other("the backup has stopped")));
        }
    }

    fn tick(&self, log: &SharedWal, closing: bool) {
        let requests =
            std::mem::take(&mut *self.requests.lock().unwrap_or_else(|e| e.into_inner()));
        let force = closing || !requests.is_empty();
        let mut w = self.work.lock().unwrap_or_else(|e| e.into_inner());
        if w.dead {
            drop(w);
            for r in requests {
                let _ = r.send(Err(other(
                    "the backup has stopped: another process writes to its target",
                )));
            }
            return;
        }
        if !force && w.retry_at.is_some_and(|t| Instant::now() < t) {
            return;
        }
        let result = self.pass(&mut w, log, force, closing);
        let acked = log.backup_snapshot().0.acked();
        match &result {
            Ok(()) => {
                w.failures = 0;
                w.retry_at = None;
                w.last_message = None;
            }
            Err(e) => {
                if w.dead {
                    // Stop holding the log: it must not fill the disk.
                    let _ = log.set_backup_needs(None);
                }
                w.reset = true;
                w.failures += 1;
                let backoff = Duration::from_millis(500 << w.failures.min(7));
                w.retry_at = Some(Instant::now() + backoff);
                let msg = format!("backup to {}: {e}", self.target.describe());
                if w.last_message.as_deref() != Some(msg.as_str()) {
                    crate::log::warn(format!("{msg}; retrying"));
                    w.last_message = Some(msg);
                }
            }
        }
        self.update_status(&w, acked);
        if let Err(e) = &result {
            self.status
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .last_error = Some(e.to_string());
        }
        let point = w
            .state
            .timeline(&w.tl)
            .and_then(|t| t.points.last())
            .map(|p| RestorePoint {
                timeline: w.tl.clone(),
                time_ms: p.0,
                seq: p.1,
            });
        drop(w);
        for r in requests {
            let reply = match (&result, &point) {
                (Ok(()), Some(p)) => Ok(p.clone()),
                (Ok(()), None) => Err(other("nothing has been written to back up yet")),
                (Err(e), _) => Err(other(e)),
            };
            let _ = r.send(reply);
        }
    }

    fn update_status(&self, w: &Work, acked: u64) {
        let mut s = self.status.lock().unwrap_or_else(|e| e.into_inner());
        s.timeline = w.tl.clone();
        s.uploaded_seq = w.cursor.at.seq;
        s.acked_seq = acked;
        s.restore_point = w
            .state
            .timeline(&w.tl)
            .and_then(|t| t.points.last())
            .map(|p| RestorePoint {
                timeline: w.tl.clone(),
                time_ms: p.0,
                seq: p.1,
            });
        s.last_error = None;
    }

    fn count_upload(&self, bytes: usize) {
        let mut s = self.status.lock().unwrap_or_else(|e| e.into_inner());
        s.objects_uploaded += 1;
        s.bytes_uploaded += bytes as u64;
    }

    fn put(&self, key: &str, bytes: &[u8]) -> io::Result<()> {
        self.target.put(key, bytes)?;
        self.count_upload(bytes.len());
        Ok(())
    }

    fn stage(&self, w: &mut Work, e: Event) -> io::Result<()> {
        w.state
            .apply(&e)
            .map_err(|m| other(format!("backup state: {m}")))?;
        w.staged.push(e);
        Ok(())
    }

    /// One round: checkpoints and log through the acknowledged position,
    /// restore points, retention, manifest, deletions, then the log may
    /// retire what is uploaded.
    fn pass(&self, w: &mut Work, log: &SharedWal, force: bool, closing: bool) -> io::Result<()> {
        if w.reset {
            self.reconcile(w, log)?;
        }
        for p in self.inbox.take() {
            let s = p.shard as usize;
            if s < w.checkpoints.len() {
                w.checkpoints[s] = Some(p);
            }
        }
        // Read after taking the checkpoints: each completed before this
        // acknowledged position.
        let (chain, now) = log.backup_snapshot();
        let acked = chain.acked();
        if acked > w.last_sample.1 {
            let p = Point(now.max(w.last_sample.0), acked);
            w.samples.push_back(p);
            w.last_sample = p;
        }
        let interval_due = self
            .opts
            .upload_interval
            .is_some_and(|i| w.last_active_upload.is_none_or(|t| t.elapsed() >= i));
        let active = force || interval_due;
        self.ship_log(w, &chain, active)?;
        if active {
            w.last_active_upload = Some(Instant::now());
        }
        for s in 0..w.checkpoints.len() {
            if let Some(p) = w.checkpoints[s].take() {
                let r = self.ship_checkpoint(w, &p, acked);
                w.shipped.push(p);
                r?;
            }
        }
        let last = w
            .state
            .timeline(&w.tl)
            .and_then(|t| t.points.last())
            .map_or(0, |p| p.1);
        let points: Vec<Point> = w
            .samples
            .iter()
            .copied()
            .filter(|p| p.1 <= w.cursor.at.seq && p.1 > last)
            .collect();
        for p in points {
            self.stage(
                w,
                Event::Point {
                    tl: w.tl.clone(),
                    point: p,
                },
            )?;
        }
        if now >= w.last_retain_ms + RETAIN_EVERY_MS && !closing {
            w.last_retain_ms = now;
            self.retain(w, now);
        }
        self.commit(w)?;
        w.shipped.clear();
        let uploaded = w.cursor.at.seq;
        while w.samples.front().is_some_and(|p| p.1 <= uploaded) {
            w.samples.pop_front();
        }
        log.set_backup_needs(Some(uploaded + 1))?;
        Ok(())
    }

    /// Uploads sealed segments from the cursor on, and the active one's
    /// acknowledged prefix if `active`.
    fn ship_log(&self, w: &mut Work, chain: &Chain, active: bool) -> io::Result<()> {
        let segs = chain.segments();
        let acked = chain.acked();
        loop {
            let i = match segs.iter().position(|s| s.0 == w.cursor.number) {
                Some(i) => i,
                None => match segs.first() {
                    // Uploaded to its end and retired before the chain
                    // moved the cursor on: continue in the oldest segment.
                    Some(&(n, _, base)) if n > w.cursor.number && base == w.cursor.at.seq + 1 => {
                        self.seal(w, w.cursor.number)?;
                        w.cursor = Cursor {
                            number: n,
                            salt: None,
                            at: Walk {
                                end: 0,
                                ..w.cursor.at
                            },
                        };
                        continue;
                    }
                    _ => {
                        return Err(other(format!(
                            "log segment {} left the log before the backup had it",
                            w.cursor.number
                        )));
                    }
                },
            };
            let (number, len, base_seq) = segs[i];
            let next = segs.get(i + 1).copied();
            let last_here = next.map_or(acked, |n| n.2 - 1).min(acked);
            if next.is_none() && !active {
                return Ok(());
            }
            if w.cursor.at.seq < last_here {
                self.upload_segment(w, number, len, base_seq, last_here)?;
            }
            match next {
                Some(n) if w.cursor.at.seq + 1 == n.2 => {
                    self.seal(w, number)?;
                    w.cursor = Cursor {
                        number: n.0,
                        salt: None,
                        at: Walk {
                            end: 0,
                            ..w.cursor.at
                        },
                    };
                }
                _ => return Ok(()),
            }
        }
    }

    /// Marks an uploaded segment the chain has left as complete.
    fn seal(&self, w: &mut Work, number: u64) -> io::Result<()> {
        let open = w
            .state
            .timeline(&w.tl)
            .and_then(|t| t.segments.iter().find(|s| s.number == number))
            .is_some_and(|s| !s.sealed);
        if open {
            self.stage(
                w,
                Event::Seal {
                    tl: w.tl.clone(),
                    number,
                },
            )?;
        }
        Ok(())
    }

    fn upload_segment(
        &self,
        w: &mut Work,
        number: u64,
        len: u64,
        base_seq: u64,
        until: u64,
    ) -> io::Result<()> {
        let file = File::open(self.wal_dir.join(format!("{number:020}.wal")))?;
        while w.cursor.at.seq < until {
            let start = w.cursor.at.end;
            let mut bytes = Vec::new();
            let mut from = w.cursor.at;
            if start == 0 || w.cursor.salt.is_none() {
                let h = read_header(&file)?;
                if start == 0
                    && (h.segment != number
                        || h.base_seq != base_seq
                        || h.base_seq != w.cursor.at.seq + 1
                        || h.base_prev != w.cursor.at.checksum)
                {
                    return Err(other(format!(
                        "log segment {number} does not continue the chain at batch {}",
                        w.cursor.at.seq + 1
                    )));
                }
                w.cursor.salt = Some(h.salt);
                if start == 0 {
                    bytes = h.encode();
                    from.end = WAL_HEADER_BYTES;
                }
            }
            let salt = w.cursor.salt.expect("read above");
            let (at, body) = read_batches(
                &file,
                salt,
                from,
                until,
                MAX_OBJECT.saturating_sub(from.end - start),
                true,
            )?;
            if at.seq == w.cursor.at.seq {
                return Err(other(format!(
                    "log segment {number} holds no batch after {}",
                    at.seq
                )));
            }
            // Offloaded records: their blobs go up before any manifest
            // names the chunk that references them.
            let refs = batch_references(&body)?;
            let mut blobs = Vec::new();
            if !refs.is_empty() {
                let pack = pack_blobs(&self.wal_dir, &refs)?;
                let key = keys::blobs(&w.tl, number, start, at.end);
                self.put(&key, &pack)?;
                blobs.push(BlobPack {
                    key,
                    bytes: pack.len() as u64,
                    crc: crc32c::crc32c(&pack),
                });
            }
            bytes.extend_from_slice(&body);
            let crc = crc32c::crc32c(&bytes);
            let key = keys::log(&w.tl, number, start, at.end);
            self.put(&key, &bytes)?;
            let chunk = LogChunk {
                key,
                offset: start,
                end: at.end,
                first_seq: w.cursor.at.seq + 1,
                last_seq: at.seq,
                last_checksum: at.checksum,
                crc,
                level: 0,
                blobs,
            };
            self.stage(
                w,
                Event::Chunk {
                    tl: w.tl.clone(),
                    number,
                    base_seq,
                    len,
                    chunk,
                },
            )?;
            w.cursor.at = at;
            self.merge_log(w, number, &file)?;
        }
        Ok(())
    }

    /// Merges the segment's newest chunks while [`merge_plan`] says so,
    /// from the local file (which the backup holds), checked against the
    /// chunks' checksums.
    fn merge_log(&self, w: &mut Work, number: u64, file: &File) -> io::Result<()> {
        loop {
            let Some(seg) = w
                .state
                .timeline(&w.tl)
                .and_then(|t| t.segments.iter().find(|s| s.number == number))
            else {
                return Ok(());
            };
            let spans: Vec<(u64, u64, u32)> = seg
                .chunks
                .iter()
                .map(|c| (c.offset, c.end, c.level))
                .collect();
            let Some(r) = merge_plan(&spans) else {
                return Ok(());
            };
            let parts = &seg.chunks[r];
            let (first, last) = (&parts[0], &parts[parts.len() - 1]);
            let mut crc = first.crc;
            for c in &parts[1..] {
                crc = crc32c::crc32c_combine(crc, c.crc, (c.end - c.offset) as usize);
            }
            let bytes = read_range(file, first.offset, last.end - first.offset)?;
            if crc32c::crc32c(&bytes) != crc {
                return Err(other(format!(
                    "log segment {number} changed after the backup read it"
                )));
            }
            let key = keys::log(&w.tl, number, first.offset, last.end);
            let merged = LogChunk {
                key: key.clone(),
                offset: first.offset,
                end: last.end,
                first_seq: first.first_seq,
                last_seq: last.last_seq,
                last_checksum: last.last_checksum,
                crc,
                level: first.level + 1,
                // The parts' blob objects stay as they are.
                blobs: parts.iter().flat_map(|c| c.blobs.clone()).collect(),
            };
            let old: Vec<String> = parts.iter().map(|c| c.key.clone()).collect();
            self.put(&key, &bytes)?;
            self.stage(
                w,
                Event::MergeLog {
                    tl: w.tl.clone(),
                    number,
                    chunk: merged,
                },
            )?;
            w.deletes.extend(old);
        }
    }

    fn ship_checkpoint(&self, w: &mut Work, p: &PendingCheckpoint, acked: u64) -> io::Result<()> {
        let key = keys::checkpoint(&w.tl, p.shard, p.lsn, p.created_ms);
        if w.state
            .timeline(&w.tl)
            .is_some_and(|t| t.checkpoints.iter().any(|c| c.key == key))
        {
            return Ok(());
        }
        let mut epoch = 0;
        if p.store_generation != 0 {
            epoch = self.ship_store(w, p)?;
        }
        self.ship_archives(w, p.shard, p.lsn)?;
        self.put(&key, &p.bytes)?;
        let entry = CheckpointEntry {
            shard: p.shard,
            key,
            lsn: p.lsn,
            log_position: p.log_position,
            usable_from: acked,
            created_ms: p.created_ms,
            bytes: p.bytes.len() as u64,
            crc: crc32c::crc32c(&p.bytes),
            store_generation: p.store_generation,
            store_len: p.store_len,
            store_epoch: epoch,
        };
        self.stage(
            w,
            Event::Checkpoint {
                tl: w.tl.clone(),
                entry,
            },
        )
    }

    /// Uploads the part of the run store prefix `p` names that is not
    /// uploaded yet. Returns the epoch that holds the prefix.
    fn ship_store(&self, w: &mut Work, p: &PendingCheckpoint) -> io::Result<u64> {
        let file = p.store.clone().ok_or_else(|| {
            other(format!(
                "shard {}: the checkpoint names no open run store",
                p.shard
            ))
        })?;
        let generation = p.store_generation;
        let (max_epoch, uploaded_epoch) = {
            let t = w.state.timeline(&w.tl).expect("current timeline");
            let mine = t
                .stores
                .iter()
                .filter(|c| c.shard == p.shard && c.generation == generation);
            (
                mine.clone().map(|c| c.epoch).max(),
                mine.map(|c| c.epoch).max(),
            )
        };
        let mut src = match w.stores.remove(&p.shard) {
            Some(mut s) if s.generation == generation => {
                s.file = file;
                s
            }
            _ => match uploaded_epoch {
                Some(e) => StoreSource {
                    generation,
                    epoch: e,
                    file,
                    uploaded: store_end(
                        &w.state.timeline(&w.tl).expect("current").stores,
                        p.shard,
                        generation,
                        e,
                    ),
                    verified: false,
                },
                None => StoreSource {
                    generation,
                    epoch: 0,
                    file,
                    uploaded: 0,
                    verified: true,
                },
            },
        };
        if !src.verified {
            // Uploaded by an earlier process: the file must still hold those
            // bytes (it does unless it was rewritten, after a restore or a
            // damaged checkpoint).
            let t = w.state.timeline(&w.tl).expect("current");
            let same = src.uploaded <= p.store_len
                && t.stores
                    .iter()
                    .filter(|c| {
                        (c.shard, c.generation, c.epoch) == (p.shard, generation, src.epoch)
                    })
                    .all(|c| {
                        read_range(&src.file, c.offset, c.end - c.offset)
                            .is_ok_and(|b| crc32c::crc32c(&b) == c.crc)
                    });
            if !same {
                src.epoch = max_epoch.map_or(0, |e| e + 1);
                src.uploaded = 0;
            }
            src.verified = true;
        }
        if p.store_len < src.uploaded {
            src.epoch = max_epoch.map_or(0, |e| e + 1).max(src.epoch + 1);
            src.uploaded = 0;
        }
        let result = (|| {
            while src.uploaded < p.store_len {
                let n = (p.store_len - src.uploaded).min(MAX_OBJECT);
                let bytes = read_range(&src.file, src.uploaded, n)?;
                let end = src.uploaded + n;
                let key = keys::store(&w.tl, p.shard, generation, src.epoch, src.uploaded, end);
                self.put(&key, &bytes)?;
                self.stage(
                    w,
                    Event::Store {
                        tl: w.tl.clone(),
                        chunk: StoreChunk {
                            shard: p.shard,
                            generation,
                            epoch: src.epoch,
                            key,
                            offset: src.uploaded,
                            end,
                            crc: crc32c::crc32c(&bytes),
                            level: 0,
                        },
                    },
                )?;
                src.uploaded = end;
                self.merge_store(w, &src, p.shard)?;
            }
            Ok(src.epoch)
        })();
        let epoch = src.epoch;
        w.stores.insert(p.shard, src);
        result.map(|_| epoch)
    }

    fn merge_store(&self, w: &mut Work, src: &StoreSource, shard: u32) -> io::Result<()> {
        loop {
            let t = w.state.timeline(&w.tl).expect("current");
            let mut mine: Vec<&StoreChunk> = t
                .stores
                .iter()
                .filter(|c| (c.shard, c.generation, c.epoch) == (shard, src.generation, src.epoch))
                .collect();
            mine.sort_by_key(|c| c.offset);
            let spans: Vec<(u64, u64, u32)> =
                mine.iter().map(|c| (c.offset, c.end, c.level)).collect();
            let Some(r) = merge_plan(&spans) else {
                return Ok(());
            };
            let parts = &mine[r];
            let (first, last) = (parts[0], parts[parts.len() - 1]);
            let mut crc = first.crc;
            for c in &parts[1..] {
                crc = crc32c::crc32c_combine(crc, c.crc, (c.end - c.offset) as usize);
            }
            let bytes = read_range(&src.file, first.offset, last.end - first.offset)?;
            if crc32c::crc32c(&bytes) != crc {
                return Err(other(format!(
                    "shard {shard}: run store generation {} changed after the backup read it",
                    src.generation
                )));
            }
            let key = keys::store(
                &w.tl,
                shard,
                src.generation,
                src.epoch,
                first.offset,
                last.end,
            );
            let merged = StoreChunk {
                shard,
                generation: src.generation,
                epoch: src.epoch,
                key: key.clone(),
                offset: first.offset,
                end: last.end,
                crc,
                level: first.level + 1,
            };
            let old: Vec<String> = parts.iter().map(|c| c.key.clone()).collect();
            self.put(&key, &bytes)?;
            self.stage(
                w,
                Event::MergeStore {
                    tl: w.tl.clone(),
                    chunk: merged,
                },
            )?;
            w.deletes.extend(old);
        }
    }

    /// History archives a checkpoint at `lsn` covers that are not uploaded.
    fn ship_archives(&self, w: &mut Work, shard: u32, lsn: u64) -> io::Result<()> {
        let dir = self.dir.join(format!("shard-{shard:03}")).join("archive");
        let entries = match fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        let mut names: Vec<(u64, String)> = entries
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_str()?.to_owned();
                let through = name.strip_suffix(".arc")?.split('-').next()?.parse().ok()?;
                Some((through, name))
            })
            .filter(|(through, _)| *through <= lsn)
            .collect();
        names.sort();
        for (through, name) in names {
            let key = keys::archive(&w.tl, shard, &name);
            if w.state
                .timeline(&w.tl)
                .is_some_and(|t| t.archives.iter().any(|a| a.key == key))
            {
                continue;
            }
            let bytes = fs::read(dir.join(&name))?;
            self.put(&key, &bytes)?;
            self.stage(
                w,
                Event::Archive {
                    tl: w.tl.clone(),
                    entry: ArchiveEntry {
                        shard,
                        name,
                        through,
                        key,
                        bytes: bytes.len() as u64,
                        crc: crc32c::crc32c(&bytes),
                    },
                },
            )?;
        }
        Ok(())
    }

    fn retain(&self, w: &mut Work, now: u64) {
        let stores: Vec<(u32, u64, u64)> = w
            .stores
            .iter()
            .map(|(shard, s)| (*shard, s.generation, s.epoch))
            .collect();
        let points: usize = w.state.timelines.iter().map(|t| t.points.len()).sum();
        let timelines = w.state.timelines.len();
        let removed = w.state.retain(
            now,
            self.opts.retention.as_millis() as u64,
            Some(Current {
                timeline: &w.tl,
                uploaded: w.cursor.at.seq,
                stores: &stores,
            }),
        );
        let points_after: usize = w.state.timelines.iter().map(|t| t.points.len()).sum();
        if !removed.is_empty() || points_after != points || w.state.timelines.len() != timelines {
            w.snapshot_due = true;
            w.deletes.extend(removed);
        }
    }

    /// Writes the manifest (with a new snapshot when due), then deletes what
    /// it no longer names.
    fn commit(&self, w: &mut Work) -> io::Result<()> {
        if w.staged.is_empty() && !w.snapshot_due {
            return Ok(());
        }
        // One writer per target: a manifest another process wrote since
        // ours means two directories share this target.
        let current = super::restore::read_root(&*self.target)?;
        if current.as_ref().map_or(0, |r| r.version) != w.version {
            w.dead = true;
            return Err(other(
                "the manifest changed under the backup: another process writes to this target",
            ));
        }
        let version = w.version + 1;
        let snapshot_now = w.snapshot_due || w.events.len() + w.staged.len() > SNAPSHOT_EVENTS;
        let (snapshot, events) = if snapshot_now {
            let key = keys::snapshot(version);
            let bytes = serde_json::to_vec(&SnapshotRef {
                format: FORMAT,
                version,
                state: &w.state,
            })
            .map_err(other)?;
            self.put(&key, &bytes)?;
            (Some(key), Vec::new())
        } else {
            let mut e = w.events.clone();
            e.extend(w.staged.iter().cloned());
            (w.snapshot.clone(), e)
        };
        let root = Root {
            format: FORMAT,
            shards: self.shards,
            version,
            updated_ms: crate::local::unix_ms(),
            snapshot: snapshot.clone(),
            events,
        };
        if let Err(e) = self
            .target
            .put_manifest(&serde_json::to_vec(&root).map_err(other)?)
        {
            if snapshot_now && let Some(k) = &snapshot {
                let _ = self.target.delete(k);
            }
            return Err(e);
        }
        if snapshot != w.snapshot
            && let Some(old) = w.snapshot.take()
        {
            w.deletes.push(old);
        }
        w.snapshot = snapshot;
        w.events = root.events;
        w.version = version;
        w.staged.clear();
        w.snapshot_due = false;
        self.status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .manifest_writes += 1;
        let mut deleted = 0;
        for k in std::mem::take(&mut w.deletes) {
            // A failed delete leaves an object no manifest names; the next
            // start removes it.
            if self.target.delete(&k).is_ok() {
                deleted += 1;
            }
        }
        self.status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .objects_deleted += deleted;
        Ok(())
    }

    /// Loads the manifest, deletes objects it does not name (debris of an
    /// interrupted upload), and finds where the backup continues: right
    /// after the last batch uploaded if the local log still holds it with
    /// the same checksum, else in a new timeline from the oldest segment.
    fn reconcile(&self, w: &mut Work, log: &SharedWal) -> io::Result<()> {
        let names = self.target.list("")?;
        match super::restore::load_state(&*self.target)? {
            None => {
                if !names.is_empty() {
                    return Err(other(
                        "the target holds objects but no manifest; give the backup an empty \
                         target",
                    ));
                }
                w.state = State {
                    shards: self.shards,
                    timelines: Vec::new(),
                };
                w.version = 0;
                w.snapshot = None;
                w.events = Vec::new();
            }
            Some((root, state)) => {
                if state.shards != self.shards {
                    return Err(other(format!(
                        "the target backs up a directory with {} shards, this one has {}",
                        state.shards, self.shards
                    )));
                }
                w.state = state;
                w.version = root.version;
                w.snapshot = root.snapshot;
                w.events = root.events;
            }
        }
        w.staged.clear();
        w.deletes.clear();
        w.snapshot_due = false;
        w.stores.clear();
        // Checkpoints uploaded since the last manifest: again, unless the
        // shard has reported a newer one.
        for p in std::mem::take(&mut w.shipped) {
            let slot = &mut w.checkpoints[p.shard as usize];
            if slot.is_none() {
                *slot = Some(p);
            }
        }
        let mut named = w.state.keys();
        named.extend(w.snapshot.clone());
        for n in names {
            if !named.contains(&n) {
                self.target.delete(&n)?;
            }
        }
        self.target.cleanup()?;
        let (chain, now) = log.backup_snapshot();
        let resumed = self.resume(&w.state, &chain)?;
        let new_timeline = resumed.is_none();
        match resumed {
            Some((tl, cursor)) => {
                w.tl = tl;
                w.cursor = cursor;
            }
            None => {
                let &(number, _, base) = chain
                    .segments()
                    .first()
                    .ok_or_else(|| other("the log has no segment"))?;
                let h = read_header(&File::open(self.wal_dir.join(format!("{number:020}.wal")))?)?;
                w.tl = new_timeline_id();
                w.cursor = Cursor {
                    number,
                    salt: Some(h.salt),
                    at: Walk {
                        end: 0,
                        seq: base - 1,
                        checksum: h.base_prev,
                    },
                };
                self.stage(
                    w,
                    Event::Timeline {
                        id: w.tl.clone(),
                        created_ms: now,
                    },
                )?;
            }
        }
        let last = w
            .state
            .timeline(&w.tl)
            .and_then(|t| t.points.last())
            .copied()
            .unwrap_or(Point(0, 0));
        w.last_sample = Point(w.last_sample.0.max(last.0), w.last_sample.1.max(last.1));
        w.samples.retain(|p| p.1 > last.1);
        w.reset = false;
        if new_timeline && w.version > 0 {
            // Mid-life: the shards' checkpoints went to the old timeline.
            self.capture_disk_checkpoints(w)?;
        }
        Ok(())
    }

    /// Where the newest timeline continues in the local log, if it does.
    fn resume(&self, state: &State, chain: &Chain) -> io::Result<Option<(String, Cursor)>> {
        let Some(t) = state.timelines.last() else {
            return Ok(None);
        };
        let Some(seg) = t.segments.last() else {
            return Ok(None);
        };
        let Some(c) = seg.chunks.last() else {
            return Ok(None);
        };
        let segs = chain.segments();
        let open = |n: u64| File::open(self.wal_dir.join(format!("{n:020}.wal")));
        if let Some(&(number, len, base)) = segs.iter().find(|s| s.0 == seg.number) {
            if base != seg.base_seq || len != seg.len {
                return Ok(None);
            }
            let file = open(number)?;
            let Ok(h) = read_header(&file) else {
                return Ok(None);
            };
            let from = Walk {
                end: WAL_HEADER_BYTES,
                seq: h.base_seq - 1,
                checksum: h.base_prev,
            };
            return Ok(
                match read_batches(&file, h.salt, from, c.last_seq, u64::MAX, false) {
                    Ok((at, _))
                        if at.seq == c.last_seq
                            && at.end == c.end
                            && at.checksum == c.last_checksum =>
                    {
                        Some((
                            t.id.clone(),
                            Cursor {
                                number,
                                salt: Some(h.salt),
                                at,
                            },
                        ))
                    }
                    _ => None,
                },
            );
        }
        // The uploaded segment has been retired: the log must continue
        // with the batch after it in its oldest segment.
        let Some(&(number, _, base)) = segs.first() else {
            return Ok(None);
        };
        if number <= seg.number || base != c.last_seq + 1 {
            return Ok(None);
        }
        let Ok(h) = read_header(&open(number)?) else {
            return Ok(None);
        };
        if h.base_prev != c.last_checksum {
            return Ok(None);
        }
        Ok(Some((
            t.id.clone(),
            Cursor {
                number: seg.number,
                salt: None,
                at: Walk {
                    end: c.end,
                    seq: c.last_seq,
                    checksum: c.last_checksum,
                },
            },
        )))
    }

    /// Each shard's newest checkpoint on disk, for upload (unless one the
    /// shard reported since is newer).
    fn capture_disk_checkpoints(&self, w: &mut Work) -> io::Result<()> {
        for shard in 0..self.shards {
            if w.checkpoints[shard as usize].is_some() {
                continue;
            }
            let dir = self.dir.join(format!("shard-{shard:03}"));
            let mut cps: Vec<PathBuf> = match fs::read_dir(&dir) {
                Ok(e) => e
                    .filter_map(|e| e.ok().map(|e| e.path()))
                    .filter(|p| p.extension().is_some_and(|x| x == "fck"))
                    .collect(),
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            cps.sort();
            for path in cps.iter().rev() {
                let Ok(bytes) = fs::read(path) else { continue };
                let Ok(cp) = Checkpoint::decode(&bytes) else {
                    continue;
                };
                let store = if cp.store_generation == 0 {
                    None
                } else {
                    match File::open(dir.join(format!("runs-{:010}.dat", cp.store_generation))) {
                        Ok(f) => Some(Arc::new(f)),
                        Err(_) => continue,
                    }
                };
                w.checkpoints[shard as usize] = Some(PendingCheckpoint {
                    shard,
                    lsn: cp.lsn,
                    log_position: cp.log_position,
                    created_ms: cp.created_ms,
                    store_generation: cp.store_generation,
                    store_len: cp.store_len,
                    bytes,
                    store,
                });
                break;
            }
        }
        Ok(())
    }
}
