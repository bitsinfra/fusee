//! The primary's own protocol-log writes, off the I/O thread.
//!
//! An entry commits once any quorum holds it durably, so the primary does
//! not wait for its own disk: the I/O thread places each round's frames in
//! `vsr/log` and hands the write and its flush to a helper, then goes on
//! reading acknowledgments. Up to [`IN_FLIGHT`] writes run at once. While
//! the log is busy (a round's frames found a write in flight) every write
//! is padded to the end of its 4 KiB block ([`fusee_core::wal::batch_span`],
//! the shared log's rule), so the next can start at once and two writes in
//! flight never share a block, as `O_DIRECT` requires; on an idle log a
//! write is packed and the next one waits for it. Durable positions are
//! reported in placement order.
//!
//! With `O_DIRECT` ([`LogFile::direct`]) every write covers whole blocks
//! from aligned memory: the log keeps its last partial block and the next
//! write rewrites it, with the same bytes, ahead of its own. With
//! [`FlushMethod::Dsync`] the write carries `RWF_DSYNC` and needs no flush.
//! A filesystem that refuses `O_DIRECT` with `EINVAL` at a write moves the
//! log to buffered writes and `fdatasync` for good, as the shared log does.
//!
//! A finished write wakes the I/O thread only when it asked for that
//! ([`LogIo::wake_on_done`]): when the write can complete a quorum, or
//! another write waits for it. Otherwise the next acknowledgment or tick
//! finds it done, and a sequential step on a small host pays one wake-up
//! fewer.
//!
//! Backups write inline on the I/O thread: their flush is on the commit
//! path, and a handoff each way would cost more than it overlaps.
use super::{FlushMethod, flush_file};
use fusee::local::AlignedBuf;
use fusee_core::vsr::Op;
use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

pub(super) const IN_FLIGHT: usize = 2;

/// Bytes to write: packed frames, or whole blocks from aligned memory.
pub(super) enum WriteBuf {
    Plain(Vec<u8>),
    Aligned { buf: AlignedBuf, len: usize },
}

impl WriteBuf {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Plain(v) => v,
            Self::Aligned { buf, len } => &buf.as_slice()[..*len],
        }
    }
}

/// The protocol log file a write goes to.
#[derive(Clone)]
pub(super) struct LogFile {
    /// Buffered: reads, preparation, and the fallback.
    pub file: Arc<File>,
    /// Opened with `O_DIRECT`, while the filesystem accepts it.
    pub direct: Option<Arc<File>>,
    pub method: FlushMethod,
    /// Set for good once the filesystem refused an `O_DIRECT` write.
    pub refused: Arc<AtomicBool>,
    pub dir: PathBuf,
    /// Tests: the filesystem refuses every `O_DIRECT` write.
    #[cfg(test)]
    pub reject_direct: bool,
}

impl LogFile {
    /// A file written only buffered (hints).
    pub fn buffered(file: Arc<File>, method: FlushMethod) -> Self {
        Self {
            file,
            direct: None,
            method,
            refused: Arc::new(AtomicBool::new(false)),
            dir: PathBuf::new(),
            #[cfg(test)]
            reject_direct: false,
        }
    }

    /// Writes `bytes` at `offset`. Returns whether they are durable already
    /// (else the caller flushes [`Self::file`] with [`Self::method`]).
    pub fn write(&self, bytes: &[u8], offset: u64) -> io::Result<bool> {
        if let Some(d) = self.direct.as_ref()
            && !self.refused.load(Ordering::Acquire)
        {
            #[cfg(test)]
            let r = if self.reject_direct {
                Err(io::Error::from_raw_os_error(libc::EINVAL))
            } else if self.method == FlushMethod::Dsync {
                fusee::local::pwrite_dsync(d, bytes, offset).map(|()| true)
            } else {
                d.write_all_at(bytes, offset)
                    .and_then(|()| flush_file(d, self.method))
                    .map(|()| true)
            };
            #[cfg(not(test))]
            let r = if self.method == FlushMethod::Dsync {
                fusee::local::pwrite_dsync(d, bytes, offset).map(|()| true)
            } else {
                // O_DIRECT bypasses the page cache, not the device's cache.
                d.write_all_at(bytes, offset)
                    .and_then(|()| flush_file(d, self.method))
                    .map(|()| true)
            };
            match r {
                Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
                    // The write transferred nothing, or a prefix of the same
                    // bytes: writing them buffered is exact.
                    if !self.refused.swap(true, Ordering::AcqRel) {
                        fusee::local::note_direct_refused(&self.dir);
                    }
                }
                other => return other,
            }
        }
        self.file.write_all_at(bytes, offset)?;
        Ok(false)
    }

    /// Writes and makes durable, adding `delay` before the flush
    /// (`LocalOptions::emulated_flush`).
    fn write_durable(
        &self,
        bytes: &[u8],
        offset: u64,
        delay: std::time::Duration,
    ) -> io::Result<()> {
        let durable = self.write(bytes, offset)?;
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        if durable {
            return Ok(());
        }
        flush_file(&self.file, self.method)
    }
}

struct Job {
    /// 0: a hint write, outside the log's ordering.
    seq: u64,
    target: LogFile,
    offset: u64,
    bytes: WriteBuf,
    /// Added to the flush (`LocalOptions::emulated_flush`).
    delay: std::time::Duration,
}

#[derive(Default)]
struct Queue {
    jobs: VecDeque<Job>,
    done: Vec<(u64, io::Result<()>)>,
    stop: bool,
}

/// Helper threads that write and flush.
pub(super) struct LogIo {
    queue: Mutex<Queue>,
    work: Condvar,
    finished: Condvar,
    any_done: AtomicBool,
    /// Whether a finished write wakes the I/O thread. The I/O thread sets
    /// it before it parks and reads [`Self::has_done`] after: with both
    /// sides sequentially consistent, either the helper sees it set or the
    /// I/O thread sees the write done.
    wake_on_done: AtomicBool,
    wake: Arc<tokio::sync::Notify>,
}

impl LogIo {
    pub fn start(wake: Arc<tokio::sync::Notify>) -> io::Result<Arc<Self>> {
        let io = Arc::new(Self {
            queue: Mutex::new(Queue::default()),
            work: Condvar::new(),
            finished: Condvar::new(),
            any_done: AtomicBool::new(false),
            wake_on_done: AtomicBool::new(true),
            wake,
        });
        for i in 0..IN_FLIGHT {
            let io = io.clone();
            std::thread::Builder::new()
                .name(format!("fusee-replica-log-{i}"))
                .spawn(move || io.work())?;
        }
        Ok(io)
    }

    fn work(&self) {
        let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if q.stop {
                return;
            }
            let Some(job) = q.jobs.pop_front() else {
                q = self.work.wait(q).unwrap_or_else(|e| e.into_inner());
                continue;
            };
            drop(q);
            let result = job
                .target
                .write_durable(job.bytes.as_slice(), job.offset, job.delay);
            q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
            q.done.push((job.seq, result));
            self.any_done.store(true, Ordering::SeqCst);
            self.finished.notify_all();
            if job.seq != 0 && self.wake_on_done.load(Ordering::SeqCst) {
                self.wake.notify_one();
            }
        }
    }

    pub fn has_done(&self) -> bool {
        self.any_done.load(Ordering::SeqCst)
    }

    pub fn stop(&self) {
        self.queue.lock().unwrap_or_else(|e| e.into_inner()).stop = true;
        self.work.notify_all();
    }
}

struct Pending {
    seq: u64,
    last_op: Op,
    padded: bool,
    done: bool,
}

/// The I/O thread's side: placement and in-order completion.
pub(super) struct Writes {
    io: Arc<LogIo>,
    in_flight: VecDeque<Pending>,
    /// Frames placed after a packed write in flight, waiting for it.
    waiting: Vec<u8>,
    waiting_last: Op,
    next_seq: u64,
    delay: std::time::Duration,
    /// Frames arrived while a write was in flight: pad until the log idles.
    busy: bool,
}

/// Where the next write goes and how its bytes are padded.
pub(super) struct Placement {
    /// Where the bytes go: the frames' own position, or (aligned writes)
    /// the start of the block they begin in.
    pub offset: u64,
    pub bytes: WriteBuf,
    /// The bytes end on a block boundary: the next write may overlap this one.
    pub padded: bool,
}

impl Writes {
    pub fn new(io: Arc<LogIo>, delay: std::time::Duration) -> Self {
        Self {
            io,
            in_flight: VecDeque::new(),
            waiting: Vec::new(),
            waiting_last: 0,
            next_seq: 1,
            delay,
            busy: false,
        }
    }

    pub fn idle(&self) -> bool {
        self.in_flight.is_empty() && self.waiting.is_empty()
    }

    pub fn has_done(&self) -> bool {
        self.io.has_done()
    }

    /// Before the I/O thread parks: whether a write that finishes meanwhile
    /// must wake it. `quorum` says the primary's own durability would
    /// commit entries; frames waiting for a write in flight always need it.
    pub fn wake_when_done(&self, quorum: bool) {
        self.io
            .wake_on_done
            .store(quorum || !self.waiting.is_empty(), Ordering::SeqCst);
    }

    /// Writes `bytes` at the start of `file` and flushes it, in the
    /// background and in no particular order with log writes.
    pub fn hint(&mut self, file: Arc<File>, bytes: Vec<u8>, method: FlushMethod) {
        let mut q = self.io.queue.lock().unwrap_or_else(|e| e.into_inner());
        q.jobs.push_back(Job {
            seq: 0,
            target: LogFile::buffered(file, method),
            offset: 0,
            bytes: WriteBuf::Plain(bytes),
            delay: std::time::Duration::ZERO,
        });
        drop(q);
        self.io.work.notify_one();
    }

    fn can_start(&self) -> bool {
        self.in_flight.len() < IN_FLIGHT && self.in_flight.back().is_none_or(|w| w.padded)
    }

    /// Queues `frames` (ending with op `last_op`). `place` positions bytes
    /// in the log: given the bytes and whether they must be padded to a
    /// block of their own, it reserves room and returns the placement.
    pub fn submit(
        &mut self,
        frames: Vec<u8>,
        last_op: Op,
        target: &LogFile,
        place: &mut dyn FnMut(Vec<u8>, bool) -> io::Result<Placement>,
    ) -> io::Result<()> {
        if self.waiting.is_empty() {
            self.waiting = frames;
        } else {
            self.waiting.extend_from_slice(&frames);
        }
        self.waiting_last = last_op;
        if !self.in_flight.is_empty() {
            self.busy = true;
        }
        self.start(target, place)
    }

    fn start(
        &mut self,
        target: &LogFile,
        place: &mut dyn FnMut(Vec<u8>, bool) -> io::Result<Placement>,
    ) -> io::Result<()> {
        if self.waiting.is_empty() || !self.can_start() {
            return Ok(());
        }
        // Busy log: the next write may start before this one completes.
        let pad = self.busy || !self.in_flight.is_empty();
        let p = place(std::mem::take(&mut self.waiting), pad)?;
        let seq = self.next_seq;
        self.next_seq += 1;
        self.in_flight.push_back(Pending {
            seq,
            last_op: self.waiting_last,
            padded: p.padded,
            done: false,
        });
        let mut q = self.io.queue.lock().unwrap_or_else(|e| e.into_inner());
        q.jobs.push_back(Job {
            seq,
            target: target.clone(),
            offset: p.offset,
            bytes: p.bytes,
            delay: self.delay,
        });
        drop(q);
        self.io.work.notify_one();
        Ok(())
    }

    /// Takes finished writes; returns the op the log is now durable
    /// through, if that advanced. Starts a waiting write when a slot frees.
    pub fn poll(
        &mut self,
        target: &LogFile,
        place: &mut dyn FnMut(Vec<u8>, bool) -> io::Result<Placement>,
    ) -> io::Result<Option<Op>> {
        if !self.io.has_done() {
            return Ok(None);
        }
        let done = {
            let mut q = self.io.queue.lock().unwrap_or_else(|e| e.into_inner());
            self.io.any_done.store(false, Ordering::SeqCst);
            std::mem::take(&mut q.done)
        };
        self.complete(done)?;
        let durable = self.pop_done();
        self.start(target, place)?;
        Ok(durable)
    }

    fn complete(&mut self, done: Vec<(u64, io::Result<()>)>) -> io::Result<()> {
        for (seq, result) in done {
            // A failed write leaves the log's state on disk uncertain.
            result?;
            if let Some(p) = self.in_flight.iter_mut().find(|p| p.seq == seq) {
                p.done = true;
            }
        }
        Ok(())
    }

    fn pop_done(&mut self) -> Option<Op> {
        let mut durable = None;
        while self.in_flight.front().is_some_and(|p| p.done) {
            durable = self.in_flight.pop_front().map(|p| p.last_op);
        }
        if self.idle() {
            self.busy = false;
        }
        durable
    }

    /// Waits until every queued write is durable (a barrier before a view
    /// save, a log replacement or a superblock). Returns the op the log is
    /// durable through, if that advanced.
    pub fn drain(
        &mut self,
        target: &LogFile,
        place: &mut dyn FnMut(Vec<u8>, bool) -> io::Result<Placement>,
    ) -> io::Result<Option<Op>> {
        let mut durable = None;
        loop {
            self.start(target, place)?;
            if self.idle() {
                break;
            }
            let done = {
                let mut q = self.io.queue.lock().unwrap_or_else(|e| e.into_inner());
                while !q.done.iter().any(|(seq, _)| *seq != 0) {
                    q = self.io.finished.wait(q).unwrap_or_else(|e| e.into_inner());
                }
                self.io.any_done.store(false, Ordering::SeqCst);
                std::mem::take(&mut q.done)
            };
            self.complete(done)?;
            if let Some(op) = self.pop_done() {
                durable = Some(op);
            }
        }
        Ok(durable)
    }
}

impl Drop for Writes {
    fn drop(&mut self) {
        self.io.stop();
    }
}
