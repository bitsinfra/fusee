//! One thread per shard of a lease.
//!
//! The thread owns the shard's storage, handles that shard's requests in the
//! order they arrived, does all of its blocking file I/O (flushes, run-store
//! reads and writes, recovery reads, compaction) and writes the replies to
//! the socket itself. Shards therefore flush in parallel, a slow shard holds
//! up no other shard, and the connection task only reads, routes and keeps
//! the lease alive. Each shard's replies leave in the order its requests
//! were handled, which the worker's run-store offset check relies on.

use super::storage::WorkerDisk;
use super::{Server, SharedLog};
use crate::proto::{self, req, resp};
use crate::spin::{self, Signal};
use fusee::local::{LocalEnv, LocalOptions};
use fusee_core::wire::{Reader, put_bytes, put_u32, put_u64, put_varint};
use fusee_core::{Lsn, frame};
use std::collections::VecDeque;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// A shard stops taking requests off the socket for itself once this many
/// request bytes wait for its thread; the connection then stops reading.
const QUEUE_LIMIT: usize = 32 << 20;
/// The longest a shard thread spins for its next request.
const SPIN_CAP: Duration = Duration::from_micros(100);

pub(super) type TryWriteFn = dyn Fn(&[u8]) -> io::Result<usize> + Send + Sync;

/// Replies to the lease holder, from the connection task and every shard
/// thread. Frames are written whole and in the order they were sent; what
/// the socket does not take at once waits here for the connection task.
pub(super) struct Outbox {
    state: Mutex<Out>,
    write: Box<TryWriteFn>,
    /// Tells the connection task that output is waiting for the socket.
    pub(super) queued: tokio::sync::Notify,
}

struct Out {
    buf: Vec<u8>,
    /// When the waiting output last made progress.
    since: Instant,
    failed: Option<String>,
}

impl Outbox {
    pub(super) fn new(write: Box<TryWriteFn>) -> Self {
        Self {
            state: Mutex::new(Out {
                buf: Vec::new(),
                since: Instant::now(),
                failed: None,
            }),
            write,
            queued: tokio::sync::Notify::new(),
        }
    }

    pub(super) fn send(&self, frames: &[u8]) {
        if frames.is_empty() {
            return;
        }
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if s.failed.is_some() {
            return;
        }
        let mut rest = frames;
        if s.buf.is_empty() {
            match self.write_some(rest) {
                Ok(n) => rest = &rest[n..],
                Err(e) => {
                    s.failed = Some(e.to_string());
                    drop(s);
                    self.queued.notify_one();
                    return;
                }
            }
            if rest.is_empty() {
                return;
            }
            s.since = Instant::now();
        }
        s.buf.extend_from_slice(rest);
        drop(s);
        self.queued.notify_one();
    }

    fn write_some(&self, mut data: &[u8]) -> io::Result<usize> {
        let total = data.len();
        while !data.is_empty() {
            match (self.write)(data) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => data = &data[n..],
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(total - data.len())
    }

    pub(super) fn has_queued(&self) -> bool {
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        !s.buf.is_empty() || s.failed.is_some()
    }

    /// Writes what the socket takes now. Returns how long the output still
    /// waiting has made no progress, if any is waiting.
    pub(super) fn write_queued(&self) -> io::Result<Option<Duration>> {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = &s.failed {
            return Err(io::Error::other(e.clone()));
        }
        if s.buf.is_empty() {
            return Ok(None);
        }
        let n = match self.write_some(&s.buf) {
            Ok(n) => n,
            Err(e) => {
                s.failed = Some(e.to_string());
                return Err(e);
            }
        };
        if n > 0 {
            s.buf.drain(..n);
            s.since = Instant::now();
        }
        Ok((!s.buf.is_empty()).then(|| s.since.elapsed()))
    }
}

/// Requests waiting for one shard's thread.
pub(super) struct Inbox {
    jobs: Mutex<Jobs>,
    signal: Signal,
    bytes: AtomicUsize,
    /// Shared by all shards of a lease: a queue fell below its limit.
    drained: Arc<tokio::sync::Notify>,
    /// The shard thread stopped the shard: it accepts no more writes.
    stopped: AtomicBool,
}

#[derive(Default)]
struct Jobs {
    queue: VecDeque<(u8, Vec<u8>)>,
    closed: bool,
    /// The thread took jobs and has not finished them (or sent their
    /// replies) yet.
    busy: bool,
}

impl Inbox {
    fn new(drained: Arc<tokio::sync::Notify>) -> Self {
        Self {
            jobs: Mutex::new(Jobs::default()),
            signal: Signal::default(),
            bytes: AtomicUsize::new(0),
            drained,
            stopped: AtomicBool::new(false),
        }
    }

    /// Runs `f` if every request routed to this shard so far has been
    /// handled and answered and the shard accepts writes: a request `f`
    /// handles then keeps its place in the shard's order. Returns `None`
    /// (without running `f`) otherwise.
    pub(super) fn if_idle<T>(&self, f: impl FnOnce() -> T) -> Option<T> {
        let j = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        if j.busy || !j.queue.is_empty() || j.closed || self.stopped.load(Ordering::Acquire) {
            return None;
        }
        // Held across `f`: the thread cannot take a job meanwhile, and only
        // the connection task, which is running `f`, pushes them.
        Some(f())
    }

    pub(super) fn push(&self, kind: u8, body: Vec<u8>) {
        self.bytes.fetch_add(body.len(), Ordering::AcqRel);
        self.jobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .queue
            .push_back((kind, body));
        self.signal.raise();
    }

    /// Background I/O finished: the thread polls its storage.
    fn wake(&self) {
        self.signal.raise();
    }

    fn close(&self) {
        self.jobs.lock().unwrap_or_else(|e| e.into_inner()).closed = true;
        self.signal.raise();
    }

    /// The shard thread stopped the shard: it accepts no more writes.
    pub(super) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub(super) fn over_limit(&self) -> bool {
        self.bytes.load(Ordering::Acquire) > QUEUE_LIMIT
    }

    fn take(&self) -> (VecDeque<(u8, Vec<u8>)>, bool) {
        self.signal.take();
        let mut j = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        j.busy = !j.queue.is_empty();
        (std::mem::take(&mut j.queue), j.closed)
    }

    /// The jobs taken last are handled and their replies sent.
    fn done(&self) {
        self.jobs.lock().unwrap_or_else(|e| e.into_inner()).busy = false;
    }

    fn handled(&self, len: usize) {
        let before = self.bytes.fetch_sub(len, Ordering::AcqRel);
        if before > QUEUE_LIMIT && before - len <= QUEUE_LIMIT {
            self.drained.notify_one();
        }
    }
}

/// Where a shard thread gets its storage. With `log`, the shard's records
/// go to the server's shared log (log format 3) and its directory keeps
/// only the run store and checkpoints.
pub(super) enum Disk {
    Open {
        dir: PathBuf,
        options: LocalOptions,
        log: Option<Arc<SharedLog>>,
    },
    Ready(WorkerDisk),
}

/// One shard's view of the shared log: which shard, and what its two
/// newest durable checkpoints cover (the log keeps every record after the
/// older one).
struct LogShard {
    log: Arc<SharedLog>,
    /// The shard's directory (its checkpoint files).
    dir: PathBuf,
    /// Checkpoints sent to the disk and not yet reported, by LSN.
    pending: Vec<Lsn>,
    retained: Vec<Lsn>,
    /// Sections of an unfinished `RECOVER_LOG`, with the shard's last LSN.
    recovering: Option<(Vec<fusee::wal::RawSection>, Lsn)>,
}

impl LogShard {
    fn report(&self, shard: u32) -> io::Result<()> {
        match self.retained.iter().copied().min() {
            Some(min) => self.log.set_retain(shard, min),
            None => Ok(()),
        }
    }
}

/// What the two newest valid checkpoint files of a shard directory cover,
/// oldest first; `[0]` when it has none (the log keeps all its records).
pub(super) fn retained_checkpoints(dir: &std::path::Path) -> io::Result<Vec<Lsn>> {
    let mut files: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(d) => d
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "fck"))
            .collect(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e),
    };
    files.sort();
    let mut out = Vec::new();
    for path in files.iter().rev() {
        if out.len() == 2 {
            break;
        }
        if let Ok(cp) = fusee_core::store::Checkpoint::decode(&std::fs::read(path)?) {
            out.push(cp.lsn);
        }
    }
    out.reverse();
    if out.is_empty() {
        out.push(0);
    }
    Ok(out)
}

/// The shard threads of one lease.
#[derive(Default)]
pub(super) struct Shards {
    threads: Mutex<Vec<(Arc<Inbox>, oneshot::Receiver<()>)>>,
    /// Set when any of them ends, including by a panic.
    exited: Arc<AtomicBool>,
    /// Shards of the shared log granted to this lease: `(shard, epoch)`.
    granted: Mutex<Vec<(u32, u64)>>,
    /// The lease's shared memory, when its worker uses it.
    pub(super) shm: Mutex<Option<Arc<super::ring::ShmConn>>>,
}

/// Marks a shard thread's end and wakes the connection task.
struct Exit {
    exited: Arc<AtomicBool>,
    outbox: Arc<Outbox>,
}

impl Drop for Exit {
    fn drop(&mut self) {
        self.exited.store(true, Ordering::Release);
        self.outbox.queued.notify_one();
    }
}

impl Shards {
    /// Starts one shard's thread. Resolves once its storage is open, with
    /// the first segment's size, so the lease can be granted.
    pub(super) fn start(
        &self,
        server: Arc<Server>,
        shard: u32,
        epoch: u64,
        disk: Disk,
        outbox: Arc<Outbox>,
        drained: Arc<tokio::sync::Notify>,
    ) -> io::Result<(Arc<Inbox>, oneshot::Receiver<io::Result<u64>>)> {
        let inbox = Arc::new(Inbox::new(drained));
        let (opened, ready) = oneshot::channel();
        let (done, finished) = oneshot::channel();
        let thread_inbox = inbox.clone();
        let exit = Exit {
            exited: self.exited.clone(),
            outbox: outbox.clone(),
        };
        std::thread::Builder::new()
            .name(format!("fusee-server-shard-{shard}"))
            .spawn(move || {
                let _exit = exit;
                let mut log = None;
                let mut disk = match disk {
                    Disk::Ready(d) => d,
                    Disk::Open {
                        dir,
                        options,
                        log: shared,
                    } => {
                        log = shared.map(|log| LogShard {
                            log,
                            dir: dir.clone(),
                            pending: Vec::new(),
                            retained: Vec::new(),
                            recovering: None,
                        });
                        match LocalEnv::open(&dir, options) {
                            Ok(env) => WorkerDisk::Local(Box::new(env)),
                            Err(e) => {
                                let _ = opened.send(Err(e));
                                return;
                            }
                        }
                    }
                };
                let wake = thread_inbox.clone();
                disk.set_notify(Arc::new(move || wake.wake()));
                let _ = opened.send(Ok(disk.first_segment_bytes(u64::MAX)));
                let state = ShardState {
                    server,
                    shard,
                    epoch,
                    disk,
                    log,
                    inflight: None,
                    ckpt: None,
                    stopped: None,
                };
                state.run(&thread_inbox, &outbox);
                // The storage is closed by now; the next lease may open it.
                let _ = done.send(());
            })?;
        self.threads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((inbox.clone(), finished));
        Ok((inbox, ready))
    }

    pub(super) fn granted(&self, shard: u32, epoch: u64) {
        self.granted
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((shard, epoch));
    }

    pub(super) fn take_granted(&self) -> Vec<(u32, u64)> {
        std::mem::take(&mut *self.granted.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Whether a shard thread has ended. While the lease lasts, that is a
    /// failure: the shard's requests would go unanswered.
    pub(super) fn exited(&self) -> bool {
        self.exited.load(Ordering::Acquire)
    }

    /// Stops every shard thread and waits until each has closed its storage.
    /// Requests still queued are dropped unanswered.
    pub(super) async fn stop(&self) {
        let threads = std::mem::take(&mut *self.threads.lock().unwrap_or_else(|e| e.into_inner()));
        for (inbox, _) in &threads {
            inbox.close();
        }
        for (_, finished) in threads {
            let _ = finished.await;
        }
    }
}

struct ShardState {
    server: Arc<Server>,
    shard: u32,
    epoch: u64,
    disk: WorkerDisk,
    /// Log format 3: the shard's records go to the shared log.
    log: Option<LogShard>,
    /// The request whose flush runs in the background.
    inflight: Option<u64>,
    /// The request whose checkpoint runs in the background.
    ckpt: Option<u64>,
    /// Set once a run-store append failed: the shard accepts no further
    /// writes in this lease, since a later record could reference the
    /// missing frame.
    stopped: Option<String>,
}

impl ShardState {
    fn run(mut self, inbox: &Inbox, outbox: &Outbox) {
        let mut out = Vec::new();
        let mut gap = Duration::ZERO;
        loop {
            let (jobs, closed) = inbox.take();
            if closed {
                return;
            }
            for (kind, body) in jobs {
                // A shared-log write may publish the shard's position from
                // inside `handle`: every earlier reply leaves first, so the
                // worker checks each run-store append before it sees a
                // flush that follows it.
                if kind == req::APPEND && !out.is_empty() {
                    outbox.send(&out);
                    out.clear();
                }
                self.handle(kind, &body, &mut out);
                if self.stopped.is_some() {
                    inbox.stopped.store(true, Ordering::Release);
                }
                inbox.handled(body.len());
                // A flush acknowledgment is what the worker is waiting for.
                if kind == req::SUBMIT {
                    outbox.send(&out);
                    out.clear();
                }
            }
            self.poll(&mut out);
            outbox.send(&out);
            out.clear();
            inbox.done();
            // Background work notifies when it ends; poll as well while some
            // is outstanding, as storage may also fail without a word.
            let nap = if self.inflight.is_some() || self.ckpt.is_some() {
                Duration::from_millis(50)
            } else {
                Duration::from_secs(3600)
            };
            let idle = Instant::now();
            inbox.signal.wait(spin::budget(gap, SPIN_CAP), nap, None);
            gap = idle.elapsed();
        }
    }

    /// Reports background flushes and checkpoints that finished.
    fn poll(&mut self, out: &mut Vec<u8>) {
        if let Some(res) = self.disk.poll_flushed() {
            let id = self.inflight.take().unwrap_or(0);
            let mut b = Vec::new();
            put_u64(&mut b, id);
            put_u32(&mut b, self.shard);
            match res {
                Ok(lsn) => {
                    put_u64(&mut b, lsn);
                    frame::encode(resp::FLUSHED, &b, out);
                }
                Err(e) => {
                    put_bytes(&mut b, e.to_string().as_bytes());
                    frame::encode(resp::IO_FAILED, &b, out);
                }
            }
        }
        if let Some(mut res) = self.disk.poll_checkpoint() {
            let id = self.ckpt.take().unwrap_or(0);
            // Log format 3: the log keeps every record after the older of
            // the shard's two newest checkpoints.
            if let (Ok(lsn), Some(log)) = (&res, &mut self.log)
                && let Some(i) = log.pending.iter().position(|l| l == lsn)
            {
                log.pending.remove(i);
                log.retained.push(*lsn);
                if log.retained.len() > 2 {
                    log.retained.remove(0);
                }
                if let Err(e) = log.report(self.shard) {
                    res = Err(e);
                }
            }
            let mut b = Vec::new();
            put_u64(&mut b, id);
            put_u32(&mut b, self.shard);
            match res {
                Ok(lsn) => {
                    put_u64(&mut b, lsn);
                    frame::encode(resp::CHECKPOINTED, &b, out);
                }
                Err(e) => {
                    put_bytes(&mut b, e.to_string().as_bytes());
                    frame::encode(resp::IO_FAILED, &b, out);
                }
            }
        }
    }

    fn handle(&mut self, kind: u8, body: &[u8], out: &mut Vec<u8>) {
        let mut r = Reader::new(body);
        let Ok(h) = proto::read_head(&mut r) else {
            return;
        };
        let reply = |out: &mut Vec<u8>, res: io::Result<Vec<u8>>| {
            let mut b = Vec::new();
            put_u64(&mut b, h.id);
            match res {
                Ok(data) => {
                    b.extend_from_slice(&data);
                    frame::encode(resp::OK, &b, out);
                }
                Err(e) => {
                    put_bytes(&mut b, e.to_string().as_bytes());
                    frame::encode(resp::ERR, &b, out);
                }
            }
        };
        if h.epoch != self.epoch
            || h.shard != self.shard
            || h.epoch != self.server.shard_epoch(self.shard)
        {
            reply(
                out,
                Err(io::Error::other(
                    "lease epoch is stale: another worker holds the shards",
                )),
            );
            return;
        }
        let env = &mut self.disk;
        let bad = |e: fusee_core::wire::DecodeError| io::Error::other(e.to_string());
        // Asynchronous writes report failure as `IO_FAILED`, which stops the
        // worker's shard; any later acknowledgment for it becomes an error.
        let io_failed = |out: &mut Vec<u8>, message: &str| {
            let mut b = Vec::new();
            put_u64(&mut b, h.id);
            put_u32(&mut b, h.shard);
            put_bytes(&mut b, message.as_bytes());
            frame::encode(resp::IO_FAILED, &b, out);
        };
        if let Some(reason) = &self.stopped {
            match kind {
                req::SUBMIT
                | req::APPEND
                | req::STORE_APPEND
                | req::STORE_APPEND_AT
                | req::CHECKPOINT => {
                    io_failed(out, reason);
                    return;
                }
                req::COMPACT_STORE | req::SYNC_RECOVERED | req::DISCARD | req::OPEN_LOG => {
                    reply(out, Err(io::Error::other(reason.clone())));
                    return;
                }
                _ => {}
            }
        }
        if let Some(log) = &mut self.log {
            // Log format 3: the shard's records live in the shared log.
            match kind {
                req::APPEND => {
                    let res = proto::decode_section(&mut r)
                        .map_err(bad)
                        .and_then(|batch| log.log.append(self.shard, self.epoch, batch, true));
                    if let Err(e) = res {
                        let message = format!("shard {}: {e}", self.shard);
                        io_failed(out, &message);
                        self.stopped = Some(message);
                    }
                    return;
                }
                req::RECOVER_LOG => {
                    let res = (|| -> io::Result<Vec<u8>> {
                        let after = r.u64().map_err(bad)?;
                        let start = r.u32().map_err(bad)? as usize;
                        if start == 0 && log.recovering.is_none() {
                            log.recovering = Some(log.log.recover(self.shard, self.epoch, after)?);
                        }
                        let Some((sections, last)) = &log.recovering else {
                            return Err(io::Error::other("no log recovery in progress"));
                        };
                        let total = sections.len();
                        let mut end = start.min(total);
                        let mut bytes = 0;
                        while end < total && (end == start || bytes < proto::RECOVERY_PAGE) {
                            bytes += sections[end].2.len() + 16;
                            end += 1;
                        }
                        let mut b = Vec::with_capacity(bytes + 32);
                        put_u64(&mut b, *last);
                        put_u32(&mut b, total as u32);
                        put_u32(&mut b, (end - start) as u32);
                        proto::encode_sections(&mut b, &sections[start..end]);
                        if end == total {
                            log.recovering = None;
                        }
                        Ok(b)
                    })();
                    reply(out, res);
                    return;
                }
                req::SUBMIT => {
                    reply(
                        out,
                        Err(io::Error::other(
                            "this server keeps one shared log: send sections, not segment batches",
                        )),
                    );
                    return;
                }
                req::LOAD_CHECKPOINT => {
                    let res = (|| -> io::Result<Vec<u8>> {
                        let cp = env.load_checkpoint()?;
                        log.retained = retained_checkpoints(&log.dir)?;
                        if cp.is_none() {
                            log.retained = vec![0];
                        }
                        log.report(self.shard)?;
                        let mut b = Vec::new();
                        match cp {
                            Some(c) => {
                                b.push(1);
                                put_bytes(&mut b, &c.encode());
                            }
                            None => b.push(0),
                        }
                        Ok(b)
                    })();
                    reply(out, res);
                    return;
                }
                req::CHECKPOINT => {
                    let res = (|| -> io::Result<()> {
                        let cp = fusee_core::store::Checkpoint::decode(r.blob().map_err(bad)?)
                            .map_err(bad)?;
                        // Segments of the shared log are retired by what
                        // every shard's retained checkpoints cover.
                        log.pending.push(cp.lsn);
                        self.ckpt = Some(h.id);
                        env.checkpoint(cp, None);
                        Ok(())
                    })();
                    if let Err(e) = res {
                        reply(out, Err(e));
                    }
                    return;
                }
                req::SEGMENTS => {
                    reply(out, Ok(vec![0]));
                    return;
                }
                req::SYNC_RECOVERED | req::DISCARD | req::OPEN_LOG => {
                    reply(out, Ok(Vec::new()));
                    return;
                }
                req::READ_SEGMENT | req::SEGMENT_EXTENT | req::SEGMENT_RANGE => {
                    reply(
                        out,
                        Err(io::Error::other("a shared log has no per-shard segments")),
                    );
                    return;
                }
                _ => {}
            }
        }
        match kind {
            req::SUBMIT => match proto::decode_batch(&mut r) {
                Ok(batch) => {
                    // On a fast disk, flush on this thread: two handoffs
                    // fewer per step. On a slow one, keep this thread free
                    // for the shard's reads and use the flusher thread.
                    if env.supports_inline() && env.last_flush_us() <= 200 {
                        let mut b = Vec::new();
                        put_u64(&mut b, h.id);
                        put_u32(&mut b, h.shard);
                        match env.flush_inline(batch) {
                            Ok(lsn) => {
                                put_u64(&mut b, lsn);
                                frame::encode(resp::FLUSHED, &b, out);
                            }
                            Err(e) => {
                                put_bytes(&mut b, e.to_string().as_bytes());
                                frame::encode(resp::IO_FAILED, &b, out);
                            }
                        }
                    } else {
                        self.inflight = Some(h.id);
                        env.submit(batch);
                    }
                }
                Err(e) => reply(out, Err(bad(e))),
            },
            req::STORE_APPEND | req::STORE_APPEND_AT => {
                let mismatch = |expected: u64, actual: u64| {
                    io::Error::other(format!(
                        "run-store append offset mismatch on shard {}: the worker expected offset {expected}, the server's store is at {actual}; the shard accepts no more writes in this lease",
                        h.shard
                    ))
                };
                let res = (|| {
                    let expected = match kind {
                        req::STORE_APPEND_AT => Some(r.u64().map_err(bad)?),
                        _ => None,
                    };
                    let frame = r.blob().map_err(bad)?;
                    // Never write a frame where the worker's reference
                    // would not find it.
                    if let Some(expected) = expected
                        && env.store_len() != expected
                    {
                        return Err(mismatch(expected, env.store_len()));
                    }
                    let offset = if r.is_empty() {
                        env.store_append(frame)?
                    } else {
                        let run = r.u64().map_err(bad)?;
                        let lsn = r.u64().map_err(bad)?;
                        env.store_append_history(frame, run, lsn)?
                    };
                    match expected {
                        Some(expected) if expected != offset => Err(mismatch(expected, offset)),
                        _ => Ok(offset),
                    }
                })();
                match res {
                    // Verified appends report the offset written; the
                    // original kind reports only failures.
                    Ok(offset) if kind == req::STORE_APPEND_AT => {
                        let mut b = Vec::with_capacity(20);
                        put_u64(&mut b, h.id);
                        put_u32(&mut b, h.shard);
                        put_u64(&mut b, offset);
                        frame::encode(resp::STORE_APPENDED, &b, out);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        let message = e.to_string();
                        io_failed(out, &message);
                        self.stopped = Some(message);
                    }
                }
            }
            req::STORE_READ => {
                let res = (|| -> io::Result<Vec<u8>> {
                    let off = r.u64().map_err(bad)?;
                    let len = r.u32().map_err(bad)?;
                    let data = env.store_read(off, len)?;
                    let mut b = Vec::new();
                    put_bytes(&mut b, &data);
                    Ok(b)
                })();
                reply(out, res);
            }
            req::CHECKPOINT => {
                let res = (|| -> io::Result<()> {
                    let cp = fusee_core::store::Checkpoint::decode(r.blob().map_err(bad)?)
                        .map_err(bad)?;
                    let del = r.u64().map_err(bad)?;
                    self.ckpt = Some(h.id);
                    env.checkpoint(cp, (del > 0).then_some(del));
                    Ok(())
                })();
                if let Err(e) = res {
                    reply(out, Err(e));
                }
            }
            req::LOAD_CHECKPOINT => {
                let res = env.load_checkpoint().map(|cp| {
                    let mut b = Vec::new();
                    match cp {
                        Some(c) => {
                            b.push(1);
                            put_bytes(&mut b, &c.encode());
                        }
                        None => b.push(0),
                    }
                    b
                });
                reply(out, res);
            }
            req::LOAD_STORE => {
                let res = (|| -> io::Result<Vec<u8>> {
                    let generation = r.u64().map_err(bad)?;
                    let len = r.u64().map_err(bad)?;
                    let mut b = Vec::new();
                    if !r.is_empty() && r.u8().map_err(bad)? == 1 {
                        env.files().open_store_prefix(generation, len)?;
                        put_u64(&mut b, u64::MAX);
                        put_u64(&mut b, env.store_generation());
                        put_u64(&mut b, env.store_len());
                        b.push(7); // paged compaction, history archival and visibility index
                        return Ok(b);
                    }
                    let data = env.load_store(generation, len)?;
                    put_u64(&mut b, env.store_generation());
                    put_bytes(&mut b, &data);
                    Ok(b)
                })();
                reply(out, res);
            }
            req::COMPACT_STORE => {
                let res = (|| -> io::Result<Vec<u8>> {
                    let mut result = Vec::new();
                    match r.u8().map_err(bad)? {
                        0 => env.files().begin_store_rewrite()?,
                        1 if r.rest().len() <= 4096 * 20 => {
                            while !r.is_empty() {
                                let run = r.u64().map_err(bad)?;
                                let offset = r.u64().map_err(bad)?;
                                let len = r.u32().map_err(bad)?;
                                env.files().copy_store_frame(
                                    run,
                                    fusee_core::shard::BlobRef { offset, len },
                                )?;
                            }
                        }
                        2 => {
                            env.files().finish_store_rewrite()?;
                            put_u64(&mut result, env.store_generation());
                            put_u64(&mut result, env.store_len());
                        }
                        _ => return Err(io::Error::other("bad run-store compaction frame")),
                    }
                    Ok(result)
                })();
                reply(out, res);
            }
            req::VISIBILITY => {
                let res = (|| -> io::Result<Vec<u8>> {
                    let filter = serde_json::from_slice(r.bytes().map_err(bad)?)?;
                    let limit = r.u32().map_err(bad)?.min(4096) as usize;
                    serde_json::to_vec(&env.visibility_candidates(&filter, limit))
                        .map_err(io::Error::other)
                })();
                reply(out, res);
            }
            req::SEGMENTS => {
                let res = env.segments().map(|segs| {
                    let mut b = Vec::new();
                    put_varint(&mut b, segs.len() as u64);
                    for s in segs {
                        put_varint(&mut b, s);
                    }
                    b
                });
                reply(out, res);
            }
            req::SEGMENT_EXTENT => {
                let res = r
                    .u64()
                    .map_err(bad)
                    .and_then(|id| env.files().recovery_segment_extent(id))
                    .map(|(length, prefix)| {
                        let mut b = Vec::with_capacity(16);
                        put_u64(&mut b, length);
                        put_u64(&mut b, prefix);
                        b
                    });
                reply(out, res);
            }
            req::SEGMENT_RANGE => {
                let res = (|| -> io::Result<Vec<u8>> {
                    let id = r.u64().map_err(bad)?;
                    let offset = r.u64().map_err(bad)?;
                    let size = r.u32().map_err(bad)? as usize;
                    if size > proto::RECOVERY_PAGE {
                        return Err(io::Error::other("recovery page exceeds bound"));
                    }
                    env.files().read_recovery_range(id, offset, size)
                })();
                reply(out, res);
            }
            req::READ_SEGMENT => {
                let res = r
                    .u64()
                    .map_err(bad)
                    .and_then(|s| env.read_segment(s))
                    .map(|data| {
                        let mut b = Vec::new();
                        put_bytes(&mut b, &data);
                        b
                    });
                reply(out, res);
            }
            req::SYNC_RECOVERED => reply(out, env.sync_recovered().map(|()| Vec::new())),
            req::DISCARD => reply(
                out,
                r.u64()
                    .map_err(bad)
                    .and_then(|s| env.discard_segment(s))
                    .map(|()| Vec::new()),
            ),
            req::OPEN_LOG => reply(
                out,
                r.u64()
                    .map_err(bad)
                    .and_then(|s| env.open_log(s))
                    .map(|()| Vec::new()),
            ),
            _ => reply(out, Err(io::Error::other("unknown request"))),
        }
    }
}
