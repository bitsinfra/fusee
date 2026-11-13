//! The server: holds the log (one shared log for every shard, or a log per
//! shard in directories from before it), each shard's run store and
//! checkpoints on its own disk, and leases the shards to worker
//! connections. Each shard is leased on its own, with an epoch of its own:
//! a worker takes free shards up to the number it asked for (all, unless
//! it said otherwise) and waits as a standby for more. When a holder
//! disconnects or goes silent past its [`Liveness`] timeout, its shards go
//! to waiting workers with new epochs, which recover them from disk.

mod log;
mod ring;
mod shard;
mod storage;
use crate::invoke::dispatch::{Dispatch, IdleEntry, Wanted};
use crate::limits::{ServerLimits, op};
use crate::proto::{self, Head, req, resp};
use crate::replicated::{ReplicaNode, ReplicaStorage, WorkerSession};
use crate::security::{Acceptor, Purpose, Scope, Secured, Security, Token};
use fusee::limits::LimitSpec;
use fusee::local::LocalOptions;
use fusee_core::frame;
use fusee_core::wire::{Reader, put_bytes, put_u32, put_u64};
pub(crate) use ring::ring_head;
use shard::{Disk, Inbox, Outbox, Shards};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use storage::{Dedicated, WorkerDisk};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Notify;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub dir: PathBuf,
    pub shards: u32,
    pub local: LocalOptions,
    /// TLS and the token for worker and client connections. TLS applies to
    /// TCP listeners; a Unix socket checks the token only.
    pub security: Security,
}

/// When the server revokes the lease of a worker it no longer hears from.
///
/// A worker that offers [`proto::LEASE_LIVENESS`] sends `PING` every
/// `ping_interval` while it holds the lease. The server revokes the lease
/// once it has spent `timeout` waiting to read from the worker without
/// receiving a byte, or once a write to the worker has made no progress for
/// `timeout`. Disk work runs on shard threads, never on the reader, and time
/// the reader does not read (a stalled runtime, a shard's full queue) does
/// not count: the worker's pings wait in the socket meanwhile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Liveness {
    /// How often the lease holder pings. Default one second.
    pub ping_interval: Duration,
    /// Silence after which the lease is revoked; longer than the interval.
    /// Default five seconds.
    pub timeout: Duration,
}

impl Default for Liveness {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(1),
            timeout: Duration::from_secs(5),
        }
    }
}

impl Liveness {
    fn millis(d: Duration) -> io::Result<u32> {
        u32::try_from(d.as_millis())
            .ok()
            .filter(|ms| *ms > 0)
            .ok_or_else(|| io::Error::other("liveness durations must be 1 ms to u32::MAX ms"))
    }

    fn validate(&self) -> io::Result<(u32, u32)> {
        let interval = Self::millis(self.ping_interval)?;
        let timeout = Self::millis(self.timeout)?;
        if timeout <= interval {
            return Err(io::Error::other(
                "the lease timeout must be longer than the ping interval",
            ));
        }
        Ok((interval, timeout))
    }
}

/// The longest the connection loop sleeps before it checks the lease's
/// liveness and the shard threads again.
const TICK: Duration = Duration::from_millis(50);

/// What a worker connection asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Want {
    /// Every shard at once under one epoch: workers from before per-shard
    /// leases.
    All,
    /// Free shards one at a time, up to this many held at once.
    UpTo(u32),
}

/// Shards granted to a connection: `(shard, epoch)`.
type Grant = Vec<(u32, u64)>;

struct Waiter {
    conn: u64,
    want: Want,
    grants: UnboundedSender<Grant>,
}

struct Lease {
    /// Per shard: the connection holding it and its lease's epoch.
    shards: Vec<Option<(u64, u64)>>,
    /// Per shard: its invoked holder is leaving; calls and messages for it
    /// wait at the server for the next holder.
    draining: Vec<bool>,
    /// The last epoch granted; every grant takes a new one, so an epoch
    /// names one lease of one shard (or, for [`Want::All`], of all).
    epoch: u64,
    /// Connections that may take more shards, in arrival order.
    waiting: VecDeque<Waiter>,
}

impl Lease {
    /// Grants free shards to waiting connections, in order: a
    /// [`Want::All`] connection only when every shard is free.
    fn distribute(&mut self) {
        let mut i = 0;
        while i < self.waiting.len() {
            let free: Vec<u32> = (0..self.shards.len() as u32)
                .filter(|&s| self.shards[s as usize].is_none())
                .collect();
            if free.is_empty() {
                return;
            }
            let (conn, want) = (self.waiting[i].conn, self.waiting[i].want);
            let mut grant = Grant::new();
            match want {
                Want::All if free.len() == self.shards.len() => {
                    self.epoch += 1;
                    grant = free.iter().map(|&s| (s, self.epoch)).collect();
                }
                Want::All => {}
                Want::UpTo(max) => {
                    let held = self
                        .shards
                        .iter()
                        .filter(|h| h.is_some_and(|(c, _)| c == conn))
                        .count() as u32;
                    for &s in free.iter().take(max.saturating_sub(held) as usize) {
                        self.epoch += 1;
                        grant.push((s, self.epoch));
                    }
                }
            }
            if grant.is_empty() {
                i += 1;
                continue;
            }
            if self.waiting[i].grants.send(grant.clone()).is_err() {
                self.waiting.remove(i);
                continue;
            }
            for (s, e) in grant {
                self.shards[s as usize] = Some((conn, e));
            }
            if want == Want::All {
                self.waiting.remove(i);
            } else {
                i += 1;
            }
        }
    }
}

struct Relay {
    holder: u64,
    client: u64,
    request: u64,
    reply: UnboundedSender<Vec<u8>>,
    /// A listing sent to every holder: their answers are merged.
    gather: Option<Arc<Mutex<Gather>>>,
    /// A call that only waits (for a result or stream chunks), with its
    /// shard and bytes: when an invoked holder leaves without answering,
    /// it waits at the server for the shard's next holder.
    waits: Option<(u32, Vec<u8>)>,
}

/// A client call waiting at the server until a worker holds its shard.
struct Parked {
    client: u64,
    request: u64,
    reply: UnboundedSender<Vec<u8>>,
    /// The call as a worker gets it: `method, JSON arguments`.
    call: Vec<u8>,
    gather: Option<Arc<Mutex<Gather>>>,
    /// Makes its shard due (a call that waited at a holder that left
    /// does not).
    triggers: bool,
    /// The workflow type of a start.
    start: Option<String>,
    /// Answered with an error once this passes.
    until: Option<Instant>,
}

/// How long a call waits at the server for a holder of its shard.
const PARK_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a stream read whose holder left waits for the next one (a
/// client gives up on a call after two minutes).
const PARKED_READ_TIMEOUT: Duration = Duration::from_secs(120);
/// Calls waiting at the server, at most.
const PARKED_MAX: usize = 4096;

/// Whether a call only waits: for a run's result or for stream chunks.
/// A relay to a holder whose call queue is closed: the holder is dying and
/// never received the call, so it may go to the next holder.
const WORKER_GONE: &str = "the worker went away";

fn call_waits(call: &[u8]) -> Option<&str> {
    let method = Reader::new(call).str().ok()?;
    matches!(method, "result" | "streamRead").then_some(method)
}

/// A client's listing, answered once every holder it went to answered.
struct Gather {
    /// Holders yet to answer.
    left: usize,
    /// What they listed so far.
    runs: Vec<serde_json::Value>,
    done: bool,
}

impl Gather {
    /// Takes one holder's answer: the frame for the client once every
    /// holder answered or one failed.
    fn answer(&mut self, request: u64, ok: bool, data: &[u8]) -> Option<Vec<u8>> {
        if self.done {
            return None;
        }
        if !ok {
            self.done = true;
            return Some(err_frame(request, &String::from_utf8_lossy(data)));
        }
        let v: serde_json::Value = serde_json::from_slice(data).unwrap_or_default();
        if v.get("error").is_some() {
            self.done = true;
            return Some(ok_frame(request, data));
        }
        if let Some(serde_json::Value::Array(runs)) = v.get("value") {
            self.runs.extend(runs.iter().cloned());
        }
        self.left = self.left.saturating_sub(1);
        if self.left > 0 {
            return None;
        }
        self.done = true;
        // In shard order, as a worker holding every shard lists them.
        let mut runs = std::mem::take(&mut self.runs);
        runs.sort_by_key(|r| r.get("shard").and_then(serde_json::Value::as_u64));
        let body = serde_json::json!({ "value": runs }).to_string();
        Some(ok_frame(request, body.as_bytes()))
    }
}

/// Relayed messages for a shard without a relaying holder are kept, up to
/// this many frames or bytes per shard (the oldest go first; senders
/// resend what stays unacknowledged).
const HELD_FRAMES: usize = 1024;
const HELD_BYTES: usize = 8 << 20;

/// Where messages between workflows on different workers' shards go.
#[derive(Default)]
struct Routes {
    /// The output of each connection that relays messages, once its lease
    /// frame is out.
    outboxes: HashMap<u64, Arc<Outbox>>,
    /// `MESSAGES` frames for shards whose holder cannot take them yet.
    held: HashMap<u32, (VecDeque<Vec<u8>>, usize)>,
}

/// The server's shared log (log format 3) over the files of `<dir>/wal`.
pub(crate) type SharedLog = log::LogService<Arc<fusee::wal::FileDevice>>;

enum Storage {
    Local {
        cfg: ServerConfig,
        _writer: std::fs::File,
        /// Log format 3: one log for every shard. `None`: a directory
        /// created before it, with a log per shard (format 2).
        log: Option<Arc<SharedLog>>,
    },
    Replicated(Dedicated),
}

/// Durable positions for the connection holding a lease, written to it as
/// `DURABLE` frames by whichever thread completed the write.
struct ConnSink {
    outbox: Arc<Outbox>,
}

impl log::Sink for ConnSink {
    fn durable(&self, marks: &[log::Mark], position: u64, segment: u64) {
        self.outbox.send(&durable_frame(marks, position, segment));
    }

    fn failed(&self, shards: &[u32], message: &str) {
        let mut out = Vec::new();
        for &shard in shards {
            let mut b = Vec::new();
            put_u64(&mut b, 0);
            put_u32(&mut b, shard);
            put_bytes(&mut b, message.as_bytes());
            frame::encode(resp::IO_FAILED, &b, &mut out);
        }
        self.outbox.send(&out);
    }
}

fn durable_frame(marks: &[log::Mark], position: u64, segment: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(28 + marks.len() * 13);
    put_u64(&mut b, 0);
    put_u64(&mut b, position);
    put_u64(&mut b, segment);
    put_u32(&mut b, marks.len() as u32);
    for m in marks {
        put_u32(&mut b, m.shard);
        put_u64(&mut b, m.lsn);
        b.push(if m.pins { proto::DURABLE_PINS } else { 0 });
    }
    let mut f = Vec::with_capacity(b.len() + frame::HEADER);
    frame::encode(resp::DURABLE, &b, &mut f);
    f
}

/// Opens the shared log of a log-format-3 server directory, or `None` for
/// a format-2 one. A directory whose meta file says format 3 but which
/// holds per-shard segments and no shared log was written by a build that
/// labelled server directories wrongly: it is format 2, and its meta file
/// is corrected.
fn open_log(cfg: &ServerConfig) -> io::Result<Option<Arc<SharedLog>>> {
    let dir = &cfg.dir;
    let format = fusee::engine::read_meta(dir)
        .map_err(io::Error::other)?
        .map_or(3, |m| m.1);
    if format != 3 {
        return Ok(None);
    }
    if !dir.join("wal").exists() && has_segments(dir, cfg.shards)? {
        eprintln!(
            "Fusee server: {} holds a log per shard (log format 2) but its meta file says \
             format 3; correcting it. Run `fusee upgrade {}` with the server stopped to \
             convert it to the shared log.",
            dir.display(),
            dir.display()
        );
        fusee::engine::write_meta(dir, cfg.shards, 2).map_err(io::Error::other)?;
        return Ok(None);
    }
    let opts = fusee::wal::WalOptions {
        segment_bytes: cfg.local.segment_bytes,
        first_segment_bytes: cfg.local.segment_bytes.min(1 << 20),
        ..Default::default()
    };
    let dev = Arc::new(fusee::wal::FileDevice::open(
        &dir.join("wal"),
        cfg.local.clone(),
    )?);
    let log = log::LogService::open(dev, cfg.shards, opts)?;
    // Until a shard's lease reports its checkpoints, what is on disk says
    // which of its records the log must keep.
    for shard in 0..cfg.shards {
        let retained = shard::retained_checkpoints(&dir.join(format!("shard-{shard:03}")))?;
        if let Some(min) = retained.iter().copied().min() {
            log.set_retain(shard, min)?;
        }
    }
    Ok(Some(log))
}

/// Whether any shard directory holds log-format-2 segment files.
fn has_segments(dir: &std::path::Path, shards: u32) -> io::Result<bool> {
    for shard in 0..shards {
        let log = dir.join(format!("shard-{shard:03}")).join("log");
        let entries = match std::fs::read_dir(&log) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for e in entries {
            if e?.path().extension().is_some_and(|x| x == "seg") {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub struct Server {
    storage: Storage,
    lease: Mutex<Lease>,
    next_conn: Mutex<u64>,
    /// Step limits; permits are leases.
    pub limits: ServerLimits,
    limit_epoch_ms: u64,
    limit_clock: std::time::Instant,
    /// Frames to each worker holding shards, by connection.
    workers: Mutex<HashMap<u64, tokio::sync::mpsc::Sender<Vec<u8>>>>,
    /// Client calls relayed to the holder: relay ID → (client, call ID).
    relays: Mutex<HashMap<u64, Relay>>,
    next_relay: AtomicU64,
    /// Messages between workflows on shards of different workers.
    routes: Mutex<Routes>,
    /// Applied to leases granted from now on; `None` disables revocation.
    liveness: Mutex<Option<Liveness>>,
    /// Applied to connections accepted from now on.
    acceptor: Mutex<Arc<Acceptor>>,
    /// The log thread of same-host workers using shared memory, once one
    /// connected.
    hub: Mutex<Option<Arc<ring::Hub>>>,
    /// Invokes workers over HTTP for shards with work and no holder.
    invoke: std::sync::OnceLock<Arc<Dispatch>>,
    /// Client calls waiting for a holder of their shard, by shard.
    parked: Mutex<HashMap<u32, VecDeque<Parked>>>,
    /// Ends a worker connection's lease from outside (a finished invocation).
    revokes: Mutex<HashMap<u64, Arc<Notify>>>,
}

impl Server {
    /// Opens (or creates) a server directory. New directories keep one
    /// log for every shard (log format 3); directories created before it
    /// keep a log per shard (format 2) and open as they are.
    pub fn new(cfg: ServerConfig) -> io::Result<Arc<Self>> {
        Self::open(cfg, true)
    }

    /// Like [`new`](Self::new); `shared_log: false` creates a new
    /// directory with a log per shard (log format 2), as servers before the
    /// shared log did. Existing directories keep their format either way.
    pub fn open(cfg: ServerConfig, shared_log: bool) -> io::Result<Arc<Self>> {
        let config = fusee::Config::local(&cfg.dir)
            .shards(cfg.shards)
            .shared_log(shared_log);
        let (_, writer) = fusee::engine::open_directory(&config).map_err(io::Error::other)?;
        let log = open_log(&cfg)?;
        let limits = ServerLimits::open(&cfg.dir, cfg.local.flush)?;
        let acceptor = Acceptor::new(&cfg.security, Purpose::Server)?;
        let storage = Storage::Local {
            cfg,
            _writer: writer.ok_or_else(|| io::Error::other("missing storage directory lock"))?,
            log,
        };
        let server = Self::with_storage(storage, limits);
        *server.acceptor.lock().unwrap_or_else(|e| e.into_inner()) = Arc::new(acceptor);
        Ok(server)
    }

    /// Serve a single-shard replication group. Only its ready primary serves
    /// connections. A replacement worker claims a fresh quorum-ordered session.
    /// Workers connect without TLS or a token until [`set_security`](Self::set_security).
    pub fn replicated(node: &ReplicaNode) -> Arc<Self> {
        Self::with_storage(
            Storage::Replicated(Dedicated::new(ReplicaStorage::new(node))),
            ServerLimits::default(),
        )
    }

    fn with_storage(storage: Storage, limits: ServerLimits) -> Arc<Self> {
        let shards = match &storage {
            Storage::Local { cfg, .. } => cfg.shards,
            Storage::Replicated(_) => 1,
        };
        Arc::new(Self {
            storage,
            lease: Mutex::new(Lease {
                shards: vec![None; shards as usize],
                draining: vec![false; shards as usize],
                epoch: 0,
                waiting: VecDeque::new(),
            }),
            next_conn: Mutex::new(1),
            limits,
            limit_epoch_ms: fusee::local::unix_ms(),
            limit_clock: std::time::Instant::now(),
            workers: Mutex::new(HashMap::new()),
            relays: Mutex::new(HashMap::new()),
            next_relay: AtomicU64::new(1),
            routes: Mutex::new(Routes::default()),
            liveness: Mutex::new(Some(Liveness::default())),
            acceptor: Mutex::new(Arc::new(Acceptor::open(Purpose::Server))),
            hub: Mutex::new(None),
            invoke: std::sync::OnceLock::new(),
            parked: Mutex::new(HashMap::new()),
            revokes: Mutex::new(HashMap::new()),
        })
    }

    /// Sets TLS and the token for worker and client connections accepted
    /// after this call.
    pub fn set_security(&self, security: &Security) -> io::Result<()> {
        self.set_security_scoped(security, Vec::new())
    }

    /// [`set_security`](Self::set_security) with tokens of less reach than
    /// the worker's [`Security::token`]: a [`Scope::Client`] token makes
    /// calls and holds no shard, a [`Scope::Read`] token makes calls that
    /// only read. Connections made after the call use the new settings
    /// (call it again to reload them, on `SIGHUP`); open ones keep theirs.
    pub fn set_security_scoped(
        &self,
        security: &Security,
        scoped: Vec<(Token, Scope)>,
    ) -> io::Result<()> {
        let acceptor = Arc::new(Acceptor::new(security, Purpose::Server)?.with_scoped(scoped)?);
        *self.acceptor.lock().unwrap_or_else(|e| e.into_inner()) = acceptor;
        Ok(())
    }

    fn acceptor(&self) -> Arc<Acceptor> {
        self.acceptor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Sets the lease liveness rule for leases granted after this call.
    /// `None` keeps a silent worker's lease until its connection closes.
    pub fn set_liveness(&self, liveness: Option<Liveness>) -> io::Result<()> {
        if let Some(l) = &liveness {
            l.validate()?;
        }
        *self.liveness.lock().unwrap_or_else(|e| e.into_inner()) = liveness;
        Ok(())
    }

    pub fn liveness(&self) -> Option<Liveness> {
        *self.liveness.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn shards(&self) -> u32 {
        match &self.storage {
            Storage::Local { cfg, .. } => cfg.shards,
            Storage::Replicated(_) => 1,
        }
    }

    fn log(&self) -> Option<&Arc<SharedLog>> {
        match &self.storage {
            Storage::Local { log, .. } => log.as_ref(),
            Storage::Replicated(_) => None,
        }
    }

    /// Whether this server keeps one log for every shard (log format 3).
    pub fn shared_log(&self) -> bool {
        self.log().is_some()
    }

    /// The shared-memory log thread, started on first use.
    fn hub(&self) -> io::Result<Arc<ring::Hub>> {
        let log = self
            .log()
            .ok_or_else(|| io::Error::other("shared memory needs the shared log"))?;
        let mut hub = self.hub.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(h) = hub.as_ref() {
            return Ok(h.clone());
        }
        let h = ring::Hub::start(log.clone())?;
        *hub = Some(h.clone());
        Ok(h)
    }

    /// Shared memory for a same-host worker on a Unix socket: its mapping,
    /// and a watch on its process. `None` where it cannot be had; the
    /// worker then uses the socket alone.
    fn shared_memory(
        &self,
        conn: u64,
        fd: std::os::fd::RawFd,
        outbox: &Arc<Outbox>,
    ) -> Option<(Arc<ring::ShmConn>, crate::shm::ProcWatch, Arc<ring::Hub>)> {
        let attempt = || -> io::Result<_> {
            let pid = crate::shm::peer_pid(fd)?;
            let layout = crate::shm::Layout::new(self.shards(), crate::shm::RING_BYTES)?;
            let shared = crate::shm::Shared::create(layout, pid)?;
            let watch = crate::shm::ProcWatch::new(pid)?;
            let hub = self.hub()?;
            let c = Arc::new(ring::ShmConn::new(
                conn,
                shared,
                outbox.clone(),
                self.shards(),
            ));
            Ok((c, watch, hub))
        };
        match attempt() {
            Ok(v) => Some(v),
            Err(e) => {
                eprintln!(
                    "Fusee server: shared memory unavailable for a worker ({e}); it uses the socket"
                );
                None
            }
        }
    }

    fn lease(&self) -> std::sync::MutexGuard<'_, Lease> {
        self.lease.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Queues `conn` for shards; grants arrive on the returned channel.
    fn acquire(&self, conn: u64, want: Want) -> tokio::sync::mpsc::UnboundedReceiver<Grant> {
        let (tx, rx) = unbounded_channel();
        let mut l = self.lease();
        l.waiting.push_back(Waiter {
            conn,
            want,
            grants: tx,
        });
        l.distribute();
        rx
    }

    /// `conn` went away: its shards go to the connections waiting.
    fn release(&self, conn: u64) {
        let mut l = self.lease();
        l.waiting.retain(|w| w.conn != conn);
        let l = &mut *l;
        for (h, draining) in l.shards.iter_mut().zip(l.draining.iter_mut()) {
            if h.is_some_and(|(c, _)| c == conn) {
                *h = None;
                *draining = false;
            }
        }
        l.distribute();
    }

    /// An invoked worker takes those of `shards` that are free now, each
    /// under a new epoch; it never waits for the rest.
    fn acquire_only(&self, conn: u64, shards: &[u32]) -> Grant {
        let mut l = self.lease();
        let mut grant = Grant::new();
        for &s in shards {
            if l.shards.get(s as usize).is_some_and(Option::is_none) {
                l.epoch += 1;
                let e = l.epoch;
                l.shards[s as usize] = Some((conn, e));
                grant.push((s, e));
            }
        }
        grant
    }

    /// The connection a call or message for `shard` goes to now: its
    /// holder, unless that holder is leaving.
    fn routable(&self, shard: u32) -> Option<u64> {
        let l = self.lease();
        match l.shards.get(shard as usize).copied().flatten() {
            Some((c, _)) if !l.draining[shard as usize] => Some(c),
            _ => None,
        }
    }

    /// Invokes workers at `endpoints` for shards that have work and no
    /// holder ([`crate::invoke`]). Every shard is due once now: what each
    /// has left is known only to a worker that held it. Call it once, on
    /// the runtime that serves connections.
    pub fn set_invokers(
        self: &Arc<Self>,
        endpoints: Vec<crate::invoke::Endpoint>,
    ) -> io::Result<()> {
        if !matches!(self.storage, Storage::Local { .. }) {
            return Err(io::Error::other(
                "invoked workers need a server whose shards are leased one at a time",
            ));
        }
        let d = Arc::new(Dispatch::new(endpoints, self.shards())?);
        self.invoke
            .set(d.clone())
            .map_err(|_| io::Error::other("the server's invokers are already set"))?;
        tokio::spawn(d.run(Arc::downgrade(self)));
        Ok(())
    }

    /// The invoker's counters, if it has endpoints.
    pub fn invocation_metrics(&self) -> Option<crate::invoke::InvokeMetrics> {
        let parked = self
            .parked
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(VecDeque::len)
            .sum();
        self.invoke.get().map(|d| d.metrics(parked))
    }

    /// Per shard: the connection holding it.
    pub(crate) fn holders(&self) -> Vec<Option<u64>> {
        self.lease()
            .shards
            .iter()
            .map(|h| h.map(|(c, _)| c))
            .collect()
    }

    /// Per shard: what waits at the server for a holder.
    pub(crate) fn wanted(&self) -> Vec<Wanted> {
        let mut out = vec![Wanted::default(); self.shards() as usize];
        for (s, q) in self.parked.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            let Some(w) = out.get_mut(*s as usize) else {
                continue;
            };
            for p in q.iter().filter(|p| p.triggers) {
                w.calls = true;
                if let Some(t) = &p.start
                    && !w.starts.contains(t)
                {
                    w.starts.push(t.clone());
                }
            }
        }
        for s in self.routes().held.keys() {
            if let Some(w) = out.get_mut(*s as usize) {
                w.messages = true;
            }
        }
        out
    }

    /// Whether `conn` still holds a shard.
    pub(crate) fn holds(&self, conn: u64) -> bool {
        self.holds_any(conn)
    }

    /// Ends `conn`'s lease: its connection closes as if it went silent.
    pub(crate) fn revoke(&self, conn: u64) {
        if let Some(n) = self
            .revokes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&conn)
        {
            n.notify_one();
        }
    }

    /// Keeps a call until a worker holds `shard`, and wakes the invoker.
    fn park(&self, shard: u32, p: Parked) {
        {
            let mut parked = self.parked.lock().unwrap_or_else(|e| e.into_inner());
            let total: usize = parked.values().map(VecDeque::len).sum();
            if total >= PARKED_MAX {
                drop(parked);
                Self::refuse_parked(p, "too many calls are waiting for workers");
                return;
            }
            parked.entry(shard).or_default().push_back(p);
        }
        // A holder that took the shard meanwhile gets it now.
        if let Some(holder) = self.routable(shard)
            && self
                .workers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(&holder)
        {
            self.deliver_parked(holder, &[shard]);
        }
        if let Some(d) = self.invoke.get() {
            d.wake.notify_one();
        }
    }

    fn refuse_parked(p: Parked, message: &str) {
        let f = match &p.gather {
            Some(g) => g.lock().unwrap_or_else(|e| e.into_inner()).answer(
                p.request,
                false,
                message.as_bytes(),
            ),
            None => Some(err_frame(p.request, message)),
        };
        if let Some(f) = f {
            let _ = p.reply.send(f);
        }
    }

    /// Calls waiting for `shards` go to `holder`, which holds them now.
    fn deliver_parked(&self, holder: u64, shards: &[u32]) {
        let mut taken = Vec::new();
        {
            let mut parked = self.parked.lock().unwrap_or_else(|e| e.into_inner());
            for s in shards {
                if let Some(q) = parked.remove(s) {
                    taken.extend(q.into_iter().map(|p| (*s, p)));
                }
            }
        }
        let mut requeue = Vec::new();
        for (shard, p) in taken {
            if let Err(e) = self.send_call(
                holder,
                shard,
                p.client,
                p.request,
                &p.reply,
                &p.call,
                p.gather.clone(),
            ) {
                if e == WORKER_GONE && self.invoke.get().is_some() {
                    // The holder is dying and never got the call: it waits
                    // for the next one (its disconnect hands the shard on).
                    requeue.push((shard, p));
                } else {
                    Self::refuse_parked(p, &e);
                }
            }
        }
        if !requeue.is_empty() {
            let mut shards: Vec<u32> = requeue.iter().map(|(s, _)| *s).collect();
            shards.dedup();
            let mut parked = self.parked.lock().unwrap_or_else(|e| e.into_inner());
            for (shard, p) in requeue {
                parked.entry(shard).or_default().push_back(p);
            }
            drop(parked);
            // A shard already handed on (the dying holder's disconnect ran
            // first) goes to its new holder now, not at the park timeout.
            for shard in shards {
                if let Some(next) = self.routable(shard).filter(|h| *h != holder)
                    && self
                        .workers
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .contains_key(&next)
                {
                    self.deliver_parked(next, &[shard]);
                }
            }
            if let Some(d) = self.invoke.get() {
                d.wake.notify_one();
            }
        }
    }

    /// Answers waiting calls whose time is up; returns when the next one's
    /// is (Unix ms).
    pub(crate) fn expire_parked(&self) -> Option<u64> {
        let now = Instant::now();
        let mut expired = Vec::new();
        let mut next: Option<Instant> = None;
        {
            let mut parked = self.parked.lock().unwrap_or_else(|e| e.into_inner());
            for (shard, q) in parked.iter_mut() {
                let mut keep = VecDeque::with_capacity(q.len());
                for p in q.drain(..) {
                    match p.until {
                        Some(t) if t <= now => expired.push((*shard, p)),
                        Some(t) => {
                            next = Some(next.map_or(t, |n| n.min(t)));
                            keep.push_back(p);
                        }
                        None => keep.push_back(p),
                    }
                }
                *q = keep;
            }
            parked.retain(|_, q| !q.is_empty());
        }
        for (shard, p) in expired {
            Self::refuse_parked(
                p,
                &format!(
                    "no worker took shard {shard} within {} s",
                    PARK_TIMEOUT.as_secs()
                ),
            );
        }
        next.map(|t| {
            fusee::local::unix_ms()
                .saturating_add(t.saturating_duration_since(now).as_millis() as u64)
        })
    }

    /// Relays a client's call to `holder`. `Err`: why it could not.
    fn send_call(
        &self,
        holder: u64,
        shard: u32,
        client: u64,
        request: u64,
        reply: &UnboundedSender<Vec<u8>>,
        call: &[u8],
        gather: Option<Arc<Mutex<Gather>>>,
    ) -> Result<(), String> {
        let workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
        let Some(tx) = workers.get(&holder) else {
            return Err(format!("no worker holds shard {shard} right now"));
        };
        let mut pending = self.relays.lock().unwrap_or_else(|e| e.into_inner());
        if pending.len() >= 4096 {
            return Err("too many pending worker calls".into());
        }
        let relay = self.next_relay.fetch_add(1, Ordering::Relaxed);
        let waits =
            (gather.is_none() && call_waits(call).is_some()).then(|| (shard, call.to_vec()));
        pending.insert(
            relay,
            Relay {
                holder,
                client,
                request,
                reply: reply.clone(),
                gather,
                waits,
            },
        );
        let mut b = Vec::with_capacity(call.len() + 8);
        put_u64(&mut b, relay);
        b.extend_from_slice(call);
        let mut f = Vec::new();
        frame::encode(resp::CALL, &b, &mut f);
        if let Err(e) = tx.try_send(f) {
            pending.remove(&relay);
            return Err(match e {
                tokio::sync::mpsc::error::TrySendError::Full(_) => "worker call queue is full",
                tokio::sync::mpsc::error::TrySendError::Closed(_) => WORKER_GONE,
            }
            .into());
        }
        Ok(())
    }

    /// The epoch of `shard`'s current lease (0 when it has none).
    fn shard_epoch(&self, shard: u32) -> u64 {
        self.lease()
            .shards
            .get(shard as usize)
            .copied()
            .flatten()
            .map_or(0, |(_, e)| e)
    }

    /// The connection holding `shard`, if any.
    fn holder_of(&self, shard: u32) -> Option<u64> {
        self.lease()
            .shards
            .get(shard as usize)
            .copied()
            .flatten()
            .map(|(c, _)| c)
    }

    fn holds_any(&self, conn: u64) -> bool {
        self.lease()
            .shards
            .iter()
            .any(|h| h.is_some_and(|(c, _)| c == conn))
    }

    fn routes(&self) -> std::sync::MutexGuard<'_, Routes> {
        self.routes.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Messages from `from` (lease `epoch`) to `target`: to the target's
    /// holder, or kept until a holder that relays messages takes it.
    fn forward_messages(&self, target: u32, from: u32, epoch: u64, entries: &[u8]) {
        if target >= self.shards() {
            return;
        }
        let mut b = Vec::with_capacity(entries.len() + 32);
        put_u64(&mut b, 0);
        put_u32(&mut b, target);
        put_u32(&mut b, from);
        put_u64(&mut b, epoch);
        put_bytes(&mut b, entries);
        let mut f = Vec::with_capacity(b.len() + frame::HEADER);
        frame::encode(resp::MESSAGES, &b, &mut f);
        // Under the routes lock, so a holder's first delivery of what was
        // kept cannot miss a frame kept meanwhile.
        let mut routes = self.routes();
        if let Some(out) = self.routable(target).and_then(|c| routes.outboxes.get(&c)) {
            out.send(&f);
            return;
        }
        let (held, bytes) = routes.held.entry(target).or_default();
        *bytes += f.len();
        held.push_back(f);
        while held.len() > HELD_FRAMES || *bytes > HELD_BYTES {
            let Some(old) = held.pop_front() else { break };
            *bytes -= old.len();
        }
        drop(routes);
        // A shard without a holder now has work.
        if let Some(d) = self.invoke.get() {
            d.wake.notify_one();
        }
    }

    /// Acknowledgments `(shard, epoch, outbox id)`: each to the holder of
    /// its shard under that epoch. One for an ended lease is dropped: its
    /// shard's next holder renumbered the outbox and resends.
    fn forward_acks(&self, acks: &[(u32, u64, u64)]) {
        let mut by_holder: HashMap<u64, Vec<(u32, u64, u64)>> = HashMap::new();
        {
            let l = self.lease();
            for &(shard, epoch, id) in acks {
                if let Some(Some((conn, e))) = l.shards.get(shard as usize)
                    && *e == epoch
                {
                    by_holder.entry(*conn).or_default().push((shard, epoch, id));
                }
            }
        }
        let routes = self.routes();
        for (conn, acks) in by_holder {
            let Some(out) = routes.outboxes.get(&conn) else {
                continue;
            };
            let mut b = Vec::with_capacity(12 + acks.len() * 20);
            put_u64(&mut b, 0);
            put_u32(&mut b, acks.len() as u32);
            for (shard, epoch, id) in acks {
                put_u32(&mut b, shard);
                put_u64(&mut b, epoch);
                put_u64(&mut b, id);
            }
            let mut f = Vec::new();
            frame::encode(resp::MESSAGE_ACKS, &b, &mut f);
            out.send(&f);
        }
    }

    /// `conn` relays messages from now on: messages kept for the shards it
    /// holds go to it, and later ones straight to `outbox`.
    fn relay_to(&self, conn: u64, outbox: &Arc<Outbox>) {
        let mut routes = self.routes();
        routes.outboxes.insert(conn, outbox.clone());
        let held: Vec<u32> = routes.held.keys().copied().collect();
        for shard in held {
            if self.holder_of(shard) == Some(conn)
                && let Some((frames, _)) = routes.held.remove(&shard)
            {
                for f in frames {
                    outbox.send(&f);
                }
            }
        }
    }

    /// Why this server takes no calls right now: a replica that is not the
    /// ready primary of its group (it closes every connection meanwhile).
    pub(crate) fn not_serving(&self) -> Option<String> {
        let Storage::Replicated(replica) = &self.storage else {
            return None;
        };
        let s = *replica.replica.status().borrow();
        (!s.ready).then(|| {
            let role = if s.stopped {
                "stopped after a storage or protocol error"
            } else if s.retired {
                "retired from its group"
            } else if s.primary {
                "the new primary, still catching up"
            } else {
                "a backup"
            };
            format!(
                "this replica is {role} (view {}): calls go to the primary of the group",
                s.view
            )
        })
    }

    /// Serves one connection until it closes.
    pub async fn serve_conn<S: Socket>(self: Arc<Self>, stream: S) {
        self.serve_conn_scoped(stream, Scope::Worker).await
    }

    /// Serves one connection whose token has `scope`: checked at its first
    /// frame (a worker's hello or a client's), never per frame after.
    pub async fn serve_conn_scoped<S: Socket>(self: Arc<Self>, stream: S, scope: Scope) {
        let conn = {
            let mut n = self.next_conn.lock().unwrap_or_else(|e| e.into_inner());
            *n += 1;
            *n
        };
        let (reader, writer) = stream.split();
        let threads = Arc::new(Shards::default());
        let revoke = Arc::new(Notify::new());
        self.revokes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(conn, revoke.clone());
        let work = self.clone().run_conn(
            conn,
            reader,
            Arc::new(writer),
            threads.clone(),
            revoke,
            scope,
        );
        match &self.storage {
            Storage::Local { .. } => {
                let _ = work.await;
            }
            Storage::Replicated(replica) => {
                let mut fence = replica.fence.subscribe();
                let mut status = replica.replica.status();
                let initial = *status.borrow_and_update();
                if initial.ready {
                    tokio::select! {
                        biased;
                        _ = status.wait_for(|s| !s.ready || s.view != initial.view) => {}
                        _ = fence.changed() => {}
                        _ = work => {}
                    }
                }
            }
        }
        // Its ring is read no further, and its waiters stop waiting.
        let shm = threads.shm.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(shm) = shm {
            if let Some(hub) = self.hub.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
                hub.remove(conn);
            }
            shm.close();
        }
        // The next holder opens the same storage: every shard thread of this
        // lease must have closed it first.
        threads.stop().await;
        if let Some(log) = self.log() {
            for (shard, epoch) in threads.take_granted() {
                log.release(shard, epoch);
            }
        }
        self.limits.disconnected(conn);
        self.revokes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&conn);
        let invoked = self.invoke.get().is_some_and(|d| d.is_invoked(conn));
        let mut wait_on = Vec::new();
        // Remove only this connection's calls before waking a standby. A new
        // holder's replies must never be drained by the departing worker.
        {
            self.workers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&conn);
            // Messages for its shards are kept for the next holder.
            self.routes().outboxes.remove(&conn);
            self.parked
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values_mut()
                .for_each(|q| q.retain(|p| p.client != conn));
            self.relays
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retain(|_, relay| {
                    if relay.holder == conn
                        && invoked
                        && let Some((shard, call)) = relay.waits.take()
                    {
                        // A wait outlives the invocation: it goes to the
                        // shard's next holder, without invoking one.
                        let read = call_waits(&call) == Some("streamRead");
                        wait_on.push((
                            shard,
                            Parked {
                                client: relay.client,
                                request: relay.request,
                                reply: relay.reply.clone(),
                                call,
                                gather: None,
                                triggers: false,
                                start: None,
                                until: read.then(|| Instant::now() + PARKED_READ_TIMEOUT),
                            },
                        ));
                        return false;
                    }
                    if relay.holder == conn {
                        let message = "the worker went away before answering";
                        let f = match &relay.gather {
                            Some(g) => g.lock().unwrap_or_else(|e| e.into_inner()).answer(
                                relay.request,
                                false,
                                message.as_bytes(),
                            ),
                            None => Some(err_frame(relay.request, message)),
                        };
                        if let Some(f) = f {
                            let _ = relay.reply.send(f);
                        }
                        false
                    } else {
                        relay.client != conn
                    }
                });
        }
        {
            let mut parked = self.parked.lock().unwrap_or_else(|e| e.into_inner());
            for (shard, p) in wait_on {
                parked.entry(shard).or_default().push_back(p);
            }
        }
        let held: Vec<(u32, u64)> = self
            .lease()
            .shards
            .iter()
            .enumerate()
            .filter_map(|(s, h)| h.filter(|(c, _)| *c == conn).map(|(_, e)| (s as u32, e)))
            .collect();
        self.release(conn);
        if let Some(d) = self.invoke.get()
            && (invoked || !held.is_empty())
        {
            d.released(conn, &held);
        }
    }

    /// A client connection: its calls are relayed to the lease holder.
    async fn run_client<R: AsyncRead + Unpin, W: TryWrite>(
        self: Arc<Self>,
        conn: u64,
        mut stream: R,
        writer: &W,
        mut dec: frame::Decoder,
        read_only: bool,
    ) -> io::Result<()> {
        let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
        let mut buf = vec![0u8; 1 << 16];
        loop {
            while let Some((kind, body)) = dec.next().map_err(io::Error::other)? {
                if kind != req::CALL {
                    continue;
                }
                let mut r = Reader::new(&body);
                let Ok(h) = proto::read_head(&mut r) else {
                    continue;
                };
                let method = Reader::new(r.rest()).str().ok();
                if read_only && !method.is_some_and(|m| crate::security::READ_CALLS.contains(&m)) {
                    let why = format!(
                        "this token only reads: '{}' needs a client's or a worker's token",
                        method.unwrap_or_default()
                    );
                    // An answer, so the caller sees Rejected, not a broken connection.
                    let v =
                        serde_json::json!({"error": why, "kind": "rejected", "code": "Rejected"});
                    let _ = tx.send(ok_frame(h.id, v.to_string().as_bytes()));
                    continue;
                }
                // The server answers this one itself.
                if method == Some("invocations") {
                    let v = serde_json::json!({ "value": self.invocation_metrics() });
                    let _ = tx.send(ok_frame(h.id, v.to_string().as_bytes()));
                    continue;
                }
                // An edit of an object's state is the call it is: a call of
                // the key's edit handler, which every worker answers.
                let edit;
                let call_body: &[u8] = if method == Some("editState") {
                    match edit_state_call(r.rest()) {
                        Ok(b) => {
                            edit = b;
                            &edit
                        }
                        Err(e) => {
                            let _ = tx.send(err_frame(h.id, &e));
                            continue;
                        }
                    }
                } else {
                    r.rest()
                };
                // The call goes to the worker holding the shard of the
                // workflow it names; a listing goes to every holder.
                let shard = match self.call_shard(call_body) {
                    Ok(Some(shard)) => shard,
                    Ok(None) => {
                        if let Err(e) = self.gather_list(conn, h.id, r.rest(), &tx) {
                            let _ = tx.send(err_frame(h.id, &e));
                        }
                        continue;
                    }
                    Err(e) => {
                        let _ = tx.send(err_frame(h.id, &e));
                        continue;
                    }
                };
                let call = call_body;
                // A holder that is still starting, or already leaving, has
                // no call queue: with invokers the call waits for the next.
                let invoking = self.invoke.get().is_some();
                let Some(holder) = self.routable(shard).filter(|h| {
                    !invoking
                        || self
                            .workers
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .contains_key(h)
                }) else {
                    if self.invoke.get().is_some() {
                        // An invoked worker takes the shard; the call waits.
                        self.park(
                            shard,
                            Parked {
                                client: conn,
                                request: h.id,
                                reply: tx.clone(),
                                call: call.to_vec(),
                                gather: None,
                                triggers: true,
                                start: start_type(call),
                                until: Some(Instant::now() + PARK_TIMEOUT),
                            },
                        );
                    } else {
                        let _ = tx.send(err_frame(
                            h.id,
                            &format!("no worker holds shard {shard} right now"),
                        ));
                    }
                    continue;
                };
                if let Err(e) = self.send_call(holder, shard, conn, h.id, &tx, call, None) {
                    if e == WORKER_GONE && invoking {
                        // The holder died before taking the call (its
                        // disconnect is not handled yet): the call waits for
                        // the shard's next holder, as when nobody holds it.
                        self.park(
                            shard,
                            Parked {
                                client: conn,
                                request: h.id,
                                reply: tx.clone(),
                                call: call.to_vec(),
                                gather: None,
                                triggers: true,
                                start: start_type(call),
                                until: Some(Instant::now() + PARK_TIMEOUT),
                            },
                        );
                    } else {
                        let _ = tx.send(err_frame(h.id, &e));
                    }
                }
            }
            tokio::select! {
                n = stream.read(&mut buf) => {
                    let n = n?;
                    if n == 0 {
                        return Ok(());
                    }
                    dec.feed(&buf[..n]);
                }
                Some(f) = rx.recv() => write_all(writer, &f).await?,
            }
        }
    }

    /// The shard a client call is for: its workflow's, or its completion
    /// token's; `None` for a listing, which goes to every holder.
    fn call_shard(&self, call: &[u8]) -> Result<Option<u32>, String> {
        let shards = self.shards();
        let mut r = Reader::new(call);
        let method = r.str().map_err(|e| e.to_string())?;
        if method == "list" {
            return Ok(None);
        }
        let args: serde_json::Value =
            serde_json::from_slice(r.bytes().map_err(|e| e.to_string())?).unwrap_or_default();
        let id = match method {
            "complete" | "fail" => args
                .get("token")
                .and_then(|t| t.as_str())
                .and_then(fusee::runtime::parse_token)
                .map(|t| t.0),
            _ => args.get("id").and_then(|i| i.as_str()).map(str::to_owned),
        };
        Ok(Some(
            id.map_or(0, |id| fusee_core::shard::shard_of(&id, shards)),
        ))
    }

    /// Sends a client's listing to every worker holding shards, each for
    /// the shards it holds, and answers once all have.
    fn gather_list(
        &self,
        client: u64,
        request: u64,
        call: &[u8],
        tx: &UnboundedSender<Vec<u8>>,
    ) -> Result<(), String> {
        let mut r = Reader::new(call);
        let method = r.str().map_err(|e| e.to_string())?.to_owned();
        let mut args: serde_json::Value =
            serde_json::from_slice(r.bytes().map_err(|e| e.to_string())?).unwrap_or_default();
        if !args.is_object() {
            args = serde_json::json!({});
        }
        // Each holder lists its own shards, in shard order. With invokers, a
        // shard without one waits for an invoked worker to list it.
        let mut holders: Vec<(u64, Vec<u32>)> = Vec::new();
        let mut unheld = Vec::new();
        {
            let l = self.lease();
            for (shard, h) in l.shards.iter().enumerate() {
                let conn = match h {
                    Some((conn, _)) if !l.draining[shard] => conn,
                    _ if self.invoke.get().is_some() => {
                        unheld.push(shard as u32);
                        continue;
                    }
                    _ => return Err(format!("no worker holds shard {shard} right now")),
                };
                match holders.iter_mut().find(|(c, _)| c == conn) {
                    Some((_, s)) => s.push(shard as u32),
                    None => holders.push((*conn, vec![shard as u32])),
                }
            }
        }
        let workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
        let mut pending = self.relays.lock().unwrap_or_else(|e| e.into_inner());
        if pending.len() + holders.len() > 4096 {
            return Err("too many pending worker calls".into());
        }
        let mut frames = Vec::new();
        for (conn, shards) in &holders {
            let Some(holder) = workers.get(conn) else {
                return Err(format!("no worker holds shard {} right now", shards[0]));
            };
            let mut a = args.clone();
            a["shards"] = serde_json::json!(shards);
            let mut b = Vec::new();
            let relay = self.next_relay.fetch_add(1, Ordering::Relaxed);
            put_u64(&mut b, relay);
            put_bytes(&mut b, method.as_bytes());
            put_bytes(&mut b, a.to_string().as_bytes());
            let mut f = Vec::new();
            frame::encode(resp::CALL, &b, &mut f);
            frames.push((relay, *conn, holder.clone(), f));
        }
        let gather = Arc::new(Mutex::new(Gather {
            left: frames.len() + unheld.len(),
            runs: Vec::new(),
            done: false,
        }));
        for (relay, holder, _, _) in &frames {
            pending.insert(
                *relay,
                Relay {
                    holder: *holder,
                    client,
                    request,
                    reply: tx.clone(),
                    gather: Some(gather.clone()),
                    waits: None,
                },
            );
        }
        let ids: Vec<u64> = frames.iter().map(|(relay, ..)| *relay).collect();
        for (_, _, holder, f) in frames {
            if holder.try_send(f).is_err() {
                // Answers to what was sent already find no call.
                gather.lock().unwrap_or_else(|e| e.into_inner()).done = true;
                for r in &ids {
                    pending.remove(r);
                }
                return Err("worker call queue is full".into());
            }
        }
        drop(pending);
        drop(workers);
        for shard in unheld {
            let mut a = args.clone();
            a["shards"] = serde_json::json!([shard]);
            let mut call = Vec::new();
            put_bytes(&mut call, method.as_bytes());
            put_bytes(&mut call, a.to_string().as_bytes());
            self.park(
                shard,
                Parked {
                    client,
                    request,
                    reply: tx.clone(),
                    call,
                    gather: Some(gather.clone()),
                    triggers: true,
                    start: None,
                    until: Some(Instant::now() + PARK_TIMEOUT),
                },
            );
        }
        Ok(())
    }

    async fn run_conn<R: AsyncRead + Unpin, W: TryWrite>(
        self: Arc<Self>,
        conn: u64,
        mut stream: R,
        writer: Arc<W>,
        threads: Arc<Shards>,
        revoke: Arc<Notify>,
        scope: Scope,
    ) -> io::Result<()> {
        let mut dec = frame::Decoder::new();
        let mut buf = vec![0u8; 1 << 16];
        // Wait for Hello: `id u64[, offered capabilities u32[, most shards
        // to hold u32[, invocation u64]]]`.
        let (hello_id, offered, most, invocation) = loop {
            if let Some((kind, body)) = dec.next().map_err(io::Error::other)? {
                if kind == req::QUORUM_PROBE && body.is_empty() {
                    let Storage::Replicated(replica) = &self.storage else {
                        return Err(io::Error::other("this server is not a replica"));
                    };
                    tokio::time::timeout(Duration::from_secs(2), replica.replica.confirm_quorum())
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::TimedOut, "replica quorum is unavailable")
                        })??;
                    let mut out = Vec::new();
                    frame::encode(resp::QUORUM_READY, &[], &mut out);
                    write_all(&*writer, &out).await?;
                    continue;
                }
                if kind == req::HELLO && scope != Scope::Worker {
                    let why = "this token is a client's: it makes calls and cannot hold shards \
                               (a worker needs the worker token)";
                    let _ = write_all(&*writer, &err_frame(0, why)).await;
                    return Err(io::Error::new(io::ErrorKind::PermissionDenied, why));
                }
                if kind == req::HELLO {
                    let mut r = Reader::new(&body);
                    let id = r.u64().map_err(io::Error::other)?;
                    let offered = if r.is_empty() {
                        0
                    } else {
                        r.u32().map_err(io::Error::other)?
                    };
                    let most = if r.is_empty() {
                        0
                    } else {
                        r.u32().map_err(io::Error::other)?
                    };
                    let invocation = if offered & proto::INVOKED != 0 {
                        Some(r.u64().map_err(io::Error::other)?)
                    } else {
                        None
                    };
                    break (id, offered, most, invocation);
                }
                if kind == req::CLIENT_HELLO {
                    let read_only = scope == Scope::Read;
                    return self
                        .run_client(conn, stream, &*writer, dec, read_only)
                        .await;
                }
                if kind == crate::security::AUTH_HELLO {
                    let refusal = err_frame(
                        0,
                        "this server does not use token authentication: drop token-file from the address",
                    );
                    let _ = write_all(&*writer, &refusal).await;
                }
                return Err(io::Error::other("expected hello"));
            }
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                return Ok(());
            }
            dec.feed(&buf[..n]);
        };
        if self.shared_log() && offered & proto::SHARED_LOG == 0 {
            let refusal = err_frame(
                hello_id,
                "this server keeps one log for every shard (log format 3); upgrade Fusee on the worker",
            );
            let _ = write_all(&*writer, &refusal).await;
            return Err(io::Error::other(
                "the worker does not support the shared log",
            ));
        }
        // Per-shard leases need storage whose shards are independent.
        let per_shard =
            offered & proto::SHARD_LEASES != 0 && matches!(self.storage, Storage::Local { .. });
        let want = if per_shard {
            Want::UpTo(if most == 0 { self.shards() } else { most })
        } else {
            Want::All
        };
        // An invoked worker takes its invocation's free shards, or none.
        let mut invoked = None;
        if let Some(id) = invocation {
            let bound = match self.invoke.get() {
                None => Err("this server does not invoke workers".to_owned()),
                Some(_) if !per_shard => Err("an invoked worker needs per-shard leases".to_owned()),
                Some(d) => d.bind(id, conn),
            };
            let refused = match bound {
                Ok((shards, left)) => {
                    let grant = self.acquire_only(conn, &shards);
                    if grant.is_empty() {
                        Err("the invocation's shards are held by other workers now".to_owned())
                    } else {
                        Ok((grant, left))
                    }
                }
                Err(e) => Err(e),
            };
            match refused {
                Ok(v) => invoked = Some(v),
                Err(why) => {
                    let _ = write_all(&*writer, &err_frame(hello_id, &why)).await;
                    return Err(io::Error::new(io::ErrorKind::PermissionDenied, why));
                }
            }
        }
        let (keep_grants, mut grants) = match &invoked {
            Some((grant, _)) => {
                let (tx, rx) = unbounded_channel();
                let _ = tx.send(grant.clone());
                (Some(tx), rx)
            }
            None => (None, self.acquire(conn, want)),
        };
        let _keep_grants = keep_grants;
        // Standby: wait for a first shard while watching for disconnects.
        let first = tokio::select! {
            g = grants.recv() => g.ok_or_else(|| io::Error::other("lease channel closed"))?,
            _ = async {
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            } => return Ok(()),
        };
        // The epoch the lease frame names, and connection-level requests
        // carry: this connection's first grant.
        let epoch = first[0].1;
        let outbox = {
            let w = writer.clone();
            Arc::new(Outbox::new(Box::new(move |b: &[u8]| w.try_write(b))))
        };
        let drained = Arc::new(Notify::new());
        // Same-host shared memory, offered on a Unix socket.
        let mut shm = None;
        if offered & proto::SHARED_MEMORY != 0
            && per_shard
            && self.shared_log()
            && crate::shm::SUPPORTED
            && let Some(fd) = writer.unix_fd()
        {
            shm = self.shared_memory(conn, fd, &outbox);
        }
        if let Some((c, _, _)) = &shm {
            *threads.shm.lock().unwrap_or_else(|e| e.into_inner()) = Some(c.clone());
        }
        let shm_conn = shm.as_ref().map(|(c, _, _)| c.clone());
        let mut session = None;
        let mut inboxes: Vec<Option<Arc<Inbox>>> = vec![None; self.shards() as usize];
        if let Storage::Replicated(replica) = &self.storage {
            let (worker, env) = replica.replica.claim().await?;
            let (inbox, opened) = threads.start(
                self.clone(),
                0,
                epoch,
                Disk::Ready(WorkerDisk::Replicated(Box::new(env))),
                outbox.clone(),
                drained.clone(),
            )?;
            opened
                .await
                .map_err(|_| io::Error::other("a shard thread exited while opening"))??;
            inboxes[0] = Some(inbox);
            session = Some(worker);
        }
        // The lease names the first shard's size.
        let mut initial = match &self.storage {
            Storage::Local { .. } => {
                self.open_shards(
                    &first,
                    &threads,
                    &outbox,
                    &drained,
                    &mut inboxes,
                    shm_conn.as_ref(),
                )
                .await?
            }
            Storage::Replicated(_) => u64::MAX,
        };
        let (limit_tx, mut limit_rx) = unbounded_channel();
        let (relay_tx, mut relay_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1024);
        self.workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(conn, relay_tx);
        let mut out = Vec::new();
        let mut body = Vec::new();
        put_u64(&mut body, hello_id);
        put_u32(&mut body, self.shards());
        put_u64(&mut body, epoch);
        let liveness = self
            .liveness()
            .filter(|_| offered & proto::LEASE_LIVENESS != 0)
            .and_then(|l| l.validate().ok().map(|ms| (l, ms)));
        let mut capabilities = proto::PAGED_RECOVERY | proto::STORE_OFFSETS;
        if self.shared_log() {
            capabilities |= proto::SHARED_LOG;
            initial = u64::MAX;
        } else if matches!(self.storage, Storage::Local { .. }) {
            capabilities |= proto::EXCLUSIVE_LOG;
        }
        if initial < u64::MAX {
            capabilities |= proto::FIRST_SEGMENT_LIMIT;
        }
        if liveness.is_some() {
            capabilities |= proto::LEASE_LIVENESS;
        }
        if per_shard {
            capabilities |= proto::SHARD_LEASES;
        }
        let relays = per_shard && offered & proto::MESSAGE_RELAY != 0;
        if relays {
            capabilities |= proto::MESSAGE_RELAY;
        }
        if shm.is_some() {
            capabilities |= proto::SHARED_MEMORY;
        }
        if invoked.is_some() {
            capabilities |= proto::INVOKED;
        }
        put_u32(&mut body, capabilities);
        if initial < u64::MAX {
            put_u64(&mut body, initial);
        }
        if let Some((_, (interval, timeout))) = liveness {
            put_u32(&mut body, interval);
            put_u32(&mut body, timeout);
        }
        if per_shard {
            put_grant(&mut body, &first);
        }
        if shm.is_some() {
            put_u64(&mut body, crate::shm::RING_BYTES as u64);
        }
        if let Some((_, left)) = &invoked {
            put_u32(&mut body, left.as_millis().min(u128::from(u32::MAX)) as u32);
        }
        let liveness = liveness.map(|(l, _)| l);
        frame::encode(resp::LEASE, &body, &mut out);
        // Nothing else is written before the lease: no request was routed yet.
        match (&shm, writer.unix_fd()) {
            // The mapping and the doorbell travel with the lease's first byte.
            (Some((c, _, hub)), Some(fd)) => {
                let fds = [c.shared.fd(), hub.bell.fd()];
                let sent = loop {
                    match crate::shm::send_with_fds(fd, &out, &fds) {
                        Ok(n) => break n,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            writer.writable().await?
                        }
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                        Err(e) => return Err(e),
                    }
                };
                write_all(&*writer, &out[sent..]).await?;
                hub.add(c.clone());
            }
            _ => write_all(&*writer, &out).await?,
        }
        out.clear();
        if relays {
            self.relay_to(conn, &outbox);
        }
        // Calls that waited for these shards follow the lease.
        if let Some(d) = self.invoke.get() {
            d.leased(conn, &first);
        }
        let firsts: Vec<u32> = first.iter().map(|(s, _)| *s).collect();
        self.deliver_parked(conn, &firsts);
        let stall = liveness.map(|l| l.timeout);
        // Time spent waiting to read without receiving anything. Time spent
        // routing requests, or not reading because a shard's queue is full,
        // is not silence.
        let mut silent = Duration::ZERO;
        // Shared memory: the worker's heartbeat replaces its pings, and idle
        // shards learn from their watermark lines what pings told them.
        let mut beat = shm_conn
            .as_ref()
            .map_or(0, |c| c.shared.heartbeat().load(Ordering::Acquire));
        let mut refreshed = Instant::now();
        let watch = shm.as_ref().map(|(_, w, _)| w);
        loop {
            while let Some((kind, body)) = dec.next().map_err(io::Error::other)? {
                self.route(
                    conn,
                    kind,
                    body,
                    epoch,
                    &inboxes,
                    session.as_ref(),
                    &limit_tx,
                    &mut out,
                );
                self.limits.healthy()?;
            }
            outbox.send(&out);
            out.clear();
            let full = inboxes.iter().flatten().any(|i| i.over_limit());
            let queued = outbox.has_queued();
            let waiting = Instant::now();
            let tick = stall.map_or(TICK, |t| TICK.min(t.saturating_sub(silent)));
            let mut heard = false;
            let mut granted = None;
            tokio::select! {
                // Reads first: pings that queued while this task was busy or
                // not scheduled must count before the silence timer can.
                biased;
                n = stream.read(&mut buf), if !full => {
                    let n = n?;
                    if n == 0 {
                        return Ok(());
                    }
                    dec.feed(&buf[..n]);
                    heard = true;
                }
                r = writer.writable(), if queued => r?,
                _ = outbox.queued.notified(), if !queued => {}
                _ = drained.notified(), if full => {}
                Some(f) = relay_rx.recv() => outbox.send(&f),
                Some(result) = limit_rx.recv() => outbox.send(&result?),
                Some(g) = grants.recv() => granted = Some(g),
                _ = revoke.notified() => {
                    eprintln!("Fusee server revoked the lease of epoch {epoch}: its invocation ended");
                    return Err(io::Error::other("the invocation ended"));
                }
                _ = async { watch.expect("checked").exited().await }, if watch.is_some() => {
                    eprintln!("Fusee server revoked the lease of epoch {epoch}: its worker process exited");
                    return Err(io::Error::other("the worker process exited"));
                }
                _ = async { shm_conn.as_ref().expect("checked").revoke.notified().await }, if shm_conn.is_some() => {
                    let why = shm_conn
                        .as_ref()
                        .and_then(|c| *c.corrupt.lock().unwrap_or_else(|e| e.into_inner()))
                        .unwrap_or("broken ring");
                    eprintln!("Fusee server revoked the lease of epoch {epoch}: its shared-memory ring is corrupt ({why})");
                    return Err(io::Error::other(format!("the worker's ring is corrupt: {why}")));
                }
                _ = tokio::time::sleep(tick) => {}
            }
            if let Some(c) = &shm_conn {
                let now = c.shared.heartbeat().load(Ordering::Acquire);
                if now != beat {
                    beat = now;
                    heard = true;
                }
                if refreshed.elapsed() >= Duration::from_millis(250)
                    && let Some(log) = self.log()
                {
                    refreshed = Instant::now();
                    let (position, segment) = log.position();
                    c.shared.position().fetch_max(position, Ordering::AcqRel);
                    c.shared.segment().fetch_max(segment, Ordering::AcqRel);
                    for shard in 0..inboxes.len() as u32 {
                        if inboxes[shard as usize].is_some()
                            && let Some(line) = c.shared.line(shard)
                        {
                            line.pins()
                                .store(u32::from(log.pins(shard)), Ordering::Relaxed);
                        }
                    }
                }
            }
            // More shards freed up: open them, then tell the worker.
            if let Some(g) = granted {
                self.open_shards(
                    &g,
                    &threads,
                    &outbox,
                    &drained,
                    &mut inboxes,
                    shm_conn.as_ref(),
                )
                .await?;
                let mut b = Vec::new();
                put_u64(&mut b, 0);
                put_grant(&mut b, &g);
                let mut f = Vec::new();
                frame::encode(resp::GRANT, &b, &mut f);
                outbox.send(&f);
                // Messages kept for the new shards follow.
                if relays {
                    self.relay_to(conn, &outbox);
                }
                let shards: Vec<u32> = g.iter().map(|(s, _)| *s).collect();
                self.deliver_parked(conn, &shards);
            }
            // A worker that keeps the socket readable must not starve these.
            while let Ok(f) = relay_rx.try_recv() {
                outbox.send(&f);
            }
            while let Ok(result) = limit_rx.try_recv() {
                outbox.send(&result?);
            }
            if threads.exited() {
                return Err(io::Error::other("a shard thread of this lease stopped"));
            }
            let blocked = outbox.write_queued()?;
            let Some(timeout) = stall else { continue };
            if blocked.is_some_and(|b| b >= timeout) {
                eprintln!(
                    "Fusee server revoked the lease of epoch {epoch}: its worker read nothing for {} ms",
                    timeout.as_millis()
                );
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "lease revoked: the worker stopped reading",
                ));
            }
            if heard {
                silent = Duration::ZERO;
            } else if !full {
                silent += waiting.elapsed();
                if silent >= timeout {
                    eprintln!(
                        "Fusee server revoked the lease of epoch {epoch}: no traffic from its worker for {} ms",
                        timeout.as_millis()
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "lease revoked: the worker sent nothing within the liveness timeout",
                    ));
                }
            }
        }
    }

    /// Starts a thread for each shard of `grant`, each owning fresh storage
    /// handles, and waits until all have opened. Returns the first one's
    /// first segment size (log format 2).
    async fn open_shards(
        self: &Arc<Self>,
        grant: &[(u32, u64)],
        threads: &Arc<Shards>,
        outbox: &Arc<Outbox>,
        drained: &Arc<Notify>,
        inboxes: &mut [Option<Arc<Inbox>>],
        shm: Option<&Arc<ring::ShmConn>>,
    ) -> io::Result<u64> {
        let Storage::Local { cfg, log, .. } = &self.storage else {
            return Ok(u64::MAX);
        };
        let mut opening = Vec::new();
        for &(shard, epoch) in grant {
            if let Some(log) = log {
                // The previous holder's shard thread is gone; what it staged
                // is dropped and its writes in flight finish before this
                // lease recovers the shard.
                let sink: Arc<dyn log::Sink> = match shm {
                    Some(c) => Arc::new(ring::ShmSink { conn: c.clone() }),
                    None => Arc::new(ConnSink {
                        outbox: outbox.clone(),
                    }),
                };
                log.grant(shard, epoch, sink)?;
                threads.granted(shard, epoch);
            }
            let disk = Disk::Open {
                dir: cfg.dir.join(format!("shard-{shard:03}")),
                options: cfg.local.clone(),
                log: log.clone(),
            };
            let (inbox, opened) = threads.start(
                self.clone(),
                shard,
                epoch,
                disk,
                outbox.clone(),
                drained.clone(),
            )?;
            if let Some(c) = shm {
                c.granted(shard, epoch, inbox.clone());
            }
            inboxes[shard as usize] = Some(inbox);
            opening.push(opened);
        }
        // Shards open in parallel.
        let mut initial = u64::MAX;
        for (i, opened) in opening.into_iter().enumerate() {
            let size = opened
                .await
                .map_err(|_| io::Error::other("a shard thread exited while opening"))??;
            if i == 0 {
                initial = size;
            }
        }
        Ok(initial)
    }

    /// Answers connection-level requests here and hands every shard
    /// request to its shard's thread, in arrival order.
    #[allow(clippy::too_many_arguments)]
    fn route(
        &self,
        conn: u64,
        kind: u8,
        body: Vec<u8>,
        epoch: u64,
        inboxes: &[Option<Arc<Inbox>>],
        session: Option<&WorkerSession>,
        limit_tx: &UnboundedSender<io::Result<Vec<u8>>>,
        out: &mut Vec<u8>,
    ) {
        let mut r = Reader::new(&body);
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
        if kind == req::PING {
            reply(out, Ok(Vec::new()));
            // Idle shards learn here that one more checkpoint of theirs
            // lets the shared log retire a segment.
            if let Some(log) = self.log() {
                let marks: Vec<log::Mark> = (0..inboxes.len() as u32)
                    .filter(|&shard| inboxes[shard as usize].is_some())
                    .map(|shard| log::Mark {
                        shard,
                        lsn: log.durable(shard),
                        pins: log.pins(shard),
                    })
                    .collect();
                let (position, segment) = log.position();
                out.extend_from_slice(&durable_frame(&marks, position, segment));
            }
            return;
        }
        if kind == req::DRAIN {
            let Ok(n) = r.u32() else { return };
            {
                let mut l = self.lease();
                for _ in 0..n.min(self.shards()) {
                    let Ok(s) = r.u32() else { break };
                    if l.shards
                        .get(s as usize)
                        .copied()
                        .flatten()
                        .is_some_and(|(c, _)| c == conn)
                    {
                        l.draining[s as usize] = true;
                    }
                }
            }
            // After every call routed to this worker before it.
            if let Some(tx) = self
                .workers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&conn)
            {
                let _ = tx.try_send(ok_frame(h.id, &[]));
            }
            return;
        }
        if kind == req::IDLE {
            let Some(d) = self.invoke.get() else { return };
            let entries = parse_idle(&mut r);
            let l = self.lease();
            let entries: Vec<IdleEntry> = entries
                .into_iter()
                .filter(|e| {
                    l.shards.get(e.shard as usize).copied().flatten() == Some((conn, e.epoch))
                })
                .collect();
            drop(l);
            d.report(conn, entries);
            return;
        }
        if kind == req::RELAY || kind == req::RELAY_ACKS {
            // Only the shard's current holder may send for it.
            let holds = self.lease().shards.get(h.shard as usize).copied().flatten()
                == Some((conn, h.epoch));
            if !holds {
                return;
            }
            if kind == req::RELAY {
                if let (Ok(target), Ok(entries)) = (r.u32(), r.bytes()) {
                    self.forward_messages(target, h.shard, h.epoch, entries);
                }
                return;
            }
            let Ok(n) = r.u32() else { return };
            let mut acks = Vec::with_capacity(n.min(4096) as usize);
            for _ in 0..n {
                let (Ok(shard), Ok(epoch), Ok(id)) = (r.u32(), r.u64(), r.u64()) else {
                    break;
                };
                acks.push((shard, epoch, id));
            }
            self.forward_acks(&acks);
            return;
        }
        if kind != req::LIMIT && kind != req::CALL_REPLY {
            let Some(Some(inbox)) = inboxes.get(h.shard as usize) else {
                reply(
                    out,
                    Err(io::Error::other(format!(
                        "this worker does not hold shard {}",
                        h.shard
                    ))),
                );
                return;
            };
            // A section for the shared log needs no shard thread: when
            // that thread has nothing pending, this task stages it (and on
            // a fast disk with nothing in flight writes it) itself, saving
            // a thread handoff. The log checks the epoch. Anything else, or
            // a failure, goes to the thread, which answers in order.
            if kind == req::APPEND
                && let Some(log) = self.log()
                && inbox
                    .if_idle(|| {
                        proto::decode_section(&mut r)
                            .ok()
                            .is_some_and(|batch| log.append(h.shard, h.epoch, batch, true).is_ok())
                    })
                    .unwrap_or(false)
            {
                return;
            }
            // The shard thread checks the epoch, in order with its replies.
            inbox.push(kind, body);
            return;
        }
        if h.epoch != epoch || !self.holds_any(conn) {
            reply(
                out,
                Err(io::Error::other(
                    "lease epoch is stale: another worker holds the shards",
                )),
            );
            return;
        }
        if kind == req::LIMIT {
            let parsed = (|| {
                let o = r.u8()?;
                let name = r.str()?.to_owned();
                let ticket = r.u64()?;
                let a = r.u32()?;
                let b = f64::from_bits(r.u64()?);
                let priority = crate::limits::read_priority(&mut r)?;
                Ok::<_, fusee_core::wire::DecodeError>((o, name, ticket, a, b, priority))
            })();
            let Ok((o, name, ticket, a, b, priority)) = parsed else {
                return;
            };
            if let Some(session) = session {
                if let Err(e) =
                    session.request_limit(o, name, ticket, a, b, priority, limit_tx.clone())
                {
                    let _ = limit_tx.send(Err(e));
                }
                return;
            }
            let now = self
                .limit_epoch_ms
                .saturating_add(self.limit_clock.elapsed().as_millis() as u64);
            match o {
                op::DEFINE => self.limits.define(
                    &name,
                    LimitSpec {
                        concurrent: a,
                        per_second: b,
                    },
                ),
                op::ACQUIRE => {
                    use crate::limits::Grant;
                    let res = self.limits.acquire(
                        &name,
                        conn,
                        ticket,
                        &priority,
                        now,
                        crate::limits::LEASE_MS,
                    );
                    if res != Grant::Queued {
                        let mut f = Vec::new();
                        // The grant may name another of this worker's tickets.
                        put_u64(
                            &mut f,
                            if let Grant::Granted(t) = res {
                                t
                            } else {
                                ticket
                            },
                        );
                        f.push(u8::from(matches!(res, Grant::Granted(_))));
                        let delay = match res {
                            Grant::RetryAt(at) => Some(at.saturating_sub(now)),
                            _ => None,
                        };
                        put_u64(
                            &mut f,
                            delay.map_or(0, |d| fusee::local::unix_ms().saturating_add(d)),
                        );
                        put_u64(&mut f, delay.unwrap_or(0));
                        frame::encode(resp::LIMITED, &f, out);
                    }
                }
                op::RELEASE => self.limits.release(&name, conn, ticket),
                op::RENEW => self.limits.renew(conn, now, crate::limits::LEASE_MS),
                _ => {}
            }
            return;
        }
        // A worker's answer to a relayed call.
        let relay = h.id;
        let ok = r.u8().unwrap_or(0) == 1;
        let data = r.bytes().unwrap_or_default().to_vec();
        let mut pending = self.relays.lock().unwrap_or_else(|e| e.into_inner());
        if pending.get(&relay).is_some_and(|req| req.holder == conn)
            && let Some(Relay {
                reply: tx,
                request: id,
                gather,
                ..
            }) = pending.remove(&relay)
        {
            let f = match gather {
                Some(g) => g
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .answer(id, ok, &data),
                None if ok => Some(ok_frame(id, &data)),
                None => Some(err_frame(id, &String::from_utf8_lossy(&data))),
            };
            if let Some(f) = f {
                let _ = tx.send(f);
            }
        }
    }
}

/// A connection the server can serve: read by the connection task, written
/// by whichever thread produced a reply.
pub trait Socket: Send + 'static {
    type Read: AsyncRead + Unpin + Send + 'static;
    type Write: TryWrite;
    fn split(self) -> (Self::Read, Self::Write);
}

/// The write side of a [`Socket`]. Shard threads write without a runtime.
pub trait TryWrite: Send + Sync + 'static {
    /// Writes what the socket takes now; `WouldBlock` when it takes nothing.
    fn try_write(&self, buf: &[u8]) -> io::Result<usize>;
    fn writable(&self) -> impl Future<Output = io::Result<()>> + Send;
    /// The socket's descriptor if it is a Unix socket, which can carry
    /// descriptors (same-host shared memory).
    fn unix_fd(&self) -> Option<std::os::fd::RawFd> {
        None
    }
}

impl Socket for tokio::net::TcpStream {
    type Read = tokio::net::tcp::OwnedReadHalf;
    type Write = tokio::net::tcp::OwnedWriteHalf;
    fn split(self) -> (Self::Read, Self::Write) {
        self.into_split()
    }
}

impl TryWrite for tokio::net::tcp::OwnedWriteHalf {
    fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        tokio::net::tcp::OwnedWriteHalf::try_write(self, buf)
    }
    fn writable(&self) -> impl Future<Output = io::Result<()>> + Send {
        tokio::net::tcp::OwnedWriteHalf::writable(self)
    }
}

impl Socket for tokio::net::UnixStream {
    type Read = tokio::net::unix::OwnedReadHalf;
    type Write = tokio::net::unix::OwnedWriteHalf;
    fn split(self) -> (Self::Read, Self::Write) {
        self.into_split()
    }
}

impl TryWrite for tokio::net::unix::OwnedWriteHalf {
    fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        tokio::net::unix::OwnedWriteHalf::try_write(self, buf)
    }
    fn writable(&self) -> impl Future<Output = io::Result<()>> + Send {
        tokio::net::unix::OwnedWriteHalf::writable(self)
    }
    fn unix_fd(&self) -> Option<std::os::fd::RawFd> {
        use std::os::fd::AsRawFd;
        let stream: &tokio::net::UnixStream = self.as_ref();
        Some(stream.as_raw_fd())
    }
}

async fn write_all<W: TryWrite + ?Sized>(writer: &W, mut data: &[u8]) -> io::Result<()> {
    while !data.is_empty() {
        match writer.try_write(data) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => data = &data[n..],
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => writer.writable().await?,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Listens on a TCP address (and optionally a Unix socket) until the task
/// is cancelled. Each connection first completes the server's TLS handshake
/// and token exchange, if it has either ([`Server::set_security`]). A
/// listener on a non-loopback address with neither prints a warning.
pub async fn serve_tcp(server: Arc<Server>, listener: tokio::net::TcpListener) -> io::Result<()> {
    if let Ok(addr) = listener.local_addr() {
        let acceptor = server.acceptor();
        crate::security::warn_if_exposed("server", addr, acceptor.has_tls(), acceptor.has_token());
    }
    loop {
        let (sock, peer) = listener.accept().await?;
        let _ = sock.set_nodelay(true);
        let acceptor = server.acceptor();
        if acceptor.is_open() {
            tokio::spawn(server.clone().serve_conn(sock));
            continue;
        }
        let server = server.clone();
        tokio::spawn(async move {
            match acceptor.accept_tcp_scoped(sock).await {
                Ok((Secured::Plain(sock), scope)) => server.serve_conn_scoped(sock, scope).await,
                Ok((Secured::Tls(sock), scope)) => server.serve_conn_scoped(sock, scope).await,
                Err(e) => crate::security::refused(
                    format_args!("server: refused a connection from {peer}"),
                    &e,
                ),
            }
        });
    }
}

/// Listens on a Unix socket. The token applies here as on TCP; TLS does not.
pub async fn serve_unix(server: Arc<Server>, listener: tokio::net::UnixListener) -> io::Result<()> {
    loop {
        let (sock, _) = listener.accept().await?;
        let acceptor = server.acceptor();
        if !acceptor.has_token() {
            tokio::spawn(server.clone().serve_conn(sock));
            continue;
        }
        let server = server.clone();
        tokio::spawn(async move {
            match acceptor.accept_unix_scoped(sock).await {
                Ok((sock, scope)) => server.serve_conn_scoped(sock, scope).await,
                Err(e) => crate::security::refused(
                    format_args!("server: refused a Unix socket connection"),
                    &e,
                ),
            }
        });
    }
}

/// The entries of an `IDLE` frame (see [`proto::req::IDLE`]).
fn parse_idle(r: &mut Reader<'_>) -> Vec<IdleEntry> {
    let mut out = Vec::new();
    let Ok(n) = r.u32() else { return out };
    for _ in 0..n.min(1 << 16) {
        let entry = (|| {
            let shard = r.u32()?;
            let epoch = r.u64()?;
            let flags = r.u8()?;
            let due_in_ms = r.u64()?;
            let steps = r.u64()?;
            let open_us = r.u64()?;
            let k = r.u32()?;
            let mut types = Vec::new();
            for _ in 0..k.min(256) {
                types.push(String::from_utf8_lossy(r.bytes()?).into_owned());
            }
            Ok::<_, fusee_core::wire::DecodeError>(IdleEntry {
                shard,
                epoch,
                flags,
                due_in_ms,
                steps,
                open_us,
                types,
            })
        })();
        match entry {
            Ok(e) => out.push(e),
            Err(_) => break,
        }
    }
    out
}

/// The workflow type a `start` or `updateWithStart` call names.
fn start_type(call: &[u8]) -> Option<String> {
    let mut r = Reader::new(call);
    let method = r.str().ok()?;
    if !matches!(method, "start" | "updateWithStart") {
        return None;
    }
    let args: serde_json::Value = serde_json::from_slice(r.bytes().ok()?).ok()?;
    args.get("type")?.as_str().map(str::to_owned)
}

/// `count u32, then shard u32, epoch u64` each.
fn put_grant(out: &mut Vec<u8>, grant: &[(u32, u64)]) {
    put_u32(out, grant.len() as u32);
    for &(shard, epoch) in grant {
        put_u32(out, shard);
        put_u64(out, epoch);
    }
}

fn ok_frame(id: u64, data: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(data.len() + 8);
    put_u64(&mut b, id);
    b.extend_from_slice(data);
    let mut f = Vec::new();
    frame::encode(resp::OK, &b, &mut f);
    f
}

fn err_frame(id: u64, msg: &str) -> Vec<u8> {
    let mut b = Vec::new();
    put_u64(&mut b, id);
    put_bytes(&mut b, msg.as_bytes());
    let mut f = Vec::new();
    frame::encode(resp::ERR, &b, &mut f);
    f
}

pub fn head_for(id: u64, shard: u32, epoch: u64) -> Head {
    Head { id, shard, epoch }
}

/// An `editState` call's body as the `updateWithStart` call it is
/// ([`fusee::http::calls::edit_state_call`]).
fn edit_state_call(call: &[u8]) -> Result<Vec<u8>, String> {
    let mut r = Reader::new(call);
    let _ = r.str().map_err(|e| e.to_string())?;
    let args: serde_json::Value =
        serde_json::from_slice(r.bytes().map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
    let args = fusee::http::calls::edit_state_call(&args).map_err(|e| e.to_string())?;
    let mut b = Vec::new();
    fusee_core::wire::put_bytes(&mut b, b"updateWithStart");
    fusee_core::wire::put_bytes(&mut b, args.to_string().as_bytes());
    Ok(b)
}
