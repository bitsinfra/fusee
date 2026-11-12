//! The embedded engine: one thread per shard, each running a
//! [`ShardRuntime`] inside a single-threaded Tokio runtime so step functions
//! can use Tokio I/O.

use crate::codec::Codec;
use crate::ctx::{Ctx, decode, encode};
use crate::defs::{AsQuery, AsSignal, AsUpdate, AsWorkflow, Make, Op, WorkflowRef};
use crate::env::Notify;
use crate::error::{Error, Result};
use crate::limits::{LimitBackend, LimitSpec, LocalLimits};
use crate::local::{LocalEnv, LocalOptions, MemoryEnv};
use crate::queue::{Batch, Debounce, ENDED, EVENT, Event, Receipt, Rule};
use crate::runtime::{
    IdPolicy, Message, Metrics, OnRunning, ResultState, RunDetail, RunOutcome, RunSummary,
    RuntimeConfig, Services, ShardRuntime, SharedRegistry, StartRequest, UpdateRequest, WorkflowFn,
};
use crate::schedule::{CONTROL, Command, SCHEDULE_TYPE, ScheduleSpec, ScheduleState, schedule_id};
use crate::visibility::ListFilter;
use fusee_core::record::{Payload, Status};
use fusee_core::shard::{Output, shard_of};
use rustc_hash::FxHashMap;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Creates a shard's storage (server mode and other custom backends).
pub type EnvFactory = Arc<dyn Fn(u32) -> std::io::Result<Box<dyn crate::env::Env>> + Send + Sync>;

/// What survives what. See the documentation's guarantees page.
#[derive(Clone)]
pub enum Durability {
    /// Outputs wait for a durable flush of this machine's disk. Survives
    /// process crashes, and power loss on drives that honor flushes.
    Local,
    /// Nothing is written. Survives nothing. For tests.
    Memory,
    /// Storage provided by a factory, for example a Fusee server
    /// (`fusee_server::remote_config`). Outputs wait for that storage's
    /// durable flush.
    Custom(EnvFactory),
}

impl std::fmt::Debug for Durability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Local => "Local",
            Self::Memory => "Memory",
            Self::Custom(_) => "Custom",
        })
    }
}

impl PartialEq for Durability {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Custom(a), Self::Custom(b)) => Arc::ptr_eq(a, b),
            _ => std::mem::discriminant(self) == std::mem::discriminant(other),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub dir: PathBuf,
    pub durability: Durability,
    /// Number of shards (threads, logs). Fixed for a data directory.
    pub shards: Option<u32>,
    pub local: LocalOptions,
    pub retention: Duration,
    /// The longest a suspended run stays loaded (its code and journal in
    /// memory); `Duration::MAX` never parks.
    pub park_after: Duration,
    /// Bytes loaded runs may take across all shards (an estimate). Above
    /// it, each shard parks the runs suspended longest before
    /// `park_after`. 0: no budget.
    pub memory_budget: u64,
    pub checkpoint_interval: Duration,
    /// Checkpoint each shard on [`Engine::shutdown`], so the next open
    /// replays nothing and covered log segments are deleted.
    pub checkpoint_on_shutdown: bool,
    /// New local directories use log format 3: one log shared by every
    /// shard ([`crate::wal`]). Existing directories keep the format they
    /// were created with.
    pub shared_log: bool,
    pub max_loaded_per_shard: usize,
    pub inline_flush: bool,
    /// A shard flushes inline only while the last flush took at most this
    /// long (on a slower disk, blocking the shard thread would delay new
    /// work by a whole flush).
    pub inline_flush_max: Duration,
    /// Bounded command queue per shard; a full queue makes calls `Busy`.
    pub queue: usize,
    /// Keep finished runs' whole journals (for audit, `inspect`, export
    /// and reset of finished runs).
    pub keep_history: bool,
    /// What a step does when its retries run out, unless its retry policy
    /// says ([`crate::Exhausted`]; default: fail).
    pub on_exhausted: crate::ctx::Exhausted,
    /// Archive complete terminal journals beyond hot retention.
    pub archive_history: bool,
    #[cfg(feature = "opentelemetry")]
    pub telemetry: Option<Arc<crate::telemetry::Telemetry>>,
    /// Payload encryption, applied before payloads become records.
    pub codec: Option<Arc<dyn Codec>>,
    /// Where step limits live; `None` keeps them in this process.
    pub limits: Option<Arc<dyn LimitBackend>>,
    /// Continuous backup to object storage ([`crate::backup`]).
    pub backup: Option<crate::backup::BackupConfig>,
    /// A record whose encoding is longer than this many bytes (a large step
    /// result, signal, update, start input or stream chunk) is written to
    /// the shared log's blob area, in parallel with the log, instead of
    /// into a log batch, so it does not delay other shards' records
    /// (docs/durability.md, "Large payloads"). 0: never. Local directories
    /// in log format 3 only: `memory` mode ignores it and server and
    /// replicated mode refuse a nonzero value.
    pub payload_offload_bytes: u64,
    /// `Production`, or `Dev` for development ([`crate::dev`]). `None` reads
    /// `FUSEE_PROFILE` when the engine opens; it is never inferred.
    pub profile: Option<crate::dev::Profile>,
    /// Serve the HTTP API and the web UI here once open. Under the dev
    /// profile, `None` means `FUSEE_HTTP`, else `127.0.0.1:7601`, else a
    /// free port; `off` serves nothing.
    pub http: Option<String>,
    /// Where the engine's HTTP API is reached (completion URLs); set by
    /// [`Engine::open`] when it serves, or [`Engine::set_callback_url`].
    #[doc(hidden)]
    pub callback_url: Arc<RwLock<Option<String>>>,
    /// The test kit's hooks ([`crate::testing`]).
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub test_hooks: Option<Arc<crate::testing::Hooks>>,
}

/// [`Config::payload_offload_bytes`] of [`Config::local`].
pub const DEFAULT_PAYLOAD_OFFLOAD_BYTES: u64 = 64 << 10;

/// The retention a tool opens a data directory with (the CLI's `--dir`,
/// `fusee backup`, `fusee export`, `fusee reshard`): nothing expires.
/// Retention is the application's: a shard removes finished runs past it
/// when it opens, so a tool opening the directory with its own (the 24 h
/// default) would delete runs the application keeps longer.
pub const TOOL_RETENTION: Duration = Duration::from_secs(100 * 365 * 86_400);

impl Config {
    /// `local` durability with the data directory at `dir`.
    pub fn local(dir: impl AsRef<Path>) -> Self {
        Self {
            dir: dir.as_ref().to_owned(),
            durability: Durability::Local,
            shards: None,
            local: LocalOptions::default(),
            retention: Duration::from_secs(24 * 3600),
            park_after: Duration::from_secs(1),
            memory_budget: 128 << 20,
            checkpoint_interval: Duration::from_secs(30),
            checkpoint_on_shutdown: true,
            shared_log: true,
            max_loaded_per_shard: 1_000_000,
            inline_flush: true,
            inline_flush_max: Duration::from_micros(200),
            queue: 65_536,
            keep_history: false,
            on_exhausted: crate::ctx::Exhausted::Fail,
            archive_history: false,
            #[cfg(feature = "opentelemetry")]
            telemetry: None,
            codec: None,
            limits: None,
            backup: None,
            payload_offload_bytes: DEFAULT_PAYLOAD_OFFLOAD_BYTES,
            profile: None,
            http: None,
            callback_url: Arc::new(RwLock::new(None)),
            #[cfg(feature = "testing")]
            test_hooks: None,
        }
    }

    /// `memory` durability: nothing is written.
    pub fn memory() -> Self {
        Self {
            durability: Durability::Memory,
            payload_offload_bytes: 0,
            ..Self::local("")
        }
    }

    /// See [`Config::payload_offload_bytes`]; 0 turns offload off.
    pub fn payload_offload_bytes(mut self, bytes: u64) -> Self {
        self.payload_offload_bytes = bytes;
        self
    }

    pub fn shards(mut self, n: u32) -> Self {
        self.shards = Some(n.max(1));
        self
    }

    pub fn retention(mut self, d: Duration) -> Self {
        self.retention = d;
        self
    }

    pub fn park_after(mut self, d: Duration) -> Self {
        self.park_after = d;
        self
    }

    pub fn memory_budget(mut self, bytes: u64) -> Self {
        self.memory_budget = bytes;
        self
    }

    pub fn shared_log(mut self, on: bool) -> Self {
        self.shared_log = on;
        self
    }

    pub fn checkpoint_on_shutdown(mut self, on: bool) -> Self {
        self.checkpoint_on_shutdown = on;
        self
    }

    pub fn checkpoint_interval(mut self, d: Duration) -> Self {
        self.checkpoint_interval = d;
        self
    }

    pub fn segment_bytes(mut self, n: u64) -> Self {
        self.local.segment_bytes = n;
        self
    }

    pub fn local_options(mut self, o: LocalOptions) -> Self {
        self.local = o;
        self
    }

    pub fn keep_history(mut self, on: bool) -> Self {
        self.keep_history = on;
        self
    }

    /// What a step does when its retries run out, for every step whose
    /// retry policy does not say ([`crate::Exhausted::Pause`]: the run
    /// waits for [`RunHandle::resume`](crate::RunHandle::resume)).
    pub fn on_exhausted(mut self, then: crate::ctx::Exhausted) -> Self {
        self.on_exhausted = then;
        self
    }

    /// See [`crate::dev`]. Without it, `FUSEE_PROFILE` decides.
    pub fn profile(mut self, p: crate::dev::Profile) -> Self {
        self.profile = Some(p);
        self
    }

    /// The dev profile ([`crate::dev`]): every run's history kept, each run
    /// replayed against the code when it waits or ends, the UI served.
    pub fn dev(self) -> Self {
        self.profile(crate::dev::Profile::Dev)
    }

    /// Serves the HTTP API and the web UI at `addr` (loopback; for a token
    /// or another interface use [`crate::http::serve`]).
    /// [`Engine::http_url`] gives the address.
    pub fn http(mut self, addr: impl Into<String>) -> Self {
        self.http = Some(addr.into());
        self
    }

    /// Write immutable, checksummed segments for finished workflows. Archived
    /// journals remain available to offline export after hot retention expires.
    pub fn archive_history(mut self, on: bool) -> Self {
        self.archive_history = on;
        self
    }

    pub fn inline_flush(mut self, on: bool) -> Self {
        self.inline_flush = on;
        self
    }

    /// Ships every sealed segment of the shared log and every completed
    /// checkpoint to `target` in the background, for restore to a point in
    /// time with [`crate::backup::restore`] or `fusee restore`. Local
    /// directories in log format 3 only. See [`crate::backup`].
    pub fn backup(
        mut self,
        target: Arc<dyn crate::backup::Target>,
        options: crate::backup::BackupOptions,
    ) -> Self {
        self.backup = Some(crate::backup::BackupConfig::new(target, options));
        self
    }

    /// Encrypts payloads with `codec` (see [`crate::codec::aes_gcm`]).
    pub fn codec(mut self, codec: Arc<dyn Codec>) -> Self {
        self.codec = Some(codec);
        self
    }

    /// Use the application's OpenTelemetry providers. Set `traces` to capture
    /// flush and durable-gate spans as well as per-shard metrics.
    #[cfg(feature = "opentelemetry")]
    pub fn opentelemetry(mut self, traces: bool) -> Self {
        self.telemetry = Some(Arc::new(crate::telemetry::Telemetry::new(traces)));
        self
    }

    /// The services every shard of this engine shares.
    pub fn services(&self) -> Services {
        Services {
            codec: self.codec.clone(),
            limits: self
                .limits
                .clone()
                .unwrap_or_else(|| Arc::new(LocalLimits::new())),
        }
    }
}

/// How a run is started.
#[derive(Clone, Debug, Default)]
pub struct StartOptions {
    /// Workflow deadline: the run ends `TimedOut` after this long.
    pub timeout: Option<Duration>,
    /// What happens when the workflow ID is running or recently finished.
    pub id_policy: IdPolicy,
    /// Run a specific registered version instead of the newest.
    pub version: Option<u32>,
    /// Start the run at this time (Unix ms) instead of now
    /// ([`crate::delay`]).
    pub at_ms: Option<u64>,
    /// The W3C `traceparent` the run joins: its spans, and its children's,
    /// go in that trace. With the `opentelemetry` feature, the current
    /// span's context is used when this is not set.
    pub trace: Option<String>,
    /// For a type with a limit on runs at once ([`Engine::limit_runs`]),
    /// the key the limit counts by (a tenant): runs of one key take turns
    /// apart from other keys'.
    pub concurrency_key: Option<String>,
}

/// The trace context a start records: the one given, else (with the
/// `opentelemetry` feature) the current span's.
fn trace_of(opts: &StartOptions) -> Option<Payload> {
    let t = opts.trace.clone().or_else(current_traceparent)?;
    Some(Arc::from(t.into_bytes()))
}

#[cfg(feature = "opentelemetry")]
fn current_traceparent() -> Option<String> {
    use opentelemetry::trace::TraceContextExt;
    let cx = opentelemetry::Context::current();
    let span = cx.span();
    let sc = span.span_context();
    sc.is_valid().then(|| {
        format!(
            "00-{}-{}-{:02x}",
            sc.trace_id(),
            sc.span_id(),
            sc.trace_flags().to_u8()
        )
    })
}

#[cfg(not(feature = "opentelemetry"))]
fn current_traceparent() -> Option<String> {
    None
}

impl StartOptions {
    pub fn id_policy(mut self, p: IdPolicy) -> Self {
        self.id_policy = p;
        self
    }

    /// Return the running workflow instead of failing.
    pub fn use_existing(mut self) -> Self {
        self.id_policy.running = OnRunning::UseExisting;
        self
    }
}

type Reply<T> = oneshot::Sender<Result<T>>;

/// A function the shard thread calls with a run's result
/// ([`Engine::run_raw_then`]).
type ResultFn = Box<dyn FnOnce(Result<(Status, Payload)>) + Send>;

/// Where a run's result goes: a caller's channel, or a function the shard
/// thread calls with it. A function dropped unanswered (the shard
/// stopped) is called with [`Error::Closed`], which is what a channel's
/// receiver sees then.
struct ResultReply(Option<ReplyTo>);

enum ReplyTo {
    Channel(Reply<(Status, Payload)>),
    Call(ResultFn),
}

impl ResultReply {
    fn send(mut self, r: Result<(Status, Payload)>) {
        match self.0.take() {
            Some(ReplyTo::Channel(tx)) => {
                let _ = tx.send(r);
            }
            Some(ReplyTo::Call(f)) => call_result(f, r),
            None => {}
        }
    }
}

impl From<Reply<(Status, Payload)>> for ResultReply {
    fn from(tx: Reply<(Status, Payload)>) -> Self {
        Self(Some(ReplyTo::Channel(tx)))
    }
}

impl Drop for ResultReply {
    fn drop(&mut self) {
        if let Some(ReplyTo::Call(f)) = self.0.take() {
            call_result(f, Err(Error::Closed));
        }
    }
}

/// A caller's panic stays the caller's: the shard thread that called it
/// keeps running.
fn call_result(f: ResultFn, r: Result<(Status, Payload)>) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || f(r)));
}

enum Cmd {
    Start(StartRequest, Reply<u64>),
    /// A start and a wait for its result in one command: the reply is the
    /// run's outcome.
    Run(StartRequest, ResultReply),
    /// Update-with-start: the reply is the update's result.
    StartUpdate(StartRequest, Reply<(Status, Payload)>),
    Update {
        id: String,
        name: String,
        update_id: String,
        payload: Payload,
        reply: Reply<(Status, Payload)>,
    },
    Query {
        id: String,
        name: String,
        arg: Payload,
        reply: Reply<(Status, Payload)>,
    },
    Complete {
        token: String,
        status: Status,
        payload: Payload,
        reply: Reply<()>,
    },
    Reset {
        id: String,
        before_step: String,
        reapply_signals: bool,
        version: Option<u32>,
        /// Who asked (audit; see [`crate::audit`]).
        by: Option<Payload>,
        reply: Reply<()>,
    },
    ListWhere {
        filter: ListFilter,
        limit: usize,
        reply: Reply<Vec<RunSummary>>,
    },
    Signal {
        id: String,
        name: String,
        payload: Payload,
        dedup: u64,
        by: Option<Payload>,
        reply: Reply<()>,
    },
    Cancel {
        id: String,
        reason: String,
        by: Option<Payload>,
        reply: Reply<()>,
    },
    Resume {
        id: String,
        by: Option<Payload>,
        reply: Reply<()>,
    },
    Terminate {
        id: String,
        reason: String,
        by: Option<Payload>,
        reply: Reply<()>,
    },
    Result {
        id: String,
        reply: Reply<(Status, Payload)>,
    },
    Summary {
        id: String,
        reply: Reply<Option<RunSummary>>,
    },
    Inspect {
        id: String,
        reply: Reply<Option<RunDetail>>,
    },
    StreamRead {
        id: String,
        run: Option<u64>,
        name: String,
        from: u64,
        wait_ms: u64,
        reply: Reply<(Status, Payload)>,
    },
    List {
        limit: usize,
        reply: Reply<Vec<RunSummary>>,
    },
    Registered(String),
    Message {
        from: u32,
        id: u64,
        msg: Message,
    },
    MessageAck(u64),
    /// A message from a shard another process holds, relayed under that
    /// shard's lease `epoch`.
    Relayed {
        from: u32,
        epoch: u64,
        id: u64,
        msg: Message,
    },
    Metrics(Reply<Metrics>),
    /// Dev profile: fail the result waiters of a stuck run.
    RejectStuck(String),
    /// The test kit: whether a step function waits on outside work, and
    /// the shard's earliest timer.
    #[cfg(feature = "testing")]
    NextTimer(Reply<(bool, Option<u64>)>),
    /// The test kit: a journal loaded as a run (after a simulated crash).
    #[cfg(feature = "testing")]
    Import(Vec<fusee_core::record::RecordBuf>, Reply<fusee_core::RunId>),
    Shutdown(Reply<()>),
}

/// A shard's queue of commands from callers, bounded by a count instead of
/// a buffer allocated up front: the channel grows in small blocks as
/// commands arrive and gives them back as they are taken (a 65,536-slot
/// channel per shard would cost megabytes whether or not it is ever used).
struct Inbox {
    rx: Receiver<Cmd>,
    held: Arc<std::sync::atomic::AtomicUsize>,
}

impl Inbox {
    fn try_recv(&self) -> std::result::Result<Cmd, TryRecvError> {
        let cmd = self.rx.try_recv()?;
        self.held.fetch_sub(1, Ordering::AcqRel);
        Ok(cmd)
    }
}

struct ShardHandle {
    queued: Arc<std::sync::atomic::AtomicUsize>,
    tx: Sender<Cmd>,
    /// Commands in `tx` not yet taken, and the most it may hold.
    held: Arc<std::sync::atomic::AtomicUsize>,
    cap: usize,
    /// Unbounded queue for engine-internal commands, so a shard routing a
    /// message to itself (or to a busy peer) never blocks.
    itx: Sender<Cmd>,
    wake: Arc<tokio::sync::Notify>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl ShardHandle {
    fn send(&self, cmd: Cmd) -> Result<()> {
        self.send_or_return(cmd).map_err(|(e, _)| e)
    }

    /// [`send`](Self::send), giving the command back (boxed: only a
    /// refusal pays for it) when it is refused.
    fn send_or_return(&self, cmd: Cmd) -> std::result::Result<(), (Error, Box<Cmd>)> {
        if self.held.fetch_add(1, Ordering::AcqRel) >= self.cap {
            self.held.fetch_sub(1, Ordering::AcqRel);
            return Err((Error::Busy, Box::new(cmd)));
        }
        self.queued.fetch_add(1, Ordering::AcqRel);
        match self.tx.send(cmd) {
            Ok(()) => {
                self.wake.notify_one();
                Ok(())
            }
            Err(std::sync::mpsc::SendError(cmd)) => {
                self.queued.fetch_sub(1, Ordering::AcqRel);
                self.held.fetch_sub(1, Ordering::AcqRel);
                Err((Error::Closed, Box::new(cmd)))
            }
        }
    }

    /// Internal commands are never refused.
    fn send_blocking(&self, cmd: Cmd) {
        self.queued.fetch_add(1, Ordering::AcqRel);
        if self.itx.send(cmd).is_ok() {
            self.wake.notify_one();
        }
    }
}

struct EngineInner {
    shards: Vec<ShardHandle>,
    registry: SharedRegistry,
    closed: AtomicBool,
    config: Config,
    services: Services,
    /// Calls waiting for a shard's reply.
    calls: std::sync::atomic::AtomicUsize,
    /// How long a lone call spins for its reply (see [`Engine::call`]).
    reply_spin: crate::spin::Budget,
    /// Where each workflow type and version was registered, so a second
    /// registration can name both places.
    registered_at: Mutex<FxHashMap<(String, u32), &'static std::panic::Location<'static>>>,
    /// The dev profile's checks and the served UI (dropped after the shards).
    dev: Mutex<Option<crate::dev::Dev>>,
    http_url: Mutex<Option<String>>,
    /// Runs at once per workflow type ([`Engine::limit_runs`]).
    run_limits: std::sync::RwLock<FxHashMap<String, u32>>,
}

/// The longest a lone caller spins for its reply before it parks.
const REPLY_SPIN_CAP: Duration = Duration::from_micros(50);

/// Counts a call while it waits for its reply.
struct InCall<'a>(&'a std::sync::atomic::AtomicUsize);

impl Drop for InCall<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for EngineInner {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            for s in &self.shards {
                let (tx, _rx) = oneshot::channel();
                s.send_blocking(Cmd::Shutdown(tx));
            }
        }
        for s in &self.shards {
            if let Ok(mut t) = s.thread.lock()
                && let Some(t) = t.take()
            {
                let _ = t.join();
            }
        }
    }
}

/// An embedded Fusee engine. Cheap to clone.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

/// An engine handle that does not keep it open (the dev profile's threads
/// and its HTTP server hold one, so dropping the engine still closes it).
#[derive(Clone)]
pub(crate) struct WeakEngine(std::sync::Weak<EngineInner>);

impl WeakEngine {
    pub(crate) fn upgrade(&self) -> Option<Engine> {
        self.0.upgrade().map(|inner| Engine { inner })
    }
}

impl crate::http::calls::Calls for WeakEngine {
    fn call<'a>(
        &'a self,
        method: &'a str,
        args: serde_json::Value,
    ) -> crate::http::calls::CallFuture<'a> {
        let engine = self.upgrade();
        Box::pin(async move {
            match engine {
                Some(e) => crate::http::calls::Calls::call(&e, method, args).await,
                None => Err(Error::Closed),
            }
        })
    }
}

/// Peers a shard thread can route messages to.
struct Peers {
    tx: Vec<Sender<Cmd>>,
    wake: Vec<Arc<tokio::sync::Notify>>,
    queued: Vec<Arc<std::sync::atomic::AtomicUsize>>,
}

impl Peers {
    fn send(&self, shard: u32, cmd: Cmd) {
        let i = shard as usize % self.tx.len().max(1);
        if let (Some(tx), Some(w), Some(q)) = (self.tx.get(i), self.wake.get(i), self.queued.get(i))
        {
            q.fetch_add(1, Ordering::AcqRel);
            if tx.send(cmd).is_ok() {
                w.notify_one();
            }
        }
    }
}

pub(crate) const META: &str = "fusee.meta";

/// Creates the data directory if needed and returns its shard count.
pub fn prepare_dir(config: &Config) -> Result<u32> {
    Ok(open_directory(config)?.0)
}

pub fn open_directory(config: &Config) -> Result<(u32, Option<std::fs::File>)> {
    let lock = match config.durability {
        Durability::Local => Some(crate::local::lock_directory(&config.dir).map_err(|e| {
            // The lock's own message names the directory; an OS error does not.
            if e.kind() == std::io::ErrorKind::WouldBlock {
                Error::Io(e.to_string())
            } else {
                Error::Io(format!(
                    "cannot open the data directory {}: {e}",
                    config.dir.display()
                ))
            }
        })?),
        _ => None,
    };
    Ok((directory_shards(config)?, lock))
}

fn directory_shards(config: &Config) -> Result<u32> {
    Ok(match config.durability {
        Durability::Memory | Durability::Custom(_) => config.shards.unwrap_or(1),
        Durability::Local => {
            match (read_meta(&config.dir)?.map(|m| m.0), config.shards) {
                (Some(existing), Some(asked)) if existing != asked => {
                    return Err(Error::Io(format!(
                        "{} was created with {existing} shards; open it with {existing}, or \
                         change its shard count first with `fusee reshard {} {asked}` while \
                         nothing has it open",
                        config.dir.display(),
                        config.dir.display()
                    )));
                }
                (Some(existing), _) => existing,
                (None, asked) => {
                    let n = asked.unwrap_or_else(|| {
                        // F_FULLFSYNC flushes the whole drive, so shards'
                        // flushes serialize on macOS: one shard is faster.
                        if config.local.flush == crate::local::FlushMethod::FullFsync {
                            return 1;
                        }
                        std::thread::available_parallelism()
                            .map(|n| n.get().min(8) as u32)
                            .unwrap_or(1)
                    });
                    write_meta(&config.dir, n, if config.shared_log { 3 } else { 2 })?;
                    n
                }
            }
        }
    })
}

/// The runtime settings for one shard of an engine.
pub fn runtime_config(config: &Config, shard: u32, shards: u32) -> RuntimeConfig {
    RuntimeConfig {
        shard,
        shards,
        retention_ms: crate::ctx::millis(config.retention),
        park_after_ms: if config.park_after == Duration::MAX {
            u64::MAX
        } else {
            crate::ctx::millis(config.park_after)
        },
        memory_budget: config.memory_budget / u64::from(shards.max(1)),
        checkpoint_interval_ms: crate::ctx::millis(config.checkpoint_interval),
        max_loaded: config.max_loaded_per_shard,
        segment_bytes: config.local.segment_bytes,
        inline_flush: config.inline_flush,
        inline_max_us: config.inline_flush_max.as_micros() as u64,
        keep_history: config.keep_history || config.archive_history,
        on_exhausted: config.on_exhausted,
        archive_history: config.archive_history,
        step_times: config.profile == Some(crate::dev::Profile::Dev),
        #[cfg(feature = "opentelemetry")]
        telemetry: config.telemetry.clone(),
        replay_only: false,
        checkpoint_on_close: config.checkpoint_on_shutdown,
        index_chunk_records: crate::runtime::INDEX_CHUNK_RECORDS,
        index_rebase_deltas: 64,
        sabotage: Default::default(),
    }
}

/// The shared log of a local directory created with log format 3, opened
/// and recovered; `None` for other directories and modes.
pub fn open_shared_log(config: &Config, shards: u32) -> Result<Option<Arc<crate::wal::SharedWal>>> {
    if config.payload_offload_bytes > 0 && matches!(config.durability, Durability::Custom(_)) {
        return Err(Error::Io(
            "payload offload (Config::payload_offload_bytes) needs local durability; server and \
             replicated mode write large payloads into their logs: set it to 0"
                .into(),
        ));
    }
    let shared = matches!(config.durability, Durability::Local)
        && matches!(read_meta(&config.dir)?, Some((_, 3)));
    if !shared {
        if config.backup.is_some() {
            return Err(Error::Io(
                "continuous backup needs a local directory in log format 3 (`fusee upgrade` \
                 converts an older one)"
                    .into(),
            ));
        }
        return Ok(None);
    }
    let opts = crate::wal::WalOptions {
        segment_bytes: config.local.segment_bytes,
        first_segment_bytes: config.local.segment_bytes.min(1 << 20),
        ..Default::default()
    };
    let wal = crate::wal::SharedWal::open_with_offload(
        &config.dir.join("wal"),
        shards,
        config.local.clone(),
        opts,
        config.payload_offload_bytes,
    )?;
    if let Some(backup) = &config.backup {
        crate::backup::attach(&wal, &config.dir, shards, backup)?;
    }
    Ok(Some(wal))
}

/// `(shards, log format)` of a data directory (`None` before its first
/// open). Server mode reads it to pick its log.
pub fn read_meta(dir: &Path) -> Result<Option<(u32, u32)>> {
    let path = dir.join(META);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut shards = None;
    // Directories from before log format 3 have per-shard segments.
    let mut log = 2;
    let mut resharding = false;
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("log=") {
            log = match v.trim() {
                "2" => 2,
                "3" => 3,
                other => {
                    return Err(Error::Io(format!(
                        "{}: log format {other} is not supported by this build",
                        path.display()
                    )));
                }
            };
        }
        if let Some(v) = line.strip_prefix("format=") {
            match v.trim() {
                "1" => {}
                // `fusee reshard` is switching the directory's files over.
                crate::reshard::SWITCHING => resharding = true,
                _ => {
                    return Err(Error::Io(format!(
                        "{}: data directory format {v} is not supported by this build",
                        path.display()
                    )));
                }
            }
        }
        if let Some(v) = line.strip_prefix("shards=") {
            shards = v.trim().parse().ok();
        }
    }
    if resharding {
        let to = shards.unwrap_or(0);
        return Err(Error::Io(format!(
            "{}: changing its shard count to {to} was interrupted; run `fusee reshard {} {to}` \
             to finish it",
            dir.display(),
            dir.display()
        )));
    }
    shards
        .filter(|&n| n > 0)
        .map(|n| Some((n, log)))
        .ok_or_else(|| {
            Error::Io(format!(
                "{} contains an invalid shard count",
                path.display()
            ))
        })
}

/// Writes `fusee.meta` atomically and durably.
pub fn write_meta(dir: &Path, shards: u32, log: u32) -> Result<()> {
    let text =
        format!("# Fusee data directory. Do not edit.\nformat=1\nshards={shards}\nlog={log}\n");
    let tmp = dir.join("fusee.meta.tmp");
    std::fs::write(&tmp, text)?;
    let f = std::fs::File::open(&tmp)?;
    crate::local::flush_file(&f, crate::local::FlushMethod::platform_default())?;
    std::fs::rename(&tmp, dir.join(META))?;
    crate::local::sync_dir(dir, crate::local::FlushMethod::platform_default())?;
    Ok(())
}

/// The error a caller sees for a run (or update, or call) that ended with
/// `status`. A failure's envelope comes back as the error it was: a step's
/// failure as [`Error::Step`], a child's as [`Error::Child`].
pub(crate) fn status_error(status: Status, payload: &[u8]) -> Error {
    match status {
        Status::Ok => Error::failed("unexpected ok"),
        Status::Failed => Error::from_failure(crate::failure::Failure::from_payload(payload)),
        Status::Cancelled => Error::Cancelled(String::from_utf8_lossy(payload).into_owned()),
        Status::TimedOut => Error::TimedOut,
        Status::Terminated | Status::ContinuedAsNew | Status::Reset => Error::Terminated,
    }
}

/// Maps an error returned by workflow code to the run's final status. A
/// failure records its envelope.
pub(crate) fn outcome_of(e: Error) -> (Status, Payload) {
    let (status, msg) = match e {
        Error::Cancelled(r) => (Status::Cancelled, r),
        Error::TimedOut => (Status::TimedOut, "timed out".to_owned()),
        Error::Terminated => (Status::Terminated, "terminated".to_owned()),
        other => return (Status::Failed, other.to_failure().to_payload()),
    };
    (status, Arc::from(msg.into_bytes()))
}

/// Type-erases workflow code: JSON input in, JSON output out.
pub(crate) fn erase<I, O, F, Fut>(f: F) -> WorkflowFn
where
    I: Serialize + DeserializeOwned + 'static,
    O: Serialize + DeserializeOwned + 'static,
    F: Fn(Ctx, I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O>> + 'static,
{
    Arc::new(move |ctx: Ctx| {
        // The code's future is made here, through `f` borrowed, rather than
        // inside the boxed future from a clone of `f`: that clone's count is
        // shared by every shard and moved twice on every start and wake.
        let input: I = match ctx.input_bytes(|bytes| decode(bytes)) {
            Ok(input) => input,
            Err(e) => {
                let e = outcome_of(e);
                return Box::pin(async move { Err(e) })
                    as std::pin::Pin<Box<dyn Future<Output = RunOutcome>>>;
            }
        };
        let undo = ctx.clone();
        let code = f(ctx, input);
        Box::pin(async move {
            let out = match code.await {
                Ok(out) => out,
                Err(e) => {
                    // Failed or cancelled: undo what is registered first.
                    // Boxed, so that every run's future does not carry the
                    // room this rare path takes (1,320 of 1,592 bytes for a
                    // sleeper, past the allocator's per-thread cache).
                    if undo.has_compensations() {
                        Box::pin(undo.run_compensations()).await;
                    }
                    return Err(outcome_of(e));
                }
            };
            encode(&out).map_err(outcome_of)
        }) as std::pin::Pin<Box<dyn Future<Output = RunOutcome>>>
    })
}

impl Engine {
    /// Opens (or creates) an engine. Recovery runs on the shard threads
    /// before this returns; runs resume as their workflow types register.
    pub async fn open(mut config: Config) -> Result<Self> {
        let profile = match config.profile {
            Some(p) => p,
            None => crate::dev::Profile::from_env()?,
        };
        config.profile = Some(profile);
        // This engine's own (a clone of a config must not share it).
        config.callback_url = Arc::new(RwLock::new(None));
        let dev = profile == crate::dev::Profile::Dev;
        let mut plain_fsync = false;
        if dev {
            config.keep_history = true;
            if config.durability == Durability::Local
                && config.local.flush == crate::local::FlushMethod::FullFsync
            {
                config.local.flush = crate::local::FlushMethod::Fsync;
                plain_fsync = true;
            }
        }
        config.backup = config.backup.as_ref().map(|b| b.for_engine());
        let (shards, lock) = open_directory(&config)?;
        let lock = lock.map(Arc::new);
        let wal = open_shared_log(&config, shards)?;
        let registry: SharedRegistry = Arc::new(RwLock::new(Default::default()));
        if let Ok(mut r) = registry.write() {
            crate::schedule::register_builtins(&mut r);
        }
        let services = config.services();
        let mut txs = Vec::new();
        let mut itxs = Vec::new();
        let mut rxs = Vec::new();
        let mut wakes = Vec::new();
        let mut queued = Vec::new();
        let mut helds = Vec::new();
        for _ in 0..shards {
            queued.push(Arc::new(std::sync::atomic::AtomicUsize::new(0)));
            let held = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let (tx, rx) = std::sync::mpsc::channel::<Cmd>();
            let (itx, irx) = std::sync::mpsc::channel::<Cmd>();
            txs.push(tx);
            itxs.push(itx);
            rxs.push((
                Inbox {
                    rx,
                    held: held.clone(),
                },
                irx,
            ));
            helds.push(held);
            wakes.push(Arc::new(tokio::sync::Notify::new()));
        }
        let peers = Arc::new(Peers {
            tx: itxs.clone(),
            wake: wakes.clone(),
            queued: queued.clone(),
        });
        let mut handles = Vec::new();
        let mut ready = Vec::new();
        for (shard, rx) in rxs.into_iter().enumerate() {
            let shard = shard as u32;
            let (rtx, rrx) = oneshot::channel::<Result<()>>();
            ready.push(rrx);
            let cfg = config.clone();
            let reg = registry.clone();
            let wake = wakes[shard as usize].clone();
            let peers = peers.clone();
            let q = queued[shard as usize].clone();
            let svc = services.clone();
            let lock = lock.clone();
            let wal = wal.clone();
            let thread = std::thread::Builder::new()
                .name(format!("fusee-shard-{shard}"))
                .spawn(move || {
                    let _writer = lock;
                    shard_thread(shard, shards, cfg, reg, svc, rx, wake, peers, q, rtx, wal)
                })?;
            handles.push(ShardHandle {
                queued: queued[shard as usize].clone(),
                tx: txs[shard as usize].clone(),
                held: helds[shard as usize].clone(),
                cap: config.queue.max(16),
                itx: itxs[shard as usize].clone(),
                wake: wakes[shard as usize].clone(),
                thread: Mutex::new(Some(thread)),
            });
        }
        let engine = Engine {
            inner: Arc::new(EngineInner {
                shards: handles,
                registry,
                closed: AtomicBool::new(false),
                config,
                services,
                calls: std::sync::atomic::AtomicUsize::new(0),
                reply_spin: crate::spin::Budget::new(REPLY_SPIN_CAP),
                registered_at: Mutex::new(FxHashMap::default()),
                dev: Mutex::new(None),
                http_url: Mutex::new(None),
                run_limits: std::sync::RwLock::new(FxHashMap::default()),
            }),
        };
        for r in ready {
            r.await.map_err(|_| Error::Closed)??;
        }
        crate::dev::start(&engine, dev, plain_fsync)?;
        Ok(engine)
    }

    /// The address of the HTTP API and UI this engine serves itself
    /// ([`Config::http`], or the dev profile's), as `http://host:port`.
    pub fn http_url(&self) -> Option<String> {
        self.inner.http_url.lock().ok()?.clone()
    }

    /// Unix ms by this engine's clock (the test kit's virtual time moves it).
    fn wall_ms(&self) -> u64 {
        #[cfg(feature = "testing")]
        if let Some(h) = &self.inner.config.test_hooks {
            return crate::local::unix_ms() + h.offset_ms();
        }
        crate::local::unix_ms()
    }

    /// The test kit: the earliest timer of any shard, or `None` while a
    /// step function waits on outside work (time must not skip under it).
    #[cfg(feature = "testing")]
    pub(crate) async fn next_timer_ms(&self) -> Result<Option<u64>> {
        let mut next: Option<u64> = None;
        for s in &self.inner.shards {
            let (tx, rx) = oneshot::channel();
            s.send(Cmd::NextTimer(tx))?;
            let (outside, at) = rx.await.map_err(|_| Error::Closed)??;
            if outside {
                return Ok(None);
            }
            if let Some(at) = at {
                next = Some(next.map_or(at, |n| n.min(at)));
            }
        }
        Ok(next)
    }

    /// The test kit: loads a journal as a run on its workflow ID's shard.
    #[cfg(feature = "testing")]
    pub(crate) async fn import_journal(
        &self,
        id: &str,
        records: Vec<fusee_core::record::RecordBuf>,
    ) -> Result<fusee_core::RunId> {
        self.call(id, |reply| Cmd::Import(records, reply)).await
    }

    /// The test kit: wakes every shard (after the clock moved).
    #[cfg(feature = "testing")]
    pub(crate) fn kick_all(&self) {
        for s in &self.inner.shards {
            s.wake.notify_one();
        }
    }

    /// Whether this engine runs under the dev profile.
    pub fn is_dev(&self) -> bool {
        self.inner.config.profile == Some(crate::dev::Profile::Dev)
    }

    pub(crate) fn downgrade(&self) -> WeakEngine {
        WeakEngine(Arc::downgrade(&self.inner))
    }

    pub(crate) fn set_dev(&self, dev: crate::dev::Dev, url: Option<String>) {
        if let Ok(mut d) = self.inner.dev.lock() {
            *d = Some(dev);
        }
        if let Some(u) = &url {
            self.set_callback_url(u);
        }
        if let Ok(mut u) = self.inner.http_url.lock() {
            *u = url;
        }
    }

    /// The URL this engine's HTTP API is reached at from outside (behind a
    /// proxy, or served with [`crate::http::serve`]): completions'
    /// [`url`](crate::ctx::Completion::url) is built from it.
    pub fn set_callback_url(&self, url: &str) {
        if let Ok(mut u) = self.inner.config.callback_url.write() {
            *u = Some(url.trim_end_matches('/').to_owned());
        }
    }

    pub(crate) fn registry(&self) -> SharedRegistry {
        self.inner.registry.clone()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Dev profile: fails the result waiters of `id` if the run is stuck.
    pub(crate) fn reject_stuck(&self, id: &str) {
        let i = shard_of(id, self.inner.shards.len() as u32) as usize;
        if let Some(s) = self.inner.shards.get(i) {
            let _ = s.send(Cmd::RejectStuck(id.to_owned()));
        }
    }

    pub fn shards(&self) -> u32 {
        self.inner.shards.len() as u32
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    /// The continuous backup, when the engine was opened with
    /// [`Config::backup`].
    pub fn backup(&self) -> Option<crate::backup::Backup> {
        self.inner.config.backup.as_ref().map(|b| b.handle())
    }

    fn shard_for(&self, id: &str) -> &ShardHandle {
        let n = self.inner.shards.len() as u32;
        &self.inner.shards[shard_of(id, n) as usize]
    }

    /// Sends a command to the run's shard and waits for the reply. A call
    /// that is the engine's only one spins for its reply first, within the
    /// budget the last such call set ([`crate::spin`]), and stops spinning
    /// once another call starts: a caller waiting alone on a quick answer
    /// should not pay for a thread wake-up, and concurrent callers park.
    async fn call<T>(&self, id: &str, make: impl FnOnce(Reply<T>) -> Cmd) -> Result<T> {
        use tokio::sync::oneshot::error::TryRecvError;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let calls = &self.inner.calls;
        let lone = calls.fetch_add(1, Ordering::AcqRel) == 0;
        let _in_call = InCall(calls);
        let (tx, mut rx) = oneshot::channel();
        let sent = lone.then(Instant::now);
        self.shard_for(id).send(make(tx))?;
        let Some(sent) = sent else {
            return rx.await.map_err(|_| Error::Closed)?;
        };
        let spin = &self.inner.reply_spin;
        let mut reply = None;
        crate::spin::spin_until(spin.get(), || match rx.try_recv() {
            Ok(r) => {
                reply = Some(Ok(r));
                true
            }
            Err(TryRecvError::Closed) => {
                reply = Some(Err(Error::Closed));
                true
            }
            Err(TryRecvError::Empty) => calls.load(Ordering::Acquire) > 1,
        });
        let r = match reply {
            Some(r) => r,
            None => rx.await.map_err(|_| Error::Closed),
        };
        spin.record(sent.elapsed());
        r?
    }

    /// Sends a command to the run's shard and waits for the reply, as a
    /// long poll: without spinning and without counting as a call, so a
    /// reader waiting for a stream does not change how other calls wait.
    async fn call_waiting<T>(&self, id: &str, make: impl FnOnce(Reply<T>) -> Cmd) -> Result<T> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let (tx, rx) = oneshot::channel();
        self.shard_for(id).send(make(tx))?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Reads stream `name` of workflow `id` from offset `from`: the durable
    /// chunks there are at once, else the first ones appended within
    /// `wait` (none if none were), and the end once the read has every
    /// chunk. `run` (from an earlier answer's [`crate::StreamBatch::run`])
    /// keeps reading that run when the ID has moved on to a newer one.
    pub async fn stream_read(
        &self,
        id: &str,
        run: Option<u64>,
        name: &str,
        from: u64,
        wait: Duration,
    ) -> Result<crate::stream::StreamBatch> {
        let p = self.stream_read_raw(id, run, name, from, wait).await?;
        crate::stream::StreamBatch::parse(&p)
    }

    /// [`Self::stream_read`]'s answer as JSON, as relays pass it on.
    #[doc(hidden)]
    pub async fn stream_read_raw(
        &self,
        id: &str,
        run: Option<u64>,
        name: &str,
        from: u64,
        wait: Duration,
    ) -> Result<Payload> {
        let (id2, name) = (id.to_owned(), name.to_owned());
        let wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX);
        let (status, payload) = self
            .call_waiting(id, |reply| Cmd::StreamRead {
                id: id2,
                run,
                name,
                from,
                wait_ms,
                reply,
            })
            .await?;
        match status {
            Status::Ok => Ok(payload),
            s => Err(status_error(s, &payload)),
        }
    }

    /// Defines an object type (see [`crate::object`]): add its handlers,
    /// then [`register`](crate::object::ObjectType::register) it.
    pub fn object_type(&self, name: &str) -> crate::object::ObjectType {
        crate::object::ObjectType::new(self.clone(), name)
    }

    /// Key `key` of object type `wtype`: `engine.object("cart", "user-7")
    /// .call("add", &item).await?`.
    pub fn object(&self, wtype: &str, key: &str) -> crate::object::ObjectRef {
        crate::object::ObjectRef::new(self.clone(), wtype, key)
    }

    /// Keys of object type `wtype`, up to `limit` per shard.
    pub async fn objects(&self, wtype: &str, limit: usize) -> Result<Vec<String>> {
        let runs = self
            .list_where(&serde_json::json!({ "type": wtype }), limit)
            .await?;
        Ok(runs
            .into_iter()
            .filter(|r| r.object && r.finished_ms.is_none())
            .filter_map(|r| crate::object::key_of(&r.id, wtype).map(str::to_owned))
            .collect())
    }

    /// The registered workflow types and their versions, as the `types`
    /// call answers.
    pub fn types_json(&self) -> serde_json::Value {
        let Ok(r) = self.inner.registry.read() else {
            return serde_json::json!([]);
        };
        let mut names = r.names();
        names.sort();
        serde_json::Value::Array(
            names
                .into_iter()
                .filter(|n| !n.starts_with("fusee."))
                .map(|n| {
                    serde_json::json!({
                        "name": n,
                        "versions": r.versions(&n),
                        "object": r.is_object(&n),
                    })
                })
                .collect(),
        )
    }

    /// What this engine is, as the `info` call answers.
    pub fn info_json(&self) -> serde_json::Value {
        let c = &self.inner.config;
        serde_json::json!({
            "engine": "fusee",
            "version": env!("CARGO_PKG_VERSION"),
            "sdk": "rust",
            "durability": match c.durability {
                Durability::Local => "local",
                Durability::Memory => "memory",
                Durability::Custom(_) => "custom",
            },
            "dir": matches!(c.durability, Durability::Local).then(|| c.dir.display().to_string()),
            "shards": self.inner.shards.len(),
            "keepHistory": c.keep_history,
        })
    }

    /// Records where `name` (this version) is registered; a second
    /// registration panics naming both places.
    pub(crate) fn claim(
        &self,
        name: &str,
        version: u32,
        at: &'static std::panic::Location<'static>,
    ) {
        let mut seen = self
            .inner
            .registered_at
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(first) = seen.insert((name.to_owned(), version), at) {
            panic!(
                "workflow type '{name}' (version {version}) is registered twice on this engine: \
                 first at {first}, again at {at}. Register each type once; give a new version \
                 of its code a new version number"
            );
        }
    }

    pub(crate) fn register_object(
        &self,
        name: &str,
        version: u32,
        f: WorkflowFn,
        def: crate::object::ObjectDef,
    ) {
        if let Ok(mut r) = self.inner.registry.write() {
            r.register(name, version, f);
            r.register_object(name, def);
        }
        for s in &self.inner.shards {
            s.send_blocking(Cmd::Registered(name.to_owned()));
        }
    }

    /// A handle to an existing run by workflow ID.
    pub fn run<O: DeserializeOwned>(&self, id: &str) -> RunHandle<O> {
        RunHandle {
            engine: self.clone(),
            id: id.to_owned(),
            _t: PhantomData,
        }
    }

    /// Starts a run of `wtype` with an already-encoded JSON input.
    #[doc(hidden)]
    pub async fn start_raw(
        &self,
        wtype: &str,
        id: &str,
        input: Payload,
        opts: StartOptions,
    ) -> Result<()> {
        // A type with a limit on runs at once starts a slot that waits for
        // its turn ([`crate::concurrency`]).
        if let Some(limit) = self.run_limit(wtype) {
            let q = crate::concurrency::Queued {
                wtype: wtype.to_owned(),
                id: id.to_owned(),
                input: serde_json::from_slice(&input).unwrap_or(serde_json::Value::Null),
                limit,
                key: opts.concurrency_key.clone(),
                timeout_ms: opts.timeout.map(crate::ctx::millis),
            };
            let slot = crate::concurrency::slot_id(id);
            let slot_opts = StartOptions {
                id_policy: opts.id_policy,
                trace: opts.trace.clone(),
                ..Default::default()
            };
            return Box::pin(self.start_raw(
                crate::concurrency::SLOT_TYPE,
                &slot,
                encode(&q)?,
                slot_opts,
            ))
            .await;
        }
        let deadline_ms = opts
            .timeout
            .map(|t| crate::ctx::after(self.wall_ms(), t))
            .unwrap_or(0);
        let mut req = StartRequest::new(id, wtype, input, opts.id_policy);
        req.version = opts.version;
        req.deadline_ms = deadline_ms;
        req.trace = trace_of(&opts);
        match self.call(id, |r| Cmd::Start(req, r)).await {
            Ok(_) => Ok(()),
            Err(Error::AlreadyExists(_)) => Err(self.already_exists(id).await),
            Err(e) => Err(e),
        }
    }

    /// A delayed start ([`crate::delay`]): the coordinator run
    /// `fusee.delay/<id>`, under the start's ID policy.
    #[doc(hidden)]
    pub async fn start_delayed(
        &self,
        wtype: &str,
        id: &str,
        input: Payload,
        at_ms: u64,
        opts: StartOptions,
    ) -> Result<()> {
        // A type with no code fails now, not when the time comes.
        let names = match self.inner.registry.read() {
            Ok(r) if r.latest(wtype).is_some() => None,
            Ok(r) => Some(r.names()),
            Err(_) => None,
        };
        if let Some(names) = names {
            return Err(Error::UnknownWorkflow(crate::error::unknown_workflow(
                wtype, &names,
            )));
        }
        let d = crate::delay::DelayedStart {
            wtype: wtype.to_owned(),
            id: id.to_owned(),
            input: serde_json::from_slice(&input)?,
            at_ms,
            timeout_ms: opts.timeout.map(crate::ctx::millis),
        };
        let coord = StartOptions {
            id_policy: opts.id_policy,
            ..StartOptions::default()
        };
        self.start_raw(
            crate::delay::DELAY_TYPE,
            &crate::delay::coordinator_id(id),
            encode(&d)?,
            coord,
        )
        .await
    }

    /// [`signal_with_start`](Self::signal_with_start) by names: the start
    /// (the running run is used if there is one) and the signal go to the
    /// shard back to back, so the signal is recorded before the run's code
    /// first runs; returns once both are durable.
    #[doc(hidden)]
    pub async fn signal_with_start_raw(
        &self,
        wtype: &str,
        id: &str,
        input: Payload,
        opts: StartOptions,
        signal: &str,
        payload: Payload,
    ) -> Result<()> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let deadline_ms = opts
            .timeout
            .map(|t| crate::ctx::after(self.wall_ms(), t))
            .unwrap_or(0);
        let mut ids = opts.id_policy;
        if ids.running == OnRunning::Fail {
            ids.running = OnRunning::UseExisting;
        }
        let mut req = StartRequest::new(id, wtype, input, ids);
        req.version = opts.version;
        req.deadline_ms = deadline_ms;
        req.trace = trace_of(&opts);
        let shard = self.shard_for(id);
        let (started_tx, started) = oneshot::channel();
        shard.send(Cmd::Start(req, started_tx))?;
        let (signalled_tx, signalled) = oneshot::channel();
        shard.send(Cmd::Signal {
            id: id.to_owned(),
            name: signal.to_owned(),
            payload,
            dedup: 0,
            by: crate::audit::current_bytes(),
            reply: signalled_tx,
        })?;
        match started.await.map_err(|_| Error::Closed)? {
            Ok(_) => {}
            Err(Error::AlreadyExists(_)) => return Err(self.already_exists(id).await),
            Err(e) => return Err(e),
        }
        signalled.await.map_err(|_| Error::Closed)?
    }

    /// The error for a start refused because `id` is taken, naming the run
    /// that holds it (a lookup on the refusal path only).
    pub(crate) async fn already_exists(&self, id: &str) -> Error {
        let holder = self.status(id).await.ok().flatten();
        Error::AlreadyExists(crate::error::already_exists(
            id,
            holder.as_ref().map(|s| (s.run, s.status.as_str())),
        ))
    }

    /// Starts the workflow if it is not running and delivers an update in
    /// the same record; if it is running, delivers the update to it.
    /// Returns the update's result once it is durable.
    #[doc(hidden)]
    pub async fn update_with_start_raw(
        &self,
        wtype: &str,
        id: &str,
        input: Payload,
        opts: StartOptions,
        update: &str,
        update_id: &str,
        arg: Payload,
    ) -> Result<(Status, Payload)> {
        let deadline_ms = opts
            .timeout
            .map(|t| crate::ctx::after(self.wall_ms(), t))
            .unwrap_or(0);
        let mut ids = opts.id_policy;
        if ids.running == OnRunning::Fail {
            ids.running = OnRunning::UseExisting;
        }
        let mut req = StartRequest::new(id, wtype, input, ids);
        req.version = opts.version;
        req.deadline_ms = deadline_ms;
        req.trace = trace_of(&opts);
        req.update = Some(UpdateRequest {
            name: update.to_owned(),
            id: update_id.to_owned(),
            payload: arg,
            token: None,
        });
        self.call(id, |r| Cmd::StartUpdate(req, r)).await
    }

    /// Sends an update and waits for its result. `update_id` (empty: a new
    /// one) makes a retried call return the recorded result.
    #[doc(hidden)]
    pub async fn update_raw(
        &self,
        id: &str,
        name: &str,
        update_id: &str,
        payload: Payload,
    ) -> Result<(Status, Payload)> {
        let (id2, name, update_id) = (id.to_owned(), name.to_owned(), update_id.to_owned());
        self.call(id, |reply| Cmd::Update {
            id: id2,
            name,
            update_id,
            payload,
            reply,
        })
        .await
    }

    #[doc(hidden)]
    pub async fn query_raw(&self, id: &str, name: &str, arg: Payload) -> Result<Payload> {
        let (id2, name) = (id.to_owned(), name.to_owned());
        let (status, payload) = self
            .call(id, |reply| Cmd::Query {
                id: id2,
                name,
                arg,
                reply,
            })
            .await?;
        match status {
            Status::Ok => Ok(payload),
            s => Err(status_error(s, &payload)),
        }
    }

    /// Finishes an async completion (`ctx.await_completion`) with a result.
    /// Resolves once the result is recorded durably; a stale token (an old
    /// attempt, a finished step) is rejected.
    pub async fn complete<T: Serialize>(&self, token: &str, value: &T) -> Result<()> {
        self.complete_raw(token, Status::Ok, encode(value)?).await
    }

    /// Fails an async completion.
    pub async fn fail(&self, token: &str, error: &str) -> Result<()> {
        self.complete_raw(token, Status::Failed, Arc::from(error.as_bytes()))
            .await
    }

    #[doc(hidden)]
    pub async fn complete_raw(&self, token: &str, status: Status, payload: Payload) -> Result<()> {
        let Some((id, ..)) = crate::runtime::parse_token(token) else {
            return Err(Error::Rejected("not a completion token".into()));
        };
        let token = token.to_owned();
        self.call(&id, |reply| Cmd::Complete {
            token,
            status,
            payload,
            reply,
        })
        .await
    }

    /// Starts a new run of `id` from the current run's journal cut just
    /// before step `before_step`. See [`ShardRuntime::reset`].
    pub async fn reset(&self, id: &str, before_step: &str, reapply_signals: bool) -> Result<()> {
        let (id2, before_step) = (id.to_owned(), before_step.to_owned());
        let by = crate::audit::current_bytes();
        self.call(id, |reply| Cmd::Reset {
            id: id2,
            before_step,
            reapply_signals,
            version: None,
            by,
            reply,
        })
        .await
    }

    /// Runs matching a filter (see [`crate::visibility`]): the newest
    /// `limit` of each shard, each shard's newest first.
    pub async fn list_where(
        &self,
        filter: &serde_json::Value,
        limit: usize,
    ) -> Result<Vec<RunSummary>> {
        let all: Vec<u32> = (0..self.shards()).collect();
        self.list_where_in(&all, filter, limit).await
    }

    /// [`list_where`](Self::list_where) over `shards` only, in that order:
    /// in server mode, the shards this worker holds.
    pub async fn list_where_in(
        &self,
        shards: &[u32],
        filter: &serde_json::Value,
        limit: usize,
    ) -> Result<Vec<RunSummary>> {
        let filter = ListFilter::parse(filter)?;
        let mut out = Vec::new();
        for &shard in shards {
            let s = self
                .inner
                .shards
                .get(shard as usize)
                .ok_or_else(|| Error::Rejected(format!("this engine has no shard {shard}")))?;
            let (tx, rx) = oneshot::channel();
            s.send(Cmd::ListWhere {
                filter: filter.clone(),
                limit,
                reply: tx,
            })?;
            out.extend(rx.await.map_err(|_| Error::Closed)??);
        }
        Ok(out)
    }

    /// Defines (or resizes) a concurrency and rate limit for steps.
    pub fn limit(&self, name: &str, spec: LimitSpec) -> Result<()> {
        spec.validate()?;
        self.inner.services.limits.define(name, spec);
        Ok(())
    }

    /// Permits of a limit held right now.
    pub fn limit_in_use(&self, name: &str) -> u32 {
        self.inner.services.limits.in_use(name)
    }

    /// This process's steps waiting for a permit of a limit right now.
    pub fn limit_waiting(&self, name: &str) -> usize {
        self.inner.services.limits.waiting(name)
    }

    /// Every limit defined here with its permits now, by name (`fusee
    /// limits`, the UI's Limits page).
    pub fn limits(&self) -> Vec<crate::limits::LimitInfo> {
        let l = &self.inner.services.limits;
        l.list()
            .into_iter()
            .map(|(name, spec)| crate::limits::LimitInfo {
                in_use: l.in_use(&name),
                waiting: l.waiting(&name),
                concurrent: spec.concurrent,
                per_second: spec.per_second,
                name,
            })
            .collect()
    }

    /// Creates a schedule (or updates its spec) that starts `spec.workflow`
    /// on its cron expression or interval.
    pub async fn schedule(&self, name: &str, spec: ScheduleSpec) -> Result<ScheduleHandle> {
        spec.validate_named(name)?;
        let id = schedule_id(name);
        let state = ScheduleState {
            name: name.to_owned(),
            spec: spec.clone(),
            ..Default::default()
        };
        self.start_raw(
            SCHEDULE_TYPE,
            &id,
            encode(&state)?,
            StartOptions::default().use_existing(),
        )
        .await?;
        let handle = ScheduleHandle {
            engine: self.clone(),
            id,
        };
        handle.send(Command::Update { spec }).await?;
        Ok(handle)
    }

    /// A handle to an existing schedule.
    pub fn schedule_handle(&self, name: &str) -> ScheduleHandle {
        ScheduleHandle {
            engine: self.clone(),
            id: schedule_id(name),
        }
    }

    /// Debounces a start of `workflow` under `key` with a JSON `input`
    /// (see [`crate::queue`]). Resolves once the start is durable, with the
    /// receipt of the run it joined. `event_id` (empty: none) makes a
    /// retried call join once.
    #[doc(hidden)]
    pub async fn debounce_raw(
        &self,
        workflow: &str,
        key: &str,
        event_id: &str,
        input: Payload,
        rule: &Debounce,
    ) -> Result<Receipt> {
        let event = Event {
            id: event_id.to_owned(),
            rule: Rule::Debounce(rule.clone()),
            input: decode(&input)?,
        };
        self.enqueue(workflow, key, event).await
    }

    /// Adds a JSON event to `workflow`'s batch under `key` (see
    /// [`crate::queue`]). Resolves once the event is durable, with the
    /// receipt of the run it joined.
    #[doc(hidden)]
    pub async fn batch_raw(
        &self,
        workflow: &str,
        key: &str,
        event_id: &str,
        input: Payload,
        rule: &Batch,
    ) -> Result<Receipt> {
        let event = Event {
            id: event_id.to_owned(),
            rule: Rule::Batch(rule.clone()),
            input: decode(&input)?,
        };
        self.enqueue(workflow, key, event).await
    }

    async fn enqueue(&self, workflow: &str, key: &str, event: Event) -> Result<Receipt> {
        event.rule.validate()?;
        let (wtype, coordinator, state, arg) = crate::queue::request(workflow, key, &event);
        let (state, arg) = (encode(&state)?, encode(&arg)?);
        // A coordinator that ended as the event arrived refused it before
        // its handler ran: send it to the next one.
        for _ in 0..100 {
            let (status, reply) = self
                .update_with_start_raw(
                    wtype,
                    &coordinator,
                    state.clone(),
                    StartOptions::default(),
                    EVENT,
                    &event.id,
                    arg.clone(),
                )
                .await?;
            match status {
                Status::Ok => return decode(&reply),
                Status::Failed if &*reply == ENDED => continue,
                s => return Err(status_error(s, &reply)),
            }
        }
        Err(Error::Rejected(format!(
            "coordinator '{coordinator}' kept ending before taking the event"
        )))
    }

    #[doc(hidden)]
    pub async fn signal_raw(&self, id: &str, name: &str, payload: Payload) -> Result<()> {
        let (id2, name) = (id.to_owned(), name.to_owned());
        let by = crate::audit::current_bytes();
        self.call(id, |reply| Cmd::Signal {
            id: id2,
            name,
            payload,
            dedup: 0,
            by,
            reply,
        })
        .await
    }

    #[doc(hidden)]
    pub async fn result_raw(&self, id: &str) -> Result<(Status, Payload)> {
        let id2 = id.to_owned();
        match self.call(id, |reply| Cmd::Result { id: id2, reply }).await {
            // A delayed start, or a run waiting for its turn: the run that
            // will start it gives its result (on the not-found path only).
            Err(Error::NotFound(m)) if !id.starts_with("fusee.") => {
                for waiting in [
                    crate::delay::coordinator_id(id),
                    crate::concurrency::slot_id(id),
                ] {
                    match self
                        .call(&waiting, |reply| Cmd::Result {
                            id: waiting.clone(),
                            reply,
                        })
                        .await
                    {
                        Err(Error::NotFound(_)) => continue,
                        r => return r,
                    }
                }
                Err(Error::NotFound(m))
            }
            r => r,
        }
    }

    /// At most `limit` runs of `wtype` run at once (per key, with
    /// [`StartOptions::concurrency_key`]); the others wait their turn, in
    /// the order they were started, as `fusee.queued/<id>` runs. The turns
    /// are durable: a run keeps its turn while parked and across crashes.
    /// See [`crate::concurrency`]. Set it before starting runs of the type,
    /// on every engine that starts them.
    pub fn limit_runs(&self, wtype: &str, limit: u32) {
        self.inner
            .run_limits
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(wtype.to_owned(), limit.max(1));
    }

    fn run_limit(&self, wtype: &str) -> Option<u32> {
        if wtype.starts_with("fusee.") {
            return None;
        }
        let limits = self
            .inner
            .run_limits
            .read()
            .unwrap_or_else(|e| e.into_inner());
        if limits.is_empty() {
            return None;
        }
        limits.get(wtype).copied()
    }

    /// [`start_raw`](Self::start_raw) and [`result_raw`](Self::result_raw)
    /// in one command to the shard and one reply, when the run finishes.
    /// The result is durable, so the start is.
    #[doc(hidden)]
    pub async fn run_raw(
        &self,
        wtype: &str,
        id: &str,
        input: Payload,
        opts: StartOptions,
    ) -> Result<(Status, Payload)> {
        if self.run_limit(wtype).is_some() {
            self.start_raw(wtype, id, input, opts).await?;
            return self.result_raw(id).await;
        }
        let deadline_ms = opts
            .timeout
            .map(|t| crate::ctx::after(self.wall_ms(), t))
            .unwrap_or(0);
        let mut req = StartRequest::new(id, wtype, input, opts.id_policy);
        req.version = opts.version;
        req.deadline_ms = deadline_ms;
        req.trace = trace_of(&opts);
        match self.call(id, |r| Cmd::Run(req, r.into())).await {
            Err(Error::AlreadyExists(_)) => Err(self.already_exists(id).await),
            r => r,
        }
    }

    /// [`run_raw`](Self::run_raw), with the result handed to `done` on the
    /// run's shard thread instead of a future to await: no task, channel
    /// or thread wake per run, for a caller that only records results.
    /// `done` must be short and must not block, since the shard's other
    /// runs wait for it. An error returned here means `done` is not
    /// called; an `AlreadyExists` reaches `done` without the holder's
    /// details that `run_raw` adds. Types with a run limit
    /// ([`limit_runs`](Self::limit_runs)) take `run_raw`.
    #[doc(hidden)]
    pub fn run_raw_then(
        &self,
        wtype: &str,
        id: &str,
        input: Payload,
        opts: StartOptions,
        done: impl FnOnce(Result<(Status, Payload)>) + Send + 'static,
    ) -> Result<()> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if self.run_limit(wtype).is_some() {
            return Err(Error::Rejected(format!(
                "`{wtype}` has a run limit: start it with run_raw"
            )));
        }
        let deadline_ms = opts
            .timeout
            .map(|t| crate::ctx::after(self.wall_ms(), t))
            .unwrap_or(0);
        let mut req = StartRequest::new(id, wtype, input, opts.id_policy);
        req.version = opts.version;
        req.deadline_ms = deadline_ms;
        req.trace = trace_of(&opts);
        let reply = ResultReply(Some(ReplyTo::Call(Box::new(done))));
        // A refused send drops the command unanswered: forget the function
        // first, so the error is the only answer.
        let cmd = Cmd::Run(req, reply);
        let shard = self.shard_for(id);
        shard.send_or_return(cmd).map_err(|(e, cmd)| {
            if let Cmd::Run(_, mut reply) = *cmd {
                reply.0 = None;
            }
            e
        })
    }

    pub async fn status(&self, id: &str) -> Result<Option<RunSummary>> {
        let id2 = id.to_owned();
        self.call(id, |reply| Cmd::Summary { id: id2, reply }).await
    }

    /// A run's summary and journal, if it exists.
    pub async fn describe(&self, id: &str) -> Result<Option<RunDetail>> {
        let id2 = id.to_owned();
        self.call(id, |reply| Cmd::Inspect { id: id2, reply }).await
    }

    /// The newest `limit` runs of each shard, each shard's newest first.
    pub async fn list(&self, limit: usize) -> Result<Vec<RunSummary>> {
        let mut out = Vec::new();
        for s in &self.inner.shards {
            let (tx, rx) = oneshot::channel();
            s.send(Cmd::List { limit, reply: tx })?;
            out.extend(rx.await.map_err(|_| Error::Closed)??);
        }
        Ok(out)
    }

    /// Per-shard metrics in shard-number order.
    pub async fn shard_metrics(&self) -> Result<Vec<Metrics>> {
        let mut metrics = Vec::with_capacity(self.inner.shards.len());
        for shard in &self.inner.shards {
            let (tx, rx) = oneshot::channel();
            shard.send(Cmd::Metrics(tx))?;
            metrics.push(rx.await.map_err(|_| Error::Closed)??);
        }
        Ok(metrics)
    }

    /// Counters summed over shards.
    pub async fn metrics(&self) -> Result<Metrics> {
        let mut total = Metrics::default();
        for m in self.shard_metrics().await? {
            total.records += m.records;
            total.log_bytes += m.log_bytes;
            total.flushes += m.flushes;
            total.inline_flushes += m.inline_flushes;
            total.flushed_records += m.flushed_records;
            total.flush_us_total += m.flush_us_total;
            total.steps_run += m.steps_run;
            total.steps_replayed += m.steps_replayed;
            total.runs_started += m.runs_started;
            total.runs_finished += m.runs_finished;
            total.runs_parked += m.runs_parked;
            total.parked_entries += m.parked_entries;
            total.runs_loaded += m.runs_loaded;
            total.checkpoints += m.checkpoints;
            total.index_bases_built += m.index_bases_built;
            total.determinism_errors += m.determinism_errors;
            total.busy += m.busy;
            total.gate_waits += m.gate_waits;
            total.gate_wait_us_total += m.gate_wait_us_total;
            total.stalls += m.stalls;
            total.max_tick_us = total.max_tick_us.max(m.max_tick_us);
            total.max_flush_us = total.max_flush_us.max(m.max_flush_us);
            total.volatile_timers += m.volatile_timers;
            total.log_writes += m.log_writes;
            total.log_device_bytes += m.log_device_bytes;
            total.log_rolls += m.log_rolls;
            total.log_owner = total.log_owner.max(m.log_owner);
            total.run_table_bytes += m.run_table_bytes;
            total.loaded_bytes += m.loaded_bytes;
            total.open_store_frames += m.open_store_frames;
            total.open_store_bytes += m.open_store_bytes;
            total.open_us = total.open_us.max(m.open_us);
            total.budget_parks += m.budget_parks;
            total.object_compactions += m.object_compactions;
            total.journal_frames_read += m.journal_frames_read;
            total.journal_bytes_read += m.journal_bytes_read;
        }
        Ok(total)
    }

    /// Flushes and stops every shard (and the HTTP API it serves). Pending
    /// work resumes on next open. The same as [`shutdown`](Self::shutdown);
    /// `close` is the name in every SDK.
    pub async fn close(&self) -> Result<()> {
        self.shutdown().await
    }

    /// Flushes and stops every shard. Pending work resumes on next open.
    pub async fn shutdown(&self) -> Result<()> {
        let mut waits = Vec::new();
        if !self.inner.closed.swap(true, Ordering::AcqRel) {
            for s in &self.inner.shards {
                let (tx, rx) = oneshot::channel();
                s.send_blocking(Cmd::Shutdown(tx));
                waits.push(rx);
            }
        }
        let mut result = Ok(());
        for w in waits {
            result = result.and(w.await.unwrap_or(Err(Error::Closed)));
        }
        for s in &self.inner.shards {
            let mut slot = s.thread.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(thread) = slot.take()
                && thread.join().is_err()
            {
                result = result.and(Err(Error::Io("shard thread panicked".into())));
            }
        }
        // The shared log closes with the last shard; its backup then makes
        // a last upload.
        if let Some(backup) = self.backup() {
            backup.stopped().await;
        }
        result
    }
}

/// A schedule, addressed by name.
#[derive(Clone)]
pub struct ScheduleHandle {
    engine: Engine,
    id: String,
}

impl ScheduleHandle {
    async fn send(&self, cmd: Command) -> Result<()> {
        self.engine
            .signal_raw(&self.id, CONTROL, encode(&cmd)?)
            .await
    }

    pub async fn pause(&self) -> Result<()> {
        self.send(Command::Pause).await
    }

    pub async fn resume(&self) -> Result<()> {
        self.send(Command::Resume).await
    }

    /// Starts one run now.
    pub async fn trigger(&self) -> Result<()> {
        self.send(Command::Trigger).await
    }

    /// Starts a run for every scheduled time in `[from_ms, to_ms]`.
    pub async fn backfill(&self, from_ms: u64, to_ms: u64) -> Result<()> {
        self.send(Command::Backfill { from_ms, to_ms }).await
    }

    pub async fn update(&self, spec: ScheduleSpec) -> Result<()> {
        spec.validate()?;
        self.send(Command::Update { spec }).await
    }

    /// The schedule's state: spec, next time, running run, counters.
    pub async fn describe(&self) -> Result<serde_json::Value> {
        let p = self
            .engine
            .query_raw(&self.id, "describe", Arc::from(&b"null"[..]))
            .await?;
        decode(&p)
    }

    /// Stops the schedule. Runs it started keep running.
    pub async fn delete(&self) -> Result<()> {
        self.engine
            .run::<serde_json::Value>(&self.id)
            .terminate("schedule deleted")
            .await
    }
}

/// Options of a start (see [`Engine::start`]).
impl<M: Make<StartOptions>> Op<StartOptions, M> {
    /// What happens when the workflow ID is taken.
    pub fn id_policy(mut self, policy: IdPolicy) -> Self {
        self.options_mut().id_policy = policy;
        self
    }

    /// Attaches to a run of this ID that is still going (a client that
    /// retries a start should).
    pub fn use_existing(mut self) -> Self {
        self.options_mut().id_policy.running = OnRunning::UseExisting;
        self
    }

    /// The run's deadline from now.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.options_mut().timeout = Some(timeout);
        self
    }

    /// The version to run (default: the newest registered).
    pub fn version(mut self, version: u32) -> Self {
        self.options_mut().version = Some(version);
        self
    }

    /// Starts the run after `delay` instead of now: a durable timer, then
    /// the start ([`crate::delay`]). `result()` waits through the delay.
    pub fn delay(mut self, delay: Duration) -> Self {
        self.options_mut().at_ms = Some(crate::ctx::after(crate::local::unix_ms(), delay));
        self
    }

    /// Starts the run at `at` instead of now ([`delay`](Self::delay)).
    pub fn at(mut self, at: std::time::SystemTime) -> Self {
        let ms = at
            .duration_since(std::time::UNIX_EPOCH)
            .map(crate::ctx::millis)
            .unwrap_or(0);
        self.options_mut().at_ms = Some(ms);
        self
    }

    /// The W3C `traceparent` the run joins ([`StartOptions::trace`]).
    pub fn trace(mut self, traceparent: impl Into<String>) -> Self {
        self.options_mut().trace = Some(traceparent.into());
        self
    }

    /// The key a limit on runs at once counts by ([`StartOptions::concurrency_key`]).
    pub fn concurrency_key(mut self, key: impl Into<String>) -> Self {
        self.options_mut().concurrency_key = Some(key.into());
        self
    }
}

/// The options of a debounced start or batched event.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EventOptions {
    /// A retried call with the same event ID joins (or adds the event) once.
    pub event_id: String,
}

impl<M: Make<EventOptions>> Op<EventOptions, M> {
    pub fn event_id(mut self, id: &str) -> Self {
        self.options_mut().event_id = id.to_owned();
        self
    }
}

/// The options of an update.
#[derive(Clone, Debug, Default)]
pub struct UpdateOptions {
    /// A retried update with the same ID gets the recorded result instead
    /// of running the handler twice.
    pub id: String,
    /// For update-with-start: how the run starts if it must.
    pub start: StartOptions,
}

impl<M: Make<UpdateOptions>> Op<UpdateOptions, M> {
    pub fn id(mut self, id: &str) -> Self {
        self.options_mut().id = id.to_owned();
        self
    }

    /// For update-with-start: the start's options.
    pub fn start_options(mut self, start: StartOptions) -> Self {
        self.options_mut().start = start;
        self
    }
}

impl Engine {
    /// Registers the code of a workflow type (at the reference's version,
    /// else version 1) and returns its typed reference:
    ///
    /// ```ignore
    /// pub const CHECKOUT: WorkflowRef<Order, String> = WorkflowRef::new("checkout");
    /// engine.register(CHECKOUT, checkout);
    /// let run = engine.start(CHECKOUT, "order-123", &order).await?;
    /// ```
    ///
    /// Runs finish on the version they started with; new starts use the
    /// newest.
    ///
    /// # Panics
    ///
    /// If this version of the type is already registered on this engine: two
    /// registrations would leave which code runs to chance. The message
    /// names both places.
    #[track_caller]
    pub fn register<I, O, F, Fut>(&self, workflow: impl AsWorkflow<I, O>, f: F) -> WorkflowRef<I, O>
    where
        I: Serialize + DeserializeOwned + 'static,
        O: Serialize + DeserializeOwned + 'static,
        F: Fn(Ctx, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + 'static,
    {
        let name = workflow.name().to_owned();
        let version = workflow.version().unwrap_or(1);
        self.claim(&name, version, std::panic::Location::caller());
        let erased = erase(f);
        if let Ok(mut r) = self.inner.registry.write() {
            r.register(&name, version, erased);
        }
        for s in &self.inner.shards {
            s.send_blocking(Cmd::Registered(name.clone()));
        }
        WorkflowRef::named(name)
    }

    /// Starts a run; resolves once the start is durable, with its handle.
    /// Options by builder: `.id_policy(p)` (or `.use_existing()`),
    /// `.timeout(d)`, `.version(v)`. Starting an ID that is running fails
    /// with [`Error::AlreadyExists`], naming the run.
    pub fn start<'a, I, O>(
        &'a self,
        workflow: impl AsWorkflow<I, O> + 'a,
        id: &'a str,
        input: &'a I,
    ) -> Op<
        StartOptions,
        impl Make<
            StartOptions,
            Output = Result<RunHandle<O>>,
            Fut = impl Future<Output = Result<RunHandle<O>>> + 'a,
        > + 'a,
    >
    where
        I: Serialize + 'a,
        O: DeserializeOwned + 'a,
    {
        Op::new(
            StartOptions::default(),
            move |mut opts: StartOptions| async move {
                if opts.version.is_none() {
                    opts.version = workflow.version();
                }
                if let Some(at_ms) = opts.at_ms.take() {
                    self.start_delayed(workflow.name(), id, encode(input)?, at_ms, opts)
                        .await?;
                    return Ok(self.run(id));
                }
                self.start_raw(workflow.name(), id, encode(input)?, opts)
                    .await?;
                Ok(RunHandle {
                    engine: self.clone(),
                    id: id.to_owned(),
                    _t: PhantomData,
                })
            },
        )
    }

    /// Signals the run `id`, starting it first if it is not running (with
    /// `input`): the signal is recorded before the new run's code first
    /// runs. Returns the run.
    ///
    /// ```ignore
    /// engine.signal_with_start(CART, "cart-7", &Cart::default(), ADD, &item).await?;
    /// ```
    pub fn signal_with_start<'a, I, O, T>(
        &'a self,
        workflow: impl AsWorkflow<I, O> + 'a,
        id: &'a str,
        input: &'a I,
        signal: impl AsSignal<T> + 'a,
        value: &'a T,
    ) -> Op<
        StartOptions,
        impl Make<
            StartOptions,
            Output = Result<RunHandle<O>>,
            Fut = impl Future<Output = Result<RunHandle<O>>> + 'a,
        > + 'a,
    >
    where
        I: Serialize + 'a,
        O: DeserializeOwned + 'a,
        T: Serialize + 'a,
    {
        Op::new(
            StartOptions::default(),
            move |mut opts: StartOptions| async move {
                if opts.version.is_none() {
                    opts.version = workflow.version();
                }
                self.signal_with_start_raw(
                    workflow.name(),
                    id,
                    encode(input)?,
                    opts,
                    signal.name(),
                    encode(value)?,
                )
                .await?;
                Ok(self.run(id))
            },
        )
    }

    /// Starts a run and waits for its result, in one call to the shard
    /// instead of [`start`](Self::start) then [`RunHandle::result`].
    pub fn execute<'a, I, O>(
        &'a self,
        workflow: impl AsWorkflow<I, O> + 'a,
        id: &'a str,
        input: &'a I,
    ) -> Op<
        StartOptions,
        impl Make<StartOptions, Output = Result<O>, Fut = impl Future<Output = Result<O>> + 'a> + 'a,
    >
    where
        I: Serialize + 'a,
        O: DeserializeOwned + 'a,
    {
        Op::new(
            StartOptions::default(),
            move |mut opts: StartOptions| async move {
                if opts.version.is_none() {
                    opts.version = workflow.version();
                }
                let (status, payload) = self
                    .run_raw(workflow.name(), id, encode(input)?, opts)
                    .await?;
                match status {
                    Status::Ok => decode(&payload),
                    s => Err(status_error(s, &payload)),
                }
            },
        )
    }

    /// [`execute`](Self::execute), with the result handed to `done` on the
    /// run's shard thread instead of a future: for callers that record
    /// results and need no task or wake per run. `done` must be short and
    /// must not block. An error returned here means `done` is not called.
    /// See [`run_raw_then`](Self::run_raw_then).
    pub fn execute_then<I, O>(
        &self,
        workflow: impl AsWorkflow<I, O>,
        id: &str,
        input: &I,
        done: impl FnOnce(Result<O>) + Send + 'static,
    ) -> Result<()>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let opts = StartOptions {
            version: workflow.version(),
            ..StartOptions::default()
        };
        self.run_raw_then(workflow.name(), id, encode(input)?, opts, move |r| {
            done(r.and_then(|(status, payload)| match status {
                Status::Ok => decode(&payload),
                s => Err(status_error(s, &payload)),
            }))
        })
    }

    /// Debounces a start under `key`: starts inside the window collapse
    /// into one run, which begins with the latest input when the window
    /// closes (see [`crate::queue`]). Resolves once this start is durable,
    /// with the handle of that run; the run exists once the window closes
    /// ([`RunHandle::started`]). `.event_id(id)` makes a retried call join
    /// once.
    pub fn debounce<'a, I, O>(
        &'a self,
        workflow: impl AsWorkflow<I, O> + 'a,
        key: &'a str,
        input: &'a I,
        rule: &'a Debounce,
    ) -> Op<
        EventOptions,
        impl Make<
            EventOptions,
            Output = Result<RunHandle<O>>,
            Fut = impl Future<Output = Result<RunHandle<O>>> + 'a,
        > + 'a,
    >
    where
        I: Serialize + 'a,
        O: DeserializeOwned + 'a,
    {
        Op::new(EventOptions::default(), move |o: EventOptions| async move {
            let r = self
                .debounce_raw(workflow.name(), key, &o.event_id, encode(input)?, rule)
                .await?;
            Ok(self.run(&r.id))
        })
    }

    /// Adds an event to the batch under `key`: once the batch holds
    /// `max_events` events or `max_wait` has passed since its first, one
    /// run starts with all of them as its input (see [`crate::queue`]).
    /// Resolves once the event is durable, with the handle of that run.
    pub fn batch<'a, E, O>(
        &'a self,
        workflow: impl AsWorkflow<Vec<E>, O> + 'a,
        key: &'a str,
        event: &'a E,
        rule: &'a Batch,
    ) -> Op<
        EventOptions,
        impl Make<
            EventOptions,
            Output = Result<RunHandle<O>>,
            Fut = impl Future<Output = Result<RunHandle<O>>> + 'a,
        > + 'a,
    >
    where
        E: Serialize + 'a,
        O: DeserializeOwned + 'a,
    {
        Op::new(EventOptions::default(), move |o: EventOptions| async move {
            let r = self
                .batch_raw(workflow.name(), key, &o.event_id, encode(event)?, rule)
                .await?;
            Ok(self.run(&r.id))
        })
    }

    /// Starts the workflow if it is not running and delivers an update in
    /// the same record; returns the run and the update's result.
    pub fn update_with_start<'a, I, O, A, R>(
        &'a self,
        workflow: impl AsWorkflow<I, O> + 'a,
        id: &'a str,
        input: &'a I,
        update: impl AsUpdate<A, R> + 'a,
        arg: &'a A,
    ) -> Op<
        UpdateOptions,
        impl Make<
            UpdateOptions,
            Output = Result<(RunHandle<O>, R)>,
            Fut = impl Future<Output = Result<(RunHandle<O>, R)>> + 'a,
        > + 'a,
    >
    where
        I: Serialize + 'a,
        O: DeserializeOwned + 'a,
        A: Serialize + 'a,
        R: DeserializeOwned + 'a,
    {
        Op::new(
            UpdateOptions::default(),
            move |mut o: UpdateOptions| async move {
                if o.start.version.is_none() {
                    o.start.version = workflow.version();
                }
                let (status, payload) = self
                    .update_with_start_raw(
                        workflow.name(),
                        id,
                        encode(input)?,
                        o.start,
                        update.name(),
                        &o.id,
                        encode(arg)?,
                    )
                    .await?;
                let r = match status {
                    Status::Ok => decode(&payload)?,
                    s => return Err(status_error(s, &payload)),
                };
                Ok((self.run(id), r))
            },
        )
    }
}

/// A run, addressed by workflow ID.
pub struct RunHandle<O> {
    engine: Engine,
    id: String,
    _t: PhantomData<fn() -> O>,
}

impl<O> std::fmt::Debug for RunHandle<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunHandle").field("id", &self.id).finish()
    }
}

impl<O> Clone for RunHandle<O> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            id: self.id.clone(),
            _t: PhantomData,
        }
    }
}

impl<O: DeserializeOwned> RunHandle<O> {
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Waits until a run with this ID exists. A debounced or batched run
    /// starts only when its window closes; until then `status` is `None`
    /// and `result` fails with `NotFound`.
    pub async fn started(&self) -> Result<()> {
        let mut pause = Duration::from_millis(1);
        while self.engine.status(&self.id).await?.is_none() {
            tokio::time::sleep(pause).await;
            pause = (pause * 2).min(Duration::from_millis(50));
        }
        Ok(())
    }

    /// Sends a signal: `run.signal(REVIEW, &true)`. Resolves once it is
    /// durable.
    pub async fn signal<T: Serialize>(&self, signal: impl AsSignal<T>, value: &T) -> Result<()> {
        self.engine
            .signal_raw(&self.id, signal.name(), encode(value)?)
            .await
    }

    /// Waits for the run's result.
    pub async fn result(&self) -> Result<O> {
        let (status, payload) = self.engine.result_raw(&self.id).await?;
        match status {
            Status::Ok => decode(&payload),
            s => Err(status_error(s, &payload)),
        }
    }

    /// Requests cancellation: every operation the code is waiting on fails
    /// with `Error::Cancelled`, and the code may run compensation.
    pub async fn cancel(&self, reason: &str) -> Result<()> {
        let (id, reason) = (self.id.clone(), reason.to_owned());
        self.engine
            .call(&self.id, |reply| Cmd::Cancel {
                id,
                reason,
                by: crate::audit::current_bytes(),
                reply,
            })
            .await
    }

    /// Resumes a run that paused because a step's retries ran out
    /// ([`crate::Exhausted::Pause`]): the step runs again at once, with a
    /// fresh budget, on the code this process has now. [`Error::Rejected`]
    /// if nothing is paused.
    pub async fn resume(&self) -> Result<()> {
        let id = self.id.clone();
        self.engine
            .call(&self.id, |reply| Cmd::Resume {
                id,
                by: crate::audit::current_bytes(),
                reply,
            })
            .await
    }

    /// Ends the run at once; its code stops.
    pub async fn terminate(&self, reason: &str) -> Result<()> {
        let (id, reason) = (self.id.clone(), reason.to_owned());
        self.engine
            .call(&self.id, |reply| Cmd::Terminate {
                id,
                reason,
                by: crate::audit::current_bytes(),
                reply,
            })
            .await
    }

    /// The run's summary; [`Error::NotFound`] if there is none.
    pub async fn status(&self) -> Result<RunSummary> {
        self.engine
            .status(&self.id)
            .await?
            .ok_or_else(|| Error::NotFound(self.id.clone()))
    }

    /// Calls a query handler: `run.query(STATUS, &()).await?`. Its answer
    /// waits until the state it read is durable.
    pub async fn query<A: Serialize, R: DeserializeOwned>(
        &self,
        query: impl AsQuery<A, R>,
        arg: &A,
    ) -> Result<R> {
        let p = self
            .engine
            .query_raw(&self.id, query.name(), encode(arg)?)
            .await?;
        decode(&p)
    }

    /// Sends an update and waits for its handler's durable result;
    /// `.id(id)` makes a retried call get the recorded result instead of
    /// running the handler twice.
    pub fn update<'a, A: Serialize + 'a, R: DeserializeOwned + 'a>(
        &'a self,
        update: impl AsUpdate<A, R> + 'a,
        arg: &'a A,
    ) -> Op<
        UpdateOptions,
        impl Make<UpdateOptions, Output = Result<R>, Fut = impl Future<Output = Result<R>> + 'a> + 'a,
    > {
        Op::new(
            UpdateOptions::default(),
            move |o: UpdateOptions| async move {
                let (status, payload) = self
                    .engine
                    .update_raw(&self.id, update.name(), &o.id, encode(arg)?)
                    .await?;
                match status {
                    Status::Ok => decode(&payload),
                    s => Err(status_error(s, &payload)),
                }
            },
        )
    }

    /// Starts a new run whose journal is this one's cut before `before_step`.
    pub async fn reset(&self, before_step: &str) -> Result<()> {
        self.engine.reset(&self.id, before_step, true).await
    }

    /// Runs a failed run again from its failed step: a reset before it (the
    /// steps before keep their results).
    pub async fn retry(&self) -> Result<()> {
        let detail = self.describe().await?;
        let failed = detail.journal.iter().rev().find_map(|r| match r {
            fusee_core::record::Record::Step { name, status, .. } if *status == Status::Failed => {
                Some(name.to_string())
            }
            _ => None,
        });
        match failed {
            Some(step) => self.reset(&step).await,
            None => Err(Error::Rejected(format!(
                "workflow '{}' has no failed step to retry (status {})",
                self.id, detail.summary.status
            ))),
        }
    }

    /// The run's summary and journal.
    pub async fn describe(&self) -> Result<RunDetail> {
        self.engine
            .describe(&self.id)
            .await?
            .ok_or_else(|| Error::NotFound(self.id.clone()))
    }

    /// A reader of the run's stream `name`, from its first chunk
    /// ([`StreamReader::from`](crate::StreamReader::from) picks another
    /// offset). It yields each chunk once it is durable, and ends once the
    /// stream is complete.
    pub fn stream(&self, name: &str) -> crate::stream::StreamReader {
        crate::stream::StreamReader::new(self.engine.clone(), &self.id, name)
    }
}

enum Pending {
    Unit(Reply<()>),
    Start(Reply<u64>),
    Value(Reply<(Status, Payload)>),
}

impl Pending {
    fn refuse(self, error: Error) {
        match self {
            Self::Unit(r) => {
                let _ = r.send(Err(error));
            }
            Self::Start(r) => {
                let _ = r.send(Err(error));
            }
            Self::Value(r) => {
                let _ = r.send(Err(error));
            }
        }
    }
}

/// Callers waiting for a run's result, with the workflow ID they asked for
/// (a run that continues as new hands them to its successor).
type Waiters = FxHashMap<u64, Vec<(String, ResultReply)>>;

#[allow(clippy::too_many_arguments)]
fn shard_thread(
    shard: u32,
    shards: u32,
    config: Config,
    registry: SharedRegistry,
    services: Services,
    rx: (Inbox, Receiver<Cmd>),
    wake: Arc<tokio::sync::Notify>,
    peers: Arc<Peers>,
    queued: Arc<std::sync::atomic::AtomicUsize>,
    ready: oneshot::Sender<Result<()>>,
    wal: Option<Arc<crate::wal::SharedWal>>,
) {
    shard_nice();
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = ready.send(Err(e.into()));
            return;
        }
    };
    // Raised with every wake from the runtime (a flush completed, a run
    // was woken from another thread), so the idle shard can spin on it.
    let kicked = Arc::new(AtomicBool::new(false));
    let notify: Notify = {
        let w = wake.clone();
        let k = kicked.clone();
        Arc::new(move || {
            k.store(true, Ordering::Release);
            w.notify_one();
        })
    };
    let rcfg = runtime_config(&config, shard, shards);
    crate::ctx::CALLBACK_BASE.with(|b| *b.borrow_mut() = Some(config.callback_url.clone()));
    #[cfg(feature = "testing")]
    crate::testing::install(config.test_hooks.clone());
    match config.durability.clone() {
        Durability::Memory => {
            #[allow(unused_mut)]
            let mut env = MemoryEnv::default();
            #[cfg(feature = "testing")]
            {
                env.offset = config.test_hooks.as_ref().map(|h| h.clock());
            }
            let runtime = ShardRuntime::open_with(env, rcfg, registry, false, services, notify);
            run_shard(
                &rt,
                runtime,
                rx,
                (wake, kicked),
                peers,
                queued,
                ready,
                shards,
                shard,
            );
        }
        Durability::Custom(factory) => {
            let mut env = match factory(shard) {
                Ok(e) => e,
                Err(e) => {
                    let _ = ready.send(Err(e.into()));
                    return;
                }
            };
            // Server mode: another worker holds this shard. The engine opens
            // without it; calls to it wait until this worker takes it over.
            let (ready, held) = if env.standby() {
                let _ = ready.send(Ok(()));
                match standby(&mut env, &rx, &queued) {
                    Some(held) => (oneshot::channel().0, held),
                    None => return,
                }
            } else {
                (ready, Vec::new())
            };
            let runtime = ShardRuntime::open_with(env, rcfg, registry, false, services, notify);
            run_shard_with(
                &rt,
                runtime,
                rx,
                (wake, kicked),
                peers,
                queued,
                ready,
                shard,
                held,
            );
        }
        Durability::Local if wal.is_some() => {
            let dir = config.dir.join(format!("shard-{shard:03}"));
            let env =
                match wal.map(|w| crate::wal::WalEnv::open(&dir, shard, config.local.clone(), w)) {
                    Some(Ok(e)) => e,
                    Some(Err(e)) => {
                        let _ = ready.send(Err(e.into()));
                        return;
                    }
                    None => unreachable!(),
                };
            let runtime = ShardRuntime::open_with(env, rcfg, registry, false, services, notify);
            run_shard(
                &rt,
                runtime,
                rx,
                (wake, kicked),
                peers,
                queued,
                ready,
                shards,
                shard,
            );
        }
        Durability::Local => {
            let dir = config.dir.join(format!("shard-{shard:03}"));
            let env = match LocalEnv::open(&dir, config.local.clone()) {
                Ok(e) => e,
                Err(e) => {
                    let _ = ready.send(Err(e.into()));
                    return;
                }
            };
            let runtime = ShardRuntime::open_with(env, rcfg, registry, false, services, notify);
            run_shard(
                &rt,
                runtime,
                rx,
                (wake, kicked),
                peers,
                queued,
                ready,
                shards,
                shard,
            );
        }
    }
}

/// The longest a busy shard goes without visiting its thread's reactor
/// (tokio's I/O and timers) while no step function waits on it.
const REACTOR_EVERY: Duration = Duration::from_millis(1);

/// The longest an idle shard spins for its next work before it parks; the
/// budget follows how soon work came after the last idle tick
/// ([`crate::spin`]).
const IDLE_SPIN_CAP: Duration = Duration::from_micros(50);

/// An idle shard spins only while it has at most this many loaded runs (a
/// lone workflow, perhaps with one finishing): under concurrency the spin
/// costs a core per shard and buys little.
const SPIN_MAX_LOADED: usize = 2;

/// `FUSEE_SHARD_NICE=n` (opt-in, Linux): the shard thread
/// runs at niceness `n` (1–19), below the log's owner and I/O threads, so
/// that on a host whose shards keep every core busy the thread every write
/// passes through is not the one left waiting for a core.
fn shard_nice() {
    #[cfg(target_os = "linux")]
    if let Some(n) = std::env::var("FUSEE_SHARD_NICE")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .filter(|n| (1..=19).contains(n))
    {
        // SAFETY: plain system calls on the calling thread's own id.
        unsafe {
            let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
            libc::setpriority(libc::PRIO_PROCESS, tid, n);
        }
    }
}

/// `FUSEE_SHARD_SPIN=busy` (opt-in): an idle shard spins
/// for its next work (up to [`IDLE_SPIN_CAP`]) however many runs it holds
/// and however many shards are busy, instead of parking at once under
/// load. `spin::spare_core` counts every shard that worked in the last
/// 2 ms, which at 1.64 M steps/s is all eight while the process uses about
/// three cores, so it is not asked.
fn shard_spin_busy() -> bool {
    static BUSY: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *BUSY.get_or_init(|| std::env::var("FUSEE_SHARD_SPIN").is_ok_and(|v| v == "busy"))
}

/// Takes the permit a wake left while the shard was not waiting, so the
/// next wait does not return at once.
fn take_permit(wake: &tokio::sync::Notify) {
    let notified = std::pin::pin!(wake.notified());
    let _ = notified.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()));
}

/// Waits for a server-mode shard's lease, keeping the calls that arrive
/// meanwhile for the shard to handle once it runs. `None`: the engine shut
/// down or the lease can never arrive; the shard thread ends.
fn standby(
    env: &mut Box<dyn crate::env::Env>,
    rx: &(Inbox, Receiver<Cmd>),
    queued: &std::sync::atomic::AtomicUsize,
) -> Option<Vec<Cmd>> {
    let mut held = Vec::new();
    loop {
        while let Ok(cmd) = rx.1.try_recv().or_else(|_| rx.0.try_recv()) {
            queued.fetch_sub(1, Ordering::AcqRel);
            if let Cmd::Shutdown(reply) = cmd {
                let _ = reply.send(Ok(()));
                return None;
            }
            held.push(cmd);
        }
        match env.wait_lease(Duration::from_millis(20)) {
            Ok(true) => return Some(held),
            Ok(false) => {}
            Err(_) => return None,
        }
    }
}

fn run_shard<E: crate::env::Env + 'static>(
    rt: &tokio::runtime::Runtime,
    runtime: Result<ShardRuntime<E>>,
    rx: (Inbox, Receiver<Cmd>),
    (wake, kicked): (Arc<tokio::sync::Notify>, Arc<AtomicBool>),
    peers: Arc<Peers>,
    queued: Arc<std::sync::atomic::AtomicUsize>,
    ready: oneshot::Sender<Result<()>>,
    shards: u32,
    shard: u32,
) {
    let _ = shards;
    run_shard_with(
        rt,
        runtime,
        rx,
        (wake, kicked),
        peers,
        queued,
        ready,
        shard,
        Vec::new(),
    );
}

/// [`run_shard`], first handling `held`: calls that arrived while the shard
/// waited for its lease.
fn run_shard_with<E: crate::env::Env + 'static>(
    rt: &tokio::runtime::Runtime,
    runtime: Result<ShardRuntime<E>>,
    rx: (Inbox, Receiver<Cmd>),
    (wake, kicked): (Arc<tokio::sync::Notify>, Arc<AtomicBool>),
    peers: Arc<Peers>,
    queued: Arc<std::sync::atomic::AtomicUsize>,
    ready: oneshot::Sender<Result<()>>,
    shard: u32,
    held: Vec<Cmd>,
) {
    let runtime = match runtime {
        Ok(r) => {
            let _ = ready.send(Ok(()));
            r
        }
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    let next_token = AtomicU64::new(1);
    runtime.watch_host_queue(queued.clone());
    // Server mode: messages to and from shards other processes hold go
    // through the server.
    let relay = runtime.with(|i| crate::env::Env::relay(&i.env));
    if let Some(r) = &relay {
        r.attach(
            shard,
            Box::new(RelayInbox {
                peers: Arc::downgrade(&peers),
                shard,
            }),
        );
    }
    rt.block_on(async move {
        let mut acks: FxHashMap<u64, Pending> = FxHashMap::default();
        let mut waiters: Waiters = FxHashMap::default();
        let mut shutdown: Option<Reply<()>> = None;
        let mut relayed = Relayed::default();
        // When the thread's reactor last ran.
        let mut reactor_at = Instant::now();
        let idle_spin = crate::spin::Budget::new(IDLE_SPIN_CAP);
        for cmd in held {
            handle(
                &runtime,
                cmd,
                &next_token,
                &mut acks,
                &mut waiters,
                &mut shutdown,
                &mut relayed,
            );
        }
        loop {
            kicked.store(false, Ordering::Relaxed);
            let mut handled = 0;
            // A shard that is behind takes only other shards' commands
            // until it catches up; callers' wait in its queue
            // ([`ShardRuntime::behind`]).
            let behind = runtime.behind();
            while let Ok(cmd) = if behind {
                rx.1.try_recv()
            } else {
                rx.1.try_recv().or_else(|_| rx.0.try_recv())
            } {
                queued.fetch_sub(1, Ordering::AcqRel);
                handled += 1;
                handle(
                    &runtime,
                    cmd,
                    &next_token,
                    &mut acks,
                    &mut waiters,
                    &mut shutdown,
                    &mut relayed,
                );
                if handled >= 1024 {
                    break;
                }
            }
            let tick = runtime.tick_with(&|| queued.load(Ordering::Acquire) > 0);
            for out in runtime.take_released() {
                route(&runtime, out, &mut acks, &mut waiters);
            }
            let mut remote: FxHashMap<u32, Vec<crate::runtime::Outgoing>> = FxHashMap::default();
            for o in runtime.take_outgoing() {
                match &relay {
                    Some(r) if !r.holds(o.target) => remote.entry(o.target).or_default().push(o),
                    _ => peers.send(
                        o.target,
                        Cmd::Message {
                            from: shard,
                            id: o.id,
                            msg: o.msg,
                        },
                    ),
                }
            }
            if let Some(r) = &relay {
                for (target, entries) in remote {
                    // Frames of about a mebibyte: a backed-up outbox never
                    // makes one larger than the wire allows.
                    let mut chunk = Vec::new();
                    let mut bytes = 0;
                    for o in entries {
                        let n = relayed_len(&o.msg);
                        if !chunk.is_empty() && bytes + n > RELAY_CHUNK {
                            r.send(shard, target, &crate::runtime::encode_outbox(&chunk));
                            chunk.clear();
                            bytes = 0;
                        }
                        bytes += n;
                        chunk.push(o);
                    }
                    if !chunk.is_empty() {
                        r.send(shard, target, &crate::runtime::encode_outbox(&chunk));
                    }
                }
            }
            let mut remote_acks = Vec::new();
            for (to, id) in runtime.take_message_acks() {
                if id >= RELAYED_BASE {
                    // A relayed message: the ack goes back the way it came.
                    remote_acks.extend(relayed.take(id));
                } else {
                    peers.send(to, Cmd::MessageAck(id));
                }
            }
            if let Some(r) = relay.as_ref().filter(|_| !remote_acks.is_empty()) {
                r.ack(shard, &remote_acks);
            }
            if let Some(error) = runtime.with(|i| i.failed().map(|s| Error::Io(s.to_owned()))) {
                for (_, pending) in acks.drain() {
                    pending.refuse(error.clone());
                }
                for (_, waiting) in waiters.drain() {
                    for (_, reply) in waiting {
                        reply.send(Err(error.clone()));
                    }
                }
            }
            if let Some(reply) = shutdown.take() {
                let r = runtime.close();
                let _ = reply.send(r);
                return;
            }
            // A shard that left callers' commands queued as behind looks
            // again rather than parking: its tick just recounted the round,
            // and the wake those commands sent is spent.
            if behind && queued.load(Ordering::Acquire) > 0 {
                continue;
            }
            if tick.busy || handled > 0 {
                crate::spin::mark_busy(shard);
                // Busy ticks run back to back: a visit to the reactor is a
                // system call. It follows every tick while a step function
                // is pending (its I/O or timers may need it), else at least
                // every REACTOR_EVERY.
                if runtime.waits_outside() || reactor_at.elapsed() >= REACTOR_EVERY {
                    tokio::task::yield_now().await;
                    reactor_at = Instant::now();
                }
                continue;
            }
            // The shard's own clock: the test kit's virtual time moves it.
            #[cfg(feature = "testing")]
            let now = runtime.with(|i| crate::env::Env::now_ms(&i.env));
            #[cfg(not(feature = "testing"))]
            let now = crate::local::unix_ms();
            if tick.next_wake_ms.is_some_and(|at| at <= now) {
                tokio::task::yield_now().await;
                reactor_at = Instant::now();
                continue;
            }
            // Idle: work that came soon after the last idle tick is likely
            // to come soon again. Spin for it first, then park; not while a
            // step function waits on the reactor, or once the reactor is due.
            let idle_at = Instant::now();
            // Spinning pays for a lone workflow waiting on its own flush; with
            // several runs loaded the next work is batched behind other runs'
            // anyway, and spinning would only burn a core per shard.
            let busy_spin = shard_spin_busy();
            let budget = if runtime.waits_outside()
                || idle_at - reactor_at >= REACTOR_EVERY
                || (!busy_spin
                    && (runtime.with(|i| i.core.loaded_len()) > SPIN_MAX_LOADED
                        || !crate::spin::few_busy()))
            {
                Duration::ZERO
            } else {
                idle_spin.get()
            };
            let arrived = crate::spin::spin_until(budget, || {
                queued.load(Ordering::Acquire) > 0 || kicked.load(Ordering::Acquire)
            });
            if arrived {
                take_permit(&wake);
            } else {
                match tick.next_wake_ms {
                    Some(at) => {
                        let d = Duration::from_millis(at.saturating_sub(now));
                        let _ = tokio::time::timeout(d, wake.notified()).await;
                    }
                    None => wake.notified().await,
                }
                reactor_at = Instant::now();
            }
            idle_spin.record(idle_at.elapsed());
        }
    });
}

/// IDs the runtime sees for relayed messages start here, above any outbox
/// ID of this process, so their acknowledgments are told apart.
const RELAYED_BASE: u64 = 1 << 62;
/// Relayed messages awaiting the runtime's acknowledgment, at most. The
/// oldest go first: a sender resends what stays unacknowledged, and a
/// resend gets an entry of its own.
const RELAYED_MAX: usize = 1 << 16;

/// Messages relayed to one shard go in frames of about this many bytes.
const RELAY_CHUNK: usize = 1 << 20;

/// About how many bytes `msg` takes in a relay frame.
fn relayed_len(msg: &Message) -> usize {
    64 + match msg {
        Message::ChildStart {
            wtype, id, input, ..
        } => wtype.len() + id.len() + input.len(),
        Message::ChildResult { payload, .. } => payload.len(),
        Message::Signal {
            target,
            name,
            payload,
            ..
        } => target.len() + name.len() + payload.len(),
        Message::Close {
            target,
            wtype,
            reason,
            ..
        } => target.len() + wtype.len() + reason.len(),
    }
}

/// Relayed messages the runtime has not acknowledged yet: the ID it knows
/// each by, to the sender's `(shard, lease epoch, outbox ID)`.
#[derive(Default)]
struct Relayed {
    next: u64,
    pending: std::collections::BTreeMap<u64, (u32, u64, u64)>,
}

impl Relayed {
    fn insert(&mut self, from: (u32, u64, u64)) -> u64 {
        let id = RELAYED_BASE + self.next;
        self.next += 1;
        self.pending.insert(id, from);
        while self.pending.len() > RELAYED_MAX {
            self.pending.pop_first();
        }
        id
    }

    fn take(&mut self, id: u64) -> Option<(u32, u64, u64)> {
        self.pending.remove(&id)
    }
}

/// Delivers what the relay brings for one shard to its thread.
struct RelayInbox {
    peers: std::sync::Weak<Peers>,
    shard: u32,
}

impl crate::env::RelayInbound for RelayInbox {
    fn messages(&self, from: u32, epoch: u64, entries: &[u8]) {
        let (Some(peers), Ok(entries)) =
            (self.peers.upgrade(), crate::runtime::decode_outbox(entries))
        else {
            return;
        };
        for e in entries {
            peers.send(
                self.shard,
                Cmd::Relayed {
                    from,
                    epoch,
                    id: e.id,
                    msg: e.msg,
                },
            );
        }
    }

    fn acked(&self, id: u64) {
        if let Some(peers) = self.peers.upgrade() {
            peers.send(self.shard, Cmd::MessageAck(id));
        }
    }
}

fn handle<E: crate::env::Env + 'static>(
    rt: &ShardRuntime<E>,
    cmd: Cmd,
    next_token: &AtomicU64,
    acks: &mut FxHashMap<u64, Pending>,
    waiters: &mut Waiters,
    shutdown: &mut Option<Reply<()>>,
    relayed: &mut Relayed,
) {
    let token = || next_token.fetch_add(1, Ordering::Relaxed);
    match cmd {
        Cmd::Start(mut req, reply) => {
            let t = token();
            req.token = Some(t);
            match rt.start(req) {
                Ok(_) => {
                    acks.insert(t, Pending::Start(reply));
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            }
        }
        Cmd::Run(req, reply) => {
            // No acknowledgment of the start: the result is released once
            // the run's records are durable.
            let id = req.id.clone();
            match rt.start(req) {
                Ok(_) => wait_result(rt, id, reply, waiters),
                Err(e) => {
                    reply.send(Err(e));
                }
            }
        }
        Cmd::StartUpdate(mut req, reply) => {
            let t = token();
            if let Some(u) = req.update.as_mut() {
                u.token = Some(t);
            }
            match rt.start(req) {
                Ok(_) => {
                    acks.insert(t, Pending::Value(reply));
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            }
        }
        Cmd::Update {
            id,
            name,
            update_id,
            payload,
            reply,
        } => {
            let t = token();
            acks.insert(t, Pending::Value(reply));
            if let Err(e) = rt.update(&id, &name, &update_id, payload, Some(t))
                && let Some(Pending::Value(r)) = acks.remove(&t)
            {
                let _ = r.send(Err(e));
            }
        }
        Cmd::Query {
            id,
            name,
            arg,
            reply,
        } => {
            let t = token();
            acks.insert(t, Pending::Value(reply));
            if let Err(e) = rt.query(&id, &name, arg, t)
                && let Some(Pending::Value(r)) = acks.remove(&t)
            {
                let _ = r.send(Err(e));
            }
        }
        Cmd::Complete {
            token: tok,
            status,
            payload,
            reply,
        } => {
            let t = token();
            acks.insert(t, Pending::Unit(reply));
            if let Err(e) = rt.complete(&tok, status, payload, Some(t))
                && let Some(Pending::Unit(r)) = acks.remove(&t)
            {
                let _ = r.send(Err(e));
            }
        }
        Cmd::Reset {
            id,
            before_step,
            reapply_signals,
            version,
            by,
            reply,
        } => {
            let t = token();
            match rt.with_by(by, || {
                rt.reset(&id, &before_step, reapply_signals, version, Some(t))
            }) {
                Ok(_) => {
                    acks.insert(t, Pending::Unit(reply));
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            }
        }
        Cmd::ListWhere {
            filter,
            limit,
            reply,
        } => {
            let _ = reply.send(Ok(rt.list_where(&filter, limit)));
        }
        Cmd::Signal {
            id,
            name,
            payload,
            dedup,
            by,
            reply,
        } => {
            let t = token();
            match rt.with_by(by, || rt.signal(&id, &name, payload, dedup, Some(t))) {
                Ok(()) => {
                    acks.insert(t, Pending::Unit(reply));
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            }
        }
        Cmd::Cancel {
            id,
            reason,
            by,
            reply,
        } => {
            let t = token();
            match rt.with_by(by, || rt.cancel(&id, &reason, Some(t))) {
                Ok(()) => {
                    acks.insert(t, Pending::Unit(reply));
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            }
        }
        Cmd::Resume { id, by, reply } => {
            let t = token();
            match rt.with_by(by, || rt.resume(&id, Some(t))) {
                Ok(()) => {
                    acks.insert(t, Pending::Unit(reply));
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            }
        }
        Cmd::Terminate {
            id,
            reason,
            by,
            reply,
        } => {
            let t = token();
            match rt.with_by(by, || rt.terminate(&id, &reason, Some(t))) {
                Ok(()) => {
                    acks.insert(t, Pending::Unit(reply));
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            }
        }
        Cmd::Result { id, reply } => wait_result(rt, id, reply.into(), waiters),
        #[cfg(feature = "testing")]
        Cmd::NextTimer(reply) => {
            let _ = reply.send(Ok((rt.waits_outside(), rt.next_timer_ms())));
        }
        #[cfg(feature = "testing")]
        Cmd::Import(records, reply) => {
            let _ = reply.send(rt.import(&records));
        }
        Cmd::RejectStuck(id) => {
            if let Some(run) = rt.find(&id)
                && let Some(reason) = rt.stuck_reason(run)
                && let Some(ws) = waiters.remove(&run)
            {
                for (_, w) in ws {
                    w.send(Err(Error::Determinism(reason.clone())));
                }
            }
        }
        Cmd::Summary { id, reply } => {
            let s = rt.find(&id).and_then(|r| rt.summary(r));
            let _ = reply.send(Ok(s));
        }
        Cmd::Inspect { id, reply } => {
            let _ = reply.send(rt.inspect(&id));
        }
        Cmd::StreamRead {
            id,
            run,
            name,
            from,
            wait_ms,
            reply,
        } => {
            let t = token();
            acks.insert(t, Pending::Value(reply));
            if let Err(e) = rt.stream_read(&id, run, &name, from, wait_ms, t)
                && let Some(Pending::Value(r)) = acks.remove(&t)
            {
                let _ = r.send(Err(e));
            }
        }
        Cmd::List { limit, reply } => {
            let _ = reply.send(Ok(rt.list(limit)));
        }
        Cmd::Registered(name) => rt.registered(&name),
        Cmd::Message { from, id, msg } => {
            // Errors leave the message unacknowledged; the sender resends.
            let _ = rt.message(msg, Some((from, id)));
        }
        Cmd::MessageAck(id) => rt.outbox_ack(id),
        Cmd::Relayed {
            from,
            epoch,
            id,
            msg,
        } => {
            let local = relayed.insert((from, epoch, id));
            if rt.message(msg, Some((from, local))).is_err() {
                relayed.take(local);
            }
        }
        Cmd::Metrics(reply) => {
            let _ = reply.send(Ok(rt.metrics()));
        }
        Cmd::Shutdown(reply) => *shutdown = Some(reply),
    }
}

fn wait_result<E: crate::env::Env + 'static>(
    rt: &ShardRuntime<E>,
    id: String,
    reply: ResultReply,
    waiters: &mut Waiters,
) {
    match rt.result(&id) {
        Ok(ResultState::Done {
            status, payload, ..
        }) => {
            reply.send(Ok((status, payload)));
        }
        Ok(ResultState::Running(run)) => waiters.entry(run).or_default().push((id, reply)),
        Ok(ResultState::NotFound) => {
            reply.send(Err(Error::NotFound(id)));
        }
        Err(e) => {
            reply.send(Err(e));
        }
    }
}

fn route<E: crate::env::Env + 'static>(
    rt: &ShardRuntime<E>,
    out: Output,
    acks: &mut FxHashMap<u64, Pending>,
    waiters: &mut Waiters,
) {
    match out {
        Output::Ack(t) => match acks.remove(&t) {
            Some(Pending::Unit(r)) => {
                let _ = r.send(Ok(()));
            }
            Some(Pending::Start(r)) => {
                let _ = r.send(Ok(t));
            }
            Some(Pending::Value(r)) => {
                let _ = r.send(Ok((Status::Ok, Arc::from(&b"null"[..]))));
            }
            None => {}
        },
        Output::Reject(t) => match acks.remove(&t) {
            Some(Pending::Unit(r)) => {
                let _ = r.send(Err(Error::Rejected(
                    "the workflow has already finished".into(),
                )));
            }
            Some(Pending::Start(r)) => {
                let _ = r.send(Err(Error::AlreadyExists("workflow ID in use".into())));
            }
            Some(Pending::Value(r)) => {
                let _ = r.send(Err(Error::AlreadyExists("workflow ID in use".into())));
            }
            None => {}
        },
        Output::Reply {
            token,
            status,
            payload,
        } => match acks.remove(&token) {
            Some(Pending::Value(r)) => {
                let _ = r.send(Ok((status, payload)));
            }
            Some(Pending::Unit(r)) => {
                let _ = r.send(Ok(()));
            }
            Some(Pending::Start(r)) => {
                let _ = r.send(Ok(token));
            }
            None => {}
        },
        Output::Refuse { token, message } => {
            let e = Error::Rejected(String::from_utf8_lossy(&message).into_owned());
            if let Some(pending) = acks.remove(&token) {
                pending.refuse(e);
            }
        }
        Output::Result {
            run,
            status,
            payload,
        } => {
            if let Some(ws) = waiters.remove(&run) {
                for (id, w) in ws {
                    if status.is_handover() {
                        // Follow the ID to the run that took over.
                        wait_result(rt, id, w, waiters);
                    } else {
                        w.send(Ok((status, payload.clone())));
                    }
                }
            }
        }
        // The runtime routes messages to other shards itself (outbox).
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_command_queue_is_bounded_by_count_and_allocated_as_used() {
        let held = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (tx, rx) = std::sync::mpsc::channel::<Cmd>();
        let (itx, _irx) = std::sync::mpsc::channel::<Cmd>();
        let inbox = Inbox {
            rx,
            held: held.clone(),
        };
        let handle = ShardHandle {
            queued: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            tx,
            held,
            cap: 3,
            itx,
            wake: Arc::new(tokio::sync::Notify::new()),
            thread: Mutex::new(None),
        };
        for _ in 0..3 {
            handle.send(Cmd::Registered("x".into())).unwrap();
        }
        assert!(matches!(
            handle.send(Cmd::Registered("x".into())),
            Err(Error::Busy)
        ));
        assert!(inbox.try_recv().is_ok());
        handle.send(Cmd::Registered("x".into())).unwrap();
        // Internal commands are never refused.
        handle.send_blocking(Cmd::Registered("x".into()));
        // A closed shard refuses with `Closed` while there is room.
        assert!(inbox.try_recv().is_ok());
        drop(inbox);
        assert!(matches!(
            handle.send(Cmd::Registered("x".into())),
            Err(Error::Closed)
        ));
    }
}
