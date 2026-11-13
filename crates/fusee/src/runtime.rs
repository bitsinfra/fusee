//! The shard runtime: one shard's log writer, core state, journal machines
//! and workflow code, driven through an [`Env`].
//!
//! The runtime is single-threaded and deterministic given its `Env`. The
//! embedded engine runs one per shard thread; the simulator runs many in one
//! thread; language bindings run one on their host language's event loop.
//!
//! ## The common path
//!
//! 1. Workflow code starts a step. The step may run once every earlier record
//!    of its run is durable (its **gate**). That is the only wait.
//! 2. The step function returns. Its result is queued for delivery; when the
//!    run's code is quiescent the runtime appends the `Step` record and hands
//!    the result to the code at once, without waiting for the disk.
//! 3. The code starts the next step, whose gate is that record.
//! 4. Whenever no flush is in flight and records are pending, the runtime
//!    flushes all of them: inline when it has nothing else to do, otherwise
//!    on the flusher thread while code keeps running. Batches grow with load
//!    and an idle shard flushes a single record immediately.
//! 5. Outputs (acknowledgments, results, messages to other shards) are
//!    released only when the records behind them are durable.

use crate::codec::Codec;
use crate::ctx::{Ctx, Handlers};
use crate::env::{Env, Notify};
use crate::error::Error;
use crate::limits::{Acquire, LimitBackend, LocalLimits, Priority};
use crate::object::{ObjectDef, ObjectDetail, Objects};
use fusee_core::record::{
    Lsn, Name, Origin, PAUSED, ParentRef, Payload, Record, RecordBuf, RecordRef, RunId, Seq,
    StartMeta, StartUpdate, Status, StepTime, decode_canonical_all, decode_canonical_into,
    encode_canonical, origin, policy,
};
use fusee_core::run::{
    Arm, AwaitStart, Completion, Next, Outcome, RunInfo, RunMachine, StepStart, StreamOpen, empty,
};
use fusee_core::segment::{LogWriter, Progress, ScanError, Scanned, SegmentScanner, Tail};
use fusee_core::shard::{BlobRef, CANCEL_SIGNAL, Event, Found, Mode, Output, ShardCore, TimerKind};
use fusee_core::store::{
    Checkpoint, CheckpointRun, EntryRef, MAX_CHAIN, StoreEntry, chain_journal, encode_entry,
    encode_entry_into, read_entry,
};
use fusee_core::wire::hash_id;
use rustc_hash::FxHashMap;
use std::cell::{Cell, RefCell};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll, Wake, Waker};

mod objects;

/// What workflow code returns: the result payload, or a status and message.
pub type RunOutcome = Result<Payload, (Status, Payload)>;
pub type LocalFuture = Pin<Box<dyn Future<Output = RunOutcome>>>;
/// Type-erased workflow code: `ctx -> future`. The code reads its input
/// from the context ([`Ctx::input_bytes`]): handing it a copy of the
/// payload cost a reference count increment and decrement on every start
/// and wake.
pub type WorkflowFn = Arc<dyn Fn(Ctx) -> LocalFuture + Send + Sync>;

/// Registered workflow code, shared by every shard of an engine.
#[derive(Default)]
pub struct Registry {
    types: FxHashMap<String, BTreeMap<u32, WorkflowFn>>,
    /// Object types (see [`crate::object`]): which handlers are shared.
    objects: FxHashMap<String, Arc<ObjectDef>>,
}

impl Registry {
    /// Marks `name` as an object type with these handlers. A start of the
    /// type then creates a key's run, and its calls take turns.
    pub fn register_object(&mut self, name: &str, def: ObjectDef) {
        self.objects.insert(name.to_owned(), Arc::new(def));
        REGISTRY_EPOCH.fetch_add(1, Ordering::Release);
    }

    pub fn object(&self, name: &str) -> Option<Arc<ObjectDef>> {
        self.objects.get(name).cloned()
    }

    pub fn register(&mut self, name: &str, version: u32, f: WorkflowFn) {
        self.types
            .entry(name.to_owned())
            .or_default()
            .insert(version, f);
        REGISTRY_EPOCH.fetch_add(1, Ordering::Release);
    }

    pub fn get(&self, name: &str, version: u32) -> Option<WorkflowFn> {
        self.types.get(name)?.get(&version).cloned()
    }

    pub fn latest(&self, name: &str) -> Option<u32> {
        self.types.get(name)?.keys().next_back().copied()
    }

    pub fn names(&self) -> Vec<String> {
        self.types.keys().cloned().collect()
    }

    /// The registered versions of `name`, oldest first.
    pub fn versions(&self, name: &str) -> Vec<u32> {
        self.types
            .get(name)
            .map(|v| v.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Whether `name` is an object type.
    pub fn is_object(&self, name: &str) -> bool {
        self.objects.contains_key(name)
    }
}

pub type SharedRegistry = Arc<RwLock<Registry>>;

/// Bumped by every change to any [`Registry`]: a shard keeps the code it
/// looked up until this moves ([`CodeCache`]).
static REGISTRY_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A shard's own copy of the code it has started, by type and version.
/// Looking each run's code up in the shared registry took its lock and
/// cloned two reference counts that every shard shares, on every start and
/// wake: eight shards waking at once contended on those cache lines.
#[derive(Default)]
struct CodeCache {
    epoch: u64,
    fns: FxHashMap<String, Vec<(u32, WorkflowFn)>>,
}

impl CodeCache {
    /// The code of `wtype` at `version`, from `registry` the first time.
    fn get(&mut self, registry: &SharedRegistry, wtype: &str, version: u32) -> Option<&WorkflowFn> {
        let epoch = REGISTRY_EPOCH.load(Ordering::Acquire);
        if self.epoch != epoch {
            self.fns.clear();
            self.epoch = epoch;
        }
        let known = self
            .fns
            .get(wtype)
            .is_some_and(|v| v.iter().any(|(ver, _)| *ver == version));
        if !known {
            let f = registry.read().ok()?.get(wtype, version)?;
            self.fns
                .entry(wtype.to_owned())
                .or_default()
                .push((version, f));
        }
        self.cached(wtype, version)
    }

    /// The code [`get`](Self::get) found, without looking at the registry.
    fn cached(&self, wtype: &str, version: u32) -> Option<&WorkflowFn> {
        self.fns
            .get(wtype)?
            .iter()
            .find(|(ver, _)| *ver == version)
            .map(|(_, f)| f)
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub shard: u32,
    pub shards: u32,
    /// How long finished runs keep their results.
    pub retention_ms: u64,
    /// Park a run (drop its code and journal from memory) once it has been
    /// suspended this long. `u64::MAX` disables parking.
    pub park_after_ms: u64,
    /// While loaded runs take more than this many bytes (an estimate: a
    /// fixed cost per run plus its journal in memory), the runs suspended
    /// longest are parked before `park_after_ms`, which stays the ceiling.
    /// 0: no budget.
    pub memory_budget: u64,
    /// Checkpoint at least this often while records are being written.
    pub checkpoint_interval_ms: u64,
    /// At most this many loaded runs per shard; starts beyond it are `Busy`.
    pub max_loaded: usize,
    pub segment_bytes: u64,
    /// Flush on the shard's own thread when it has nothing else to do and
    /// the last flush took at most `inline_max_us` (on a slow disk, blocking
    /// the shard thread would delay new work by a whole flush).
    pub inline_flush: bool,
    pub inline_max_us: u64,
    /// Deliberately broken behavior, used only to prove the simulator's
    /// checkers catch violations. Never set outside tests.
    /// Keep each finished run's whole journal in the run store (an archive
    /// of histories) instead of only its start and result.
    pub keep_history: bool,
    /// What a step does when its retries run out, unless its retry policy
    /// says.
    pub on_exhausted: crate::ctx::Exhausted,
    /// Archive complete terminal journals beyond hot retention.
    pub archive_history: bool,
    /// Record when each step began and how long it took (`STEP_TIMED`
    /// records): the dev profile only, as it reads the clock when a step
    /// begins and adds two varints to its record.
    pub step_times: bool,
    #[cfg(feature = "opentelemetry")]
    pub telemetry: Option<Arc<crate::telemetry::Telemetry>>,
    /// Replay testing: step functions never run and timers never fire, so
    /// the code can only go as far as its journal takes it.
    pub replay_only: bool,
    /// [`ShardRuntime::close`] checkpoints before returning, so the next
    /// open replays nothing.
    pub checkpoint_on_close: bool,
    /// Parked runs per frame of an index base written between checkpoints
    /// (see [`Inner::advance_index_build`]).
    pub index_chunk_records: usize,
    /// Delta frames on top of an index base after which a new base is
    /// written (at least as many as the base has frames).
    pub index_rebase_deltas: usize,
    #[doc(hidden)]
    pub sabotage: Sabotage,
}

/// Process-wide services a shard runtime uses.
#[derive(Clone, Debug)]
pub struct Services {
    /// Payload encryption (see [`crate::codec`]).
    pub codec: Option<Arc<dyn Codec>>,
    /// Where step limits live (see [`crate::limits`]).
    pub limits: Arc<dyn LimitBackend>,
}

impl Default for Services {
    fn default() -> Self {
        Self {
            codec: None,
            limits: Arc::new(LocalLimits::new()),
        }
    }
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Sabotage {
    #[default]
    None,
    /// Run step functions without waiting for their gate.
    SkipGate,
    /// Release outputs as soon as their records are appended.
    EarlyRelease,
    /// Accept an update again when its ID is retried.
    DuplicateUpdates,
    /// Grant limit permits whatever the limit's size.
    IgnoreLimits,
    /// Starts between workflows create a new run when one finished.
    NoAttach,
    /// Answer queries before the state they read is durable.
    EarlyAnswers,
    /// Hand stream chunks to readers before they are durable.
    EarlyChunks,
    /// Run a key's exclusive calls without waiting for the one running.
    UnlockedObjects,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            shard: 0,
            shards: 1,
            retention_ms: 24 * 3_600_000,
            park_after_ms: 1_000,
            memory_budget: 0,
            checkpoint_interval_ms: 30_000,
            max_loaded: 1_000_000,
            segment_bytes: fusee_core::segment::DEFAULT_SEGMENT_BYTES,
            inline_flush: true,
            inline_max_us: 200,
            keep_history: false,
            on_exhausted: crate::ctx::Exhausted::Fail,
            archive_history: false,
            step_times: false,
            #[cfg(feature = "opentelemetry")]
            telemetry: None,
            replay_only: false,
            checkpoint_on_close: true,
            index_chunk_records: INDEX_CHUNK_RECORDS,
            index_rebase_deltas: 64,
            sabotage: Sabotage::None,
        }
    }
}

/// Counters, per shard. Cheap to copy; exported by the engine.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Metrics {
    pub records: u64,
    pub log_bytes: u64,
    pub flushes: u64,
    pub inline_flushes: u64,
    pub flushed_records: u64,
    pub flush_us_total: u64,
    pub steps_run: u64,
    pub steps_replayed: u64,
    pub runs_started: u64,
    pub runs_finished: u64,
    /// Number of times a live workflow was parked since opening the engine.
    pub runs_parked: u64,
    /// Current run-store index entries, including completed entries retained on disk.
    pub parked_entries: u64,
    /// Heap bytes of the run table (every run's entry and its indexes).
    pub run_table_bytes: u64,
    /// Bytes of run state held in memory by loaded runs (an estimate; see
    /// [`RuntimeConfig::memory_budget`]).
    pub loaded_bytes: u64,
    /// The last open: run store frames and bytes it read (the persisted
    /// index, or every frame of the store prefix when it had none), and
    /// how long it took.
    pub open_store_frames: u64,
    pub open_store_bytes: u64,
    pub open_us: u64,
    /// Runs parked early because loaded state exceeded the memory budget.
    pub budget_parks: u64,
    /// Keys of object types continued as new with a snapshot of their
    /// state (see [`crate::object`]).
    pub object_compactions: u64,
    /// Run store frames read to rebuild journals (a parked run's newest
    /// frame and the `Delta` frames it extends), and their bytes.
    pub journal_frames_read: u64,
    pub journal_bytes_read: u64,
    pub runs_loaded: u64,
    pub checkpoints: u64,
    /// Index bases written a frame at a time between checkpoints and then
    /// named by one (see [`Inner::write_index`]).
    pub index_bases_built: u64,
    pub determinism_errors: u64,
    pub busy: u64,
    pub gate_waits: u64,
    pub gate_wait_us_total: u64,
    /// Ticks and flushes over the stall threshold (see [`Stall`]).
    pub stalls: u64,
    /// Longest tick and flush seen while stall tracing is on.
    pub max_tick_us: u64,
    pub max_flush_us: u64,
    /// Current volatile timers (step timeouts, heartbeat timeouts, retry
    /// backoff), including cancelled ones the timer heap still holds.
    pub volatile_timers: u64,
    /// The shared log's writes to its device, the bytes they put there
    /// (headers and padding included; against `log_bytes`, the records'),
    /// and its segment rolls, since it opened. One shard of a log reports
    /// them, so a sum over shards counts each once.
    pub log_writes: u64,
    pub log_device_bytes: u64,
    pub log_rolls: u64,
    /// 1 when the shared log's owner thread writes (Linux, `io_uring`).
    pub log_owner: u64,
}

/// A finished run's summary from its whole journal: its `Start` without the
/// input, its attributes, its streams and its `Done`, as `park_done` keeps
/// for a successful run.
fn done_summary(journal: &[u8]) -> Result<Vec<u8>, Error> {
    let records = decode_canonical_all(journal).map_err(|e| Error::Io(e.to_string()))?;
    let mut out = Vec::with_capacity(journal.len().min(4096));
    for r in &records {
        match r {
            Record::Start { .. } => {
                let mut start = r.clone();
                if let Record::Start { input, update, .. } = &mut start {
                    *input = fusee_core::run::empty();
                    *update = None;
                }
                encode_canonical(&start, &mut out);
            }
            Record::Attributes { .. }
            | Record::Chunk { .. }
            | Record::StreamEnd { .. }
            | Record::Done { .. } => encode_canonical(r, &mut out),
            _ => {}
        }
    }
    Ok(out)
}

/// Steps one poll of a run may deliver as they finish before the run
/// returns to the drive, which lets other runs in.
const INLINE_DELIVERIES: u32 = 64;

/// Stall tracing for diagnosis. While on, a tick or flush that takes longer
/// than the threshold is recorded with what else the shard was doing. The
/// threshold comes from `FUSEE_STALL_US` (unset or 0: off) or
/// [`set_stall_threshold_us`]; `FUSEE_STALL_TRACE=<file>` also appends each
/// stall to that file as one JSON line.
static STALL_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(u64::MAX);
static STALL_TRACE: Mutex<Option<std::fs::File>> = Mutex::new(None);

/// Sets the stall-tracing threshold in microseconds (0 turns tracing off).
pub fn set_stall_threshold_us(us: u64) {
    STALL_US.store(us, Ordering::Relaxed);
}

fn stall_threshold_us() -> u64 {
    let v = STALL_US.load(Ordering::Relaxed);
    if v != u64::MAX {
        return v;
    }
    let v = std::env::var("FUSEE_STALL_US")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if v != 0
        && let Ok(path) = std::env::var("FUSEE_STALL_TRACE")
        && let Ok(file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        && let Ok(mut t) = STALL_TRACE.lock()
    {
        *t = Some(file);
    }
    STALL_US.store(v, Ordering::Relaxed);
    v
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum StallKind {
    /// One call of the shard loop's tick.
    Tick,
    /// A flush on the shard thread.
    InlineFlush,
    /// A background flush, from submission to durability.
    Flush,
}

/// A tick or flush that took longer than the stall threshold, and what was
/// happening at the time.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Stall {
    pub shard: u32,
    pub kind: StallKind,
    pub us: u64,
    pub at_ms: u64,
    /// Records and bytes in the batch (flushes only).
    pub records: u32,
    pub bytes: u64,
    /// A segment file was being zero-filled in the background.
    pub preparing_segment: bool,
    pub checkpointing: bool,
    /// Runs the park sweep parked during this tick, and the time it took.
    pub parked: u32,
    pub maintenance_us: u64,
    pub loaded: usize,
    pub slots: usize,
}

/// What a start does when the workflow ID is already running.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum OnRunning {
    /// Fail with [`Error::AlreadyExists`].
    #[default]
    Fail,
    /// Return the running workflow.
    UseExisting,
    /// Terminate it and start a new run.
    TerminateExisting,
}

/// What a start does when the workflow ID's last run finished within
/// retention.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum OnFinished {
    /// Start a new run.
    #[default]
    Allow,
    /// Start a new run only if the last one did not succeed.
    AllowIfFailed,
    /// Fail with [`Error::AlreadyExists`].
    Refuse,
    /// Return the finished run (its result).
    UseExisting,
}

/// What happens to a child workflow when its parent closes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ParentClose {
    #[default]
    Terminate,
    RequestCancel,
    Abandon,
}

/// Workflow ID reuse rules for a start. Recorded with the start.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct IdPolicy {
    pub running: OnRunning,
    pub finished: OnFinished,
}

impl IdPolicy {
    /// Attach to whatever run exists (starts between workflows, schedules).
    pub const ATTACH: Self = Self {
        running: OnRunning::UseExisting,
        finished: OnFinished::UseExisting,
    };

    pub fn bits(self) -> u8 {
        let r = match self.running {
            OnRunning::Fail => policy::RUNNING_FAIL,
            OnRunning::UseExisting => policy::RUNNING_USE_EXISTING,
            OnRunning::TerminateExisting => policy::RUNNING_TERMINATE,
        };
        let f = match self.finished {
            OnFinished::Allow => policy::FINISHED_ALLOW,
            OnFinished::AllowIfFailed => policy::FINISHED_IF_FAILED,
            OnFinished::Refuse => policy::FINISHED_REFUSE,
            OnFinished::UseExisting => policy::FINISHED_USE_EXISTING,
        };
        r | f
    }

    pub fn from_bits(b: u8) -> Self {
        Self {
            running: match b & policy::RUNNING_MASK {
                policy::RUNNING_USE_EXISTING => OnRunning::UseExisting,
                policy::RUNNING_TERMINATE => OnRunning::TerminateExisting,
                _ => OnRunning::Fail,
            },
            finished: match b & policy::FINISHED_MASK {
                policy::FINISHED_IF_FAILED => OnFinished::AllowIfFailed,
                policy::FINISHED_REFUSE => OnFinished::Refuse,
                policy::FINISHED_USE_EXISTING => OnFinished::UseExisting,
                _ => OnFinished::Allow,
            },
        }
    }
}

impl ParentClose {
    pub fn bits(self) -> u8 {
        match self {
            Self::Terminate => policy::CLOSE_TERMINATE,
            Self::RequestCancel => policy::CLOSE_CANCEL,
            Self::Abandon => policy::CLOSE_ABANDON,
        }
    }
}

/// An update delivered with a start (update-with-start).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateRequest {
    pub name: String,
    /// Deduplicates retried calls; empty picks a fresh one.
    pub id: String,
    pub payload: Payload,
    /// Answer through [`Output::Reply`] once the handler's result is durable.
    pub token: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct StartRequest {
    pub id: String,
    pub wtype: String,
    /// `None` picks the newest registered version.
    pub version: Option<u32>,
    pub input: Payload,
    pub codec: u8,
    /// Absolute Unix ms; 0 = none.
    pub deadline_ms: u64,
    pub parent: Option<ParentRef>,
    /// ID and close policy bits ([`policy`]).
    pub policy: u8,
    /// Deliver this update in the start record if a new run is created, or
    /// to the running workflow if one is used.
    pub update: Option<UpdateRequest>,
    /// The input already went through the codec (a start from another
    /// workflow).
    pub sealed: bool,
    /// Acknowledge through [`Output::Ack`] once the start is durable.
    pub token: Option<u64>,
    /// The W3C `traceparent` the run starts under (see
    /// [`StartMeta::trace`]); a child inherits its parent's.
    pub trace: Option<Payload>,
}

impl StartRequest {
    /// A start from outside the engine with the given ID policy.
    pub fn new(id: &str, wtype: &str, input: Payload, ids: IdPolicy) -> Self {
        Self {
            id: id.to_owned(),
            wtype: wtype.to_owned(),
            version: None,
            input,
            codec: 1,
            deadline_ms: 0,
            parent: None,
            policy: ids.bits(),
            update: None,
            sealed: false,
            token: None,
            trace: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartOutcome {
    Created { run: RunId },
    Existing { run: RunId, done: bool },
}

/// A run's state as seen from outside.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultState {
    NotFound,
    Running(RunId),
    Done {
        run: RunId,
        status: Status,
        payload: Payload,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunSummary {
    pub shard: u32,
    pub run: RunId,
    pub id: String,
    pub wtype: String,
    pub version: u32,
    /// `running`, `suspended`, `parked`, `stuck`, `waiting_for_code`, or a
    /// final status (`ok`, `failed`, `cancelled`, `timed_out`, `terminated`).
    pub status: String,
    pub started_ms: u64,
    pub finished_ms: Option<u64>,
    pub error: Option<String>,
    /// Search attributes, a JSON object (`{}` when none were set).
    pub attributes: String,
    /// The run is a key of an object type (see [`crate::object`]).
    pub object: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunDetail {
    pub summary: RunSummary,
    pub journal: Vec<RecordBuf>,
    /// For a key of an object type: its state and pending calls.
    pub object: Option<Box<ObjectDetail>>,
}

/// Events for a host that runs workflow code itself (language bindings).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForeignEvent {
    /// Start the run's code. If `replaying`, its journal holds results.
    Start { run: RunId, info: RunInfo },
    /// Something changed for the run: re-check step gates and pump
    /// deliveries.
    Wake { run: RunId },
    /// The run hit a determinism error and was stopped.
    Stuck { run: RunId, message: String },
    /// The run ended from outside (deadline or termination); drop its code.
    Ended { run: RunId },
}

/// A message between shards of one engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    ChildStart {
        parent: ParentRef,
        wtype: String,
        id: String,
        flags: u8,
        deadline_ms: u64,
        input: Payload,
        /// The parent's trace context, which the child joins.
        trace: Option<Payload>,
    },
    ChildResult {
        parent: ParentRef,
        status: Status,
        payload: Payload,
    },
    Signal {
        target: String,
        name: String,
        payload: Payload,
        dedup: u64,
    },
    /// Terminate or cancel a workflow. With `parent`, only a run that is a
    /// child of that parent (its parent closed).
    Close {
        target: String,
        wtype: String,
        parent: Option<ParentRef>,
        terminate: bool,
        reason: Payload,
    },
}

/// One delivery for a foreign host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Delivery {
    Resolve {
        seq: Seq,
        outcome: Outcome,
    },
    CancelAll {
        seqs: Vec<Seq>,
        reason: Payload,
    },
    /// Start an update's handler now. With `token` (a new request) run its
    /// validator first and report the decision with
    /// [`ShardRuntime::decide_update`] before anything else.
    Update {
        ordinal: u64,
        name: String,
        id: String,
        payload: Payload,
        token: Option<u64>,
    },
}

/// A message to another shard, kept in the outbox until acknowledged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outgoing {
    pub id: u64,
    pub target: u32,
    pub msg: Message,
    sent_ms: u64,
}

const MSG_TOKEN: u64 = 1 << 63;
const RESEND_MS: u64 = 1_000;

fn encode_message(msg: &Message, out: &mut Vec<u8>) {
    use fusee_core::wire::{put_bytes, put_varint};
    let parent = |out: &mut Vec<u8>, p: &ParentRef| {
        put_varint(out, u64::from(p.shard));
        put_varint(out, p.run);
        put_varint(out, u64::from(p.seq));
    };
    match msg {
        Message::ChildStart {
            parent: p,
            wtype,
            id,
            flags,
            deadline_ms,
            input,
            trace,
        } => {
            // Kind 5 is a child start with its parent's trace context.
            out.push(if trace.is_some() { 5 } else { 1 });
            parent(out, p);
            put_bytes(out, wtype.as_bytes());
            put_bytes(out, id.as_bytes());
            out.push(*flags);
            put_varint(out, *deadline_ms);
            put_bytes(out, input);
            if let Some(t) = trace {
                put_bytes(out, t);
            }
        }
        Message::ChildResult {
            parent: p,
            status,
            payload,
        } => {
            out.push(2);
            parent(out, p);
            out.push(*status as u8);
            put_bytes(out, payload);
        }
        Message::Signal {
            target,
            name,
            payload,
            dedup,
        } => {
            out.push(3);
            put_bytes(out, target.as_bytes());
            put_bytes(out, name.as_bytes());
            put_bytes(out, payload);
            put_varint(out, *dedup);
        }
        Message::Close {
            target,
            wtype,
            parent: p,
            terminate,
            reason,
        } => {
            out.push(4);
            put_bytes(out, target.as_bytes());
            put_bytes(out, wtype.as_bytes());
            match p {
                None => out.push(0),
                Some(p) => {
                    out.push(1);
                    parent(out, p);
                }
            }
            out.push(u8::from(*terminate));
            put_bytes(out, reason);
        }
    }
}

fn decode_message(r: &mut fusee_core::wire::Reader<'_>) -> fusee_core::wire::DecodeResult<Message> {
    use fusee_core::wire::DecodeError;
    let parent =
        |r: &mut fusee_core::wire::Reader<'_>| -> fusee_core::wire::DecodeResult<ParentRef> {
            Ok(ParentRef {
                shard: r.varint_u32()?,
                run: r.varint()?,
                seq: r.varint_u32()?,
            })
        };
    Ok(match r.u8()? {
        k @ (1 | 5) => Message::ChildStart {
            parent: parent(r)?,
            wtype: r.str()?.to_owned(),
            id: r.str()?.to_owned(),
            flags: r.u8()?,
            deadline_ms: r.varint()?,
            input: Arc::from(r.bytes()?),
            trace: if k == 5 {
                Some(Arc::from(r.bytes()?))
            } else {
                None
            },
        },
        2 => Message::ChildResult {
            parent: parent(r)?,
            status: Status::from_u8(r.u8()?)?,
            payload: Arc::from(r.bytes()?),
        },
        3 => Message::Signal {
            target: r.str()?.to_owned(),
            name: r.str()?.to_owned(),
            payload: Arc::from(r.bytes()?),
            dedup: r.varint()?,
        },
        4 => {
            let target = r.str()?.to_owned();
            let wtype = r.str()?.to_owned();
            let p = match r.u8()? {
                0 => None,
                1 => Some(parent(r)?),
                _ => return Err(DecodeError("bad parent flag")),
            };
            Message::Close {
                target,
                wtype,
                parent: p,
                terminate: r.u8()? != 0,
                reason: Arc::from(r.bytes()?),
            }
        }
        _ => return Err(DecodeError("unknown message kind")),
    })
}

pub fn encode_outbox(entries: &[Outgoing]) -> Vec<u8> {
    use fusee_core::wire::put_varint;
    let mut out = Vec::new();
    put_varint(&mut out, entries.len() as u64);
    for e in entries {
        put_varint(&mut out, e.id);
        put_varint(&mut out, u64::from(e.target));
        encode_message(&e.msg, &mut out);
    }
    out
}

pub fn decode_outbox(bytes: &[u8]) -> fusee_core::wire::DecodeResult<Vec<Outgoing>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut r = fusee_core::wire::Reader::new(bytes);
    let n = r.varint()?;
    let mut out = Vec::new();
    for _ in 0..n {
        let id = r.varint()?;
        let target = r.varint_u32()?;
        let msg = decode_message(&mut r)?;
        out.push(Outgoing {
            id,
            target,
            msg,
            sent_ms: 0,
        });
    }
    Ok(out)
}

/// Runs whose code was woken outside their shard's tick (from another
/// thread, or by a host call between ticks), and the callback that wakes
/// the shard. Wakes during the tick, on its thread, take the tick's list
/// instead (see [`LocalTick`]).
pub struct ReadyQueue {
    queue: Mutex<Vec<RunId>>,
    /// `queue` may hold runs: set after a push, cleared before a drain.
    queued: AtomicBool,
    wake: Notify,
}

impl ReadyQueue {
    fn push(&self, run: RunId) {
        if let Ok(mut q) = self.queue.lock() {
            q.push(run);
        }
        self.queued.store(true, Ordering::Release);
        (self.wake)();
    }
}

/// The runs woken on this thread during the tick of the runtime ticking
/// here. The tick picks them up in its next round, so a wake from inside
/// the tick (a commit opening a gate, a delivery, a timer) takes neither
/// the ready queue's mutex nor the cross-thread wake.
struct LocalReady {
    queue: Arc<ReadyQueue>,
    runs: Vec<RunId>,
}

thread_local! {
    static LOCAL_READY: RefCell<Option<LocalReady>> = const { RefCell::new(None) };
}

/// Queues `run` on the tick's list if `queue`'s runtime is ticking on this
/// thread. Returns false otherwise.
fn push_local(queue: &Arc<ReadyQueue>, run: RunId) -> bool {
    LOCAL_READY
        .try_with(|l| match l.try_borrow_mut().as_deref_mut() {
            Ok(Some(l)) if Arc::ptr_eq(&l.queue, queue) => {
                l.runs.push(run);
                true
            }
            _ => false,
        })
        .unwrap_or(false)
}

/// Moves the runs on the tick's list of `queue`'s runtime to `out`, or
/// tells whether there are any.
fn take_local(queue: &Arc<ReadyQueue>, out: Option<&mut Vec<RunId>>) -> bool {
    LOCAL_READY
        .try_with(|l| match l.try_borrow_mut().as_deref_mut() {
            Ok(Some(l)) if Arc::ptr_eq(&l.queue, queue) => {
                let any = !l.runs.is_empty();
                if let Some(out) = out {
                    out.append(&mut l.runs);
                }
                any
            }
            _ => false,
        })
        .unwrap_or(false)
}

/// A runtime's tick on this thread. Wakes on the thread go to the tick's
/// list while it lasts; dropping it moves the wakes the tick did not get
/// to onto the ready queue, with the usual wake of the shard, and gives
/// the thread back to a runtime that was ticking around this one.
struct LocalTick<'a> {
    spare: &'a RefCell<Vec<RunId>>,
    outer: Option<LocalReady>,
}

impl<'a> LocalTick<'a> {
    fn enter(queue: Arc<ReadyQueue>, spare: &'a RefCell<Vec<RunId>>) -> Self {
        let runs = std::mem::take(&mut *spare.borrow_mut());
        let outer = LOCAL_READY
            .try_with(|l| {
                l.try_borrow_mut()
                    .ok()
                    .and_then(|mut l| l.replace(LocalReady { queue, runs }))
            })
            .ok()
            .flatten();
        Self { spare, outer }
    }
}

impl Drop for LocalTick<'_> {
    fn drop(&mut self) {
        let outer = self.outer.take();
        let mine = LOCAL_READY
            .try_with(|l| {
                l.try_borrow_mut()
                    .ok()
                    .and_then(|mut l| std::mem::replace(&mut *l, outer))
            })
            .ok()
            .flatten();
        if let Some(LocalReady { queue, mut runs }) = mine {
            if !runs.is_empty() {
                if let Ok(mut q) = queue.queue.lock() {
                    q.append(&mut runs);
                }
                queue.queued.store(true, Ordering::Release);
                (queue.wake)();
            }
            *self.spare.borrow_mut() = runs;
        }
    }
}

struct RunWaker {
    run: RunId,
    queued: AtomicBool,
    woken: AtomicBool,
    ready: Arc<ReadyQueue>,
}

impl RunWaker {
    /// A wake from the shard's own thread: a plain load and store instead
    /// of the swap, an atomic read-modify-write that costs a few percent of
    /// a shard's time at high step rates. A wake from another
    /// thread racing this one can at worst queue the run twice, which
    /// costs one poll that finds nothing new.
    fn wake_local(&self) {
        self.woken.store(true, Ordering::Relaxed);
        if !self.queued.load(Ordering::Relaxed) {
            self.queued.store(true, Ordering::Relaxed);
            if !push_local(&self.ready, self.run) {
                self.ready.push(self.run);
            }
        }
    }
}

impl Wake for RunWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Release);
        if !self.queued.swap(true, Ordering::AcqRel) && !push_local(&self.ready, self.run) {
            self.ready.push(self.run);
        }
    }
}

/// Runs waiting for their gate, `(lsn, run, since)`, lowest position first.
/// A run's gate is almost always the position of the record it has just
/// appended, so gates come in the order of the log: those go in a queue,
/// each in constant time, and only one that comes out of order goes in a
/// heap (a heap for all of them cost about 2% of a shard's time near its
/// capacity).
#[derive(Default)]
struct Gates {
    fifo: VecDeque<(Lsn, RunId, u64)>,
    heap: BinaryHeap<Reverse<(Lsn, RunId, u64)>>,
}

impl Gates {
    fn push(&mut self, g: (Lsn, RunId, u64)) {
        if self.fifo.back().is_none_or(|b| b.0 <= g.0) {
            self.fifo.push_back(g);
        } else {
            self.heap.push(Reverse(g));
        }
    }

    /// The lowest gate.
    fn peek(&self) -> Option<(Lsn, RunId, u64)> {
        match (self.fifo.front(), self.heap.peek()) {
            (Some(&f), Some(&Reverse(h))) => Some(if h.0 < f.0 { h } else { f }),
            (Some(&f), None) => Some(f),
            (None, Some(&Reverse(h))) => Some(h),
            (None, None) => None,
        }
    }

    /// Removes the gate [`peek`](Self::peek) returned.
    fn pop(&mut self) {
        match (self.fifo.front(), self.heap.peek()) {
            (Some(f), Some(Reverse(h))) if h.0 < f.0 => {
                self.heap.pop();
            }
            (Some(_), _) => {
                self.fifo.pop_front();
            }
            (None, _) => {
                self.heap.pop();
            }
        }
    }
}

/// A run's journal for [`Inner::new_slot`]: borrowed, or decoded for the
/// slot alone, whose start's names and payloads then move into the run.
enum JournalIn<'a> {
    Borrowed(&'a [RecordBuf]),
    Decoded(&'a mut Vec<RecordBuf>),
}

/// The [`RunWaker`] behind a [`Code`]'s waker.
fn run_waker(handle: &Waker) -> &RunWaker {
    // SAFETY: a `Code`'s waker is made from an `Arc<RunWaker>` (`Code::new`
    // is the only constructor), whose data pointer is the `RunWaker`, and
    // the reference the waker holds keeps it alive as long as the waker.
    unsafe { &*handle.data().cast::<RunWaker>() }
}

/// A run's code: its future and the waker it is polled with, made once
/// when the code starts. The waker holds the one reference to the run's
/// [`RunWaker`] (two, an `Arc` beside the `Waker`, cost an atomic
/// increment and decrement a wake).
struct Code {
    fut: LocalFuture,
    handle: Waker,
}

impl Code {
    fn new(fut: LocalFuture, waker: &Arc<RunWaker>) -> Self {
        Self {
            fut,
            handle: Waker::from(waker.clone()),
        }
    }

    /// The run's waker state.
    fn waker(&self) -> &RunWaker {
        run_waker(&self.handle)
    }

    /// Whether `waker` is the one this code is polled with.
    fn polled_with(&self, waker: &Arc<RunWaker>) -> bool {
        std::ptr::eq(self.handle.data().cast::<RunWaker>(), Arc::as_ptr(waker))
    }
}

struct Slot {
    machine: RunMachine,
    waker: Arc<RunWaker>,
    foreign: bool,
    /// When the park sweep queued this run as suspended; cleared whenever it
    /// becomes active again (a queued entry with another time is stale).
    suspended_since: Option<u64>,
    /// On the dirty list: active since the sweep last saw it suspended.
    dirty: bool,
    /// Wakes of the run's code that name no future (see [`Host::broadcasts`]).
    broadcasts: u64,
    /// Foreign runs: a `Wake` is queued for the host and not yet taken.
    wake_queued: bool,
    /// The lowest durability gate this run has an entry for in the gate
    /// heap. A later wait for a higher gate needs no entry of its own: the
    /// run re-checks every gate when the lower one wakes it.
    gate_at: Option<Lsn>,
    /// Foreign runs: on the list of runs with deliveries the host has not
    /// pumped yet.
    undelivered: bool,
    /// A delivered result the code has not taken yet, kept here rather
    /// than in [`Inner::resolved`] while no future of the shard waits with
    /// a waker of its own (a sequential run's only resolution).
    resolved: Option<(Seq, Outcome)>,
}

/// What a removed slot tells its remover.
struct Removed {
    foreign: bool,
}

/// Slots of runs that ended or parked, kept for the runs that load or start
/// next: their boxes, machines and wakers are reused (see
/// [`Inner::new_slot`]). An idle shard keeps [`SLOT_SPARE_IDLE`].
const SLOT_SPARE: usize = 2048;

/// Runs ended within a tick after which the tick reaps them before its
/// next round (see `ShardRuntime::tick_rounds`).
const REAP_EARLY: usize = 256;
/// Spare slots kept once a shard loaded and started nothing for a
/// maintenance interval.
const SLOT_SPARE_IDLE: usize = 64;

/// A delivered result, or the waker of the future that waits for it.
enum Resolution {
    Ready(Outcome),
    Waiting(Waker),
}

/// The loaded runs' slots in a slab. One hash lookup maps a run ID to its
/// index and a one-entry cache skips even that for consecutive calls about
/// the same run, as all of a step's host calls are. The runtime keeps each
/// run's code at its slot's index, so an index is reused only after the
/// runtime dropped the code of the run that had it (`ShardRuntime::reap`).
#[derive(Default)]
struct Slots {
    entries: Vec<Option<Box<Slot>>>,
    index: FxHashMap<RunId, u32>,
    /// Indices free for reuse, lowest first, so the slab stays compact.
    free: std::collections::BTreeSet<u32>,
    last: Cell<Option<(RunId, u32)>>,
}

impl Slots {
    // A step makes about fifteen lookups, nearly all for the run of the
    // one before: the cached case is inlined into the caller, the hash
    // lookup is not.
    #[inline]
    fn idx(&self, run: RunId) -> Option<u32> {
        if let Some((r, i)) = self.last.get()
            && r == run
        {
            return Some(i);
        }
        self.idx_slow(run)
    }

    #[cold]
    #[inline(never)]
    fn idx_slow(&self, run: RunId) -> Option<u32> {
        let i = *self.index.get(&run)?;
        self.last.set(Some((run, i)));
        Some(i)
    }

    #[inline]
    fn get(&self, run: &RunId) -> Option<&Slot> {
        let i = self.idx(*run)?;
        self.entries.get(i as usize)?.as_deref()
    }

    #[inline]
    fn get_mut(&mut self, run: &RunId) -> Option<&mut Slot> {
        let i = self.idx(*run)?;
        self.entries.get_mut(i as usize)?.as_deref_mut()
    }

    #[inline]
    fn contains_key(&self, run: &RunId) -> bool {
        self.idx(*run).is_some()
    }

    fn insert(&mut self, run: RunId, slot: Box<Slot>) {
        if let Some(i) = self.index.get(&run) {
            // A run has one slot at a time; replace it where it is.
            self.entries[*i as usize] = Some(slot);
            return;
        }
        let i = match self.free.pop_first() {
            Some(i) => i,
            None => {
                self.entries.push(None);
                (self.entries.len() - 1) as u32
            }
        };
        self.entries[i as usize] = Some(slot);
        self.index.insert(run, i);
        self.last.set(Some((run, i)));
    }

    /// Removes `run`'s slot. Its index stays taken until [`Slots::release`].
    fn remove(&mut self, run: &RunId) -> Option<(u32, Box<Slot>)> {
        let i = self.index.remove(run)?;
        if self.last.get().is_some_and(|(r, _)| r == *run) {
            self.last.set(None);
        }
        self.entries[i as usize].take().map(|s| (i, s))
    }

    /// The code of the run that had index `i` has been dropped: reuse it.
    fn release(&mut self, i: u32) {
        if self.entries.get(i as usize).is_none_or(Option::is_some) {
            return;
        }
        self.free.insert(i);
        // Free entries at the end go, so the slab follows the population.
        while self.entries.last().is_some_and(Option::is_none)
            && self
                .free
                .last()
                .is_some_and(|&top| top as usize + 1 == self.entries.len())
        {
            self.free.pop_last();
            self.entries.pop();
        }
    }

    fn len(&self) -> usize {
        self.index.len()
    }

    fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Gives back memory after the population fell.
    fn shrink(&mut self) {
        let n = self.index.len();
        if self.index.capacity() > 1024 && self.index.capacity() > 4 * n {
            self.index.shrink_to(n * 2);
        }
        let n = self.entries.len();
        if self.entries.capacity() > 1024 && self.entries.capacity() > 4 * n {
            self.entries.shrink_to(n * 2);
        }
    }
}

#[derive(Clone, Debug)]
enum Stashed {
    Complete(Seq, Completion),
    Wake,
}

pub struct Inner<E: Env> {
    pub env: E,
    pub core: ShardCore,
    writer: ShardLog,
    cfg: RuntimeConfig,
    slots: Slots,
    resolved: FxHashMap<(RunId, Seq), Resolution>,
    gates: Gates,
    /// When the current round began ([`Self::metric_us`]): a run's wait at
    /// its gate (`Metrics::gate_wait_us_total`) is counted from the start
    /// of the round in which it reached the gate, which costs a clock read
    /// a round instead of one a step (a foreign host's gate between ticks
    /// reads the clock).
    round_us: u64,
    flush_started_us: u64,
    timers: BinaryHeap<Reverse<(u64, u64)>>,
    timer_wakers: FxHashMap<u64, Waker>,
    timer_seq: u64,
    durable: Lsn,
    commit: Lsn,
    /// Runs a round may load while the host's queue has room for their
    /// events ([`ShardRuntime::set_load_room`]; unlimited otherwise).
    load_room: usize,
    /// Commits advanced at append, by storage durable on append
    /// ([`Env::durable_on_append`]), and not yet released (see
    /// [`Inner::flush_pending`]).
    commit_dirty: bool,
    /// Steps of the run being polled that may still be delivered as they
    /// finish, inside the poll (see the `Host::step_done` of the runtime).
    inline_deliveries: u32,
    /// Buffers of [`Inner::load_batch`], kept between batches.
    load_buf: Vec<RunId>,
    load_at: Vec<(BlobRef, RunId)>,
    /// The last position submitted and not yet durable (`None`: nothing in
    /// flight).
    in_flight: Option<Lsn>,
    /// Every batch in flight: its last position and when it was submitted
    /// ([`Inner::metric_us`]), oldest first. More than one only where the
    /// environment allows it ([`Env::max_in_flight`]).
    submitted: VecDeque<(Lsn, u64)>,
    ready: Arc<ReadyQueue>,
    registry: SharedRegistry,
    /// A language binding runs code the registry does not have.
    foreign_host: bool,
    codec: Option<Arc<dyn Codec>>,
    pub limits: Arc<dyn LimitBackend>,
    /// Callers waiting for an update's result: `(run, update ID)`.
    update_waiters: FxHashMap<(RunId, Name), Vec<u64>>,
    /// Keys of object types: their state, waiting calls and callers.
    obj: Objects,
    /// Live update requests delivered to code and not yet decided:
    /// token → (run, name, ID, sealed payload).
    offered: FxHashMap<u64, (RunId, Name, Name, Payload)>,
    next_update_id: u64,
    /// Callers of `complete`/`fail` waiting for the `Completed` record.
    completion_acks: FxHashMap<(RunId, Seq), Vec<u64>>,
    /// Coalesced heartbeats, appended just before the next flush.
    heartbeats: BTreeMap<(RunId, Seq), RecordBuf>,
    /// When live steps began, Unix ms, while `cfg.step_times` (dev).
    step_began: FxHashMap<(RunId, Seq), u64>,
    released: Vec<Output>,
    outbox: BTreeMap<u64, Outgoing>,
    next_out: u64,
    msg_tokens: FxHashMap<u64, (u32, u64)>,
    next_msg_token: u64,
    child_acks: FxHashMap<(RunId, Seq), Vec<(u32, u64)>>,
    message_acks: Vec<(u32, u64)>,
    foreign: VecDeque<ForeignEvent>,
    stash: FxHashMap<RunId, Vec<Stashed>>,
    awaiting: FxHashMap<String, Vec<RunId>>,
    instantiate: Vec<RunId>,
    cancel_tokens: FxHashMap<RunId, Vec<u64>>,
    /// Who asked for the request being applied (audit): set around a
    /// signal, cancel, terminate or reset by [`ShardRuntime::with_by`] and
    /// taken by the record it writes.
    by: Option<Payload>,
    subscribers: FxHashMap<RunId, Vec<ParentRef>>,
    stuck: FxHashMap<RunId, String>,
    failed: Option<String>,
    metrics: Metrics,
    ckpt_pending: Option<(Checkpoint, Option<u64>)>,
    ckpt_running: bool,
    last_ckpt_ms: u64,
    last_ckpt_lsn: Lsn,
    last_ckpt_store_len: u64,
    obsolete_store_bytes: u64,
    last_ckpt_segment: u64,
    last_sweep_ms: u64,
    current_step: FxHashMap<RunId, Seq>,
    /// Optional replication: commit advances by quorum instead of by local
    /// durability. Installed by replicated mode.
    pub replicator: Option<Box<dyn Replicator>>,
    /// Stall tracing (see [`Stall`]): the threshold when this shard opened
    /// (0: off), recent stalls, and context gathered during the tick.
    stall_us: u64,
    stalls: VecDeque<Stall>,
    /// Runs active since the park sweep last saw them suspended, and runs
    /// seen suspended, in the order (and with the time) they were seen.
    dirty: Vec<RunId>,
    park_queue: VecDeque<(u64, RunId)>,
    /// Runs whose slot was removed since the last reap, with its index.
    removed: Vec<(RunId, u32)>,
    /// Foreign runs that may have deliveries their host has not pumped.
    undelivered: Vec<RunId>,
    tick_parked: u32,
    tick_maintenance_us: u64,
    flush_records: u32,
    flush_bytes: u64,
    clock: TickClock,
    /// Flushes a run may still make from its durability gate this round
    /// ([`Inner::flush_at_gate`]); nonzero only while a round drives runs.
    gate_flushes: u32,
    /// Runs the round has yet to drive after the one being driven.
    drive_backlog: usize,
    /// Commands the host has queued for this shard (`None`: unknown, and
    /// then no run flushes from its gate, unless `host_never`).
    host_queue: Option<Arc<std::sync::atomic::AtomicUsize>>,
    /// The tick in progress was called with no host queue to look at
    /// ([`ShardRuntime::tick`]), the same as one that is always empty.
    host_never: bool,
    /// Parked runs whose timer came due, waiting to be loaded (the memory
    /// budget was full, or a round's share of loads was taken).
    held_loads: VecDeque<RunId>,
    /// Runs parked and loaded again whose old code is being dropped: its
    /// futures must not forget the new code's operations.
    reaping: Vec<RunId>,
    /// The run store takes `Delta` and `Index` frames (format 3).
    store_v3: bool,
    /// The store cannot be compacted (the `Env` declined once).
    no_compaction: bool,
    /// The persisted index: its frames in store generation `index_gen`, the
    /// bytes of its base and of the deltas written after the base. `None`:
    /// the next checkpoint writes a base.
    index: Option<Vec<BlobRef>>,
    index_gen: u64,
    index_base: (usize, u64),
    index_delta_bytes: u64,
    /// A new base being written a frame at a time (see [`IndexBuild`]).
    index_build: Option<IndexBuild>,
    /// Flushes in a row that took longer than `inline_max_us`.
    slow_flushes: u32,
    /// Run store frames not yet written (see [`StoreTail`]).
    tail: StoreTail,
    /// Reused buffers: a finished run's summary journal, an active run's
    /// timers.
    enc: Vec<u8>,
    timer_scratch: Vec<(Seq, TimerKind, u64)>,
    /// A loaded journal's decoded records (reused), and the names decoding
    /// has seen (workflow types and step names repeat across runs).
    decoded: Vec<RecordBuf>,
    names: fusee_core::record::InlineDecoder,
    /// See [`SLOT_SPARE`]. The boxes are kept: the slab holds boxed slots,
    /// so reusing one saves its allocation.
    #[allow(clippy::vec_box)]
    slot_spare: Vec<Box<Slot>>,
    /// Stream logs by run (see [`StreamLog`]), and the bytes of their chunks.
    streams: FxHashMap<RunId, RunStreams>,
    stream_bytes: usize,
    /// Reads waiting for a stream to grow, by run; runs whose readers to
    /// look at again once the log is committed through an LSN; the
    /// earliest deadline of a waiting read.
    stream_waiters: FxHashMap<RunId, Vec<StreamWaiter>>,
    stream_due: BinaryHeap<Reverse<(Lsn, RunId)>>,
    stream_wait_until: u64,
    /// The most loaded memory seen since the allocator was last told what
    /// parking freed (see [`crate::trim`]).
    trim_peak: u64,
    /// Runs loaded and started at the last maintenance: unchanged means the
    /// shard was idle, and its spare pools shrink.
    spare_mark: u64,
    /// Set when the pools above shrank: the shard runtime shrinks its own
    /// (handler sets) at the end of the tick.
    pools_shrunk: bool,
}

/// The shard's clock while it ticks. A step reads the time several times
/// (its record, the flush, the commit, the timers of the next round);
/// inside a tick a reading is kept and reused, so a tick that runs many
/// steps reads the clock a few times instead of several times per step.
///
/// The kept reading is renewed every [`CLOCK_DRIVES`] drives and after a
/// flush that took time. Time that is recorded or acted on
/// ([`Inner::now`]: when a run ends or starts, when a limit refills) is
/// read again once a step function ran since, as such a function may take
/// long; uses that only get later with an older reading (timers fire,
/// segment headers) take the kept one ([`Inner::tick_now`]). Time the code
/// asks for (`ctx.now`, the deadlines of sleeps, timeouts and retries) is
/// always read exactly.
#[derive(Default)]
struct TickClock {
    /// Inside a tick.
    on: bool,
    ms: Cell<Option<u64>>,
    /// A step function ran since `ms` was read.
    ran: Cell<bool>,
    us: Cell<Option<u64>>,
    /// Drives since the kept readings were taken.
    drives: u32,
}

/// Drives between renewals of the tick's clock readings.
const CLOCK_DRIVES: u32 = 64;

impl TickClock {
    fn renew(&self) {
        self.ms.set(None);
        self.ran.set(false);
        self.us.set(None);
    }
}

/// A run's stored journal, read from its newest frame and the frames it
/// extends.
struct Stored {
    records: Vec<RecordBuf>,
    last_lsn: Lsn,
    /// Bytes of the frames, and how many precede the newest.
    chain: u64,
    depth: u8,
}

/// A run's stored journal as canonical bytes, with the newest frame's
/// metadata.
struct Chain {
    /// The run the newest frame is about.
    run: RunId,
    journal: Vec<u8>,
    last_lsn: Lsn,
    id_hash: u64,
    timers: Vec<(Seq, TimerKind, u64)>,
    chain: u64,
    depth: u8,
}

/// Run store frames appended and not yet handed to the [`Env`]. A round
/// that finishes or parks many runs writes their frames with one
/// `store_append` (see [`Inner::store_flush`]) instead of one each; reads
/// of them are served from here meanwhile. Nothing waits for these bytes to
/// be durable: a checkpoint flushes them first and then syncs the store.
#[derive(Default)]
struct StoreTail {
    /// The store offset of the first byte of `buf`.
    at: u64,
    buf: Vec<u8>,
}

/// The tail is written once it holds this many bytes.
const STORE_TAIL_MAX: usize = 256 << 10;

impl StoreTail {
    /// Encodes `entry` at the end of the store, whose written part is
    /// `store_len` bytes long.
    fn put(&mut self, store_len: u64, entry: EntryRef<'_>) -> Result<BlobRef, Error> {
        if self.buf.is_empty() {
            self.at = store_len;
        }
        let from = self.buf.len();
        encode_entry_into(entry, &mut self.buf);
        let len = u32::try_from(self.buf.len() - from).map_err(|_| {
            self.buf.truncate(from);
            Error::Io("run store frame exceeds 4 GiB".into())
        })?;
        Ok(BlobRef {
            offset: self.at + from as u64,
            len,
        })
    }

    fn full(&self) -> bool {
        self.buf.len() >= STORE_TAIL_MAX
    }

    /// The bytes at `offset` if they are in the tail.
    fn get(&self, offset: u64, len: u32) -> Option<&[u8]> {
        let from = usize::try_from(offset.checked_sub(self.at)?).ok()?;
        if self.buf.is_empty() {
            return None;
        }
        self.buf.get(from..from.checked_add(len as usize)?)
    }
}

/// Reads `len` bytes of the run store at `offset`, from the unwritten tail
/// when they are there.
fn read_store<E: Env>(env: &mut E, tail: &StoreTail, offset: u64, len: u32) -> io::Result<Vec<u8>> {
    match tail.get(offset, len) {
        Some(bytes) => Ok(bytes.to_vec()),
        None => env.store_read(offset, len),
    }
}

/// The recent chunks of one stream, kept while its run is loaded, a reader
/// waits on it, or some of them are not durable yet. Every live append
/// enters a log and only durable chunks leave one, so the chunks before
/// `first` (and every chunk of a stream that has no log) are durable. A
/// reader at `first` or later is answered from here; one behind it reads
/// the run's journal once and then catches up here.
struct StreamLog {
    name: Name,
    /// The offset of `chunks[0]`.
    first: u64,
    /// `(LSN, payload as recorded)`.
    chunks: VecDeque<(Lsn, Payload)>,
    bytes: usize,
    /// The stream's length: the next chunk's offset.
    len: u64,
    /// Its `StreamEnd`.
    end: Option<(Lsn, Status)>,
}

/// A run's stream logs, its `Done` (LSN 0 when read durable from the
/// journal), its workflow ID, and when a reader last read it.
#[derive(Default)]
struct RunStreams {
    logs: Vec<StreamLog>,
    done: Option<(Lsn, Status)>,
    id: Option<Name>,
    read_at: u64,
}

/// A read waiting for the stream to grow (a long poll).
struct StreamWaiter {
    name: Name,
    from: u64,
    token: u64,
    deadline_ms: u64,
}

/// What one read of a stream found.
struct StreamRead {
    run: RunId,
    from: u64,
    /// Durable chunks from `from` on, as recorded.
    chunks: Vec<Payload>,
    end: Option<crate::stream::StreamEnd>,
    /// More becomes readable once the log is committed through this.
    wait: Option<Lsn>,
    /// The workflow ID the journal names, when the read looked at it.
    id: Option<Name>,
}

/// What [`Inner::scan_stream`] found in a run's journal.
struct StreamScan {
    chunks: Vec<Payload>,
    /// More chunks follow the batch.
    more: bool,
    len: u64,
    end: Option<Status>,
    done: Option<Status>,
    last_lsn: Lsn,
    id: Option<Name>,
}

/// What a stream's log keeps once its chunks are durable.
const STREAM_LOG_BYTES: usize = 64 << 10;
const STREAM_LOG_CHUNKS: usize = 1024;
/// A stream read from the journal is kept in its log up to this size, so
/// a reader catching up reads the journal once, not once per batch.
const STREAM_CACHE_BYTES: usize = 8 << 20;
/// A log is kept this long after a read, for the reader's next one.
const STREAM_READ_KEEP_MS: u64 = 10_000;

fn end_of(status: Status, closed: bool) -> crate::stream::StreamEnd {
    crate::stream::StreamEnd {
        status: status.as_str().to_owned(),
        closed,
    }
}

/// Index records per `Index` frame (1 MiB).
const INDEX_FRAME_RECORDS: usize = 1 << 15;

/// Slow flushes in a row after which the disk counts as slow every time
/// (see [`Inner::flush_pending`]).
const SLOW_DISK_FLUSHES: u32 = 16;

/// Index records per frame of a base written between checkpoints (256 KiB:
/// about half a millisecond to encode and append).
pub const INDEX_CHUNK_RECORDS: usize = 1 << 13;

/// How long one sweep (every 100 ms) spends writing an index base, and at
/// most how many frames it writes: a base of a million parked runs takes
/// about three seconds.
const INDEX_BUILD_SLICE_US: u64 = 300;
const INDEX_BUILD_SLICE_FRAMES: usize = 4;

/// A new index base written a frame at a time between checkpoints (see
/// [`Inner::write_index`]). `frames` hold the parked runs in the slots
/// before `cursor` (`None`: every slot), in store generation `generation`.
/// Slots are held for the walk ([`ShardCore::hold_slots`]), so it visits
/// every entry the table held when it began. Each entry is recorded as it
/// was when its slot was visited; a change after that is in the change
/// lists, so in a delta written after the build began. Once done, the next
/// checkpoint names `frames`, then the current chain's deltas from
/// `deltas_from` on, then its own delta.
struct IndexBuild {
    cursor: Option<fusee_core::table::Slot>,
    frames: Vec<BlobRef>,
    bytes: u64,
    generation: u64,
    deltas_from: usize,
}

/// What a loaded run costs in memory besides its journal, for the memory
/// budget: its state in the core, its slot, its code's machine and future
/// (5.7 KB measured for a one-sleep workflow, `tests/parked_index.rs`).
pub const LOADED_RUN_BYTES: u64 = 5632;

/// Most runs the memory budget parks per sweep (every 100 ms), so a burst
/// over budget is parked over a few sweeps instead of in one long tick.
const BUDGET_PARKS: usize = 16 * 1024;

/// The shard's side of its log: segment files of its own (format 2), or
/// one section per batch of a log shared with the other shards (format 3).
enum ShardLog {
    Segments(LogWriter),
    Sections(fusee_core::wal::SectionWriter),
}

impl ShardLog {
    fn append<S: AsRef<str>, B: AsRef<[u8]>>(
        &mut self,
        rec: &Record<S, B>,
        now_ms: u64,
    ) -> Result<Lsn, fusee_core::segment::TooLarge> {
        match self {
            Self::Segments(w) => w.append(rec, now_ms),
            Self::Sections(w) => w.append(rec),
        }
    }

    fn take(&mut self) -> Option<fusee_core::segment::Batch> {
        match self {
            Self::Segments(w) => w.take(),
            // A backlog goes out in sections of bounded size: the rest
            // stays pending for the next flush.
            Self::Sections(w) => w.take_up_to(fusee_core::wal::MAX_SECTION),
        }
    }

    fn has_pending(&self) -> bool {
        match self {
            Self::Segments(w) => w.has_pending(),
            Self::Sections(w) => w.has_pending(),
        }
    }

    fn last_lsn(&self) -> Lsn {
        match self {
            Self::Segments(w) => w.last_lsn(),
            Self::Sections(w) => w.last_lsn(),
        }
    }

    /// The canonical encoding of the last record, when the log keeps it.
    fn last_encoded(&self) -> Option<&[u8]> {
        match self {
            Self::Segments(_) => None,
            Self::Sections(w) => Some(w.last_encoded()),
        }
    }
}

/// Hook for replicated mode (see `crate::replicated`).
pub trait Replicator {
    /// A record was appended locally at `lsn`.
    fn appended(&mut self, lsn: Lsn, canonical: &[u8]);
    /// The local log is durable through `lsn`.
    fn durable(&mut self, lsn: Lsn);
    /// The highest LSN a quorum holds durably.
    fn commit(&self) -> Lsn;
}

impl<E: Env> Inner<E> {
    /// A loaded run's trace context, which the children it starts join.
    fn trace_of(&self, run: RunId) -> Option<Payload> {
        self.slots
            .get(&run)
            .and_then(|s| s.machine.info().trace.clone())
    }

    /// The active segment: the shard's own, or the shared log's.
    fn log_segment(&self) -> u64 {
        match &self.writer {
            ShardLog::Segments(w) => w.segment(),
            // `memory` mode has no log files; it checkpoints (and compacts
            // its run store) as often as segments of its records would roll.
            ShardLog::Sections(_) if !self.env.recovers() => {
                let framed = self.metrics.log_bytes
                    + self.metrics.flushed_records * fusee_core::segment::FRAME_HEADER as u64;
                1 + framed / self.cfg.segment_bytes.max(1)
            }
            ShardLog::Sections(_) => self.env.log_segment(),
        }
    }

    /// Records a stall if `us` is over the threshold (tracing on).
    fn trace_stall(&mut self, kind: StallKind, us: u64) {
        match kind {
            StallKind::Tick => self.metrics.max_tick_us = self.metrics.max_tick_us.max(us),
            _ => self.metrics.max_flush_us = self.metrics.max_flush_us.max(us),
        }
        if us <= self.stall_us {
            return;
        }
        self.metrics.stalls += 1;
        let flush = kind != StallKind::Tick;
        let stall = Stall {
            shard: self.cfg.shard,
            kind,
            us,
            at_ms: self.env.now_ms(),
            records: if flush { self.flush_records } else { 0 },
            bytes: if flush { self.flush_bytes } else { 0 },
            preparing_segment: self.env.preparing_segment(),
            checkpointing: self.ckpt_running,
            parked: self.tick_parked,
            maintenance_us: self.tick_maintenance_us,
            loaded: self.core.loaded_len(),
            slots: self.slots.len(),
        };
        if let Ok(mut t) = STALL_TRACE.lock()
            && let Some(file) = t.as_mut()
            && let Ok(mut line) = serde_json::to_vec(&stall)
        {
            use std::io::Write;
            line.push(b'\n');
            let _ = file.write_all(&line);
        }
        if self.stalls.len() >= 4096 {
            self.stalls.pop_front();
        }
        self.stalls.push_back(stall);
    }

    /// A finished live step's times, under the dev profile (`None` in
    /// production, where nothing was read when it began).
    #[inline]
    fn step_time(&mut self, run: RunId, seq: Seq) -> Option<StepTime> {
        if !self.cfg.step_times {
            return None;
        }
        let began_ms = self.step_began.remove(&(run, seq))?;
        Some(StepTime {
            began_ms,
            took_ms: self.now().saturating_sub(began_ms),
        })
    }

    /// The time in Unix ms: inside a tick the tick's reading unless a step
    /// function ran since it was taken (see [`TickClock`]), else the
    /// environment's.
    fn now(&self) -> u64 {
        match self.clock.ms.get() {
            Some(t) if self.clock.on && !self.clock.ran.get() => t,
            _ => self.now_exact(),
        }
    }

    /// The tick's reading even if step functions ran since, for uses that
    /// only get later with an older one: timers that fire, segment headers.
    fn tick_now(&self) -> u64 {
        match self.clock.ms.get() {
            Some(t) if self.clock.on => t,
            _ => self.now_exact(),
        }
    }

    /// The environment's time in Unix ms, also kept as the tick's reading.
    fn now_exact(&self) -> u64 {
        let t = self.env.now_ms();
        if self.clock.on {
            self.clock.ms.set(Some(t));
            self.clock.ran.set(false);
        }
        t
    }

    /// The monotonic clock in µs, for metrics and the stall tracer. Inside
    /// a tick of a shard whose flushes take no time (memory mode) every
    /// interval it could measure is zero, and it is the tick's reading;
    /// otherwise it is read exactly.
    fn metric_us(&self) -> u64 {
        if !self.clock.on || self.stall_us != 0 || self.env.last_flush_us() > 0 {
            return self.env.monotonic_us();
        }
        match self.clock.us.get() {
            Some(t) => t,
            None => {
                let t = self.env.monotonic_us();
                self.clock.us.set(Some(t));
                t
            }
        }
    }

    /// Enters or leaves a tick: the clock is kept only inside one.
    fn set_ticking(&mut self, on: bool) {
        self.clock.on = on;
        self.clock.drives = 0;
        self.clock.renew();
    }

    /// Counts a drive; every [`CLOCK_DRIVES`] the tick's clock is read again.
    fn count_drive(&mut self) {
        self.clock.drives += 1;
        if self.clock.drives >= CLOCK_DRIVES {
            self.clock.drives = 0;
            self.clock.renew();
        }
    }

    /// Passes a payload through the codec on its way into the journal.
    pub fn seal(&self, p: Payload) -> Result<Payload, Error> {
        match &self.codec {
            Some(c) if !p.is_empty() => Ok(Arc::from(c.encode(&p).map_err(Error::Codec)?)),
            _ => Ok(p),
        }
    }

    /// Passes a payload from the journal back through the codec.
    pub fn open(&self, p: Payload) -> Result<Payload, Error> {
        match &self.codec {
            Some(c) if !p.is_empty() => Ok(Arc::from(c.decode(&p).map_err(Error::Codec)?)),
            _ => Ok(p),
        }
    }

    fn open_outcome(&self, o: Outcome) -> Result<Outcome, Error> {
        if o.status != Status::Ok {
            return Ok(o);
        }
        Ok(Outcome {
            status: o.status,
            payload: self.open(o.payload)?,
        })
    }

    /// Whether this process has native code for a workflow type.
    fn native_code(&self, wtype: &str, version: u32) -> bool {
        self.registry
            .read()
            .is_ok_and(|r| r.get(wtype, version).is_some())
    }

    fn runs_foreign(&self, wtype: &str, version: u32) -> bool {
        self.foreign_host && !self.native_code(wtype, version)
    }

    pub fn durable(&self) -> Lsn {
        self.durable
    }

    pub fn commit(&self) -> Lsn {
        self.commit
    }

    pub fn last_lsn(&self) -> Lsn {
        self.writer.last_lsn()
    }

    /// Estimated memory of loaded runs (see [`RuntimeConfig::memory_budget`]).
    fn loaded_bytes(&self) -> u64 {
        self.core.loaded_len() as u64 * LOADED_RUN_BYTES
            + self.core.journal_bytes() as u64
            + self.stream_bytes as u64
    }

    pub fn metrics(&self) -> Metrics {
        let log = self.env.log_stats();
        Metrics {
            log_writes: log.writes,
            log_device_bytes: log.device_bytes,
            log_rolls: log.rolls,
            log_owner: u64::from(log.owner),
            parked_entries: self.core.parked_len() as u64,
            run_table_bytes: self.core.table_bytes() as u64,
            loaded_bytes: self.loaded_bytes(),
            volatile_timers: self.timers.len() as u64,
            ..self.metrics
        }
    }

    pub fn failed(&self) -> Option<&str> {
        self.failed.as_deref()
    }

    fn check_alive(&self) -> Result<(), Error> {
        match &self.failed {
            Some(m) => Err(Error::Io(m.clone())),
            None => Ok(()),
        }
    }

    /// Appends a record: log, core state, run gate.
    fn append(&mut self, rec: &RecordBuf) -> Result<Lsn, Error> {
        self.append_with(rec, Some(rec))
    }

    /// [`append`](Self::append) of a record that may borrow its strings
    /// and payload (a live step's result). `owned`: the same record
    /// owned, which only a stream record needs.
    fn append_with<S: fusee_core::record::ToName, B: fusee_core::record::ToPayload>(
        &mut self,
        rec: &Record<S, B>,
        owned: Option<&RecordBuf>,
    ) -> Result<Lsn, Error> {
        self.check_alive()?;
        // Only a new segment's header records it.
        let now = self.tick_now();
        let lsn = self.writer.append(rec, now).map_err(|_| {
            Error::Codec(
                "the record is longer than the log holds in one piece (a segment of a \
                     per-shard log, 64 MiB in the shared log): keep data of that size outside \
                     the journal and record a reference to it"
                    .into(),
            )
        })?;
        // A shared-log section holds the record's canonical encoding, which
        // is also what the run's journal keeps: encoded once.
        match self.writer.last_encoded() {
            Some(canonical) => self.core.apply_encoded(lsn, rec, Mode::LIVE, canonical),
            None => self.core.apply(lsn, rec, Mode::LIVE),
        }
        let run = rec.run();
        // Progress reports and stream chunks carry nothing a later step
        // depends on.
        match rec {
            Record::Heartbeat { .. } => {}
            Record::Chunk { .. } | Record::StreamEnd { .. } => {
                if let Some(rec) = owned {
                    self.stream_appended(lsn, rec);
                }
            }
            _ => {
                if let Record::Done { status, .. } = rec
                    && (!self.streams.is_empty() || !self.stream_waiters.is_empty())
                {
                    self.stream_run_done(run, lsn, *status);
                }
                if let Some(slot) = self.slots.get_mut(&run) {
                    slot.machine.gate = lsn;
                }
            }
        }
        self.metrics.records += 1;
        #[cfg(feature = "opentelemetry")]
        if let Some(telemetry) = &self.cfg.telemetry {
            telemetry.append(self.cfg.shard);
        }
        // Storage that holds nothing (`memory` mode): the record is durable
        // now. No batch, no flush, no gate to wait at; what it commits is
        // released where the round would have flushed.
        if self.replicator.is_none()
            && self.cfg.sabotage == Sabotage::None
            && self.env.durable_on_append()
            && let ShardLog::Sections(w) = &mut self.writer
            && let Some((records, bytes, last)) = w.discard()
        {
            self.metrics.flushed_records += u64::from(records);
            self.metrics.log_bytes += bytes;
            self.durable = self.durable.max(last);
            self.commit = self.commit.max(last);
            self.commit_dirty = true;
        }
        if let Some(r) = self.replicator.as_mut() {
            match self.writer.last_encoded() {
                Some(buf) => r.appended(lsn, buf),
                None => {
                    let mut buf = Vec::with_capacity(64);
                    encode_canonical(rec, &mut buf);
                    r.appended(lsn, &buf);
                }
            }
        }
        Ok(lsn)
    }

    /// A `Chunk` or `StreamEnd` was appended at `lsn`: it enters its
    /// stream's log, and readers waiting on the run look again once it is
    /// committed.
    fn stream_appended(&mut self, lsn: Lsn, rec: &RecordBuf) {
        let run = rec.run();
        let commit = self.commit;
        let (name, at) = match rec {
            Record::Chunk { stream, offset, .. } => (stream, *offset),
            Record::StreamEnd { stream, chunks, .. } => (stream, *chunks),
            _ => return,
        };
        let rs = self.streams.entry(run).or_default();
        if rs.id.is_none() {
            rs.id = self.core.get(run).map(|l| l.id.clone());
        }
        let i = match rs.logs.iter().position(|l| *l.name == **name) {
            Some(i) => i,
            None => {
                rs.logs.push(StreamLog {
                    name: name.clone(),
                    first: at,
                    chunks: VecDeque::new(),
                    bytes: 0,
                    len: at,
                    end: None,
                });
                rs.logs.len() - 1
            }
        };
        let log = &mut rs.logs[i];
        match rec {
            Record::Chunk {
                offset, payload, ..
            } => {
                if *offset != log.len {
                    // Not the log's next chunk (offsets come in order, so
                    // this does not happen): the log starts over at it.
                    self.stream_bytes = self.stream_bytes.saturating_sub(log.bytes);
                    log.chunks.clear();
                    log.bytes = 0;
                    log.first = *offset;
                }
                log.chunks.push_back((lsn, payload.clone()));
                log.bytes += payload.len();
                self.stream_bytes += payload.len();
                log.len = offset + 1;
                while (log.bytes > STREAM_LOG_BYTES || log.chunks.len() > STREAM_LOG_CHUNKS)
                    && log.chunks.front().is_some_and(|(l, _)| *l <= commit)
                {
                    if let Some((_, p)) = log.chunks.pop_front() {
                        log.bytes -= p.len();
                        self.stream_bytes = self.stream_bytes.saturating_sub(p.len());
                        log.first += 1;
                    }
                }
            }
            Record::StreamEnd { status, chunks, .. } => {
                log.end = Some((lsn, *status));
                log.len = log.len.max(*chunks);
            }
            _ => {}
        }
        if self.stream_waiters.contains_key(&run) {
            self.stream_due.push(Reverse((lsn, run)));
        }
    }

    /// The run's `Done` was appended: streams still open end with it.
    fn stream_run_done(&mut self, run: RunId, lsn: Lsn, status: Status) {
        let waiting = self.stream_waiters.contains_key(&run);
        if waiting || self.streams.contains_key(&run) {
            self.streams.entry(run).or_default().done = Some((lsn, status));
        }
        if waiting {
            self.stream_due.push(Reverse((lsn, run)));
        }
    }

    /// The chunks of stream `name` at offsets `[from, until)` the run's
    /// journal holds (at most `cap` bytes of them, one at least), the
    /// stream's length and end, the run's end and last LSN, and its ID.
    fn scan_stream(
        &mut self,
        run: RunId,
        name: &str,
        from: u64,
        until: u64,
        cap: usize,
    ) -> Result<StreamScan, Error> {
        let (records, last_lsn, decoded) = match self.core.get(run) {
            Some(l) => {
                let last = l.last_lsn;
                (self.journal_of(run)?, last, false)
            }
            None => match self.core.parked(run) {
                Some(p) => {
                    let s = self.read_stored(p.blob())?;
                    (s.records, s.last_lsn, true)
                }
                None => return Err(Error::NotFound(format!("run {run}"))),
            },
        };
        let mut scan = StreamScan {
            chunks: Vec::new(),
            more: false,
            len: 0,
            end: None,
            done: None,
            last_lsn,
            id: None,
        };
        let mut bytes = 0;
        for r in &records {
            match r {
                Record::Start { id, .. } if scan.id.is_none() => scan.id = Some(id.clone()),
                Record::Chunk {
                    stream,
                    offset,
                    payload,
                    ..
                } if &**stream == name => {
                    scan.len = scan.len.max(offset + 1);
                    if *offset < from || *offset >= until || scan.more {
                        continue;
                    }
                    if !scan.chunks.is_empty() && bytes + payload.len() > cap {
                        scan.more = true;
                        continue;
                    }
                    if *offset == from + scan.chunks.len() as u64 {
                        bytes += payload.len();
                        scan.chunks.push(payload.clone());
                    }
                }
                Record::StreamEnd {
                    stream,
                    status,
                    chunks,
                    ..
                } if &**stream == name => {
                    scan.end = Some(*status);
                    scan.len = scan.len.max(*chunks);
                }
                Record::Done { status, .. } => scan.done = Some(*status),
                _ => {}
            }
        }
        if decoded {
            self.reuse_decoded(records);
        }
        Ok(scan)
    }

    /// Reads the chunks of stream `name` from `from` up to its log (all of
    /// them, when it has none) out of the run's journal and into the log,
    /// which this read and the ones after it are answered from. Every chunk
    /// outside a log is durable. A stream too large to keep in memory gets
    /// one batch straight from the journal instead, returned here.
    fn fill_stream_log(
        &mut self,
        run: RunId,
        name: &str,
        from: u64,
        first: Option<u64>,
    ) -> Result<Option<StreamRead>, Error> {
        let mut s = self.scan_stream(
            run,
            name,
            from,
            first.unwrap_or(u64::MAX),
            STREAM_CACHE_BYTES,
        )?;
        let commit = self.commit;
        let rs = self.streams.entry(run).or_default();
        if rs.id.is_none() {
            rs.id = s.id.clone();
        }
        if rs.done.is_none()
            && let Some(status) = s.done
        {
            rs.done = Some((if s.last_lsn <= commit { 0 } else { s.last_lsn }, status));
        }
        let bytes: usize = s.chunks.iter().map(|c| c.len()).sum();
        let fits = !s.more && first.is_none_or(|f| from + s.chunks.len() as u64 == f);
        if !fits {
            // One batch from the journal; the log stays as it was.
            let mut taken = 0;
            let mut n = 0;
            for c in &s.chunks {
                if n > 0
                    && (taken + c.len() > crate::stream::BATCH_BYTES
                        || n >= crate::stream::BATCH_CHUNKS)
                {
                    break;
                }
                taken += c.len();
                n += 1;
            }
            s.chunks.truncate(n);
            return Ok(Some(StreamRead {
                run,
                from,
                chunks: s.chunks,
                end: None,
                wait: None,
                id: s.id,
            }));
        }
        match rs.logs.iter_mut().find(|l| &*l.name == name) {
            Some(log) => {
                for c in s.chunks.into_iter().rev() {
                    log.chunks.push_front((0, c));
                }
                log.first = from.min(log.first);
                log.bytes += bytes;
            }
            None => rs.logs.push(StreamLog {
                name: Arc::from(name),
                first: from.min(s.len),
                chunks: s.chunks.into_iter().map(|c| (0, c)).collect(),
                bytes,
                len: s.len,
                end: s.end.map(|status| (0, status)),
            }),
        }
        self.stream_bytes += bytes;
        Ok(None)
    }

    /// The durable chunks of stream `name` of `run` from offset `from`,
    /// and its end once the read has every chunk.
    fn read_stream(&mut self, run: RunId, name: &str, from: u64) -> Result<StreamRead, Error> {
        let commit = if self.cfg.sabotage == Sabotage::EarlyChunks {
            Lsn::MAX
        } else {
            self.commit
        };
        let mut out = StreamRead {
            run,
            from,
            chunks: Vec::new(),
            end: None,
            wait: None,
            id: None,
        };
        let first = self
            .streams
            .get(&run)
            .and_then(|rs| rs.logs.iter().find(|l| &*l.name == name))
            .map(|l| l.first);
        if first.is_none_or(|f| from < f)
            && let Some(direct) = self.fill_stream_log(run, name, from, first)?
        {
            return Ok(direct);
        }
        let now = self.tick_now();
        let Some(rs) = self.streams.get_mut(&run) else {
            return Ok(out);
        };
        rs.read_at = now;
        out.id = rs.id.clone();
        let done = rs.done;
        let Some(log) = rs.logs.iter().find(|l| &*l.name == name) else {
            return Ok(out);
        };
        if from < log.first {
            return Ok(out);
        }
        let mut bytes = 0;
        let skip = usize::try_from(from - log.first).unwrap_or(usize::MAX);
        if skip < log.chunks.len() {
            for (lsn, p) in log.chunks.range(skip..) {
                if *lsn > commit {
                    out.wait = Some(*lsn);
                    return Ok(out);
                }
                if !out.chunks.is_empty()
                    && (bytes + p.len() > crate::stream::BATCH_BYTES
                        || out.chunks.len() >= crate::stream::BATCH_CHUNKS)
                {
                    return Ok(out);
                }
                bytes += p.len();
                out.chunks.push(p.clone());
            }
        }
        if from + out.chunks.len() as u64 >= log.len {
            let closed = log.end.is_some();
            match log.end.or(done) {
                Some((lsn, status)) if lsn <= commit => out.end = Some(end_of(status, closed)),
                Some((lsn, _)) => out.wait = Some(lsn),
                None => {}
            }
        }
        Ok(out)
    }

    /// A read's answer, as every host gets it (JSON, chunks decrypted).
    fn stream_answer(&self, r: &StreamRead) -> Result<Payload, Error> {
        let mut chunks = Vec::with_capacity(r.chunks.len());
        for c in &r.chunks {
            chunks.push(self.open(c.clone())?);
        }
        Ok(Arc::from(crate::stream::encode_batch(
            r.run,
            r.from,
            &chunks,
            r.end.as_ref(),
        )))
    }

    /// Answers the reads waiting on `run` that now have something (or, with
    /// `now`, whose wait is over); the others go on waiting.
    fn answer_readers(&mut self, run: RunId, now: Option<u64>) {
        let Some(waiters) = self.stream_waiters.remove(&run) else {
            return;
        };
        let mut keep = Vec::new();
        for w in waiters {
            let r = self.read_stream(run, &w.name, w.from);
            let answer = match r {
                Ok(r)
                    if !r.chunks.is_empty()
                        || r.end.is_some()
                        || now.is_some_and(|t| t >= w.deadline_ms) =>
                {
                    self.stream_answer(&r)
                }
                Ok(r) => {
                    if let Some(lsn) = r.wait {
                        self.stream_due.push(Reverse((lsn, run)));
                    }
                    keep.push(w);
                    continue;
                }
                Err(e) => Err(e),
            };
            self.released.push(match answer {
                Ok(payload) => Output::Reply {
                    token: w.token,
                    status: Status::Ok,
                    payload,
                },
                Err(e) => Output::Reply {
                    token: w.token,
                    status: Status::Failed,
                    payload: Arc::from(e.to_string().as_bytes()),
                },
            });
        }
        if !keep.is_empty() {
            self.stream_waiters.insert(run, keep);
        }
    }

    /// Readers of runs whose streams grew durably, and reads whose wait is
    /// over.
    fn wake_stream_readers(&mut self) {
        while let Some(Reverse((lsn, run))) = self.stream_due.peek().copied() {
            if lsn > self.commit && self.cfg.sabotage != Sabotage::EarlyChunks {
                break;
            }
            self.stream_due.pop();
            self.answer_readers(run, None);
        }
    }

    fn expire_stream_reads(&mut self) {
        if self.stream_waiters.is_empty() {
            self.stream_wait_until = u64::MAX;
            return;
        }
        let now = self.now();
        if now < self.stream_wait_until {
            return;
        }
        let runs: Vec<RunId> = self.stream_waiters.keys().copied().collect();
        for run in runs {
            self.answer_readers(run, Some(now));
        }
        self.stream_wait_until = self
            .stream_waiters
            .values()
            .flatten()
            .map(|w| w.deadline_ms)
            .min()
            .unwrap_or(u64::MAX);
    }

    /// Drops the logs of runs that left memory once every chunk in them is
    /// durable and no reader waits on them.
    fn drop_stream_logs(&mut self) {
        if self.streams.is_empty() {
            return;
        }
        let commit = self.commit;
        let now = self.tick_now();
        let core = &self.core;
        let waiters = &self.stream_waiters;
        let mut freed = 0;
        self.streams.retain(|run, rs| {
            let durable = |at: Option<Lsn>| at.is_none_or(|l| l <= commit);
            let settled = durable(rs.done.map(|d| d.0))
                && rs
                    .logs
                    .iter()
                    .all(|l| durable(l.chunks.back().map(|c| c.0)) && durable(l.end.map(|e| e.0)));
            let keep = !settled
                || waiters.contains_key(run)
                || now.saturating_sub(rs.read_at) < STREAM_READ_KEEP_MS
                || core.get(*run).is_some_and(|l| !l.is_done());
            if !keep {
                freed += rs.logs.iter().map(|l| l.bytes).sum::<usize>();
            }
            keep
        });
        self.stream_bytes = self.stream_bytes.saturating_sub(freed);
        if self.streams.capacity() > 1024 && self.streams.capacity() > 4 * self.streams.len() {
            self.streams.shrink_to(self.streams.len() * 2);
        }
    }

    /// The run a stream read is for: `run` if given (a reader keeps reading
    /// the run it started on), else the ID's newest.
    fn stream_run(&mut self, id: &str, run: Option<RunId>) -> Result<RunId, Error> {
        let not_found = || Error::NotFound(id.to_owned());
        let run = match run {
            Some(r) => r,
            None => match self.core.find(id) {
                Some(Found::Loaded(r) | Found::Parked(r)) => r,
                None => return Err(not_found()),
            },
        };
        match self.core.get(run) {
            Some(l) if *l.id == *id => Ok(run),
            Some(_) => Err(not_found()),
            None if self.core.parked(run).is_some() => Ok(run),
            None => Err(not_found()),
        }
    }

    /// Wakes the run's code for no particular future (a gate opened, an
    /// update or a cancellation arrived): combinators poll every branch.
    fn wake(&mut self, run: RunId) {
        if let Some(slot) = self.slots.get_mut(&run) {
            slot.broadcasts += 1;
        }
        self.schedule(run);
    }

    /// Queues the run's code to be driven. What the drive delivers wakes
    /// the future it resolves.
    fn schedule(&mut self, run: RunId) {
        self.mark_active(run);
        if let Some(slot) = self.slots.get_mut(&run) {
            if slot.foreign && !slot.wake_queued {
                slot.wake_queued = true;
                self.foreign.push_back(ForeignEvent::Wake { run });
            }
            slot.waker.wake_local();
        } else if self.core.get(run).is_some() || self.core.is_parked(run) {
            self.stash.entry(run).or_default().push(Stashed::Wake);
        }
    }

    fn complete(&mut self, run: RunId, seq: Seq, c: Completion) {
        match self.slots.get_mut(&run) {
            Some(slot) => {
                if slot.machine.complete(seq, c) {
                    self.note_undelivered(run);
                    self.schedule(run);
                }
            }
            None => self
                .stash
                .entry(run)
                .or_default()
                .push(Stashed::Complete(seq, c)),
        }
    }

    /// The run is doing something: the park sweep looks at it again, and a
    /// pending park of it is stale.
    fn mark_active(&mut self, run: RunId) {
        if let Some(slot) = self.slots.get_mut(&run) {
            slot.suspended_since = None;
            if !slot.dirty {
                slot.dirty = true;
                self.dirty.push(run);
            }
        }
    }

    /// A foreign run has a delivery its host has not pumped yet.
    fn note_undelivered(&mut self, run: RunId) {
        if let Some(slot) = self.slots.get_mut(&run)
            && slot.foreign
            && !slot.undelivered
        {
            slot.undelivered = true;
            self.undelivered.push(run);
        }
    }

    /// Whether some foreign run still has deliveries to pump. Drops runs
    /// whose queue drained, so the list follows activity, not population.
    fn has_undelivered(&mut self) -> bool {
        let slots = &mut self.slots;
        self.undelivered.retain(|run| match slots.get_mut(run) {
            Some(slot) if slot.machine.has_queued_deliveries() => true,
            Some(slot) => {
                slot.undelivered = false;
                false
            }
            None => false,
        });
        !self.undelivered.is_empty()
    }

    /// Removes a run's slot; the runtime drops its code at the end of the tick.
    fn remove_slot(&mut self, run: RunId) -> Option<Removed> {
        let (idx, mut slot) = self.slots.remove(&run)?;
        self.removed.push((run, idx));
        if !self.step_began.is_empty() {
            self.step_began.retain(|(r, _), _| *r != run);
        }
        let foreign = slot.foreign;
        // Kept for the next run that loads or starts (see `new_slot`).
        if self.slot_spare.len() < SLOT_SPARE && slot.machine.reusable() {
            slot.machine.release();
            slot.resolved = None;
            self.slot_spare.push(slot);
        }
        Some(Removed { foreign })
    }

    /// Gives `run` a slot whose machine is built from `journal` (see
    /// [`RunMachine::new`]), reusing the memory of a slot a run left.
    fn new_slot(
        &mut self,
        run: RunId,
        journal: JournalIn<'_>,
        gate: Lsn,
        foreign: bool,
    ) -> Result<(), fusee_core::run::DeterminismError> {
        let slot = match self.slot_spare.pop() {
            Some(mut slot) => {
                match journal {
                    JournalIn::Borrowed(j) => slot.machine.rebuild(j, gate)?,
                    JournalIn::Decoded(j) => slot.machine.rebuild_drain(j, gate)?,
                }
                // The waker too, unless code of the old run kept a clone.
                match Arc::get_mut(&mut slot.waker) {
                    Some(w) => {
                        w.run = run;
                        *w.queued.get_mut() = false;
                        *w.woken.get_mut() = false;
                    }
                    None => slot.waker = self.new_waker(run),
                }
                let Slot {
                    machine: _,
                    waker: _,
                    foreign: f,
                    suspended_since,
                    dirty,
                    broadcasts,
                    wake_queued,
                    gate_at,
                    undelivered,
                    resolved,
                } = &mut *slot;
                *f = foreign;
                *suspended_since = None;
                *dirty = true;
                *broadcasts = 0;
                *wake_queued = false;
                *gate_at = None;
                *undelivered = false;
                *resolved = None;
                slot
            }
            None => Box::new(Slot {
                machine: match journal {
                    JournalIn::Borrowed(j) => RunMachine::new(j, gate)?,
                    JournalIn::Decoded(j) => {
                        let m = RunMachine::new(j, gate)?;
                        j.clear();
                        m
                    }
                },
                waker: self.new_waker(run),
                foreign,
                suspended_since: None,
                dirty: true,
                broadcasts: 0,
                wake_queued: false,
                gate_at: None,
                undelivered: false,
                resolved: None,
            }),
        };
        // A key's run (an object) starts from the state its start holds.
        let object = (slot.machine.info().policy & fusee_core::record::policy::OBJECT != 0)
            .then(|| slot.machine.info().input.clone());
        self.slots.insert(run, slot);
        self.dirty.push(run);
        if let Some(input) = object {
            self.object_on_slot(run, input);
        }
        Ok(())
    }

    fn new_waker(&self, run: RunId) -> Arc<RunWaker> {
        Arc::new(RunWaker {
            run,
            queued: AtomicBool::new(false),
            woken: AtomicBool::new(false),
            ready: self.ready.clone(),
        })
    }

    /// Creates the journal machine for a loaded run and queues its code.
    /// `journal`, decoded for this run alone, is left empty.
    fn prepare(&mut self, run: RunId, journal: &mut Vec<RecordBuf>) {
        let Some(l) = self.core.get(run) else {
            return;
        };
        if l.is_done() || self.slots.contains_key(&run) {
            return;
        }
        let gate = l.last_lsn;
        let foreign = self.runs_foreign(&l.wtype, l.version);
        match self.new_slot(run, JournalIn::Decoded(journal), gate, foreign) {
            Ok(()) => self.instantiate.push(run),
            Err(e) => {
                self.metrics.determinism_errors += 1;
                self.stuck.insert(run, e.to_string());
            }
        }
    }

    /// The run's whole journal: stored prefix plus in-memory suffix.
    fn journal_of(&mut self, run: RunId) -> Result<Vec<RecordBuf>, Error> {
        let (blob, suffix) = match self.core.get(run) {
            Some(l) => (l.blob, l.journal.clone()),
            None => match self.core.parked(run) {
                Some(p) => (Some(p.blob()), Vec::new()),
                None => return Ok(Vec::new()),
            },
        };
        let mut out = Vec::new();
        if let Some(b) = blob {
            out.extend(self.blob_records(b)?);
        }
        out.extend(decode_canonical_all(&suffix).map_err(|e| Error::Io(e.to_string()))?);
        Ok(out)
    }

    fn blob_records(&mut self, b: BlobRef) -> Result<Vec<RecordBuf>, Error> {
        Ok(self.read_stored(b)?.records)
    }

    /// The journal stored at `b`: the newest frame and the frames it extends.
    fn read_chain(&mut self, b: BlobRef) -> Result<Chain, Error> {
        let frame = read_store(&mut self.env, &self.tail, b.offset, b.len)?;
        self.chain_from(b, &frame)
    }

    /// [`Self::read_chain`] with the newest frame's bytes already read.
    fn chain_from(&mut self, b: BlobRef, frame: &[u8]) -> Result<Chain, Error> {
        let mut newest = read_entry(frame).map_err(io_err)?;
        let run = newest.run();
        let (last_lsn, id_hash, timers) = match &mut newest {
            StoreEntry::Park {
                last_lsn,
                id_hash,
                timers,
                ..
            }
            | StoreEntry::Delta {
                last_lsn,
                id_hash,
                timers,
                ..
            } => (*last_lsn, *id_hash, std::mem::take(timers)),
            StoreEntry::Done {
                last_lsn, id_hash, ..
            } => (*last_lsn, *id_hash, Vec::new()),
            StoreEntry::Tombstone { .. } | StoreEntry::Index { .. } => (0, 0, Vec::new()),
        };
        let env = &mut self.env;
        let tail = &self.tail;
        let mut failed = None;
        let read = chain_journal(newest, b.len, &mut |at| match read_store(
            env, tail, at.offset, at.len,
        ) {
            Ok(f) => read_entry(&f),
            Err(e) => {
                failed = Some(e);
                Err(fusee_core::wire::DecodeError(
                    "unreadable frame in a delta chain",
                ))
            }
        });
        if let Some(e) = failed {
            return Err(e.into());
        }
        let (journal, chain, depth) = read.map_err(io_err)?;
        self.metrics.journal_frames_read += 1 + u64::from(depth);
        self.metrics.journal_bytes_read += chain;
        Ok(Chain {
            run,
            journal,
            last_lsn,
            id_hash,
            timers,
            chain,
            depth,
        })
    }

    fn read_stored(&mut self, b: BlobRef) -> Result<Stored, Error> {
        self.stored_from(b, None)
    }

    /// [`Self::read_stored`], with the newest frame's bytes when they were
    /// read already.
    fn stored_from(&mut self, b: BlobRef, frame: Option<&[u8]>) -> Result<Stored, Error> {
        let read;
        let frame = match frame {
            Some(f) => f,
            None => {
                read = read_store(&mut self.env, &self.tail, b.offset, b.len)?;
                &read
            }
        };
        // Decoded into a reused list with the shard's name cache.
        let mut records = std::mem::take(&mut self.decoded);
        // A journal in one frame decodes from the frame's bytes in place.
        if let Some((_, last_lsn, journal)) =
            fusee_core::store::read_whole_journal(frame).map_err(io_err)?
        {
            self.metrics.journal_frames_read += 1;
            self.metrics.journal_bytes_read += u64::from(b.len);
            decode_canonical_into(journal, &mut self.names, &mut records).map_err(io_err)?;
            return Ok(Stored {
                records,
                last_lsn,
                chain: u64::from(b.len),
                depth: 0,
            });
        }
        let c = self.chain_from(b, frame)?;
        decode_canonical_into(&c.journal, &mut self.names, &mut records).map_err(io_err)?;
        Ok(Stored {
            records,
            last_lsn: c.last_lsn,
            chain: c.chain,
            depth: c.depth,
        })
    }

    /// Keeps a decoded journal's list for the next load.
    fn reuse_decoded(&mut self, mut records: Vec<RecordBuf>) {
        if records.capacity() <= 1024 {
            records.clear();
            self.decoded = records;
        }
    }

    /// Writes the run store frames appended since the last call.
    fn store_flush(&mut self) -> Result<(), Error> {
        if self.tail.buf.is_empty() {
            return Ok(());
        }
        let written = self.env.store_append(&self.tail.buf);
        let expected = self.tail.at;
        self.tail.buf.clear();
        if self.tail.buf.capacity() > 4 * STORE_TAIL_MAX {
            self.tail.buf = Vec::new();
        }
        if written? != expected {
            return Err(Error::Io(
                "run store frames were written at an unexpected offset".into(),
            ));
        }
        Ok(())
    }

    /// [`Self::store_flush`], failing the shard on an error.
    fn store_flush_or_fail(&mut self) {
        if let Err(e) = self.store_flush() {
            self.fail(format!("run store write failed: {e}"));
        }
    }

    /// The run store's length, frames not yet written included.
    fn store_len(&self) -> u64 {
        if self.tail.buf.is_empty() {
            self.env.store_len()
        } else {
            self.tail.at + self.tail.buf.len() as u64
        }
    }

    /// Loads a parked run back without preparing its code (recovery).
    fn unpark_stored(&mut self, run: RunId) -> Result<Option<Stored>, Error> {
        self.unpark_from(run, None)
    }

    /// [`Self::unpark_stored`]; `frame`: the bytes read for the frame at
    /// that location, used if it is still the run's newest.
    fn unpark_from(
        &mut self,
        run: RunId,
        frame: Option<(BlobRef, &[u8])>,
    ) -> Result<Option<Stored>, Error> {
        let Some(p) = self.core.parked(run) else {
            return Ok(None);
        };
        let frame = frame.filter(|(b, _)| *b == p.blob()).map(|(_, f)| f);
        let s = self.stored_from(p.blob(), frame)?;
        let now = self.now();
        if !self.core.unpark(run, s.last_lsn, &s.records, now) {
            return Err(Error::Io(format!(
                "run {run}: stored journal is unreadable"
            )));
        }
        self.core.spilled(run, p.blob(), s.chain, s.depth);
        Ok(Some(s))
    }

    /// Loads a parked run back into memory and prepares its code.
    fn load(&mut self, run: RunId) -> Result<(), Error> {
        self.load_from(run, None)
    }

    /// [`Self::load`], with the newest frame's bytes when they were read
    /// already (see [`Self::unpark_from`]).
    fn load_from(&mut self, run: RunId, frame: Option<(BlobRef, &[u8])>) -> Result<(), Error> {
        let Some(mut s) = self.unpark_from(run, frame)? else {
            return Ok(());
        };
        self.metrics.runs_loaded += 1;
        if self.core.get(run).is_some_and(|l| !l.is_done()) {
            self.prepare(run, &mut s.records);
            self.core.reoffer(run);
        }
        self.reuse_decoded(s.records);
        Ok(())
    }

    fn ensure_loaded(&mut self, run: RunId) -> Result<(), Error> {
        if self.core.is_parked(run) {
            self.load(run)?;
        }
        Ok(())
    }

    /// Writes a finished run's summary to the run store and parks it.
    fn park_done(&mut self, run: RunId) -> Result<(), Error> {
        let Some(l) = self.core.get(run) else {
            return Ok(());
        };
        if l.done.is_none() {
            return Ok(());
        }
        // A run that finishes after it was loaded from its stored frames:
        // what it appended since (its last records and `Done`, already in
        // canonical form) chained to those frames, instead of a frame
        // encoded anew. The chain is its whole journal; compaction writes
        // the `Done` frame later.
        if !self.cfg.keep_history
            && !self.cfg.archive_history
            && self.store_v3
            && let Some(prev) = l.blob
            && l.blob_depth < MAX_CHAIN
            && !l.journal.is_empty()
        {
            let entry = EntryRef::Delta {
                run,
                last_lsn: l.last_lsn,
                id_hash: hash_id(&l.id),
                timers: &[],
                prev,
                depth: l.blob_depth + 1,
                chain: l.blob_chain,
                journal: &l.journal,
            };
            let blob = self.tail.put(self.env.store_len(), entry)?;
            self.core.park(run, blob, true);
            if self.tail.full() {
                self.store_flush()?;
            }
            return Ok(());
        }
        let streamed = l.streams;
        // A run that did not succeed keeps its whole journal for its
        // retention whatever `keep_history` says: what failed, and the steps
        // before it, are what someone will look for, and it can be reset
        // or retried. Successful runs keep their start and result.
        let unsuccessful = l.done.is_some_and(|(s, _)| {
            matches!(
                s,
                Status::Failed | Status::Cancelled | Status::TimedOut | Status::Terminated
            )
        });
        let history = if self.cfg.keep_history || unsuccessful {
            Some(self.journal_of(run)?)
        } else {
            None
        };
        // A finished run's streams stay readable for its retention: its
        // summary keeps them.
        let streams: Vec<RecordBuf> = if history.is_none() && streamed {
            self.journal_of(run)?
                .into_iter()
                .filter(|r| matches!(r, Record::Chunk { .. } | Record::StreamEnd { .. }))
                .collect()
        } else {
            Vec::new()
        };
        let Some(l) = self.core.get(run) else {
            return Ok(());
        };
        let Some((status, finished_ms)) = l.done else {
            return Ok(());
        };
        let (last_lsn, id_hash) = (l.last_lsn, hash_id(&l.id));
        let mut journal = std::mem::take(&mut self.enc);
        journal.clear();
        match history {
            Some(records) => {
                for r in records {
                    encode_canonical(&r, &mut journal);
                }
            }
            None => {
                let start: RecordRef<'_> = Record::Start {
                    run,
                    id: &l.id,
                    wtype: &l.wtype,
                    version: l.version,
                    codec: l.codec,
                    started_ms: l.started_ms,
                    deadline_ms: l.deadline_ms,
                    parent: l.parent,
                    policy: l.policy,
                    from: l.from,
                    copies: 0,
                    update: None,
                    input: &[],
                    meta: None,
                };
                encode_canonical(&start, &mut journal);
                if !l.attrs.is_empty() {
                    let attrs = merge_attributes(&l.attrs);
                    if attrs != "{}" {
                        let a: RecordRef<'_> = Record::Attributes {
                            run,
                            seq: 0,
                            payload: attrs.as_bytes(),
                        };
                        encode_canonical(&a, &mut journal);
                    }
                }
                for r in &streams {
                    encode_canonical(r, &mut journal);
                }
                let done: RecordRef<'_> = Record::Done {
                    run,
                    status,
                    finished_ms,
                    payload: l.result.as_deref().unwrap_or(&[]),
                    by: None,
                };
                encode_canonical(&done, &mut journal);
            }
        }
        let entry = EntryRef::Done {
            run,
            last_lsn,
            id_hash,
            status,
            finished_ms,
            journal: &journal,
        };
        let blob = if self.cfg.archive_history {
            let mut frame = Vec::new();
            encode_entry_into(entry, &mut frame);
            self.store_flush()?;
            let len = u32::try_from(frame.len())
                .map_err(|_| Error::Io("run store frame exceeds 4 GiB".into()))?;
            let offset = self.env.store_append_history(&frame, run, last_lsn)?;
            BlobRef { offset, len }
        } else {
            self.tail.put(self.env.store_len(), entry)?
        };
        self.enc = journal;
        if let Some(l) = self.core.get(run)
            && l.blob.is_some()
        {
            self.obsolete_store_bytes += l.blob_chain;
        }
        self.core.park(run, blob, false);
        if self.tail.full() {
            self.store_flush()?;
        }
        Ok(())
    }

    /// Stores an active run's journal: a `Delta` frame with the records
    /// appended since its newest frame, or (with no frame, a chain at its
    /// length limit, or a format 2 store) the whole journal. The bytes
    /// already in the store use the same canonical encoding as the suffix,
    /// including encrypted payloads. Returns the newest frame, the bytes of
    /// its chain and how many frames precede it.
    fn store_active(&mut self, run: RunId) -> Result<(BlobRef, u64, u8), Error> {
        let l = self
            .core
            .get(run)
            .ok_or_else(|| Error::Io("missing active run".into()))?;
        let old = l.blob;
        let (old_chain, old_depth) = (l.blob_chain, l.blob_depth);
        if let Some(b) = old
            && l.journal.is_empty()
        {
            // Nothing new since the stored frames (their timers follow from
            // the same records).
            return Ok((b, old_chain, old_depth));
        }
        let delta = old.filter(|_| self.store_v3 && old_depth < MAX_CHAIN);
        let mut whole = Vec::new();
        if let (None, Some(b)) = (delta, old) {
            let c = self.read_chain(b)?;
            if c.run != run || c.journal.is_empty() {
                return Err(Error::Io("wrong active journal in run store".into()));
            }
            whole = c.journal;
        }
        let l = self
            .core
            .get(run)
            .ok_or_else(|| Error::Io("missing active run".into()))?;
        let (last_lsn, id_hash) = (l.last_lsn, hash_id(&l.id));
        let mut timers = std::mem::take(&mut self.timer_scratch);
        timers.clear();
        timers.extend(l.armed_timers());
        let store_len = self.env.store_len();
        let put = match delta {
            Some(prev) => self
                .tail
                .put(
                    store_len,
                    EntryRef::Delta {
                        run,
                        last_lsn,
                        id_hash,
                        timers: &timers,
                        prev,
                        depth: old_depth + 1,
                        chain: old_chain,
                        journal: &l.journal,
                    },
                )
                .map(|blob| (blob, old_chain + u64::from(blob.len), old_depth + 1)),
            None => {
                whole.extend_from_slice(&l.journal);
                self.tail
                    .put(
                        store_len,
                        EntryRef::Park {
                            run,
                            last_lsn,
                            id_hash,
                            timers: &timers,
                            journal: &whole,
                        },
                    )
                    .map(|blob| (blob, u64::from(blob.len), 0))
            }
        };
        self.timer_scratch = timers;
        let stored = put?;
        if stored.2 == 0 && old.is_some() {
            self.obsolete_store_bytes += old_chain;
        }
        if self.tail.full() {
            self.store_flush()?;
        }
        Ok(stored)
    }

    /// Writes a suspended run's journal to the run store and parks it.
    fn park(&mut self, run: RunId) -> Result<bool, Error> {
        if self.core.get(run).is_none_or(|l| l.is_done()) {
            return Ok(false);
        }
        if self.object_idle(run)
            && self
                .slots
                .get(&run)
                .is_some_and(|s| s.machine.journal_size() > 0)
        {
            // A key leaves one record behind, not its calls since the last
            // compaction: the new run parks at a later sweep.
            if let Some(new) = self.object_compact(run)? {
                self.object_release(new)?;
            }
            return Ok(false);
        }
        if self.core.get(run).is_some_and(|l| l.has_queued_events()) {
            // A timer fired for it and waits in the event queue: look at it
            // again at the next sweep.
            self.mark_active(run);
            return Ok(false);
        }
        let (blob, _, depth) = self.store_active(run)?;
        self.core.park(run, blob, depth > 0);
        self.object_parked(run);
        if self.remove_slot(run).is_some_and(|slot| slot.foreign) {
            // The host can discard this execution. On a wake, load() prepares
            // a new slot from the stored journal and emits a fresh Start.
            self.foreign.push_back(ForeignEvent::Ended { run });
        }
        self.current_step.remove(&run);
        self.metrics.runs_parked += 1;
        Ok(true)
    }

    /// Ends a run from outside its code (termination, deadline).
    fn end_run(&mut self, run: RunId, status: Status, message: &str) -> Result<Option<Lsn>, Error> {
        self.ensure_loaded(run)?;
        if self.core.get(run).is_none_or(|l| l.is_done()) {
            return Ok(None);
        }
        let now = self.now();
        let rec = Record::Done {
            run,
            status,
            finished_ms: now,
            payload: Arc::from(message.as_bytes()),
            by: self.by.take(),
        };
        let lsn = self.append(&rec)?;
        if let Some(slot) = self.remove_slot(run)
            && slot.foreign
        {
            self.foreign.push_back(ForeignEvent::Ended { run });
        }
        self.release_waiting_requests(run);
        Ok(Some(lsn))
    }

    fn process_events(&mut self) -> bool {
        /// Loads per call. The rest wait for the next round, after the runs
        /// loaded so far were driven and their records flushed, so a mass
        /// wake continues its first runs long before the last one loads.
        const LOAD_CHUNK: usize = 1024;
        /// Loads per call while loaded runs take more than the memory
        /// budget: enough to always make progress (some loaded runs may be
        /// unable to park), few enough that finishing runs keep up.
        const OVER_BUDGET: usize = 32;
        // A foreign host takes a bounded batch of events per tick: loads
        // stop when the runs loaded would fill it, so a mass wake reaches
        // the host's code in small waves.
        let share = |i: &Self| {
            let share = if i.over_budget() {
                OVER_BUDGET
            } else {
                LOAD_CHUNK
            };
            share.min(i.load_room.saturating_sub(i.foreign.len()))
        };
        let mut any = false;
        let mut loads = 0;
        // Loads held back earlier go first.
        while loads < share(self) && !self.held_loads.is_empty() {
            let n = (share(self) - loads).min(self.held_loads.len());
            let batch: Vec<RunId> = self.held_loads.drain(..n).collect();
            loads += n;
            any = true;
            self.load_batch(batch);
        }
        while let Some(ev) = self.core.take_event() {
            any = true;
            if let Event::NeedLoad { run } = ev {
                // Load consecutive requests together in run-store order, so
                // the reads walk the file forward.
                let mut batch = std::mem::take(&mut self.load_buf);
                batch.clear();
                batch.push(run);
                while batch.len() + loads < LOAD_CHUNK
                    && matches!(self.core.peek_event(), Some(Event::NeedLoad { .. }))
                {
                    if let Some(Event::NeedLoad { run }) = self.core.take_event() {
                        batch.push(run);
                    }
                }
                if loads + batch.len() > share(self) || !self.held_loads.is_empty() {
                    // Past this call's share (smaller while loaded runs take
                    // the memory budget): these load in a later round, in
                    // order, while the other events go on (runs already
                    // loaded need them to continue and make room).
                    self.held_loads.extend(batch.drain(..));
                    self.load_buf = batch;
                    continue;
                }
                loads += batch.len();
                self.load_batch(batch);
                continue;
            }
            match ev {
                // Slots (and foreign start events) are created by `start`
                // and by loading; the core's notice adds nothing.
                Event::Started { .. } => {}
                Event::Timer { run, seq, kind } => match kind {
                    TimerKind::Sleep => self.complete(run, seq, Completion::Fired),
                    TimerKind::WaitTimeout => self.complete(run, seq, Completion::WaitTimedOut),
                    TimerKind::Retry => self.wake(run),
                    TimerKind::Deadline => {
                        if let Err(e) =
                            self.end_run(run, Status::TimedOut, "workflow deadline exceeded")
                        {
                            self.fail(e.to_string());
                        }
                    }
                },
                Event::SignalReady {
                    run,
                    seq,
                    ordinal,
                    payload,
                } => self.complete(run, seq, Completion::Signal { ordinal, payload }),
                Event::Finished { run } => {
                    self.metrics.runs_finished += 1;
                    self.release_waiting_requests(run);
                    if let Some(slot) = self.remove_slot(run)
                        && slot.foreign
                    {
                        self.foreign.push_back(ForeignEvent::Ended { run });
                    }
                    self.current_step.remove(&run);
                    self.stash.remove(&run);
                    if let Err(e) = self.park_done(run) {
                        self.fail(e.to_string());
                    }
                }
                Event::NeedLoad { .. } => {}
            }
        }
        // The summaries of the runs that finished, in one write.
        self.store_flush_or_fail();
        any
    }

    /// Loads `batch` in run store order. Frames close together (runs
    /// parked together usually are) are read with one read per span.
    fn load_batch(&mut self, mut batch: Vec<RunId>) {
        /// Most bytes one read takes, and the most unwanted bytes between
        /// two wanted frames it reads through.
        const SPAN: u64 = 256 << 10;
        const GAP: u64 = 4 << 10;
        let mut at = std::mem::take(&mut self.load_at);
        at.clear();
        at.extend(
            batch
                .drain(..)
                .filter_map(|r| self.core.parked(r).map(|p| (p.blob(), r))),
        );
        // The buffers go back for the next batch.
        self.load_buf = batch;
        at.sort_unstable_by_key(|(b, r)| (b.offset, *r));
        // Frames past this are in the unwritten tail.
        let written = self.env.store_len();
        let mut span = Vec::new();
        let mut i = 0;
        while i < at.len() {
            let start = at[i].0.offset;
            let mut end = start + u64::from(at[i].0.len);
            let mut j = i + 1;
            while j < at.len() {
                let (b, _) = at[j];
                let b_end = b.offset + u64::from(b.len);
                if b.offset > end + GAP || b_end - start > SPAN || b_end > written {
                    break;
                }
                end = end.max(b_end);
                j += 1;
            }
            let read = if j - i > 1 {
                match self.env.store_read(start, (end - start) as u32) {
                    Ok(bytes) => {
                        span = bytes;
                        true
                    }
                    Err(e) => {
                        self.fail(e.to_string());
                        return;
                    }
                }
            } else {
                false
            };
            for &(b, run) in &at[i..j] {
                let frame = read.then(|| {
                    let from = (b.offset - start) as usize;
                    (b, &span[from..from + b.len as usize])
                });
                if let Err(e) = self.load_from(run, frame) {
                    self.fail(e.to_string());
                }
            }
            i = j;
        }
        self.load_at = at;
    }

    /// Whether loaded runs take more than the memory budget.
    fn over_budget(&self) -> bool {
        self.cfg.memory_budget != 0 && self.loaded_bytes() > self.cfg.memory_budget
    }

    fn fail(&mut self, message: String) {
        if self.failed.is_none() {
            self.failed = Some(message);
        }
    }

    fn send_out(&mut self, target: u32, msg: Message) {
        let id = self.next_out;
        self.next_out += 1;
        self.outbox.insert(
            id,
            Outgoing {
                id,
                target,
                msg,
                sent_ms: 0,
            },
        );
    }

    fn msg_token(&mut self, from: (u32, u64)) -> u64 {
        let t = self.next_msg_token;
        self.next_msg_token += 1;
        self.msg_tokens.insert(t, from);
        t
    }

    /// Acknowledges a received message once the current tail is durable.
    fn ack_at_tail(&mut self, from: (u32, u64)) {
        let t = self.msg_token(from);
        let tail = self.writer.last_lsn();
        self.core.add_output(tail, Output::Ack(t));
    }

    fn route_output(&mut self, o: Output) {
        let shards = self.cfg.shards;
        match o {
            Output::Ack(t) if t >= MSG_TOKEN => {
                if let Some(from) = self.msg_tokens.remove(&t) {
                    self.message_acks.push(from);
                }
            }
            Output::Reject(t) if t >= MSG_TOKEN => {
                if let Some(from) = self.msg_tokens.remove(&t) {
                    self.message_acks.push(from);
                }
            }
            Output::Ack(_) | Output::Reject(_) | Output::Reply { .. } | Output::Refuse { .. } => {
                self.released.push(o)
            }
            Output::Result {
                run,
                status,
                ref payload,
            } => {
                if !status.is_handover() {
                    for parent in self.subscribers.remove(&run).unwrap_or_default() {
                        self.send_out(
                            parent.shard,
                            Message::ChildResult {
                                parent,
                                status,
                                payload: payload.clone(),
                            },
                        );
                    }
                }
                let payload = if status == Status::Ok {
                    match self.open(payload.clone()) {
                        Ok(p) => p,
                        Err(e) => {
                            self.released.push(Output::Result {
                                run,
                                status: Status::Failed,
                                payload: Arc::from(e.to_string().as_bytes()),
                            });
                            return;
                        }
                    }
                } else {
                    payload.clone()
                };
                self.released.push(Output::Result {
                    run,
                    status,
                    payload,
                });
            }
            Output::UpdateResult {
                run,
                id,
                status,
                payload,
                ..
            } => {
                let tokens = self.update_waiters.remove(&(run, id)).unwrap_or_default();
                if tokens.is_empty() {
                    return;
                }
                let (status, payload) = match (status, self.open(payload.clone())) {
                    (Status::Ok, Ok(p)) => (Status::Ok, p),
                    (Status::Ok, Err(e)) => (Status::Failed, Arc::from(e.to_string().as_bytes())),
                    (s, _) => (s, payload),
                };
                for token in tokens {
                    self.released.push(Output::Reply {
                        token,
                        status,
                        payload: payload.clone(),
                    });
                }
            }
            Output::Close {
                target,
                wtype,
                parent,
                terminate,
                reason,
            } => {
                let shard = fusee_core::shard::shard_of(&target, shards);
                self.send_out(
                    shard,
                    Message::Close {
                        target: target.to_string(),
                        wtype: wtype.to_string(),
                        parent,
                        terminate,
                        reason,
                    },
                );
            }
            Output::ChildStart {
                parent,
                wtype,
                id,
                flags,
                deadline_ms,
                input,
            } => {
                let target = fusee_core::shard::shard_of(&id, shards);
                let trace = self.trace_of(parent.run);
                self.send_out(
                    target,
                    Message::ChildStart {
                        parent,
                        wtype: wtype.to_string(),
                        id: id.to_string(),
                        flags,
                        deadline_ms,
                        input,
                        trace,
                    },
                );
            }
            Output::ChildResult {
                parent,
                status,
                payload,
            } => self.send_out(
                parent.shard,
                Message::ChildResult {
                    parent,
                    status,
                    payload,
                },
            ),
            Output::Signal {
                target,
                name,
                payload,
                dedup,
            } => {
                let shard = fusee_core::shard::shard_of(&target, shards);
                self.send_out(
                    shard,
                    Message::Signal {
                        target: target.to_string(),
                        name: name.to_string(),
                        payload,
                        dedup,
                    },
                );
            }
        }
    }

    fn on_commit(&mut self) -> bool {
        if self.failed.is_some() {
            return false;
        }
        let mut any = false;
        let mut out = Vec::new();
        let upto = if self.cfg.sabotage == Sabotage::EarlyRelease {
            self.writer.last_lsn()
        } else {
            self.commit
        };
        self.core.release(upto, &mut out);
        any |= !out.is_empty();
        for o in out {
            self.route_output(o);
        }
        // One reading serves every gate this commit opens.
        let mut now_us = None;
        while let Some((lsn, run, began)) = self.gates.peek() {
            if lsn > self.commit {
                break;
            }
            self.gates.pop();
            if let Some(slot) = self.slots.get_mut(&run)
                && slot.gate_at.is_some_and(|g| g <= self.commit)
            {
                slot.gate_at = None;
            }
            let now_us = *now_us.get_or_insert_with(|| self.metric_us());
            let elapsed = now_us.saturating_sub(began);
            self.metrics.gate_wait_us_total += elapsed;
            #[cfg(feature = "opentelemetry")]
            if let Some(telemetry) = &self.cfg.telemetry {
                telemetry.gate(self.cfg.shard, elapsed);
            }
            self.wake(run);
            any = true;
        }
        if let Some((cp, _)) = &self.ckpt_pending
            && cp.lsn <= self.commit
            && !self.ckpt_running
            && let Some((cp, del)) = self.ckpt_pending.take()
        {
            self.ckpt_running = true;
            self.env.checkpoint(cp, del);
        }
        if !self.stream_due.is_empty() {
            self.wake_stream_readers();
        }
        any
    }

    fn set_durable(&mut self, lsn: Lsn, kind: StallKind) {
        let elapsed = self.metric_us().saturating_sub(self.flush_started_us);
        self.slow_flushes = if self.env.last_flush_us() > self.cfg.inline_max_us {
            self.slow_flushes.saturating_add(1)
        } else {
            0
        };
        self.metrics.flush_us_total += elapsed;
        #[cfg(feature = "opentelemetry")]
        if let Some(telemetry) = &self.cfg.telemetry {
            telemetry.flush(self.cfg.shard, elapsed);
        }
        if self.stall_us != 0 {
            self.trace_stall(kind, elapsed);
        }
        self.durable = self.durable.max(lsn);
        match self.replicator.as_mut() {
            Some(r) => {
                r.durable(self.durable);
                self.commit = self.commit.max(r.commit());
            }
            None => self.commit = self.durable,
        }
    }

    fn poll_io(&mut self) -> bool {
        let mut any = false;
        if let Some(res) = self.env.poll_flushed() {
            any = true;
            match res {
                Ok(lsn) => {
                    // The batches it covers are done; a later one may still
                    // be in flight.
                    let mut started = None;
                    while let Some(&(last, at)) = self.submitted.front() {
                        if last > lsn {
                            break;
                        }
                        started.get_or_insert(at);
                        self.submitted.pop_front();
                    }
                    if let Some(at) = started {
                        self.flush_started_us = at;
                    }
                    self.in_flight = self.submitted.back().map(|&(last, _)| last);
                    self.set_durable(lsn, StallKind::Flush);
                }
                Err(e) => {
                    self.in_flight = None;
                    self.submitted.clear();
                    self.fail(format!("log flush failed: {e}; the shard has stopped"));
                }
            }
        }
        if let Some(r) = self.replicator.as_ref() {
            let c = r.commit();
            if c > self.commit {
                self.commit = c;
                any = true;
            }
        }
        if let Some(res) = self.env.poll_checkpoint() {
            any = true;
            self.ckpt_running = false;
            match res {
                Ok(lsn) => {
                    self.last_ckpt_lsn = lsn;
                    self.metrics.checkpoints += 1;
                }
                Err(e) => self.fail(format!("checkpoint failed: {e}")),
            }
        }
        if any {
            self.on_commit();
        }
        any
    }

    fn poll_timers(&mut self) -> bool {
        if self.cfg.replay_only {
            return false;
        }
        let now = self.tick_now();
        let mut any = false;
        if self.core.next_timer().is_some_and(|t| t <= now) {
            self.core.poll_timers(now);
            // A mass wake fires in chunks: more may still be due.
            any = self.core.has_events() || self.core.next_timer().is_some_and(|t| t <= now);
        }
        while let Some(Reverse((at, id))) = self.timers.peek().copied() {
            if at > now {
                break;
            }
            self.timers.pop();
            if let Some(w) = self.timer_wakers.remove(&id) {
                w.wake();
                any = true;
            }
        }
        any
    }

    /// A run reached its durability gate while it is the shard's only work
    /// (no run is ready or waiting to be driven this round, the host has
    /// queued nothing, no event or foreign call is pending): flushes now,
    /// from inside its poll, the same flush the round would make once the
    /// run returned pending, which spares the run's wake, a round and a
    /// poll per step. A bounded number per round ([`ShardRuntime::drive_ready`]
    /// sets it), so timers and I/O completions are looked at about as often
    /// as without it; the host's queue is looked at before each.
    fn flush_at_gate(&mut self) -> bool {
        if self.gate_flushes == 0
            || self.drive_backlog > 0
            || self.in_flight.is_some()
            || self.failed.is_some()
            || !self.foreign.is_empty()
            || !self.instantiate.is_empty()
            || self.core.has_events()
            || self.ready.queued.load(Ordering::Acquire)
            || take_local(&self.ready, None)
            || match &self.host_queue {
                Some(q) => q.load(Ordering::Acquire) > 0,
                None => !self.host_never,
            }
        {
            return false;
        }
        self.gate_flushes -= 1;
        self.flush_pending(true, true)
    }

    /// Starts a flush if records are pending and none is in flight.
    fn maybe_flush(&mut self, idle: bool) -> bool {
        self.flush_pending(idle, false)
    }

    /// [`maybe_flush`](Self::maybe_flush). `lone`: a run's durability gate
    /// while the run is the shard's only work ([`Inner::flush_at_gate`]):
    /// the flush is inline whatever the disk's speed, since the shard would
    /// only wait for it, on a disk that is slow every time. So is an idle
    /// shard's while at most two shards are busy: on gp3, handing a 1 ms
    /// write to the log's threads and back costs about a quarter of a
    /// millisecond more than writing it inline.
    fn flush_pending(&mut self, idle: bool, lone: bool) -> bool {
        // A busy shard submits whenever records are pending, while the
        // environment has room for another batch: a record does not wait
        // for the shard's previous section to be durable.
        if self.failed.is_some()
            || (self.in_flight.is_some() && self.submitted.len() >= self.env.max_in_flight())
        {
            return false;
        }
        if !self.heartbeats.is_empty() {
            for (_, rec) in std::mem::take(&mut self.heartbeats) {
                let run = rec.run();
                let live = match (&rec, self.slots.get(&run)) {
                    (Record::Heartbeat { seq, .. }, Some(slot)) => {
                        slot.machine.is_live_pending(*seq)
                    }
                    _ => false,
                };
                if live && let Err(e) = self.append(&rec) {
                    self.fail(e.to_string());
                }
            }
        }
        if self.commit_dirty {
            // Not a flush: nothing was written (`flushed_records` and
            // `log_bytes` still count the records).
            self.commit_dirty = false;
            self.on_commit();
            return true;
        }
        if !self.writer.has_pending() {
            return false;
        }
        let Some(batch) = self.writer.take() else {
            return false;
        };
        let submitted_at = self.metric_us();
        if self.in_flight.is_none() {
            self.flush_started_us = submitted_at;
        }
        #[cfg(feature = "opentelemetry")]
        if let Some(telemetry) = &self.cfg.telemetry {
            telemetry.batch(self.cfg.shard, batch.records);
        }
        self.metrics.flushes += 1;
        self.metrics.flushed_records += u64::from(batch.records);
        self.metrics.log_bytes += batch.bytes;
        self.flush_records = batch.records;
        self.flush_bytes = batch.bytes;
        // A slow flush sends the next ones to the log's threads, so that the
        // shard thread can take new work meanwhile. On a disk that is slow
        // every time (gp3), a lone gate's flush, or an idle shard's while
        // at most two shards are at work, stays inline: there is little
        // work to take, and the hand-off costs more than the wait. A fast
        // disk's occasional slow flush goes to the log's threads.
        let slow_disk = self.slow_flushes >= SLOW_DISK_FLUSHES;
        let fast = self.env.last_flush_us() <= self.cfg.inline_max_us
            || (slow_disk && (lone || (idle && crate::spin::few_busy())));
        // A foreign host pumps one delivery per turn of its event loop and
        // each appends a record: flushing inline between them would block the
        // loop once per record of a fan-out. Let them batch in the background.
        if idle
            && fast
            && self.in_flight.is_none()
            && self.cfg.inline_flush
            && self.env.supports_inline()
            && !self.has_undelivered()
        {
            self.metrics.inline_flushes += 1;
            let flushed = self.env.flush_inline(batch);
            // A flush that took time moved the clock on.
            if self.env.last_flush_us() > 0 {
                self.clock.renew();
            }
            match flushed {
                Ok(lsn) => {
                    self.set_durable(lsn, StallKind::InlineFlush);
                    self.on_commit();
                }
                Err(e) => self.fail(format!("log flush failed: {e}; the shard has stopped")),
            }
        } else {
            self.in_flight = Some(batch.last_lsn);
            self.submitted.push_back((batch.last_lsn, submitted_at));
            self.env.submit(batch);
        }
        true
    }

    fn maintenance(&mut self) {
        if self.failed.is_some() {
            return;
        }
        let started = (self.stall_us != 0).then(|| self.env.monotonic_us());
        if self.stream_wait_until != u64::MAX {
            self.expire_stream_reads();
        }
        self.trim_peak = self.trim_peak.max(self.loaded_bytes());
        let swept = self.last_sweep_ms;
        self.maintain();
        // Pools sized for a mass wake shrink once the shard is idle again.
        let activity = self.metrics.runs_loaded + self.metrics.runs_started;
        if activity == self.spare_mark {
            let freed = self.slot_spare.len().saturating_sub(SLOT_SPARE_IDLE);
            if freed > 0 {
                self.slot_spare.truncate(SLOT_SPARE_IDLE);
                self.slot_spare.shrink_to(SLOT_SPARE_IDLE);
            }
            let parts = self.core.shrink_spare(fusee_core::shard::SPARE_RUNS_IDLE);
            self.pools_shrunk = true;
            // What the pools held goes back to the operating system with the
            // sweep's own frees (a slot and its containers: about 1 KiB).
            crate::trim::note_freed(((freed + parts) as u64) << 10);
        }
        self.spare_mark = activity;
        // A mass park (loaded state down to under half its high-water mark,
        // by at least 8 MiB) can go back to the operating system. Runs that
        // finish while others load (a steady stream) do not count: a trim
        // holds every arena's lock in turn, which stalls each shard thread
        // that allocates meanwhile (tens of milliseconds at once on every
        // shard).
        let loaded = self.loaded_bytes();
        if loaded.saturating_mul(2) < self.trim_peak && self.trim_peak - loaded >= 8 << 20 {
            crate::trim::note_freed(self.trim_peak - loaded);
            self.trim_peak = loaded;
        }
        // A new index base, a slice per sweep.
        if self.last_sweep_ms != swept {
            self.advance_index_build();
        }
        // The journals of the runs the sweep parked, in one write.
        self.store_flush_or_fail();
        if let Some(t) = started {
            self.tick_maintenance_us = self.env.monotonic_us().saturating_sub(t);
        }
    }

    /// What this shard has left to do (see [`Due`]).
    pub fn due(&self) -> Due {
        let busy = self.ready.queued.load(Ordering::Acquire)
            || self.core.has_events()
            || !self.instantiate.is_empty()
            || !self.foreign.is_empty()
            || self.undelivered.iter().any(|r| {
                self.slots
                    .get(r)
                    .is_some_and(|s| s.machine.has_queued_deliveries())
            })
            || !self.held_loads.is_empty()
            || !self.heartbeats.is_empty()
            || !self.offered.is_empty()
            || self.in_flight.is_some()
            || self.writer.has_pending()
            || self.commit_dirty
            || self.ckpt_running
            || self.ckpt_pending.is_some();
        let mut running: Vec<String> = Vec::new();
        for slot in self.slots.entries.iter().flatten() {
            if !slot.machine.is_suspended() {
                let t: &str = &slot.machine.info().wtype;
                if !running.iter().any(|r| r == t) {
                    running.push(t.to_owned());
                }
            }
        }
        let mut at = self.core.next_timer();
        if let Some(Reverse((t, _))) = self.timers.peek() {
            at = Some(at.map_or(*t, |a| a.min(*t)));
        }
        if let Some(sent) = self.outbox.values().map(|o| o.sent_ms).min() {
            let t = sent.saturating_add(RESEND_MS);
            at = Some(at.map_or(t, |a| a.min(t)));
        }
        Due {
            busy,
            running,
            awaiting: self.awaiting.keys().cloned().collect(),
            at_ms: at,
            steps: self.metrics.steps_run,
            open_us: self.metrics.open_us,
            unacked: self.outbox.len(),
        }
    }

    fn maintain(&mut self) {
        let now = self.now();
        if now.saturating_sub(self.last_sweep_ms) >= 100 {
            self.last_sweep_ms = now;
            // Park runs that have been suspended long enough. Only runs that
            // were active since the last sweep are examined, and the queue of
            // suspended ones is in time order: the cost follows activity, not
            // the number of loaded runs.
            let park_after = self.cfg.park_after_ms;
            let mut to_park = Vec::new();
            for run in std::mem::take(&mut self.dirty) {
                let parkable = self.parkable(run);
                let Some(slot) = self.slots.get_mut(&run) else {
                    continue;
                };
                if park_after == u64::MAX {
                    slot.dirty = false;
                } else if parkable {
                    slot.dirty = false;
                    slot.suspended_since = Some(now);
                    self.park_queue.push_back((now, run));
                } else {
                    self.dirty.push(run);
                }
            }
            while let Some(&(since, run)) = self.park_queue.front() {
                if now.saturating_sub(since) < park_after {
                    break;
                }
                self.park_queue.pop_front();
                let Some(slot) = self.slots.get(&run) else {
                    continue;
                };
                if slot.suspended_since != Some(since) {
                    continue;
                }
                if self.parkable(run) {
                    to_park.push(run);
                } else {
                    // Resumed without a wake (a foreign host's call): look
                    // at it again at the next sweep.
                    self.mark_active(run);
                }
            }
            {
                self.tick_parked = to_park.len() as u32;
                for run in to_park {
                    if let Err(e) = self.park(run) {
                        self.fail(e.to_string());
                    }
                }
            }
            self.park_over_budget();
            self.core.shrink();
            fn fit<K: std::hash::Hash + Eq, V>(m: &mut FxHashMap<K, V>) {
                if m.capacity() > 1024 && m.capacity() > 4 * m.len() {
                    m.shrink_to(m.len() * 2);
                }
            }
            self.slots.shrink();
            self.drop_stream_logs();
            fit(&mut self.resolved);
            fit(&mut self.stash);
            fit(&mut self.current_step);
            if let Err(e) = self.expire_retained(now) {
                self.fail(e.to_string());
            }
        }
        // Checkpoint once per segment, or on the interval while active. On
        // the shared log a roll asks only the shards holding the segment it
        // retires next: one segment's worth of writes no longer makes
        // every shard checkpoint.
        if !self.ckpt_running && self.ckpt_pending.is_none() {
            let rolled = self.log_segment() != self.last_ckpt_segment
                && self.checkpoint_needed()
                && self.env.checkpoint_on_roll();
            let active = self.checkpoint_needed();
            let due = now.saturating_sub(self.last_ckpt_ms) >= self.cfg.checkpoint_interval_ms;
            // A shared log cannot retire a segment this idle shard's older
            // checkpoint still covers records in.
            let wanted = due && self.env.checkpoint_wanted();
            if (active && (rolled || due)) || wanted {
                self.start_checkpoint(now);
            }
        }
    }

    /// While loaded runs exceed the memory budget, parks the runs that have
    /// been suspended longest (least recently used), down to 90% of it.
    fn park_over_budget(&mut self) {
        let budget = self.cfg.memory_budget;
        if budget == 0 || self.cfg.park_after_ms == u64::MAX || self.loaded_bytes() <= budget {
            return;
        }
        let target = budget - budget / 10;
        let mut parked = 0;
        while parked < BUDGET_PARKS && self.loaded_bytes() > target {
            let Some((since, run)) = self.park_queue.pop_front() else {
                break;
            };
            let Some(slot) = self.slots.get(&run) else {
                continue;
            };
            if slot.suspended_since != Some(since) {
                continue;
            }
            if !self.parkable(run) {
                self.mark_active(run);
                continue;
            }
            match self.park(run) {
                Ok(true) => {
                    parked += 1;
                    self.metrics.budget_parks += 1;
                }
                Ok(false) => {}
                Err(e) => {
                    self.fail(e.to_string());
                    return;
                }
            }
        }
        self.tick_parked += parked as u32;
    }

    fn expire_retained(&mut self, now: u64) -> Result<(), Error> {
        let mut expired = Vec::new();
        self.obsolete_store_bytes += self.core.expire(now, &mut expired);
        for run in expired {
            let blob = self
                .tail
                .put(self.env.store_len(), EntryRef::Tombstone { run })?;
            self.obsolete_store_bytes += u64::from(blob.len);
            if self.tail.full() {
                self.store_flush()?;
            }
        }
        self.store_flush()
    }

    fn should_compact(&self) -> bool {
        !self.no_compaction
            && self.obsolete_store_bytes >= 8 << 20
            && self.obsolete_store_bytes >= self.store_len() / 2
    }

    fn checkpoint_needed(&self) -> bool {
        self.writer.last_lsn() > self.last_ckpt_lsn
            || self.store_len() != self.last_ckpt_store_len
            || self.should_compact()
    }

    fn compact_store_if_needed(&mut self) -> Result<(), Error> {
        // Account for replaced blobs on the write path. A fully live store,
        // even with millions of sleepers, never needs a full index scan here.
        // A format 2 store is rewritten once, so it can take format 3 frames.
        let upgrade = !self.store_v3 && !self.no_compaction && self.env.recovers();
        if !self.should_compact() && !upgrade {
            return Ok(());
        }
        // Compaction copies frames as they are, and a `Delta` names its
        // predecessor by offset: write chained journals whole first.
        let chained: Vec<_> = self.core.chained_runs().collect();
        for (run, blob) in chained {
            let c = self.read_chain(blob)?;
            // A finished run's chain becomes its `Done` frame: the
            // whole journal, or for a success without `keep_history` its
            // start and result, as `park_done` writes them.
            let done = self.core.parked(run).and_then(|p| p.done());
            let frame = match done.zip(fusee_core::store::journal_done(&c.journal)) {
                Some((_, (status, finished_ms))) => encode_entry(&StoreEntry::Done {
                    run,
                    last_lsn: c.last_lsn,
                    id_hash: c.id_hash,
                    status,
                    finished_ms,
                    journal: if status == Status::Ok && !self.cfg.keep_history {
                        done_summary(&c.journal)?
                    } else {
                        c.journal
                    },
                }),
                None => encode_entry(&StoreEntry::Park {
                    run,
                    last_lsn: c.last_lsn,
                    id_hash: c.id_hash,
                    timers: c.timers,
                    journal: c.journal,
                }),
            };
            let len = u32::try_from(frame.len())
                .map_err(|_| Error::Io("run store frame exceeds 4 GiB".into()))?;
            let offset = self.env.store_append(&frame)?;
            self.obsolete_store_bytes += c.chain;
            self.core.reparked(run, BlobRef { offset, len });
        }
        let loaded: Vec<_> = self
            .core
            .loaded_runs()
            .filter(|(_, l)| l.blob_depth > 0)
            .filter_map(|(r, l)| l.blob.map(|b| (r, b)))
            .collect();
        for (run, blob) in loaded {
            let c = self.read_chain(blob)?;
            let frame = encode_entry(&StoreEntry::Park {
                run,
                last_lsn: c.last_lsn,
                id_hash: c.id_hash,
                timers: c.timers,
                journal: c.journal,
            });
            let len = u32::try_from(frame.len())
                .map_err(|_| Error::Io("run store frame exceeds 4 GiB".into()))?;
            let offset = self.env.store_append(&frame)?;
            self.obsolete_store_bytes += c.chain;
            self.core
                .set_blob(run, BlobRef { offset, len }, u64::from(len), 0);
        }
        let mut live: Vec<_> = self
            .core
            .parked_runs()
            .map(|(r, p)| (r, p.blob()))
            .chain(
                self.core
                    .loaded_runs()
                    .filter_map(|(r, l)| l.blob.map(|b| (r, b))),
            )
            .collect();
        live.sort_unstable_by_key(|(_, b)| b.offset);
        if self.env.compact_store(&live)? {
            let mut offset = fusee_core::store::STORE_HEADER;
            for (run, blob) in live {
                self.core.relocate_blob(run, offset);
                offset += u64::from(blob.len);
            }
            if offset != self.env.store_len() {
                return Err(Error::Io("unexpected length of rewritten run store".into()));
            }
            // A new generation (with this build's header), and the index
            // starts over in it.
            self.store_v3 = self.store_takes_v3();
            self.index = None;
            self.obsolete_store_bytes = 0;
        } else {
            self.no_compaction = true;
        }
        Ok(())
    }

    /// Whether the current run store's header admits format 3 frames.
    fn store_takes_v3(&mut self) -> bool {
        self.env
            .store_read(0, fusee_core::store::STORE_HEADER as u32)
            .ok()
            .and_then(|h| fusee_core::store::store_format(&h).ok())
            .is_some_and(|f| f >= 3)
    }

    /// Writes the persisted index for a checkpoint and returns the frames
    /// the checkpoint names, oldest first. A new base (every parked run) is
    /// written at once only when there is none in this store generation or
    /// the change lists overflowed. Otherwise the checkpoint adds one delta
    /// with what changed; once the deltas outweigh the base (in bytes or in
    /// frames), a new base is written a frame at a time between checkpoints
    /// ([`advance_index_build`](Self::advance_index_build)) and the first
    /// checkpoint after it is done names it. Rewriting the base at once took
    /// tens of milliseconds per million parked runs on the shard thread.
    fn write_index(&mut self) -> Result<Vec<BlobRef>, Error> {
        let generation = self.env.store_generation();
        let whole =
            self.index.is_none() || self.index_gen != generation || self.core.index_needs_base();
        const FRAME: usize = INDEX_FRAME_RECORDS * fusee_core::table::RECORD;
        let mut records = Vec::new();
        if whole {
            self.drop_index_build();
            if let Some(old) = self.index.take()
                && self.index_gen == generation
            {
                self.obsolete_store_bytes += old.iter().map(|b| u64::from(b.len)).sum::<u64>();
            }
            self.core.begin_index_snapshot();
            let mut frames = Vec::new();
            let mut from = Some(0);
            while let Some(at) = from {
                records.clear();
                from = self
                    .core
                    .index_snapshot(at, INDEX_FRAME_RECORDS, &mut records);
                if !records.is_empty() {
                    frames.push(self.append_index(&records)?);
                }
            }
            self.index_base = (frames.len(), frames.iter().map(|b| u64::from(b.len)).sum());
            self.index_delta_bytes = 0;
            self.index_gen = generation;
            self.index = Some(frames);
        } else {
            self.adopt_index_build();
            self.core.index_delta(&mut records);
            let mut frames = Vec::new();
            for chunk in records.chunks(FRAME) {
                frames.push(self.append_index(chunk)?);
            }
            self.index_delta_bytes += frames.iter().map(|b| u64::from(b.len)).sum::<u64>();
            if let Some(index) = self.index.as_mut() {
                index.extend(frames);
            }
            // Frames read at an open stay within about nine per 256 KiB of
            // base, and rewriting the base costs a fraction of what the
            // deltas since the last one took.
            let deltas = self.index.as_ref().map_or(0, Vec::len) - self.index_base.0;
            let chunks =
                self.index_base.1 / (INDEX_CHUNK_RECORDS * fusee_core::table::RECORD) as u64;
            if self.index_build.is_none()
                && (self.index_delta_bytes > self.index_base.1 / 2 + (1 << 20)
                    || deltas as u64 >= (self.cfg.index_rebase_deltas as u64).max(8 * chunks))
            {
                self.index_build = Some(IndexBuild {
                    cursor: Some(0),
                    frames: Vec::new(),
                    bytes: 0,
                    generation,
                    deltas_from: self.index.as_ref().map_or(0, Vec::len),
                });
                self.core.hold_slots(true);
            }
        }
        Ok(self.index.clone().unwrap_or_default())
    }

    /// Writes the next frames of an index base being built (once per
    /// sweep), for about
    /// [`INDEX_BUILD_SLICE_US`] (at least one frame, at most
    /// [`INDEX_BUILD_SLICE_FRAMES`]).
    fn advance_index_build(&mut self) {
        let Some(mut build) = self.index_build.take() else {
            return;
        };
        if build.generation != self.env.store_generation() {
            // The store was rewritten; the next checkpoint writes a base.
            self.index_build = Some(build);
            self.drop_index_build();
            return;
        }
        // Frames buffered for the store take their offsets from its end:
        // write them before appending past it.
        if let Err(e) = self.store_flush() {
            self.index_build = Some(build);
            self.fail(format!("run store write failed: {e}"));
            return;
        }
        let started = self.env.monotonic_us();
        let mut records = Vec::new();
        let mut written = 0;
        while let Some(at) = build.cursor {
            records.clear();
            build.cursor =
                self.core
                    .index_snapshot(at, self.cfg.index_chunk_records.max(1), &mut records);
            if !records.is_empty() {
                match self.append_index(&records) {
                    Ok(frame) => {
                        build.bytes += u64::from(frame.len);
                        build.frames.push(frame);
                    }
                    Err(e) => {
                        self.index_build = Some(build);
                        self.drop_index_build();
                        self.fail(format!("run index write failed: {e}"));
                        return;
                    }
                }
            }
            written += 1;
            if written >= INDEX_BUILD_SLICE_FRAMES
                || self.env.monotonic_us().saturating_sub(started) >= INDEX_BUILD_SLICE_US
            {
                break;
            }
        }
        if build.cursor.is_none() {
            // Every slot visited: entries may move again.
            self.core.hold_slots(false);
        }
        self.index_build = Some(build);
    }

    /// A finished base takes the place of the current one: the chain becomes
    /// its frames followed by the deltas written since it began.
    fn adopt_index_build(&mut self) {
        let Some(build) = self.index_build.take_if(|b| b.cursor.is_none()) else {
            return;
        };
        let old = self.index.take().unwrap_or_default();
        let from = build.deltas_from.min(old.len());
        self.obsolete_store_bytes += old[..from].iter().map(|b| u64::from(b.len)).sum::<u64>();
        let carried = &old[from..];
        self.index_base = (build.frames.len(), build.bytes);
        self.index_delta_bytes = carried.iter().map(|b| u64::from(b.len)).sum();
        let mut frames = build.frames;
        frames.extend_from_slice(carried);
        self.metrics.index_bases_built += 1;
        self.index = Some(frames);
    }

    /// Abandons a base being built; its frames are dead store bytes.
    fn drop_index_build(&mut self) {
        if let Some(build) = self.index_build.take() {
            if build.generation == self.env.store_generation() {
                self.obsolete_store_bytes += build.bytes;
            }
            self.core.hold_slots(false);
        }
    }

    fn append_index(&mut self, records: &[u8]) -> Result<BlobRef, Error> {
        let frame = encode_entry(&StoreEntry::Index {
            records: records.to_vec(),
        });
        let len = u32::try_from(frame.len())
            .map_err(|_| Error::Io("run index frame exceeds 4 GiB".into()))?;
        let offset = self.env.store_append(&frame)?;
        Ok(BlobRef { offset, len })
    }

    fn start_checkpoint(&mut self, now: u64) {
        // Each checkpoint names immutable frames. Reuse unchanged active
        // journals, including a previously unparked run with an empty suffix.
        let changed: Vec<_> = self
            .core
            .loaded_runs()
            .filter(|(_, l)| !l.is_done() && !l.journal.is_empty())
            .map(|(run, _)| run)
            .collect();
        for run in changed {
            match self.store_active(run) {
                Ok((blob, chain, depth)) => self.core.spilled(run, blob, chain, depth),
                Err(e) => {
                    self.fail(format!("active journal checkpoint failed: {e}"));
                    return;
                }
            }
        }
        // The checkpoint names the store's length: every frame is written
        // before it, and compaction and the index append behind them.
        if let Err(e) = self.store_flush() {
            self.fail(format!("active journal checkpoint failed: {e}"));
            return;
        }
        if let Err(e) = self.compact_store_if_needed() {
            self.fail(format!("run-store compaction failed: {e}"));
            return;
        }
        let index = if self.store_v3 && self.env.recovers() {
            match self.write_index() {
                Ok(frames) => Some(frames),
                Err(e) => {
                    self.fail(format!("run index checkpoint failed: {e}"));
                    return;
                }
            }
        } else {
            None
        };
        self.last_ckpt_store_len = self.env.store_len();
        let lsn = self.writer.last_lsn();
        let runs = self
            .core
            .loaded_runs()
            .map(|(run, l)| CheckpointRun {
                run,
                last_lsn: l.last_lsn,
                blob: l.blob,
                journal: l.journal.clone(),
            })
            .collect();
        let cp = Checkpoint {
            log_position: self.env.log_position(),
            shard: self.cfg.shard,
            shards: self.cfg.shards,
            lsn,
            next_run: self.core.next_run_id(),
            store_generation: self.env.store_generation(),
            store_len: self.env.store_len(),
            created_ms: now,
            index,
            obsolete: self.obsolete_store_bytes,
            index_runs: self.core.parked_len() as u64,
            runs,
            outbox: {
                // Unacknowledged messages, plus messages whose records are
                // covered by this checkpoint but not yet durable: replay
                // after the checkpoint would not regenerate them.
                let mut all: Vec<Outgoing> = self.outbox.values().cloned().collect();
                let shards = self.cfg.shards;
                for o in self.core.unreleased() {
                    let (target, msg) = match o {
                        Output::ChildStart {
                            parent,
                            wtype,
                            id,
                            flags,
                            deadline_ms,
                            input,
                        } => (
                            fusee_core::shard::shard_of(id, shards),
                            Message::ChildStart {
                                parent: *parent,
                                wtype: wtype.to_string(),
                                id: id.to_string(),
                                flags: *flags,
                                deadline_ms: *deadline_ms,
                                input: input.clone(),
                                trace: self.trace_of(parent.run),
                            },
                        ),
                        Output::ChildResult {
                            parent,
                            status,
                            payload,
                        } => (
                            parent.shard,
                            Message::ChildResult {
                                parent: *parent,
                                status: *status,
                                payload: payload.clone(),
                            },
                        ),
                        Output::Signal {
                            target,
                            name,
                            payload,
                            dedup,
                        } => (
                            fusee_core::shard::shard_of(target, shards),
                            Message::Signal {
                                target: target.to_string(),
                                name: name.to_string(),
                                payload: payload.clone(),
                                dedup: *dedup,
                            },
                        ),
                        Output::Close {
                            target,
                            wtype,
                            parent,
                            terminate,
                            reason,
                        } => (
                            fusee_core::shard::shard_of(target, shards),
                            Message::Close {
                                target: target.to_string(),
                                wtype: wtype.to_string(),
                                parent: *parent,
                                terminate: *terminate,
                                reason: reason.clone(),
                            },
                        ),
                        _ => continue,
                    };
                    all.push(Outgoing {
                        id: 0,
                        target,
                        msg,
                        sent_ms: 0,
                    });
                }
                encode_outbox(&all)
            },
        };
        let seg = self.log_segment();
        // A shared log retires its segments by checkpoint position instead.
        let delete = (seg > 1 && matches!(self.writer, ShardLog::Segments(_))).then(|| seg - 1);
        self.last_ckpt_ms = now;
        self.last_ckpt_segment = seg;
        self.ckpt_pending = Some((cp, delete));
        self.on_commit();
    }

    /// The next delivery for `run`, applied: records appended, results
    /// stored for native futures (or returned for foreign hosts).
    fn deliver(&mut self, run: RunId) -> Result<Option<Delivery>, Error> {
        self.check_alive()?;
        let Some(slot) = self.slots.get_mut(&run) else {
            return Ok(None);
        };
        match slot.machine.next() {
            Next::Deliver {
                seq,
                outcome,
                record,
            } => {
                if let Some(r) = record {
                    // The streams the step opened end with its result, just
                    // before it.
                    if let Record::Step { status, .. } | Record::Completed { status, .. } = &r
                        && let Some(slot) = self.slots.get_mut(&run)
                        && slot.machine.owns_streams(seq)
                    {
                        for e in slot.machine.stream_ends(seq, *status) {
                            self.append(&e)?;
                        }
                    }
                    let lsn = self.append(&r)?;
                    if matches!(r, Record::ChildDone { .. })
                        && let Some(froms) = self.child_acks.remove(&(run, seq))
                    {
                        for from in froms {
                            let t = self.msg_token(from);
                            self.core.add_output(lsn, Output::Ack(t));
                        }
                    }
                    if matches!(r, Record::Completed { .. })
                        && let Some(tokens) = self.completion_acks.remove(&(run, seq))
                    {
                        for t in tokens {
                            self.core.add_output(lsn, Output::Ack(t));
                        }
                    }
                } else {
                    self.metrics.steps_replayed += 1;
                }
                match self.open_outcome(outcome) {
                    Ok(outcome) => Ok(Some(Delivery::Resolve { seq, outcome })),
                    Err(e) => {
                        self.make_stuck(run, format!("a recorded payload cannot be read: {e}"));
                        Ok(None)
                    }
                }
            }
            // A live step's result: its record borrows the outcome's
            // payload.
            Next::DeliverLive {
                seq,
                outcome,
                record,
            } => {
                // The streams the step opened end with its result, just
                // before it.
                if let Some(slot) = self.slots.get_mut(&run)
                    && slot.machine.owns_streams(seq)
                {
                    for e in slot.machine.stream_ends(seq, outcome.status) {
                        self.append(&e)?;
                    }
                }
                let time = self.step_time(run, seq);
                let lsn = self.append_with(&record.record(run, seq, &outcome, time), None)?;
                match record {
                    fusee_core::run::LiveRecord::Completed { .. } => {
                        if let Some(tokens) = self.completion_acks.remove(&(run, seq)) {
                            for t in tokens {
                                self.core.add_output(lsn, Output::Ack(t));
                            }
                        }
                    }
                    // Back to the run's name cache, as from an inline step.
                    fusee_core::run::LiveRecord::Step { name, .. } => {
                        if let Some(slot) = self.slots.get_mut(&run) {
                            slot.machine.return_name(name);
                        }
                    }
                }
                match self.open_outcome(outcome) {
                    Ok(outcome) => Ok(Some(Delivery::Resolve { seq, outcome })),
                    Err(e) => {
                        self.make_stuck(run, format!("a recorded payload cannot be read: {e}"));
                        Ok(None)
                    }
                }
            }
            Next::Update {
                ordinal,
                name,
                id,
                payload,
                live,
            } => match self.open(payload) {
                Ok(payload) => {
                    if live.is_none() {
                        self.object_started(run, ordinal, &name);
                    }
                    Ok(Some(Delivery::Update {
                        ordinal,
                        name: name.to_string(),
                        id: id.to_string(),
                        payload,
                        token: live,
                    }))
                }
                Err(e) => {
                    if let Some(t) = live {
                        self.decide_update(t, Err(e.to_string()))?;
                        return Ok(None);
                    }
                    self.make_stuck(run, format!("a recorded update cannot be read: {e}"));
                    Ok(None)
                }
            },
            Next::CancelAll {
                seqs,
                reason,
                record,
            } => {
                if let Some(r) = record {
                    let mut ends = Vec::new();
                    if let Some(slot) = self.slots.get_mut(&run) {
                        for s in &seqs {
                            if slot.machine.owns_streams(*s) {
                                ends.extend(slot.machine.stream_ends(*s, Status::Cancelled));
                            }
                        }
                    }
                    for e in &ends {
                        self.append(e)?;
                    }
                    let lsn = self.append(&r)?;
                    if let Some(tokens) = self.cancel_tokens.remove(&run) {
                        for t in tokens {
                            self.core.add_output(lsn, Output::Ack(t));
                        }
                    }
                }
                Ok(Some(Delivery::CancelAll { seqs, reason }))
            }
            Next::Stalled => match slot.machine.stall_error() {
                Some(e) => {
                    self.make_stuck(run, e.to_string());
                    Ok(None)
                }
                None => Ok(None),
            },
            Next::Idle => Ok(None),
        }
    }

    /// One line per pause: a paused run waits for someone, as a stuck one
    /// does.
    fn report_pause(&mut self, run: RunId, seq: Seq, error: &[u8]) {
        if self.cfg.replay_only {
            return;
        }
        let Some(slot) = self.slots.get(&run) else {
            return;
        };
        let info = slot.machine.info();
        let step = slot.machine.pending_name(seq).unwrap_or_default();
        let failure = crate::failure::Failure::from_payload(error);
        let line = format!(
            "workflow '{}' ({} v{}, run {run}) paused: step '{step}' ran out of retries: {}. \
             Fix the cause, then resume it: fusee runs resume {}",
            info.id, info.wtype, info.version, failure.message, info.id
        );
        crate::log::emit(&crate::log::Record {
            workflow_id: Some(&info.id),
            workflow_type: Some(&info.wtype),
            run: Some(run),
            ..crate::log::Record::engine(crate::log::Level::Warn, &line)
        });
    }

    fn make_stuck(&mut self, run: RunId, message: String) {
        self.metrics.determinism_errors += 1;
        if !self.cfg.replay_only {
            // One line per run that sticks: a stuck run waits silently
            // otherwise, and whoever waits on its result waits forever.
            let info = self.slots.get(&run).map(|s| s.machine.info());
            let who = info
                .map(|i| {
                    format!(
                        "workflow '{}' ({} v{}, run {run})",
                        i.id, i.wtype, i.version
                    )
                })
                .unwrap_or_else(|| format!("run {run}"));
            let line = format!(
                "{who} is stuck and will not progress: {message}. Fix: deploy code that matches \
                 its journal, guard the change with patched(), or reset the run to before that \
                 step. See {}",
                crate::error::docs_link("Determinism")
            );
            crate::log::emit(&crate::log::Record {
                workflow_id: info.map(|i| &*i.id),
                workflow_type: info.map(|i| &*i.wtype),
                run: Some(run),
                ..crate::log::Record::engine(crate::log::Level::Error, &line)
            });
        }
        self.release_waiting_requests(run);
        if let Some(slot) = self.remove_slot(run)
            && slot.foreign
        {
            self.foreign.push_back(ForeignEvent::Stuck {
                run,
                message: message.clone(),
            });
        }
        self.stuck.insert(run, message);
    }

    fn store_delivery(&mut self, run: RunId, d: Delivery) {
        match d {
            // Native updates are started by the shard runtime.
            Delivery::Update { .. } => {}
            Delivery::Resolve { seq, outcome } => self.resolve(run, seq, outcome),
            Delivery::CancelAll { seqs, reason } => {
                for s in seqs {
                    self.resolve(
                        run,
                        s,
                        Outcome {
                            status: Status::Cancelled,
                            payload: reason.clone(),
                        },
                    );
                }
            }
        }
    }

    /// Stores `seq`'s result and wakes the future waiting for it, so a
    /// combinator polls only the branch it resolves.
    fn resolve(&mut self, run: RunId, seq: Seq, outcome: Outcome) {
        // No future of the shard waits with a waker of its own: the drive
        // polls the run's code next, and the result waits in its slot.
        if self.resolved.is_empty()
            && let Some(slot) = self.slots.get_mut(&run)
            && slot.resolved.is_none()
        {
            slot.resolved = Some((seq, outcome));
            return;
        }
        if let Some(Resolution::Waiting(w)) =
            self.resolved.insert((run, seq), Resolution::Ready(outcome))
        {
            w.wake();
        }
    }

    /// Drops a volatile timer. Its heap entry goes when it reaches the top,
    /// or when cancelled entries are most of the heap, so neither the
    /// earliest deadline nor the heap's size is set by cancelled timers.
    fn cancel_timer(&mut self, id: u64) {
        if self.timer_wakers.remove(&id).is_none() {
            return;
        }
        while let Some(Reverse((_, top))) = self.timers.peek()
            && !self.timer_wakers.contains_key(top)
        {
            self.timers.pop();
        }
        if self.timers.len() > 64 && self.timers.len() > 2 * self.timer_wakers.len() {
            self.timers
                .retain(|Reverse((_, id))| self.timer_wakers.contains_key(id));
        }
    }

    /// A run ended: requests that were waiting on its code are answered.
    fn release_waiting_requests(&mut self, run: RunId) {
        self.release_child_acks(run);
        self.object_ended(run, "the object's run ended before the call started");
        let tail = self.writer.last_lsn();
        if let Some(tokens) = self.cancel_tokens.remove(&run) {
            for t in tokens {
                self.core.add_output(tail, Output::Ack(t));
            }
        }
        // Most runs end with nobody waiting: the maps below are then empty
        // and nothing is looked at or allocated.
        let keys: Vec<(RunId, Name)> = if self.update_waiters.is_empty() {
            Vec::new()
        } else {
            self.update_waiters
                .keys()
                .filter(|k| k.0 == run)
                .cloned()
                .collect()
        };
        if !keys.is_empty() {
            let ended: Payload = Arc::from(&b"the workflow ended before the update finished"[..]);
            for k in keys {
                for t in self.update_waiters.remove(&k).unwrap_or_default() {
                    self.core.add_output(
                        tail,
                        Output::Reply {
                            token: t,
                            status: Status::Failed,
                            payload: ended.clone(),
                        },
                    );
                }
            }
        }
        if !self.offered.is_empty() {
            self.offered.retain(|_, v| v.0 != run);
        }
        if let Some(slot) = self.slots.get_mut(&run) {
            slot.machine.take_offered_updates();
        }
        if self.completion_acks.is_empty() {
            return;
        }
        let keys: Vec<(RunId, Seq)> = self
            .completion_acks
            .keys()
            .filter(|k| k.0 == run)
            .copied()
            .collect();
        for k in keys {
            for t in self.completion_acks.remove(&k).unwrap_or_default() {
                self.core.add_output(
                    tail,
                    Output::Refuse {
                        token: t,
                        message: Arc::from(&b"the step is no longer waiting for a completion"[..]),
                    },
                );
            }
        }
    }

    /// Records a child that was closed before its start arrived: a start
    /// and an immediate end.
    fn start_closed(
        &mut self,
        id: &str,
        wtype: &str,
        parent: ParentRef,
        status: Status,
        reason: &[u8],
    ) -> Result<Lsn, Error> {
        let run = self.core.allocate_run();
        let now = self.now();
        let version = self
            .registry
            .read()
            .ok()
            .and_then(|r| r.latest(wtype))
            .unwrap_or(1);
        let start: RecordBuf = Record::Start {
            run,
            id: Arc::from(id),
            wtype: Arc::from(wtype),
            version,
            codec: 1,
            started_ms: now,
            deadline_ms: 0,
            parent: Some(parent),
            policy: policy::ATTACH | policy::DETACHED,
            from: None,
            copies: 0,
            update: None,
            input: empty(),
            meta: None,
        };
        self.append(&start)?;
        self.metrics.runs_started += 1;
        self.append(&Record::Done {
            run,
            status,
            finished_ms: now,
            payload: Arc::from(reason),
            by: None,
        })
    }

    fn fresh_update_id(&mut self) -> String {
        let n = self.next_update_id;
        self.next_update_id += 1;
        format!("auto-{}-{n}", self.now())
    }

    /// Offers an update to a loaded run. A retried call with the same
    /// update ID gets the recorded result instead of running the handler
    /// twice.
    fn offer_update(
        &mut self,
        run: RunId,
        name: &str,
        id: &str,
        payload: Payload,
        token: Option<u64>,
    ) -> Result<(), Error> {
        if self.object_answered(run, id, token) {
            return Ok(());
        }
        let Some(l) = self.core.get(run) else {
            return Err(err_slot(run));
        };
        let key: (RunId, Name) = (run, Name::from(id));
        let known = if self.cfg.sabotage == Sabotage::DuplicateUpdates {
            None
        } else {
            l.update(id)
        };
        let object = l.policy & policy::OBJECT != 0 && !l.is_done();
        if let Some((_, slot)) = known {
            let result = slot.result.clone();
            match (result, token) {
                (Some((status, p, lsn)), Some(t)) => {
                    let (status, payload) = match (status, self.open(p.clone())) {
                        (Status::Ok, Ok(p)) => (Status::Ok, p),
                        (Status::Ok, Err(e)) => {
                            (Status::Failed, Arc::from(e.to_string().as_bytes()))
                        }
                        (s, _) => (s, p),
                    };
                    self.core.add_output(
                        lsn,
                        Output::Reply {
                            token: t,
                            status,
                            payload,
                        },
                    );
                }
                (None, Some(t)) if l.is_done() => {
                    let at = l.last_lsn.max(self.writer.last_lsn());
                    self.core.add_output(
                        at,
                        Output::Reply {
                            token: t,
                            status: Status::Failed,
                            payload: Arc::from(
                                &b"the workflow ended before the update finished"[..],
                            ),
                        },
                    );
                }
                (None, Some(t)) => self.update_waiters.entry(key).or_default().push(t),
                _ => {}
            }
            return Ok(());
        }
        if l.is_done() {
            if let Some(t) = token {
                let at = l.last_lsn.max(self.writer.last_lsn());
                self.core.add_output(
                    at,
                    Output::Refuse {
                        token: t,
                        message: Arc::from(&b"the workflow has already finished"[..]),
                    },
                );
            }
            return Ok(());
        }
        let in_flight = self
            .offered
            .values()
            .any(|(r, _, i, _)| *r == run && **i == *id);
        if let Some(t) = token {
            self.update_waiters.entry(key).or_default().push(t);
        }
        if in_flight {
            return Ok(());
        }
        if object && self.object_hold(run, name, id, &payload) {
            return Ok(());
        }
        self.offer_now(run, name, id, payload)
    }

    /// Hands an update request to the run's code.
    fn offer_now(
        &mut self,
        run: RunId,
        name: &str,
        id: &str,
        payload: Payload,
    ) -> Result<(), Error> {
        if !self.slots.contains_key(&run) {
            const NO_CODE: &str =
                "the workflow is not running code (it is stuck or its code is not registered)";
            let tail = self.writer.last_lsn();
            for t in self
                .update_waiters
                .remove(&(run, Name::from(id)))
                .unwrap_or_default()
            {
                self.core.add_output(
                    tail,
                    Output::Refuse {
                        token: t,
                        message: Arc::from(NO_CODE.as_bytes()),
                    },
                );
            }
            if let Some(o) = self.obj.runs.get_mut(&run)
                && o.offered.as_deref() == Some(id)
            {
                o.offered = None;
            }
            self.object_fail_caller(run, id, NO_CODE);
            return Ok(());
        }
        let sealed = self.seal(payload)?;
        let internal = self.next_update_id;
        self.next_update_id += 1;
        let (name, id): (Name, Name) = (Name::from(name), Name::from(id));
        self.offered
            .insert(internal, (run, name.clone(), id.clone(), sealed.clone()));
        if let Some(slot) = self.slots.get_mut(&run) {
            slot.machine.offer_update(internal, name, id, sealed);
        }
        self.wake(run);
        Ok(())
    }

    /// The validator's decision on a live update the code was handed.
    /// Accepting appends its `Update` record; refusing records nothing and
    /// answers the callers once the state the validator saw is durable.
    pub fn decide_update(
        &mut self,
        token: u64,
        decision: Result<(), String>,
    ) -> Result<Option<u64>, Error> {
        let Some((run, name, id, payload)) = self.offered.remove(&token) else {
            return Ok(None);
        };
        let object = self.obj.runs.get_mut(&run).map(|o| {
            if o.offered.as_ref() == Some(&id) {
                o.offered = None;
            }
        });
        match decision {
            Ok(()) => {
                let Some(slot) = self.slots.get_mut(&run) else {
                    return Ok(None);
                };
                let (ordinal, rec) = slot
                    .machine
                    .accept_update(name.clone(), id.clone(), payload);
                if object.is_some() {
                    self.object_accept(run, ordinal, &name, &id, rec)?;
                } else {
                    self.append(&rec)?;
                }
                Ok(Some(ordinal))
            }
            Err(message) => {
                let at = self
                    .core
                    .get(run)
                    .map(|l| l.last_lsn)
                    .unwrap_or(0)
                    .max(self.commit);
                let text = message;
                let message: Payload = Arc::from(text.as_bytes());
                for t in self
                    .update_waiters
                    .remove(&(run, id.clone()))
                    .unwrap_or_default()
                {
                    self.core.add_output(
                        at,
                        Output::Refuse {
                            token: t,
                            message: message.clone(),
                        },
                    );
                }
                if object.is_some() {
                    self.object_fail_caller(run, &id, &text);
                    self.object_settle(run);
                }
                self.on_commit();
                Ok(None)
            }
        }
    }

    /// An update handler returned.
    fn update_done(&mut self, run: RunId, ordinal: u64, outcome: RunOutcome) -> Result<(), Error> {
        let (status, plain) = match outcome {
            Ok(p) => (Status::Ok, p),
            Err((s, p)) => (s, p),
        };
        let sealed = if status == Status::Ok {
            self.seal(plain.clone())?
        } else {
            plain.clone()
        };
        let object = self.obj.runs.contains_key(&run);
        let state = self.object_commit(run, ordinal, status)?;
        let slot = self.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        let r = slot.machine.update_done(ordinal, status, sealed);
        let (_, mut rec) = stop_on(self, run, r)?;
        if let Some(Record::UpdateDone { state: s, .. }) = &mut rec {
            *s = state;
        }
        let lsn = match rec {
            Some(rec) => self.append(&rec)?,
            None => self.writer.last_lsn(),
        };
        let id = self
            .core
            .get(run)
            .and_then(|l| l.updates.get(ordinal as usize))
            .map(|u| u.id.clone());
        if let Some(id) = id {
            for t in self.update_waiters.remove(&(run, id)).unwrap_or_default() {
                self.core.add_output(
                    lsn,
                    Output::Reply {
                        token: t,
                        status,
                        payload: plain.clone(),
                    },
                );
            }
        }
        if object {
            self.object_settle(run);
        }
        Ok(())
    }

    /// Ends `run` and starts a new run of its workflow ID with `input`, in
    /// one record.
    fn continue_as_new(
        &mut self,
        run: RunId,
        input: Payload,
        version: Option<u32>,
    ) -> Result<RunId, Error> {
        let slot = self.slots.get(&run).ok_or_else(|| err_slot(run))?;
        let r = slot.machine.check_end();
        stop_on(self, run, r)?;
        let l = self.core.get(run).ok_or_else(|| err_slot(run))?;
        let (id, wtype, parent, pol, deadline, old_version) = (
            l.id.clone(),
            l.wtype.clone(),
            l.parent,
            l.policy,
            l.deadline_ms,
            l.version,
        );
        let version = version
            .or_else(|| self.registry.read().ok()?.latest(&wtype))
            .unwrap_or(old_version);
        let input = self.seal(input)?;
        let new = self.core.allocate_run();
        let now = self.now();
        let start: RecordBuf = Record::Start {
            run: new,
            id,
            wtype: wtype.clone(),
            version,
            codec: 1,
            started_ms: now,
            deadline_ms: deadline,
            parent,
            policy: pol,
            from: Some(Origin {
                run,
                cause: origin::CONTINUED,
            }),
            copies: 0,
            update: None,
            input,
            // The next run continues under the same trace.
            meta: StartMeta::of(None, self.trace_of(run)),
        };
        let mut payload = Vec::new();
        encode_canonical(&start, &mut payload);
        let done: RecordBuf = Record::Done {
            run,
            status: Status::ContinuedAsNew,
            finished_ms: now,
            payload: Arc::from(payload),
            by: None,
        };
        let lsn = self.append(&done)?;
        if let Some(slot) = self.remove_slot(run)
            && slot.foreign
        {
            self.foreign.push_back(ForeignEvent::Ended { run });
        }
        if let Some(subs) = self.subscribers.remove(&run) {
            self.subscribers.insert(new, subs);
        }
        self.release_waiting_requests(run);
        self.metrics.runs_started += 1;
        let foreign = self.runs_foreign(&wtype, version);
        self.new_slot(
            new,
            JournalIn::Borrowed(std::slice::from_ref(&start)),
            lsn,
            foreign,
        )?;
        self.instantiate.push(new);
        Ok(new)
    }

    fn release_child_acks(&mut self, run: RunId) {
        if self.child_acks.is_empty() {
            return;
        }
        let keys: Vec<(RunId, Seq)> = self
            .child_acks
            .keys()
            .filter(|k| k.0 == run)
            .copied()
            .collect();
        for k in keys {
            for from in self.child_acks.remove(&k).unwrap_or_default() {
                self.ack_at_tail(from);
            }
        }
    }

    fn finish(&mut self, run: RunId, outcome: RunOutcome) {
        let (status, payload) = match outcome {
            Ok(p) => match self.seal(p) {
                Ok(p) => (Status::Ok, p),
                Err(e) => (Status::Failed, Arc::from(e.to_string().as_bytes())),
            },
            Err((s, p)) => (s, p),
        };
        let now = self.now();
        let Some(slot) = self.slots.get_mut(&run) else {
            return;
        };
        match slot.machine.finish(status, payload, now) {
            Ok(rec) => {
                if let Err(e) = self.append(&rec) {
                    self.fail(e.to_string());
                }
            }
            Err(e) => self.make_stuck(run, e.to_string()),
        }
        if !self.resolved.is_empty() {
            self.resolved.retain(|(r, _), _| *r != run);
        }
        self.release_waiting_requests(run);
    }
}

/// What workflow code calls through its [`Ctx`]. Implemented by the
/// runtime; also the API language bindings use.
pub trait Host {
    fn begin_step(&self, run: RunId, name: &str) -> Result<(Seq, StepStart, Lsn), Error>;
    /// Whether a step gated on `gate` may run now. If not, arranges a wake.
    fn may_run(&self, run: RunId, gate: Lsn, not_before_ms: u64, waker: &Waker) -> bool;
    fn set_current_step(&self, run: RunId, seq: Option<Seq>);
    fn current_step(&self, run: RunId) -> Option<Seq>;
    fn step_done(&self, run: RunId, seq: Seq, status: Status, payload: Payload);
    /// A live step's successful result, encoded in `payload`, recorded and
    /// settled at once when it is the only delivery the run could take next
    /// (see [`RunMachine::take_live_step`]): then true, and the code reads
    /// the result from those bytes, as a delivery of the recorded result
    /// would give it. False changes nothing: call
    /// [`step_done`](Self::step_done) instead.
    fn step_done_inline(&self, _run: RunId, _seq: Seq, _payload: &[u8]) -> bool {
        false
    }
    /// A combinator is about to poll several unfinished branches: until
    /// [`resume_inline`](Self::resume_inline), a step's result is queued
    /// for delivery after the poll instead of being settled at once. A
    /// result settled inside the pass moves the run's gate, so each branch
    /// that starts its step later in the same pass would wait for that
    /// record's flush: the legs of a fan-out, one flush apiece. Returns
    /// what to hand back.
    fn hold_inline(&self) -> u32 {
        0
    }
    /// Ends [`hold_inline`](Self::hold_inline).
    fn resume_inline(&self, _held: u32) {}
    /// Records a failed attempt; returns the new gate.
    fn step_retry(
        &self,
        run: RunId,
        seq: Seq,
        retry_at_ms: u64,
        error: Payload,
    ) -> Result<Lsn, Error>;
    fn step_attempt(&self, run: RunId, seq: Seq) -> u32;
    fn begin_sleep(&self, run: RunId, name: &str, fire_at_ms: u64) -> Result<Seq, Error>;
    fn begin_wait(&self, run: RunId, name: &str, deadline_ms: u64) -> Result<Seq, Error>;
    /// Returns `None` for a detached child (nothing to wait for). `flags`
    /// is the child's policy: close policy and [`policy::DETACHED`].
    fn begin_child(
        &self,
        run: RunId,
        wtype: &str,
        id: &str,
        flags: u8,
        deadline_ms: u64,
        input: Payload,
    ) -> Result<Option<Seq>, Error>;
    fn value(
        &self,
        run: RunId,
        kind: u8,
        make: &mut dyn FnMut(u64, u64) -> Payload,
    ) -> Result<Payload, Error>;
    fn send(&self, run: RunId, target: &str, name: &str, payload: Payload) -> Result<(), Error>;
    /// Takes operation `seq`'s delivered result. If it has none yet, `waker`
    /// is woken when it is delivered.
    fn take_resolution(&self, run: RunId, seq: Seq, waker: &Waker) -> Option<Outcome>;
    fn forget(&self, run: RunId, seq: Seq);
    fn now_ms(&self) -> u64;
    /// Whether step `seq` is paused: its retries ran out and no resume has
    /// come. A resume wakes the run.
    fn step_paused(&self, _run: RunId, _seq: Seq) -> bool {
        false
    }
    /// Whether step `seq` still waits for its code to run it; false once it
    /// is resolved (cancelled) without it.
    fn step_pending(&self, _run: RunId, _seq: Seq) -> bool {
        true
    }
    /// The engine's default for steps whose retry policy does not say what
    /// happens when retries run out (read only then).
    fn on_exhausted(&self) -> crate::ctx::Exhausted {
        crate::ctx::Exhausted::Fail
    }
    fn key(&self, run: RunId, seq: Seq) -> String;
    fn info(&self, run: RunId) -> Option<RunInfo>;
    /// Whether the run's code is replaying its journal (logs stay silent).
    fn replaying(&self, _run: RunId) -> bool {
        false
    }
    /// The name of the running operation at `seq` (a step), for logs.
    fn op_name(&self, _run: RunId, _seq: Seq) -> Option<String> {
        None
    }
    /// Wakes `waker` at `at_ms` (volatile). Returns the timer's ID for
    /// [`cancel_timer`](Self::cancel_timer).
    fn wake_at(&self, at_ms: u64, waker: &Waker) -> u64;
    /// Drops a timer from [`wake_at`](Self::wake_at) and its waker.
    fn cancel_timer(&self, id: u64);
    /// Counts wakes of the run's code that name no future: a gate opening,
    /// an update or cancellation arriving. Other wakes go through the waker
    /// of the future they concern, so a combinator polls only the branches
    /// whose wakers fired, and every branch when this count has changed.
    fn broadcasts(&self, run: RunId) -> u64;
    /// Wakes the run's code at `at_ms` (foreign hosts).
    fn wake_run_at(&self, run: RunId, at_ms: u64);

    /// Starts an async completion. Returns its position, what to do, and
    /// the gate its token function waits for.
    fn begin_await(&self, run: RunId, name: &str) -> Result<(Seq, AwaitStart, Lsn), Error>;
    /// The completion token for async completion `seq`'s current attempt.
    fn completion_token(&self, run: RunId, seq: Seq) -> Result<String, Error>;
    /// The token function returned: the token is out, the step waits.
    fn awaiting(&self, run: RunId, seq: Seq) -> Result<(), Error>;
    /// Progress of a running step. Coalesced; never waits for the disk.
    fn heartbeat(&self, run: RunId, seq: Seq, progress: Payload) -> Result<(), Error>;
    /// The latest progress step `seq` reported (also from before a crash).
    fn progress(&self, run: RunId, seq: Seq) -> Option<Payload>;
    /// `ctx.patched` (`deprecated: false`) or `ctx.deprecatePatch`.
    fn patched(&self, run: RunId, id: &str, deprecated: bool) -> Result<bool, Error>;
    /// Records search attributes (a JSON object).
    fn set_attributes(&self, run: RunId, attributes: Payload) -> Result<(), Error>;
    /// A key's state value (see [`crate::object`]): an exclusive handler
    /// (`exclusive`) reads its own writes, others the committed state.
    fn object_get(&self, run: RunId, key: &str, exclusive: bool) -> Result<Option<Payload>, Error>;
    /// Writes a key's state value (`None` clears it) from the exclusive
    /// handler running; committed with its result.
    fn object_set(&self, run: RunId, key: &str, value: Option<Payload>) -> Result<(), Error>;
    /// The state's keys, sorted.
    fn object_keys(&self, run: RunId, exclusive: bool) -> Result<Vec<String>, Error>;
    /// The committed state, a JSON object.
    fn object_state(&self, run: RunId) -> Result<Payload, Error>;
    /// Applies a state edit ([`crate::object::edit`]) from the exclusive
    /// handler running ([`crate::object::EDIT_HANDLER`]): its changes are
    /// that handler's writes. Returns the edit's answer.
    fn object_edit(&self, run: RunId, edit: &[u8]) -> Result<Payload, Error>;
    /// Calls handler `handler` of key `key` of object type `wtype` from
    /// workflow code: a `Child` operation flagged [`policy::OBJECT`].
    /// Returns `None` for a one-way call (`detached`).
    fn begin_call(
        &self,
        run: RunId,
        wtype: &str,
        key: &str,
        handler: &str,
        arg: Payload,
        detached: bool,
    ) -> Result<Option<Seq>, Error>;
    /// An update handler returned.
    fn update_done(&self, run: RunId, ordinal: u64, outcome: RunOutcome) -> Result<(), Error>;
    /// Ends the run and starts a new run of its ID with `input`.
    fn continue_as_new(
        &self,
        run: RunId,
        input: Payload,
        version: Option<u32>,
    ) -> Result<(), Error>;
    /// Journal operations the code has started (deterministic).
    fn journal_size(&self, run: RunId) -> u32;
    /// Requests cancellation of another workflow once this point is durable.
    fn cancel_workflow(&self, run: RunId, target: &str, reason: &str) -> Result<(), Error>;
    /// A ticket for one attempt's limit permit.
    fn limit_ticket(&self) -> u64;
    /// Asks for a permit, waiting at `priority` (see [`crate::limits`]).
    /// `waker: None` wakes the run's code (foreign hosts).
    fn acquire_in(
        &self,
        run: RunId,
        limit: &str,
        ticket: u64,
        priority: &Priority,
        waker: Option<&Waker>,
    ) -> Acquire;
    /// [`acquire_in`](Self::acquire_in) at the default priority.
    fn acquire(&self, run: RunId, limit: &str, ticket: u64, waker: Option<&Waker>) -> Acquire {
        self.acquire_in(run, limit, ticket, &Priority::default(), waker)
    }
    /// Returns a permit (or withdraws a queued ticket).
    fn release(&self, limit: &str, ticket: u64);
    /// Opens stream `name` from workflow code (`step: None`) or the
    /// function of the step at `step` (see [`crate::stream`]).
    fn stream_open(&self, run: RunId, name: &str, step: Option<Seq>) -> Result<StreamOpen, Error>;
    /// Appends a chunk: a tagged payload (raw bytes after a zero byte,
    /// JSON otherwise). Never waits for the disk.
    fn stream_append(
        &self,
        run: RunId,
        name: &str,
        step: Option<Seq>,
        payload: Payload,
    ) -> Result<(), Error>;
    fn stream_close(&self, run: RunId, name: &str, step: Option<Seq>) -> Result<(), Error>;
    /// The first `limit` chunks the step at `step` recorded to stream
    /// `name`, decrypted.
    fn stream_recorded(
        &self,
        run: RunId,
        name: &str,
        step: Seq,
        limit: u64,
    ) -> Result<Vec<Payload>, Error>;
}

/// The text form of a completion token: `fusee:<run>:<seq>:<attempt>:<hex ID>`.
pub fn format_token(id: &str, run: RunId, seq: Seq, attempt: u32) -> String {
    let hex: String = id.bytes().map(|b| format!("{b:02x}")).collect();
    format!("fusee:{run}:{seq}:{attempt}:{hex}")
}

/// Parses a completion token into `(workflow ID, run, seq, attempt)`.
pub fn parse_token(token: &str) -> Option<(String, RunId, Seq, u32)> {
    let mut parts = token.strip_prefix("fusee:")?.splitn(4, ':');
    let run = parts.next()?.parse().ok()?;
    let seq = parts.next()?.parse().ok()?;
    let attempt = parts.next()?.parse().ok()?;
    let hex = parts.next()?;
    if hex.len() % 2 != 0 {
        return None;
    }
    let bytes: Option<Vec<u8>> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect();
    Some((String::from_utf8(bytes?).ok()?, run, seq, attempt))
}

/// Whether a journal ends with a step paused: an `Attempt` that waits for
/// a resume, with no resume or result after it.
fn journal_paused(recs: &[RecordBuf]) -> bool {
    let mut paused: Vec<Seq> = Vec::new();
    for r in recs {
        match r {
            Record::Attempt {
                seq, retry_at_ms, ..
            } => {
                paused.retain(|s| s != seq);
                if *retry_at_ms == PAUSED {
                    paused.push(*seq);
                }
            }
            Record::Resume { seq, .. } | Record::Step { seq, .. } => paused.retain(|s| s != seq),
            Record::Cancel { .. } => paused.clear(),
            _ => {}
        }
    }
    !paused.is_empty()
}

/// Merges search attribute objects, later keys winning, into JSON text.
pub fn merge_attributes(objects: &[Payload]) -> String {
    let mut merged = serde_json::Map::new();
    for o in objects {
        if let Ok(serde_json::Value::Object(m)) = serde_json::from_slice::<serde_json::Value>(o) {
            for (k, v) in m {
                if v.is_null() {
                    merged.remove(&k);
                } else {
                    merged.insert(k, v);
                }
            }
        }
    }
    serde_json::Value::Object(merged).to_string()
}

fn err_slot(run: RunId) -> Error {
    Error::Rejected(format!("run {run} is not active on this shard"))
}

/// Why a run stopped when its code asked for an idempotency key with no
/// step running.
pub const KEY_OUTSIDE_STEP: &str = "key() was called outside a step's function, where there is no \
step to key. Call it inside the step's function (in TypeScript and Python, use the step context's \
s.key). See https://github.com/bitsinfra/fusee/blob/main/docs/errors.md#key-outside-a-step";

/// A determinism error stops the run (it is not a workflow failure): the
/// run stays as it is until code that matches its journal is deployed.
fn stop_on<E: Env, T>(
    i: &mut Inner<E>,
    run: RunId,
    r: Result<T, fusee_core::run::DeterminismError>,
) -> Result<T, Error> {
    r.map_err(|e| {
        let msg = e.to_string();
        i.make_stuck(run, msg.clone());
        Error::Determinism(msg)
    })
}

impl<E: Env> Host for RefCell<Inner<E>> {
    fn begin_step(&self, run: RunId, name: &str) -> Result<(Seq, StepStart, Lsn), Error> {
        let mut i = self.borrow_mut();
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        let gate = slot.machine.gate;
        let r = slot.machine.begin_step(name);
        let (seq, start) = stop_on(&mut i, run, r)?;
        if i.cfg.step_times && matches!(start, StepStart::Run { .. }) {
            let now = i.now();
            i.step_began.entry((run, seq)).or_insert(now);
        }
        Ok((seq, start, gate))
    }

    fn may_run(&self, run: RunId, gate: Lsn, not_before_ms: u64, waker: &Waker) -> bool {
        let mut i = self.borrow_mut();
        if i.cfg.replay_only || i.failed.is_some() {
            return false;
        }
        let mut durable = i.commit >= gate || i.cfg.sabotage == Sabotage::SkipGate;
        // The tick's reading may trail the code's, which found the time
        // reached: read again before waiting for it.
        let time = not_before_ms == 0 || i.now() >= not_before_ms || i.now_exact() >= not_before_ms;
        if !durable && time && i.flush_at_gate() {
            durable = i.commit >= gate;
        }
        if durable && time {
            return true;
        }
        if !durable {
            i.metrics.gate_waits += 1;
            // Inside a tick, the round's start; a foreign host's call
            // between ticks reads the clock.
            let began = if i.clock.on {
                i.round_us
            } else {
                i.metric_us()
            };
            let register = match i.slots.get_mut(&run) {
                Some(slot) if slot.gate_at.is_some_and(|g| g <= gate) => false,
                Some(slot) => {
                    slot.gate_at = Some(gate);
                    true
                }
                None => true,
            };
            if register {
                i.gates.push((gate, run, began));
            }
        }
        // A no-op waker (a foreign host that keeps its own timer) cannot be
        // woken: registering it would only leave an entry behind.
        if !time && !waker.will_wake(Waker::noop()) {
            let id = i.timer_seq;
            i.timer_seq += 1;
            i.timers.push(Reverse((not_before_ms, id)));
            i.timer_wakers.insert(id, waker.clone());
        }
        false
    }

    fn set_current_step(&self, run: RunId, seq: Option<Seq>) {
        let mut i = self.borrow_mut();
        match seq {
            Some(s) => {
                i.current_step.insert(run, s);
            }
            None => {
                i.current_step.remove(&run);
            }
        }
    }

    fn current_step(&self, run: RunId) -> Option<Seq> {
        self.borrow().current_step.get(&run).copied()
    }

    fn step_done_inline(&self, run: RunId, seq: Seq, payload: &[u8]) -> bool {
        let mut i = self.borrow_mut();
        // A codec seals what is recorded: such results take `step_done`.
        if i.codec.is_some() || i.inline_deliveries == 0 {
            return false;
        }
        let Some(slot) = i.slots.get_mut(&run) else {
            return false;
        };
        if slot.foreign || slot.resolved.is_some() {
            return false;
        }
        let Some(record) = slot.machine.take_live_step(seq) else {
            return false;
        };
        let fusee_core::run::LiveRecord::Step { name, attempt } = record else {
            return false;
        };
        i.inline_deliveries -= 1;
        i.clock.ran.set(true);
        i.metrics.steps_run += 1;
        #[cfg(feature = "opentelemetry")]
        if let Some(telemetry) = &i.cfg.telemetry {
            telemetry.step(i.cfg.shard);
        }
        if !i.heartbeats.is_empty() {
            i.heartbeats.remove(&(run, seq));
        }
        let time = i.step_time(run, seq);
        let rec: fusee_core::record::RecordRef<'_> = Record::Step {
            run,
            seq,
            name: &name,
            attempt,
            status: Status::Ok,
            payload,
            time,
        };
        let appended = i.append_with(&rec, None);
        // The name goes back to the run's cache for its next step, moved
        // rather than dropped (a reference count decrement a step).
        if let Some(slot) = i.slots.get_mut(&run) {
            slot.machine.return_name(name);
        }
        if let Err(e) = appended {
            i.fail(e.to_string());
        }
        true
    }

    fn hold_inline(&self) -> u32 {
        std::mem::take(&mut self.borrow_mut().inline_deliveries)
    }

    fn resume_inline(&self, held: u32) {
        self.borrow_mut().inline_deliveries = held;
    }

    fn step_done(&self, run: RunId, seq: Seq, status: Status, payload: Payload) {
        let mut i = self.borrow_mut();
        i.clock.ran.set(true);
        i.metrics.steps_run += 1;
        #[cfg(feature = "opentelemetry")]
        if let Some(telemetry) = &i.cfg.telemetry {
            telemetry.step(i.cfg.shard);
        }
        if !i.heartbeats.is_empty() {
            i.heartbeats.remove(&(run, seq));
        }
        let (status, payload) = if status == Status::Ok {
            match i.seal(payload) {
                Ok(p) => (status, p),
                Err(e) => (Status::Failed, Arc::from(e.to_string().as_bytes())),
            }
        } else {
            (status, payload)
        };
        // The result is the only delivery the run could take next: record
        // it and resolve the step now, without queueing the completion for
        // `deliver` to take back (what the block below does, in fewer
        // moves).
        if i.inline_deliveries > 0
            && let Some(slot) = i.slots.get_mut(&run)
            && !slot.foreign
            && slot.resolved.is_none()
            && let Some(record) = slot.machine.take_live_step(seq)
        {
            i.inline_deliveries -= 1;
            let outcome = Outcome { status, payload };
            let time = i.step_time(run, seq);
            match i.append_with(&record.record(run, seq, &outcome, time), None) {
                Ok(_) => match i.open_outcome(outcome) {
                    Ok(o) => i.resolve(run, seq, o),
                    Err(e) => {
                        i.make_stuck(run, format!("a recorded payload cannot be read: {e}"));
                    }
                },
                Err(e) => i.fail(e.to_string()),
            }
            return;
        }
        if let Some(slot) = i.slots.get_mut(&run)
            && slot
                .machine
                .complete(seq, Completion::Step { status, payload })
        {
            i.note_undelivered(run);
            // Complete and deliver in one: when this result is the only
            // delivery the run could take next, take it now, so the code
            // continues in this poll instead of returning pending for the
            // drive to deliver and poll again. Deliveries keep their order
            //. A budget per poll lets other runs
            // in.
            if i.inline_deliveries > 0
                && let Some(slot) = i.slots.get(&run)
                && !slot.foreign
                && slot.resolved.is_none()
                && slot.machine.delivers_only(seq)
            {
                i.inline_deliveries -= 1;
                match i.deliver(run) {
                    Ok(Some(d @ Delivery::Resolve { .. })) => i.store_delivery(run, d),
                    Ok(_) => {}
                    Err(e) => i.fail(e.to_string()),
                }
            }
        }
    }

    fn step_retry(
        &self,
        run: RunId,
        seq: Seq,
        retry_at_ms: u64,
        error: Payload,
    ) -> Result<Lsn, Error> {
        let mut i = self.borrow_mut();
        i.clock.ran.set(true);
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        let paused = (retry_at_ms == PAUSED).then(|| error.clone());
        match slot.machine.step_retry(seq, retry_at_ms, error) {
            Some(rec) => {
                let lsn = i.append(&rec)?;
                if let Some(error) = paused {
                    i.report_pause(run, seq, &error);
                }
                Ok(lsn)
            }
            None => Ok(i.commit),
        }
    }

    fn step_paused(&self, run: RunId, seq: Seq) -> bool {
        self.borrow()
            .slots
            .get(&run)
            .is_some_and(|s| s.machine.step_paused(seq))
    }

    fn step_pending(&self, run: RunId, seq: Seq) -> bool {
        self.borrow()
            .slots
            .get(&run)
            .is_some_and(|s| s.machine.step_pending(seq))
    }

    fn on_exhausted(&self) -> crate::ctx::Exhausted {
        self.borrow().cfg.on_exhausted
    }

    fn step_attempt(&self, run: RunId, seq: Seq) -> u32 {
        self.borrow()
            .slots
            .get(&run)
            .and_then(|s| s.machine.step_attempt(seq))
            .unwrap_or(1)
    }

    fn begin_sleep(&self, run: RunId, name: &str, fire_at_ms: u64) -> Result<Seq, Error> {
        let mut i = self.borrow_mut();
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        let r = slot.machine.begin_sleep(name, fire_at_ms);
        let (seq, arm) = stop_on(&mut i, run, r)?;
        if let Arm::Append(rec) = arm {
            i.append(&rec)?;
        }
        Ok(seq)
    }

    fn begin_wait(&self, run: RunId, name: &str, deadline_ms: u64) -> Result<Seq, Error> {
        let mut i = self.borrow_mut();
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        let r = slot.machine.begin_wait(name, deadline_ms);
        let (seq, arm) = stop_on(&mut i, run, r)?;
        if let Arm::Append(rec) = arm {
            i.append(&rec)?;
        }
        Ok(seq)
    }

    fn begin_child(
        &self,
        run: RunId,
        wtype: &str,
        id: &str,
        flags: u8,
        deadline_ms: u64,
        input: Payload,
    ) -> Result<Option<Seq>, Error> {
        let detached = flags & policy::DETACHED != 0;
        let mut i = self.borrow_mut();
        let input = i.seal(input)?;
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        let r = slot
            .machine
            .begin_child(wtype, id, flags, deadline_ms, input);
        let (seq, arm) = stop_on(&mut i, run, r)?;
        if let Arm::Append(rec) = arm {
            i.append(&rec)?;
        }
        Ok((!detached).then_some(seq))
    }

    fn value(
        &self,
        run: RunId,
        kind: u8,
        make: &mut dyn FnMut(u64, u64) -> Payload,
    ) -> Result<Payload, Error> {
        let mut i = self.borrow_mut();
        let now = i.now_exact();
        let rnd = i.env.random();
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        let r = slot.machine.value(kind, || make(now, rnd));
        let (_, v, rec) = stop_on(&mut i, run, r)?;
        if let Some(rec) = rec {
            i.append(&rec)?;
        }
        Ok(v)
    }

    fn send(&self, run: RunId, target: &str, name: &str, payload: Payload) -> Result<(), Error> {
        let mut i = self.borrow_mut();
        let payload = i.seal(payload)?;
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        let r = slot.machine.send(target, name, payload);
        let (_, rec) = stop_on(&mut i, run, r)?;
        if let Some(rec) = rec {
            i.append(&rec)?;
        }
        Ok(())
    }

    fn take_resolution(&self, run: RunId, seq: Seq, waker: &Waker) -> Option<Outcome> {
        use std::collections::hash_map::Entry;
        let mut guard = self.borrow_mut();
        let i = &mut *guard;
        // The drive polls the run's code after every delivery, so a future
        // polled with the run's own waker needs no wake.
        let own = match i.slots.get_mut(&run) {
            Some(slot) => {
                if slot.resolved.as_ref().is_some_and(|(s, _)| *s == seq) {
                    return slot.resolved.take().map(|(_, o)| o);
                }
                std::ptr::eq(waker.data(), Arc::as_ptr(&slot.waker).cast())
            }
            None => false,
        };
        if own && i.resolved.is_empty() {
            return None;
        }
        match i.resolved.entry((run, seq)) {
            Entry::Occupied(mut e) => match e.get_mut() {
                Resolution::Ready(_) => match e.remove() {
                    Resolution::Ready(o) => Some(o),
                    Resolution::Waiting(_) => None,
                },
                Resolution::Waiting(w) => {
                    if !w.will_wake(waker) {
                        *w = waker.clone();
                    }
                    None
                }
            },
            Entry::Vacant(e) => {
                if !own {
                    e.insert(Resolution::Waiting(waker.clone()));
                }
                None
            }
        }
    }

    fn forget(&self, run: RunId, seq: Seq) {
        if let Ok(mut i) = self.try_borrow_mut() {
            if i.reaping.contains(&run) {
                return;
            }
            let abandon = i
                .slots
                .get_mut(&run)
                .and_then(|slot| slot.machine.forget(seq));
            if let Some(rec) = abandon
                && let Err(e) = i.append(&rec)
            {
                i.fail(e.to_string());
            }
            if let Some(slot) = i.slots.get_mut(&run)
                && slot.resolved.as_ref().is_some_and(|(s, _)| *s == seq)
            {
                slot.resolved = None;
            }
            if !i.resolved.is_empty() {
                i.resolved.remove(&(run, seq));
            }
            i.heartbeats.remove(&(run, seq));
            if let Some(tokens) = i.completion_acks.remove(&(run, seq)) {
                let tail = i.writer.last_lsn();
                for t in tokens {
                    i.core.add_output(
                        tail,
                        Output::Refuse {
                            token: t,
                            message: Arc::from(&b"the step is no longer waiting"[..]),
                        },
                    );
                }
            }
        }
    }

    fn now_ms(&self) -> u64 {
        // The code computes deadlines from it: never the tick's reading.
        self.borrow().now_exact()
    }

    fn replaying(&self, run: RunId) -> bool {
        self.borrow()
            .slots
            .get(&run)
            .is_some_and(|s| s.machine.is_replaying())
    }

    fn op_name(&self, run: RunId, seq: Seq) -> Option<String> {
        self.borrow()
            .slots
            .get(&run)
            .and_then(|s| s.machine.pending_name(seq).map(str::to_owned))
    }

    fn key(&self, run: RunId, seq: Seq) -> String {
        if seq == Seq::MAX {
            // No step's function is running, so there is no step to key:
            // one key for every such call would make unrelated effects look
            // like retries of one another. The run stops, as for a
            // determinism error, with a message that says what to change.
            let mut i = self.borrow_mut();
            if i.slots.contains_key(&run) {
                i.make_stuck(run, KEY_OUTSIDE_STEP.to_owned());
            }
            return String::new();
        }
        self.borrow()
            .slots
            .get(&run)
            .map(|s| s.machine.key(seq))
            .unwrap_or_default()
    }

    fn info(&self, run: RunId) -> Option<RunInfo> {
        self.borrow()
            .slots
            .get(&run)
            .map(|s| s.machine.info().clone())
    }

    fn wake_at(&self, at_ms: u64, waker: &Waker) -> u64 {
        let mut i = self.borrow_mut();
        let id = i.timer_seq;
        i.timer_seq += 1;
        i.timers.push(Reverse((at_ms, id)));
        i.timer_wakers.insert(id, waker.clone());
        id
    }

    fn cancel_timer(&self, id: u64) {
        // A future dropped while the runtime is borrowed leaves its timer to fire.
        if let Ok(mut i) = self.try_borrow_mut() {
            i.cancel_timer(id);
        }
    }

    fn broadcasts(&self, run: RunId) -> u64 {
        self.borrow().slots.get(&run).map_or(0, |s| s.broadcasts)
    }

    fn wake_run_at(&self, run: RunId, at_ms: u64) {
        let w = self
            .borrow()
            .slots
            .get(&run)
            .map(|s| Waker::from(s.waker.clone()));
        if let Some(w) = w {
            self.wake_at(at_ms, &w);
        }
    }

    fn begin_await(&self, run: RunId, name: &str) -> Result<(Seq, AwaitStart, Lsn), Error> {
        let mut i = self.borrow_mut();
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        let gate = slot.machine.gate;
        let r = slot.machine.begin_await(name);
        let (seq, start) = stop_on(&mut i, run, r)?;
        Ok((seq, start, gate))
    }

    fn completion_token(&self, run: RunId, seq: Seq) -> Result<String, Error> {
        let i = self.borrow();
        let slot = i.slots.get(&run).ok_or_else(|| err_slot(run))?;
        let attempt = slot
            .machine
            .await_attempt(seq)
            .ok_or_else(|| Error::Rejected("the step is not waiting for a completion".into()))?;
        Ok(format_token(&slot.machine.info().id, run, seq, attempt))
    }

    fn awaiting(&self, run: RunId, seq: Seq) -> Result<(), Error> {
        let mut i = self.borrow_mut();
        i.clock.ran.set(true);
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        if let Some(rec) = slot.machine.awaiting(seq) {
            i.append(&rec)?;
        }
        Ok(())
    }

    fn heartbeat(&self, run: RunId, seq: Seq, progress: Payload) -> Result<(), Error> {
        let mut i = self.borrow_mut();
        let progress = i.seal(progress)?;
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        if let Some(rec) = slot.machine.heartbeat(seq, progress) {
            i.heartbeats.insert((run, seq), rec);
        }
        Ok(())
    }

    fn progress(&self, run: RunId, seq: Seq) -> Option<Payload> {
        let i = self.borrow();
        let p = i.slots.get(&run)?.machine.progress(seq)?;
        i.open(p).ok()
    }

    fn patched(&self, run: RunId, id: &str, deprecated: bool) -> Result<bool, Error> {
        let mut i = self.borrow_mut();
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        let (on, rec) = slot.machine.patched(id);
        if let Some(rec) = rec {
            i.append(&rec)?;
        }
        Ok(on || deprecated)
    }

    fn set_attributes(&self, run: RunId, attributes: Payload) -> Result<(), Error> {
        let mut i = self.borrow_mut();
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        let r = slot.machine.set_attributes(attributes);
        let (_, rec) = stop_on(&mut i, run, r)?;
        if let Some(rec) = rec {
            i.append(&rec)?;
        }
        Ok(())
    }

    fn object_get(&self, run: RunId, key: &str, exclusive: bool) -> Result<Option<Payload>, Error> {
        Ok(self.borrow().object_run(run)?.get(key, exclusive))
    }

    fn object_set(&self, run: RunId, key: &str, value: Option<Payload>) -> Result<(), Error> {
        self.borrow_mut().object_write(run, key, value)
    }

    fn object_keys(&self, run: RunId, exclusive: bool) -> Result<Vec<String>, Error> {
        Ok(self.borrow().object_run(run)?.keys(exclusive))
    }

    fn object_state(&self, run: RunId) -> Result<Payload, Error> {
        Ok(crate::object::state_json(
            &self.borrow().object_run(run)?.state,
        ))
    }

    fn object_edit(&self, run: RunId, edit: &[u8]) -> Result<Payload, Error> {
        self.borrow_mut().object_edit(run, edit)
    }

    fn begin_call(
        &self,
        run: RunId,
        wtype: &str,
        key: &str,
        handler: &str,
        arg: Payload,
        detached: bool,
    ) -> Result<Option<Seq>, Error> {
        let (id, flags, input) = self
            .borrow()
            .object_call(run, wtype, key, handler, &arg, detached)?;
        self.begin_child(run, wtype, &id, flags, 0, input)
    }

    fn update_done(&self, run: RunId, ordinal: u64, outcome: RunOutcome) -> Result<(), Error> {
        let mut i = self.borrow_mut();
        let r = i.update_done(run, ordinal, outcome);
        i.on_commit();
        r
    }

    fn continue_as_new(
        &self,
        run: RunId,
        input: Payload,
        version: Option<u32>,
    ) -> Result<(), Error> {
        self.borrow_mut()
            .continue_as_new(run, input, version)
            .map(|_| ())
    }

    fn journal_size(&self, run: RunId) -> u32 {
        self.borrow()
            .slots
            .get(&run)
            .map(|s| s.machine.journal_size())
            .unwrap_or(0)
    }

    fn cancel_workflow(&self, run: RunId, target: &str, reason: &str) -> Result<(), Error> {
        let mut i = self.borrow_mut();
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        let r = slot
            .machine
            .send(target, CANCEL_SIGNAL, Arc::from(reason.as_bytes()));
        let (_, rec) = stop_on(&mut i, run, r)?;
        if let Some(rec) = rec {
            i.append(&rec)?;
        }
        Ok(())
    }

    fn limit_ticket(&self) -> u64 {
        self.borrow().limits.ticket()
    }

    fn acquire_in(
        &self,
        run: RunId,
        limit: &str,
        ticket: u64,
        priority: &Priority,
        waker: Option<&Waker>,
    ) -> Acquire {
        let (limits, now, own) = {
            let i = self.borrow();
            let own = match waker {
                Some(_) => None,
                None => i.slots.get(&run).map(|s| Waker::from(s.waker.clone())),
            };
            (i.limits.clone(), i.now(), own)
        };
        let w = waker.or(own.as_ref());
        if self.borrow().cfg.sabotage == Sabotage::IgnoreLimits {
            limits.define(
                limit,
                crate::limits::LimitSpec {
                    concurrent: 1_000,
                    per_second: 0.0,
                },
            );
        }
        let a = limits.acquire_in(limit, ticket, priority, now, w);
        if let (
            Acquire::Wait {
                retry_at_ms: Some(at),
            },
            Some(w),
        ) = (a, w)
        {
            self.wake_at(at, w);
        }
        a
    }

    fn release(&self, limit: &str, ticket: u64) {
        let limits = self.borrow().limits.clone();
        limits.release(limit, ticket);
    }

    fn stream_open(&self, run: RunId, name: &str, step: Option<Seq>) -> Result<StreamOpen, Error> {
        let mut i = self.borrow_mut();
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        slot.machine
            .stream_open(name, step)
            .map_err(Error::Rejected)
    }

    fn stream_append(
        &self,
        run: RunId,
        name: &str,
        step: Option<Seq>,
        payload: Payload,
    ) -> Result<(), Error> {
        let mut i = self.borrow_mut();
        let payload = i.seal(payload)?;
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        if let Some(rec) = slot
            .machine
            .stream_append(name, step, payload)
            .map_err(Error::Rejected)?
        {
            i.append(&rec)?;
        }
        Ok(())
    }

    fn stream_close(&self, run: RunId, name: &str, step: Option<Seq>) -> Result<(), Error> {
        let mut i = self.borrow_mut();
        let slot = i.slots.get_mut(&run).ok_or_else(|| err_slot(run))?;
        if let Some(rec) = slot
            .machine
            .stream_close(name, step)
            .map_err(Error::Rejected)?
        {
            i.append(&rec)?;
        }
        Ok(())
    }

    fn stream_recorded(
        &self,
        run: RunId,
        name: &str,
        step: Seq,
        limit: u64,
    ) -> Result<Vec<Payload>, Error> {
        let mut i = self.borrow_mut();
        let mut out = Vec::new();
        for r in i.journal_of(run)? {
            if out.len() as u64 >= limit {
                break;
            }
            if let Record::Chunk {
                seq,
                stream,
                payload,
                ..
            } = r
                && seq == step
                && *stream == *name
            {
                out.push(i.open(payload)?);
            }
        }
        Ok(out)
    }
}

/// What a shard has left to do, for a host that closes it once it is idle
/// (server mode's invoked workers, see [`Env::due_wanted`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Due {
    /// Work goes on without any run: records or a checkpoint wait for the
    /// log, events or deliveries wait for code, or a tick hit its limit.
    pub busy: bool,
    /// Workflow types of loaded runs that are not suspended (a step is
    /// running, code is replaying, or a delivery waits for it). A host
    /// without code for a type cannot advance its runs.
    pub running: Vec<String>,
    /// Workflow types with runs waiting for their code to register here.
    pub awaiting: Vec<String>,
    /// The earliest time the shard has work again by itself: a timer
    /// (durable, or a step's retry or timeout), or the resend of a message
    /// another shard has not acknowledged. May be early, never late.
    pub at_ms: Option<u64>,
    /// Live step executions since the shard opened.
    pub steps: u64,
    /// How long the shard's last open took, in microseconds.
    pub open_us: u64,
    /// Messages to other shards not acknowledged yet.
    pub unacked: usize,
}

/// What a [`ShardRuntime::tick`] left behind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Tick {
    /// Call `tick` again by this time even if nothing wakes the shard.
    pub next_wake_ms: Option<u64>,
    /// Work is ready right now (the round limit was hit).
    pub busy: bool,
}

pub struct ShardRuntime<E: Env + 'static> {
    inner: Rc<RefCell<Inner<E>>>,
    host: Rc<dyn Host>,
    /// Native runs' code, at their slot's index (see [`Slots`]).
    futures: RefCell<Vec<Option<Code>>>,
    /// Running update handlers of native runs, in ordinal order.
    update_futs: RefCell<FxHashMap<RunId, Vec<(u64, LocalFuture)>>>,
    /// Query and update handlers native code registered.
    handlers: RefCell<FxHashMap<RunId, Rc<Handlers>>>,
    /// Emptied handler sets of finished code, for the next run's (one
    /// allocation fewer per wake).
    spare_handlers: RefCell<Vec<Rc<Handlers>>>,
    /// Queries waiting for their run to finish replaying.
    deferred: RefCell<Vec<(RunId, String, Payload, u64)>>,
    /// Native step functions that are pending (see [`Self::waits_outside`]).
    outside: Rc<Cell<u32>>,
    /// The list `drive_ready` takes woken runs into, kept between rounds.
    spare: RefCell<Vec<RunId>>,
    /// How many runs the last round drove ([`Self::behind`]).
    last_drives: Cell<usize>,
    /// The tick's list of wakes (see [`LocalTick`]), kept between ticks.
    local: RefCell<Vec<RunId>>,
    /// The code this shard has started ([`CodeCache`]).
    code: RefCell<CodeCache>,
}

impl<E: Env + 'static> Drop for ShardRuntime<E> {
    fn drop(&mut self) {
        // Runs still loaded: their code first, then their handlers, which
        // hold each run's `Ctx` (and through it the host) in a cycle.
        self.update_futs.get_mut().clear();
        self.futures.get_mut().clear();
        for h in self.handlers.get_mut().values() {
            h.clear();
        }
        self.handlers.get_mut().clear();
    }
}

/// A registry holding only the built-in workflows (schedules).
pub fn builtin_registry() -> SharedRegistry {
    let mut r = Registry::default();
    crate::schedule::register_builtins(&mut r);
    Arc::new(RwLock::new(r))
}

fn io_err(e: impl std::fmt::Display) -> Error {
    Error::Io(e.to_string())
}

/// Scans segment `seg` reading only as much of the file as the scan needs:
/// the valid records and the first zero block after them, or up to the next
/// zero block after a damaged frame. Collects records after `after`. A
/// damaged header comes back with the whole file, which callers need to
/// decide whether it was ever flushed.
#[allow(clippy::type_complexity)]
fn read_valid_prefix<E: Env>(
    env: &mut E,
    seg: u64,
    after: Lsn,
    records: &mut Vec<(Lsn, RecordBuf)>,
) -> Result<Result<Option<Scanned>, (ScanError, Vec<u8>)>, Error> {
    const FIRST_READ: usize = 256 << 10;
    const MAX_READ: usize = 16 << 20;
    let (mut bytes, mut eof) = env.read_segment_from(seg, 0, FIRST_READ)?;
    let more = |env: &mut E, bytes: &mut Vec<u8>, len: usize| -> Result<bool, Error> {
        let (next, end) = env.read_segment_from(seg, bytes.len() as u64, len)?;
        if next.is_empty() && !end {
            return Err(io_err(format!(
                "reading log segment {seg} made no progress"
            )));
        }
        bytes.extend_from_slice(&next);
        Ok(end)
    };
    let mut scanner = match SegmentScanner::start(&bytes, eof) {
        Ok(Some(s)) => s,
        Ok(None) => return Ok(Ok(None)),
        Err(e) => {
            while !eof {
                eof = more(env, &mut bytes, MAX_READ)?;
            }
            return Ok(Err((e, bytes)));
        }
    };
    let mut chunk = FIRST_READ;
    loop {
        let progress = scanner.advance(&bytes, eof, &mut |lsn, rec| {
            if lsn > after {
                records.push((lsn, rec));
            }
        });
        match progress {
            Progress::Done(s) => return Ok(Ok(Some(s))),
            Progress::NeedMore => {
                eof = more(env, &mut bytes, chunk)?;
                chunk = (chunk * 2).min(MAX_READ);
            }
        }
    }
}

impl<E: Env + 'static> ShardRuntime<E> {
    /// Opens a shard: recovers its state from `env`, then resumes its runs.
    /// `registry: None` means a foreign host runs the code (see
    /// [`ForeignEvent`]).
    pub fn open(
        env: E,
        cfg: RuntimeConfig,
        registry: Option<SharedRegistry>,
        wake: Notify,
    ) -> Result<Self, Error> {
        let (registry, foreign_host) = match registry {
            Some(r) => (r, false),
            None => (builtin_registry(), true),
        };
        Self::open_with(env, cfg, registry, foreign_host, Services::default(), wake)
    }

    /// Opens a shard. `foreign_host`: a language binding runs workflow code
    /// the registry does not have.
    pub fn open_with(
        mut env: E,
        cfg: RuntimeConfig,
        registry: SharedRegistry,
        foreign_host: bool,
        services: Services,
        wake: Notify,
    ) -> Result<Self, Error> {
        crate::trim::tune();
        env.set_notify(wake.clone());
        let now = env.now_ms();
        let opened_us = env.monotonic_us();
        let mut core = ShardCore::new(cfg.shard, cfg.shards, cfg.retention_ms, now);

        // 1. Checkpoint and run store.
        let cp = env.load_checkpoint()?;
        if let Some(cp) = &cp
            && (cp.shards != cfg.shards || cp.shard != cfg.shard)
        {
            return Err(Error::Io(format!(
                "data directory was created with {} shards (this is shard {} of it); \
                     open it with the same shard count",
                cp.shards, cp.shard
            )));
        }
        let (generation, len) = cp
            .as_ref()
            .map(|c| (c.store_generation, c.store_len))
            .unwrap_or((0, 0));
        let saved_index = cp.as_ref().and_then(|c| c.index.clone());
        // An environment that can opens the prefix without reading it when
        // the checkpoint names a persisted index; others visit every frame,
        // and the newest frame of each run rebuilds the table.
        let mut scanned: FxHashMap<RunId, (u64, u32, StoreEntry)> = FxHashMap::default();
        // Runs whose newest frame is a `Delta` ending with `Done`.
        let mut done_deltas: FxHashMap<RunId, (Status, u64)> = FxHashMap::default();
        let mut visited = 0u64;
        env.recover_store(generation, len, &mut |off, len, e| {
            visited += 1;
            match e {
                StoreEntry::Tombstone { run } => {
                    scanned.remove(&run);
                }
                StoreEntry::Index { .. } => {}
                mut e => {
                    if let StoreEntry::Delta { run, journal, .. } = &e {
                        match fusee_core::store::journal_done(journal) {
                            Some(done) => done_deltas.insert(*run, done),
                            None => done_deltas.remove(run),
                        };
                    } else {
                        done_deltas.remove(&e.run());
                    }
                    // Keep only what the table needs; journals are read on load.
                    match &mut e {
                        StoreEntry::Park { journal, .. }
                        | StoreEntry::Done { journal, .. }
                        | StoreEntry::Delta { journal, .. } => *journal = Vec::new(),
                        StoreEntry::Tombstone { .. } | StoreEntry::Index { .. } => {}
                    }
                    scanned.insert(e.run(), (off, len, e));
                }
            }
        })?;
        let indexed = visited == 0 && saved_index.is_some();
        let mut store_read = 0u64;
        if indexed {
            let frames = saved_index.as_deref().unwrap_or_default();
            core.reserve_runs(cp.as_ref().map_or(0, |c| c.index_runs as usize));
            for b in frames {
                let frame = env.store_read(b.offset, b.len)?;
                store_read += u64::from(b.len);
                match read_entry(&frame).map_err(io_err)? {
                    StoreEntry::Index { records } => core.load_index(&records).map_err(io_err)?,
                    _ => {
                        return Err(Error::Io(format!(
                            "run store frame at {} is not part of the run index",
                            b.offset
                        )));
                    }
                }
            }
            core.index_loaded();
        } else {
            for (run, (offset, len, e)) in scanned {
                let blob = BlobRef { offset, len };
                let earliest = |timers: &[(Seq, TimerKind, u64)]| {
                    timers.iter().map(|t| t.2).min().unwrap_or(0)
                };
                match e {
                    StoreEntry::Park {
                        id_hash, timers, ..
                    } => core.insert_parked(run, blob, None, false, id_hash, earliest(&timers)),
                    StoreEntry::Delta {
                        id_hash, timers, ..
                    } => match done_deltas.get(&run) {
                        Some(&(status, finished_ms)) => {
                            core.insert_parked(run, blob, Some(status), true, id_hash, finished_ms)
                        }
                        None => {
                            core.insert_parked(run, blob, None, true, id_hash, earliest(&timers))
                        }
                    },
                    StoreEntry::Done {
                        id_hash,
                        status,
                        finished_ms,
                        ..
                    } => core.insert_parked(run, blob, Some(status), false, id_hash, finished_ms),
                    StoreEntry::Tombstone { .. } | StoreEntry::Index { .. } => {}
                }
            }
        }
        // The store takes format 3 frames only if its header says so (an
        // older store is rewritten once, at its first checkpoint).
        let store_v3 = env
            .store_read(0, fusee_core::store::STORE_HEADER as u32)
            .ok()
            .and_then(|h| fusee_core::store::store_format(&h).ok())
            .is_some_and(|f| f >= 3);
        let after = cp.as_ref().map(|c| c.lsn).unwrap_or(0);
        let ready = Arc::new(ReadyQueue {
            queue: Mutex::new(Vec::new()),
            queued: AtomicBool::new(false),
            wake: wake.clone(),
        });
        let mut inner = Inner {
            env,
            core,
            writer: ShardLog::Segments(LogWriter::new(cfg.shard, 1, 1, cfg.segment_bytes, now)),
            cfg: cfg.clone(),
            slots: Slots::default(),
            resolved: FxHashMap::default(),
            gates: Gates::default(),
            round_us: 0,
            flush_started_us: 0,
            timers: BinaryHeap::new(),
            timer_wakers: FxHashMap::default(),
            timer_seq: 0,
            durable: after,
            commit: after,
            load_room: usize::MAX,
            commit_dirty: false,
            inline_deliveries: 0,
            load_buf: Vec::new(),
            load_at: Vec::new(),
            in_flight: None,
            submitted: VecDeque::new(),
            ready,
            registry,
            foreign_host,
            codec: services.codec,
            limits: services.limits,
            update_waiters: FxHashMap::default(),
            obj: Objects::default(),
            offered: FxHashMap::default(),
            next_update_id: 1,
            completion_acks: FxHashMap::default(),
            heartbeats: BTreeMap::new(),
            step_began: FxHashMap::default(),
            released: Vec::new(),
            outbox: BTreeMap::new(),
            next_out: 1,
            msg_tokens: FxHashMap::default(),
            next_msg_token: MSG_TOKEN,
            child_acks: FxHashMap::default(),
            message_acks: Vec::new(),
            foreign: VecDeque::new(),
            stash: FxHashMap::default(),
            awaiting: FxHashMap::default(),
            instantiate: Vec::new(),
            cancel_tokens: FxHashMap::default(),
            by: None,
            subscribers: FxHashMap::default(),
            stuck: FxHashMap::default(),
            failed: None,
            metrics: Metrics::default(),
            ckpt_pending: None,
            ckpt_running: false,
            last_ckpt_ms: now,
            last_ckpt_lsn: after,
            last_ckpt_store_len: len.max(fusee_core::store::STORE_HEADER),
            obsolete_store_bytes: 0,
            last_ckpt_segment: 0,
            last_sweep_ms: now,
            current_step: FxHashMap::default(),
            replicator: None,
            stall_us: stall_threshold_us(),
            stalls: VecDeque::new(),
            dirty: Vec::new(),
            park_queue: VecDeque::new(),
            removed: Vec::new(),
            undelivered: Vec::new(),
            tick_parked: 0,
            tick_maintenance_us: 0,
            flush_records: 0,
            flush_bytes: 0,
            clock: TickClock::default(),
            held_loads: VecDeque::new(),
            reaping: Vec::new(),
            store_v3,
            no_compaction: false,
            index_gen: generation,
            index_base: saved_index.as_ref().map_or((0, 0), |f| {
                (f.len(), f.iter().map(|b| u64::from(b.len)).sum())
            }),
            index: if indexed { saved_index.clone() } else { None },
            index_delta_bytes: 0,
            index_build: None,
            slow_flushes: 0,
            tail: StoreTail::default(),
            gate_flushes: 0,
            drive_backlog: 0,
            host_queue: None,
            host_never: false,
            enc: Vec::new(),
            timer_scratch: Vec::new(),
            decoded: Vec::new(),
            names: Default::default(),
            slot_spare: Vec::new(),
            streams: FxHashMap::default(),
            stream_bytes: 0,
            stream_waiters: FxHashMap::default(),
            stream_due: BinaryHeap::new(),
            stream_wait_until: u64::MAX,
            trim_peak: 0,
            spare_mark: 0,
            pools_shrunk: false,
        };
        inner.metrics.open_store_frames = if indexed {
            saved_index.as_ref().map_or(0, |f| f.len() as u64)
        } else {
            visited
        };
        inner.metrics.open_store_bytes = if indexed { store_read } else { len };
        if let Some(cp) = &cp {
            // Outbox IDs are per process: acknowledgments in flight died with
            // the old one, so entries are renumbered.
            for mut o in decode_outbox(&cp.outbox).map_err(io_err)? {
                o.id = inner.next_out;
                inner.next_out += 1;
                inner.outbox.insert(o.id, o);
            }
            inner.core.set_next_run_id(cp.next_run);
            for r in &cp.runs {
                let stored = match r.blob {
                    Some(b) => Some((b, inner.read_stored(b)?)),
                    None => None,
                };
                if let Some((_, s)) = &stored {
                    for rec in &s.records {
                        inner.core.apply(r.last_lsn, rec, Mode::BLOB);
                    }
                }
                if let Some((b, s)) = &stored {
                    inner.core.spilled(r.run, *b, s.chain, s.depth);
                }
                let suffix = decode_canonical_all(&r.journal).map_err(io_err)?;
                for rec in &suffix {
                    inner.core.apply(r.last_lsn, rec, Mode::SUFFIX);
                }
            }
        }

        // 2. Log tail after the checkpoint.
        let mut last = after;
        let shared = inner.env.shared_log();
        let mut next_segment = 0;
        let early_checkpoint;
        if shared {
            // Format 3: the shared log hands this shard its records.
            let (records, tail) = inner.env.recover_log(after)?;
            // Replaying a tail again at the next open costs what it costs
            // now: checkpoint as soon as the shard runs.
            early_checkpoint = !records.is_empty();
            for (lsn, rec) in records {
                let run = rec.run();
                if inner.core.is_parked(run) && !matches!(rec, Record::Start { .. }) {
                    inner.unpark_stored(run)?;
                }
                inner.core.apply(lsn, &rec, Mode::RECOVER);
            }
            last = tail.max(after);
        } else {
            let segments = inner.env.segments()?;
            let last_segment = segments.last().copied();
            let max_segment = last_segment.unwrap_or(0);
            // Segments wholly behind the checkpoint hold nothing to replay: a
            // later segment that starts at or before `after + 1` proves it. Only
            // their headers are read. The checkpoint covers durable records only,
            // so no batch can have torn across the boundary skipped here.
            let mut begin = 0;
            if after > 0 {
                for (index, seg) in segments.iter().enumerate().rev() {
                    let head = inner.env.read_segment_header(*seg)?;
                    if let Ok(Some(h)) = fusee_core::segment::SegmentHeader::decode(&head)
                        && h.segment == *seg
                        && h.base_lsn <= after + 1
                    {
                        begin = index;
                        break;
                    }
                }
            }
            let mut expected_next: Option<Lsn> = None;
            let mut first = true;
            // Trailing segments without a valid record (never activated, or
            // holding only a torn first batch). Nothing in them was acknowledged,
            // so they are removed and the first of their numbers is reused, when
            // the engine is the only writer of its segments.
            let exclusive = inner.env.exclusive_log();
            let mut empty_tail: Vec<u64> = Vec::new();
            // Lowest number from which every segment was discarded.
            let mut free_from: Option<u64> = None;
            let mut tail_segments = 0usize;
            let mut tail_bytes = 0u64;
            for &seg in &segments[begin..] {
                let mut records: Vec<(Lsn, RecordBuf)> = Vec::new();
                let scanned: Option<Scanned> = match read_valid_prefix(
                    &mut inner.env,
                    seg,
                    after,
                    &mut records,
                )? {
                    Ok(s) => s,
                    Err((fusee_core::segment::ScanError::BadHeader, bytes))
                        if Some(seg) == last_segment =>
                    {
                        // The newest segment's header is unreadable. If no later
                        // batch landed, its first flush (header included) never
                        // completed and nothing in it was acknowledged.
                        let base = expected_next.unwrap_or(after + 1);
                        if fusee_core::segment::headerless_was_flushed(&bytes, base) {
                            return Err(Error::Io(format!(
                                "log segment {seg} has a damaged header but holds flushed records: \
                                     acknowledged records were lost. Restore from a replica or backup."
                            )));
                        }
                        inner.env.discard_segment(seg)?;
                        free_from = Some(seg);
                        None
                    }
                    Err((e, _)) => return Err(Error::Io(format!("log segment {seg}: {e}"))),
                };
                let Some(s) = scanned else {
                    if free_from != Some(seg) && exclusive {
                        empty_tail.push(seg);
                    }
                    continue;
                };
                if s.header.shard != cfg.shard {
                    return Err(Error::Io(format!(
                        "segment {seg} belongs to shard {}, not {}",
                        s.header.shard, cfg.shard
                    )));
                }
                if first {
                    first = false;
                    if s.header.base_lsn > after + 1 {
                        return Err(Error::Io(format!(
                            "log records {}..{} are missing (segment {seg} starts at {})",
                            after + 1,
                            s.header.base_lsn - 1,
                            s.header.base_lsn
                        )));
                    }
                } else if let Some(n) = expected_next {
                    if s.header.base_lsn > n && s.max_batch_start <= n {
                        // A flush batch spanned a segment roll and tore: the new
                        // segment's start landed but record `n` at the old one's
                        // end did not. Nothing in that batch was acknowledged, so
                        // the log ends before it; drop the rest.
                        inner.env.discard_segment(seg)?;
                        let later: Vec<u64> = inner
                            .env
                            .segments()?
                            .into_iter()
                            .filter(|x| *x > seg)
                            .collect();
                        for l in later {
                            inner.env.discard_segment(l)?;
                        }
                        free_from = Some(seg);
                        break;
                    }
                    if s.header.base_lsn != n {
                        return Err(Error::Io(format!(
                            "log segment {seg} starts at record {} but the previous segment ends at {}: \
                             durable records were lost",
                            s.header.base_lsn,
                            n - 1
                        )));
                    }
                }
                if let Tail::Corrupt { at, later_lsn } = s.tail {
                    return Err(Error::Io(format!(
                        "log segment {seg} is damaged at offset {at} (record {}), but record {later_lsn} \
                         after it is intact: acknowledged records were lost. Restore from a replica or \
                         backup.",
                        s.last_lsn + 1
                    )));
                }
                if s.records == 0 && exclusive {
                    empty_tail.push(seg);
                } else {
                    empty_tail.clear();
                }
                if !records.is_empty() {
                    tail_segments += 1;
                    tail_bytes += s.end as u64;
                }
                for (lsn, rec) in records {
                    let run = rec.run();
                    if inner.core.is_parked(run) && !matches!(rec, Record::Start { .. }) {
                        inner.unpark_stored(run)?;
                    }
                    inner.core.apply(lsn, &rec, Mode::RECOVER);
                }
                last = last.max(s.last_lsn.max(after));
                expected_next = Some(s.last_lsn + 1);
            }
            for &seg in &empty_tail {
                inner.env.discard_segment(seg)?;
            }
            next_segment = empty_tail
                .first()
                .copied()
                .or(free_from)
                .unwrap_or(max_segment + 1);
            // A long tail would be replayed again by the next open, and a
            // crash loop keeps adding segments that only a checkpoint deletes
            // (the older of the two retained checkpoints still needs its
            // suffix): checkpoint as soon as the shard runs.
            let present = inner.env.segments()?.len();
            early_checkpoint =
                tail_segments >= 2 || present >= 3 || tail_bytes >= cfg.segment_bytes / 4;
        }
        inner.obsolete_store_bytes = match &cp {
            // Format 3 checkpoints count it (the store holds `Delta` chains
            // and index frames, which the table does not size).
            Some(c) if c.index.is_some() => c.obsolete,
            _ => {
                let live_len = inner
                    .core
                    .parked_runs()
                    .map(|(_, p)| u64::from(p.blob().len))
                    .chain(inner.core.loaded_runs().filter_map(|(_, l)| {
                        l.blob.map(|b| {
                            if l.blob_depth > 0 {
                                l.blob_chain
                            } else {
                                u64::from(b.len)
                            }
                        })
                    }))
                    .sum::<u64>();
                inner
                    .env
                    .store_len()
                    .saturating_sub(fusee_core::store::STORE_HEADER + live_len)
            }
        };
        inner.env.sync_recovered()?;
        while inner.core.take_event().is_some() {}
        // `memory` mode writes nothing: its records take the shared log's
        // encoding (the default pipeline's, and the journal's own), not a
        // segment's, so each is encoded once.
        if shared || !inner.env.recovers() {
            inner.writer = ShardLog::Sections(fusee_core::wal::SectionWriter::new(last + 1));
            inner.last_ckpt_segment = inner.log_segment();
        } else {
            let segment = next_segment;
            inner.env.open_log(segment)?;
            let mut writer = LogWriter::new(cfg.shard, segment, last + 1, cfg.segment_bytes, now);
            writer.limit_first_segment(inner.env.first_segment_bytes(cfg.segment_bytes));
            inner.writer = ShardLog::Segments(writer);
            inner.last_ckpt_segment = segment;
        }
        if early_checkpoint {
            inner.last_ckpt_ms = 0;
        }
        inner.durable = last;
        inner.commit = last;
        inner.on_commit();

        // 3. Resume: park finished runs, prepare code for active ones.
        let loaded: Vec<(RunId, bool)> = inner
            .core
            .loaded_runs()
            .map(|(r, l)| (r, l.is_done()))
            .collect();
        for (run, done) in loaded {
            if done {
                inner.park_done(run)?;
            } else {
                let mut journal = inner.journal_of(run)?;
                inner.prepare(run, &mut journal);
                inner.core.reoffer(run);
            }
        }

        // Downtime can take the shard past retention with no more traffic.
        // Record those expirations before serving a query or rescheduling a checkpoint.
        inner.expire_retained(now)?;
        inner.metrics.open_us = inner.env.monotonic_us().saturating_sub(opened_us);

        let inner = Rc::new(RefCell::new(inner));
        let host: Rc<dyn Host> = inner.clone();
        let rt = Self {
            inner,
            host,
            futures: RefCell::new(Vec::new()),
            update_futs: RefCell::new(FxHashMap::default()),
            handlers: RefCell::new(FxHashMap::default()),
            spare_handlers: RefCell::new(Vec::new()),
            code: RefCell::new(CodeCache::default()),
            deferred: RefCell::new(Vec::new()),
            outside: Rc::new(Cell::new(0)),
            spare: RefCell::new(Vec::new()),
            last_drives: Cell::new(0),
            local: RefCell::new(Vec::new()),
        };
        rt.instantiate_pending();
        Ok(rt)
    }

    pub fn with<R>(&self, f: impl FnOnce(&mut Inner<E>) -> R) -> R {
        f(&mut self.inner.borrow_mut())
    }

    pub fn host(&self) -> Rc<dyn Host> {
        self.host.clone()
    }

    pub fn metrics(&self) -> Metrics {
        self.inner.borrow().metrics()
    }

    /// Some native step function is pending: it waits on something other
    /// than the runtime, such as I/O or a timer of the thread's reactor, so
    /// a busy host should visit that reactor after every tick.
    pub fn waits_outside(&self) -> bool {
        self.outside.get() > 0
    }

    fn instantiate_pending(&self) -> bool {
        if self.inner.borrow().instantiate.is_empty() {
            return false;
        }
        let pending = std::mem::take(&mut self.inner.borrow_mut().instantiate);
        if pending.is_empty() {
            return false;
        }
        for run in pending {
            let (info, foreign, stash) = {
                let mut i = self.inner.borrow_mut();
                let Some(slot) = i.slots.get(&run) else {
                    continue;
                };
                let info = slot.machine.info().clone();
                let foreign = slot.foreign;
                let stash = i.stash.remove(&run).unwrap_or_default();
                (info, foreign, stash)
            };
            let info = {
                let i = self.inner.borrow();
                let mut info = info;
                match i.open(info.input.clone()) {
                    Ok(p) => info.input = p,
                    Err(e) => {
                        drop(i);
                        self.inner
                            .borrow_mut()
                            .make_stuck(run, format!("the workflow input cannot be read: {e}"));
                        continue;
                    }
                }
                info
            };
            if foreign {
                let mut i = self.inner.borrow_mut();
                i.foreign.push_back(ForeignEvent::Start { run, info });
                for s in stash {
                    match s {
                        Stashed::Complete(seq, c) => i.complete(run, seq, c),
                        Stashed::Wake => i.wake(run),
                    }
                }
                continue;
            }
            let known = {
                let i = self.inner.borrow();
                self.code
                    .borrow_mut()
                    .get(&i.registry, &info.wtype, info.version)
                    .is_some()
            };
            if !known {
                let mut i = self.inner.borrow_mut();
                i.awaiting
                    .entry(info.wtype.to_string())
                    .or_default()
                    .push(run);
                i.stash.insert(run, stash);
                continue;
            }
            let handlers = self
                .spare_handlers
                .borrow_mut()
                .pop()
                .unwrap_or_else(|| Rc::new(Handlers::default()));
            self.handlers.borrow_mut().insert(run, handlers.clone());
            // Called through the shard's copy (found just above): no shared
            // count moves. The run's information moves into its context
            // rather than a copy of it: each copy moved the counts of its
            // names and input.
            let fut = {
                let code = self.code.borrow();
                let Some(f) = code.cached(&info.wtype, info.version) else {
                    continue;
                };
                let ctx = Ctx::new(self.host.clone(), run, info, handlers, self.outside.clone());
                f(ctx)
            };
            let mut i = self.inner.borrow_mut();
            if let Some(idx) = i.slots.idx(run)
                && let Some(slot) = i.slots.entries[idx as usize].as_deref()
            {
                let mut futures = self.futures.borrow_mut();
                let idx = idx as usize;
                if futures.len() <= idx {
                    futures.resize_with(idx + 1, || None);
                }
                futures[idx] = Some(Code::new(fut, &slot.waker));
            }
            for s in stash {
                match s {
                    Stashed::Complete(seq, c) => i.complete(run, seq, c),
                    Stashed::Wake => {}
                }
            }
            i.wake(run);
        }
        true
    }

    /// New code was registered: resume runs that were waiting for it.
    pub fn registered(&self, wtype: &str) {
        let runs = self
            .inner
            .borrow_mut()
            .awaiting
            .remove(wtype)
            .unwrap_or_default();
        if runs.is_empty() {
            return;
        }
        self.inner.borrow_mut().instantiate.extend(runs);
        self.instantiate_pending();
    }

    /// Polls one run's code until it is quiescent, delivering results one at
    /// a time in between.
    fn drive(&self, run: RunId) {
        {
            let mut i = self.inner.borrow_mut();
            i.count_drive();
            i.mark_active(run);
        }
        for _ in 0..10_000 {
            // The run's code is held here for the whole iteration and goes
            // back to its place where the loop continues or the run stays:
            // its waker was made once, when the code started, so a drive
            // moves no reference count (a `Waker` made per drive took two
            // increments and two decrements a step).
            let (idx, mut code) = {
                let i = self.inner.borrow();
                let Some(idx) = i.slots.idx(run) else {
                    return;
                };
                let idx = idx as usize;
                let Some(s) = i.slots.entries[idx].as_deref() else {
                    return;
                };
                let taken = self
                    .futures
                    .borrow_mut()
                    .get_mut(idx)
                    .and_then(Option::take);
                let Some(mut code) = taken else {
                    // No code to poll yet (its type is not registered): let
                    // the next wake queue the run again.
                    s.waker.queued.store(false, Ordering::Release);
                    return;
                };
                if !code.polled_with(&s.waker) {
                    code = Code::new(code.fut, &s.waker);
                }
                (idx, code)
            };
            code.waker().queued.store(false, Ordering::Release);
            code.waker().woken.store(false, Ordering::Release);
            let Code { fut, handle } = &mut code;
            let mut cx = Context::from_waker(handle);
            self.inner.borrow_mut().inline_deliveries = INLINE_DELIVERIES;
            let polled = fut.as_mut().poll(&mut cx);
            self.inner.borrow_mut().inline_deliveries = 0;
            match polled {
                Poll::Ready(outcome) => {
                    drop(code);
                    self.inner.borrow_mut().finish(run, outcome);
                    return;
                }
                Poll::Pending => {
                    if self.inner.borrow().slots.idx(run) != Some(idx as u32) {
                        // Ended from outside during the poll.
                        return;
                    }
                }
            }
            if !self.poll_updates(run, &mut cx) {
                return;
            }
            if run_waker(handle).woken.load(Ordering::Acquire) {
                self.put_code(idx, code);
                continue;
            }
            let next = {
                let mut i = self.inner.borrow_mut();
                match i.deliver(run) {
                    Ok(d) => d,
                    Err(e) => {
                        i.fail(e.to_string());
                        None
                    }
                }
            };
            let delivered = match next {
                Some(Delivery::Update {
                    ordinal,
                    name,
                    id,
                    payload,
                    token,
                }) => {
                    self.start_update(run, ordinal, &name, &id, payload, token);
                    true
                }
                Some(d) => {
                    // The loop polls again at once: the wake of the future
                    // this resolves need not queue the run.
                    run_waker(handle).queued.store(true, Ordering::Release);
                    self.inner.borrow_mut().store_delivery(run, d);
                    true
                }
                None => false,
            };
            // A run that ended meanwhile drops its code here.
            if self.inner.borrow().slots.contains_key(&run) {
                self.put_code(idx, code);
            }
            if !delivered {
                return;
            }
        }
        // Defensive: give other runs a turn.
        if let Some(slot) = self.inner.borrow().slots.get(&run) {
            slot.waker.queued.store(false, Ordering::Release);
            slot.waker.wake_by_ref();
        }
    }

    /// Puts a run's code back in its place after a drive took it.
    fn put_code(&self, idx: usize, code: Code) {
        if let Some(place) = self.futures.borrow_mut().get_mut(idx) {
            *place = Some(code);
        }
    }

    /// Polls the run's update handlers in ordinal order. Returns false if
    /// the run ended meanwhile.
    fn poll_updates(&self, run: RunId, cx: &mut Context<'_>) -> bool {
        let Some(mut list) = ({
            let mut u = self.update_futs.borrow_mut();
            if u.is_empty() { None } else { u.remove(&run) }
        }) else {
            return true;
        };
        let mut k = 0;
        while k < list.len() {
            match list[k].1.as_mut().poll(cx) {
                Poll::Ready(outcome) => {
                    let (ordinal, f) = list.remove(k);
                    drop(f);
                    let _ = self.host.update_done(run, ordinal, outcome);
                }
                Poll::Pending => k += 1,
            }
            if !self.inner.borrow().slots.contains_key(&run) {
                return false;
            }
        }
        if !list.is_empty() {
            self.update_futs.borrow_mut().insert(run, list);
        }
        true
    }

    /// An update reached its turn in a native run: validate a new request,
    /// then start its handler.
    fn start_update(
        &self,
        run: RunId,
        ordinal: u64,
        name: &str,
        id: &str,
        payload: Payload,
        token: Option<u64>,
    ) {
        let entry = self
            .handlers
            .borrow()
            .get(&run)
            .and_then(|h| h.update(name));
        let ordinal = match (token, entry.as_ref()) {
            (Some(t), None) => {
                let r = self
                    .inner
                    .borrow_mut()
                    .decide_update(t, Err(format!("no handler for update '{name}'")));
                if let Err(e) = r {
                    self.inner.borrow_mut().fail(e.to_string());
                }
                return;
            }
            (Some(t), Some((validate, _))) => {
                let decision = validate(&payload);
                let accepted = decision.is_ok();
                let decided = self.inner.borrow_mut().decide_update(t, decision);
                match decided {
                    Ok(Some(o)) if accepted => o,
                    Ok(_) => return,
                    Err(e) => {
                        self.inner.borrow_mut().fail(e.to_string());
                        return;
                    }
                }
            }
            (None, None) => {
                self.inner.borrow_mut().make_stuck(
                    run,
                    format!(
                        "determinism error: the journal delivers update '{name}' (id '{id}') but \
                         the code registered no handler for it"
                    ),
                );
                return;
            }
            (None, Some(_)) => ordinal,
        };
        if let Some((_, handle)) = entry {
            let fut = handle(payload);
            self.update_futs
                .borrow_mut()
                .entry(run)
                .or_default()
                .push((ordinal, fut));
        }
    }

    fn drive_ready(&self) -> bool {
        // The queue and the list taken from it trade places, so neither
        // allocates once both have grown to the usual number of wakes.
        let mut runs = std::mem::take(&mut *self.spare.borrow_mut());
        {
            let i = self.inner.borrow();
            // Runs woken outside the tick were queued before the tick's
            // own wakes, so they go first.
            if i.ready.queued.swap(false, Ordering::Acquire)
                && let Ok(mut q) = i.ready.queue.lock()
            {
                std::mem::swap(&mut *q, &mut runs);
            }
            take_local(&i.ready, Some(&mut runs));
        }
        // A round with nothing to drive is not behind either.
        self.last_drives.set(runs.len());
        if runs.is_empty() {
            *self.spare.borrow_mut() = runs;
            return false;
        }
        {
            // Flushes that take no time (memory mode) may come from gates
            // freely, a few dozen a round. A real write from a gate starts
            // sooner than the round's would, which loses other shards'
            // records to share it with when several shards write: only a
            // shard working nearly alone does it, once a round.
            let mut i = self.inner.borrow_mut();
            i.gate_flushes = if i.env.last_flush_us() == 0 {
                64
            } else if crate::spin::few_busy() {
                1
            } else {
                0
            };
        }
        let n = runs.len();
        let every = drive_flush_every();
        for (k, &run) in runs.iter().enumerate() {
            let foreign = {
                let mut i = self.inner.borrow_mut();
                i.drive_backlog = n - k - 1;
                match i.slots.get(&run) {
                    Some(s) => s.foreign,
                    None => continue,
                }
            };
            if foreign {
                let mut i = self.inner.borrow_mut();
                if let Some(s) = i.slots.get(&run) {
                    s.waker.queued.store(false, Ordering::Release);
                }
                i.mark_active(run);
                // A waker fired for foreign code (a limit permit, a timer):
                // tell the host to look at the run.
                if let Some(slot) = i.slots.get_mut(&run)
                    && !slot.wake_queued
                {
                    slot.wake_queued = true;
                    i.foreign.push_back(ForeignEvent::Wake { run });
                }
                continue;
            }
            self.drive(run);
            // Opt-in (`FUSEE_DRIVE_FLUSH_EVERY`): every so many runs of a
            // long round, hand the log what they appended and take the
            // writes that completed, instead of only between rounds. A
            // record then waits for part of the round, not all of it; the
            // runs it releases run in the next round.
            if every > 0 && (k + 1) % every == 0 && k + 1 < n {
                let mut i = self.inner.borrow_mut();
                i.poll_io();
                i.maybe_flush(false);
            }
        }
        {
            let mut i = self.inner.borrow_mut();
            i.gate_flushes = 0;
            i.drive_backlog = 0;
        }
        runs.clear();
        *self.spare.borrow_mut() = runs;
        true
    }

    /// The shard is behind: its last round drove more runs than
    /// `FUSEE_ADMIT_ROUND` (2,048 by default; 0 turns this off). Its host
    /// then leaves callers' commands queued until it catches up: new runs
    /// admitted into a backlog make every round longer and colder (a
    /// round of thousands of runs touches more state than the caches
    /// hold, and a step cost twice as much), so an overload made the
    /// shard slower than the load. Runs already started keep going, and
    /// what they wait for never comes from a caller's queue alone.
    pub fn behind(&self) -> bool {
        let limit = admit_round();
        limit != 0 && self.last_drives.get() > limit
    }

    fn has_ready(&self) -> bool {
        let i = self.inner.borrow();
        i.ready.queued.load(Ordering::Acquire)
            || take_local(&i.ready, None)
            || i.core.has_events()
            || !i.instantiate.is_empty()
    }

    /// A host handle can be used between ticks without going through
    /// ForeignEngine. The runtime and its erased host own the two base Rcs.
    pub(crate) fn needs_foreign_poll(&self, deadline: Option<u64>) -> bool {
        let i = self.inner.borrow();
        Rc::strong_count(&self.host) > 2
            || i.in_flight.is_some()
            || i.ckpt_running
            || !i.outbox.is_empty()
            || deadline.is_some_and(|d| d <= i.now())
    }

    /// The count of commands the host has queued for this shard. With it, a
    /// run that is the shard's only work flushes from its durability gate
    /// while the count is zero ([`Inner::flush_at_gate`]).
    pub fn watch_host_queue(&self, queued: Arc<std::sync::atomic::AtomicUsize>) {
        self.inner.borrow_mut().host_queue = Some(queued);
    }

    /// Does all the work that is possible now.
    pub fn tick(&self) -> Tick {
        self.inner.borrow_mut().host_never = true;
        let tick = self.tick_with(&|| false);
        self.inner.borrow_mut().host_never = false;
        tick
    }

    /// Like [`tick`](Self::tick); `external()` tells whether the host has
    /// input queued for this shard. The runtime then never blocks on an
    /// inline flush and returns after starting a flush so the host can feed
    /// it, letting new work join the next batch.
    pub fn tick_with(&self, external: &dyn Fn() -> bool) -> Tick {
        let traced = {
            let mut i = self.inner.borrow_mut();
            i.tick_parked = 0;
            i.tick_maintenance_us = 0;
            i.set_ticking(true);
            (i.stall_us != 0).then(|| i.env.monotonic_us())
        };
        let local = LocalTick::enter(self.inner.borrow().ready.clone(), &self.local);
        let tick = self.tick_rounds(external);
        drop(local);
        self.inner.borrow_mut().set_ticking(false);
        if let Some(t) = traced {
            let mut i = self.inner.borrow_mut();
            let us = i.env.monotonic_us().saturating_sub(t);
            i.trace_stall(StallKind::Tick, us);
        }
        tick
    }

    /// Stalls recorded since the last call (see [`Stall`]).
    pub fn take_stalls(&self) -> Vec<Stall> {
        self.inner.borrow_mut().stalls.drain(..).collect()
    }

    fn tick_rounds(&self, external: &dyn Fn() -> bool) -> Tick {
        let mut busy = false;
        for round in 0.. {
            if round >= 256 {
                busy = true;
                break;
            }
            let mut progressed = false;
            {
                let mut i = self.inner.borrow_mut();
                i.round_us = i.metric_us();
            }
            progressed |= self.inner.borrow_mut().poll_io();
            // A failed disk has uncertain state. No workflow, checkpoint or
            // cross-shard acknowledgment may advance until the shard reopens.
            if self.inner.borrow().failed.is_some() {
                return Tick {
                    next_wake_ms: None,
                    busy: false,
                };
            }
            // A foreign host whose queue is full takes no more loads this
            // tick, so firing more parked runs' timers would only queue
            // their loads: in a mass wake the first tick fired every due
            // timer (a million on one Node thread: 266 ms) before the host
            // ran any code. They stay due for the next tick.
            let host_full = {
                let i = self.inner.borrow();
                i.load_room != usize::MAX && i.foreign.len() >= i.load_room
            };
            if !host_full {
                progressed |= self.inner.borrow_mut().poll_timers();
            }
            progressed |= self.inner.borrow_mut().process_events();
            progressed |= self.instantiate_pending();
            progressed |= self.drive_ready();
            progressed |= self.compact_objects();
            progressed |= self.retry_deferred() && self.has_ready();
            progressed |= self.inner.borrow_mut().process_events();
            let waiting = external();
            let idle = !waiting && !self.has_ready() && {
                // The host's code for a lone start cannot go past its first
                // step before the start record is durable anyway.
                let i = self.inner.borrow();
                i.foreign.is_empty()
                    || (i.foreign.len() == 1
                        && matches!(i.foreign.front(), Some(ForeignEvent::Start { .. })))
            };
            let flushed = self.inner.borrow_mut().maybe_flush(idle);
            progressed |= flushed;
            // A mass wake ends runs round after round: their futures and
            // handler sets go back to the pools for the next rounds' loads
            // instead of waiting for the tick's end.
            if self.inner.borrow().removed.len() >= REAP_EARLY {
                self.reap();
            }
            if !progressed {
                break;
            }
            if waiting {
                busy = true;
                break;
            }
        }
        // Loads held back (past a round's share, or the host's room) need
        // another tick even when this one could make no progress.
        if !self.inner.borrow().held_loads.is_empty() {
            busy = true;
        }
        self.reap();
        let mut i = self.inner.borrow_mut();
        i.maintenance();
        if std::mem::take(&mut i.pools_shrunk) {
            let mut spare = self.spare_handlers.borrow_mut();
            spare.truncate(SLOT_SPARE_IDLE);
            spare.shrink_to(SLOT_SPARE_IDLE);
        }
        let mut next = i.core.next_timer();
        if let Some(Reverse((at, _))) = i.timers.peek() {
            next = Some(next.map_or(*at, |n| n.min(*at)));
        }
        let sweep = i.last_sweep_ms + 100;
        if i.stream_wait_until != u64::MAX {
            let at = i.stream_wait_until;
            next = Some(next.map_or(at, |n| n.min(at)));
        }
        // An index base being written needs maintenance passes too.
        let building = i.index_build.as_ref().is_some_and(|b| b.cursor.is_some());
        if !i.slots.is_empty() || i.core.loaded_len() > 0 || !i.streams.is_empty() || building {
            next = Some(next.map_or(sweep, |n| n.min(sweep)));
        }
        if let Some(expiry) = i.core.next_expiry() {
            let at = expiry.max(sweep);
            next = Some(next.map_or(at, |n| n.min(at)));
        }
        if !i.ckpt_running && i.ckpt_pending.is_none() && i.checkpoint_needed() {
            let c = i.last_ckpt_ms + i.cfg.checkpoint_interval_ms;
            next = Some(next.map_or(c, |n| n.min(c)));
        }
        if i.env.due_wanted() {
            let mut due = i.due();
            due.busy |= busy;
            i.env.report_due(due, false);
        }
        Tick {
            next_wake_ms: next,
            busy,
        }
    }

    /// Drops code of runs whose slot was removed (parked, ended, stuck) since
    /// the last call. Only those runs are looked at, not every loaded one.
    fn reap(&self) {
        if self.inner.borrow().removed.is_empty() {
            return;
        }
        let removed = std::mem::take(&mut self.inner.borrow_mut().removed);
        let mut dropped: Vec<Code> = Vec::new();
        let mut updates = Vec::new();
        let mut handlers = Vec::new();
        let mut reloaded = Vec::new();
        {
            let i = self.inner.borrow();
            let mut f = self.futures.borrow_mut();
            let mut u = self.update_futs.borrow_mut();
            let mut h = self.handlers.borrow_mut();
            for &(run, idx) in &removed {
                dropped.extend(f.get_mut(idx as usize).and_then(Option::take));
                // A run parked and loaded again in the same tick has a new
                // slot, with new handlers: keep them.
                if i.slots.contains_key(&run) {
                    reloaded.push(run);
                    continue;
                }
                updates.extend(u.remove(&run));
                handlers.extend(h.remove(&run));
            }
        }
        // Futures may call back into the host while they drop. The old code
        // of a run loaded again must not forget its new code's operations
        // (their results would be taken for delivered and dropped).
        self.inner.borrow_mut().reaping = reloaded;
        drop(dropped);
        drop(updates);
        // Handlers hold the run's `Ctx`, which holds them: break the cycle.
        for h in &handlers {
            h.clear();
        }
        // Sets nothing else holds any more serve the next runs: as many as
        // slots (a round loads up to two chunks of runs), down to a few
        // once the shard is idle.
        {
            let mut spare = self.spare_handlers.borrow_mut();
            for h in handlers {
                if spare.len() < SLOT_SPARE && Rc::strong_count(&h) == 1 && Rc::weak_count(&h) == 0
                {
                    spare.push(h);
                }
            }
        }
        self.inner.borrow_mut().reaping.clear();
        // Their indices may now go to other runs.
        let mut i = self.inner.borrow_mut();
        for (_, idx) in removed {
            i.slots.release(idx);
        }
        let mut f = self.futures.borrow_mut();
        f.truncate(i.slots.entries.len());
        let n = f.len();
        if f.capacity() > 1024 && f.capacity() > 4 * n {
            f.shrink_to(n * 2);
        }
    }

    /// Released outputs for the host to route.
    pub fn take_released(&self) -> Vec<Output> {
        std::mem::take(&mut self.inner.borrow_mut().released)
    }

    pub fn take_foreign(&self) -> Vec<ForeignEvent> {
        let mut i = self.inner.borrow_mut();
        let events: Vec<ForeignEvent> = i.foreign.drain(..).collect();
        for e in &events {
            if let ForeignEvent::Wake { run } = e
                && let Some(slot) = i.slots.get_mut(run)
            {
                slot.wake_queued = false;
            }
        }
        events
    }

    pub fn start(&self, req: StartRequest) -> Result<StartOutcome, Error> {
        let outcome = self.start_locked(req)?;
        self.instantiate_pending();
        Ok(outcome)
    }

    fn start_locked(&self, mut req: StartRequest) -> Result<StartOutcome, Error> {
        let mut i = self.inner.borrow_mut();
        i.check_alive()?;
        let object = i.object_start(&mut req)?;
        let version = match req.version {
            Some(v) => v,
            None => {
                let registry = i
                    .registry
                    .read()
                    .map_err(|_| Error::Io("registry poisoned".into()))?;
                match registry.latest(&req.wtype) {
                    Some(v) => v,
                    None if i.foreign_host => 1,
                    None => {
                        return Err(Error::UnknownWorkflow(crate::error::unknown_workflow(
                            &req.wtype,
                            &registry.names(),
                        )));
                    }
                }
            }
        };
        let ids = IdPolicy::from_bits(req.policy);
        if let Some(found) = i.core.find(&req.id) {
            let run = match found {
                Found::Loaded(r) | Found::Parked(r) => r,
            };
            let done = match found {
                Found::Loaded(r) => i.core.get(r).and_then(|l| l.done.map(|d| d.0)),
                Found::Parked(r) => i.core.parked(r).and_then(|p| p.done()),
            };
            if object && done.is_none() {
                i.object_check_held(run, &req)?;
            }
            #[derive(PartialEq)]
            enum Act {
                Use,
                New,
                Refuse,
                Replace,
            }
            let act = match done {
                None => match ids.running {
                    OnRunning::Fail => Act::Refuse,
                    OnRunning::UseExisting => Act::Use,
                    OnRunning::TerminateExisting => Act::Replace,
                },
                Some(status) => match ids.finished {
                    OnFinished::Allow => Act::New,
                    OnFinished::AllowIfFailed if status != Status::Ok => Act::New,
                    OnFinished::AllowIfFailed | OnFinished::Refuse => Act::Refuse,
                    OnFinished::UseExisting => Act::Use,
                },
            };
            let done = done.is_some();
            match act {
                Act::Refuse => {
                    return match req.token {
                        Some(t) => {
                            // Refuse once the state the refusal saw is durable.
                            let tail = i.writer.last_lsn();
                            i.core.add_output(tail, Output::Reject(t));
                            i.on_commit();
                            Ok(StartOutcome::Existing { run, done })
                        }
                        None => Err(Error::AlreadyExists(req.id)),
                    };
                }
                Act::Use => {
                    let own_parent = i.core.get(run).and_then(|l| l.parent);
                    let detached = req.policy & policy::DETACHED != 0;
                    let mut ack = true;
                    if let (Some(parent), false) = (req.parent, detached) {
                        if done {
                            // Tell the parent at once.
                            let (status, payload) = match self.result_sealed(&mut i, run)? {
                                Some(r) => r,
                                None => (Status::Failed, empty()),
                            };
                            i.send_out(
                                parent.shard,
                                Message::ChildResult {
                                    parent,
                                    status,
                                    payload,
                                },
                            );
                        } else if own_parent != Some(parent) {
                            // Another parent attaches to a running child. The
                            // subscription lives in memory, so the parent's
                            // start stays unacknowledged (and is resent) until
                            // the result arrives.
                            let subs = i.subscribers.entry(run).or_default();
                            if !subs.contains(&parent) {
                                subs.push(parent);
                            }
                            ack = false;
                        }
                    }
                    if let Some(u) = req.update.take() {
                        i.ensure_loaded(run)?;
                        let uid = if u.id.is_empty() {
                            i.fresh_update_id()
                        } else {
                            u.id
                        };
                        i.offer_update(run, &u.name, &uid, u.payload, u.token)?;
                    }
                    if let (Some(t), true) = (req.token, ack) {
                        let tail = i.writer.last_lsn();
                        i.core.add_output(tail, Output::Ack(t));
                    }
                    i.on_commit();
                    return Ok(StartOutcome::Existing { run, done });
                }
                Act::Replace => {
                    i.end_run(
                        run,
                        Status::Terminated,
                        "terminated by a new start with the same workflow ID",
                    )?;
                }
                Act::New => {}
            }
        }
        if i.core.loaded_len() >= i.cfg.max_loaded {
            i.metrics.busy += 1;
            return Err(Error::Busy);
        }
        let run = i.core.allocate_run();
        let now = i.now();
        let input = if req.sealed {
            req.input
        } else {
            i.seal(req.input)?
        };
        let mut accepted = None;
        let update = match req.update.take() {
            Some(u) => {
                let uid = if u.id.is_empty() {
                    i.fresh_update_id()
                } else {
                    u.id
                };
                let payload = i.seal(u.payload)?;
                match u.token {
                    // A one-way call is answered once the start that
                    // carries it is durable.
                    Some(t) if object && crate::object::is_send(&uid) => accepted = Some(t),
                    Some(t) => i
                        .update_waiters
                        .entry((run, Name::from(uid.as_str())))
                        .or_default()
                        .push(t),
                    None => {}
                }
                Some(Box::new(StartUpdate {
                    name: Name::from(u.name.as_str()),
                    id: Name::from(uid.as_str()),
                    payload,
                }))
            }
            None => None,
        };
        let start: RecordBuf = Record::Start {
            run,
            id: Arc::from(req.id.as_str()),
            wtype: Arc::from(req.wtype.as_str()),
            version,
            codec: req.codec,
            started_ms: now,
            deadline_ms: req.deadline_ms,
            parent: req.parent,
            policy: req.policy,
            from: None,
            copies: 0,
            update,
            input,
            meta: StartMeta::of(None, req.trace),
        };
        let lsn = i.append(&start)?;
        if let Some(t) = req.token {
            i.core.add_output(lsn, Output::Ack(t));
        }
        if let Some(t) = accepted {
            i.core.add_output(
                lsn,
                Output::Reply {
                    token: t,
                    status: Status::Ok,
                    payload: Arc::from(&b"null"[..]),
                },
            );
        }
        i.metrics.runs_started += 1;
        let foreign = i.runs_foreign(&req.wtype, version);
        i.new_slot(
            run,
            JournalIn::Borrowed(std::slice::from_ref(&start)),
            lsn,
            foreign,
        )?;
        i.instantiate.push(run);
        Ok(StartOutcome::Created { run })
    }

    /// A finished run's status and its payload as recorded (still sealed).
    fn result_sealed(
        &self,
        i: &mut Inner<E>,
        run: RunId,
    ) -> Result<Option<(Status, Payload)>, Error> {
        if let Some(l) = i.core.get(run) {
            return Ok(l
                .done
                .map(|(s, _)| (s, l.result.clone().unwrap_or_else(empty))));
        }
        let Some(p) = i.core.parked(run) else {
            return Ok(None);
        };
        if p.done().is_none() {
            return Ok(None);
        }
        for rec in i.blob_records(p.blob())? {
            if let Record::Done {
                status, payload, ..
            } = rec
            {
                return Ok(Some((status, payload)));
            }
        }
        Ok(None)
    }

    fn result_locked(&self, i: &mut Inner<E>, run: RunId) -> Result<ResultState, Error> {
        let (status, payload, id) = if let Some(l) = i.core.get(run) {
            match l.done {
                Some(_) if l.last_lsn > i.commit => return Ok(ResultState::Running(run)),
                Some((status, _)) => (status, l.result.clone().unwrap_or_else(empty), l.id.clone()),
                None => return Ok(ResultState::Running(run)),
            }
        } else {
            let Some(p) = i.core.parked(run) else {
                return Ok(ResultState::NotFound);
            };
            if p.done().is_none() {
                return Ok(ResultState::Running(run));
            }
            // The frame has the run's last LSN (the table does not).
            let b = p.blob();
            let frame = read_store(&mut i.env, &i.tail, b.offset, b.len)?;
            // A run that finished after a load: its newest frame, a
            // `Delta`, holds its `Done`; the frames before it are needed
            // only for the ID a hand-over's result follows.
            if let Ok(StoreEntry::Delta {
                last_lsn, journal, ..
            }) = read_entry(&frame)
                && let Some(Record::Done {
                    status, payload, ..
                }) = decode_canonical_all(&journal)
                    .ok()
                    .and_then(|mut v| v.pop())
                && !status.is_handover()
            {
                if last_lsn > i.commit {
                    return Ok(ResultState::Running(run));
                }
                return self.finished_result(i, run, status, payload);
            }
            let stored = i.stored_from(b, Some(&frame))?;
            if stored.last_lsn > i.commit {
                return Ok(ResultState::Running(run));
            }
            let recs = stored.records;
            let id = match recs.first() {
                Some(Record::Start { id, .. }) => id.clone(),
                _ => return Ok(ResultState::NotFound),
            };
            match recs.into_iter().find_map(|r| match r {
                Record::Done {
                    status, payload, ..
                } => Some((status, payload)),
                _ => None,
            }) {
                Some((status, payload)) => (status, payload, id),
                None => return Ok(ResultState::NotFound),
            }
        };
        if status.is_handover() {
            // Follow the workflow ID to the run that took over.
            return match i.core.find(&id) {
                Some(Found::Loaded(r) | Found::Parked(r)) if r > run => self.result_locked(i, r),
                _ => Ok(ResultState::Running(run)),
            };
        }
        self.finished_result(i, run, status, payload)
    }

    /// A finished run's result, its payload opened.
    fn finished_result(
        &self,
        i: &mut Inner<E>,
        run: RunId,
        status: Status,
        payload: Payload,
    ) -> Result<ResultState, Error> {
        let payload = if status == Status::Ok {
            match i.open(payload) {
                Ok(p) => p,
                Err(e) => {
                    return Ok(ResultState::Done {
                        run,
                        status: Status::Failed,
                        payload: Arc::from(e.to_string().as_bytes()),
                    });
                }
            }
        } else {
            payload
        };
        Ok(ResultState::Done {
            run,
            status,
            payload,
        })
    }

    /// The run's result, if it finished.
    pub fn result(&self, id: &str) -> Result<ResultState, Error> {
        let mut i = self.inner.borrow_mut();
        let run = match i.core.find(id) {
            Some(Found::Loaded(r) | Found::Parked(r)) => r,
            None => return Ok(ResultState::NotFound),
        };
        self.result_locked(&mut i, run)
    }

    pub fn find(&self, id: &str) -> Option<RunId> {
        match self.inner.borrow().core.find(id)? {
            Found::Loaded(r) | Found::Parked(r) => Some(r),
        }
    }

    /// Delivers a signal. `dedup` (0 = none) drops repeats of one message.
    /// Runs one request (a signal, cancel, terminate or reset) recording
    /// `by`, who asked, on the record it writes (audit).
    pub fn with_by<T>(&self, by: Option<Payload>, f: impl FnOnce() -> T) -> T {
        if by.is_none() {
            return f();
        }
        self.inner.borrow_mut().by = by;
        let out = f();
        self.inner.borrow_mut().by = None;
        out
    }

    pub fn signal(
        &self,
        id: &str,
        name: &str,
        payload: Payload,
        dedup: u64,
        token: Option<u64>,
    ) -> Result<(), Error> {
        let payload = self.inner.borrow().seal(payload)?;
        self.signal_sealed(id, name, payload, dedup, token)
    }

    fn signal_sealed(
        &self,
        id: &str,
        name: &str,
        payload: Payload,
        dedup: u64,
        token: Option<u64>,
    ) -> Result<(), Error> {
        {
            let mut i = self.inner.borrow_mut();
            let by = i.by.take();
            i.check_alive()?;
            let run = match i.core.find(id) {
                Some(Found::Loaded(r) | Found::Parked(r)) => r,
                None => return Err(Error::NotFound(id.to_owned())),
            };
            i.ensure_loaded(run)?;
            if let Some(l) = i.core.get(run).filter(|l| l.is_done()) {
                let at = l.last_lsn.max(i.writer.last_lsn());
                return match token {
                    // Refuse only once the run's end is durable.
                    Some(t) => {
                        i.core.add_output(at, Output::Reject(t));
                        i.on_commit();
                        Ok(())
                    }
                    None => Err(Error::Rejected(format!(
                        "workflow '{id}' has already finished"
                    ))),
                };
            }
            let lsn = if i.core.seen_signal(run, dedup) {
                i.writer.last_lsn()
            } else {
                let rec: RecordBuf = Record::Signal {
                    run,
                    name: Arc::from(name),
                    dedup,
                    payload,
                    by,
                };
                i.append(&rec)?
            };
            if let Some(t) = token {
                i.core.add_output(lsn, Output::Ack(t));
            }
            i.on_commit();
        }
        self.instantiate_pending();
        Ok(())
    }

    /// Requests cancellation; acknowledged once it is recorded.
    pub fn cancel(&self, id: &str, reason: &str, token: Option<u64>) -> Result<(), Error> {
        {
            let mut i = self.inner.borrow_mut();
            let by = i.by.take();
            i.check_alive()?;
            let run = match i.core.find(id) {
                Some(Found::Loaded(r) | Found::Parked(r)) => r,
                None => return Err(Error::NotFound(id.to_owned())),
            };
            i.ensure_loaded(run)?;
            if i.core.get(run).is_some_and(|l| l.is_done()) {
                if let Some(t) = token {
                    let tail = i.writer.last_lsn();
                    i.core.add_output(tail, Output::Ack(t));
                    i.on_commit();
                }
                return Ok(());
            }
            if i.slots
                .get(&run)
                .is_some_and(|slot| slot.machine.cancellation_covers_current_operations())
            {
                if let Some(t) = token {
                    let tail = i.writer.last_lsn();
                    i.core.add_output(tail, Output::Ack(t));
                    i.on_commit();
                }
                return Ok(());
            }
            match i.slots.get_mut(&run) {
                Some(slot) => {
                    slot.machine
                        .request_cancel_by(Arc::from(reason.as_bytes()), by);
                    if let Some(t) = token {
                        i.cancel_tokens.entry(run).or_default().push(t);
                    }
                    i.wake(run);
                }
                None => {
                    // No code yet: end it.
                    i.by = by;
                    let lsn = i.end_run(run, Status::Cancelled, reason);
                    i.by = None;
                    let lsn = lsn?;
                    if let (Some(t), Some(lsn)) = (token, lsn) {
                        i.core.add_output(lsn, Output::Ack(t));
                    }
                }
            }
        }
        self.instantiate_pending();
        Ok(())
    }

    /// Resumes the run's paused steps: each runs its next attempt at once
    /// with a fresh retry budget. Acknowledged once the `Resume` records
    /// are durable; refused if nothing is paused.
    pub fn resume(&self, id: &str, token: Option<u64>) -> Result<(), Error> {
        {
            let mut i = self.inner.borrow_mut();
            let by = i.by.take();
            i.check_alive()?;
            let run = match i.core.find(id) {
                Some(Found::Loaded(r) | Found::Parked(r)) => r,
                None => return Err(Error::NotFound(id.to_owned())),
            };
            i.ensure_loaded(run)?;
            if i.core.get(run).is_some_and(|l| l.is_done()) {
                return Err(Error::Rejected(format!(
                    "workflow '{id}' has finished: nothing to resume"
                )));
            }
            let Some(slot) = i.slots.get_mut(&run) else {
                return Err(Error::Rejected(format!(
                    "workflow '{id}' is not paused (no code for it runs here)"
                )));
            };
            let records = slot.machine.resume(by);
            if records.is_empty() {
                return Err(Error::Rejected(format!(
                    "workflow '{id}' is not paused: no step of it ran out of retries"
                )));
            }
            let mut lsn = i.commit;
            for rec in &records {
                lsn = i.append(rec)?;
            }
            if let Some(t) = token {
                i.core.add_output(lsn, Output::Ack(t));
            }
            i.wake(run);
        }
        self.instantiate_pending();
        Ok(())
    }

    /// Ends a run at once with `Status::Terminated`; its code is dropped.
    pub fn terminate(&self, id: &str, reason: &str, token: Option<u64>) -> Result<(), Error> {
        let mut i = self.inner.borrow_mut();
        let by = i.by.take();
        i.check_alive()?;
        let run = match i.core.find(id) {
            Some(Found::Loaded(r) | Found::Parked(r)) => r,
            None => return Err(Error::NotFound(id.to_owned())),
        };
        let idx = i.slots.idx(run);
        i.by = by;
        let lsn = i.end_run(run, Status::Terminated, reason);
        i.by = None;
        let lsn = lsn?;
        let ended = idx.filter(|_| !i.slots.contains_key(&run));
        drop(i);
        if let Some(idx) = ended {
            let fut = self
                .futures
                .borrow_mut()
                .get_mut(idx as usize)
                .and_then(Option::take);
            drop(fut);
        }
        let mut i = self.inner.borrow_mut();
        let lsn = lsn.unwrap_or_else(|| i.writer.last_lsn());
        if let Some(t) = token {
            i.core.add_output(lsn, Output::Ack(t));
        }
        i.on_commit();
        Ok(())
    }

    /// Handles a message from another shard. `from` is the sender's shard
    /// and outbox ID; it is acknowledged once the message's effect is
    /// durable.
    pub fn message(&self, msg: Message, from: Option<(u32, u64)>) -> Result<(), Error> {
        self.inner.borrow().check_alive()?;
        let token = from.map(|f| self.inner.borrow_mut().msg_token(f));
        match msg {
            Message::ChildStart {
                parent,
                wtype,
                id,
                flags,
                input,
                ..
            } if flags & policy::OBJECT != 0 => {
                self.object_call_message(parent, wtype, id, flags, input, token, from)
            }
            Message::ChildStart {
                parent,
                wtype,
                id,
                flags,
                deadline_ms,
                input,
                trace,
            } => {
                let detached = flags & policy::DETACHED != 0;
                let req = StartRequest {
                    id,
                    wtype,
                    version: None,
                    input,
                    codec: 1,
                    deadline_ms,
                    parent: Some(parent),
                    policy: if self.inner.borrow().cfg.sabotage == Sabotage::NoAttach {
                        policy::RUNNING_USE_EXISTING
                    } else {
                        policy::ATTACH
                    } | (flags & (policy::CLOSE_MASK | policy::DETACHED)),
                    update: None,
                    sealed: true,
                    token,
                    trace,
                };
                match self.start(req) {
                    Ok(_) => Ok(()),
                    Err(e @ Error::UnknownWorkflow(_)) => {
                        let mut i = self.inner.borrow_mut();
                        if !detached {
                            i.send_out(
                                parent.shard,
                                Message::ChildResult {
                                    parent,
                                    status: Status::Failed,
                                    payload: e.to_failure().to_payload(),
                                },
                            );
                        }
                        if let Some(f) = from {
                            i.ack_at_tail(f);
                        }
                        i.on_commit();
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
            Message::ChildResult {
                parent,
                status,
                payload,
            } => {
                {
                    let mut i = self.inner.borrow_mut();
                    i.ensure_loaded(parent.run)?;
                    // The child answered: stop resending our start to it.
                    i.outbox.retain(|_, o| {
                        !matches!(&o.msg, Message::ChildStart { parent: p, .. } if *p == parent)
                    });
                    let run = parent.run;
                    let alive = i.core.get(run).is_some_and(|l| !l.is_done());
                    let resolved = match i.slots.get(&run) {
                        Some(slot) => {
                            parent.seq < slot.machine.next_seq()
                                && !slot.machine.is_live_pending(parent.seq)
                        }
                        None => false,
                    };
                    if alive && !resolved {
                        i.complete(run, parent.seq, Completion::Child { status, payload });
                        if let Some(f) = from {
                            i.child_acks.entry((run, parent.seq)).or_default().push(f);
                        }
                    } else if let Some(f) = from {
                        i.ack_at_tail(f);
                    }
                    if let Some(t) = token {
                        i.msg_tokens.remove(&t);
                    }
                    i.on_commit();
                }
                self.instantiate_pending();
                Ok(())
            }
            Message::Signal {
                target,
                name,
                payload,
                dedup,
            } => match self.signal_sealed(&target, &name, payload, dedup, token) {
                Ok(()) => Ok(()),
                // A signal to a missing or finished workflow is dropped, as
                // it would be had it arrived from a client.
                Err(Error::NotFound(_) | Error::Failed(_)) => {
                    let mut i = self.inner.borrow_mut();
                    if let Some(t) = token {
                        let tail = i.writer.last_lsn();
                        i.core.add_output(tail, Output::Ack(t));
                    }
                    i.on_commit();
                    Ok(())
                }
                Err(e) => Err(e),
            },
            Message::Close {
                target,
                wtype,
                parent,
                terminate,
                reason,
            } => {
                let applies = {
                    let mut i = self.inner.borrow_mut();
                    match i.core.find(&target) {
                        Some(Found::Loaded(r) | Found::Parked(r)) => {
                            i.ensure_loaded(r)?;
                            i.core.get(r).is_some_and(|l| {
                                !l.is_done() && (parent.is_none() || l.parent == parent)
                            })
                        }
                        None => {
                            if let Some(p) = parent.filter(|_| !wtype.is_empty()) {
                                // The parent closed before its child's start
                                // arrived: record the child as closed, so the
                                // late start attaches to it instead of running.
                                let status = if terminate {
                                    Status::Terminated
                                } else {
                                    Status::Cancelled
                                };
                                let lsn = i.start_closed(&target, &wtype, p, status, &reason)?;
                                if let Some(t) = token {
                                    i.core.add_output(lsn, Output::Ack(t));
                                }
                                i.on_commit();
                                return Ok(());
                            }
                            false
                        }
                    }
                };
                let reason = String::from_utf8_lossy(&reason).into_owned();
                if applies {
                    if terminate {
                        self.terminate(&target, &reason, token)
                    } else {
                        self.cancel(&target, &reason, token)
                    }
                } else {
                    let mut i = self.inner.borrow_mut();
                    if let Some(t) = token {
                        let tail = i.writer.last_lsn();
                        i.core.add_output(tail, Output::Ack(t));
                    }
                    i.on_commit();
                    Ok(())
                }
            }
        }
    }

    /// Messages to send to other shards: new ones, and unacknowledged ones
    /// due for a resend.
    pub fn take_outgoing(&self) -> Vec<Outgoing> {
        let mut i = self.inner.borrow_mut();
        if i.failed.is_some() {
            return Vec::new();
        }
        if i.cfg.replay_only {
            i.outbox.clear();
            return Vec::new();
        }
        let now = i.now();
        let mut out = Vec::new();
        for o in i.outbox.values_mut() {
            if o.sent_ms == 0 || now.saturating_sub(o.sent_ms) >= RESEND_MS {
                o.sent_ms = now.max(1);
                out.push(o.clone());
            }
        }
        out
    }

    /// Acknowledgments to send back: `(sender shard, outbox ID)`.
    pub fn take_message_acks(&self) -> Vec<(u32, u64)> {
        std::mem::take(&mut self.inner.borrow_mut().message_acks)
    }

    /// The receiver acknowledged outbox entry `id`.
    pub fn outbox_ack(&self, id: u64) {
        self.inner.borrow_mut().outbox.remove(&id);
    }

    pub fn outbox_len(&self) -> usize {
        self.inner.borrow().outbox.len()
    }

    pub fn summary(&self, run: RunId) -> Option<RunSummary> {
        let mut i = self.inner.borrow_mut();
        let shard = i.cfg.shard;
        if let Some(l) = i.core.get(run) {
            let status = if let Some((s, _)) = l.done {
                s.as_str().to_owned()
            } else if i.stuck.contains_key(&run) {
                "stuck".into()
            } else if let Some(slot) = i.slots.get(&run) {
                if slot.machine.is_paused() {
                    "paused".into()
                } else if i.parkable(run) {
                    "suspended".into()
                } else {
                    "running".into()
                }
            } else {
                "waiting_for_code".into()
            };
            return Some(RunSummary {
                shard,
                run,
                id: l.id.to_string(),
                wtype: l.wtype.to_string(),
                version: l.version,
                status,
                started_ms: l.started_ms,
                finished_ms: l.done.map(|d| d.1),
                error: i.stuck.get(&run).cloned().or_else(|| {
                    l.done
                        .filter(|d| d.0 != Status::Ok && !d.0.is_handover())
                        .and(l.result.as_ref())
                        .map(|p| String::from_utf8_lossy(p).into_owned())
                }),
                attributes: merge_attributes(&l.attrs),
                object: l.policy & policy::OBJECT != 0,
            });
        }
        let p = i.core.parked(run)?;
        let recs = i.blob_records(p.blob()).ok()?;
        let (id, wtype, version, started_ms, object) = match recs.first()? {
            Record::Start {
                id,
                wtype,
                version,
                started_ms,
                policy: pol,
                ..
            } => (
                id.to_string(),
                wtype.to_string(),
                *version,
                *started_ms,
                pol & policy::OBJECT != 0,
            ),
            _ => return None,
        };
        let finished = recs.iter().find_map(|r| match r {
            Record::Done { finished_ms, .. } => Some(*finished_ms),
            _ => None,
        });
        let error = recs.iter().find_map(|r| match r {
            Record::Done {
                status, payload, ..
            } if *status != Status::Ok && !status.is_handover() => {
                Some(String::from_utf8_lossy(payload).into_owned())
            }
            _ => None,
        });
        let attrs: Vec<Payload> = recs
            .iter()
            .filter_map(|r| match r {
                Record::Attributes { payload, .. } => Some(payload.clone()),
                _ => None,
            })
            .collect();
        Some(RunSummary {
            shard,
            run,
            id,
            wtype,
            version,
            status: p.done().map(|s| s.as_str().to_owned()).unwrap_or_else(|| {
                if journal_paused(&recs) {
                    "paused".into()
                } else {
                    "parked".into()
                }
            }),
            started_ms,
            finished_ms: p.done().and(finished),
            error,
            attributes: merge_attributes(&attrs),
            object,
        })
    }

    /// The newest `limit` runs (highest run number first).
    pub fn list(&self, limit: usize) -> Vec<RunSummary> {
        self.newest_matching(limit, |_| true)
    }

    /// The newest runs whose summary `keep` accepts, at most `limit`, newest
    /// first. The shard is borrowed one batch of run numbers at a time;
    /// [`summary`](Self::summary) borrows it again for each run.
    fn newest_matching(&self, limit: usize, keep: impl Fn(&RunSummary) -> bool) -> Vec<RunSummary> {
        let mut out = Vec::new();
        if limit == 0 {
            return out;
        }
        let mut cursor = self.inner.borrow().core.newest_first();
        loop {
            let want = (limit - out.len()).clamp(64, 4096);
            let batch = cursor.next_batch(&self.inner.borrow().core, want);
            if batch.is_empty() {
                return out;
            }
            for r in batch {
                if let Some(s) = self.summary(r)
                    && keep(&s)
                {
                    out.push(s);
                    if out.len() >= limit {
                        return out;
                    }
                }
            }
        }
    }

    pub fn inspect(&self, id: &str) -> Result<Option<RunDetail>, Error> {
        let Some(run) = self.find(id) else {
            return Ok(None);
        };
        let Some(summary) = self.summary(run) else {
            return Ok(None);
        };
        let mut i = self.inner.borrow_mut();
        let journal = i.journal_of(run)?;
        let object = i.object_detail(run, &journal).map(Box::new);
        Ok(Some(RunDetail {
            summary,
            journal,
            object,
        }))
    }

    /// The newest runs matching `filter`, at most `limit`, newest first.
    pub fn list_where(
        &self,
        filter: &crate::visibility::ListFilter,
        limit: usize,
    ) -> Vec<RunSummary> {
        if limit == 0 {
            return Vec::new();
        }
        if !filter.is_empty() {
            let window = limit.saturating_add(64).min(4096);
            let candidates = {
                self.inner
                    .borrow_mut()
                    .env
                    .visibility_candidates(filter, window)
            };
            if let Some(c) = candidates {
                let current = {
                    let i = self.inner.borrow();
                    if c.generation != i.env.store_generation() {
                        None
                    } else {
                        let mut runs: Vec<_> = i
                            .core
                            .loaded_runs()
                            .map(|(r, _)| r)
                            .chain(
                                i.core
                                    .parked_runs()
                                    .filter(|(_, p)| p.blob().offset >= c.length)
                                    .map(|(r, _)| r),
                            )
                            .collect();
                        runs.extend(c.runs);
                        runs.sort_unstable_by(|a, b| b.cmp(a));
                        runs.dedup();
                        Some(runs)
                    }
                };
                if let Some(runs) = current {
                    let result: Vec<_> = runs
                        .into_iter()
                        .filter_map(|r| self.summary(r))
                        .filter(|s| filter.matches(s))
                        .take(limit)
                        .collect();
                    // Every match was a candidate, or the window held the
                    // newest indexed matches and enough of them still match
                    // (a run that changed since the index is a candidate
                    // either way: loaded, or parked past its length).
                    if !c.truncated || (c.newest && result.len() == limit) {
                        return result;
                    }
                }
            }
        }
        self.newest_matching(limit, |s| filter.matches(s))
    }

    /// Sends an update. `payload` is plain (the codec is applied here).
    /// The answer comes as [`Output::Reply`] (the handler's result, once
    /// durable) or [`Output::Refuse`] (the validator said no; nothing was
    /// recorded).
    pub fn update(
        &self,
        id: &str,
        name: &str,
        update_id: &str,
        payload: Payload,
        token: Option<u64>,
    ) -> Result<(), Error> {
        {
            let mut i = self.inner.borrow_mut();
            i.check_alive()?;
            let run = match i.core.find(id) {
                Some(Found::Loaded(r) | Found::Parked(r)) => r,
                None => return Err(Error::NotFound(id.to_owned())),
            };
            i.ensure_loaded(run)?;
            let uid = if update_id.is_empty() {
                i.fresh_update_id()
            } else {
                update_id.to_owned()
            };
            i.offer_update(run, name, &uid, payload, token)?;
            i.on_commit();
        }
        self.instantiate_pending();
        Ok(())
    }

    /// A foreign host's validator decided on a live update. Returns the
    /// update's ordinal when accepted.
    pub fn decide_update(
        &self,
        token: u64,
        decision: Result<(), String>,
    ) -> Result<Option<u64>, Error> {
        let mut i = self.inner.borrow_mut();
        let r = i.decide_update(token, decision);
        i.on_commit();
        r
    }

    /// Runs a query handler of native code against the run's state. The
    /// answer is released once the state it read is durable.
    pub fn query(&self, id: &str, name: &str, arg: Payload, token: u64) -> Result<(), Error> {
        let run = {
            let mut i = self.inner.borrow_mut();
            i.check_alive()?;
            let run = match i.core.find(id) {
                Some(Found::Loaded(r) | Found::Parked(r)) => r,
                None => return Err(Error::NotFound(id.to_owned())),
            };
            i.ensure_loaded(run)?;
            run
        };
        self.instantiate_pending();
        self.drive(run);
        self.try_query(run, name, arg, token);
        Ok(())
    }

    fn try_query(&self, run: RunId, name: &str, arg: Payload, token: u64) {
        let refuse = |i: &mut Inner<E>, at: Lsn, msg: &str| {
            i.core.add_output(
                at,
                Output::Refuse {
                    token,
                    message: Arc::from(msg.as_bytes()),
                },
            );
            i.on_commit();
        };
        let lsn = {
            let mut i = self.inner.borrow_mut();
            let Some(l) = i.core.get(run) else {
                let tail = i.writer.last_lsn();
                refuse(&mut i, tail, "the workflow is no longer loaded");
                return;
            };
            let lsn = l.last_lsn;
            if l.is_done() {
                refuse(&mut i, lsn, "the workflow has already finished");
                return;
            }
            match i.slots.get(&run) {
                None => {
                    refuse(
                        &mut i,
                        lsn,
                        "the workflow is not running code (it is stuck or its code is not registered)",
                    );
                    return;
                }
                Some(s) if s.machine.is_replaying() => {
                    drop(i);
                    self.deferred
                        .borrow_mut()
                        .push((run, name.to_owned(), arg, token));
                    return;
                }
                Some(_) => lsn,
            }
        };
        let handler = self.handlers.borrow().get(&run).and_then(|h| h.query(name));
        let answer = match handler {
            None => Err(format!("no handler for query '{name}'")),
            Some(f) => f(arg),
        };
        let mut i = self.inner.borrow_mut();
        if let (Sabotage::EarlyAnswers, Ok(v)) = (i.cfg.sabotage, &answer) {
            i.released.push(Output::Reply {
                token,
                status: Status::Ok,
                payload: v.clone(),
            });
            return;
        }
        match answer {
            Ok(v) => {
                i.core.add_output(
                    lsn,
                    Output::Reply {
                        token,
                        status: Status::Ok,
                        payload: v,
                    },
                );
                i.on_commit();
            }
            Err(m) => refuse(&mut i, lsn, &m),
        }
    }

    fn retry_deferred(&self) -> bool {
        if self.deferred.borrow().is_empty() {
            return false;
        }
        let pending = std::mem::take(&mut *self.deferred.borrow_mut());
        if pending.is_empty() {
            return false;
        }
        for (run, name, arg, token) in pending {
            self.try_query(run, &name, arg, token);
        }
        true
    }

    /// Reads stream `name` of workflow `id` (of run `run` when given) from
    /// offset `from`. The answer goes out as [`Output::Reply`] for `token`
    /// (JSON, see [`crate::stream::StreamBatch`]): at once when durable
    /// chunks or the end are there, else once some are, or with no chunks
    /// after `wait_ms`. Chunks are handed out only once durable (committed,
    /// when replicated), so a reader never sees one a crash takes back.
    pub fn stream_read(
        &self,
        id: &str,
        run: Option<RunId>,
        name: &str,
        from: u64,
        wait_ms: u64,
        token: u64,
    ) -> Result<(), Error> {
        let mut i = self.inner.borrow_mut();
        i.check_alive()?;
        let run = i.stream_run(id, run)?;
        let mut r = i.read_stream(run, name, from)?;
        if r.id.is_none() {
            // A parked run found by its ID's hash: its journal names the ID.
            r.id = i.scan_stream(run, name, 0, 0, 0)?.id;
            if let Some(rs) = i.streams.get_mut(&run) {
                rs.id = r.id.clone();
            }
        }
        if r.id.as_deref().is_some_and(|x| x != id) {
            return Err(Error::NotFound(id.to_owned()));
        }
        if !r.chunks.is_empty() || r.end.is_some() || wait_ms == 0 {
            let payload = i.stream_answer(&r)?;
            i.released.push(Output::Reply {
                token,
                status: Status::Ok,
                payload,
            });
            return Ok(());
        }
        if let Some(lsn) = r.wait {
            i.stream_due.push(Reverse((lsn, run)));
        }
        let deadline_ms = i.now().saturating_add(wait_ms);
        i.stream_wait_until = i.stream_wait_until.min(deadline_ms);
        i.stream_waiters.entry(run).or_default().push(StreamWaiter {
            name: Arc::from(name),
            from,
            token,
            deadline_ms,
        });
        Ok(())
    }

    /// Reads waiting for streams to grow.
    pub fn stream_readers(&self) -> usize {
        self.inner
            .borrow()
            .stream_waiters
            .values()
            .map(Vec::len)
            .sum()
    }

    /// For foreign hosts answering queries themselves: the run's ID check
    /// and the log position the answer must wait for.
    pub fn query_point(&self, id: &str) -> Result<(RunId, Lsn), Error> {
        let mut i = self.inner.borrow_mut();
        let run = match i.core.find(id) {
            Some(Found::Loaded(r) | Found::Parked(r)) => r,
            None => return Err(Error::NotFound(id.to_owned())),
        };
        i.ensure_loaded(run)?;
        match i.core.get(run) {
            Some(l) if l.is_done() => {
                Err(Error::Rejected("the workflow has already finished".into()))
            }
            Some(l) => Ok((run, l.last_lsn)),
            None => Err(Error::NotFound(id.to_owned())),
        }
    }

    /// Acknowledges `token` once the log is durable through `lsn`.
    pub fn ack_at(&self, lsn: Lsn, token: u64) {
        let mut i = self.inner.borrow_mut();
        i.core.add_output(lsn, Output::Ack(token));
        i.on_commit();
    }

    /// Finishes an async completion from its token. `payload` is plain.
    /// Acknowledged once the result is recorded; a stale token (an older
    /// attempt, a finished step, another run) is refused.
    pub fn complete(
        &self,
        token: &str,
        status: Status,
        payload: Payload,
        reply: Option<u64>,
    ) -> Result<(), Error> {
        {
            let mut i = self.inner.borrow_mut();
            i.check_alive()?;
            let refuse = |i: &mut Inner<E>, msg: &str| {
                if let Some(t) = reply {
                    let tail = i.writer.last_lsn();
                    i.core.add_output(
                        tail,
                        Output::Refuse {
                            token: t,
                            message: Arc::from(msg.as_bytes()),
                        },
                    );
                    i.on_commit();
                }
            };
            let Some((id, run, seq, attempt)) = parse_token(token) else {
                return Err(Error::Rejected("not a completion token".into()));
            };
            match i.core.find(&id) {
                Some(Found::Loaded(r) | Found::Parked(r)) if r == run => {}
                _ => {
                    refuse(&mut i, "the workflow run this token belongs to is gone");
                    return Ok(());
                }
            }
            i.ensure_loaded(run)?;
            let marker = i
                .core
                .get(run)
                .filter(|l| !l.is_done())
                .and_then(|l| l.awaiting.get(&seq).copied());
            let live = i.slots.get(&run).and_then(|s| s.machine.await_attempt(seq));
            if marker != Some(attempt) && live != Some(attempt) {
                refuse(
                    &mut i,
                    "stale completion token: the step finished or moved on to another attempt",
                );
                return Ok(());
            }
            let payload = if status == Status::Ok {
                i.seal(payload)?
            } else {
                payload
            };
            if let Some(t) = reply {
                i.completion_acks.entry((run, seq)).or_default().push(t);
            }
            i.complete(
                run,
                seq,
                Completion::Completed {
                    attempt,
                    status,
                    payload,
                },
            );
            i.on_commit();
        }
        self.instantiate_pending();
        Ok(())
    }

    /// Starts a new run of `id` whose journal is the current run's cut just
    /// before the first record of step `before_step`, then runs the code
    /// from there. The old run is kept and marked reset (it is ended if it
    /// was still running). Signals received after the cut are reapplied or
    /// dropped. Returns the new run.
    pub fn reset(
        &self,
        id: &str,
        before_step: &str,
        reapply_signals: bool,
        version: Option<u32>,
        token: Option<u64>,
    ) -> Result<RunId, Error> {
        let new = {
            let mut i = self.inner.borrow_mut();
            let by = i.by.take();
            i.check_alive()?;
            let run = match i.core.find(id) {
                Some(Found::Loaded(r) | Found::Parked(r)) => r,
                None => return Err(Error::NotFound(id.to_owned())),
            };
            i.ensure_loaded(run)?;
            let journal = i.journal_of(run)?;
            let target = journal.iter().find_map(|r| match r {
                Record::Step { seq, name, .. }
                | Record::Attempt { seq, name, .. }
                | Record::Awaiting { seq, name, .. }
                    if &**name == before_step =>
                {
                    Some(*seq)
                }
                _ => None,
            });
            let Some(target) = target else {
                let thin = journal.iter().any(|r| matches!(r, Record::Done { .. }))
                    && journal.iter().all(|r| {
                        matches!(
                            r,
                            Record::Start { .. }
                                | Record::Done { .. }
                                | Record::Attributes { .. }
                                | Record::Chunk { .. }
                                | Record::StreamEnd { .. }
                        )
                    });
                return Err(Error::Rejected(if thin {
                    format!(
                        "run of '{id}' finished and only its result was kept; open the engine \
                         with keep_history to reset finished runs"
                    )
                } else {
                    format!("the run of '{id}' has no step named '{before_step}'")
                }));
            };
            let cut = journal
                .iter()
                .skip(1)
                .position(|r| r.seq() == Some(target))
                .map(|p| p + 1)
                .unwrap_or(journal.len());
            let Some(Record::Start {
                id: rid,
                wtype,
                version: old_version,
                codec,
                started_ms,
                deadline_ms,
                parent,
                policy: pol,
                update,
                input,
                meta,
                ..
            }) = journal.first().cloned()
            else {
                return Err(Error::Io(format!("run {run}: journal has no start")));
            };
            let mut copies: Vec<RecordBuf> = journal[1..cut]
                .iter()
                .filter(|r| !matches!(r, Record::Done { .. }))
                .cloned()
                .collect();
            if reapply_signals {
                copies.extend(
                    journal[cut..]
                        .iter()
                        .filter(|r| matches!(r, Record::Signal { .. }))
                        .cloned(),
                );
            }
            let new = i.core.allocate_run();
            let start: RecordBuf = Record::Start {
                run: new,
                id: rid,
                wtype,
                version: version.unwrap_or(old_version),
                codec,
                started_ms,
                deadline_ms,
                parent,
                policy: pol,
                from: Some(Origin {
                    run,
                    cause: origin::RESET,
                }),
                copies: copies.len() as u32,
                update,
                input,
                // The new run keeps the old one's trace context.
                meta: StartMeta::of(by.clone(), meta.and_then(|m| m.trace)),
            };
            let running = i.core.get(run).is_some_and(|l| !l.is_done());
            let now = i.now();
            let mut lsn = if running {
                let mut payload = Vec::new();
                encode_canonical(&start, &mut payload);
                i.append(&Record::Done {
                    run,
                    status: Status::Reset,
                    finished_ms: now,
                    payload: Arc::from(payload),
                    by,
                })?
            } else {
                i.append(&start)?
            };
            if let Some(slot) = i.remove_slot(run)
                && slot.foreign
            {
                i.foreign.push_back(ForeignEvent::Ended { run });
            }
            i.release_waiting_requests(run);
            for c in &copies {
                lsn = i.append(&c.with_run(new))?;
            }
            let mut journal = i.journal_of(new)?;
            i.prepare(new, &mut journal);
            i.core.reoffer(new);
            if let Some(t) = token {
                i.core.add_output(lsn, Output::Ack(t));
            }
            i.metrics.runs_started += 1;
            i.on_commit();
            new
        };
        self.instantiate_pending();
        Ok(new)
    }

    /// The earliest durable or volatile timer (a sleep, a deadline, a
    /// retry), without the shard's own housekeeping (the test kit's time
    /// skip).
    pub fn next_timer_ms(&self) -> Option<u64> {
        let i = self.inner.borrow();
        let mut next = i.core.next_timer();
        if let Some(Reverse((at, _))) = i.timers.peek() {
            next = Some(next.map_or(*at, |n| n.min(*at)));
        }
        next
    }

    /// Loads an exported journal as a new run (replay testing). Its
    /// records are copies: nothing they caused is sent again. With
    /// [`RuntimeConfig::replay_only`] the code then replays against it.
    pub fn import(&self, journal: &[RecordBuf]) -> Result<RunId, Error> {
        let new = {
            let mut i = self.inner.borrow_mut();
            let Some(Record::Start { .. }) = journal.first() else {
                return Err(Error::Codec(
                    "the journal does not begin with a start".into(),
                ));
            };
            let copies: Vec<RecordBuf> = journal[1..]
                .iter()
                .filter(|r| !matches!(r, Record::Done { .. }))
                .cloned()
                .collect();
            let new = i.core.allocate_run();
            let mut start = journal[0].with_run(new);
            if let Record::Start {
                copies: c, from, ..
            } = &mut start
            {
                *c = copies.len() as u32;
                *from = None;
            }
            i.append(&start)?;
            for c in &copies {
                i.append(&c.with_run(new))?;
            }
            let mut journal = i.journal_of(new)?;
            i.prepare(new, &mut journal);
            i.core.reoffer(new);
            new
        };
        self.instantiate_pending();
        Ok(new)
    }

    /// The determinism error a run stopped on, if any.
    pub fn stuck_reason(&self, run: RunId) -> Option<String> {
        self.inner.borrow().stuck.get(&run).cloned()
    }

    /// Whether a run's code is live (has a slot) and whether its journal
    /// still has recorded deliveries ahead.
    pub fn code_state(&self, run: RunId) -> Option<(bool, bool)> {
        let i = self.inner.borrow();
        let s = i.slots.get(&run)?;
        Some((s.machine.is_replaying(), s.machine.has_live_pending()))
    }

    pub(crate) fn is_foreign_slot(&self, run: RunId) -> bool {
        self.inner
            .borrow()
            .slots
            .get(&run)
            .is_some_and(|slot| slot.foreign)
    }

    // ---- Foreign hosts ----

    /// Pumps one delivery for a foreign run whose code is quiescent.
    pub fn quiescent(&self, run: RunId) -> Result<Option<Delivery>, Error> {
        self.inner.borrow_mut().deliver(run)
    }

    /// Foreign code returned.
    pub fn foreign_finish(&self, run: RunId, outcome: RunOutcome) {
        self.inner.borrow_mut().finish(run, outcome);
    }

    /// Foreign code hit an error it cannot recover from.
    pub fn foreign_stuck(&self, run: RunId, message: String) {
        self.inner.borrow_mut().make_stuck(run, message);
    }

    /// A single foreign workflow can flush its pending dependency directly
    /// when it has reached an external step. At higher load the usual tick
    /// combines records from multiple workflows into the same flush.
    pub(crate) fn flush_solo_gate(
        &self,
        run: RunId,
        gate: Lsn,
        not_before: u64,
    ) -> Result<bool, Error> {
        let mut i = self.inner.borrow_mut();
        i.check_alive()?;
        // Other runs may be loaded, as long as none has anything to deliver.
        if (i.slots.len() != 1 && (i.has_undelivered() || !i.foreign.is_empty()))
            || !i.slots.get(&run).is_some_and(|s| s.foreign)
            || i.cfg.replay_only
            || !i.cfg.inline_flush
            || i.in_flight.is_some()
            || i.env.last_flush_us() > i.cfg.inline_max_us
            || !i.env.supports_inline()
            || gate <= i.commit
            || gate > i.writer.last_lsn()
            || not_before > i.now()
        {
            return Ok(false);
        }
        i.maybe_flush(true);
        i.check_alive()?;
        Ok(i.commit >= gate)
    }

    pub fn is_idle(&self) -> bool {
        let i = self.inner.borrow();
        i.slots.is_empty() && i.in_flight.is_none() && !i.writer.has_pending()
    }

    // ---- A foreign host's flush off its own thread (see `foreign.rs`) ----

    /// Hands what is pending to the log's threads (never inline) if no
    /// flush is in flight. Returns the last record in flight.
    pub(crate) fn submit_flush(&self) -> Option<Lsn> {
        let mut i = self.inner.borrow_mut();
        i.maybe_flush(false);
        i.in_flight
    }

    /// Takes a finished flush as a tick would: false while none finished.
    pub(crate) fn poll_flush(&self) -> bool {
        let mut i = self.inner.borrow_mut();
        i.in_flight.is_some() && i.poll_io()
    }

    /// How long the log's last write took, as the environment measured it.
    pub(crate) fn last_flush_us(&self) -> u64 {
        self.inner.borrow().env.last_flush_us()
    }

    /// Whether an idle tick may flush on this thread (`RuntimeConfig::inline_flush`).
    pub(crate) fn set_inline_flush(&self, on: bool) {
        self.inner.borrow_mut().cfg.inline_flush = on;
    }

    /// How many events the host can still take this tick: loading stops
    /// before the runs it loads would queue more (`usize::MAX`: no bound).
    pub(crate) fn set_load_room(&self, room: usize) {
        self.inner.borrow_mut().load_room = room;
    }

    /// Events queued for the foreign host (see [`take_foreign`](Self::take_foreign)).
    pub(crate) fn foreign_len(&self) -> usize {
        self.inner.borrow().foreign.len()
    }

    /// Nothing waits for the host's thread here: no delivery, event or
    /// start queued, and (`writes`) no record pending or in flight.
    pub(crate) fn quiet(&self, writes: bool) -> bool {
        let mut i = self.inner.borrow_mut();
        !i.has_undelivered()
            && i.foreign.is_empty()
            && i.instantiate.is_empty()
            && !i.core.has_events()
            && (!writes || (i.in_flight.is_none() && !i.writer.has_pending()))
    }

    pub fn loaded(&self) -> usize {
        self.inner.borrow().core.loaded_len()
    }

    pub fn parked(&self) -> usize {
        self.inner.borrow().core.parked_len()
    }

    /// Clean shutdown: flushes everything pending, then checkpoints whatever
    /// changed since the last checkpoint and waits for it, so the next open
    /// replays nothing and the checkpoint can delete covered segments.
    ///
    /// Only I/O moves from here on: no run is driven and no timer fires, so
    /// nothing is written after the final checkpoint and a close ends even
    /// while runs are busy. Their unfinished work resumes on the next open.
    pub fn close(&self) -> Result<(), Error> {
        let mut started_final = false;
        loop {
            {
                let mut i = self.inner.borrow_mut();
                i.poll_io();
                i.maybe_flush(true);
                i.check_alive()?;
                if !i.ckpt_running
                    && i.ckpt_pending.is_none()
                    && !i.writer.has_pending()
                    && i.in_flight.is_none()
                {
                    // A checkpoint already in flight at shutdown can predate
                    // the last flush: decide once it has completed.
                    let needs_checkpoint = !started_final
                        && !i.cfg.replay_only
                        && i.cfg.checkpoint_on_close
                        && (i.checkpoint_needed() || i.env.checkpoint_wanted_on_close());
                    if !needs_checkpoint {
                        let due = i.due();
                        i.env.report_due(due, true);
                        return Ok(());
                    }
                    let now = i.now();
                    i.start_checkpoint(now);
                    started_final = true;
                    continue;
                }
            }
            std::thread::sleep(std::time::Duration::from_micros(100));
        }
    }

    /// Flushes everything pending and waits for it (shutdown, tests).
    pub fn flush_all(&self) -> Result<(), Error> {
        loop {
            self.tick();
            let (pending, in_flight) = {
                let i = self.inner.borrow();
                i.check_alive()?;
                (i.writer.has_pending(), i.in_flight.is_some())
            };
            if !pending && !in_flight {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_micros(50));
        }
    }
}

/// `FUSEE_DRIVE_FLUSH_EVERY`: how many runs a round drives between
/// mid-round submissions and completion checks; 0 (the default) checks
/// only between rounds.
fn drive_flush_every() -> usize {
    static EVERY: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *EVERY.get_or_init(|| {
        std::env::var("FUSEE_DRIVE_FLUSH_EVERY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    })
}

/// `FUSEE_ADMIT_ROUND`: see [`ShardRuntime::behind`].
fn admit_round() -> usize {
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("FUSEE_ADMIT_ROUND")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2048)
    })
}
