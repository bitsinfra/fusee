//! Native half of the `fusee` npm package. The TypeScript SDK in `src/`
//! drives workflow code on the JavaScript thread; this module holds the
//! engine (shards, logs, flusher threads) and exposes its operations.

use fusee::engine::Config;
use fusee::foreign::{EngineEvent, ForeignEngine, Group};
use fusee::limits::{Acquire, LimitSpec};
use fusee::local::FlushMethod;
use fusee::runtime::{
    Delivery, ResultState, RuntimeConfig, Services, ShardRuntime, StartOutcome, StartRequest,
    UpdateRequest,
};
use fusee::visibility::ListFilter;
use fusee_core::record::{Name, Payload, RunId, Status};
use fusee_core::run::{AwaitStart, StepStart};
use fusee_server::foreign::ForeignConnection;
use napi::JsString;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::ThreadsafeFunctionCallMode;
use napi_derive::napi;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use std::task::Waker;
use std::time::Duration;

mod driver;

// The scratch array the SDK shares with this module (`io` in src/index.ts):
// hot calls write their results there instead of returning objects.
// Slots 0..8 hold the last call's outputs.
const IO_OWED: usize = 8;
const IO_NEXT: usize = 9;
const IO_EVENTS: usize = 10;
/// Set by the SDK before `tick()` when nothing else waits for its thread:
/// the tick may then wait briefly for a flush in flight (see
/// `fusee::foreign::Flush::Pipeline`).
const IO_IDLE: usize = 13;
/// Set by the SDK before `tick()` when the turn only pumps deliveries: the
/// tick then skips the engine unless something woke it since the last one.
const IO_LIGHT: usize = 14;
/// Commit positions, one per shard, refreshed by `tick` and by gate checks.
const IO_COMMIT: usize = 16;
const MAX_COMMIT_SHARDS: usize = 1024;
/// Events from `tick`, `EV_STRIDE` numbers each.
const IO_EV: usize = IO_COMMIT + MAX_COMMIT_SHARDS;
const EV_STRIDE: usize = 6;

const EV_WAKE: f64 = 1.0;
const EV_ACK: f64 = 2.0;
const EV_REJECT: f64 = 3.0;
const EV_START: f64 = 4.0;
const EV_STUCK: f64 = 5.0;
const EV_ENDED: f64 = 6.0;
const EV_RESULT: f64 = 7.0;
const EV_REPLY: f64 = 8.0;
const EV_REFUSE: f64 = 9.0;

/// A payload that is not JSON: raw bytes after a zero byte, which no JSON
/// text starts with.
const RAW: u8 = 0;

thread_local! {
    /// Depth of calls from JavaScript into this module on this thread. A
    /// wake raised meanwhile needs no callback: the SDK ticks after the call.
    static IN_CALL: Cell<u32> = const { Cell::new(0) };
}

fn raw_payload(b: &[u8]) -> Payload {
    let mut v = Vec::with_capacity(b.len() + 1);
    v.push(RAW);
    v.extend_from_slice(b);
    Arc::from(v)
}

/// JSON text crosses as a string; raw bytes (a `Uint8Array` the code gave)
/// as bytes.
fn wire_payload(v: &Either<String, Uint8Array>) -> Payload {
    match v {
        Either::A(s) => Arc::from(s.as_bytes()),
        Either::B(b) => raw_payload(b),
    }
}

type JsPayload<'e> = Either<JsString<'e>, BufferSlice<'e>>;

/// A recorded payload for JavaScript: JSON or text as a string (no decoder
/// on the other side), raw bytes as a copied buffer.
fn js_payload<'e>(env: &'e Env, p: &[u8]) -> Result<JsPayload<'e>> {
    if p.first() == Some(&RAW) {
        return Ok(Either::B(BufferSlice::copy_from(env, &p[1..])?));
    }
    Ok(Either::A(env.create_string(&*String::from_utf8_lossy(p))?))
}

/// An error as `Code: message`, the code the same in every SDK; a failure
/// (a step's, a child's, a workflow's) as `Code: <its envelope as JSON>`.
fn err(e: fusee::Error) -> napi::Error {
    if e.failure().is_some() {
        let envelope = e.to_failure().to_payload();
        return napi::Error::new(
            napi::Status::GenericFailure,
            format!("{}: {}", e.code(), String::from_utf8_lossy(&envelope)),
        );
    }
    napi::Error::new(napi::Status::GenericFailure, format!("{}: {e}", e.code()))
}

fn payload(b: &[u8]) -> Payload {
    Arc::from(b)
}

fn status_from(v: u32) -> Status {
    match v {
        0 => Status::Ok,
        2 => Status::Cancelled,
        3 => Status::TimedOut,
        4 => Status::Terminated,
        _ => Status::Failed,
    }
}

#[napi(object)]
pub struct CodecKey {
    pub id: String,
    pub key: Buffer,
}

#[napi(object)]
pub struct OpenOptions {
    pub dir: Option<String>,
    pub durability: Option<String>,
    pub shards: Option<u32>,
    pub park_after_ms: Option<f64>,
    pub retention_ms: Option<f64>,
    pub checkpoint_interval_ms: Option<f64>,
    pub flush: Option<String>,
    pub segment_bytes: Option<f64>,
    pub keep_history: Option<bool>,
    pub archive_history: Option<bool>,
    /// AES-256-GCM keys; the first encrypts.
    pub codec_keys: Option<Vec<CodecKey>>,
    /// Flush on the JavaScript thread when nothing else is pending (default
    /// on); off, every flush goes to the log thread.
    pub inline_flush: Option<bool>,
    /// "native": a libuv handle runs the SDK's turns (see `driver`); the
    /// notify function is then the turn. Anything else: a threadsafe
    /// callback per wake.
    pub driver: Option<String>,
    /// Open part `worker` of the group `group` (see `NativeGroup`) on this
    /// thread; the group's options apply and the ones above are ignored
    /// (but `driver`).
    pub group: Option<f64>,
    pub worker: Option<u32>,
}

/// The engine settings the options describe (`connection` for server and
/// replicated mode). Without a shard count, a local engine on one thread
/// has one shard, and a group (`split`) one per worker.
fn config(
    opts: &OpenOptions,
    connection: Option<&ForeignConnection>,
    split: bool,
) -> Result<Config> {
    let mut cfg = match opts.durability.as_deref().unwrap_or("local") {
        "local" => Config::local(opts.dir.clone().unwrap_or_else(|| "./fusee-data".into())),
        "memory" => Config::memory(),
        "server" | "replicated" => connection
            .ok_or_else(|| napi::Error::from_reason("missing prepared connection"))?
            .config
            .clone(),
        mode => {
            return Err(napi::Error::from_reason(format!(
                "unsupported durability '{mode}'"
            )));
        }
    };
    if connection.is_some() {
        if opts.shards.is_some() && opts.shards != cfg.shards {
            return Err(napi::Error::from_reason(
                "shard count must match the remote configuration",
            ));
        }
    } else if let Some(s) = opts.shards {
        cfg = cfg.shards(s);
    } else if opts.durability.as_deref().unwrap_or("local") == "local" && !split {
        cfg.shards = Some(cfg.shards.unwrap_or(1));
    }
    if let Some(v) = opts.park_after_ms {
        cfg.park_after = Duration::from_millis(v as u64);
    }
    if let Some(v) = opts.retention_ms {
        cfg.retention = Duration::from_millis(v as u64);
    }
    if let Some(v) = opts.checkpoint_interval_ms {
        cfg.checkpoint_interval = Duration::from_millis(v as u64);
    }
    if let Some(v) = opts.segment_bytes {
        cfg.local.segment_bytes = v as u64;
    }
    cfg.keep_history = opts.keep_history.unwrap_or(false);
    cfg.archive_history = opts.archive_history.unwrap_or(false);
    cfg.codec = codec(&opts.codec_keys)?;
    if let Some(on) = opts.inline_flush {
        cfg.inline_flush = on;
    }
    match opts.flush.as_deref() {
        Some("fsync") => cfg.local.flush = FlushMethod::Fsync,
        Some("fullfsync") => cfg.local.flush = FlushMethod::FullFsync,
        Some("fdatasync") => cfg.local.flush = FlushMethod::Fdatasync,
        Some("dsync") => cfg.local.flush = FlushMethod::Dsync,
        _ => {}
    }
    Ok(cfg)
}

/// Groups opened in this process, for worker threads to find by ID.
fn groups() -> std::sync::MutexGuard<'static, HashMap<u64, Arc<Group>>> {
    static GROUPS: OnceLock<Mutex<HashMap<u64, Arc<Group>>>> = OnceLock::new();
    GROUPS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// One engine's shards split across worker threads, held by the thread
/// that opened it: each worker opens its part with
/// `NativeEngine.open({ group: id, worker })`, and this handle answers
/// reads across all parts (it blocks until each part's thread ticks, so
/// only a thread that holds no part may call them).
#[napi]
pub struct NativeGroup {
    id: u64,
    group: Option<Arc<Group>>,
}

impl NativeGroup {
    fn g(&self) -> Result<&Arc<Group>> {
        self.group.as_ref().ok_or_else(|| err(fusee::Error::Closed))
    }
}

#[napi]
impl NativeGroup {
    /// Takes the directory lock and opens the shared log for `workers`
    /// parts (at most one per shard). Local and memory durability only.
    #[napi(factory)]
    pub fn open(opts: OpenOptions, workers: u32) -> Result<Self> {
        if !matches!(opts.durability.as_deref(), None | Some("local" | "memory")) {
            return Err(napi::Error::from_reason(
                "worker threads need local or memory durability",
            ));
        }
        let cfg = config(&opts, None, true)?;
        let group = Group::open(&cfg, workers.max(1)).map_err(err)?;
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        groups().insert(id, group.clone());
        Ok(Self {
            id,
            group: Some(group),
        })
    }

    /// `[id, shards, workers]`.
    #[napi]
    pub fn info(&self) -> Result<Vec<f64>> {
        let g = self.g()?;
        Ok(vec![
            self.id as f64,
            f64::from(g.shards()),
            f64::from(g.workers()),
        ])
    }

    /// The worker that owns a workflow ID's shard, or, with `token`, the
    /// shard of a completion token's workflow (-1 for a malformed token).
    #[napi]
    pub fn route(&self, key: String, token: bool) -> Result<i32> {
        let g = self.g()?;
        let id = if token {
            match fusee::runtime::parse_token(&key) {
                Some((id, ..)) => id,
                None => return Ok(-1),
            }
        } else {
            key
        };
        Ok(g.owner(g.shard_of(&id)) as i32)
    }

    #[napi]
    pub fn limit(&self, name: String, concurrent: u32, per_second: f64) -> Result<()> {
        self.g()?
            .limit(
                &name,
                LimitSpec {
                    concurrent,
                    per_second,
                },
            )
            .map_err(err)
    }

    /// `[permits held, steps waiting]` of a limit, across every part.
    #[napi]
    pub fn limit_usage(&self, name: String) -> Result<Vec<f64>> {
        let (held, waiting) = self.g()?.limit_usage(&name);
        Ok(vec![f64::from(held), waiting as f64])
    }

    #[napi]
    pub fn list(&self, limit: u32) -> Result<String> {
        let runs = self.g()?.list_where(None, limit as usize).map_err(err)?;
        Ok(serde_json::Value::Array(runs.iter().map(summary_json).collect()).to_string())
    }

    #[napi]
    pub fn list_where(&self, filter: String, limit: u32) -> Result<String> {
        let v: serde_json::Value = serde_json::from_str(&filter)
            .map_err(|e| napi::Error::new(napi::Status::InvalidArg, e.to_string()))?;
        let f = ListFilter::parse(&v).map_err(err)?;
        let runs = self
            .g()?
            .list_where(Some(&f), limit as usize)
            .map_err(err)?;
        Ok(serde_json::Value::Array(runs.iter().map(summary_json).collect()).to_string())
    }

    #[napi]
    pub fn inspect(&self, id: String) -> Result<Option<String>> {
        Ok(self
            .g()?
            .inspect(&id)
            .map_err(err)?
            .map(|d| detail_json(&d)))
    }

    #[napi]
    pub fn export_journal(&self, id: String) -> Result<Option<Buffer>> {
        Ok(self
            .g()?
            .inspect(&id)
            .map_err(err)?
            .map(|d| Buffer::from(fusee::export::ExportedJournal::from_detail(&d).encode())))
    }

    #[napi]
    pub fn shard_metrics(&self) -> Result<String> {
        let m = self.g()?.shard_metrics().map_err(err)?;
        serde_json::to_string(&m).map_err(|e| err(e.into()))
    }

    /// Drops this thread's hold on the group. The directory lock and the
    /// log are released once every part has closed as well.
    #[napi]
    pub fn close(&mut self) {
        groups().remove(&self.id);
        self.group.take();
    }
}

impl Drop for NativeGroup {
    fn drop(&mut self) {
        groups().remove(&self.id);
    }
}

fn detail_json(d: &fusee::RunDetail) -> String {
    let journal = fusee::journal::normalize(&d.journal);
    let mut v = serde_json::json!({
        "id": d.summary.id, "type": d.summary.wtype, "version": d.summary.version,
        "status": d.summary.status, "startedMs": d.summary.started_ms,
        "finishedMs": d.summary.finished_ms, "error": d.summary.error,
        "shard": d.summary.shard, "run": d.summary.run, "journal": journal,
    });
    if let Some(o) = &d.object {
        v["object"] = serde_json::json!({"state": o.state, "version": o.version,
            "running": o.running, "waiting": o.waiting});
    }
    v.to_string()
}

fn codec(keys: &Option<Vec<CodecKey>>) -> Result<Option<Arc<dyn fusee::Codec>>> {
    let Some(keys) = keys else { return Ok(None) };
    let mut list = Vec::new();
    for k in keys {
        let key: [u8; 32] = k.key.as_ref().try_into().map_err(|_| {
            napi::Error::new(napi::Status::InvalidArg, "an AES-256-GCM key is 32 bytes")
        })?;
        list.push((k.id.clone(), key));
    }
    Ok(Some(fusee::aes_gcm(list)))
}

#[napi(object)]
pub struct ResultInfo {
    /// "notfound", "running" or "done".
    pub state: String,
    pub shard: u32,
    pub run: f64,
    pub status: u32,
    pub payload: Option<Buffer>,
}

#[napi(object)]
pub struct DeliveryInfo {
    /// "resolve", "cancel" or "update".
    pub kind: String,
    /// -1 when `seqs` lists operations cancelled together.
    pub seq: i64,
    pub status: u32,
    pub payload: Buffer,
    pub seqs: Vec<u32>,
    /// Updates: the ordinal, name and ID; `token` is -1 for a recorded
    /// update (start its handler) or the request to validate.
    pub ordinal: f64,
    pub name: Option<String>,
    pub id: Option<String>,
    pub token: f64,
}

#[napi(object)]
pub struct EventInfo {
    /// start | wake | stuck | ended | ack | reject | result | reply | refuse
    pub kind: String,
    pub shard: u32,
    pub run: f64,
    pub token: f64,
    pub id: Option<String>,
    pub wtype: Option<String>,
    pub version: u32,
    pub started_ms: f64,
    pub input: Option<Buffer>,
    pub status: u32,
    pub payload: Option<Buffer>,
    pub message: Option<String>,
}

#[napi(object)]
pub struct TickInfo {
    pub events: Vec<EventInfo>,
    pub next_wake_ms: Option<f64>,
}

fn event(kind: &str) -> EventInfo {
    EventInfo {
        kind: kind.into(),
        shard: 0,
        run: 0.0,
        token: 0.0,
        id: None,
        wtype: None,
        version: 0,
        started_ms: 0.0,
        input: None,
        status: 0,
        payload: None,
        message: None,
    }
}

#[napi]
pub struct NativeClient {
    client: fusee_server::Client,
}

#[napi]
pub async fn connect_client(address: String) -> Result<NativeClient> {
    let client = tokio::task::spawn_blocking(move || fusee_server::Client::connect(&address))
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?
        .map_err(|e| err(e.into()))?;
    Ok(NativeClient { client })
}

#[napi]
impl NativeClient {
    #[napi]
    pub async fn call(&self, method: String, args: String) -> Result<String> {
        let client = self.client.clone();
        tokio::task::spawn_blocking(move || {
            let params = serde_json::from_str(&args).map_err(|e| err(e.into()))?;
            client
                .call(&method, &params)
                .map(|v| v.to_string())
                .map_err(err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?
    }

    #[napi]
    pub fn close(&self) -> Result<()> {
        self.client.close().map_err(|e| err(e.into()))
    }
}

#[napi]
pub struct NativeConnection {
    link: Option<ForeignConnection>,
}

#[napi]
#[derive(Default)]
pub struct NativeReplicaMetrics {
    metrics: fusee_server::foreign::ReplicaMetrics,
}
#[napi]
impl NativeReplicaMetrics {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self::default()
    }
    #[napi]
    pub fn snapshot(&self) -> Option<String> {
        self.metrics.snapshot().map(|v| v.to_string())
    }
}

#[napi]
pub fn prepare_connection<'a>(
    env: &'a napi::Env,
    mode: String,
    address: Option<String>,
    id: Option<u8>,
    peers: Option<Vec<String>>,
    dir: Option<String>,
    flush: Option<String>,
    metrics: Option<&NativeReplicaMetrics>,
) -> Result<PromiseRaw<'a, NativeConnection>> {
    let observer = metrics.map(|m| m.metrics.clone());
    env.spawn_future(async move {
        let conn = tokio::task::spawn_blocking(move || {
            ForeignConnection::prepare_with(&mode, address, id, peers, dir, flush, observer)
        })
        .await
        .map_err(|e| napi::Error::from_reason(e.to_string()))?
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(NativeConnection { link: Some(conn) })
    })
}

#[napi]
pub struct NativeEngine {
    engine: Option<ForeignEngine>,
    connection: Option<ForeignConnection>,
    wake_scheduled: Arc<AtomicBool>,
    /// A wake was raised during a call from JavaScript and not sent.
    owed: Arc<AtomicBool>,
    /// The engine asked for a tick since the last one (a flush finished,
    /// another part left mail): a light tick (`IO_LIGHT`) is then full.
    woken: Arc<AtomicBool>,
    /// Queues the threadsafe callback (coalesced with `wake_scheduled`), or
    /// sends the driver's handle.
    send: Arc<dyn Fn() + Send + Sync>,
    driver: Option<driver::Driver>,
    /// The shared scratch array (kept alive by this reference) and its data.
    _io: Float64Array,
    io_ptr: *mut f64,
    io_len: usize,
    /// Step results JavaScript recorded and still holds: their delivery
    /// carries no payload back.
    held: RefCell<HashSet<(u32, RunId, u32)>>,
    events: RefCell<Vec<f64>>,
    /// Step names JavaScript refers to by number ([`Self::step_name`]):
    /// a name converted from a JavaScript string on every step cost an
    /// allocation and a copy each.
    step_names: RefCell<Vec<String>>,
    /// This engine's entry in `ENGINES`, once `slot` gave it one.
    slot: Cell<Option<u32>>,
    /// The HTTP API's calls (`serve`), answered by JavaScript through
    /// `next_call` and `reply_call`.
    relay: Option<fusee::http::Relay>,
}

thread_local! {
    /// The engines open on this thread, by slot. The calls on a step's path
    /// are module functions that take a slot (`engine_*` below) rather than
    /// methods: napi-rs registers every `&self` method call in a
    /// process-wide map behind one mutex, which serializes calls across
    /// worker threads.
    static ENGINES: RefCell<Vec<*const NativeEngine>> = const { RefCell::new(Vec::new()) };
}

/// Runs `f` on the engine in `slot` on this thread.
fn with_engine<T>(slot: u32, f: impl FnOnce(&NativeEngine) -> Result<T>) -> Result<T> {
    let p = ENGINES
        .with(|e| e.borrow().get(slot as usize).copied())
        .filter(|p| !p.is_null())
        .ok_or_else(|| err(fusee::Error::Closed))?;
    // SAFETY: a slot holds the address of a live NativeEngine owned by a
    // JavaScript object on this thread: napi-rs keeps a wrapped value in
    // place for the object's life, and `close` and `Drop` clear the slot.
    // Only `close` and `next_call` take `&mut self`, and neither can run
    // during `f`: no call below enters JavaScript.
    f(unsafe { &*p })
}

/// Marks a call from JavaScript; on the way out, reports an unsent wake.
struct InCall<'a>(&'a NativeEngine);

impl Drop for InCall<'_> {
    fn drop(&mut self) {
        IN_CALL.with(|c| c.set(c.get() - 1));
        if self.0.owed.swap(false, Ordering::AcqRel) {
            self.0.io()[IO_OWED] = 1.0;
        }
    }
}

/// Extra values of a tick's events (strings and payloads).
enum Extra {
    Text(String),
    Bytes(Payload),
    /// A shared name, made a JS string without a copy first.
    Name(Name),
    /// A workflow type: one JS string per type and tick, however many runs
    /// of it start (a mass wake starts thousands of one type a tick).
    Type(Name),
    /// Nothing (`null`).
    None,
}

/// Payload codec descriptor plus a memory runtime for replay testing.
#[napi]
pub struct NativeReplay {
    rt: Option<ShardRuntime<Box<dyn fusee::env::Env>>>,
}

fn delivery(d: Option<Delivery>) -> Option<DeliveryInfo> {
    let base = |kind: &str| DeliveryInfo {
        kind: kind.into(),
        seq: -1,
        status: 0,
        payload: Buffer::from(Vec::new()),
        seqs: Vec::new(),
        ordinal: 0.0,
        name: None,
        id: None,
        token: -1.0,
    };
    match d? {
        Delivery::Resolve { seq, outcome } => Some(DeliveryInfo {
            seq: i64::from(seq),
            status: outcome.status as u32,
            payload: Buffer::from(outcome.payload.to_vec()),
            ..base("resolve")
        }),
        Delivery::CancelAll { seqs, reason } => Some(DeliveryInfo {
            status: Status::Cancelled as u32,
            payload: Buffer::from(reason.to_vec()),
            seqs,
            ..base("cancel")
        }),
        Delivery::Update {
            ordinal,
            name,
            id,
            payload,
            token,
        } => Some(DeliveryInfo {
            payload: Buffer::from(payload.to_vec()),
            ordinal: ordinal as f64,
            name: Some(name),
            id: Some(id),
            token: token.map(|t| t as f64).unwrap_or(-1.0),
            ..base("update")
        }),
    }
}

fn summary_json(s: &fusee::RunSummary) -> serde_json::Value {
    let attrs: serde_json::Value =
        serde_json::from_str(&s.attributes).unwrap_or(serde_json::Value::Null);
    serde_json::json!({"id": s.id, "type": s.wtype, "version": s.version, "status": s.status,
        "startedMs": s.started_ms, "finishedMs": s.finished_ms, "error": s.error,
        "shard": s.shard, "run": s.run, "attributes": attrs, "object": s.object})
}

#[napi]
impl NativeReplay {
    /// A memory engine where step functions never run and timers never
    /// fire: code can only go as far as an imported journal takes it.
    #[napi(factory)]
    pub fn open(codec_keys: Option<Vec<CodecKey>>) -> Result<Self> {
        let cfg = RuntimeConfig {
            replay_only: true,
            park_after_ms: u64::MAX,
            ..Default::default()
        };
        let services = Services {
            codec: codec(&codec_keys)?,
            ..Default::default()
        };
        let rt = ShardRuntime::open_with(
            Box::new(fusee::local::MemoryEnv::default()) as Box<dyn fusee::env::Env>,
            cfg,
            fusee::runtime::builtin_registry(),
            true,
            services,
            Arc::new(|| {}),
        )
        .map_err(err)?;
        Ok(Self { rt: Some(rt) })
    }

    fn rt(&self) -> Result<&ShardRuntime<Box<dyn fusee::env::Env>>> {
        self.rt.as_ref().ok_or_else(|| err(fusee::Error::Closed))
    }

    /// Imports an exported journal file's bytes; returns its header as JSON
    /// with the new run's number.
    #[napi]
    pub fn import(&self, file: Buffer) -> Result<String> {
        let j = fusee::export::ExportedJournal::decode(&file).map_err(err)?;
        let complete = j.is_complete();
        let run = self.rt()?.import(&j.records).map_err(err)?;
        Ok(serde_json::json!({"run": run, "workflowId": j.workflow_id, "workflowType": j.workflow_type,
            "version": j.version, "complete": complete})
        .to_string())
    }

    #[napi]
    pub fn tick(&self) -> Result<TickInfo> {
        let rt = self.rt()?;
        rt.tick();
        let events = rt
            .take_foreign()
            .into_iter()
            .map(|e| foreign_event(0, e))
            .collect();
        Ok(TickInfo {
            events,
            next_wake_ms: None,
        })
    }

    /// `[stuck message or "", replaying, liveWork]`
    #[napi]
    pub fn state(&self, run: f64) -> Result<Vec<String>> {
        let rt = self.rt()?;
        let stuck = rt.stuck_reason(run as RunId).unwrap_or_default();
        let (r, l) = rt.code_state(run as RunId).unwrap_or((false, false));
        let finished = rt
            .summary(run as RunId)
            .is_some_and(|s| s.finished_ms.is_some());
        Ok(vec![
            stuck,
            r.to_string(),
            l.to_string(),
            finished.to_string(),
        ])
    }

    #[napi]
    pub fn host_call(
        &self,
        op: String,
        run: f64,
        args: Vec<String>,
        bufs: Vec<Buffer>,
    ) -> Result<Vec<f64>> {
        host_call(self.rt()?.host().as_ref(), op, run, args, bufs)
    }

    #[napi]
    pub fn host_bytes(
        &self,
        op: String,
        run: f64,
        args: Vec<String>,
        bufs: Vec<Buffer>,
    ) -> Result<Option<Buffer>> {
        host_bytes(self.rt()?.host().as_ref(), op, run, args, bufs)
    }

    #[napi]
    pub fn quiescent(&self, run: f64) -> Result<Option<DeliveryInfo>> {
        Ok(delivery(self.rt()?.quiescent(run as RunId).map_err(err)?))
    }

    #[napi]
    pub fn decide_update(&self, token: f64, ok: bool, message: String) -> Result<f64> {
        let d = if ok { Ok(()) } else { Err(message) };
        Ok(self
            .rt()?
            .decide_update(token as u64, d)
            .map_err(err)?
            .map(|o| o as f64)
            .unwrap_or(-1.0))
    }

    #[napi]
    pub fn finish(&self, run: f64, status: u32, value: Buffer) -> Result<()> {
        let s = status_from(status);
        let p = payload(&value);
        let outcome = if s == Status::Ok { Ok(p) } else { Err((s, p)) };
        self.rt()?.foreign_finish(run as RunId, outcome);
        Ok(())
    }

    #[napi]
    pub fn close(&mut self) {
        self.rt = None;
    }
}

fn foreign_event(shard: u32, e: fusee::runtime::ForeignEvent) -> EventInfo {
    use fusee::runtime::ForeignEvent;
    match e {
        ForeignEvent::Start { run, info } => EventInfo {
            shard,
            run: run as f64,
            id: Some(info.id.to_string()),
            wtype: Some(info.wtype.to_string()),
            version: info.version,
            started_ms: info.started_ms as f64,
            input: Some(Buffer::from(info.input.to_vec())),
            ..event("start")
        },
        ForeignEvent::Wake { run } => EventInfo {
            shard,
            run: run as f64,
            ..event("wake")
        },
        ForeignEvent::Stuck { run, message } => EventInfo {
            shard,
            run: run as f64,
            message: Some(message),
            ..event("stuck")
        },
        ForeignEvent::Ended { run } => EventInfo {
            shard,
            run: run as f64,
            ..event("ended")
        },
    }
}

fn arg_u64(args: &[String], i: usize) -> u64 {
    args.get(i).and_then(|s| s.parse().ok()).unwrap_or(0)
}

fn arg(args: &[String], i: usize) -> &str {
    args.get(i).map(String::as_str).unwrap_or("")
}

/// A stream operation's writer from `args[i]`: a step's position, or
/// anything else (`"w"`) for workflow code.
fn step_arg(args: &[String], i: usize) -> Option<u32> {
    args.get(i).and_then(|s| s.parse().ok())
}

/// Payloads in one buffer: each a 4-byte little-endian length, then it.
pub(crate) fn framed(payloads: &[Payload]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payloads.iter().map(|p| p.len() + 4).sum());
    for p in payloads {
        out.extend_from_slice(&(p.len() as u32).to_le_bytes());
        out.extend_from_slice(p);
    }
    out
}

/// A step's place among a limit's waiters from `args[at..at + 3]`: level,
/// fairness key and weight (see `fusee::limits`).
fn priority(args: &[String], at: usize) -> fusee::Priority {
    let level: i64 = args.get(at).and_then(|s| s.parse().ok()).unwrap_or(0);
    fusee::Priority {
        level: level.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32,
        fairness_key: arg(args, at + 1).to_owned(),
        fairness_weight: arg_u64(args, at + 2).min(u64::from(u32::MAX)) as u32,
    }
}

/// One debounced start or batched event as the update-with-start that
/// carries it (`fusee::queue::request`): `kind` is "debounce" (`a` the
/// window, `b` the longest wait, in ms) or "batch" (`a` the most events,
/// `b` the longest wait); `c` is the dedup window. Absent numbers take the
/// Rust defaults. Returns `[coordinator type, coordinator ID, start input,
/// update argument]` as JSON text; a rule the coordinator would refuse is
/// rejected here.
#[allow(clippy::too_many_arguments)]
fn queue_event(
    kind: &str,
    workflow: &str,
    key: &str,
    event_id: &str,
    input: &str,
    a: Option<f64>,
    b: Option<f64>,
    c: Option<f64>,
) -> std::result::Result<[String; 4], fusee::Error> {
    use fusee::queue::{Batch, Debounce, Event, Rule};
    let ms = |v: Option<f64>, default: u64| v.map(|v| v.max(0.0) as u64).unwrap_or(default);
    let rule = match kind {
        "debounce" => {
            let d = Debounce::default();
            Rule::Debounce(Debounce {
                window_ms: ms(a, d.window_ms),
                max_wait_ms: ms(b, d.max_wait_ms),
                dedup_ms: ms(c, d.dedup_ms),
            })
        }
        "batch" => {
            let d = Batch::default();
            Rule::Batch(Batch {
                max_events: a
                    .map(|v| v.clamp(0.0, f64::from(u32::MAX)) as u32)
                    .unwrap_or(d.max_events),
                max_wait_ms: ms(b, d.max_wait_ms),
                dedup_ms: ms(c, d.dedup_ms),
            })
        }
        other => return Err(fusee::Error::Rejected(format!("no queue rule '{other}'"))),
    };
    rule.validate()?;
    let event = Event {
        id: event_id.to_owned(),
        rule,
        input: serde_json::from_str(input)?,
    };
    let (wtype, id, state, arg) = fusee::queue::request(workflow, key, &event);
    Ok([wtype.to_owned(), id, state.to_string(), arg.to_string()])
}

fn begin_step(host: &dyn fusee::runtime::Host, run: RunId, name: &str) -> Result<Vec<f64>> {
    let (seq, start, gate) = host.begin_step(run, name).map_err(err)?;
    Ok(match start {
        StepStart::Replay => vec![f64::from(seq), 1.0, gate as f64, 0.0, 0.0],
        StepStart::Run {
            attempt,
            not_before_ms,
        } => vec![
            f64::from(seq),
            0.0,
            gate as f64,
            f64::from(attempt),
            not_before_ms as f64,
        ],
    })
}

/// The run-scoped operations workflow code makes, shared by the engine and
/// the replayer: `op` names the operation, `args` and `bufs` carry its
/// parameters. Returns numbers.
fn host_call(
    host: &dyn fusee::runtime::Host,
    op: String,
    run: f64,
    args: Vec<String>,
    bufs: Vec<Buffer>,
) -> Result<Vec<f64>> {
    let run = run as RunId;
    let buf = |i: usize| -> Payload {
        bufs.get(i)
            .map(|b| payload(b))
            .unwrap_or_else(|| Arc::from(&[][..]))
    };
    Ok(match op.as_str() {
        "beginStep" => begin_step(host, run, arg(&args, 0))?,
        "beginAwait" => {
            let (seq, start, gate) = host.begin_await(run, arg(&args, 0)).map_err(err)?;
            match start {
                AwaitStart::Replay => vec![f64::from(seq), 1.0, gate as f64, 0.0, 0.0],
                AwaitStart::Waiting { attempt } => {
                    vec![f64::from(seq), 2.0, gate as f64, f64::from(attempt), 0.0]
                }
                AwaitStart::Run {
                    attempt,
                    not_before_ms,
                } => vec![
                    f64::from(seq),
                    0.0,
                    gate as f64,
                    f64::from(attempt),
                    not_before_ms as f64,
                ],
            }
        }
        "mayRun" => vec![f64::from(u8::from(host.may_run(
            run,
            arg_u64(&args, 0),
            arg_u64(&args, 1),
            Waker::noop(),
        )))],
        "stepDone" => {
            let status = if arg(&args, 1) == "1" {
                Status::Ok
            } else {
                Status::Failed
            };
            host.step_done(run, arg_u64(&args, 0) as u32, status, buf(0));
            vec![]
        }
        "stepRetry" => vec![
            host.step_retry(run, arg_u64(&args, 0) as u32, arg_u64(&args, 1), buf(0))
                .map_err(err)? as f64,
        ],
        "stepPaused" => vec![f64::from(u8::from(
            host.step_paused(run, arg_u64(&args, 0) as u32),
        ))],
        "beginSleep" => vec![f64::from(
            host.begin_sleep(run, arg(&args, 0), arg_u64(&args, 1))
                .map_err(err)?,
        )],
        "beginWait" => vec![f64::from(
            host.begin_wait(run, arg(&args, 0), arg_u64(&args, 1))
                .map_err(err)?,
        )],
        "beginChild" => vec![
            host.begin_child(
                run,
                arg(&args, 0),
                arg(&args, 1),
                arg_u64(&args, 2) as u8,
                arg_u64(&args, 3),
                buf(0),
            )
            .map_err(err)?
            .map(f64::from)
            .unwrap_or(-1.0),
        ],
        "send" => {
            host.send(run, arg(&args, 0), arg(&args, 1), buf(0))
                .map_err(err)?;
            vec![]
        }
        "awaiting" => {
            host.awaiting(run, arg_u64(&args, 0) as u32).map_err(err)?;
            vec![]
        }
        "heartbeat" => {
            host.heartbeat(run, arg_u64(&args, 0) as u32, buf(0))
                .map_err(err)?;
            vec![]
        }
        "patched" => vec![f64::from(u8::from(
            host.patched(run, arg(&args, 0), arg(&args, 1) == "1")
                .map_err(err)?,
        ))],
        "setAttributes" => {
            host.set_attributes(run, buf(0)).map_err(err)?;
            vec![]
        }
        "updateDone" => {
            let s = status_from(arg_u64(&args, 1) as u32);
            let outcome = if s == Status::Ok {
                Ok(buf(0))
            } else {
                Err((s, buf(0)))
            };
            host.update_done(run, arg_u64(&args, 0), outcome)
                .map_err(err)?;
            vec![]
        }
        "continueAsNew" => {
            let v = arg_u64(&args, 0);
            host.continue_as_new(run, buf(0), (v > 0).then_some(v as u32))
                .map_err(err)?;
            vec![]
        }
        "journalSize" => vec![f64::from(host.journal_size(run))],
        "cancelWorkflow" => {
            host.cancel_workflow(run, arg(&args, 0), arg(&args, 1))
                .map_err(err)?;
            vec![]
        }
        "forget" => {
            host.forget(run, arg_u64(&args, 0) as u32);
            vec![]
        }
        "limitTicket" => vec![host.limit_ticket() as f64],
        // `[limit, ticket, level, fairness key, weight]`; the last three are
        // the step's priority (absent: the default, arrival order).
        "acquire" => match host.acquire_in(
            run,
            arg(&args, 0),
            arg_u64(&args, 1),
            &priority(&args, 2),
            None,
        ) {
            Acquire::Granted => vec![0.0],
            Acquire::Wait { retry_at_ms } => vec![retry_at_ms.map(|t| t as f64).unwrap_or(-1.0)],
        },
        "release" => {
            host.release(arg(&args, 0), arg_u64(&args, 1));
            vec![]
        }
        "wakeAt" => {
            host.wake_run_at(run, arg_u64(&args, 0));
            vec![]
        }
        // `[name, step]`: `[chunks the step recorded before, closed]`.
        "streamOpen" => {
            let o = host
                .stream_open(run, arg(&args, 0), step_arg(&args, 1))
                .map_err(err)?;
            vec![o.recorded as f64, f64::from(u8::from(o.closed))]
        }
        // `[name, step]`, the chunk's tagged payload.
        "streamAppend" => {
            host.stream_append(run, arg(&args, 0), step_arg(&args, 1), buf(0))
                .map_err(err)?;
            vec![]
        }
        "streamClose" => {
            host.stream_close(run, arg(&args, 0), step_arg(&args, 1))
                .map_err(err)?;
            vec![]
        }
        // `[key, "1" to set the value in bufs[0] | "0" to clear]`.
        "objectSet" => {
            let value = (arg(&args, 1) == "1").then(|| buf(0));
            host.object_set(run, arg(&args, 0), value).map_err(err)?;
            vec![]
        }
        // `[type, key, handler, "1" one-way]`, the argument: `[seq]`, -1
        // for a one-way call.
        "beginCall" => vec![
            host.begin_call(
                run,
                arg(&args, 0),
                arg(&args, 1),
                arg(&args, 2),
                buf(0),
                arg(&args, 3) == "1",
            )
            .map_err(err)?
            .map(f64::from)
            .unwrap_or(-1.0),
        ],
        other => {
            return Err(napi::Error::new(
                napi::Status::InvalidArg,
                format!("unknown host operation '{other}'"),
            ));
        }
    })
}

/// `ctx.now()`: the time the run recorded at this point, or `live_ms`,
/// recorded now (the `value` operation of kind 0, as `host_bytes` makes it,
/// without the buffers on either side).
fn value_now(host: &dyn fusee::runtime::Host, run: f64, live_ms: f64) -> Result<f64> {
    let live = (live_ms as u64).to_le_bytes();
    let v = host
        .value(run as RunId, 0, &mut |_, _| Arc::from(&live[..]))
        .map_err(err)?;
    let b: [u8; 8] = v
        .get(..8)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| napi::Error::from_reason("a recorded time is not 8 bytes"))?;
    Ok(u64::from_le_bytes(b) as f64)
}

/// Host operations that return bytes or text.
fn host_bytes(
    host: &dyn fusee::runtime::Host,
    op: String,
    run: f64,
    args: Vec<String>,
    bufs: Vec<Buffer>,
) -> Result<Option<Buffer>> {
    let run = run as RunId;
    Ok(match op.as_str() {
        "value" => {
            let c = bufs
                .first()
                .map(|b| payload(b))
                .unwrap_or_else(|| Arc::from(&[][..]));
            let v = host
                .value(run, arg_u64(&args, 0) as u8, &mut |_, _| c.clone())
                .map_err(err)?;
            Some(Buffer::from(v.to_vec()))
        }
        "key" => Some(Buffer::from(
            host.key(run, arg_u64(&args, 0) as u32).into_bytes(),
        )),
        "token" => Some(Buffer::from(
            host.completion_token(run, arg_u64(&args, 0) as u32)
                .map_err(err)?
                .into_bytes(),
        )),
        "progress" => host
            .progress(run, arg_u64(&args, 0) as u32)
            .map(|p| Buffer::from(p.to_vec())),
        // `[key, "1" exclusive]`: the value's JSON, or null when absent.
        "objectGet" => host
            .object_get(run, arg(&args, 0), arg(&args, 1) == "1")
            .map_err(err)?
            .map(|p| Buffer::from(p.to_vec())),
        // `["1" exclusive]`: the keys, a JSON array.
        "objectKeys" => {
            let keys = host.object_keys(run, arg(&args, 0) == "1").map_err(err)?;
            Some(Buffer::from(
                serde_json::to_vec(&keys).map_err(|e| err(e.into()))?,
            ))
        }
        // The committed state, a JSON object.
        "objectState" => Some(Buffer::from(host.object_state(run).map_err(err)?.to_vec())),
        // The edit's JSON in bufs[0]: applies it as the running edit
        // handler's writes; the answer.
        "objectEdit" => {
            let edit = bufs.first().map(|b| b.to_vec()).unwrap_or_default();
            Some(Buffer::from(
                host.object_edit(run, &edit).map_err(err)?.to_vec(),
            ))
        }
        // `[name, step, limit]`: the chunks, framed (see `framed`).
        "streamRecorded" => {
            let chunks = host
                .stream_recorded(
                    run,
                    arg(&args, 0),
                    arg_u64(&args, 1) as u32,
                    arg_u64(&args, 2),
                )
                .map_err(err)?;
            Some(Buffer::from(framed(&chunks)))
        }
        other => {
            return Err(napi::Error::new(
                napi::Status::InvalidArg,
                format!("unknown host operation '{other}'"),
            ));
        }
    })
}

impl NativeEngine {
    fn e(&self) -> Result<&ForeignEngine> {
        if let Some(c) = &self.connection {
            c.check().map_err(|e| err(e.into()))?;
        }
        self.engine
            .as_ref()
            .ok_or_else(|| err(fusee::Error::Closed))
    }

    fn enter(&self) -> InCall<'_> {
        IN_CALL.with(|c| c.set(c.get() + 1));
        InCall(self)
    }

    #[allow(clippy::mut_from_ref)]
    fn io(&self) -> &mut [f64] {
        // SAFETY: the array lives as long as `_io`; only the JavaScript
        // thread calls in, and no two of these slices are alive at once.
        unsafe { std::slice::from_raw_parts_mut(self.io_ptr, self.io_len) }
    }

    fn note_commit(&self, e: &ForeignEngine, shard: u32) {
        if (shard as usize) < MAX_COMMIT_SHARDS
            && let Some(c) = e.commit(shard)
        {
            self.io()[IO_COMMIT + shard as usize] = c as f64;
        }
    }

    fn drop_held(&self, shard: u32, run: RunId) {
        let mut held = self.held.borrow_mut();
        if !held.is_empty() {
            held.retain(|&(s, r, _)| s != shard || r != run);
        }
    }

    fn gate(
        &self,
        e: &ForeignEngine,
        shard: u32,
        run: RunId,
        gate: u64,
        not_before: u64,
        inline: bool,
    ) -> Result<bool> {
        let open = e
            .may_run_step(shard, run, gate, not_before, inline)
            .map_err(err)?;
        self.note_commit(e, shard);
        Ok(open)
    }
}

#[napi]
impl NativeEngine {
    /// Opens the engine. `notify` is called (from any thread) when the
    /// engine wants `tick()` to run, for example after a flush completed.
    #[napi(factory)]
    pub fn open(
        env: Env,
        opts: OpenOptions,
        notify: Function<(), ()>,
        prepared: Option<&mut NativeConnection>,
        io: Float64Array,
    ) -> Result<Self> {
        if io.len() < IO_EV + EV_STRIDE {
            return Err(napi::Error::new(
                napi::Status::InvalidArg,
                "the shared array is too short",
            ));
        }
        let connection = prepared
            .map(|p| p.link.take().ok_or_else(|| err(fusee::Error::Closed)))
            .transpose()?;
        enum Source {
            Config(Box<Config>),
            Part(Arc<Group>, u32),
        }
        let source = match opts.group {
            Some(id) => {
                let group = groups().get(&(id as u64)).cloned().ok_or_else(|| {
                    napi::Error::from_reason(format!("no engine group {id} in this process"))
                })?;
                Source::Part(group, opts.worker.unwrap_or(0))
            }
            None => Source::Config(Box::new(config(&opts, connection.as_ref(), false)?)),
        };
        let mut io = io;
        // SAFETY: the array is not shared with another thread.
        let io_ptr = unsafe { io.as_mut().as_mut_ptr() };
        let io_len = io.len();
        let driver = match opts.driver.as_deref() {
            // SAFETY: on the JS thread; the shared array outlives the driver
            // (both belong to this engine).
            Some("native") => unsafe { driver::Driver::start(env.raw(), notify.raw(), io_ptr) },
            _ => None,
        };
        let tsfn = notify
            .build_threadsafe_function::<()>()
            .weak::<true>()
            .build()?;
        let wake_scheduled = Arc::new(AtomicBool::new(false));
        let owed = Arc::new(AtomicBool::new(false));
        let pending = wake_scheduled.clone();
        let handle = driver.as_ref().map(|d| d.sender.clone());
        let send: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            if let Some(h) = &handle {
                h.send();
                return;
            }
            if !pending.swap(true, Ordering::AcqRel)
                && tsfn.call((), ThreadsafeFunctionCallMode::NonBlocking) != napi::Status::Ok
            {
                pending.store(false, Ordering::Release);
            }
        });
        let unsent = owed.clone();
        let woken = Arc::new(AtomicBool::new(false));
        let rang = woken.clone();
        let sender = send.clone();
        // This thread's call counter, by address: wakes raised on other
        // threads (flushes, other workers of a group) are always sent.
        let home = IN_CALL.with(|c| c as *const Cell<u32> as usize);
        let notify: fusee::env::Notify = Arc::new(move || {
            // Inside a call from JavaScript on this engine's thread: the SDK
            // looks again when the call returns (see `InCall`).
            if IN_CALL
                .try_with(|c| c as *const Cell<u32> as usize == home && c.get() > 0)
                .unwrap_or(false)
            {
                unsent.store(true, Ordering::Release);
                return;
            }
            rang.store(true, Ordering::Release);
            sender();
        });
        if let Some(c) = &connection {
            c.set_notify(notify.clone());
        }
        let engine = match source {
            Source::Config(cfg) => ForeignEngine::open(&cfg, notify),
            Source::Part(group, worker) => ForeignEngine::open_part(group, worker, notify),
        }
        .map_err(err)?;
        Ok(Self {
            engine: Some(engine),
            connection,
            wake_scheduled,
            owed,
            woken,
            send,
            driver,
            _io: io,
            io_ptr,
            io_len,
            held: RefCell::new(HashSet::new()),
            events: RefCell::new(Vec::new()),
            step_names: RefCell::new(Vec::new()),
            slot: Cell::new(None),
            relay: None,
        })
    }

    /// This engine's slot for the `engine*` module functions (the calls on
    /// a step's path), on this thread.
    #[napi]
    pub fn slot(&self) -> u32 {
        if let Some(s) = self.slot.get() {
            return s;
        }
        let me = self as *const NativeEngine;
        let s = ENGINES.with(|e| {
            let mut e = e.borrow_mut();
            match e.iter().position(|p| p.is_null()) {
                Some(free) => {
                    e[free] = me;
                    free
                }
                None => {
                    e.push(me);
                    e.len() - 1
                }
            }
        }) as u32;
        self.slot.set(Some(s));
        s
    }

    fn release_slot(&self) {
        if let Some(s) = self.slot.take() {
            let _ = ENGINES.try_with(|e| {
                if let Some(p) = e.borrow_mut().get_mut(s as usize) {
                    *p = std::ptr::null();
                }
            });
        }
    }

    #[napi]
    pub fn shards(&self) -> Result<u32> {
        Ok(self.e()?.shards())
    }

    /// Marks `name` as an object type with these shared handlers (see
    /// `fusee::object`).
    #[napi]
    pub fn register_object(&self, name: String, shared: Vec<String>) -> Result<()> {
        self.e()?.register_object(&name, &shared).map_err(err)
    }

    /// Whether this engine holds a workflow ID's shard (always, unless it
    /// is one worker's part of a group).
    pub fn owns_id(&self, id: String) -> Result<bool> {
        let e = self.e()?;
        Ok(e.owns(e.shard_of(&id)))
    }

    /// Asks for the notify callback, as a background wake does (coalesced
    /// until the next tick).
    pub fn wake(&self) {
        (self.send)();
    }

    /// Whether a native driver runs the turns (`driver: "native"` and
    /// libuv was found).
    #[napi]
    pub fn driving(&self) -> bool {
        self.driver.is_some()
    }

    /// Returns 0 if a run was created, 1 if one already existed. `policy`
    /// is the ID policy byte; with `updateName`, the update is delivered in
    /// the start record (or to the running workflow) and answered on
    /// `updateToken`.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        &self,
        id: String,
        wtype: String,
        version: u32,
        input: Either<String, Uint8Array>,
        deadline_ms: f64,
        token: f64,
        policy: u32,
        update_name: Option<String>,
        update_id: Option<String>,
        update_arg: Option<Either<String, Uint8Array>>,
        update_token: Option<f64>,
        trace: Option<String>,
    ) -> Result<u32> {
        let _g = self.enter();
        let mut req = StartRequest::new(&id, &wtype, wire_payload(&input), Default::default());
        req.version = Some(version.max(1));
        req.deadline_ms = deadline_ms as u64;
        req.policy = policy as u8;
        req.token = (token >= 0.0).then_some(token as u64);
        req.trace = trace
            .filter(|t| !t.is_empty())
            .map(|t| Arc::from(t.into_bytes()));
        if let Some(name) = update_name {
            req.update = Some(UpdateRequest {
                name,
                id: update_id.unwrap_or_default(),
                payload: update_arg
                    .as_ref()
                    .map(wire_payload)
                    .unwrap_or_else(|| Arc::from(&b"null"[..])),
                token: update_token.map(|t| t as u64),
            });
        }
        match self.e()?.start(req).map_err(err)? {
            (_, StartOutcome::Created { .. }) => Ok(0),
            (_, StartOutcome::Existing { .. }) => Ok(1),
        }
    }

    /// `by`: who asked (audit), recorded with the signal.
    #[napi]
    pub fn signal(
        &self,
        id: String,
        name: String,
        value: Either<String, Uint8Array>,
        token: f64,
        by: Option<String>,
    ) -> Result<()> {
        let _g = self.enter();
        let e = self.e()?;
        e.audited(&id, by.as_deref(), || {
            e.signal(&id, &name, wire_payload(&value), 0, Some(token as u64))
        })
        .map_err(err)
    }

    #[napi]
    pub fn cancel(&self, id: String, reason: String, token: f64, by: Option<String>) -> Result<()> {
        let _g = self.enter();
        let e = self.e()?;
        e.audited(&id, by.as_deref(), || {
            e.cancel(&id, &reason, Some(token as u64))
        })
        .map_err(err)
    }

    #[napi]
    pub fn resume(&self, id: String, token: f64, by: Option<String>) -> Result<()> {
        let _g = self.enter();
        let e = self.e()?;
        e.audited(&id, by.as_deref(), || e.resume(&id, Some(token as u64)))
            .map_err(err)
    }

    #[napi]
    pub fn terminate(
        &self,
        id: String,
        reason: String,
        token: f64,
        by: Option<String>,
    ) -> Result<()> {
        let _g = self.enter();
        let e = self.e()?;
        e.audited(&id, by.as_deref(), || {
            e.terminate(&id, &reason, Some(token as u64))
        })
        .map_err(err)
    }

    #[napi]
    pub fn result(&self, id: String) -> Result<ResultInfo> {
        let (shard, state) = self.e()?.result(&id).map_err(err)?;
        Ok(match state {
            ResultState::NotFound => ResultInfo {
                state: "notfound".into(),
                shard,
                run: 0.0,
                status: 0,
                payload: None,
            },
            ResultState::Running(run) => ResultInfo {
                state: "running".into(),
                shard,
                run: run as f64,
                status: 0,
                payload: None,
            },
            ResultState::Done {
                run,
                status,
                payload,
            } => ResultInfo {
                state: "done".into(),
                shard,
                run: run as f64,
                status: status as u32,
                payload: Some(Buffer::from(payload.to_vec())),
            },
        })
    }

    /// The run's summary and journal as JSON (or null).
    #[napi]
    pub fn inspect(&self, id: String) -> Result<Option<String>> {
        Ok(self
            .e()?
            .inspect(&id)
            .map_err(err)?
            .map(|d| detail_json(&d)))
    }

    #[napi]
    pub fn list(&self, limit: u32) -> Result<String> {
        let runs: Vec<serde_json::Value> = self
            .e()?
            .list(limit as usize)
            .into_iter()
            .map(|s| summary_json(&s))
            .collect();
        Ok(serde_json::Value::Array(runs).to_string())
    }

    #[napi]
    pub fn shard_metrics(&self) -> Result<String> {
        serde_json::to_string(&self.e()?.shard_metrics()).map_err(|e| err(e.into()))
    }

    /// Begins a step and, for one that runs, checks its gate (gate 0 for a
    /// read-only step): writes `[seq, replay, gate, attempt, notBefore]` to
    /// the shared array and returns 2 to replay, 1 to run now, 0 to wait.
    pub fn begin_step(&self, shard: u32, run: f64, name: String, read_only: bool) -> Result<u32> {
        self.begin_step_str(shard, run, &name, read_only)
    }

    /// Keeps a step name for [`Self::begin_step_named`]; returns its number.
    #[napi]
    pub fn step_name(&self, name: String) -> u32 {
        let mut names = self.step_names.borrow_mut();
        names.push(name);
        (names.len() - 1) as u32
    }

    /// [`Self::begin_step`] with a name kept by [`Self::step_name`].
    pub fn begin_step_named(
        &self,
        shard: u32,
        run: f64,
        name: u32,
        read_only: bool,
    ) -> Result<u32> {
        let names = self.step_names.borrow();
        let name = names.get(name as usize).ok_or_else(|| {
            err(fusee::Error::Rejected(format!(
                "no step name number {name}"
            )))
        })?;
        self.begin_step_str(shard, run, name, read_only)
    }

    fn begin_step_str(&self, shard: u32, run: f64, name: &str, read_only: bool) -> Result<u32> {
        let _g = self.enter();
        let e = self.e()?;
        let run = run as RunId;
        let (seq, start, gate) = e
            .with_host(shard, |host| host.begin_step(run, name))
            .map_err(err)?
            .map_err(err)?;
        let io = self.io();
        io[0] = f64::from(seq);
        io[2] = gate as f64;
        match start {
            StepStart::Replay => {
                io[1] = 1.0;
                io[3] = 0.0;
                io[4] = 0.0;
                Ok(2)
            }
            StepStart::Run {
                attempt,
                not_before_ms,
            } => {
                io[1] = 0.0;
                io[3] = f64::from(attempt);
                io[4] = not_before_ms as f64;
                let gate = if read_only { 0 } else { gate };
                Ok(u32::from(self.gate(
                    e,
                    shard,
                    run,
                    gate,
                    not_before_ms,
                    true,
                )?))
            }
        }
    }

    pub fn step_gate(
        &self,
        shard: u32,
        run: f64,
        gate: f64,
        not_before: f64,
        inline: Option<bool>,
    ) -> Result<bool> {
        let _g = self.enter();
        let e = self.e()?;
        self.gate(
            e,
            shard,
            run as RunId,
            gate as u64,
            not_before as u64,
            inline.unwrap_or(false),
        )
    }

    /// Records a step's JSON result (or failure text). JavaScript keeps a
    /// successful result, so its delivery does not send it back.
    pub fn finish_step(
        &self,
        shard: u32,
        run: f64,
        seq: u32,
        ok: bool,
        value: String,
    ) -> Result<()> {
        let _g = self.enter();
        let run = run as RunId;
        self.e()?
            .with_host(shard, |host| {
                host.step_done(
                    run,
                    seq,
                    if ok { Status::Ok } else { Status::Failed },
                    Arc::from(value.as_bytes()),
                )
            })
            .map_err(err)?;
        if ok {
            self.held.borrow_mut().insert((shard, run, seq));
        }
        Ok(())
    }

    /// Records a step result that is bytes, without JSON.
    pub fn finish_step_bytes(
        &self,
        shard: u32,
        run: f64,
        seq: u32,
        value: Uint8Array,
    ) -> Result<()> {
        let _g = self.enter();
        self.e()?
            .with_host(shard, |host| {
                host.step_done(run as RunId, seq, Status::Ok, raw_payload(&value))
            })
            .map_err(err)
    }

    /// A run-scoped operation of workflow code (see `host_call`).
    pub fn host_call(
        &self,
        shard: u32,
        op: String,
        run: f64,
        args: Vec<String>,
        bufs: Vec<Buffer>,
    ) -> Result<Vec<f64>> {
        let _g = self.enter();
        self.e()?
            .with_host(shard, |host| host_call(host, op, run, args, bufs))
            .map_err(err)?
    }

    pub fn host_bytes(
        &self,
        shard: u32,
        op: String,
        run: f64,
        args: Vec<String>,
        bufs: Vec<Buffer>,
    ) -> Result<Option<Buffer>> {
        let _g = self.enter();
        self.e()?
            .with_host(shard, |host| host_bytes(host, op, run, args, bufs))
            .map_err(err)?
    }

    /// `ctx.now()` (see `value_now`).
    pub fn value_now(&self, shard: u32, run: f64, live_ms: f64) -> Result<f64> {
        let _g = self.enter();
        self.e()?
            .with_host(shard, |host| value_now(host, run, live_ms))
            .map_err(err)?
    }

    /// `ctx.sleep`: begins sleep `name` until `at_ms`; returns its sequence
    /// number (the `beginSleep` operation without its string arguments).
    pub fn begin_sleep(&self, shard: u32, run: f64, name: String, at_ms: f64) -> Result<u32> {
        let _g = self.enter();
        self.e()?
            .with_host(shard, |host| {
                host.begin_sleep(run as RunId, &name, at_ms as u64)
                    .map_err(err)
            })
            .map_err(err)?
    }

    #[napi]
    pub fn update(
        &self,
        id: String,
        name: String,
        update_id: String,
        arg: Either<String, Uint8Array>,
        token: f64,
    ) -> Result<()> {
        let _g = self.enter();
        self.e()?
            .update(
                &id,
                &name,
                &update_id,
                wire_payload(&arg),
                Some(token as u64),
            )
            .map_err(err)
    }

    /// The validator's decision; returns the ordinal or -1.
    #[napi]
    pub fn decide_update(&self, shard: u32, token: f64, ok: bool, message: String) -> Result<f64> {
        let d = if ok { Ok(()) } else { Err(message) };
        Ok(self
            .e()?
            .decide_update(shard, token as u64, d)
            .map_err(err)?
            .map(|o| o as f64)
            .unwrap_or(-1.0))
    }

    /// `[shard, run, lsn, foreign]` for answering a query.
    #[napi]
    pub fn query_point(&self, id: String) -> Result<Vec<f64>> {
        let (shard, run, lsn, foreign) = self.e()?.query_route(&id).map_err(err)?;
        Ok(vec![
            f64::from(shard),
            run as f64,
            lsn as f64,
            f64::from(foreign),
        ])
    }

    #[napi]
    pub fn ack_at(&self, shard: u32, lsn: f64, token: f64) -> Result<()> {
        self.e()?
            .ack_at(shard, lsn as u64, token as u64)
            .map_err(err)
    }

    #[napi]
    pub fn complete(
        &self,
        token: String,
        ok: bool,
        value: Either<String, Uint8Array>,
        reply: f64,
    ) -> Result<()> {
        let _g = self.enter();
        let status = if ok { Status::Ok } else { Status::Failed };
        self.e()?
            .complete(&token, status, wire_payload(&value), Some(reply as u64))
            .map_err(err)
    }

    #[napi]
    pub fn reset(
        &self,
        id: String,
        before_step: String,
        reapply_signals: bool,
        version: u32,
        token: f64,
        by: Option<String>,
    ) -> Result<()> {
        let e = self.e()?;
        e.audited(&id, by.as_deref(), || {
            e.reset(
                &id,
                &before_step,
                reapply_signals,
                (version > 0).then_some(version),
                Some(token as u64),
            )
        })
        .map(|_| ())
        .map_err(err)
    }

    #[napi]
    pub fn limit(&self, name: String, concurrent: u32, per_second: f64) -> Result<()> {
        self.e()?
            .limit(
                &name,
                LimitSpec {
                    concurrent,
                    per_second,
                },
            )
            .map_err(err)
    }

    /// `[permits held, steps waiting]` of a limit.
    #[napi]
    pub fn limit_usage(&self, name: String) -> Result<Vec<f64>> {
        let (held, waiting) = self.e()?.limit_usage(&name);
        Ok(vec![f64::from(held), waiting as f64])
    }

    /// Runs matching a JSON filter, as JSON.
    #[napi]
    pub fn list_where(&self, filter: String, limit: u32) -> Result<String> {
        let v: serde_json::Value = serde_json::from_str(&filter)
            .map_err(|e| napi::Error::new(napi::Status::InvalidArg, e.to_string()))?;
        let f = ListFilter::parse(&v).map_err(err)?;
        let runs: Vec<serde_json::Value> = self
            .e()?
            .list_where(&f, limit as usize)
            .iter()
            .map(summary_json)
            .collect();
        Ok(serde_json::Value::Array(runs).to_string())
    }

    /// A run's journal as an export file (for replay testing), or null.
    #[napi]
    pub fn export_journal(&self, id: String) -> Result<Option<Buffer>> {
        Ok(self
            .e()?
            .inspect(&id)
            .map_err(err)?
            .map(|d| Buffer::from(fusee::export::ExportedJournal::from_detail(&d).encode())))
    }

    #[napi]
    pub fn stuck(&self, shard: u32, run: f64, message: String) -> Result<()> {
        self.e()?.stuck(shard, run as RunId, message).map_err(err)
    }

    /// `[replaying, liveWork]`, or empty if the run has no code slot.
    #[napi]
    pub fn code_state(&self, shard: u32, run: f64) -> Result<Vec<bool>> {
        Ok(
            match self.e()?.code_state(shard, run as RunId).map_err(err)? {
                Some((a, b)) => vec![a, b],
                None => Vec::new(),
            },
        )
    }

    /// Reads stream `name` of `id` from `from` (of run `run` when it is
    /// not negative); the answer, JSON, is the reply for `token`.
    #[napi]
    pub fn stream_read(
        &self,
        id: String,
        run: f64,
        name: String,
        from: f64,
        wait_ms: f64,
        token: f64,
    ) -> Result<()> {
        let _g = self.enter();
        let run = (run >= 0.0).then_some(run as RunId);
        self.e()?
            .stream_read(
                &id,
                run,
                &name,
                from.max(0.0) as u64,
                wait_ms.max(0.0) as u64,
                token as u64,
            )
            .map_err(err)
    }

    #[napi]
    pub fn query_native(&self, id: String, name: String, arg: Buffer, token: f64) -> Result<()> {
        self.e()?
            .query_native(&id, &name, payload(&arg), token as u64)
            .map_err(err)
    }

    /// Checks a schedule spec (JSON); returns the problem or null.
    #[napi]
    pub fn validate_schedule(&self, spec: String) -> Option<String> {
        match serde_json::from_str::<fusee::ScheduleSpec>(&spec) {
            Ok(s) => s.validate().err().map(|e| e.to_string()),
            Err(e) => Some(e.to_string()),
        }
    }

    /// The next delivery for a run whose code is quiescent. Writes `[kind,
    /// seq, status, payload state]` to the shared array: kind 0 none, 1 a
    /// result (returned: its payload, unless the state says it is empty (0)
    /// or held by JavaScript (2)), 2 anything else (returned as an object).
    pub fn quiescent<'e>(
        &self,
        env: &'e Env,
        shard: u32,
        run: f64,
    ) -> Result<Option<Either3<JsString<'e>, BufferSlice<'e>, DeliveryInfo>>> {
        let _g = self.enter();
        let run = run as RunId;
        let d = self.e()?.quiescent(shard, run).map_err(err)?;
        let io = self.io();
        match d {
            None => {
                io[0] = 0.0;
                Ok(None)
            }
            Some(Delivery::Resolve { seq, outcome }) => {
                io[0] = 1.0;
                io[1] = f64::from(seq);
                io[2] = f64::from(outcome.status as u32);
                let held = {
                    let mut h = self.held.borrow_mut();
                    !h.is_empty() && h.remove(&(shard, run, seq))
                };
                if held && outcome.status == Status::Ok {
                    io[3] = 2.0;
                    return Ok(None);
                }
                if outcome.payload.is_empty() {
                    io[3] = 0.0;
                    return Ok(None);
                }
                io[3] = 1.0;
                Ok(Some(match js_payload(env, &outcome.payload)? {
                    Either::A(s) => Either3::A(s),
                    Either::B(b) => Either3::B(b),
                }))
            }
            Some(other) => {
                io[0] = 2.0;
                Ok(delivery(Some(other)).map(Either3::C))
            }
        }
    }

    pub fn finish(
        &self,
        shard: u32,
        run: f64,
        status: u32,
        value: Either<String, Uint8Array>,
    ) -> Result<()> {
        let _g = self.enter();
        let s = status_from(status);
        let p = wire_payload(&value);
        let outcome = if s == Status::Ok { Ok(p) } else { Err((s, p)) };
        self.drop_held(shard, run as RunId);
        self.e()?.finish(shard, run as RunId, outcome).map_err(err)
    }

    /// Advances the engine. Events go to the shared array (`EV_STRIDE`
    /// numbers each, their count at `IO_EVENTS`), with the next wake time
    /// at `IO_NEXT` and each shard's commit position. Returns null, or an
    /// array whose first element holds the events when they do not fit the
    /// shared array, followed by the events' strings and payloads.
    pub fn tick<'e>(&self, env: &'e Env) -> Result<Option<Array<'e>>> {
        let _g = self.enter();
        // Clear before polling. A subsequent background completion can then
        // always queue a new callback, including when it races this tick.
        self.wake_scheduled.store(false, Ordering::Release);
        let e = self.e()?;
        // A turn that only delivers: nothing for the engine to do unless
        // it asked for a tick (the SDK runs a full one after the burst).
        if self.io()[IO_LIGHT] != 0.0
            && !self.woken.load(Ordering::Acquire)
            && !self.owed.load(Ordering::Acquire)
        {
            self.io()[IO_EVENTS] = 0.0;
            return Ok(None);
        }
        self.woken.store(false, Ordering::Release);
        let idle = self.io()[IO_IDLE] != 0.0;
        let (events, next) = e.tick_idle(idle);
        e.check_health().map_err(err)?;
        let io = self.io();
        io[IO_NEXT] = next.map(|n| n as f64).unwrap_or(-1.0);
        for shard in 0..e.shards().min(MAX_COMMIT_SHARDS as u32) {
            self.note_commit(e, shard);
        }
        // A wake raised on this thread during the tick is covered by the
        // tick itself, unless it left a shard dirty.
        if self.owed.swap(false, Ordering::AcqRel) && e.has_dirty() {
            io[IO_OWED] = 1.0;
        }
        let mut ev = self.events.borrow_mut();
        ev.clear();
        let mut extras: Vec<Extra> = Vec::new();
        let mut extra = |x: Extra| {
            extras.push(x);
            (extras.len() - 1) as f64
        };
        for event in events {
            let row: [f64; EV_STRIDE] = match event {
                EngineEvent::Wake { shard, run } => {
                    [EV_WAKE, f64::from(shard), run as f64, 0.0, 0.0, 0.0]
                }
                EngineEvent::Ack { token } => [EV_ACK, token as f64, 0.0, 0.0, 0.0, 0.0],
                EngineEvent::Reject { token } => [EV_REJECT, token as f64, 0.0, 0.0, 0.0, 0.0],
                EngineEvent::Start { shard, run, info } => {
                    let x = extra(Extra::Name(info.id.clone()));
                    extra(Extra::Type(info.wtype.clone()));
                    extra(Extra::Bytes(info.input.clone()));
                    // The trace the run joins (null for none).
                    extra(match info.trace.as_deref() {
                        Some(t) if !t.is_empty() => {
                            Extra::Text(String::from_utf8_lossy(t).into_owned())
                        }
                        _ => Extra::None,
                    });
                    [
                        EV_START,
                        f64::from(shard),
                        run as f64,
                        f64::from(info.version),
                        info.started_ms as f64,
                        x,
                    ]
                }
                EngineEvent::Stuck {
                    shard,
                    run,
                    message,
                } => {
                    self.drop_held(shard, run);
                    let x = extra(Extra::Text(message));
                    [EV_STUCK, f64::from(shard), run as f64, 0.0, 0.0, x]
                }
                EngineEvent::Ended { shard, run } => {
                    self.drop_held(shard, run);
                    [EV_ENDED, f64::from(shard), run as f64, 0.0, 0.0, 0.0]
                }
                EngineEvent::Result {
                    shard,
                    run,
                    status,
                    payload,
                } => {
                    let x = extra(Extra::Bytes(payload));
                    [
                        EV_RESULT,
                        f64::from(shard),
                        run as f64,
                        f64::from(status as u32),
                        0.0,
                        x,
                    ]
                }
                EngineEvent::Reply {
                    token,
                    status,
                    payload,
                } => {
                    let x = extra(Extra::Bytes(payload));
                    [
                        EV_REPLY,
                        token as f64,
                        f64::from(status as u32),
                        0.0,
                        0.0,
                        x,
                    ]
                }
                EngineEvent::Refuse { token, message } => {
                    let x = extra(Extra::Text(message));
                    [EV_REFUSE, token as f64, 0.0, 0.0, 0.0, x]
                }
            };
            ev.extend_from_slice(&row);
        }
        io[IO_EVENTS] = (ev.len() / EV_STRIDE) as f64;
        let fits = ev.len() <= io.len() - IO_EV;
        if fits {
            io[IO_EV..IO_EV + ev.len()].copy_from_slice(&ev);
            if extras.is_empty() {
                return Ok(None);
            }
        }
        let mut out = env.create_array(1 + extras.len() as u32)?;
        if fits {
            out.set(0, Null)?;
        } else {
            out.set(0, Float64Array::new(std::mem::take(&mut *ev)))?;
        }
        let mut types: Vec<(Name, JsString<'_>)> = Vec::new();
        for (i, x) in extras.into_iter().enumerate() {
            let i = 1 + i as u32;
            match x {
                Extra::Text(t) => out.set(i, env.create_string(&t)?)?,
                Extra::Name(n) => out.set(i, env.create_string(&n)?)?,
                Extra::Type(n) => {
                    let s = match types.iter().find(|(t, _)| *t == n) {
                        Some((_, s)) => *s,
                        None => {
                            let s = env.create_string(&n)?;
                            types.push((n, s));
                            s
                        }
                    };
                    out.set(i, s)?
                }
                Extra::None => out.set(i, Null)?,
                Extra::Bytes(p) if p.is_empty() => out.set(i, Null)?,
                Extra::Bytes(p) => out.set(i, js_payload(env, &p)?)?,
            }
        }
        Ok(Some(out))
    }

    /// The test kit: the earliest timer of this engine's shards (ms), if any.
    #[napi]
    pub fn next_timer_ms(&self) -> Result<Option<f64>> {
        Ok(self.e()?.next_timer_ms().map(|t| t as f64))
    }

    /// The test kit's crash: loads an exported journal as a run.
    #[napi]
    pub fn import_journal(&self, file: Buffer, before_step: Option<u32>) -> Result<()> {
        self.e()?.import_journal(&file, before_step).map_err(err)
    }

    /// Serves the HTTP API (and the UI) on its own thread; its calls reach
    /// `next_call`. Returns the address it listens on.
    #[napi]
    pub fn serve(
        &mut self,
        addr: String,
        token: Option<String>,
        insecure: Option<bool>,
        profile: Option<String>,
    ) -> Result<String> {
        let relay = match &self.relay {
            Some(r) => r.clone(),
            None => {
                let send = self.send.clone();
                let r = fusee::http::Relay::new(Arc::new(move || send()));
                self.relay = Some(r.clone());
                r
            }
        };
        let opts = fusee::http::HttpOptions {
            token,
            insecure: insecure.unwrap_or(false),
            profile,
            ..Default::default()
        };
        let bound = fusee::http::serve_thread(relay, &addr, opts)
            .map_err(|e| err(fusee::Error::Io(e.to_string())))?;
        Ok(bound.to_string())
    }

    #[napi]
    pub fn next_call(&mut self) -> Result<Option<String>> {
        if let Some(c) = self.relay.as_ref().and_then(|r| r.next()) {
            return Ok(Some(
                serde_json::json!({"relay": c.id.to_string(), "method": c.method, "args": c.args})
                    .to_string(),
            ));
        }
        self.connection
            .as_mut()
            .map(|c| c.next_call_json())
            .transpose()
            .map(Option::flatten)
            .map_err(|e| err(e.into()))
    }

    #[napi]
    pub fn reply_call(&self, relay: String, response: String) -> Result<()> {
        let relay: u64 = relay
            .parse()
            .map_err(|_| napi::Error::from_reason("invalid relay ID"))?;
        if fusee::http::Relay::owns(relay)
            && let Some(r) = &self.relay
        {
            let answer: serde_json::Value =
                serde_json::from_str(&response).unwrap_or(serde_json::Value::Null);
            r.reply(relay, answer);
            return Ok(());
        }
        self.connection
            .as_ref()
            .ok_or_else(|| err(fusee::Error::Closed))?
            .reply(relay, response.into_bytes())
            .map_err(|e| err(e.into()))
    }

    /// An invoked worker: `running`, `idle`, `expired` or `lost`; null
    /// otherwise (see `fusee_server::invoke`).
    #[napi]
    pub fn invocation_state(&self, awaiting: Vec<String>) -> Option<String> {
        self.connection
            .as_ref()
            .and_then(|c| c.invocation_state(&awaiting))
            .map(str::to_owned)
    }

    /// Flushes everything and closes the engine.
    #[napi]
    pub fn close(&mut self) -> Result<()> {
        self.release_slot();
        if let Some(r) = self.relay.take() {
            r.close();
        }
        let result = self.engine.take().map(|e| e.close()).transpose();
        self.connection.take();
        self.driver.take();
        result.map(|_| ()).map_err(err)
    }
}

impl Drop for NativeEngine {
    fn drop(&mut self) {
        self.release_slot();
    }
}

/// See `queue_event`.
#[napi]
#[allow(clippy::too_many_arguments)]
pub fn queue_request(
    kind: String,
    workflow: String,
    key: String,
    event_id: String,
    input: String,
    a: Option<f64>,
    b: Option<f64>,
    c: Option<f64>,
) -> Result<Vec<String>> {
    queue_event(&kind, &workflow, &key, &event_id, &input, a, b, c)
        .map(Vec::from)
        .map_err(err)
}

// The calls on a step's path, by slot (see `ENGINES`).

#[napi]
pub fn engine_owns_id(slot: u32, id: String) -> Result<bool> {
    with_engine(slot, |e| e.owns_id(id))
}

#[napi]
pub fn engine_wake(slot: u32) -> Result<()> {
    with_engine(slot, |e| {
        e.wake();
        Ok(())
    })
}

#[napi]
pub fn engine_begin_step(
    slot: u32,
    shard: u32,
    run: f64,
    name: String,
    read_only: bool,
) -> Result<u32> {
    with_engine(slot, |e| e.begin_step(shard, run, name, read_only))
}

#[napi]
pub fn engine_begin_step_named(
    slot: u32,
    shard: u32,
    run: f64,
    name: u32,
    read_only: bool,
) -> Result<u32> {
    with_engine(slot, |e| e.begin_step_named(shard, run, name, read_only))
}

#[napi]
pub fn engine_step_gate(
    slot: u32,
    shard: u32,
    run: f64,
    gate: f64,
    not_before: f64,
    inline: Option<bool>,
) -> Result<bool> {
    with_engine(slot, |e| e.step_gate(shard, run, gate, not_before, inline))
}

#[napi]
pub fn engine_finish_step(
    slot: u32,
    shard: u32,
    run: f64,
    seq: u32,
    ok: bool,
    value: String,
) -> Result<()> {
    with_engine(slot, |e| e.finish_step(shard, run, seq, ok, value))
}

/// [`engine_finish_step`], then the run's next delivery as
/// [`engine_quiescent`] makes it: one call where the SDK delivers at once.
#[napi]
pub fn engine_finish_step_deliver<'e>(
    env: &'e Env,
    slot: u32,
    shard: u32,
    run: f64,
    seq: u32,
    ok: bool,
    value: String,
) -> Result<Option<Either3<JsString<'e>, BufferSlice<'e>, DeliveryInfo>>> {
    with_engine(slot, |e| {
        e.finish_step(shard, run, seq, ok, value)?;
        e.quiescent(env, shard, run)
    })
}

#[napi]
pub fn engine_finish_step_bytes(
    slot: u32,
    shard: u32,
    run: f64,
    seq: u32,
    value: Uint8Array,
) -> Result<()> {
    with_engine(slot, |e| e.finish_step_bytes(shard, run, seq, value))
}

#[napi]
pub fn engine_host_call(
    slot: u32,
    shard: u32,
    op: String,
    run: f64,
    args: Vec<String>,
    bufs: Vec<Buffer>,
) -> Result<Vec<f64>> {
    with_engine(slot, |e| e.host_call(shard, op, run, args, bufs))
}

#[napi]
pub fn engine_host_bytes(
    slot: u32,
    shard: u32,
    op: String,
    run: f64,
    args: Vec<String>,
    bufs: Vec<Buffer>,
) -> Result<Option<Buffer>> {
    with_engine(slot, |e| e.host_bytes(shard, op, run, args, bufs))
}

#[napi]
pub fn engine_value_now(slot: u32, shard: u32, run: f64, live_ms: f64) -> Result<f64> {
    with_engine(slot, |e| e.value_now(shard, run, live_ms))
}

#[napi]
pub fn engine_begin_sleep(
    slot: u32,
    shard: u32,
    run: f64,
    name: String,
    at_ms: f64,
) -> Result<u32> {
    with_engine(slot, |e| e.begin_sleep(shard, run, name, at_ms))
}

#[napi]
pub fn engine_quiescent<'e>(
    env: &'e Env,
    slot: u32,
    shard: u32,
    run: f64,
) -> Result<Option<Either3<JsString<'e>, BufferSlice<'e>, DeliveryInfo>>> {
    with_engine(slot, |e| e.quiescent(env, shard, run))
}

#[napi]
pub fn engine_finish(
    slot: u32,
    shard: u32,
    run: f64,
    status: u32,
    value: Either<String, Uint8Array>,
) -> Result<()> {
    with_engine(slot, |e| e.finish(shard, run, status, value))
}

#[napi]
pub fn engine_tick<'e>(env: &'e Env, slot: u32) -> Result<Option<Array<'e>>> {
    with_engine(slot, |e| e.tick(env))
}

/// The test kit: moves the virtual clock of `memory` engines (process-wide)
/// to `offset_ms` ahead of the wall clock.
#[napi]
pub fn set_test_clock(offset_ms: f64) {
    fusee::local::TEST_CLOCK_MS.store(
        offset_ms.max(0.0) as u64,
        std::sync::atomic::Ordering::Release,
    );
}
