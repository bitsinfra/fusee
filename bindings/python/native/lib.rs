//! Native half of the `fusee` Python package. The Python SDK drives
//! workflow coroutines with its own deterministic scheduler on the asyncio
//! thread; this module holds the engine and exposes its operations.

use fusee::engine::Config;
use fusee::foreign::{EngineEvent, ForeignEngine};
use fusee::limits::{Acquire, LimitSpec};
use fusee::local::FlushMethod;
use fusee::runtime::{
    Delivery, ForeignEvent, Host, ResultState, RuntimeConfig, Services, ShardRuntime, StartOutcome,
    StartRequest, UpdateRequest,
};
use fusee::visibility::ListFilter;
use fusee_core::record::{Payload, RunId, Status};
use fusee_core::run::{AwaitStart, StepStart};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Waker;
use std::time::Duration;

/// A payload that is not JSON: raw bytes after a zero byte, which no JSON
/// text starts with.
const RAW: u8 = 0;

// Event kinds in the flat event list `tick` returns (6 numbers each).
const EV_WAKE: u64 = 1;
const EV_ACK: u64 = 2;
const EV_REJECT: u64 = 3;
const EV_START: u64 = 4;
const EV_STUCK: u64 = 5;
const EV_ENDED: u64 = 6;
const EV_RESULT: u64 = 7;
const EV_REPLY: u64 = 8;
const EV_REFUSE: u64 = 9;

// Delivery kinds `quiescent` returns first in its tuple.
const D_RESOLVE: u32 = 1;
const D_HELD: u32 = 2;
const D_CANCEL: u32 = 3;
const D_UPDATE: u32 = 4;

thread_local! {
    /// Depth of calls from Python into this module on this thread. A wake
    /// raised meanwhile writes no byte: the SDK ticks after the call.
    static IN_CALL: Cell<u32> = const { Cell::new(0) };
}

struct InCall;

impl InCall {
    fn enter() -> Self {
        IN_CALL.with(|c| c.set(c.get() + 1));
        InCall
    }
}

impl Drop for InCall {
    fn drop(&mut self) {
        IN_CALL.with(|c| c.set(c.get() - 1));
    }
}

fn raw_payload(b: &[u8]) -> Payload {
    let mut v = Vec::with_capacity(b.len() + 1);
    v.push(RAW);
    v.extend_from_slice(b);
    Arc::from(v)
}

/// JSON text crosses as `str`, raw bytes as `bytes` (tagged here).
fn wire_payload(v: &Bound<'_, PyAny>) -> PyResult<Payload> {
    if let Ok(s) = v.cast::<PyString>() {
        return Ok(Arc::from(s.to_cow()?.as_bytes()));
    }
    if let Ok(b) = v.cast::<PyBytes>() {
        return Ok(raw_payload(b.as_bytes()));
    }
    Ok(raw_payload(&v.extract::<Vec<u8>>()?))
}

/// A recorded payload for Python: JSON or text as `str`, raw bytes as
/// `bytes`, nothing as `None`.
fn py_payload<'py>(py: Python<'py>, p: &[u8]) -> Bound<'py, PyAny> {
    match p.first() {
        None => py.None().into_bound(py),
        Some(&RAW) => PyBytes::new(py, &p[1..]).into_any(),
        Some(_) => PyString::new(py, &String::from_utf8_lossy(p)).into_any(),
    }
}

/// An error for Python as `Code: message`; one that carries a failure
/// sends its envelope, so the SDK revives its type, code and cause.
fn err(e: fusee::Error) -> PyErr {
    if e.failure().is_some() {
        let envelope = e.to_failure().to_payload();
        return PyRuntimeError::new_err(format!(
            "{}: {}",
            e.code(),
            String::from_utf8_lossy(&envelope)
        ));
    }
    PyRuntimeError::new_err(format!("{}: {e}", e.code()))
}

/// A failed connection to a server, as `Connection: ...` (other failures on
/// the way, a bad option or directory, stay `Io`).
fn connection_err(e: std::io::Error) -> PyErr {
    use std::io::ErrorKind as K;
    match e.kind() {
        K::ConnectionRefused
        | K::ConnectionReset
        | K::ConnectionAborted
        | K::NotFound
        | K::TimedOut
        | K::AddrNotAvailable
        | K::NotConnected
        | K::PermissionDenied => err(fusee::Error::Connection(e.to_string())),
        _ => err(fusee::Error::Io(e.to_string())),
    }
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

fn codec(keys: Option<Vec<(String, Vec<u8>)>>) -> PyResult<Option<Arc<dyn fusee::Codec>>> {
    let Some(keys) = keys else { return Ok(None) };
    let mut list = Vec::new();
    for (id, k) in keys {
        let key: [u8; 32] = k
            .as_slice()
            .try_into()
            .map_err(|_| PyRuntimeError::new_err("Failed: an AES-256-GCM key is 32 bytes"))?;
        list.push((id, key));
    }
    Ok(Some(fusee::aes_gcm(list)))
}

fn arg_u64(args: &[String], i: usize) -> u64 {
    args.get(i).and_then(|s| s.parse().ok()).unwrap_or(0)
}

/// A stream operation's writer from `args[i]`: a step's position, or
/// anything else (`"w"`) for workflow code.
fn step_arg(args: &[String], i: usize) -> Option<u32> {
    args.get(i).and_then(|s| s.parse().ok())
}

fn arg(args: &[String], i: usize) -> &str {
    args.get(i).map(String::as_str).unwrap_or("")
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
/// `b` the longest wait); `c` is the dedup window. `None` takes the Rust
/// default. Returns `(coordinator type, coordinator ID, start input, update
/// argument)`, the last two JSON text; a rule the coordinator would refuse
/// is rejected here.
#[pyfunction]
#[pyo3(signature = (kind, workflow, key, event_id, input, a=None, b=None, c=None))]
#[allow(clippy::too_many_arguments)]
fn queue_request(
    kind: &str,
    workflow: &str,
    key: &str,
    event_id: &str,
    input: &str,
    a: Option<f64>,
    b: Option<f64>,
    c: Option<f64>,
) -> PyResult<(String, String, String, String)> {
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
        other => {
            return Err(err(fusee::Error::Rejected(format!(
                "no queue rule '{other}'"
            ))));
        }
    };
    rule.validate().map_err(err)?;
    let event = Event {
        id: event_id.to_owned(),
        rule,
        input: serde_json::from_str(input).map_err(|e| err(e.into()))?,
    };
    let (wtype, id, state, arg) = fusee::queue::request(workflow, key, &event);
    Ok((wtype.to_owned(), id, state.to_string(), arg.to_string()))
}

/// `(seq, replay, gate, attempt, not_before_ms)` for a step the code begins.
type StepBegin = (u32, bool, u64, u32, u64);

// Typed step operations: a sequential step makes these on every attempt, so
// they skip the string-typed `host_call` (decimal arguments, fresh vectors).

fn begin_step(host: &dyn Host, run: RunId, name: &str) -> PyResult<StepBegin> {
    let (seq, start, gate) = host.begin_step(run, name).map_err(err)?;
    Ok(match start {
        StepStart::Replay => (seq, true, gate, 0, 0),
        StepStart::Run {
            attempt,
            not_before_ms,
        } => (seq, false, gate, attempt, not_before_ms),
    })
}

fn may_run(host: &dyn Host, run: RunId, gate: u64, not_before_ms: u64) -> bool {
    host.may_run(run, gate, not_before_ms, Waker::noop())
}

fn step_done(host: &dyn Host, run: RunId, seq: u32, ok: bool, value: &[u8]) {
    let status = if ok { Status::Ok } else { Status::Failed };
    host.step_done(run, seq, status, payload(value));
}

fn step_retry(
    host: &dyn Host,
    run: RunId,
    seq: u32,
    retry_at_ms: u64,
    error: &[u8],
) -> PyResult<u64> {
    host.step_retry(run, seq, retry_at_ms, payload(error))
        .map_err(err)
}

/// The run-scoped operations workflow code makes (shared by the engine and
/// the replayer). Returns numbers.
fn host_call(
    host: &dyn Host,
    op: &str,
    run: RunId,
    args: Vec<String>,
    bufs: Vec<Vec<u8>>,
) -> PyResult<Vec<i64>> {
    let buf = |i: usize| -> Payload {
        bufs.get(i)
            .map(|b| payload(b))
            .unwrap_or_else(|| Arc::from(&[][..]))
    };
    Ok(match op {
        "beginStep" => {
            let (seq, replay, gate, attempt, not_before_ms) = begin_step(host, run, arg(&args, 0))?;
            vec![
                i64::from(seq),
                i64::from(replay),
                gate as i64,
                i64::from(attempt),
                not_before_ms as i64,
            ]
        }
        "beginAwait" => {
            let (seq, start, gate) = host.begin_await(run, arg(&args, 0)).map_err(err)?;
            match start {
                AwaitStart::Replay => vec![i64::from(seq), 1, gate as i64, 0, 0],
                AwaitStart::Waiting { attempt } => {
                    vec![i64::from(seq), 2, gate as i64, i64::from(attempt), 0]
                }
                AwaitStart::Run {
                    attempt,
                    not_before_ms,
                } => vec![
                    i64::from(seq),
                    0,
                    gate as i64,
                    i64::from(attempt),
                    not_before_ms as i64,
                ],
            }
        }
        "mayRun" => vec![i64::from(may_run(
            host,
            run,
            arg_u64(&args, 0),
            arg_u64(&args, 1),
        ))],
        "stepDone" => {
            step_done(
                host,
                run,
                arg_u64(&args, 0) as u32,
                arg(&args, 1) == "1",
                &buf(0),
            );
            vec![]
        }
        "stepRetry" => vec![step_retry(
            host,
            run,
            arg_u64(&args, 0) as u32,
            arg_u64(&args, 1),
            &buf(0),
        )? as i64],
        "stepPaused" => vec![i64::from(host.step_paused(run, arg_u64(&args, 0) as u32))],
        "beginSleep" => vec![i64::from(
            host.begin_sleep(run, arg(&args, 0), arg_u64(&args, 1))
                .map_err(err)?,
        )],
        "beginWait" => vec![i64::from(
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
            .map(i64::from)
            .unwrap_or(-1),
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
        "patched" => vec![i64::from(
            host.patched(run, arg(&args, 0), arg(&args, 1) == "1")
                .map_err(err)?,
        )],
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
        "journalSize" => vec![i64::from(host.journal_size(run))],
        "cancelWorkflow" => {
            host.cancel_workflow(run, arg(&args, 0), arg(&args, 1))
                .map_err(err)?;
            vec![]
        }
        "forget" => {
            host.forget(run, arg_u64(&args, 0) as u32);
            vec![]
        }
        "limitTicket" => vec![host.limit_ticket() as i64],
        // `[limit, ticket, level, fairness key, weight]`; the last three are
        // the step's priority (absent: the default, arrival order).
        "acquire" => match host.acquire_in(
            run,
            arg(&args, 0),
            arg_u64(&args, 1),
            &priority(&args, 2),
            None,
        ) {
            Acquire::Granted => vec![0],
            Acquire::Wait { retry_at_ms } => vec![retry_at_ms.map(|t| t as i64).unwrap_or(-1)],
        },
        "release" => {
            host.release(arg(&args, 0), arg_u64(&args, 1));
            vec![]
        }
        // `[name, step]`: `[chunks the step recorded before, closed]`.
        "streamOpen" => {
            let o = host
                .stream_open(run, arg(&args, 0), step_arg(&args, 1))
                .map_err(err)?;
            vec![o.recorded as i64, i64::from(o.closed)]
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
            .map(i64::from)
            .unwrap_or(-1),
        ],
        other => {
            return Err(PyRuntimeError::new_err(format!(
                "Failed: unknown host operation '{other}'"
            )));
        }
    })
}

/// Host operations that return bytes (None when there is nothing).
fn host_bytes(
    host: &dyn Host,
    op: &str,
    run: RunId,
    args: Vec<String>,
    bufs: Vec<Vec<u8>>,
) -> PyResult<Option<Vec<u8>>> {
    Ok(match op {
        "value" => {
            let c: Payload = bufs
                .first()
                .map(|b| payload(b))
                .unwrap_or_else(|| Arc::from(&[][..]));
            Some(
                host.value(run, arg_u64(&args, 0) as u8, &mut |_, _| c.clone())
                    .map_err(err)?
                    .to_vec(),
            )
        }
        "key" => Some(host.key(run, arg_u64(&args, 0) as u32).into_bytes()),
        "token" => Some(
            host.completion_token(run, arg_u64(&args, 0) as u32)
                .map_err(err)?
                .into_bytes(),
        ),
        "progress" => host
            .progress(run, arg_u64(&args, 0) as u32)
            .map(|p| p.to_vec()),
        // `[key, "1" exclusive]`: the value's JSON, or None when absent.
        "objectGet" => host
            .object_get(run, arg(&args, 0), arg(&args, 1) == "1")
            .map_err(err)?
            .map(|p| p.to_vec()),
        // `["1" exclusive]`: the keys, a JSON array.
        "objectKeys" => {
            let keys = host.object_keys(run, arg(&args, 0) == "1").map_err(err)?;
            Some(serde_json::to_vec(&keys).map_err(|e| PyRuntimeError::new_err(e.to_string()))?)
        }
        // The committed state, a JSON object.
        "objectState" => Some(host.object_state(run).map_err(err)?.to_vec()),
        // The edit's JSON in bufs[0]: applies it as the running edit
        // handler's writes; the answer.
        "objectEdit" => {
            let edit = bufs.first().cloned().unwrap_or_default();
            Some(host.object_edit(run, &edit).map_err(err)?.to_vec())
        }
        // `[name, step, limit]`: the chunks, each a 4-byte little-endian
        // length and then the tagged payload.
        "streamRecorded" => {
            let chunks = host
                .stream_recorded(
                    run,
                    arg(&args, 0),
                    arg_u64(&args, 1) as u32,
                    arg_u64(&args, 2),
                )
                .map_err(err)?;
            let mut out = Vec::new();
            for c in &chunks {
                out.extend_from_slice(&(c.len() as u32).to_le_bytes());
                out.extend_from_slice(c);
            }
            Some(out)
        }
        other => {
            return Err(PyRuntimeError::new_err(format!(
                "Failed: unknown host operation '{other}'"
            )));
        }
    })
}

type Held = RefCell<HashSet<(u32, RunId, u32)>>;

/// A delivery as a tuple whose first item is its kind:
/// `(D_RESOLVE, seq, status, payload)`, `(D_HELD, seq, status, None)` for a
/// step result the SDK recorded and still holds, `(D_CANCEL, -1, status,
/// reason, seqs)` or `(D_UPDATE, -1, 0, payload, ordinal, name, id, token)`.
fn delivery<'py>(
    py: Python<'py>,
    shard: u32,
    run: RunId,
    d: Option<Delivery>,
    held: Option<&Held>,
) -> PyResult<Option<Bound<'py, PyTuple>>> {
    let Some(d) = d else { return Ok(None) };
    Ok(Some(match d {
        Delivery::Resolve { seq, outcome } => {
            let kept = held.is_some_and(|h| {
                let mut h = h.borrow_mut();
                !h.is_empty() && h.remove(&(shard, run, seq))
            });
            if kept && outcome.status == Status::Ok {
                (D_HELD, seq, outcome.status as u32, py.None()).into_pyobject(py)?
            } else {
                (
                    D_RESOLVE,
                    seq,
                    outcome.status as u32,
                    py_payload(py, &outcome.payload),
                )
                    .into_pyobject(py)?
            }
        }
        Delivery::CancelAll { seqs, reason } => (
            D_CANCEL,
            -1,
            Status::Cancelled as u32,
            String::from_utf8_lossy(&reason).into_owned(),
            seqs,
        )
            .into_pyobject(py)?,
        Delivery::Update {
            ordinal,
            name,
            id,
            payload,
            token,
        } => (
            D_UPDATE,
            -1,
            0,
            py_payload(py, &payload),
            ordinal,
            name,
            id,
            token.map(|t| t as i64).unwrap_or(-1),
        )
            .into_pyobject(py)?,
    }))
}

/// `(seq, replay, gate, attempt, not_before_ms, open)`: begins a step and,
/// for one that runs, says whether its gate is open now.
type StepStarted = (u32, bool, u64, u32, u64, bool);

fn summary_json(s: &fusee::RunSummary) -> serde_json::Value {
    let attrs: serde_json::Value =
        serde_json::from_str(&s.attributes).unwrap_or(serde_json::Value::Null);
    serde_json::json!({"id": s.id, "type": s.wtype, "version": s.version, "status": s.status,
        "started_ms": s.started_ms, "finished_ms": s.finished_ms, "error": s.error,
        "shard": s.shard, "run": s.run, "attributes": attrs, "object": s.object})
}

fn foreign_dict<'py>(
    py: Python<'py>,
    d: &Bound<'py, PyDict>,
    shard: u32,
    e: ForeignEvent,
) -> PyResult<()> {
    match e {
        ForeignEvent::Start { run, info } => {
            d.set_item("kind", "start")?;
            d.set_item("shard", shard)?;
            d.set_item("run", run)?;
            d.set_item("id", info.id.as_ref())?;
            d.set_item("wtype", info.wtype.as_ref())?;
            d.set_item("version", info.version)?;
            d.set_item("started_ms", info.started_ms)?;
            d.set_item("input", PyBytes::new(py, &info.input))?;
        }
        ForeignEvent::Wake { run } => {
            d.set_item("kind", "wake")?;
            d.set_item("shard", shard)?;
            d.set_item("run", run)?;
        }
        ForeignEvent::Stuck { run, message } => {
            d.set_item("kind", "stuck")?;
            d.set_item("shard", shard)?;
            d.set_item("run", run)?;
            d.set_item("message", message)?;
        }
        ForeignEvent::Ended { run } => {
            d.set_item("kind", "ended")?;
            d.set_item("shard", shard)?;
            d.set_item("run", run)?;
        }
    }
    Ok(())
}

/// A memory runtime for replay testing: step functions never run and timers
/// never fire.
#[pyclass(unsendable)]
struct NativeReplay {
    rt: Option<ShardRuntime<Box<dyn fusee::env::Env>>>,
}

impl NativeReplay {
    fn rt(&self) -> PyResult<&ShardRuntime<Box<dyn fusee::env::Env>>> {
        self.rt.as_ref().ok_or_else(|| err(fusee::Error::Closed))
    }
}

#[pymethods]
impl NativeReplay {
    #[new]
    #[pyo3(signature = (codec_keys=None))]
    fn new(codec_keys: Option<Vec<(String, Vec<u8>)>>) -> PyResult<Self> {
        let cfg = RuntimeConfig {
            replay_only: true,
            park_after_ms: u64::MAX,
            ..Default::default()
        };
        let services = Services {
            codec: codec(codec_keys)?,
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

    /// Imports an export file's bytes; returns its header as JSON.
    fn import_journal(&self, file: &[u8]) -> PyResult<String> {
        let j = fusee::export::ExportedJournal::decode(file).map_err(err)?;
        let complete = j.is_complete();
        let run = self.rt()?.import(&j.records).map_err(err)?;
        Ok(serde_json::json!({"run": run, "workflow_id": j.workflow_id, "workflow_type": j.workflow_type,
            "version": j.version, "complete": complete})
        .to_string())
    }

    fn tick<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let rt = self.rt()?;
        rt.tick();
        let list = PyList::empty(py);
        for e in rt.take_foreign() {
            let d = PyDict::new(py);
            foreign_dict(py, &d, 0, e)?;
            list.append(d)?;
        }
        Ok(list)
    }

    /// `(stuck message, replaying, live work, finished)`
    fn state(&self, run: RunId) -> PyResult<(String, bool, bool, bool)> {
        let rt = self.rt()?;
        let (r, l) = rt.code_state(run).unwrap_or((false, false));
        let finished = rt.summary(run).is_some_and(|s| s.finished_ms.is_some());
        Ok((rt.stuck_reason(run).unwrap_or_default(), r, l, finished))
    }

    #[pyo3(signature = (_shard, op, run, args, bufs))]
    fn host_call(
        &self,
        _shard: u32,
        op: &str,
        run: RunId,
        args: Vec<String>,
        bufs: Vec<Vec<u8>>,
    ) -> PyResult<Vec<i64>> {
        host_call(self.rt()?.host().as_ref(), op, run, args, bufs)
    }

    /// Step functions never run on replay: the gate stays shut.
    #[pyo3(signature = (_shard, run, name, _read_only=false, _inline=false))]
    fn begin_step(
        &self,
        _shard: u32,
        run: RunId,
        name: &str,
        _read_only: bool,
        _inline: bool,
    ) -> PyResult<StepStarted> {
        let (seq, replay, gate, attempt, not_before) =
            begin_step(self.rt()?.host().as_ref(), run, name)?;
        Ok((seq, replay, gate, attempt, not_before, false))
    }

    #[pyo3(signature = (_shard, run, gate, not_before_ms, _inline=false))]
    fn may_run(
        &self,
        _shard: u32,
        run: RunId,
        gate: u64,
        not_before_ms: u64,
        _inline: bool,
    ) -> PyResult<bool> {
        Ok(may_run(
            self.rt()?.host().as_ref(),
            run,
            gate,
            not_before_ms,
        ))
    }

    #[pyo3(signature = (_shard, run, seq, ok, value))]
    fn step_done(&self, _shard: u32, run: RunId, seq: u32, ok: bool, value: &str) -> PyResult<()> {
        step_done(self.rt()?.host().as_ref(), run, seq, ok, value.as_bytes());
        Ok(())
    }

    #[pyo3(signature = (_shard, run, seq, value))]
    fn step_done_bytes(&self, _shard: u32, run: RunId, seq: u32, value: &[u8]) -> PyResult<()> {
        self.rt()?
            .host()
            .step_done(run, seq, Status::Ok, raw_payload(value));
        Ok(())
    }

    #[pyo3(signature = (_shard, run, seq, retry_at_ms, error))]
    fn step_retry(
        &self,
        _shard: u32,
        run: RunId,
        seq: u32,
        retry_at_ms: u64,
        error: &[u8],
    ) -> PyResult<u64> {
        step_retry(self.rt()?.host().as_ref(), run, seq, retry_at_ms, error)
    }

    #[pyo3(signature = (_shard, op, run, args, bufs))]
    fn host_bytes<'py>(
        &self,
        py: Python<'py>,
        _shard: u32,
        op: &str,
        run: RunId,
        args: Vec<String>,
        bufs: Vec<Vec<u8>>,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        Ok(host_bytes(self.rt()?.host().as_ref(), op, run, args, bufs)?
            .map(|b| PyBytes::new(py, &b)))
    }

    #[pyo3(signature = (_shard, run))]
    fn quiescent<'py>(
        &self,
        py: Python<'py>,
        _shard: u32,
        run: RunId,
    ) -> PyResult<Option<Bound<'py, PyTuple>>> {
        delivery(py, 0, run, self.rt()?.quiescent(run).map_err(err)?, None)
    }

    #[pyo3(signature = (_shard, token, ok, message))]
    fn decide_update(&self, _shard: u32, token: u64, ok: bool, message: String) -> PyResult<i64> {
        let d = if ok { Ok(()) } else { Err(message) };
        Ok(self
            .rt()?
            .decide_update(token, d)
            .map_err(err)?
            .map(|o| o as i64)
            .unwrap_or(-1))
    }

    #[pyo3(signature = (_shard, run, status, value))]
    fn finish(
        &self,
        _shard: u32,
        run: RunId,
        status: u32,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let s = status_from(status);
        let p = wire_payload(value)?;
        let outcome = if s == Status::Ok { Ok(p) } else { Err((s, p)) };
        self.rt()?.foreign_finish(run, outcome);
        Ok(())
    }

    #[pyo3(signature = (_shard, run, message))]
    fn stuck(&self, _shard: u32, run: RunId, message: String) -> PyResult<()> {
        self.rt()?.foreign_stuck(run, message);
        Ok(())
    }

    fn close(&mut self) {
        self.rt = None;
    }
}

#[pyclass]
struct NativeClient {
    client: fusee_server::Client,
}

#[pymethods]
impl NativeClient {
    #[new]
    fn new(py: Python<'_>, address: String) -> PyResult<Self> {
        let client = py
            .detach(move || fusee_server::Client::connect(&address))
            .map_err(connection_err)?;
        Ok(Self { client })
    }

    fn call(&self, py: Python<'_>, method: String, args: String) -> PyResult<String> {
        let params = serde_json::from_str(&args).map_err(|e| err(e.into()))?;
        let result = py
            .detach(|| self.client.call(&method, &params))
            .map_err(err)?;
        Ok(result.to_string())
    }

    fn close(&self) -> PyResult<()> {
        self.client.close()?;
        Ok(())
    }
}

#[pyclass]
struct NativeConnection {
    link: Option<fusee_server::foreign::ForeignConnection>,
}

#[pyclass]
#[derive(Default)]
struct NativeReplicaMetrics {
    metrics: fusee_server::foreign::ReplicaMetrics,
}
#[pymethods]
impl NativeReplicaMetrics {
    #[new]
    fn new() -> Self {
        Self::default()
    }
    fn snapshot(&self) -> Option<String> {
        self.metrics.snapshot().map(|v| v.to_string())
    }
}

#[pyfunction]
#[pyo3(signature=(mode, address=None, id=None, peers=None, dir=None, flush=None, metrics=None))]
fn prepare_connection(
    py: Python<'_>,
    mode: String,
    address: Option<String>,
    id: Option<u8>,
    peers: Option<Vec<String>>,
    dir: Option<String>,
    flush: Option<String>,
    metrics: Option<PyRef<'_, NativeReplicaMetrics>>,
) -> PyResult<NativeConnection> {
    let observer = metrics.map(|m| m.metrics.clone());
    let link = py
        .detach(move || {
            fusee_server::foreign::ForeignConnection::prepare_with(
                &mode, address, id, peers, dir, flush, observer,
            )
        })
        .map_err(connection_err)?;
    Ok(NativeConnection { link: Some(link) })
}

#[pyclass(unsendable)]
struct NativeEngine {
    engine: Option<ForeignEngine>,
    connection: Option<fusee_server::foreign::ForeignConnection>,
    /// A wake byte is in the pipe and no tick has started since.
    wake_pending: Arc<AtomicBool>,
    /// A wake was raised during a call from Python and wrote no byte.
    owed: Arc<AtomicBool>,
    /// Writes the wake byte (coalesced with `wake_pending`).
    send: Arc<dyn Fn() + Send + Sync>,
    /// Step results Python recorded and still holds: their delivery carries
    /// no payload back.
    held: Held,
    /// Calls from the HTTP API (`serve`), answered by Python's tick.
    relay: Option<fusee::http::Relay>,
}

impl NativeEngine {
    fn e(&self) -> PyResult<&ForeignEngine> {
        if let Some(c) = &self.connection {
            c.check()?;
        }
        self.engine
            .as_ref()
            .ok_or_else(|| err(fusee::Error::Closed))
    }

    fn drop_held(&self, shard: u32, run: RunId) {
        let mut held = self.held.borrow_mut();
        if !held.is_empty() {
            held.retain(|&(s, r, _)| s != shard || r != run);
        }
    }
}

#[pymethods]
impl NativeEngine {
    /// Opens the engine. When background work completes, one byte is written
    /// to `notify_fd` (a pipe the asyncio loop watches).
    #[new]
    #[pyo3(signature = (dir=None, durability="local", shards=None, park_after_ms=None, retention_ms=None, checkpoint_interval_ms=None, flush=None, segment_bytes=None, notify_fd=-1, keep_history=false, codec_keys=None, prepared=None, archive_history=false, inline_flush=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        dir: Option<String>,
        durability: &str,
        shards: Option<u32>,
        park_after_ms: Option<u64>,
        retention_ms: Option<u64>,
        checkpoint_interval_ms: Option<u64>,
        flush: Option<String>,
        segment_bytes: Option<u64>,
        notify_fd: i32,
        keep_history: bool,
        codec_keys: Option<Vec<(String, Vec<u8>)>>,
        prepared: Option<PyRefMut<'_, NativeConnection>>,
        archive_history: bool,
        inline_flush: Option<bool>,
    ) -> PyResult<Self> {
        let connection = prepared
            .map(|mut p| p.link.take().ok_or_else(|| err(fusee::Error::Closed)))
            .transpose()?;
        let mut cfg = match durability {
            "memory" => Config::memory(),
            "local" => Config::local(dir.unwrap_or_else(|| "./fusee-data".into())),
            "server" | "replicated" => connection
                .as_ref()
                .ok_or_else(|| PyRuntimeError::new_err("missing prepared connection"))?
                .config
                .clone(),
            mode => {
                return Err(PyRuntimeError::new_err(format!(
                    "unsupported durability '{mode}'"
                )));
            }
        };
        if connection.is_some() {
            if shards.is_some() && shards != cfg.shards {
                return Err(PyRuntimeError::new_err(
                    "shard count must match the remote configuration",
                ));
            }
        } else {
            cfg.shards = Some(shards.unwrap_or(1));
        }
        if let Some(v) = park_after_ms {
            cfg.park_after = Duration::from_millis(v);
        }
        if let Some(v) = retention_ms {
            cfg.retention = Duration::from_millis(v);
        }
        if let Some(v) = checkpoint_interval_ms {
            cfg.checkpoint_interval = Duration::from_millis(v);
        }
        if let Some(v) = segment_bytes {
            cfg.local.segment_bytes = v;
        }
        cfg.keep_history = keep_history;
        cfg.archive_history = archive_history;
        cfg.codec = codec(codec_keys)?;
        if let Some(on) = inline_flush {
            cfg.inline_flush = on;
        }
        match flush.as_deref() {
            Some("fsync") => cfg.local.flush = FlushMethod::Fsync,
            Some("fullfsync") => cfg.local.flush = FlushMethod::FullFsync,
            Some("fdatasync") => cfg.local.flush = FlushMethod::Fdatasync,
            Some("dsync") => cfg.local.flush = FlushMethod::Dsync,
            _ => {}
        }
        // Some transport callbacks outlive the Python handle. Give every such
        // callback a shared, owned pipe descriptor so fd reuse cannot redirect it.
        use std::io::Write;
        use std::os::fd::FromRawFd;
        let pipe = if notify_fd >= 0 {
            // SAFETY: fcntl duplicates the caller-owned fd without taking ownership.
            let fd = unsafe { libc::fcntl(notify_fd, libc::F_DUPFD_CLOEXEC, 0) };
            if fd < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            // SAFETY: fd is a fresh, exclusively owned descriptor from fcntl.
            Some(unsafe { std::fs::File::from_raw_fd(fd) })
        } else {
            None
        };
        // One byte per batch of wakes: the next tick clears the flag, and a
        // tick sees everything that completed before it started.
        let wake_pending = Arc::new(AtomicBool::new(false));
        let owed = Arc::new(AtomicBool::new(false));
        let pending = wake_pending.clone();
        let send: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            if let Some(mut writer) = pipe.as_ref()
                && !pending.swap(true, Ordering::AcqRel)
            {
                // A full pipe already holds a byte; a closed one means the
                // engine is closing. Either way nothing more is needed.
                let _ = writer.write(&[1]);
            }
        });
        let unsent = owed.clone();
        let sender = send.clone();
        let notify: fusee::env::Notify = Arc::new(move || {
            // Inside a call from Python on this thread: the SDK looks again
            // when the call returns.
            if IN_CALL.with(|c| c.get() > 0) {
                unsent.store(true, Ordering::Release);
                return;
            }
            sender();
        });
        if let Some(c) = &connection {
            c.set_notify(notify.clone());
        }
        let engine = ForeignEngine::open(&cfg, notify).map_err(err)?;
        Ok(Self {
            engine: Some(engine),
            connection,
            wake_pending,
            owed,
            send,
            held: RefCell::new(HashSet::new()),
            relay: None,
        })
    }

    /// The test kit: the earliest timer of this engine's shards (ms), if any.
    fn next_timer_ms(&self) -> PyResult<Option<u64>> {
        Ok(self.e()?.next_timer_ms())
    }

    /// The test kit's crash: loads an exported journal as a run.
    #[pyo3(signature = (file, before_step=None))]
    fn import_journal(&self, file: &[u8], before_step: Option<u32>) -> PyResult<()> {
        self.e()?.import_journal(file, before_step).map_err(err)
    }

    /// Serves the HTTP API (and the UI) on its own thread; its calls reach
    /// `next_call`. Returns the address it listens on.
    #[pyo3(signature = (addr, token=None, insecure=false, profile=None))]
    fn serve(
        &mut self,
        addr: &str,
        token: Option<String>,
        insecure: bool,
        profile: Option<String>,
    ) -> PyResult<String> {
        let relay = match &self.relay {
            Some(r) => r.clone(),
            None => {
                let r = fusee::http::Relay::new(self.send.clone());
                self.relay = Some(r.clone());
                r
            }
        };
        let opts = fusee::http::HttpOptions {
            token,
            insecure,
            profile,
            ..Default::default()
        };
        let bound = fusee::http::serve_thread(relay, addr, opts)
            .map_err(|e| err(fusee::Error::Io(e.to_string())))?;
        Ok(bound.to_string())
    }

    /// Writes the wake byte, as a background wake does (coalesced until
    /// the next tick).
    fn wake(&self) {
        (self.send)();
    }

    /// A wake was raised during a call from Python and wrote no byte since
    /// the last time this was asked (the SDK then ticks soon).
    fn owed(&self) -> bool {
        self.owed.swap(false, Ordering::AcqRel)
    }

    fn shards(&self) -> PyResult<u32> {
        Ok(self.e()?.shards())
    }

    /// Returns 0 if a run was created, 1 if one already existed. `token`
    /// -1 asks for no acknowledgment; `update` is `(name, id, arg, token)`.
    /// `trace`: the W3C traceparent the run joins.
    #[pyo3(signature = (id, wtype, version, input, deadline_ms, token, policy, update=None, trace=None))]
    #[allow(clippy::too_many_arguments)]
    fn start(
        &self,
        id: String,
        wtype: String,
        version: u32,
        input: &Bound<'_, PyAny>,
        deadline_ms: u64,
        token: i64,
        policy: u32,
        update: Option<(String, String, Bound<'_, PyAny>, u64)>,
        trace: Option<&str>,
    ) -> PyResult<u32> {
        let _g = InCall::enter();
        let mut req = StartRequest::new(&id, &wtype, wire_payload(input)?, Default::default());
        req.version = Some(version.max(1));
        req.deadline_ms = deadline_ms;
        req.policy = policy as u8;
        req.token = (token >= 0).then_some(token as u64);
        req.trace = trace.map(|t| Arc::from(t.as_bytes()));
        if let Some((name, uid, arg, t)) = update {
            req.update = Some(UpdateRequest {
                name,
                id: uid,
                payload: wire_payload(&arg)?,
                token: Some(t),
            });
        }
        Ok(match self.e()?.start(req).map_err(err)? {
            (_, StartOutcome::Created { .. }) => 0,
            (_, StartOutcome::Existing { .. }) => 1,
        })
    }

    /// `by`: who asked (audit), recorded with the signal.
    #[pyo3(signature = (id, name, value, token, by=None))]
    fn signal(
        &self,
        id: &str,
        name: &str,
        value: &Bound<'_, PyAny>,
        token: u64,
        by: Option<&str>,
    ) -> PyResult<()> {
        let _g = InCall::enter();
        let e = self.e()?;
        let payload = wire_payload(value)?;
        e.audited(id, by, || e.signal(id, name, payload, 0, Some(token)))
            .map_err(err)
    }

    #[pyo3(signature = (id, reason, token, by=None))]
    fn cancel(&self, id: &str, reason: &str, token: u64, by: Option<&str>) -> PyResult<()> {
        let _g = InCall::enter();
        let e = self.e()?;
        e.audited(id, by, || e.cancel(id, reason, Some(token)))
            .map_err(err)
    }

    #[pyo3(signature = (id, token, by=None))]
    fn resume(&self, id: &str, token: u64, by: Option<&str>) -> PyResult<()> {
        let _g = InCall::enter();
        let e = self.e()?;
        e.audited(id, by, || e.resume(id, Some(token))).map_err(err)
    }

    #[pyo3(signature = (id, reason, token, by=None))]
    fn terminate(&self, id: &str, reason: &str, token: u64, by: Option<&str>) -> PyResult<()> {
        let _g = InCall::enter();
        let e = self.e()?;
        e.audited(id, by, || e.terminate(id, reason, Some(token)))
            .map_err(err)
    }

    /// `(state, shard, run, status, payload)` with state "notfound", "running" or "done".
    fn result<'py>(
        &self,
        py: Python<'py>,
        id: &str,
    ) -> PyResult<(String, u32, u64, u32, Bound<'py, PyBytes>)> {
        let (shard, state) = self.e()?.result(id).map_err(err)?;
        Ok(match state {
            ResultState::NotFound => ("notfound".into(), shard, 0, 0, PyBytes::new(py, b"")),
            ResultState::Running(run) => ("running".into(), shard, run, 0, PyBytes::new(py, b"")),
            ResultState::Done {
                run,
                status,
                payload,
            } => (
                "done".into(),
                shard,
                run,
                status as u32,
                PyBytes::new(py, &payload),
            ),
        })
    }

    fn inspect(&self, id: &str) -> PyResult<Option<String>> {
        let Some(d) = self.e()?.inspect(id).map_err(err)? else {
            return Ok(None);
        };
        let journal = fusee::journal::normalize(&d.journal);
        let mut v = serde_json::json!({
            "id": d.summary.id, "type": d.summary.wtype, "version": d.summary.version,
            "status": d.summary.status, "started_ms": d.summary.started_ms,
            "finished_ms": d.summary.finished_ms, "error": d.summary.error,
            "shard": d.summary.shard, "run": d.summary.run, "journal": journal,
        });
        if let Some(o) = &d.object {
            v["object"] = serde_json::json!({"state": o.state, "version": o.version,
                "running": o.running, "waiting": o.waiting});
        }
        Ok(Some(v.to_string()))
    }

    /// Marks `name` as an object type with these shared handlers (see
    /// `fusee::object`).
    fn register_object(&self, name: &str, shared: Vec<String>) -> PyResult<()> {
        self.e()?.register_object(name, &shared).map_err(err)
    }

    fn shard_metrics(&self) -> PyResult<String> {
        serde_json::to_string(&self.e()?.shard_metrics())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn list(&self, limit: usize) -> PyResult<String> {
        let runs: Vec<serde_json::Value> = self
            .e()?
            .list(limit)
            .into_iter()
            .map(|s| summary_json(&s))
            .collect();
        Ok(serde_json::Value::Array(runs).to_string())
    }

    fn host_call(
        &self,
        shard: u32,
        op: &str,
        run: RunId,
        args: Vec<String>,
        bufs: Vec<Vec<u8>>,
    ) -> PyResult<Vec<i64>> {
        let _g = InCall::enter();
        self.e()?
            .with_host(shard, |host| host_call(host, op, run, args, bufs))
            .map_err(err)?
    }

    /// Begins a step and, for one that runs, checks its gate (gate 0 for a
    /// read-only one). With `inline`, a lone workflow may flush here
    /// instead of waiting for a tick.
    #[pyo3(signature = (shard, run, name, read_only=false, inline=true))]
    fn begin_step(
        &self,
        shard: u32,
        run: RunId,
        name: &str,
        read_only: bool,
        inline: bool,
    ) -> PyResult<StepStarted> {
        let _g = InCall::enter();
        let e = self.e()?;
        let (seq, replay, gate, attempt, not_before) = e
            .with_host(shard, |host| begin_step(host, run, name))
            .map_err(err)??;
        let open = !replay
            && e.may_run_step(
                shard,
                run,
                if read_only { 0 } else { gate },
                not_before,
                inline,
            )
            .map_err(err)?;
        Ok((seq, replay, gate, attempt, not_before, open))
    }

    #[pyo3(signature = (shard, run, gate, not_before_ms, inline=true))]
    fn may_run(
        &self,
        shard: u32,
        run: RunId,
        gate: u64,
        not_before_ms: u64,
        inline: bool,
    ) -> PyResult<bool> {
        let _g = InCall::enter();
        self.e()?
            .may_run_step(shard, run, gate, not_before_ms, inline)
            .map_err(err)
    }

    /// Records a step's JSON result (or failure text). Python keeps a
    /// successful result, so its delivery does not send it back.
    fn step_done(&self, shard: u32, run: RunId, seq: u32, ok: bool, value: &str) -> PyResult<()> {
        let _g = InCall::enter();
        self.e()?
            .with_host(shard, |host| {
                step_done(host, run, seq, ok, value.as_bytes())
            })
            .map_err(err)?;
        if ok {
            self.held.borrow_mut().insert((shard, run, seq));
        }
        Ok(())
    }

    /// Records a step result that is bytes, without JSON.
    fn step_done_bytes(&self, shard: u32, run: RunId, seq: u32, value: &[u8]) -> PyResult<()> {
        let _g = InCall::enter();
        self.e()?
            .with_host(shard, |host| {
                host.step_done(run, seq, Status::Ok, raw_payload(value))
            })
            .map_err(err)
    }

    /// [`Self::step_done`], then the run's next delivery as
    /// [`Self::quiescent`] gives it: one call where the SDK delivers at
    /// once.
    #[allow(clippy::too_many_arguments)]
    fn step_done_deliver<'py>(
        &self,
        py: Python<'py>,
        shard: u32,
        run: RunId,
        seq: u32,
        ok: bool,
        value: &str,
    ) -> PyResult<Option<Bound<'py, PyTuple>>> {
        self.step_done(shard, run, seq, ok, value)?;
        self.quiescent(py, shard, run)
    }

    fn step_retry(
        &self,
        shard: u32,
        run: RunId,
        seq: u32,
        retry_at_ms: u64,
        error: &[u8],
    ) -> PyResult<u64> {
        let _g = InCall::enter();
        self.e()?
            .with_host(shard, |host| step_retry(host, run, seq, retry_at_ms, error))
            .map_err(err)?
    }

    fn host_bytes<'py>(
        &self,
        py: Python<'py>,
        shard: u32,
        op: &str,
        run: RunId,
        args: Vec<String>,
        bufs: Vec<Vec<u8>>,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let _g = InCall::enter();
        Ok(self
            .e()?
            .with_host(shard, |host| host_bytes(host, op, run, args, bufs))
            .map_err(err)??
            .map(|b| PyBytes::new(py, &b)))
    }

    fn update(
        &self,
        id: &str,
        name: &str,
        update_id: &str,
        arg: &Bound<'_, PyAny>,
        token: u64,
    ) -> PyResult<()> {
        let _g = InCall::enter();
        self.e()?
            .update(id, name, update_id, wire_payload(arg)?, Some(token))
            .map_err(err)
    }

    fn decide_update(&self, shard: u32, token: u64, ok: bool, message: String) -> PyResult<i64> {
        let d = if ok { Ok(()) } else { Err(message) };
        Ok(self
            .e()?
            .decide_update(shard, token, d)
            .map_err(err)?
            .map(|o| o as i64)
            .unwrap_or(-1))
    }

    /// `(shard, run, lsn, foreign)` for answering a query.
    fn query_point(&self, id: &str) -> PyResult<(u32, RunId, u64, bool)> {
        self.e()?.query_route(id).map_err(err)
    }

    fn query_native(&self, id: &str, name: &str, arg: &[u8], token: u64) -> PyResult<()> {
        self.e()?
            .query_native(id, name, payload(arg), token)
            .map_err(err)
    }

    /// Reads stream `name` of `id` from `start` (of run `run` when given);
    /// the answer, JSON, is the reply for `token`.
    #[pyo3(signature = (id, run, name, start, wait_ms, token))]
    fn stream_read(
        &self,
        id: &str,
        run: Option<RunId>,
        name: &str,
        start: u64,
        wait_ms: u64,
        token: u64,
    ) -> PyResult<()> {
        let _g = InCall::enter();
        self.e()?
            .stream_read(id, run, name, start, wait_ms, token)
            .map_err(err)
    }

    fn ack_at(&self, shard: u32, lsn: u64, token: u64) -> PyResult<()> {
        self.e()?.ack_at(shard, lsn, token).map_err(err)
    }

    fn complete(
        &self,
        token: &str,
        ok: bool,
        value: &Bound<'_, PyAny>,
        reply: u64,
    ) -> PyResult<()> {
        let _g = InCall::enter();
        let status = if ok { Status::Ok } else { Status::Failed };
        self.e()?
            .complete(token, status, wire_payload(value)?, Some(reply))
            .map_err(err)
    }

    #[pyo3(signature = (id, before_step, reapply_signals, version, token, by=None))]
    fn reset(
        &self,
        id: &str,
        before_step: &str,
        reapply_signals: bool,
        version: u32,
        token: u64,
        by: Option<&str>,
    ) -> PyResult<()> {
        let e = self.e()?;
        e.audited(id, by, || {
            e.reset(
                id,
                before_step,
                reapply_signals,
                (version > 0).then_some(version),
                Some(token),
            )
        })
        .map(|_| ())
        .map_err(err)
    }

    fn limit(&self, name: &str, concurrent: u32, per_second: f64) -> PyResult<()> {
        self.e()?
            .limit(
                name,
                LimitSpec {
                    concurrent,
                    per_second,
                },
            )
            .map_err(err)
    }

    /// `(permits held, steps waiting)` of a limit.
    fn limit_usage(&self, name: &str) -> PyResult<(u32, usize)> {
        Ok(self.e()?.limit_usage(name))
    }

    fn list_where(&self, filter: &str, limit: usize) -> PyResult<String> {
        let v: serde_json::Value = serde_json::from_str(filter)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed: {e}")))?;
        let f = ListFilter::parse(&v).map_err(err)?;
        let runs: Vec<serde_json::Value> = self
            .e()?
            .list_where(&f, limit)
            .iter()
            .map(summary_json)
            .collect();
        Ok(serde_json::Value::Array(runs).to_string())
    }

    fn validate_schedule(&self, spec: &str) -> Option<String> {
        match serde_json::from_str::<fusee::ScheduleSpec>(spec) {
            Ok(s) => s.validate().err().map(|e| e.to_string()),
            Err(e) => Some(e.to_string()),
        }
    }

    fn stuck(&self, shard: u32, run: RunId, message: String) -> PyResult<()> {
        self.e()?.stuck(shard, run, message).map_err(err)
    }

    /// `(replaying, live work)`, or None without a code slot.
    fn code_state(&self, shard: u32, run: RunId) -> PyResult<Option<(bool, bool)>> {
        self.e()?.code_state(shard, run).map_err(err)
    }

    fn export_journal<'py>(
        &self,
        py: Python<'py>,
        id: &str,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        Ok(self.e()?.inspect(id).map_err(err)?.map(|d| {
            PyBytes::new(
                py,
                &fusee::export::ExportedJournal::from_detail(&d).encode(),
            )
        }))
    }

    /// `None`, or the next delivery as a tuple whose first item is its kind
    /// (see `delivery`).
    fn quiescent<'py>(
        &self,
        py: Python<'py>,
        shard: u32,
        run: RunId,
    ) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let _g = InCall::enter();
        let d = self.e()?.quiescent(shard, run).map_err(err)?;
        delivery(py, shard, run, d, Some(&self.held))
    }

    /// The next delivery of each run (`None` where there is none), in one
    /// call: the SDK delivers to many runs a round at a time.
    fn quiescent_many<'py>(
        &self,
        py: Python<'py>,
        shards: Vec<u32>,
        runs: Vec<RunId>,
    ) -> PyResult<Bound<'py, PyList>> {
        let _g = InCall::enter();
        let e = self.e()?;
        let out = PyList::empty(py);
        for (shard, run) in shards.into_iter().zip(runs) {
            let d = e.quiescent(shard, run).map_err(err)?;
            match delivery(py, shard, run, d, Some(&self.held))? {
                Some(t) => out.append(t)?,
                None => out.append(py.None())?,
            }
        }
        Ok(out)
    }

    /// [`Self::quiescent_many`], each run's held step result (`seqs[i]`
    /// at or above 0, its JSON in `values[i]`) recorded just before its
    /// delivery: the SDK records a step's result with the delivery that
    /// follows it, one native call for both.
    fn quiescent_many_done<'py>(
        &self,
        py: Python<'py>,
        shards: Vec<u32>,
        runs: Vec<RunId>,
        seqs: Vec<i64>,
        values: Vec<Option<String>>,
    ) -> PyResult<Bound<'py, PyList>> {
        {
            let _g = InCall::enter();
            for (((&shard, &run), &seq), value) in shards.iter().zip(&runs).zip(&seqs).zip(&values)
            {
                if let (Ok(seq), Some(value)) = (u32::try_from(seq), value) {
                    self.step_done(shard, run, seq, true, value)?;
                }
            }
        }
        self.quiescent_many(py, shards, runs)
    }

    fn finish(
        &self,
        shard: u32,
        run: RunId,
        status: u32,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let _g = InCall::enter();
        let s = status_from(status);
        let p = wire_payload(value)?;
        let outcome = if s == Status::Ok { Ok(p) } else { Err((s, p)) };
        self.drop_held(shard, run);
        self.e()?.finish(shard, run, outcome).map_err(err)
    }

    /// `(events, extras, next_wake_ms, commits, owed)`. `events` is flat,
    /// six numbers per event, the first its kind (`EV_*`); strings and
    /// payloads are in `extras` (or it is `None`) at the index the sixth
    /// number gives. `commits` holds each shard's commit position: a step
    /// whose gate is at or below it may run. `owed`: a wake raised during
    /// this tick left a shard with work, so tick again soon.
    #[allow(clippy::type_complexity)]
    fn tick<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(
        Vec<u64>,
        Option<Bound<'py, PyList>>,
        Option<u64>,
        Vec<u64>,
        bool,
    )> {
        let _g = InCall::enter();
        let engine = self.e()?;
        self.wake_pending.store(false, Ordering::Release);
        // The GIL stays held across the tick, inline flush included: an
        // inline flush is short (the engine flushes inline only after a fast
        // one), and releasing the GIL for it lets a CPU-bound Python thread
        // keep it for a whole switch interval. `inline_flush=False` sends
        // every flush to the log thread instead.
        let (events, next) = engine.tick();
        engine.check_health().map_err(err)?;
        let owed = self.owed.swap(false, Ordering::AcqRel) && engine.has_dirty();
        let commits = (0..engine.shards())
            .map(|s| engine.commit(s).unwrap_or(0))
            .collect();
        let mut ev = Vec::with_capacity(events.len() * 6);
        let mut extras: Option<Bound<'py, PyList>> = None;
        let mut extra = |x: Bound<'py, PyAny>| -> PyResult<u64> {
            let list = extras.get_or_insert_with(|| PyList::empty(py));
            list.append(x)?;
            Ok((list.len() - 1) as u64)
        };
        for e in events {
            let row: [u64; 6] = match e {
                EngineEvent::Wake { shard, run } => [EV_WAKE, u64::from(shard), run, 0, 0, 0],
                EngineEvent::Ack { token } => [EV_ACK, token, 0, 0, 0, 0],
                EngineEvent::Reject { token } => [EV_REJECT, token, 0, 0, 0, 0],
                EngineEvent::Start { shard, run, info } => {
                    let x = extra(PyString::new(py, &info.id).into_any())?;
                    extra(PyString::new(py, &info.wtype).into_any())?;
                    extra(py_payload(py, &info.input))?;
                    // The trace the run joins, or None.
                    extra(match &info.trace {
                        Some(t) => PyString::new(py, &String::from_utf8_lossy(t)).into_any(),
                        None => py.None().into_bound(py),
                    })?;
                    [
                        EV_START,
                        u64::from(shard),
                        run,
                        u64::from(info.version),
                        info.started_ms,
                        x,
                    ]
                }
                EngineEvent::Stuck {
                    shard,
                    run,
                    message,
                } => {
                    self.drop_held(shard, run);
                    let x = extra(PyString::new(py, &message).into_any())?;
                    [EV_STUCK, u64::from(shard), run, 0, 0, x]
                }
                EngineEvent::Ended { shard, run } => {
                    self.drop_held(shard, run);
                    [EV_ENDED, u64::from(shard), run, 0, 0, 0]
                }
                EngineEvent::Result {
                    shard,
                    run,
                    status,
                    payload,
                } => {
                    let x = extra(py_payload(py, &payload))?;
                    [EV_RESULT, u64::from(shard), run, status as u64, 0, x]
                }
                EngineEvent::Reply {
                    token,
                    status,
                    payload,
                } => {
                    let x = extra(py_payload(py, &payload))?;
                    [EV_REPLY, token, status as u64, 0, 0, x]
                }
                EngineEvent::Refuse { token, message } => {
                    let x = extra(PyString::new(py, &message).into_any())?;
                    [EV_REFUSE, token, 0, 0, 0, x]
                }
            };
            ev.extend_from_slice(&row);
        }
        Ok((ev, extras, next, commits, owed))
    }

    fn next_call(&mut self) -> PyResult<Option<String>> {
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
            .map_err(Into::into)
    }

    fn reply_call(&self, relay: u64, response: String) -> PyResult<()> {
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
            .reply(relay, response.into_bytes())?;
        Ok(())
    }

    /// An invoked worker: `running`, `idle`, `expired` or `lost`; `None`
    /// otherwise (see `fusee_server::invoke`).
    fn invocation_state(&self, awaiting: Vec<String>) -> Option<&'static str> {
        self.connection
            .as_ref()
            .and_then(|c| c.invocation_state(&awaiting))
    }

    fn close(&mut self) -> PyResult<()> {
        if let Some(r) = self.relay.take() {
            r.close();
        }
        let result = self.engine.take().map(|e| e.close()).transpose();
        self.connection.take();
        result.map(|_| ()).map_err(err)
    }
}

/// The test kit: moves the virtual clock of `memory` engines (process-wide)
/// to `offset_ms` ahead of the wall clock.
#[pyfunction]
fn set_test_clock(offset_ms: u64) {
    fusee::local::TEST_CLOCK_MS.store(offset_ms, std::sync::atomic::Ordering::Release);
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(set_test_clock, m)?)?;
    m.add_class::<NativeEngine>()?;
    m.add_class::<NativeConnection>()?;
    m.add_class::<NativeReplicaMetrics>()?;
    m.add_class::<NativeClient>()?;
    m.add_function(wrap_pyfunction!(prepare_connection, m)?)?;
    m.add_function(wrap_pyfunction!(queue_request, m)?)?;
    m.add_class::<NativeReplay>()?;
    Ok(())
}
