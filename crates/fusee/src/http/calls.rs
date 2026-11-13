//! The call protocol every tool speaks: a method name and JSON arguments,
//! answered with a JSON value or an error. Server mode relays these calls
//! to the worker that holds a run's shard; the HTTP API turns its routes
//! into them; an SDK's engine answers them on its own thread through a
//! [`Relay`].
//!
//! | Method | Arguments | Answer |
//! | --- | --- | --- |
//! | `start` | `type`, `id`, `input`, `version`, `options: {timeout, idPolicy}` | `null` |
//! | `updateWithStart` | the same, and `name`, `arg`, `updateId` | the update's result |
//! | `signal` | `id`, `name`, `value` | `null` |
//! | `query` | `id`, `name`, `arg` | the answer |
//! | `update` | `id`, `name`, `arg`, `updateId` | the result |
//! | `result` | `id` | the result |
//! | `cancel`, `terminate` | `id`, `reason` | `null` |
//! | `editState` | `id` (`<type>/<key>`), `edit` (see [`crate::object::edit`]), `updateId` | `{version, changed}` |
//! | `resume` | `id` | `null` (refused if nothing is paused) |
//! | `reset` | `id`, `beforeStep`, `reapplySignals`, `version` | `null` |
//! | `complete` | `token`, `value` | `null` |
//! | `fail` | `token`, `error` (a failure envelope or text) | `null` |
//! | `list` | `where`, `limit` | run summaries |
//! | `describe` | `id` | a summary with `journal` (and, for a key of an object type, `objectDetail`) |
//! | `streamRead` | `id`, `run`, `name`, `from`, `waitMs` | a stream batch |
//! | `types` | | registered workflow types and their versions |
//! | `metrics` | | `{ total, shards }` counters |
//! | `info` | | what serves the calls |
//!
//! An error answers `{"error": message, "kind": snake_case code, "code":
//! Code}`; one that carries a failure sends its envelope as `error`.

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::runtime::RunSummary;
use crate::{Failure, StartOptions};
use fusee_core::record::Status;
use serde_json::{Value, json};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

/// The longest a relayed stream read waits.
pub const MAX_STREAM_WAIT_MS: u64 = 60_000;

pub type CallFuture<'a> = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'a>>;

/// Something that answers protocol calls: an engine, a relay to an SDK's
/// engine, a server.
pub trait Calls: Send + Sync + 'static {
    fn call<'a>(&'a self, method: &'a str, args: Value) -> CallFuture<'a>;
}

impl Calls for Engine {
    fn call<'a>(&'a self, method: &'a str, args: Value) -> CallFuture<'a> {
        Box::pin(async move { run_call(self, method, &args).await })
    }
}

/// The error's kind as calls carry it.
pub fn error_kind(e: &Error) -> &'static str {
    match e {
        Error::NotFound(_) => "not_found",
        Error::AlreadyExists(_) => "already_exists",
        Error::UnknownWorkflow(_) => "unknown_workflow",
        Error::Rejected(_) => "rejected",
        Error::Cancelled(_) => "cancelled",
        Error::TimedOut => "timed_out",
        Error::Terminated => "terminated",
        Error::Busy => "busy",
        Error::Closed => "closed",
        Error::Codec(_) => "codec",
        Error::Connection(_) => "connection",
        Error::Io(_) => "io",
        Error::Determinism(_) => "determinism",
        _ => "failed",
    }
}

/// An error as a call answers it.
pub fn error_json(e: &Error) -> Value {
    let message = match e.failure() {
        Some(_) => String::from_utf8_lossy(&e.to_failure().to_payload()).into_owned(),
        None => e.to_string(),
    };
    json!({"error": message, "kind": error_kind(e), "code": e.code()})
}

/// The error an answer carried.
pub fn decode_error(kind: Option<&str>, msg: &str) -> Error {
    match kind {
        Some("not_found") => Error::NotFound(msg.to_owned()),
        Some("already_exists") => Error::AlreadyExists(msg.to_owned()),
        Some("unknown_workflow") => Error::UnknownWorkflow(msg.to_owned()),
        Some("rejected") => Error::Rejected(msg.to_owned()),
        Some("cancelled") => Error::Cancelled(msg.trim_start_matches("cancelled: ").to_owned()),
        Some("timed_out") => Error::TimedOut,
        Some("terminated") => Error::Terminated,
        Some("busy") => Error::Busy,
        Some("closed") => Error::Closed,
        Some("codec") => Error::Codec(msg.to_owned()),
        Some("connection") => Error::Connection(msg.to_owned()),
        Some("io") => Error::Io(msg.to_owned()),
        Some("determinism") => Error::Determinism(msg.to_owned()),
        _ => Error::from_failure(Failure::from_payload(msg.as_bytes())),
    }
}

/// Refuses a call whose arguments do not have the types the table above
/// gives them, so a malformed request is a `Rejected` error (400 over
/// HTTP) rather than whatever the engine answering it does with a number
/// for a name: an SDK's engine may throw a type error, with its stack. A
/// method not in the table is left to the engine, which refuses it.
pub fn check_args(method: &str, a: &Value) -> Result<()> {
    let bad = |m: String| Err(Error::Rejected(format!("{method}: {m}")));
    let Some(obj) = a.as_object() else {
        return bad("the arguments are a JSON object".into());
    };
    let absent = |k: &str| obj.get(k).is_none_or(Value::is_null);
    let text = |k: &str| match obj.get(k) {
        Some(Value::String(_)) => Ok(()),
        _ => bad(format!("{k} is a string")),
    };
    let opt_text = |k: &str| if absent(k) { Ok(()) } else { text(k) };
    let opt_count = |k: &str| match obj.get(k) {
        _ if absent(k) => Ok(()),
        Some(v) if v.as_u64().is_some() => Ok(()),
        _ => bad(format!("{k} is a whole number of 0 or more")),
    };
    let opt_bool = |k: &str| match obj.get(k) {
        _ if absent(k) => Ok(()),
        Some(Value::Bool(_)) => Ok(()),
        _ => bad(format!("{k} is true or false")),
    };
    let opt_object = |k: &str| match obj.get(k) {
        _ if absent(k) => Ok(()),
        Some(Value::Object(_)) => Ok(()),
        _ => bad(format!("{k} is an object")),
    };
    // A start's options: `timeout`, `delay` and `at` are ms (an SDK also
    // takes a duration such as "10s"), `idPolicy` is an `IdPolicy`.
    let start = || -> Result<()> {
        text("type")?;
        text("id")?;
        opt_count("version")?;
        opt_object("options")?;
        let Some(Value::Object(o)) = obj.get("options") else {
            return Ok(());
        };
        for k in ["timeout", "delay", "at"] {
            match o.get(k) {
                None | Some(Value::Null | Value::String(_)) => {}
                Some(v) if v.as_u64().is_some() => {}
                _ => return bad(format!("options.{k} is ms or a duration such as \"10s\"")),
            }
        }
        for k in ["trace", "concurrencyKey"] {
            if !matches!(o.get(k), None | Some(Value::Null | Value::String(_))) {
                return bad(format!("options.{k} is a string"));
            }
        }
        match o.get("idPolicy") {
            None | Some(Value::Null) => Ok(()),
            Some(p) => serde_json::from_value::<crate::IdPolicy>(p.clone())
                .map(|_| ())
                .or_else(|e| bad(format!("options.idPolicy: {e}"))),
        }
    };
    match method {
        "start" => start(),
        "signalWithStart" => {
            start()?;
            text("name")
        }
        "updateWithStart" => {
            start()?;
            text("name")?;
            opt_text("updateId")
        }
        "signal" | "query" => {
            text("id")?;
            text("name")
        }
        "update" => {
            text("id")?;
            text("name")?;
            opt_text("updateId")
        }
        "result" | "describe" | "resume" | "export" => text("id"),
        "cancel" | "terminate" => {
            text("id")?;
            opt_text("reason")
        }
        "reset" => {
            text("id")?;
            text("beforeStep")?;
            opt_bool("reapplySignals")?;
            opt_count("version")
        }
        "editState" => {
            text("id")?;
            match obj.get("edit") {
                Some(Value::Object(_)) => {}
                _ => return bad("edit is an object".into()),
            }
            opt_text("updateId")
        }
        "complete" => text("token"),
        "fail" => {
            text("token")?;
            match obj.get("error") {
                None | Some(Value::Null | Value::String(_) | Value::Object(_)) => Ok(()),
                _ => bad("error is a string or a failure envelope".into()),
            }
        }
        "list" => {
            opt_object("where")?;
            opt_count("limit")?;
            match obj.get("shards") {
                _ if absent("shards") => Ok(()),
                Some(Value::Array(s)) if s.iter().all(|n| n.as_u64().is_some()) => Ok(()),
                _ => bad("shards is a list of shard numbers".into()),
            }
        }
        "streamRead" => {
            text("id")?;
            text("name")?;
            opt_count("run")?;
            opt_count("from")?;
            opt_count("waitMs")
        }
        _ => Ok(()),
    }
}

/// An answer, as a value or the error it carried.
pub fn decode_answer(answer: &Value) -> Result<Value> {
    match answer.get("error") {
        Some(e) => Err(decode_error(
            answer.get("kind").and_then(Value::as_str),
            e.as_str().unwrap_or_default(),
        )),
        None => Ok(answer.get("value").cloned().unwrap_or(Value::Null)),
    }
}

/// A result as a call answers it: `{"value": ...}` or the error.
pub fn answer(r: Result<Value>) -> Value {
    match r {
        Ok(v) => json!({ "value": v }),
        Err(e) => error_json(&e),
    }
}

fn bytes(v: &Value) -> Arc<[u8]> {
    Arc::from(v.to_string().into_bytes())
}

/// A payload as an answer carries it; one a codec encrypted reads as
/// `{"$encoded": base64}` (see [`crate::journal`]).
fn parse(p: &[u8]) -> Value {
    serde_json::from_slice(p).unwrap_or_else(|_| crate::journal::payload(p))
}

/// A start's options as calls carry them.
pub fn wire_options(options: &StartOptions) -> Value {
    json!({"timeout":options.timeout.map(|t| u64::try_from(t.as_millis()).unwrap_or(u64::MAX)),
        "idPolicy":options.id_policy})
}

/// A start's options from a call's arguments.
pub fn start_options(args: &Value) -> Result<StartOptions> {
    let opts = &args["options"];
    let version = serde_json::from_value(args["version"].clone())?;
    let timeout: Option<u64> = serde_json::from_value(opts["timeout"].clone())?;
    let id_policy = if opts["idPolicy"].is_null() {
        Default::default()
    } else {
        serde_json::from_value(opts["idPolicy"].clone())?
    };
    // `delay` (ms from now) or `at` (Unix ms): a delayed start.
    let delay: Option<u64> = serde_json::from_value(opts["delay"].clone())?;
    let at: Option<u64> = serde_json::from_value(opts["at"].clone())?;
    // `trace`: the W3C traceparent the run joins (an HTTP request's
    // `traceparent` header).
    let trace = opts["trace"]
        .as_str()
        .filter(|t| is_traceparent(t))
        .map(str::to_owned);
    Ok(StartOptions {
        timeout: timeout.map(Duration::from_millis),
        version,
        id_policy,
        at_ms: at.or(delay.map(|d| crate::local::unix_ms() + d)),
        trace,
        concurrency_key: opts["concurrencyKey"].as_str().map(str::to_owned),
    })
}

/// A W3C `traceparent` (`00-<32 hex>-<16 hex>-<2 hex>`); anything else is
/// not recorded.
pub fn is_traceparent(t: &str) -> bool {
    let p: Vec<&str> = t.split('-').collect();
    let hex = |s: &str, n: usize| s.len() == n && s.bytes().all(|b| b.is_ascii_hexdigit());
    p.len() == 4 && hex(p[0], 2) && hex(p[1], 32) && hex(p[2], 16) && hex(p[3], 2)
}

fn update_result((status, payload): (Status, Arc<[u8]>)) -> Result<Value> {
    if status == Status::Ok {
        Ok(parse(&payload))
    } else {
        Err(Error::from_failure(Failure::from_payload(&payload)))
    }
}

/// A run's summary as calls carry it.
pub fn summary_json(r: &RunSummary) -> Value {
    json!({"id":r.id,"type":r.wtype,"version":r.version, "status":r.status,"shard":r.shard,
        "run":r.run,"startedMs":r.started_ms,"finishedMs":r.finished_ms, "error":r.error,
        "attributes":serde_json::from_str::<Value>(&r.attributes).unwrap_or(Value::Null),
        "object": r.object})
}

/// Standard base64 (for files inside JSON answers).
pub fn base64(b: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(b.len().div_ceil(3) * 4);
    for c in b.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if c.len() > 1 {
            T[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if c.len() > 2 {
            T[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Decodes standard base64.
pub fn unbase64(s: &str) -> Option<Vec<u8>> {
    let val = |c: u8| -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        } as u32)
    };
    let s: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    for c in s.chunks(4) {
        let pad = c.iter().filter(|&&b| b == b'=').count();
        let mut n = 0u32;
        for (i, &b) in c.iter().enumerate() {
            n |= if b == b'=' { 0 } else { val(b)? } << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Some(out)
}

/// Answers one call against a Rust engine.
pub async fn run_call(engine: &Engine, method: &str, a: &Value) -> Result<Value> {
    // The requester an API named (audit).
    match a.get("by").and_then(Value::as_str) {
        Some(by) if AUDITED.contains(&method) && !by.is_empty() => {
            crate::audit::as_requester(by, run_call_as(engine, method, a)).await
        }
        _ => run_call_as(engine, method, a).await,
    }
}

async fn run_call_as(engine: &Engine, method: &str, a: &Value) -> Result<Value> {
    let s = |k: &str| {
        a.get(k)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let v = |k: &str| a.get(k).cloned().unwrap_or(Value::Null);
    match method {
        "complete" => engine
            .complete(&s("token"), &v("value"))
            .await
            .map(|()| Value::Null),
        "fail" => {
            let error = match a.get("error") {
                Some(Value::String(t)) => t.clone(),
                Some(other) => other.to_string(),
                None => String::new(),
            };
            engine.fail(&s("token"), &error).await.map(|()| Value::Null)
        }
        "start" => {
            let mut opts = start_options(a)?;
            match opts.at_ms.take() {
                Some(at) => engine
                    .start_delayed(&s("type"), &s("id"), bytes(&v("input")), at, opts)
                    .await
                    .map(|()| Value::Null),
                None => engine
                    .start_raw(&s("type"), &s("id"), bytes(&v("input")), opts)
                    .await
                    .map(|()| Value::Null),
            }
        }
        "signal" => engine
            .signal_raw(&s("id"), &s("name"), bytes(&v("value")))
            .await
            .map(|()| Value::Null),
        "signalWithStart" => engine
            .signal_with_start_raw(
                &s("type"),
                &s("id"),
                bytes(&v("input")),
                start_options(a)?,
                &s("name"),
                bytes(&v("value")),
            )
            .await
            .map(|()| Value::Null),
        "updateWithStart" => update_result(
            engine
                .update_with_start_raw(
                    &s("type"),
                    &s("id"),
                    bytes(&v("input")),
                    start_options(a)?,
                    &s("name"),
                    &s("updateId"),
                    bytes(&v("arg")),
                )
                .await?,
        ),
        "update" => update_result(
            engine
                .update_raw(&s("id"), &s("name"), &s("updateId"), bytes(&v("arg")))
                .await?,
        ),
        "reset" => engine
            .reset(
                &s("id"),
                &s("beforeStep"),
                a["reapplySignals"].as_bool().unwrap_or(true),
            )
            .await
            .map(|_| Value::Null),
        "list" => {
            let limit = serde_json::from_value::<Option<usize>>(a["limit"].clone())?.unwrap_or(100);
            let filter = a.get("where").cloned().unwrap_or_else(|| json!({}));
            // The server sends a listing to every worker holding shards,
            // each for the shards it holds.
            let results = match serde_json::from_value::<Option<Vec<u32>>>(a["shards"].clone())? {
                Some(shards) => engine.list_where_in(&shards, &filter, limit).await?,
                None => engine.list_where(&filter, limit).await?,
            };
            Ok(Value::Array(results.iter().map(summary_json).collect()))
        }
        "describe" => match engine.describe(&s("id")).await? {
            None => Err(Error::NotFound(s("id"))),
            Some(d) => {
                let mut out = summary_json(&d.summary);
                out["journal"] = Value::Array(
                    d.journal
                        .iter()
                        .map(crate::journal::normalize_record)
                        .collect(),
                );
                if let Some(t) = crate::journal::step_times(&d.journal) {
                    out["stepTimes"] = t;
                }
                if let Some(o) = &d.object {
                    out["objectDetail"] = json!({"state": o.state, "version": o.version,
                        "running": o.running, "waiting": o.waiting});
                }
                Ok(out)
            }
        },
        "query" => engine
            .query_raw(&s("id"), &s("name"), bytes(&v("arg")))
            .await
            .map(|p| parse(&p)),
        "result" => {
            let (status, p) = engine.result_raw(&s("id")).await?;
            match status {
                Status::Ok => Ok(parse(&p)),
                Status::Cancelled => Err(Error::Cancelled(String::from_utf8_lossy(&p).into())),
                Status::TimedOut => Err(Error::TimedOut),
                Status::Terminated => Err(Error::Terminated),
                _ => Err(Error::from_failure(Failure::from_payload(&p))),
            }
        }
        "cancel" => engine
            .run::<Value>(&s("id"))
            .cancel(&s("reason"))
            .await
            .map(|()| Value::Null),
        "resume" => engine
            .run::<Value>(&s("id"))
            .resume()
            .await
            .map(|()| Value::Null),
        "editState" => Box::pin(run_call(engine, "updateWithStart", &edit_state_call(a)?)).await,
        "terminate" => engine
            .run::<Value>(&s("id"))
            .terminate(&s("reason"))
            .await
            .map(|()| Value::Null),
        "streamRead" => {
            let wait = a["waitMs"]
                .as_u64()
                .unwrap_or(crate::stream::WAIT_MS)
                .min(MAX_STREAM_WAIT_MS);
            let p = engine
                .stream_read_raw(
                    &s("id"),
                    a["run"].as_u64(),
                    &s("name"),
                    a["from"].as_u64().unwrap_or(0),
                    Duration::from_millis(wait),
                )
                .await?;
            Ok(serde_json::from_slice(&p)?)
        }
        "types" => Ok(engine.types_json()),
        "metrics" => {
            let shards = engine.shard_metrics().await?;
            let total = engine.metrics().await?;
            let mut total = serde_json::to_value(total)?;
            let mut shards = shards
                .iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            // Gauges of runs that need someone: read from the run list (a
            // tool's path, not the step path).
            for (status, key) in [
                ("stuck", "runs_stuck"),
                ("paused", "runs_paused"),
                ("waiting_for_code", "runs_waiting_for_code"),
            ] {
                let mut per = vec![0u64; shards.len()];
                for r in engine
                    .list_where(&json!({ "status": status }), usize::MAX)
                    .await?
                {
                    if let Some(n) = per.get_mut(r.shard as usize) {
                        *n += 1;
                    }
                }
                total[key] = json!(per.iter().sum::<u64>());
                for (s, n) in shards.iter_mut().zip(per) {
                    s[key] = json!(n);
                }
            }
            Ok(json!({ "total": total, "shards": shards }))
        }
        "info" => Ok(engine.info_json()),
        "limits" => Ok(serde_json::to_value(engine.limits())?),
        "export" => match engine.describe(&s("id")).await? {
            None => Err(Error::NotFound(s("id"))),
            Some(d) => Ok(json!({
                "file": base64(&crate::export::ExportedJournal::from_detail(&d).encode()),
                "name": crate::export::file_name(&s("id")),
            })),
        },
        other => Err(Error::Rejected(format!("unknown call '{other}'"))),
    }
}

/// The calls that change a run, whose records name who asked (audit).
pub const AUDITED: &[&str] = &[
    "signal",
    "signalWithStart",
    "cancel",
    "resume",
    "terminate",
    "reset",
    "editState",
];

/// An `editState` call as the call it is: `updateWithStart` of the key's
/// edit handler (starting the key's run if it has none), with who asked in
/// the edit, which its `Update` record keeps.
pub fn edit_state_call(a: &Value) -> Result<Value> {
    let id = a["id"].as_str().unwrap_or_default();
    let Some((wtype, key)) = id
        .split_once('/')
        .filter(|(t, k)| !t.is_empty() && !k.is_empty())
    else {
        return Err(Error::Rejected(format!(
            "editState takes the key's workflow ID, <type>/<key>, not '{id}'"
        )));
    };
    let mut edit = a["edit"].clone();
    if !edit.is_object() {
        return Err(Error::Rejected("editState's edit is a JSON object".into()));
    }
    if let Some(by) = a["by"].as_str().filter(|b| !b.is_empty()) {
        edit["by"] = json!(by);
    }
    Ok(json!({
        "type": wtype,
        "id": crate::object::object_id(wtype, key),
        "input": null,
        "name": crate::object::EDIT_HANDLER,
        "arg": edit,
        "updateId": a["updateId"].as_str().unwrap_or_default(),
        "version": null,
        "options": {"timeout": null, "idPolicy": {"running": "use-existing", "finished": "allow"}},
    }))
}

/// Calls that name their requester: the HTTP API's token or proxy
/// identity goes into the arguments of the [`AUDITED`] calls as `by`
/// (replacing any the request gave), so it reaches the engine that runs
/// them, across a server too.
pub struct Stamped {
    inner: Arc<dyn Calls>,
    by: String,
    trace: Option<String>,
}

impl Stamped {
    /// `trace`: the request's W3C `traceparent` header, which starts the
    /// request makes join.
    pub fn new(inner: Arc<dyn Calls>, by: String, trace: Option<String>) -> Self {
        Self { inner, by, trace }
    }
}

impl Calls for Stamped {
    fn call<'a>(&'a self, method: &'a str, mut args: Value) -> CallFuture<'a> {
        if AUDITED.contains(&method) && args.is_object() {
            args["by"] = json!(self.by);
        }
        if let Some(t) = &self.trace
            && matches!(method, "start" | "signalWithStart" | "updateWithStart")
            && args.is_object()
        {
            if !args["options"].is_object() {
                args["options"] = json!({});
            }
            if args["options"]["trace"].is_null() {
                args["options"]["trace"] = json!(t);
            }
        }
        self.inner.call(method, args)
    }
}

/// Calls from a request, each refused unless its arguments have the
/// protocol's types (see [`check_args`]): the HTTP API's outermost layer.
pub struct Checked(pub Arc<dyn Calls>);

impl Calls for Checked {
    fn call<'a>(&'a self, method: &'a str, args: Value) -> CallFuture<'a> {
        match check_args(method, &args) {
            Ok(()) => self.0.call(method, args),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }
}

/// How a refusal for a namespace's token begins (answered with 403).
pub const OUTSIDE_NAMESPACE: &str = "this token reaches namespace";

/// Calls limited to one namespace, a prefix of the workflow ID: what a
/// namespace's token reaches. Calls on a run need its ID under
/// `<namespace>/`, a completion token must be for such a run, lists show
/// only those runs, and of the calls on the engine as a whole only `info`,
/// `types` and `limits` answer (schedules and objects, whose IDs start
/// with their own prefixes, need the full token).
pub struct Namespaced {
    inner: Arc<dyn Calls>,
    namespace: String,
    prefix: String,
}

impl Namespaced {
    pub fn new(inner: Arc<dyn Calls>, namespace: &str) -> Self {
        let namespace = namespace.trim_end_matches('/').to_owned();
        let prefix = format!("{namespace}/");
        Self {
            inner,
            namespace,
            prefix,
        }
    }

    fn owns(&self, id: &str) -> bool {
        id.len() > self.prefix.len() && id.starts_with(&self.prefix)
    }

    fn refuse(&self, why: &str) -> Error {
        Error::Rejected(format!(
            "{OUTSIDE_NAMESPACE} '{}' only: {why}",
            self.namespace
        ))
    }
}

impl Calls for Namespaced {
    fn call<'a>(&'a self, method: &'a str, mut args: Value) -> CallFuture<'a> {
        Box::pin(async move {
            match method {
                "start" | "signal" | "signalWithStart" | "updateWithStart" | "update" | "reset"
                | "describe" | "query" | "result" | "cancel" | "resume" | "terminate"
                | "streamRead" | "export" | "editState" => {
                    let id = args.get("id").and_then(Value::as_str).unwrap_or_default();
                    if !self.owns(id) {
                        return Err(self.refuse(&format!(
                            "workflow ID '{id}' does not start with '{}'",
                            self.prefix
                        )));
                    }
                    // The engine's own types (schedules, delayed starts,
                    // queue slots, debounce and batch coordinators) take the
                    // IDs of the runs they start or follow from their input,
                    // so one under the namespace's ID could reach any run.
                    // Their callers start them for a run's own ID instead
                    // (`start` with `delay`, a limited type's start).
                    if matches!(method, "start" | "signalWithStart" | "updateWithStart")
                        && let Some(t) = args.get("type").and_then(Value::as_str)
                        && t.starts_with("fusee.")
                    {
                        return Err(self
                            .refuse(&format!("the engine's own type '{t}' needs the full token")));
                    }
                }
                "complete" | "fail" => {
                    let token = args
                        .get("token")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    match crate::runtime::parse_token(token) {
                        Some((id, ..)) if self.owns(&id) => {}
                        Some(_) => {
                            return Err(self.refuse("the completion is for a run outside it"));
                        }
                        None => return Err(Error::Rejected("not a completion token".into())),
                    }
                }
                "list" => {
                    if !args.is_object() {
                        args = json!({});
                    }
                    let mut filter = match args.get("where") {
                        Some(Value::Object(m)) => m.clone(),
                        _ => Default::default(),
                    };
                    // The namespace as an ID range ('0' follows '/'), unless
                    // the filter names IDs itself; the rows are checked
                    // either way.
                    filter.entry("id").or_insert_with(
                        || json!({"gte": self.prefix, "lt": format!("{}0", self.namespace)}),
                    );
                    args["where"] = Value::Object(filter);
                    let rows = self.inner.call(method, args).await?;
                    return Ok(Value::Array(
                        rows.as_array()
                            .into_iter()
                            .flatten()
                            .filter(|r| r["id"].as_str().is_some_and(|id| self.owns(id)))
                            .cloned()
                            .collect(),
                    ));
                }
                "info" => {
                    let mut info = self.inner.call(method, args).await?;
                    info["namespace"] = json!(self.namespace);
                    return Ok(info);
                }
                "types" | "limits" => {}
                other => return Err(self.refuse(&format!("'{other}' needs the full token"))),
            }
            self.inner.call(method, args).await
        })
    }
}

/// One call waiting for an SDK's engine to answer it.
pub struct RelayedCall {
    pub id: u64,
    pub method: String,
    pub args: Value,
}

struct RelayState {
    next: u64,
    queue: std::collections::VecDeque<RelayedCall>,
    waiting: rustc_hash::FxHashMap<u64, oneshot::Sender<Value>>,
    closed: bool,
}

/// Calls for an engine that lives on another thread (an SDK's): the HTTP
/// thread queues each call and wakes the engine's thread, which takes them
/// with [`next`](Self::next) and answers with [`reply`](Self::reply).
#[derive(Clone)]
pub struct Relay {
    state: Arc<Mutex<RelayState>>,
    wake: Arc<dyn Fn() + Send + Sync>,
}

/// Relay IDs start here, above a server connection's, so one host can
/// answer both.
pub const RELAY_BASE: u64 = 1 << 62;

impl Relay {
    /// `wake` makes the engine's thread take the calls (a wake byte, a
    /// condition variable).
    pub fn new(wake: Arc<dyn Fn() + Send + Sync>) -> Self {
        Self {
            state: Arc::new(Mutex::new(RelayState {
                next: RELAY_BASE,
                queue: Default::default(),
                waiting: Default::default(),
                closed: false,
            })),
            wake,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RelayState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The next call to answer, on the engine's thread.
    pub fn next(&self) -> Option<RelayedCall> {
        self.lock().queue.pop_front()
    }

    /// Answers call `id` with `{"value": ...}` or an error (see the module).
    pub fn reply(&self, id: u64, answer: Value) -> bool {
        match self.lock().waiting.remove(&id) {
            Some(tx) => tx.send(answer).is_ok(),
            None => false,
        }
    }

    /// Whether `id` is one of this relay's calls.
    pub fn owns(id: u64) -> bool {
        id >= RELAY_BASE
    }

    /// Refuses every call from now on (the engine closed).
    pub fn close(&self) {
        let mut st = self.lock();
        st.closed = true;
        st.queue.clear();
        st.waiting.clear();
    }
}

impl Calls for Relay {
    fn call<'a>(&'a self, method: &'a str, args: Value) -> CallFuture<'a> {
        Box::pin(async move {
            // The SDK's engine answers the call an edit is.
            let (method, args) = if method == "editState" {
                ("updateWithStart", edit_state_call(&args)?)
            } else {
                (method, args)
            };
            let rx = {
                let mut st = self.lock();
                if st.closed {
                    return Err(Error::Closed);
                }
                let id = st.next;
                st.next += 1;
                let (tx, rx) = oneshot::channel();
                st.waiting.insert(id, tx);
                st.queue.push_back(RelayedCall {
                    id,
                    method: method.to_owned(),
                    args,
                });
                rx
            };
            (self.wake)();
            match rx.await {
                Ok(answer) => decode_answer(&answer),
                Err(_) => Err(Error::Closed),
            }
        })
    }
}
