//! A normalized, language-neutral view of a journal, used by `inspect` and
//! by the cross-SDK conformance suite. Times, run IDs and recorded random
//! values are left out, so the same workflow produces the same view in every
//! SDK and on every run.

use fusee_core::record::{Record, RecordBuf, WaitOutcome, policy, value_kind};
use serde_json::{Value, json};

/// A payload as JSON: its value, text, or `{"$encoded": base64}`.
pub(crate) fn payload(p: &[u8]) -> Value {
    if p.is_empty() {
        return Value::Null;
    }
    match serde_json::from_slice::<Value>(p) {
        Ok(v) => sort(v),
        Err(_) => match std::str::from_utf8(p) {
            Ok(s) if !p.starts_with(crate::codec::MARK) => Value::String(s.to_owned()),
            _ => encoded(p),
        },
    }
}

/// A payload a codec produced (or other bytes that are not text), as
/// `{"$encoded": base64}`: the UI sends these to the operator's codec
/// endpoint to show them decoded.
fn encoded(p: &[u8]) -> Value {
    let mut b64 = String::with_capacity(p.len().div_ceil(3) * 4);
    crate::stream::base64_encode(p, &mut b64);
    json!({ ENCODED: b64 })
}

/// The key of an encoded payload's JSON form.
pub const ENCODED: &str = "$encoded";

/// Sorts object keys so encoders that order keys differently agree.
fn sort(v: Value) -> Value {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<_> = m.into_iter().collect();
            keys.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Object(keys.into_iter().map(|(k, v)| (k, sort(v))).collect())
        }
        Value::Array(a) => Value::Array(a.into_iter().map(sort).collect()),
        other => other,
    }
}

/// Generated update IDs carry a time; the normalized view leaves it out.
fn update_id(id: &str) -> &str {
    if id.starts_with("auto-") {
        "auto"
    } else if id.starts_with("fusee.send/auto-") {
        "fusee.send/auto"
    } else {
        id
    }
}

/// An object's snapshot without the times its calls were kept at.
fn snapshot(p: &[u8]) -> Value {
    let mut v = payload(p);
    if let Some(Value::Array(calls)) = v.get_mut("calls") {
        for c in calls {
            if let Value::Object(m) = c {
                m.remove("at");
            }
        }
    }
    v
}

fn text(p: &[u8]) -> Value {
    Value::String(String::from_utf8_lossy(p).into_owned())
}

/// A recorded failure as its envelope, without the stack (it names files
/// and lines of one language). Text recorded before the envelope reads as
/// `{"message": text}`.
fn failure(p: &[u8]) -> Value {
    fn strip(v: &mut Value) {
        if let Value::Object(m) = v {
            m.remove("stack");
            if let Some(c) = m.get_mut("cause") {
                strip(c);
            }
        }
    }
    let mut v =
        serde_json::to_value(crate::failure::Failure::from_payload(p)).unwrap_or(Value::Null);
    strip(&mut v);
    sort(v)
}

/// A step's, call's or run's payload by its status.
fn outcome(status: &str, p: &[u8]) -> Value {
    match status {
        "ok" => payload(p),
        "failed" => failure(p),
        _ => text(p),
    }
}

/// A stream record's writer: a step's position, or `"workflow"`.
fn writer(seq: u32) -> Value {
    if seq == fusee_core::record::stream::WORKFLOW {
        json!("workflow")
    } else {
        json!(seq)
    }
}

pub fn normalize_record(r: &RecordBuf) -> Value {
    let mut v = record_json(r);
    // Audit: who asked, when the request carried it.
    let by = match r {
        Record::Start { meta, .. } => meta.as_ref().and_then(|m| m.by.as_ref()),
        Record::Signal { by, .. }
        | Record::Cancel { by, .. }
        | Record::Done { by, .. }
        | Record::Resume { by, .. } => by.as_ref(),
        _ => None,
    };
    if let Some(by) = by {
        v["by"] = text(by);
    }
    if let Record::Start { meta: Some(m), .. } = r
        && let Some(t) = &m.trace
    {
        v["trace"] = text(t);
    }
    v
}

fn record_json(r: &RecordBuf) -> Value {
    match r {
        Record::Start {
            wtype,
            version,
            input,
            parent,
            deadline_ms,
            policy: pol,
            from,
            copies,
            update,
            ..
        } => {
            let input = if pol & policy::OBJECT != 0 {
                snapshot(input)
            } else {
                payload(input)
            };
            let mut v = json!({"kind": "start", "type": &**wtype, "version": version, "input": input,
                    "child": parent.is_some(), "deadline": *deadline_ms > 0, "policy": pol});
            if let Some(o) = from {
                v["from"] = json!(if o.cause == fusee_core::record::origin::RESET {
                    "reset"
                } else {
                    "continued"
                });
            }
            if *copies > 0 {
                v["copies"] = json!(copies);
            }
            if let Some(u) = update {
                v["update"] = json!({"name": &*u.name, "id": update_id(&u.id), "payload": payload(&u.payload)});
            }
            v
        }
        Record::Step {
            seq,
            name,
            attempt,
            status,
            payload: p,
            ..
        } => {
            json!({"kind": "step", "seq": seq, "name": &**name, "attempt": attempt, "status": status.as_str(),
                    "payload": outcome(status.as_str(), p)})
        }
        Record::Attempt {
            seq,
            name,
            attempt,
            error,
            retry_at_ms,
            ..
        } => {
            let mut v = json!({"kind": "attempt", "seq": seq, "name": &**name, "attempt": attempt, "error": failure(error)});
            if *retry_at_ms == fusee_core::record::PAUSED {
                v["paused"] = json!(true);
            }
            v
        }
        Record::Resume { seq, attempt, .. } => {
            json!({"kind": "resume", "seq": seq, "attempt": attempt})
        }
        Record::Timer { seq, name, .. } => json!({"kind": "timer", "seq": seq, "name": &**name}),
        Record::Fired { seq, .. } => json!({"kind": "fired", "seq": seq}),
        Record::Signal {
            name, payload: p, ..
        } => json!({"kind": "signal", "name": &**name, "payload": payload(p)}),
        Record::Wait {
            seq,
            name,
            deadline_ms,
            ..
        } => json!({"kind": "wait", "seq": seq, "name": &**name, "timeout": *deadline_ms > 0}),
        Record::Waited { seq, outcome, .. } => {
            json!({"kind": "waited", "seq": seq, "outcome": match outcome {
                WaitOutcome::Signal(o) => json!({"signal": o}),
                WaitOutcome::TimedOut => json!("timed_out"),
                WaitOutcome::Abandoned => json!("abandoned"),
            }})
        }
        Record::Value { seq, kind, .. } => json!({"kind": "value", "seq": seq, "of": match *kind {
            value_kind::NOW => "now",
            value_kind::RANDOM => "random",
            value_kind::UUID => "uuid",
            _ => "custom",
        }}),
        Record::Child {
            seq,
            wtype,
            id,
            flags,
            input,
            ..
        } if *flags & policy::OBJECT != 0 => {
            // A call to an object; its ID is made from the caller's run.
            let (handler, arg) = match crate::object::decode_call(input) {
                Ok((h, _, a)) => (h, payload(&a)),
                Err(_) => (String::new(), Value::Null),
            };
            json!({"kind": "call", "seq": seq, "type": &**wtype, "id": &**id, "handler": handler,
                    "detached": *flags & policy::DETACHED != 0, "input": arg})
        }
        Record::Child {
            seq,
            wtype,
            id,
            flags,
            input,
            ..
        } => {
            json!({"kind": "child", "seq": seq, "type": &**wtype, "id": &**id,
                    "detached": *flags & policy::DETACHED != 0,
                    "close": match *flags & policy::CLOSE_MASK {
                        policy::CLOSE_CANCEL => "request-cancel",
                        policy::CLOSE_ABANDON => "abandon",
                        _ => "terminate",
                    },
                    "input": payload(input)})
        }
        Record::ChildDone {
            seq,
            status,
            payload: p,
            ..
        } => json!({"kind": "child_done", "seq": seq, "status": status.as_str(),
                    "payload": outcome(status.as_str(), p)}),
        Record::Send {
            seq,
            target,
            name,
            payload: p,
            ..
        } => {
            json!({"kind": "send", "seq": seq, "target": &**target, "name": &**name, "payload": payload(p)})
        }
        Record::Cancel { reason, .. } => json!({"kind": "cancel", "reason": text(reason)}),
        Record::Done {
            status, payload: p, ..
        } => json!({"kind": "done", "status": status.as_str(),
                    "payload": if status.is_handover() {
                        // The next run's start.
                        fusee_core::record::decode_canonical_all(p)
                            .ok()
                            .and_then(|v| v.first().map(normalize_record))
                            .unwrap_or(Value::Null)
                    } else { outcome(status.as_str(), p) }}),
        Record::Update {
            name,
            id,
            payload: p,
            caller,
            ..
        } => {
            let mut v = json!({"kind": "update", "name": &**name, "id": update_id(id), "payload": payload(p)});
            if caller.is_some() {
                // A workflow's call: its ID is made from the caller's run.
                v["id"] = json!("call");
                v["caller"] = json!(true);
            }
            v
        }
        Record::UpdateDone {
            seq,
            ordinal,
            status,
            payload: p,
            state,
            ..
        } => {
            let mut v = json!({"kind": "update_done", "seq": seq, "update": ordinal, "status": status.as_str(),
                    "payload": outcome(status.as_str(), p)});
            if !state.is_empty() {
                v["state"] = payload(state);
            }
            v
        }
        Record::Heartbeat {
            seq, payload: p, ..
        } => json!({"kind": "heartbeat", "seq": seq, "progress": payload(p)}),
        Record::Awaiting {
            seq, name, attempt, ..
        } => json!({"kind": "awaiting", "seq": seq, "name": &**name, "attempt": attempt}),
        Record::Completed {
            seq,
            status,
            payload: p,
            ..
        } => json!({"kind": "completed", "seq": seq, "status": status.as_str(),
                    "payload": outcome(status.as_str(), p)}),
        Record::Patch { at, id, .. } => json!({"kind": "patch", "at": at, "id": &**id}),
        Record::Attributes {
            seq, payload: p, ..
        } => json!({"kind": "attributes", "seq": seq, "attributes": payload(p)}),
        Record::Chunk {
            seq,
            attempt,
            stream,
            offset,
            payload: p,
            ..
        } => json!({"kind": "chunk", "stream": &**stream, "seq": writer(*seq), "attempt": attempt,
                    "offset": offset, "payload": payload(p)}),
        Record::StreamEnd {
            seq,
            stream,
            chunks,
            status,
            ..
        } => {
            json!({"kind": "stream_end", "stream": &**stream, "seq": writer(*seq), "chunks": chunks,
                    "status": status.as_str()})
        }
    }
}

pub fn normalize(journal: &[RecordBuf]) -> Value {
    Value::Array(journal.iter().map(normalize_record).collect())
}

/// When each step began (Unix ms) and how long it took (ms), by sequence
/// number, `{"<seq>": [began, took]}`: the dev profile records them
/// (`STEP_TIMED`). Kept out of [`normalize`], whose journal has no times
/// (snapshot tests compare it). `None` when no step has times.
pub fn step_times(journal: &[RecordBuf]) -> Option<Value> {
    let mut out = serde_json::Map::new();
    for r in journal {
        if let Record::Step {
            seq, time: Some(t), ..
        } = r
        {
            out.insert(seq.to_string(), json!([t.began_ms, t.took_ms]));
        }
    }
    (!out.is_empty()).then_some(Value::Object(out))
}
