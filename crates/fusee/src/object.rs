//! Keyed objects: handlers per key with key/value state, built from Fusee's
//! records.
//!
//! An object type is a set of handlers; each key of the type is one object
//! with its own key/value state (JSON values under string keys). A key is a
//! long-lived run with workflow ID `<type>/<key>`, marked
//! [`policy::OBJECT`] in its `Start` record, and each call is an update of
//! that run. So there is no new kind of storage: a key is recovered,
//! replicated, parked, listed and exported as a run, and every client that
//! can send an update-with-start can call an object.
//!
//! What the engine adds (in [`crate::runtime`]):
//!
//! - **Exclusive calls run one at a time per key**, in the order they reach
//!   the key's shard. A call is *accepted* (its `Update` record appended)
//!   when its handler starts; an exclusive call that arrives while another
//!   runs waits for the key, in memory, in arrival order. Shared calls are
//!   accepted at once.
//! - **State lives next to the run.** Reads are synchronous host calls and
//!   are not journaled. An exclusive handler's writes are its own until it
//!   returns; then they are committed with its result, in its `UpdateDone`
//!   record ([`encode_delta`]). A failed handler commits nothing. Commits
//!   happen when handlers return, at positions the journal fixes, so a
//!   replayed handler reads what it read the first time.
//! - **Calls from workflow code** are `Child` operations flagged
//!   [`policy::OBJECT`] ([`encode_call`]): they go through the durable
//!   outbox, the key's `Update` record names the caller, and its
//!   `UpdateDone` sends the result back as a child result.
//! - **Retries with an ID run once**: within a run by the update ID, and
//!   across compaction through the calls the snapshot keeps ([`Kept`]).
//! - **Compaction**: when no handler runs and the key's run has made
//!   [`COMPACT_OPS`] journal operations, or it is idle and about to be
//!   parked, the engine continues the run as new with a [`Snapshot`] as the
//!   new run's input. Loading or recovering a key replays at most that much
//!   handler code, and an idle key's journal is one record.
//!
//! See `docs/features.md#objects-keyed-state` for the semantics.

use crate::ctx::{Ctx, decode, encode};
use crate::defs::{Make, Op};
use crate::engine::{Engine, StartOptions, outcome_of, status_error};
use crate::error::{Error, Result};
use crate::runtime::{LocalFuture, RunDetail, RunOutcome, WorkflowFn};
use fusee_core::record::{Name, Payload, Record, RecordBuf, Status, policy};
use fusee_core::wire::{Reader, put_bytes};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

pub mod edit;

pub use edit::EDIT_HANDLER;

/// The query every object answers with its committed state.
pub const STATE_QUERY: &str = "fusee.state";
/// Journal operations after which a key's run is compacted at the next
/// moment no handler runs.
pub const COMPACT_OPS: u32 = 128;
/// Calls with IDs a key remembers across compaction, at most.
pub const KEPT_CALLS: usize = 1_024;
/// How long a key remembers a call's ID and result after compaction.
pub const KEPT_CALL_MS: u64 = 3_600_000;
/// Calls that may wait for one key at once; more are refused as busy.
pub const MAX_WAITING: usize = 10_000;
/// Update IDs of one-way calls (`send`) start with this: their callers are
/// answered once the call is accepted, not when it finishes.
pub const SEND_PREFIX: &str = "fusee.send/";

/// The workflow ID of key `key` of object type `wtype`.
pub fn object_id(wtype: &str, key: &str) -> String {
    format!("{wtype}/{key}")
}

/// The key of an object run's workflow ID.
pub fn key_of<'a>(id: &'a str, wtype: &str) -> Option<&'a str> {
    id.strip_prefix(wtype)?.strip_prefix('/')
}

/// Whether an update ID was made up by the engine (no caller ID to keep).
pub fn is_auto_id(id: &str) -> bool {
    let id = id.strip_prefix(SEND_PREFIX).unwrap_or(id);
    id.is_empty() || id.starts_with("auto-")
}

/// Whether the update is a one-way call.
pub fn is_send(id: &str) -> bool {
    id.starts_with(SEND_PREFIX)
}

/// What the runtime knows about an object type: which handlers are shared
/// (every other handler is exclusive).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObjectDef {
    pub shared: FxHashSet<String>,
}

impl ObjectDef {
    pub fn new<I: IntoIterator<Item = S>, S: Into<String>>(shared: I) -> Self {
        Self {
            shared: shared.into_iter().map(Into::into).collect(),
        }
    }

    pub fn is_shared(&self, handler: &str) -> bool {
        self.shared.contains(handler)
    }
}

/// Checks an object type's name: it is the first part of its keys' IDs.
pub fn check_type(name: &str) -> Result<()> {
    if name.is_empty() || name.contains('/') {
        return Err(Error::Rejected(format!(
            "an object type's name is not empty and has no '/': '{name}'"
        )));
    }
    Ok(())
}

/// A call from workflow code, as a `Child` record's input: the handler, the
/// call's ID (made from the caller's journal position) and the argument.
pub fn encode_call(handler: &str, id: &str, arg: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(handler.len() + id.len() + arg.len() + 4);
    put_bytes(&mut out, handler.as_bytes());
    put_bytes(&mut out, id.as_bytes());
    out.extend_from_slice(arg);
    out
}

/// [`encode_call`] back: `(handler, id, argument)`.
pub fn decode_call(p: &[u8]) -> Result<(String, String, Payload)> {
    let bad = |_| Error::Codec("a malformed object call".into());
    let mut r = Reader::new(p);
    let handler = r.str().map_err(bad)?.to_owned();
    let id = r.str().map_err(bad)?.to_owned();
    Ok((handler, id, Arc::from(r.rest())))
}

/// A finished call a key remembers across compaction, so a retry with its
/// ID gets its result instead of running again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Kept {
    pub id: Name,
    pub status: Status,
    /// The result as the caller gets it (plain, not sealed).
    pub result: Payload,
    pub at_ms: u64,
}

/// A key's state and remembered calls: the input of each of its runs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// JSON values, as their handlers encoded them.
    pub state: BTreeMap<String, Payload>,
    /// Oldest first.
    pub calls: VecDeque<Kept>,
}

fn json_str(s: &str, out: &mut Vec<u8>) {
    // A string cannot fail to encode.
    let _ = serde_json::to_writer(&mut *out, s);
}

/// Appends `{"k":v,...}` from raw JSON values.
fn object_json<'a>(
    entries: impl Iterator<Item = (&'a String, Option<&'a Payload>)>,
    out: &mut Vec<u8>,
) {
    out.push(b'{');
    let mut first = true;
    for (k, v) in entries {
        if !first {
            out.push(b',');
        }
        first = false;
        json_str(k, out);
        out.push(b':');
        match v {
            Some(v) if !v.is_empty() => out.extend_from_slice(v),
            _ => out.extend_from_slice(b"null"),
        }
    }
    out.push(b'}');
}

/// A JSON value as raw bytes, as a handler would have encoded it.
fn raw(v: &Value) -> Payload {
    Arc::from(serde_json::to_vec(v).unwrap_or_else(|_| b"null".to_vec()))
}

fn status_name(s: Status) -> &'static str {
    s.as_str()
}

fn status_of(name: &str) -> Status {
    match name {
        "ok" => Status::Ok,
        "cancelled" => Status::Cancelled,
        "timed_out" => Status::TimedOut,
        "terminated" => Status::Terminated,
        _ => Status::Failed,
    }
}

impl Snapshot {
    /// `{"state":{...},"calls":[{"id","status","result"|"error"|"result64","at"}]}`,
    /// fields left out when empty (an empty key is `{}`).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(b'{');
        if !self.state.is_empty() {
            out.extend_from_slice(b"\"state\":");
            object_json(self.state.iter().map(|(k, v)| (k, Some(v))), &mut out);
        }
        if !self.calls.is_empty() {
            if !self.state.is_empty() {
                out.push(b',');
            }
            out.extend_from_slice(b"\"calls\":[");
            for (i, c) in self.calls.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(b"{\"id\":");
                json_str(&c.id, &mut out);
                out.extend_from_slice(b",\"status\":");
                json_str(status_name(c.status), &mut out);
                let json = c.status == Status::Ok
                    && serde_json::from_slice::<serde::de::IgnoredAny>(&c.result).is_ok();
                if json {
                    out.extend_from_slice(b",\"result\":");
                    out.extend_from_slice(&c.result);
                } else if c.status == Status::Ok {
                    let mut s = String::new();
                    crate::stream::base64_encode(&c.result, &mut s);
                    out.extend_from_slice(b",\"result64\":");
                    json_str(&s, &mut out);
                } else {
                    out.extend_from_slice(b",\"error\":");
                    json_str(&String::from_utf8_lossy(&c.result), &mut out);
                }
                out.extend_from_slice(format!(",\"at\":{}}}", c.at_ms).as_bytes());
            }
            out.push(b']');
        }
        out.push(b'}');
        out
    }

    pub fn decode(p: &[u8]) -> Result<Self> {
        if p.is_empty() {
            return Ok(Self::default());
        }
        let v: Value = serde_json::from_slice(p)?;
        let mut snap = Self::default();
        if let Some(Value::Object(m)) = v.get("state") {
            for (k, v) in m {
                if !v.is_null() {
                    snap.state.insert(k.clone(), raw(v));
                }
            }
        }
        if let Some(Value::Array(calls)) = v.get("calls") {
            for c in calls {
                let id = c.get("id").and_then(Value::as_str).unwrap_or_default();
                let status = status_of(c.get("status").and_then(Value::as_str).unwrap_or("failed"));
                let result = if let Some(r) = c.get("result") {
                    raw(r)
                } else if let Some(r) = c.get("result64").and_then(Value::as_str) {
                    Arc::from(crate::stream::base64_decode(r).unwrap_or_default())
                } else {
                    Arc::from(
                        c.get("error")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .as_bytes(),
                    )
                };
                snap.calls.push_back(Kept {
                    id: Arc::from(id),
                    status,
                    result,
                    at_ms: c.get("at").and_then(Value::as_u64).unwrap_or(0),
                });
            }
        }
        Ok(snap)
    }

    /// Keeps the newest call per ID, at most [`KEPT_CALLS`], none older
    /// than [`KEPT_CALL_MS`].
    pub fn prune(&mut self, now_ms: u64) {
        let cutoff = now_ms.saturating_sub(KEPT_CALL_MS);
        let mut seen = FxHashSet::default();
        let mut kept = VecDeque::new();
        for c in self.calls.drain(..).rev() {
            if c.at_ms < cutoff || kept.len() >= KEPT_CALLS || !seen.insert(c.id.clone()) {
                continue;
            }
            kept.push_front(c);
        }
        self.calls = kept;
    }
}

/// One handler's committed state changes, as its `UpdateDone` carries
/// them: a JSON object, `null` for a cleared key.
pub fn encode_delta(writes: &BTreeMap<String, Option<Payload>>) -> Vec<u8> {
    let mut out = Vec::new();
    object_json(writes.iter().map(|(k, v)| (k, v.as_ref())), &mut out);
    out
}

pub fn decode_delta(p: &[u8]) -> Result<Vec<(String, Option<Payload>)>> {
    if p.is_empty() {
        return Ok(Vec::new());
    }
    let Value::Object(m) = serde_json::from_slice::<Value>(p)? else {
        return Err(Error::Codec("a state change is a JSON object".into()));
    };
    Ok(m.into_iter()
        .map(|(k, v)| {
            let v = (!v.is_null()).then(|| raw(&v));
            (k, v)
        })
        .collect())
}

/// The state as one JSON object.
pub fn state_json(state: &BTreeMap<String, Payload>) -> Payload {
    let mut out = Vec::new();
    object_json(state.iter().map(|(k, v)| (k, Some(v))), &mut out);
    Arc::from(out)
}

/// A call to a key that has not finished: running, or waiting for the key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PendingCall {
    pub handler: String,
    pub id: String,
}

/// What `inspect` adds for an object run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectDetail {
    /// The committed state, a JSON object.
    pub state: Value,
    /// Its version, for an edit's `ifVersion` ([`edit::version`]).
    pub version: String,
    /// Accepted calls whose handlers have not returned.
    pub running: Vec<PendingCall>,
    /// Exclusive calls waiting for the key, in arrival order.
    pub waiting: Vec<PendingCall>,
}

/// An object run's committed state and running calls from its journal
/// alone: the snapshot, then every recorded commit in order. No code runs.
/// `open` removes the codec from a sealed payload.
pub fn view(
    journal: &[RecordBuf],
    open: &dyn Fn(Payload) -> Result<Payload>,
) -> Result<(BTreeMap<String, Payload>, Vec<PendingCall>)> {
    let mut state = BTreeMap::new();
    let mut running: Vec<(u64, PendingCall)> = Vec::new();
    let mut ordinal = 0u64;
    for r in journal {
        match r {
            Record::Start { input, update, .. } => {
                state = Snapshot::decode(&open(input.clone())?)?.state;
                if let Some(u) = update {
                    running.push((
                        0,
                        PendingCall {
                            handler: u.name.to_string(),
                            id: u.id.to_string(),
                        },
                    ));
                    ordinal = 1;
                }
            }
            Record::Update { name, id, .. } => {
                running.push((
                    ordinal,
                    PendingCall {
                        handler: name.to_string(),
                        id: id.to_string(),
                    },
                ));
                ordinal += 1;
            }
            Record::UpdateDone {
                ordinal: o,
                state: delta,
                ..
            } => {
                running.retain(|(n, _)| n != o);
                if !delta.is_empty() {
                    for (k, v) in decode_delta(&open(delta.clone())?)? {
                        match v {
                            Some(v) => state.insert(k, v),
                            None => state.remove(&k),
                        };
                    }
                }
            }
            _ => {}
        }
    }
    Ok((state, running.into_iter().map(|(_, c)| c).collect()))
}

/// Whether a journal is an object run's.
pub fn is_object_journal(journal: &[RecordBuf]) -> bool {
    matches!(journal.first(), Some(Record::Start { policy: p, .. }) if p & policy::OBJECT != 0)
}

// ---------------------------------------------------------------------------
// The runtime's per-key state.

/// A call waiting for its key.
#[derive(Clone, Debug)]
pub(crate) struct Waiting {
    pub name: Name,
    pub id: Name,
    /// Plain (the codec is applied when it is offered).
    pub payload: Payload,
    pub shared: bool,
}

/// A workflow waiting for a call it made that its key has not accepted yet.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CallWaiter {
    /// `None` for a one-way call.
    pub parent: Option<fusee_core::record::ParentRef>,
    /// The message to acknowledge once the call is accepted.
    pub from: Option<(u32, u64)>,
}

/// One key's state in the runtime, beside its run.
#[derive(Debug, Default)]
pub(crate) struct ObjectRun {
    /// Committed state, as of the code's position in the journal (rebuilt
    /// from the snapshot whenever the run's code is set up).
    pub state: BTreeMap<String, Payload>,
    /// The exclusive handler running and its uncommitted writes.
    pub owner: Option<u64>,
    pub writes: BTreeMap<String, Option<Payload>>,
    /// Calls from earlier runs of the key (the snapshot's).
    pub calls: VecDeque<Kept>,
    /// Calls waiting for the key, in arrival order.
    pub waiting: VecDeque<Waiting>,
    /// The exclusive call offered to the code and not decided yet.
    pub offered: Option<Name>,
    /// A compaction is due: new calls wait for the new run.
    pub compacting: bool,
}

impl ObjectRun {
    pub fn reset(&mut self, snap: Snapshot) {
        self.state = snap.state;
        self.calls = snap.calls;
        self.owner = None;
        self.writes.clear();
        self.offered = None;
    }

    pub fn kept(&self, id: &str) -> Option<&Kept> {
        self.calls.iter().rev().find(|k| *k.id == *id)
    }

    /// What `get` returns: an exclusive handler sees its own writes.
    pub fn get(&self, key: &str, exclusive: bool) -> Option<Payload> {
        if exclusive
            && self.owner.is_some()
            && let Some(w) = self.writes.get(key)
        {
            return w.clone();
        }
        self.state.get(key).cloned()
    }

    pub fn keys(&self, exclusive: bool) -> Vec<String> {
        let mut keys: Vec<String> = self.state.keys().cloned().collect();
        if exclusive && self.owner.is_some() {
            for (k, v) in &self.writes {
                match v {
                    Some(_) if !self.state.contains_key(k) => keys.push(k.clone()),
                    None => keys.retain(|x| x != k),
                    _ => {}
                }
            }
            keys.sort();
        }
        keys
    }
}

/// Every key's runtime state on a shard.
#[derive(Debug, Default)]
pub(crate) struct Objects {
    pub runs: FxHashMap<fusee_core::record::RunId, ObjectRun>,
    /// Keys to compact once the tick reaches a safe point.
    pub compact_due: Vec<fusee_core::record::RunId>,
    /// Calls from workflows not accepted yet: `(key's run, call ID)`.
    pub callers: FxHashMap<(fusee_core::record::RunId, Name), CallWaiter>,
}

// ---------------------------------------------------------------------------
// The Rust API.

type Handler = Arc<dyn Fn(ObjectCtx, Payload) -> LocalFuture + Send + Sync>;

/// An object type being defined: [`Engine::object`], then its handlers,
/// then [`register`](Self::register).
pub struct ObjectType {
    engine: Engine,
    name: String,
    version: u32,
    handlers: ObjectHandlers,
}

/// An object type's handlers, and the workflow code they become: what
/// [`ObjectType`] registers on an engine, and what a simulator or another
/// registry takes ([`into_code`](Self::into_code)).
#[derive(Default)]
pub struct ObjectHandlers {
    handlers: BTreeMap<String, (bool, Handler)>,
}

impl ObjectHandlers {
    pub fn new() -> Self {
        Self::default()
    }

    /// See [`ObjectType::exclusive`].
    pub fn exclusive<A, R, F, Fut>(mut self, name: &str, f: F) -> Self
    where
        A: DeserializeOwned + 'static,
        R: Serialize + 'static,
        F: Fn(ObjectCtx, A) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + 'static,
    {
        self.handlers
            .insert(name.to_owned(), (false, erase_handler(f)));
        self
    }

    /// See [`ObjectType::shared`].
    pub fn shared<A, R, F, Fut>(mut self, name: &str, f: F) -> Self
    where
        A: DeserializeOwned + 'static,
        R: Serialize + 'static,
        F: Fn(ObjectCtx, A) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + 'static,
    {
        self.handlers
            .insert(name.to_owned(), (true, erase_handler(f)));
        self
    }

    /// The type's workflow code (its handlers as updates of the key's run,
    /// and the state query) and what the runtime needs to know of it.
    pub fn into_code(self) -> (WorkflowFn, ObjectDef) {
        let def = ObjectDef {
            shared: self
                .handlers
                .iter()
                .filter(|(_, (s, _))| *s)
                .map(|(n, _)| n.clone())
                .collect(),
        };
        let handlers = Arc::new(self.handlers);
        let f: WorkflowFn = Arc::new(move |ctx: Ctx| {
            let handlers = handlers.clone();
            Box::pin(async move {
                for (name, (shared, h)) in handlers.iter() {
                    let (c, h, exclusive) = (ctx.clone(), h.clone(), !*shared);
                    ctx.on_update_raw(
                        name,
                        Rc::new(|_| Ok(())),
                        Rc::new(move |arg: Payload| {
                            h(
                                ObjectCtx {
                                    ctx: c.clone(),
                                    exclusive,
                                },
                                arg,
                            )
                        }),
                    );
                }
                // Every type takes edits of its state from outside.
                let c = ctx.clone();
                ctx.on_update_raw(
                    EDIT_HANDLER,
                    Rc::new(|_| Ok(())),
                    Rc::new(move |edit: Payload| {
                        let r = c
                            .host_ref()
                            .object_edit(c.run_id(), &edit)
                            .map_err(outcome_of);
                        Box::pin(async move { r }) as Pin<Box<dyn Future<Output = RunOutcome>>>
                    }),
                );
                let c = ctx.clone();
                ctx.on_query_raw(
                    STATE_QUERY,
                    Rc::new(move |_| {
                        c.host_ref()
                            .object_state(c.run_id())
                            .map_err(|e| e.to_string())
                    }),
                );
                // An object's own code never ends: its handlers run as
                // updates, and the engine compacts it.
                std::future::pending::<()>().await;
                Ok(Arc::from(&b"null"[..]))
            }) as Pin<Box<dyn Future<Output = RunOutcome>>>
        });
        (f, def)
    }
}

fn erase_handler<A, R, F, Fut>(f: F) -> Handler
where
    A: DeserializeOwned + 'static,
    R: Serialize + 'static,
    F: Fn(ObjectCtx, A) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R>> + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |ctx: ObjectCtx, arg: Payload| {
        let f = f.clone();
        Box::pin(async move {
            let a: A = decode(&arg).map_err(outcome_of)?;
            // A handler's own failure records its message as it is, as an
            // exception's message is in the other SDKs.
            let r = f(ctx, a).await.map_err(|e| match e {
                Error::Failed(m) => (Status::Failed, m.to_payload()),
                e => outcome_of(e),
            })?;
            encode(&r).map_err(outcome_of)
        }) as Pin<Box<dyn Future<Output = RunOutcome>>>
    })
}

impl ObjectType {
    pub(crate) fn new(engine: Engine, name: &str) -> Self {
        Self {
            engine,
            name: name.to_owned(),
            version: 1,
            handlers: ObjectHandlers::new(),
        }
    }

    /// Adds an exclusive handler: one call at a time per key, in arrival
    /// order; it may change state.
    pub fn exclusive<A, R, F, Fut>(mut self, name: &str, f: F) -> Self
    where
        A: DeserializeOwned + 'static,
        R: Serialize + 'static,
        F: Fn(ObjectCtx, A) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + 'static,
    {
        self.handlers = self.handlers.exclusive(name, f);
        self
    }

    /// Adds a shared handler: it runs at once, next to other calls, and
    /// only reads state.
    pub fn shared<A, R, F, Fut>(mut self, name: &str, f: F) -> Self
    where
        A: DeserializeOwned + 'static,
        R: Serialize + 'static,
        F: Fn(ObjectCtx, A) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + 'static,
    {
        self.handlers = self.handlers.shared(name, f);
        self
    }

    /// Registers this code as `version` of the type (default 1). A new
    /// version takes over each key at its next compaction.
    pub fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    /// Registers the type with its handlers. Keys whose runs were waiting
    /// for this code resume.
    pub fn register(self) -> Result<Object> {
        check_type(&self.name)?;
        let (f, def) = self.handlers.into_code();
        self.engine
            .register_object(&self.name, self.version, f, def);
        Ok(Object {
            engine: self.engine,
            name: self.name,
        })
    }
}

/// A registered object type.
#[derive(Clone)]
pub struct Object {
    engine: Engine,
    name: String,
}

impl Object {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The object with key `key`.
    pub fn key(&self, key: &str) -> ObjectRef {
        self.engine.object(&self.name, key)
    }
}

/// What an object's handler gets: the workflow context of the key's run,
/// plus its state and key.
#[derive(Clone)]
pub struct ObjectCtx {
    ctx: Ctx,
    exclusive: bool,
}

impl std::ops::Deref for ObjectCtx {
    type Target = Ctx;
    fn deref(&self) -> &Ctx {
        &self.ctx
    }
}

impl ObjectCtx {
    /// The key's state. In an exclusive handler it includes the handler's
    /// own writes; in a shared one it is the committed state.
    pub fn state(&self) -> State<'_> {
        State {
            ctx: &self.ctx,
            exclusive: self.exclusive,
        }
    }

    /// The object's key.
    pub fn object_key(&self) -> &str {
        key_of(self.ctx.workflow_id(), self.ctx.workflow_type()).unwrap_or_default()
    }

    /// Whether this handler is exclusive (it may change state).
    pub fn is_exclusive(&self) -> bool {
        self.exclusive
    }

    /// The workflow context itself.
    pub fn ctx(&self) -> &Ctx {
        &self.ctx
    }
}

/// A key's state, as a handler sees it.
pub struct State<'a> {
    ctx: &'a Ctx,
    exclusive: bool,
}

impl State<'_> {
    pub fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        match self.get_raw(key)? {
            Some(p) => decode(&p).map(Some),
            None => Ok(None),
        }
    }

    /// The value's JSON as recorded.
    pub fn get_raw(&self, key: &str) -> Result<Option<Payload>> {
        self.ctx
            .host_ref()
            .object_get(self.ctx.run_id(), key, self.exclusive)
    }

    /// Sets `key` (null clears it). Only an exclusive handler may.
    pub fn set<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let p = encode(value)?;
        let v = (&*p != b"null").then_some(p);
        self.write(key, v)
    }

    pub fn set_raw(&self, key: &str, json: Payload) -> Result<()> {
        self.write(key, Some(json))
    }

    pub fn clear(&self, key: &str) -> Result<()> {
        self.write(key, None)
    }

    /// Clears every key.
    pub fn clear_all(&self) -> Result<()> {
        for k in self.keys()? {
            self.write(&k, None)?;
        }
        Ok(())
    }

    /// The keys that hold a value, sorted.
    pub fn keys(&self) -> Result<Vec<String>> {
        self.ctx
            .host_ref()
            .object_keys(self.ctx.run_id(), self.exclusive)
    }

    fn write(&self, key: &str, value: Option<Payload>) -> Result<()> {
        if !self.exclusive {
            return Err(Error::Rejected(
                "a shared handler cannot change state".into(),
            ));
        }
        self.ctx
            .host_ref()
            .object_set(self.ctx.run_id(), key, value)
    }
}

/// A key of an object type, to call from outside workflow code.
#[derive(Clone)]
pub struct ObjectRef {
    engine: Engine,
    wtype: String,
    key: String,
}

impl std::fmt::Debug for ObjectRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectRef({}/{})", self.wtype, self.key)
    }
}

/// The options of a call to an object's handler.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CallOptions {
    /// Calls with the same ID run once; every one gets the recorded result.
    pub id: String,
}

impl<M: Make<CallOptions>> Op<CallOptions, M> {
    pub fn id(mut self, id: &str) -> Self {
        self.options_mut().id = id.to_owned();
        self
    }
}

impl ObjectRef {
    pub(crate) fn new(engine: Engine, wtype: &str, key: &str) -> Self {
        Self {
            engine,
            wtype: wtype.to_owned(),
            key: key.to_owned(),
        }
    }

    /// The key's workflow ID (`<type>/<key>`).
    pub fn id(&self) -> String {
        object_id(&self.wtype, &self.key)
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    /// Calls a handler and waits for its durable result. Without an ID a
    /// retry is a new call; with `.id(id)`, calls with the same ID run
    /// once, and every one gets the recorded result.
    pub fn call<'a, A: Serialize + 'a, R: DeserializeOwned + 'a>(
        &'a self,
        handler: &'a str,
        arg: &'a A,
    ) -> Op<
        CallOptions,
        impl Make<CallOptions, Output = Result<R>, Fut = impl Future<Output = Result<R>> + 'a> + 'a,
    > {
        Op::new(CallOptions::default(), move |o: CallOptions| async move {
            let (status, p) = self.call_raw(handler, encode(arg)?, &o.id).await?;
            match status {
                Status::Ok => decode(&p),
                s => Err(status_error(s, &p)),
            }
        })
    }

    pub async fn call_raw(
        &self,
        handler: &str,
        arg: Payload,
        id: &str,
    ) -> Result<(Status, Payload)> {
        self.engine
            .update_with_start_raw(
                &self.wtype,
                &self.id(),
                Arc::from(&b"null"[..]),
                StartOptions::default().use_existing(),
                handler,
                id,
                arg,
            )
            .await
    }

    /// Calls a handler without waiting for its result: resolves once the
    /// call is accepted (durable, and its handler started). `.id(id)` makes
    /// a retried send run once.
    pub fn send<'a, A: Serialize + 'a>(
        &'a self,
        handler: &'a str,
        arg: &'a A,
    ) -> Op<
        CallOptions,
        impl Make<CallOptions, Output = Result<()>, Fut = impl Future<Output = Result<()>> + 'a> + 'a,
    > {
        Op::new(CallOptions::default(), move |o: CallOptions| async move {
            let (status, p) = self
                .call_raw(handler, encode(arg)?, &format!("{SEND_PREFIX}{}", o.id))
                .await?;
            match status {
                Status::Ok => Ok(()),
                s => Err(status_error(s, &p)),
            }
        })
    }

    /// Edits the state from outside its handlers ([`edit`]): an exclusive
    /// call, so it waits for the key and commits as a handler's writes do.
    /// Returns `{"version", "changed"}`.
    pub async fn edit_state(&self, edit: &Value) -> Result<Value> {
        let (status, p) = self
            .call_raw(EDIT_HANDLER, Arc::from(edit.to_string().into_bytes()), "")
            .await?;
        match status {
            Status::Ok => decode(&p),
            s => Err(status_error(s, &p)),
        }
    }

    /// Sets one field of the state.
    pub async fn set_state<V: Serialize>(&self, field: &str, value: &V) -> Result<Value> {
        let mut set = serde_json::Map::new();
        set.insert(field.to_owned(), serde_json::to_value(value)?);
        self.edit_state(&serde_json::json!({ "set": set })).await
    }

    /// Applies a JSON Patch (RFC 6902) to the state as one JSON object.
    pub async fn patch_state(&self, ops: &Value) -> Result<Value> {
        self.edit_state(&serde_json::json!({ "patch": ops })).await
    }

    /// Clears one field, or (`None`) the whole state; the key remains.
    pub async fn clear_state(&self, field: Option<&str>) -> Result<Value> {
        match field {
            Some(f) => self.edit_state(&serde_json::json!({ "clear": [f] })).await,
            None => {
                self.edit_state(&serde_json::json!({ "clearAll": true }))
                    .await
            }
        }
    }

    /// The committed state, a JSON object (`{}` for a key never called).
    /// Answered once it is durable.
    pub async fn state(&self) -> Result<Value> {
        match self
            .engine
            .query_raw(&self.id(), STATE_QUERY, Arc::from(&b"null"[..]))
            .await
        {
            Ok(p) => Ok(serde_json::from_slice(&p)?),
            Err(Error::NotFound(_)) => Ok(Value::Object(Default::default())),
            Err(e) => Err(e),
        }
    }

    /// The key's run: its journal, state and pending calls.
    /// [`Error::NotFound`] for a key never called.
    pub async fn describe(&self) -> Result<RunDetail> {
        self.engine
            .describe(&self.id())
            .await?
            .ok_or_else(|| Error::NotFound(self.id()))
    }

    /// Deletes the key: its run ends and its state goes with it. The next
    /// call starts with empty state.
    pub async fn delete(&self) -> Result<()> {
        match self
            .engine
            .run::<Value>(&self.id())
            .terminate("object deleted")
            .await
        {
            Err(Error::NotFound(_)) => Ok(()),
            r => r,
        }
    }
}

/// A key of an object type, called from workflow code (or another
/// object's handler): [`Ctx::object`].
pub struct ObjectCall<'a> {
    pub(crate) ctx: &'a Ctx,
    pub(crate) wtype: String,
    pub(crate) key: String,
}

impl ObjectCall<'_> {
    /// Calls a handler and waits for its result. The call is a journal
    /// operation: it is sent through the durable outbox, runs once, and its
    /// result is recorded.
    pub async fn call<A: Serialize, R: DeserializeOwned>(
        &self,
        handler: &str,
        arg: &A,
    ) -> Result<R> {
        let seq = self.ctx.host_ref().begin_call(
            self.ctx.run_id(),
            &self.wtype,
            &self.key,
            handler,
            encode(arg)?,
            false,
        )?;
        let Some(seq) = seq else {
            return Err(Error::Failed("the call did not register".into()));
        };
        let o = self.ctx.delivery(seq).await;
        match o.status {
            Status::Ok => decode(&o.payload),
            s => Err(status_error(s, &o.payload)),
        }
    }

    /// Calls a handler without waiting: the outbox delivers the call once
    /// this point of the journal is durable, and it runs once.
    pub fn send<A: Serialize>(&self, handler: &str, arg: &A) -> Result<()> {
        self.ctx.host_ref().begin_call(
            self.ctx.run_id(),
            &self.wtype,
            &self.key,
            handler,
            encode(arg)?,
            true,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> Payload {
        Arc::from(s.as_bytes())
    }

    #[test]
    fn snapshots_round_trip() {
        let mut snap = Snapshot::default();
        assert_eq!(snap.encode(), b"{}");
        snap.state.insert("items".into(), p("[1,2]"));
        snap.state.insert("n".into(), p("3"));
        snap.calls.push_back(Kept {
            id: Arc::from("a"),
            status: Status::Ok,
            result: p("{\"x\":1}"),
            at_ms: 5,
        });
        snap.calls.push_back(Kept {
            id: Arc::from("b"),
            status: Status::Failed,
            result: p("out of stock"),
            at_ms: 6,
        });
        snap.calls.push_back(Kept {
            id: Arc::from("c"),
            status: Status::Ok,
            result: Arc::from(&[0xff, 0x00][..]),
            at_ms: 7,
        });
        let e = snap.encode();
        let back = Snapshot::decode(&e).unwrap();
        assert_eq!(back, snap, "{}", String::from_utf8_lossy(&e));
    }

    #[test]
    fn pruning_keeps_the_newest_per_id() {
        let mut snap = Snapshot::default();
        for (i, id) in ["a", "b", "a", "c"].iter().enumerate() {
            snap.calls.push_back(Kept {
                id: Arc::from(*id),
                status: Status::Ok,
                result: p(&i.to_string()),
                at_ms: 1_000 + i as u64,
            });
        }
        snap.prune(1_003);
        let ids: Vec<(&str, &[u8])> = snap.calls.iter().map(|c| (&*c.id, &c.result[..])).collect();
        assert_eq!(ids, vec![("b", &b"1"[..]), ("a", b"2"), ("c", b"3")]);
        snap.prune(1_000 + KEPT_CALL_MS + 3);
        assert_eq!(snap.calls.len(), 1);
    }

    #[test]
    fn deltas_and_calls_encode() {
        let mut w = BTreeMap::new();
        w.insert("a".to_owned(), Some(p("{\"q\":1}")));
        w.insert("b\"".to_owned(), None);
        let d = encode_delta(&w);
        assert_eq!(d, b"{\"a\":{\"q\":1},\"b\\\"\":null}");
        let back = decode_delta(&d).unwrap();
        assert_eq!(back.len(), 2);
        let c = encode_call("add", "wf/1/3", b"[1]");
        let (h, id, arg) = decode_call(&c).unwrap();
        assert_eq!(
            (h.as_str(), id.as_str(), &arg[..]),
            ("add", "wf/1/3", &b"[1]"[..])
        );
        assert_eq!(key_of("cart/u/7", "cart"), Some("u/7"));
        assert!(is_auto_id("fusee.send/auto-1-2") && is_auto_id("") && !is_auto_id("req-1"));
    }
}
