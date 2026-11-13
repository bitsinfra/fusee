//! Debounce and batching: starts sent under one key become one run.
//!
//! - **Debounce.** Starts of a workflow type with the same key inside the
//!   window collapse into one run that begins when the window closes:
//!   `window_ms` after the latest start (trailing edge), or `max_wait_ms`
//!   after the first one, whichever comes first. The run's input is the
//!   latest start's.
//! - **Batching.** Events with the same key accumulate until `max_events`
//!   have arrived or `max_wait_ms` has passed since the first; then one run
//!   starts with every event, in arrival order, as its input (a JSON array).
//!
//! Each key has a coordinator: a built-in workflow ([`DEBOUNCE_TYPE`] or
//! [`BATCH_TYPE`], ID [`debounce_id`] or [`batch_id`]) built from existing
//! primitives the way schedules are, so there is no new record kind and
//! nothing new to replay. An event is an update ([`EVENT`]) sent with
//! update-with-start: one record starts the coordinator if the key has none
//! and delivers the event. The update's handler adds the event to the open
//! window and returns a [`Receipt`] naming the run the event joined; a
//! durable timer closes the window and a detached child start begins the
//! run.
//!
//! What a caller can rely on:
//!
//! - The call returns once the update's result is durable. From then on
//!   the event is part of exactly one window, across any crash: the handler
//!   is replayed at its place in the coordinator's journal. A call that
//!   fails was not counted unless the error came after durability (a crash
//!   of the caller's own process); send an event ID to make the retry
//!   join once.
//! - A window starts one run, whose workflow ID (`<key>@<uuid>`, the uuid
//!   recorded when the window opens) is in the receipt before the run
//!   exists. The start is a child start from the coordinator's journal, so
//!   after a crash it is sent again with the same ID and attaches.
//! - Event IDs: a retry with an ID the key has seen gets the first receipt
//!   and adds nothing, for `dedup_ms` after the key's last event with an
//!   ID (default ten minutes; the coordinator stays parked that long).
//!
//! The coordinator ends once no window is open and no remembered event ID
//! is younger than its dedup window, and continues as new every few
//! hundred journal operations, carrying the open window and the remembered
//! IDs. An event whose update reaches the coordinator as it ends is refused
//! with "the workflow ended before the update finished" before its handler
//! ran; [`crate::Engine::debounce_raw`] sends it again to the next one.

use crate::ctx::{ChildOptions, Ctx};
use crate::error::{Error, Result};
use crate::runtime::{ParentClose, Registry, RunOutcome, WorkflowFn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Poll, Waker};
use std::time::Duration;

/// The built-in workflow type that debounces starts.
pub const DEBOUNCE_TYPE: &str = "fusee.debounce";
/// The built-in workflow type that batches events.
pub const BATCH_TYPE: &str = "fusee.batch";
/// The update coordinators take events on.
pub const EVENT: &str = "fusee.event";
/// What a coordinator's update fails with when it ended before the
/// handler ran: the event was not counted and may be sent again.
pub const ENDED: &[u8] = b"the workflow ended before the update finished";

/// Event IDs are remembered this long after a key's last event by default.
pub const DEFAULT_DEDUP_MS: u64 = 600_000;
/// The most events one batch takes.
pub const MAX_BATCH: u32 = 10_000;
/// Event IDs a key remembers at most.
const SEEN_CAP: usize = 1_024;
/// Journal operations after which a coordinator continues as new.
const CONTINUE_AT: u32 = 400;
/// A coordinator run lives at least this long, so the update that started
/// it (or one sent again after the previous run ended) is delivered first.
const MIN_LIFE_MS: u64 = 1_000;

/// The coordinator that debounces starts of `workflow` under `key`.
pub fn debounce_id(workflow: &str, key: &str) -> String {
    format!("{DEBOUNCE_TYPE}/{workflow}/{key}")
}

/// The coordinator that batches events for `workflow` under `key`.
pub fn batch_id(workflow: &str, key: &str) -> String {
    format!("{BATCH_TYPE}/{workflow}/{key}")
}

/// One event as an update-with-start, for any client (the engine, the
/// server's `Client`, the SDKs): the coordinator's workflow type, its ID,
/// its start input, and the [`EVENT`] update's argument. Send it with the
/// running policy `use-existing`, the event ID as the update ID, and send
/// it again when the answer is [`ENDED`].
pub fn request(workflow: &str, key: &str, event: &Event) -> (&'static str, String, Value, Value) {
    let (wtype, id) = match event.rule {
        Rule::Debounce(_) => (DEBOUNCE_TYPE, debounce_id(workflow, key)),
        Rule::Batch(_) => (BATCH_TYPE, batch_id(workflow, key)),
    };
    let state = QueueState {
        workflow: workflow.to_owned(),
        key: key.to_owned(),
        ..Default::default()
    };
    (
        wtype,
        id,
        serde_json::to_value(&state).unwrap_or_default(),
        serde_json::to_value(event).unwrap_or_default(),
    )
}

/// How starts with one key are collapsed into one run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Debounce {
    /// The run starts once this long passes without another start.
    pub window_ms: u64,
    /// The run starts at most this long after the window's first start
    /// (0: no bound, so a steady stream of starts can delay it forever).
    pub max_wait_ms: u64,
    /// How long the key remembers event IDs after its last event.
    pub dedup_ms: u64,
}

impl Default for Debounce {
    fn default() -> Self {
        Self {
            window_ms: 0,
            max_wait_ms: 0,
            dedup_ms: DEFAULT_DEDUP_MS,
        }
    }
}

impl Debounce {
    pub fn new(window: Duration) -> Self {
        Self {
            window_ms: crate::ctx::millis(window),
            ..Self::default()
        }
    }

    pub fn max_wait(mut self, d: Duration) -> Self {
        self.max_wait_ms = crate::ctx::millis(d);
        self
    }

    pub fn dedup_window(mut self, d: Duration) -> Self {
        self.dedup_ms = crate::ctx::millis(d);
        self
    }
}

/// When a batch of events starts its run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Batch {
    /// The run starts as soon as the batch holds this many events.
    pub max_events: u32,
    /// ... or this long after the batch's first event.
    pub max_wait_ms: u64,
    /// How long the key remembers event IDs after its last event.
    pub dedup_ms: u64,
}

impl Default for Batch {
    fn default() -> Self {
        Self {
            max_events: 100,
            max_wait_ms: 1_000,
            dedup_ms: DEFAULT_DEDUP_MS,
        }
    }
}

impl Batch {
    pub fn new(max_events: u32, max_wait: Duration) -> Self {
        Self {
            max_events,
            max_wait_ms: crate::ctx::millis(max_wait),
            ..Self::default()
        }
    }

    pub fn dedup_window(mut self, d: Duration) -> Self {
        self.dedup_ms = crate::ctx::millis(d);
        self
    }
}

/// The rule an event is sent with. The latest event's rule applies to the
/// open window.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Rule {
    Debounce(Debounce),
    Batch(Batch),
}

impl Rule {
    pub fn validate(&self) -> Result<()> {
        let bad = |m: &str| Err(Error::Rejected(m.into()));
        match self {
            Rule::Debounce(_) => Ok(()),
            Rule::Batch(b) if b.max_events == 0 => bad("a batch takes at least one event"),
            Rule::Batch(b) if b.max_events > MAX_BATCH => {
                bad(&format!("a batch takes at most {MAX_BATCH} events"))
            }
            Rule::Batch(_) => Ok(()),
        }
    }

    fn dedup_ms(&self) -> u64 {
        match self {
            Rule::Debounce(d) => d.dedup_ms,
            Rule::Batch(b) => b.dedup_ms,
        }
    }
}

/// One event: the update a coordinator takes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    /// The caller's event ID ("" for none): a retry with the same ID joins
    /// once and gets the same receipt.
    #[serde(default)]
    pub id: String,
    pub rule: Rule,
    /// The run's input (debounce) or one element of it (batch).
    pub input: Value,
}

/// What an event's caller gets back.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Receipt {
    /// The workflow ID of the run the event joined. The run exists once its
    /// window closes.
    pub id: String,
    /// When the window closes, as of this event (later events can move a
    /// debounce window on; a full batch closes it at once).
    pub due_ms: u64,
}

/// The open window: the events of the next run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Window {
    pub id: String,
    pub opened_ms: u64,
    pub last_ms: u64,
    pub rule: Rule,
    /// Debounce: the latest input. Batch: every event's input.
    pub inputs: Vec<Value>,
}

impl Window {
    fn due(&self) -> u64 {
        match &self.rule {
            Rule::Debounce(d) => {
                let quiet = self.last_ms.saturating_add(d.window_ms);
                if d.max_wait_ms > 0 {
                    quiet.min(self.opened_ms.saturating_add(d.max_wait_ms))
                } else {
                    quiet
                }
            }
            Rule::Batch(b) if self.inputs.len() >= b.max_events as usize => self.last_ms,
            Rule::Batch(b) => self.opened_ms.saturating_add(b.max_wait_ms),
        }
    }

    fn input(&self) -> Value {
        match &self.rule {
            Rule::Debounce(_) => self.inputs.last().cloned().unwrap_or(Value::Null),
            Rule::Batch(_) => Value::Array(self.inputs.clone()),
        }
    }
}

/// An event ID a key has seen.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Seen {
    pub id: String,
    pub receipt: Receipt,
    pub until_ms: u64,
}

/// A coordinator's state, carried across continue-as-new.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct QueueState {
    /// The workflow type each window starts.
    pub workflow: String,
    pub key: String,
    pub open: Option<Window>,
    /// Remembered event IDs, oldest first.
    pub seen: VecDeque<Seen>,
    /// Runs this coordinator and its predecessors (since continue-as-new)
    /// started.
    pub started: u64,
    /// Journal operations after which the coordinator continues as new (0:
    /// the default). Tests set it low.
    #[doc(hidden)]
    #[serde(skip_serializing_if = "is_zero")]
    pub continue_at: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Debounce,
    Batch,
}

/// State shared by the coordinator's main loop and its update handler.
struct Shared {
    state: RefCell<QueueState>,
    /// The time the main loop's timer is set for.
    armed: Cell<Option<u64>>,
    woken: Cell<bool>,
    waker: RefCell<Option<Waker>>,
    continue_at: u32,
}

impl Shared {
    fn wake(&self) {
        self.woken.set(true);
        if let Some(w) = self.waker.borrow_mut().take() {
            w.wake();
        }
    }
}

/// Starts the open window's run and closes the window.
fn fire(ctx: &Ctx, sh: &Shared) -> Result<()> {
    let (workflow, id, input) = {
        let st = sh.state.borrow();
        let Some(w) = st.open.as_ref() else {
            return Ok(());
        };
        (st.workflow.clone(), w.id.clone(), w.input())
    };
    ctx.run_start_child(
        &workflow,
        &id,
        &input,
        ChildOptions {
            close: ParentClose::Abandon,
            timeout: None,
        },
    )?;
    let mut st = sh.state.borrow_mut();
    st.open = None;
    st.started += 1;
    Ok(())
}

fn check(kind: Kind, ev: &Event) -> Result<()> {
    match (&ev.rule, kind) {
        (Rule::Debounce(_), Kind::Debounce) | (Rule::Batch(_), Kind::Batch) => ev.rule.validate(),
        _ => Err(Error::Rejected(
            "the event's rule does not match its coordinator".into(),
        )),
    }
}

/// The update handler: adds one event to the open window. It never waits,
/// so a caller's reply means the event is in the window.
fn accept(ctx: &Ctx, kind: Kind, sh: &Shared, ev: Event) -> Result<Receipt> {
    // The validator does not run for the update a start carries.
    check(kind, &ev)?;
    if !ev.id.is_empty()
        && let Some(s) = sh.state.borrow().seen.iter().find(|s| s.id == ev.id)
    {
        return Ok(s.receipt.clone());
    }
    let now = ctx.now_ms()?;
    let opening = sh.state.borrow().open.is_none();
    let fresh = if opening {
        let key = sh.state.borrow().key.clone();
        Some(format!("{key}@{}", ctx.uuid()?))
    } else {
        None
    };
    let (receipt, full) = {
        let mut st = sh.state.borrow_mut();
        let w = st.open.get_or_insert_with(|| Window {
            id: fresh.unwrap_or_default(),
            opened_ms: now,
            last_ms: now,
            rule: ev.rule.clone(),
            inputs: Vec::new(),
        });
        w.last_ms = w.last_ms.max(now);
        w.rule = ev.rule.clone();
        if matches!(w.rule, Rule::Debounce(_)) {
            w.inputs.clear();
        }
        w.inputs.push(ev.input);
        let full = matches!(&w.rule, Rule::Batch(b) if w.inputs.len() >= b.max_events as usize);
        let receipt = Receipt {
            id: w.id.clone(),
            due_ms: w.due(),
        };
        if !ev.id.is_empty() {
            st.seen.push_back(Seen {
                id: ev.id,
                receipt: receipt.clone(),
                until_ms: now.saturating_add(ev.rule.dedup_ms()),
            });
            while st.seen.len() > SEEN_CAP {
                st.seen.pop_front();
            }
        }
        (receipt, full)
    };
    if full {
        fire(ctx, sh)?;
    }
    // Wake the main loop only when what it waits for came earlier, or the
    // journal is due to be trimmed.
    let due = sh.state.borrow().open.as_ref().map(Window::due);
    let earlier = matches!((due, sh.armed.get()), (Some(d), Some(a)) if d < a);
    if earlier || ctx.journal_size() >= sh.continue_at {
        sh.wake();
    }
    Ok(receipt)
}

/// Waits until `until` or until the handler wakes the loop.
async fn wait(ctx: &Ctx, sh: &Shared, until: u64) -> Result<()> {
    let mut timer = std::pin::pin!(ctx.sleep_until("window", until));
    std::future::poll_fn(|cx| {
        if sh.woken.replace(false) {
            return Poll::Ready(Ok(()));
        }
        *sh.waker.borrow_mut() = Some(cx.waker().clone());
        timer.as_mut().poll(cx)
    })
    .await
}

async fn coordinate(ctx: Ctx, kind: Kind, input: QueueState) -> Result<Value> {
    let continue_at = match input.continue_at {
        0 => CONTINUE_AT,
        n => n.max(8),
    };
    let sh = Rc::new(Shared {
        state: RefCell::new(input),
        armed: Cell::new(None),
        woken: Cell::new(false),
        waker: RefCell::new(None),
        continue_at,
    });
    {
        let s = sh.clone();
        ctx.on_query("describe", move |_: Value| {
            let st = s.state.borrow();
            serde_json::json!({
                "workflow": st.workflow,
                "key": st.key,
                "open": st.open.as_ref().map(|w| serde_json::json!({
                    "id": w.id,
                    "events": w.inputs.len(),
                    "dueMs": w.due(),
                })),
                "started": st.started,
                "rememberedIds": st.seen.len(),
            })
        });
    }
    {
        let (s, c) = (sh.clone(), ctx.clone());
        ctx.on_update(
            EVENT,
            move |ev: &Event| check(kind, ev).map_err(|e| e.to_string()),
            move |ev: Event| {
                let r = accept(&c, kind, &s, ev);
                async move { r }
            },
        );
    }
    loop {
        if ctx.journal_size() >= continue_at {
            let st = sh.state.borrow().clone();
            return ctx.continue_as_new(&st).await;
        }
        let now = ctx.now_ms()?;
        let due = sh.state.borrow().open.as_ref().map(Window::due);
        let until = match due {
            Some(due) if due <= now => {
                fire(&ctx, &sh)?;
                continue;
            }
            Some(due) => due,
            None => {
                let mut st = sh.state.borrow_mut();
                st.seen.retain(|s| s.until_ms > now);
                let linger = st
                    .seen
                    .iter()
                    .map(|s| s.until_ms)
                    .max()
                    .unwrap_or(0)
                    .max(ctx.started_ms().saturating_add(MIN_LIFE_MS));
                if linger <= now {
                    return Ok(serde_json::json!({ "started": st.started }));
                }
                linger
            }
        };
        sh.armed.set(Some(until));
        let r = wait(&ctx, &sh, until).await;
        sh.armed.set(None);
        r?;
    }
}

/// Registers the coordinator workflows.
pub fn register_builtins(r: &mut Registry) {
    for (name, kind) in [(DEBOUNCE_TYPE, Kind::Debounce), (BATCH_TYPE, Kind::Batch)] {
        let f: WorkflowFn = Arc::new(move |ctx: Ctx| {
            let st = ctx.input_bytes(crate::ctx::decode::<QueueState>);
            Box::pin(async move {
                let st: QueueState = st.map_err(crate::engine::outcome_of)?;
                let out = coordinate(ctx, kind, st)
                    .await
                    .map_err(crate::engine::outcome_of)?;
                crate::ctx::encode(&out).map_err(crate::engine::outcome_of)
            }) as Pin<Box<dyn Future<Output = RunOutcome>>>
        });
        r.register(name, 1, f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_close_on_their_rule() {
        let w = |rule, n| Window {
            id: "k@1".into(),
            opened_ms: 1_000,
            last_ms: 1_400,
            rule,
            inputs: vec![Value::Null; n],
        };
        let d = Debounce::new(Duration::from_millis(300));
        assert_eq!(w(Rule::Debounce(d.clone()), 1).due(), 1_700);
        let capped = d.max_wait(Duration::from_millis(500));
        assert_eq!(w(Rule::Debounce(capped), 1).due(), 1_500);
        let b = Batch::new(3, Duration::from_millis(2_000));
        assert_eq!(w(Rule::Batch(b.clone()), 2).due(), 3_000);
        assert_eq!(w(Rule::Batch(b), 3).due(), 1_400);
        assert!(
            Rule::Batch(Batch::new(0, Duration::ZERO))
                .validate()
                .is_err()
        );
        assert!(
            Rule::Batch(Batch::new(MAX_BATCH + 1, Duration::ZERO))
                .validate()
                .is_err()
        );
    }

    #[test]
    fn events_have_a_stable_json_form() {
        let ev = Event {
            id: "e-1".into(),
            rule: Rule::Batch(Batch::new(10, Duration::from_secs(1))),
            input: serde_json::json!({"n": 1}),
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "id": "e-1",
                "rule": {"kind": "batch", "maxEvents": 10, "maxWaitMs": 1000, "dedupMs": 600000},
                "input": {"n": 1},
            })
        );
        let d: Event = serde_json::from_value(serde_json::json!({
            "rule": {"kind": "debounce", "windowMs": 50},
            "input": 7,
        }))
        .unwrap();
        assert_eq!(d.id, "");
        assert_eq!(
            d.rule,
            Rule::Debounce(Debounce {
                window_ms: 50,
                max_wait_ms: 0,
                dedup_ms: DEFAULT_DEDUP_MS,
            })
        );
    }
}
