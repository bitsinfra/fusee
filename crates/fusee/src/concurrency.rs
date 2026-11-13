//! Runs at once per type or key: at most `limit` runs of a workflow type (or
//! of one key of it, a tenant say) run at a time; the others wait, in the
//! order they were started, and show as queued.
//!
//! Built from runs, so a turn is kept while its run is parked and across
//! crashes, and nothing of it touches a step:
//!
//! - A start of a limited type starts a *slot* instead, `fusee.queued/<id>`,
//!   whose input names the type, ID and input.
//! - The slot starts the type's (or key's) *coordinator*,
//!   `fusee.concurrency/<type>[/<key>]`, attaching to it if it runs, and
//!   asks it for a turn (a signal).
//! - The coordinator keeps the queue and the runs holding a turn in its
//!   journal, admits runs in the order they asked while fewer than `limit`
//!   hold one (a signal back), and continues as new now and then with that
//!   state.
//! - An admitted slot runs the workflow under its own ID as a child, gives
//!   the turn back when it ends however it ends, and returns its result.
//!
//! `result(id)` follows the slot until the run exists (as for a delayed
//! start). Cancelling a slot that waits takes its request back; terminate
//! the run, not its slot, once it runs (a terminated slot keeps its turn).

use crate::ctx::{ChildOptions, Ctx};
use crate::error::Result;
use crate::runtime::{ParentClose, Registry, RunOutcome, WorkflowFn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// The slot's workflow type.
pub const SLOT_TYPE: &str = "fusee.queued";
/// The coordinator's workflow type.
pub const COORDINATOR_TYPE: &str = "fusee.concurrency";

/// The slot's workflow ID for a run's ID.
pub fn slot_id(id: &str) -> String {
    format!("{SLOT_TYPE}/{id}")
}

/// The coordinator's workflow ID for a type, and a key if the limit is per
/// key.
pub fn coordinator_id(wtype: &str, key: Option<&str>) -> String {
    match key {
        Some(k) => format!("{COORDINATOR_TYPE}/{wtype}/{k}"),
        None => format!("{COORDINATOR_TYPE}/{wtype}"),
    }
}

/// A slot's input: the run to start once admitted.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Queued {
    #[serde(rename = "type")]
    pub wtype: String,
    pub id: String,
    pub input: Value,
    /// Runs of the type (or key) at once.
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// The run's own deadline once it starts, ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

/// What a slot tells the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Event {
    /// "request" or "release".
    kind: String,
    id: String,
    limit: u32,
}

/// The coordinator's state, carried across continue-as-new.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Turns {
    pub limit: u32,
    /// Waiting, in the order they asked.
    pub queue: VecDeque<String>,
    /// Holding a turn, in the order they were admitted.
    pub running: Vec<String>,
    #[serde(default)]
    events: u32,
}

const EVENT: &str = "event";
const ADMIT: &str = "admit";
/// Events a coordinator run takes before it continues as new (about three
/// journal operations each).
const EVENTS_PER_RUN: u32 = 300;

async fn slot(ctx: Ctx, q: Queued) -> Result<Value> {
    let coordinator = coordinator_id(&q.wtype, q.key.as_deref());
    let limit = q.limit.max(1);
    // One coordinator per type or key: a start attaches to it when it runs.
    ctx.run_start_child(
        COORDINATOR_TYPE,
        &coordinator,
        &Turns {
            limit,
            ..Default::default()
        },
        ChildOptions {
            close: ParentClose::Abandon,
            timeout: None,
        },
    )?;
    let ask = |kind: &str| Event {
        kind: kind.into(),
        id: q.id.clone(),
        limit,
    };
    ctx.signal_workflow(&coordinator, EVENT, &ask("request"))?;
    if let Err(e) = ctx.wait_for::<bool>(ADMIT).await {
        // Cancelled while waiting: take the request back.
        ctx.signal_workflow(&coordinator, EVENT, &ask("release"))?;
        return Err(e);
    }
    let out = ctx
        .run_child::<_, Value>(
            &q.wtype,
            &q.id,
            &q.input,
            ChildOptions {
                close: ParentClose::Abandon,
                timeout: q.timeout_ms.map(std::time::Duration::from_millis),
            },
        )
        .await;
    // The turn goes back however the run ended.
    ctx.signal_workflow(&coordinator, EVENT, &ask("release"))?;
    out
}

async fn coordinator(ctx: Ctx, mut t: Turns) -> Result<Value> {
    loop {
        // Admit whoever fits (the first time round, those carried over).
        while (t.running.len() as u32) < t.limit.max(1) {
            let Some(id) = t.queue.pop_front() else { break };
            ctx.signal_workflow(&slot_id(&id), ADMIT, &true)?;
            t.running.push(id);
        }
        if t.events >= EVENTS_PER_RUN {
            t.events = 0;
            return ctx.continue_as_new(&t).await;
        }
        let Some(e) = ctx.wait_for::<Event>(EVENT).await? else {
            continue;
        };
        t.events += 1;
        // The newest start's limit wins: a deploy can change it.
        t.limit = e.limit.max(1);
        match e.kind.as_str() {
            "request" if !t.running.contains(&e.id) && !t.queue.contains(&e.id) => {
                t.queue.push_back(e.id)
            }
            "release" => {
                t.running.retain(|r| r != &e.id);
                t.queue.retain(|r| r != &e.id);
            }
            _ => {}
        }
    }
}

fn workflow<I, F, Fut>(f: F) -> WorkflowFn
where
    I: serde::de::DeserializeOwned + 'static,
    F: Fn(Ctx, I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value>> + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |ctx: Ctx| {
        let f = f.clone();
        let i = ctx.input_bytes(crate::ctx::decode::<I>);
        Box::pin(async move {
            let i: I = i.map_err(crate::engine::outcome_of)?;
            let out = f(ctx, i).await.map_err(crate::engine::outcome_of)?;
            crate::ctx::encode(&out).map_err(crate::engine::outcome_of)
        }) as Pin<Box<dyn Future<Output = RunOutcome>>>
    })
}

pub(crate) fn register(r: &mut Registry) {
    r.register(SLOT_TYPE, 1, workflow(slot));
    r.register(COORDINATOR_TYPE, 1, workflow(coordinator));
}
