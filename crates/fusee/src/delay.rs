//! Delayed start: `engine.start(wf, id, &input).delay(d)` (or `.at(t)`).
//!
//! A coordinator run, `fusee.delay/<id>`, sleeps on a durable timer until
//! the time comes, then starts the run under `id` as a child that outlives
//! it, and returns that run's result. So the delay survives crashes like any
//! timer, `fusee runs ls` shows what is waiting, and `result()` of the
//! delayed run works before the run exists. Cancelling the coordinator
//! before the time cancels the start. Nothing of this touches a step.

use crate::ctx::{ChildOptions, Ctx};
use crate::error::Result;
use crate::runtime::{ParentClose, Registry, RunOutcome, WorkflowFn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// The coordinator's workflow type.
pub const DELAY_TYPE: &str = "fusee.delay";

/// The coordinator's workflow ID for a delayed run's ID.
pub fn coordinator_id(id: &str) -> String {
    format!("{DELAY_TYPE}/{id}")
}

/// The coordinator's input.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DelayedStart {
    /// The workflow type to start.
    #[serde(rename = "type")]
    pub wtype: String,
    pub id: String,
    pub input: Value,
    /// When to start it, Unix ms.
    pub at_ms: u64,
    /// The run's own deadline once started, ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

async fn run(ctx: Ctx, d: DelayedStart) -> Result<Value> {
    ctx.sleep_until("delay", d.at_ms).await?;
    let opts = ChildOptions {
        close: ParentClose::Abandon,
        timeout: d.timeout_ms.map(std::time::Duration::from_millis),
    };
    ctx.run_child(&d.wtype, &d.id, &d.input, opts).await
}

pub(crate) fn register(r: &mut Registry) {
    let f: WorkflowFn = Arc::new(|ctx: Ctx| {
        let d = ctx.input_bytes(crate::ctx::decode::<DelayedStart>);
        Box::pin(async move {
            let d: DelayedStart = d.map_err(crate::engine::outcome_of)?;
            let out = run(ctx, d).await.map_err(crate::engine::outcome_of)?;
            crate::ctx::encode(&out).map_err(crate::engine::outcome_of)
        }) as Pin<Box<dyn Future<Output = RunOutcome>>>
    });
    r.register(DELAY_TYPE, 1, f);
}
