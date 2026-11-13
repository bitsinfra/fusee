#![doc(html_no_source)]

//! # Fusee
//!
//! Durable execution at the speed of the disk. Workflows are ordinary async
//! Rust; each step's result is appended to a per-shard log, one `fdatasync`
//! covers every record that arrived in the meantime, and the next step
//! starts as soon as its input is durable. Code runs ahead of the disk; only
//! work that leaves the process waits, and only for the records it depends
//! on.
//!
//! ```no_run
//! use fusee::{Config, Engine, Error, RetryPolicy, Signal, WorkflowRef};
//! use std::time::Duration;
//!
//! pub const CHECKOUT: WorkflowRef<u64, String> = WorkflowRef::new("checkout");
//! pub const REVIEW: Signal<bool> = Signal::new("review");
//!
//! # async fn charge(cents: u64, key: String) -> Result<String, Error> { Ok(key) }
//! # async fn demo() -> Result<(), Error> {
//! let engine = Engine::open(Config::local("./data")).await?;
//! engine.register(CHECKOUT, |ctx, order: u64| async move {
//!     let quote: u64 = ctx.step("quote", || async move { Ok::<_, Error>(order * 2) }).await?;
//!     let id: String = ctx
//!         .step("charge", || charge(quote, ctx.key()))
//!         .retry(RetryPolicy::attempts(5))
//!         .await?;
//!     match ctx.wait_for(REVIEW).timeout(Duration::from_secs(86_400)).await? {
//!         Some(true) => Ok(id),
//!         _ => ctx.step("refund", || async { Ok::<_, Error>(String::new()) }).await,
//!     }
//! });
//! let run = engine.start(CHECKOUT, "order-123", &21).await?;
//! run.signal(REVIEW, &true).await?;
//! println!("{}", run.result().await?);
//! # Ok(()) }
//! ```
//!
//! Options are builder methods on the operation, then `.await`: a step's
//! `.retry(..)`, `.timeout(..)`, `.read_only()`, `.limit(..)`; a wait's
//! `.timeout(..)`; a start's `.id_policy(..)`, `.timeout(..)`; an update's
//! or call's `.id(..)`. A step's error is anything that converts into a
//! [`Failure`] (every error type does); return [`Failure::fatal`] to fail
//! without retries.
//!
//! ## Guarantees
//!
//! - An acknowledged start, signal or step result is never lost within the
//!   durability mode's failure model.
//! - A recorded step result never changes, and code after it sees the same
//!   value on every run.
//! - A step's function runs at least once. It runs again only if the
//!   process failed before that step's result became durable.
//!   [`Ctx::key`] returns a stable key; pass it to services that accept
//!   idempotency keys to make the effect exactly once.
//! - Nothing leaves the process based on state a crash could take back.

mod archive;
pub mod audit;
pub mod backup;
pub mod codec;
pub mod concurrency;
pub mod ctx;
pub mod defs;
pub mod delay;
pub mod dev;
/// For the bindings and the server; not part of the documented interface.
#[doc(hidden)]
pub mod disk;
pub mod engine;
/// For the bindings and the server; not part of the documented interface.
#[doc(hidden)]
pub mod env;
pub mod error;
pub mod export;
pub mod failure;
/// For the bindings and the server; not part of the documented interface.
#[doc(hidden)]
pub mod foreign;
pub mod http;
/// For the bindings and the server; not part of the documented interface.
#[doc(hidden)]
pub mod journal;
pub mod limits;
/// For the bindings and the server; not part of the documented interface.
#[doc(hidden)]
pub mod local;
pub mod log;
pub mod object;
pub mod queue;
pub mod reshard;
/// For the bindings and the server; not part of the documented interface.
#[doc(hidden)]
pub mod runtime;
pub mod schedule;
/// Spin-then-park budgets, shared with the replication I/O thread.
#[doc(hidden)]
pub mod spin;
pub mod stream;
#[cfg(feature = "opentelemetry")]
pub mod telemetry;
#[cfg(feature = "testing")]
pub mod testing;
mod trim;
pub mod upgrade;
/// For the bindings and the server; not part of the documented interface.
#[doc(hidden)]
pub mod visibility;
/// For the bindings and the server; not part of the documented interface.
#[doc(hidden)]
pub mod wal;

pub use codec::{Codec, aes_gcm};
pub use ctx::{
    ChildOptions, Completion, Ctx, Exhausted, RetryPolicy, StepOptions, WaitOptions, WorkflowLog,
};
pub use defs::{AsQuery, AsSignal, AsUpdate, AsWorkflow, Op, Query, Signal, Update, WorkflowRef};
pub use dev::Profile;
pub use engine::{
    Config, Durability, Engine, EventOptions, RunHandle, ScheduleHandle, StartOptions,
    UpdateOptions,
};
pub use error::{Error, Result};
pub use export::{ReplayReport, Replayer};
pub use failure::{Failure, FailureData, IntoFailure};
pub use fusee_core as core;
pub use limits::{LimitInfo, LimitSpec, Priority};
pub use local::{FlushMethod, LocalOptions, Preallocate};
pub use object::{
    CallOptions, Object, ObjectCall, ObjectCtx, ObjectHandlers, ObjectRef, ObjectType, State,
};
pub use queue::{Batch, Debounce, Receipt};
pub use runtime::{IdPolicy, Metrics, OnFinished, OnRunning, ParentClose, RunDetail, RunSummary};
pub use schedule::{Overlap, ScheduleSpec};
pub use stream::{Chunk, ChunkValue, StreamBatch, StreamEnd, StreamReader, StreamWriter};
