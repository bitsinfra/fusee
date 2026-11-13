//! Deterministic simulation testing for Fusee.
//!
//! The real shard runtime, journal machines and Rust SDK run inside one
//! thread under a seeded scheduler. The disk tears writes, loses unflushed
//! data at crashes and fails flushes; messages between shards are delayed
//! and lost with the process. A mock external service logs every call. A
//! seed reproduces a run exactly.
//!
//! The checkers, after every recovery and at the end:
//!
//! - every acknowledged start and signal exists;
//! - a result once reported never changes;
//! - no external call was made with inputs the recovered journal
//!   contradicts (one idempotency key, two different inputs);
//! - once faults stop, every workflow finishes.
//!
//! [`scenario`] runs your own workflows the same way.

pub mod disk;
pub mod env;
pub mod rng;
pub mod scenario;
pub mod world;

pub use scenario::Scenario;
pub use world::{Failure, Report, SimConfig, run_seed};

/// Waits `d` of simulated time, inside a step body under the simulator.
pub async fn sleep(d: std::time::Duration) {
    world::sim_sleep(d.as_millis() as u64).await
}
