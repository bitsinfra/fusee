#![doc(html_no_source)]

//! The deterministic core of [Fusee](https://github.com/bitsinfra/fusee).
//!
//! Nothing in this crate performs I/O, reads a clock, spawns a thread or
//! takes a lock. Each piece is a state machine driven by its host:
//!
//! - [`record`]: journal records and their encodings.
//! - [`segment`]: the log segment format, the in-memory [`segment::LogWriter`]
//!   and the recovery scanner that tells torn tails from lost records.
//! - [`run`]: the per-run [`run::RunMachine`] that assigns journal positions,
//!   replays recorded results in journal order and detects nondeterminism.
//! - [`shard`]: [`shard::ShardCore`], the state derived from a shard's log:
//!   runs, timers, signals, and outputs held until their records are durable.
//! - [`wheel`]: the hierarchical timing wheel.
//! - [`store`]: run store and checkpoint formats.
//! - [`table`]: the compact table of every run a shard knows.
//! - [`vsr`]: the replication protocol as a pure state machine.
//! - [`frame`]: the framed binary protocol shared by replication and server
//!   mode.
//!
//! Because the core is pure, the same code runs in production and inside the
//! deterministic simulator (`fusee-sim`), where disks tear writes and networks
//! drop messages under a seeded scheduler.

#![forbid(unsafe_code)]

pub mod frame;
pub mod record;
pub mod run;
pub mod segment;
pub mod shard;
pub mod store;
pub mod table;
pub mod vsr;
pub mod wal;
pub mod wheel;
pub mod wire;

pub use record::{Lsn, Name, Payload, Record, RecordBuf, RecordRef, RunId, Seq, Status};
