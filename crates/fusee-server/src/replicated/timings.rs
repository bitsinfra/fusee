//! Optional protocol-stage timing for paired network and storage measurements.
//!
//! The critical path of one replicated step, as the stages below divide it
//! (`docs/replication.md`, Where a step's time goes):
//!
//! | Stage | Member | From | To |
//! | --- | --- | --- | --- |
//! | `shard_stage_to_take` | primary | a shard hands the I/O thread its section | the I/O thread takes it |
//! | `take_to_prepare_sent` | primary | taken | its Prepare written to every backup's socket |
//! | `prepare_sent_to_ack_read` | primary | Prepare written | the read that brought the acknowledgment completing its quorum |
//! | `prepare_read_to_write` | backup | the read that brought a Prepare | its log write starts |
//! | `protocol_log_append`, `protocol_log_sync` | backup | write | durable |
//! | `durable_to_ack_sent` | backup | durable | its acknowledgment written to the socket |
//! | `prepare_read_to_ack_sent` | backup | the read that brought a Prepare | its acknowledgment written |
//! | `ack_read_to_release` | primary | that read | committed position published to the shards |
//! | `release_to_shard_resume` | primary | published | the shard sees its records committed |
//! | `shard_resume_to_stage` | primary | the shard resumed | it hands the I/O thread its next section |
//!
//! Beside the path, `io_thread_tick` is the I/O thread's timer work and
//! `io_thread_lock_hold` each hold of the protocol lock by its main task.
//!
//! Network time is `prepare_sent_to_ack_read` less the faster backup's
//! `prepare_read_to_ack_sent`. Every stage is recorded only while timings
//! are enabled: the normal protocol reads no clock for them.
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::OnceLock,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Instant,
};

const LIMITS_NS: [u64; 27] = [
    1_000,
    2_000,
    3_000,
    5_000,
    7_500,
    10_000,
    15_000,
    20_000,
    25_000,
    30_000,
    35_000,
    40_000,
    50_000,
    60_000,
    75_000,
    100_000,
    125_000,
    150_000,
    200_000,
    300_000,
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    100_000_000,
    u64::MAX,
];

#[derive(Default)]
pub(super) struct Stage {
    total: AtomicU64,
    max: AtomicU64,
    buckets: [AtomicU64; LIMITS_NS.len()],
}

/// Cumulative nanoseconds and exact bucket counts. Subtract consecutive
/// observations to measure one trial on a single member.
#[derive(Serialize)]
pub struct StageDistribution {
    pub total_ns: u64,
    pub max_ns: u64,
    pub histogram_ns: Vec<(u64, u64)>,
}

/// Nanoseconds since this process first asked (never 0), so instants fit
/// in an atomic shared with other threads.
pub(super) fn now_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    (START.get_or_init(Instant::now).elapsed().as_nanos() as u64).max(1)
}

impl Stage {
    pub(super) fn observe(&self, start: Option<Instant>) {
        let Some(start) = start else { return };
        self.record(start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
    }

    /// Between two instants (`end` at or after `start`).
    pub(super) fn between(&self, start: Option<Instant>, end: Option<Instant>) {
        if let (Some(s), Some(e)) = (start, end) {
            self.record(e.saturating_duration_since(s).as_nanos() as u64);
        }
    }

    /// Since a [`now_ns`] reading (0: none).
    pub(super) fn since_ns(&self, start: u64) {
        if start != 0 {
            self.record(now_ns().saturating_sub(start));
        }
    }

    fn record(&self, ns: u64) {
        self.total.fetch_add(ns, Ordering::Relaxed);
        self.max.fetch_max(ns, Ordering::Relaxed);
        self.buckets[LIMITS_NS.partition_point(|&top| top < ns)].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> StageDistribution {
        StageDistribution {
            total_ns: self.total.load(Ordering::Relaxed),
            max_ns: self.max.load(Ordering::Relaxed),
            histogram_ns: LIMITS_NS
                .iter()
                .zip(&self.buckets)
                .map(|(&limit, count)| (limit, count.load(Ordering::Relaxed)))
                .collect(),
        }
    }
}

/// What one member's stages took between two snapshots: for each stage
/// that ran, its count, mean and bucket bounds of p50 and p99 in
/// microseconds (the true value is at most the bound, above the previous
/// bucket's), as JSON. A count stage (`*_count_us`) reports counts instead
/// of microseconds.
pub fn stage_summary(
    before: &BTreeMap<&'static str, StageDistribution>,
    after: &BTreeMap<&'static str, StageDistribution>,
) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for (name, a) in after {
        let b = before.get(name);
        let hist: Vec<(u64, u64)> = a
            .histogram_ns
            .iter()
            .map(|&(limit, n)| {
                let was = b
                    .and_then(|b| b.histogram_ns.iter().find(|(l, _)| *l == limit))
                    .map_or(0, |&(_, n)| n);
                (limit, n.saturating_sub(was))
            })
            .collect();
        let count: u64 = hist.iter().map(|(_, n)| n).sum();
        if count == 0 {
            continue;
        }
        let total = a.total_ns.saturating_sub(b.map_or(0, |b| b.total_ns));
        let bound = |q: f64| {
            let need = (q * count as f64).ceil() as u64;
            let mut seen = 0;
            for &(limit, n) in &hist {
                seen += n;
                if seen >= need.max(1) {
                    return if limit == u64::MAX {
                        f64::INFINITY
                    } else {
                        limit as f64 / 1000.0
                    };
                }
            }
            f64::INFINITY
        };
        out.insert(
            name.to_string(),
            serde_json::json!({
                "count": count,
                "mean_us": (total as f64 / count as f64 / 10.0).round() / 100.0,
                "p50_bound_us": bound(0.5),
                "p99_bound_us": bound(0.99),
            }),
        );
    }
    serde_json::Value::Object(out)
}

#[derive(Default)]
pub(super) struct Timings {
    enabled: AtomicBool,
    pub app_queue: Stage,
    pub protocol_append: Stage,
    pub protocol_sync: Stage,
    pub quorum: Stage,
    pub materialize: Stage,
    pub outbound_queue: Stage,
    pub inbound_queue: Stage,
    pub hint: Stage,
    // The critical path of a step (see the module documentation).
    pub stage_to_take: Stage,
    pub take_to_sent: Stage,
    pub sent_to_ack: Stage,
    pub ack_to_release: Stage,
    pub release_to_resume: Stage,
    pub resume_to_stage: Stage,
    pub prepare_to_write: Stage,
    pub durable_to_ack: Stage,
    pub prepare_to_ack: Stage,
    /// Entries per Prepare the primary sent, and sections per entry (as
    /// counts in nanosecond buckets: 1 µs = one).
    pub entries_per_prepare: Stage,
    pub sections_per_entry: Stage,
    /// The I/O thread's timer work (heartbeats, timeouts, checkpoints),
    /// and each hold of the protocol lock by its main task: a shard that
    /// stages a section meanwhile, or an acknowledgment that arrives, waits
    /// for it.
    pub io_tick: Stage,
    pub io_lock_hold: Stage,
    /// The parts of a tick: the protocol's timers, processing what they
    /// produced, and checkpoint upkeep; within that, publishing a
    /// generation and reclaiming old ones (each holds the protocol lock).
    pub tick_protocol: Stage,
    pub tick_process: Stage,
    pub tick_checkpoints: Stage,
    pub checkpoint_publish: Stage,
    pub checkpoint_reclaim: Stage,
    pub checkpoint_capture: Stage,
    pub checkpoint_install: Stage,
    /// Within a publication or an installation: writing the new generation
    /// (its log tail, superblock, leases and pointer) and switching to it
    /// (the old generation's files removed).
    pub generation_write: Stage,
    pub generation_switch: Stage,
    /// Within an installation: checking the tail (with what came since),
    /// and replacing the replica's protocol state with it.
    pub install_check: Stage,
    pub install_replica: Stage,
}

impl Timings {
    pub(super) fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }
    pub(super) fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }
    pub(super) fn start(&self) -> Option<Instant> {
        self.enabled().then(Instant::now)
    }
    /// [`now_ns`] while enabled, else 0.
    pub(super) fn stamp(&self) -> u64 {
        if self.enabled() { now_ns() } else { 0 }
    }
    /// Records a count (one per microsecond bucket).
    pub(super) fn count(&self, stage: &Stage, n: usize) {
        if self.enabled() {
            stage.record(n as u64 * 1000);
        }
    }
    pub(super) fn snapshot(&self) -> BTreeMap<&'static str, StageDistribution> {
        [
            ("application_proposal_queue", &self.app_queue),
            ("protocol_log_append", &self.protocol_append),
            ("protocol_log_sync", &self.protocol_sync),
            ("proposal_to_quorum", &self.quorum),
            ("committed_batch_materialization", &self.materialize),
            ("outbound_frame_queue", &self.outbound_queue),
            ("inbound_frame_queue", &self.inbound_queue),
            ("applied_watermark_sync", &self.hint),
            ("shard_stage_to_take", &self.stage_to_take),
            ("take_to_prepare_sent", &self.take_to_sent),
            ("prepare_sent_to_ack_read", &self.sent_to_ack),
            ("ack_read_to_release", &self.ack_to_release),
            ("release_to_shard_resume", &self.release_to_resume),
            ("shard_resume_to_stage", &self.resume_to_stage),
            ("prepare_read_to_write", &self.prepare_to_write),
            ("durable_to_ack_sent", &self.durable_to_ack),
            ("prepare_read_to_ack_sent", &self.prepare_to_ack),
            ("entries_per_prepare_count_us", &self.entries_per_prepare),
            ("sections_per_entry_count_us", &self.sections_per_entry),
            ("io_thread_tick", &self.io_tick),
            ("io_thread_lock_hold", &self.io_lock_hold),
            ("io_tick_protocol", &self.tick_protocol),
            ("io_tick_process", &self.tick_process),
            ("io_tick_checkpoints", &self.tick_checkpoints),
            ("checkpoint_publish", &self.checkpoint_publish),
            ("checkpoint_reclaim", &self.checkpoint_reclaim),
            ("checkpoint_capture", &self.checkpoint_capture),
            ("checkpoint_install", &self.checkpoint_install),
            ("generation_write", &self.generation_write),
            ("generation_switch", &self.generation_switch),
            ("install_check", &self.install_check),
            ("install_replica", &self.install_replica),
        ]
        .into_iter()
        .map(|(name, stage)| (name, stage.snapshot()))
        .collect()
    }
}
