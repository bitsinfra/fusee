//! The CI seed set, and proof that the checkers catch broken engines.

use fusee::runtime::Sabotage;
use fusee_sim::{SimConfig, run_seed};

/// Read-only steps must really run before the records ahead of them are
/// durable: some seed crashes after a read ran on state the crash took back,
/// and the read runs again with a different input. (A gated read could never
/// see a different input.) The service checker still holds for the step with
/// effects that consumes the reads.
#[test]
fn read_only_steps_run_ahead_of_the_disk() {
    let mut rerun = 0;
    for seed in 0..300 {
        match run_seed(SimConfig::for_seed(seed)) {
            Ok(r) => rerun += r.reads_rerun,
            Err(f) => panic!("{f}"),
        }
        if rerun > 0 {
            eprintln!("a read-only step re-ran on taken-back state by seed {seed}");
            return;
        }
    }
    panic!("no read-only step ran ahead of the disk in 300 seeds");
}

#[test]
fn ci_seed_set_is_clean() {
    let n: u64 = std::env::var("FUSEE_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);
    for seed in 0..n {
        if let Err(f) = run_seed(SimConfig::for_seed(seed)) {
            panic!("{f}");
        }
    }
}

fn caught(sabotage: Sabotage) -> (u64, String) {
    for seed in 0..2000 {
        let mut cfg = SimConfig::for_seed(seed);
        cfg.sabotage = sabotage;
        cfg.crash_every_ms = cfg.crash_every_ms.max(150);
        if let Err(f) = run_seed(cfg) {
            return (seed, f.message);
        }
    }
    panic!("no seed caught {sabotage:?}");
}

#[test]
fn checkers_catch_steps_that_skip_the_durability_gate() {
    let (seed, msg) = caught(Sabotage::SkipGate);
    eprintln!("SkipGate caught by seed {seed}: {msg}");
}

#[test]
fn checkers_catch_outputs_released_before_durability() {
    let (seed, msg) = caught(Sabotage::EarlyRelease);
    eprintln!("EarlyRelease caught by seed {seed}: {msg}");
}

#[test]
fn checkers_catch_updates_accepted_twice() {
    let (seed, msg) = caught(Sabotage::DuplicateUpdates);
    eprintln!("DuplicateUpdates caught by seed {seed}: {msg}");
}

#[test]
fn checkers_catch_limits_handing_out_too_many_permits() {
    let (seed, msg) = caught(Sabotage::IgnoreLimits);
    eprintln!("IgnoreLimits caught by seed {seed}: {msg}");
}

#[test]
fn checkers_catch_a_scheduled_time_firing_twice() {
    let (seed, msg) = caught(Sabotage::NoAttach);
    eprintln!("NoAttach caught by seed {seed}: {msg}");
}

#[test]
fn checkers_catch_query_answers_released_early() {
    let (seed, msg) = caught(Sabotage::EarlyAnswers);
    eprintln!("EarlyAnswers caught by seed {seed}: {msg}");
}

#[test]
fn checkers_catch_stream_chunks_read_before_durability() {
    let (seed, msg) = caught(Sabotage::EarlyChunks);
    eprintln!("EarlyChunks caught by seed {seed}: {msg}");
}

/// The shared log must not write a batch before the blobs its offloaded
/// records reference are durable: some seed crashes with such a batch
/// durable and its blob torn, and recovery finds the damage.
#[test]
fn checkers_catch_batches_written_before_their_blobs_are_durable() {
    for seed in (0..4000).filter(|s| s % 2 == 1) {
        let mut cfg = SimConfig::for_seed(seed);
        if cfg.offload_bytes == 0 {
            continue;
        }
        cfg.early_blobs = true;
        cfg.crash_every_ms = cfg.crash_every_ms.max(150);
        if let Err(f) = run_seed(cfg) {
            eprintln!("early blobs caught by seed {seed}: {}", f.message);
            return;
        }
    }
    panic!("no seed caught batches written ahead of their blobs");
}

/// Offloaded records go through crashes in most shared-log seeds.
#[test]
fn shared_log_seeds_offload_records_through_crashes() {
    let (mut seeds, mut records) = (0, 0);
    for seed in (0..400).filter(|s| s % 2 == 1) {
        let r = run_seed(SimConfig::for_seed(seed)).unwrap_or_else(|f| panic!("{f}"));
        if r.offloaded > 0 && r.crashes > 0 {
            seeds += 1;
            records += r.offloaded;
        }
    }
    eprintln!("{records} records offloaded in {seeds} seeds with crashes");
    assert!(
        seeds > 50 && records > 5000,
        "{seeds} seeds, {records} records"
    );
}

/// Readers follow streams to their end in most seeds, through crashes.
#[test]
fn readers_follow_streams_through_crashes() {
    let (mut chunks, mut ended, mut crashed) = (0, 0, 0);
    for seed in 0..200 {
        let r = run_seed(SimConfig::for_seed(seed)).unwrap_or_else(|f| panic!("{f}"));
        chunks += r.chunks_read;
        ended += r.streams_read;
        if r.crashes > 0 && r.streams_read > 0 {
            crashed += 1;
        }
    }
    eprintln!(
        "{chunks} chunks read, {ended} streams read to the end, {crashed} seeds with crashes"
    );
    assert!(chunks > 1000 && ended > 300 && crashed > 50);
}

#[test]
fn checkpoint_cleanup_preserves_a_contiguous_log_suffix() {
    for seed in [
        710324065656280,
        710324065656850,
        710324065657162,
        710324065657257,
        710324065657500,
    ] {
        fusee_sim::run_seed(fusee_sim::SimConfig::for_seed(seed)).unwrap();
    }
}

#[test]
fn a_retried_cancellation_finishes_after_recovery() {
    for seed in [3410130088469687, 3939016583237521843] {
        run_seed(SimConfig::for_seed(seed)).unwrap();
    }
}

#[test]
fn a_failed_parent_shard_cannot_acknowledge_a_child_result() {
    // Two successful children resend to a parent whose disk failed. Their
    // results must stay in the outbox until recovery durably accepts them.
    let seed = 3738131864211340;
    fusee_sim::run_seed(fusee_sim::SimConfig::for_seed(seed)).unwrap();
}
