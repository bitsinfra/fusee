//! How a foreign host's deliveries reach the disk. The SDKs pump one
//! delivery per turn of their event loop and tick the engine in between, so
//! these tests drive `ForeignEngine` the same way.

use fusee::core::record::{Payload, Status};
use fusee::core::run::StepStart;
use fusee::foreign::{EngineEvent, ForeignEngine};
use fusee::runtime::{Delivery, StartRequest};
use fusee::{Config, FlushMethod, IdPolicy, Metrics};
use std::sync::Arc;
use std::time::Duration;

fn tmp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-foreign-{name}-{}-{}",
        std::process::id(),
        fusee::local::unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn payload(b: &[u8]) -> Payload {
    Arc::from(b)
}

fn open(dir: &std::path::Path) -> ForeignEngine {
    let mut cfg = Config::local(dir).shards(1);
    cfg.local.flush = FlushMethod::Fsync;
    ForeignEngine::open(&cfg, Arc::new(|| {})).unwrap()
}

fn metrics(engine: &ForeignEngine) -> Metrics {
    engine.shard_metrics()[0]
}

/// Starts one run and waits for its Start event.
fn start(engine: &ForeignEngine, id: &str) -> (u32, u64) {
    let (shard, _) = engine
        .start(StartRequest::new(
            id,
            "fan",
            payload(b"null"),
            IdPolicy::default(),
        ))
        .unwrap();
    loop {
        let (events, _) = engine.tick();
        for e in events {
            if let EngineEvent::Start { run, .. } = e {
                return (shard, run);
            }
        }
    }
}

/// A fan-out of `n` steps whose functions all finish before the host pumps
/// the first delivery. Returns the metrics of the delivery phase.
fn fan_out(engine: &ForeignEngine, id: &str, n: usize) -> (Metrics, Metrics) {
    let (shard, run) = start(engine, id);
    let host = engine.host(shard).unwrap();
    let mut steps = Vec::new();
    for i in 0..n {
        let (seq, start, gate) = host.begin_step(run, &format!("s{i}")).unwrap();
        assert!(matches!(start, StepStart::Run { .. }));
        steps.push((seq, gate));
    }
    for (_, gate) in &steps {
        while !engine.may_run_step(shard, run, *gate, 0, false).unwrap() {
            engine.tick();
            std::thread::sleep(Duration::from_micros(50));
        }
    }
    for (seq, _) in &steps {
        host.step_done(run, *seq, Status::Ok, payload(b"1"));
    }
    let before = metrics(engine);
    let mut delivered = 0;
    while delivered < n {
        // One turn of the host's event loop per delivery.
        engine.tick();
        match engine.quiescent(shard, run).unwrap() {
            Some(Delivery::Resolve { .. }) => delivered += 1,
            Some(other) => panic!("unexpected delivery {other:?}"),
            None => {}
        }
    }
    engine.tick();
    let after = metrics(engine);
    let journal = engine.inspect(id).unwrap().unwrap().journal;
    let steps = journal
        .iter()
        .filter(|r| matches!(r, fusee::core::Record::Step { .. }))
        .count();
    assert_eq!(steps, n);
    drop(host);
    engine.finish(shard, run, Ok(payload(b"42"))).unwrap();
    loop {
        let (events, _) = engine.tick();
        if events
            .iter()
            .any(|e| matches!(e, EngineEvent::Result { run: r, .. } if *r == run))
        {
            break;
        }
        std::thread::sleep(Duration::from_micros(50));
    }
    (before, after)
}

fn memory() -> ForeignEngine {
    // Flushes take no time here, so every one is fast enough to run inline:
    // the counts below show which ones the engine chose to run inline.
    ForeignEngine::open(&Config::memory().shards(1), Arc::new(|| {})).unwrap()
}

#[test]
fn a_foreign_fan_out_batches_its_step_records() {
    let engine = memory();
    let n = 100;
    let (before, after) = fan_out(&engine, "fan-1", n);
    // Before, each delivery's record was flushed inline on the host's thread
    // (one blocking flush per branch). Now they go to the background, where
    // records that arrive during a flush share the next one.
    let inline = after.inline_flushes - before.inline_flushes;
    let records = after.flushed_records - before.flushed_records;
    assert_eq!(records, n as u64);
    assert!(
        inline <= 1,
        "{inline} inline flushes blocked the host's loop while it had deliveries to pump"
    );
}

#[test]
fn a_foreign_fan_out_on_disk_keeps_every_result() {
    let dir = tmp("fanout");
    let engine = open(&dir);
    fan_out(&engine, "fan-1", 100);
    engine.close().unwrap();
    drop(engine);
    let engine = open(&dir);
    match engine.result("fan-1").unwrap().1 {
        fusee::runtime::ResultState::Done {
            status, payload, ..
        } => {
            assert_eq!((status, &payload[..]), (Status::Ok, &b"42"[..]));
        }
        other => panic!("{other:?}"),
    }
    drop(engine);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_lone_start_flushes_inline() {
    // In `memory` mode a record is durable once appended
    // (`Env::durable_on_append`): the start takes no flush at all, on this
    // thread or the flusher's, and is acknowledged at once.
    let engine = memory();
    let before = metrics(&engine);
    let (shard, run) = start(&engine, "lone");
    let after = metrics(&engine);
    assert_eq!(
        after.flushes - before.flushes,
        0,
        "the start record was flushed"
    );
    assert_eq!(after.flushed_records - before.flushed_records, 1);
    engine.finish(shard, run, Ok(payload(b"1"))).unwrap();
}
