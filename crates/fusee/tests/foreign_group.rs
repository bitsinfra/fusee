//! One engine's shards split across threads (`Group`, the Node SDK's worker
//! threads): messages between parts, reads across parts, and recovery of
//! every part's shards by an engine with a different split.

use fusee::core::record::{Payload, Status};
use fusee::foreign::{EngineEvent, ForeignEngine, Group};
use fusee::runtime::{Delivery, StartRequest};
use fusee::{Config, FlushMethod, IdPolicy};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

fn tmp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-group-{name}-{}-{}",
        std::process::id(),
        fusee::local::unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn payload(b: &[u8]) -> Payload {
    Arc::from(b)
}

/// The first `prefix-N` whose shard `worker` owns.
fn id_on(group: &Group, worker: u32, prefix: &str) -> String {
    (0..)
        .map(|i| format!("{prefix}-{i}"))
        .find(|id| group.owner(group.shard_of(id)) == worker)
        .unwrap()
}

fn start(engine: &ForeignEngine, id: &str, wtype: &str, token: u64) {
    let mut req = StartRequest::new(id, wtype, payload(b"null"), IdPolicy::default());
    req.token = Some(token);
    engine.start(req).unwrap();
}

/// Ticks until `done`, running two workflow types: `receiver` waits for a
/// signal and returns it; `sender` signals `target` (its input) and ends.
fn drive(engine: &ForeignEngine, done: &AtomicBool, mut on: impl FnMut(&EngineEvent)) {
    let mut waits: HashMap<(u32, u64), u32> = HashMap::new();
    while !done.load(Ordering::Acquire) {
        let (events, _) = engine.tick();
        for e in &events {
            on(e);
            match e {
                EngineEvent::Start { shard, run, info } if &*info.wtype == "receiver" => {
                    let host = engine.host(*shard).unwrap();
                    let seq = host.begin_wait(*run, "ping", 0).unwrap();
                    waits.insert((*shard, *run), seq);
                }
                EngineEvent::Start { shard, run, info } if &*info.wtype == "sender" => {
                    let target = String::from_utf8(info.input.to_vec()).unwrap();
                    let host = engine.host(*shard).unwrap();
                    host.send(*run, &target, "ping", payload(b"\"hello\""))
                        .unwrap();
                    drop(host);
                    engine
                        .finish(*shard, *run, Ok(payload(b"\"sent\"")))
                        .unwrap();
                }
                EngineEvent::Wake { shard, run } if waits.contains_key(&(*shard, *run)) => {
                    if let Some(Delivery::Resolve { outcome, .. }) =
                        engine.quiescent(*shard, *run).unwrap()
                    {
                        waits.remove(&(*shard, *run));
                        engine.finish(*shard, *run, Ok(outcome.payload)).unwrap();
                    }
                }
                _ => {}
            }
        }
        std::thread::sleep(Duration::from_micros(200));
    }
}

#[test]
fn parts_on_two_threads_signal_each_other_and_list_together() {
    let group = Group::open(&Config::memory().shards(4), 2).unwrap();
    assert_eq!((group.shards(), group.workers()), (4, 2));
    let receiver = id_on(&group, 1, "receiver");
    let sender = id_on(&group, 0, "sender");
    let done = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicU32::new(0));
    // The receiver exists before the signal is sent.
    let ready = Arc::new(AtomicBool::new(false));
    let threads: Vec<_> = (0..2u32)
        .map(|worker| {
            let (group, done, finished, ready) =
                (group.clone(), done.clone(), finished.clone(), ready.clone());
            let (receiver, sender) = (receiver.clone(), sender.clone());
            std::thread::spawn(move || {
                let engine =
                    ForeignEngine::open_part(group.clone(), worker, Arc::new(|| {})).unwrap();
                let mine = if worker == 0 { &sender } else { &receiver };
                assert!(engine.owns(group.shard_of(mine)));
                if worker == 1 {
                    start(&engine, &receiver, "receiver", 1);
                } else {
                    while !ready.load(Ordering::Acquire) {
                        std::thread::sleep(Duration::from_micros(200));
                    }
                    let mut req = StartRequest::new(
                        &sender,
                        "sender",
                        payload(receiver.as_bytes()),
                        IdPolicy::default(),
                    );
                    req.token = Some(2);
                    engine.start(req).unwrap();
                }
                drive(&engine, &done, |e| match e {
                    EngineEvent::Ack { token: 1 } => ready.store(true, Ordering::Release),
                    EngineEvent::Result { status, .. } => {
                        assert_eq!(*status, Status::Ok);
                        finished.fetch_add(1, Ordering::AcqRel);
                    }
                    _ => {}
                });
                engine.close().unwrap();
            })
        })
        .collect();
    let until = Instant::now() + Duration::from_secs(20);
    while finished.load(Ordering::Acquire) < 2 {
        assert!(Instant::now() < until, "the signal did not cross parts");
        std::thread::sleep(Duration::from_millis(1));
    }
    let runs = group.list_where(None, 100).unwrap();
    let mut ids: Vec<&str> = runs.iter().map(|r| r.id.as_str()).collect();
    ids.sort();
    let mut want = vec![receiver.as_str(), sender.as_str()];
    want.sort();
    assert_eq!(ids, want);
    // Shard order, as one engine lists them.
    assert!(runs.windows(2).all(|w| w[0].shard <= w[1].shard));
    let detail = group.inspect(&receiver).unwrap().unwrap();
    assert_eq!(detail.summary.status, "ok");
    assert_eq!(group.shard_metrics().unwrap().len(), 4);
    done.store(true, Ordering::Release);
    for t in threads {
        t.join().unwrap();
    }
    assert!(
        group.list_where(None, 1).is_err(),
        "closed parts answer nothing"
    );
}

#[test]
fn another_split_recovers_every_parts_shards() {
    let dir = tmp("recover");
    let mut cfg = Config::local(&dir);
    cfg.local.flush = FlushMethod::Fsync;
    let ids: Vec<String> = {
        let group = Group::open(&cfg, 4).unwrap();
        assert_eq!(group.shards(), 4, "a new directory gets a shard per part");
        let ids: Vec<String> = (0..4).map(|w| id_on(&group, w, "wait")).collect();
        let threads: Vec<_> = (0..4u32)
            .map(|w| {
                let (group, id) = (group.clone(), ids[w as usize].clone());
                std::thread::spawn(move || {
                    let engine = ForeignEngine::open_part(group, w, Arc::new(|| {})).unwrap();
                    start(&engine, &id, "receiver", 7);
                    loop {
                        let (events, _) = engine.tick();
                        if events.contains(&EngineEvent::Ack { token: 7 }) {
                            break;
                        }
                        std::thread::sleep(Duration::from_micros(200));
                    }
                    // Dropped without closing: no checkpoint, as in a crash.
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        ids
    };
    // The same directory, all four shards in one engine.
    let engine = ForeignEngine::open(&cfg.clone().shards(4), Arc::new(|| {})).unwrap();
    let mut started = Vec::new();
    let until = Instant::now() + Duration::from_secs(10);
    while started.len() < 4 {
        assert!(Instant::now() < until, "recovered runs did not start");
        let (events, _) = engine.tick();
        for e in events {
            if let EngineEvent::Start { info, .. } = e {
                started.push(info.id.to_string());
            }
        }
    }
    started.sort();
    let mut want = ids.clone();
    want.sort();
    assert_eq!(started, want);
    engine.close().unwrap();
    drop(engine);
    // And split in two.
    let group = Group::open(&cfg, 2).unwrap();
    assert_eq!((group.shards(), group.workers()), (4, 2));
    let found = Arc::new(AtomicU32::new(0));
    let threads: Vec<_> = (0..2u32)
        .map(|w| {
            let (group, found) = (group.clone(), found.clone());
            std::thread::spawn(move || {
                let engine = ForeignEngine::open_part(group, w, Arc::new(|| {})).unwrap();
                let until = Instant::now() + Duration::from_secs(10);
                let mut mine = 0;
                while mine < 2 && Instant::now() < until {
                    let (events, _) = engine.tick();
                    mine += events
                        .iter()
                        .filter(|e| matches!(e, EngineEvent::Start { .. }))
                        .count();
                    std::thread::sleep(Duration::from_micros(200));
                }
                found.fetch_add(mine as u32, Ordering::AcqRel);
                engine.close().unwrap();
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(found.load(Ordering::Acquire), 4);
    drop(group);
    let _ = std::fs::remove_dir_all(&dir);
}
