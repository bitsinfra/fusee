//! Changing a local directory's shard count offline (`fusee reshard`).
//!
//! - Round trips through 1 → 4 → 3 → 8 → 1 shards with every kind of run
//!   state open across each change: runs in the middle of a step, suspended
//!   on timers and signals, with buffered signals and accepted updates,
//!   async completions waiting for their tokens, continue-as-new chains,
//!   parents and children on different shards, a schedule, debounce and
//!   batch windows, search attributes, and finished runs.
//! - A directory whose log tail regenerates every message at recovery: each
//!   is sent again after the change and received once.
//! - History archives move to the shard their IDs hash to.
//! - A failure at every step of the tool, then a rerun (with a second
//!   failure on the way): the directory opens before the switch as it was,
//!   refuses to open during it, and ends correct.

use fusee::reshard::{ReshardOptions, ReshardReport, Resharded, reshard};
use fusee::{Batch, Config, Ctx, Debounce, Engine, Error, ScheduleSpec};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn tmp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-reshard-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cfg(dir: &Path, shards: u32) -> Config {
    let mut c = Config::local(dir)
        .shards(shards)
        .park_after(Duration::from_millis(50));
    c.local.flush = fusee::FlushMethod::Fsync;
    c
}

fn opts() -> ReshardOptions {
    let mut o = ReshardOptions::default();
    o.local.flush = fusee::FlushMethod::Fsync;
    o
}

async fn eventually(what: &str, mut f: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !f().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn shard_of(id: &str, shards: u32) -> u32 {
    fusee::core::shard::shard_of(id, shards)
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let target = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &target);
        } else {
            std::fs::copy(e.path(), target).unwrap();
        }
    }
}

fn done(r: Resharded) -> ReshardReport {
    match r {
        Resharded::Done(r) => r,
        other => panic!("expected a reshard, got {other:?}"),
    }
}

/// What workflow code shares with the test.
#[derive(Default)]
struct Shared {
    /// `busy`: `Ctx::key` at each attempt of its step, and when to finish.
    keys: Mutex<Vec<String>>,
    release: AtomicBool,
    /// `approval`: the token its async step handed out, by workflow ID.
    tokens: Mutex<BTreeMap<String, String>>,
    ticks: AtomicU32,
    debounced: AtomicU32,
    /// Child step effects, by child ID.
    effects: Mutex<BTreeMap<String, u32>>,
}

const GATES: u32 = 12;
const PARENTS: u32 = 6;
const KIDS: u32 = 3;
const QUICK: u32 = 10;
const TAGGED: u32 = 5;
const CHAIN_END: u32 = 4;

type Windows = (
    fusee::WorkflowRef<u32, u32>,
    fusee::WorkflowRef<Vec<u32>, Vec<u32>>,
);

fn register(engine: &Engine, sh: &Arc<Shared>) -> Windows {
    // Waits for `go1`, `go2`, ... and sums them.
    engine.register("gate", |ctx: Ctx, phases: u32| async move {
        let mut sum = 0;
        for p in 1..=phases {
            let v: Option<u32> = ctx.wait_for(&format!("go{p}")).await?;
            sum += v.unwrap_or(0);
        }
        Ok(sum)
    });
    engine.register("parent", |ctx: Ctx, i: u32| async move {
        let ids: Vec<String> = (0..KIDS).map(|j| format!("child-{i}-{j}")).collect();
        let inputs: Vec<u32> = (0..KIDS).collect();
        let kids = ids
            .iter()
            .zip(&inputs)
            .map(|(id, j)| ctx.child::<u32, u32>("child", id, j));
        let mut sum = 0;
        for r in ctx.all(kids).await {
            sum += r?;
        }
        Ok(sum)
    });
    let s = sh.clone();
    engine.register("child", move |ctx: Ctx, j: u32| {
        let s = s.clone();
        async move {
            let id = ctx.workflow_id().to_owned();
            ctx.step("effect", move || {
                let (s, id) = (s.clone(), id.clone());
                async move {
                    *s.effects.lock().unwrap().entry(id).or_default() += 1;
                    Ok::<_, Error>(())
                }
            })
            .await?;
            let v: Option<u32> = ctx.wait_for("done").await?;
            Ok(j * 10 + v.unwrap_or(0))
        }
    });
    engine.register("quick", |_ctx: Ctx, n: u32| async move { Ok(n * 2) });
    let s = sh.clone();
    engine.register("busy", move |ctx: Ctx, _: ()| {
        let s = s.clone();
        async move {
            let c = ctx.clone();
            let v: u32 = ctx
                .step("work", move || {
                    let (s, c) = (s.clone(), c.clone());
                    async move {
                        s.keys.lock().unwrap().push(c.key());
                        while !s.release.load(Ordering::SeqCst) {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        Ok::<_, Error>(7)
                    }
                })
                .await?;
            Ok(v)
        }
    });
    engine.register("sleeper", |ctx: Ctx, ms: u64| async move {
        ctx.sleep("nap", Duration::from_millis(ms)).await?;
        Ok(ms)
    });
    engine.register("buffered", |ctx: Ctx, _: ()| async move {
        let a: Option<u32> = ctx.wait_for("first").await?;
        let b: Option<u32> = ctx.wait_for("later").await?;
        Ok(a.unwrap_or(0) * 100 + b.unwrap_or(0))
    });
    engine.register("counter", |ctx: Ctx, _: ()| async move {
        let total = std::rc::Rc::new(std::cell::Cell::new(0u32));
        let t = total.clone();
        ctx.on_update(
            "add",
            |_: &u32| Ok(()),
            move |n: u32| {
                let t = t.clone();
                async move {
                    t.set(t.get() + n);
                    Ok(t.get())
                }
            },
        );
        let _: Option<()> = ctx.wait_for("stop").await?;
        Ok(total.get())
    });
    let s = sh.clone();
    engine.register("approval", move |ctx: Ctx, _: ()| {
        let s = s.clone();
        async move {
            let id = ctx.workflow_id().to_owned();
            let ok: bool = ctx
                .await_completion("approve", move |c: fusee::Completion| {
                    let token = c.token;
                    let (s, id) = (s.clone(), id.clone());
                    async move {
                        s.tokens.lock().unwrap().insert(id, token);
                        Ok::<_, String>(())
                    }
                })
                .await?;
            Ok(ok)
        }
    });
    engine.register("chain", |ctx: Ctx, n: u32| async move {
        if n >= CHAIN_END {
            return Ok(n);
        }
        let _: Option<u32> = ctx.wait_for("next").await?;
        ctx.continue_as_new(&(n + 1)).await
    });
    engine.register("tagged", |ctx: Ctx, n: u32| async move {
        ctx.set_attributes(&serde_json::json!({"team": "blue", "n": n}))?;
        let _: Option<()> = ctx.wait_for("end").await?;
        Ok(n)
    });
    let s = sh.clone();
    engine.register("tick", move |ctx: Ctx, _: serde_json::Value| {
        let s = s.clone();
        async move {
            ctx.step("tick", move || {
                let s = s.clone();
                async move { Ok::<_, Error>(s.ticks.fetch_add(1, Ordering::SeqCst)) }
            })
            .await?;
            Ok(())
        }
    });
    let s = sh.clone();
    let reindex = engine.register("reindex", move |_ctx: Ctx, n: u32| {
        s.debounced.fetch_add(1, Ordering::SeqCst);
        async move { Ok(n) }
    });
    let bulk = engine.register(
        "bulk",
        |_ctx: Ctx, events: Vec<u32>| async move { Ok(events) },
    );
    (reindex, bulk)
}

/// IDs of every run the scenario starts, finished or not.
fn ids() -> Vec<String> {
    let mut v = Vec::new();
    v.extend((0..GATES).map(|i| format!("gate-{i}")));
    v.extend((0..PARENTS).map(|i| format!("parent-{i}")));
    for i in 0..PARENTS {
        v.extend((0..KIDS).map(|j| format!("child-{i}-{j}")));
    }
    v.extend((0..QUICK).map(|i| format!("quick-{i}")));
    v.extend((0..TAGGED).map(|i| format!("tagged-{i}")));
    for id in [
        "busy-0",
        "sleeper-0",
        "buffered-0",
        "counter-0",
        "approval-0",
        "chain-0",
    ] {
        v.push(id.to_owned());
    }
    v
}

async fn populate(engine: &Engine, sh: &Arc<Shared>, phases: u32) {
    for i in 0..GATES {
        start(engine, "gate", &format!("gate-{i}"), &phases).await;
    }
    for i in 0..PARENTS {
        start(engine, "parent", &format!("parent-{i}"), &i).await;
    }
    for i in 0..QUICK {
        start(engine, "quick", &format!("quick-{i}"), &i).await;
    }
    for i in 0..TAGGED {
        start(engine, "tagged", &format!("tagged-{i}"), &i).await;
    }
    start(engine, "busy", "busy-0", &()).await;
    start(engine, "sleeper", "sleeper-0", &2_500u64).await;
    start(engine, "buffered", "buffered-0", &()).await;
    start(engine, "counter", "counter-0", &()).await;
    start(engine, "approval", "approval-0", &()).await;
    start(engine, "chain", "chain-0", &0u32).await;
    // A signal buffered before the run waits for it.
    signal(engine, "buffered-0", "later", &7u32).await;
    let total: u32 = engine
        .run::<u32>("counter-0")
        .update("add", &5u32)
        .id("u-1")
        .await
        .unwrap();
    assert_eq!(total, 5);
    engine
        .schedule(
            "ticker",
            ScheduleSpec {
                every_ms: Some(200),
                workflow: "tick".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    for i in 0..QUICK {
        let r: u32 = result::<u32>(engine, &format!("quick-{i}")).await;
        assert_eq!(r, i * 2);
    }
    eventually("children started", async || {
        sh.effects.lock().unwrap().len() == (PARENTS * KIDS) as usize
    })
    .await;
    eventually("the busy step", async || {
        !sh.keys.lock().unwrap().is_empty()
    })
    .await;
    eventually("the approval token", async || {
        sh.tokens.lock().unwrap().contains_key("approval-0")
    })
    .await;
}

/// A run's result, or a panic naming the run after 20 seconds.
async fn result<O: serde::de::DeserializeOwned>(engine: &Engine, id: &str) -> O {
    match tokio::time::timeout(Duration::from_secs(20), engine.run::<O>(id).result()).await {
        Ok(r) => r.unwrap_or_else(|e| panic!("{id}: {e}")),
        Err(_) => {
            let mut related = vec![id.to_owned()];
            if let Some(i) = id.strip_prefix("parent-") {
                related.extend((0..KIDS).map(|j| format!("child-{i}-{j}")));
            }
            for r in related {
                if let Ok(Some(d)) = engine.describe(&r).await {
                    eprintln!("{r}: {:?}", d.summary);
                    for rec in &d.journal {
                        eprintln!("    {rec:?}");
                    }
                }
            }
            panic!("no result for {id}")
        }
    }
}

async fn start<I: serde::Serialize>(engine: &Engine, wtype: &str, id: &str, input: &I) {
    let input = serde_json::to_vec(input).unwrap();
    engine
        .start_raw(wtype, id, Arc::from(input), Default::default())
        .await
        .unwrap();
}

async fn signal<T: serde::Serialize>(engine: &Engine, id: &str, name: &str, value: &T) {
    engine
        .run::<serde_json::Value>(id)
        .signal(name, value)
        .await
        .unwrap();
}

/// Every run is where its ID hashes to, and the newest run of the chain
/// is the one its ID finds.
async fn check_placement(engine: &Engine, shards: u32) {
    for id in ids() {
        let s = engine
            .status(&id)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{id} is missing after resharding to {shards}"));
        assert_eq!(s.shard, shard_of(&id, shards), "{id}");
    }
    let chain = engine.status("chain-0").await.unwrap().unwrap();
    assert_ne!(chain.status, "continued_as_new", "{chain:?}");
    let tagged = engine
        .list_where(&serde_json::json!({"team": "blue"}), 100)
        .await
        .unwrap();
    assert_eq!(tagged.len(), TAGGED as usize, "{tagged:?}");
}

/// Applies a reshard's renumbering to the tokens the test holds.
fn follow_tokens(sh: &Shared, report: &ReshardReport) {
    let mut tokens = sh.tokens.lock().unwrap();
    for r in &report.renumbered_unfinished {
        for (old, new) in &r.tokens {
            for t in tokens.values_mut() {
                if t == old {
                    *t = new.clone();
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn round_trips_keep_every_run_where_it_was() {
    let dir = tmp("round");
    let counts = [1u32, 4, 3, 8, 1];
    let phases = counts.len() as u32 - 1;
    let sh = Arc::new(Shared::default());
    let mut renumbered_busy = 0;
    {
        let engine = Engine::open(cfg(&dir, counts[0])).await.unwrap();
        register(&engine, &sh);
        populate(&engine, &sh, phases).await;
        engine.shutdown().await.unwrap();
    }
    let mut windows: Option<(String, String)> = None;
    for (p, &n) in counts.iter().enumerate().skip(1) {
        let p = p as u32;
        // The old count refuses to open with the new one, naming the tool.
        let err = Engine::open(cfg(&dir, n))
            .await
            .err()
            .expect("count mismatch");
        assert!(err.to_string().contains("fusee reshard"), "{err}");
        let report = done(reshard(&dir, n, &opts()).unwrap());
        eprintln!(
            "{} -> {n}: {} active, {} suspended, {} finished, {} messages, {} renumbered ({} unfinished: {:?})",
            report.from,
            report.active,
            report.suspended,
            report.finished,
            report.messages,
            report.renumbered,
            report.renumbered_unfinished.len(),
            report
                .renumbered_unfinished
                .iter()
                .map(|r| (&r.workflow_id, r.old_run, r.new_run, r.tokens.len()))
                .collect::<Vec<_>>()
        );
        assert_eq!((report.from, report.to), (counts[p as usize - 1], n));
        assert!(
            report.active + report.suspended + report.finished >= ids().len() as u64,
            "{report:?}"
        );
        if counts[p as usize - 1] == 1 {
            // Growing from one shard renumbers nothing.
            assert_eq!(report.renumbered, 0, "{report:?}");
        }
        renumbered_busy += report
            .renumbered_unfinished
            .iter()
            .filter(|r| r.workflow_id == "busy-0")
            .count();
        follow_tokens(&sh, &report);
        assert_eq!(
            fusee::engine::read_meta(&dir).unwrap(),
            Some((n, 3)),
            "{report:?}"
        );
        for leftover in ["reshard.new", "reshard.old"] {
            assert!(!dir.join(leftover).exists(), "{leftover}");
        }
        let engine = Engine::open(cfg(&dir, n)).await.unwrap();
        let (reindex, bulk) = register(&engine, &sh);
        check_placement(&engine, n).await;
        // Windows opened just before the last change close after it.
        if let Some((debounced, batched)) = windows.take() {
            let r: u32 = {
                let run = engine.run::<u32>(&debounced);
                run.started().await.unwrap();
                run.result().await.unwrap()
            };
            assert_eq!(r, 3);
            assert_eq!(sh.debounced.load(Ordering::SeqCst), 1);
            let run = engine.run::<Vec<u32>>(&batched);
            run.started().await.unwrap();
            assert_eq!(run.result().await.unwrap(), vec![0, 1, 2]);
        }
        // Finished runs keep their results.
        for i in 0..QUICK {
            let r: u32 = result::<u32>(&engine, &format!("quick-{i}")).await;
            assert_eq!(r, i * 2);
        }
        // Gates take one more signal per phase.
        for i in 0..GATES {
            signal(&engine, &format!("gate-{i}"), &format!("go{p}"), &p).await;
        }
        // The schedule keeps firing.
        let ticks = sh.ticks.load(Ordering::SeqCst);
        eventually("a tick", async || sh.ticks.load(Ordering::SeqCst) > ticks).await;
        match p {
            1 => {
                // A retried update gets its recorded result; a new one adds.
                let run = engine.run::<u32>("counter-0");
                let again: u32 = run.update("add", &5u32).id("u-1").await.unwrap();
                assert_eq!(again, 5);
                let total: u32 = run.update("add", &7u32).id("u-2").await.unwrap();
                assert_eq!(total, 12);
                // Some children finish now, before more changes.
                for i in 0..PARENTS {
                    signal(&engine, &format!("child-{i}-0"), "done", &1u32).await;
                }
            }
            2 => {
                signal(&engine, "buffered-0", "first", &3u32).await;
                let r: u32 = result::<u32>(&engine, "buffered-0").await;
                assert_eq!(r, 307);
            }
            _ => {}
        }
        // The chain continues as new once per phase.
        if p < CHAIN_END {
            let before = engine.status("chain-0").await.unwrap().unwrap().run;
            signal(&engine, "chain-0", "next", &0u32).await;
            eventually("the chain's next run", async || {
                engine
                    .status("chain-0")
                    .await
                    .unwrap()
                    .is_some_and(|s| s.run != before && s.status != "continued_as_new")
            })
            .await;
        }
        if p + 1 < counts.len() as u32 {
            if p == 2 {
                // Debounce and batch windows open across the next change.
                let rule = Debounce::new(Duration::from_millis(1_500));
                let mut debounced = String::new();
                for k in 1..=3u32 {
                    debounced = engine
                        .debounce(&reindex, "doc", &k, &rule)
                        .await
                        .unwrap()
                        .id()
                        .to_owned();
                }
                let rule = Batch::new(10, Duration::from_millis(1_500));
                let mut batched = String::new();
                for k in 0..3u32 {
                    batched = engine
                        .batch(&bulk, "tenant", &k, &rule)
                        .await
                        .unwrap()
                        .id()
                        .to_owned();
                }
                assert!(engine.status(&debounced).await.unwrap().is_none());
                windows = Some((debounced, batched));
            }
            engine.shutdown().await.unwrap();
            continue;
        }
        // Last phase: everything finishes.
        sh.release.store(true, Ordering::SeqCst);
        let r: u32 = result::<u32>(&engine, "busy-0").await;
        assert_eq!(r, 7);
        let keys = sh.keys.lock().unwrap().clone();
        let distinct: std::collections::BTreeSet<&String> = keys.iter().collect();
        // The step ran again after each change (it was in flight); its key
        // changed only when its run was renumbered.
        assert!(keys.len() > phases as usize, "{keys:?}");
        assert_eq!(distinct.len(), 1 + renumbered_busy, "{keys:?}");
        for i in 0..GATES {
            let r: u32 = result::<u32>(&engine, &format!("gate-{i}")).await;
            assert_eq!(r, (1..=phases).sum::<u32>());
        }
        for i in 0..PARENTS {
            for j in 1..KIDS {
                signal(&engine, &format!("child-{i}-{j}"), "done", &1u32).await;
            }
        }
        for i in 0..PARENTS {
            let r: u32 = result::<u32>(&engine, &format!("parent-{i}")).await;
            assert_eq!(r, (0..KIDS).map(|j| j * 10 + 1).sum::<u32>(), "parent-{i}");
        }
        // Each child's step ran once: no child started twice.
        assert!(
            sh.effects.lock().unwrap().values().all(|n| *n == 1),
            "{:?}",
            sh.effects.lock().unwrap()
        );
        let r: u64 = result::<u64>(&engine, "sleeper-0").await;
        assert_eq!(r, 2_500);
        signal(&engine, "counter-0", "stop", &()).await;
        assert_eq!(result::<u32>(&engine, "counter-0").await, 12);
        let token = sh.tokens.lock().unwrap()["approval-0"].clone();
        tokio::time::timeout(Duration::from_secs(10), engine.complete(&token, &true))
            .await
            .expect("completion answered")
            .unwrap();
        assert!(result::<bool>(&engine, "approval-0").await);
        while engine.status("chain-0").await.unwrap().unwrap().status != "ok" {
            let before = engine.status("chain-0").await.unwrap().unwrap().run;
            signal(&engine, "chain-0", "next", &0u32).await;
            eventually("the chain's next run", async || {
                engine
                    .status("chain-0")
                    .await
                    .unwrap()
                    .is_some_and(|s| s.run != before)
            })
            .await;
        }
        assert_eq!(result::<u32>(&engine, "chain-0").await, CHAIN_END);
        for i in 0..TAGGED {
            signal(&engine, &format!("tagged-{i}"), "end", &()).await;
            assert_eq!(result::<u32>(&engine, &format!("tagged-{i}")).await, i);
        }
        engine.shutdown().await.unwrap();
    }
    // Same count: nothing to do.
    assert_eq!(
        reshard(&dir, 1, &opts()).unwrap(),
        Resharded::AlreadyCurrent { shards: 1 }
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A directory that shut down without checkpoints: recovery replays its
/// whole log and regenerates every message its records produced. Each is
/// sent again from the new shards and received once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn messages_regenerated_from_the_log_are_received_once() {
    let dir = tmp("messages");
    const SENDERS: u32 = 16;
    let pings = Arc::new(AtomicU32::new(0));
    let reg = |engine: &Engine, pings: Arc<AtomicU32>| {
        // Counts distinct pings, then waits for the end.
        engine.register("sink", move |ctx: Ctx, n: u32| {
            let pings = pings.clone();
            async move {
                let mut got = 0;
                while got < n {
                    let _: Option<u32> = ctx.wait_for("ping").await?;
                    got += 1;
                    let p = pings.clone();
                    ctx.step(&format!("seen-{got}"), move || {
                        let p = p.clone();
                        async move { Ok::<_, Error>(p.fetch_add(1, Ordering::SeqCst)) }
                    })
                    .await?;
                }
                let _: Option<()> = ctx.wait_for("end").await?;
                Ok(got)
            }
        });
        engine.register("sender", |ctx: Ctx, i: u32| async move {
            ctx.signal_workflow("sink-0", "ping", &i)?;
            // A child on (usually) another shard, finished before the change.
            let r: u32 = ctx.child("leaf", &format!("leaf-{i}"), &i).await?;
            Ok(r)
        });
        engine.register("leaf", |_ctx: Ctx, i: u32| async move { Ok(i + 1) });
    };
    {
        let c = cfg(&dir, 2)
            .checkpoint_on_shutdown(false)
            .checkpoint_interval(Duration::from_secs(3_600));
        let engine = Engine::open(c).await.unwrap();
        reg(&engine, pings.clone());
        start(&engine, "sink", "sink-0", &SENDERS).await;
        for i in 0..SENDERS {
            start(&engine, "sender", &format!("sender-{i}"), &i).await;
        }
        for i in 0..SENDERS {
            let r: u32 = result::<u32>(&engine, &format!("sender-{i}")).await;
            assert_eq!(r, i + 1);
        }
        eventually("every ping", async || {
            pings.load(Ordering::SeqCst) == SENDERS
        })
        .await;
        engine.shutdown().await.unwrap();
    }
    let report = done(reshard(&dir, 3, &opts()).unwrap());
    // Every send, child start and child result in the log was regenerated.
    assert!(report.messages >= u64::from(3 * SENDERS), "{report:?}");
    let engine = Engine::open(cfg(&dir, 3)).await.unwrap();
    reg(&engine, pings.clone());
    // Give the resent messages time to arrive (and be dropped).
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    for i in 0..SENDERS {
        let r: u32 = result::<u32>(&engine, &format!("sender-{i}")).await;
        assert_eq!(r, i + 1);
        let leaf = engine.status(&format!("leaf-{i}")).await.unwrap().unwrap();
        assert_eq!(leaf.status, "ok");
    }
    signal(&engine, "sink-0", "end", &()).await;
    assert_eq!(result::<u32>(&engine, "sink-0").await, SENDERS);
    assert_eq!(pings.load(Ordering::SeqCst), SENDERS);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archived_histories_move_to_their_ids_shards() {
    let dir = tmp("archives");
    let c = |shards| {
        cfg(&dir, shards)
            .archive_history(true)
            .retention(Duration::from_millis(200))
            .checkpoint_interval(Duration::from_millis(50))
    };
    let ids: Vec<String> = (0..12).map(|i| format!("done-{i:02}")).collect();
    {
        let engine = Engine::open(c(2)).await.unwrap();
        engine.register("w", |ctx: Ctx, n: u32| async move {
            ctx.step("s", move || async move { Ok::<_, Error>(n + 1) })
                .await
        });
        for (i, id) in ids.iter().enumerate() {
            start(&engine, "w", id, &(i as u32)).await;
            result::<u32>(&engine, id).await;
        }
        // Past retention: the runs are gone from lookups, kept in archives.
        eventually("expiry", async || {
            let mut gone = true;
            for id in &ids {
                gone &= engine.status(id).await.unwrap().is_none();
            }
            gone
        })
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        engine.shutdown().await.unwrap();
    }
    for id in &ids {
        assert_eq!(
            fusee::export::export_from_dir(&dir, Some(id), 1).unwrap()[0].workflow_id,
            *id
        );
    }
    let report = done(reshard(&dir, 3, &opts()).unwrap());
    assert!(report.archived >= ids.len() as u64, "{report:?}");
    for id in &ids {
        let j = fusee::export::export_from_dir(&dir, Some(id), 1).unwrap();
        assert_eq!(j.len(), 1, "{id}");
        assert!(j[0].is_complete(), "{id}");
    }
    // Each archived journal sits in its ID's shard.
    for s in 0..3u32 {
        let adir = dir.join(format!("shard-{s:03}")).join("archive");
        let Ok(files) = std::fs::read_dir(&adir) else {
            continue;
        };
        for f in files {
            let p = f.unwrap().path();
            if p.extension().is_none_or(|e| e != "arc") {
                continue;
            }
            let bytes = std::fs::read(&p).unwrap();
            for id in &ids {
                if bytes.windows(id.len()).any(|w| w == id.as_bytes()) {
                    assert_eq!(shard_of(id, 3), s, "{id} archived in shard {s}");
                }
            }
        }
    }
    let engine = Engine::open(c(3)).await.unwrap();
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// What a crash test checks after a failure at one step.
async fn check_crash_state(dir: &Path, from: u32, to: u32, sh: &Arc<Shared>) {
    let meta = std::fs::read_to_string(dir.join("fusee.meta")).unwrap();
    if meta.contains("format=reshard") {
        // During the switch nothing opens it, and the message says what to do.
        for n in [from, to] {
            let err = Engine::open(cfg(dir, n)).await.err().expect("refused");
            assert!(err.to_string().contains("fusee reshard"), "{err}");
        }
        return;
    }
    let count = fusee::engine::read_meta(dir).unwrap().unwrap().0;
    // A copy opens with the count its meta file names and holds every run.
    let copy = tmp("crash-copy");
    copy_dir(dir, &copy);
    {
        let engine = Engine::open(cfg(&copy, count)).await.unwrap();
        register(&engine, sh);
        for id in ids() {
            assert!(
                engine.status(&id).await.unwrap().is_some(),
                "{id} at {count}"
            );
        }
        engine.shutdown().await.unwrap();
    }
    let _ = std::fs::remove_dir_all(&copy);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failure_at_any_step_leaves_a_directory_a_rerun_finishes() {
    let template = tmp("crash-template");
    let (from, to) = (3u32, 5u32);
    let sh = Arc::new(Shared::default());
    {
        let engine = Engine::open(cfg(&template, from)).await.unwrap();
        register(&engine, &sh);
        populate(&engine, &sh, 1).await;
        engine.shutdown().await.unwrap();
    }
    let total = {
        let dir = tmp("crash-count");
        copy_dir(&template, &dir);
        let r = done(reshard(&dir, to, &opts()).unwrap());
        let _ = std::fs::remove_dir_all(&dir);
        r.steps
    };
    assert!(total >= 10, "{total}");
    for k in 0..total {
        let dir = tmp(&format!("crash-{k}"));
        copy_dir(&template, &dir);
        let mut o = opts();
        o.fail_at = Some(k);
        let err = reshard(&dir, to, &o).unwrap_err();
        assert!(err.to_string().contains("injected failure"), "{err}");
        check_crash_state(&dir, from, to, &sh).await;
        // A second failure in the rerun, then a clean one.
        o.fail_at = Some(k / 3);
        if let Err(e) = reshard(&dir, to, &o) {
            assert!(e.to_string().contains("injected failure"), "{e}");
            check_crash_state(&dir, from, to, &sh).await;
        }
        reshard(&dir, to, &opts()).unwrap();
        assert_eq!(
            fusee::engine::read_meta(&dir).unwrap(),
            Some((to, 3)),
            "step {k}"
        );
        for leftover in ["reshard.new", "reshard.old"] {
            assert!(!dir.join(leftover).exists(), "{leftover} after step {k}");
        }
        for s in to..from.max(to) + 3 {
            assert!(
                !dir.join(format!("shard-{s:03}")).exists(),
                "shard {s} after step {k}"
            );
        }
        let engine = Engine::open(cfg(&dir, to)).await.unwrap();
        register(&engine, &sh);
        check_placement(&engine, to).await;
        for i in 0..GATES {
            signal(&engine, &format!("gate-{i}"), "go1", &1u32).await;
        }
        for i in 0..GATES {
            let r: u32 = result::<u32>(&engine, &format!("gate-{i}")).await;
            assert_eq!(r, 1, "step {k}");
        }
        for i in 0..PARENTS {
            for j in 0..KIDS {
                signal(&engine, &format!("child-{i}-{j}"), "done", &1u32).await;
            }
            let r: u32 = result::<u32>(&engine, &format!("parent-{i}")).await;
            assert_eq!(r, (0..KIDS).map(|j| j * 10 + 1).sum::<u32>(), "step {k}");
        }
        for i in 0..QUICK {
            let r: u32 = result::<u32>(&engine, &format!("quick-{i}")).await;
            assert_eq!(r, i * 2);
        }
        assert_eq!(
            engine
                .run::<u32>("counter-0")
                .update::<u32, u32>("add", &5u32)
                .id("u-1")
                .await
                .unwrap(),
            5
        );
        engine.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
    let _ = std::fs::remove_dir_all(&template);
}

/// Two shards number their runs from 1 each, so merging them renumbers
/// runs. References follow: completion tokens through the report, a
/// renumbered step's `Ctx::key`, the newest run of a continue-as-new chain,
/// and children reporting to renumbered parents.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merging_shards_renumbers_runs_and_every_reference_follows() {
    let dir = tmp("merge");
    let sh = Arc::new(Shared::default());
    const N: u32 = 8;
    {
        let engine = Engine::open(cfg(&dir, 2)).await.unwrap();
        register(&engine, &sh);
        for i in 0..N {
            start(&engine, "approval", &format!("approval-{i}"), &()).await;
            start(&engine, "busy", &format!("busy-{i}"), &()).await;
            start(&engine, "chain", &format!("chain-{i}"), &0u32).await;
            start(&engine, "parent", &format!("parent-{i}"), &i).await;
        }
        eventually("tokens, steps and children", async || {
            sh.tokens.lock().unwrap().len() == N as usize
                && sh.keys.lock().unwrap().len() == N as usize
                && sh.effects.lock().unwrap().len() == (N * KIDS) as usize
        })
        .await;
        // Each chain's ID gets two finished runs before its current one.
        for _ in 0..2 {
            for i in 0..N {
                let id = format!("chain-{i}");
                let before = engine.status(&id).await.unwrap().unwrap().run;
                signal(&engine, &id, "next", &0u32).await;
                eventually("the chain's next run", async || {
                    engine
                        .status(&id)
                        .await
                        .unwrap()
                        .is_some_and(|s| s.run != before && s.status != "continued_as_new")
                })
                .await;
            }
        }
        engine.shutdown().await.unwrap();
    }
    let report = done(reshard(&dir, 1, &opts()).unwrap());
    let renumbered: BTreeMap<String, fusee::reshard::Renumbered> = report
        .renumbered_unfinished
        .iter()
        .map(|r| (r.workflow_id.clone(), r.clone()))
        .collect();
    let moved = |prefix: &str| renumbered.keys().filter(|k| k.starts_with(prefix)).count();
    for prefix in ["approval-", "busy-", "chain-", "parent-", "child-"] {
        assert!(
            moved(prefix) > 0,
            "no {prefix} run was renumbered: {report:?}"
        );
    }
    let engine = Engine::open(cfg(&dir, 1)).await.unwrap();
    register(&engine, &sh);
    for i in 0..N {
        let id = format!("approval-{i}");
        let old = sh.tokens.lock().unwrap()[&id].clone();
        let token = match renumbered.get(&id) {
            Some(r) => {
                assert_eq!(r.tokens.len(), 1, "{r:?}");
                assert_eq!(r.tokens[0].0, old);
                // The token handed out before the change names the old run.
                let err =
                    tokio::time::timeout(Duration::from_secs(10), engine.complete(&old, &true))
                        .await
                        .expect("stale completion answered")
                        .unwrap_err();
                assert!(matches!(err, Error::Rejected(_)), "{err}");
                r.tokens[0].1.clone()
            }
            None => old,
        };
        tokio::time::timeout(Duration::from_secs(10), engine.complete(&token, &true))
            .await
            .expect("completion answered")
            .unwrap();
        assert!(result::<bool>(&engine, &id).await, "{id}");
    }
    sh.release.store(true, Ordering::SeqCst);
    for i in 0..N {
        let id = format!("busy-{i}");
        assert_eq!(result::<u32>(&engine, &id).await, 7);
        let keys: std::collections::BTreeSet<String> = sh
            .keys
            .lock()
            .unwrap()
            .iter()
            .filter(|k| k.starts_with(&format!("{id}/")))
            .cloned()
            .collect();
        let expect = 1 + usize::from(renumbered.contains_key(&id));
        assert_eq!(keys.len(), expect, "{id}: {keys:?}");
    }
    for i in 0..N {
        let id = format!("chain-{i}");
        let s = engine.status(&id).await.unwrap().unwrap();
        assert_ne!(s.status, "continued_as_new", "{s:?}");
        while engine.status(&id).await.unwrap().unwrap().status != "ok" {
            let before = engine.status(&id).await.unwrap().unwrap().run;
            signal(&engine, &id, "next", &0u32).await;
            eventually("the chain's next run", async || {
                engine
                    .status(&id)
                    .await
                    .unwrap()
                    .is_some_and(|s| s.run != before)
            })
            .await;
        }
        assert_eq!(result::<u32>(&engine, &id).await, CHAIN_END);
    }
    // Children answer their own parents (each sends its parent's number).
    for i in 0..N {
        for j in 0..KIDS {
            signal(&engine, &format!("child-{i}-{j}"), "done", &i).await;
        }
    }
    for i in 0..N {
        let r: u32 = result::<u32>(&engine, &format!("parent-{i}")).await;
        assert_eq!(r, (0..KIDS).map(|j| j * 10 + i).sum::<u32>(), "parent-{i}");
    }
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn refuses_what_it_does_not_handle() {
    let dir = tmp("refuse");
    std::fs::create_dir_all(&dir).unwrap();
    let err = reshard(&dir, 2, &opts()).unwrap_err();
    assert!(
        err.to_string().contains("not a Fusee data directory"),
        "{err}"
    );
    fusee::engine::write_meta(&dir, 2, 2).unwrap();
    let err = reshard(&dir, 3, &opts()).unwrap_err();
    assert!(err.to_string().contains("fusee upgrade"), "{err}");
    assert!(reshard(&dir, 0, &opts()).is_err());
    std::fs::create_dir_all(dir.join("vsr")).unwrap();
    let err = reshard(&dir, 3, &opts()).unwrap_err();
    assert!(err.to_string().contains("replicated"), "{err}");
    let _ = std::fs::remove_dir_all(&dir);
}
