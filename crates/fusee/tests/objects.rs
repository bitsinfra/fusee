//! Keyed objects end to end on the embedded engine with real files:
//! exclusive order, shared reads, state across restarts, exactly-once
//! calls, calls from workflows and between objects, and compaction.

use fusee::object::{COMPACT_OPS, ObjectCtx};
use fusee::{Config, Ctx, Engine, Error, StepOptions};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

fn tmp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-obj-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cfg(dir: &std::path::Path) -> Config {
    let mut c = Config::local(dir).shards(2);
    c.local.flush = fusee::FlushMethod::Fsync;
    c
}

async fn eventually(what: &str, mut f: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !f().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A cart: `add` (exclusive, with a step) appends an item and returns the
/// count; `get` (shared) returns the items; `fail` changes state, then
/// fails. `ran` counts how often `add`'s step really ran.
fn cart(engine: &Engine, ran: Arc<AtomicU32>) -> fusee::Object {
    engine
        .object_type("cart")
        .exclusive("add", move |ctx: ObjectCtx, item: String| {
            let ran = ran.clone();
            async move {
                let priced: String = ctx
                    .step("price", || {
                        let ran = ran.clone();
                        let item = item.clone();
                        async move {
                            ran.fetch_add(1, Ordering::SeqCst);
                            Ok::<_, Error>(format!("{item}$"))
                        }
                    })
                    .await?;
                let mut items: Vec<String> = ctx.state().get("items")?.unwrap_or_default();
                items.push(priced);
                ctx.state().set("items", &items)?;
                Ok(items.len())
            }
        })
        .shared("get", |ctx: ObjectCtx, _: ()| async move {
            Ok(ctx.state().get::<Vec<String>>("items")?.unwrap_or_default())
        })
        .exclusive("fail", |ctx: ObjectCtx, _: ()| async move {
            ctx.state().set("items", &vec!["lost"])?;
            Err::<(), _>(Error::Failed("out of stock".into()))
        })
        .exclusive("clear", |ctx: ObjectCtx, _: ()| async move {
            ctx.state().clear_all()?;
            Ok(ctx.state().keys()?.len())
        })
        .register()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn calls_change_state_and_cost_two_records() {
    let dir = tmp("basic");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let ran = Arc::new(AtomicU32::new(0));
    let carts = cart(&engine, ran.clone());
    let c = carts.key("u-1");
    assert_eq!(c.call::<_, usize>("add", &"apple").await.unwrap(), 1);
    assert_eq!(c.call::<_, usize>("add", &"pear").await.unwrap(), 2);
    let items: Vec<String> = c.call("get", &()).await.unwrap();
    assert_eq!(items, vec!["apple$", "pear$"]);
    assert_eq!(
        c.state().await.unwrap(),
        json!({"items": ["apple$", "pear$"]})
    );
    // A failed handler commits nothing.
    let e = c.call::<_, ()>("fail", &()).await.unwrap_err();
    assert!(e.to_string().contains("out of stock"), "{e}");
    assert_eq!(
        c.state().await.unwrap(),
        json!({"items": ["apple$", "pear$"]})
    );
    // Another key is another object.
    assert_eq!(
        carts
            .key("u-2")
            .call::<_, usize>("add", &"fig")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        engine.object("cart", "u-9").state().await.unwrap(),
        json!({})
    );
    // The journal: a start, then an update and its result per call (the
    // step adds its own record); the result carries the state change.
    let d = c.describe().await.unwrap();
    let kinds: Vec<&str> = d
        .journal
        .iter()
        .map(|r| match r {
            fusee::core::Record::Start { .. } => "start",
            fusee::core::Record::Update { .. } => "update",
            fusee::core::Record::UpdateDone { .. } => "done",
            fusee::core::Record::Step { .. } => "step",
            _ => "other",
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            "start", "step", "done", "update", "step", "done", "update", "done", "update", "done"
        ],
        "{:?}",
        d.journal
    );
    assert!(d.summary.object);
    let o = d.object.unwrap();
    assert_eq!(o.state, json!({"items": ["apple$", "pear$"]}));
    assert!(o.running.is_empty() && o.waiting.is_empty());
    let mut keys = engine.objects("cart", 100).await.unwrap();
    keys.sort();
    assert_eq!(keys, vec!["u-1", "u-2"]);
    // Clearing every key.
    assert_eq!(c.call::<_, usize>("clear", &()).await.unwrap(), 0);
    assert_eq!(c.state().await.unwrap(), json!({}));
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exclusive_calls_run_one_at_a_time_in_arrival_order() {
    let dir = tmp("order");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let busy = Arc::new(AtomicBool::new(false));
    let overlaps = Arc::new(AtomicU32::new(0));
    let (b, o) = (busy.clone(), overlaps.clone());
    let counter = engine
        .object_type("counter")
        .exclusive("bump", move |ctx: ObjectCtx, who: u32| {
            let (b, o) = (b.clone(), o.clone());
            async move {
                if b.swap(true, Ordering::SeqCst) {
                    o.fetch_add(1, Ordering::SeqCst);
                }
                // A step that takes a while, so calls overlap if they can.
                ctx.step("work", || async {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    Ok::<_, Error>(())
                })
                .await?;
                let mut seen: Vec<u32> = ctx.state().get("seen")?.unwrap_or_default();
                seen.push(who);
                ctx.state().set("seen", &seen)?;
                b.store(false, Ordering::SeqCst);
                Ok(seen.len())
            }
        })
        .register()
        .unwrap();
    let k = counter.key("k");
    let mut tasks = Vec::new();
    for who in 0..40u32 {
        let k = k.clone();
        tasks.push(tokio::spawn(async move {
            (who, k.call::<_, usize>("bump", &who).await.unwrap())
        }));
    }
    let mut places = Vec::new();
    for t in tasks {
        places.push(t.await.unwrap());
    }
    assert_eq!(
        overlaps.load(Ordering::SeqCst),
        0,
        "exclusive calls overlapped"
    );
    let state = k.state().await.unwrap();
    let seen: Vec<u32> = serde_json::from_value(state["seen"].clone()).unwrap();
    assert_eq!(seen.len(), 40);
    // Each caller's place is where its call appears in the state: the
    // calls ran one after another, each seeing every earlier one.
    for (who, place) in places {
        assert_eq!(seen[place - 1], who, "{seen:?}");
    }
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_calls_do_not_wait_for_each_other_or_for_an_exclusive_one() {
    let dir = tmp("shared");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let gate = Arc::new(tokio::sync::Notify::new());
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let (g, br) = (gate.clone(), barrier.clone());
    let doc = engine
        .object_type("doc")
        .exclusive("edit", move |ctx: ObjectCtx, text: String| {
            let g = g.clone();
            async move {
                ctx.state().set("text", &text)?;
                // Holds the key until the test lets it go.
                ctx.step("slow", || {
                    let g = g.clone();
                    async move {
                        g.notified().await;
                        Ok::<_, Error>(())
                    }
                })
                .await?;
                Ok(())
            }
        })
        .shared("read", |ctx: ObjectCtx, _: ()| async move {
            ctx.state().get::<String>("text")
        })
        .shared("meet", move |ctx: ObjectCtx, _: ()| {
            let br = br.clone();
            async move {
                // Two shared calls must be inside at once to pass.
                ctx.step("meet", || {
                    let br = br.clone();
                    async move {
                        br.wait().await;
                        Ok::<_, Error>(())
                    }
                })
                .await?;
                Ok(true)
            }
        })
        .register()
        .unwrap();
    let d = doc.key("d");
    assert_eq!(
        d.call::<_, Option<String>>("read", &()).await.unwrap(),
        None
    );
    let edit = {
        let d = d.clone();
        tokio::spawn(async move { d.call::<_, ()>("edit", &"draft").await })
    };
    eventually("the edit to start", async || {
        d.describe()
            .await
            .ok()
            .and_then(|x| x.object)
            .is_some_and(|o| !o.running.is_empty())
    })
    .await;
    // The edit holds the key: a shared read answers at once, with the state
    // as last committed (not the edit's uncommitted write).
    let read = tokio::time::timeout(
        Duration::from_secs(5),
        d.call::<_, Option<String>>("read", &()),
    )
    .await
    .expect("a shared read waited for the exclusive call")
    .unwrap();
    assert_eq!(read, None);
    // Two shared calls run at once.
    let (a, b) = tokio::join!(
        d.call::<_, bool>("meet", &()),
        d.call::<_, bool>("meet", &())
    );
    assert!(a.unwrap() && b.unwrap());
    // An exclusive call waits behind the edit.
    let second = {
        let d = d.clone();
        tokio::spawn(async move { d.call::<_, ()>("edit", &"final").await })
    };
    eventually("the second edit to wait", async || {
        d.describe()
            .await
            .ok()
            .and_then(|x| x.object)
            .is_some_and(|o| o.waiting.len() == 1)
    })
    .await;
    gate.notify_one();
    edit.await.unwrap().unwrap();
    assert_eq!(
        d.call::<_, Option<String>>("read", &())
            .await
            .unwrap()
            .as_deref(),
        Some("draft")
    );
    gate.notify_one();
    second.await.unwrap().unwrap();
    assert_eq!(
        d.call::<_, Option<String>>("read", &())
            .await
            .unwrap()
            .as_deref(),
        Some("final")
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_and_calls_survive_restarts_and_retries_run_once() {
    let dir = tmp("restart");
    let ran = Arc::new(AtomicU32::new(0));
    {
        let mut c = cfg(&dir);
        c.checkpoint_on_shutdown = false;
        let engine = Engine::open(c).await.unwrap();
        let carts = cart(&engine, ran.clone());
        let k = carts.key("r");
        assert_eq!(
            k.call::<_, usize>("add", &"a").id("req-1").await.unwrap(),
            1
        );
        assert_eq!(
            k.call::<_, usize>("add", &"b").id("req-2").await.unwrap(),
            2
        );
        // The same ID again: the recorded result, and the handler does not run.
        assert_eq!(
            k.call::<_, usize>("add", &"b").id("req-2").await.unwrap(),
            2
        );
        assert_eq!(ran.load(Ordering::SeqCst), 2);
        engine.shutdown().await.unwrap();
    }
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let carts = cart(&engine, ran.clone());
    let k = carts.key("r");
    assert_eq!(k.state().await.unwrap(), json!({"items": ["a$", "b$"]}));
    assert_eq!(
        k.call::<_, usize>("add", &"b").id("req-2").await.unwrap(),
        2
    );
    assert_eq!(
        k.call::<_, usize>("add", &"c").id("req-3").await.unwrap(),
        3
    );
    assert_eq!(ran.load(Ordering::SeqCst), 3);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_interrupted_by_a_restart_finishes_once() {
    let dir = tmp("interrupted");
    let ran = Arc::new(AtomicU32::new(0));
    let hold = Arc::new(AtomicBool::new(true));
    let define = |engine: &Engine, ran: Arc<AtomicU32>, hold: Arc<AtomicBool>| {
        engine
            .object_type("job")
            .exclusive("run", move |ctx: ObjectCtx, n: u32| {
                let (ran, hold) = (ran.clone(), hold.clone());
                async move {
                    let out: u32 = ctx
                        .step("work", || {
                            let (ran, hold) = (ran.clone(), hold.clone());
                            async move {
                                ran.fetch_add(1, Ordering::SeqCst);
                                while hold.load(Ordering::SeqCst) {
                                    tokio::time::sleep(Duration::from_millis(5)).await;
                                }
                                Ok::<_, Error>(n * 10)
                            }
                        })
                        .await?;
                    ctx.state().set("last", &out)?;
                    Ok(out)
                }
            })
            .register()
            .unwrap()
    };
    {
        let mut c = cfg(&dir);
        c.checkpoint_on_shutdown = false;
        let engine = Engine::open(c).await.unwrap();
        let jobs = define(&engine, ran.clone(), hold.clone());
        let k = jobs.key("j");
        let call = {
            let k = k.clone();
            tokio::spawn(async move { k.call::<_, u32>("run", &4).id("once").await })
        };
        eventually("the step to start", async || {
            ran.load(Ordering::SeqCst) == 1
        })
        .await;
        // The process goes away while the handler runs: the caller sees an
        // error, the accepted call stays in the key's journal.
        call.abort();
        let _ = tokio::time::timeout(Duration::from_secs(2), engine.shutdown()).await;
    }
    hold.store(false, Ordering::SeqCst);
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let jobs = define(&engine, ran.clone(), hold.clone());
    let k = jobs.key("j");
    // The retry with the same ID gets the result of the call that resumed.
    assert_eq!(k.call::<_, u32>("run", &4).id("once").await.unwrap(), 40);
    assert_eq!(k.state().await.unwrap(), json!({"last": 40}));
    assert!(
        ran.load(Ordering::SeqCst) <= 2,
        "the step runs at most once more"
    );
    assert_eq!(k.call::<_, u32>("run", &4).id("once").await.unwrap(), 40);
    let runs = ran.load(Ordering::SeqCst);
    assert_eq!(k.call::<_, u32>("run", &4).id("once").await.unwrap(), 40);
    assert_eq!(ran.load(Ordering::SeqCst), runs);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflows_and_objects_call_objects_exactly_once() {
    let dir = tmp("calls");
    let mut c = cfg(&dir);
    c.keep_history = true;
    let engine = Engine::open(c).await.unwrap();
    let ran = Arc::new(AtomicU32::new(0));
    let _carts = cart(&engine, ran.clone());
    // An object whose handler calls another object.
    engine
        .object_type("order")
        .exclusive("place", |ctx: ObjectCtx, items: Vec<String>| async move {
            let key = ctx.object_key().to_owned();
            let mut n = 0;
            for i in &items {
                n = ctx.object("cart", &key).call::<_, usize>("add", i).await?;
            }
            ctx.state().set("placed", &n)?;
            Ok(n)
        })
        .register()
        .unwrap();
    let wf = engine.register("shopper", |ctx: Ctx, key: String| async move {
        let a: usize = ctx.object("cart", &key).call("add", &"tea").await?;
        ctx.object("cart", &key).send("add", &"milk")?;
        let placed: usize = ctx
            .object("order", &key)
            .call("place", &vec!["jam".to_owned(), "bread".to_owned()])
            .await?;
        let items: Vec<String> = ctx.object("cart", &key).call("get", &()).await?;
        Ok((a, placed, items))
    });
    let run = engine.start(&wf, "shop-1", &"s1".to_owned()).await.unwrap();
    let (a, placed, items) = run.result().await.unwrap();
    assert_eq!(a, 1);
    // The send is ordered before the order's calls only by arrival; every
    // item is there once.
    assert!(placed >= 3, "{placed}");
    let mut sorted = items.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["bread$", "jam$", "milk$", "tea$"], "{items:?}");
    assert_eq!(ran.load(Ordering::SeqCst), 4);
    assert_eq!(
        engine.object("order", "s1").state().await.unwrap()["placed"],
        json!(placed)
    );
    // The caller's journal holds each call as one operation.
    let d = run.describe().await.unwrap();
    let norm = fusee::journal::normalize(&d.journal);
    let calls = norm
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["kind"] == "call")
        .count();
    assert_eq!(calls, 4, "{norm:#}");
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_keeps_state_and_ids_and_bounds_history() {
    let dir = tmp("compact");
    let mut c = cfg(&dir);
    c.checkpoint_on_shutdown = false;
    let engine = Engine::open(c).await.unwrap();
    let ran = Arc::new(AtomicU32::new(0));
    let r = ran.clone();
    let tally = engine
        .object_type("tally")
        .exclusive("add", move |ctx: ObjectCtx, n: u64| {
            let r = r.clone();
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                let sum: u64 = ctx.state().get("sum")?.unwrap_or(0);
                ctx.state().set("sum", &(sum + n))?;
                Ok(sum + n)
            }
        })
        .register()
        .unwrap();
    let k = tally.key("t");
    let total = 3 * COMPACT_OPS as u64;
    for n in 1..=total {
        let got: u64 = k.call("add", &n).id(&format!("id-{n}")).await.unwrap();
        assert_eq!(got, n * (n + 1) / 2);
    }
    let m = engine.metrics().await.unwrap();
    assert!(m.object_compactions >= 2, "{m:?}");
    let d = k.describe().await.unwrap();
    assert!(
        d.journal.len() <= 2 * COMPACT_OPS as usize + 4,
        "history is bounded: {} records",
        d.journal.len()
    );
    assert_eq!(
        d.object.unwrap().state,
        json!({"sum": total * (total + 1) / 2})
    );
    // An ID from before the compactions is still recognized.
    let before = ran.load(Ordering::SeqCst);
    assert_eq!(k.call::<_, u64>("add", &5).id("id-5").await.unwrap(), 15);
    assert_eq!(ran.load(Ordering::SeqCst), before);
    engine.shutdown().await.unwrap();
    // Reopened, the key replays at most one compaction's worth.
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let r = ran.clone();
    let tally = engine
        .object_type("tally")
        .exclusive("add", move |ctx: ObjectCtx, n: u64| {
            let r = r.clone();
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                let sum: u64 = ctx.state().get("sum")?.unwrap_or(0);
                ctx.state().set("sum", &(sum + n))?;
                Ok(sum + n)
            }
        })
        .register()
        .unwrap();
    let k = tally.key("t");
    assert_eq!(
        k.state().await.unwrap(),
        json!({"sum": total * (total + 1) / 2})
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_key_is_compacted_to_one_record_and_parked() {
    let dir = tmp("idle");
    let mut c = cfg(&dir).park_after(Duration::from_millis(50));
    c.checkpoint_on_shutdown = false;
    let engine = Engine::open(c).await.unwrap();
    let ran = Arc::new(AtomicU32::new(0));
    let carts = cart(&engine, ran.clone());
    let k = carts.key("idle");
    for i in 0..5 {
        k.call::<_, usize>("add", &format!("i{i}")).await.unwrap();
    }
    eventually("the key to park", async || {
        k.describe()
            .await
            .ok()
            .is_some_and(|d| d.summary.status == "parked" && d.journal.len() == 1)
    })
    .await;
    let d = k.describe().await.unwrap();
    assert_eq!(
        d.object.unwrap().state["items"].as_array().unwrap().len(),
        5
    );
    // Its state, read from the parked key.
    let state = tokio::time::timeout(Duration::from_secs(10), k.state())
        .await
        .expect("reading a parked key's state hung")
        .unwrap();
    assert_eq!(state["items"].as_array().unwrap().len(), 5);
    // Loading it back runs no handler code again.
    let before = ran.load(Ordering::SeqCst);
    assert_eq!(k.call::<_, usize>("add", &"again").await.unwrap(), 6);
    assert_eq!(ran.load(Ordering::SeqCst), before + 1);
    let _ = StepOptions::default();
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn objects_refuse_what_they_are_not() {
    let dir = tmp("refuse");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let ran = Arc::new(AtomicU32::new(0));
    let carts = cart(&engine, ran);
    let wf = engine.register("plain", |ctx: Ctx, _: ()| async move {
        let _: Option<u32> = ctx.wait_for("never").await?;
        Ok(())
    });
    engine.start(&wf, "cart/taken", &()).await.unwrap();
    let e = carts
        .key("taken")
        .call::<_, usize>("add", &"x")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("not an object"), "{e}");
    // A shared handler cannot write.
    let o = engine
        .object_type("ro")
        .shared("write", |ctx: ObjectCtx, _: ()| async move {
            ctx.state().set("k", &1)?;
            Ok(())
        })
        .register()
        .unwrap();
    let e = o.key("a").call::<_, ()>("write", &()).await.unwrap_err();
    assert!(e.to_string().contains("shared handler"), "{e}");
    assert!(engine.object_type("a/b").register().is_err());
    let _: Value = json!(null);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
