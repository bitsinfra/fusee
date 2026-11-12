//! The workflow features (docs/features.md), end to end on the embedded
//! engine with real files.

use fusee::{
    Batch, ChildOptions, Config, Ctx, Debounce, Engine, Error, IdPolicy, LimitSpec, OnFinished,
    OnRunning, Overlap, ParentClose, RetryPolicy, ScheduleSpec, StartOptions, StepOptions,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

fn tmp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-feat-{name}-{}-{}",
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
    c.keep_history = true;
    c
}

async fn eventually(what: &str, mut f: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_reruns_from_the_named_step() {
    let dir = tmp("reset");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    let wf = engine.register("pay", move |ctx: Ctx, n: u32| {
        let c = c.clone();
        async move {
            let quote: u32 = ctx
                .step("quote", || async move { Ok::<_, Error>(n) })
                .await?;
            let c2 = c.clone();
            let key: String = ctx
                .step("charge", || {
                    let key = ctx.key();
                    let c2 = c2.clone();
                    async move {
                        c2.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, Error>(key)
                    }
                })
                .await?;
            let go: Option<u32> = ctx.wait_for("go").await?;
            Ok(format!("{quote}/{key}/{}", go.unwrap_or(0)))
        }
    });
    let run = engine.start(&wf, "pay-1", &7).await.unwrap();
    eventually("the wait", async || {
        matches!(
            run.status().await.ok().map(|s| s.status).as_deref(),
            Some("suspended" | "parked")
        )
    })
    .await;
    run.signal("go", &1).await.unwrap();
    let first = run.result().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    run.reset("charge").await.unwrap();
    // The new run re-runs charge with a new idempotency key, and gets the
    // signal again (reapplied).
    let second = run.result().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_ne!(first, second);
    assert!(
        second.starts_with("7/") && second.ends_with("/1"),
        "{second}"
    );
    let s = run.status().await.unwrap();
    assert_eq!(s.status, "ok");
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_of_a_running_run_hands_over() {
    let dir = tmp("reset-running");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("w", |ctx: Ctx, n: u32| async move {
        let a: u32 = ctx.step("a", || async move { Ok::<_, Error>(n) }).await?;
        let b: u32 = ctx
            .step("b", || async move { Ok::<_, Error>(a + 1) })
            .await?;
        let go: Option<u32> = ctx.wait_for("go").await?;
        Ok(a + b + go.unwrap_or(0))
    });
    let run = engine.start(&wf, "r-1", &1).await.unwrap();
    eventually("the wait", async || {
        matches!(
            run.status().await.ok().map(|s| s.status).as_deref(),
            Some("suspended" | "parked")
        )
    })
    .await;
    run.reset("b").await.unwrap();
    let s = run.status().await.unwrap();
    assert_ne!(s.status, "reset", "{s:?}");
    run.signal("go", &10).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 13);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queries_and_updates() {
    let dir = tmp("updates");
    let calls = Arc::new(AtomicU32::new(0));
    let register = |engine: &Engine, calls: Arc<AtomicU32>| {
        engine.register("cart", move |ctx: Ctx, _: ()| {
            let calls = calls.clone();
            async move {
                let items = std::rc::Rc::new(std::cell::RefCell::new(Vec::<String>::new()));
                let i2 = items.clone();
                ctx.on_query("items", move |_: ()| i2.borrow().clone());
                let (i3, c2) = (items.clone(), ctx.clone());
                ctx.on_update(
                    "add",
                    |item: &String| {
                        if item.is_empty() {
                            Err("empty item".into())
                        } else {
                            Ok(())
                        }
                    },
                    move |item: String| {
                        let (items, ctx, calls) = (i3.clone(), c2.clone(), calls.clone());
                        async move {
                            let price: u32 = ctx
                                .step("price", || {
                                    let calls = calls.clone();
                                    let n = item.len() as u32;
                                    async move {
                                        calls.fetch_add(1, Ordering::SeqCst);
                                        Ok::<_, Error>(n * 10)
                                    }
                                })
                                .await?;
                            items.borrow_mut().push(item);
                            Ok(price)
                        }
                    },
                );
                let _: Option<()> = ctx.wait_for("checkout").await?;
                Ok(items.borrow().clone())
            }
        })
    };
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let wf = register(&engine, calls.clone());
        let run = engine.start(&wf, "cart-1", &()).await.unwrap();
        let price: u32 = run.update("add", &"apple").id("u-1").await.unwrap();
        assert_eq!(price, 50);
        // A retried call with the same ID gets the recorded result.
        let again: u32 = run.update("add", &"apple").id("u-1").await.unwrap();
        assert_eq!(again, 50);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // The validator rejects; nothing is recorded.
        let err = run.update::<_, u32>("add", &"").await.unwrap_err();
        assert!(
            matches!(err, Error::Rejected(ref m) if m.contains("empty")),
            "{err}"
        );
        let items: Vec<String> = run.query("items", &()).await.unwrap();
        assert_eq!(items, vec!["apple"]);
        let err = run.query::<_, ()>("nope", &()).await.unwrap_err();
        assert!(matches!(err, Error::Rejected(_)), "{err}");
        let j = run.describe().await.unwrap().journal;
        let updates = j
            .iter()
            .filter(|r| matches!(r, fusee::core::Record::Update { .. }))
            .count();
        assert_eq!(updates, 1);
        engine.shutdown().await.unwrap();
    }
    {
        // After a restart the update's dedup and the run's state survive.
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let _wf = register(&engine, calls.clone());
        let run = engine.run::<Vec<String>>("cart-1");
        let again: u32 = run.update("add", &"apple").id("u-1").await.unwrap();
        assert_eq!(again, 50);
        let p: u32 = run.update("add", &"pear").await.unwrap();
        assert_eq!(p, 40);
        let items: Vec<String> = run.query("items", &()).await.unwrap();
        assert_eq!(items, vec!["apple", "pear"]);
        run.signal("checkout", &()).await.unwrap();
        assert_eq!(run.result().await.unwrap(), vec!["apple", "pear"]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        engine.shutdown().await.unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_with_start_is_one_record() {
    let dir = tmp("uws");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("counter", |ctx: Ctx, start: u32| async move {
        let n = std::rc::Rc::new(std::cell::Cell::new(start));
        let n2 = n.clone();
        ctx.on_update(
            "inc",
            |_: &u32| Ok(()),
            move |by: u32| {
                let n = n2.clone();
                async move {
                    n.set(n.get() + by);
                    Ok(n.get())
                }
            },
        );
        let _: Option<()> = ctx.wait_for("stop").await?;
        Ok(n.get())
    });
    let (run, v): (_, u32) = engine
        .update_with_start(&wf, "c-1", &10, "inc", &5)
        .await
        .unwrap();
    assert_eq!(v, 15);
    let j = run.describe().await.unwrap().journal;
    assert!(
        matches!(&j[0], fusee::core::Record::Start { update: Some(u), .. } if &*u.name == "inc"),
        "{j:?}"
    );
    // Running already: the update goes to the running workflow.
    let (_, v): (_, u32) = engine
        .update_with_start(&wf, "c-1", &99, "inc", &1)
        .await
        .unwrap();
    assert_eq!(v, 16);
    run.signal("stop", &()).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 16);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeats_resume_after_restart() {
    let dir = tmp("heartbeat");
    let started_from = Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));
    let register = |engine: &Engine, from: Arc<std::sync::Mutex<Vec<u32>>>| {
        engine.register("export", move |ctx: Ctx, _: ()| {
            let from = from.clone();
            async move {
                let c = ctx.clone();
                let opts = StepOptions::default()
                    .retry(RetryPolicy::exponential(
                        3,
                        Duration::from_millis(1),
                        Duration::from_millis(5),
                    ))
                    .heartbeat_timeout(Duration::from_millis(200));
                let pages: u32 = ctx
                    .step("export", move || {
                        let (c, from) = (c.clone(), from.clone());
                        async move {
                            let start = c.progress::<u32>()?.unwrap_or(0);
                            from.lock().unwrap().push(start);
                            for p in start..6 {
                                c.heartbeat(&(p + 1))?;
                                tokio::time::sleep(Duration::from_millis(15)).await;
                            }
                            Ok::<_, Error>(6u32)
                        }
                    })
                    .options(opts)
                    .await?;
                Ok(pages)
            }
        })
    };
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let wf = register(&engine, started_from.clone());
        engine.start(&wf, "exp-1", &()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The process stops in the middle of the step.
        engine.shutdown().await.unwrap();
    }
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let _wf = register(&engine, started_from.clone());
        let r: u32 = engine.run("exp-1").result().await.unwrap();
        assert_eq!(r, 6);
        let from = started_from.lock().unwrap().clone();
        assert_eq!(from[0], 0);
        assert!(
            from[1] > 0,
            "the retry resumed from durable progress: {from:?}"
        );
        engine.shutdown().await.unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_timeout_retries_a_stuck_step() {
    let dir = tmp("hb-timeout");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    let wf = engine.register("w", move |ctx: Ctx, _: ()| {
        let a = a.clone();
        async move {
            let c = ctx.clone();
            let opts = StepOptions::default()
                .retry(RetryPolicy::exponential(
                    3,
                    Duration::from_millis(1),
                    Duration::from_millis(5),
                ))
                .heartbeat_timeout(Duration::from_millis(100));
            ctx.step("slow", move || {
                let (c, a) = (c.clone(), a.clone());
                async move {
                    let n = a.fetch_add(1, Ordering::SeqCst);
                    let start = c.progress::<u32>()?.unwrap_or(0);
                    c.heartbeat(&(start + 1))?;
                    if n == 0 {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                    Ok::<_, Error>(start)
                }
            })
            .options(opts)
            .await
        }
    });
    let r: u32 = engine
        .start(&wf, "w-1", &())
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(r, 1, "the second attempt saw the first one's progress");
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_completion_with_stale_tokens() {
    let dir = tmp("complete");
    let tokens = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let register = |engine: &Engine, tokens: Arc<std::sync::Mutex<Vec<String>>>| {
        engine.register("approve", move |ctx: Ctx, _: ()| {
            let tokens = tokens.clone();
            async move {
                let opts = StepOptions::default().retry(RetryPolicy::exponential(
                    3,
                    Duration::from_millis(1),
                    Duration::from_millis(2),
                ));
                let ok: bool = ctx
                    .await_completion("approval", move |c: fusee::Completion| {
                        let token = c.token;
                        let tokens = tokens.clone();
                        async move {
                            let mut t = tokens.lock().unwrap();
                            t.push(token);
                            // The first attempt to send fails.
                            if t.len() == 1 {
                                Err("mail server down".to_string())
                            } else {
                                Ok(())
                            }
                        }
                    })
                    .options(opts)
                    .await?;
                Ok(ok)
            }
        })
    };
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let wf = register(&engine, tokens.clone());
        engine.start(&wf, "a-1", &()).await.unwrap();
        eventually("two tokens", async || tokens.lock().unwrap().len() == 2).await;
        engine.shutdown().await.unwrap();
    }
    {
        // Another process (here: after a restart, with the run parked)
        // completes the step with the token.
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let _wf = register(&engine, tokens.clone());
        let t = tokens.lock().unwrap().clone();
        let err = tokio::time::timeout(Duration::from_secs(5), engine.complete(&t[0], &true))
            .await
            .expect("stale completion answered")
            .unwrap_err();
        assert!(matches!(err, Error::Rejected(_)), "{err}");
        tokio::time::timeout(Duration::from_secs(5), engine.complete(&t[1], &true))
            .await
            .expect("completion answered")
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), engine.run::<bool>("a-1").result())
                .await
                .expect("result")
                .unwrap()
        );
        let err = engine.complete(&t[1], &false).await.unwrap_err();
        assert!(matches!(err, Error::Rejected(_)), "{err}");
        assert_eq!(
            tokens.lock().unwrap().len(),
            2,
            "the token was not sent again"
        );
        engine.shutdown().await.unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continue_as_new_keeps_the_id() {
    let dir = tmp("can");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("loop", |ctx: Ctx, (left, acc): (u32, u32)| async move {
        let v: u32 = ctx
            .step("work", || async move { Ok::<_, Error>(left) })
            .await?;
        assert!(ctx.journal_size() <= 2);
        if left == 0 {
            return Ok(acc + v);
        }
        ctx.continue_as_new(&(left - 1, acc + v)).await
    });
    let run = engine.start(&wf, "loop-1", &(5, 0)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 15);
    let s = run.status().await.unwrap();
    assert_eq!(s.status, "ok");
    let detail = run.describe().await.unwrap();
    assert!(matches!(
        detail.journal[0],
        fusee::core::Record::Start { from: Some(_), .. }
    ));
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patching_a_sleeping_workflow() {
    let dir = tmp("patch");
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let wf = engine.register("refund", |ctx: Ctx, _: ()| async move {
            let a: u32 = ctx.step("a", || async { Ok::<_, Error>(1) }).await?;
            let _: Option<()> = ctx.wait_for("go").await?;
            Ok(a)
        });
        engine.start(&wf, "old", &()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        engine.shutdown().await.unwrap();
    }
    {
        // New code: the fix applies to new runs and to the old run when it
        // reaches the patch point; replaying its old journal is unaffected.
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let wf = engine.register("refund", |ctx: Ctx, _: ()| async move {
            let fixed_early = ctx.patched("early")?;
            let a: u32 = ctx.step("a", || async { Ok::<_, Error>(1) }).await?;
            let _: Option<()> = ctx.wait_for("go").await?;
            let fixed = ctx.patched("rounding")?;
            Ok(a + if fixed { 10 } else { 0 } + if fixed_early { 100 } else { 0 })
        });
        let old = engine.run::<u32>("old");
        old.signal("go", &()).await.unwrap();
        assert_eq!(old.result().await.unwrap(), 11);
        let new = engine.start(&wf, "new", &()).await.unwrap();
        new.signal("go", &()).await.unwrap();
        assert_eq!(new.result().await.unwrap(), 111);
        engine.shutdown().await.unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminate_applies_the_parent_close_policy() {
    let dir = tmp("close");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let _kid = engine.register("kid", |ctx: Ctx, _: ()| async move {
        let _: Option<()> = ctx.wait_for("never").await?;
        Ok(())
    });
    let wf = engine.register("parent", |ctx: Ctx, _: ()| async move {
        ctx.start_child("kid", "kid-terminate", &()).await?;
        ctx.start_child("kid", "kid-cancel", &())
            .options(ChildOptions {
                close: ParentClose::RequestCancel,
                timeout: None,
            })
            .await?;
        ctx.start_child("kid", "kid-abandon", &())
            .options(ChildOptions {
                close: ParentClose::Abandon,
                timeout: None,
            })
            .await?;
        let _: Option<()> = ctx.wait_for("never").await?;
        Ok(())
    });
    let run = engine.start(&wf, "parent-1", &()).await.unwrap();
    eventually("children", async || {
        engine.status("kid-abandon").await.unwrap().is_some()
            && engine.status("kid-cancel").await.unwrap().is_some()
            && engine.status("kid-terminate").await.unwrap().is_some()
    })
    .await;
    run.terminate("stop").await.unwrap();
    assert!(matches!(run.result().await, Err(Error::Terminated)));
    let st = |id: &'static str| {
        let e = engine.clone();
        async move { e.status(id).await.unwrap().unwrap().status }
    };
    eventually("children closed", async || {
        st("kid-terminate").await == "terminated" && st("kid-cancel").await == "cancelled"
    })
    .await;
    assert!(matches!(
        st("kid-abandon").await.as_str(),
        "suspended" | "parked" | "running"
    ));
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paged_backfill_remembers_instants_after_child_retention() {
    let engine = Engine::open(Config::memory().retention(Duration::from_millis(25)))
        .await
        .unwrap();
    let issued = Arc::new(AtomicU32::new(0));
    let count = issued.clone();
    engine.register("tick", move |ctx: Ctx, _: serde_json::Value| {
        let count = count.clone();
        async move {
            ctx.step("effect", || async {
                Ok::<_, Error>(count.fetch_add(1, Ordering::SeqCst))
            })
            .await
        }
    });
    let spec = ScheduleSpec {
        every_ms: Some(1),
        workflow: "tick".into(),
        paused: true,
        ..Default::default()
    };
    let schedule = engine.schedule("range", spec).await.unwrap();
    schedule.backfill(1, 1001).await.unwrap();
    eventually("entire requested range", async || {
        issued.load(Ordering::SeqCst) == 1001
    })
    .await;
    eventually("expired first child", async || {
        engine
            .status(&fusee::schedule::run_id("range", 1))
            .await
            .unwrap()
            .is_none()
    })
    .await;
    schedule.backfill(1, 1002).await.unwrap();
    eventually("only the new instant", async || {
        issued.load(Ordering::SeqCst) >= 1002
    })
    .await;
    assert_eq!(schedule.describe().await.unwrap()["started"], 1002);
    assert_eq!(issued.load(Ordering::SeqCst), 1002);
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schedules_start_one_run_per_time() {
    let dir = tmp("schedule");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let runs = Arc::new(AtomicU32::new(0));
    let r = runs.clone();
    let _report = engine.register("report", move |ctx: Ctx, _: serde_json::Value| {
        let r = r.clone();
        async move {
            r.fetch_add(1, Ordering::SeqCst);
            Ok(ctx.workflow_id().to_owned())
        }
    });
    let spec = ScheduleSpec {
        every_ms: Some(100),
        overlap: Overlap::Skip,
        workflow: "report".into(),
        ..Default::default()
    };
    let s = engine.schedule("nightly", spec.clone()).await.unwrap();
    eventually("three runs", async || runs.load(Ordering::SeqCst) >= 3).await;
    let d = s.describe().await.unwrap();
    assert!(d["started"].as_u64().unwrap() >= 3, "{d}");
    s.pause().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let n = runs.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(runs.load(Ordering::SeqCst), n, "paused");
    // A backfill over times that already ran starts nothing twice.
    let now = fusee::local::unix_ms();
    s.backfill(now - 2_000, now).await.unwrap();
    eventually("backfill", async || runs.load(Ordering::SeqCst) > n).await;
    let ids: Vec<String> = engine
        .list_where(&serde_json::json!({"type": "report"}), 1000)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.id)
        .collect();
    let unique: std::collections::BTreeSet<&String> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "{ids:?}");
    assert!(ids.iter().all(|i| i.starts_with("nightly@")), "{ids:?}");
    // Calling schedule again (an application restart) keeps one schedule.
    engine.schedule("nightly", spec).await.unwrap();
    s.delete().await.unwrap();
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_attributes_and_listing() {
    let dir = tmp("attrs");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("order", |ctx: Ctx, amount: u32| async move {
        ctx.set_attributes(&serde_json::json!({"customer": "c-1", "amount": amount}))?;
        if amount > 100 {
            return Err(Error::Failed("too big".into()));
        }
        ctx.set_attributes(&serde_json::json!({"stage": "paid"}))?;
        Ok(amount)
    });
    for (i, a) in [50u32, 150, 80].iter().enumerate() {
        let run = engine.start(&wf, &format!("o-{i}"), a).await.unwrap();
        let _ = run.result().await;
    }
    let q = |f: serde_json::Value| {
        let e = engine.clone();
        async move {
            let mut ids: Vec<String> = e
                .list_where(&f, 100)
                .await
                .unwrap()
                .into_iter()
                .map(|s| s.id)
                .collect();
            ids.sort();
            ids
        }
    };
    assert_eq!(
        q(serde_json::json!({"customer": "c-1"})).await,
        vec!["o-0", "o-1", "o-2"]
    );
    assert_eq!(
        q(serde_json::json!({"status": "failed"})).await,
        vec!["o-1"]
    );
    assert_eq!(
        q(serde_json::json!({"amount": {"gte": 60, "lt": 200}, "stage": "paid"})).await,
        vec!["o-2"]
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limits_bound_concurrent_steps() {
    let dir = tmp("limits");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    engine
        .limit(
            "stripe",
            LimitSpec {
                concurrent: 2,
                per_second: 0.0,
            },
        )
        .unwrap();
    let now = Arc::new(AtomicU32::new(0));
    let max = Arc::new(AtomicU32::new(0));
    let (n2, m2) = (now.clone(), max.clone());
    let wf = engine.register("charge", move |ctx: Ctx, _: ()| {
        let (now, max) = (n2.clone(), m2.clone());
        async move {
            ctx.step("charge", || {
                let (now, max) = (now.clone(), max.clone());
                async move {
                    let n = now.fetch_add(1, Ordering::SeqCst) + 1;
                    max.fetch_max(n, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    now.fetch_sub(1, Ordering::SeqCst);
                    Ok::<_, Error>(())
                }
            })
            .options(StepOptions::default().limit("stripe"))
            .await
        }
    });
    let mut runs = Vec::new();
    for i in 0..10 {
        runs.push(engine.start(&wf, &format!("c-{i}"), &()).await.unwrap());
    }
    for r in runs {
        r.result().await.unwrap();
    }
    assert_eq!(max.load(Ordering::SeqCst), 2);
    assert_eq!(engine.limit_in_use("stripe"), 0);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiting_steps_take_permits_by_priority_then_fairness_key() {
    let dir = tmp("limit-order");
    let mut c = Config::local(&dir).shards(1);
    c.local.flush = fusee::FlushMethod::Fsync;
    let engine = Engine::open(c).await.unwrap();
    engine
        .limit(
            "one",
            LimitSpec {
                concurrent: 1,
                per_second: 0.0,
            },
        )
        .unwrap();
    let open = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let served = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let o2 = open.clone();
    let hold = engine.register("hold", move |ctx: Ctx, _: ()| {
        let open = o2.clone();
        async move {
            ctx.step("hold", || {
                let open = open.clone();
                async move {
                    while !open.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    Ok::<_, Error>(())
                }
            })
            .options(StepOptions::default().limit("one"))
            .await
        }
    });
    let s2 = served.clone();
    let job = engine.register("job", move |ctx: Ctx, (level, key): (i32, String)| {
        let served = s2.clone();
        async move {
            let opts = StepOptions::default()
                .limit("one")
                .priority(level)
                .fairness(&key);
            ctx.step("work", || {
                let (served, key) = (served.clone(), key.clone());
                async move {
                    served.lock().unwrap().push(format!("{key}@{level}"));
                    Ok::<_, Error>(())
                }
            })
            .options(opts)
            .await
        }
    });
    let blocker = engine.start(&hold, "hold", &()).await.unwrap();
    eventually("the blocker's permit", async || {
        engine.limit_in_use("one") == 1
    })
    .await;
    // Tenant "big" queues six steps, then "a" and "b" two each, then one
    // urgent step: each start waits until its step is queued, so the
    // arrival order is fixed.
    let mut asks: Vec<(i32, &str)> = vec![(0, "big"); 6];
    asks.extend([(0, "a"), (0, "b"), (0, "a"), (0, "b"), (5, "urgent")]);
    let mut runs = Vec::new();
    for (i, (level, key)) in asks.iter().enumerate() {
        runs.push(
            engine
                .start(&job, &format!("job-{i}"), &(*level, key.to_string()))
                .await
                .unwrap(),
        );
        eventually("the step to queue", async || {
            engine.limit_waiting("one") == i + 1
        })
        .await;
    }
    open.store(true, Ordering::SeqCst);
    blocker.result().await.unwrap();
    for r in runs {
        r.result().await.unwrap();
    }
    assert_eq!(
        *served.lock().unwrap(),
        [
            "urgent@5", "big@0", "a@0", "b@0", "big@0", "a@0", "b@0", "big@0", "big@0", "big@0",
            "big@0"
        ]
    );
    assert_eq!(engine.limit_in_use("one"), 0);
    assert_eq!(engine.limit_waiting("one"), 0);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypted_payloads_never_reach_the_disk() {
    let dir = tmp("codec");
    let key = [3u8; 32];
    let mk = || cfg(&dir).codec(fusee::aes_gcm([("k1", key)]));
    {
        let engine = Engine::open(mk()).await.unwrap();
        let wf = engine.register("secret", |ctx: Ctx, card: String| async move {
            let masked: String = ctx
                .step("mask", || {
                    let c = card.clone();
                    async move { Ok::<_, Error>(format!("{}-SECRETRESULT", &c[..4])) }
                })
                .await?;
            let note: Option<String> = ctx.wait_for("note").await?;
            Ok(format!("{masked}/{}", note.unwrap_or_default()))
        });
        let run = engine
            .start(&wf, "s-1", &"4242SECRETCARD".to_string())
            .await
            .unwrap();
        run.signal("note", &"SECRETSIGNAL").await.unwrap();
        assert_eq!(
            run.result().await.unwrap(),
            "4242-SECRETRESULT/SECRETSIGNAL"
        );
        engine.shutdown().await.unwrap();
    }
    let mut leaked = Vec::new();
    for entry in walk(&dir) {
        let bytes = std::fs::read(&entry).unwrap();
        for needle in ["SECRETCARD", "SECRETRESULT", "SECRETSIGNAL"] {
            if bytes.windows(needle.len()).any(|w| w == needle.as_bytes()) {
                leaked.push(format!("{} holds {needle}", entry.display()));
            }
        }
    }
    assert!(leaked.is_empty(), "{leaked:?}");
    {
        // The same key reads it back; no key cannot.
        let engine = Engine::open(mk()).await.unwrap();
        let r: String = engine.run("s-1").result().await.unwrap();
        assert_eq!(r, "4242-SECRETRESULT/SECRETSIGNAL");
        engine.shutdown().await.unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_testing_reports_the_first_difference() {
    let dir = tmp("replay");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("pay", |ctx: Ctx, _: ()| async move {
        let q: u32 = ctx.step("quote", || async { Ok::<_, Error>(3) }).await?;
        let c: u32 = ctx
            .step("charge", || async move { Ok::<_, Error>(q * 2) })
            .await?;
        Ok(c)
    });
    engine
        .start(&wf, "p-1", &())
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    let detail = engine.describe("p-1").await.unwrap().unwrap();
    let export = fusee::export::ExportedJournal::from_detail(&detail);
    let file = dir.join(fusee::export::file_name("p-1"));
    export.write(&file).unwrap();
    engine.shutdown().await.unwrap();

    let same = fusee::Replayer::new();
    same.register("pay", |ctx: Ctx, _: ()| async move {
        let q: u32 = ctx.step("quote", || async { Ok::<_, Error>(0) }).await?;
        let c: u32 = ctx
            .step("charge", || async move { Ok::<_, Error>(q) })
            .await?;
        Ok(c)
    });
    let r = same.replay_file(&file).unwrap();
    assert!(r.ok && r.finished, "{r}");

    let changed = fusee::Replayer::new();
    changed.register("pay", |ctx: Ctx, _: ()| async move {
        let q: u32 = ctx.step("quote", || async { Ok::<_, Error>(3) }).await?;
        let c: u32 = ctx
            .step("refund", || async move { Ok::<_, Error>(q) })
            .await?;
        Ok(c)
    });
    let r = changed.replay_file(&file).unwrap();
    assert!(!r.ok, "{r}");
    let e = r.error.unwrap();
    assert!(e.contains("'charge'") && e.contains("'refund'"), "{e}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_id_policies() {
    let dir = tmp("ids");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("w", |ctx: Ctx, fail: bool| async move {
        let _: Option<()> = ctx
            .wait_for("go")
            .timeout(Duration::from_millis(50))
            .await?;
        if fail {
            return Err(Error::Failed("no".into()));
        }
        Ok(ctx.run_id())
    });
    let a = engine.start(&wf, "id-1", &false).await.unwrap();
    // Running: fails by default.
    let err = engine.start(&wf, "id-1", &false).await.unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)), "{err}");
    // use-existing returns the running workflow.
    let same = engine
        .start(&wf, "id-1", &false)
        .options(StartOptions::default().use_existing())
        .await
        .unwrap();
    let first = a.result().await.unwrap();
    assert_eq!(same.result().await.unwrap(), first);
    // Finished: allowed by default (a new run).
    let b = engine.start(&wf, "id-1", &true).await.unwrap();
    assert!(b.result().await.is_err());
    // allow-if-failed: the last run failed, so a new one starts.
    let only_failed = IdPolicy {
        running: OnRunning::Fail,
        finished: OnFinished::AllowIfFailed,
    };
    let c = engine
        .start(&wf, "id-1", &false)
        .options(StartOptions::default().id_policy(only_failed))
        .await
        .unwrap();
    assert!(c.result().await.unwrap() > first);
    // ...and now it succeeded, so allow-if-failed refuses.
    let err = engine
        .start(&wf, "id-1", &false)
        .options(StartOptions::default().id_policy(only_failed))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)), "{err}");
    let refuse = IdPolicy {
        running: OnRunning::Fail,
        finished: OnFinished::Refuse,
    };
    assert!(
        engine
            .start(&wf, "id-1", &false)
            .options(StartOptions::default().id_policy(refuse))
            .await
            .is_err()
    );
    // terminate-existing replaces a running workflow.
    let slow = engine.register("slow", |ctx: Ctx, _: ()| async move {
        let _: Option<()> = ctx.wait_for("go").await?;
        Ok(ctx.run_id())
    });
    let old = engine.start(&slow, "id-2", &()).await.unwrap();
    let replace = IdPolicy {
        running: OnRunning::TerminateExisting,
        finished: OnFinished::Allow,
    };
    engine
        .start(&slow, "id-2", &())
        .options(StartOptions::default().id_policy(replace))
        .await
        .unwrap();
    old.signal("go", &()).await.unwrap();
    assert!(
        old.result().await.unwrap() > 0,
        "the new run answers for the ID"
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_from_a_data_directory() {
    let dir = tmp("export");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("w", |ctx: Ctx, n: u32| async move {
        let a: u32 = ctx.step("a", || async move { Ok::<_, Error>(n) }).await?;
        let _: Option<()> = ctx.wait_for("go").await?;
        Ok(a)
    });
    for i in 0..3 {
        engine.start(&wf, &format!("e-{i}"), &i).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    // While the engine runs.
    let one = fusee::export::export_from_dir(&dir, Some("e-1"), 1).unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].workflow_id, "e-1");
    let all = fusee::export::export_from_dir(&dir, None, 10).unwrap();
    assert_eq!(all.len(), 3);
    let r = fusee::Replayer::new();
    r.register("w", |ctx: Ctx, n: u32| async move {
        let a: u32 = ctx.step("a", || async move { Ok::<_, Error>(n) }).await?;
        let _: Option<()> = ctx.wait_for("go").await?;
        Ok(a)
    });
    for j in &all {
        let rep = r.replay(j);
        assert!(rep.ok && !rep.finished, "{rep}");
    }
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn debounced_starts_collapse_into_one_run_with_the_latest_input() {
    let dir = tmp("debounce");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let starts = Arc::new(AtomicU32::new(0));
    let s2 = starts.clone();
    let wf = engine.register("reindex", move |_ctx: Ctx, n: u32| {
        s2.fetch_add(1, Ordering::SeqCst);
        async move { Ok(n) }
    });
    let rule = Debounce::new(Duration::from_millis(150));
    let a = engine.debounce(&wf, "doc-1", &1, &rule).await.unwrap();
    let b = engine.debounce(&wf, "doc-1", &2, &rule).await.unwrap();
    // Another key has its own window.
    let other = engine.debounce(&wf, "doc-2", &20, &rule).await.unwrap();
    let c = engine.debounce(&wf, "doc-1", &3, &rule).await.unwrap();
    assert_eq!(a.id(), b.id());
    assert_eq!(a.id(), c.id());
    assert_ne!(a.id(), other.id());
    assert!(a.id().starts_with("doc-1@"), "{}", a.id());
    // Nothing runs until the window closes.
    assert!(a.status().await.ok().is_none());
    a.started().await.unwrap();
    assert_eq!(a.result().await.unwrap(), 3);
    other.started().await.unwrap();
    assert_eq!(other.result().await.unwrap(), 20);
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    // After the window closed, a start opens a new window and a new run.
    let d = engine.debounce(&wf, "doc-1", &4, &rule).await.unwrap();
    assert_ne!(d.id(), a.id());
    d.started().await.unwrap();
    assert_eq!(d.result().await.unwrap(), 4);
    // A maximum wait bounds a window that starts keep extending.
    let capped = Debounce::new(Duration::from_millis(80)).max_wait(Duration::from_millis(200));
    let mut ids = std::collections::BTreeSet::new();
    let begin = Instant::now();
    let mut n = 100;
    while begin.elapsed() < Duration::from_millis(700) {
        ids.insert(
            engine
                .debounce(&wf, "busy", &n, &capped)
                .await
                .unwrap()
                .id()
                .to_owned(),
        );
        n += 1;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ids.len() >= 3, "{ids:?}");
    for id in &ids {
        let run = engine.run::<u32>(id);
        run.started().await.unwrap();
        run.result().await.unwrap();
    }
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batches_start_one_run_per_full_or_timed_out_batch() {
    let dir = tmp("batch");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register(
        "bulk",
        |_ctx: Ctx, events: Vec<u32>| async move { Ok(events) },
    );
    let rule = Batch::new(3, Duration::from_millis(200));
    let mut handles = Vec::new();
    for i in 0..7u32 {
        handles.push(engine.batch(&wf, "tenant-1", &i, &rule).await.unwrap());
    }
    // A retried event with the same ID is added once.
    let again = engine
        .batch(&wf, "tenant-1", &7, &rule)
        .event_id("evt-7")
        .await
        .unwrap();
    let dup = engine
        .batch(&wf, "tenant-1", &7, &rule)
        .event_id("evt-7")
        .await
        .unwrap();
    assert_eq!(again.id(), dup.id());
    let ids: Vec<&str> = handles.iter().map(|h| h.id()).collect();
    assert_eq!(ids[0], ids[2]);
    assert_ne!(ids[2], ids[3]);
    assert_eq!(ids[3], ids[5]);
    assert_ne!(ids[5], ids[6]);
    assert_eq!(ids[6], again.id());
    let mut results = Vec::new();
    for id in [ids[0], ids[3], ids[6]] {
        let run = engine.run::<Vec<u32>>(id);
        run.started().await.unwrap();
        results.push(run.result().await.unwrap());
    }
    assert_eq!(results, vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7]]);
    // A rule the coordinator cannot use is refused before anything is sent.
    let err = engine
        .batch(&wf, "tenant-1", &1, &Batch::new(0, Duration::ZERO))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Rejected(_)), "{err}");
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_acknowledged_debounced_start_survives_a_restart() {
    let dir = tmp("debounce-restart");
    let register =
        |engine: &Engine| engine.register("notify", |_ctx: Ctx, n: u32| async move { Ok(n * 10) });
    let rule = Debounce::new(Duration::from_millis(300));
    let id = {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let wf = register(&engine);
        let first = engine
            .debounce(&wf, "user-9", &1, &rule)
            .event_id("e-1")
            .await
            .unwrap();
        let second = engine
            .debounce(&wf, "user-9", &2, &rule)
            .event_id("e-2")
            .await
            .unwrap();
        assert_eq!(first.id(), second.id());
        // The process stops before the window closes.
        engine.shutdown().await.unwrap();
        first.id().to_owned()
    };
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = register(&engine);
    // A retry of an acknowledged event joins the same window once.
    let retried = engine
        .debounce(&wf, "user-9", &2, &rule)
        .event_id("e-2")
        .await
        .unwrap();
    assert_eq!(retried.id(), id);
    let run = engine.run::<u32>(&id);
    run.started().await.unwrap();
    assert_eq!(run.result().await.unwrap(), 20);
    // Once fired, the same event ID still answers with its run.
    let late = engine
        .debounce(&wf, "user-9", &2, &rule)
        .event_id("e-2")
        .await
        .unwrap();
    assert_eq!(late.id(), id);
    let listed = engine.list(100).await.unwrap();
    assert_eq!(
        listed.iter().filter(|r| r.wtype == "notify").count(),
        1,
        "{listed:?}"
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
