//! End-to-end tests of the embedded engine with real files.

use fusee::{Config, Ctx, Engine, Error, RetryPolicy, StartOptions, StepOptions};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

fn tmp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-test-{name}-{}-{}",
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
    // Plain fsync keeps the suite fast on macOS; durability semantics are
    // identical from the engine's point of view.
    c.local.flush = fusee::FlushMethod::Fsync;
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_directory_has_one_writer_until_shutdown() {
    let dir = tmp("writer");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("job", |_ctx: Ctx, n: u32| async move { Ok(n) });
    assert_eq!(
        engine
            .start(&wf, "a", &17)
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        17
    );
    assert!(matches!(Engine::open(cfg(&dir)).await, Err(Error::Io(e))
            if e.contains(&format!("is open in another process (pid {})", std::process::id()))));
    engine.shutdown().await.unwrap();
    let reopened = Engine::open(cfg(&dir)).await.unwrap();
    assert_eq!(reopened.run::<u32>("a").result().await.unwrap(), 17);
    reopened.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_permit_covers_step_future_construction() {
    let engine = Engine::open(Config::memory()).await.unwrap();
    engine
        .limit(
            "one",
            fusee::limits::LimitSpec {
                concurrent: 1,
                per_second: 0.0,
            },
        )
        .unwrap();
    let active = Arc::new(AtomicU32::new(0));
    let wf = engine.register("limited", move |ctx: Ctx, _: ()| {
        let active = active.clone();
        async move {
            let steps = (0..4).map(|_| {
                let active = active.clone();
                let ctx = ctx.clone();
                async move {
                    ctx.step("effect", || {
                        let previous = active.fetch_add(1, Ordering::SeqCst);
                        let active = active.clone();
                        async move {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                            active.fetch_sub(1, Ordering::SeqCst);
                            Ok::<_, Error>(previous)
                        }
                    })
                    .options(StepOptions::default().limit("one"))
                    .await
                }
            });
            ctx.all(steps)
                .await
                .into_iter()
                .collect::<Result<Vec<_>, Error>>()
        }
    });
    assert_eq!(
        engine
            .start(&wf, "p", &())
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        [0; 4]
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(target_os = "linux")]
async fn linux_flush_methods_recover_acknowledged_results() {
    use fusee::FlushMethod;
    let mut paths = vec![
        (FlushMethod::Fdatasync, false),
        (FlushMethod::Fdatasync, true),
        (FlushMethod::Dsync, false),
        (FlushMethod::Dsync, true),
    ];
    if cfg!(feature = "io-uring") {
        paths.push((FlushMethod::IoUring, false));
    }
    for (method, direct) in paths {
        let dir = tmp(&format!("linux-{method:?}-{direct}"));
        let mut config = Config::local(&dir).shards(1).segment_bytes(64 * 1024);
        config.local.flush = method;
        config.local.direct_io = direct;
        let engine = Engine::open(config.clone()).await.unwrap();
        let wf = engine.register("write", |ctx: Ctx, _: ()| async move {
            for _ in 0..20 {
                ctx.step("page", || async { Ok::<_, Error>(vec![7u8; 8192]) })
                    .await?;
            }
            Ok(42)
        });
        assert_eq!(
            engine
                .start(&wf, "id", &())
                .await
                .unwrap()
                .result()
                .await
                .unwrap(),
            42
        );
        engine.shutdown().await.unwrap();
        let engine = Engine::open(config).await.unwrap();
        assert_eq!(engine.run::<u32>("id").result().await.unwrap(), 42);
        engine.shutdown().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequential_steps_and_inspect() {
    let dir = tmp("seq");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("chain", |ctx: Ctx, n: u32| async move {
        let mut total = 0u32;
        for i in 0..n {
            total += ctx
                .step(&format!("s{i}"), || async move { Ok::<_, Error>(i) })
                .await?;
        }
        Ok(total)
    });
    let run = engine.start(&wf, "chain-1", &10).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 45);
    let detail = run.describe().await.unwrap();
    assert_eq!(detail.summary.status, "ok");
    let m = engine.metrics().await.unwrap();
    assert!(m.steps_run >= 10);
    // A start that attaches returns the same run and result.
    let again = engine
        .start(&wf, "chain-1", &99)
        .options(StartOptions::default().id_policy(fusee::IdPolicy::ATTACH))
        .await
        .unwrap();
    assert_eq!(again.result().await.unwrap(), 45);
    // By default a finished ID may start again.
    let fresh = engine.start(&wf, "chain-1", &3).await.unwrap();
    assert_eq!(fresh.result().await.unwrap(), 3);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_does_not_rerun_recorded_steps() {
    let dir = tmp("recover");
    let calls = Arc::new(AtomicU32::new(0));
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let c = calls.clone();
        let wf = engine.register("pausing", move |ctx: Ctx, _: ()| {
            let c = c.clone();
            async move {
                let a: u32 = ctx
                    .step("a", || {
                        let c = c.clone();
                        async move {
                            c.fetch_add(1, Ordering::SeqCst);
                            Ok::<_, Error>(7)
                        }
                    })
                    .await?;
                let approved: Option<bool> = ctx.wait_for("approve").await?;
                let b: u32 = ctx
                    .step("b", || {
                        let c = c.clone();
                        async move {
                            c.fetch_add(100, Ordering::SeqCst);
                            Ok::<_, Error>(a + 1)
                        }
                    })
                    .await?;
                Ok((b, approved.unwrap_or(false)))
            }
        });
        engine.start(&wf, "p-1", &()).await.unwrap();
        // Wait until step a ran.
        for _ in 0..200 {
            if calls.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        engine.shutdown().await.unwrap();
    }
    // Reopen: step a is replayed (not re-run); the signal resumes the run.
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let c = calls.clone();
    let wf = engine.register("pausing", move |ctx: Ctx, _: ()| {
        let c = c.clone();
        async move {
            let a: u32 = ctx
                .step("a", || {
                    let c = c.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, Error>(7)
                    }
                })
                .await?;
            let approved: Option<bool> = ctx.wait_for("approve").await?;
            let b: u32 = ctx
                .step("b", || {
                    let c = c.clone();
                    async move {
                        c.fetch_add(100, Ordering::SeqCst);
                        Ok::<_, Error>(a + 1)
                    }
                })
                .await?;
            Ok((b, approved.unwrap_or(false)))
        }
    });
    let run = engine.run::<(u32, bool)>("p-1");
    run.signal("approve", &true).await.unwrap();
    assert_eq!(run.result().await.unwrap(), (8, true));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        101,
        "step a must not run twice"
    );
    let _ = wf;
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waits_time_out_and_sleeps_fire() {
    let dir = tmp("timers");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("timers", |ctx: Ctx, _: ()| async move {
        let got: Option<String> = ctx
            .wait_for("never")
            .timeout(Duration::from_millis(30))
            .await?;
        ctx.sleep("nap", Duration::from_millis(20)).await?;
        let t = ctx.now_ms()?;
        Ok((got, t > 0))
    });
    let run = engine.start(&wf, "t-1", &()).await.unwrap();
    assert_eq!(run.result().await.unwrap(), (None, true));
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn children_parallel_steps_and_races() {
    let dir = tmp("children");
    let engine = Engine::open(Config::local(&dir).shards(4)).await.unwrap();
    engine.register("double", |ctx: Ctx, x: u64| async move {
        ctx.step("mul", || async move { Ok::<_, Error>(x * 2) })
            .await
    });
    let wf = engine.register("parent", |ctx: Ctx, n: u64| async move {
        let kids: Vec<_> = (0..n)
            .map(|i| {
                let ctx = ctx.clone();
                async move {
                    let id = format!("{}/kid-{i}", ctx.workflow_id());
                    ctx.child::<u64, u64>("double", &id, &i).await
                }
            })
            .collect();
        let results = ctx.all(kids).await;
        let sum: u64 = results
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .sum();
        let a = ctx.clone();
        let b = ctx.clone();
        let (winner, _) = ctx
            .race(vec![
                Box::pin(async move {
                    a.sleep("slow", Duration::from_millis(500))
                        .await
                        .map(|_| 0u64)
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, Error>>>>,
                Box::pin(async move { b.step("fast", || async { Ok::<_, Error>(1u64) }).await }),
            ])
            .await;
        Ok((sum, winner))
    });
    let run = engine.start(&wf, "parent-1", &8).await.unwrap();
    assert_eq!(run.result().await.unwrap(), (56, 1));
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_then_success_and_final_failure() {
    let dir = tmp("retry");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let tries = Arc::new(AtomicU32::new(0));
    let t = tries.clone();
    let wf = engine.register("flaky", move |ctx: Ctx, fail_always: bool| {
        let t = t.clone();
        async move {
            let opts = StepOptions::default().retry(RetryPolicy::exponential(
                4,
                Duration::from_millis(5),
                Duration::from_millis(20),
            ));
            let v: u32 = ctx
                .step("call", || {
                    let t = t.clone();
                    async move {
                        let n = t.fetch_add(1, Ordering::SeqCst) + 1;
                        if fail_always || n < 3 {
                            Err(format!("attempt {n} failed"))
                        } else {
                            Ok(n)
                        }
                    }
                })
                .options(opts)
                .await?;
            Ok(v)
        }
    });
    assert_eq!(
        engine
            .start(&wf, "f-1", &false)
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        3
    );
    tries.store(0, Ordering::SeqCst);
    let err = engine
        .start(&wf, "f-2", &true)
        .await
        .unwrap()
        .result()
        .await
        .unwrap_err();
    assert!(
        // The caller sees the step and its last failure, not just text.
        matches!(err, Error::Step { ref name, ref failure }
            if name == "call" && failure.message == "attempt 4 failed"),
        "{err}"
    );
    assert_eq!(tries.load(Ordering::SeqCst), 4);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_runs_compensation() {
    let dir = tmp("cancel");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("saga", |ctx: Ctx, _: ()| async move {
        ctx.step("reserve", || async { Ok::<_, Error>(()) }).await?;
        match ctx.wait_for::<bool>("confirm").await {
            Ok(_) => Ok("confirmed".to_string()),
            Err(Error::Cancelled(reason)) => {
                ctx.step("release", || async { Ok::<_, Error>(()) }).await?;
                Err(Error::Cancelled(format!("compensated: {reason}")))
            }
            Err(e) => Err(e),
        }
    });
    let run = engine.start(&wf, "s-1", &()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    run.cancel("customer left").await.unwrap();
    let err = run.result().await.unwrap_err();
    assert_eq!(err, Error::Cancelled("compensated: customer left".into()));
    let detail = run.describe().await.unwrap();
    assert_eq!(detail.summary.status, "cancelled");
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_deadline_and_terminate() {
    let dir = tmp("deadline");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("forever", |ctx: Ctx, _: ()| async move {
        ctx.wait_for::<()>("never").await?;
        Ok(())
    });
    let run = engine
        .start(&wf, "d-1", &())
        .options(StartOptions {
            timeout: Some(Duration::from_millis(40)),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(run.result().await.unwrap_err(), Error::TimedOut);
    let run2 = engine.start(&wf, "d-2", &()).await.unwrap();
    run2.terminate("ops").await.unwrap();
    assert_eq!(run2.result().await.unwrap_err(), Error::Terminated);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_code_is_a_determinism_error() {
    let dir = tmp("determinism");
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let wf = engine.register("pay", |ctx: Ctx, _: ()| async move {
            ctx.step("charge", || async { Ok::<_, Error>(1) }).await?;
            ctx.wait_for::<()>("go").await?;
            Ok(())
        });
        engine.start(&wf, "pay-1", &()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        engine.shutdown().await.unwrap();
    }
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    engine.register("pay", |ctx: Ctx, _: ()| async move {
        ctx.step("refund", || async { Ok::<_, Error>(1) }).await?;
        Ok(())
    });
    let mut status = None;
    for _ in 0..100 {
        status = engine.status("pay-1").await.unwrap();
        if status.as_ref().is_some_and(|s| s.status == "stuck") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let s = status.unwrap();
    assert_eq!(s.status, "stuck");
    let msg = s.error.unwrap();
    assert!(
        msg.contains("'charge'") && msg.contains("'refund'"),
        "{msg}"
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn suspended_runs_park_and_resume_after_restart() {
    let dir = tmp("park");
    let c = cfg(&dir).park_after(Duration::from_millis(0));
    {
        let engine = Engine::open(c.clone()).await.unwrap();
        let wf = engine.register("sleeper", |ctx: Ctx, i: u32| async move {
            ctx.wait_for::<u32>("wake").await?;
            ctx.sleep("nap", Duration::from_millis(5)).await?;
            Ok(i)
        });
        for i in 0..50u32 {
            engine.start(&wf, &format!("sl-{i}"), &i).await.unwrap();
        }
        for _ in 0..200 {
            if engine.metrics().await.unwrap().runs_parked >= 50 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(engine.metrics().await.unwrap().runs_parked >= 50);
        // Wake half of them now, from the parked state.
        for i in 0..25u32 {
            engine
                .run::<u32>(&format!("sl-{i}"))
                .signal("wake", &1)
                .await
                .unwrap();
        }
        for i in 0..25u32 {
            assert_eq!(
                engine
                    .run::<u32>(&format!("sl-{i}"))
                    .result()
                    .await
                    .unwrap(),
                i
            );
        }
        engine.shutdown().await.unwrap();
    }
    let engine = Engine::open(c).await.unwrap();
    engine.register("sleeper", |ctx: Ctx, i: u32| async move {
        ctx.wait_for::<u32>("wake").await?;
        ctx.sleep("nap", Duration::from_millis(5)).await?;
        Ok(i)
    });
    for i in 0..50u32 {
        let h = engine.run::<u32>(&format!("sl-{i}"));
        if i >= 25 {
            h.signal("wake", &1).await.unwrap();
        }
        assert_eq!(h.result().await.unwrap(), i);
    }
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoints_compact_the_log_and_recover() {
    let dir = tmp("compact");
    let c = cfg(&dir)
        .segment_bytes(64 * 1024)
        .checkpoint_interval(Duration::from_millis(50));
    let wf_fn = |ctx: Ctx, n: u32| async move {
        let mut acc = 0u64;
        for i in 0..n {
            acc += ctx
                .step(&format!("s{}", i % 7), || async move {
                    Ok::<_, Error>(u64::from(i))
                })
                .await?;
        }
        ctx.wait_for::<()>("finish").await?;
        Ok(acc)
    };
    {
        let engine = Engine::open(c.clone()).await.unwrap();
        let wf = engine.register("long", wf_fn);
        for r in 0..20 {
            engine.start(&wf, &format!("long-{r}"), &300).await.unwrap();
        }
        // Let them finish their steps and checkpoints run.
        tokio::time::sleep(Duration::from_millis(400)).await;
        let m = engine.metrics().await.unwrap();
        assert!(m.checkpoints > 0, "{m:?}");
        engine.shutdown().await.unwrap();
    }
    // Old segments were deleted (per-shard logs) or retired and reused
    // (the shared log).
    let segs: usize = (0..2)
        .map(|s| {
            std::fs::read_dir(dir.join(format!("shard-{s:03}/log")))
                .map(|d| d.count())
                .unwrap_or(0)
        })
        .sum::<usize>()
        + std::fs::read_dir(dir.join("wal"))
            .map(|d| d.count())
            .unwrap_or(0);
    assert!(segs < 40, "segments were not compacted: {segs}");
    let engine = Engine::open(c).await.unwrap();
    engine.register("long", wf_fn);
    for r in 0..20 {
        let h = engine.run::<u64>(&format!("long-{r}"));
        h.signal("finish", &()).await.unwrap();
        assert_eq!(h.result().await.unwrap(), (0..300u64).sum::<u64>());
    }
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_mode_and_signals_between_workflows() {
    let engine = Engine::open(Config::memory().shards(3)).await.unwrap();
    let sender = engine.register("sender", |ctx: Ctx, target: String| async move {
        ctx.signal_workflow(&target, "ping", &"hello".to_string())?;
        Ok(())
    });
    let listener = engine
        .start(
            engine.register::<(), String, _, _>("listener", |ctx: Ctx, _: ()| async move {
                let v: Option<String> = ctx.wait_for("ping").await?;
                Ok(v.unwrap_or_default())
            }),
            "l-1",
            &(),
        )
        .await
        .unwrap();
    engine
        .start(&sender, "s-1", &"l-1".to_string())
        .await
        .unwrap();
    assert_eq!(listener.result().await.unwrap(), "hello");
    engine.shutdown().await.unwrap();
}

/// Read-only steps run without waiting for the disk: with a slow flush, a
/// chain of them never waits at a gate, while the same chain of ordinary
/// steps waits for a flush before every step after the first. The results
/// are the same either way and survive a restart without anything running
/// again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_only_steps_skip_their_gate() {
    let dir = tmp("read-only");
    let mut c = Config::local(&dir).shards(1);
    c.local.flush = fusee::FlushMethod::Fsync;
    c.local.emulated_flush = Duration::from_millis(15);
    let engine = Engine::open(c).await.unwrap();
    let wf = engine.register("chain", |ctx: Ctx, read_only: bool| async move {
        let opts = if read_only {
            StepOptions::default().read_only()
        } else {
            StepOptions::default()
        };
        let mut v = 0u32;
        for i in 0..8 {
            let prev = v;
            v = ctx
                .step(
                    &format!("s{i}"),
                    move || async move { Ok::<_, Error>(prev + 1) },
                )
                .options(opts.clone())
                .await?;
        }
        Ok(v)
    });
    let waits = |m: fusee::Metrics| m.gate_waits;
    let before = waits(engine.metrics().await.unwrap());
    let t = std::time::Instant::now();
    assert_eq!(
        engine
            .start(&wf, "reads", &true)
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        8
    );
    let read_only_time = t.elapsed();
    let mid = waits(engine.metrics().await.unwrap());
    assert_eq!(mid - before, 0, "a read-only step waited at its gate");
    let t = std::time::Instant::now();
    assert_eq!(
        engine
            .start(&wf, "gated", &false)
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        8
    );
    let gated_time = t.elapsed();
    let after = waits(engine.metrics().await.unwrap());
    assert!(
        after - mid >= 7,
        "ordinary steps waited at {} gates, expected 7",
        after - mid
    );
    assert!(
        read_only_time < gated_time,
        "read-only chain took {read_only_time:?}, ordinary chain {gated_time:?}"
    );
    engine.shutdown().await.unwrap();
    // Reopened: both results are durable and nothing runs again.
    let engine = Engine::open(Config::local(&dir).shards(1)).await.unwrap();
    let ran = Arc::new(AtomicU32::new(0));
    let r = ran.clone();
    engine.register("chain", move |ctx: Ctx, read_only: bool| {
        let r = r.clone();
        async move {
            let opts = if read_only {
                StepOptions::default().read_only()
            } else {
                StepOptions::default()
            };
            let mut v = 0u32;
            for i in 0..8 {
                let prev = v;
                let r = r.clone();
                v = ctx
                    .step(&format!("s{i}"), move || {
                        r.fetch_add(1, Ordering::SeqCst);
                        async move { Ok::<_, Error>(prev + 1) }
                    })
                    .options(opts.clone())
                    .await?;
            }
            Ok(v)
        }
    });
    assert_eq!(engine.run::<u32>("reads").result().await.unwrap(), 8);
    assert_eq!(engine.run::<u32>("gated").result().await.unwrap(), 8);
    assert_eq!(ran.load(Ordering::SeqCst), 0);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successive_checkpoints_reuse_unchanged_active_journals() {
    use fusee::core::store::Checkpoint;
    let dir = tmp("incremental");
    let config = cfg(&dir)
        .shards(1)
        .park_after(Duration::from_secs(3600))
        .checkpoint_interval(Duration::from_millis(25));
    let waiter = |ctx: Ctx, _: ()| async move {
        let first = ctx.wait_for::<u32>("first").await?.unwrap();
        let second = ctx.wait_for::<u32>("second").await?.unwrap();
        Ok(first + second)
    };
    // Read an installed checkpoint that contains the first (still active) run.
    let checkpoint = |after| {
        let shard = dir.join("shard-000");
        async move {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let newest = std::fs::read_dir(&shard)
                        .unwrap()
                        .filter_map(Result::ok)
                        .map(|f| f.path())
                        .filter(|p| p.extension().is_some_and(|e| e == "fck"))
                        .filter_map(|p| Checkpoint::decode(&std::fs::read(p).ok()?).ok())
                        .filter(|c| c.lsn > after && c.runs.iter().any(|r| r.run == 1))
                        .max_by_key(|c| c.lsn);
                    if let Some(c) = newest {
                        return c;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap()
        }
    };
    let e = Engine::open(config.clone()).await.unwrap();
    let h = e
        .start(e.register("waiter", waiter), "active", &())
        .await
        .unwrap();
    let first = checkpoint(0).await;
    let original = first.runs.iter().find(|r| r.run == 1).unwrap();
    let blob = original.blob.unwrap();
    assert!(original.journal.is_empty());
    e.start(
        e.register("other", |_: Ctx, _: ()| async { Ok(()) }),
        "independent",
        &(),
    )
    .await
    .unwrap()
    .result()
    .await
    .unwrap();
    let second = checkpoint(first.lsn).await;
    let unchanged = second.runs.iter().find(|r| r.run == 1).unwrap();
    assert_eq!(unchanged.blob, Some(blob));
    assert!(unchanged.journal.is_empty());
    h.signal("first", &19u32).await.unwrap();
    let changed = checkpoint(second.lsn).await;
    assert_ne!(
        changed.runs.iter().find(|r| r.run == 1).unwrap().blob,
        Some(blob)
    );
    e.shutdown().await.unwrap();
    let e = Engine::open(config).await.unwrap();
    e.register("waiter", waiter);
    let recovered = e.run::<u32>("active");
    recovered.signal("second", &23u32).await.unwrap();
    assert_eq!(recovered.result().await.unwrap(), 42);
    e.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

/// Retention can expire with a completely idle shard. Reclaim the expired
/// history, then recover an acknowledged result from the preceding checkpoint
/// and the segment tail after damaging the newer checkpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retention_rewrites_live_frames_and_preserves_checkpoint_fallback() {
    use fusee::core::store::Checkpoint;
    let dir = tmp("store-generations");
    let shard = dir.join("shard-000");
    let c = cfg(&dir)
        .shards(1)
        .keep_history(true)
        .segment_bytes(2 << 20)
        .park_after(Duration::from_millis(20))
        .checkpoint_interval(Duration::from_millis(30));
    let checkpoints = || {
        let mut saved: Vec<_> = std::fs::read_dir(&shard)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|s| s == "fck"))
            .filter_map(|p| {
                Checkpoint::decode(&std::fs::read(&p).unwrap())
                    .ok()
                    .map(|c| (p, c))
            })
            .collect();
        saved.sort_by_key(|(_, c)| c.lsn);
        saved
    };
    let sleeper = |ctx: Ctx, _: ()| async move {
        let v = ctx.wait_for::<u32>("finish").await?;
        Ok(v.unwrap())
    };
    {
        let engine = Engine::open(c.clone()).await.unwrap();
        engine
            .start(engine.register("waiter", sleeper), "stay", &())
            .await
            .unwrap();
        let w = engine.register(
            "large",
            |_: Ctx, input: String| async move { Ok(input.len()) },
        );
        let payload = "s".repeat(384 << 10);
        for id in 0..32 {
            assert_eq!(
                engine
                    .start(&w, &format!("old-{id}"), &payload)
                    .await
                    .unwrap()
                    .result()
                    .await
                    .unwrap(),
                payload.len()
            );
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if checkpoints()
                    .last()
                    .is_some_and(|(_, c)| c.store_len > (10 << 20))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .unwrap();
        engine.shutdown().await.unwrap();
    }
    // All terminal frames must have expired before recovery. Otherwise a
    // legitimate first rewrite may retain a few still-live large frames.
    tokio::time::sleep(Duration::from_millis(150)).await;
    {
        let engine = Engine::open(c.clone().retention(Duration::from_millis(100)))
            .await
            .unwrap();
        engine.register("waiter", sleeper);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if checkpoints()
                    .last()
                    .is_some_and(|(_, c)| c.store_generation > 1 && c.store_len < 1 << 20)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            engine.run::<usize>("old-0").result().await,
            Err(Error::NotFound(_))
        ));
        engine.shutdown().await.unwrap();
    }
    let engine = Engine::open(c.clone()).await.unwrap();
    engine.register("waiter", sleeper);
    assert!(matches!(
        engine.run::<usize>("old-0").result().await,
        Err(Error::NotFound(_))
    ));
    let resumed = engine.run::<u32>("stay");
    resumed.signal("finish", &42u32).await.unwrap();
    assert_eq!(resumed.result().await.unwrap(), 42);
    tokio::time::sleep(Duration::from_millis(180)).await;
    engine.shutdown().await.unwrap();
    let mut saved = checkpoints();
    assert_eq!(saved.len(), 2);
    std::fs::write(saved.pop().unwrap().0, b"damaged newest checkpoint").unwrap();
    let engine = Engine::open(c.clone()).await.unwrap();
    assert_eq!(engine.run::<u32>("stay").result().await.unwrap(), 42);
    engine.shutdown().await.unwrap();
    for (p, _) in checkpoints() {
        std::fs::write(p, b"damaged checkpoint").unwrap();
    }
    assert!(
        matches!(Engine::open(c).await,Err(Error::Io(msg)) if msg.contains("checkpoints are damaged"))
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archived_journals_survive_idle_retention_and_detect_corruption() {
    let dir = tmp("archive");
    let config = cfg(&dir)
        .shards(1)
        .archive_history(true)
        .retention(Duration::from_millis(100))
        .checkpoint_interval(Duration::from_millis(20));
    let engine = Engine::open(config).await.unwrap();
    let wf = engine.register("retained", |ctx: Ctx, n: u32| async move {
        ctx.step("plus", || async move { Ok::<_, Error>(n + 1) })
            .await
    });
    assert_eq!(
        engine
            .start(&wf, "customer", &41)
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        42
    );
    let arc = dir.join("shard-000/archive");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if arc.read_dir().is_ok_and(|mut files| {
                files.any(|p| p.unwrap().path().extension().is_some_and(|x| x == "arc"))
            }) && engine.describe("customer").await.unwrap().is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    engine.shutdown().await.unwrap();
    let one = fusee::export::export_from_dir(&dir, Some("customer"), 1).unwrap();
    assert_eq!(one.len(), 1);
    assert!(one[0].is_complete());
    assert_eq!(fusee::export::export_from_dir(&dir, None, 1).unwrap(), one);
    let file = arc.read_dir().unwrap().next().unwrap().unwrap().path();
    let mut bytes = std::fs::read(&file).unwrap();
    bytes.pop();
    std::fs::write(file, bytes).unwrap();
    assert!(fusee::export::export_from_dir(&dir, Some("customer"), 1).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_directory_keeps_the_log_format_it_was_created_with() {
    let dir = tmp("format2");
    let wf_fn = |ctx: Ctx, n: u32| async move {
        ctx.step("double", || async move { Ok::<_, Error>(n * 2) })
            .await
    };
    {
        let engine = Engine::open(cfg(&dir).shared_log(false)).await.unwrap();
        let wf = engine.register("double", wf_fn);
        assert_eq!(
            engine
                .start(&wf, "a", &21)
                .await
                .unwrap()
                .result()
                .await
                .unwrap(),
            42
        );
        engine.shutdown().await.unwrap();
    }
    // Reopened with the default (shared log): still per-shard segments.
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = engine.register("double", wf_fn);
    assert_eq!(engine.run::<u32>("a").result().await.unwrap(), 42);
    assert_eq!(
        engine
            .start(&wf, "b", &4)
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        8
    );
    engine.shutdown().await.unwrap();
    assert!(!dir.join("wal").exists());
    let meta = std::fs::read_to_string(dir.join("fusee.meta")).unwrap();
    assert!(meta.contains("log=2"), "{meta}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_shared_log_survives_restarts_and_stays_small() {
    let dir = tmp("shared");
    let c = cfg(&dir)
        .shards(4)
        .segment_bytes(64 * 1024)
        .checkpoint_interval(Duration::from_millis(20));
    let wf_fn = |ctx: Ctx, n: u32| async move {
        let mut acc = 0u32;
        for i in 0..n {
            acc += ctx
                .step(&format!("s{i}"), || async move { Ok::<_, Error>(i) })
                .await?;
        }
        Ok(acc)
    };
    for round in 0..4u32 {
        let engine = Engine::open(c.clone()).await.unwrap();
        let wf = engine.register("sum", wf_fn);
        for earlier in 0..round {
            for i in 0..25 {
                let r = engine.run::<u32>(&format!("r{earlier}-{i}"));
                assert_eq!(r.result().await.unwrap(), (0..40).sum::<u32>());
            }
        }
        let mut handles = Vec::new();
        for i in 0..25 {
            handles.push(
                engine
                    .start(&wf, &format!("r{round}-{i}"), &40)
                    .await
                    .unwrap(),
            );
        }
        for h in handles {
            assert_eq!(h.result().await.unwrap(), (0..40).sum::<u32>());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        engine.shutdown().await.unwrap();
        let meta = std::fs::read_to_string(dir.join("fusee.meta")).unwrap();
        assert!(meta.contains("log=3"), "{meta}");
        // The chain spans what the shards' older checkpoints still need
        // (with 64 KiB segments, a few), plus two spares and one prepared.
        let files = std::fs::read_dir(dir.join("wal")).unwrap().count();
        assert!(files <= 12, "round {round}: {files} log files");
    }
    // Two idle restarts: each shard's two retained checkpoints then cover
    // everything, so every segment but the active one is retired.
    for _ in 0..2 {
        let engine = Engine::open(c.clone()).await.unwrap();
        engine.register("sum", wf_fn);
        engine.shutdown().await.unwrap();
    }
    let engine = Engine::open(c.clone()).await.unwrap();
    let wf = engine.register("sum", wf_fn);
    for i in 0..25 {
        let r = engine.run::<u32>(&format!("r3-{i}"));
        assert_eq!(r.result().await.unwrap(), (0..40).sum::<u32>());
    }
    drop(wf);
    engine.shutdown().await.unwrap();
    let files = std::fs::read_dir(dir.join("wal")).unwrap().count();
    assert!(files <= 4, "after idle restarts: {files} log files");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A shard removes finished runs past the retention it is opened with, so a
/// tool that opens a directory with a shorter one than the application's
/// deletes history; tools open with `TOOL_RETENTION`, which removes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tool_opening_a_directory_expires_nothing() {
    let dir = tmp("tool-retention");
    let app = cfg(&dir).retention(Duration::from_secs(30 * 86_400));
    let engine = Engine::open(app.clone()).await.unwrap();
    let wf = engine.register("job", |_ctx: Ctx, n: u32| async move { Ok(n) });
    for id in ["a", "b"] {
        engine
            .start(&wf, id, &1)
            .await
            .unwrap()
            .result()
            .await
            .unwrap();
    }
    engine.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let count = |e: Engine| async move {
        let n = e.list(100).await.unwrap().len();
        e.shutdown().await.unwrap();
        n
    };
    // A tool: both runs stay.
    let tool = cfg(&dir).retention(fusee::engine::TOOL_RETENTION);
    assert_eq!(count(Engine::open(tool).await.unwrap()).await, 2);
    assert_eq!(count(Engine::open(app.clone()).await.unwrap()).await, 2);
    // What a shorter retention does at open: the runs past it go.
    let short = cfg(&dir).retention(Duration::from_millis(10));
    assert_eq!(count(Engine::open(short).await.unwrap()).await, 0);
    assert_eq!(count(Engine::open(app).await.unwrap()).await, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Listings return a shard's newest runs, not the first ones it holds:
/// without a filter, with an equality (which the index answers newest
/// first), with a range, and after a reopen, when they are parked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listings_return_the_newest_runs() {
    let dir = tmp("newest");
    let mut c = cfg(&dir).shards(1);
    c.park_after = Duration::from_millis(1);
    let newest: Vec<String> = (250..300).map(|i| format!("run-{i:04}")).collect();
    let check = |engine: Engine| {
        let newest = newest.clone();
        async move {
            let ids = |rows: Vec<fusee::RunSummary>| {
                let mut v: Vec<String> = rows.into_iter().map(|r| r.id).collect();
                v.sort();
                v
            };
            assert_eq!(ids(engine.list(50).await.unwrap()), newest);
            for filter in [
                serde_json::json!({"type": "job"}),
                serde_json::json!({"status": "ok"}),
                serde_json::json!({"id": {"gte": "run-"}}),
            ] {
                let rows = engine.list_where(&filter, 50).await.unwrap();
                assert_eq!(ids(rows), newest, "{filter}");
            }
            engine.shutdown().await.unwrap();
        }
    };
    let engine = Engine::open(c.clone()).await.unwrap();
    let wf = engine.register("job", |_ctx: Ctx, n: u32| async move { Ok(n) });
    for i in 0..300u32 {
        let id = format!("run-{i:04}");
        engine
            .start(&wf, &id, &i)
            .await
            .unwrap()
            .result()
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    check(engine).await;
    check(Engine::open(c).await.unwrap()).await;
    let _ = std::fs::remove_dir_all(&dir);
}
