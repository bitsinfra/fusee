//! Developer-experience checks, each asserting the specific error or state
//! a mistake produces instead of a silent failure.

use fusee::{Config, Ctx, Engine, Error, Failure, RetryPolicy, ScheduleSpec, StepOptions};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

fn memory() -> Config {
    Config::memory().shards(1)
}

fn fast(attempts: u32) -> StepOptions {
    StepOptions::default().retry(RetryPolicy::exponential(
        attempts,
        Duration::from_millis(1),
        Duration::from_millis(2),
    ))
}

async fn status_of(engine: &Engine, id: &str, want: &str) -> fusee::RunSummary {
    for _ in 0..400 {
        if let Some(s) = engine.status(id).await.unwrap()
            && s.status == want
        {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("{id} never reached {want}: {:?}", engine.status(id).await);
}

/// Finding 1: a start of a type that was never registered fails at once,
/// naming the registered types and the nearest one.
#[tokio::test]
async fn unknown_type_is_refused_with_the_nearest_name() {
    let engine = Engine::open(memory()).await.unwrap();
    engine.register("checkout", |_c: Ctx, _: ()| async move { Ok(()) });
    let err = engine
        .start_raw(
            "chekout",
            "o-1",
            Arc::from(&b"null"[..]),
            Default::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "UnknownWorkflow");
    assert_eq!(
        err.to_string(),
        "no code is registered for workflow type 'chekout' (did you mean 'checkout'?). \
         Registered types: checkout"
    );
    engine.shutdown().await.unwrap();
}

/// Finding 3: `ctx.key()` outside a step stops the run with a message
/// instead of handing every caller the same key.
#[tokio::test]
async fn key_outside_a_step_stops_the_run() {
    let engine = Engine::open(memory()).await.unwrap();
    let wf = engine.register("keyed", |ctx: Ctx, _: ()| async move {
        let _ = ctx.key();
        ctx.wait_for::<()>("never").await?;
        Ok(())
    });
    engine.start(&wf, "k-1", &()).await.unwrap();
    let s = status_of(&engine, "k-1", "stuck").await;
    assert_eq!(s.error.as_deref(), Some(fusee::runtime::KEY_OUTSIDE_STEP));
    engine.shutdown().await.unwrap();
}

#[derive(Debug)]
struct CardDeclined;

impl std::fmt::Display for CardDeclined {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("card declined")
    }
}

impl std::error::Error for CardDeclined {}

/// Finding 4: the caller gets the step, its failure's type, code and
/// details, not a message alone.
#[tokio::test]
async fn a_failure_keeps_its_type_code_and_details() {
    let engine = Engine::open(memory()).await.unwrap();
    let wf = engine.register("pay", |ctx: Ctx, _: ()| async move {
        ctx.step("charge", || async {
            Err::<(), _>(
                Failure::from_error(&CardDeclined, Some("CardDeclined".into()))
                    .code("card_declined")
                    .details(serde_json::json!({ "last4": "4242" })),
            )
        })
        .options(fast(3))
        .await?;
        Ok(())
    });
    let err = engine
        .start(&wf, "p-1", &())
        .await
        .unwrap()
        .result()
        .await
        .unwrap_err();
    let Error::Step { name, failure } = err else {
        panic!("{err:?}");
    };
    assert_eq!(name, "charge");
    assert_eq!(failure.kind.as_deref(), Some("CardDeclined"));
    assert_eq!(failure.code.as_deref(), Some("card_declined"));
    assert_eq!(
        failure.details,
        Some(serde_json::json!({ "last4": "4242" }))
    );
    // A type that implements `Error` keeps its name without help.
    let wf = engine.register("pay2", |ctx: Ctx, _: ()| async move {
        ctx.step("charge", || async { Err::<(), _>(CardDeclined) })
            .options(fast(1))
            .await?;
        Ok(())
    });
    let err = engine
        .start(&wf, "p-2", &())
        .await
        .unwrap()
        .result()
        .await
        .unwrap_err();
    assert_eq!(
        err.failure().and_then(|f| f.kind.clone()).as_deref(),
        Some("CardDeclined")
    );
    engine.shutdown().await.unwrap();
}

/// Finding 5: a failure marked final is not retried, and neither is one
/// the policy names.
#[tokio::test]
async fn final_failures_are_not_retried() {
    let engine = Engine::open(memory()).await.unwrap();
    let tries = Arc::new(AtomicU32::new(0));
    let t = tries.clone();
    let wf = engine.register("fatal", move |ctx: Ctx, by_policy: bool| {
        let t = t.clone();
        async move {
            let opts = if by_policy {
                StepOptions::default().retry(
                    RetryPolicy::exponential(5, Duration::from_millis(1), Duration::from_millis(1))
                        .non_retryable("CardDeclined"),
                )
            } else {
                fast(5)
            };
            ctx.step("charge", || {
                let t = t.clone();
                async move {
                    t.fetch_add(1, Ordering::SeqCst);
                    if by_policy {
                        Err::<(), _>(Failure::new("declined").kind("CardDeclined"))
                    } else {
                        Err(Failure::fatal("declined"))
                    }
                }
            })
            .options(opts)
            .await?;
            Ok(())
        }
    });
    for (id, by_policy) in [("f-1", false), ("f-2", true)] {
        tries.store(0, Ordering::SeqCst);
        let err = engine
            .start(&wf, id, &by_policy)
            .await
            .unwrap()
            .result()
            .await
            .unwrap_err();
        assert_eq!(err.code(), "StepFailed", "{err}");
        assert_eq!(tries.load(Ordering::SeqCst), 1, "{id} was retried");
    }
    engine.shutdown().await.unwrap();
}

/// Finding 6: a result that cannot be encoded is a final codec failure
/// naming what to change, not a retried step failure.
#[tokio::test]
async fn an_unencodable_result_is_not_retried() {
    let engine = Engine::open(memory()).await.unwrap();
    let tries = Arc::new(AtomicU32::new(0));
    let t = tries.clone();
    let wf = engine.register("codec", move |ctx: Ctx, _: ()| {
        let t = t.clone();
        async move {
            // JSON object keys are strings; a tuple key cannot be encoded.
            let _: HashMap<(u32, u32), u32> = ctx
                .step("load", || {
                    let t = t.clone();
                    async move {
                        t.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, Error>(HashMap::from([((1u32, 2u32), 3u32)]))
                    }
                })
                .options(fast(5))
                .await?;
            Ok(())
        }
    });
    let err = engine
        .start(&wf, "c-1", &())
        .await
        .unwrap()
        .result()
        .await
        .unwrap_err();
    let failure = err.failure().unwrap();
    assert_eq!(failure.kind.as_deref(), Some("Codec"), "{failure:?}");
    assert!(!failure.is_retryable());
    assert_eq!(tries.load(Ordering::SeqCst), 1);
    engine.shutdown().await.unwrap();
}

/// Finding 15: the messages name the object and the next action.
#[tokio::test]
async fn messages_name_the_object_and_the_fix() {
    let engine = Engine::open(memory()).await.unwrap();
    let wf = engine.register("job", |ctx: Ctx, _: ()| async move {
        ctx.wait_for::<()>("go").await?;
        Ok(())
    });
    engine.start(&wf, "order-1", &()).await.unwrap();
    let err = engine.start(&wf, "order-1", &()).await.unwrap_err();
    assert_eq!(err.code(), "AlreadyExists");
    let m = err.to_string();
    assert!(
        m.starts_with("workflow 'order-1' is already running (run "),
        "{m}"
    );
    assert!(m.contains("running = use-existing"), "{m}");

    let bad = ScheduleSpec {
        workflow: "job".into(),
        cron: Some("not cron".into()),
        ..Default::default()
    };
    let err = engine.schedule("nightly", bad).await.err().unwrap();
    assert_eq!(
        err,
        Error::Rejected(
            "schedule 'nightly': a cron expression has five fields, 'not cron' has 2".into()
        )
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn registering_a_type_twice_names_both_places() {
    let engine = Engine::open(memory()).await.unwrap();
    engine.register("twice", |_c: Ctx, _: ()| async move { Ok(()) });
    let e2 = engine.clone();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        e2.register("twice", |_c: Ctx, _: ()| async move { Ok(()) });
    }))
    .unwrap_err();
    let m = panic.downcast_ref::<String>().unwrap();
    assert!(
        m.starts_with("workflow type 'twice' (version 1) is registered twice"),
        "{m}"
    );
    assert_eq!(m.matches("tests/probes.rs:").count(), 2, "{m}");
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_bad_directory_is_named() {
    let err = Engine::open(Config::local("/dev/null/fusee"))
        .await
        .err()
        .unwrap();
    assert!(err.to_string().contains("/dev/null/fusee"), "{err}");
}

/// Finding 10: a failed run keeps its steps without `keep_history`; a
/// successful one keeps its start and result.
#[tokio::test]
async fn a_failed_run_keeps_its_journal() {
    let engine = Engine::open(memory().park_after(Duration::ZERO))
        .await
        .unwrap();
    let wf = engine.register("hist", |ctx: Ctx, fail: bool| async move {
        ctx.step("one", || async { Ok::<_, Error>(1) }).await?;
        ctx.step("two", || async move {
            if fail { Err(Failure::new("no")) } else { Ok(2) }
        })
        .options(fast(1))
        .await?;
        Ok(())
    });
    engine
        .start(&wf, "h-ok", &false)
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    engine
        .start(&wf, "h-bad", &true)
        .await
        .unwrap()
        .result()
        .await
        .unwrap_err();
    // Finished runs move to the run store when they park.
    let steps = |id: &'static str| {
        let engine = engine.clone();
        async move {
            for _ in 0..400 {
                if let Some(d) = engine.describe(id).await.unwrap()
                    && d.summary.finished_ms.is_some()
                {
                    let n = d
                        .journal
                        .iter()
                        .filter(|r| matches!(r, fusee::core::record::Record::Step { .. }))
                        .count();
                    return n;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            panic!("{id} did not finish");
        }
    };
    // The successful run was parked to its start and result: the check
    // below sees the run store, not the run in memory.
    assert_eq!(steps("h-ok").await, 0);
    assert_eq!(steps("h-bad").await, 2);
    engine.shutdown().await.unwrap();
}

/// The dev profile's strict replay: a choice outside a step that differs on
/// replay is found with the step names, without waiting for a crash.
#[tokio::test]
async fn the_dev_profile_replays_runs_against_their_code() {
    let engine = Engine::open(memory().dev().http("off")).await.unwrap();
    assert!(engine.is_dev() && engine.http_url().is_none());
    let flips = Arc::new(AtomicU32::new(0));
    let f = flips.clone();
    let wf = engine.register("pick", move |ctx: Ctx, _: ()| {
        // Wrong on purpose: state outside a step.
        let n = f.fetch_add(1, Ordering::SeqCst);
        async move {
            let name = if n.is_multiple_of(2) {
                "first"
            } else {
                "second"
            };
            ctx.step(name, || async { Ok::<_, Error>(1) }).await?;
            Ok(name.to_owned())
        }
    });
    let ok = engine.register("steady", |ctx: Ctx, _: ()| async move {
        ctx.step("one", || async { Ok::<_, Error>(1) }).await?;
        Ok(())
    });
    assert_eq!(
        engine
            .start(&wf, "p-1", &())
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        "first"
    );
    engine
        .start(&ok, "s-1", &())
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    let found = engine.replay_check().await.unwrap();
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].workflow_id, "p-1");
    let e = found[0].error.as_deref().unwrap();
    assert!(e.contains("'first'") && e.contains("'second'"), "{e}");
    engine.shutdown().await.unwrap();
}

/// Under the dev profile a stuck run's result fails at once instead of
/// waiting for code that matches its journal.
#[tokio::test]
async fn the_dev_profile_fails_the_result_of_a_stuck_run() {
    let engine = Engine::open(memory().dev().http("off")).await.unwrap();
    let wf = engine.register("keyed", |ctx: Ctx, _: ()| async move {
        let _ = ctx.key();
        ctx.wait_for::<()>("never").await?;
        Ok(())
    });
    let run = engine.start(&wf, "k-1", &()).await.unwrap();
    let r = tokio::time::timeout(Duration::from_secs(5), run.result()).await;
    match r {
        Ok(Err(Error::Determinism(m))) => assert_eq!(m, fusee::runtime::KEY_OUTSIDE_STEP),
        other => panic!("expected a determinism error, got {other:?}"),
    }
    engine.shutdown().await.unwrap();
}

/// `Config::http` serves the API and UI from the engine itself, and closing
/// the engine stops it.
#[tokio::test]
async fn an_engine_serves_its_own_ui() {
    let engine = Engine::open(memory().http("127.0.0.1:0")).await.unwrap();
    let url = engine.http_url().unwrap();
    let client = fusee::http::HttpClient::new(&url, None).unwrap();
    let info = client
        .call_method("info", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(info["shards"], 1, "{info}");
    drop(client);
    engine.shutdown().await.unwrap();
    drop(engine);
    let addr = url.trim_start_matches("http://").to_owned();
    for _ in 0..100 {
        if std::net::TcpStream::connect(&addr).is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{url} still answers after the engine closed");
}

/// `ctx.log()`: records carry the run and the running step, go through
/// the hook, and are silent while the run replays its journal.
#[tokio::test]
async fn workflow_logs_are_tagged_and_written_once() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let s = seen.clone();
    fusee::log::set_hook(move |r| {
        if r.workflow_id == Some("log-1") {
            s.lock().unwrap().push(fusee::log::text(r));
        }
    });
    let dir = std::env::temp_dir().join(format!("fusee-probe-log-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let open = || async {
        let engine = Engine::open(Config::local(&dir).shards(1)).await.unwrap();
        let wf = engine.register("logged", |ctx: Ctx, _: ()| async move {
            ctx.log().info("started");
            let c = &ctx;
            ctx.step("charge", || async move {
                c.log().info("charging");
                Ok::<_, Error>(1)
            })
            .await?;
            ctx.wait_for::<()>("go").await?;
            ctx.log().info("done");
            Ok(())
        });
        (engine, wf)
    };
    let (engine, wf) = open().await;
    engine.start(&wf, "log-1", &()).await.unwrap();
    status_of(&engine, "log-1", "suspended").await;
    engine.shutdown().await.unwrap();
    drop(engine);
    // Reopened: the signal replays the run's code up to the wait.
    let (engine, _) = open().await;
    engine.run::<()>("log-1").signal("go", &()).await.unwrap();
    engine.run::<()>("log-1").result().await.unwrap();
    engine.shutdown().await.unwrap();
    fusee::log::reset_hook();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        *seen.lock().unwrap(),
        [
            "info  log-1: started",
            "info  log-1 step 'charge': charging",
            "info  log-1: done"
        ]
    );
}

/// `signal_with_start`: starts the run if needed and records the signal
/// before the code first runs; a second call signals the same run.
#[tokio::test]
async fn signal_with_start_starts_once_and_signals() {
    let engine = Engine::open(memory()).await.unwrap();
    const ADD: fusee::Signal<String> = fusee::Signal::new("add");
    let cart = engine.register("cart", |ctx: Ctx, mut items: Vec<String>| async move {
        while items.len() < 2 {
            if let Some(item) = ctx.wait_for(ADD).await? {
                items.push(item);
            }
        }
        Ok(items)
    });
    let run = engine
        .signal_with_start(&cart, "cart-1", &vec![], ADD, &"apple".to_string())
        .await
        .unwrap();
    engine
        .signal_with_start(&cart, "cart-1", &vec![], ADD, &"pear".to_string())
        .await
        .unwrap();
    assert_eq!(run.result().await.unwrap(), ["apple", "pear"]);
    engine.shutdown().await.unwrap();
}

/// `ctx.compensate`: when the workflow fails or is cancelled, registered
/// compensations run as steps, newest first, and the failure is returned.
#[tokio::test]
async fn compensations_run_newest_first_on_failure_and_cancel() {
    let engine = Engine::open(memory()).await.unwrap();
    let undone = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let u = undone.clone();
    let trip = engine.register("trip", move |ctx: Ctx, fail: bool| {
        let u = u.clone();
        async move {
            for what in ["flight", "hotel"] {
                ctx.step(&format!("book {what}"), || async { Ok::<_, Error>(()) })
                    .await?;
                let u = u.clone();
                ctx.compensate(&format!("cancel {what}"), move || {
                    let u = u.clone();
                    async move {
                        u.lock().unwrap().push(what.to_owned());
                        Ok::<_, Error>(())
                    }
                });
            }
            if fail {
                ctx.step("pay", || async {
                    Err::<(), _>(Failure::fatal("card declined"))
                })
                .await?;
            } else {
                ctx.wait_for::<()>("never").await?;
            }
            Ok(())
        }
    });
    let e = engine
        .start(&trip, "t-1", &true)
        .await
        .unwrap()
        .result()
        .await
        .unwrap_err();
    assert!(
        matches!(e, Error::Step { ref name, .. } if name == "pay"),
        "{e:?}"
    );
    assert_eq!(*undone.lock().unwrap(), ["hotel", "flight"]);
    let d = engine.describe("t-1").await.unwrap().unwrap();
    let steps: Vec<String> = d
        .journal
        .iter()
        .filter_map(|r| match r {
            fusee::core::record::Record::Step { name, .. } => Some(name.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(
        steps,
        [
            "book flight",
            "book hotel",
            "pay",
            "cancel hotel",
            "cancel flight"
        ]
    );

    undone.lock().unwrap().clear();
    let run = engine.start(&trip, "t-2", &false).await.unwrap();
    status_of(&engine, "t-2", "suspended").await;
    run.cancel("changed plans").await.unwrap();
    assert!(matches!(run.result().await, Err(Error::Cancelled(_))));
    assert_eq!(*undone.lock().unwrap(), ["hotel", "flight"]);
    engine.shutdown().await.unwrap();
}

/// A delayed start: a durable timer, then the run under the caller's ID;
/// `result()` waits through the delay; an unknown type fails at once.
#[tokio::test]
async fn a_delayed_start_runs_later_under_its_id() {
    let engine = Engine::open(memory()).await.unwrap();
    let wf = engine.register("remind", |ctx: Ctx, who: String| async move {
        let at = ctx.now_ms()?;
        Ok((format!("hi {who}"), at))
    });
    let began = fusee::local::unix_ms();
    let run = engine
        .start(&wf, "remind-1", &"ana".to_string())
        .delay(Duration::from_millis(300))
        .await
        .unwrap();
    assert_eq!(run.id(), "remind-1");
    // Not started yet: the coordinator waits.
    let c = engine
        .status("fusee.delay/remind-1")
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(c.status.as_str(), "suspended" | "parked"), "{c:?}");
    let (msg, at) = run.result().await.unwrap();
    assert_eq!(msg, "hi ana");
    assert!(at >= began + 300, "started {}ms after the call", at - began);
    let e = engine
        .start::<String, ()>("remnd", "remind-2", &"x".to_string())
        .delay(Duration::from_secs(60))
        .await
        .unwrap_err();
    assert!(
        matches!(e, Error::UnknownWorkflow(ref m) if m.contains("did you mean 'remind'")),
        "{e}"
    );
    engine.shutdown().await.unwrap();
}

/// Limits by key: each key gets its own permits, of the base limit's size.
#[tokio::test]
async fn limits_by_key_hold_each_key_apart() {
    let engine = Engine::open(memory()).await.unwrap();
    engine
        .limit(
            "tenant",
            fusee::LimitSpec {
                concurrent: 1,
                per_second: 0.0,
            },
        )
        .unwrap();
    let gate = Arc::new(tokio::sync::Notify::new());
    let g = gate.clone();
    let wf = engine.register("sync", move |ctx: Ctx, tenant: String| {
        let g = g.clone();
        async move {
            ctx.step("call", || {
                let g = g.clone();
                async move {
                    g.notified().await;
                    Ok::<_, Error>(())
                }
            })
            .limit_by("tenant", &tenant)
            .await?;
            Ok(())
        }
    });
    for (id, t) in [("a-1", "acme"), ("a-2", "acme"), ("b-1", "bolt")] {
        engine.start(&wf, id, &t.to_string()).await.unwrap();
    }
    // acme holds one permit and queues one; bolt holds its own.
    for _ in 0..400 {
        if engine.limit_in_use("tenant/acme") == 1 && engine.limit_in_use("tenant/bolt") == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(engine.limit_in_use("tenant/acme"), 1);
    assert_eq!(engine.limit_waiting("tenant/acme"), 1);
    assert_eq!(engine.limit_in_use("tenant/bolt"), 1);
    let names: Vec<String> = engine.limits().into_iter().map(|l| l.name).collect();
    assert_eq!(names, ["tenant", "tenant/acme", "tenant/bolt"]);
    for _ in 0..3 {
        gate.notify_waiters();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for id in ["a-1", "a-2", "b-1"] {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                gate.notify_waiters();
                if let Ok(r) =
                    tokio::time::timeout(Duration::from_millis(20), engine.run::<()>(id).result())
                        .await
                {
                    return r;
                }
            }
        })
        .await
        .unwrap()
        .unwrap();
    }
    engine.shutdown().await.unwrap();
}

/// A start's trace context is recorded in its `Start`, read back by the
/// code, joined by its children, and kept across continue-as-new.
#[tokio::test]
async fn the_trace_context_travels_with_the_run() {
    const TP: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let engine = Engine::open(Config::memory().keep_history(true))
        .await
        .unwrap();
    engine.register("kid", |ctx: Ctx, _: ()| async move {
        Ok(ctx.traceparent().map(str::to_owned))
    });
    engine.register("parent", |ctx: Ctx, _: ()| async move {
        let mine = ctx.traceparent().map(str::to_owned);
        let kid: Option<String> = ctx
            .child("kid", &format!("{}/kid", ctx.workflow_id()), &())
            .await?;
        Ok((mine, kid))
    });
    let run = engine.start("parent", "t-1", &()).trace(TP).await.unwrap();
    let (mine, kid): (Option<String>, Option<String>) = run.result().await.unwrap();
    assert_eq!(mine.as_deref(), Some(TP));
    assert_eq!(
        kid.as_deref(),
        Some(TP),
        "the child joins its parent's trace"
    );
    let d = engine.describe("t-1").await.unwrap().unwrap();
    let start = fusee::journal::normalize_record(&d.journal[0]);
    assert_eq!(start["trace"], TP);
    // Not traced: nothing recorded.
    let kid = engine.start("kid", "t-2", &()).await.unwrap();
    let none: Option<String> = kid.result().await.unwrap();
    assert_eq!(none, None);
    let d = engine.describe("t-2").await.unwrap().unwrap();
    assert!(
        fusee::journal::normalize_record(&d.journal[0])
            .get("trace")
            .is_none()
    );
    engine.shutdown().await.unwrap();
}

/// At most `limit` runs of a type at once: the others wait, in order, as
/// `fusee.queued/<id>`, and the turns survive a restart.
#[tokio::test]
async fn runs_at_once_per_type_take_turns() {
    let dir = std::env::temp_dir().join(format!("fusee-probe-turns-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let open = || async {
        let engine = Engine::open(Config::local(&dir).keep_history(true))
            .await
            .unwrap();
        engine.register("job", |ctx: Ctx, n: u32| async move {
            let go: Option<bool> = ctx.wait_for("go").await?;
            Ok(format!("job {n}: {}", go.unwrap_or(false)))
        });
        engine.limit_runs("job", 2);
        engine
    };
    let running = |engine: Engine| async move {
        let rows = engine
            .list_where(
                &serde_json::json!({"type": "job", "status": "suspended"}),
                100,
            )
            .await
            .unwrap();
        let mut ids: Vec<String> = rows.into_iter().map(|r| r.id).collect();
        ids.sort();
        ids
    };
    let engine = open().await;
    for i in 0..5u32 {
        let _: fusee::RunHandle<String> =
            engine.start("job", &format!("job-{i}"), &i).await.unwrap();
    }
    // Two run (and wait for "go"); three wait their turn.
    let mut got = Vec::new();
    for _ in 0..400 {
        got = running(engine.clone()).await;
        if got.len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(got, ["job-0", "job-1"]);
    let queued = engine
        .list_where(&serde_json::json!({"type": "fusee.queued"}), 100)
        .await
        .unwrap();
    assert_eq!(queued.len(), 5, "every start has its slot");
    // A run that has not started yet: its result follows the slot.
    let late = engine.run::<String>("job-4");
    // Restart: the turns are in the coordinator's journal.
    engine.shutdown().await.unwrap();
    drop(late);
    let engine = open().await;
    engine
        .run::<String>("job-0")
        .signal("go", &true)
        .await
        .unwrap();
    assert_eq!(
        engine.run::<String>("job-0").result().await.unwrap(),
        "job 0: true"
    );
    // job-2 takes the freed turn, not job-3 or job-4.
    for _ in 0..400 {
        got = running(engine.clone()).await;
        if got.contains(&"job-2".to_string()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(got, ["job-1", "job-2"]);
    for i in [1u32, 2, 3, 4] {
        let id = format!("job-{i}");
        // Signal each once it runs; its result arrives through the slot.
        for _ in 0..400 {
            if engine.run::<String>(&id).signal("go", &true).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            engine.run::<String>(&id).result().await.unwrap(),
            format!("job {i}: true")
        );
    }
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Under the dev profile a step's record
/// says when the step began and how long it took, and the HTTP API returns
/// them for the UI's timeline; a production step's record carries neither.
#[tokio::test]
async fn the_dev_profile_records_step_times() {
    use fusee::core::record::Record;
    for dev in [true, false] {
        let config = if dev {
            memory().dev().http("127.0.0.1:0")
        } else {
            memory().keep_history(true).http("off")
        };
        let engine = Engine::open(config).await.unwrap();
        let wf = engine.register("timed", |ctx: Ctx, _: ()| async move {
            ctx.step("quick", || async { Ok::<_, Error>(1) }).await?;
            ctx.step("slow", || async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Ok::<_, Error>(2)
            })
            .await?;
            Ok(())
        });
        engine
            .start(&wf, "t-1", &())
            .await
            .unwrap()
            .result()
            .await
            .unwrap();
        let d = engine.describe("t-1").await.unwrap().unwrap();
        let steps: Vec<_> = d
            .journal
            .iter()
            .filter_map(|r| match r {
                Record::Step { name, time, .. } => Some((name.to_string(), *time)),
                _ => None,
            })
            .collect();
        assert_eq!(steps.len(), 2, "{steps:?}");
        if !dev {
            assert!(steps.iter().all(|(_, t)| t.is_none()), "{steps:?}");
            engine.shutdown().await.unwrap();
            continue;
        }
        let (quick, slow) = (steps[0].1.unwrap(), steps[1].1.unwrap());
        let started = d.summary.started_ms;
        let finished = d.summary.finished_ms.unwrap();
        assert!(
            quick.began_ms >= started && quick.began_ms <= slow.began_ms,
            "{steps:?}"
        );
        assert!(slow.took_ms >= 29, "{steps:?}");
        assert!(
            slow.began_ms + slow.took_ms <= finished + 1,
            "{steps:?} {finished}"
        );
        // The API returns them apart from the journal, which has no times.
        let client = fusee::http::HttpClient::new(&engine.http_url().unwrap(), None).unwrap();
        let out = client
            .call_method("describe", serde_json::json!({"id": "t-1"}))
            .await
            .unwrap();
        let times = out["stepTimes"].as_object().expect("stepTimes");
        assert_eq!(times.len(), 2, "{out}");
        assert!(
            times.values().any(|t| t[1].as_u64().unwrap() >= 29),
            "{out}"
        );
        assert!(!out["journal"].to_string().contains("took"), "{out}");
        drop(client);
        engine.shutdown().await.unwrap();
    }
}

/// `Duration::MAX` means "forever" for a sleep, a wait's timeout, a
/// workflow's timeout and a step's: each deadline saturates. Added to the
/// clock without a check it wrapped to a millisecond in the past, so the
/// sleep returned at once, the wait and the workflow timed out at once
/// (and a debug build panicked on the overflow).
#[tokio::test]
async fn forever_is_not_now() {
    let engine = Engine::open(memory()).await.unwrap();
    let sleeper = engine.register("sleeper", |ctx: Ctx, _: ()| async move {
        ctx.sleep("forever", Duration::MAX).await?;
        Ok("woke".to_string())
    });
    let waiter = engine.register("waiter", |ctx: Ctx, _: ()| async move {
        let step: u32 = ctx
            .step("quick", || async { Ok::<_, Error>(7u32) })
            .timeout(Duration::MAX)
            .await?;
        let got: Option<u32> = ctx.wait_for("go").timeout(Duration::MAX).await?;
        Ok(got.map(|g| g + step))
    });
    engine.start(&sleeper, "s-1", &()).await.unwrap();
    let run = engine
        .start(&waiter, "w-1", &())
        .timeout(Duration::MAX)
        .await
        .unwrap();
    status_of(&engine, "s-1", "suspended").await;
    status_of(&engine, "w-1", "suspended").await;
    // Long enough for a deadline in the past to have fired.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        engine.status("s-1").await.unwrap().unwrap().status,
        "suspended"
    );
    assert_eq!(
        engine.status("w-1").await.unwrap().unwrap().status,
        "suspended"
    );
    engine
        .run::<Option<u32>>("w-1")
        .signal("go", &35u32)
        .await
        .unwrap();
    assert_eq!(run.result().await.unwrap(), Some(42));
    engine.shutdown().await.unwrap();
}
