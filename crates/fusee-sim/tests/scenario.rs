//! User scenarios under the simulator.

use fusee::{Ctx, Error};
use fusee_sim::scenario::Scenario;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A payment workflow that sleeps a day, charges once per run (by its
/// idempotency key) and waits for a receipt signal.
fn payments(sim: &mut fusee_sim::scenario::Setup, n: u64) -> Arc<Mutex<BTreeMap<String, u64>>> {
    let ledger = Arc::new(Mutex::new(BTreeMap::<String, u64>::new()));
    let l = ledger.clone();
    sim.register("pay", move |ctx: Ctx, amount: u64| {
        let l = l.clone();
        async move {
            ctx.sleep("a-day", Duration::from_secs(86_400)).await?;
            let charged = ctx
                .step("charge", || {
                    let l = l.clone();
                    let key = ctx.key();
                    async move {
                        fusee_sim::sleep(Duration::from_millis(2)).await;
                        let mut l = l.lock().map_err(|_| Error::Failed("poisoned".into()))?;
                        // The payment provider deduplicates by key.
                        let v = *l.entry(key).or_insert(amount);
                        Ok::<_, Error>(v)
                    }
                })
                .await?;
            let receipt: Option<u64> = ctx.wait_for("receipt").await?;
            Ok(charged + receipt.unwrap_or(0))
        }
    });
    for i in 0..n {
        let at = Duration::from_millis(sim.random(400));
        let id = format!("pay-{i}");
        sim.start(at, &id, "pay", &(i * 10));
        let later = at + Duration::from_millis(sim.random(300));
        sim.signal(later, &id, "receipt", &1u64);
    }
    ledger
}

#[test]
fn a_user_scenario_survives_crashes_and_skips_time() {
    let scenario = Scenario::new(|sim| {
        let ledger = payments(sim, 12);
        sim.check(move |out| {
            for i in 0..12u64 {
                let id = format!("pay-{i}");
                if out.result::<u64>(&id) != Some(Ok(i * 10 + 1)) {
                    return Err(format!("{id}: {:?}", out.result::<u64>(&id)));
                }
            }
            let l = ledger.lock().unwrap();
            if l.len() != 12 {
                return Err(format!("{} charges for 12 payments", l.len()));
            }
            Ok(())
        });
    })
    .crash_every(Duration::from_millis(150))
    .settle_within(Duration::from_secs(2 * 86_400));
    let wall = std::time::Instant::now();
    let s = scenario.run_seeds(0..20).unwrap_or_else(|f| panic!("{f}"));
    assert_eq!(s.seeds, 20);
    assert!(s.crashes > 20, "crashes: {}", s.crashes);
    // A day of sleeping per workflow, in simulated time only.
    assert!(s.simulated_ms >= 20 * 86_400_000);
    assert!(wall.elapsed() < Duration::from_secs(120));
}

#[test]
fn nondeterministic_workflow_code_is_caught_on_replay() {
    // The step name depends on how often the code ran: a replay after a
    // crash or a park takes a different path than the journal recorded.
    let scenario = Scenario::new(|sim| {
        let executions = Arc::new(AtomicU64::new(0));
        sim.register("flaky", move |ctx: Ctx, _: ()| {
            let n = executions.fetch_add(1, Ordering::SeqCst);
            async move {
                let name = format!("step-{n}");
                ctx.step(&name, || async { Ok::<_, Error>(1u32) }).await?;
                ctx.sleep("pause", Duration::from_millis(500)).await?;
                ctx.step("after", || async { Ok::<_, Error>(2u32) }).await
            }
        });
        for i in 0..10 {
            sim.start(
                Duration::from_millis(i * 7),
                &format!("f-{i}"),
                "flaky",
                &(),
            );
        }
    })
    .shards(1)
    .crash_every(Duration::from_millis(40))
    .flush_failures_per_mille(0);
    let failure = scenario
        .run_seeds(0..40)
        .expect_err("replaying nondeterministic code must fail some seed");
    assert!(failure.scenario);
    assert!(
        failure.message.contains("determinism"),
        "{}",
        failure.message
    );
    // The failing seed reproduces.
    let again = scenario.run(failure.seed).expect_err("the seed reproduces");
    assert_eq!(again.message, failure.message);
    assert!(
        failure
            .to_string()
            .contains(&format!("scenario.run({})", failure.seed))
    );
}

#[test]
fn a_failing_check_names_the_seed() {
    let scenario = Scenario::new(|sim| {
        sim.register("double", |_ctx: Ctx, n: u32| async move {
            Ok::<_, Error>(n * 2)
        });
        sim.start(Duration::ZERO, "d", "double", &21u32);
        sim.check(|out| {
            assert_eq!(out.status("d"), Some("ok"));
            assert_eq!(out.result::<u32>("d"), Some(Ok(42)));
            Err("the user's invariant does not hold".into())
        });
    })
    .no_faults();
    let f = scenario.run(7).expect_err("the check fails");
    assert_eq!(f.seed, 7);
    assert!(
        f.message.contains("the user's invariant does not hold"),
        "{}",
        f.message
    );

    let panicking = Scenario::new(|sim| {
        sim.register("double", |_ctx: Ctx, n: u32| async move {
            Ok::<_, Error>(n * 2)
        });
        sim.start(Duration::ZERO, "d", "double", &21u32);
        sim.check(|out| {
            assert_eq!(out.result::<u32>("d"), Some(Ok(41)), "wrong total");
            Ok(())
        });
    })
    .no_faults();
    let f = panicking
        .run(3)
        .expect_err("a panicking check fails the seed");
    assert!(f.message.contains("wrong total"), "{}", f.message);
}

#[test]
fn cancels_and_failures_reach_the_outcome() {
    let scenario = Scenario::new(|sim| {
        sim.register("wait", |ctx: Ctx, _: ()| async move {
            let v: Option<u32> = ctx.wait_for("never").await?;
            Ok::<_, Error>(v.unwrap_or(0))
        });
        sim.register("boom", |ctx: Ctx, _: ()| async move {
            ctx.step("fail", || async {
                Err::<u32, _>(Error::Failed("card declined".into()))
            })
            .await
        });
        sim.start(Duration::ZERO, "w", "wait", &());
        sim.cancel(Duration::from_millis(30), "w");
        sim.start(Duration::ZERO, "b", "boom", &());
        sim.check(|out| {
            assert_eq!(out.status("w"), Some("cancelled"));
            assert_eq!(out.status("b"), Some("failed"));
            let e = out.result::<u32>("b").unwrap().unwrap_err();
            assert!(e.contains("card declined"), "{e}");
            Ok(())
        });
    })
    .crash_every(Duration::from_millis(100));
    scenario.run_seeds(0..30).unwrap_or_else(|f| panic!("{f}"));
}

#[test]
fn debounced_starts_survive_crashes_with_one_run_per_window() {
    let scenario = Scenario::new(|sim| {
        sim.register("reindex", |ctx: Ctx, n: u64| async move {
            let v: u64 = ctx
                .step("index", || async move { Ok::<_, Error>(n) })
                .await?;
            Ok::<_, Error>(v)
        });
        let rule =
            fusee::Debounce::new(Duration::from_millis(40)).max_wait(Duration::from_millis(150));
        // Four documents, each saved in bursts with quiet gaps between.
        let mut sent = Vec::new();
        for doc in 0..4u64 {
            let mut at = sim.random(100);
            for i in 0..10u64 {
                at += if sim.random(4) == 0 {
                    60 + sim.random(80)
                } else {
                    sim.random(20)
                };
                let id = format!("doc-{doc}/save-{i}");
                let input = doc * 100 + i;
                sim.debounce(
                    Duration::from_millis(at),
                    "reindex",
                    &format!("doc-{doc}"),
                    &id,
                    &input,
                    &rule,
                );
                sent.push((id, input));
            }
        }
        sim.check(move |out| {
            // Every acknowledged save names a run, and that run took the
            // input of one of its own window's saves.
            let mut windows: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
            for (id, input) in &sent {
                let run = out.receipt(id).ok_or(format!("{id} has no receipt"))?;
                windows.entry(run).or_default().push(*input);
            }
            for (run, inputs) in &windows {
                let got = out
                    .result::<u64>(run)
                    .ok_or(format!("window run {run} never ran"))??;
                if !inputs.contains(&got) {
                    return Err(format!("{run} ran with {got}, not one of {inputs:?}"));
                }
                let doc = got / 100;
                if inputs.iter().any(|i| i / 100 != doc) || !run.starts_with(&format!("doc-{doc}@"))
                {
                    return Err(format!("{run} mixes keys: {inputs:?}"));
                }
            }
            if windows.len() < 4 || windows.len() == sent.len() {
                return Err(format!(
                    "{} windows for {} saves",
                    windows.len(),
                    sent.len()
                ));
            }
            Ok(())
        });
    })
    .crash_every(Duration::from_millis(120));
    let s = scenario.run_seeds(0..60).unwrap_or_else(|f| panic!("{f}"));
    assert!(s.crashes > 40, "crashes: {}", s.crashes);
}

#[test]
fn batched_events_survive_crashes_each_in_exactly_one_run() {
    let scenario = Scenario::new(|sim| {
        sim.register("bulk", |ctx: Ctx, events: Vec<u64>| async move {
            let events: Vec<u64> = ctx
                .step("load", move || {
                    let events = events.clone();
                    async move { Ok::<_, Error>(events) }
                })
                .await?;
            Ok::<_, Error>(events)
        });
        let rule = fusee::Batch::new(4, Duration::from_millis(60));
        let mut sent = Vec::new();
        for tenant in 0..3u64 {
            for i in 0..14u64 {
                let at = Duration::from_millis(sim.random(1_200));
                let id = format!("t{tenant}/e{i}");
                let event = tenant * 100 + i;
                sim.batch(at, "bulk", &format!("tenant-{tenant}"), &id, &event, &rule);
                sent.push((id, event));
            }
        }
        sim.check(move |out| {
            let mut runs: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
            for (id, _) in &sent {
                let run = out.receipt(id).ok_or(format!("{id} has no receipt"))?;
                if !runs.contains_key(run) {
                    let got = out
                        .result::<Vec<u64>>(run)
                        .ok_or(format!("batch run {run} never ran"))??;
                    runs.insert(run, got);
                }
            }
            // Each event is in its receipt's run, once, and in no other.
            let mut seen = BTreeMap::new();
            for (run, events) in &runs {
                if events.is_empty() || events.len() > 4 {
                    return Err(format!("{run} has {} events", events.len()));
                }
                for e in events {
                    if let Some(other) = seen.insert(*e, *run) {
                        return Err(format!("event {e} is in {other} and {run}"));
                    }
                }
            }
            for (id, event) in &sent {
                if seen.get(event) != out.receipt(id).as_ref() {
                    return Err(format!(
                        "{id} ({event}) joined {:?} but ran in {:?}",
                        out.receipt(id),
                        seen.get(event)
                    ));
                }
            }
            Ok(())
        });
    })
    .crash_every(Duration::from_millis(120));
    let s = scenario.run_seeds(0..60).unwrap_or_else(|f| panic!("{f}"));
    assert!(s.crashes > 40, "crashes: {}", s.crashes);
}

/// A model call that streams its tokens, read by clients that reconnect
/// after every crash: they see each token once, and the stream holds what
/// the step returned (an attempt that runs again continues after the
/// recorded prefix instead of starting over).
#[test]
fn streamed_tokens_survive_crashes_without_repeats() {
    let scenario = Scenario::new(|sim| {
        let calls = Arc::new(AtomicU64::new(0));
        let c2 = calls.clone();
        sim.register("answer", move |ctx: Ctx, n: u64| {
            let calls = c2.clone();
            async move {
                let c = ctx.clone();
                let tokens: Vec<u64> = ctx
                    .step("generate", move || {
                        let (c, calls) = (c.clone(), calls.clone());
                        async move {
                            let out = c.stream("tokens")?;
                            let mut toks: Vec<u64> = out.recorded()?;
                            for _ in toks.len() as u64..n {
                                fusee_sim::sleep(Duration::from_millis(1)).await;
                                // A new value on every call, as sampling gives.
                                let t = calls.fetch_add(1, Ordering::SeqCst);
                                out.append(&t)?;
                                toks.push(t);
                            }
                            Ok::<_, Error>(toks)
                        }
                    })
                    .await?;
                Ok(tokens)
            }
        });
        for i in 0..6u64 {
            let at = Duration::from_millis(sim.random(300));
            let id = format!("q-{i}");
            sim.start(at, &id, "answer", &(5 + i));
            let read_at = at + Duration::from_millis(sim.random(20));
            sim.read_stream(read_at, &id, "tokens");
        }
        sim.check(move |out| {
            for i in 0..6u64 {
                let id = format!("q-{i}");
                let Some(Ok(tokens)) = out.result::<Vec<u64>>(&id) else {
                    return Err(format!("{id}: {:?}", out.result::<Vec<u64>>(&id)));
                };
                let seen: Vec<u64> = out
                    .stream(&id, "tokens")
                    .unwrap_or_default()
                    .iter()
                    .map(|v| v.as_u64().unwrap_or(u64::MAX))
                    .collect();
                if seen != tokens || out.stream_end(&id, "tokens") != Some("ok") {
                    return Err(format!(
                        "{id}: readers saw {seen:?}, the step returned {tokens:?}"
                    ));
                }
            }
            Ok(())
        });
    })
    .crash_every(Duration::from_millis(100));
    let s = scenario.run_seeds(0..60).unwrap_or_else(|f| panic!("{f}"));
    assert!(s.crashes > 40, "crashes: {}", s.crashes);
}
