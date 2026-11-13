//! Fan-out cost and volatile timers, counted rather than timed.

use fusee::{Config, Ctx, Engine, Error, RetryPolicy, StepOptions};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// Counts how often a combinator polls a branch.
struct Counted<F> {
    polls: Arc<AtomicU64>,
    fut: Pin<Box<F>>,
}

impl<F: Future> Future for Counted<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        self.fut.as_mut().poll(cx)
    }
}

fn counted<F: Future>(polls: &Arc<AtomicU64>, fut: F) -> Counted<F> {
    Counted {
        polls: polls.clone(),
        fut: Box::pin(fut),
    }
}

/// Pending once (waking itself), then ready: a step function that does not
/// finish on its first poll, so its timeout timers are armed.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// The payloads of a journal's step records, in journal order.
fn step_payloads(journal: &Value, name: &str) -> Vec<u64> {
    journal
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["kind"] == "step" && r["name"] == name)
        .map(|r| r["payload"].as_u64().unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fan_out_of_ten_thousand_steps_polls_each_branch_a_bounded_number_of_times() {
    let engine = Engine::open(Config::memory().shards(1).keep_history(true))
        .await
        .unwrap();
    let polls = Arc::new(AtomicU64::new(0));
    let p = polls.clone();
    let wf = engine.register("fan", move |ctx: Ctx, n: u64| {
        let p = p.clone();
        async move {
            let legs = (0..n).map(|i| {
                let ctx = ctx.clone();
                counted(&p, async move {
                    ctx.step("leg", || async move { Ok::<_, Error>(i) }).await
                })
            });
            let mut sum = 0;
            for r in ctx.all(legs).await {
                sum += r?;
            }
            Ok(sum)
        }
    });
    let n = 10_000u64;
    let sum: u64 = engine
        .start(&wf, "f", &n)
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    assert_eq!(sum, n * (n - 1) / 2);
    // Polling every branch on every delivery would be about n²/2 = 5e7.
    let polls = polls.load(Ordering::Relaxed);
    eprintln!("{polls} branch polls for {n} branches");
    assert!(polls <= 5 * n, "{polls} branch polls for {n} branches");
    // One delivery at a time, in the order the steps finished.
    let d = engine.describe("f").await.unwrap().unwrap();
    let journal = fusee::journal::normalize(&d.journal);
    assert_eq!(step_payloads(&journal, "leg"), (0..n).collect::<Vec<_>>());
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_fan_out_and_a_wide_race_stay_linear() {
    let engine = Engine::open(Config::memory().shards(1).keep_history(true))
        .await
        .unwrap();
    let polls = Arc::new(AtomicU64::new(0));
    let p = polls.clone();
    let wf = engine.register("nested", move |ctx: Ctx, n: u64| {
        let p = p.clone();
        async move {
            // n groups of n steps, each group two steps deep.
            let groups = (0..n).map(|g| {
                let (ctx, p) = (ctx.clone(), p.clone());
                let outer = p.clone();
                counted(&outer, async move {
                    let legs = (0..n).map(|i| {
                        let ctx = ctx.clone();
                        counted(&p, async move {
                            let a = ctx
                                .step("a", || async move { Ok::<_, Error>(g * n + i) })
                                .await?;
                            ctx.step("b", || async move { Ok::<_, Error>(a) }).await
                        })
                    });
                    let mut sum = 0;
                    for r in ctx.all(legs).await {
                        sum += r?;
                    }
                    Ok::<_, Error>(sum)
                })
            });
            let mut sum = 0;
            for r in ctx.all(groups).await {
                sum += r?;
            }
            // The first step to finish wins; the rest are dropped.
            let runners = (0..n * n).map(|i| {
                let ctx = ctx.clone();
                counted(&p, async move {
                    ctx.step("runner", || async move { Ok::<_, Error>(i) })
                        .await
                })
            });
            let (winner, v) = ctx.race(runners).await;
            Ok((sum, winner as u64, v?))
        }
    });
    let n = 100u64;
    let (sum, winner, v): (u64, u64, u64) = engine
        .start(&wf, "n", &n)
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    let steps = n * n;
    assert_eq!(sum, steps * (steps - 1) / 2);
    assert_eq!((winner, v), (0, 0));
    // Polling every pending branch on every delivery would be about
    // 2 * steps * (n + n) / 2 = 4e6 for the groups alone.
    let polls = polls.load(Ordering::Relaxed);
    eprintln!("{polls} branch polls for {steps} steps in {n} groups");
    assert!(
        polls <= 20 * (n + steps),
        "{polls} branch polls for {steps} steps"
    );
    let d = engine.describe("n").await.unwrap().unwrap();
    let journal = fusee::journal::normalize(&d.journal);
    let order: Vec<u64> = (0..steps).collect();
    assert_eq!(step_payloads(&journal, "a"), order);
    assert_eq!(step_payloads(&journal, "b"), order);
    assert_eq!(step_payloads(&journal, "runner"), vec![0]);
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finished_steps_leave_no_volatile_timers_behind() {
    let engine = Engine::open(Config::memory().shards(1)).await.unwrap();
    let hour = Duration::from_secs(3600);
    type Release = Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>;
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let release: Release = Arc::new(Mutex::new(Some(rx)));
    let r = release.clone();
    let wf = engine.register("timeouts", move |ctx: Ctx, n: u64| {
        let r = r.clone();
        async move {
            let long = StepOptions::default()
                .timeout(2 * hour)
                .heartbeat_timeout(2 * hour);
            let mut sum = 0;
            // A loop of short steps with long timeouts.
            for i in 0..n {
                let c = ctx.clone();
                sum += ctx
                    .step("seq", || {
                        let c = c.clone();
                        async move {
                            YieldOnce(false).await;
                            c.heartbeat(&i)?;
                            YieldOnce(false).await;
                            Ok::<_, Error>(i)
                        }
                    })
                    .options(long.clone())
                    .await?;
            }
            // A fan-out whose earliest deadline belongs to the last step,
            // which waits for the test: the cancelled timers behind it must
            // not pile up in the heap.
            let legs = (0..=n).map(|i| {
                let (ctx, r, long) = (ctx.clone(), r.clone(), long.clone());
                Box::pin(async move {
                    if i < n {
                        return ctx
                            .step("leg", || async move {
                                YieldOnce(false).await;
                                Ok::<_, Error>(i)
                            })
                            .options(long)
                            .await;
                    }
                    ctx.step("last", || {
                        let rx = r.lock().unwrap().take();
                        async move {
                            if let Some(rx) = rx {
                                let _ = rx.await;
                            }
                            Ok::<_, Error>(0)
                        }
                    })
                    .options(StepOptions::default().timeout(hour))
                    .await
                }) as Pin<Box<dyn Future<Output = Result<u64, Error>>>>
            });
            for r in ctx.all(legs).await {
                sum += r?;
            }
            // A step waiting out an hour of retry backoff loses a race.
            let slow = RetryPolicy::exponential(2, hour, hour);
            let (c1, c2, long) = (ctx.clone(), ctx.clone(), long.clone());
            let (winner, v) = ctx
                .race(vec![
                    Box::pin(async move {
                        c1.step("flaky", || async { Err::<u64, _>("not yet") })
                            .options(StepOptions::default().retry(slow))
                            .await
                    }) as Pin<Box<dyn Future<Output = Result<u64, Error>>>>,
                    Box::pin(async move {
                        c2.step("quick", || async {
                            YieldOnce(false).await;
                            Ok::<_, Error>(7)
                        })
                        .options(long)
                        .await
                    }),
                ])
                .await;
            Ok((sum + v?, winner as u64))
        }
    });
    let n = 2_000u64;
    let run = engine.start(&wf, "t", &n).await.unwrap();

    // Every step but the last has finished: one live timer, and a bounded heap.
    let began = Instant::now();
    let mid = loop {
        let m = engine.metrics().await.unwrap();
        if m.steps_run >= 2 * n {
            break m;
        }
        assert!(began.elapsed() < Duration::from_secs(120), "{m:?}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    eprintln!(
        "{} volatile timers with one step running",
        mid.volatile_timers
    );
    assert!(
        (1..=130).contains(&mid.volatile_timers),
        "{} volatile timers with one step running",
        mid.volatile_timers
    );
    tx.send(()).unwrap();
    let (sum, winner): (u64, u64) = run.result().await.unwrap();
    assert_eq!((sum, winner), (n * (n - 1) + 7, 1));
    assert_eq!(engine.metrics().await.unwrap().volatile_timers, 0);
    engine.shutdown().await.unwrap();
}

/// A fan-out's legs do not depend on each other, so their results reach
/// the log together: a flush for the start, one for the legs, one for the
/// join and the end. A leg's result settled inside the pass that starts
/// the legs would be the run's newest record, and every leg started after
/// it would wait for that record's flush: a flush per leg.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fan_out_takes_a_few_flushes_whatever_its_width() {
    let dir = std::env::temp_dir().join(format!(
        "fusee-fanout-flushes-{}-{}",
        std::process::id(),
        fusee::local::unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let mut config = Config::local(&dir).shards(1);
    // The count is what is tested, not the disk: flushes quick enough
    // that the shard makes them on its own thread, as on NVMe.
    config.local.flush = fusee::FlushMethod::Fsync;
    let engine = Engine::open(config).await.unwrap();
    let wf = engine.register("fan", |ctx: Ctx, n: u32| async move {
        let legs: Vec<_> = (0..n)
            .map(|i| {
                let c = ctx.clone();
                async move { c.step("leg", || async move { Ok::<_, Error>(i) }).await }
            })
            .collect();
        let sum: u32 = ctx
            .all(legs)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .sum();
        ctx.step("join", || async move { Ok::<_, Error>(sum) })
            .await
    });
    let width = 100u32;
    let mut before = 0;
    for run in 0..6 {
        let sum = engine
            .execute(&wf, &format!("f{run}"), &width)
            .await
            .unwrap();
        assert_eq!(sum, width * (width - 1) / 2);
        let flushes = engine.metrics().await.unwrap().flushes;
        // Three where every flush is made at once; a slow disk may split
        // the legs' records over a flush or two more.
        assert!(
            flushes - before <= 6,
            "fan-out {run} of {width} took {} flushes",
            flushes - before
        );
        before = flushes;
    }
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
