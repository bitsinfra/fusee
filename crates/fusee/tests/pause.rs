//! A step whose retries run out pauses its run instead of failing it, and a
//! resume runs it again with a fresh budget.

use fusee::{Config, Ctx, Engine, Error, Exhausted, Failure, RetryPolicy, StepOptions};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

fn tmp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-pause-{name}-{}-{}",
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

/// A charge that fails while `broken` is set; `calls` counts attempts.
#[derive(Clone)]
struct Service {
    broken: Arc<AtomicBool>,
    calls: Arc<AtomicU32>,
}

impl Service {
    fn new() -> Self {
        Self {
            broken: Arc::new(AtomicBool::new(true)),
            calls: Arc::new(AtomicU32::new(0)),
        }
    }

    async fn charge(&self, n: u32) -> Result<u32, Failure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.broken.load(Ordering::SeqCst) {
            Err(Failure::new("payments are down"))
        } else {
            Ok(n * 10)
        }
    }
}

fn pausing() -> StepOptions {
    StepOptions::default().retry(
        RetryPolicy::exponential(2, Duration::from_millis(1), Duration::from_millis(1))
            .on_exhausted(Exhausted::Pause),
    )
}

fn register(engine: &Engine, svc: &Service) -> fusee::WorkflowRef<u32, u32> {
    let svc = svc.clone();
    engine.register("pay", move |ctx: Ctx, n: u32| {
        let svc = svc.clone();
        async move {
            let charged: u32 = ctx
                .step("charge", || {
                    let svc = svc.clone();
                    async move { svc.charge(n).await }
                })
                .options(pausing())
                .await?;
            Ok(charged + 1)
        }
    })
}

async fn status(engine: &Engine, id: &str) -> String {
    engine
        .status(id)
        .await
        .unwrap()
        .map(|s| s.status)
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_child_keeps_its_parent_waiting_until_resumed() {
    let dir = tmp("child");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let svc = Service::new();
    let pay = register(&engine, &svc);
    let order = engine.register("order", move |ctx: Ctx, n: u32| {
        let pay = pay.clone();
        async move {
            let paid: u32 = ctx.child(pay, "pay-1", &n).await?;
            Ok(paid)
        }
    });
    let run = engine.start(&order, "order-1", &4).await.unwrap();
    eventually("the pause", async || {
        status(&engine, "pay-1").await == "paused"
    })
    .await;
    assert_eq!(svc.calls.load(Ordering::SeqCst), 2);
    // The parent waits on its child; the child waits for a resume.
    assert!(matches!(
        status(&engine, "order-1").await.as_str(),
        "suspended" | "parked"
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        svc.calls.load(Ordering::SeqCst),
        2,
        "nothing retries while paused"
    );

    svc.broken.store(false, Ordering::SeqCst);
    engine.run::<u32>("pay-1").resume().await.unwrap();
    assert_eq!(run.result().await.unwrap(), 41);
    assert_eq!(svc.calls.load(Ordering::SeqCst), 3);
    let journal = engine.run::<u32>("pay-1").describe().await.unwrap().journal;
    let kinds: Vec<String> = journal
        .iter()
        .map(|r| {
            fusee::journal::normalize_record(r)["kind"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(
        kinds,
        ["start", "attempt", "attempt", "resume", "step", "done"]
    );
    let paused = fusee::journal::normalize_record(&journal[2]);
    assert_eq!(paused["paused"], true, "{paused}");
    // Nothing is paused any more.
    let again = engine.run::<u32>("pay-1").resume().await.unwrap_err();
    assert!(matches!(again, Error::Rejected(_)), "{again:?}");
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_stays_paused_across_a_restart_and_resumes_with_a_fresh_budget() {
    let dir = tmp("restart");
    let svc = Service::new();
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let pay = register(&engine, &svc);
        engine.start(&pay, "pay-2", &1).await.unwrap();
        eventually("the pause", async || {
            status(&engine, "pay-2").await == "paused"
        })
        .await;
        engine.shutdown().await.unwrap();
    }
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    register(&engine, &svc);
    eventually("the paused run", async || {
        status(&engine, "pay-2").await == "paused"
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(svc.calls.load(Ordering::SeqCst), 2);
    // Still broken: the resume gets a fresh budget of two attempts, then
    // pauses again.
    engine.run::<u32>("pay-2").resume().await.unwrap();
    eventually("two more attempts", async || {
        svc.calls.load(Ordering::SeqCst) == 4
    })
    .await;
    eventually("the second pause", async || {
        status(&engine, "pay-2").await == "paused"
    })
    .await;
    svc.broken.store(false, Ordering::SeqCst);
    engine.run::<u32>("pay-2").resume().await.unwrap();
    assert_eq!(engine.run::<u32>("pay-2").result().await.unwrap(), 11);
    assert_eq!(svc.calls.load(Ordering::SeqCst), 5);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parked_paused_run_lists_as_paused_and_resumes() {
    let dir = tmp("parked");
    let engine = Engine::open(cfg(&dir).park_after(Duration::from_millis(20)))
        .await
        .unwrap();
    let svc = Service::new();
    let pay = register(&engine, &svc);
    engine.start(&pay, "pay-3", &2).await.unwrap();
    eventually("the parked pause", async || {
        engine.metrics().await.unwrap().runs_parked > 0
            && status(&engine, "pay-3").await == "paused"
    })
    .await;
    let listed = engine
        .list_where(&serde_json::json!({"status": "paused"}), 10)
        .await
        .unwrap();
    assert_eq!(
        listed.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        ["pay-3"]
    );
    svc.broken.store(false, Ordering::SeqCst);
    engine.run::<u32>("pay-3").resume().await.unwrap();
    assert_eq!(engine.run::<u32>("pay-3").result().await.unwrap(), 21);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_failures_fail_and_the_engine_default_pauses() {
    let dir = tmp("default");
    let engine = Engine::open(cfg(&dir).on_exhausted(Exhausted::Pause))
        .await
        .unwrap();
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    let wf = engine.register("w", move |ctx: Ctx, fatal: bool| {
        let c = c.clone();
        async move {
            let v: u32 = ctx
                .step("s", || {
                    let c = c.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        if fatal {
                            Err(Failure::fatal("card declined"))
                        } else {
                            Err(Failure::new("flaky"))
                        }
                    }
                })
                .retry(RetryPolicy::exponential(
                    2,
                    Duration::from_millis(1),
                    Duration::from_millis(1),
                ))
                .await?;
            Ok(v)
        }
    });
    let fatal = engine.start(&wf, "fatal", &true).await.unwrap();
    assert!(fatal.result().await.is_err());
    assert_eq!(status(&engine, "fatal").await, "failed");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a final failure is not retried"
    );
    engine.start(&wf, "flaky", &false).await.unwrap();
    eventually("the engine's default", async || {
        status(&engine, "flaky").await == "paused"
    })
    .await;
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_and_deadlines_reach_a_paused_run() {
    let dir = tmp("cancel");
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let svc = Service::new();
    let pay = register(&engine, &svc);
    let a = engine.start(&pay, "pay-c", &1).await.unwrap();
    eventually("the pause", async || {
        status(&engine, "pay-c").await == "paused"
    })
    .await;
    a.cancel("not needed").await.unwrap();
    assert!(matches!(a.result().await, Err(Error::Cancelled(_))));
    let b = engine
        .start(&pay, "pay-t", &1)
        .timeout(Duration::from_millis(300))
        .await
        .unwrap();
    assert!(matches!(b.result().await, Err(Error::TimedOut)));
    assert_eq!(status(&engine, "pay-t").await, "timed_out");
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
