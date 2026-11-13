//! The test kit (`--features testing`): virtual time, mocks, crashes and
//! crash_everywhere.

use fusee::testing::{Case, TestEngine, crash_everywhere};
use fusee::{Ctx, Error, Failure, RetryPolicy, Signal, WorkflowRef};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const RENEW: WorkflowRef<String, String> = WorkflowRef::new("renew");
const APPROVE: Signal<bool> = Signal::new("approve");

async fn renew(ctx: Ctx, plan: String) -> fusee::Result<String> {
    let quote: u32 = ctx.step("quote", || async { Ok::<_, Error>(42) }).await?;
    ctx.sleep("a day", Duration::from_secs(86_400)).await?;
    let charge: String = ctx
        .step("charge", || async { Ok::<_, Error>("real".to_string()) })
        .await?;
    Ok(format!("{plan}:{quote}:{charge}"))
}

/// A workflow that sleeps a day is tested in under a second.
#[tokio::test]
async fn a_day_long_sleep_takes_no_time() {
    let t = TestEngine::open().await.unwrap();
    t.register(RENEW, renew);
    t.mock_step(RENEW, "charge", |_| Ok(json!("ch_1")));
    let began = Instant::now();
    let before = t.now_ms();
    t.engine()
        .start(RENEW, "r-1", &"pro".to_string())
        .await
        .unwrap();
    let out: String = t.result("r-1").await.unwrap();
    assert_eq!(out, "pro:42:ch_1");
    assert!(
        began.elapsed() < Duration::from_secs(1),
        "{:?}",
        began.elapsed()
    );
    assert!(t.now_ms() - before >= 86_400_000);
    assert_eq!(t.steps("r-1").await.unwrap(), ["quote", "charge"]);
    t.check().await.unwrap();
    let snap = t.snapshot("r-1").await.unwrap();
    assert!(snap.to_string().contains("\"charge\""), "{snap}");
    t.close().await.unwrap();
}

/// `advance` fires a wait's timeout.
#[tokio::test]
async fn advance_fires_timeouts() {
    let t = TestEngine::open().await.unwrap();
    let wf = t.register("approval", |ctx: Ctx, _: ()| async move {
        let ok = ctx
            .wait_for(APPROVE)
            .timeout(Duration::from_secs(3600))
            .await?;
        Ok(ok.is_none())
    });
    t.engine().start(&wf, "a-1", &()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    t.advance(Duration::from_secs(7200)).await;
    assert!(t.engine().run::<bool>("a-1").result().await.unwrap());
    t.close().await.unwrap();
}

/// A mock can fail for some attempts; retries wait in virtual time.
#[tokio::test]
async fn mocks_fail_then_succeed() {
    let t = TestEngine::open().await.unwrap();
    let wf = t.register("flaky", |ctx: Ctx, _: ()| async move {
        let v: String = ctx
            .step("call", || async { Ok::<_, Error>("real".to_string()) })
            .retry(RetryPolicy::exponential(
                3,
                Duration::from_secs(60),
                Duration::from_secs(600),
            ))
            .await?;
        Ok(v)
    });
    t.mock_step("flaky", "call", |attempt| {
        if attempt < 3 {
            Err(Failure::new("unavailable"))
        } else {
            Ok(json!("third time"))
        }
    });
    t.engine().start(&wf, "f-1", &()).await.unwrap();
    let began = Instant::now();
    assert_eq!(t.result::<String>("f-1").await.unwrap(), "third time");
    assert!(began.elapsed() < Duration::from_secs(1));
    t.close().await.unwrap();
}

/// After a crash the run continues from its journal: a recorded step does
/// not run again.
#[tokio::test]
async fn a_crash_recovers_from_the_journal() {
    let t = TestEngine::open().await.unwrap();
    let ran = Arc::new(AtomicU32::new(0));
    let r = ran.clone();
    let wf = t.register("crashy", move |ctx: Ctx, _: ()| {
        let r = r.clone();
        async move {
            ctx.step("once", || async {
                r.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(1)
            })
            .await?;
            let ok = ctx.wait_for(APPROVE).await?;
            Ok(ok)
        }
    });
    t.engine().start(&wf, "c-1", &()).await.unwrap();
    for _ in 0..200 {
        if t.steps("c-1").await.unwrap().len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    t.crash().await.unwrap();
    t.engine()
        .run::<bool>("c-1")
        .signal(APPROVE, &true)
        .await
        .unwrap();
    assert!(t.result::<bool>("c-1").await.unwrap());
    assert_eq!(ran.load(Ordering::SeqCst), 1);
    t.close().await.unwrap();
}

/// A fake payment service: with a key, a repeated call is the same charge.
#[derive(Default)]
struct Payments {
    charges: Mutex<Vec<String>>,
    emails: Mutex<u32>,
}

impl Payments {
    fn charge(&self, key: &str) {
        let mut c = self.charges.lock().unwrap();
        if !c.iter().any(|k| k == key) {
            c.push(key.to_owned());
        }
    }

    fn effects(&self) -> Value {
        json!({"charges": self.charges.lock().unwrap().len(), "emails": *self.emails.lock().unwrap()})
    }
}

fn case(t: &TestEngine, planted: bool) -> Case {
    let pay = Arc::new(Payments::default());
    let p = pay.clone();
    t.register("order", move |ctx: Ctx, cents: u32| {
        let p = p.clone();
        async move {
            let (c, pay) = (&ctx, &p);
            ctx.step("charge", || async move {
                pay.charge(&c.key());
                Ok::<_, Error>(cents)
            })
            .await?;
            ctx.step("email", || async move {
                if planted {
                    // Not idempotent: a second run sends a second email.
                    *pay.emails.lock().unwrap() += 1;
                } else {
                    *pay.emails.lock().unwrap() = 1;
                }
                Ok::<_, Error>(())
            })
            .await?;
            Ok(cents)
        }
    });
    Case {
        workflow: "order".into(),
        id: "o-1".into(),
        input: json!(500),
        effects: Box::new(move || pay.effects()),
    }
}

/// crash_everywhere finds a planted non-idempotent step.
#[tokio::test]
async fn crash_everywhere_finds_a_step_that_is_not_idempotent() {
    crash_everywhere(|t| async move { Ok(case(&t, false)) })
        .await
        .unwrap();
    let e = crash_everywhere(|t| async move { Ok(case(&t, true)) })
        .await
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("step 'email'") || e.contains("step 'charge'"),
        "{e}"
    );
    assert!(e.contains("\"emails\":2"), "{e}");
}
