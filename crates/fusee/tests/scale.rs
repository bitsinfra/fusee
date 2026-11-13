//! Work that must follow activity, not population: parking many suspended
//! runs and waking them all at once.

use fusee::{Config, Ctx, Engine, Error};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mass_wake_of_parked_runs_finishes_every_run() {
    let engine = Engine::open(
        Config::memory()
            .shards(2)
            .park_after(Duration::from_millis(20)),
    )
    .await
    .unwrap();
    let wake_at = fusee::local::unix_ms() + 1_500;
    let wf = engine.register("sleeper", move |ctx: Ctx, n: u32| async move {
        ctx.sleep_until("nap", wake_at).await?;
        ctx.step("after", || async move { Ok::<_, Error>(n) }).await
    });
    const RUNS: u32 = 20_000;
    for i in 0..RUNS {
        engine.start(&wf, &format!("s-{i}"), &i).await.unwrap();
    }
    // Every run is suspended in its sleep: the sweep parks them.
    let until = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let m = engine.metrics().await.unwrap();
        if m.runs_parked >= u64::from(RUNS) || fusee::local::unix_ms() >= wake_at {
            break;
        }
        assert!(std::time::Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let parked = engine.metrics().await.unwrap().runs_parked;
    assert!(parked > 0, "nothing was parked before the wake");
    // All timers come due together; every run loads, continues and ends.
    for i in (0..RUNS).step_by(97) {
        let r = engine.run::<u32>(&format!("s-{i}"));
        assert_eq!(r.result().await.unwrap(), i);
    }
    let until = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let m = engine.metrics().await.unwrap();
        if m.runs_finished >= u64::from(RUNS) {
            break;
        }
        assert!(std::time::Instant::now() < until, "{m:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    engine.shutdown().await.unwrap();
}
