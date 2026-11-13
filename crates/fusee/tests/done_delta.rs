//! A run that finishes after it was loaded from its stored frames appends
//! a `Delta` frame with its last records instead of a new summary
//!. Its result, status and journal survive a
//! reopen, whether recovery reads the persisted index or scans the store;
//! the visibility index lists it by its final status, and a reshard keeps
//! it finished.
use fusee::{Config, Ctx, Engine, Error};
use std::path::{Path, PathBuf};
use std::time::Duration;

fn tmp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-done-delta-{name}-{}-{}",
        std::process::id(),
        fusee::local::unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cfg(dir: &Path) -> Config {
    let mut c = Config::local(dir)
        .shards(1)
        .park_after(Duration::from_millis(0));
    c.local.flush = fusee::FlushMethod::Fsync;
    c
}

const RUNS: u32 = 200;

/// Waits for a signal (parking meanwhile), takes a step, ends: the load
/// that delivers the signal is followed by the run's finish.
fn waiter(engine: &Engine, fail_odd: bool) -> fusee::WorkflowRef<u32, u32> {
    engine.register("waiter", move |ctx: Ctx, n: u32| async move {
        let add: u32 = ctx.wait_for("go").await?.unwrap_or(0);
        let v = ctx
            .step("add", move || async move { Ok::<_, Error>(n + add) })
            .await?;
        if fail_odd && n % 2 == 1 {
            return Err(Error::Failed("odd".into()));
        }
        Ok(v)
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runs_finished_after_a_load_keep_their_results_across_a_reopen() {
    let dir = tmp("reopen");
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let wf = waiter(&engine, true);
        for i in 0..RUNS {
            engine.start(&wf, &format!("w-{i}"), &i).await.unwrap();
        }
        // Every run parks at its wait.
        for _ in 0..2000 {
            if engine.metrics().await.unwrap().runs_parked >= u64::from(RUNS) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for i in 0..RUNS {
            engine
                .run::<u32>(&format!("w-{i}"))
                .signal("go", &5u32)
                .await
                .unwrap();
        }
        for i in 0..RUNS {
            let r = engine.run::<u32>(&format!("w-{i}")).result().await;
            if i % 2 == 1 {
                assert!(r.is_err(), "w-{i}");
            } else {
                assert_eq!(r.unwrap(), i + 5, "w-{i}");
            }
        }
        engine.shutdown().await.unwrap();
    }
    for round in 0..2 {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let _wf = waiter(&engine, true);
        check(&engine, &format!("round {round}")).await;
        // The visibility index reads the store's frames: a chain ending
        // with `Done` is listed as finished.
        for status in ["failed", "ok"] {
            let mut n = 0;
            for _ in 0..400 {
                n = engine
                    .list_where(&serde_json::json!({"status": status}), 1000)
                    .await
                    .unwrap()
                    .len();
                if n == RUNS as usize / 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert_eq!(
                n,
                RUNS as usize / 2,
                "round {round}: runs listed as {status}"
            );
        }
        engine.shutdown().await.unwrap();
    }
    // A reshard moves each run by its newest frame: still finished, with
    // its result.
    let mut opts = fusee::reshard::ReshardOptions::default();
    opts.local.flush = fusee::FlushMethod::Fsync;
    fusee::reshard::reshard(&dir, 3, &opts).unwrap();
    let engine = Engine::open(cfg(&dir).shards(3)).await.unwrap();
    let _wf = waiter(&engine, true);
    check(&engine, "after a reshard").await;
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

async fn check(engine: &Engine, when: &str) {
    for i in 0..RUNS {
        let id = format!("w-{i}");
        let r = tokio::time::timeout(Duration::from_secs(10), engine.run::<u32>(&id).result())
            .await
            .unwrap_or_else(|_| panic!("{when}: {id} has no result"));
        if i % 2 == 1 {
            assert!(r.is_err(), "{when}: {id}");
        } else {
            assert_eq!(r.unwrap(), i + 5, "{when}: {id}");
        }
        let s = engine.status(&id).await.unwrap().unwrap();
        assert_ne!(s.status, "parked", "{when}: {id} is finished");
        assert_ne!(s.status, "running", "{when}: {id} is finished");
    }
}
