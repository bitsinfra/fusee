//! Update and query handlers usually hold the run's `Ctx` (to run steps),
//! and the `Ctx` holds its handlers: the runtime must break that cycle when
//! it drops a run's code, or each such run (and the shard's host behind its
//! `Ctx`) stays allocated for the life of the process.

use fusee::{Config, Ctx, Engine, Error};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Counts its drops.
struct Flag(Arc<AtomicUsize>);

impl Drop for Flag {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

async fn eventually(what: &str, f: impl Fn() -> bool) {
    for _ in 0..400 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

fn register(engine: &Engine, drops: Arc<AtomicUsize>) -> fusee::WorkflowRef<u32, u32> {
    engine.register("held", move |ctx: Ctx, n: u32| {
        let drops = drops.clone();
        async move {
            let flag = Arc::new(Flag(drops));
            let (c, f) = (ctx.clone(), flag.clone());
            ctx.on_update(
                "bump",
                |_: &u32| Ok(()),
                move |by: u32| {
                    let (c, _f) = (c.clone(), f.clone());
                    async move {
                        c.step("bump", move || async move { Ok::<_, Error>(by) })
                            .await
                    }
                },
            );
            let (c, f) = (ctx.clone(), flag);
            ctx.on_query("id", move |_: ()| {
                let _f = &f;
                c.workflow_id().to_owned()
            });
            let go: Option<u32> = ctx.wait_for("go").await?;
            Ok(n + go.unwrap_or(0))
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finished_runs_drop_handlers_that_hold_their_ctx() {
    let engine = Engine::open(Config::memory().shards(2)).await.unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let wf = register(&engine, drops.clone());
    for k in 0..8u32 {
        let h = engine.start(&wf, &format!("r{k}"), &k).await.unwrap();
        assert_eq!(h.update::<_, u32>("bump", &3).await.unwrap(), 3);
        h.signal("go", &1u32).await.unwrap();
        assert_eq!(h.result().await.unwrap(), k + 1);
    }
    eventually("every finished run's handlers to drop", || {
        drops.load(Ordering::SeqCst) == 8
    })
    .await;
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_the_engine_drops_handlers_of_running_runs() {
    let engine = Engine::open(Config::memory().shards(2)).await.unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let wf = register(&engine, drops.clone());
    for k in 0..4u32 {
        let h = engine.start(&wf, &format!("w{k}"), &k).await.unwrap();
        assert_eq!(h.update::<_, u32>("bump", &1).await.unwrap(), 1);
    }
    assert_eq!(drops.load(Ordering::SeqCst), 0, "the runs still wait");
    engine.shutdown().await.unwrap();
    drop(engine);
    eventually("the running runs' handlers to drop at close", || {
        drops.load(Ordering::SeqCst) == 4
    })
    .await;
}
