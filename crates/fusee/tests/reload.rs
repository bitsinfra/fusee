//! A run parked and loaded again before its old code was dropped: dropping
//! the old code must not forget the new code's operations. Otherwise the
//! old futures' drops would remove the new incarnation's pending child
//! operations, the children's results that arrive next would be taken for
//! delivered, acknowledged and dropped, and the parent would wait forever.

use fusee::{Config, Ctx, Engine};
use std::time::Duration;

fn tmp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-reload-{name}-{}-{}",
        std::process::id(),
        fusee::local::unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn children_results_reach_a_parent_parked_and_loaded_again() {
    const PARENTS: u32 = 8;
    const KIDS: u32 = 4;
    for round in 0..3 {
        let dir = tmp(&format!("kids-{round}"));
        let mut c = Config::local(&dir)
            .shards(3)
            // Parks the parent at every suspension, between children's results.
            .park_after(Duration::ZERO);
        c.local.flush = fusee::FlushMethod::Fsync;
        let engine = Engine::open(c).await.unwrap();
        let parent = engine.register("parent", |ctx: Ctx, i: u32| async move {
            let ids: Vec<String> = (0..KIDS).map(|j| format!("kid-{i}-{j}")).collect();
            let kids = ids.iter().map(|id| ctx.child::<u32, u32>("kid", id, &1));
            let mut sum = 0;
            for r in ctx.all(kids).await {
                sum += r?;
            }
            Ok(sum)
        });
        engine.register("kid", |ctx: Ctx, n: u32| async move {
            let v: Option<u32> = ctx.wait_for("go").await?;
            Ok(n + v.unwrap_or(0))
        });
        for i in 0..PARENTS {
            engine
                .start(&parent, &format!("parent-{i}"), &i)
                .await
                .unwrap();
        }
        for i in 0..PARENTS {
            for j in 0..KIDS {
                engine
                    .run::<u32>(&format!("kid-{i}-{j}"))
                    .started()
                    .await
                    .unwrap();
            }
        }
        // Let every run park.
        tokio::time::sleep(Duration::from_millis(150)).await;
        // One child of each parent at a time, each after the park sweep
        // (every 100 ms) parked its parent again: its result loads the
        // parent before the parent's old code is dropped.
        for j in 0..KIDS {
            for i in 0..PARENTS {
                engine
                    .run::<u32>(&format!("kid-{i}-{j}"))
                    .signal("go", &1u32)
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        for i in 0..PARENTS {
            let id = format!("parent-{i}");
            let r = tokio::time::timeout(Duration::from_secs(10), engine.run::<u32>(&id).result())
                .await
                .unwrap_or_else(|_| panic!("round {round}: {id} never got its children's results"))
                .unwrap();
            assert_eq!(r, 2 * KIDS);
        }
        engine.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
