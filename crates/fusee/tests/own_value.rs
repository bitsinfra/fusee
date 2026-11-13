//! `StepOptions::own_value`: a live step returns
//! the value its function produced; without it, and on replay, the value
//! decoded from the recorded JSON.
use fusee::{Config, Ctx, Engine, Error};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Lossy {
    kept: u32,
    /// Not in the JSON: decoding gives 0.
    #[serde(skip)]
    dropped: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_step_returns_its_own_value_only_when_asked() {
    let engine = Engine::open(Config::memory().shards(1)).await.unwrap();
    let wf = engine.register("lossy", |ctx: Ctx, own: bool| async move {
        let make = || async {
            Ok::<_, Error>(Lossy {
                kept: 1,
                dropped: 7,
            })
        };
        let v = if own {
            ctx.step("s", make).own_value().await?
        } else {
            ctx.step("s", make).await?
        };
        Ok(v.dropped)
    });
    assert_eq!(engine.execute(&wf, "own", &true).await.unwrap(), 7);
    assert_eq!(engine.execute(&wf, "decoded", &false).await.unwrap(), 0);
    engine.shutdown().await.unwrap();
}
