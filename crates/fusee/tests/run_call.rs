//! `Workflow::run`: a start and its result in one call to the shard.
use fusee::{Config, Ctx, Engine, Error, IdPolicy, StartOptions};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_returns_the_result_of_a_new_run() {
    let engine = Engine::open(Config::memory().shards(2)).await.unwrap();
    let wf = engine.register("add", |ctx: Ctx, n: u64| async move {
        let mut v = 0;
        for i in 0..n {
            v = ctx
                .step("s", || async move { Ok::<_, Error>(v + i) })
                .await?;
        }
        Ok(v)
    });
    for k in 0..50u64 {
        assert_eq!(
            engine.execute(&wf, &format!("r{k}"), &k).await.unwrap(),
            (0..k).sum::<u64>()
        );
    }
    // The run is recorded like a started one.
    assert_eq!(
        engine
            .execute(&wf, "r7", &7)
            .options(StartOptions::default().id_policy(IdPolicy::ATTACH))
            .await
            .unwrap(),
        21
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_reports_a_failure_and_a_taken_id() {
    let engine = Engine::open(Config::memory().shards(1)).await.unwrap();
    let fail = engine.register("fail", |ctx: Ctx, (): ()| async move {
        ctx.step("boom", || async {
            Err::<u32, _>(Error::Failed("boom".into()))
        })
        .await?;
        Ok(0u32)
    });
    assert!(engine.execute(&fail, "f1", &()).await.is_err());
    let wait = engine.register("wait", |ctx: Ctx, (): ()| async move {
        ctx.wait_for::<u32>("go").await
    });
    let handle = engine.start(&wait, "w1", &()).await.unwrap();
    // The ID is running: a second run of it is refused, as a start is.
    assert!(matches!(
        engine.execute(&wait, "w1", &()).await,
        Err(Error::AlreadyExists(_))
    ));
    // Joining it waits for its result instead.
    let joined = {
        let engine = engine.clone();
        tokio::spawn(async move {
            engine
                .run_raw(
                    "wait",
                    "w1",
                    std::sync::Arc::from(&b"null"[..]),
                    StartOptions::default().id_policy(IdPolicy::ATTACH),
                )
                .await
        })
    };
    handle.signal("go", &5u32).await.unwrap();
    let (status, payload) = joined.await.unwrap().unwrap();
    assert_eq!(status, fusee_core::record::Status::Ok);
    assert_eq!(&payload[..], b"5");
    engine.shutdown().await.unwrap();
}
