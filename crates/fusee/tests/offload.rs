//! Large payloads offloaded to the shared log's blob area
//! (docs/durability.md, "Large payloads"): results, signals and inputs over
//! the threshold come back exactly, through replay of the log, exports and
//! retirement, and nothing changes below it.

use fusee::{Config, Ctx, Engine, Error};
use std::path::{Path, PathBuf};

fn tmp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-offload-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cfg(dir: &Path) -> Config {
    let mut c = Config::local(dir).shards(2);
    c.local.flush = fusee::FlushMethod::Fsync;
    c
}

fn blob_files(dir: &Path) -> Vec<PathBuf> {
    match std::fs::read_dir(dir.join("wal/blobs")) {
        Ok(d) => d.filter_map(|e| e.ok().map(|e| e.path())).collect(),
        Err(_) => Vec::new(),
    }
}

/// A string of `n` bytes that differs from every other one of its length.
fn text(seed: u64, n: usize) -> String {
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (b'a' + (x % 26) as u8) as char
        })
        .collect()
}

type Big = (usize, usize, u32, bool, bool);

fn register(engine: &Engine) -> fusee::WorkflowRef<usize, Big> {
    engine.register("big", |ctx: Ctx, n: usize| async move {
        let page: String = ctx
            .step("fetch", move || async move { Ok::<_, Error>(text(7, n)) })
            .await?;
        let note: Option<String> = ctx.wait_for("note").await?;
        let note = note.unwrap_or_default();
        let small: u32 = ctx.step("small", || async { Ok::<_, Error>(3) }).await?;
        Ok((
            page.len(),
            note.len(),
            small,
            page == text(7, n),
            note == text(9, 1 << 20),
        ))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_results_and_signals_replay_from_the_blob_area() {
    let dir = tmp("replay");
    // No checkpoint at shutdown: the next open replays the log, references
    // and all.
    let engine = Engine::open(cfg(&dir).keep_history(true).checkpoint_on_shutdown(false))
        .await
        .unwrap();
    let wf = register(&engine);
    let h = engine.start(&wf, "a", &(1 << 20)).await.unwrap();
    h.signal("note", &text(9, 1 << 20)).await.unwrap();
    let r = h.result().await.unwrap();
    assert_eq!(r, (1 << 20, 1 << 20, 3, true, true));
    engine.shutdown().await.unwrap();
    assert!(
        !blob_files(&dir).is_empty(),
        "the 1 MiB result and signal went to the blob area"
    );

    let engine = Engine::open(cfg(&dir).keep_history(true)).await.unwrap();
    register(&engine);
    let run = engine.run::<Big>("a");
    assert_eq!(run.result().await.unwrap(), r);
    let detail = run.describe().await.unwrap();
    let largest = detail
        .journal
        .iter()
        .map(|rec| match rec {
            fusee::core::Record::Step { payload, .. }
            | fusee::core::Record::Signal { payload, .. } => payload.len(),
            _ => 0,
        })
        .max()
        .unwrap();
    assert!(largest > 1 << 20, "{largest}");
    engine.shutdown().await.unwrap();

    // An export of the directory reads the journal back whole.
    let exported = fusee::export::export_from_dir(&dir, Some("a"), 1).unwrap();
    let bytes: usize = exported[0]
        .records
        .iter()
        .map(|r| match r {
            fusee::core::Record::Step { payload, .. }
            | fusee::core::Record::Signal { payload, .. } => payload.len(),
            _ => 0,
        })
        .sum();
    assert!(bytes > 2 << 20, "{bytes}");
    std::fs::remove_dir_all(dir).unwrap();
}

/// Starts with large inputs: the start record itself is offloaded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_large_start_input_is_offloaded_and_replayed() {
    let dir = tmp("input");
    let open = || Engine::open(cfg(&dir).checkpoint_on_shutdown(false));
    let engine = open().await.unwrap();
    let wf = engine.register("echo", |_ctx: Ctx, s: String| async move { Ok(s.len()) });
    let input = text(3, 300 << 10);
    assert_eq!(
        engine
            .start(&wf, "e", &input)
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        300 << 10
    );
    engine.shutdown().await.unwrap();
    assert!(!blob_files(&dir).is_empty());
    let engine = open().await.unwrap();
    engine.register("echo", |_ctx: Ctx, s: String| async move { Ok(s.len()) });
    assert_eq!(engine.run::<usize>("e").result().await.unwrap(), 300 << 10);
    engine.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

/// Below the threshold, and with offload turned off, no blob area appears.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_payloads_and_a_zero_threshold_write_no_blobs() {
    for (threshold, size) in [(64 << 10, 60 << 10), (0, 1 << 20)] {
        let dir = tmp("small");
        let engine = Engine::open(cfg(&dir).payload_offload_bytes(threshold))
            .await
            .unwrap();
        let wf = engine.register("echo", |_ctx: Ctx, s: String| async move { Ok(s.len()) });
        for i in 0..5 {
            let id = format!("s{i}");
            assert_eq!(
                engine
                    .start(&wf, &id, &text(i, size))
                    .await
                    .unwrap()
                    .result()
                    .await
                    .unwrap(),
                size
            );
        }
        engine.shutdown().await.unwrap();
        assert!(!dir.join("wal/blobs").exists(), "threshold {threshold}");
        std::fs::remove_dir_all(dir).unwrap();
    }
}

/// Once checkpoints cover the records that reference them, blob files go
/// with the log segments that held those records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoints_retire_blob_files() {
    let dir = tmp("retire");
    let mut c = cfg(&dir).shards(1);
    c.local.segment_bytes = 1 << 20;
    let engine = Engine::open(c.clone()).await.unwrap();
    let wf = engine.register("echo", |_ctx: Ctx, s: String| async move { Ok(s.len()) });
    for i in 0..12u64 {
        let id = format!("r{i}");
        engine
            .start(&wf, &id, &text(i, 512 << 10))
            .await
            .unwrap()
            .result()
            .await
            .unwrap();
    }
    let most = blob_files(&dir).len();
    assert!(most >= 2, "{most} blob files");
    // Small work rolls the log on; the shutdown checkpoint covers it all.
    for i in 0..3000u64 {
        let id = format!("t{i}");
        engine
            .start(&wf, &id, &text(i, 600))
            .await
            .unwrap()
            .result()
            .await
            .unwrap();
    }
    engine.shutdown().await.unwrap();
    let engine = Engine::open(c).await.unwrap();
    engine.register("echo", |_ctx: Ctx, s: String| async move { Ok(s.len()) });
    assert_eq!(engine.run::<usize>("r3").result().await.unwrap(), 512 << 10);
    engine.shutdown().await.unwrap();
    let left = blob_files(&dir).len();
    assert!(left < most, "blob files retired: {most} -> {left}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_mode_ignores_offload_and_custom_storage_refuses_it() {
    let engine = Engine::open(Config::memory().payload_offload_bytes(1024))
        .await
        .unwrap();
    let wf = engine.register("echo", |_ctx: Ctx, s: String| async move { Ok(s.len()) });
    assert_eq!(
        engine
            .start(&wf, "m", &text(1, 100_000))
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        100_000
    );
    engine.shutdown().await.unwrap();
    let factory: fusee::engine::EnvFactory = std::sync::Arc::new(|_| {
        Ok(Box::new(fusee::local::MemoryEnv::default()) as Box<dyn fusee::env::Env>)
    });
    let mut custom = Config::memory().payload_offload_bytes(1024);
    custom.durability = fusee::Durability::Custom(factory);
    match Engine::open(custom).await {
        Err(Error::Io(m)) => assert!(m.contains("payload offload"), "{m}"),
        Err(e) => panic!("{e}"),
        Ok(_) => panic!("server-mode storage accepted payload offload"),
    }
}

/// A run suspended after an offloaded 1 MiB result moves with it through
/// `reshard` (whose reader resolves references), and the step does not run
/// again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reshard_carries_offloaded_records() {
    let dir = tmp("reshard");
    let fetched = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let register_counted = |engine: &Engine| {
        let fetched = fetched.clone();
        engine.register("big", move |ctx: Ctx, n: usize| {
            let fetched = fetched.clone();
            async move {
                let page: String = ctx
                    .step("fetch", move || {
                        let fetched = fetched.clone();
                        async move {
                            fetched.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            Ok::<_, Error>(text(7, n))
                        }
                    })
                    .await?;
                let note: Option<String> = ctx.wait_for("note").await?;
                Ok::<_, Error>((page.len(), page == text(7, n), note.unwrap_or_default()))
            }
        })
    };
    // No checkpoint at shutdown: the reshard reads the run from the log,
    // where its result is a reference.
    let engine = Engine::open(cfg(&dir).checkpoint_on_shutdown(false))
        .await
        .unwrap();
    let wf = register_counted(&engine);
    engine.start(&wf, "a", &(1 << 20)).await.unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let d = engine
            .run::<(usize, bool, String)>("a")
            .describe()
            .await
            .ok();
        if d.is_some_and(|d| {
            d.journal
                .iter()
                .any(|r| matches!(r, fusee::core::Record::Step { payload, .. } if payload.len() > 1 << 20))
        }) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the result was never recorded"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    engine.shutdown().await.unwrap();
    assert!(
        !blob_files(&dir).is_empty(),
        "the 1 MiB result went to the blob area"
    );

    let mut opts = fusee::reshard::ReshardOptions::default();
    opts.local.flush = fusee::FlushMethod::Fsync;
    match fusee::reshard::reshard(&dir, 3, &opts).unwrap() {
        fusee::reshard::Resharded::Done(r) => assert_eq!((r.from, r.to), (2, 3)),
        other => panic!("expected a reshard, got {other:?}"),
    }

    let mut c = Config::local(&dir).shards(3);
    c.local.flush = fusee::FlushMethod::Fsync;
    let engine = Engine::open(c).await.unwrap();
    register_counted(&engine);
    let run = engine.run::<(usize, bool, String)>("a");
    run.signal("note", &"done".to_string()).await.unwrap();
    assert_eq!(
        run.result().await.unwrap(),
        (1 << 20, true, "done".to_string())
    );
    assert_eq!(
        fetched.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the recorded step ran again after the reshard"
    );
    engine.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
