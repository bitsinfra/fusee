//! Upgrading a log-format-2 directory to the shared log.

use fusee::upgrade::{Upgraded, upgrade_to_shared_log};
use fusee::{Config, Ctx, Engine, Error};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

fn tmp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-upgrade-{name}-{}-{}",
        std::process::id(),
        fusee::local::unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cfg(dir: &std::path::Path) -> Config {
    let mut c = Config::local(dir)
        .shards(2)
        .segment_bytes(64 * 1024)
        .checkpoint_interval(Duration::from_secs(3600));
    c.local.flush = fusee::FlushMethod::Fsync;
    c
}

fn segs(dir: &std::path::Path) -> usize {
    (0..2)
        .map(|s| {
            std::fs::read_dir(dir.join(format!("shard-{s:03}/log")))
                .map(|d| {
                    d.filter(|e| {
                        e.as_ref()
                            .is_ok_and(|e| e.file_name().to_string_lossy().ends_with(".seg"))
                    })
                    .count()
                })
                .unwrap_or(0)
        })
        .sum()
}

fn register(engine: &Engine, ran: Arc<AtomicU32>) -> fusee::WorkflowRef<u32, u32> {
    engine.register("count", move |ctx: Ctx, n: u32| {
        let ran = ran.clone();
        async move {
            let mut acc = 0;
            for i in 0..n {
                let ran = ran.clone();
                acc += ctx
                    .step(&format!("s{i}"), move || {
                        let ran = ran.clone();
                        async move {
                            ran.fetch_add(1, Ordering::SeqCst);
                            Ok::<_, Error>(i)
                        }
                    })
                    .await?;
            }
            if n % 2 == 1 {
                let more: Option<u32> = ctx.wait_for("more").await?;
                acc += more.unwrap_or(0);
            }
            Ok(acc)
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_format_2_directory_upgrades_and_keeps_every_run() {
    let dir = tmp("convert");
    let ran = Arc::new(AtomicU32::new(0));
    {
        // Format 2, and no shutdown checkpoint: the log holds a real tail.
        let engine = Engine::open(cfg(&dir).shared_log(false).checkpoint_on_shutdown(false))
            .await
            .unwrap();
        let wf = register(&engine, ran.clone());
        for i in 0..30u32 {
            let h = engine
                .start(&wf, &format!("c{i}"), &(20 + i))
                .await
                .unwrap();
            if i % 2 == 0 {
                assert_eq!(h.result().await.unwrap(), (0..20 + i).sum::<u32>());
            }
        }
        // Every odd run must reach its wait before the shutdown.
        for i in (1..30u32).step_by(2) {
            let h = engine.run::<u32>(&format!("c{i}"));
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            // At its wait: suspended, or parked out of memory under load.
            while !matches!(
                h.status().await.ok().map(|s| s.status).as_deref(),
                Some("suspended" | "parked")
            ) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "c{i} never reached its wait"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        engine.shutdown().await.unwrap();
    }
    let before = ran.load(Ordering::SeqCst);
    assert!(segs(&dir) > 2, "the test should span several segments");
    let local = cfg(&dir).local;
    match upgrade_to_shared_log(&dir, &local).unwrap() {
        Upgraded::Converted { records } => assert!(records.iter().sum::<u64>() > 600),
        other => panic!("{other:?}"),
    }
    let meta = std::fs::read_to_string(dir.join("fusee.meta")).unwrap();
    assert!(meta.contains("log=3"), "{meta}");
    assert_eq!(segs(&dir), 0);
    assert!(dir.join("wal").is_dir());
    assert_eq!(
        upgrade_to_shared_log(&dir, &local).unwrap(),
        Upgraded::AlreadyCurrent
    );

    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let wf = register(&engine, ran.clone());
    for i in 0..30u32 {
        let h = engine.run::<u32>(&format!("c{i}"));
        if i % 2 == 1 {
            h.signal("more", &1000).await.unwrap();
        }
        let want = (0..20 + i).sum::<u32>() + if i % 2 == 1 { 1000 } else { 0 };
        assert_eq!(h.result().await.unwrap(), want, "c{i}");
    }
    assert_eq!(
        ran.load(Ordering::SeqCst),
        before,
        "a recorded step ran again"
    );
    drop(wf);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crashed_upgrade_is_retried_from_the_old_format() {
    let dir = tmp("retry");
    {
        let engine = Engine::open(cfg(&dir).shared_log(false)).await.unwrap();
        let wf = register(&engine, Arc::new(AtomicU32::new(0)));
        assert_eq!(
            engine
                .start(&wf, "x", &4)
                .await
                .unwrap()
                .result()
                .await
                .unwrap(),
            6
        );
        engine.shutdown().await.unwrap();
    }
    // A previous attempt died after writing a log but before the switch.
    std::fs::create_dir_all(dir.join("wal")).unwrap();
    std::fs::write(dir.join("wal/00000000000000000001.wal"), b"junk").unwrap();
    let local = cfg(&dir).local;
    assert!(matches!(
        upgrade_to_shared_log(&dir, &local).unwrap(),
        Upgraded::Converted { .. }
    ));
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    register(&engine, Arc::new(AtomicU32::new(0)));
    assert_eq!(engine.run::<u32>("x").result().await.unwrap(), 6);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
