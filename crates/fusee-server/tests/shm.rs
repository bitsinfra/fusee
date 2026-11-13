//! Same-host workers over shared memory (`unix:/path?shm=1`): sections go
//! through a ring, durable positions come back in the mapping, and
//! everything else stays on the Unix socket.

use fusee::{Ctx, Engine, Error, FlushMethod, LocalOptions};
use fusee_server::{Server, ServerConfig};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("fusee-shm-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// A server on a Unix socket in `dir`; returns the socket's path.
async fn start_server(dir: &std::path::Path, shards: u32) -> (String, Arc<Server>) {
    let server = Server::new(ServerConfig {
        dir: dir.join("data"),
        shards,
        local: LocalOptions {
            flush: FlushMethod::Fsync,
            ..Default::default()
        },
        security: Default::default(),
    })
    .unwrap();
    let path = dir.join("s.sock");
    let _ = std::fs::remove_file(&path);
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    tokio::spawn(fusee_server::server::serve_unix(server.clone(), listener));
    (path.to_string_lossy().into_owned(), server)
}

async fn worker(addr: &str) -> (Engine, fusee_server::remote::Remote) {
    let a = addr.to_owned();
    let (cfg, remote) = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a))
        .await
        .unwrap()
        .unwrap();
    (Engine::open(cfg).await.unwrap(), remote)
}

fn chain(engine: &Engine, calls: Arc<AtomicU32>) -> fusee::WorkflowRef<u32, u32> {
    engine.register("chain", move |ctx: Ctx, n: u32| {
        let calls = calls.clone();
        async move {
            let mut acc = 0;
            for i in 0..n {
                let c = calls.clone();
                acc += ctx
                    .step("s", move || {
                        let c = c.clone();
                        async move {
                            c.fetch_add(1, Ordering::SeqCst);
                            Ok::<_, Error>(i)
                        }
                    })
                    .await?;
            }
            let go: u32 = ctx.wait_for("go").await?.unwrap_or(0);
            Ok(acc + go)
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflows_run_over_shared_memory_and_resume_on_the_next_worker() {
    let dir = tmp("basic");
    std::fs::create_dir_all(&dir).unwrap();
    let (path, _server) = start_server(&dir, 4).await;
    let addr = format!("unix:{path}?shm=1");
    let calls = Arc::new(AtomicU32::new(0));
    let (engine, remote) = worker(&addr).await;
    assert!(remote.shared_memory(), "the worker uses shared memory");
    let wf = chain(&engine, calls.clone());
    // Sequential steps (inline waits on the watermark) and many runs at
    // once (background flushes notified over the socket).
    let h = engine.start(&wf, "solo", &50).await.unwrap();
    let mut runs = Vec::new();
    for i in 0..40 {
        runs.push(engine.start(&wf, &format!("many-{i}"), &5).await.unwrap());
    }
    while calls.load(Ordering::SeqCst) < 50 + 40 * 5 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    engine.run::<u32>("solo").signal("go", &1u32).await.unwrap();
    assert_eq!(h.result().await.unwrap(), (0..50).sum::<u32>() + 1);
    for i in 0..20 {
        engine
            .run::<u32>(&format!("many-{i}"))
            .signal("go", &2u32)
            .await
            .unwrap();
    }
    for h in runs.drain(..20) {
        assert_eq!(h.result().await.unwrap(), 10 + 2);
    }
    // The second half waits; the next worker resumes it without rerunning
    // recorded steps.
    engine.shutdown().await.unwrap();
    // The engine's storage holds the connection: both go.
    drop((engine, wf, remote, h, runs));
    let calls2 = Arc::new(AtomicU32::new(0));
    let (engine, remote) = worker(&addr).await;
    assert!(remote.shared_memory());
    let _wf = chain(&engine, calls2.clone());
    for i in 20..40 {
        engine
            .run::<u32>(&format!("many-{i}"))
            .signal("go", &3u32)
            .await
            .unwrap();
        assert_eq!(
            engine
                .run::<u32>(&format!("many-{i}"))
                .result()
                .await
                .unwrap(),
            13
        );
    }
    assert_eq!(engine.run::<u32>("solo").result().await.unwrap(), 1226);
    assert_eq!(calls2.load(Ordering::SeqCst), 0, "recorded steps ran again");
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_shared_memory_workers_share_one_log_and_relay_messages() {
    let dir = tmp("two");
    std::fs::create_dir_all(&dir).unwrap();
    let (path, _server) = start_server(&dir, 4).await;
    let addr = format!("unix:{path}?shm=1&max-shards=2");
    let (a, ra) = worker(&addr).await;
    let (b, rb) = worker(&addr).await;
    assert!(ra.shared_memory() && rb.shared_memory());
    let (on_a, on_b) = (ra.held_shards(), rb.held_shards());
    assert_eq!((on_a.len(), on_b.len()), (2, 2));
    for e in [&a, &b] {
        e.register("child", |_ctx: Ctx, n: u32| async move { Ok(n * 3) });
        e.register("parent", |ctx: Ctx, child: String| async move {
            let v: u32 = ctx.child("child", &child, &5u32).await?;
            Ok(v + 1)
        });
    }
    let pick = |prefix: &str, shards: &[u32]| {
        (0..)
            .map(|k| format!("{prefix}-{k}"))
            .find(|id| shards.contains(&fusee_core::shard::shard_of(id, 4)))
            .unwrap()
    };
    for (engine, mine, theirs) in [(&a, &on_a, &on_b), (&b, &on_b, &on_a)] {
        let parent = pick("p", mine);
        let child = pick("c", theirs);
        let input: Arc<[u8]> = Arc::from(serde_json::to_vec(&child).unwrap());
        engine
            .start_raw("parent", &parent, input, Default::default())
            .await
            .unwrap();
        let v = tokio::time::timeout(Duration::from_secs(30), engine.run::<u32>(&parent).result())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v, 16);
    }
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_memory_is_off_unless_asked_for() {
    let dir = tmp("off");
    std::fs::create_dir_all(&dir).unwrap();
    let (path, _server) = start_server(&dir, 1).await;
    let (engine, remote) = worker(&format!("unix:{path}")).await;
    assert!(!remote.shared_memory());
    let wf = engine.register("one", |_ctx: Ctx, n: u32| async move { Ok(n + 1) });
    assert_eq!(
        engine
            .start(&wf, "x", &1)
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        2
    );
    engine.shutdown().await.unwrap();
    drop((engine, wf, remote));
    let (engine, remote) = worker(&format!("unix:{path}?shm=0")).await;
    assert!(!remote.shared_memory());
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
