//! Durable streams read from other machines through a server: the worker
//! holding the shard answers, a reader that reconnects continues at its
//! offset, and a worker failover in the middle of a streaming step keeps
//! the recorded prefix.

use fusee::{Ctx, Engine, Error, FlushMethod, LocalOptions};
use fusee_server::{Client, Server, ServerConfig};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "fusee-server-streams-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&d);
    d
}

async fn start_server(dir: &std::path::Path) -> (String, Arc<Server>) {
    let server = Server::open(
        ServerConfig {
            dir: dir.to_owned(),
            shards: 2,
            local: LocalOptions {
                flush: FlushMethod::Fsync,
                ..Default::default()
            },
            security: Default::default(),
        },
        true,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(fusee_server::server::serve_tcp(server.clone(), listener));
    (addr, server)
}

fn token(i: u64) -> String {
    format!("t{i} ")
}

/// A model call as a step: continues after what earlier attempts recorded.
/// Each token waits `pace` ms; tokens after `pause_at` wait for a release.
fn register(engine: &Engine, generated: Arc<AtomicU32>, release: Arc<AtomicU32>) {
    engine.register("llm", move |ctx: Ctx, n: u64| {
        let (generated, release) = (generated.clone(), release.clone());
        async move {
            let c = ctx.clone();
            ctx.step("generate", move || {
                let (c, generated, release) = (c.clone(), generated.clone(), release.clone());
                async move {
                    let out = c.stream("tokens")?;
                    let prefix: Vec<String> = out.recorded()?;
                    let mut text = prefix.concat();
                    for i in prefix.len() as u64..n {
                        while i >= u64::from(release.load(Ordering::SeqCst)) {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                        generated.fetch_add(1, Ordering::SeqCst);
                        out.append(&token(i))?;
                        text.push_str(&token(i));
                    }
                    Ok::<_, Error>(text)
                }
            })
            .await
        }
    });
}

/// A worker engine serving relayed calls.
struct Worker {
    engine: Engine,
    calls: tokio::task::JoinHandle<()>,
}

impl Worker {
    /// Shuts the engine down and closes its connection: the lease goes.
    async fn stop(self) {
        self.calls.abort();
        let _ = self.calls.await;
        self.engine.shutdown().await.unwrap();
    }
}

async fn worker(addr: &str, generated: Arc<AtomicU32>, release: Arc<AtomicU32>) -> Worker {
    let a = addr.to_owned();
    let (cfg, remote) = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a))
        .await
        .unwrap()
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    register(&engine, generated, release);
    let calls = tokio::spawn(fusee_server::serve_calls(remote, engine.clone()));
    Worker { engine, calls }
}

/// Reads `want` chunks of `id`'s tokens from `from` through the server,
/// connecting again (at the offset it reached) whenever a read fails.
fn read_remote(addr: &str, id: &str, from: u64, want: usize) -> (Vec<String>, Option<String>) {
    let mut out = Vec::new();
    let mut at = from;
    let mut end = None;
    for _ in 0..200 {
        let Ok(c) = Client::connect(addr) else {
            std::thread::sleep(Duration::from_millis(20));
            continue;
        };
        let mut s = c.stream(id, "tokens", at);
        loop {
            match s.next() {
                Some(Ok(chunk)) => {
                    assert_eq!(chunk.offset, at, "chunks come in order, once");
                    out.push(chunk.json::<String>().unwrap());
                    at += 1;
                    if out.len() == want {
                        return (out, None);
                    }
                }
                Some(Err(_)) => break,
                None => {
                    end = s.end().map(|e| e.status.clone());
                    return (out, end);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    (out, end)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_reader_follows_a_stream_and_reconnects_at_its_offset() {
    let dir = tmp("follow");
    let (addr, _server) = start_server(&dir).await;
    let generated = Arc::new(AtomicU32::new(0));
    let release = Arc::new(AtomicU32::new(10));
    let w = worker(&addr, generated.clone(), release.clone()).await;
    let a = addr.clone();
    tokio::task::spawn_blocking(move || {
        Client::connect(&a)
            .unwrap()
            .start("llm", "q-1", &serde_json::json!(30))
            .unwrap()
    })
    .await
    .unwrap();
    // A reader on another machine takes the first ten tokens, then leaves.
    let a = addr.clone();
    let (first, end) = tokio::task::spawn_blocking(move || read_remote(&a, "q-1", 0, 10))
        .await
        .unwrap();
    assert_eq!(first, (0..10).map(token).collect::<Vec<_>>());
    assert!(end.is_none());
    // It connects again at its offset while the step goes on.
    release.store(u32::MAX, Ordering::SeqCst);
    let a = addr.clone();
    let (rest, end) = tokio::task::spawn_blocking(move || read_remote(&a, "q-1", 10, usize::MAX))
        .await
        .unwrap();
    assert_eq!(rest, (10..30).map(token).collect::<Vec<_>>());
    assert_eq!(end.as_deref(), Some("ok"));
    let a = addr.clone();
    let (text, missing) = tokio::task::spawn_blocking(move || {
        let c = Client::connect(&a).unwrap();
        let text = c.result("q-1").unwrap();
        let missing = c.stream_read("nobody", None, "tokens", 0, 0);
        (text, missing)
    })
    .await
    .unwrap();
    assert_eq!(
        text,
        serde_json::json!((0..30).map(token).collect::<String>())
    );
    assert!(matches!(missing, Err(Error::NotFound(_))), "{missing:?}");
    w.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The worker running the step goes away mid-stream; a standby takes the
/// shards over and runs the step again from the server's log. It continues
/// after the recorded prefix: the remote reader sees each token once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_failover_mid_stream_keeps_the_recorded_prefix() {
    let dir = tmp("failover");
    let (addr, _server) = start_server(&dir).await;
    let generated = Arc::new(AtomicU32::new(0));
    let release = Arc::new(AtomicU32::new(12));
    let first = worker(&addr, generated.clone(), release.clone()).await;
    first
        .engine
        .start_raw(
            "llm",
            "f-1",
            Arc::from(&b"40"[..]),
            fusee::StartOptions::default(),
        )
        .await
        .unwrap();
    let a = addr.clone();
    let reader = tokio::task::spawn_blocking(move || read_remote(&a, "f-1", 0, usize::MAX));
    for _ in 0..1000 {
        if generated.load(Ordering::SeqCst) >= 12 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // The worker stops in the middle of the step; a standby takes over.
    first.stop().await;
    release.store(u32::MAX, Ordering::SeqCst);
    let second = worker(&addr, generated.clone(), release.clone()).await;
    let (seen, end) = reader.await.unwrap();
    assert_eq!(seen, (0..40).map(token).collect::<Vec<_>>());
    assert_eq!(end.as_deref(), Some("ok"));
    assert_eq!(
        generated.load(Ordering::SeqCst),
        40,
        "no recorded token was generated again"
    );
    let text: String = second.engine.run::<String>("f-1").result().await.unwrap();
    assert_eq!(text, (0..40).map(token).collect::<String>());
    second.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// From a worker's append to a remote reader holding the chunk, over
/// loopback TCP (printed; `--nocapture` shows it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_reader_latency() {
    let dir = tmp("latency");
    let (addr, _server) = start_server(&dir).await;
    let sent = Arc::new(std::sync::Mutex::new(Vec::<std::time::Instant>::new()));
    let a = addr.clone();
    let (cfg, remote) = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a))
        .await
        .unwrap()
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    let s = sent.clone();
    engine.register("paced", move |ctx: Ctx, n: usize| {
        let s = s.clone();
        async move {
            let c = ctx.clone();
            ctx.step("generate", move || {
                let (c, s) = (c.clone(), s.clone());
                async move {
                    let out = c.stream("tokens")?;
                    for i in 0..n {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                        s.lock().unwrap().push(std::time::Instant::now());
                        out.append(&(i as u32))?;
                    }
                    Ok::<_, Error>(())
                }
            })
            .await
        }
    });
    let calls = tokio::spawn(fusee_server::serve_calls(remote, engine.clone()));
    let samples = 200usize;
    engine
        .start_raw(
            "paced",
            "l-1",
            Arc::from(samples.to_string().into_bytes()),
            fusee::StartOptions::default(),
        )
        .await
        .unwrap();
    let a = addr.clone();
    let s = sent.clone();
    let mut lat = tokio::task::spawn_blocking(move || {
        let c = Client::connect(&a).unwrap();
        let mut lat = Vec::new();
        for chunk in c.stream("l-1", "tokens", 0) {
            let chunk = chunk.unwrap();
            let at = std::time::Instant::now();
            let sent_at = s.lock().unwrap()[chunk.offset as usize];
            lat.push(at.duration_since(sent_at).as_micros() as u64);
        }
        lat
    })
    .await
    .unwrap();
    assert_eq!(lat.len(), samples);
    lat.sort_unstable();
    eprintln!(
        "remote reader latency (append to a client over loopback TCP, server fsync): p50 {} us, p90 {} us, p99 {} us",
        lat[lat.len() / 2],
        lat[lat.len() * 9 / 10],
        lat[lat.len() * 99 / 100]
    );
    calls.abort();
    let _ = calls.await;
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
