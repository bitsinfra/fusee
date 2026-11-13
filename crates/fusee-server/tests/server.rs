use fusee::{Ctx, Engine, Error, FlushMethod, LocalOptions};
use fusee_server::{Server, ServerConfig, remote_config};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("fusee-server-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

async fn start_server(dir: &std::path::Path) -> String {
    start_server_with(dir).await.0
}

async fn start_server_with(dir: &std::path::Path) -> (String, Arc<Server>) {
    start_server_in(dir, true).await
}

/// A server whose new directory keeps a log per shard (log format 2), as
/// servers before the shared log did.
async fn start_per_shard_server(dir: &std::path::Path) -> String {
    start_server_in(dir, false).await.0
}

async fn start_server_in(dir: &std::path::Path, shared_log: bool) -> (String, Arc<Server>) {
    let local = LocalOptions {
        flush: FlushMethod::Fsync,
        ..Default::default()
    };
    let server = Server::open(
        ServerConfig {
            dir: dir.to_owned(),
            shards: 2,
            local,
            security: Default::default(),
        },
        shared_log,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(fusee_server::server::serve_tcp(server.clone(), listener));
    (addr, server)
}

fn register(engine: &Engine, calls: Arc<AtomicU32>) -> fusee::WorkflowRef<u32, u32> {
    engine.register("job", move |ctx: Ctx, n: u32| {
        let calls = calls.clone();
        async move {
            let a: u32 = ctx
                .step("a", || {
                    let c = calls.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, Error>(n + 1)
                    }
                })
                .await?;
            let go: Option<u32> = ctx.wait_for("go").await?;
            let b: u32 = ctx
                .step("b", || async move { Ok::<_, Error>(a + go.unwrap_or(0)) })
                .await?;
            Ok(b)
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_failover_resumes_runs_from_the_server_log() {
    let dir = tmp("failover");
    let addr = start_server(&dir).await;
    let calls = Arc::new(AtomicU32::new(0));

    let a = addr.clone();
    let cfg = tokio::task::spawn_blocking(move || remote_config(&a))
        .await
        .unwrap()
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    let wf = register(&engine, calls.clone());
    for i in 0..20u32 {
        engine.start(&wf, &format!("job-{i}"), &i).await.unwrap();
    }
    for _ in 0..500 {
        if calls.load(Ordering::SeqCst) >= 20 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 20);

    // A standby connects and waits for the lease.
    let a = addr.clone();
    let standby = tokio::task::spawn_blocking(move || remote_config(&a));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !standby.is_finished(),
        "the standby must wait while the lease is held"
    );

    // The first worker goes away.
    engine.shutdown().await.unwrap();
    drop(wf);
    drop(engine);

    let cfg = standby.await.unwrap().unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    let _wf = register(&engine, calls.clone());
    for i in 0..20u32 {
        let h = engine.run::<u32>(&format!("job-{i}"));
        h.signal("go", &100).await.unwrap();
        assert_eq!(h.result().await.unwrap(), i + 101);
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        20,
        "recorded steps were not run again"
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn step_latency_is_one_round_trip_plus_the_flush() {
    let dir = tmp("latency");
    let addr = start_server(&dir).await;
    let a = addr.clone();
    let cfg = tokio::task::spawn_blocking(move || remote_config(&a))
        .await
        .unwrap()
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    let wf = engine.register("chain", |ctx: Ctx, n: u32| async move {
        for i in 0..n {
            ctx.step("noop", || async move { Ok::<_, Error>(i) })
                .await?;
        }
        Ok(n)
    });
    let t = std::time::Instant::now();
    assert_eq!(
        engine
            .start(&wf, "c", &500)
            .await
            .unwrap()
            .result()
            .await
            .unwrap(),
        500
    );
    let per = t.elapsed().as_secs_f64() * 1e6 / 500.0;
    let m = engine.metrics().await.unwrap();
    eprintln!(
        "server mode over loopback TCP, plain fsync: {per:.1} µs per sequential step; {} of {} flushes waited inline",
        m.inline_flushes, m.flushes
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn other_machines_complete_update_and_query_through_the_server() {
    let dir = tmp("calls");
    let addr = start_server(&dir).await;
    let a = addr.clone();
    let (cfg, remote) = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a))
        .await
        .unwrap()
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    tokio::spawn(fusee_server::serve_calls(remote, engine.clone()));
    let tokens = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let t2 = tokens.clone();
    let wf = engine.register("approval", move |ctx: Ctx, _: ()| {
        let t2 = t2.clone();
        async move {
            let notes = std::rc::Rc::new(std::cell::RefCell::new(Vec::<String>::new()));
            let n2 = notes.clone();
            ctx.on_query("notes", move |_: ()| n2.borrow().clone());
            let n3 = notes.clone();
            ctx.on_update(
                "note",
                |_: &String| Ok(()),
                move |s: String| {
                    let n = n3.clone();
                    async move {
                        n.borrow_mut().push(s);
                        Ok(n.borrow().len())
                    }
                },
            );
            let ok: bool = ctx
                .await_completion("approve", move |c: fusee::Completion| {
                    let token = c.token;
                    let t2 = t2.clone();
                    async move {
                        t2.lock().unwrap().push(token);
                        Ok::<_, Error>(())
                    }
                })
                .await?;
            Ok((ok, notes.borrow().clone()))
        }
    });
    engine.start(&wf, "doc-1", &()).await.unwrap();
    for _ in 0..500 {
        if !tokens.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let token = tokens.lock().unwrap()[0].clone();
    // Another machine: no lease, just a client.
    let a = addr.clone();
    let out = tokio::task::spawn_blocking(move || {
        let c = fusee_server::Client::connect(&a).unwrap();
        let n = c
            .update_with_start(
                "approval",
                "doc-1",
                &serde_json::Value::Null,
                &fusee::StartOptions::default(),
                "note",
                "n1",
                &serde_json::json!("looks good"),
            )
            .unwrap();
        let again = c
            .update("doc-1", "note", "n1", &serde_json::json!("looks good"))
            .unwrap();
        let notes = c.query("doc-1", "notes", &serde_json::Value::Null).unwrap();
        c.complete(&token, &serde_json::json!(true)).unwrap();
        let stale = c.complete(&token, &serde_json::json!(true));
        let result = c.result("doc-1").unwrap();
        let listed = c
            .list_where(&serde_json::json!({"status":"ok"}), 10)
            .unwrap();
        assert!(
            listed
                .as_array()
                .unwrap()
                .iter()
                .any(|row| row["id"] == "doc-1" && row["startedMs"].is_number())
        );
        let missing = c.signal("nobody", "x", &serde_json::Value::Null);
        (n, again, notes, stale, result, missing)
    })
    .await
    .unwrap();
    assert_eq!(out.0, serde_json::json!(1));
    assert_eq!(out.1, serde_json::json!(1), "deduplicated by update ID");
    assert_eq!(out.2, serde_json::json!(["looks good"]));
    assert!(matches!(out.3, Err(Error::Rejected(_))), "{:?}", out.3);
    assert_eq!(out.4, serde_json::json!([true, ["looks good"]]));
    assert!(matches!(out.5, Err(Error::NotFound(_))), "{:?}", out.5);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn limits_live_on_the_server() {
    let dir = tmp("limits");
    let (addr, server) = start_server_with(&dir).await;
    let a = addr.clone();
    let cfg = tokio::task::spawn_blocking(move || remote_config(&a))
        .await
        .unwrap()
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    engine
        .limit(
            "stripe",
            fusee::LimitSpec {
                concurrent: 2,
                per_second: 0.0,
            },
        )
        .unwrap();
    let now = Arc::new(AtomicU32::new(0));
    let max = Arc::new(AtomicU32::new(0));
    let (n2, m2) = (now.clone(), max.clone());
    let wf = engine.register("charge", move |ctx: Ctx, _: ()| {
        let (now, max) = (n2.clone(), m2.clone());
        async move {
            ctx.step("charge", || {
                let (now, max) = (now.clone(), max.clone());
                async move {
                    let n = now.fetch_add(1, Ordering::SeqCst) + 1;
                    max.fetch_max(n, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    now.fetch_sub(1, Ordering::SeqCst);
                    Ok::<_, Error>(())
                }
            })
            .options(fusee::StepOptions::default().limit("stripe"))
            .await
        }
    });
    let mut runs = Vec::new();
    for i in 0..8 {
        runs.push(engine.start(&wf, &format!("c-{i}"), &()).await.unwrap());
    }
    for r in runs {
        r.result().await.unwrap();
    }
    assert_eq!(max.load(Ordering::SeqCst), 2);
    assert_eq!(server.limits.max_in_use("stripe"), 2);
    assert_eq!(server.limits.in_use("stripe", fusee::local::unix_ms()), 0);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_server_serves_waiting_steps_by_priority_then_fairness_key() {
    let dir = tmp("limit-order");
    let (addr, server) = start_server_with(&dir).await;
    let a = addr.clone();
    let cfg = tokio::task::spawn_blocking(move || remote_config(&a))
        .await
        .unwrap()
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    engine
        .limit(
            "one",
            fusee::LimitSpec {
                concurrent: 1,
                per_second: 0.0,
            },
        )
        .unwrap();
    async fn until(what: &str, f: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !f() {
            assert!(std::time::Instant::now() < deadline, "timed out: {what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let open = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let served = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let o2 = open.clone();
    let hold = engine.register("hold", move |ctx: Ctx, _: ()| {
        let open = o2.clone();
        async move {
            ctx.step("hold", || {
                let open = open.clone();
                async move {
                    while !open.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    Ok::<_, Error>(())
                }
            })
            .options(fusee::StepOptions::default().limit("one"))
            .await
        }
    });
    let s2 = served.clone();
    let job = engine.register("job", move |ctx: Ctx, (level, key): (i32, String)| {
        let served = s2.clone();
        async move {
            let opts = fusee::StepOptions::default()
                .limit("one")
                .priority(level)
                .fairness(&key);
            ctx.step("work", || {
                let (served, key) = (served.clone(), key.clone());
                async move {
                    served.lock().unwrap().push(format!("{key}@{level}"));
                    Ok::<_, Error>(())
                }
            })
            .options(opts)
            .await
        }
    });
    let blocker = engine.start(&hold, "hold", &()).await.unwrap();
    let now = fusee::local::unix_ms;
    until("the blocker's lease", || {
        server.limits.in_use("one", now()) == 1
    })
    .await;
    let mut asks: Vec<(i32, &str)> = vec![(0, "big"); 4];
    asks.extend([(0, "a"), (0, "b"), (0, "a"), (7, "urgent")]);
    let mut runs = Vec::new();
    for (i, (level, key)) in asks.iter().enumerate() {
        runs.push(
            engine
                .start(&job, &format!("job-{i}"), &(*level, key.to_string()))
                .await
                .unwrap(),
        );
        // The server holds the queue: each step is queued there in turn.
        until("the step to queue", || {
            server.limits.waiting("one") == i + 1
        })
        .await;
    }
    open.store(true, Ordering::SeqCst);
    blocker.result().await.unwrap();
    for r in runs {
        r.result().await.unwrap();
    }
    assert_eq!(
        *served.lock().unwrap(),
        [
            "urgent@7", "big@0", "a@0", "b@0", "big@0", "a@0", "big@0", "big@0"
        ]
    );
    assert_eq!(server.limits.max_in_use("one"), 1);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_ranges_preserve_padding_and_nonzero_bytes_beyond_the_first_page() {
    use fusee::env::Env;
    use fusee_server::remote::Remote;
    let directory = tmp("recovery-ranges");
    let addr = start_per_shard_server(&directory).await;
    let log = directory.join("shard-000/log");
    std::fs::create_dir_all(&log).unwrap();
    let mut original = vec![0; 3 * 1024 * 1024 + 19];
    original[0] = 84;
    original[1024 * 1024 - 1] = 19;
    // A damaged tail still travels through the ordinary recovery decoder.
    original[2 * 1024 * 1024 + 17] = 93;
    std::fs::write(log.join("00000000000000000001.seg"), &original).unwrap();
    std::fs::write(log.join("00000000000000000002.seg"), [0; 4097]).unwrap();
    tokio::task::spawn_blocking(move || {
        let remote = Remote::connect(&addr).unwrap();
        let mut env = remote.env(0);
        assert_eq!(env.read_segment(1).unwrap(), original);
        assert_eq!(env.read_segment(2).unwrap(), vec![0; 4097]);
        env.sync_recovered().unwrap();
    })
    .await
    .unwrap();
}

/// Reads from a blocking socket until a frame is complete.
fn next_frame(
    sock: &mut std::net::TcpStream,
    dec: &mut fusee_core::frame::Decoder,
) -> Option<(u8, Vec<u8>)> {
    use std::io::Read;
    let mut buf = [0u8; 4096];
    loop {
        if let Some(f) = dec.next().unwrap() {
            return Some(f);
        }
        match sock.read(&mut buf) {
            Ok(0) | Err(_) => return None,
            Ok(n) => dec.feed(&buf[..n]),
        }
    }
}

/// Polls a shard's flush slot until it reports something; an acknowledgment
/// after a failed append fails the test.
fn flush_error(env: &mut impl fusee::env::Env) -> String {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match env.poll_flushed() {
            Some(Err(e)) => return e.to_string(),
            Some(Ok(lsn)) => panic!("a flush was acknowledged at {lsn} after a failed append"),
            None => {}
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the failure was not reported"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_store_append_at_a_diverged_offset_is_refused_and_stops_the_shard() {
    use fusee::env::Env;
    use fusee_server::remote::Remote;
    let dir = tmp("store-offsets");
    let addr = start_server(&dir).await;
    tokio::task::spawn_blocking(move || {
        let remote = Remote::connect(&addr).unwrap();
        // Two writers for one shard: the second one's idea of the store
        // length goes stale, as after any divergence.
        let mut first = remote.env(0);
        first.recover_store(0, 0, &mut |_, _, _| {}).unwrap();
        let mut stale = remote.env(0);
        stale.recover_store(0, 0, &mut |_, _, _| {}).unwrap();
        let start = first.store_len();
        assert_eq!(stale.store_len(), start);
        assert_eq!(first.store_append(b"first-writer-frame").unwrap(), start);
        // The stale writer references the same offset; the server refuses.
        assert_eq!(stale.store_append(b"stale-writer-frame").unwrap(), start);
        let err = flush_error(&mut stale);
        assert!(err.contains("offset mismatch"), "{err}");
        assert!(err.contains(&format!("expected offset {start}")), "{err}");
        // The shard stays stopped for every later write.
        let again = stale.store_append(b"later-frame").unwrap_err().to_string();
        assert!(again.contains("offset mismatch"), "{again}");
        assert!(first.store_append(b"later-frame").is_err());
    })
    .await
    .unwrap();
    let mut written = Vec::new();
    for entry in std::fs::read_dir(dir.join("shard-000")).unwrap() {
        let path = entry.unwrap().path();
        if path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("runs-")
        {
            written.extend(std::fs::read(&path).unwrap());
        }
    }
    let has = |needle: &[u8]| written.windows(needle.len()).any(|w| w == needle);
    assert!(has(b"first-writer-frame"));
    assert!(
        !has(b"stale-writer-frame"),
        "the server wrote a frame where the worker's reference does not point"
    );
    assert!(!has(b"later-frame"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_worker_stops_the_shard_when_the_server_reports_another_offset() {
    use fusee::env::Env;
    use fusee_core::frame;
    use fusee_core::wire::{Reader, put_u32, put_u64};
    use fusee_server::proto::{self, req, resp};
    use std::io::Write;
    // A server that claims to write every frame one byte later than asked.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        let mut dec = frame::Decoder::new();
        let (kind, body) = next_frame(&mut sock, &mut dec).unwrap();
        assert_eq!(kind, req::HELLO);
        let mut r = Reader::new(&body);
        r.u64().unwrap();
        let offered = r.u32().unwrap();
        let mut b = Vec::new();
        put_u64(&mut b, 1);
        put_u32(&mut b, 1);
        put_u64(&mut b, 7);
        put_u32(&mut b, proto::STORE_OFFSETS);
        let mut out = Vec::new();
        frame::encode(resp::LEASE, &b, &mut out);
        sock.write_all(&out).unwrap();
        out.clear();
        // No liveness was granted, so the worker sends no pings.
        let (kind, body) = next_frame(&mut sock, &mut dec).expect("worker went away");
        assert_eq!(kind, req::STORE_APPEND_AT, "unexpected request");
        let mut r = Reader::new(&body);
        let head = proto::read_head(&mut r).unwrap();
        let expected = r.u64().unwrap();
        let mut b = Vec::new();
        put_u64(&mut b, head.id);
        put_u32(&mut b, head.shard);
        put_u64(&mut b, expected + 1);
        frame::encode(resp::STORE_APPENDED, &b, &mut out);
        // A later batch's acknowledgment must not count either.
        let mut b = Vec::new();
        put_u64(&mut b, 99);
        put_u32(&mut b, head.shard);
        put_u64(&mut b, 5);
        frame::encode(resp::FLUSHED, &b, &mut out);
        sock.write_all(&out).unwrap();
        // Hold the connection until the worker closes it.
        while next_frame(&mut sock, &mut dec).is_some() {}
        offered
    });
    let remote = fusee_server::remote::Remote::connect(&addr).unwrap();
    assert_eq!(remote.epoch(), 7);
    let mut env = remote.env(0);
    assert_eq!(env.store_append(b"blob").unwrap(), 0);
    let err = flush_error(&mut env);
    assert!(err.contains("offset mismatch"), "{err}");
    assert!(err.contains("referenced offset 0"), "{err}");
    assert!(err.contains("wrote at 1"), "{err}");
    for _ in 0..40 {
        if let Some(Ok(lsn)) = env.poll_flushed() {
            panic!("the acknowledgment of LSN {lsn} counted after the mismatch");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let again = env.store_append(b"more").unwrap_err().to_string();
    assert!(again.contains("offset mismatch"), "{again}");
    drop(env);
    drop(remote);
    let offered = server.join().unwrap();
    assert_ne!(offered & proto::STORE_OFFSETS, 0);
    assert_ne!(offered & proto::LEASE_LIVENESS, 0);
}

const FAST: fusee_server::Liveness = fusee_server::Liveness {
    ping_interval: Duration::from_millis(100),
    timeout: Duration::from_millis(1000),
};

/// A TCP relay that can stop forwarding in both directions while keeping
/// both sockets open, like a worker process that hangs.
struct Relay {
    addr: String,
    to_server: tokio::sync::watch::Sender<bool>,
    to_worker: tokio::sync::watch::Sender<bool>,
    /// Bytes from the server held back by a freeze.
    held: Arc<std::sync::atomic::AtomicUsize>,
    pipes: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl Relay {
    async fn start(target: String) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (to_server, server_gate) = tokio::sync::watch::channel(false);
        let (to_worker, worker_gate) = tokio::sync::watch::channel(false);
        let held = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let pipes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (h, p) = (held.clone(), pipes.clone());
        tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                let outbound = tokio::net::TcpStream::connect(&target).await.unwrap();
                let _ = inbound.set_nodelay(true);
                let _ = outbound.set_nodelay(true);
                let (from_worker, to_worker) = inbound.into_split();
                let (from_server, to_server) = outbound.into_split();
                let mut p = p.lock().unwrap();
                p.push(tokio::spawn(pipe(
                    from_worker,
                    to_server,
                    server_gate.clone(),
                    None,
                )));
                p.push(tokio::spawn(pipe(
                    from_server,
                    to_worker,
                    worker_gate.clone(),
                    Some(h.clone()),
                )));
            }
        });
        Self {
            addr,
            to_server,
            to_worker,
            held,
            pipes,
        }
    }

    fn freeze(&self, frozen: bool) {
        self.to_server.send_replace(frozen);
        self.to_worker.send_replace(frozen);
    }

    /// The worker's requests still reach the server; its replies do not.
    fn freeze_replies(&self) {
        self.to_worker.send_replace(true);
    }

    fn held(&self) -> usize {
        self.held.load(Ordering::SeqCst)
    }

    /// Both connections close at once, as when the worker process dies.
    fn kill(&self) {
        for p in self.pipes.lock().unwrap().drain(..) {
            p.abort();
        }
    }
}

async fn pipe(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    mut gate: tokio::sync::watch::Receiver<bool>,
    held: Option<Arc<std::sync::atomic::AtomicUsize>>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = vec![0u8; 1 << 16];
    loop {
        let n = from.read(&mut buf).await.unwrap_or(0);
        if *gate.borrow()
            && let Some(held) = &held
        {
            held.fetch_add(n, Ordering::SeqCst);
        }
        // Bytes and closes that arrive while frozen are held until thawed.
        if gate.wait_for(|frozen| !*frozen).await.is_err() || n == 0 {
            break;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
    let _ = to.shutdown().await;
}

async fn finish(
    engine: &Engine,
    wf: &fusee::WorkflowRef<u32, u32>,
    id: &str,
    n: u32,
    go: u32,
) -> u32 {
    let h = engine.start(wf, id, &n).await.unwrap();
    h.signal("go", &go).await.unwrap();
    h.result().await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silent_worker_loses_its_lease_to_a_standby_that_sees_its_acknowledged_work() {
    let dir = tmp("liveness-revoke");
    let (addr, server) = start_server_with(&dir).await;
    server.set_liveness(Some(FAST)).unwrap();
    let relay = Relay::start(addr.clone()).await;
    let calls = Arc::new(AtomicU32::new(0));
    let a = relay.addr.clone();
    let (cfg, first) = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a))
        .await
        .unwrap()
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    let wf = register(&engine, calls.clone());
    for i in 0..10u32 {
        assert_eq!(
            finish(&engine, &wf, &format!("job-{i}"), i, 100).await,
            i + 101
        );
    }

    // The worker hangs with its connection open.
    relay.freeze(true);
    let frozen_at = std::time::Instant::now();
    let a = addr.clone();
    let standby = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a));
    let (cfg, second) = tokio::time::timeout(Duration::from_secs(30), standby)
        .await
        .expect("the standby never got the lease")
        .unwrap()
        .unwrap();
    let waited = frozen_at.elapsed();
    assert!(
        waited >= FAST.timeout - FAST.ping_interval * 2,
        "the lease was revoked after {waited:?} of silence"
    );
    assert!(second.epoch() > first.epoch());
    let engine2 = Engine::open(cfg).await.unwrap();
    let wf2 = register(&engine2, calls.clone());
    for i in 0..10u32 {
        let h = engine2.run::<u32>(&format!("job-{i}"));
        assert_eq!(h.result().await.unwrap(), i + 101);
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        10,
        "acknowledged steps ran again"
    );

    // The old session is fenced: once its traffic flows again it fails, and
    // nothing it sends lands.
    relay.freeze(false);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut attempt = 0;
    loop {
        let id = format!("late-{attempt}");
        attempt += 1;
        match tokio::time::timeout(Duration::from_secs(2), engine.start(&wf, &id, &0)).await {
            Ok(Err(_)) => break,
            Ok(Ok(_)) => panic!("the revoked worker had a start acknowledged"),
            Err(_) => {}
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the revoked worker never noticed"
        );
    }
    for k in 0..attempt {
        assert!(
            engine2
                .status(&format!("late-{k}"))
                .await
                .unwrap()
                .is_none(),
            "a write from the revoked worker landed"
        );
    }
    assert_eq!(finish(&engine2, &wf2, "after", 1, 1).await, 3);
    engine2.shutdown().await.unwrap();
    let _ = engine.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A server on its own single-threaded runtime, so a test can stall it the
/// way a long inline flush or a large recovery read does.
fn start_stallable_server(dir: &std::path::Path) -> (String, tokio::runtime::Handle) {
    let server = Server::new(ServerConfig {
        dir: dir.to_owned(),
        shards: 2,
        local: LocalOptions {
            flush: FlushMethod::Fsync,
            ..Default::default()
        },
        security: Default::default(),
    })
    .unwrap();
    server.set_liveness(Some(FAST)).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = runtime.handle().clone();
    std::thread::spawn(move || {
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let _ = fusee_server::server::serve_tcp(server, listener).await;
        })
    });
    (addr, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_worker_keeps_its_lease_while_its_server_stalls() {
    let dir = tmp("liveness-idle");
    let (addr, server) = start_stallable_server(&dir);
    let calls = Arc::new(AtomicU32::new(0));
    let a = addr.clone();
    let (cfg, remote) = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a))
        .await
        .unwrap()
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    let wf = register(&engine, calls.clone());
    assert_eq!(finish(&engine, &wf, "before", 1, 1).await, 3);
    let a = addr.clone();
    let standby = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a));

    // Idle for several timeouts: pings keep the lease.
    tokio::time::sleep(FAST.timeout * 3).await;
    assert!(
        !standby.is_finished(),
        "an idle, healthy worker lost its lease"
    );

    // The server stalls for well over the timeout while a request waits on
    // it. The pings queue in its socket; they count once it reads again.
    let stall = FAST.timeout * 5 / 2;
    let (started, begun) = tokio::sync::oneshot::channel();
    let stalled = server.spawn(async move {
        let _ = started.send(());
        std::thread::sleep(stall);
    });
    begun.await.unwrap();
    let t = std::time::Instant::now();
    assert_eq!(finish(&engine, &wf, "during", 2, 1).await, 4);
    assert!(
        t.elapsed() >= stall / 2,
        "the request did not wait on the stall"
    );
    stalled.await.unwrap();
    tokio::time::sleep(FAST.timeout * 2).await;
    assert!(
        !standby.is_finished(),
        "the server's own stall fenced its healthy worker"
    );
    assert_eq!(finish(&engine, &wf, "after", 3, 1).await, 5);

    // Leaving hands the lease over at once.
    engine.shutdown().await.unwrap();
    drop(wf);
    drop(engine);
    drop(remote);
    let (cfg, _standby) = tokio::time::timeout(Duration::from_secs(30), standby)
        .await
        .expect("the standby never got the lease")
        .unwrap()
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    let h = engine.run::<u32>("during");
    assert_eq!(h.result().await.unwrap(), 4);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

fn counted_chain(engine: &Engine, calls: Arc<AtomicU32>) -> fusee::WorkflowRef<u32, u32> {
    engine.register("chain", move |ctx: Ctx, n: u32| {
        let calls = calls.clone();
        async move {
            for i in 0..n {
                let c = calls.clone();
                ctx.step("count", move || {
                    let c = c.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, Error>(i)
                    }
                })
                .await?;
            }
            Ok(n)
        }
    })
}

async fn worker_at(addr: &str) -> (fusee::Config, fusee_server::remote::Remote) {
    let a = addr.to_owned();
    tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a))
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inline_flushes_over_a_real_server_return_durable_results() {
    let dir = tmp("inline-flush");
    let addr = start_server(&dir).await;
    let relay = Relay::start(addr.clone()).await;
    let calls = Arc::new(AtomicU32::new(0));
    let (cfg, _remote) = worker_at(&relay.addr).await;
    let engine = Engine::open(cfg).await.unwrap();
    let wf = counted_chain(&engine, calls.clone());
    for k in 0..3 {
        let h = engine
            .start(&wf, &format!("inline-{k}"), &40)
            .await
            .unwrap();
        assert_eq!(h.result().await.unwrap(), 40);
    }
    let m = engine.metrics().await.unwrap();
    eprintln!(
        "{} of {} flushes waited inline on the shard thread",
        m.inline_flushes, m.flushes
    );
    assert!(m.inline_flushes > 0, "no flush took the inline path: {m:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 120);

    // The worker dies without shutting down: every returned result must
    // already be on the server's disk.
    let standby = tokio::spawn(async move { worker_at(&addr).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    relay.kill();
    let (cfg, _second) = tokio::time::timeout(Duration::from_secs(30), standby)
        .await
        .expect("the standby never got the lease")
        .unwrap();
    let engine2 = Engine::open(cfg).await.unwrap();
    let _wf = counted_chain(&engine2, calls.clone());
    for k in 0..3 {
        let h = engine2.run::<u32>(&format!("inline-{k}"));
        assert_eq!(h.result().await.unwrap(), 40);
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        120,
        "acknowledged steps ran again"
    );
    engine2.shutdown().await.unwrap();
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_that_dies_between_submit_and_reply_loses_nothing_acknowledged() {
    let dir = tmp("crash-mid-flush");
    let (addr, server) = start_server_with(&dir).await;
    // No pings: every byte the server sends answers a request.
    server.set_liveness(None).unwrap();
    let relay = Relay::start(addr.clone()).await;
    let calls = Arc::new(AtomicU32::new(0));
    let (cfg, _remote) = worker_at(&relay.addr).await;
    let engine = Engine::open(cfg).await.unwrap();
    let wf = register(&engine, calls.clone());
    for i in 0..5u32 {
        assert_eq!(
            finish(&engine, &wf, &format!("done-{i}"), i, 100).await,
            i + 101
        );
    }
    let h = engine.start(&wf, "in-flight", &7).await.unwrap();
    for _ in 0..1000 {
        if calls.load(Ordering::SeqCst) == 6 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 6);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The next flush reaches the server, which writes it and replies; the
    // reply never reaches the worker, which then dies.
    relay.freeze_replies();
    let signal = tokio::spawn(async move { h.signal("go", &1).await });
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while relay.held() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the server never answered the flush"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let standby = tokio::spawn(async move { worker_at(&addr).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    relay.kill();
    let outcome = tokio::time::timeout(Duration::from_secs(10), signal)
        .await
        .expect("the interrupted signal never returned")
        .unwrap();
    assert!(
        outcome.is_err(),
        "a signal was acknowledged without the server's reply"
    );

    let (cfg, _second) = tokio::time::timeout(Duration::from_secs(30), standby)
        .await
        .expect("the standby never got the lease")
        .unwrap();
    let engine2 = Engine::open(cfg).await.unwrap();
    let _wf = register(&engine2, calls.clone());
    for i in 0..5u32 {
        let h = engine2.run::<u32>(&format!("done-{i}"));
        assert_eq!(h.result().await.unwrap(), i + 101);
    }
    // The server did write the unacknowledged signal, so the run resumes
    // with it; its recorded first step does not run again.
    let h = engine2.run::<u32>("in-flight");
    assert_eq!(h.result().await.unwrap(), 9);
    assert_eq!(calls.load(Ordering::SeqCst), 6);
    engine2.shutdown().await.unwrap();
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eight_shards_flush_in_parallel_on_the_server() {
    const SHARDS: u32 = 8;
    const STEPS: u32 = 6;
    let dir = tmp("parallel-shards");
    let server = Server::new(ServerConfig {
        dir: dir.clone(),
        shards: SHARDS,
        local: LocalOptions {
            flush: FlushMethod::Fsync,
            emulated_flush: Duration::from_millis(25),
            ..Default::default()
        },
        security: Default::default(),
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(fusee_server::server::serve_tcp(server, listener));
    let (cfg, _remote) = worker_at(&addr).await;
    let engine = Engine::open(cfg).await.unwrap();
    let wf = Arc::new(counted_chain(&engine, Arc::new(AtomicU32::new(0))));
    let mut ids = std::collections::BTreeMap::new();
    for k in 0.. {
        let id = format!("p-{k}");
        ids.entry(fusee_core::shard::shard_of(&id, SHARDS))
            .or_insert(id);
        if ids.len() == SHARDS as usize {
            break;
        }
    }
    let mut runs = Vec::new();
    for id in ids.into_values() {
        let (wf, engine) = (wf.clone(), engine.clone());
        runs.push(tokio::spawn(async move {
            engine
                .start(&wf, &id, &STEPS)
                .await
                .unwrap()
                .result()
                .await
                .unwrap()
        }));
    }
    for h in runs {
        assert_eq!(h.await.unwrap(), STEPS);
    }
    // Count the shard sections committed together in the actual log. Runner
    // scheduling or filesystem latency cannot turn a shared write into two.
    let mut batched_shards = std::collections::BTreeSet::new();
    for entry in std::fs::read_dir(dir.join("wal")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|ext| ext != "wal") {
            continue;
        }
        let bytes = std::fs::read(path).unwrap();
        if let Some(mut scanner) = fusee_core::wal::WalScanner::start(&bytes, true).unwrap() {
            scanner.advance(&bytes, true, bytes.len(), &mut |batch| {
                if batch.sections().count() > 1 {
                    batched_shards.extend(batch.sections().map(|section| section.shard));
                }
            });
        }
    }
    assert_eq!(
        batched_shards.len(),
        SHARDS as usize,
        "shards in shared writes: {batched_shards:?}"
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A scripted server for one worker: grants a plain lease, then answers each
/// `SUBMIT` by calling `answer` with the socket, the request ID and shard.
fn scripted_server(
    answer: impl Fn(&mut std::net::TcpStream, u64, u32) -> bool + Send + 'static,
) -> (String, std::thread::JoinHandle<()>) {
    use fusee_core::frame;
    use fusee_core::wire::{Reader, put_u32, put_u64};
    use fusee_server::proto::{self, req, resp};
    use std::io::Write;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        let mut dec = frame::Decoder::new();
        let (kind, _) = next_frame(&mut sock, &mut dec).unwrap();
        assert_eq!(kind, req::HELLO);
        let mut b = Vec::new();
        put_u64(&mut b, 1);
        put_u32(&mut b, 1);
        put_u64(&mut b, 3);
        put_u32(&mut b, 0);
        let mut out = Vec::new();
        frame::encode(resp::LEASE, &b, &mut out);
        sock.write_all(&out).unwrap();
        while let Some((kind, body)) = next_frame(&mut sock, &mut dec) {
            if kind == req::SUBMIT {
                let head = proto::read_head(&mut Reader::new(&body)).unwrap();
                if !answer(&mut sock, head.id, head.shard) {
                    return;
                }
            }
        }
    });
    (addr, server)
}

fn flushed(sock: &mut std::net::TcpStream, id: u64, shard: u32, lsn: u64) {
    use fusee_core::frame;
    use fusee_core::wire::{put_u32, put_u64};
    use std::io::Write;
    let mut b = Vec::new();
    put_u64(&mut b, id);
    put_u32(&mut b, shard);
    put_u64(&mut b, lsn);
    let mut out = Vec::new();
    frame::encode(fusee_server::proto::resp::FLUSHED, &b, &mut out);
    sock.write_all(&out).unwrap();
}

fn batch(last_lsn: u64) -> fusee_core::segment::Batch {
    fusee_core::segment::Batch {
        chunks: Vec::new(),
        first_lsn: last_lsn,
        last_lsn,
        records: 1,
        bytes: 0,
        section: Vec::new(),
    }
}

#[test]
fn an_inline_flush_takes_its_reply_from_the_reader_without_waking_the_runtime() {
    use fusee::env::Env;
    let replies = Arc::new(AtomicU32::new(0));
    let r = replies.clone();
    let (addr, server) = scripted_server(move |sock, id, shard| {
        // The first reply comes long after any spin budget: the shard thread
        // parks and the reader thread wakes it.
        if r.fetch_add(1, Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(30));
        }
        flushed(sock, id, shard, 40 + u64::from(r.load(Ordering::SeqCst)));
        true
    });
    let remote = fusee_server::remote::Remote::connect(&addr).unwrap();
    let mut env = remote.env(0);
    let wakes = Arc::new(AtomicU32::new(0));
    let w = wakes.clone();
    env.set_notify(Arc::new(move || {
        w.fetch_add(1, Ordering::SeqCst);
    }));
    assert!(env.supports_inline());
    assert_eq!(env.flush_inline(batch(41)).unwrap(), 41);
    assert!(
        env.last_flush_us() >= 30_000,
        "the slow round trip was not reported: {} µs",
        env.last_flush_us()
    );
    for lsn in 42..52 {
        assert_eq!(env.flush_inline(batch(lsn)).unwrap(), lsn);
    }
    assert!(env.last_flush_us() < 30_000);
    assert!(
        env.poll_flushed().is_none(),
        "an inline reply was left for the runtime"
    );
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        0,
        "inline replies woke the runtime"
    );
    drop(env);
    drop(remote);
    server.join().unwrap();
}

#[test]
fn an_inline_flush_fails_at_once_when_the_connection_drops() {
    use fusee::env::Env;
    let (addr, server) = scripted_server(|sock, _, _| {
        let _ = sock.shutdown(std::net::Shutdown::Both);
        false
    });
    let remote = fusee_server::remote::Remote::connect(&addr).unwrap();
    let mut env = remote.env(0);
    let t = std::time::Instant::now();
    let err = env.flush_inline(batch(7)).unwrap_err().to_string();
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    assert!(err.contains("lost"), "{err}");
    // The shard stays stopped.
    assert!(env.flush_inline(batch(8)).is_err());
    server.join().unwrap();
}

/// A server on `dir` that [`stop`] can close completely, releasing the
/// directory for the next one.
async fn open_server(
    dir: &std::path::Path,
    shared_log: bool,
) -> (
    String,
    Arc<Server>,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
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
        shared_log,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(fusee_server::server::serve_tcp(server.clone(), listener));
    (addr, server, task)
}

/// Stops listening and waits until every connection of `server` ended.
async fn stop(server: Arc<Server>, task: tokio::task::JoinHandle<std::io::Result<()>>) {
    task.abort();
    let _ = task.await;
    for _ in 0..1000 {
        if Arc::strong_count(&server) == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        Arc::strong_count(&server),
        1,
        "a connection outlived its worker"
    );
}

/// Opens a worker engine with the `job` workflow registered.
async fn job_worker(addr: &str, calls: Arc<AtomicU32>) -> (Engine, fusee::WorkflowRef<u32, u32>) {
    let a = addr.to_owned();
    let cfg = tokio::task::spawn_blocking(move || remote_config(&a))
        .await
        .unwrap()
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    let wf = register(&engine, calls);
    (engine, wf)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_format_2_server_directory_opens_as_it_is_and_upgrades_to_the_shared_log() {
    let dir = tmp("format-2");
    let calls = Arc::new(AtomicU32::new(0));
    // A server directory with a log per shard (format 2).
    let (addr, server, task) = open_server(&dir, false).await;
    assert!(!server.shared_log());
    let (engine, wf) = job_worker(&addr, calls.clone()).await;
    for i in 0..10u32 {
        engine.start(&wf, &format!("job-{i}"), &i).await.unwrap();
    }
    while calls.load(Ordering::SeqCst) < 10 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    engine.shutdown().await.unwrap();
    drop((wf, engine));
    stop(server, task).await;
    assert!(
        dir.join("shard-000/log")
            .read_dir()
            .unwrap()
            .next()
            .is_some()
    );

    // A server with the shared log opens it as it is.
    let (addr, server, task) = open_server(&dir, true).await;
    assert!(
        !server.shared_log(),
        "an existing directory keeps its format"
    );
    let (engine, wf) = job_worker(&addr, calls.clone()).await;
    for i in 0..5u32 {
        let h = engine.run::<u32>(&format!("job-{i}"));
        h.signal("go", &100).await.unwrap();
        assert_eq!(h.result().await.unwrap(), i + 101);
    }
    engine.shutdown().await.unwrap();
    drop((wf, engine));
    stop(server, task).await;

    // `fusee upgrade` converts the stopped server's directory.
    let local = LocalOptions {
        flush: FlushMethod::Fsync,
        ..Default::default()
    };
    match fusee::upgrade::upgrade_to_shared_log(&dir, &local).unwrap() {
        fusee::upgrade::Upgraded::Converted { records } => {
            assert!(records.iter().sum::<u64>() > 0)
        }
        other => panic!("not converted: {other:?}"),
    }
    let (addr, server, task) = open_server(&dir, true).await;
    assert!(server.shared_log());
    let (engine, wf) = job_worker(&addr, calls.clone()).await;
    for i in 0..10u32 {
        let h = engine.run::<u32>(&format!("job-{i}"));
        if i >= 5 {
            h.signal("go", &100).await.unwrap();
        }
        assert_eq!(h.result().await.unwrap(), i + 101);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 10, "recorded steps ran again");
    engine.shutdown().await.unwrap();
    drop((wf, engine));
    stop(server, task).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A directory whose meta file says `log=3` while it keeps a log per shard
/// opens as format 2 and its meta file is corrected; it is never read as an
/// empty shared log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_server_directory_mislabelled_as_the_shared_log_keeps_its_records() {
    let dir = tmp("mislabelled");
    let calls = Arc::new(AtomicU32::new(0));
    let (addr, server, task) = open_server(&dir, false).await;
    let (engine, wf) = job_worker(&addr, calls.clone()).await;
    for i in 0..6u32 {
        engine.start(&wf, &format!("job-{i}"), &i).await.unwrap();
    }
    while calls.load(Ordering::SeqCst) < 6 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    engine.shutdown().await.unwrap();
    drop((wf, engine));
    stop(server, task).await;
    let meta = dir.join("fusee.meta");
    let text = std::fs::read_to_string(&meta).unwrap();
    std::fs::write(&meta, text.replace("log=2", "log=3")).unwrap();

    let (addr, server, task) = open_server(&dir, true).await;
    assert!(!server.shared_log());
    assert!(std::fs::read_to_string(&meta).unwrap().contains("log=2"));
    let (engine, wf) = job_worker(&addr, calls.clone()).await;
    for i in 0..6u32 {
        let h = engine.run::<u32>(&format!("job-{i}"));
        h.signal("go", &1).await.unwrap();
        assert_eq!(h.result().await.unwrap(), i + 2);
    }
    engine.shutdown().await.unwrap();
    drop((wf, engine));
    stop(server, task).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_without_the_shared_log_is_refused_with_a_clear_message() {
    use fusee_core::frame;
    use fusee_core::wire::{Reader, put_u32, put_u64};
    use fusee_server::proto::{self, req, resp};
    use std::io::Write;
    let dir = tmp("old-worker");
    let addr = start_server(&dir).await;
    tokio::task::spawn_blocking(move || {
        let mut sock = std::net::TcpStream::connect(&addr).unwrap();
        let mut hello = Vec::new();
        put_u64(&mut hello, 1);
        // A worker from before the shared log offers only these.
        put_u32(&mut hello, proto::STORE_OFFSETS | proto::LEASE_LIVENESS);
        let mut out = Vec::new();
        frame::encode(req::HELLO, &hello, &mut out);
        sock.write_all(&out).unwrap();
        let mut dec = frame::Decoder::new();
        let (kind, body) = next_frame(&mut sock, &mut dec).unwrap();
        assert_eq!(kind, resp::ERR);
        let mut r = Reader::new(&body);
        r.u64().unwrap();
        let message = r.str().unwrap();
        assert!(message.contains("log format 3"), "{message}");
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A mass wake loads runs parked one after another: their store reads
/// continue a scan, and most are served from what earlier reads fetched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waking_runs_parked_in_order_reads_the_store_in_few_round_trips() {
    const RUNS: u32 = 300;
    let dir = tmp("mass-wake");
    let addr = start_server(&dir).await;
    let (mut cfg, remote) = worker_at(&addr).await;
    cfg.park_after = Duration::from_millis(1);
    let engine = Engine::open(cfg).await.unwrap();
    let calls = Arc::new(AtomicU32::new(0));
    let wf = register(&engine, calls.clone());
    for i in 0..RUNS {
        engine
            .start(&wf, &format!("wake-{i:04}"), &i)
            .await
            .unwrap();
    }
    // Every run reaches its wait and parks.
    for _ in 0..2000 {
        let m = engine.metrics().await.unwrap();
        if m.runs_parked >= u64::from(RUNS) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(engine.metrics().await.unwrap().runs_parked >= u64::from(RUNS));
    let before = remote.store_round_trips();
    for i in 0..RUNS {
        engine
            .run::<u32>(&format!("wake-{i:04}"))
            .signal("go", &1)
            .await
            .unwrap();
    }
    for i in 0..RUNS {
        let h = engine.run::<u32>(&format!("wake-{i:04}"));
        assert_eq!(h.result().await.unwrap(), i + 2);
    }
    let reads = remote.store_round_trips() - before;
    eprintln!("{RUNS} parked runs woke with {reads} run-store round trips");
    assert!(
        reads < u64::from(RUNS) / 4,
        "{reads} round trips for {RUNS} wakes"
    );
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

async fn start_server_shards(dir: &std::path::Path, shards: u32) -> (String, Arc<Server>) {
    let server = Server::new(ServerConfig {
        dir: dir.to_owned(),
        shards,
        local: LocalOptions {
            flush: FlushMethod::Fsync,
            ..Default::default()
        },
        security: Default::default(),
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(fusee_server::server::serve_tcp(server.clone(), listener));
    (addr, server)
}

/// `per` workflow IDs on each of `shards` shards.
fn ids_by_shard(prefix: &str, shards: u32, per: usize) -> Vec<Vec<String>> {
    let mut ids: Vec<Vec<String>> = vec![Vec::new(); shards as usize];
    for k in 0.. {
        let id = format!("{prefix}-{k}");
        let s = fusee_core::shard::shard_of(&id, shards) as usize;
        if ids[s].len() < per {
            ids[s].push(id);
        }
        if ids.iter().all(|v| v.len() == per) {
            return ids;
        }
    }
    unreachable!()
}

/// A worker engine serving relayed calls, holding at most `most` shards.
struct ShardWorker {
    engine: Engine,
    remote: fusee_server::remote::Remote,
    calls: tokio::task::JoinHandle<()>,
}

impl ShardWorker {
    /// Shuts the engine down and closes the connection.
    async fn stop(self) {
        self.calls.abort();
        let _ = self.calls.await;
        let _ = self.engine.shutdown().await;
    }
}

async fn shard_worker(addr: &str, most: u32, calls: Arc<AtomicU32>) -> ShardWorker {
    let a = format!("{addr}?max-shards={most}");
    let (cfg, remote) = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a))
        .await
        .unwrap()
        .unwrap();
    let engine = Engine::open(cfg).await.unwrap();
    register(&engine, calls);
    let calls = tokio::spawn(fusee_server::serve_calls(remote.clone(), engine.clone()));
    ShardWorker {
        engine,
        remote,
        calls,
    }
}

async fn client_call<T: Send + 'static>(
    client: &fusee_server::Client,
    f: impl FnOnce(&fusee_server::Client) -> T + Send + 'static,
) -> T {
    let c = client.clone();
    tokio::task::spawn_blocking(move || f(&c)).await.unwrap()
}

fn start_job(c: &fusee_server::Client, id: &str, n: u32) {
    // A start can race a shard changing hands; it is idempotent by ID.
    for _ in 0..100 {
        match c.start("job", id, &serde_json::json!(n)) {
            Ok(()) | Err(Error::AlreadyExists(_)) => return,
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    panic!("{id} never started");
}

fn finish_job(c: &fusee_server::Client, id: &str, go: u32) -> u32 {
    for _ in 0..500 {
        if c.signal(id, "go", &serde_json::json!(go)).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    for _ in 0..500 {
        if let Ok(v) = c.result(id) {
            return serde_json::from_value(v).unwrap();
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("{id} never finished");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_workers_run_their_own_shards_and_a_hung_one_loses_only_its_shards() {
    const SHARDS: u32 = 4;
    let dir = tmp("two-workers");
    let (addr, server) = start_server_shards(&dir, SHARDS).await;
    server.set_liveness(Some(FAST)).unwrap();
    let relay = Relay::start(addr.clone()).await;
    let (calls_a, calls_b, calls_c) = (
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
    );
    // A reaches the server through a relay that can freeze it.
    let a = shard_worker(&relay.addr, 2, calls_a.clone()).await;
    let b = shard_worker(&addr, 2, calls_b.clone()).await;
    let held_a = a.remote.held_shards();
    let held_b = b.remote.held_shards();
    assert_eq!(held_a.len(), 2, "{held_a:?}");
    assert_eq!(held_b.len(), 2, "{held_b:?}");
    assert!(held_a.iter().all(|s| !held_b.contains(s)));

    // Calls from anywhere reach the worker holding the workflow's shard,
    // and both workers run workflows at once.
    let client = fusee_server::Client::connect(&addr).unwrap();
    let ids = ids_by_shard("w", SHARDS, 3);
    for shard in &ids {
        for id in shard {
            let id = id.clone();
            client_call(&client, move |c| start_job(c, &id, 1)).await;
        }
    }
    while calls_a.load(Ordering::SeqCst) + calls_b.load(Ordering::SeqCst) < 12 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        calls_a.load(Ordering::SeqCst),
        6,
        "A ran its two shards' runs"
    );
    assert_eq!(
        calls_b.load(Ordering::SeqCst),
        6,
        "B ran its two shards' runs"
    );
    // B's runs finish; A's wait for their signal.
    for &s in &held_b {
        for id in &ids[s as usize] {
            let id = id.clone();
            assert_eq!(
                client_call(&client, move |c| finish_job(c, &id, 10)).await,
                12
            );
        }
    }

    // A hangs with its connection open. C waits as a standby.
    relay.freeze(true);
    let frozen_at = std::time::Instant::now();
    let c_addr = addr.clone();
    let calls = calls_c.clone();
    let standby = tokio::spawn(async move { shard_worker(&c_addr, 2, calls).await });
    // B keeps working while A is hung.
    let more = ids_by_shard("during", SHARDS, 2);
    for &s in &held_b {
        for id in &more[s as usize] {
            let (id, id2) = (id.clone(), id.clone());
            client_call(&client, move |c| start_job(c, &id, 5)).await;
            assert_eq!(
                client_call(&client, move |c| finish_job(c, &id2, 1)).await,
                7
            );
        }
    }
    let c = tokio::time::timeout(Duration::from_secs(30), standby)
        .await
        .expect("C never got A's shards")
        .unwrap();
    assert!(frozen_at.elapsed() >= FAST.timeout - FAST.ping_interval * 2);
    let mut held_c = c.remote.held_shards();
    held_c.sort();
    let mut was_a = held_a.clone();
    was_a.sort();
    assert_eq!(held_c, was_a, "C took exactly A's shards");
    assert_eq!(b.remote.held_shards(), held_b, "B kept its shards");
    // A's acknowledged runs resume on C without running recorded steps.
    for &s in &held_a {
        for id in &ids[s as usize] {
            let id = id.clone();
            assert_eq!(
                client_call(&client, move |c| finish_job(c, &id, 100)).await,
                102
            );
        }
    }
    assert_eq!(
        calls_c.load(Ordering::SeqCst),
        0,
        "recorded steps ran again on C"
    );

    // A, thawed, is fenced: nothing it writes lands.
    relay.freeze(false);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let late = &ids_by_shard("late", SHARDS, 1)[held_a[0] as usize][0];
    loop {
        let run = a.engine.run::<u32>(late);
        match tokio::time::timeout(Duration::from_secs(2), run.signal("x", &0)).await {
            Ok(Err(_)) => break,
            Ok(Ok(())) => panic!("the revoked worker had a signal acknowledged"),
            Err(_) => {}
        }
        assert!(std::time::Instant::now() < deadline, "A never noticed");
    }
    a.stop().await;
    b.stop().await;
    c.stop().await;
    client.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_workers_share_four_shards_and_the_standby_takes_over_a_departed_one() {
    const SHARDS: u32 = 4;
    let dir = tmp("three-workers");
    let (addr, _server) = start_server_shards(&dir, SHARDS).await;
    let calls: Vec<Arc<AtomicU32>> = (0..3).map(|_| Arc::new(AtomicU32::new(0))).collect();
    let a = shard_worker(&addr, 2, calls[0].clone()).await;
    let b = shard_worker(&addr, 2, calls[1].clone()).await;
    // Every shard is held: C waits.
    let c_addr = addr.clone();
    let c_calls = calls[2].clone();
    let standby = tokio::spawn(async move { shard_worker(&c_addr, 2, c_calls).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !standby.is_finished(),
        "a third worker holds no shard while two hold all"
    );

    let client = fusee_server::Client::connect(&addr).unwrap();
    let ids = ids_by_shard("t", SHARDS, 2);
    for shard in &ids {
        for id in shard {
            let id = id.clone();
            client_call(&client, move |c| start_job(c, &id, 3)).await;
        }
    }
    while calls[0].load(Ordering::SeqCst) + calls[1].load(Ordering::SeqCst) < 8 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // A listing covers the shards of every worker.
    let listed = client_call(&client, |c| c.list_where(&serde_json::json!({}), 10))
        .await
        .unwrap();
    let mut listed: Vec<String> = listed
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_owned())
        .collect();
    listed.sort();
    let mut all: Vec<String> = ids.iter().flatten().cloned().collect();
    all.sort();
    assert_eq!(listed, all, "a listing over shards held by several workers");

    // A leaves cleanly; C takes its shards.
    let held_a = a.remote.held_shards();
    a.stop().await;
    let c = tokio::time::timeout(Duration::from_secs(30), standby)
        .await
        .expect("C never got a shard")
        .unwrap();
    assert_eq!(c.remote.held_shards(), held_a);
    for shard in &ids {
        for id in shard {
            let id = id.clone();
            assert_eq!(
                client_call(&client, move |c| finish_job(c, &id, 1)).await,
                5
            );
        }
    }
    assert_eq!(
        calls[2].load(Ordering::SeqCst),
        0,
        "recorded steps ran again"
    );
    // B's engine is the only one that ran B's shards throughout.
    assert_eq!(b.remote.held_shards().len(), 2);
    b.stop().await;
    c.stop().await;
    client.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Without `max-shards`, the first worker takes every shard, and an engine
/// opens even while some of its shards are held elsewhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_takes_every_shard_unless_it_asks_for_fewer() {
    let dir = tmp("all-shards");
    let (addr, _server) = start_server_shards(&dir, 3).await;
    let calls = Arc::new(AtomicU32::new(0));
    let one = shard_worker(&addr, 1, calls.clone()).await;
    assert_eq!(one.remote.held_shards().len(), 1);
    let a = addr.clone();
    let (cfg, all) = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(all.held_shards().len(), 2, "takes every free shard");
    let second = Engine::open(cfg).await.unwrap();
    register(&second, calls.clone());
    one.stop().await;
    for _ in 0..500 {
        if all.held_shards().len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(all.held_shards().len(), 3, "and the rest once they free up");
    let wf = second.register("job2", |_ctx: Ctx, n: u32| async move { Ok(n) });
    for i in 0..12u32 {
        assert_eq!(
            second
                .start(&wf, &format!("z-{i}"), &i)
                .await
                .unwrap()
                .result()
                .await
                .unwrap(),
            i
        );
    }
    second.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A run whose journal is longer than a protocol request carries, and than
/// the bound of a single payload: its frame is appended to the server's run
/// store and read back in pieces, and the run continues from it. (The frame
/// was sent as one request, which the server refused; and a journal over
/// 64 MiB was written but could not be decoded again.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_with_a_long_journal_parks_and_wakes_through_the_server() {
    const STEPS: u32 = 6;
    const STEP_BYTES: usize = 12 << 20;
    let dir = tmp("long-journal");
    let addr = start_server(&dir).await;
    let (mut cfg, remote) = worker_at(&addr).await;
    cfg.park_after = Duration::from_millis(1);
    let engine = Engine::open(cfg).await.unwrap();
    let wf = engine.register("hoard", |ctx: Ctx, n: u32| async move {
        let mut total = 0u64;
        for i in 0..n {
            let s: String = ctx
                .step("load", move || async move {
                    Ok::<_, Error>("x".repeat(STEP_BYTES + i as usize))
                })
                .await?;
            total += s.len() as u64;
        }
        let go: Option<u32> = ctx.wait_for("go").await?;
        Ok(total + u64::from(go.unwrap_or(0)))
    });
    let run = engine.start(&wf, "hoard-1", &STEPS).await.unwrap();
    for _ in 0..6000 {
        if engine.metrics().await.unwrap().runs_parked >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(engine.metrics().await.unwrap().runs_parked >= 1, "parked");
    let before = remote.store_round_trips();
    engine
        .run::<u64>("hoard-1")
        .signal("go", &7u32)
        .await
        .unwrap();
    let expected: u64 = (0..STEPS)
        .map(|i| STEP_BYTES as u64 + u64::from(i))
        .sum::<u64>()
        + 7;
    assert_eq!(run.result().await.unwrap(), expected);
    let reads = remote.store_round_trips() - before;
    assert!(reads >= 3, "{reads} reads of a 72 MiB frame");
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
