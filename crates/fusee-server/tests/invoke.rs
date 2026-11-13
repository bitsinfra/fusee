//! Workers invoked over HTTP. Workflows sleep, take signals and finish with
//! no worker alive between invocations (each invocation a fresh process);
//! a process that aborts mid-step is recovered by the next invocation, and
//! one that outlives its invocation is fenced; a failing, slow endpoint
//! backs off within its concurrency; HTTPS with client certificates and the
//! signature; long-lived workers alongside invoked ones; and children whose
//! results cross shards between invocations.
//!
//! The fresh processes are this test binary started again with
//! `FUSEE_INVOKE_CHILD` set: [`invocation_child`] then answers one
//! invocation and exits.

use fusee::{Ctx, Engine, Error, FlushMethod, LocalOptions};
use fusee_server::invoke::{Endpoint, Handler, Reply, Secret};
use fusee_server::security::Tls;
use fusee_server::{Client, Server, ServerConfig};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SECRET: &[u8] = b"an invocation secret of 32 bytes";
const REPLY: &str = "FUSEE-INVOKE-REPLY";

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("fusee-invoke-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

async fn start_server(dir: &Path, shards: u32) -> (String, Arc<Server>) {
    let server = Server::new(ServerConfig {
        dir: dir.join("server"),
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

/// Where step bodies record that they ran: `key value` lines.
#[derive(Clone)]
enum Sink {
    Memory(Arc<Mutex<Vec<(String, String)>>>),
    File(PathBuf),
}

impl Sink {
    fn memory() -> Self {
        Sink::Memory(Default::default())
    }

    fn record(&self, key: &str, value: &str) {
        match self {
            Sink::Memory(m) => m.lock().unwrap().push((key.into(), value.into())),
            Sink::File(path) => {
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .unwrap();
                f.write_all(format!("{key} {value}\n").as_bytes()).unwrap();
            }
        }
    }

    fn entries(&self) -> Vec<(String, String)> {
        match self {
            Sink::Memory(m) => m.lock().unwrap().clone(),
            Sink::File(path) => std::fs::read_to_string(path)
                .unwrap_or_default()
                .lines()
                .filter_map(|l| l.split_once(' '))
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
        }
    }

    fn count(&self, key: &str) -> usize {
        self.entries().iter().filter(|(k, _)| k == key).count()
    }

    fn values(&self, key: &str) -> Vec<String> {
        self.entries()
            .into_iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v)
            .collect()
    }
}

/// A step that records `who` under `workflow/step` each time its body runs.
async fn counted<T>(ctx: &Ctx, sink: &Sink, name: &str, who: &str, v: T) -> fusee::Result<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Clone + 'static,
{
    let key = format!("{}/{name}", ctx.workflow_id());
    let (sink, who) = (sink.clone(), who.to_owned());
    ctx.step(name, move || {
        let (sink, key, who, v) = (sink.clone(), key.clone(), who.clone(), v.clone());
        async move {
            sink.record(&key, &who);
            Ok::<_, Error>(v)
        }
    })
    .await
}

/// Registers the test workflows; `who` names the process or worker in the
/// sink, and `marks` is where the crash and freeze workflows leave marks.
fn register(engine: &Engine, sink: Sink, who: &str, marks: Option<PathBuf>) {
    let (fx, me) = (sink.clone(), who.to_owned());
    // A step, a sleep, a signal, another step.
    engine.register("sleeper", move |ctx: Ctx, n: u32| {
        let (fx, me) = (fx.clone(), me.clone());
        async move {
            let a: u32 = counted(&ctx, &fx, "first", &me, n + 1).await?;
            ctx.sleep("nap", Duration::from_millis(1200)).await?;
            let go: u32 = ctx.wait_for("go").await?.unwrap_or(0);
            let b: u32 = counted(&ctx, &fx, "second", &me, a * 10 + go).await?;
            Ok(b)
        }
    });
    let (fx, me) = (sink.clone(), who.to_owned());
    engine.register("quick", move |ctx: Ctx, n: u32| {
        let (fx, me) = (fx.clone(), me.clone());
        async move { counted(&ctx, &fx, "only", &me, n * 2).await }
    });
    engine.register("parent", |ctx: Ctx, child: String| async move {
        let v: u32 = ctx.child("quick", &child, &21u32).await?;
        Ok(v + 1)
    });
    // `n` steps of `ms` each (waiting on the reactor, not blocking it).
    let (fx, me) = (sink.clone(), who.to_owned());
    engine.register("long", move |ctx: Ctx, (n, ms): (u32, u64)| {
        let (fx, me) = (fx.clone(), me.clone());
        async move {
            for i in 0..n {
                let key = format!("{}/s{i}", ctx.workflow_id());
                let (fx, me) = (fx.clone(), me.clone());
                ctx.step(&format!("s{i}"), move || {
                    let (fx, key, me) = (fx.clone(), key.clone(), me.clone());
                    async move {
                        tokio::time::sleep(Duration::from_millis(ms)).await;
                        fx.record(&key, &me);
                        Ok::<_, Error>(())
                    }
                })
                .await?;
            }
            Ok(n)
        }
    });
    // Step "before", then step "work", whose first run aborts the process
    // ("crash") or blocks it past its invocation ("freeze"). Returns the
    // process that recorded "work".
    for mode in ["crash", "freeze"] {
        let (fx, me, marks) = (sink.clone(), who.to_owned(), marks.clone());
        engine.register(mode, move |ctx: Ctx, _: u32| {
            let (fx, me, marks) = (fx.clone(), me.clone(), marks.clone());
            async move {
                counted(&ctx, &fx, "before", &me, 1u32).await?;
                let key = format!("{}/work", ctx.workflow_id());
                let pid: String = ctx
                    .step("work", move || {
                        let (fx, key, me, marks) =
                            (fx.clone(), key.clone(), me.clone(), marks.clone());
                        async move {
                            fx.record(&key, &me);
                            if let Some(marks) = &marks {
                                let mark = marks.join(mode);
                                if !mark.exists() {
                                    std::fs::write(&mark, &me).unwrap();
                                    if mode == "crash" {
                                        std::process::abort();
                                    }
                                    // Past the budget and its grace, on the
                                    // shard's own thread.
                                    std::thread::sleep(Duration::from_secs(5));
                                }
                            }
                            Ok::<_, Error>(me)
                        }
                    })
                    .await?;
                Ok(pid)
            }
        });
    }
}

/// Not a test of its own: one invocation in a fresh process.
#[test]
fn invocation_child() {
    let Some(addr) = std::env::var_os("FUSEE_INVOKE_CHILD") else {
        return;
    };
    let addr = addr.to_string_lossy().into_owned();
    let sink = Sink::File(std::env::var_os("FUSEE_INVOKE_EFFECTS").unwrap().into());
    let marks: PathBuf = std::env::var_os("FUSEE_INVOKE_MARKS").unwrap().into();
    let sig = std::env::var("FUSEE_INVOKE_SIG").ok();
    let body = std::env::var("FUSEE_INVOKE_BODY").unwrap();
    let who = std::process::id().to_string();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let reply = rt.block_on(async move {
        let handler = Handler::new(addr, SECRET, move |engine: &Engine| {
            register(engine, sink.clone(), &who, Some(marks.clone()))
        })
        .unwrap();
        handler.handle(sig.as_deref(), body.as_bytes()).await
    });
    println!(
        "{REPLY} {} {}",
        reply.status,
        String::from_utf8_lossy(&reply.body)
    );
}

/// An endpoint on localhost that runs each invocation in a fresh process.
/// Returns its URL and the invocations it started.
async fn process_endpoint(addr: &str, dir: &Path) -> (String, Arc<AtomicU32>) {
    let started = Arc::new(AtomicU32::new(0));
    let (n, addr, dir) = (started.clone(), addr.to_owned(), dir.to_owned());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/invoke", listener.local_addr().unwrap());
    tokio::spawn(fusee_server::invoke::serve_with(
        listener,
        None,
        move |sig: Option<String>, body: Vec<u8>| {
            let (n, addr, dir) = (n.clone(), addr.clone(), dir.clone());
            async move {
                n.fetch_add(1, Ordering::Relaxed);
                let out = tokio::task::spawn_blocking(move || {
                    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
                    cmd.args([
                        "invocation_child",
                        "--exact",
                        "--nocapture",
                        "--test-threads=1",
                    ])
                    .env("FUSEE_INVOKE_CHILD", &addr)
                    .env("FUSEE_INVOKE_EFFECTS", dir.join("effects"))
                    .env("FUSEE_INVOKE_MARKS", &dir)
                    .env("FUSEE_INVOKE_BODY", String::from_utf8_lossy(&body).as_ref());
                    if let Some(s) = sig {
                        cmd.env("FUSEE_INVOKE_SIG", s);
                    }
                    if std::env::var_os("FUSEE_INVOKE_TRACE").is_some() {
                        cmd.stderr(std::process::Stdio::inherit());
                    }
                    cmd.output().unwrap()
                })
                .await
                .unwrap();
                let stdout = String::from_utf8_lossy(&out.stdout);
                // The harness may print its own words before ours.
                match stdout
                    .lines()
                    .find_map(|l| l.split_once(REPLY).map(|(_, r)| r))
                {
                    Some(line) => {
                        let (status, body) = line.trim().split_once(' ').unwrap();
                        Reply {
                            status: status.parse().unwrap(),
                            body: body.as_bytes().to_vec(),
                        }
                    }
                    // The process died before answering.
                    None => Reply {
                        status: 502,
                        body: br#"{"error":"the worker process died"}"#.to_vec(),
                    },
                }
            }
        },
    ));
    (url, started)
}

/// An endpoint whose handler opens a fresh engine in this process per
/// invocation. Returns its URL and how many engines it opened.
async fn engine_endpoint(addr: &str, sink: Sink, tls: Option<Tls>) -> (String, Arc<AtomicU32>) {
    let opened = Arc::new(AtomicU32::new(0));
    let n = opened.clone();
    let handler = Handler::new(addr, SECRET, move |engine: &Engine| {
        n.fetch_add(1, Ordering::Relaxed);
        register(engine, sink.clone(), "invoked", None);
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let scheme = if tls.is_some() { "https" } else { "http" };
    let url = format!("{scheme}://{}/invoke", listener.local_addr().unwrap());
    tokio::spawn(fusee_server::invoke::serve(listener, handler, tls));
    (url, opened)
}

fn secret() -> Option<Secret> {
    Some(Secret::new(SECRET).unwrap())
}

fn id_on(prefix: &str, shard: u32, of: u32) -> String {
    (0..)
        .map(|k| format!("{prefix}-{k}"))
        .find(|id| fusee_core::shard::shard_of(id, of) == shard)
        .unwrap()
}

async fn client(addr: &str) -> Client {
    let a = addr.to_owned();
    tokio::task::spawn_blocking(move || Client::connect(&a))
        .await
        .unwrap()
        .unwrap()
}

/// Runs a blocking client call off the runtime.
async fn call<T: Send + 'static>(c: &Client, f: impl FnOnce(&Client) -> T + Send + 'static) -> T {
    let c = c.clone();
    tokio::task::spawn_blocking(move || f(&c)).await.unwrap()
}

/// Starts a run whose worker may go away before it answers: starts again,
/// as a client does, attaching to the run if it exists, running or finished
/// (the first start may have run to completion before the retry arrives:
/// with `use_existing` for running runs only, the retry would start a second
/// run of the ID).
async fn start_attached(c: &Client, wtype: &'static str, id: &'static str) {
    loop {
        let opts = fusee::StartOptions::default().id_policy(fusee::IdPolicy::ATTACH);
        match call(c, move |c| {
            c.start_with(wtype, id, &serde_json::json!(0), &opts)
        })
        .await
        {
            Ok(()) => return,
            Err(Error::Io(m)) if m.contains("went away") => continue,
            Err(e) => panic!("{e}"),
        }
    }
}

async fn result(c: &Client, id: &str, within: Duration) -> serde_json::Value {
    let id = id.to_owned();
    tokio::time::timeout(within, call(c, move |c| c.result(&id)))
        .await
        .expect("the workflow finished in time")
        .unwrap()
}

/// Waits until no invocation is in flight and no shard is due.
async fn wait_quiet(server: &Server, within: Duration) {
    let deadline = Instant::now() + within;
    loop {
        let m = server.invocation_metrics().unwrap();
        if m.invoking_shards == 0
            && m.due_shards == 0
            && m.endpoints.iter().all(|e| e.in_flight == 0)
        {
            return;
        }
        assert!(Instant::now() < deadline, "invocations did not end: {m:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for(what: &str, within: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflows_sleep_take_signals_and_finish_with_no_process_between_invocations() {
    let dir = tmp("sleep");
    let (addr, server) = start_server(&dir, 2).await;
    let (url, processes) = process_endpoint(&addr, &dir).await;
    let sink = Sink::File(dir.join("effects"));
    server
        .set_invokers(vec![Endpoint::new(&url, secret())])
        .unwrap();
    // Every shard is invoked once at startup, and the invocations end.
    wait_quiet(&server, Duration::from_secs(20)).await;
    let c = client(&addr).await;
    let ids: Vec<String> = (0..2).map(|s| id_on("sleeper", s, 2)).collect();
    for (k, id) in ids.iter().enumerate() {
        let id = id.clone();
        call(&c, move |c| {
            c.start("sleeper", &id, &serde_json::json!(k as u32))
        })
        .await
        .unwrap();
    }
    wait_for("the first steps", Duration::from_secs(20), || {
        ids.iter().all(|id| sink.count(&format!("{id}/first")) == 1)
    })
    .await;
    // While the runs sleep, no worker process holds a shard.
    wait_quiet(&server, Duration::from_secs(10)).await;
    // The sleep ends with nobody up: the timer invokes a process, the run
    // then waits for its signal, and that process ends too.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    wait_quiet(&server, Duration::from_secs(10)).await;
    for (k, id) in ids.iter().enumerate() {
        let id2 = id.clone();
        call(&c, move |c| {
            c.signal(&id2, "go", &serde_json::json!(k as u32 + 5))
        })
        .await
        .unwrap();
        let v = result(&c, id, Duration::from_secs(20)).await;
        assert_eq!(v, serde_json::json!((k as u32 + 1) * 10 + k as u32 + 5));
    }
    // Each step ran once, in some process, though no process saw it all.
    let mut pids = std::collections::BTreeSet::new();
    for id in &ids {
        assert_eq!(sink.count(&format!("{id}/first")), 1, "{id}");
        assert_eq!(sink.count(&format!("{id}/second")), 1, "{id}");
        pids.extend(sink.values(&format!("{id}/first")));
        pids.extend(sink.values(&format!("{id}/second")));
    }
    assert!(pids.len() >= 2, "steps ran in {pids:?}");
    wait_quiet(&server, Duration::from_secs(10)).await;
    let m = server.invocation_metrics().unwrap();
    let e = &m.endpoints[0];
    assert_eq!(e.failed + e.timed_out + e.unleased, 0, "{m:?}");
    assert_eq!(m.crashes, 0, "{m:?}");
    eprintln!(
        "{} processes for {} invocations; steps {}, mean open {:.1} ms",
        processes.load(Ordering::Relaxed),
        e.invocations,
        e.steps,
        e.open_us_total as f64 / e.opens.max(1) as f64 / 1e3
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_process_that_aborts_mid_step_is_recovered_without_duplicate_effects() {
    let dir = tmp("crash");
    let (addr, server) = start_server(&dir, 2).await;
    let (url, _) = process_endpoint(&addr, &dir).await;
    let sink = Sink::File(dir.join("effects"));
    server
        .set_invokers(vec![Endpoint::new(&url, secret())])
        .unwrap();
    let c = client(&addr).await;
    start_attached(&c, "crash", "crash-1").await;
    let pid = result(&c, "crash-1", Duration::from_secs(30)).await;
    let crashed = std::fs::read_to_string(dir.join("crash")).unwrap();
    let work = sink.values("crash-1/work");
    // "work" ran in the process that aborted, then once more in another,
    // whose result counts; "before" was recorded before the abort and
    // never ran again.
    assert_eq!(work.len(), 2, "{work:?}");
    assert_eq!(work[0], crashed);
    assert_ne!(pid, serde_json::json!(crashed));
    assert_eq!(pid, serde_json::json!(work[1]));
    assert_eq!(sink.count("crash-1/before"), 1);
    let m = server.invocation_metrics().unwrap();
    assert!(m.crashes >= 1, "{m:?}");
    wait_quiet(&server, Duration::from_secs(20)).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_invocation_past_its_budget_is_fenced_and_its_late_step_is_refused() {
    let dir = tmp("fence");
    let (addr, server) = start_server(&dir, 1).await;
    let (url, _) = process_endpoint(&addr, &dir).await;
    let sink = Sink::File(dir.join("effects"));
    server
        .set_invokers(vec![
            Endpoint::new(&url, secret())
                .budget(Duration::from_secs(2))
                .grace(Duration::from_millis(500)),
        ])
        .unwrap();
    let c = client(&addr).await;
    start_attached(&c, "freeze", "freeze-1").await;
    // The first process blocks in "work" for 5 s; the server stops waiting
    // after 2.5 s, revokes its lease, and the next process finishes.
    let pid = result(&c, "freeze-1", Duration::from_secs(30)).await;
    let frozen = std::fs::read_to_string(dir.join("freeze")).unwrap();
    assert_ne!(pid, serde_json::json!(frozen));
    // The frozen process wakes up, finishes its step and cannot record it.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let work = sink.values("freeze-1/work");
    assert_eq!(work.len(), 2, "{work:?}");
    assert_eq!(pid, serde_json::json!(work[1]));
    assert_eq!(result(&c, "freeze-1", Duration::from_secs(10)).await, pid);
    assert_eq!(sink.count("freeze-1/before"), 1);
    let m = server.invocation_metrics().unwrap();
    assert!(m.endpoints[0].timed_out >= 1, "{m:?}");
    wait_quiet(&server, Duration::from_secs(20)).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_slow_endpoint_backs_off_within_its_concurrency() {
    let dir = tmp("backoff");
    let (addr, server) = start_server(&dir, 4).await;
    let sink = Sink::memory();
    let handler = Handler::new(&addr, SECRET, {
        let sink = sink.clone();
        move |engine: &Engine| register(engine, sink.clone(), "invoked", None)
    })
    .unwrap();
    // The first three requests fail at once; then each takes 150 ms more.
    let calls = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let (now_in, most) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(fusee_server::invoke::serve_with(listener, None, {
        let (calls, now_in, most) = (calls.clone(), now_in.clone(), most.clone());
        move |sig: Option<String>, body: Vec<u8>| {
            let (calls, now_in, most, handler) =
                (calls.clone(), now_in.clone(), most.clone(), handler.clone());
            async move {
                let n = now_in.fetch_add(1, Ordering::SeqCst) + 1;
                most.fetch_max(n, Ordering::SeqCst);
                let k = {
                    let mut c = calls.lock().unwrap();
                    c.push(Instant::now());
                    c.len()
                };
                let reply = if k <= 3 {
                    Reply {
                        status: 503,
                        body: br#"{"error":"cold"}"#.to_vec(),
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    handler.handle(sig.as_deref(), &body).await
                };
                now_in.fetch_sub(1, Ordering::SeqCst);
                reply
            }
        }
    }));
    server
        .set_invokers(vec![
            Endpoint::new(&url, secret()).concurrency(1).max_shards(1),
        ])
        .unwrap();
    // Four shards due at startup, one invocation at a time, after three
    // failures that each back off longer.
    wait_for("the startup invocations", Duration::from_secs(30), || {
        server.invocation_metrics().unwrap().endpoints[0].succeeded >= 4
    })
    .await;
    let c = client(&addr).await;
    let ids: Vec<String> = (0..4).map(|s| id_on("quick", s, 4)).collect();
    for id in &ids {
        let id = id.clone();
        call(&c, move |c| c.start("quick", &id, &serde_json::json!(4)))
            .await
            .unwrap();
    }
    for id in &ids {
        assert_eq!(
            result(&c, id, Duration::from_secs(30)).await,
            serde_json::json!(8)
        );
    }
    wait_quiet(&server, Duration::from_secs(20)).await;
    assert_eq!(most.load(Ordering::SeqCst), 1, "concurrency 1 was exceeded");
    let at = calls.lock().unwrap().clone();
    let gaps: Vec<u128> = at
        .windows(2)
        .take(3)
        .map(|w| (w[1] - w[0]).as_millis())
        .collect();
    // 100, 200 and 400 ms, each within a fifth (plus scheduling slack).
    for (gap, want) in gaps.iter().zip([100u128, 200, 400]) {
        assert!(
            *gap >= want * 8 / 10 && *gap <= want * 12 / 10 + 150,
            "gaps {gaps:?}"
        );
    }
    let m = server.invocation_metrics().unwrap();
    assert_eq!(m.endpoints[0].failed, 3, "{m:?}");
    assert_eq!(m.endpoints[0].failures_in_a_row, 0, "{m:?}");
    assert_eq!(
        m.endpoints[0]
            .last_error
            .as_deref()
            .map(|e| e.contains("503")),
        Some(true)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A CA and leaf certificates for one test.
struct Pki {
    dir: PathBuf,
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
}

impl Pki {
    fn new(dir: &Path) -> Self {
        use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose};
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "invoke ca");
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        std::fs::write(dir.join("ca.pem"), cert.pem()).unwrap();
        Self {
            dir: dir.to_owned(),
            issuer: rcgen::Issuer::new(params, key),
        }
    }

    fn ca(&self) -> PathBuf {
        self.dir.join("ca.pem")
    }

    fn leaf(&self, who: &str, names: &[&str]) -> (PathBuf, PathBuf) {
        use rcgen::{CertificateParams, DnType, ExtendedKeyUsagePurpose, KeyPair, KeyUsagePurpose};
        let mut params =
            CertificateParams::new(names.iter().map(|n| n.to_string()).collect::<Vec<_>>())
                .unwrap();
        params.distinguished_name.push(DnType::CommonName, who);
        params.use_authority_key_identifier_extension = true;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        let (c, k) = (
            self.dir.join(format!("{who}.pem")),
            self.dir.join(format!("{who}.key")),
        );
        std::fs::write(&c, cert.pem()).unwrap();
        std::fs::write(&k, key.serialize_pem()).unwrap();
        (c, k)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn https_with_client_certificates_and_the_signature() {
    let dir = tmp("tls");
    let pki = Pki::new(&dir);
    let (cert, key) = pki.leaf("endpoint", &["127.0.0.1"]);
    let (client_cert, client_key) = pki.leaf("server", &["fusee-server"]);
    let listener_tls = Tls::from_pem_files(Some(&cert), Some(&key), Some(&pki.ca())).unwrap();
    let mutual =
        Tls::from_pem_files(Some(&client_cert), Some(&client_key), Some(&pki.ca())).unwrap();
    let anonymous = Tls::from_pem_files(None, None, Some(&pki.ca())).unwrap();
    for (name, tls, secret, works) in [
        ("good", mutual.clone(), secret(), true),
        (
            "wrong-secret",
            mutual,
            Some(Secret::new(b"not the endpoint's secret!").unwrap()),
            false,
        ),
        ("no-client-cert", anonymous, secret(), false),
    ] {
        let sub = dir.join(name);
        std::fs::create_dir_all(&sub).unwrap();
        let (addr, server) = start_server(&sub, 1).await;
        let (url, opened) =
            engine_endpoint(&addr, Sink::memory(), Some(listener_tls.clone())).await;
        assert!(url.starts_with("https://"));
        server
            .set_invokers(vec![Endpoint::new(&url, secret).tls(tls)])
            .unwrap();
        if works {
            let c = client(&addr).await;
            call(&c, |c| c.start("quick", "q", &serde_json::json!(5)))
                .await
                .unwrap();
            assert_eq!(
                result(&c, "q", Duration::from_secs(20)).await,
                serde_json::json!(10)
            );
            wait_quiet(&server, Duration::from_secs(20)).await;
            let m = server.invocation_metrics().unwrap();
            assert_eq!(m.endpoints[0].failed, 0, "{m:?}");
        } else {
            wait_for("a refused invocation", Duration::from_secs(20), || {
                server.invocation_metrics().unwrap().endpoints[0].failed >= 1
            })
            .await;
            let m = server.invocation_metrics().unwrap();
            assert_eq!(
                opened.load(Ordering::Relaxed),
                0,
                "{name}: an engine opened"
            );
            assert_eq!(m.endpoints[0].succeeded, 0, "{name}: {m:?}");
            let error = m.endpoints[0].last_error.clone().unwrap_or_default();
            if name == "wrong-secret" {
                assert!(error.contains("401"), "{error}");
            }
            eprintln!("{name}: refused as expected ({error})");
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_lived_workers_alongside_invoked_ones() {
    let dir = tmp("mixed");
    let (addr, server) = start_server(&dir, 4).await;
    let sink = Sink::memory();
    // A long-lived worker holds two shards (0 and 1).
    let a = format!("{addr}?max-shards=2");
    let (cfg, remote) = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a))
        .await
        .unwrap()
        .unwrap();
    let long = Engine::open(cfg).await.unwrap();
    register(&long, sink.clone(), "long", None);
    let calls = tokio::spawn(fusee_server::serve_calls(remote.clone(), long.clone()));
    assert_eq!(remote.held_shards(), vec![0, 1]);
    let (url, _) = engine_endpoint(&addr, sink.clone(), None).await;
    server
        .set_invokers(vec![Endpoint::new(&url, secret())])
        .unwrap();
    let c = client(&addr).await;
    let ids: Vec<String> = (0..4).map(|s| id_on("quick", s, 4)).collect();
    for id in &ids {
        let id = id.clone();
        call(&c, move |c| c.start("quick", &id, &serde_json::json!(1)))
            .await
            .unwrap();
    }
    for id in &ids {
        assert_eq!(
            result(&c, id, Duration::from_secs(20)).await,
            serde_json::json!(2)
        );
    }
    // Shards 0 and 1 ran on the long-lived worker, 2 and 3 were invoked.
    for (s, id) in ids.iter().enumerate() {
        let want = if s < 2 { "long" } else { "invoked" };
        assert_eq!(
            sink.values(&format!("{id}/only")),
            vec![want.to_owned()],
            "{id}"
        );
    }
    // The long-lived worker leaves: its shards are invoked from then on.
    long.shutdown().await.unwrap();
    calls.abort();
    let _ = calls.await;
    drop(long);
    drop(remote);
    // It left without a report: its shards are due at once, and invoked.
    wait_for(
        "the long-lived worker's shards",
        Duration::from_secs(20),
        || server.invocation_metrics().unwrap().crashes >= 2,
    )
    .await;
    wait_quiet(&server, Duration::from_secs(20)).await;
    let later: Vec<String> = (0..2).map(|s| id_on("later", s, 4)).collect();
    for id in &later {
        let id = id.clone();
        call(&c, move |c| c.start("quick", &id, &serde_json::json!(3)))
            .await
            .unwrap();
    }
    for id in &later {
        assert_eq!(
            result(&c, id, Duration::from_secs(20)).await,
            serde_json::json!(6)
        );
        assert_eq!(
            sink.values(&format!("{id}/only")),
            vec!["invoked".to_owned()]
        );
    }
    wait_quiet(&server, Duration::from_secs(20)).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn work_longer_than_a_budget_is_handed_back_and_a_step_that_never_fits_backs_off() {
    let dir = tmp("budget");
    let (addr, server) = start_server(&dir, 1).await;
    let sink = Sink::memory();
    let (url, opened) = engine_endpoint(&addr, sink.clone(), None).await;
    server
        .set_invokers(vec![
            Endpoint::new(&url, secret()).budget(Duration::from_secs(1)),
        ])
        .unwrap();
    wait_quiet(&server, Duration::from_secs(10)).await;
    let c = client(&addr).await;
    // 12 steps of 200 ms: about 2.4 s of work in invocations of 0.9 s each.
    call(&c, |c| {
        c.start("long", "long-1", &serde_json::json!([12, 200]))
    })
    .await
    .unwrap();
    assert_eq!(
        result(&c, "long-1", Duration::from_secs(30)).await,
        serde_json::json!(12)
    );
    // Every step recorded once; a step cut off at a budget ran again in
    // the next invocation.
    for i in 0..12 {
        let n = sink.count(&format!("long-1/s{i}"));
        assert!((1..=2).contains(&n), "step {i} ran {n} times");
    }
    let m = server.invocation_metrics().unwrap();
    assert!(m.handbacks >= 2, "{m:?}");
    assert_eq!(m.no_progress, 0, "{m:?}");
    // A step of 3 s never fits a 1 s budget: the shard backs off instead of
    // being invoked again at once.
    call(&c, |c| {
        c.start("long", "long-2", &serde_json::json!([1, 3000]))
    })
    .await
    .unwrap();
    wait_for(
        "an invocation without progress",
        Duration::from_secs(20),
        || server.invocation_metrics().unwrap().no_progress >= 2,
    )
    .await;
    let before = opened.load(Ordering::Relaxed);
    let m = server.invocation_metrics().unwrap();
    assert!(m.backoff_shards == 1 || m.invoking_shards == 1, "{m:?}");
    // The third try waits at least 2 s (1 s, doubling, less a fifth).
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        opened.load(Ordering::Relaxed) <= before + 1,
        "invoked again at once"
    );
    call(&c, |c| c.terminate("long-2", "never fits"))
        .await
        .unwrap();
    wait_quiet(&server, Duration::from_secs(30)).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_results_cross_shards_between_invocations() {
    let dir = tmp("child");
    let (addr, server) = start_server(&dir, 4).await;
    let sink = Sink::memory();
    let (url, opened) = engine_endpoint(&addr, sink.clone(), None).await;
    server
        .set_invokers(vec![Endpoint::new(&url, secret()).max_shards(1)])
        .unwrap();
    let c = client(&addr).await;
    // Parent and child on different shards; each invocation holds one.
    let parent = id_on("parent", 0, 4);
    let child = id_on("child", 3, 4);
    let (p, ch) = (parent.clone(), child.clone());
    call(&c, move |c| c.start("parent", &p, &serde_json::json!(ch)))
        .await
        .unwrap();
    assert_eq!(
        result(&c, &parent, Duration::from_secs(20)).await,
        serde_json::json!(43)
    );
    assert_eq!(sink.count(&format!("{child}/only")), 1);
    wait_quiet(&server, Duration::from_secs(20)).await;
    let m = server.invocation_metrics().unwrap();
    assert_eq!(m.crashes, 0, "{m:?}");
    eprintln!("{} engines opened; {m:?}", opened.load(Ordering::Relaxed));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_server_answers_listings_and_metrics_through_invocations() {
    let dir = tmp("list");
    let (addr, server) = start_server(&dir, 2).await;
    let (url, _) = engine_endpoint(&addr, Sink::memory(), None).await;
    server
        .set_invokers(vec![Endpoint::new(&url, secret())])
        .unwrap();
    let c = client(&addr).await;
    for s in 0..2 {
        let id = id_on("listed", s, 2);
        call(&c, move |c| c.start("quick", &id, &serde_json::json!(1)))
            .await
            .unwrap();
        assert_eq!(
            result(&c, &id_on("listed", s, 2), Duration::from_secs(20)).await,
            serde_json::json!(2)
        );
    }
    wait_quiet(&server, Duration::from_secs(20)).await;
    // No worker holds a shard now: the listing invokes them.
    let runs = call(&c, |c| c.list(10)).await.unwrap();
    assert_eq!(runs.as_array().map(Vec::len), Some(2), "{runs}");
    let m = call(&c, |c| c.call("invocations", &serde_json::json!({})))
        .await
        .unwrap();
    assert!(
        m["endpoints"][0]["invocations"].as_u64().unwrap() >= 2,
        "{m}"
    );
    wait_quiet(&server, Duration::from_secs(20)).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn unsigned_and_tampered_requests_are_refused_before_connecting() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // The address is never dialled.
        let opened = Arc::new(AtomicU32::new(0));
        let n = opened.clone();
        let h = Handler::new("127.0.0.1:1", SECRET, move |_: &Engine| {
            n.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
        let body = br#"{"version":1,"invocation":"00000000000000aa","shards":[0],"budgetMs":1000}"#;
        let now = fusee::local::unix_ms();
        let good = fusee_server::invoke::sign(SECRET, now, body);
        assert_eq!(h.handle(None, body).await.status, 401);
        let mut tampered = body.to_vec();
        tampered[12] ^= 1;
        assert_eq!(h.handle(Some(&good), &tampered).await.status, 401);
        let old = fusee_server::invoke::sign(SECRET, now - 600_000, body);
        assert_eq!(h.handle(Some(&old), body).await.status, 401);
        // A good signature on a bad body is a 400, still before connecting.
        let junk = b"{}";
        let sig = fusee_server::invoke::sign(SECRET, now, junk);
        assert_eq!(h.handle(Some(&sig), junk).await.status, 400);
        assert_eq!(opened.load(Ordering::Relaxed), 0);
    });
}
