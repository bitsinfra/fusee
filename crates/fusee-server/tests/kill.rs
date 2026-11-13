//! The server process is killed with SIGKILL under load, over and over, and
//! restarted on the same directory. Every workflow whose start a worker saw
//! acknowledged must finish, and every result a worker saw before a kill
//! must be the result after it.
//!
//! The server runs in a child process: this test binary started again with
//! `FUSEE_KILL_SERVER_DIR` set runs [`server_child`] only, which serves
//! until it is killed. Steps return a fresh clock reading each time they
//! run, so a step that ran again after a kill (its acknowledged record
//! lost) would change the workflow's result.

use fusee::{Ctx, Engine, Error, FlushMethod, LocalOptions};
use fusee_server::{Server, ServerConfig};
use std::collections::BTreeMap;
use std::io::BufRead;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const READY: &str = "FUSEE-KILL-SERVER-READY";

/// Not a test of its own: the server process of the kill tests.
#[test]
fn server_child() {
    let Some(dir) = std::env::var_os("FUSEE_KILL_SERVER_DIR") else {
        return;
    };
    let shared = std::env::var_os("FUSEE_KILL_SHARED_LOG").is_none_or(|v| v != "0");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let server = Server::open(
            ServerConfig {
                dir: dir.into(),
                shards: 4,
                local: LocalOptions {
                    flush: FlushMethod::Fsync,
                    ..Default::default()
                },
                security: Default::default(),
            },
            shared,
        )
        .unwrap();
        // Same-host workers over shared memory: a Unix socket next to the
        // directory.
        if let Some(path) = std::env::var_os("FUSEE_KILL_SHM_SOCKET") {
            let _ = std::fs::remove_file(&path);
            let listener = tokio::net::UnixListener::bind(&path).unwrap();
            println!("{READY} unix:{}?shm=1", path.to_string_lossy());
            let _ = fusee_server::server::serve_unix(server, listener).await;
            return;
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        println!("{READY} {}", listener.local_addr().unwrap());
        let _ = fusee_server::server::serve_tcp(server, listener).await;
    });
}

/// The server child; killed when dropped, so a failed test leaves none.
struct ServerProcess(Child);

impl ServerProcess {
    /// SIGKILL: nothing of the server's runs after this.
    fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.kill();
    }
}

fn start_server(dir: &std::path::Path, shared: bool, shm: bool) -> (ServerProcess, String) {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", "server_child", "--nocapture", "--test-threads=1"])
        .env("FUSEE_KILL_SERVER_DIR", dir)
        .env("FUSEE_KILL_SHARED_LOG", if shared { "1" } else { "0" });
    if shm {
        cmd.env("FUSEE_KILL_SHM_SOCKET", socket_path(dir));
    }
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let out = child.stdout.take().unwrap();
    let child = ServerProcess(child);
    let mut lines = std::io::BufReader::new(out).lines();
    let addr = loop {
        let line = lines
            .next()
            .expect("the server exited before it was ready")
            .unwrap();
        // The test harness may print its own text before ours on the line.
        if let Some((_, addr)) = line.split_once(READY) {
            break addr.trim().to_owned();
        }
    };
    (child, addr)
}

/// A Unix socket path short enough for every host, beside `dir`.
fn socket_path(dir: &std::path::Path) -> std::path::PathBuf {
    let mut name = dir.file_name().unwrap().to_owned();
    name.push(".sock");
    std::env::temp_dir().join(name)
}

fn clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn register(engine: &Engine) -> fusee::WorkflowRef<u64, u64> {
    engine.register("chain", |ctx: Ctx, n: u64| async move {
        let mut acc = n;
        for _ in 0..6 {
            let v: u64 = ctx
                .step("tick", || async { Ok::<_, Error>(clock()) })
                .await?;
            acc = acc.wrapping_mul(31).wrapping_add(v);
        }
        Ok(acc)
    })
}

async fn worker(addr: &str) -> Engine {
    let a = addr.to_owned();
    let cfg = tokio::task::spawn_blocking(move || fusee_server::remote_config(&a))
        .await
        .unwrap()
        .unwrap();
    Engine::open(cfg).await.unwrap()
}

#[derive(Default)]
struct Seen {
    /// Workflow IDs whose start was acknowledged.
    started: Vec<String>,
    /// Results seen before a kill.
    finished: BTreeMap<String, u64>,
}

fn kill_campaign(name: &str, shared: bool, kills: u32, shm: bool) {
    if std::env::var_os("FUSEE_KILL_SERVER_DIR").is_some()
        || std::env::var_os("FUSEE_KILL_WORKER_ADDR").is_some()
    {
        return;
    }
    let dir = std::env::temp_dir().join(format!("fusee-kill-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let seen = Arc::new(Mutex::new(Seen::default()));
    let mut rng = u64::from(std::process::id()) | 1;
    for kill in 0..kills {
        let (mut child, addr) = start_server(&dir, shared, shm);
        rt.block_on(async {
            let engine = worker(&addr).await;
            let wf = Arc::new(register(&engine));
            let mut load = Vec::new();
            for task in 0..8 {
                let (wf, seen, engine) = (wf.clone(), seen.clone(), engine.clone());
                load.push(tokio::spawn(async move {
                    for k in 0.. {
                        let id = format!("k{kill}-t{task}-{k}");
                        let Ok(h) = engine.start(&wf, &id, &k).await else {
                            return;
                        };
                        seen.lock().unwrap().started.push(id.clone());
                        let Ok(v) = h.result().await else { return };
                        seen.lock().unwrap().finished.insert(id, v);
                    }
                }));
            }
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            tokio::time::sleep(Duration::from_millis(80 + rng % 250)).await;
            child.kill();
            for t in load {
                let _ = tokio::time::timeout(Duration::from_secs(20), t).await;
            }
            let _ = tokio::time::timeout(Duration::from_secs(5), engine.shutdown()).await;
        });
    }
    // A last restart: every acknowledged start finishes, and every result
    // seen before a kill stands.
    let (mut child, addr) = start_server(&dir, shared, shm);
    let seen = std::mem::take(&mut *seen.lock().unwrap());
    rt.block_on(async {
        let engine = worker(&addr).await;
        let _wf = register(&engine);
        let deadline = Instant::now() + Duration::from_secs(120);
        for id in &seen.started {
            let left = deadline.saturating_duration_since(Instant::now());
            let r = tokio::time::timeout(left, engine.run::<u64>(id).result())
                .await
                .unwrap_or_else(|_| panic!("{id} did not finish after the last restart"))
                .unwrap_or_else(|e| panic!("{id} was acknowledged but failed: {e}"));
            if let Some(before) = seen.finished.get(id) {
                assert_eq!(r, *before, "{id}: its result changed after a server kill");
            }
        }
        let _ = tokio::time::timeout(Duration::from_secs(10), engine.shutdown()).await;
    });
    child.kill();
    eprintln!(
        "{name}: {kills} server kills, {} acknowledged starts all finished, {} results unchanged",
        seen.started.len(),
        seen.finished.len()
    );
    assert!(
        seen.started.len() > kills as usize * 4,
        "too little load reached the server"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn acknowledged_work_survives_server_kills_with_the_shared_log() {
    kill_campaign("shared", true, 20, false);
}

#[test]
fn acknowledged_work_survives_server_kills_with_a_log_per_shard() {
    kill_campaign("per-shard", false, 4, false);
}

#[test]
fn acknowledged_work_survives_server_kills_over_shared_memory() {
    kill_campaign("shm", true, 20, true);
}

const STARTED: &str = "FUSEE-KILL-STARTED";
const FINISHED: &str = "FUSEE-KILL-FINISHED";

/// Not a test of its own: a worker process of the worker kill test. It
/// prints every start and result it saw acknowledged until it is killed.
#[test]
fn worker_child() {
    let Some(addr) = std::env::var_os("FUSEE_KILL_WORKER_ADDR") else {
        return;
    };
    let addr = addr.to_string_lossy().into_owned();
    let round = std::env::var("FUSEE_KILL_WORKER_ROUND").unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let engine = worker(&addr).await;
        let wf = Arc::new(register(&engine));
        let mut load = Vec::new();
        for task in 0..8 {
            let (wf, round, engine) = (wf.clone(), round.clone(), engine.clone());
            load.push(tokio::spawn(async move {
                for k in 0.. {
                    let id = format!("w{round}-t{task}-{k}");
                    let Ok(h) = engine.start(&wf, &id, &k).await else {
                        return;
                    };
                    println!("{STARTED} {id}");
                    let Ok(v) = h.result().await else { return };
                    println!("{FINISHED} {id} {v}");
                }
            }));
        }
        for t in load {
            let _ = t.await;
        }
    });
}

/// The worker process is killed with SIGKILL under load, over and over,
/// while the server keeps running; the next worker takes its shards over.
/// Every start a killed worker saw acknowledged finishes, with the result
/// it saw if it saw one: a frame half-written to the ring, or published
/// and not yet staged, is never acknowledged.
#[test]
fn acknowledged_work_survives_worker_kills_over_shared_memory() {
    if std::env::var_os("FUSEE_KILL_SERVER_DIR").is_some()
        || std::env::var_os("FUSEE_KILL_WORKER_ADDR").is_some()
    {
        return;
    }
    let dir = std::env::temp_dir().join(format!("fusee-wkill-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let (mut server, addr) = start_server(&dir, true, true);
    let mut seen = Seen::default();
    let mut rng = u64::from(std::process::id()) | 1;
    const KILLS: u32 = 10;
    for round in 0..KILLS {
        let mut child = ServerProcess(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "worker_child", "--nocapture", "--test-threads=1"])
                .env("FUSEE_KILL_WORKER_ADDR", &addr)
                .env("FUSEE_KILL_WORKER_ROUND", round.to_string())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let out = child.0.stdout.take().unwrap();
        let lines = std::thread::spawn(move || {
            let mut got = Vec::new();
            for line in std::io::BufReader::new(out).lines() {
                let Ok(line) = line else { break };
                got.push(line);
            }
            got
        });
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        std::thread::sleep(Duration::from_millis(300 + rng % 400));
        child.kill();
        for line in lines.join().unwrap() {
            if let Some((_, rest)) = line.split_once(STARTED) {
                seen.started.push(rest.trim().to_owned());
            } else if let Some((_, rest)) = line.split_once(FINISHED) {
                let mut parts = rest.split_whitespace();
                let (id, v) = (parts.next().unwrap(), parts.next().unwrap());
                seen.finished.insert(id.to_owned(), v.parse().unwrap());
            }
        }
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let engine = worker(&addr).await;
        let _wf = register(&engine);
        let deadline = Instant::now() + Duration::from_secs(120);
        for id in &seen.started {
            let left = deadline.saturating_duration_since(Instant::now());
            let r = tokio::time::timeout(left, engine.run::<u64>(id).result())
                .await
                .unwrap_or_else(|_| panic!("{id} did not finish after the last kill"))
                .unwrap_or_else(|e| panic!("{id} was acknowledged but failed: {e}"));
            if let Some(before) = seen.finished.get(id) {
                assert_eq!(r, *before, "{id}: its result changed after a worker kill");
            }
        }
        let _ = tokio::time::timeout(Duration::from_secs(10), engine.shutdown()).await;
    });
    server.kill();
    eprintln!(
        "worker kills over shared memory: {KILLS} kills, {} acknowledged starts all finished, {} results unchanged",
        seen.started.len(),
        seen.finished.len()
    );
    assert!(
        seen.started.len() > KILLS as usize * 4,
        "too little load reached the server"
    );
    let _ = std::fs::remove_file(socket_path(&dir));
    let _ = std::fs::remove_dir_all(&dir);
}
