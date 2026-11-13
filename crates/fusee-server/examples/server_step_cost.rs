//! What one sequential step costs in server mode against local mode, in one
//! process: per-step latency (median of the gaps between consecutive steps,
//! and the mean), voluntary context switches (threads that blocked and
//! were woken), socket messages sent and received, and the ratio of
//! server-mode to local-mode step latency, over a Unix socket, over
//! same-host shared memory (`?shm=1`) and over loopback TCP.
//!
//! ```text
//! cargo run --release -p fusee-server --example server_step_cost -- [steps] [rounds]
//! ```
//!
//! Every mode uses plain `fsync` (`FlushMethod::Fsync`), one shard, and the
//! same directory layout, so the difference is the server path. Rounds
//! alternate the modes (local, socket, shared memory, TCP, then again), so
//! a host whose load drifts affects them alike. The counts cover every
//! thread of the process (worker and server together).

use fusee::{Config, Ctx, Engine, Error, FlushMethod, LocalOptions};
use fusee_server::{Server, ServerConfig};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone, Copy, Default, Debug)]
struct Usage {
    nvcsw: i64,
    nivcsw: i64,
    msgsnd: i64,
    msgrcv: i64,
}

fn usage() -> Usage {
    // SAFETY: getrusage writes the struct it is given.
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    Usage {
        nvcsw: u.ru_nvcsw as i64,
        nivcsw: u.ru_nivcsw as i64,
        msgsnd: u.ru_msgsnd as i64,
        msgrcv: u.ru_msgrcv as i64,
    }
}

fn local() -> LocalOptions {
    LocalOptions {
        flush: FlushMethod::Fsync,
        ..Default::default()
    }
}

struct Trial {
    p50_us: f64,
    p99_us: f64,
    mean_us: f64,
    /// Share of flushes the shard thread waited for itself.
    inline: f64,
    nvcsw: f64,
    nivcsw: f64,
    msgsnd: f64,
    msgrcv: f64,
}

type Stamps = Arc<Mutex<Vec<Instant>>>;

fn register(engine: &Engine, stamps: Stamps) -> fusee::WorkflowRef<u32, u32> {
    engine.register("chain", move |ctx: Ctx, n: u32| {
        let stamps = stamps.clone();
        async move {
            for i in 0..n {
                let s = stamps.clone();
                ctx.step("noop", move || {
                    let s = s.clone();
                    async move {
                        s.lock().unwrap().push(Instant::now());
                        Ok::<_, Error>(i)
                    }
                })
                .await?;
            }
            Ok(n)
        }
    })
}

async fn trial(
    engine: &Engine,
    wf: &fusee::WorkflowRef<u32, u32>,
    stamps: &Stamps,
    id: &str,
    steps: u32,
) -> Trial {
    stamps.lock().unwrap().clear();
    let m0 = engine.metrics().await.unwrap();
    let before = usage();
    let started = Instant::now();
    let n = engine
        .start(wf, id, &steps)
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    assert_eq!(n, steps);
    let elapsed = started.elapsed();
    let after = usage();
    let m1 = engine.metrics().await.unwrap();
    let per = |a: i64, b: i64| (a - b) as f64 / f64::from(steps);
    // A step's latency: from one step body to the next, which runs once
    // the first one's record is durable.
    let s = stamps.lock().unwrap();
    let mut gaps: Vec<f64> = s
        .windows(2)
        .map(|w| (w[1] - w[0]).as_secs_f64() * 1e6)
        .collect();
    gaps.sort_by(|a, b| a.total_cmp(b));
    let p99 = gaps.get(gaps.len() * 99 / 100).copied().unwrap_or(0.0);
    let flushes = (m1.flushes - m0.flushes).max(1) as f64;
    Trial {
        p50_us: median(gaps),
        p99_us: p99,
        mean_us: elapsed.as_secs_f64() * 1e6 / f64::from(steps),
        inline: (m1.inline_flushes - m0.inline_flushes) as f64 / flushes,
        nvcsw: per(after.nvcsw, before.nvcsw),
        nivcsw: per(after.nivcsw, before.nivcsw),
        msgsnd: per(after.msgsnd, before.msgsnd),
        msgrcv: per(after.msgrcv, before.msgrcv),
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.total_cmp(b));
    v[v.len() / 2]
}

fn report(name: &str, trials: &[Trial]) -> f64 {
    let m = |f: fn(&Trial) -> f64| median(trials.iter().map(f).collect());
    let p = m(|t| t.p50_us);
    println!(
        "{name:<16} p50 {p:>6.1} µs  p99 {:>7.1}  mean {:>6.1}  inline {:>3.0}%  vol.cs {:>4.2}  invol {:>4.2}  msgs sent {:>4.2} recv {:>4.2}  (per step)",
        m(|t| t.p99_us),
        m(|t| t.mean_us),
        m(|t| t.inline * 100.0),
        m(|t| t.nvcsw),
        m(|t| t.nivcsw),
        m(|t| t.msgsnd),
        m(|t| t.msgrcv),
    );
    p
}

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("fusee-step-cost-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// One trial on a fresh worker at `addr`; the worker leaves afterwards, so
/// the next one takes the shard.
async fn server_trial(addr: &str, steps: u32, id: &str, shm: bool) -> Trial {
    let a = addr.to_owned();
    let (cfg, remote) = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remote.shared_memory(), shm, "{addr}");
    let engine = Engine::open(cfg).await.unwrap();
    let stamps = Stamps::default();
    let wf = register(&engine, stamps.clone());
    // Warm up: the first steps open files and fill caches.
    trial(&engine, &wf, &stamps, &format!("{id}-warm"), 50).await;
    let t = trial(&engine, &wf, &stamps, id, steps).await;
    engine.shutdown().await.unwrap();
    drop((wf, engine, remote));
    t
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let mut args = std::env::args().skip(1);
    let steps: u32 = args.next().and_then(|a| a.parse().ok()).unwrap_or(500);
    let rounds: u32 = args.next().and_then(|a| a.parse().ok()).unwrap_or(7);

    let ldir = tmp("local");
    let engine = Engine::open(Config {
        local: local(),
        ..Config::local(&ldir).shards(1)
    })
    .await
    .unwrap();
    let stamps = Stamps::default();
    let wf = register(&engine, stamps.clone());
    trial(&engine, &wf, &stamps, "local-warm", 50).await;

    let dir = tmp("server");
    let server = Server::new(ServerConfig {
        dir: dir.clone(),
        shards: 1,
        local: local(),
        security: Default::default(),
    })
    .unwrap();
    let sock = dir.with_extension("sock");
    let _ = std::fs::remove_file(&sock);
    let unix = tokio::net::UnixListener::bind(&sock).unwrap();
    tokio::spawn(fusee_server::server::serve_unix(server.clone(), unix));
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_addr = tcp.local_addr().unwrap().to_string();
    tokio::spawn(fusee_server::server::serve_tcp(server.clone(), tcp));
    let unix_addr = format!("unix:{}", sock.display());
    let shm_addr = format!("{unix_addr}?shm=1");

    let (mut l, mut u, mut s, mut t) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for r in 0..rounds {
        l.push(trial(&engine, &wf, &stamps, &format!("local-{r}"), steps).await);
        u.push(server_trial(&unix_addr, steps, &format!("unix-{r}"), false).await);
        s.push(server_trial(&shm_addr, steps, &format!("shm-{r}"), true).await);
        t.push(server_trial(&tcp_addr, steps, &format!("tcp-{r}"), false).await);
    }
    engine.shutdown().await.unwrap();

    println!(
        "{steps} sequential steps, {rounds} alternating rounds, medians; plain fsync, one shard"
    );
    let lp = report("local", &l);
    let up = report("server unix", &u);
    let sp = report("server unix+shm", &s);
    let tp = report("server tcp", &t);
    println!(
        "server/local step p50: unix {:.2}x (+{:.1} µs), shared memory {:.2}x (+{:.1} µs), tcp {:.2}x (+{:.1} µs)",
        up / lp,
        up - lp,
        sp / lp,
        sp - lp,
        tp / lp,
        tp - lp,
    );
    let _ = std::fs::remove_file(&sock);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&ldir);
}
