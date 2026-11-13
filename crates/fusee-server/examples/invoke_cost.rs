//! What invoked workers cost, in one process over loopback: the server, an
//! HTTP endpoint whose handler opens a fresh engine per invocation, and a
//! client.
//!
//! ```text
//! cargo run --release -p fusee-server --example invoke_cost -- [cold] [burst] [wakes] [parked]
//! ```
//!
//! - **Cold start**: `cold` times, a start for a shard nobody holds, from
//!   the client's call to the first step's body: the server's dispatch, the
//!   HTTP request, the handler connecting and taking the lease, the engine
//!   opening the shard (recovery from the server), and the step. Also the
//!   engine open alone, as each shard reports it.
//! - **Steps per invocation**: `burst` workflows of 4 steps started at once
//!   on 4 shards: invocations used and steps each ran.
//! - **Overhead per resumed run**: `wakes` workflows that sleep 1 s and run
//!   a step, on invoked workers and then on a long-lived worker: how late
//!   the step runs after the timer is due, and the engine-open and
//!   invocation time per resumed run.
//! - **Cold start on a full shard**: `parked` workflows sleeping for an
//!   hour on one shard, then cold starts there.
//!
//! Plain `fsync`, a server directory under the system's temp directory.

use fusee::{Ctx, Engine, Error, FlushMethod, LocalOptions};
use fusee_server::invoke::{Endpoint, Handler, Secret};
use fusee_server::{Client, Server, ServerConfig};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SECRET: &[u8] = b"an invocation secret of 32 bytes";

/// When each workflow's steps ran: id to (step, Unix ms) in order.
type Marks = Arc<Mutex<HashMap<String, Vec<(String, u64, Instant)>>>>;

fn register(engine: &Engine, marks: Marks) {
    let m = marks.clone();
    engine.register("one", move |ctx: Ctx, _: u32| {
        let m = m.clone();
        async move { mark(&ctx, &m, "only").await }
    });
    let m = marks.clone();
    engine.register("four", move |ctx: Ctx, _: u32| {
        let m = m.clone();
        async move {
            for s in ["a", "b", "c", "d"] {
                mark(&ctx, &m, s).await?;
            }
            Ok(0u64)
        }
    });
    // Sleeps an hour: a long-sleeping workflow, parked on its shard.
    engine.register("hold", |ctx: Ctx, _: u32| async move {
        ctx.sleep("hold", Duration::from_secs(3600)).await?;
        Ok(0u32)
    });
    let m = marks;
    engine.register("nap", move |ctx: Ctx, _: u32| {
        let m = m.clone();
        async move {
            let before = mark(&ctx, &m, "before").await?;
            ctx.sleep("nap", Duration::from_secs(1)).await?;
            let after = mark(&ctx, &m, "after").await?;
            // How late the step after the sleep ran, in ms.
            Ok(after.saturating_sub(before + 1000))
        }
    });
}

async fn mark(ctx: &Ctx, marks: &Marks, step: &str) -> fusee::Result<u64> {
    let (id, s, marks) = (ctx.workflow_id().to_owned(), step.to_owned(), marks.clone());
    ctx.step(step, move || {
        let (id, s, marks) = (id.clone(), s.clone(), marks.clone());
        async move {
            let now = fusee::local::unix_ms();
            marks
                .lock()
                .unwrap()
                .entry(id)
                .or_default()
                .push((s, now, Instant::now()));
            Ok::<_, Error>(now)
        }
    })
    .await
}

fn id_on(prefix: &str, shard: u32, of: u32, k: usize) -> String {
    (0..)
        .map(|j| format!("{prefix}-{k}-{j}"))
        .find(|id| fusee_core::shard::shard_of(id, of) == shard)
        .unwrap()
}

async fn quiet(server: &Server) {
    loop {
        let m = server.invocation_metrics().unwrap();
        if m.invoking_shards == 0 && m.endpoints.iter().all(|e| e.in_flight == 0) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.total_cmp(b));
    v[((v.len() - 1) as f64 * p).round() as usize]
}

async fn blocking<T: Send + 'static>(
    c: &Client,
    f: impl FnOnce(&Client) -> T + Send + 'static,
) -> T {
    let c = c.clone();
    tokio::task::spawn_blocking(move || f(&c)).await.unwrap()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<usize> = std::env::args()
        .skip(1)
        .filter_map(|a| a.parse().ok())
        .collect();
    let (cold, burst, wakes) = (
        args.first().copied().unwrap_or(30),
        args.get(1).copied().unwrap_or(200),
        args.get(2).copied().unwrap_or(100),
    );
    let dir = std::env::temp_dir().join(format!("fusee-invoke-cost-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let server = Server::new(ServerConfig {
        dir: dir.clone(),
        shards: 4,
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
    let marks = Marks::default();
    let opened = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let (m2, o2) = (marks.clone(), opened.clone());
    let handler = Handler::new(&addr, SECRET, move |engine: &Engine| {
        o2.lock().unwrap().push(Instant::now());
        register(engine, m2.clone());
    })
    .unwrap();
    let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", http.local_addr().unwrap());
    tokio::spawn(fusee_server::invoke::serve(http, handler, None));
    server
        .set_invokers(vec![Endpoint::new(
            &url,
            Some(Secret::new(SECRET).unwrap()),
        )])
        .unwrap();
    let a = addr.clone();
    let c = tokio::task::spawn_blocking(move || Client::connect(&a))
        .await
        .unwrap()
        .unwrap();
    quiet(&server).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    quiet(&server).await;

    // Cold start: invocation to first step.
    let before = server.invocation_metrics().unwrap().endpoints[0].clone();
    let mut first = Vec::new();
    let mut to_open = Vec::new();
    for k in 0..cold {
        let id = id_on("cold", (k % 4) as u32, 4, k);
        let t0 = Instant::now();
        let n = opened.lock().unwrap().len();
        let i2 = id.clone();
        blocking(&c, move |c| c.start("one", &i2, &serde_json::json!(0)))
            .await
            .unwrap();
        let ran = loop {
            if let Some((_, _, at)) = marks.lock().unwrap().get(&id).and_then(|v| v.first()) {
                break *at;
            }
            tokio::time::sleep(Duration::from_micros(200)).await;
        };
        first.push((ran - t0).as_secs_f64() * 1e3);
        if let Some(at) = opened.lock().unwrap().get(n) {
            to_open.push((*at - t0).as_secs_f64() * 1e3);
        }
        quiet(&server).await;
    }
    let after = server.invocation_metrics().unwrap().endpoints[0].clone();
    let opens = (after.opens - before.opens).max(1);
    let open_ms = (after.open_us_total - before.open_us_total) as f64 / opens as f64 / 1e3;
    println!(
        "cold start, call to first step ({cold} starts): p50 {:.2} ms, p90 {:.2} ms, max {:.2} ms",
        pct(&mut first, 0.5),
        pct(&mut first, 0.9),
        pct(&mut first, 1.0)
    );
    println!(
        "  call to engine open and registered: p50 {:.2} ms; shard recovery inside the open: mean {:.3} ms; HTTP round trip (invocation sent to answered): mean {:.2} ms",
        pct(&mut to_open, 0.5),
        open_ms,
        (after.total_ms - before.total_ms) as f64
            / (after.invocations - before.invocations).max(1) as f64
    );

    // Steps per invocation: 4-step workflows on 4 shards, started all at
    // once (a burst) and then one after another (a trickle).
    for (kind, concurrent) in [("burst", true), ("trickle", false)] {
        let before = server.invocation_metrics().unwrap().endpoints[0].clone();
        let t0 = Instant::now();
        let ids: Vec<String> = (0..burst)
            .map(|k| id_on(kind, (k % 4) as u32, 4, k))
            .collect();
        let mut starts = Vec::new();
        for id in &ids {
            let (id, c) = (id.clone(), c.clone());
            let start = tokio::task::spawn_blocking(move || {
                c.start("four", &id, &serde_json::json!(0)).unwrap()
            });
            if concurrent {
                starts.push(start);
            } else {
                start.await.unwrap();
            }
        }
        for s in starts {
            s.await.unwrap();
        }
        for id in &ids {
            let id = id.clone();
            blocking(&c, move |c| c.result(&id)).await.unwrap();
        }
        let took = t0.elapsed();
        quiet(&server).await;
        let after = server.invocation_metrics().unwrap().endpoints[0].clone();
        let inv = after.invocations - before.invocations;
        let steps = after.steps - before.steps;
        println!(
            "{kind} of {burst} workflows x 4 steps on 4 shards: {inv} invocations, {:.1} steps per invocation, {:.0} ms in all",
            steps as f64 / inv.max(1) as f64,
            took.as_secs_f64() * 1e3
        );
    }

    // Overhead per resumed run: the step after a 1 s sleep, invoked.
    let lateness = |ids: &[String], marks: &Marks| -> Vec<f64> {
        let m = marks.lock().unwrap();
        ids.iter()
            .filter_map(|id| {
                let v = m.get(id)?;
                let b = v.iter().find(|(s, ..)| s == "before")?.1;
                let a = v.iter().find(|(s, ..)| s == "after")?.1;
                Some(a.saturating_sub(b + 1000) as f64)
            })
            .collect()
    };
    let before = server.invocation_metrics().unwrap().endpoints[0].clone();
    let ids: Vec<String> = (0..wakes)
        .map(|k| id_on("nap", (k % 4) as u32, 4, k))
        .collect();
    for id in &ids {
        let id = id.clone();
        blocking(&c, move |c| c.start("nap", &id, &serde_json::json!(0)))
            .await
            .unwrap();
    }
    for id in &ids {
        let id = id.clone();
        blocking(&c, move |c| c.result(&id)).await.unwrap();
    }
    quiet(&server).await;
    let after = server.invocation_metrics().unwrap().endpoints[0].clone();
    let mut invoked = lateness(&ids, &marks);

    // The same on a long-lived worker holding every shard.
    let a = addr.clone();
    let (cfg, remote) = tokio::task::spawn_blocking(move || fusee_server::remote_worker(&a))
        .await
        .unwrap()
        .unwrap();
    let long = Engine::open(cfg).await.unwrap();
    register(&long, marks.clone());
    let calls = tokio::spawn(fusee_server::serve_calls(remote.clone(), long.clone()));
    let ids2: Vec<String> = (0..wakes)
        .map(|k| id_on("nap-long", (k % 4) as u32, 4, k))
        .collect();
    for id in &ids2 {
        let id = id.clone();
        blocking(&c, move |c| c.start("nap", &id, &serde_json::json!(0)))
            .await
            .unwrap();
    }
    for id in &ids2 {
        let id = id.clone();
        blocking(&c, move |c| c.result(&id)).await.unwrap();
    }
    let mut lived = lateness(&ids2, &marks);
    let (i50, l50) = (pct(&mut invoked, 0.5), pct(&mut lived, 0.5));
    let inv = (after.invocations - before.invocations).max(1);
    println!(
        "resumed after a 1 s sleep ({wakes} runs on 4 shards): step late by p50 {i50:.0} ms (p90 {:.0}) invoked, {l50:.0} ms (p90 {:.0}) on a long-lived worker; {inv} invocations (start and wake), engine open {:.3} ms and invocation time {:.2} ms per run",
        pct(&mut invoked, 0.9),
        pct(&mut lived, 0.9),
        (after.open_us_total - before.open_us_total) as f64 / 1e3 / wakes as f64,
        (after.total_ms - before.total_ms) as f64 / wakes as f64,
    );
    let left = server.invocation_metrics().unwrap().crashes;
    long.shutdown().await.unwrap();
    calls.abort();
    let _ = calls.await;
    drop(long);
    drop(remote);
    // It leaves without a report: wait until its four shards are released.
    while server.invocation_metrics().unwrap().crashes < left + 4 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    quiet(&server).await;

    // Cold start on a shard that holds `parked` runs sleeping for an hour:
    // recovery reads the shard's checkpoint and run-store index from the
    // server, timers included.
    let parked = args.get(3).copied().unwrap_or(10_000);
    let mut starts = Vec::new();
    for k in 0..parked {
        let (id, c) = (id_on("parked", 0, 4, k), c.clone());
        starts.push(tokio::task::spawn_blocking(move || {
            c.start("hold", &id, &serde_json::json!(0)).unwrap()
        }));
    }
    for s in starts {
        s.await.unwrap();
    }
    quiet(&server).await;
    let mut cold_parked = Vec::new();
    let before = server.invocation_metrics().unwrap().endpoints[0].clone();
    for k in 0..10 {
        let id = id_on("after-parked", 0, 4, k);
        let t0 = Instant::now();
        let i2 = id.clone();
        blocking(&c, move |c| c.start("one", &i2, &serde_json::json!(0)))
            .await
            .unwrap();
        let ran = loop {
            if let Some((_, _, at)) = marks.lock().unwrap().get(&id).and_then(|v| v.first()) {
                break *at;
            }
            tokio::time::sleep(Duration::from_micros(200)).await;
        };
        cold_parked.push((ran - t0).as_secs_f64() * 1e3);
        quiet(&server).await;
    }
    let after = server.invocation_metrics().unwrap().endpoints[0].clone();
    println!(
        "cold start on a shard with {parked} runs sleeping for an hour: call to first step p50 {:.2} ms, max {:.2} ms; shard recovery mean {:.3} ms",
        pct(&mut cold_parked, 0.5),
        pct(&mut cold_parked, 1.0),
        (after.open_us_total - before.open_us_total) as f64
            / (after.opens - before.opens).max(1) as f64
            / 1e3
    );
    let _ = std::fs::remove_dir_all(&dir);
    std::process::exit(0);
}
