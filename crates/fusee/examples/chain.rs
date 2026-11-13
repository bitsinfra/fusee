//! Quick hot-path measurement: per-step overhead of a sequential no-op chain
//! (closed loop, one workflow) and throughput with many concurrent workflows.
//!
//! cargo run --release -p fusee --example chain -- [local|fsync|memory] [dir]
//!
//! `FUSEE_EMULATED_FLUSH_US=<µs>` adds that long to every flush (a slow
//! disk such as gp3, on a fast one).
use fusee::{Config, Ctx, Engine, Error, FlushMethod};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Error> {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "local".into());
    let dir = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "/tmp/fusee-chain".into());
    let shards: u32 = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let _ = std::fs::remove_dir_all(&dir);
    let mut cfg = match mode.as_str() {
        "memory" => Config::memory(),
        _ => Config::local(&dir),
    }
    .shards(shards);
    if mode == "fsync" {
        cfg.local.flush = FlushMethod::Fsync;
    }
    if let Some(us) = std::env::var("FUSEE_EMULATED_FLUSH_US")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        cfg.local.emulated_flush = std::time::Duration::from_micros(us);
    }
    let engine = Engine::open(cfg).await?;
    let stamps: Arc<Mutex<Vec<Instant>>> = Arc::new(Mutex::new(Vec::with_capacity(4096)));
    let s = stamps.clone();
    let chain = engine.register("chain", move |ctx: Ctx, n: u32| {
        let s = s.clone();
        async move {
            for i in 0..n {
                let s = s.clone();
                ctx.step("noop", move || {
                    let s = s.clone();
                    async move {
                        if n > 1000 {
                            s.lock().unwrap().push(Instant::now());
                        }
                        Ok::<_, Error>(i)
                    }
                })
                .await?;
            }
            Ok(n)
        }
    });
    // Closed loop: one workflow, 2000 sequential steps.
    let t = Instant::now();
    engine.start(&chain, "solo", &2000).await?.result().await?;
    let total = t.elapsed();
    let st = stamps.lock().unwrap().clone();
    let mut gaps: Vec<f64> = st
        .windows(2)
        .map(|w| (w[1] - w[0]).as_secs_f64() * 1e6)
        .collect();
    gaps.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |p: f64| gaps[((gaps.len() - 1) as f64 * p) as usize];
    println!(
        "{mode}: sequential step-to-step: p50 {:.1} µs  p99 {:.1} µs  p99.9 {:.1} µs  (2000 steps in {:.1} ms)",
        pct(0.5),
        pct(0.99),
        pct(0.999),
        total.as_secs_f64() * 1e3
    );
    // Throughput: many concurrent workflows.
    let wfs = 2000u32;
    let steps = 50u32;
    let t = Instant::now();
    let chain = Arc::new(chain);
    let mut tasks = Vec::new();
    for i in 0..wfs {
        let (chain, engine) = (chain.clone(), engine.clone());
        tasks.push(tokio::spawn(async move {
            let h = engine.start(&chain, &format!("w{i}"), &steps).await?;
            h.result().await
        }));
    }
    for t in tasks {
        t.await.map_err(|e| Error::failed(e.to_string()))??;
    }
    let el = t.elapsed().as_secs_f64();
    let m = engine.metrics().await?;
    println!(
        "{mode}: {} durable steps in {:.2} s = {:.0} steps/s; flushes {} ({} inline), {:.1} records/flush",
        wfs * steps,
        el,
        f64::from(wfs * steps) / el,
        m.flushes,
        m.inline_flushes,
        m.flushed_records as f64 / m.flushes.max(1) as f64
    );
    engine.shutdown().await?;
    Ok(())
}
