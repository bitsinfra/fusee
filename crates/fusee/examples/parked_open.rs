//! Opening a directory that holds many parked runs, across every shard.
//!
//! cargo run --release -p fusee --example parked_open -- <dir> <runs> [shards]
//!
//! Starts `runs` workflows that sleep for a day, waits until all are parked,
//! shuts the engine down, then opens the directory again in a fresh process
//! and prints how long the open took, what it read, and the process's
//! resident size with every run parked: right after the open and after a few
//! seconds of idling.
use fusee::{Config, Ctx, Engine, Error};
use std::time::{Duration, Instant};

const DAY_MS: u64 = 86_400_000;

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Resident size of this process, in KiB.
fn rss_kib() -> u64 {
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        return s
            .lines()
            .find_map(|l| l.strip_prefix("VmRSS:"))
            .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
            .unwrap_or(0);
    }
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn cfg(dir: &str, shards: u32) -> Config {
    Config::local(dir)
        .shards(shards)
        .park_after(Duration::from_millis(0))
        .checkpoint_interval(Duration::from_secs(3600))
}

fn register(engine: &Engine) -> fusee::WorkflowRef<u64, ()> {
    engine.register("nap", |ctx: Ctx, at: u64| async move {
        ctx.sleep_until("nap", at).await?;
        Ok(())
    })
}

async fn create(dir: &str, n: u64, shards: u32) -> Result<(), Error> {
    let engine = Engine::open(cfg(dir, shards)).await?;
    let wf = std::sync::Arc::new(register(&engine));
    let at = unix_ms() + DAY_MS;
    let t0 = Instant::now();
    let batch = 50_000u64;
    for b in (0..n).step_by(batch as usize) {
        let mut tasks = Vec::new();
        for k in b..(b + batch).min(n) {
            let (wf, engine) = (wf.clone(), engine.clone());
            tasks.push(tokio::spawn(async move {
                loop {
                    match engine.start(&wf, &format!("p{k}"), &at).await {
                        Ok(_) => return,
                        Err(Error::Busy) => tokio::time::sleep(Duration::from_millis(1)).await,
                        Err(e) => panic!("{e}"),
                    }
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
    }
    let deadline = Instant::now() + Duration::from_secs(1800);
    loop {
        let m = engine.metrics().await?;
        if m.runs_parked >= n {
            break;
        }
        assert!(Instant::now() < deadline, "runs never parked: {m:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let created_s = t0.elapsed().as_secs_f64();
    let rss = rss_kib();
    engine.shutdown().await?;
    println!(
        "CREATE runs={n} shards={shards} seconds={created_s:.1} rss_mb_while_creating={}",
        rss / 1024
    );
    Ok(())
}

async fn open(dir: &str, n: u64, shards: u32) -> Result<(), Error> {
    let rss0 = rss_kib();
    let t0 = Instant::now();
    let engine = Engine::open(cfg(dir, shards)).await?;
    let open_ms = t0.elapsed().as_secs_f64() * 1e3;
    let _wf = register(&engine);
    let rss_open = rss_kib();
    let m = engine.metrics().await?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let m2 = engine.metrics().await?;
    let rss_idle = rss_kib();
    println!(
        "OPEN runs={n} shards={shards} open_ms={open_ms:.0} shard_open_us={} store_frames_read={} \
         store_bytes_read={} parked={} loaded={} rss_mb_before={} rss_mb_open={} rss_mb_idle={} \
         bytes_per_parked_run={:.1}",
        m.open_us,
        m.open_store_frames,
        m.open_store_bytes,
        m2.runs_parked.max(m2.parked_entries),
        m2.runs_loaded,
        rss0 / 1024,
        rss_open / 1024,
        rss_idle / 1024,
        (rss_idle as f64 * 1024.0) / n.max(1) as f64,
    );
    engine.shutdown().await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let dir = args.first().map(String::as_str).unwrap_or_else(|| {
        eprintln!("usage: parked_open <dir> <runs> [shards]");
        std::process::exit(2)
    });
    let n: u64 = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let shards: u32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8);
    if args.get(3).map(String::as_str) == Some("--open") {
        return open(dir, n, shards).await;
    }
    create(dir, n, shards).await?;
    // A fresh process, so the open's figures carry nothing from the creation.
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([dir, &n.to_string(), &shards.to_string(), "--open"])
        .status()
        .map_err(|e| Error::Io(e.to_string()))?;
    assert!(status.success(), "open child failed: {status}");
    Ok(())
}
