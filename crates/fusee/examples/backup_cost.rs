//! What continuous backup costs the write path: step latency of a
//! sequential chain and records per flush of a parallel load, with the
//! backup off and on, runs interleaved so that load on the host affects
//! every mode alike.
//!
//! cargo run --release -p fusee --example backup_cost -- [rounds] [fullfsync-steps] [fsync-steps]
//!
//! (`fullfsync-steps` 0 skips the platform's default flush where it is not `fsync`.)
//!
//! Modes: `off`; `on/disk`, a backup to a directory on the same disk
//! (its uploads flush that disk, the worst case for the log); `on/nosync`,
//! a backup to a directory without flushes (the shipper's reads and CPU
//! without disk contention, close to a remote target). The backup runs
//! with its defaults (upload every second) and, in `on/50ms`, uploading
//! every 50 ms to the same disk.
use fusee::backup::{BackupOptions, LocalTarget, Target};
use fusee::{Config, Ctx, Engine, Error, FlushMethod};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Off,
    Disk,
    NoSync,
    Fast,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Off => "off",
            Mode::Disk => "on/disk",
            Mode::NoSync => "on/nosync",
            Mode::Fast => "on/50ms",
        }
    }
}

struct Sample {
    p50_us: f64,
    p99_us: f64,
    records_per_flush: f64,
    par_runs_per_s: f64,
    uploaded: u64,
}

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "fusee-backup-cost-{name}-{}-{}",
        std::process::id(),
        fusee::local::unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.total_cmp(b));
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

async fn once(mode: Mode, flush: FlushMethod, steps: u32) -> Sample {
    let dir = tmp("dir");
    let tdir = tmp("target");
    let mut config = Config::local(&dir).shards(2);
    config.local.flush = flush;
    if mode != Mode::Off {
        let t = LocalTarget::new(&tdir).unwrap();
        let t = if mode == Mode::NoSync {
            t.without_sync()
        } else {
            t
        };
        let target: Arc<dyn Target> = Arc::new(t);
        let mut opts = BackupOptions::default();
        if mode == Mode::Fast {
            opts.upload_interval = Some(Duration::from_millis(50));
            opts.resolution = Duration::from_millis(50);
        }
        config = config.backup(target, opts);
    }
    let engine = Engine::open(config).await.unwrap();
    let marks: Arc<Mutex<Vec<Instant>>> = Arc::new(Mutex::new(Vec::with_capacity(steps as usize)));
    let m = marks.clone();
    let chain = engine.register("chain", move |ctx: Ctx, n: u32| {
        let m = m.clone();
        async move {
            for i in 0..n {
                let m = m.clone();
                ctx.step(&format!("s{i}"), move || {
                    m.lock().unwrap().push(Instant::now());
                    async move { Ok::<_, Error>(i) }
                })
                .await?;
            }
            Ok(n)
        }
    });
    let par = engine.register("par", |ctx: Ctx, n: u32| async move {
        for i in 0..10u32 {
            ctx.step(
                &format!("p{i}"),
                move || async move { Ok::<_, Error>(n + i) },
            )
            .await?;
        }
        Ok(n)
    });
    engine
        .start(&chain, "chain", &steps)
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    let mut deltas: Vec<f64> = marks
        .lock()
        .unwrap()
        .windows(2)
        .skip(10)
        .map(|w| (w[1] - w[0]).as_secs_f64() * 1e6)
        .collect();
    let before = engine.metrics().await.unwrap();
    let started = Instant::now();
    let runs = if flush == FlushMethod::Fsync {
        2000
    } else {
        400
    };
    let mut handles = Vec::new();
    for i in 0..runs {
        handles.push(engine.start(&par, &format!("p{i}"), &i).await.unwrap());
    }
    for h in handles {
        h.result().await.unwrap();
    }
    let secs = started.elapsed().as_secs_f64();
    let after = engine.metrics().await.unwrap();
    let uploaded = engine
        .backup()
        .and_then(|b| b.status())
        .map_or(0, |s| s.objects_uploaded);
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&tdir);
    Sample {
        p50_us: pct(&mut deltas, 0.5),
        p99_us: pct(&mut deltas, 0.99),
        records_per_flush: (after.flushed_records - before.flushed_records) as f64
            / (after.flushes - before.flushes).max(1) as f64,
        par_runs_per_s: runs as f64 / secs,
        uploaded,
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    pct(&mut v, 0.5)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rounds: usize = args.first().and_then(|s| s.parse().ok()).unwrap_or(5);
    let full_steps: u32 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1500);
    let fsync_steps: u32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(20_000);
    let modes = [Mode::Off, Mode::Disk, Mode::NoSync, Mode::Fast];
    let mut flushes = vec![(FlushMethod::Fsync, fsync_steps)];
    let default = FlushMethod::platform_default();
    if default != FlushMethod::Fsync && full_steps > 0 {
        flushes.insert(0, (default, full_steps));
    }
    for (flush, steps) in flushes {
        println!(
            "flush {flush:?}: chain of {steps} steps, parallel load; {rounds} rounds, modes rotated"
        );
        let mut results: Vec<Vec<Sample>> = modes.iter().map(|_| Vec::new()).collect();
        for round in 0..rounds {
            for k in 0..modes.len() {
                let i = (k + round) % modes.len();
                results[i].push(once(modes[i], flush, steps).await);
            }
        }
        let off_p50 = median(results[0].iter().map(|s| s.p50_us).collect());
        let off_rpf = median(results[0].iter().map(|s| s.records_per_flush).collect());
        let off_rate = median(results[0].iter().map(|s| s.par_runs_per_s).collect());
        println!(
            "  {:<10} {:>10} {:>10} {:>8} {:>12} {:>9} {:>11} {:>8} {:>9}",
            "mode",
            "p50 µs",
            "p99 µs",
            "p50 x",
            "recs/flush",
            "rpf x",
            "par runs/s",
            "rate x",
            "uploads"
        );
        for (i, m) in modes.iter().enumerate() {
            let r = &results[i];
            let p50 = median(r.iter().map(|s| s.p50_us).collect());
            let p99 = median(r.iter().map(|s| s.p99_us).collect());
            let rpf = median(r.iter().map(|s| s.records_per_flush).collect());
            let rate = median(r.iter().map(|s| s.par_runs_per_s).collect());
            let up = median(r.iter().map(|s| s.uploaded as f64).collect());
            println!(
                "  {:<10} {:>10.1} {:>10.1} {:>8.3} {:>12.2} {:>9.3} {:>11.0} {:>8.3} {:>9.0}",
                m.name(),
                p50,
                p99,
                p50 / off_p50,
                rpf,
                rpf / off_rpf,
                rate,
                rate / off_rate,
                up
            );
        }
        // Per-round ratios against the same round's `off`, for spread.
        for (i, m) in modes.iter().enumerate().skip(1) {
            let ratios: Vec<String> = results[i]
                .iter()
                .zip(&results[0])
                .map(|(on, off)| format!("{:.3}", on.p50_us / off.p50_us))
                .collect();
            println!("  {} p50 ratio per round: {}", m.name(), ratios.join(" "));
            let rates: Vec<String> = results[i]
                .iter()
                .zip(&results[0])
                .map(|(on, off)| format!("{:.3}", on.par_runs_per_s / off.par_runs_per_s))
                .collect();
            println!(
                "  {} parallel rate ratio per round: {}",
                m.name(),
                rates.join(" ")
            );
        }
    }
}
