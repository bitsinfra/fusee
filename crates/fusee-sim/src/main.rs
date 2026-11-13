//! `fusee-sim`: run simulation seeds.
//!
//! ```text
//! fusee-sim --seed 42              one seed, verbose
//! fusee-sim --seeds 0..1000        a range (CI)
//! fusee-sim --random 10000         new random seeds (continuous job)
//! ```
//! Exit status 1 and the failing seed on stderr if any seed fails.

use fusee_sim::{SimConfig, run_seed};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut seeds: Vec<u64> = Vec::new();
    let mut verbose = false;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                seeds.push(args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(0));
                verbose = true;
                i += 1;
            }
            "--seeds" => {
                let spec = args.get(i + 1).cloned().unwrap_or_else(|| "0..100".into());
                let (a, b) = spec.split_once("..").unwrap_or(("0", "100"));
                let (a, b): (u64, u64) = (a.parse().unwrap_or(0), b.parse().unwrap_or(100));
                seeds.extend(a..b);
                i += 1;
            }
            "--random" => {
                let n: u64 = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(100);
                let base = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(1);
                seeds.extend(
                    (0..n).map(|k| base.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(k)),
                );
                i += 1;
            }
            "-v" | "--verbose" => verbose = true,
            _ => {}
        }
        i += 1;
    }
    if seeds.is_empty() {
        seeds.extend(0..100);
    }
    let started = std::time::Instant::now();
    let mut failures = 0;
    let mut ran = 0usize;
    let mut crashes = 0u64;
    let mut shared_seeds = 0u64;
    let mut shared_batches = 0u64;
    let (mut offload_seeds, mut offloaded) = (0u64, 0u64);
    let mut workflows = 0u64;
    let mut cover = [0u64; 9];
    let (mut chunks_read, mut streams_read) = (0u64, 0u64);
    for seed in &seeds {
        ran += 1;
        let cfg = SimConfig::for_seed(*seed);
        if verbose {
            eprintln!("{cfg:?}");
        }
        match run_seed(cfg) {
            Ok(r) => {
                if r.shared_log {
                    shared_seeds += 1;
                    shared_batches += r.log_batches;
                }
                if r.offloaded > 0 {
                    offload_seeds += 1;
                    offloaded += r.offloaded;
                }
                crashes += u64::from(r.crashes);
                workflows += u64::from(r.finished);
                chunks_read += r.chunks_read;
                streams_read += u64::from(r.streams_read);
                for (c, v) in cover.iter_mut().zip([
                    r.updates,
                    r.queries,
                    r.completions,
                    r.scheduled,
                    r.resets,
                    r.heartbeats,
                    r.reads_rerun,
                    r.queued,
                    r.windows,
                ]) {
                    *c += u64::from(v);
                }
                if verbose {
                    eprintln!("{r:?}");
                }
            }
            Err(f) => {
                failures += 1;
                eprintln!("{f}");
                if failures >= 5 {
                    break;
                }
            }
        }
    }
    println!(
        "{} seeds run, {} failed, {} crashes and recoveries, {} workflows finished, {:.1} s",
        ran,
        failures,
        crashes,
        workflows,
        started.elapsed().as_secs_f64()
    );
    println!(
        "covered: {} update IDs answered, {} queried runs, {} completions, {} scheduled runs, {} resets, {} heartbeats, {} read-only steps re-run on state a crash took back, {} debounced or batched events in {} windows",
        cover[0], cover[1], cover[2], cover[3], cover[4], cover[5], cover[6], cover[7], cover[8]
    );
    println!(
        "log format 3 (shared log): {shared_seeds} seeds, {shared_batches} batches in the final logs"
    );
    println!(
        "payload offload: {offload_seeds} seeds, {offloaded} records written to the blob area"
    );
    println!(
        "streams: {chunks_read} chunks read by readers, {streams_read} streams read to their end"
    );
    if failures > 0 {
        std::process::exit(1);
    }
}
