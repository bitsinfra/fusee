//! Development-machine storage floors on uncached regular files.
use serde_json::json;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Barrier;
use std::time::{Duration, Instant};

struct Probe(PathBuf);
impl Drop for Probe {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn probe(
    file: &File,
    block: usize,
    depth: usize,
    full: bool,
    period: Duration,
) -> io::Result<Vec<Vec<u64>>> {
    const EXTENT: u64 = 2 << 20;
    let ready = Barrier::new(depth);
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for lane in 0..depth {
            let ready = &ready;
            workers.push(scope.spawn(move || -> io::Result<Vec<u64>> {
                let buf = vec![0x5a; block];
                let mut samples = Vec::new();
                let mut offset = 0u64;
                ready.wait();
                let until = Instant::now() + period;
                while Instant::now() < until || samples.is_empty() {
                    let start = Instant::now();
                    file.write_all_at(&buf, lane as u64 * EXTENT + offset)?;
                    if full {
                        fusee::local::flush_file(file, fusee::FlushMethod::FullFsync)?;
                    } else {
                        file.sync_all()?;
                    }
                    samples.push(start.elapsed().as_nanos() as u64);
                    offset = (offset + block as u64) % EXTENT;
                }
                Ok(samples)
            }));
        }
        workers
            .into_iter()
            .map(|worker| {
                worker
                    .join()
                    .map_err(|_| io::Error::other("floor lane panicked"))?
            })
            .collect()
    })
}

pub fn run(args: &[String]) -> io::Result<()> {
    let invalid = || io::Error::other("io-floors <data-dir> <raw-output-dir> [seconds-per-case]");
    if !(2..=3).contains(&args.len()) {
        return Err(invalid());
    }
    let seconds = args
        .get(2)
        .map_or(Ok(2), |v| v.parse::<u64>().map_err(|_| invalid()))?;
    if seconds == 0 {
        return Err(invalid());
    }
    let data = Path::new(&args[0]);
    let raw = Path::new(&args[1]);
    std::fs::create_dir_all(data)?;
    std::fs::create_dir_all(raw)?;
    let path = data.join(format!("fusee-floor-{}", std::process::id()));
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    let _cleanup = Probe(path);
    // SAFETY: F_NOCACHE takes an integer argument and file holds a live descriptor.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let initial = vec![0u8; 1 << 20];
    for mb in 0..64 {
        file.write_all_at(&initial, mb << 20)?;
    }
    fusee::local::flush_file(&file, fusee::FlushMethod::FullFsync)?;
    let host = super::machine();
    for full in [true, false] {
        let method = if full { "F_FULLFSYNC" } else { "fsync" };
        for bytes in [4096, 8192, 16384, 32768, 65536] {
            for qd in [1, 2, 4, 8, 16, 32] {
                let lanes = probe(&file, bytes, qd, full, Duration::from_secs(seconds))?;
                let name = format!("{method}-{bytes}-{qd}.ns");
                let mut output = io::BufWriter::new(File::create_new(raw.join(&name))?);
                let mut h = super::hist();
                let counts = lanes.iter().map(Vec::len).collect::<Vec<_>>();
                for samples in lanes {
                    for ns in samples {
                        output.write_all(&ns.to_le_bytes())?;
                        h.record((ns / 1000).max(1)).map_err(io::Error::other)?;
                    }
                }
                output.flush()?;
                println!(
                    "{}",
                    json!({"machine":host,"workload":"mac-io-floor","method":method,"bytes":bytes,
                    "open_flags":"F_NOCACHE","queue_depth":qd,"seconds_per_lane":seconds,"lane_counts":counts,
                    "samples_u64le_ns":name,"latency":super::summary(&h)})
                );
            }
        }
    }
    Ok(())
}
