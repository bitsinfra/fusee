//! `fusee disk-check`: measures the flush latency of the disk under a data
//! directory, so people can see their own floor.
//!
//! Each probe preallocates a zero-filled file, then repeats "write N bytes
//! at the next offset, then make it durable" and records each iteration's
//! time. This is the same pattern as the log's flusher at low load.

use crate::local::{FlushMethod, flush_file};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq)]
pub struct Probe {
    pub method: &'static str,
    pub bytes: usize,
    pub samples: usize,
    pub p50_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub max_us: f64,
    pub mean_us: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DiskReport {
    pub dir: String,
    pub platform: &'static str,
    pub probes: Vec<Probe>,
    pub notes: Vec<String>,
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

pub fn summarize(method: &'static str, bytes: usize, mut us: Vec<f64>) -> Probe {
    us.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mean = if us.is_empty() {
        0.0
    } else {
        us.iter().sum::<f64>() / us.len() as f64
    };
    Probe {
        method,
        bytes,
        samples: us.len(),
        p50_us: percentile(&us, 0.50),
        p99_us: percentile(&us, 0.99),
        p999_us: percentile(&us, 0.999),
        max_us: us.last().copied().unwrap_or(0.0),
        mean_us: mean,
    }
}

fn probe_file(dir: &Path, name: &str, size: u64) -> io::Result<File> {
    let path = dir.join(name);
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;
    let zeros = vec![0u8; 1 << 20];
    let mut off = 0;
    while off < size {
        let n = (size - off).min(zeros.len() as u64) as usize;
        f.write_all_at(&zeros[..n], off)?;
        off += n as u64;
    }
    f.sync_all()?;
    Ok(f)
}

fn run_probe(
    dir: &Path,
    method: FlushMethod,
    label: &'static str,
    bytes: usize,
    budget: Duration,
    max_samples: usize,
) -> io::Result<Probe> {
    let size = 64u64 << 20;
    let f = probe_file(dir, ".fusee-disk-check", size)?;
    let buf = vec![0xa5u8; bytes];
    let mut us = Vec::with_capacity(max_samples);
    let start = Instant::now();
    let mut off = 0u64;
    while us.len() < max_samples && (start.elapsed() < budget || us.len() < 20) {
        if off + bytes as u64 > size {
            off = 0;
        }
        let t = Instant::now();
        if method == FlushMethod::Dsync {
            crate::local::pwrite_dsync(&f, &buf, off)?;
        } else {
            f.write_all_at(&buf, off)?;
            flush_file(&f, method)?;
        }
        us.push(t.elapsed().as_secs_f64() * 1e6);
        off += bytes as u64;
    }
    drop(f);
    let _ = std::fs::remove_file(dir.join(".fusee-disk-check"));
    Ok(summarize(label, bytes, us))
}

/// `O_DIRECT` writes of `bytes` from an aligned buffer with `RWF_DSYNC`,
/// over a written, synced file: what the local log does by default.
#[cfg(target_os = "linux")]
fn direct_probe(
    dir: &Path,
    bytes: usize,
    budget: Duration,
    max_samples: usize,
) -> io::Result<Probe> {
    use std::os::unix::fs::OpenOptionsExt;
    const ALIGN: usize = 4096;
    let size = 64u64 << 20;
    let path = dir.join(".fusee-disk-check");
    drop(probe_file(dir, ".fusee-disk-check", size)?);
    let opened = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(&path);
    let f = match opened {
        Ok(f) => f,
        Err(e) => {
            let _ = std::fs::remove_file(&path);
            return Err(e);
        }
    };
    let layout = std::alloc::Layout::from_size_align(bytes.max(ALIGN), ALIGN)
        .map_err(|e| io::Error::other(e.to_string()))?;
    // SAFETY: the layout has non-zero size; freed below with the same layout.
    let raw = unsafe { std::alloc::alloc_zeroed(layout) };
    if raw.is_null() {
        std::alloc::handle_alloc_error(layout);
    }
    // SAFETY: `raw` holds at least `bytes` zeroed bytes owned here.
    let buf = unsafe { std::slice::from_raw_parts_mut(raw, bytes) };
    buf.fill(0xa5);
    let mut us = Vec::with_capacity(max_samples);
    let start = Instant::now();
    let mut off = 0u64;
    let mut result = Ok(());
    while us.len() < max_samples && (start.elapsed() < budget || us.len() < 20) {
        if off + bytes as u64 > size {
            off = 0;
        }
        let t = Instant::now();
        if let Err(e) = crate::local::pwrite_dsync(&f, buf, off) {
            result = Err(e);
            break;
        }
        us.push(t.elapsed().as_secs_f64() * 1e6);
        off += bytes as u64;
    }
    // SAFETY: allocated above with this layout; `buf` is not used after.
    unsafe { std::alloc::dealloc(raw, layout) };
    drop(f);
    let _ = std::fs::remove_file(&path);
    result?;
    Ok(summarize("O_DIRECT + RWF_DSYNC write", bytes, us))
}

/// Measures flush latency under `dir` for about `budget` per probe.
pub fn disk_check(dir: &Path, budget: Duration) -> io::Result<DiskReport> {
    std::fs::create_dir_all(dir)?;
    let mut probes = Vec::new();
    let mut notes = Vec::new();
    let platform = if cfg!(target_vendor = "apple") {
        "macos"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else {
        "other"
    };
    for bytes in [4096usize, 16384, 65536] {
        if cfg!(target_vendor = "apple") {
            probes.push(run_probe(
                dir,
                FlushMethod::FullFsync,
                "F_FULLFSYNC",
                bytes,
                budget,
                2000,
            )?);
        } else {
            probes.push(run_probe(
                dir,
                FlushMethod::Fdatasync,
                "fdatasync",
                bytes,
                budget,
                20000,
            )?);
        }
    }
    if cfg!(target_vendor = "apple") {
        probes.push(run_probe(
            dir,
            FlushMethod::Fsync,
            "fsync (not durable on macOS)",
            4096,
            budget,
            20000,
        )?);
        notes.push(
            "macOS: only F_FULLFSYNC reaches stable media; plain fsync leaves data in the drive's \
             cache. Fusee's local mode uses F_FULLFSYNC."
                .into(),
        );
    }
    if cfg!(target_os = "linux") {
        probes.push(run_probe(
            dir,
            FlushMethod::Dsync,
            "RWF_DSYNC write",
            4096,
            budget,
            20000,
        )?);
        // The local log's default write: the floor its steps pay.
        #[cfg(target_os = "linux")]
        match direct_probe(dir, 4096, budget, 20000) {
            Ok(p) => probes.push(p),
            Err(e) => notes.push(format!(
                "O_DIRECT + RWF_DSYNC not measured ({e}); the log falls back to buffered writes here"
            )),
        }
        if let Some(p) = probes.first()
            && p.p50_us > 300.0
        {
            notes.push(format!(
                "p50 flush is {:.0} µs: this looks like a drive without power-loss protection, \
                     network storage, or a virtual disk. Datacenter NVMe drives flush in 1-30 µs.",
                p.p50_us
            ));
        }
    }
    Ok(DiskReport {
        dir: dir.display().to_string(),
        platform,
        probes,
        notes,
    })
}

impl std::fmt::Display for DiskReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Flush latency under {} ({})", self.dir, self.platform)?;
        writeln!(
            f,
            "{:<30} {:>8} {:>8} {:>10} {:>10} {:>10} {:>10}",
            "method", "bytes", "samples", "p50 µs", "p99 µs", "p99.9 µs", "max µs"
        )?;
        for p in &self.probes {
            writeln!(
                f,
                "{:<30} {:>8} {:>8} {:>10.1} {:>10.1} {:>10.1} {:>10.1}",
                p.method, p.bytes, p.samples, p.p50_us, p.p99_us, p.p999_us, p.max_us
            )?;
        }
        for n in &self.notes {
            writeln!(f, "\nnote: {n}")?;
        }
        Ok(())
    }
}
