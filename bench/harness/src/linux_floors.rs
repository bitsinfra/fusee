//! Linux storage floors. Each lane has one request outstanding against the
//! same file. Lanes use separate extents so their writes never overlap.
use io_uring::{IoUring, opcode, squeue, types};
use serde_json::json;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Barrier;
use std::time::{Duration, Instant};

/// Bytes each lane cycles through (2 MiB). `FUSEE_FLOOR_EXTENT_MIB` sets it:
/// a log segment's size (64) makes each write land on a block not written
/// since the file was zero-filled, as the log's appends do.
fn extent() -> u64 {
    std::env::var("FUSEE_FLOOR_EXTENT_MIB")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&m| (1..=1024).contains(&m))
        .unwrap_or(2)
        << 20
}
#[repr(align(4096))]
struct Block([u8; 64 << 10]);

#[derive(Clone, Copy)]
enum Method {
    Fdatasync,
    Dsync,
    Uring,
}

impl Method {
    fn name(self) -> &'static str {
        match self {
            Self::Fdatasync => "pwrite+fdatasync",
            Self::Dsync => "pwritev2(RWF_DSYNC)",
            Self::Uring => "io_uring linked write+fdatasync",
        }
    }
}

fn linked(ring: &mut IoUring, file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    let fd = types::Fd(file.as_raw_fd());
    let write = opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
        .offset(offset)
        .build()
        .flags(squeue::Flags::IO_LINK)
        .user_data(buf.len() as u64);
    let flush = opcode::Fsync::new(fd)
        .flags(types::FsyncFlags::DATASYNC)
        .build();
    // SAFETY: the slice and the file live until both completions are consumed.
    unsafe { ring.submission().push_multiple(&[write, flush]) }.map_err(io::Error::other)?;
    let mut completed = 0;
    let mut error = None;
    while completed < 2 {
        match ring.submit_and_wait(2 - completed) {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            r => {
                r?;
            }
        }
        for item in ring.completion() {
            completed += 1;
            if item.result() < 0 {
                error.get_or_insert(io::Error::from_raw_os_error(-item.result()));
            } else if item.result() as u64 != item.user_data() {
                error.get_or_insert(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "short linked write",
                ));
            }
        }
    }
    error.map_or(Ok(()), Err)
}

fn probe(
    file: &File,
    bytes: usize,
    qd: usize,
    method: Method,
    budget: Duration,
) -> io::Result<Vec<Vec<u64>>> {
    let rings = (0..qd)
        .map(|_| match method {
            Method::Uring => IoUring::new(4).map(Some),
            _ => Ok(None),
        })
        .collect::<io::Result<Vec<_>>>()?;
    let barrier = Barrier::new(qd);
    std::thread::scope(|scope| {
        let workers = rings
            .into_iter()
            .enumerate()
            .map(|(lane, mut ring)| {
                let barrier = &barrier;
                scope.spawn(move || -> io::Result<Vec<u64>> {
                    let block = Block([0x5a; 64 << 10]);
                    let buf = &block.0[..bytes];
                    let base = (lane as u64) * extent();
                    let mut off = 0;
                    let mut samples = Vec::new();
                    barrier.wait();
                    let end = Instant::now() + budget;
                    while Instant::now() < end || samples.is_empty() {
                        let t = Instant::now();
                        match method {
                            Method::Fdatasync => {
                                file.write_all_at(buf, base + off)?;
                                file.sync_data()?;
                            }
                            Method::Dsync => fusee::local::pwrite_dsync(file, buf, base + off)?,
                            Method::Uring => linked(ring.as_mut().unwrap(), file, buf, base + off)?,
                        }
                        samples.push(t.elapsed().as_nanos() as u64);
                        off = (off + bytes as u64) % extent();
                    }
                    Ok(samples)
                })
            })
            .collect::<Vec<_>>();
        workers
            .into_iter()
            .map(|w| {
                w.join()
                    .map_err(|_| io::Error::other("floor worker panicked"))?
            })
            .collect()
    })
}

struct ProbeFile(PathBuf);
impl Drop for ProbeFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

pub fn run(args: &[String]) -> io::Result<()> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "io-floors <data-dir> <raw-output-dir> [seconds-per-case] [depths,comma-separated]",
        )
    };
    if !(2..=4).contains(&args.len()) {
        return Err(invalid());
    }
    let dir = Path::new(&args[0]);
    let raw = Path::new(&args[1]);
    let secs: u64 = args
        .get(2)
        .map_or(Ok(2), |n| n.parse().map_err(|_| invalid()))?;
    let depths = args
        .get(3)
        .map(String::as_str)
        .unwrap_or("1,2,4,8,16,32")
        .split(',')
        .map(|n| n.parse::<usize>().map_err(|_| invalid()))
        .collect::<io::Result<Vec<_>>>()?;
    if secs == 0 || depths.iter().any(|&d| !(1..=32).contains(&d)) {
        return Err(invalid());
    }
    std::fs::create_dir_all(dir)?;
    std::fs::create_dir_all(raw)?;
    let path = dir.join(format!(".fusee-floor-{}", std::process::id()));
    let mut init = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)?;
    let cleanup = ProbeFile(path);
    let zeros = vec![0u8; 1 << 20];
    let lanes = depths.iter().copied().max().unwrap_or(1) as u64;
    for _ in 0..((lanes * extent()) >> 20).max(64) {
        init.write_all(&zeros)?;
    }
    init.sync_all()?;
    drop(init);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(&cleanup.0)?;
    let host = super::machine();
    for method in [Method::Fdatasync, Method::Dsync, Method::Uring] {
        for bytes in [4096, 8192, 16384, 32768, 65536] {
            for &qd in &depths {
                let lanes = probe(&file, bytes, qd, method, Duration::from_secs(secs))?;
                let name = format!(
                    "{}-{bytes}-{qd}.ns",
                    method.name().split_whitespace().next().unwrap()
                );
                let mut out = std::io::BufWriter::new(File::create_new(raw.join(&name))?);
                let mut hist = super::hist();
                let counts = lanes.iter().map(Vec::len).collect::<Vec<_>>();
                for samples in lanes {
                    for ns in samples {
                        out.write_all(&ns.to_le_bytes())?;
                        hist.record((ns / 1000).max(1)).map_err(io::Error::other)?;
                    }
                }
                out.flush()?;
                println!(
                    "{}",
                    json!({"machine":host,"workload":"linux-io-floor","method":method.name(),"bytes":bytes,
                    "open_flags":"O_DIRECT","queue_depth":qd,"seconds_per_lane":secs,"lane_counts":counts,
                    "samples_u64le_ns":name,"latency":super::summary(&hist)})
                );
            }
        }
    }
    Ok(())
}

/// CPU time of this thread so far: user and system, in µs, and voluntary
/// context switches.
fn thread_cpu() -> (f64, f64, u64) {
    let mut u = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the output on success.
    if unsafe { libc::getrusage(libc::RUSAGE_THREAD, u.as_mut_ptr()) } != 0 {
        return (0.0, 0.0, 0);
    }
    // SAFETY: as above.
    let u = unsafe { u.assume_init() };
    let us = |t: libc::timeval| t.tv_sec as f64 * 1e6 + t.tv_usec as f64;
    (us(u.ru_utime), us(u.ru_stime), u.ru_nvcsw as u64)
}

/// Every CPU's time in `/proc/stat` so far, in µs: (busy, irq + softirq).
/// Busy is everything but idle and iowait.
fn system_cpu() -> (f64, f64) {
    let Ok(stat) = std::fs::read_to_string("/proc/stat") else {
        return (0.0, 0.0);
    };
    let Some(cpu) = stat.lines().find(|l| l.starts_with("cpu ")) else {
        return (0.0, 0.0);
    };
    let v: Vec<f64> = cpu
        .split_whitespace()
        .skip(1)
        .filter_map(|x| x.parse().ok())
        .collect();
    // SAFETY: sysconf has no preconditions.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as f64;
    let tick = 1e6 / hz;
    let get = |i: usize| v.get(i).copied().unwrap_or(0.0);
    // user nice system idle iowait irq softirq steal
    let busy = get(0) + get(1) + get(2) + get(5) + get(6) + get(7);
    (busy * tick, (get(5) + get(6)) * tick)
}

/// `write-cpu <data-dir> [seconds]`: what one small durable write costs in
/// CPU, by method, at queue depth 1 on an otherwise idle host: the writing
/// thread's user and system time and voluntary context switches per write,
/// and every CPU's busy and interrupt (irq + softirq) time per write from
/// `/proc/stat` (tick resolution: run long enough for many ticks). What
/// all CPUs' busy time adds to the thread's is what the write costs
/// elsewhere: the completion interrupt and any kernel thread.
pub fn write_cpu(args: &[String]) -> io::Result<()> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "write-cpu <data-dir> [seconds]",
        )
    };
    if !(1..=2).contains(&args.len()) {
        return Err(invalid());
    }
    let dir = Path::new(&args[0]);
    let secs: u64 = args
        .get(1)
        .map_or(Ok(3), |n| n.parse().map_err(|_| invalid()))?;
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!(".fusee-write-cpu-{}", std::process::id()));
    let mut init = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)?;
    let cleanup = ProbeFile(path);
    let zeros = vec![0u8; 1 << 20];
    for _ in 0..(extent() >> 20) {
        init.write_all(&zeros)?;
    }
    init.sync_all()?;
    drop(init);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(&cleanup.0)?;
    let host = super::machine();
    let block = Block([0x5a; 64 << 10]);
    let mut ring = IoUring::new(4)?;
    for (name, bytes) in [
        ("pwritev2(RWF_DSYNC)", 4096usize),
        ("pwrite+fdatasync", 4096),
        ("io_uring write(RWF_DSYNC)", 4096),
        ("pwritev2(RWF_DSYNC)", 16384),
    ] {
        let buf = &block.0[..bytes];
        let mut hist = super::hist();
        let mut off = 0u64;
        let mut writes = 0u64;
        let (u0, s0, c0) = thread_cpu();
        let (b0, i0) = system_cpu();
        let started = Instant::now();
        let end = started + Duration::from_secs(secs);
        while Instant::now() < end {
            let t = Instant::now();
            match name {
                "pwrite+fdatasync" => {
                    file.write_all_at(buf, off)?;
                    file.sync_data()?;
                }
                "io_uring write(RWF_DSYNC)" => {
                    let fd = types::Fd(file.as_raw_fd());
                    let write = opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
                        .offset(off)
                        .rw_flags(libc::RWF_DSYNC)
                        .build()
                        .user_data(1);
                    // SAFETY: the buffer and the file outlive the completion,
                    // which is consumed below before either is touched.
                    unsafe { ring.submission().push(&write) }.map_err(io::Error::other)?;
                    loop {
                        match ring.submit_and_wait(1) {
                            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                            r => {
                                r?;
                                break;
                            }
                        }
                    }
                    for c in ring.completion() {
                        if c.result() < 0 {
                            return Err(io::Error::from_raw_os_error(-c.result()));
                        }
                    }
                }
                _ => fusee::local::pwrite_dsync(&file, buf, off)?,
            }
            hist.record((t.elapsed().as_nanos() as u64 / 1000).max(1))
                .map_err(io::Error::other)?;
            writes += 1;
            off = (off + bytes as u64) % extent();
        }
        let wall = started.elapsed().as_secs_f64() * 1e6;
        let (u1, s1, c1) = thread_cpu();
        let (b1, i1) = system_cpu();
        let per = |v: f64| (v / writes.max(1) as f64 * 1000.0).round() / 1000.0;
        println!(
            "{}",
            json!({"machine":host,"workload":"write-cpu","method":name,"bytes":bytes,"open_flags":"O_DIRECT",
                "queue_depth":1,"writes":writes,"wall_us_per_write":per(wall),"latency":super::summary(&hist),
                "thread_user_us_per_write":per(u1 - u0),"thread_system_us_per_write":per(s1 - s0),
                "thread_voluntary_switches_per_write":per((c1 - c0) as f64),
                "all_cpus_busy_us_per_write":per(b1 - b0),"all_cpus_irq_us_per_write":per(i1 - i0)})
        );
    }
    Ok(())
}
