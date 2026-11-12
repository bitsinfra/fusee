//! CPU by thread, so the engine's cost per step is reported without the load
//! generator's.
//!
//! On Linux each snapshot reads `/proc/self/task/<tid>/{stat,comm,schedstat}`
//! and the process's `getrusage`. Threads are grouped by name: the engine's
//! (`fusee-shard-N`, `fusee-flush`, `fusee-segment`, `fusee-checkpoint*`,
//! `fusee-remote`, `fusee-ping`, `fusee-client`, `fusee-replica-N`, and the
//! tokio workers, `tokio-rt-worker`) and the harness's (`fusee-bench`, the
//! main thread; `fusee-bench-gen`, the arrival generator; other
//! `fusee-bench-*` helpers). A thread that exits inside the window (a segment
//! zero-fill, say) is not in the end snapshot; its CPU appears as
//! `exited_threads_us`, the process total minus the live threads. Other
//! systems report `null` with the reason.
//!
//! Each group also reports its context switches (voluntary: the thread
//! blocked, typically one per park or wait; involuntary: preempted) from
//! `status`, and, where `perf_event_open` has a PMU, instructions and
//! cycles per thread (kernel included when `perf_event_paranoid` allows,
//! else user space only). Counters open at a thread's first snapshot, so a
//! thread created inside the window is not counted; `counted_threads` says
//! how many were.

use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::time::Instant;

#[derive(Clone, Debug)]
struct Thread {
    comm: String,
    user_ticks: u64,
    system_ticks: u64,
    /// Time on CPU from `schedstat`, in nanoseconds (more precise than ticks).
    run_ns: Option<u64>,
    /// Voluntary and involuntary context switches so far.
    switches: Option<(u64, u64)>,
    /// Instructions and cycles since the thread's counters opened.
    counts: Option<(u64, u64)>,
}

#[derive(Debug)]
pub struct Snapshot {
    at: Instant,
    user_us: u64,
    system_us: u64,
    threads: Option<BTreeMap<u32, Thread>>,
}

pub fn snapshot() -> Snapshot {
    let threads = tasks();
    let (user_us, system_us) = rusage();
    Snapshot {
        at: Instant::now(),
        user_us,
        system_us,
        threads,
    }
}

fn rusage() -> (u64, u64) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the output on success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return (0, 0);
    }
    // SAFETY: as above.
    let usage = unsafe { usage.assume_init() };
    let micros = |t: libc::timeval| t.tv_sec as u64 * 1_000_000 + t.tv_usec as u64;
    (micros(usage.ru_utime), micros(usage.ru_stime))
}

#[cfg(target_os = "linux")]
fn tasks() -> Option<BTreeMap<u32, Thread>> {
    let mut out = BTreeMap::new();
    for entry in std::fs::read_dir("/proc/self/task").ok()?.flatten() {
        let Some(tid) = entry.file_name().to_str().and_then(|s| s.parse().ok()) else {
            continue;
        };
        let dir = entry.path();
        // A thread can exit between the listing and the read.
        let Ok(stat) = std::fs::read_to_string(dir.join("stat")) else {
            continue;
        };
        // The name may contain spaces and parentheses; fields resume after
        // the last ')'. utime and stime are fields 14 and 15.
        let Some((head, rest)) = stat.rsplit_once(')') else {
            continue;
        };
        let fields: Vec<&str> = rest.split_whitespace().collect();
        let (Some(user_ticks), Some(system_ticks)) = (
            fields.get(11).and_then(|v| v.parse().ok()),
            fields.get(12).and_then(|v| v.parse().ok()),
        ) else {
            continue;
        };
        let comm = std::fs::read_to_string(dir.join("comm"))
            .map(|c| c.trim_end().to_owned())
            .unwrap_or_else(|_| head.split_once('(').map_or("", |(_, n)| n).to_owned());
        let run_ns = std::fs::read_to_string(dir.join("schedstat"))
            .ok()
            .and_then(|s| s.split_whitespace().next()?.parse().ok());
        let switches = std::fs::read_to_string(dir.join("status"))
            .ok()
            .and_then(|s| {
                let field = |name: &str| {
                    s.lines()
                        .find_map(|l| l.strip_prefix(name))
                        .and_then(|v| v.trim().parse::<u64>().ok())
                };
                Some((
                    field("voluntary_ctxt_switches:")?,
                    field("nonvoluntary_ctxt_switches:")?,
                ))
            });
        out.insert(
            tid,
            Thread {
                comm,
                user_ticks,
                system_ticks,
                run_ns,
                switches,
                counts: None,
            },
        );
    }
    perf::count(&mut out);
    Some(out)
}

/// Per-thread instruction and cycle counters (`perf_event_open(2)`), kept
/// open from a thread's first snapshot until it exits.
#[cfg(target_os = "linux")]
mod perf {
    use super::Thread;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// `perf_event_attr` up to `PERF_ATTR_SIZE_VER0` (64 bytes).
    #[repr(C)]
    #[derive(Default)]
    struct Attr {
        kind: u32,
        size: u32,
        config: u64,
        sample_period: u64,
        sample_type: u64,
        read_format: u64,
        flags: u64,
        wakeup_events: u32,
        bp_type: u32,
        config1: u64,
    }

    const EXCLUDE_KERNEL: u64 = 1 << 5;
    const EXCLUDE_HV: u64 = 1 << 6;
    const PERF_FLAG_FD_CLOEXEC: libc::c_ulong = 1 << 3;

    struct State {
        /// Per thread: its instructions and cycles counters.
        fds: BTreeMap<u32, (i32, i32)>,
        /// Kernel excluded (`perf_event_paranoid` of 2 or more).
        user_only: bool,
        /// Why there are no counters, once an open failed for good.
        unavailable: Option<String>,
    }

    static STATE: Mutex<State> = Mutex::new(State {
        fds: BTreeMap::new(),
        user_only: false,
        unavailable: None,
    });

    fn open(tid: u32, config: u64, user_only: bool) -> Result<i32, i32> {
        let attr = Attr {
            kind: 0, // PERF_TYPE_HARDWARE
            size: std::mem::size_of::<Attr>() as u32,
            config,
            flags: if user_only {
                EXCLUDE_KERNEL | EXCLUDE_HV
            } else {
                0
            },
            ..Attr::default()
        };
        // SAFETY: `attr` is a valid perf_event_attr prefix of the size it
        // states; it counts thread `tid` on any CPU.
        let fd = unsafe {
            libc::syscall(
                libc::SYS_perf_event_open,
                &raw const attr,
                tid as libc::pid_t,
                -1,
                -1,
                PERF_FLAG_FD_CLOEXEC,
            )
        };
        if fd < 0 {
            Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
        } else {
            Ok(fd as i32)
        }
    }

    fn value(fd: i32) -> u64 {
        let mut v = 0u64;
        // SAFETY: reads at most 8 bytes into `v`.
        let n = unsafe { libc::read(fd, (&raw mut v).cast(), 8) };
        if n == 8 { v } else { 0 }
    }

    fn close(fd: i32) {
        // SAFETY: `fd` is a counter this module opened and still owns.
        unsafe { libc::close(fd) };
    }

    /// Opens counters for threads seen for the first time, closes those of
    /// threads gone, and records every live thread's counts.
    pub fn count(threads: &mut BTreeMap<u32, Thread>) {
        let mut s = STATE.lock().unwrap_or_else(|e| e.into_inner());
        if s.unavailable.is_some() {
            return;
        }
        // Counting events on a thread costs it on every context switch, and
        // shard threads switch on almost every step at low rates: on an
        // i8g.2xlarge the counters doubled step latency and halved the
        // highest rate held under 1 ms. Off unless asked for.
        if std::env::var("FUSEE_BENCH_COUNTERS").as_deref() != Ok("1") {
            s.unavailable = Some("off (FUSEE_BENCH_COUNTERS=1 turns them on)".into());
            return;
        }
        let gone: Vec<u32> = s
            .fds
            .keys()
            .filter(|t| !threads.contains_key(t))
            .copied()
            .collect();
        for t in gone {
            if let Some((a, b)) = s.fds.remove(&t) {
                close(a);
                close(b);
            }
        }
        for (&tid, t) in threads.iter_mut() {
            if !s.fds.contains_key(&tid) {
                let user_only = s.user_only;
                let first = match open(tid, 1, user_only) {
                    Err(e) if !user_only && (e == libc::EACCES || e == libc::EPERM) => {
                        s.user_only = true;
                        open(tid, 1, true)
                    }
                    r => r,
                };
                let instructions = match first {
                    Ok(fd) => fd,
                    // The thread exited meanwhile.
                    Err(libc::ESRCH) => continue,
                    Err(e) => {
                        s.unavailable = Some(format!(
                            "perf_event_open(instructions) failed with errno {e}"
                        ));
                        return;
                    }
                };
                match open(tid, 0, s.user_only) {
                    Ok(cycles) => {
                        s.fds.insert(tid, (instructions, cycles));
                    }
                    Err(e) => {
                        close(instructions);
                        if e != libc::ESRCH {
                            s.unavailable =
                                Some(format!("perf_event_open(cycles) failed with errno {e}"));
                            return;
                        }
                        continue;
                    }
                }
            }
            if let Some(&(i, c)) = s.fds.get(&tid) {
                t.counts = Some((value(i), value(c)));
            }
        }
    }

    /// Why there are no counters (`None` when there are), and whether they
    /// leave out the kernel.
    pub fn status() -> (Option<String>, bool) {
        let s = STATE.lock().unwrap_or_else(|e| e.into_inner());
        (s.unavailable.clone(), s.user_only)
    }
}

#[cfg(not(target_os = "linux"))]
mod perf {
    pub fn status() -> (Option<String>, bool) {
        (
            Some("hardware counters are read on Linux only".into()),
            false,
        )
    }
}

#[cfg(not(target_os = "linux"))]
fn tasks() -> Option<BTreeMap<u32, Thread>> {
    None
}

#[cfg(target_os = "linux")]
fn tick_us() -> f64 {
    // SAFETY: sysconf has no preconditions.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    1e6 / if hz > 0 { hz as f64 } else { 100.0 }
}

#[cfg(not(target_os = "linux"))]
fn tick_us() -> f64 {
    0.0
}

/// The group a thread name belongs to. Linux truncates names to 15 bytes.
pub fn group(comm: &str) -> &'static str {
    let prefixed = |p: &str| comm.starts_with(p);
    match comm {
        _ if prefixed("fusee-bench") => "harness",
        _ if prefixed("fusee-shard") => "shard",
        "fusee-flush" => "flush",
        "fusee-segment" => "segment",
        _ if prefixed("fusee-checkpoin") => "checkpoint",
        "fusee-remote" => "remote",
        "fusee-ping" => "ping",
        "fusee-client" => "client",
        _ if prefixed("fusee-replica") => "replica",
        _ if prefixed("fusee-visibilit") => "visibility",
        "fusee-limits" => "limits",
        // `tokio-rt-worker` (current tokio) or `tokio-runtime-worker`.
        _ if prefixed("tokio-") => "tokio",
        _ if prefixed("fusee-") => "engine-other",
        _ => "other",
    }
}

/// Whether a group's CPU is the engine's. Tokio workers are: replication and
/// client futures run there, but so do the harness's per-workflow tasks, so
/// `engine_dedicated_*` leaves them out.
fn engine(group: &str) -> bool {
    !matches!(group, "harness" | "other")
}

#[derive(Default)]
struct Group {
    names: std::collections::BTreeSet<String>,
    threads: u32,
    user_us: f64,
    system_us: f64,
    cpu_us: f64,
    voluntary: f64,
    involuntary: f64,
    /// Threads with counters at both ends, and their counts.
    counted: u32,
    instructions: f64,
    cycles: f64,
}

fn round(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

/// CPU between two snapshots, per thread group, per step where `steps > 0`.
pub fn report(start: &Snapshot, end: &Snapshot, steps: f64) -> Value {
    let user = end.user_us.saturating_sub(start.user_us) as f64;
    let system = end.system_us.saturating_sub(start.system_us) as f64;
    let per = |us: f64| (steps > 0.0).then(|| round(us / steps));
    let mut out = json!({
        "window_s": round(end.at.duration_since(start.at).as_secs_f64()),
        "steps": steps,
        "process_user_us": user,
        "process_system_us": system,
        "process_us_per_step": per(user + system),
    });
    let (Some(before), Some(after)) = (&start.threads, &end.threads) else {
        out["threads"] = Value::Null;
        out["engine_us_per_step"] = Value::Null;
        out["threads_unavailable"] = json!(format!(
            "per-thread user and system CPU is read from /proc/self/task, which {} does not have",
            std::env::consts::OS
        ));
        return out;
    };
    let tick = tick_us();
    let mut groups: BTreeMap<&str, Group> = BTreeMap::new();
    for (tid, t) in after {
        // A thread created inside the window spent all its CPU inside it.
        let s = before.get(tid).filter(|s| s.comm == t.comm);
        let du = t.user_ticks - s.map_or(0, |s| s.user_ticks.min(t.user_ticks));
        let ds = t.system_ticks - s.map_or(0, |s| s.system_ticks.min(t.system_ticks));
        let ticks_us = (du + ds) as f64 * tick;
        let run_us = match (t.run_ns, s.map(|s| s.run_ns)) {
            (Some(e), Some(Some(b))) => Some(e.saturating_sub(b) as f64 / 1000.0),
            (Some(e), None) => Some(e as f64 / 1000.0),
            _ => None,
        };
        let g = groups.entry(group(&t.comm)).or_default();
        g.names.insert(t.comm.clone());
        g.threads += 1;
        g.user_us += du as f64 * tick;
        g.system_us += ds as f64 * tick;
        g.cpu_us += run_us.unwrap_or(ticks_us);
        if let Some((v, n)) = t.switches {
            let (v0, n0) = s.and_then(|s| s.switches).unwrap_or((0, 0));
            g.voluntary += v.saturating_sub(v0) as f64;
            g.involuntary += n.saturating_sub(n0) as f64;
        }
        if let (Some((i, c)), Some((i0, c0))) = (t.counts, s.and_then(|s| s.counts)) {
            g.counted += 1;
            g.instructions += i.saturating_sub(i0) as f64;
            g.cycles += c.saturating_sub(c0) as f64;
        }
    }
    let sum = |f: &dyn Fn(&str) -> bool, field: fn(&Group) -> f64| {
        groups
            .iter()
            .filter(|(name, _)| f(name))
            .map(|(_, g)| field(g))
            .sum::<f64>()
    };
    let (counters_unavailable, user_only) = perf::status();
    let counted = groups.values().any(|g| g.counted > 0);
    let live = sum(&|_| true, |g| g.cpu_us);
    let engine_cpu = sum(&|n| engine(n), |g| g.cpu_us);
    let dedicated = sum(&|n| engine(n) && n != "tokio", |g| g.cpu_us);
    let harness = sum(&|n| n == "harness", |g| g.cpu_us);
    out["threads"] = Value::Object(
        groups
            .iter()
            .map(|(name, g)| {
                (
                    (*name).to_owned(),
                    {
                        let mut v = json!({"threads":g.threads,"names":g.names,"user_us":round(g.user_us),"system_us":round(g.system_us),
                            "cpu_us":round(g.cpu_us),"us_per_step":per(g.cpu_us),"engine":engine(name),
                            "voluntary_switches":g.voluntary,"involuntary_switches":g.involuntary,
                            "switches_per_step":per(g.voluntary + g.involuntary)});
                        if counted {
                            v["counted_threads"] = json!(g.counted);
                            v["instructions"] = json!(g.instructions);
                            v["cycles"] = json!(g.cycles);
                            v["instructions_per_step"] = json!(per(g.instructions));
                            v["cycles_per_step"] = json!(per(g.cycles));
                        }
                        v
                    },
                )
            })
            .collect(),
    );
    out["tick_us"] = json!(tick);
    out["engine_us_per_step"] = json!(per(engine_cpu));
    out["engine_user_us_per_step"] = json!(per(sum(&|n| engine(n), |g| g.user_us)));
    out["engine_system_us_per_step"] = json!(per(sum(&|n| engine(n), |g| g.system_us)));
    out["engine_dedicated_us_per_step"] = json!(per(dedicated));
    out["harness_us_per_step"] = json!(per(harness));
    out["exited_threads_us"] = json!(round(user + system - live));
    out["engine_switches_per_step"] =
        json!(per(sum(&|n| engine(n), |g| g.voluntary + g.involuntary)));
    if counted {
        out["engine_instructions_per_step"] = json!(per(sum(&|n| engine(n), |g| g.instructions)));
        out["engine_cycles_per_step"] = json!(per(sum(&|n| engine(n), |g| g.cycles)));
        out["counters"] = json!(if user_only {
            "user space only"
        } else {
            "user and kernel"
        });
    } else {
        out["counters"] =
            json!(counters_unavailable.unwrap_or_else(|| "no thread was counted".into()));
    }
    out
}
