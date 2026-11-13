//! The engine's stall tracer, for every cell.
//!
//! The engine reads `FUSEE_STALL_US` and `FUSEE_STALL_TRACE` once, when its
//! first shard opens, and appends one JSON line per tick or flush longer than
//! the threshold. `--stall-us N` and `--stall-trace FILE` set both before the
//! runtime starts (`--stall-us` alone traces to `<dir>/stalls-<pid>.jsonl`).
//! A [`Window`] remembers where the file ended when a cell began, so each
//! cell summarizes only its own stalls: count, by kind, by what else the
//! shard was doing, and the largest.

use fusee::{Error, Metrics};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::OnceLock;

#[derive(Debug)]
struct Tracing {
    threshold_us: u64,
    trace: Option<PathBuf>,
}

static TRACING: OnceLock<Tracing> = OnceLock::new();

fn flag(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .rev()
        .find(|w| w[0] == name)
        .map(|w| w[1].clone())
}

/// Applies the stall flags to the environment the engine reads. `main` calls
/// this before the runtime starts, while no other thread exists.
pub fn configure(args: &[String]) -> Result<(), Error> {
    let given = flag(args, "--stall-us")
        .map(|v| {
            v.parse::<u64>()
                .map_err(|_| Error::failed(format!("invalid --stall-us '{v}'")))
        })
        .transpose()?;
    let inherited = std::env::var("FUSEE_STALL_US")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());
    let threshold_us = given.or(inherited).unwrap_or(0);
    let named = flag(args, "--stall-trace").map(PathBuf::from);
    let trace = (threshold_us != 0).then(|| {
        named
            .clone()
            .or_else(|| std::env::var_os("FUSEE_STALL_TRACE").map(PathBuf::from))
            .unwrap_or_else(|| {
                flag(args, "--dir")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| std::env::temp_dir().join("fusee-bench"))
                    .join(format!("stalls-{}.jsonl", std::process::id()))
            })
    });
    if let Some(path) = &trace {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        // A trace this harness names belongs to this process alone. One the
        // caller exported is appended to, as the engine would.
        if std::env::var_os("FUSEE_STALL_TRACE").as_deref() != Some(path.as_os_str())
            || named.is_some()
        {
            std::fs::File::create(path)?;
        }
    }
    if given.is_some() || trace.is_some() {
        // SAFETY: called from `main` before the runtime or any other thread
        // starts, so nothing reads the environment concurrently.
        unsafe {
            std::env::set_var("FUSEE_STALL_US", threshold_us.to_string());
            match &trace {
                Some(path) => std::env::set_var("FUSEE_STALL_TRACE", path),
                None => std::env::remove_var("FUSEE_STALL_TRACE"),
            }
        }
    }
    let _ = TRACING.set(Tracing {
        threshold_us,
        trace,
    });
    Ok(())
}

fn tracing() -> Option<&'static Tracing> {
    TRACING.get().filter(|t| t.threshold_us != 0)
}

/// The threshold in microseconds (0: tracing off).
pub fn threshold_us() -> u64 {
    tracing().map_or(0, |t| t.threshold_us)
}

/// The stalls one cell produced.
#[derive(Debug)]
pub struct Window {
    offset: u64,
}

impl Window {
    pub fn begin() -> Self {
        let offset = tracing()
            .and_then(|t| t.trace.as_ref())
            .and_then(|p| std::fs::metadata(p).ok())
            .map_or(0, |m| m.len());
        Self { offset }
    }

    /// Complete trace lines appended since [`begin`](Self::begin), and how
    /// many of them did not parse.
    pub fn records(&self) -> (Vec<Value>, usize) {
        let Some(path) = tracing().and_then(|t| t.trace.as_ref()) else {
            return (Vec::new(), 0);
        };
        let mut bytes = Vec::new();
        let read = std::fs::File::open(path).and_then(|mut f| {
            f.seek(SeekFrom::Start(self.offset))?;
            f.read_to_end(&mut bytes)
        });
        if read.is_err() {
            return (Vec::new(), 0);
        }
        // A line still being written has no newline yet.
        let complete = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
        let mut unparsed = 0;
        let records = bytes[..complete]
            .split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
            .filter_map(|l| {
                let v = serde_json::from_slice::<Value>(l).ok();
                unparsed += usize::from(v.is_none());
                v
            })
            .collect();
        (records, unparsed)
    }

    /// The cell's summary, or null when tracing is off. `metrics` adds the
    /// engine's own counters (they cover the engine's lifetime).
    pub fn summary(&self, metrics: Option<&Metrics>) -> Value {
        let Some(t) = tracing() else {
            return Value::Null;
        };
        let (records, unparsed) = self.records();
        let mut out = summarize(&records);
        out["threshold_us"] = json!(t.threshold_us);
        out["trace"] = json!(t.trace.as_ref().map(|p| p.display().to_string()));
        out["unparsed_lines"] = json!(unparsed);
        if let Some(m) = metrics {
            out["engine_counted"] = json!(m.stalls);
            out["engine_max_tick_us"] = json!(m.max_tick_us);
            out["engine_max_flush_us"] = json!(m.max_flush_us);
        }
        out
    }
}

#[derive(Default)]
struct Tally {
    count: u64,
    max_us: u64,
    total_us: u64,
}

impl Tally {
    fn add(&mut self, us: u64) {
        self.count += 1;
        self.max_us = self.max_us.max(us);
        self.total_us += us;
    }

    fn json(&self, of: u64) -> Value {
        json!({"count":self.count,"max_us":self.max_us,"total_us":self.total_us,
            "share":if of == 0 { 0.0 } else { (self.count as f64 / of as f64 * 1e4).round() / 1e4 }})
    }
}

fn num(r: &Value, key: &str) -> u64 {
    r[key].as_u64().unwrap_or(0)
}

/// What else the shard was doing during a stall. `no_context` is a stall
/// with none of the others.
pub fn contexts(r: &Value) -> Vec<&'static str> {
    let mut c = Vec::new();
    if r["preparing_segment"].as_bool() == Some(true) {
        c.push("preparing_segment");
    }
    if r["checkpointing"].as_bool() == Some(true) {
        c.push("checkpointing");
    }
    if num(r, "parked") > 0 {
        c.push("parking");
    }
    if c.is_empty() {
        c.push("no_context");
    }
    c
}

const CONTEXTS: [&str; 4] = [
    "preparing_segment",
    "checkpointing",
    "parking",
    "no_context",
];

/// Count, by kind (tick, inline-flush, flush) and by context flag, maximum
/// and the five largest stalls verbatim.
pub fn summarize(records: &[Value]) -> Value {
    let mut all = Tally::default();
    let mut kinds: BTreeMap<String, (Tally, BTreeMap<&str, Tally>)> = BTreeMap::new();
    let mut by_context: BTreeMap<&str, Tally> = BTreeMap::new();
    let mut maxima: BTreeMap<&str, u64> = BTreeMap::new();
    for r in records {
        let us = num(r, "us");
        all.add(us);
        let kind = r["kind"].as_str().unwrap_or("unknown").to_owned();
        let entry = kinds.entry(kind).or_default();
        entry.0.add(us);
        for c in contexts(r) {
            entry.1.entry(c).or_default().add(us);
            by_context.entry(c).or_default().add(us);
        }
        for key in [
            "records",
            "bytes",
            "maintenance_us",
            "parked",
            "loaded",
            "slots",
        ] {
            let m = maxima.entry(key).or_default();
            *m = (*m).max(num(r, key));
        }
    }
    let context_json = |tallies: &BTreeMap<&str, Tally>, of: u64| {
        CONTEXTS
            .iter()
            .map(|c| {
                (
                    (*c).to_owned(),
                    tallies
                        .get(c)
                        .map_or_else(|| Tally::default().json(of), |t| t.json(of)),
                )
            })
            .collect::<serde_json::Map<_, _>>()
    };
    let by_kind = kinds
        .iter()
        .map(|(k, (t, c))| {
            let mut v = t.json(all.count);
            v["by_context"] = Value::Object(context_json(c, t.count));
            (k.clone(), v)
        })
        .collect::<serde_json::Map<_, _>>();
    let mut largest: Vec<&Value> = records.iter().collect();
    largest.sort_by_key(|r| std::cmp::Reverse(num(r, "us")));
    largest.truncate(5);
    json!({
        "count": all.count,
        "max_us": all.max_us,
        "total_us": all.total_us,
        "by_kind": by_kind,
        "by_context": context_json(&by_context, all.count),
        "max": maxima,
        "largest": largest,
    })
}

/// Stall counts per interval of `width_ms` from `origin_ms` (wall clock).
pub fn per_interval(records: &[Value], origin_ms: u64, width_ms: u64, n: usize) -> Vec<Value> {
    let mut slots: Vec<(Tally, BTreeMap<&str, u64>)> = (0..n).map(|_| Default::default()).collect();
    for r in records {
        let at = num(r, "at_ms");
        let i = ((at.saturating_sub(origin_ms)) / width_ms.max(1)) as usize;
        if let Some(slot) = slots.get_mut(i.min(n.saturating_sub(1))) {
            slot.0.add(num(r, "us"));
            for c in contexts(r) {
                *slot.1.entry(c).or_default() += 1;
            }
        }
    }
    slots
        .into_iter()
        .map(|(t, c)| json!({"count":t.count,"max_us":t.max_us,"by_context":c}))
        .collect()
}
