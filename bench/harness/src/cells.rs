//! Extra cells: payloads of 1 KiB, 64 KiB and 1 MiB; child workflows;
//! 1 ms and 50 ms steps at 10,000 concurrent workflows; an open-loop ramp to
//! saturation; and the soak. (Wide fan-out is `fanout-1000` and
//! `fanout-10000`, the fan-out workload at other widths.) Every latency is
//! reported with p50, p99, p99.9 and maximum, each sub-cell with its stall
//! summary, and the measured window with CPU by thread.

use super::{Opts, Stamps, hist, open_engine, run_prefix, summary, w_open};
use crate::{cpu, stalls};
use fusee::{Ctx, Error};
use hdrhistogram::Histogram;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// The primary's status in replicated mode (set by `main` once elected):
/// the saturation sweep ends when it loses the role, instead of the trial.
pub static PRIMARY: OnceLock<fusee_server::replicated::ReplicaObserver> = OnceLock::new();

/// Resolves once this replica stops being the ready primary; never
/// without a replica.
async fn primary_lost() {
    let Some(observer) = PRIMARY.get() else {
        return std::future::pending().await;
    };
    let mut observer = observer.clone();
    while observer.snapshot().0.ready {
        if !observer.changed().await {
            return;
        }
    }
}

/// Options only these cells and the stall tracer read.
#[derive(Clone, Debug, Default)]
pub struct Extra {
    /// Stall threshold and trace file (applied in `main`, see `stalls`).
    pub stall_us: Option<u64>,
    pub stall_trace: Option<String>,
    /// Soak rates, starts per second.
    pub rates: Vec<f64>,
    /// Width of the per-interval latency series in open loop (off if unset).
    pub window_seconds: Option<f64>,
    /// Workflows in flight at once (`service-concurrent`).
    pub concurrency: Option<u32>,
    /// Rate multiplier between saturation points.
    pub growth: Option<f64>,
    /// Saturation points run past the first miss.
    pub past: Option<u32>,
}

impl Extra {
    pub fn accepts(flag: &str) -> bool {
        matches!(
            flag,
            "--stall-us"
                | "--stall-trace"
                | "--rates"
                | "--window-seconds"
                | "--concurrency"
                | "--growth"
                | "--past"
        )
    }

    pub fn set(&mut self, flag: &str, v: &str) -> Result<(), ()> {
        let positive = |s: &str| {
            s.parse::<f64>()
                .ok()
                .filter(|x| x.is_finite() && *x > 0.0)
                .ok_or(())
        };
        match flag {
            "--stall-us" => self.stall_us = Some(v.parse().map_err(|_| ())?),
            "--stall-trace" => self.stall_trace = Some(v.to_owned()),
            "--rates" => self.rates = v.split(',').map(positive).collect::<Result<_, _>>()?,
            "--window-seconds" => self.window_seconds = Some(positive(v)?),
            "--concurrency" => {
                self.concurrency = Some(v.parse().ok().filter(|n| *n > 0).ok_or(())?);
            }
            "--growth" => {
                self.growth = Some(positive(v).and_then(|g| (g > 1.0).then_some(g).ok_or(()))?)
            }
            "--past" => self.past = Some(v.parse().map_err(|_| ())?),
            _ => return Err(()),
        }
        Ok(())
    }
}

/// p50, p99, p99.9 and maximum without the histogram buckets.
pub fn brief(h: &Histogram<u64>) -> Value {
    json!({"count":h.len(),"p50_us":h.value_at_quantile(0.50),"p99_us":h.value_at_quantile(0.99),
        "p999_us":h.value_at_quantile(0.999),"max_us":h.max()})
}

fn compact(s: &Value) -> Value {
    json!({"count":s["count"],"p50_us":s["p50_us"],"p99_us":s["p99_us"],"p999_us":s["p999_us"],"max_us":s["max_us"]})
}

fn quantiles(s: &Value) -> String {
    format!(
        "p50/p99/p99.9/max {}/{}/{}/{} µs",
        s["p50_us"], s["p99_us"], s["p999_us"], s["max_us"]
    )
}

/// Workflow and step latency per interval of scheduled arrival time, for
/// runs long enough to roll segments and take checkpoints.
#[derive(Clone, Default)]
pub struct Windows(Option<Arc<WindowsInner>>);

struct WindowsInner {
    width_ns: u64,
    origin_unix_ms: u64,
    hists: Mutex<Vec<(Histogram<u64>, Histogram<u64>)>>,
}

impl Windows {
    pub fn new(width_s: Option<f64>, seconds: f64, origin_unix_ms: u64) -> Self {
        Self(width_s.map(|w| {
            let n = (seconds / w).ceil().max(1.0) as usize;
            Arc::new(WindowsInner {
                width_ns: (w * 1e9) as u64,
                origin_unix_ms,
                hists: Mutex::new((0..n).map(|_| (hist(), hist())).collect()),
            })
        }))
    }

    fn with(&self, offset_ns: u64, f: impl FnOnce(&mut (Histogram<u64>, Histogram<u64>))) {
        if let Some(w) = &self.0
            && let Ok(mut h) = w.hists.lock()
        {
            let last = h.len() - 1;
            f(&mut h[((offset_ns / w.width_ns.max(1)) as usize).min(last)]);
        }
    }

    pub fn workflow(&self, offset_ns: u64, us: u64) {
        self.with(offset_ns, |h| {
            let _ = h.0.record(us);
        });
    }

    pub fn steps(&self, offset_ns: u64, samples: &[u64]) {
        self.with(offset_ns, |h| {
            for us in samples {
                let _ = h.1.record(*us);
            }
        });
    }

    /// Null when off.
    pub fn report(&self, stall_records: &[Value]) -> Value {
        let Some(w) = &self.0 else {
            return Value::Null;
        };
        let Ok(h) = w.hists.lock() else {
            return Value::Null;
        };
        let width_ms = w.width_ns / 1_000_000;
        let stalls = stalls::per_interval(stall_records, w.origin_unix_ms, width_ms, h.len());
        let rows: Vec<Value> = h
            .iter()
            .zip(stalls)
            .enumerate()
            .map(|(i, ((wf, step), s))| {
                json!({"from_s": i as f64 * w.width_ns as f64 / 1e9, "workflow": brief(wf),
                    "step_to_step": brief(step), "stalls": if stalls::threshold_us() == 0 { Value::Null } else { s }})
            })
            .collect();
        json!({"seconds": w.width_ns as f64 / 1e9, "by_scheduled_arrival": rows})
    }
}

/// Whether one open-loop point carried its whole offered load.
fn complete(r: &Value) -> bool {
    r["offered"] == r["completed"]
}

/// One table row of an open-loop point: p50, p99, p99.9 and maximum.
fn open_row(r: &Value) -> Value {
    let stalls = &r["stalls"];
    json!({
        "offered_starts_per_s": r["offered_starts_per_s"],
        "achieved_steps_per_s": r["achieved_steps_per_s"],
        "offered": r["offered"],
        "completed": r["completed"],
        "records_per_flush": r["records_per_flush"],
        "step_to_step": compact(&r["step_to_step"]),
        "workflow_latency_from_schedule": compact(&r["workflow_latency_from_schedule"]),
        "start_to_first_step": compact(&r["start_to_first_step"]),
        "engine_cpu_us_per_step": r["cpu"]["engine_us_per_step"],
        "process_cpu_us_per_step": r["cpu"]["process_us_per_step"],
        "stalls": if stalls.is_null() { Value::Null } else {
            json!({"count":stalls["count"],"max_us":stalls["max_us"],
                "preparing_segment":stalls["by_context"]["preparing_segment"]["count"],
                "checkpointing":stalls["by_context"]["checkpointing"]["count"]})
        },
    })
}

fn print_open(label: &str, r: &Value) {
    eprintln!(
        "  {label} {} starts/s: completed {}/{}, {:.0} steps/s; step {}; workflow {}; stalls {}",
        r["offered_starts_per_s"],
        r["completed"],
        r["offered"],
        r["achieved_steps_per_s"].as_f64().unwrap_or(0.0),
        quantiles(&r["step_to_step"]),
        quantiles(&r["workflow_latency_from_schedule"]),
        r["stalls"]["count"]
    );
}

/// The soak: open loop at each rate for `--seconds` (ten minutes on the
/// benchmark hosts), with a latency series per `--window-seconds` (10 s
/// unless given). The default rates bracket the published sweep at 32 steps
/// per workflow: 1,600 starts/s (51,200 steps/s, one record per flush),
/// 12,800 (409,600 steps/s, the device's write count already the limit) and
/// 51,200 (1.64 M steps/s, the highest rate with step p99 under 1 ms).
pub async fn soak(o: &Opts) -> Result<Value, Error> {
    let rates = if o.extra.rates.is_empty() {
        vec![1600.0, 12_800.0, 51_200.0]
    } else {
        o.extra.rates.clone()
    };
    let mut o = o.clone();
    o.extra.window_seconds = o.extra.window_seconds.or(Some(10.0));
    let mut points = Vec::new();
    for &rate in &rates {
        let r = w_open(&o, rate).await?;
        print_open("soak", &r);
        points.push(r);
    }
    let table: Vec<Value> = points
        .iter()
        .map(|r| {
            let mut row = open_row(r);
            row["complete"] = json!(complete(r));
            // The soak target: p99.9 for a 32-step workflow at most 3 ms.
            row["workflow_p999_within_3ms"] = json!(
                r["workflow_latency_from_schedule"]["p999_us"]
                    .as_u64()
                    .is_some_and(|p| p <= 3000)
            );
            row
        })
        .collect();
    Ok(
        json!({"rates": rates, "seconds_per_rate": o.seconds, "steps_per_workflow": o.steps,
        "table": table, "points": points}),
    )
}

/// Open loop to saturation: raise the rate by `--growth` (1.5 unless
/// given) from `--rate` until step p99 passes 1 ms or throughput stops
/// rising (less than 2% over the best so far), then run `--past` more
/// points (2 unless given) so the result shows whether latency recovers as
/// batches grow. A point that does not complete ends the sweep at once. The
/// reported figure is the highest complete point with step p99 at or under
/// 1 ms, wherever it falls.
pub async fn saturate(o: &Opts) -> Result<Value, Error> {
    if o.steps < 2 {
        return Err(Error::Failed(
            "saturate requires at least two dependent steps".into(),
        ));
    }
    let growth = o.extra.growth.unwrap_or(1.5);
    let past = o.extra.past.unwrap_or(2);
    let mut rate = o.rate;
    let mut peak = 0.0f64;
    let mut best = Value::Null;
    let mut best_before_miss = Value::Null;
    let mut first_miss: Option<(&str, f64)> = None;
    let mut after_miss = 0;
    let mut points = Vec::new();
    let mut table = Vec::new();
    let mut stop = "point limit";
    for _ in 0..32 {
        // A point that costs the primary its role (an overloaded group
        // whose backups time it out) ends the sweep: the points before it
        // stand, and the result says where it stopped.
        let r = tokio::select! {
            r = w_open(o, rate) => r?,
            () = primary_lost() => {
                stop = "primary changed";
                eprintln!("  saturate {rate} starts/s: the primary lost its role; the sweep stops");
                table.push(json!({"offered_starts_per_s": rate, "primary_changed": true}));
                break;
            }
        };
        print_open("saturate", &r);
        let p99 = r["step_to_step"]["p99_us"].as_u64().unwrap_or(u64::MAX);
        let achieved = r["achieved_steps_per_s"].as_f64().unwrap_or(0.0);
        let rising = achieved > peak * 1.02;
        let done = complete(&r);
        let mut row = open_row(&r);
        row["complete"] = json!(done);
        row["throughput_rising"] = json!(rising);
        row["past_first_miss"] = json!(first_miss.is_some());
        table.push(row);
        let within = p99 <= 1000 && done;
        if within && best["achieved_steps_per_s"].as_f64().unwrap_or(-1.0) < achieved {
            best = r.clone();
        }
        if within && first_miss.is_none() {
            best_before_miss = r.clone();
        }
        points.push(r);
        peak = peak.max(achieved);
        if !done {
            stop = "incomplete";
            break;
        }
        if first_miss.is_none() {
            if p99 > 1000 {
                first_miss = Some(("step p99 over 1 ms", achieved));
            } else if !rising {
                first_miss = Some(("throughput stopped rising", achieved));
            }
        } else {
            after_miss += 1;
        }
        if let Some((why, _)) = first_miss
            && after_miss >= past
        {
            stop = why;
            break;
        }
        rate *= growth;
    }
    let row_or_null = |v: &Value| {
        if v.is_null() {
            Value::Null
        } else {
            open_row(v)
        }
    };
    Ok(json!({
        "growth": growth, "past_first_miss": past, "stopped": stop,
        "first_miss_achieved_steps_per_s": first_miss.map(|(_, a)| a),
        "peak_achieved_steps_per_s": peak,
        "highest_within_1ms": row_or_null(&best),
        "highest_within_1ms_before_first_miss": row_or_null(&best_before_miss),
        "table": table, "points": points}))
}

/// Closed-loop chains whose every step returns a payload of 1 KiB, 64 KiB
/// or 1 MiB (a JSON string of that many bytes), `--steps` steps each.
pub async fn payload(o: &Opts) -> Result<Value, Error> {
    let mut out = Vec::new();
    for &bytes in &[1usize << 10, 64 << 10, 1 << 20] {
        let window = stalls::Window::begin();
        let engine = open_engine(o, &format!("payload-{bytes}")).await?;
        let body: Arc<str> = Arc::from("x".repeat(bytes));
        let stamps: Stamps = Arc::default();
        let (b, s) = (body.clone(), stamps.clone());
        let wf = engine.register("payload", move |ctx: Ctx, n: u32| {
            let (body, stamps) = (b.clone(), s.clone());
            async move {
                let mut total = 0u64;
                for _ in 0..n {
                    let (body, stamps) = (body.clone(), stamps.clone());
                    let got: String = ctx
                        .step("blob", move || {
                            let (body, stamps) = (body.clone(), stamps.clone());
                            async move {
                                let start = Instant::now();
                                let out = body.to_string();
                                if let Ok(mut m) = stamps.lock() {
                                    m.push((start, Instant::now()));
                                }
                                Ok::<_, Error>(out)
                            }
                        })
                        .await?;
                    total += got.len() as u64;
                }
                Ok(total)
            }
        });
        let (mut step, mut whole) = (hist(), hist());
        let prefix = run_prefix();
        let runs = 10 * o.trials;
        let mut before = None;
        // The first workflow pays any cold shard costs and is not measured.
        for r in 0..=runs {
            if let Ok(mut m) = stamps.lock() {
                *m = Vec::with_capacity(o.steps as usize);
            }
            if r == 1 {
                before = Some(cpu::snapshot());
            }
            let t0 = Instant::now();
            let total = engine
                .start(&wf, &format!("{prefix}-b{r}"), &o.steps)
                .await?
                .result()
                .await?;
            let elapsed = t0.elapsed().as_micros().max(1) as u64;
            let marks = stamps.lock().map(|m| m.clone()).unwrap_or_default();
            if total != o.steps as u64 * bytes as u64 || marks.len() != o.steps as usize {
                return Err(Error::Failed("incorrect payload result or samples".into()));
            }
            if r > 0 {
                let _ = whole.record(elapsed);
                for w in marks.windows(2) {
                    let _ = step.record((w[1].0 - w[0].1).as_micros().max(1) as u64);
                }
            }
        }
        let after = cpu::snapshot();
        let m = engine.metrics().await?;
        engine.shutdown().await?;
        let steps = f64::from(runs) * f64::from(o.steps);
        let mean_s = whole.mean() / 1e6;
        out.push(json!({
            "payload_bytes": bytes,
            "steps": o.steps,
            "workflows": runs,
            "step_to_step": summary(&step),
            "workflow": summary(&whole),
            "payload_mb_per_s": if mean_s > 0.0 { (f64::from(o.steps) * bytes as f64 / 1e6 / mean_s * 10.0).round() / 10.0 } else { 0.0 },
            "log_bytes": m.log_bytes,
            "flushes": m.flushes,
            "records_per_flush": m.flushed_records as f64 / m.flushes.max(1) as f64,
            "cpu": before.map_or(Value::Null, |b| cpu::report(&b, &after, steps)),
            "stalls": window.summary(Some(&m)),
        }));
    }
    Ok(json!(out))
}

type Marks = Arc<Mutex<Vec<Option<(Instant, Instant)>>>>;

/// Child workflows. `sequential`: a parent awaits 10 children one after
/// another, each running one step (start latency from the parent's call to
/// the child's step, return latency from the child's step to the parent
/// resuming). `parallel`: a parent awaits 100 children at once. The
/// children's IDs hash to every shard.
pub async fn children(o: &Opts) -> Result<Value, Error> {
    let mut out = Vec::new();
    for (pattern, k) in [("sequential", 10u32), ("parallel", 100)] {
        let parallel = pattern == "parallel";
        let window = stalls::Window::begin();
        let engine = open_engine(o, &format!("children-{pattern}")).await?;
        let kids: Marks = Arc::default();
        let calls: Marks = Arc::default();
        let km = kids.clone();
        let _child = engine.register("kid", move |ctx: Ctx, i: u32| {
            let km = km.clone();
            async move {
                ctx.step("work", move || {
                    let km = km.clone();
                    async move {
                        let t = Instant::now();
                        if let Ok(mut m) = km.lock()
                            && let Some(slot) = m.get_mut(i as usize)
                        {
                            *slot = Some((t, Instant::now()));
                        }
                        Ok::<_, Error>(i)
                    }
                })
                .await
            }
        });
        let cm = calls.clone();
        let parent = engine.register("parent", move |ctx: Ctx, k: u32| {
            let cm = cm.clone();
            async move {
                let base = ctx.workflow_id().to_owned();
                if parallel {
                    let legs = (0..k).map(|i| {
                        let (c, id) = (ctx.clone(), format!("{base}-k{i}"));
                        async move { c.child::<u32, u32>("kid", &id, &i).await }
                    });
                    let got = ctx.all(legs).await;
                    return Ok(got.into_iter().collect::<Result<Vec<_>, _>>()?.iter().sum());
                }
                let mut sum = 0u32;
                for i in 0..k {
                    let call = Instant::now();
                    sum += ctx
                        .child::<u32, u32>("kid", &format!("{base}-k{i}"), &i)
                        .await?;
                    if let Ok(mut m) = cm.lock()
                        && let Some(slot) = m.get_mut(i as usize)
                    {
                        *slot = Some((call, Instant::now()));
                    }
                }
                Ok(sum)
            }
        });
        let (mut whole, mut start, mut back) = (hist(), hist(), hist());
        let (mut first, mut last) = (hist(), hist());
        let prefix = run_prefix();
        let runs = 10 * o.trials;
        let mut before = None;
        for r in 0..=runs {
            for m in [&kids, &calls] {
                if let Ok(mut m) = m.lock() {
                    *m = vec![None; k as usize];
                }
            }
            if r == 1 {
                before = Some(cpu::snapshot());
            }
            let t0 = Instant::now();
            let sum = engine
                .start(&parent, &format!("{prefix}-p{r}"), &k)
                .await?
                .result()
                .await?;
            let elapsed = t0.elapsed().as_micros().max(1) as u64;
            let kid = kids.lock().map(|m| m.clone()).unwrap_or_default();
            let call = calls.lock().map(|m| m.clone()).unwrap_or_default();
            if sum != k * (k - 1) / 2 || kid.iter().any(Option::is_none) {
                return Err(Error::Failed("incorrect child results or samples".into()));
            }
            if r == 0 {
                continue;
            }
            let _ = whole.record(elapsed);
            let us = |d: Duration| d.as_micros().max(1) as u64;
            let kid: Vec<(Instant, Instant)> = kid.into_iter().flatten().collect();
            if parallel {
                if let (Some(a), Some(z)) =
                    (kid.iter().map(|k| k.0).min(), kid.iter().map(|k| k.1).max())
                {
                    let _ = first.record(us(a - t0));
                    let _ = last.record(us(z - t0));
                }
            } else {
                for (c, s) in call.into_iter().zip(&kid) {
                    if let Some((called, resumed)) = c {
                        let _ = start.record(us(s.0.saturating_duration_since(called)));
                        let _ = back.record(us(resumed.saturating_duration_since(s.1)));
                    }
                }
            }
        }
        let after = cpu::snapshot();
        let m = engine.metrics().await?;
        engine.shutdown().await?;
        // Each child is a workflow with one step; the parent adds none.
        let steps = f64::from(runs) * f64::from(k);
        let mut row = json!({
            "pattern": pattern,
            "children": k,
            "workflows": runs,
            "parent_workflow": summary(&whole),
            "flushes": m.flushes,
            "records_per_flush": m.flushed_records as f64 / m.flushes.max(1) as f64,
            "cpu": before.map_or(Value::Null, |b| cpu::report(&b, &after, steps)),
            "stalls": window.summary(Some(&m)),
        });
        if parallel {
            row["first_child_step_from_start"] = summary(&first);
            row["last_child_step_end_from_start"] = summary(&last);
        } else {
            row["child_call_to_child_step"] = summary(&start);
            row["child_step_to_parent_resumed"] = summary(&back);
        }
        out.push(row);
    }
    Ok(json!(out))
}

/// 1 ms and 50 ms service steps with `--concurrency` workflows (10,000
/// unless given) in flight at once, `--steps` steps each, all started
/// together. Overhead is the gap from one step function's return to the
/// next one's start; the ideal rate is concurrency / step latency.
pub async fn service_concurrent(o: &Opts) -> Result<Value, Error> {
    let n = o.extra.concurrency.unwrap_or(10_000);
    let mut out = Vec::new();
    for &lat in &[1u64, 50] {
        let window = stalls::Window::begin();
        let engine = open_engine(o, &format!("svc-concurrent-{lat}")).await?;
        let gaps = Arc::new(Mutex::new(hist()));
        let first = Arc::new(Mutex::new(hist()));
        let origin: Arc<OnceLock<Instant>> = Arc::default();
        let (g2, f2, o2) = (gaps.clone(), first.clone(), origin.clone());
        let wf = engine.register("svc", move |ctx: Ctx, steps: u32| {
            let (gaps, first, origin) = (g2.clone(), f2.clone(), o2.clone());
            async move {
                let previous = std::cell::Cell::new(None::<Instant>);
                let samples = std::cell::RefCell::new(Vec::with_capacity(steps as usize));
                for _ in 0..steps {
                    ctx.step("call", || async {
                        let t0 = Instant::now();
                        match previous.get() {
                            Some(end) => samples
                                .borrow_mut()
                                .push((t0 - end).as_micros().max(1) as u64),
                            None => {
                                if let (Some(o), Ok(mut h)) = (origin.get(), first.lock()) {
                                    let _ = h.record(
                                        t0.saturating_duration_since(*o).as_micros().max(1) as u64,
                                    );
                                }
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(lat)).await;
                        previous.set(Some(Instant::now()));
                        Ok::<_, Error>(())
                    })
                    .await?;
                }
                if let Ok(mut h) = gaps.lock() {
                    for us in samples.into_inner() {
                        let _ = h.record(us);
                    }
                }
                Ok(steps)
            }
        });
        let wf = Arc::new(wf);
        let prefix = run_prefix();
        let steps = o.steps;
        let before = cpu::snapshot();
        let t0 = *origin.get_or_init(Instant::now);
        let tasks: Vec<_> = (0..n)
            .map(|k| {
                let (wf, id, engine) = (wf.clone(), format!("{prefix}-c{k}"), engine.clone());
                tokio::spawn(async move {
                    let done = engine.start(&wf, &id, &steps).await?.result().await?;
                    Ok::<_, Error>((done == steps).then(Instant::now))
                })
            })
            .collect();
        let mut whole = hist();
        let mut ok = 0u64;
        let mut finished = t0;
        for t in tasks {
            if let Some(at) = t.await.map_err(|e| Error::failed(e.to_string()))?? {
                let _ = whole.record(at.duration_since(t0).as_micros().max(1) as u64);
                finished = finished.max(at);
                ok += 1;
            }
        }
        let after = cpu::snapshot();
        let elapsed = finished.duration_since(t0).as_secs_f64().max(1e-9);
        let m = engine.metrics().await?;
        engine.shutdown().await?;
        let done_steps = ok as f64 * f64::from(steps);
        let gaps = gaps.lock().map(|h| summary(&h)).unwrap_or(Value::Null);
        let first = first.lock().map(|h| summary(&h)).unwrap_or(Value::Null);
        out.push(json!({
            "service_ms": lat,
            "concurrency": n,
            "steps_per_workflow": steps,
            "completed": ok,
            "ideal_steps_per_s": f64::from(n) * 1000.0 / lat as f64,
            "achieved_steps_per_s": done_steps / elapsed,
            "engine_overhead_per_step": gaps,
            "start_to_first_step": first,
            "workflow_from_burst_start": summary(&whole),
            "flushes": m.flushes,
            "records_per_flush": m.flushed_records as f64 / m.flushes.max(1) as f64,
            "cpu": cpu::report(&before, &after, done_steps),
            "stalls": window.summary(Some(&m)),
        }));
    }
    Ok(json!(out))
}
