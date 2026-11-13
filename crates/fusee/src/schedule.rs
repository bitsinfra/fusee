//! Schedules: runs started on a cron expression or a fixed interval.
//!
//! A schedule is itself a Fusee workflow (type [`SCHEDULE_TYPE`], ID
//! `fusee.schedule/<name>`) built on durable timers, so there is no separate
//! scheduler and schedules survive crashes like any workflow. Each run it
//! starts has the workflow ID `<name>@<scheduled time>` (UTC, RFC 3339) and
//! attaches to a run that already has that ID, so one scheduled time never
//! fires twice, not even after a crash or a backfill over the same range.
//!
//! - Overlap rules: `skip` a time while the previous run is still running,
//!   `buffer-one` (run it when the previous one finishes; at most one
//!   waits), `allow-all`, or `cancel-other` (cancel the running one, then
//!   start).
//! - Jitter: each start is delayed by a recorded random amount below
//!   `jitter_ms`.
//! - Catch-up: after downtime, missed times within `catchup_window_ms`
//!   still start (subject to the overlap rule); older ones are skipped.
//! - Pause and resume; `backfill(from, to)` starts every scheduled time in a
//!   past range; `trigger` starts one now.
//!
//! The schedule workflow continues as new every 50 actions, so its journal
//! stays small however long it runs.

use crate::ctx::{ChildOptions, Ctx};
use crate::error::{Error, Result};
use crate::runtime::{ParentClose, Registry, RunOutcome, WorkflowFn};
use chrono::{Datelike, NaiveDate, NaiveDateTime, TimeZone, Timelike};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;

/// The built-in workflow type that runs schedules.
pub const SCHEDULE_TYPE: &str = "fusee.schedule";
/// The signal schedules take commands on.
pub const CONTROL: &str = "fusee.schedule.control";

/// The workflow ID of a schedule.
pub fn schedule_id(name: &str) -> String {
    format!("fusee.schedule/{name}")
}

/// What to do with a scheduled time while the previous run still runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Overlap {
    #[default]
    Skip,
    BufferOne,
    AllowAll,
    CancelOther,
}

/// When a schedule fires and what it starts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ScheduleSpec {
    /// Five fields (`minute hour day-of-month month day-of-week`) or an
    /// alias (`@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`).
    pub cron: Option<String>,
    /// A fixed interval instead of `cron`.
    pub every_ms: Option<u64>,
    /// Shifts interval times: they are `offset + k * every` since the epoch.
    pub offset_ms: u64,
    /// IANA time zone for `cron` (default `UTC`).
    pub timezone: String,
    pub overlap: Overlap,
    pub jitter_ms: u64,
    pub catchup_window_ms: u64,
    pub paused: bool,
    /// The workflow type each scheduled time starts, and its input.
    pub workflow: String,
    pub input: Value,
}

impl Default for ScheduleSpec {
    fn default() -> Self {
        Self {
            cron: None,
            every_ms: None,
            offset_ms: 0,
            timezone: "UTC".into(),
            overlap: Overlap::Skip,
            jitter_ms: 0,
            catchup_window_ms: 365 * 24 * 3_600_000,
            paused: false,
            workflow: String::new(),
            input: Value::Null,
        }
    }
}

impl ScheduleSpec {
    /// [`validate`](Self::validate), with the schedule's name in the error.
    pub fn validate_named(&self, name: &str) -> Result<()> {
        self.validate().map_err(|e| match e {
            Error::Rejected(m) => Error::Rejected(format!("schedule '{name}': {m}")),
            e => e,
        })
    }

    /// Checks the spec: a valid cron expression or interval and time zone.
    pub fn validate(&self) -> Result<()> {
        if self.workflow.is_empty() {
            return Err(Error::Rejected("a schedule needs a workflow type".into()));
        }
        match (&self.cron, self.every_ms) {
            (Some(c), None) => {
                Cron::parse(c)?;
                self.tz()?;
            }
            (None, Some(e)) if e > 0 => {}
            _ => {
                return Err(Error::Rejected(
                    "a schedule needs either a cron expression or an interval".into(),
                ));
            }
        }
        Ok(())
    }

    fn tz(&self) -> Result<chrono_tz::Tz> {
        self.timezone
            .parse::<chrono_tz::Tz>()
            .map_err(|_| Error::Rejected(format!("unknown time zone '{}'", self.timezone)))
    }

    /// The first scheduled time strictly after `after_ms` (Unix ms).
    pub fn next_after(&self, after_ms: u64) -> Option<u64> {
        if let Some(every) = self.every_ms.filter(|e| *e > 0) {
            let base = self.offset_ms % every;
            if after_ms < base {
                return Some(base);
            }
            return after_ms.checked_add(every - (after_ms - base) % every);
        }
        let cron = Cron::parse(self.cron.as_deref()?).ok()?;
        cron.next_after(after_ms, self.tz().ok()?)
    }
}

/// A parsed five-field cron expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cron {
    minutes: u64,
    hours: u32,
    doms: u32,
    months: u16,
    dows: u8,
    dom_any: bool,
    dow_any: bool,
}

/// Parses one cron field into a bit set. Day of week accepts 7 as Sunday.
fn field(spec: &str, lo: u32, hi: u32, names: &[&str]) -> Result<(u64, bool)> {
    let bad = || Error::Rejected(format!("bad cron field '{spec}'"));
    let dow = hi == 6;
    let max = if dow { 7 } else { hi };
    let value = |s: &str| -> Result<u32> {
        if let Some(i) = names.iter().position(|n| n.eq_ignore_ascii_case(s)) {
            return Ok(lo + i as u32);
        }
        s.parse::<u32>().map_err(|_| bad())
    };
    let mut bits = 0u64;
    for part in spec.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => (r, s.parse::<u32>().map_err(|_| bad())?),
            None => (part, 1),
        };
        if step == 0 {
            return Err(bad());
        }
        let (a, b) = if range == "*" {
            (lo, hi)
        } else if let Some((a, b)) = range.split_once('-') {
            (value(a)?, value(b)?)
        } else {
            let v = value(range)?;
            (v, if part.contains('/') { hi } else { v })
        };
        if a < lo || b > max || a > b {
            return Err(bad());
        }
        for v in (a..=b).step_by(step as usize) {
            let v = if dow && v == 7 { 0 } else { v };
            bits |= 1 << v;
        }
    }
    Ok((bits, spec == "*" || spec == "?"))
}

impl Cron {
    pub fn parse(expr: &str) -> Result<Self> {
        let e = match expr.trim() {
            "@yearly" | "@annually" => "0 0 1 1 *",
            "@monthly" => "0 0 1 * *",
            "@weekly" => "0 0 * * 0",
            "@daily" | "@midnight" => "0 0 * * *",
            "@hourly" => "0 * * * *",
            other => other,
        };
        let parts: Vec<&str> = e.split_whitespace().collect();
        let [mi, h, dom, mo, dow] = parts[..] else {
            return Err(Error::Rejected(format!(
                "a cron expression has five fields, '{expr}' has {}",
                parts.len()
            )));
        };
        const MONTHS: [&str; 12] = [
            "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
        ];
        const DAYS: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];
        let (minutes, _) = field(mi, 0, 59, &[])?;
        let (hours, _) = field(h, 0, 23, &[])?;
        let (doms, dom_any) = field(dom, 1, 31, &[])?;
        let (months, _) = field(mo, 1, 12, &MONTHS)?;
        let (dows, dow_any) = field(&dow.replace('?', "*"), 0, 6, &DAYS)?;
        Ok(Self {
            minutes,
            hours: hours as u32,
            doms: doms as u32,
            months: months as u16,
            dows: dows as u8,
            dom_any,
            dow_any,
        })
    }

    fn day_matches(&self, d: NaiveDate) -> bool {
        if self.months & (1 << d.month()) == 0 {
            return false;
        }
        let dom = self.doms & (1 << d.day()) != 0;
        let dow = self.dows & (1 << d.weekday().num_days_from_sunday()) != 0;
        // Standard cron: if both are restricted, either may match.
        match (self.dom_any, self.dow_any) {
            (true, true) => true,
            (true, false) => dow,
            (false, true) => dom,
            (false, false) => dom || dow,
        }
    }

    /// The first matching minute strictly after `after_ms`, in `tz`.
    pub fn next_after(&self, after_ms: u64, tz: chrono_tz::Tz) -> Option<u64> {
        let after = chrono::DateTime::from_timestamp_millis(after_ms as i64)?;
        let local = after.with_timezone(&tz).naive_local();
        let first_day = local.date();
        for day in 0..(366 * 5) {
            let d = first_day.checked_add_days(chrono::Days::new(day))?;
            if !self.day_matches(d) {
                continue;
            }
            for h in 0..24u32 {
                if self.hours & (1 << h) == 0 {
                    continue;
                }
                if day == 0 && h < local.hour() {
                    continue;
                }
                for m in 0..60u32 {
                    if self.minutes & (1 << m) == 0 {
                        continue;
                    }
                    let naive = NaiveDateTime::new(d, chrono::NaiveTime::from_hms_opt(h, m, 0)?);
                    let at = match tz.from_local_datetime(&naive) {
                        chrono::LocalResult::Single(t) => t,
                        chrono::LocalResult::Ambiguous(a, _) => a,
                        // Skipped by a daylight saving change.
                        chrono::LocalResult::None => continue,
                    };
                    let ms = at.timestamp_millis();
                    if ms > after_ms as i64 {
                        return u64::try_from(ms).ok();
                    }
                }
            }
        }
        None
    }
}

/// `2026-11-09T02:00:00Z`, with milliseconds when the time has them
/// (`2026-11-09T02:00:00.250Z`).
pub fn format_time(ms: u64) -> String {
    let fmt = if ms.is_multiple_of(1000) {
        "%Y-%m-%dT%H:%M:%SZ"
    } else {
        "%Y-%m-%dT%H:%M:%S%.3fZ"
    };
    match chrono::DateTime::from_timestamp_millis(ms as i64) {
        Some(t) => t.format(fmt).to_string(),
        None => ms.to_string(),
    }
}

/// The workflow ID of the run a schedule starts for scheduled time `ms`.
pub fn run_id(name: &str, ms: u64) -> String {
    format!("{name}@{}", format_time(ms))
}

/// A command sent to a schedule on [`CONTROL`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case")]
pub enum Command {
    Pause,
    Resume,
    Update {
        spec: ScheduleSpec,
    },
    Backfill {
        #[serde(rename = "fromMs")]
        from_ms: u64,
        #[serde(rename = "toMs")]
        to_ms: u64,
    },
    Trigger,
}

/// A schedule's state; also its input after continuing as new.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ScheduleState {
    pub name: String,
    pub spec: ScheduleSpec,
    /// The last scheduled time considered (fired or skipped).
    pub last_ms: u64,
    /// The run of the previous time, while it runs: `(ID, time)`.
    pub running: Option<(String, u64)>,
    pub buffered: Option<u64>,
    pub started: u64,
    pub skipped: u64,
    pub last_status: Option<String>,
    /// Issued instants survive child retention and continue-as-new.
    pub fired: Vec<FiredSpan>,
    /// The next backfill instant and inclusive end, carried between runs.
    pub backfill: Option<(u64, u64)>,
}

/// An exact arithmetic progression. Regular schedules retain one span even
/// after millions of firings. Irregular firings can add further spans.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FiredSpan {
    first: u64,
    last: u64,
    spacing: u64,
}

impl ScheduleState {
    fn remember(&mut self, t: u64) -> bool {
        if self.fired.iter().any(|s| {
            t >= s.first
                && t <= s.last
                && (s.spacing == 0 || (t - s.first).is_multiple_of(s.spacing))
        }) {
            return false;
        }
        if let Some(s) = self.fired.iter_mut().rev().find(|s| {
            s.spacing == 0
                || s.last.checked_add(s.spacing) == Some(t)
                || t.checked_add(s.spacing) == Some(s.first)
        }) {
            if s.spacing == 0 {
                s.spacing = t.abs_diff(s.first);
            }
            s.first = s.first.min(t);
            s.last = s.last.max(t);
        } else {
            self.fired.push(FiredSpan {
                first: t,
                last: t,
                spacing: 0,
            });
        }
        true
    }
}

enum Ev {
    Timer(Result<()>),
    Child(Result<Value>),
    Control(Result<Option<Command>>),
}

type Fut<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

fn child_future(ctx: &Ctx, st: &ScheduleState, id: String) -> Fut<'static, Result<Value>> {
    let ctx = ctx.clone();
    let (wtype, input) = (st.spec.workflow.clone(), st.spec.input.clone());
    Box::pin(async move {
        ctx.run_child::<Value, Value>(
            &wtype,
            &id,
            &input,
            ChildOptions {
                close: ParentClose::Abandon,
                timeout: None,
            },
        )
        .await
    })
}

async fn run_schedule(ctx: Ctx, input: ScheduleState) -> Result<Value> {
    let state = Rc::new(RefCell::new(input));
    {
        let s = state.clone();
        ctx.on_query("describe", move |_: Value| {
            let st = s.borrow();
            let next = (!st.spec.paused)
                .then(|| st.spec.next_after(st.last_ms))
                .flatten();
            serde_json::json!({
                "name": st.name,
                "spec": st.spec,
                "paused": st.spec.paused,
                "nextMs": next,
                "running": st.running.as_ref().map(|r| &r.0),
                "buffered": st.buffered,
                "started": st.started,
                "skipped": st.skipped,
                "lastStatus": st.last_status,
            })
        });
    }
    if state.borrow().last_ms == 0 {
        let now = ctx.now_ms()?;
        state.borrow_mut().last_ms = now;
    }
    let mut control: Option<Fut<'_, Result<Option<Command>>>> = None;
    let mut timer: Option<(u64, Fut<'_, Result<()>>)> = None;
    let mut child: Option<Fut<'static, Result<Value>>> = None;
    let resumed = state.borrow().running.clone();
    if let Some((id, _)) = resumed {
        // Continued as new while a run was going: attach to it again.
        child = Some(child_future(&ctx, &state.borrow(), id));
    }
    let mut actions = 0u32;
    loop {
        if actions >= 50 {
            let st = state.borrow().clone();
            return ctx.continue_as_new(&st).await;
        }
        let pending = state.borrow().backfill;
        if let Some((t, end)) = pending {
            spawn_detached(&ctx, &state, t)?;
            let mut st = state.borrow_mut();
            st.backfill = st
                .spec
                .next_after(t)
                .filter(|&next| next <= end)
                .map(|next| (next, end));
            actions += 1;
            continue;
        }
        if control.is_none() {
            control = Some(Box::pin(ctx.wait_for::<Command>(CONTROL)));
        }
        if timer.is_none() {
            let (paused, next, jitter) = {
                let st = state.borrow();
                (
                    st.spec.paused,
                    st.spec.next_after(st.last_ms),
                    st.spec.jitter_ms,
                )
            };
            if let (false, Some(t)) = (paused, next) {
                let delay = if jitter > 0 {
                    ctx.random()? % jitter
                } else {
                    0
                };
                timer = Some((t, Box::pin(ctx.sleep_until("next", t + delay))));
            }
        }
        let ev = std::future::poll_fn(|cx| {
            if let Some(c) = child.as_mut()
                && let Poll::Ready(r) = c.as_mut().poll(cx)
            {
                return Poll::Ready(Ev::Child(r));
            }
            if let Some(c) = control.as_mut()
                && let Poll::Ready(r) = c.as_mut().poll(cx)
            {
                return Poll::Ready(Ev::Control(r));
            }
            if let Some((_, t)) = timer.as_mut()
                && let Poll::Ready(r) = t.as_mut().poll(cx)
            {
                return Poll::Ready(Ev::Timer(r));
            }
            Poll::Pending
        })
        .await;
        match ev {
            Ev::Timer(r) => {
                r?;
                let Some((t, _)) = timer.take() else {
                    continue;
                };
                let now = ctx.now_ms()?;
                let mut due = vec![t];
                {
                    let st = state.borrow();
                    let mut x = t;
                    while let Some(n) = st.spec.next_after(x) {
                        if n > now || due.len() >= 1000 {
                            break;
                        }
                        due.push(n);
                        x = n;
                    }
                }
                for d in due {
                    let window = state.borrow().spec.catchup_window_ms;
                    state.borrow_mut().last_ms = d;
                    if now.saturating_sub(d) > window {
                        state.borrow_mut().skipped += 1;
                        continue;
                    }
                    act(&ctx, &state, &mut child, d)?;
                    actions += 1;
                }
            }
            Ev::Child(r) => {
                child = None;
                let next = {
                    let mut st = state.borrow_mut();
                    st.running = None;
                    st.last_status = Some(match &r {
                        Ok(_) => "ok".into(),
                        Err(e) => e.to_string(),
                    });
                    st.buffered.take()
                };
                if let Some(b) = next {
                    start(&ctx, &state, &mut child, b)?;
                    actions += 1;
                }
            }
            Ev::Control(r) => {
                control = None;
                let Some(cmd) = r? else { continue };
                match cmd {
                    Command::Pause => {
                        state.borrow_mut().spec.paused = true;
                        timer = None;
                    }
                    Command::Resume => {
                        let now = ctx.now_ms()?;
                        let mut st = state.borrow_mut();
                        st.spec.paused = false;
                        // Times passed while paused are not caught up.
                        st.last_ms = st.last_ms.max(now);
                        timer = None;
                    }
                    Command::Update { spec } => {
                        state.borrow_mut().spec = spec;
                        timer = None;
                    }
                    Command::Trigger => {
                        let now = ctx.now_ms()?;
                        act(&ctx, &state, &mut child, now)?;
                        actions += 1;
                    }
                    Command::Backfill { from_ms, to_ms } => {
                        let mut next = state
                            .borrow()
                            .spec
                            .next_after(from_ms.saturating_sub(1))
                            .filter(|&t| t <= to_ms);
                        if ctx.patched("schedule-paged-backfill")? {
                            state.borrow_mut().backfill = next.map(|t| (t, to_ms));
                        } else {
                            // Match child operations already present in a pre-upgrade journal.
                            for _ in 0..1000 {
                                let Some(t) = next else { break };
                                spawn_detached(&ctx, &state, t)?;
                                actions += 1;
                                next = state.borrow().spec.next_after(t).filter(|&n| n <= to_ms);
                            }
                        }
                    }
                }
            }
        }
    }
}

fn start(
    ctx: &Ctx,
    state: &Rc<RefCell<ScheduleState>>,
    child: &mut Option<Fut<'static, Result<Value>>>,
    t: u64,
) -> Result<()> {
    if !state.borrow_mut().remember(t) && ctx.patched("schedule-deduplicate")? {
        return Ok(());
    }
    let id = run_id(&state.borrow().name, t);
    *child = Some(child_future(ctx, &state.borrow(), id.clone()));
    let mut st = state.borrow_mut();
    st.running = Some((id, t));
    st.started += 1;
    Ok(())
}

fn spawn_detached(ctx: &Ctx, state: &Rc<RefCell<ScheduleState>>, t: u64) -> Result<()> {
    let mut st = state.borrow_mut();
    if !st.remember(t) && ctx.patched("schedule-deduplicate")? {
        return Ok(());
    }
    ctx.run_start_child(
        &st.spec.workflow,
        &run_id(&st.name, t),
        &st.spec.input,
        ChildOptions {
            close: ParentClose::Abandon,
            timeout: None,
        },
    )?;
    st.started += 1;
    Ok(())
}

fn act(
    ctx: &Ctx,
    state: &Rc<RefCell<ScheduleState>>,
    child: &mut Option<Fut<'static, Result<Value>>>,
    t: u64,
) -> Result<()> {
    let overlap = state.borrow().spec.overlap;
    let busy = child.is_some();
    match overlap {
        Overlap::Skip if busy => state.borrow_mut().skipped += 1,
        Overlap::BufferOne if busy => {
            let mut st = state.borrow_mut();
            if st.buffered.is_none() {
                st.buffered = Some(t);
            } else {
                st.skipped += 1;
            }
        }
        Overlap::CancelOther if busy => {
            let running = state.borrow().running.clone();
            if let Some((id, _)) = running {
                ctx.cancel_workflow(&id, "replaced by a newer scheduled run")?;
            }
            state.borrow_mut().buffered = Some(t);
        }
        Overlap::AllowAll if busy => {
            spawn_detached(ctx, state, t)?;
        }
        _ => start(ctx, state, child, t)?,
    }
    Ok(())
}

/// The built-in workflow types: schedules, debounce and batch coordinators.
/// Journal sampling (`fusee export --sample`) leaves them out.
pub fn is_builtin(wtype: &str) -> bool {
    matches!(
        wtype,
        SCHEDULE_TYPE | crate::queue::DEBOUNCE_TYPE | crate::queue::BATCH_TYPE
    )
}

/// Registers the built-in workflows.
pub fn register_builtins(r: &mut Registry) {
    let f: WorkflowFn = Arc::new(|ctx: Ctx| {
        let st = ctx.input_bytes(crate::ctx::decode::<ScheduleState>);
        Box::pin(async move {
            let st: ScheduleState = st.map_err(crate::engine::outcome_of)?;
            let out = run_schedule(ctx, st)
                .await
                .map_err(crate::engine::outcome_of)?;
            crate::ctx::encode(&out).map_err(crate::engine::outcome_of)
        }) as Pin<Box<dyn Future<Output = RunOutcome>>>
    });
    r.register(SCHEDULE_TYPE, 1, f);
    // Debounce and batch coordinators, built the same way.
    crate::queue::register_builtins(r);
    // Delayed starts.
    crate::delay::register(r);
    crate::concurrency::register(r);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(s: &str) -> u64 {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .timestamp_millis() as u64
    }

    #[test]
    fn cron_in_a_time_zone() {
        let spec = ScheduleSpec {
            cron: Some("0 2 * * *".into()),
            timezone: "Europe/London".into(),
            workflow: "report".into(),
            ..Default::default()
        };
        spec.validate().unwrap();
        // Winter: 02:00 London is 02:00 UTC.
        assert_eq!(
            format_time(spec.next_after(ms("2026-11-08T12:00:00Z")).unwrap()),
            "2026-11-09T02:00:00Z"
        );
        // Summer: 02:00 London is 01:00 UTC.
        assert_eq!(
            format_time(spec.next_after(ms("2026-07-01T12:00:00Z")).unwrap()),
            "2026-07-02T01:00:00Z"
        );
    }

    #[test]
    fn cron_fields() {
        let c = Cron::parse("*/15 9-17 * * mon-fri").unwrap();
        let utc = chrono_tz::UTC;
        // Saturday 2026-11-07 → Monday 09:00.
        assert_eq!(
            format_time(c.next_after(ms("2026-11-07T10:00:00Z"), utc).unwrap()),
            "2026-11-09T09:00:00Z"
        );
        assert_eq!(
            format_time(c.next_after(ms("2026-11-09T09:00:00Z"), utc).unwrap()),
            "2026-11-09T09:15:00Z"
        );
        let first = Cron::parse("0 0 1,15 * *").unwrap();
        assert_eq!(
            format_time(first.next_after(ms("2026-11-02T00:00:00Z"), utc).unwrap()),
            "2026-11-15T00:00:00Z"
        );
        assert!(Cron::parse("61 * * * *").is_err());
        assert!(Cron::parse("* * *").is_err());
        assert_eq!(
            format_time(
                Cron::parse("@weekly")
                    .unwrap()
                    .next_after(ms("2026-11-09T00:00:00Z"), utc)
                    .unwrap()
            ),
            "2026-11-15T00:00:00Z"
        );
        let sunday = Cron::parse("0 0 * * 7").unwrap();
        assert_eq!(sunday, Cron::parse("0 0 * * 0").unwrap());
    }

    #[test]
    fn intervals() {
        let spec = ScheduleSpec {
            every_ms: Some(3_600_000),
            offset_ms: 900_000,
            workflow: "x".into(),
            ..Default::default()
        };
        assert_eq!(spec.next_after(0), Some(900_000));
        assert_eq!(spec.next_after(900_000), Some(4_500_000));
        assert_eq!(run_id("nightly", 0), "nightly@1970-01-01T00:00:00Z");
        assert_eq!(run_id("n", 1_250), "n@1970-01-01T00:00:01.250Z");
    }
}
