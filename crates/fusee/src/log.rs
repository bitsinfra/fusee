//! Logs: the workflow's ([`Ctx::log`](crate::Ctx::log)) and the engine's own
//! warnings, through one hook.
//!
//! By default a record is one line on stderr: text, or JSON with
//! `FUSEE_LOG_FORMAT=json`. `FUSEE_LOG=debug` shows debug records too.
//! [`set_hook`] sends records elsewhere, for example to `tracing`:
//!
//! ```ignore
//! fusee::log::set_hook(|r| {
//!     let (id, step) = (r.workflow_id.unwrap_or(""), r.step.unwrap_or(""));
//!     match r.level {
//!         fusee::log::Level::Error => tracing::error!(workflow_id = id, step, "{}", r.message),
//!         fusee::log::Level::Warn => tracing::warn!(workflow_id = id, step, "{}", r.message),
//!         fusee::log::Level::Info => tracing::info!(workflow_id = id, step, "{}", r.message),
//!         fusee::log::Level::Debug => tracing::debug!(workflow_id = id, step, "{}", r.message),
//!     }
//! });
//! ```
//!
//! A workflow's logs are silent while its run replays the journal, so a line
//! is written once however often the code runs; they are not journaled.

use serde::Serialize;
use std::fmt::Display;
use std::sync::{OnceLock, RwLock};

/// How much a record matters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// One log record.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Record<'a> {
    pub level: Level,
    /// `fusee` for the engine's own records, `workflow` for `ctx.log()`.
    pub source: &'static str,
    pub message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run: Option<u64>,
    /// The step whose function logged, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
}

impl<'a> Record<'a> {
    /// An engine record.
    pub fn engine(level: Level, message: &'a str) -> Self {
        Self {
            level,
            source: "fusee",
            message,
            workflow_id: None,
            workflow_type: None,
            run: None,
            step: None,
            attempt: None,
        }
    }
}

type Hook = Box<dyn Fn(&Record<'_>) + Send + Sync>;

static HOOK: RwLock<Option<Hook>> = RwLock::new(None);

/// Sends every record to `f` instead of stderr (process-wide).
pub fn set_hook(f: impl Fn(&Record<'_>) + Send + Sync + 'static) {
    if let Ok(mut h) = HOOK.write() {
        *h = Some(Box::new(f));
    }
}

/// Back to the default: stderr.
pub fn reset_hook() {
    if let Ok(mut h) = HOOK.write() {
        *h = None;
    }
}

/// Writes a record through the hook.
pub fn emit(r: &Record<'_>) {
    if let Ok(h) = HOOK.read()
        && let Some(f) = h.as_ref()
    {
        f(r);
        return;
    }
    default(r);
}

/// The engine's own warning.
pub(crate) fn warn(message: impl Display) {
    emit(&Record::engine(Level::Warn, &message.to_string()));
}

/// The engine's own notice (the dev profile's announcement).
pub(crate) fn info(message: impl Display) {
    emit(&Record::engine(Level::Info, &message.to_string()));
}

struct Format {
    json: bool,
    debug: bool,
}

fn format() -> &'static Format {
    static F: OnceLock<Format> = OnceLock::new();
    F.get_or_init(|| Format {
        json: std::env::var("FUSEE_LOG_FORMAT").is_ok_and(|v| v == "json"),
        debug: std::env::var("FUSEE_LOG").is_ok_and(|v| v == "debug"),
    })
}

fn default(r: &Record<'_>) {
    let f = format();
    if r.level == Level::Debug && !f.debug {
        return;
    }
    if f.json {
        #[derive(Serialize)]
        struct Line<'a> {
            ts: u64,
            #[serde(flatten)]
            r: &'a Record<'a>,
        }
        if let Ok(s) = serde_json::to_string(&Line {
            ts: crate::local::unix_ms(),
            r,
        }) {
            eprintln!("{s}");
        }
        return;
    }
    eprintln!("{}", text(r));
}

/// The text form: `fusee: <message>` for the engine; for a workflow, the
/// level, the run and step, then the message.
pub fn text(r: &Record<'_>) -> String {
    if r.source == "fusee" {
        return format!("fusee: {}", r.message);
    }
    let mut out = format!("{:<5} {}", r.level.as_str(), r.workflow_id.unwrap_or("-"));
    if let Some(s) = r.step {
        out.push_str(&format!(" step '{s}'"));
        if let Some(a) = r.attempt.filter(|a| *a > 1) {
            out.push_str(&format!(" attempt {a}"));
        }
    }
    out.push_str(": ");
    out.push_str(r.message);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_names_the_run_and_step() {
        let r = Record {
            level: Level::Info,
            source: "workflow",
            message: "charging",
            workflow_id: Some("order-1"),
            workflow_type: Some("checkout"),
            run: Some(3),
            step: Some("charge"),
            attempt: Some(2),
        };
        assert_eq!(text(&r), "info  order-1 step 'charge' attempt 2: charging");
        assert_eq!(text(&Record::engine(Level::Warn, "x")), "fusee: x");
        let j = serde_json::to_value(r).unwrap();
        assert_eq!(j["workflowId"], "order-1");
        assert_eq!(j["level"], "info");
    }
}
