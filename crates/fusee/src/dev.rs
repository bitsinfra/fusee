//! The `dev` profile: for writing workflows, not for production.
//!
//! Chosen with [`Config::dev`](crate::Config::dev), [`Config::profile`](crate::Config::profile)
//! or `FUSEE_PROFILE=dev` (which `fusee dev` and the test kit set); never
//! inferred. The engine says so in one line when it opens, and:
//!
//! - keeps every run's history ([`Config::keep_history`](crate::Config::keep_history));
//! - on macOS flushes with `fsync` instead of `F_FULLFSYNC`: a process crash
//!   loses nothing, power loss may;
//! - **strict replay**: when a run waits or ends, its journal is replayed
//!   against the registered code on a thread of its own, and a difference is
//!   reported at once with the step name, so reading the clock, random
//!   numbers or unordered collections outside a step shows up while the code
//!   is written, not after a crash in production;
//! - `result()` of a stuck run fails with [`Error::Determinism`] instead of
//!   waiting for code that matches;
//! - serves the HTTP API and the UI ([`Config::http`](crate::Config::http));
//! - records when each step began and how long it took (a `STEP_TIMED`
//!   record), so the UI's step list is also a timeline.
//!
//! Only the step times touch the step path, and only here: a clock read
//! when a step begins and two varints on its record. A production step
//! reads no clock for them and writes the same bytes as without the
//! profile. The checks
//! poll the run list from their own thread.

use crate::engine::{Engine, WeakEngine};
use crate::error::{Error, Result};
use crate::export::{ExportedJournal, ReplayReport, Replayer};
use rustc_hash::{FxHashMap, FxHashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

/// Which defaults and checks an engine runs with.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Profile {
    #[default]
    Production,
    /// See the [module documentation](self).
    Dev,
}

impl Profile {
    /// `FUSEE_PROFILE`: `dev` or `production`; unset is production.
    pub fn from_env() -> Result<Self> {
        match std::env::var("FUSEE_PROFILE") {
            Ok(v) => Self::parse(&v),
            Err(_) => Ok(Self::Production),
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "dev" => Ok(Self::Dev),
            "production" | "" => Ok(Self::Production),
            other => Err(Error::Rejected(format!(
                "FUSEE_PROFILE is 'dev' or 'production', not '{other}'"
            ))),
        }
    }
}

/// How often the checks read the run list.
const POLL: Duration = Duration::from_millis(250);

/// The dev profile's thread and the served UI; stopped when the engine is
/// dropped.
pub(crate) struct Dev {
    stop: Arc<AtomicBool>,
    checks: Option<JoinHandle<()>>,
    http: Option<Served>,
}

impl Drop for Dev {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.http.take();
        join(self.checks.take());
    }
}

/// Joins unless the engine is being dropped on that very thread (its last
/// handle was the thread's own).
fn join(t: Option<JoinHandle<()>>) {
    if let Some(t) = t
        && t.thread().id() != std::thread::current().id()
    {
        let _ = t.join();
    }
}

/// The HTTP API on a thread with its own runtime, so an engine opened
/// outside Tokio can serve it too.
struct Served {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for Served {
    fn drop(&mut self) {
        if let Some(s) = self.stop.take() {
            let _ = s.send(());
        }
        join(self.thread.take());
    }
}

fn serve(engine: WeakEngine, addrs: Vec<String>, dev: bool) -> Result<(Served, SocketAddr)> {
    let (tx, rx) = std::sync::mpsc::channel::<std::io::Result<SocketAddr>>();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let thread = std::thread::Builder::new()
        .name("fusee-http".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            };
            rt.block_on(async move {
                let opts = crate::http::HttpOptions {
                    profile: dev.then(|| "dev".to_owned()),
                    ..Default::default()
                };
                let mut last = None;
                for a in &addrs {
                    match crate::http::serve(engine.clone(), a, opts.clone()).await {
                        Ok(server) => {
                            let _ = tx.send(Ok(server.addr()));
                            let _ = stopped.await;
                            drop(server);
                            return;
                        }
                        Err(e) => last = Some(e),
                    }
                }
                let _ = tx.send(Err(
                    last.unwrap_or_else(|| std::io::Error::other("no address"))
                ));
            });
        })?;
    let addr = rx
        .recv()
        .map_err(|_| Error::Io("the HTTP thread exited".into()))?
        .map_err(|e| Error::Io(e.to_string()))?;
    Ok((
        Served {
            stop: Some(stop),
            thread: Some(thread),
        },
        addr,
    ))
}

/// Called by [`Engine::open`]: serves the UI if asked, and under the dev
/// profile announces it and starts the checks.
pub(crate) fn start(engine: &Engine, dev: bool, plain_fsync: bool) -> Result<()> {
    let config = engine.config();
    let addrs = match (&config.http, dev) {
        (Some(a), _) if a == "off" => vec![],
        (Some(a), _) => vec![a.clone()],
        (None, false) => vec![],
        (None, true) => match std::env::var("FUSEE_HTTP") {
            Ok(a) if a == "off" => vec![],
            // Asked for (`fusee dev` sets it): a busy address is an error.
            Ok(a) if !a.is_empty() => vec![a],
            _ => vec!["127.0.0.1:7601".into(), "127.0.0.1:0".into()],
        },
    };
    let mut http = None;
    let mut url = None;
    if !addrs.is_empty() {
        let (served, addr) = serve(engine.downgrade(), addrs, dev)?;
        http = Some(served);
        url = Some(format!("http://{addr}"));
    }
    if !dev {
        if http.is_some() {
            engine.set_dev(
                Dev {
                    stop: Arc::new(AtomicBool::new(false)),
                    checks: None,
                    http,
                },
                url,
            );
        }
        return Ok(());
    }
    // A test engine says nothing: tests open many.
    #[cfg(feature = "testing")]
    let quiet = config.test_hooks.is_some();
    #[cfg(not(feature = "testing"))]
    let quiet = false;
    let where_ = match config.durability {
        crate::engine::Durability::Memory => "memory".to_owned(),
        crate::engine::Durability::Local => config.dir.display().to_string(),
        crate::engine::Durability::Custom(_) => "custom storage".to_owned(),
    };
    if !quiet {
        crate::log::info(format!(
            "dev profile ({where_}): every run's history is kept, each run is replayed against the \
         code when it waits or ends{}. Not for production.",
            if plain_fsync {
                ", flushes use fsync (a process crash loses nothing; power loss may)"
            } else {
                ""
            }
        ));
    }
    if let Some(u) = &url {
        crate::log::info(format!("UI and HTTP API at {u}"));
    }
    let stop = Arc::new(AtomicBool::new(false));
    let weak = engine.downgrade();
    let s = stop.clone();
    let checks = std::thread::Builder::new()
        .name("fusee-dev".into())
        .spawn(move || checks(weak, s))?;
    engine.set_dev(
        Dev {
            stop,
            checks: Some(checks),
            http,
        },
        url,
    );
    Ok(())
}

impl Engine {
    /// Replays every waiting or finished run's journal against the
    /// registered code now, and returns the runs whose code differs from
    /// their journal, each naming the step. Finished runs need their history
    /// ([`Config::keep_history`](crate::Config::keep_history), on under the
    /// dev profile). Under the dev profile this runs by itself as runs wait
    /// or end.
    pub async fn replay_check(&self) -> Result<Vec<ReplayReport>> {
        let replayer = Replayer::sharing(self.registry(), self.config().codec.clone());
        let mut out = Vec::new();
        for r in self.list(usize::MAX).await? {
            if r.wtype.starts_with("fusee.") || !settled(&r.status) {
                continue;
            }
            let Some(d) = self.describe(&r.id).await? else {
                continue;
            };
            let journal = ExportedJournal::from_detail(&d);
            if !journal.is_complete() {
                continue;
            }
            let report = replayer.replay(&journal);
            if report.error.is_some() {
                out.push(report);
            }
        }
        Ok(out)
    }
}

/// Statuses where a run's code is waiting or done, so its journal can be
/// replayed whole.
fn settled(status: &str) -> bool {
    matches!(
        status,
        "suspended" | "parked" | "ok" | "failed" | "cancelled" | "timed_out" | "terminated"
    )
}

fn checks(weak: WeakEngine, stop: Arc<AtomicBool>) {
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
    else {
        return;
    };
    // The status a run was last replayed at, and differences reported.
    let mut seen: FxHashMap<String, (String, Option<u64>)> = FxHashMap::default();
    let mut reported: FxHashSet<String> = FxHashSet::default();
    loop {
        let mut slept = Duration::ZERO;
        while slept < POLL {
            if stop.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
            slept += Duration::from_millis(25);
        }
        let Some(engine) = weak.upgrade() else { return };
        if engine.is_closed() {
            return;
        }
        let Ok(rows) = rt.block_on(engine.list(usize::MAX)) else {
            continue;
        };
        let replayer = Replayer::sharing(engine.registry(), engine.config().codec.clone());
        for r in rows {
            if r.wtype.starts_with("fusee.") {
                continue;
            }
            if r.status == "stuck" {
                engine.reject_stuck(&r.id);
                continue;
            }
            if !settled(&r.status) {
                continue;
            }
            let at = (r.status.clone(), r.finished_ms);
            if seen.get(&r.id) == Some(&at) {
                continue;
            }
            seen.insert(r.id.clone(), at);
            let Ok(Some(d)) = rt.block_on(engine.describe(&r.id)) else {
                continue;
            };
            let journal = ExportedJournal::from_detail(&d);
            if !journal.is_complete() {
                continue;
            }
            let report = replayer.replay(&journal);
            if let Some(e) = report.error
                && !e.starts_with("no code registered")
                && reported.insert(format!("{}\u{0}{e}", r.id))
            {
                let line = format!(
                    "strict replay: run '{}' ({} v{}) does not replay against its code: {e}. Code \
                     outside steps must make the same calls in the same order every time: read \
                     the clock, random numbers and unordered collections inside a step. {}",
                    r.id,
                    r.wtype,
                    r.version,
                    crate::error::docs_link("Determinism")
                );
                crate::log::emit(&crate::log::Record {
                    workflow_id: Some(&r.id),
                    workflow_type: Some(&r.wtype),
                    run: Some(r.run),
                    ..crate::log::Record::engine(crate::log::Level::Warn, &line)
                });
            }
        }
        // Dropped here: if this was the engine's last handle, it closes on
        // this thread, and `Dev::drop` does not join it.
        drop(engine);
    }
}
