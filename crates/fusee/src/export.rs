//! Exported journals and replay testing.
//!
//! `fusee export <dir> <workflow-id>` writes a run's journal to a `.fjnl`
//! file, and `fusee export --sample 1000 <dir>` takes the most recent runs
//! whose whole journal is available (running and suspended runs, and
//! finished runs of engines opened with `keep_history`). A [`Replayer`]
//! runs new workflow code against exported journals, without running any
//! step function or timer, and reports the first difference by step name.
//! Run it in CI before a deploy.
//!
//! File format: `FUSEEJNL`, a little-endian `u32` format version, a
//! length-prefixed JSON header (`workflowId`, `workflowType`, `version`,
//! `status`), then the journal's canonical records.

use crate::ctx::Ctx;
use crate::engine::erase;
use crate::env::Notify;
use crate::error::{Error, Result};
use crate::local::MemoryEnv;
use crate::runtime::{Registry, RunDetail, RuntimeConfig, ShardRuntime, SharedRegistry};
use fusee_core::record::{RecordBuf, decode_canonical_all, encode_canonical};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

pub const MAGIC: &[u8; 8] = b"FUSEEJNL";
pub const FORMAT: u32 = 1;

/// A journal read from an export file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportedJournal {
    pub workflow_id: String,
    pub workflow_type: String,
    pub version: u32,
    pub status: String,
    pub records: Vec<RecordBuf>,
}

impl ExportedJournal {
    pub fn from_detail(d: &RunDetail) -> Self {
        Self {
            workflow_id: d.summary.id.clone(),
            workflow_type: d.summary.wtype.clone(),
            version: d.summary.version,
            status: d.summary.status.clone(),
            records: d.journal.clone(),
        }
    }

    /// Whether the export holds the run's whole journal (a finished run of
    /// an engine without `keep_history` keeps only its start, attributes,
    /// streams and result).
    pub fn is_complete(&self) -> bool {
        use fusee_core::record::Record;
        let done = self
            .records
            .iter()
            .any(|r| matches!(r, Record::Done { .. }));
        !done
            || self.records.iter().any(|r| {
                !matches!(
                    r,
                    Record::Start { .. }
                        | Record::Done { .. }
                        | Record::Attributes { .. }
                        | Record::Chunk { .. }
                        | Record::StreamEnd { .. }
                )
            })
    }

    pub fn encode(&self) -> Vec<u8> {
        let header = serde_json::json!({
            "workflowId": self.workflow_id,
            "workflowType": self.workflow_type,
            "version": self.version,
            "status": self.status,
            "records": self.records.len(),
        })
        .to_string();
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&FORMAT.to_le_bytes());
        out.extend_from_slice(&(header.len() as u32).to_le_bytes());
        out.extend_from_slice(header.as_bytes());
        for r in &self.records {
            encode_canonical(r, &mut out);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bad = |m: &str| Error::Codec(format!("not a Fusee journal export: {m}"));
        if bytes.len() < 16 || &bytes[..8] != MAGIC {
            return Err(bad("missing header"));
        }
        let format = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        if format != FORMAT {
            return Err(bad(&format!("format {format}")));
        }
        let n = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]) as usize;
        let header = bytes
            .get(16..16 + n)
            .ok_or_else(|| bad("truncated header"))?;
        let h: serde_json::Value = serde_json::from_slice(header)?;
        let records = decode_canonical_all(&bytes[16 + n..]).map_err(|e| bad(&e.to_string()))?;
        Ok(Self {
            workflow_id: h["workflowId"].as_str().unwrap_or_default().to_owned(),
            workflow_type: h["workflowType"].as_str().unwrap_or_default().to_owned(),
            version: h["version"].as_u64().unwrap_or(1) as u32,
            status: h["status"].as_str().unwrap_or_default().to_owned(),
            records,
        })
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        std::fs::write(path, self.encode())?;
        Ok(())
    }

    pub fn read(path: &Path) -> Result<Self> {
        Self::decode(&std::fs::read(path)?)
    }
}

/// Exports from a data directory without changing it: the directory is
/// copied (checkpoints first, so the copy is consistent even while an
/// engine writes to it) and the copy is recovered and read.
pub fn export_from_dir(
    dir: &Path,
    id: Option<&str>,
    sample: usize,
) -> Result<Vec<ExportedJournal>> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let copy = loop {
        let path = std::env::temp_dir().join(format!(
            "fusee-export-{}-{}-{}",
            std::process::id(),
            crate::local::unix_ms(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        match std::fs::create_dir(&path) {
            Ok(()) => break path,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.into()),
        }
    };
    let out = (|| {
        copy_dir(dir, &copy)?;
        let mut config = crate::engine::Config::local(&copy);
        // Runs the application still keeps are exported, whatever its
        // retention (see `TOOL_RETENTION`).
        config.retention = crate::engine::TOOL_RETENTION;
        config.park_after = std::time::Duration::MAX;
        let engine = crate::foreign::ForeignEngine::open(&config, Arc::new(|| {}))?;
        let mut out = Vec::new();
        match id {
            Some(id) => match engine.inspect(id)? {
                Some(d) => out.push(ExportedJournal::from_detail(&d)),
                None => {
                    out = crate::archive::find(dir, Some(id), 1)?;
                    if out.is_empty() {
                        return Err(Error::NotFound(id.to_owned()));
                    }
                }
            },
            None => {
                let mut runs = engine.list(usize::MAX);
                runs.sort_by_key(|s| std::cmp::Reverse(s.started_ms));
                for s in runs {
                    if out.len() >= sample {
                        break;
                    }
                    if crate::schedule::is_builtin(&s.wtype) {
                        continue;
                    }
                    if let Some(d) = engine.inspect(&s.id)? {
                        let j = ExportedJournal::from_detail(&d);
                        if j.is_complete() && d.summary.run == s.run {
                            out.push(j);
                        }
                    }
                }
                if out.len() < sample {
                    let archived = crate::archive::find(dir, None, sample)?;
                    for j in archived {
                        if out.len() >= sample {
                            break;
                        }
                        if !out.iter().any(|live| {
                            live.workflow_id == j.workflow_id && live.records == j.records
                        }) {
                            out.push(j);
                        }
                    }
                }
            }
        }
        Ok(out)
    })();
    let _ = std::fs::remove_dir_all(&copy);
    out
}

fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    let mut entries: Vec<std::fs::DirEntry> =
        std::fs::read_dir(from)?.collect::<std::io::Result<_>>()?;
    // Checkpoints name the run store length and log position they cover:
    // copy them before the files they point into.
    entries.sort_by_key(|e| {
        let n = e.file_name().to_string_lossy().into_owned();
        (!n.ends_with(".fck"), n)
    });
    for e in entries {
        let target = to.join(e.file_name());
        if e.file_type()?.is_dir() {
            // Archives are immutable and read directly from the source.
            if e.file_name() != "archive" {
                copy_dir(&e.path(), &target)?;
            }
        } else if e.file_name() != "visibility.redb"
            && !e.file_name().to_string_lossy().ends_with(".lock")
        {
            std::fs::copy(e.path(), &target)?;
        }
    }
    Ok(())
}

/// A file name for an exported journal (workflow IDs may hold `/`).
pub fn file_name(id: &str) -> String {
    let safe: String = id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || "-_.@".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{safe}.fjnl")
}

/// What replaying one journal found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayReport {
    pub workflow_id: String,
    pub workflow_type: String,
    /// The code made the same journal operations, in the same order.
    pub ok: bool,
    /// The first difference, naming the step.
    pub error: Option<String>,
    /// The code reached its end within the journal.
    pub finished: bool,
}

impl std::fmt::Display for ReplayReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.error {
            None => write!(
                f,
                "{} ({}): ok{}",
                self.workflow_id,
                self.workflow_type,
                if self.finished { ", finished" } else { "" }
            ),
            Some(e) => write!(f, "{} ({}): {e}", self.workflow_id, self.workflow_type),
        }
    }
}

/// Runs workflow code against exported journals.
pub struct Replayer {
    registry: SharedRegistry,
    codec: Option<Arc<dyn crate::codec::Codec>>,
}

impl Default for Replayer {
    fn default() -> Self {
        Self::new()
    }
}

impl Replayer {
    pub fn new() -> Self {
        let mut r = Registry::default();
        crate::schedule::register_builtins(&mut r);
        Self {
            registry: Arc::new(RwLock::new(r)),
            codec: None,
        }
    }

    /// A replayer over an engine's registered code (the dev profile's
    /// strict replay).
    pub(crate) fn sharing(
        registry: SharedRegistry,
        codec: Option<Arc<dyn crate::codec::Codec>>,
    ) -> Self {
        Self { registry, codec }
    }

    /// Reads journals written with payload encryption.
    pub fn with_codec(mut self, codec: Arc<dyn crate::codec::Codec>) -> Self {
        self.codec = Some(codec);
        self
    }

    /// Registers a workflow type's code (at the reference's version, else
    /// version 1), as [`Engine::register`](crate::Engine::register) does.
    pub fn register<I, O, F, Fut>(
        &self,
        workflow: impl crate::defs::AsWorkflow<I, O>,
        f: F,
    ) -> &Self
    where
        I: Serialize + DeserializeOwned + 'static,
        O: Serialize + DeserializeOwned + 'static,
        F: Fn(Ctx, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + 'static,
    {
        if let Ok(mut r) = self.registry.write() {
            r.register(workflow.name(), workflow.version().unwrap_or(1), erase(f));
        }
        self
    }

    /// Replays one journal.
    pub fn replay(&self, journal: &ExportedJournal) -> ReplayReport {
        let mut report = ReplayReport {
            workflow_id: journal.workflow_id.clone(),
            workflow_type: journal.workflow_type.clone(),
            ok: false,
            error: None,
            finished: false,
        };
        if !journal.is_complete() {
            report.error = Some(
                "the export holds only the run's start and result (export from an engine with \
                 keep_history)"
                    .into(),
            );
            return report;
        }
        let known = self
            .registry
            .read()
            .is_ok_and(|r| r.get(&journal.workflow_type, journal.version).is_some());
        if !known {
            report.error = Some(format!(
                "no code registered for workflow type '{}' version {}",
                journal.workflow_type, journal.version
            ));
            return report;
        }
        let cfg = RuntimeConfig {
            replay_only: true,
            park_after_ms: u64::MAX,
            ..Default::default()
        };
        let notify: Notify = Arc::new(|| {});
        let services = crate::runtime::Services {
            codec: self.codec.clone(),
            ..Default::default()
        };
        let rt = match ShardRuntime::open_with(
            MemoryEnv::default(),
            cfg,
            self.registry.clone(),
            false,
            services,
            notify,
        ) {
            Ok(rt) => rt,
            Err(e) => {
                report.error = Some(e.to_string());
                return report;
            }
        };
        let run = match rt.import(&journal.records) {
            Ok(r) => r,
            Err(e) => {
                report.error = Some(e.to_string());
                return report;
            }
        };
        for _ in 0..10_000 {
            if !rt.tick().busy {
                break;
            }
        }
        rt.tick();
        if let Some(e) = rt.stuck_reason(run) {
            report.error = Some(e);
            return report;
        }
        let summary = rt.summary(run);
        report.finished = summary.as_ref().is_some_and(|s| s.finished_ms.is_some());
        // Journal deliveries the code never reached are a difference too.
        if let Some((replaying, live)) = rt.code_state(run)
            && replaying
            && !live
        {
            report.error = Some("the code stopped before reaching the end of its journal".into());
            return report;
        }
        report.ok = true;
        report
    }

    pub fn replay_file(&self, path: &Path) -> Result<ReplayReport> {
        Ok(self.replay(&ExportedJournal::read(path)?))
    }

    /// Replays every `.fjnl` file in `dir`.
    pub fn replay_dir(&self, dir: &Path) -> Result<Vec<(PathBuf, ReplayReport)>> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "fjnl"))
            .collect();
        files.sort();
        files
            .into_iter()
            .map(|p| {
                let r = self.replay_file(&p)?;
                Ok((p, r))
            })
            .collect()
    }
}
