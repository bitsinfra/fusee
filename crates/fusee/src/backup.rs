//! Continuous backup of a local data directory to object storage, and
//! restore to a point in time.
//!
//! **What is shipped.** A background thread (the shipper) uploads, from the
//! shared log (log format 3, [`crate::wal`]):
//!
//! - every sealed segment (one the chain has moved past), as soon as the
//!   chain leaves it;
//! - the active segment's durable prefix, every
//!   [`BackupOptions::upload_interval`] while it grows;
//! - every completed checkpoint of every shard, with the part of the run
//!   store it names that is not uploaded yet, and the history archives it
//!   covers.
//!
//! Objects are immutable. A log chunk holds whole batches of one segment
//! (`<timeline>/log/<segment>/<offset>-<end>.wal`); a store chunk holds a byte
//! range of one run-store generation; small chunks are merged into larger
//! ones as they accumulate (eight of one size make one of the next), so a
//! slow log does not leave one object per upload. Nothing on the write path
//! waits for any of this: the shipper reads segment files and run stores
//! that are already durable, and the log only keeps segments the backup
//! has not uploaded yet from being retired.
//!
//! **Restore points.** Every [`BackupOptions::resolution`] while the log is
//! written, the shipper reads the log's acknowledged position and the time
//! together; once the log is uploaded through that position, the pair is a
//! restore point: restoring it rebuilds exactly the batches acknowledged at
//! that time. A checkpoint is usable for a restore point when it completed
//! before the position the point names was acknowledged (so it knows
//! nothing the point does not), and a point is restorable when every shard
//! has a usable checkpoint (or the log reaches back to its first record)
//! and the log from the oldest one onward is uploaded without a gap.
//!
//! **Manifest.** `manifest.json` at the root of the target is replaced
//! atomically after each upload. It names a snapshot of the backup's state
//! (`state/<version>.json`, immutable) and the events since it (chunks,
//! checkpoints, restore points). An object is uploaded completely before
//! any manifest names it, and deleted only after a manifest that no longer
//! names it is in place, so a crash at any moment leaves a target whose
//! manifest names only complete objects; the next run deletes objects no
//! manifest names and continues from what the manifest says.
//!
//! **Loss window.** With `upload_interval` set (one second by default), at
//! most the log acknowledged since the last upload is not in the backup:
//! about one interval plus the upload's own time. Without it, at most the
//! active segment (up to the configured segment size). A clean shutdown
//! uploads everything.
//!
//! **Retention.** Restore points older than [`BackupOptions::retention`]
//! are dropped, and so is every object no remaining point needs. Points
//! older than ten minutes are thinned to one per minute; for points older
//! than an hour one checkpoint per shard per hour is kept (a restore to
//! such a point replays up to an hour of log).
//!
//! **Timelines.** A backup continues where the manifest says only if the
//! local log still holds the last batch uploaded, with the same checksum.
//! Otherwise (a directory restored to an earlier point and backed up again
//! to the same target, say) it starts a new timeline, so the two histories
//! never mix. Restore picks the newest restore point at or before the
//! requested time across timelines.
//!
//! Server-mode and replicated directories are not covered.

mod manifest;
mod restore;
#[cfg(feature = "s3")]
pub mod s3;
mod ship;
mod target;
#[cfg(test)]
mod tests;

pub use manifest::{Plan, Point, State, Timeline};
pub use restore::{RestoreOptions, Restored, Verified, load_state, restore, verify};
pub(crate) use ship::{Inbox, Shipper};
pub use target::{LocalTarget, Target, open_target};

use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

/// How a backup runs.
#[derive(Clone, Debug)]
pub struct BackupOptions {
    /// Upload the active segment's durable prefix this often while the log
    /// grows. `None`: upload only sealed segments; the loss window is then
    /// the active segment.
    pub upload_interval: Option<Duration>,
    /// Record a restore point this often while the log is written.
    pub resolution: Duration,
    /// Keep restore points (and what they need) this long.
    pub retention: Duration,
}

impl Default for BackupOptions {
    fn default() -> Self {
        Self {
            upload_interval: Some(Duration::from_secs(1)),
            resolution: Duration::from_secs(1),
            retention: Duration::from_secs(7 * 24 * 3600),
        }
    }
}

/// A backup target and its options, set with
/// [`Config::backup`](crate::Config::backup).
#[derive(Clone)]
pub struct BackupConfig {
    pub target: Arc<dyn Target>,
    pub options: BackupOptions,
    /// The shipper of the engine last opened with this configuration.
    running: Arc<Mutex<Weak<Shipper>>>,
}

impl std::fmt::Debug for BackupConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackupConfig")
            .field("target", &self.target.describe())
            .field("options", &self.options)
            .finish()
    }
}

impl BackupConfig {
    pub fn new(target: Arc<dyn Target>, options: BackupOptions) -> Self {
        Self {
            target,
            options,
            running: Arc::new(Mutex::new(Weak::new())),
        }
    }

    /// The same target and options with a handle of their own: each
    /// engine's [`crate::Engine::backup`] names its own backup, even when
    /// one configuration opens several engines in turn.
    pub(crate) fn for_engine(&self) -> Self {
        Self::new(self.target.clone(), self.options.clone())
    }

    /// The running backup of the engine opened with this configuration.
    pub fn handle(&self) -> Backup {
        Backup {
            shipper: self
                .running
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        }
    }
}

/// A position in the backup that can be restored: the log through batch
/// `seq`, exactly what was acknowledged at `time_ms`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestorePoint {
    pub timeline: String,
    pub time_ms: u64,
    pub seq: u64,
}

/// What a running backup has done.
#[derive(Clone, Debug, Default)]
pub struct BackupStatus {
    pub timeline: String,
    /// The newest restore point.
    pub restore_point: Option<RestorePoint>,
    /// The last batch uploaded and the last one acknowledged by the log.
    pub uploaded_seq: u64,
    pub acked_seq: u64,
    pub objects_uploaded: u64,
    pub bytes_uploaded: u64,
    pub manifest_writes: u64,
    pub objects_deleted: u64,
    /// The last upload error, cleared by the next successful upload.
    pub last_error: Option<String>,
}

/// A running backup ([`crate::Engine::backup`]).
#[derive(Clone)]
pub struct Backup {
    shipper: Weak<Shipper>,
}

impl Backup {
    /// Uploads everything the log has acknowledged and every completed
    /// checkpoint now, and returns the restore point that covers it: every
    /// workflow acknowledged before this call is in it.
    pub async fn sync(&self) -> io::Result<RestorePoint> {
        let rx = self
            .shipper
            .upgrade()
            .ok_or_else(|| io::Error::other("the backup has stopped"))?
            .request();
        rx.await
            .map_err(|_| io::Error::other("the backup has stopped"))?
    }

    pub fn status(&self) -> Option<BackupStatus> {
        Some(self.shipper.upgrade()?.status())
    }

    /// Waits until the backup has stopped: after the engine's shards have
    /// closed, its last upload ([`crate::Engine::shutdown`] waits for it).
    pub async fn stopped(&self) {
        while self.shipper.strong_count() > 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

/// Starts the backup of a shared log that was just opened, before any
/// shard runs: reads the target's manifest (an open fails if it cannot),
/// takes each shard's newest checkpoint for upload, and holds every
/// segment until the backup has it.
pub(crate) fn attach(
    wal: &Arc<crate::wal::SharedWal>,
    dir: &Path,
    shards: u32,
    config: &BackupConfig,
) -> crate::Result<()> {
    wal.set_backup_needs(Some(0))?;
    let shipper = Shipper::start(
        wal,
        dir,
        shards,
        config.target.clone(),
        config.options.clone(),
    )
    .map_err(|e| crate::Error::Io(format!("backup to {}: {e}", config.target.describe())))?;
    *config.running.lock().unwrap_or_else(|e| e.into_inner()) = Arc::downgrade(&shipper);
    Ok(())
}

/// Parses a time for `--at`: RFC 3339 (`2026-11-09T12:00:00Z`), Unix
/// milliseconds (`1762689600000`), or a duration before now (`-90s`,
/// `-15m`, `-2h`, `-1d`).
pub fn parse_time(text: &str, now_ms: u64) -> Result<u64, String> {
    let t = text.trim();
    if let Some(rel) = t.strip_prefix('-') {
        let (num, unit) =
            rel.split_at(rel.find(|c: char| !c.is_ascii_digit()).unwrap_or(rel.len()));
        let n: u64 = num.parse().map_err(|_| format!("bad duration {text:?}"))?;
        let ms = match unit {
            "ms" => 1,
            "s" | "" => 1000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            _ => return Err(format!("bad duration unit in {text:?} (ms, s, m, h, d)")),
        };
        return Ok(now_ms.saturating_sub(n.saturating_mul(ms)));
    }
    if !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit()) {
        return t.parse().map_err(|_| format!("bad time {text:?}"));
    }
    chrono::DateTime::parse_from_rfc3339(t)
        .map(|d| d.timestamp_millis().max(0) as u64)
        .map_err(|e| format!("bad time {text:?}: {e} (use RFC 3339, Unix ms, or -15m)"))
}

/// Formats Unix milliseconds as RFC 3339 in UTC.
pub fn format_time(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|d| d.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_else(|| ms.to_string())
}
