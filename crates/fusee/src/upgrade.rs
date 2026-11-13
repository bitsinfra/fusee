//! Converts a data directory from log format 2 (a log per shard) to log
//! format 3 (one shared log, [`crate::wal`]), offline.
//!
//! Every valid record of every shard's segments is copied, in LSN order, so
//! both retained checkpoints of every shard still find the records after
//! them (recovery skips what a checkpoint covers). Records are re-encoded
//! once into the shared log's canonical form; run stores, checkpoints and
//! archives stay where they are.
//!
//! The conversion survives a crash at any point. The new log is written
//! under `wal.tmp`, synced and renamed to `wal`; only then does `fusee.meta`
//! switch to `log=3` (written to a temporary file, synced and renamed), and
//! only after that are the old segments deleted. Until the meta file says
//! `log=3`, the directory opens as format 2 and a leftover `wal` is removed
//! by the next attempt; after it, leftover old segments are ignored and the
//! next attempt removes them.

use crate::engine::{read_meta, write_meta};
use crate::error::{Error, Result};
use crate::local::{FlushMethod, LocalOptions, sync_dir};
use crate::wal::{FileDevice, Wal, WalOptions};
use fusee_core::record::{Lsn, RecordBuf};
use fusee_core::segment::{ScanError, Tail, scan_segment};
use fusee_core::wal::SectionWriter;
use std::fs;
use std::path::{Path, PathBuf};

/// What [`upgrade_to_shared_log`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Upgraded {
    /// The directory already uses the shared log (leftover format-2
    /// segments, if any, were removed).
    AlreadyCurrent,
    /// Converted: records copied per shard.
    Converted { records: Vec<u64> },
}

fn segment_files(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut v: Vec<(u64, PathBuf)> = match fs::read_dir(dir) {
        Ok(d) => d
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let n = e.file_name().to_str()?.strip_suffix(".seg")?.parse().ok()?;
                Some((n, e.path()))
            })
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e.into()),
    };
    v.sort();
    Ok(v)
}

/// Every valid record of one shard's format-2 log, in LSN order. Refuses
/// damage and gaps rather than copying a log that lost records.
fn shard_records(shard: u32, log_dir: &Path) -> Result<Vec<(Lsn, RecordBuf)>> {
    let segments = segment_files(log_dir)?;
    let last = segments.last().map(|s| s.0);
    let mut out: Vec<(Lsn, RecordBuf)> = Vec::new();
    let mut expected: Option<Lsn> = None;
    for (n, path) in segments {
        let bytes = fs::read(&path)?;
        let mut records = Vec::new();
        let scanned = match scan_segment(&bytes, |lsn, rec| records.push((lsn, rec))) {
            Ok(Some(s)) => s,
            // Never activated.
            Ok(None) => continue,
            // The newest segment's header tore with its first flush, which
            // was never acknowledged.
            Err(ScanError::BadHeader) if Some(n) == last => continue,
            Err(e) => {
                return Err(Error::Io(format!(
                    "shard {shard} log segment {n}: {e}; open the directory with the version \
                     that wrote it and shut it down cleanly before upgrading"
                )));
            }
        };
        if scanned.header.shard != shard {
            return Err(Error::Io(format!(
                "segment {n} of shard {shard} belongs to shard {}",
                scanned.header.shard
            )));
        }
        if let Tail::Corrupt { at, later_lsn } = scanned.tail {
            return Err(Error::Io(format!(
                "shard {shard} log segment {n} is damaged at offset {at}, but record {later_lsn} \
                 after it is intact: refusing to upgrade a log that lost records"
            )));
        }
        if let Some(e) = expected
            && scanned.header.base_lsn != e
        {
            if scanned.header.base_lsn > e && scanned.max_batch_start <= e {
                // A batch torn across a roll: nothing after it was
                // acknowledged.
                break;
            }
            return Err(Error::Io(format!(
                "shard {shard}: segment {n} starts at record {} but the previous one ends at {}",
                scanned.header.base_lsn,
                e - 1
            )));
        }
        out.extend(records);
        expected = Some(scanned.last_lsn + 1);
    }
    Ok(out)
}

/// Converts the directory at `dir` (closed: this takes its lock) to the
/// shared log. `local` supplies the flush method and segment size.
pub fn upgrade_to_shared_log(dir: &Path, local: &LocalOptions) -> Result<Upgraded> {
    let _lock = crate::local::lock_directory(dir)?;
    let (shards, log) = read_meta(dir)?
        .ok_or_else(|| Error::Io(format!("{} is not a Fusee data directory", dir.display())))?;
    let shard_dir = |s: u32| dir.join(format!("shard-{s:03}"));
    let method = local.flush;
    // A server directory written by a build that labelled it log format 3
    // while keeping a log per shard has segments and no shared log: it is
    // format 2, and its segments are converted, never removed.
    let mislabelled = log == 3
        && !dir.join("wal").exists()
        && (0..shards).any(|s| {
            segment_files(&shard_dir(s).join("log")).is_ok_and(|segments| !segments.is_empty())
        });
    if log == 3 && !mislabelled {
        // A crash after the switch may have left the old segments.
        for s in 0..shards {
            remove_segments(&shard_dir(s).join("log"), method)?;
        }
        return Ok(Upgraded::AlreadyCurrent);
    }
    // A crash before the switch may have left a new log: it was never used.
    for leftover in ["wal", "wal.tmp"] {
        let p = dir.join(leftover);
        if p.exists() {
            fs::remove_dir_all(&p)?;
        }
    }
    sync_dir(dir, method)?;

    let tmp = dir.join("wal.tmp");
    let opts = WalOptions {
        segment_bytes: local.segment_bytes,
        first_segment_bytes: local.segment_bytes.min(1 << 20),
        ..WalOptions::default()
    };
    let mut wal = Wal::open(
        FileDevice::open(&tmp, local.clone())?,
        shards,
        opts,
        crate::wal::new_life(),
        crate::local::unix_ms(),
    )?;
    let mut counts = Vec::with_capacity(shards as usize);
    for s in 0..shards {
        let records = shard_records(s, &shard_dir(s).join("log"))?;
        counts.push(records.len() as u64);
        let Some(first) = records.first().map(|r| r.0) else {
            continue;
        };
        let mut w = SectionWriter::new(first);
        let mut pending = 0usize;
        for (lsn, rec) in &records {
            let assigned = w
                .append(rec)
                .map_err(|_| Error::Codec(format!("shard {s} record {lsn} is too large")))?;
            if assigned != *lsn {
                return Err(Error::Io(format!(
                    "shard {s}: records are not consecutive at {lsn}"
                )));
            }
            pending += w.last_encoded().len();
            // Batches of about 1 MiB.
            if pending >= 1 << 20 {
                if let Some(b) = w.take() {
                    wal.stage(s, b)?;
                }
                wal.write(crate::local::unix_ms())?;
                pending = 0;
            }
        }
        if let Some(b) = w.take() {
            wal.stage(s, b)?;
        }
        wal.write(crate::local::unix_ms())?;
    }
    drop(wal);
    sync_dir(&tmp, method)?;
    fs::rename(&tmp, dir.join("wal"))?;
    sync_dir(dir, method)?;
    // The switch: from here on the directory opens with the shared log.
    write_meta(dir, shards, 3)?;
    for s in 0..shards {
        remove_segments(&shard_dir(s).join("log"), method)?;
    }
    Ok(Upgraded::Converted { records: counts })
}

fn remove_segments(log_dir: &Path, method: FlushMethod) -> Result<()> {
    let segments = segment_files(log_dir)?;
    if segments.is_empty() {
        return Ok(());
    }
    for (_, p) in segments {
        fs::remove_file(p)?;
    }
    sync_dir(log_dir, method)?;
    Ok(())
}
