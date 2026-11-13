//! Restore to a point in time, and checking a backup.
//!
//! A restore builds the directory under a hidden sibling
//! (`.<name>.restoring`), syncs it and renames it into place, so `dir`
//! either does not exist yet or is complete: a restore interrupted at any
//! point leaves only the sibling, which the next attempt removes. The
//! backup itself is only read.

use super::Target;
use super::manifest::{self, Plan, Root, Snapshot, State, check_format};
use super::ship::{Walk, unpack_blobs, walk};
use crate::local::{FlushMethod, flush_file, sync_dir};
use fusee_core::wal::{WAL_HEADER_BYTES, WalHeader};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

fn other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

pub(crate) fn read_root(target: &dyn Target) -> io::Result<Option<Root>> {
    let Some(bytes) = target.get_manifest()? else {
        return Ok(None);
    };
    let root: Root = serde_json::from_slice(&bytes)
        .map_err(|e| other(format!("the backup manifest is damaged: {e}")))?;
    check_format(root.format).map_err(other)?;
    Ok(Some(root))
}

/// The backup's manifest and its state, `None` for an empty target.
pub fn load_state(target: &dyn Target) -> io::Result<Option<(Root, State)>> {
    // A running backup replaces the snapshot now and then: read again if
    // the one named is gone.
    let mut tries = 0;
    loop {
        let Some(root) = read_root(target)? else {
            return Ok(None);
        };
        let mut state = match &root.snapshot {
            None => State {
                shards: root.shards,
                timelines: Vec::new(),
            },
            Some(key) => match target.get(key)? {
                Some(bytes) => {
                    let s: Snapshot = serde_json::from_slice(&bytes)
                        .map_err(|e| other(format!("backup snapshot {key} is damaged: {e}")))?;
                    check_format(s.format).map_err(other)?;
                    s.state
                }
                None if tries < 3 => {
                    tries += 1;
                    continue;
                }
                None => return Err(other(format!("backup snapshot {key} is missing"))),
            },
        };
        for e in &root.events {
            state
                .apply(e)
                .map_err(|m| other(format!("the backup manifest does not apply: {m}")))?;
        }
        return Ok(Some((root, state)));
    }
}

/// What to restore.
#[derive(Clone, Debug, Default)]
pub struct RestoreOptions {
    /// The newest restore point at or before this Unix time (milliseconds);
    /// `None`: the newest.
    pub at_ms: Option<u64>,
    /// Only this timeline's restore points.
    pub timeline: Option<String>,
}

/// What a restore rebuilt.
#[derive(Clone, Debug)]
pub struct Restored {
    pub timeline: String,
    /// The restore point: the log through `seq`, acknowledged at `time_ms`.
    pub time_ms: u64,
    pub seq: u64,
    pub shards: u32,
    pub segments: usize,
    pub checkpoints: usize,
    pub bytes: u64,
}

fn get(target: &dyn Target, key: &str, crc: u32) -> io::Result<Vec<u8>> {
    let bytes = target
        .get(key)?
        .ok_or_else(|| other(format!("backup object {key} is missing")))?;
    if crc32c::crc32c(&bytes) != crc {
        return Err(other(format!("backup object {key} is damaged")));
    }
    Ok(bytes)
}

/// An archive's file name, as the manifest records it: one plain `.arc`
/// name, as `crate::archive` writes them. The manifest is whatever the
/// backup target holds, so a name that is a path (`../x`) would put a file
/// outside the restored directory.
pub(super) fn archive_file_name(name: &str) -> io::Result<&str> {
    let plain = name.len() <= 255
        && !name.starts_with('.')
        && name.ends_with(".arc")
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b));
    if plain {
        Ok(name)
    } else {
        Err(other(format!(
            "the backup's manifest names an archive {name:?}, which is not a plain .arc file name"
        )))
    }
}

fn create(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
}

/// Rebuilds `dir` from `target` at the newest restore point at or before
/// `opts.at_ms`: a local data directory (log format 3) that opens and
/// recovers to exactly what had been acknowledged at that time. `dir` must
/// not exist or be empty.
pub fn restore(target: &dyn Target, dir: &Path, opts: &RestoreOptions) -> io::Result<Restored> {
    if let Ok(mut entries) = fs::read_dir(dir)
        && entries.next().is_some()
    {
        return Err(other(format!("{} exists and is not empty", dir.display())));
    }
    let name = dir
        .file_name()
        .ok_or_else(|| other(format!("{} names no directory", dir.display())))?
        .to_string_lossy()
        .into_owned();
    let parent = match dir.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_owned(),
        _ => PathBuf::from("."),
    };
    fs::create_dir_all(&parent)?;
    let staging = parent.join(format!(".{name}.restoring"));
    // What a restore interrupted before left.
    match fs::remove_dir_all(&staging) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let mut tries = 0;
    let restored = loop {
        let (_, state) =
            load_state(target)?.ok_or_else(|| other("the backup target has no manifest"))?;
        let plan = manifest::choose(&state, opts.at_ms, opts.timeline.as_deref()).map_err(other)?;
        match build(target, &staging, state.shards, &plan) {
            Ok(r) => break r,
            // A running backup may have merged or dropped an object since
            // the manifest was read.
            Err(e) if tries < 2 && e.to_string().contains("is missing") => {
                tries += 1;
                fs::remove_dir_all(&staging)?;
            }
            Err(e) => {
                let _ = fs::remove_dir_all(&staging);
                return Err(e);
            }
        }
    };
    if dir.exists() {
        fs::remove_dir(dir)?;
    }
    fs::rename(&staging, dir)?;
    sync_dir(&parent, FlushMethod::platform_default())?;
    Ok(restored)
}

/// Writes the plan's files under `root`, synced.
fn build(target: &dyn Target, root: &Path, shards: u32, plan: &Plan) -> io::Result<Restored> {
    let method = FlushMethod::platform_default();
    fs::create_dir_all(root)?;
    let wal = root.join("wal");
    fs::create_dir_all(&wal)?;
    let mut bytes = 0u64;
    // The log: each segment's uploaded prefix, the last one cut after the
    // point's batch, zeros after it to the file's length.
    let mut at: Option<Walk> = None;
    let mut blob_files = std::collections::BTreeSet::new();
    for seg in &plan.segments {
        let path = wal.join(format!("{:020}.wal", seg.number));
        let file = create(&path)?;
        let mut image = Vec::new();
        for c in &seg.chunks {
            if c.offset != image.len() as u64 {
                return Err(other(format!(
                    "log segment {} has a gap in the backup at {}",
                    seg.number,
                    image.len()
                )));
            }
            image.extend_from_slice(&get(target, &c.key, c.crc)?);
        }
        let h = WalHeader::decode(&image)
            .map_err(other)?
            .ok_or_else(|| other(format!("log segment {} has no header", seg.number)))?;
        let from = match at {
            None => Walk {
                end: WAL_HEADER_BYTES,
                seq: h.base_seq - 1,
                checksum: h.base_prev,
            },
            Some(prev) => {
                if h.base_seq != prev.seq + 1 || h.base_prev != prev.checksum {
                    return Err(other(format!(
                        "log segment {} does not continue the chain",
                        seg.number
                    )));
                }
                Walk {
                    end: WAL_HEADER_BYTES,
                    ..prev
                }
            }
        };
        let until = seg
            .cut
            .unwrap_or_else(|| seg.chunks.last().map_or(0, |c| c.last_seq));
        let end_seq = walk(&image, 0, h.salt, from, until, u64::MAX)?;
        if end_seq.seq != until {
            return Err(other(format!(
                "log segment {} in the backup ends at batch {} before batch {until}",
                seg.number, end_seq.seq
            )));
        }
        image.truncate(end_seq.end as usize);
        file.write_all_at(&image, 0)?;
        file.set_len(seg.len.max(end_seq.end))?;
        flush_file(&file, method)?;
        bytes += image.len() as u64;
        at = Some(end_seq);
        // The blobs the batches reference (those past the cut are left
        // unreferenced, and the first open deletes their files).
        for pack in seg.chunks.iter().flat_map(|c| &c.blobs) {
            let b = get(target, &pack.key, pack.crc)?;
            for (number, offset, blob) in unpack_blobs(&b)? {
                let dir = wal.join("blobs");
                if !dir.exists() {
                    fs::create_dir_all(&dir)?;
                }
                let path = dir.join(format!("{number:020}.blob"));
                let f = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&path)?;
                f.write_all_at(blob, offset)?;
                blob_files.insert(path);
                bytes += blob.len() as u64;
            }
        }
    }
    for path in &blob_files {
        flush_file(&File::open(path)?, method)?;
    }
    if !blob_files.is_empty() {
        sync_dir(&wal.join("blobs"), method)?;
    }
    sync_dir(&wal, method)?;
    let mut checkpoints = 0;
    for (shard, ps) in plan.shards.iter().enumerate() {
        let sdir = root.join(format!("shard-{shard:03}"));
        fs::create_dir_all(&sdir)?;
        if let Some(cp) = &ps.checkpoint {
            if cp.store_generation != 0 {
                let path = sdir.join(format!("runs-{:010}.dat", cp.store_generation));
                let file = create(&path)?;
                for c in &ps.store {
                    let b = get(target, &c.key, c.crc)?;
                    let n = (cp.store_len.min(c.end) - c.offset) as usize;
                    file.write_all_at(&b[..n], c.offset)?;
                    bytes += n as u64;
                }
                file.set_len(cp.store_len)?;
                flush_file(&file, method)?;
            }
            if !ps.archives.is_empty() {
                let adir = sdir.join("archive");
                fs::create_dir_all(&adir)?;
                for a in &ps.archives {
                    let file = archive_file_name(&a.name)?;
                    let b = get(target, &a.key, a.crc)?;
                    let mut f = create(&adir.join(file))?;
                    f.write_all(&b)?;
                    flush_file(&f, method)?;
                    bytes += b.len() as u64;
                }
                sync_dir(&adir, method)?;
            }
            let b = get(target, &cp.key, cp.crc)?;
            fusee_core::store::Checkpoint::decode(&b)
                .map_err(|e| other(format!("checkpoint {} does not decode: {e}", cp.key)))?;
            let mut f = create(&sdir.join(format!("checkpoint-{:020}.fck", cp.lsn)))?;
            f.write_all(&b)?;
            flush_file(&f, method)?;
            bytes += b.len() as u64;
            checkpoints += 1;
        }
        sync_dir(&sdir, method)?;
    }
    crate::engine::write_meta(root, shards, 3).map_err(|e| other(e.to_string()))?;
    sync_dir(root, method)?;
    Ok(Restored {
        timeline: plan.timeline.clone(),
        time_ms: plan.point.0,
        seq: plan.point.1,
        shards,
        segments: plan.segments.len(),
        checkpoints,
        bytes,
    })
}

/// A backup checked object by object.
#[derive(Clone, Debug, Default)]
pub struct Verified {
    pub objects: usize,
    pub bytes: u64,
    /// Objects the manifest names that the target does not hold.
    pub missing: Vec<String>,
    /// Objects whose checksum does not match the manifest.
    pub damaged: Vec<String>,
    /// Per timeline: ID, restore points, restorable ones, oldest and newest
    /// restorable time.
    pub timelines: Vec<(String, usize, usize, Option<u64>, Option<u64>)>,
}

/// Reads every object the manifest names and checks it, and which restore
/// points can be restored.
pub fn verify(target: &dyn Target) -> io::Result<Verified> {
    let (_, state) =
        load_state(target)?.ok_or_else(|| other("the backup target has no manifest"))?;
    let mut out = Verified::default();
    let mut check = |key: &str, crc: u32| -> io::Result<()> {
        out.objects += 1;
        match target.get(key)? {
            None => out.missing.push(key.to_owned()),
            Some(b) => {
                out.bytes += b.len() as u64;
                if crc32c::crc32c(&b) != crc {
                    out.damaged.push(key.to_owned());
                }
            }
        }
        Ok(())
    };
    for t in &state.timelines {
        for s in &t.segments {
            for c in &s.chunks {
                check(&c.key, c.crc)?;
                for b in &c.blobs {
                    check(&b.key, b.crc)?;
                }
            }
        }
        for c in &t.checkpoints {
            check(&c.key, c.crc)?;
        }
        for c in &t.stores {
            check(&c.key, c.crc)?;
        }
        for a in &t.archives {
            check(&a.key, a.crc)?;
        }
    }
    for t in &state.timelines {
        let ok: Vec<_> = t.restorable(state.shards).collect();
        out.timelines.push((
            t.id.clone(),
            t.points.len(),
            ok.len(),
            ok.first().map(|p| p.0),
            ok.last().map(|p| p.0),
        ));
    }
    Ok(out)
}
