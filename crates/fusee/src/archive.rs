//! Immutable, checksummed journals of finished runs, copied by the checkpoint
//! thread before it publishes a checkpoint or removes its source segments.
use crate::export::ExportedJournal;
use crate::local::{FlushMethod, flush_file, sync_dir};
use fusee_core::record::{Record, decode_canonical_all};
use fusee_core::shard::BlobRef;
use fusee_core::store::{StoreEntry, read_entry};
use fusee_core::{Lsn, RunId};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

const MAGIC: &[u8; 8] = b"FUSEEAR1";
const TARGET_BYTES: u64 = 8 << 20;

pub(crate) struct Pending {
    pub source: Arc<File>,
    pub run: RunId,
    pub lsn: Lsn,
    pub blob: BlobRef,
}

pub(crate) fn persist(
    shard: &Path,
    through: Lsn,
    method: FlushMethod,
    pending: &[Pending],
) -> io::Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let dir = shard.join("archive");
    fs::create_dir_all(&dir)?;
    sync_dir(shard, method)?;
    let mut start = 0;
    while start < pending.len() {
        let mut end = start;
        let mut size = 0;
        while end < pending.len()
            && (size == 0 || size + u64::from(pending[end].blob.len) <= TARGET_BYTES)
        {
            size += u64::from(pending[end].blob.len);
            end += 1;
        }
        let tmp = dir.join("segment.tmp");
        let mut output = File::create(&tmp)?;
        output.write_all(MAGIC)?;
        output.write_all(&(end as u64 - start as u64).to_le_bytes())?;
        let mut crc = 0;
        for item in &pending[start..end] {
            let mut bytes = vec![0; item.blob.len as usize];
            item.source.read_exact_at(&mut bytes, item.blob.offset)?;
            crate::env::check_live_frame(&bytes, item.run)?;
            output.write_all(&bytes)?;
            crc = crc32c::crc32c_append(crc, &bytes);
        }
        output.write_all(&crc.to_le_bytes())?;
        flush_file(&output, method)?;
        // A repeat after a crash is idempotent. The checksum and frame count
        // identify this exact group even if a later checkpoint groups it differently.
        let name = format!(
            "{through:020}-{:020}-{crc:08x}-{size}.arc",
            pending[start].run
        );
        let path = dir.join(name);
        match fs::hard_link(&tmp, &path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                if !same_segment(&tmp, &path)? {
                    return Err(io::Error::other(
                        "different data at immutable history archive path",
                    ));
                }
            }
            Err(e) => return Err(e),
        }
        fs::remove_file(&tmp)?;
        sync_dir(&dir, method)?;
        start = end;
    }
    Ok(())
}

/// Writes `frames` (encoded `Done` entries) as archive segments under
/// `dir`, named and grouped as [`persist`] names and groups them, for
/// [`crate::reshard`], which rewrites a directory's archives into its new
/// shards.
pub(crate) fn write_frames(
    dir: &Path,
    through: Lsn,
    method: FlushMethod,
    frames: &[(RunId, Vec<u8>)],
) -> io::Result<()> {
    if frames.is_empty() {
        return Ok(());
    }
    fs::create_dir_all(dir)?;
    let mut start = 0;
    while start < frames.len() {
        let mut end = start;
        let mut size = 0u64;
        while end < frames.len() && (size == 0 || size + frames[end].1.len() as u64 <= TARGET_BYTES)
        {
            size += frames[end].1.len() as u64;
            end += 1;
        }
        let mut bytes = Vec::with_capacity(size as usize + 20);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&(end as u64 - start as u64).to_le_bytes());
        let mut crc = 0;
        for (_, f) in &frames[start..end] {
            bytes.extend_from_slice(f);
            crc = crc32c::crc32c_append(crc, f);
        }
        bytes.extend_from_slice(&crc.to_le_bytes());
        let name = format!("{through:020}-{:020}-{crc:08x}-{size}.arc", frames[start].0);
        let path = dir.join(&name);
        if path.exists() {
            if fs::read(&path)? != bytes {
                return Err(io::Error::other(
                    "different data at immutable history archive path",
                ));
            }
        } else {
            let tmp = dir.join("segment.tmp");
            let mut output = File::create(&tmp)?;
            output.write_all(&bytes)?;
            flush_file(&output, method)?;
            fs::rename(&tmp, &path)?;
        }
        start = end;
    }
    sync_dir(dir, method)
}

/// The frames of one archive segment, after checking each of them and the
/// segment's checksum and length as [`visit`] does.
pub(crate) fn read_frames(path: &Path) -> io::Result<Vec<Vec<u8>>> {
    let bytes = fs::read(path)?;
    if bytes.len() < 20 || &bytes[..8] != MAGIC {
        return Err(io::Error::other("bad history archive magic"));
    }
    let count = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let end = bytes.len() - 4;
    let mut at = 16usize;
    let mut crc = 0;
    let mut out = Vec::new();
    for _ in 0..count {
        let len = bytes
            .get(at..at + 4)
            .map(|b| u32::from_le_bytes(b.try_into().unwrap()) as usize)
            .ok_or_else(|| io::Error::other("invalid archive frame length"))?;
        let next = at
            .checked_add(len + 8)
            .filter(|n| *n <= end)
            .ok_or_else(|| io::Error::other("invalid archive frame length"))?;
        let frame = &bytes[at..next];
        if !matches!(
            read_entry(frame).map_err(io::Error::other)?,
            StoreEntry::Done { .. }
        ) {
            return Err(io::Error::other("archive contains a nonterminal frame"));
        }
        crc = crc32c::crc32c_append(crc, frame);
        out.push(frame.to_vec());
        at = next;
    }
    if at != end || bytes[end..] != crc.to_le_bytes() {
        return Err(io::Error::other(
            "history archive checksum or length mismatch",
        ));
    }
    Ok(out)
}

/// Only a retry reads both files. Compare fixed-size blocks so a single
/// unusually large terminal journal does not double peak memory.
fn same_segment(new: &Path, existing: &Path) -> io::Result<bool> {
    let mut a = File::open(new)?;
    let mut b = File::open(existing)?;
    let mut left = a.metadata()?.len();
    if left != b.metadata()?.len() {
        return Ok(false);
    }
    let (mut x, mut y) = ([0u8; 8192], [0u8; 8192]);
    while left > 0 {
        let n = left.min(x.len() as u64) as usize;
        a.read_exact(&mut x[..n])?;
        b.read_exact(&mut y[..n])?;
        if x[..n] != y[..n] {
            return Ok(false);
        }
        left -= n as u64;
    }
    Ok(true)
}

/// Read one archive segment and verify both frame and whole-segment checksums.
pub(crate) fn visit(path: &Path, mut f: impl FnMut(ExportedJournal, RunId, u64)) -> io::Result<()> {
    let mut input = File::open(path)?;
    let mut head = [0; 16];
    input.read_exact(&mut head)?;
    if &head[..8] != MAGIC {
        return Err(io::Error::other("bad history archive magic"));
    }
    let count = u64::from_le_bytes(head[8..].try_into().unwrap());
    let end = input.metadata()?.len();
    let mut offset = 16u64;
    let mut crc = 0;
    for _ in 0..count {
        let mut prefix = [0; 8];
        input.read_exact(&mut prefix)?;
        let len = u32::from_le_bytes(prefix[..4].try_into().unwrap()) as usize;
        offset = offset
            .checked_add(len as u64 + 8)
            .ok_or_else(|| io::Error::other("history offset overflow"))?;
        if offset > end.saturating_sub(4) {
            return Err(io::Error::other("invalid archive frame length"));
        }
        let mut frame = Vec::with_capacity(len + 8);
        frame.extend_from_slice(&prefix);
        frame.resize(len + 8, 0);
        input.read_exact(&mut frame[8..])?;
        crc = crc32c::crc32c_append(crc, &frame);
        let StoreEntry::Done {
            run,
            status,
            journal,
            ..
        } = read_entry(&frame).map_err(io::Error::other)?
        else {
            return Err(io::Error::other("archive contains a nonterminal frame"));
        };
        let records = decode_canonical_all(&journal).map_err(io::Error::other)?;
        let Some(Record::Start {
            id,
            wtype,
            version,
            started_ms,
            ..
        }) = records.first()
        else {
            return Err(io::Error::other("archive journal has no start"));
        };
        let started = *started_ms;
        let journal = ExportedJournal {
            workflow_id: id.to_string(),
            workflow_type: wtype.to_string(),
            version: *version,
            status: status.as_str().into(),
            records,
        };
        f(journal, run, started);
    }
    let mut footer = [0; 4];
    input.read_exact(&mut footer)?;
    if offset + 4 != end || crc.to_le_bytes() != footer {
        return Err(io::Error::other(
            "history archive checksum or length mismatch",
        ));
    }
    Ok(())
}

pub(crate) fn find(dir: &Path, id: Option<&str>, limit: usize) -> io::Result<Vec<ExportedJournal>> {
    use std::collections::BTreeMap;
    let mut found = BTreeMap::new();
    if limit == 0 {
        return Ok(Vec::new());
    }
    for shard in fs::read_dir(dir)? {
        let shard = shard?;
        if !shard.file_name().to_string_lossy().starts_with("shard-") {
            continue;
        }
        let archive = shard.path().join("archive");
        let files = match fs::read_dir(&archive) {
            Ok(files) => files,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for file in files {
            let path = file?.path();
            if path.extension().is_none_or(|e| e != "arc") {
                continue;
            }
            visit(&path, |j, run, started| {
                if id.is_some_and(|id| j.workflow_id != id)
                    || (id.is_none() && crate::schedule::is_builtin(&j.workflow_type))
                {
                    return;
                }
                let key = (started, run, shard.file_name());
                found.insert(key, j);
                if found.len() > limit {
                    found.pop_first();
                }
            })?;
        }
    }
    Ok(found.into_values().rev().collect())
}
