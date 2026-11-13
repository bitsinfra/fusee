//! A portable engine checkpoint at one protocol op.
//!
//! A log group's image (`FSSNAP02`) holds, for every shard with workflow
//! data, its checkpoint, its exact store prefix and its retained history
//! segments, under `shard-NNN/`, and the lease state at that op. Each
//! shard's checkpoint covers every record of that shard in the entries up
//! to the op; recovery replays the later records from the protocol log.
//! A segment group's image (`FSSNAP01`) holds its single shard's checkpoint,
//! store prefix and the last materialized segment through the same batch.
//! Receiving a page never writes a live engine file.
use super::{FlushMethod, annotate, flush_file, other, sync_dir};
use fusee_core::segment::{Tail, scan_segment};
use fusee_core::store::Checkpoint;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"FSSNAP01";
const MAGIC_LOG: &[u8; 8] = b"FSSNAP02";
pub(super) const PAGE: usize = 1 << 20;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Image {
    pub op: u64,
    pub checksum: u64,
    /// A segment group's image: its shard's checkpoint LSN. A log image: the sum of
    /// its shards' checkpoint LSNs. Zero: no workflow data.
    pub lsn: u64,
    pub len: u64,
    pub sha256: [u8; 32],
    /// A log image: the group's shard count (0: a segment group's image).
    #[serde(default)]
    pub shards: u32,
    /// A log image: each shard's checkpoint LSN (0: no checkpoint).
    #[serde(default)]
    pub covered: Vec<u64>,
}

impl Image {
    pub fn name(&self) -> String {
        let hash: String = self
            .sha256
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        format!("snapshot-{:020}-{hash}", self.op)
    }
}

#[derive(Clone, Copy)]
pub(super) struct CheckpointPrefix {
    pub lsn: u64,
    pub generation: u64,
    pub len: u64,
}

pub(super) struct Sources {
    prefix: CheckpointPrefix,
    checkpoint: File,
    store: Option<File>,
    shard: PathBuf,
    /// A log group's shard number; `None` for a segment group.
    index: Option<u32>,
}

fn checkpoint_name(lsn: u64) -> String {
    format!("checkpoint-{lsn:020}.fck")
}
fn store_name(generation: u64) -> String {
    format!("runs-{generation:010}.dat")
}
fn segment_name(segment: u64) -> String {
    format!("log/{segment:020}.seg")
}

impl Sources {
    /// Pin append-only files while the engine still owns this checkpoint.
    /// Later compaction may unlink a generation without changing these bytes.
    pub fn pin(shard: &Path, prefix: CheckpointPrefix, index: Option<u32>) -> io::Result<Self> {
        let checkpoint = File::open(shard.join(checkpoint_name(prefix.lsn)))?;
        let store = if prefix.len == 0 {
            None
        } else {
            let file = File::open(shard.join(store_name(prefix.generation)))?;
            if file.metadata()?.len() < prefix.len {
                return Err(other("checkpoint store prefix is missing"));
            }
            Some(file)
        };
        Ok(Self {
            prefix,
            checkpoint,
            store,
            shard: shard.to_owned(),
            index,
        })
    }

    pub fn lsn(&self) -> u64 {
        self.prefix.lsn
    }

    pub fn index(&self) -> Option<u32> {
        self.index
    }

    pub fn boundary(&self, segment: u64, bytes: u64) -> io::Result<File> {
        let file = File::open(self.shard.join(segment_name(segment)))?;
        if bytes == 0 || file.metadata()?.len() < bytes {
            return Err(other("checkpoint boundary segment is short"));
        }
        Ok(file)
    }
}

fn validate_checkpoint(bytes: &[u8], prefix: CheckpointPrefix) -> io::Result<()> {
    validate_shard_checkpoint(bytes, prefix, 0, 1)
}

fn validate_shard_checkpoint(
    bytes: &[u8],
    prefix: CheckpointPrefix,
    shard: u32,
    shards: u32,
) -> io::Result<()> {
    let cp = Checkpoint::decode(bytes).map_err(other)?;
    if cp.shard != shard
        || cp.shards != shards
        || cp.lsn != prefix.lsn
        || cp.store_generation != prefix.generation
        || cp.store_len != prefix.len
        || cp.runs.iter().filter_map(|r| r.blob).any(|b| {
            b.offset
                .checked_add(u64::from(b.len))
                .is_none_or(|v| v > cp.store_len)
        })
    {
        return Err(other("checkpoint and store disagree"));
    }
    for run in cp.runs {
        fusee_core::record::decode_canonical_all(&run.journal).map_err(other)?;
    }
    Ok(())
}

fn check_store(file: &mut File, len: u64) -> io::Result<()> {
    let mut reader = BufReader::with_capacity(PAGE, &mut *file);
    fusee::env::scan_store_reader(&mut reader, 0, len, &mut |_, _, _| Ok(()))?;
    file.rewind()
}

/// Reads a file from an offset without moving its shared position, so one
/// pinned descriptor can feed several images.
struct At<'a> {
    file: &'a File,
    pos: u64,
}

impl Read for At<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        use std::os::unix::fs::FileExt;
        let n = self.file.read_at(buf, self.pos)?;
        self.pos += n as u64;
        Ok(n)
    }
}

fn shard_prefix(shard: u32) -> String {
    format!("shard-{shard:03}/")
}

/// A log group's image at `op`: every pinned shard's checkpoint, store
/// prefix and retained histories, and the lease state. Runs outside the
/// I/O thread; visible only once written, hashed and durable.
pub(super) fn create_log(
    dir: &Path,
    sources: &[std::sync::Arc<Sources>],
    op: u64,
    checksum: u64,
    shards: u32,
    ledger: &[u8],
    method: FlushMethod,
) -> io::Result<Image> {
    let mut covered = vec![0u64; shards as usize];
    for src in sources {
        let i = src
            .index
            .ok_or_else(|| other("single-shard checkpoint in a log image"))?;
        *covered
            .get_mut(i as usize)
            .ok_or_else(|| other("checkpoint of a shard outside the group"))? = src.prefix.lsn;
    }
    let lsn = covered.iter().sum();
    let mut image = write_image_as(
        dir,
        MAGIC_LOG,
        op,
        checksum,
        lsn,
        shards,
        method,
        |output| {
            for src in sources {
                let i = src.index.unwrap_or_default();
                let name = shard_prefix(i);
                let mut cp = Vec::new();
                At {
                    file: &src.checkpoint,
                    pos: 0,
                }
                .read_to_end(&mut cp)?;
                validate_shard_checkpoint(&cp, src.prefix, i, shards)?;
                add(
                    output,
                    &format!("{name}{}", checkpoint_name(src.prefix.lsn)),
                    cp.len() as u64,
                    cp.as_slice(),
                )?;
                if let Some(store) = &src.store {
                    let mut reader = BufReader::with_capacity(
                        PAGE,
                        At {
                            file: store,
                            pos: 0,
                        },
                    );
                    fusee::env::scan_store_reader(
                        &mut reader,
                        0,
                        src.prefix.len,
                        &mut |_, _, _| Ok(()),
                    )?;
                    add(
                        output,
                        &format!("{name}{}", store_name(src.prefix.generation)),
                        src.prefix.len,
                        At {
                            file: store,
                            pos: 0,
                        },
                    )?;
                }
                add_archives(output, &src.shard, src.prefix.lsn, &name)?;
            }
            add(output, "leases", ledger.len() as u64, ledger)
        },
    )?;
    image.covered = covered;
    Ok(image)
}

/// Retained history segments of a shard through `lsn`.
fn add_archives(output: &mut impl Write, shard: &Path, lsn: u64, prefix: &str) -> io::Result<()> {
    let archive = shard.join("archive");
    if !archive.try_exists()? {
        return Ok(());
    }
    let mut files = Vec::new();
    for item in fs::read_dir(archive)? {
        let item = item?;
        let name = item
            .file_name()
            .into_string()
            .map_err(|_| other("non-UTF8 history name"))?;
        if !name.ends_with(".arc") {
            continue;
        }
        let through = archive_lsn(&name).ok_or_else(|| other("invalid history segment name"))?;
        if through <= lsn {
            files.push((name, item.path()));
        }
    }
    files.sort();
    for (name, path) in files {
        fusee::local::verify_history_segment(&path)?;
        let input = File::open(path)?;
        add(
            output,
            &format!("{prefix}archive/{name}"),
            input.metadata()?.len(),
            input,
        )?;
    }
    Ok(())
}

fn check_segment(input: impl Read, len: u64, segment: u64, lsn: u64) -> io::Result<Vec<u8>> {
    // One engine segment, independent of the size of the run store or archive.
    let mut bytes = Vec::new();
    input.take(len).read_to_end(&mut bytes)?;
    let info = scan_segment(&bytes, |_, _| {})
        .map_err(other)?
        .ok_or_else(|| other("empty checkpoint boundary"))?;
    if bytes.len() as u64 != len
        || info.header.segment != segment
        || info.header.shard != 0
        || info.last_lsn != lsn
        || info.tail != Tail::Clean
    {
        return Err(other("damaged checkpoint boundary segment"));
    }
    Ok(bytes)
}

struct DigestWriter<'a> {
    file: BufWriter<&'a File>,
    digest: Sha256,
}

impl Write for DigestWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.file.write(bytes)?;
        self.digest.update(&bytes[..written]);
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn add(writer: &mut impl Write, name: &str, size: u64, input: impl Read) -> io::Result<()> {
    let name_len = u16::try_from(name.len()).map_err(other)?;
    writer.write_all(&name_len.to_le_bytes())?;
    writer.write_all(name.as_bytes())?;
    writer.write_all(&size.to_le_bytes())?;
    if io::copy(&mut input.take(size), writer)? != size {
        return Err(other("short checkpoint image source"));
    }
    Ok(())
}

fn archive_lsn(name: &str) -> Option<u64> {
    let mut words = name.strip_suffix(".arc")?.split('-');
    let through = words.next()?;
    let run = words.next()?;
    let crc = words.next()?;
    let size = words.next()?;
    if through.len() != 20 || run.len() != 20 || crc.len() != 8 || words.next().is_some() {
        return None;
    }
    run.parse::<u64>().ok()?;
    u32::from_str_radix(crc, 16).ok()?;
    size.parse::<u64>().ok()?;
    through.parse().ok()
}

pub(super) struct Boundary {
    pub op: u64,
    pub checksum: u64,
    pub segment: u64,
    pub length: u64,
    pub file: File,
}

/// Runs outside the protocol thread. The image becomes visible only after
/// all content is written, hashed and durable. A failed attempt leaves the
/// previous protocol log and checkpoint untouched.
pub(super) fn create(
    dir: &Path,
    mut src: Sources,
    anchor: Boundary,
    ledger: &[u8],
    method: FlushMethod,
) -> io::Result<Image> {
    write_image(
        dir,
        anchor.op,
        anchor.checksum,
        src.prefix.lsn,
        method,
        |output| {
            let mut cp = Vec::new();
            src.checkpoint.read_to_end(&mut cp)?;
            validate_checkpoint(&cp, src.prefix)?;
            add(
                output,
                &checkpoint_name(src.prefix.lsn),
                cp.len() as u64,
                cp.as_slice(),
            )?;
            if let Some(mut store) = src.store {
                check_store(&mut store, src.prefix.len)?;
                add(
                    output,
                    &store_name(src.prefix.generation),
                    src.prefix.len,
                    store,
                )?;
            }
            let segment =
                check_segment(anchor.file, anchor.length, anchor.segment, src.prefix.lsn)?;
            add(
                output,
                &segment_name(anchor.segment),
                anchor.length,
                segment.as_slice(),
            )?;
            add(output, "leases", ledger.len() as u64, ledger)?;
            add_archives(output, &src.shard, src.prefix.lsn, "")
        },
    )
}

fn write_image(
    dir: &Path,
    op: u64,
    checksum: u64,
    lsn: u64,
    method: FlushMethod,
    parts: impl FnOnce(&mut DigestWriter<'_>) -> io::Result<()>,
) -> io::Result<Image> {
    write_image_as(dir, MAGIC, op, checksum, lsn, 0, method, parts)
}

/// `shards` is written after the anchor only for a log image.
#[allow(clippy::too_many_arguments)]
fn write_image_as(
    dir: &Path,
    magic: &[u8; 8],
    op: u64,
    checksum: u64,
    lsn: u64,
    shards: u32,
    method: FlushMethod,
    parts: impl FnOnce(&mut DigestWriter<'_>) -> io::Result<()>,
) -> io::Result<Image> {
    let pending = dir.join("snapshot-building.tmp");
    let file = File::create(&pending)?;
    let mut output = DigestWriter {
        file: BufWriter::with_capacity(PAGE, &file),
        digest: Sha256::new(),
    };
    output.write_all(magic)?;
    for value in [op, checksum, lsn] {
        output.write_all(&value.to_le_bytes())?;
    }
    if magic == MAGIC_LOG {
        output.write_all(&u64::from(shards).to_le_bytes())?;
    }
    parts(&mut output)?;
    output.write_all(&0u16.to_le_bytes())?;
    output.flush()?;
    let digest = output.digest.finalize().into();
    let image = Image {
        op,
        checksum,
        lsn,
        len: file.metadata()?.len(),
        sha256: digest,
        shards,
        covered: Vec::new(),
    };
    flush_file(&file, method)?;
    fs::rename(pending, dir.join(image.name()))?;
    sync_dir(dir, method)?;
    Ok(image)
}

struct HashedInput {
    file: BufReader<File>,
    digest: Sha256,
}
impl Read for HashedInput {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let n = self.file.read(bytes)?;
        self.digest.update(&bytes[..n]);
        Ok(n)
    }
}

/// Advance an engine image across a suffix containing only committed lease
/// operations. The engine state is identical at both positions.
pub(super) fn copy_control_prefix(
    dir: &Path,
    previous: Option<(Image, File)>,
    op: u64,
    checksum: u64,
    ledger: &[u8],
    method: FlushMethod,
) -> io::Result<Image> {
    let lsn = previous.as_ref().map_or(0, |(image, _)| image.lsn);
    let shards = previous.as_ref().map_or(0, |(image, _)| image.shards);
    let covered = previous
        .as_ref()
        .map_or_else(Vec::new, |(image, _)| image.covered.clone());
    let magic = if shards > 0 { MAGIC_LOG } else { MAGIC };
    let mut image = write_image_as(dir, magic, op, checksum, lsn, shards, method, |out| {
        if let Some((image, file)) = previous {
            if file.metadata()?.len() != image.len {
                return Err(other("control checkpoint source length mismatch"));
            }
            let mut input = HashedInput {
                file: BufReader::with_capacity(PAGE, file),
                digest: Sha256::new(),
            };
            let mut found = [0; 8];
            input.read_exact(&mut found)?;
            if &found != magic
                || read_u64(&mut input)? != image.op
                || read_u64(&mut input)? != image.checksum
                || read_u64(&mut input)? != lsn
                || (shards > 0 && read_u64(&mut input)? != u64::from(shards))
            {
                return Err(other("control checkpoint source anchor mismatch"));
            }
            let mut replaced = false;
            loop {
                let mut word = [0; 2];
                input.read_exact(&mut word)?;
                let n = usize::from(u16::from_le_bytes(word));
                if n == 0 {
                    break;
                }
                if n > 128 {
                    return Err(other("invalid checkpoint component name"));
                }
                let mut bytes = vec![0; n];
                input.read_exact(&mut bytes)?;
                let name = std::str::from_utf8(&bytes).map_err(other)?;
                let len = read_u64(&mut input)?;
                if len > image.len {
                    return Err(other("invalid checkpoint component length"));
                }
                if name == "leases" {
                    if replaced || io::copy(&mut (&mut input).take(len), &mut io::sink())? != len {
                        return Err(other("invalid source lease component"));
                    }
                    replaced = true;
                } else {
                    add(out, name, len, &mut input)?;
                }
            }
            if !replaced
                || input.read(&mut [0])? != 0
                || <[u8; 32]>::from(input.digest.finalize()) != image.sha256
            {
                return Err(other("control checkpoint source is damaged"));
            }
        }
        add(out, "leases", ledger.len() as u64, ledger)
    })?;
    image.covered = covered;
    Ok(image)
}

fn read_u64(r: &mut impl Read) -> io::Result<u64> {
    let mut word = [0; 8];
    r.read_exact(&mut word)?;
    Ok(u64::from_le_bytes(word))
}

fn checked_number(name: &str, prefix: &str, suffix: &str, width: usize) -> Option<u64> {
    let number = name.strip_prefix(prefix)?.strip_suffix(suffix)?;
    if number.len() < width || number.len() > 20 || !number.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let value = number.parse::<u64>().ok()?;
    (number == format!("{value:0width$}")).then_some(value)
}

/// Recover from the immutable image at restart. A lost or stale mutable
/// engine directory is never taken as proof that committed batches were applied.
/// Keep any additionally archived histories while rebuilding the derived files.
/// Every shard directory is replaced: shards the image does not hold start
/// empty and replay all their records from the protocol log.
pub(super) fn rebuild(
    path: &Path,
    image: &Image,
    engine: &Path,
    expected_ledger: &[u8],
    method: FlushMethod,
) -> io::Result<()> {
    let fresh = engine.join("rebuilding");
    let old = engine.join("before-rebuild");
    fs::create_dir_all(engine)?;
    super::generation::remove_unpublished(&fresh)?;
    let ledger = unpack(path, image, &fresh, method)?;
    if ledger != expected_ledger {
        return Err(other("checkpoint image and lease baseline disagree"));
    }
    // On a crash between directory renames, either directory can contain
    // retained archives newer than the portable image.
    for shard in shard_dirs(&fresh)? {
        let name = shard.file_name().ok_or_else(|| other("bad shard path"))?;
        for previous in [engine.join(name), old.join(name)] {
            let archive = previous.join("archive");
            if !archive.try_exists()? {
                continue;
            }
            for entry in fs::read_dir(archive)? {
                let entry = entry?;
                let file = entry.file_name();
                if file.to_str().is_none_or(|s| archive_lsn(s).is_none()) {
                    continue;
                }
                let dest = shard.join("archive").join(&file);
                if !dest.try_exists()? {
                    fusee::local::verify_history_segment(&entry.path())?;
                    fs::hard_link(entry.path(), dest)?;
                }
            }
        }
        sync_dir(&shard.join("archive"), method)?;
    }
    super::generation::remove_unpublished(&old)?;
    fs::create_dir(&old)?;
    for shard in shard_dirs(engine)? {
        let name = shard.file_name().ok_or_else(|| other("bad shard path"))?;
        fs::rename(&shard, old.join(name))?;
    }
    sync_dir(engine, method)?;
    for shard in shard_dirs(&fresh)? {
        let name = shard.file_name().ok_or_else(|| other("bad shard path"))?;
        fs::rename(&shard, engine.join(name))?;
    }
    // A segment group's engine expects its shard directory even when the image
    // holds no workflow data.
    fs::create_dir_all(engine.join("shard-000"))?;
    sync_dir(engine, method)?;
    super::generation::remove_unpublished(&old)?;
    super::generation::remove_unpublished(&fresh)?;
    sync_dir(engine, method)
}

/// `shard-NNN` directories directly under `dir`.
pub(super) fn shard_dirs(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if name
            .to_str()
            .is_some_and(|n| checked_number(n, "shard-", "", 3).is_some())
            && entry.file_type()?.is_dir()
        {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

pub(super) fn verify(path: &Path, image: &Image) -> io::Result<()> {
    let file = File::open(path)?;
    if file.metadata()?.len() != image.len {
        return Err(other("checkpoint image length mismatch"));
    }
    let mut hash = Sha256::new();
    io::copy(&mut BufReader::with_capacity(PAGE, file), &mut hash)?;
    if <[u8; 32]>::from(hash.finalize()) != image.sha256 {
        return Err(other("checkpoint image hash mismatch"));
    }
    Ok(())
}

/// Installs into `engine`, a fresh, unpublished engine directory (one
/// `shard-NNN` directory per shard the image holds), after verifying the
/// whole image. Never accept an arbitrary path from a remote peer.
pub(super) fn unpack(
    path: &Path,
    image: &Image,
    engine: &Path,
    method: FlushMethod,
) -> io::Result<Vec<u8>> {
    verify(path, image)?;
    fs::create_dir(engine).map_err(|e| annotate(e, "creating", engine))?;
    let log_image = image.shards > 0;
    let mut r = BufReader::with_capacity(PAGE, File::open(path)?);
    let mut magic = [0; 8];
    r.read_exact(&mut magic)?;
    if &magic != (if log_image { MAGIC_LOG } else { MAGIC })
        || read_u64(&mut r)? != image.op
        || read_u64(&mut r)? != image.checksum
        || read_u64(&mut r)? != image.lsn
        || (log_image && read_u64(&mut r)? != u64::from(image.shards))
    {
        return Err(other("checkpoint image anchor mismatch"));
    }
    let shard_of = |name: &str| -> io::Result<(u32, String)> {
        if !log_image {
            return Ok((0, name.to_owned()));
        }
        let (dir, rest) = name
            .split_once('/')
            .ok_or_else(|| other("unknown checkpoint component"))?;
        match checked_number(dir, "shard-", "", 3) {
            Some(i) if i < u64::from(image.shards) => Ok((i as u32, rest.to_owned())),
            _ => Err(other("checkpoint component of a shard outside the group")),
        }
    };
    let mut seen = BTreeSet::new();
    // Per shard: checkpoint LSN, store (generation, length), boundary segment.
    let mut checkpoints: BTreeMap<u32, u64> = BTreeMap::new();
    let mut stores: BTreeMap<u32, (u64, u64)> = BTreeMap::new();
    let mut segment = None;
    let mut have_ledger = false;
    loop {
        let mut word = [0; 2];
        r.read_exact(&mut word)?;
        let n = u16::from_le_bytes(word) as usize;
        if n == 0 {
            break;
        }
        if n > 128 {
            return Err(other("invalid checkpoint image part name"));
        }
        let mut bytes = vec![0; n];
        r.read_exact(&mut bytes)?;
        let name = String::from_utf8(bytes).map_err(other)?;
        if !seen.insert(name.clone()) {
            return Err(other("duplicate checkpoint component"));
        }
        let len = read_u64(&mut r)?;
        if len > image.len {
            return Err(other("invalid checkpoint component length"));
        }
        let target = if name == "leases" {
            have_ledger = true;
            engine.join("leases")
        } else {
            let (shard, part) = shard_of(&name)?;
            let dir = engine.join(format!("shard-{shard:03}"));
            if !dir.try_exists()? {
                fs::create_dir(&dir)?;
                fs::create_dir(dir.join("log"))?;
                fs::create_dir(dir.join("archive"))?;
            }
            if let Some(gen_id) = checked_number(&part, "runs-", ".dat", 10) {
                if stores.insert(shard, (gen_id, len)).is_some() {
                    return Err(other("multiple checkpoint stores"));
                }
            } else if let Some(lsn) = checked_number(&part, "checkpoint-", ".fck", 20) {
                if checkpoints.insert(shard, lsn).is_some() {
                    return Err(other("multiple checkpoints of one shard"));
                }
            } else if let Some(number) = checked_number(&part, "log/", ".seg", 20) {
                if log_image || segment.replace(number).is_some() {
                    return Err(other("unexpected boundary segment"));
                }
            } else if let Some(arc) = part.strip_prefix("archive/") {
                if archive_lsn(arc).is_none() {
                    return Err(other("invalid checkpoint history path"));
                }
            } else {
                return Err(other("unknown checkpoint component"));
            }
            dir.join(part)
        };
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&target)
            .map_err(|e| annotate(e, "unpacking", &target))?;
        if io::copy(&mut (&mut r).take(len), &mut output)? != len {
            return Err(other("truncated checkpoint component"));
        }
        flush_file(&output, method)?;
        if name.contains("archive/") {
            fusee::local::verify_history_segment(&target)?;
        }
    }
    if r.read(&mut [0])? != 0 {
        return Err(other("unexpected checkpoint image suffix"));
    }
    if !have_ledger {
        return Err(other("checkpoint image has no lease state"));
    }
    let shards = image.shards.max(1);
    let mut covered = vec![0u64; shards as usize];
    for (&shard, &lsn) in &checkpoints {
        let dir = engine.join(format!("shard-{shard:03}"));
        let bytes = fs::read(dir.join(checkpoint_name(lsn)))?;
        let cp = Checkpoint::decode(&bytes).map_err(other)?;
        let (generation, len) = stores
            .get(&shard)
            .copied()
            .unwrap_or((cp.store_generation, 0));
        validate_shard_checkpoint(
            &bytes,
            CheckpointPrefix {
                lsn,
                generation,
                len,
            },
            shard,
            shards,
        )?;
        if stores.contains_key(&shard) {
            check_store(&mut File::open(dir.join(store_name(generation)))?, len)?;
        }
        covered[shard as usize] = lsn;
    }
    if stores.keys().any(|s| !checkpoints.contains_key(s)) {
        return Err(other("checkpoint store without its checkpoint"));
    }
    for dir in shard_dirs(engine)? {
        let shard = dir
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| checked_number(n, "shard-", "", 3))
            .unwrap_or(0) as u32;
        if !checkpoints.contains_key(&shard) {
            return Err(other("checkpoint history without its checkpoint"));
        }
    }
    if log_image {
        if covered.iter().sum::<u64>() != image.lsn
            || (!image.covered.is_empty() && image.covered != covered)
        {
            return Err(other("checkpoint image and its shard positions disagree"));
        }
    } else if image.lsn == 0 {
        if seen.len() != 1 {
            return Err(other("empty engine checkpoint contains workflow data"));
        }
    } else {
        if checkpoints.get(&0) != Some(&image.lsn) {
            return Err(other("checkpoint image has no checkpoint at its position"));
        }
        let segment = segment.ok_or_else(|| other("checkpoint has no boundary segment"))?;
        let file = File::open(engine.join(format!("shard-000/{}", segment_name(segment))))?;
        let size = file.metadata()?.len();
        check_segment(file, size, segment, image.lsn)?;
    }
    let ledger = fs::read(engine.join("leases"))?;
    super::leases::Ledger::restore(&ledger)?;
    fs::remove_file(engine.join("leases"))?;
    for dir in shard_dirs(engine)? {
        for sub in [dir.join("log"), dir.join("archive"), dir.clone()] {
            sync_dir(&sub, method)?;
        }
    }
    sync_dir(engine, method)?;
    Ok(ledger)
}

#[cfg(test)]
mod control_tests {
    use super::*;
    use std::os::unix::fs::FileExt;

    #[test]
    fn reanchoring_detects_source_damage_before_publishing_an_engine_image() {
        let dir = std::env::temp_dir().join(format!("fusee-control-image-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir(&dir).unwrap();
        let ledger = super::super::leases::Ledger::default().snapshot().unwrap();
        let first = copy_control_prefix(&dir, None, 1024, 91, &ledger, FlushMethod::Fsync).unwrap();
        let path = dir.join(first.name());
        let original = fs::read(&path).unwrap();
        let pinned = File::open(&path).unwrap();
        let handle = OpenOptions::new().write(true).open(&path).unwrap();
        let byte = original.len() - 5;
        handle.write_at(&[original[byte] ^ 1], byte as u64).unwrap();
        assert!(
            copy_control_prefix(
                &dir,
                Some((first.clone(), pinned)),
                2048,
                115,
                &ledger,
                FlushMethod::Fsync
            )
            .is_err()
        );
        assert!(!fs::read_dir(&dir).unwrap().flatten().any(|p| {
            p.file_name()
                .to_string_lossy()
                .starts_with("snapshot-00000000000000002048-")
        }));
        handle.write_at(&[original[byte]], byte as u64).unwrap();
        let second = copy_control_prefix(
            &dir,
            Some((first, File::open(path).unwrap())),
            2048,
            115,
            &ledger,
            FlushMethod::Fsync,
        )
        .unwrap();
        let restored = unpack(
            &dir.join(second.name()),
            &second,
            &dir.join("installed"),
            FlushMethod::Fsync,
        )
        .unwrap();
        assert_eq!(restored, ledger);
        assert!(shard_dirs(&dir.join("installed")).unwrap().is_empty());
        fs::remove_dir_all(dir).unwrap();
    }
}
