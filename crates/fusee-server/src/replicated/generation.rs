//! A durable generation pointer couples the protocol log, its engine baseline,
//! and the applied/committed watermarks. A segment group's directory has no pointer.
use super::{
    FlushMethod, Superblock, VsrLog, annotate, flush_file, other, snapshot::Image, sync_dir,
};
use fusee_core::vsr::Entry;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Generation {
    pub image: Image,
    #[serde(default)]
    pub sequence: u64,
    pub engine: String,
    pub ledger_sha256: [u8; 32],
}

impl Generation {
    pub fn new(image: Image, engine: String, ledger: &[u8], sequence: u64) -> Self {
        Self {
            image,
            sequence,
            engine,
            ledger_sha256: Sha256::digest(ledger).into(),
        }
    }

    pub fn ledger(&self, root: &Path) -> io::Result<Vec<u8>> {
        let bytes = fs::read(self.log_dir(root).join("leases-base"))?;
        if <[u8; 32]>::from(Sha256::digest(&bytes)) != self.ledger_sha256 {
            return Err(other("damaged checkpoint lease table"));
        }
        Ok(bytes)
    }

    pub fn log_dir(&self, root: &Path) -> PathBuf {
        let name = if self.sequence == 0 {
            self.image.name()
        } else {
            Self::slot(&self.image, self.sequence)
        };
        root.join(format!("{name}.log"))
    }

    pub fn slot(image: &Image, sequence: u64) -> String {
        format!("{}.g{sequence:020}", image.name())
    }

    pub fn read(root: &Path) -> io::Result<Option<Self>> {
        let bytes = match fs::read(root.join("current")) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if bytes.len() < 12
            || &bytes[..8] != b"FSCUR001"
            || bytes[8..12] != crc32c::crc32c(&bytes[12..]).to_le_bytes()
        {
            return Err(other("damaged replica generation pointer"));
        }
        let g: Self = serde_json::from_slice(&bytes[12..])?;
        // No absolute paths or parent components can escape the group.
        if g.engine != "engine" && !g.engine.strip_prefix("engine-").is_some_and(slot_name) {
            return Err(other("invalid generated engine directory"));
        }
        if g.image.op == 0 || g.image.op == u64::MAX {
            return Err(other("invalid checkpoint position"));
        }
        Ok(Some(g))
    }

    /// Do not change the old pointer until the image, suffix, lease state
    /// and superblock of the new generation can all survive a crash. (The
    /// replica stages and commits separately; tests publish in one go.)
    #[cfg(test)]
    pub fn publish<'a>(
        &self,
        root: &Path,
        method: FlushMethod,
        tail: impl Iterator<Item = &'a Entry>,
        sb: &Superblock,
        ledger: &[u8],
    ) -> io::Result<VsrLog> {
        let staged = self.stage(root, method, tail, sb, ledger)?;
        self.commit(root, method, staged, &[])
    }

    /// The bulk of [`publish`](Self::publish), which needs nothing of the
    /// replica but what is passed in, so it runs off the I/O thread: the new
    /// generation's log (`tail`), superblock and lease state, durable in the
    /// generation's own directory. Nothing names that directory until
    /// [`commit`](Self::commit) switches the pointer; after a crash before
    /// then, [`reclaim`] removes it like any unreferenced generation.
    pub fn stage<'a>(
        &self,
        root: &Path,
        method: FlushMethod,
        tail: impl Iterator<Item = &'a Entry>,
        sb: &Superblock,
        ledger: &[u8],
    ) -> io::Result<VsrLog> {
        if sb.applied < self.image.op || sb.applied > sb.commit {
            return Err(other("invalid checkpoint watermarks"));
        }
        let target = self.log_dir(root);
        // Only an unreferenced installation attempt can have this same newer
        // base. A caller always requires an anchor strictly after current.
        remove_unpublished(&target).map_err(|e| annotate(e, "removing", &target))?;
        fs::create_dir(&target).map_err(|e| annotate(e, "creating", &target))?;
        let mut log =
            VsrLog::empty(&target, method).map_err(|e| annotate(e, "creating log in", &target))?;
        log.replace(tail)?;
        log.save(sb)?;
        write_file(&target.join("leases-base"), ledger, method)?;
        sync_dir(&target, method)?;
        sync_dir(root, method)?;
        Ok(log)
    }

    /// The rest of [`publish`](Self::publish), on the I/O thread: `extra`
    /// (entries after the staged tail, appended since it was taken) durable
    /// in the staged log, then the pointer switched. The staged superblock
    /// stands: the caller checked the view is unchanged, and a higher
    /// commit or applied position since is what the commit hint records,
    /// as after any publication.
    pub fn commit(
        &self,
        root: &Path,
        method: FlushMethod,
        mut log: VsrLog,
        extra: &[Entry],
    ) -> io::Result<VsrLog> {
        if !extra.is_empty() {
            log.append(extra)?;
            log.sync()?;
        }
        let payload = serde_json::to_vec(self)?;
        let mut bytes = b"FSCUR001".to_vec();
        bytes.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
        bytes.extend_from_slice(&payload);
        let tmp = root.join("current.tmp");
        write_file(&tmp, &bytes, method).map_err(|e| annotate(e, "writing", &tmp))?;
        fs::rename(&tmp, root.join("current")).map_err(|e| annotate(e, "renaming", &tmp))?;
        sync_dir(root, method)?;
        Ok(log)
    }
}

/// Where [`discard`] moves what it deletes: beside the replica's root, on
/// the same filesystem.
fn trash_dir(root: &Path) -> io::Result<PathBuf> {
    Ok(root
        .parent()
        .ok_or_else(|| other("missing replica root"))?
        .join("trash"))
}

/// Deletes `path` (a file or directory nothing refers to any more) without
/// waiting: it is renamed into the trash at once and removed by a thread of
/// its own. Unlinking a generation's files took milliseconds on the replica's
/// I/O thread. Nothing needs the deletion durable: a crash before it leaves
/// the files in the trash (or, if the rename was lost, unreferenced where
/// they were), and the next [`reclaim`] removes them.
pub(super) fn discard(path: &Path, root: &Path) -> io::Result<()> {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let trash = trash_dir(root)?;
    fs::create_dir_all(&trash)?;
    let name = format!(
        "{}-{}",
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    match fs::rename(path, trash.join(&name)).map_err(|e| annotate(e, "moving to the trash", path))
    {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        r => r?,
    }
    empty_trash(trash);
    Ok(())
}

/// Removes everything in `trash` on a thread of its own.
fn empty_trash(trash: PathBuf) {
    let _ = std::thread::Builder::new()
        .name("fusee-trash".into())
        .spawn(move || {
            let Ok(entries) = fs::read_dir(&trash) else {
                return;
            };
            for e in entries.flatten() {
                let path = e.path();
                let _ = match e.file_type() {
                    Ok(t) if t.is_dir() => fs::remove_dir_all(&path),
                    _ => fs::remove_file(&path),
                };
            }
        });
}

pub(super) fn remove_unpublished(path: &Path) -> io::Result<()> {
    match fs::remove_dir_all(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

fn write_file(path: &Path, bytes: &[u8], method: FlushMethod) -> io::Result<()> {
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    flush_file(&file, method)
}

/// Once a checkpoint is active the protocol prefix it replaced is removed,
/// and the superblock path holds a marker that an executable without
/// checkpoint support refuses to open.
pub(super) fn retire_prefix_log(root: &Path, method: FlushMethod) -> io::Result<()> {
    let marker = b"Fusee VSR checkpoint format 1; open with a checkpoint-capable executable.";
    if fs::read(root.join("superblock")).ok().as_deref() != Some(marker.as_slice()) {
        let tmp = root.join("retired.tmp");
        write_file(&tmp, marker, method)?;
        fs::rename(tmp, root.join("superblock"))?;
        sync_dir(root, method)?;
    }
    match fs::remove_file(root.join("log")) {
        Ok(()) => sync_dir(root, method),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn image_filename(name: &str) -> bool {
    name.strip_prefix("snapshot-").is_some_and(|rest| {
        let bytes = rest.as_bytes();
        bytes.len() == 85
            && bytes[..20].iter().all(u8::is_ascii_digit)
            && bytes[20] == b'-'
            && bytes[21..].iter().all(u8::is_ascii_hexdigit)
    })
}

fn slot_name(s: &str) -> bool {
    image_filename(s)
        || s.rsplit_once(".g").is_some_and(|(name, serial)| {
            image_filename(name) && serial.len() == 20 && serial.bytes().all(|b| b.is_ascii_digit())
        })
}

/// Reclaim obsolete generations, including an interrupted installation. An
/// old engine may still own a shard lock while shutting down after losing its
/// view; keep its directory until it releases that lock.
pub(super) fn reclaim(
    root: &Path,
    current: Option<&Generation>,
    method: FlushMethod,
) -> io::Result<bool> {
    let keep = current.map(|g| g.image.name());
    let keep_log = current.map(|g| g.log_dir(root));
    let engine = current.map_or("engine", |g| g.engine.as_str());
    let parent = root.parent().ok_or_else(|| other("missing replica root"))?;
    let _ = method;
    let mut retry = false;
    // What a crash left in the trash.
    if let Ok(trash) = trash_dir(root)
        && trash.is_dir()
    {
        empty_trash(trash);
    }
    for e in fs::read_dir(root)? {
        let entry = e?;
        let os = entry.file_name();
        let Some(name) = os.to_str() else { continue };
        if let Some(id) = name.strip_suffix(".log") {
            if slot_name(id) && keep_log.as_ref() != Some(&entry.path()) {
                discard(&entry.path(), root)?;
            }
        } else if image_filename(name) && keep.as_deref() != Some(name) {
            discard(&entry.path(), root)?;
        }
    }
    for e in fs::read_dir(parent)? {
        let entry = e?;
        let os = entry.file_name();
        let Some(name) = os.to_str() else { continue };
        if name == engine
            || (name != "engine" && !name.strip_prefix("engine-").is_some_and(slot_name))
        {
            continue;
        }
        let dir = entry.path();
        let shards = super::snapshot::shard_dirs(&dir)?;
        if shards.is_empty() {
            if name != "engine" {
                discard(&dir, root)?;
            }
            continue;
        }
        let mut locks = Vec::new();
        let mut busy = false;
        for shard in &shards {
            match fusee::local::lock_directory(shard) {
                Ok(file) => locks.push(file),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => busy = true,
                Err(e) => return Err(e),
            }
        }
        if busy {
            retry = true;
            continue;
        }
        // The engine root remains the process-wide ownership lock.
        if name == "engine" {
            for shard in &shards {
                discard(shard, root)?;
            }
        } else {
            discard(&dir, root)?;
        }
    }
    Ok(retry)
}

#[cfg(test)]
mod crash_tests {
    use super::super::snapshot;
    use super::*;
    use fusee_core::vsr::Entry;
    use std::sync::Arc;

    #[test]
    fn incomplete_generation_keeps_the_old_committed_suffix_through_retry() {
        let dir =
            std::env::temp_dir().join(format!("fusee-generation-crash-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir(&dir).unwrap();
        let method = FlushMethod::Fsync;
        let mut entries = Vec::new();
        let mut parent = 0;
        for n in 1..=3 {
            let entry = Entry::new(n, 1, parent, Arc::from([n as u8]));
            parent = entry.checksum;
            entries.push(entry);
        }
        let ledger = super::super::leases::Ledger::default().snapshot().unwrap();
        let make = |n: usize| {
            let bytes = vec![n as u8; 128];
            let image = Image {
                op: n as u64,
                lsn: n as u64,
                checksum: entries[n - 1].checksum,
                len: 128,
                sha256: Sha256::digest(&bytes).into(),
                shards: 0,
                covered: Vec::new(),
            };
            write_file(&dir.join(image.name()), &bytes, method).unwrap();
            Generation::new(image, "engine".into(), &ledger, n as u64)
        };
        let first = make(1);
        let next = make(2);
        let state = |applied| Superblock {
            view: 1,
            last_normal: 1,
            commit: 3,
            applied,
        };
        first
            .publish(&dir, method, entries[1..].iter(), &state(1), &ledger)
            .unwrap();
        let previous = fs::read(dir.join("current")).unwrap();
        // Fail between persisting the new directory and publishing its pointer.
        fs::create_dir(dir.join("current.tmp")).unwrap();
        assert!(
            next.publish(&dir, method, entries[2..].iter(), &state(2), &ledger)
                .is_err()
        );
        let check = |wanted: &Generation| {
            let actual = Generation::read(&dir).unwrap().unwrap();
            assert_eq!(actual.image, wanted.image);
            snapshot::verify(&dir.join(actual.image.name()), &actual.image).unwrap();
            assert_eq!(actual.ledger(&dir).unwrap(), ledger);
            let (_, suffix, damage) = VsrLog::open(
                &actual.log_dir(&dir),
                method,
                3,
                actual.image.op,
                actual.image.checksum,
            )
            .unwrap();
            assert!(damage.is_none());
            assert_eq!(suffix, &entries[actual.image.op as usize..]);
        };
        check(&first);
        fs::remove_dir(dir.join("current.tmp")).unwrap();
        next.publish(&dir, method, entries[2..].iter(), &state(2), &ledger)
            .unwrap();
        check(&next);
        // Before directory sync either pointer could be observed on reopen.
        let latest = fs::read(dir.join("current")).unwrap();
        write_file(&dir.join("current"), &previous, method).unwrap();
        check(&first);
        write_file(&dir.join("current"), &latest, method).unwrap();
        check(&next);
        fs::remove_dir_all(dir).unwrap();
    }
}
