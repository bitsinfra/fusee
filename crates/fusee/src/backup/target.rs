//! Where backups go: immutable named objects and one atomically replaced
//! manifest.

use crate::local::{FlushMethod, flush_file, sync_dir};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Object storage for backups.
///
/// Names are relative paths of `[A-Za-z0-9._-]` segments separated by `/`.
/// The backup writes each name once, except to replace an object no
/// manifest names (a retry after a crash). Objects written before
/// [`put_manifest`](Self::put_manifest) must be durable before the manifest
/// is, and a reader sees either the old manifest or the new one, whole.
pub trait Target: Send + Sync {
    fn put(&self, name: &str, bytes: &[u8]) -> io::Result<()>;
    /// `None` if there is no such object.
    fn get(&self, name: &str) -> io::Result<Option<Vec<u8>>>;
    /// Every object name starting with `prefix`, sorted. The manifest is
    /// not an object.
    fn list(&self, prefix: &str) -> io::Result<Vec<String>>;
    /// Removing a missing object succeeds.
    fn delete(&self, name: &str) -> io::Result<()>;
    fn put_manifest(&self, bytes: &[u8]) -> io::Result<()>;
    fn get_manifest(&self) -> io::Result<Option<Vec<u8>>>;
    /// For messages: where the target is.
    fn describe(&self) -> String;
    /// Removes debris of interrupted writes. Called with no writes running.
    fn cleanup(&self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn check_name(name: &str) -> io::Result<()> {
    let ok = !name.is_empty()
        && !name.ends_with(".tmp")
        && name.split('/').all(|part| {
            !part.is_empty()
                && part != "."
                && part != ".."
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        });
    if ok {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid backup object name {name:?}"),
        ))
    }
}

const MANIFEST: &str = "manifest.json";

/// A directory as a backup target: for tests, or a mounted volume.
///
/// Each object is written to a temporary file, flushed, and renamed into
/// place. Before a manifest is installed, the directories that gained
/// objects are synced and the drive is flushed once
/// ([`FlushMethod::platform_default`]), so the objects are durable before
/// the manifest that names them.
pub struct LocalTarget {
    root: PathBuf,
    durable: bool,
    /// Directories that gained entries since the last manifest.
    touched: Mutex<BTreeSet<PathBuf>>,
}

impl LocalTarget {
    pub fn new(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref().to_owned();
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            durable: true,
            touched: Mutex::new(BTreeSet::new()),
        })
    }

    /// Skips flushes: survives a process crash, not power loss (tests).
    pub fn without_sync(mut self) -> Self {
        self.durable = false;
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, name: &str) -> io::Result<PathBuf> {
        check_name(name)?;
        if name == MANIFEST {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the manifest is not an object",
            ));
        }
        Ok(self.root.join(name))
    }

    fn write_new(&self, path: &Path, bytes: &[u8], method: FlushMethod) -> io::Result<()> {
        let dir = path.parent().unwrap_or(&self.root);
        fs::create_dir_all(dir)?;
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        {
            let mut f = File::create(&tmp)?;
            f.write_all(bytes)?;
            if self.durable {
                flush_file(&f, method)?;
            }
        }
        fs::rename(&tmp, path)?;
        self.touched
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(dir.to_owned());
        Ok(())
    }
}

impl Target for LocalTarget {
    fn put(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        // A plain flush per object: the barrier before the next manifest
        // makes them durable on drives that cache writes.
        self.write_new(&self.path(name)?, bytes, FlushMethod::Fsync)
    }

    fn get(&self, name: &str) -> io::Result<Option<Vec<u8>>> {
        match fs::read(self.path(name)?) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        fn walk(dir: &Path, rel: &str, out: &mut Vec<String>) -> io::Result<()> {
            let entries = match fs::read_dir(dir) {
                Ok(e) => e,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(e),
            };
            for entry in entries {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                let path = if rel.is_empty() {
                    name.clone()
                } else {
                    format!("{rel}/{name}")
                };
                if entry.file_type()?.is_dir() {
                    walk(&entry.path(), &path, out)?;
                } else if !name.ends_with(".tmp") && path != MANIFEST {
                    out.push(path);
                }
            }
            Ok(())
        }
        let mut out = Vec::new();
        walk(&self.root, "", &mut out)?;
        out.retain(|n| n.starts_with(prefix));
        out.sort();
        Ok(out)
    }

    fn delete(&self, name: &str) -> io::Result<()> {
        match fs::remove_file(self.path(name)?) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn put_manifest(&self, bytes: &[u8]) -> io::Result<()> {
        if self.durable {
            let dirs = std::mem::take(&mut *self.touched.lock().unwrap_or_else(|e| e.into_inner()));
            for d in dirs.iter().rev() {
                sync_dir(d, FlushMethod::Fsync)?;
            }
        }
        // The temporary manifest's flush is the drive-wide barrier: on
        // macOS `F_FULLFSYNC` also persists every object written before it.
        self.write_new(
            &self.root.join(MANIFEST),
            bytes,
            FlushMethod::platform_default(),
        )?;
        if self.durable {
            sync_dir(&self.root, FlushMethod::platform_default())?;
        }
        Ok(())
    }

    fn get_manifest(&self) -> io::Result<Option<Vec<u8>>> {
        match fs::read(self.root.join(MANIFEST)) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn describe(&self) -> String {
        format!("file://{}", self.root.display())
    }

    fn cleanup(&self) -> io::Result<()> {
        fn walk(dir: &Path) -> io::Result<()> {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    walk(&entry.path())?;
                } else if entry.file_name().to_string_lossy().ends_with(".tmp") {
                    fs::remove_file(entry.path())?;
                }
            }
            Ok(())
        }
        walk(&self.root)
    }
}

/// Opens a target from a command-line description: a directory path (or
/// `file:///path`), or with the `s3` feature `s3://bucket/prefix` (see
/// [`crate::backup::s3::S3Target::from_url`]).
pub fn open_target(spec: &str) -> io::Result<Arc<dyn Target>> {
    if let Some(path) = spec.strip_prefix("file://") {
        return Ok(Arc::new(LocalTarget::new(path)?));
    }
    if spec.starts_with("s3://") {
        #[cfg(feature = "s3")]
        {
            return Ok(Arc::new(super::s3::S3Target::from_url(spec)?));
        }
        #[cfg(not(feature = "s3"))]
        {
            return Err(io::Error::other(
                "this build has no S3 support (cargo feature `s3` of the fusee crate)",
            ));
        }
    }
    if spec.contains("://") {
        return Err(io::Error::other(format!(
            "unknown backup target {spec:?}: use a directory, file:///path or s3://bucket/prefix"
        )));
    }
    Ok(Arc::new(LocalTarget::new(spec)?))
}
