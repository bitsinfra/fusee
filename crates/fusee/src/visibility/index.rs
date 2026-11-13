//! A disposable projection of a checkpoint's run-store prefix. A listing
//! reconciles these candidates with its loaded runs and newer store frames.
use super::{ListFilter, Op};
use crate::runtime::merge_attributes;
use fusee_core::record::{Record, decode_canonical_all};
use fusee_core::store::StoreEntry;
use redb::{Database, ReadableTable, TableDefinition};
use serde_json::{Map, Value};
use std::fs::File;
use std::io::{self, Read};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};

const FIELDS: TableDefinition<&[u8], ()> = TableDefinition::new("by-field-and-run");
const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("keys-by-run");
const META: TableDefinition<u8, u64> = TableDefinition::new("indexed-prefix");

#[derive(Clone)]
pub(crate) struct Source {
    pub file: Arc<File>,
    pub generation: u64,
    pub length: u64,
}

/// The indexed part of the store. A query considers live runs and frames
/// beyond this length alongside the matching IDs from the projection.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Candidates {
    pub generation: u64,
    pub length: u64,
    pub runs: Vec<u64>,
    pub truncated: bool,
    /// `runs` are the highest-numbered (newest) matches, highest first, so
    /// a truncated window still holds the newest. False from a server that
    /// predates it, which a caller treats as an unordered window.
    #[serde(default)]
    pub newest: bool,
}

/// A `Delta` frame's whole journal: it and the frames it extends.
fn chained(src: &Source, entry: StoreEntry, frame_len: u32) -> io::Result<Vec<u8>> {
    let mut read = |at: fusee_core::shard::BlobRef| {
        let mut frame = vec![0u8; at.len as usize];
        if at.offset + u64::from(at.len) > src.length {
            return Err(fusee_core::wire::DecodeError(
                "delta chain frame beyond the prefix",
            ));
        }
        src.file
            .read_exact_at(&mut frame, at.offset)
            .map_err(|_| fusee_core::wire::DecodeError("unreadable delta chain frame"))?;
        fusee_core::store::read_entry(&frame)
    };
    fusee_core::store::chain_journal(entry, frame_len, &mut read)
        .map(|(journal, _, _)| journal)
        .map_err(io::Error::other)
}

fn fields(src: &Source, run: StoreEntry, frame_len: u32) -> io::Result<Map<String, Value>> {
    let (journal, status, finished) = match run {
        StoreEntry::Park { journal, .. } => (journal, "parked", None),
        e @ StoreEntry::Delta { .. } => (chained(src, e, frame_len)?, "parked", None),
        StoreEntry::Done {
            journal,
            status,
            finished_ms,
            ..
        } => (journal, status.as_str(), Some(finished_ms)),
        StoreEntry::Tombstone { .. } | StoreEntry::Index { .. } => return Ok(Map::new()),
    };
    let records = decode_canonical_all(&journal).map_err(io::Error::other)?;
    // A run that finished after it was loaded is a `Delta` chain ending
    // with its `Done`.
    let (status, finished) = match (finished, records.last()) {
        (
            None,
            Some(Record::Done {
                status,
                finished_ms,
                ..
            }),
        ) => (status.as_str(), Some(*finished_ms)),
        _ => (status, finished),
    };
    let Some(Record::Start {
        id,
        wtype,
        started_ms,
        ..
    }) = records.first()
    else {
        return Err(io::Error::other("indexed journal has no start"));
    };
    let attrs = records
        .iter()
        .filter_map(|r| {
            if let Record::Attributes { payload, .. } = r {
                Some(payload.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let mut out: Map<String, Value> = serde_json::from_str(&merge_attributes(&attrs))?;
    out.insert("id".into(), id.as_ref().into());
    out.insert("type".into(), wtype.as_ref().into());
    out.insert("status".into(), status.into());
    out.insert("startedMs".into(), (*started_ms).into());
    if let Some(ms) = finished {
        out.insert("finishedMs".into(), ms.into());
    } else {
        out.remove("finishedMs");
    }
    Ok(out)
}

fn key(field: &str, value: &Value) -> (Vec<u8>, Vec<u8>) {
    let mut prefix = (field.len() as u64).to_be_bytes().to_vec();
    prefix.extend_from_slice(field.as_bytes());
    let (tag, bytes) = match value {
        Value::Null => (0, Vec::new()),
        Value::Bool(b) => (1, vec![u8::from(*b)]),
        Value::Number(n) => {
            let f = n.as_f64().unwrap_or(0.0);
            let bits = if f == 0.0 { 0 } else { f.to_bits() };
            let ordered = if bits >> 63 == 1 {
                !bits
            } else {
                bits ^ (1 << 63)
            };
            (2, ordered.to_be_bytes().to_vec())
        }
        Value::String(s) => (3, s.as_bytes().to_vec()),
        _ => (4, value.to_string().into_bytes()),
    };
    prefix.push(tag);
    let mut full = prefix.clone();
    for b in bytes {
        if b == 0 {
            full.extend_from_slice(&[0, 255]);
        } else {
            full.push(b);
        }
    }
    full.extend_from_slice(&[0, 0]);
    (prefix, full)
}

fn successor(mut bytes: Vec<u8>) -> Vec<u8> {
    while let Some(last) = bytes.pop() {
        if last < 255 {
            bytes.push(last + 1);
            break;
        }
    }
    bytes
}

/// Whether [`range`] selects the rows of one value (an equality), whose
/// keys then order by run number alone.
fn range_is_one_value(filter: &ListFilter) -> bool {
    filter.conds.iter().any(|c| c.op == Op::Eq)
}

fn range(filter: &ListFilter) -> Option<(Vec<u8>, Vec<u8>)> {
    let cond = filter
        .conds
        .iter()
        .find(|c| c.op == Op::Eq)
        .or_else(|| filter.conds.iter().find(|c| c.op != Op::Ne))?;
    let field = match cond.field.as_str() {
        "workflowType" => "type",
        "workflowId" => "id",
        s => s,
    };
    let (prefix, exact) = key(field, &cond.value);
    match cond.op {
        Op::Eq => Some((exact.clone(), successor(exact))),
        Op::Gt => Some((successor(exact), successor(prefix))),
        Op::Gte => Some((exact, successor(prefix))),
        Op::Lt => Some((prefix, exact)),
        Op::Lte => Some((prefix, successor(exact))),
        Op::Ne => None,
    }
}

struct StoreBytes<'a> {
    source: &'a Source,
    at: u64,
}
impl Read for StoreBytes<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let len = (self.source.length - self.at).min(out.len() as u64) as usize;
        let n = self.source.file.read_at(&mut out[..len], self.at)?;
        self.at += n as u64;
        Ok(n)
    }
}

/// Runs whose index rows one write transaction applies. A refresh of
/// millions of parked runs in one transaction would hold all its dirty
/// pages in memory until the commit (hundreds of megabytes); in batches, an
/// interrupted refresh resumes from the last committed batch.
const REFRESH_BATCH: usize = 20_000;

/// One run's change: its new keys, or `None` for a tombstone.
type Change = (u64, Option<Vec<Vec<u8>>>);

fn refresh(db: &Database, src: &Source) -> io::Result<()> {
    let mut from = 0;
    {
        let tx = db.begin_write().map_err(io::Error::other)?;
        {
            let meta = tx.open_table(META).map_err(io::Error::other)?;
            if meta.get(0).map_err(io::Error::other)?.map(|v| v.value()) == Some(src.generation) {
                from = meta
                    .get(1)
                    .map_err(io::Error::other)?
                    .map_or(0, |v| v.value());
            }
        }
        if from == 0 || from > src.length {
            tx.delete_table(ROWS).map_err(io::Error::other)?;
            tx.delete_table(FIELDS).map_err(io::Error::other)?;
            from = 0;
            let mut meta = tx.open_table(META).map_err(io::Error::other)?;
            meta.insert(0, src.generation).map_err(io::Error::other)?;
            meta.insert(1, 0).map_err(io::Error::other)?;
        }
        tx.commit().map_err(io::Error::other)?;
    }
    let mut pending: Vec<Change> = Vec::new();
    let mut upto = from;
    let mut input = io::BufReader::with_capacity(
        1 << 20,
        StoreBytes {
            source: src,
            at: from,
        },
    );
    crate::env::scan_store_reader(&mut input, from, src.length, &mut |offset, len, entry| {
        upto = offset + u64::from(len);
        if !matches!(entry, StoreEntry::Index { .. }) {
            let run = entry.run();
            if matches!(entry, StoreEntry::Tombstone { .. }) {
                pending.push((run, None));
            } else {
                let mut keys = Vec::new();
                for (field, value) in fields(src, entry, len)? {
                    let (_, mut k) = key(&field, &value);
                    k.extend_from_slice(&run.to_be_bytes());
                    keys.push(k);
                }
                pending.push((run, Some(keys)));
            }
        }
        if pending.len() >= REFRESH_BATCH {
            apply(db, src.generation, upto, &mut pending)?;
        }
        Ok(())
    })?;
    apply(db, src.generation, src.length, &mut pending)
}

/// Applies `pending` in one transaction and records the store offset the
/// index now covers.
fn apply(db: &Database, generation: u64, upto: u64, pending: &mut Vec<Change>) -> io::Result<()> {
    let tx = db.begin_write().map_err(io::Error::other)?;
    {
        let mut rows = tx.open_table(ROWS).map_err(io::Error::other)?;
        let mut terms = tx.open_table(FIELDS).map_err(io::Error::other)?;
        for (run, keys) in pending.drain(..) {
            if let Some(old) = rows.remove(run).map_err(io::Error::other)? {
                let old: Vec<Vec<u8>> = serde_json::from_slice(old.value())?;
                for k in &old {
                    terms.remove(k.as_slice()).map_err(io::Error::other)?;
                }
            }
            let Some(keys) = keys else {
                continue;
            };
            for k in &keys {
                terms.insert(k.as_slice(), ()).map_err(io::Error::other)?;
            }
            rows.insert(run, serde_json::to_vec(&keys)?.as_slice())
                .map_err(io::Error::other)?;
        }
        let mut meta = tx.open_table(META).map_err(io::Error::other)?;
        meta.insert(0, generation).map_err(io::Error::other)?;
        meta.insert(1, upto).map_err(io::Error::other)?;
    }
    tx.commit().map_err(io::Error::other)
}

#[derive(Default)]
struct Work {
    latest: Option<Source>,
    stop: bool,
    failed: bool,
    db: Option<Arc<Database>>,
    scheduled: Option<(u64, u64)>,
}
pub(crate) struct Index {
    state: Arc<(Mutex<Work>, Condvar)>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl Index {
    pub fn start(path: PathBuf) -> io::Result<Self> {
        let state = Arc::new((Mutex::new(Work::default()), Condvar::new()));
        let thread_state = state.clone();
        let worker = std::thread::Builder::new()
            .name("fusee-visibility".into())
            .spawn(move || {
                // The index is derived and may lag: under load (a mass wake
                // refreshes millions of entries) the shards come first.
                #[cfg(target_os = "linux")]
                // SAFETY: changes only this thread's scheduling priority.
                unsafe {
                    libc::setpriority(libc::PRIO_PROCESS as _, libc::gettid() as libc::id_t, 10);
                }
                let (lock, cv) = &*thread_state;
                let work = || -> io::Result<()> {
                    // The index is derived. Each recovery starts from the selected
                    // checkpoint, even after copying or restoring a data directory.
                    match std::fs::remove_file(&path) {
                        Ok(()) => (),
                        Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                        Err(e) => return Err(e),
                    }
                    let db = Arc::new(
                        Database::builder()
                            .set_cache_size(2 << 20)
                            .create(&path)
                            .map_err(io::Error::other)?,
                    );
                    loop {
                        let mut st = lock.lock().unwrap_or_else(|e| e.into_inner());
                        while st.latest.is_none() && !st.stop {
                            st = cv.wait(st).unwrap_or_else(|e| e.into_inner());
                        }
                        if st.stop {
                            return Ok(());
                        }
                        let src = st.latest.take().unwrap();
                        drop(st);
                        refresh(&db, &src)?;
                        lock.lock().unwrap_or_else(|e| e.into_inner()).db = Some(db.clone());
                    }
                };
                if let Err(e) = work() {
                    let mut st = lock.lock().unwrap_or_else(|e| e.into_inner());
                    st.failed = true;
                    st.db = None;
                    crate::log::warn(format!(
                        "the visibility index is unavailable ({e}); listing filters scan runs instead"
                    ));
                }
            })?;
        Ok(Self {
            state,
            worker: Some(worker),
        })
    }
    pub fn schedule(&self, src: Source) {
        let (lock, cv) = &*self.state;
        let mut st = lock.lock().unwrap_or_else(|e| e.into_inner());
        if !st.failed && st.scheduled != Some((src.generation, src.length)) {
            st.scheduled = Some((src.generation, src.length));
            st.latest = Some(src);
            cv.notify_one();
        }
    }
    pub fn candidates(&self, filter: &ListFilter, limit: usize) -> Option<Candidates> {
        let (low, high) = range(filter)?;
        let db = self.state.0.lock().ok()?.db.clone()?;
        let tx = db.begin_read().ok()?;
        let meta = tx.open_table(META).ok()?;
        let generation = meta.get(0).ok()??.value();
        let length = meta.get(1).ok()??.value();
        let table = tx.open_table(FIELDS).ok()?;
        let mut runs = Vec::new();
        let mut truncated = false;
        // Keys end with the run number, big-endian: under one value (an
        // equality), backwards is newest first, so a full window holds the
        // newest matches. A range orders by value instead.
        let newest = range_is_one_value(filter);
        let items = table.range::<&[u8]>(low.as_slice()..high.as_slice()).ok()?;
        let items: Box<dyn Iterator<Item = _>> = if newest {
            Box::new(items.rev())
        } else {
            Box::new(items)
        };
        for item in items {
            if runs.len() >= limit {
                truncated = true;
                break;
            }
            let (k, _) = item.ok()?;
            let key = k.value();
            runs.push(u64::from_be_bytes(
                key.get(key.len().checked_sub(8)?..)?.try_into().ok()?,
            ));
        }
        Some(Candidates {
            generation,
            length,
            runs,
            truncated,
            newest,
        })
    }
}
impl Drop for Index {
    fn drop(&mut self) {
        let (lock, cv) = &*self.state;
        lock.lock().unwrap_or_else(|e| e.into_inner()).stop = true;
        cv.notify_one();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fusee_core::record::{RecordBuf, Status, encode_canonical};
    use fusee_core::store::{encode_entry, store_header};
    use std::io::Write;
    use std::time::{Duration, Instant};

    fn terminal(run: u64, score: i32) -> Vec<u8> {
        let mut journal = Vec::new();
        let records: Vec<RecordBuf> = vec![
            Record::Start {
                run,
                id: format!("id-{run}").into(),
                wtype: "job".into(),
                version: 1,
                codec: 0,
                started_ms: run,
                deadline_ms: 0,
                parent: None,
                policy: 0,
                from: None,
                copies: 0,
                update: None,
                input: Arc::from(&b"null"[..]),
                meta: None,
            },
            Record::Attributes {
                run,
                seq: 1,
                payload: Arc::from(format!(r#"{{"score":{score}}}"#).as_bytes()),
            },
            Record::Done {
                run,
                status: Status::Ok,
                finished_ms: run + 1,
                payload: Arc::from(&b"42"[..]),
                by: None,
            },
        ];
        for rec in records {
            encode_canonical(&rec, &mut journal);
        }
        encode_entry(&StoreEntry::Done {
            run,
            last_lsn: run + 2,
            id_hash: 0,
            status: Status::Ok,
            finished_ms: run + 1,
            journal,
        })
    }
    #[test]
    fn refreshed_ranges_follow_replacements_tombstones_and_generations() {
        let dir = std::env::temp_dir().join(format!(
            "fusee-index-test-{}-{}",
            std::process::id(),
            crate::local::unix_ms()
        ));
        std::fs::create_dir(&dir).unwrap();
        let mut store = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.join("store"))
            .unwrap();
        store.write_all(&store_header()).unwrap();
        store.write_all(&terminal(1, -5)).unwrap();
        store.write_all(&terminal(2, 10)).unwrap();
        let index = Index::start(dir.join("visibility.redb")).unwrap();
        let filter = ListFilter::parse(&serde_json::json!({"score":{"gte":0}})).unwrap();
        let check = |generation, store: &File, expected: Vec<u64>| {
            let length = store.metadata().unwrap().len();
            index.schedule(Source {
                file: Arc::new(store.try_clone().unwrap()),
                generation,
                length,
            });
            let start = Instant::now();
            loop {
                if let Some(c) = index.candidates(&filter, 10)
                    && (c.generation, c.length) == (generation, length)
                {
                    assert_eq!(c.runs, expected);
                    break;
                }
                assert!(start.elapsed() < Duration::from_secs(5));
                std::thread::sleep(Duration::from_millis(5));
            }
        };
        check(1, &store, vec![2]);
        store.write_all(&terminal(1, 3)).unwrap();
        store
            .write_all(&encode_entry(&StoreEntry::Tombstone { run: 2 }))
            .unwrap();
        check(1, &store, vec![1]);
        let mut new = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.join("next"))
            .unwrap();
        new.write_all(&store_header()).unwrap();
        new.write_all(&terminal(3, 12)).unwrap();
        check(2, &new, vec![3]);
        drop(index);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_delta_frame_is_indexed_from_its_whole_chain() {
        let dir = std::env::temp_dir().join(format!(
            "fusee-index-delta-{}-{}",
            std::process::id(),
            crate::local::unix_ms()
        ));
        std::fs::create_dir(&dir).unwrap();
        let mut file = store_header();
        let start: RecordBuf = Record::Start {
            run: 7,
            id: "d-7".into(),
            wtype: "job".into(),
            version: 1,
            codec: 0,
            started_ms: 5,
            deadline_ms: 0,
            parent: None,
            policy: 0,
            from: None,
            copies: 0,
            update: None,
            input: Arc::from(&b"null"[..]),
            meta: None,
        };
        let mut journal = Vec::new();
        encode_canonical(&start, &mut journal);
        let base = encode_entry(&StoreEntry::Park {
            run: 7,
            last_lsn: 1,
            id_hash: 0,
            timers: Vec::new(),
            journal,
        });
        let prev = fusee_core::shard::BlobRef {
            offset: file.len() as u64,
            len: base.len() as u32,
        };
        file.extend_from_slice(&base);
        // The persisted run index sits in the same store; it is skipped.
        file.extend_from_slice(&encode_entry(&StoreEntry::Index {
            records: vec![0; 32],
        }));
        let attrs: RecordBuf = Record::Attributes {
            run: 7,
            seq: 1,
            payload: Arc::from(&b"{\"score\":9}"[..]),
        };
        let mut journal = Vec::new();
        encode_canonical(&attrs, &mut journal);
        file.extend_from_slice(&encode_entry(&StoreEntry::Delta {
            run: 7,
            last_lsn: 2,
            id_hash: 0,
            timers: Vec::new(),
            prev,
            depth: 1,
            chain: u64::from(prev.len),
            journal,
        }));
        std::fs::write(dir.join("store"), &file).unwrap();
        let index = Index::start(dir.join("visibility.redb")).unwrap();
        index.schedule(Source {
            file: Arc::new(File::open(dir.join("store")).unwrap()),
            generation: 1,
            length: file.len() as u64,
        });
        let start = Instant::now();
        // Both the start (in the base frame) and the attribute (in the
        // delta) find the run.
        for filter in [
            serde_json::json!({"score":{"gte":0}}),
            serde_json::json!({"workflowId":"d-7"}),
        ] {
            let filter = ListFilter::parse(&filter).unwrap();
            loop {
                if let Some(c) = index.candidates(&filter, 10)
                    && c.length == file.len() as u64
                {
                    assert_eq!(c.runs, vec![7]);
                    break;
                }
                assert!(start.elapsed() < Duration::from_secs(5));
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        drop(index);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
