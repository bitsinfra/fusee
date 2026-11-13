//! Changes the shard count of a closed local data directory, offline
//! (`fusee reshard <dir> <shards>`; design and invariants in
//! `docs/operations.md`, "Changing the shard count").
//!
//! **What moves.** Every run the old shards know (running, suspended,
//! finished within retention) goes to the shard its workflow ID hashes to
//! under the new count, with its whole journal: its timers, buffered
//! signals, accepted updates and their results, search attributes and
//! pending async completions are all journal records, so they move with it.
//! A run that was in memory at the old shard's last checkpoint is in memory
//! in the new shard's checkpoint and its code resumes at the next open; a
//! parked run stays parked, with its timers. Messages between shards that
//! were not acknowledged (child starts and results, signals, cancels) move
//! to the new shards' outboxes and are sent again; their receivers drop
//! repeats as they always do. History archives are rewritten into the new
//! shards.
//!
//! **Reading.** Each old shard is opened with the engine's own recovery
//! (checkpoint, run store, log tail, outputs the tail regenerates) and no
//! workflow code, and is never ticked: no timer fires, no code runs, no
//! record is appended and nothing expires.
//!
//! **Run numbers.** Run numbers count per shard, so runs that come from
//! different old shards may share one. Within each new shard a run keeps
//! its number unless another run took it first (unfinished runs first);
//! the others get numbers above every number any old shard had used, in an
//! order that keeps the newest run of each workflow ID the highest. Growing
//! to a multiple of the old count renumbers nothing (each new shard draws
//! from one old shard). Every reference to a run is rewritten with it:
//! children's parent references, a run's predecessor, the next run named
//! by a continue-as-new, and the parent references in messages. A
//! reference to a run that no longer exists (its retention ended) becomes
//! one that matches no run. A renumbered unfinished run's `Ctx::key` values
//! and completion tokens change; [`ReshardReport::renumbered_unfinished`]
//! lists them with the tokens their pending completions now answer to.
//!
//! **Crash safety.** The new shards are built under `<dir>/reshard.new`
//! and synced. The switch is `fusee.meta`: it is replaced (atomically) by
//! one with `format=reshard` and the new count, which every build refuses
//! to open. Then the old `wal` and shard directories move to
//! `<dir>/reshard.old`, the new shard directories move in, and a final
//! `fusee.meta` names the new count; only then is `reshard.old` deleted. A
//! crash before the switch leaves the old directory, which opens as it
//! was (the next attempt deletes `reshard.new`); a crash after it leaves a
//! directory that [`reshard`] finishes (every step can be repeated); a
//! crash after the final meta file leaves only leftovers the next attempt
//! deletes. The new shards start a new shared log.

use crate::engine::{Config, META, read_meta, runtime_config, write_meta};
use crate::env::Env;
use crate::error::{Error, Result};
use crate::local::{FlushMethod, LocalOptions, flush_file, sync_dir};
use crate::runtime::{
    Message, Outgoing, Registry, Services, ShardRuntime, encode_outbox, format_token,
};
use crate::wal::{SharedWal, WalEnv, WalOptions};
use fusee_core::record::{
    Lsn, ParentRef, Record, RecordBuf, RunId, Seq, Status, decode_canonical_all, encode_canonical,
};
use fusee_core::shard::{BlobRef, TimerKind, shard_of};
use fusee_core::store::{
    Checkpoint, CheckpointRun, StoreEntry, chain_journal, encode_entry, read_entry, store_header,
};
use fusee_core::wire::{DecodeError, Reader, hash_id};
use rustc_hash::{FxHashMap, FxHashSet};
use std::fs::{self, File};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// The `format=` value of a `fusee.meta` written while the directory's
/// files are being switched to a new shard count. No build opens it.
pub const SWITCHING: &str = "reshard";
/// Where the new shards are built.
const NEW: &str = "reshard.new";
/// Where the old shards wait for deletion during the switch.
const OLD: &str = "reshard.old";
/// Run store generation of the new shards.
const GENERATION: u64 = 1;

/// The most shards a directory can have: the language bindings pack a
/// shard number into 10 bits.
pub const MAX_SHARDS: u32 = 1024;

#[derive(Clone, Debug, Default)]
pub struct ReshardOptions {
    /// Flush method for the files written, and the log options the old
    /// directory is read with.
    pub local: LocalOptions,
    /// Tests: return an injected error instead of performing step `n` (the
    /// steps are counted in [`ReshardReport::steps`]); the files are left
    /// as a crash at that point would leave them.
    #[doc(hidden)]
    pub fail_at: Option<u32>,
}

/// What [`reshard`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resharded {
    /// The directory already had that many shards. Leftovers of an earlier
    /// reshard, if any, were removed (and an interrupted one finished).
    AlreadyCurrent {
        shards: u32,
    },
    Done(ReshardReport),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReshardReport {
    pub from: u32,
    pub to: u32,
    /// Runs that were in memory at their shard's last checkpoint.
    pub active: u64,
    /// Parked runs that had not finished.
    pub suspended: u64,
    /// Finished runs still within retention.
    pub finished: u64,
    /// Unacknowledged messages between shards, carried to the new shards.
    pub messages: u64,
    /// Archived journals rewritten.
    pub archived: u64,
    /// Runs that got a new run number.
    pub renumbered: u64,
    /// The unfinished ones among them.
    pub renumbered_unfinished: Vec<Renumbered>,
    /// The new shards' log position: every moved record is at or before it.
    pub checkpoint_lsn: Lsn,
    /// Durable steps performed (see [`ReshardOptions::fail_at`]).
    pub steps: u32,
}

/// An unfinished run whose run number changed: its `Ctx::key` values
/// (`<workflow ID>/<run>/<position>`) and its completion tokens change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Renumbered {
    pub workflow_id: String,
    pub old_run: RunId,
    pub new_run: RunId,
    /// Async steps waiting for `engine.complete`: the token handed out
    /// before the reshard, and the one the step answers to now.
    pub tokens: Vec<(String, String)>,
}

fn shard_name(s: u32) -> String {
    format!("shard-{s:03}")
}

/// Counts durable steps; fails at the one tests ask for.
struct Steps {
    done: u32,
    fail_at: Option<u32>,
}

impl Steps {
    fn step(&mut self, what: &str) -> Result<()> {
        if self.fail_at == Some(self.done) {
            return Err(Error::Io(format!(
                "injected failure at reshard step {} ({what})",
                self.done
            )));
        }
        self.done += 1;
        Ok(())
    }
}

/// Changes the shard count of the local data directory at `dir` to
/// `shards`. The directory must be closed: this takes its lock. It must be
/// in log format 3 (`fusee upgrade` converts an older one). Also finishes
/// a reshard that was interrupted after its switch began.
pub fn reshard(dir: &Path, shards: u32, opts: &ReshardOptions) -> Result<Resharded> {
    if !(1..=MAX_SHARDS).contains(&shards) {
        return Err(Error::Rejected(format!(
            "a directory has 1 to {MAX_SHARDS} shards, not {shards}"
        )));
    }
    if !dir.join(META).exists() {
        return Err(Error::Io(format!(
            "{} is not a Fusee data directory",
            dir.display()
        )));
    }
    for marker in ["vsr", "leases", "superblock"] {
        if dir.join(marker).exists() {
            return Err(Error::Io(format!(
                "{} belongs to a replicated group: changing its shard count is not supported",
                dir.display()
            )));
        }
    }
    let _lock = crate::local::lock_directory(dir)?;
    let method = opts.local.flush;
    let mut steps = Steps {
        done: 0,
        fail_at: opts.fail_at,
    };
    if let Some((from, to)) = switching(dir)? {
        roll_forward(dir, from, to, method, &mut steps)?;
    }
    let (from, log) = read_meta(dir)?
        .ok_or_else(|| Error::Io(format!("{} is not a Fusee data directory", dir.display())))?;
    if log != 3 {
        return Err(Error::Io(format!(
            "{} keeps a log per shard (log format 2): run `fusee upgrade` on it first",
            dir.display()
        )));
    }
    // Leftovers: a build that never switched, or a switch that finished.
    for leftover in [NEW, OLD] {
        let p = dir.join(leftover);
        if p.exists() {
            fs::remove_dir_all(&p)?;
            sync_dir(dir, method)?;
        }
    }
    if from == shards {
        return Ok(Resharded::AlreadyCurrent { shards });
    }
    let mut report = ReshardReport {
        from,
        to: shards,
        ..Default::default()
    };
    {
        let old = Old::open(dir, from, &opts.local)?;
        steps.step("read the old shards")?;
        build(dir, &old, shards, method, &mut steps, &mut report)?;
        // The old shards close (their files are moved next).
    }
    write_switching_meta(dir, from, shards, method)?;
    steps.step("switch")?;
    roll_forward(dir, from, shards, method, &mut steps)?;
    report.steps = steps.done;
    Ok(Resharded::Done(report))
}

/// `(from, to)` if `fusee.meta` says a switch is under way.
fn switching(dir: &Path) -> Result<Option<(u32, u32)>> {
    let text = match fs::read_to_string(dir.join(META)) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let field = |name: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(name))
            .map(|v| v.trim().to_owned())
    };
    if field("format=").as_deref() != Some(SWITCHING) {
        return Ok(None);
    }
    let number = |name: &str| -> Result<u32> {
        field(name)
            .and_then(|v| v.parse().ok())
            .filter(|n| *n > 0)
            .ok_or_else(|| Error::Io(format!("{}: damaged fusee.meta", dir.display())))
    };
    Ok(Some((number("from=")?, number("shards=")?)))
}

fn write_switching_meta(dir: &Path, from: u32, to: u32, method: FlushMethod) -> Result<()> {
    let text = format!(
        "# Fusee data directory, switching from {from} to {to} shards. Do not edit: run \
         `fusee reshard <dir> {to}` to finish.\nformat={SWITCHING}\nshards={to}\nlog=3\nfrom={from}\n"
    );
    let tmp = dir.join("fusee.meta.tmp");
    fs::write(&tmp, text)?;
    flush_file(&File::open(&tmp)?, method)?;
    fs::rename(&tmp, dir.join(META))?;
    sync_dir(dir, method)?;
    Ok(())
}

/// The switch after `fusee.meta` says `format=reshard`: every step checks
/// what is already done, so a crash anywhere in it can be repeated.
fn roll_forward(
    dir: &Path,
    from: u32,
    to: u32,
    method: FlushMethod,
    steps: &mut Steps,
) -> Result<()> {
    let new_root = dir.join(NEW);
    let old_root = dir.join(OLD);
    let incoming: Vec<String> = (0..to)
        .map(shard_name)
        .filter(|n| new_root.join(n).exists())
        .collect();
    // Until the first new shard moves in, every old one is still to move
    // out (all of them have, before the first moves in).
    if incoming.len() == to as usize {
        fs::create_dir_all(&old_root)?;
        sync_dir(dir, method)?;
        let mut outgoing: Vec<String> = vec!["wal".into()];
        outgoing.extend((0..from).map(shard_name));
        // Shard directories beyond both counts (an earlier, larger count's
        // leftovers) go too.
        for e in fs::read_dir(dir)? {
            let name = e?.file_name().to_string_lossy().into_owned();
            if name.starts_with("shard-") && !outgoing.contains(&name) {
                outgoing.push(name);
            }
        }
        for name in outgoing {
            let p = dir.join(&name);
            if !p.exists() {
                continue;
            }
            let target = old_root.join(&name);
            if target.exists() {
                return Err(Error::Io(format!(
                    "{} exists already; move it away and run `fusee reshard` again",
                    target.display()
                )));
            }
            fs::rename(&p, &target)?;
            sync_dir(dir, method)?;
            steps.step("move an old shard out")?;
        }
        sync_dir(&old_root, method)?;
    } else if incoming.is_empty() && (0..to).any(|s| !dir.join(shard_name(s)).exists()) {
        return Err(Error::Io(format!(
            "{}: the new shards are neither in {NEW} nor in place; restore the directory from \
             a backup",
            dir.display()
        )));
    }
    for name in incoming {
        fs::rename(new_root.join(&name), dir.join(&name))?;
        sync_dir(dir, method)?;
        steps.step("move a new shard in")?;
    }
    write_meta(dir, to, 3)?;
    steps.step("final fusee.meta")?;
    for leftover in [&old_root, &new_root] {
        if leftover.exists() {
            fs::remove_dir_all(leftover)?;
        }
    }
    sync_dir(dir, method)?;
    steps.step("remove the old shards")?;
    Ok(())
}

/// One run of an old shard.
#[derive(Clone, Copy, Debug)]
struct OldRun {
    run: RunId,
    id_hash: u64,
    done: bool,
    /// Its frame, for a parked run; `None` for a run in memory.
    parked: Option<BlobRef>,
}

struct OldShard {
    rt: ShardRuntime<WalEnv>,
    runs: Vec<OldRun>,
    outbox: Vec<Outgoing>,
    next_run: RunId,
    tail: Lsn,
}

/// The old shards, recovered and idle.
struct Old {
    dir: PathBuf,
    shards: Vec<OldShard>,
    _wal: Arc<SharedWal>,
}

/// A run's state read from its old shard.
struct RunState {
    id: String,
    records: Vec<RecordBuf>,
    last_lsn: Lsn,
    timers: Vec<(Seq, TimerKind, u64)>,
    done: Option<(Status, u64)>,
}

fn decode_err(e: DecodeError) -> Error {
    Error::Io(format!("run store: {e}"))
}

/// The frame at `b` and the whole journal of the chain it ends.
fn stored(env: &mut WalEnv, b: BlobRef) -> Result<(StoreEntry, Vec<u8>)> {
    let frame = env.store_read(b.offset, b.len)?;
    let newest = read_entry(&frame).map_err(decode_err)?;
    let mut failed: Option<Error> = None;
    let chained = chain_journal(newest.clone(), b.len, &mut |x| match env
        .store_read(x.offset, x.len)
    {
        Ok(f) => read_entry(&f),
        Err(e) => {
            failed = Some(e.into());
            Err(DecodeError("unreadable frame"))
        }
    });
    match chained {
        Ok((journal, _, _)) => Ok((newest, journal)),
        Err(e) => Err(failed.unwrap_or_else(|| decode_err(e))),
    }
}

/// The ID hash of a `Park`, `Done` or `Delta` frame, from its header
/// (`len u32, crc u32, kind u8, run varint, last_lsn varint, id_hash u64`).
/// The whole frame is checked when its journal is read.
fn frame_id_hash(frame: &[u8]) -> Result<u64> {
    let mut r = Reader::new(frame.get(8..).unwrap_or_default());
    match r.u8().map_err(decode_err)? {
        // Park, Done, Delta.
        1 | 2 | 4 => {}
        _ => return Err(Error::Io("a parked run's frame is not a journal".into())),
    }
    r.varint().map_err(decode_err)?;
    r.varint().map_err(decode_err)?;
    r.u64().map_err(decode_err)
}

impl Old {
    fn open(dir: &Path, shards: u32, local: &LocalOptions) -> Result<Self> {
        let mut config = Config::local(dir)
            .shards(shards)
            .local_options(local.clone());
        // Nothing may expire or be dropped while the shards are read; a
        // finished run loaded at the checkpoint is parked at open with its
        // whole journal, written straight to the store.
        config.retention = crate::engine::TOOL_RETENTION;
        config.park_after = Duration::MAX;
        config.memory_budget = 0;
        config.keep_history = true;
        config.archive_history = true;
        config.checkpoint_on_shutdown = false;
        let wal = SharedWal::open(
            &dir.join("wal"),
            shards,
            local.clone(),
            WalOptions {
                segment_bytes: local.segment_bytes,
                first_segment_bytes: local.segment_bytes.min(1 << 20),
                ..WalOptions::default()
            },
        )?;
        let mut out = Vec::with_capacity(shards as usize);
        for s in 0..shards {
            let env = WalEnv::open(&dir.join(shard_name(s)), s, local.clone(), wal.clone())?;
            let mut cfg = runtime_config(&config, s, shards);
            cfg.checkpoint_on_close = false;
            // No workflow code: every run waits for a host that never comes.
            let rt = ShardRuntime::open_with(
                env,
                cfg,
                Arc::new(RwLock::new(Registry::default())),
                true,
                Services::default(),
                Arc::new(|| {}),
            )?;
            let (runs, next_run, tail) = rt.with(|i| -> Result<_> {
                if let Some(e) = i.failed() {
                    return Err(Error::Io(format!("shard {s}: {e}")));
                }
                if i.core.pending_outputs() > 0 {
                    return Err(Error::Io(format!(
                        "shard {s}: messages from the log tail were not released at recovery"
                    )));
                }
                let mut runs = Vec::new();
                for (run, l) in i.core.loaded_runs() {
                    runs.push(OldRun {
                        run,
                        id_hash: hash_id(&l.id),
                        done: l.is_done(),
                        parked: None,
                    });
                }
                let parked: Vec<_> = i.core.parked_runs().collect();
                let store_len = i.env.store_len();
                for (run, p) in parked {
                    let b = p.blob();
                    if b.offset + u64::from(b.len) > store_len {
                        return Err(Error::Io(format!(
                            "shard {s}: run {run}'s frame is not in the run store"
                        )));
                    }
                    let head = i.env.store_read(b.offset, b.len.min(64))?;
                    runs.push(OldRun {
                        run,
                        id_hash: frame_id_hash(&head)?,
                        done: p.done().is_some(),
                        parked: Some(b),
                    });
                }
                Ok((runs, i.core.next_run_id(), i.last_lsn()))
            })?;
            let outbox = rt.take_outgoing();
            out.push(OldShard {
                rt,
                runs,
                outbox,
                next_run,
                tail,
            });
        }
        Ok(Self {
            dir: dir.to_owned(),
            shards: out,
            _wal: wal,
        })
    }

    fn read(&self, shard: u32, r: &OldRun) -> Result<RunState> {
        let sh = &self.shards[shard as usize];
        sh.rt.with(|i| -> Result<RunState> {
            let (journal, last_lsn, timers, done, id_hash) = match r.parked {
                None => {
                    let l = i.core.get(r.run).ok_or_else(|| {
                        Error::Io(format!("shard {shard}: run {} is gone", r.run))
                    })?;
                    let (blob, suffix, last_lsn, done) =
                        (l.blob, l.journal.clone(), l.last_lsn, l.done);
                    let timers: Vec<_> = l.armed_timers().collect();
                    let id_hash = hash_id(&l.id);
                    let mut journal = match blob {
                        Some(b) => stored(&mut i.env, b)?.1,
                        None => Vec::new(),
                    };
                    journal.extend_from_slice(&suffix);
                    (journal, last_lsn, timers, done, id_hash)
                }
                Some(b) => match stored(&mut i.env, b)? {
                    (
                        StoreEntry::Park {
                            last_lsn,
                            id_hash,
                            timers,
                            ..
                        }
                        | StoreEntry::Delta {
                            last_lsn,
                            id_hash,
                            timers,
                            ..
                        },
                        journal,
                    ) => (journal, last_lsn, timers, None, id_hash),
                    (
                        StoreEntry::Done {
                            last_lsn,
                            id_hash,
                            status,
                            finished_ms,
                            ..
                        },
                        journal,
                    ) => (
                        journal,
                        last_lsn,
                        Vec::new(),
                        Some((status, finished_ms)),
                        id_hash,
                    ),
                    _ => {
                        return Err(Error::Io(format!(
                            "shard {shard}: run {}'s frame holds no journal",
                            r.run
                        )));
                    }
                },
            };
            if id_hash != r.id_hash {
                return Err(Error::Io(format!(
                    "shard {shard}: run {}'s frame changed while it was read",
                    r.run
                )));
            }
            let records = decode_canonical_all(&journal).map_err(decode_err)?;
            // A run that finished after it was loaded is stored as a
            // `Delta` chain ending with its `Done`: finished, not parked.
            let done = done.or_else(|| match (r.parked, records.last()) {
                (
                    Some(_),
                    Some(Record::Done {
                        status,
                        finished_ms,
                        ..
                    }),
                ) => Some((*status, *finished_ms)),
                _ => None,
            });
            let id = match records.first() {
                Some(Record::Start { id, .. }) => id.to_string(),
                _ => {
                    return Err(Error::Io(format!(
                        "shard {shard}: run {}'s journal does not begin with its start",
                        r.run
                    )));
                }
            };
            Ok(RunState {
                id,
                records,
                last_lsn,
                timers,
                done,
            })
        })
    }
}

/// Where every old run goes: per old shard, `(old run, new shard, new run)`
/// sorted by old run.
struct Map {
    shards: Vec<Vec<(RunId, u32, RunId)>>,
}

impl Map {
    fn get(&self, shard: u32, run: RunId) -> Option<(u32, RunId)> {
        let v = self.shards.get(shard as usize)?;
        let i = v.binary_search_by_key(&run, |e| e.0).ok()?;
        Some((v[i].1, v[i].2))
    }

    /// The parent's new place; one that matches no run (run 0, which is
    /// never allocated) if the parent no longer exists.
    fn parent(&self, p: ParentRef) -> ParentRef {
        match self.get(p.shard, p.run) {
            Some((shard, run)) => ParentRef {
                shard,
                run,
                seq: p.seq,
            },
            None => ParentRef {
                shard: 0,
                run: 0,
                seq: p.seq,
            },
        }
    }

    fn run(&self, shard: u32, run: RunId) -> RunId {
        self.get(shard, run).map_or(0, |m| m.1)
    }
}

/// A run's place in a new shard.
#[derive(Clone, Copy, Debug)]
struct Placed {
    old_shard: u32,
    index: usize,
    new_run: RunId,
}

/// Picks every run's new number (see the module documentation). Returns
/// the map, the runs of each new shard in new-number order, and each new
/// shard's next run number.
fn assign(old: &Old, to: u32) -> (Map, Vec<Vec<Placed>>, Vec<RunId>) {
    let global_next = old
        .shards
        .iter()
        .map(|s| s.next_run)
        .max()
        .unwrap_or(1)
        .max(1);
    // Per new shard, groups of runs sharing an ID hash (always from one old
    // shard: the hash places them).
    let mut groups: Vec<FxHashMap<u64, Vec<(u32, usize)>>> =
        (0..to).map(|_| FxHashMap::default()).collect();
    for (s, sh) in old.shards.iter().enumerate() {
        for (i, r) in sh.runs.iter().enumerate() {
            let n = (r.id_hash % u64::from(to)) as usize;
            groups[n].entry(r.id_hash).or_default().push((s as u32, i));
        }
    }
    let mut map: Vec<Vec<(RunId, u32, RunId)>> = old.shards.iter().map(|_| Vec::new()).collect();
    let mut placed = Vec::with_capacity(to as usize);
    let mut next = Vec::with_capacity(to as usize);
    let run_of = |(s, i): (u32, usize)| old.shards[s as usize].runs[i];
    for (n, g) in groups.into_iter().enumerate() {
        let mut g: Vec<Vec<(u32, usize)>> = g.into_values().collect();
        for v in &mut g {
            v.sort_by_key(|&x| run_of(x).run);
        }
        // Groups whose newest run is unfinished keep their numbers first.
        g.sort_by_key(|v| {
            let newest = *v.last().expect("a group has a run");
            (run_of(newest).done, newest.0, run_of(newest).run)
        });
        let mut claimed: FxHashSet<RunId> = FxHashSet::default();
        let mut kept: Vec<Placed> = Vec::new();
        let mut moved: Vec<(u32, usize)> = Vec::new();
        for v in g {
            // A group keeps a prefix of its numbers; once one is taken, the
            // newer runs of the ID move above every old number too, so the
            // newest run stays the highest.
            let cut = v
                .iter()
                .position(|&x| claimed.contains(&run_of(x).run))
                .unwrap_or(v.len());
            for &x in &v[..cut] {
                let run = run_of(x).run;
                claimed.insert(run);
                kept.push(Placed {
                    old_shard: x.0,
                    index: x.1,
                    new_run: run,
                });
            }
            moved.extend_from_slice(&v[cut..]);
        }
        let base = claimed
            .iter()
            .max()
            .map_or(global_next, |m| (m + 1).max(global_next));
        let mut runs = kept;
        for (k, x) in moved.into_iter().enumerate() {
            runs.push(Placed {
                old_shard: x.0,
                index: x.1,
                new_run: base + k as u64,
            });
        }
        let next_run = runs
            .iter()
            .map(|p| p.new_run + 1)
            .max()
            .unwrap_or(1)
            .max(global_next);
        runs.sort_by_key(|p| p.new_run);
        for p in &runs {
            map[p.old_shard as usize].push((
                run_of((p.old_shard, p.index)).run,
                n as u32,
                p.new_run,
            ));
        }
        placed.push(runs);
        next.push(next_run);
    }
    for v in &mut map {
        v.sort_by_key(|e| e.0);
    }
    (Map { shards: map }, placed, next)
}

/// A record of a run that moves from old shard `shard` and is now run `me`.
fn remap(rec: &RecordBuf, shard: u32, me: RunId, map: &Map) -> RecordBuf {
    let mut r = rec.with_run(me);
    match &mut r {
        Record::Start { parent, from, .. } => {
            if let Some(p) = parent {
                *p = map.parent(*p);
            }
            if let Some(o) = from {
                o.run = map.run(shard, o.run);
            }
        }
        // A continue-as-new or reset names the next run's start.
        Record::Done {
            status, payload, ..
        } if status.is_handover() => {
            if let Ok(mut next) = decode_canonical_all(payload)
                && next.len() == 1
                && matches!(next[0], Record::Start { .. })
            {
                let start = next.remove(0);
                let start = remap(&start, shard, map.run(shard, start.run()), map);
                let mut bytes = Vec::new();
                encode_canonical(&start, &mut bytes);
                *payload = Arc::from(bytes);
            }
        }
        _ => {}
    }
    r
}

fn encode_all(records: &[RecordBuf]) -> Vec<u8> {
    let mut out = Vec::new();
    for r in records {
        encode_canonical(r, &mut out);
    }
    out
}

/// The async completions a journal still waits for: `(seq, attempt)`.
fn pending_completions(records: &[RecordBuf]) -> Vec<(Seq, u32)> {
    let mut awaiting: Vec<(Seq, u32)> = Vec::new();
    for r in records {
        match r {
            Record::Awaiting { seq, attempt, .. } => {
                awaiting.retain(|a| a.0 != *seq);
                awaiting.push((*seq, *attempt));
            }
            Record::Completed { seq, .. } => awaiting.retain(|a| a.0 != *seq),
            Record::Done { .. } => awaiting.clear(),
            _ => {}
        }
    }
    awaiting
}

/// A message from an old outbox in the new layout: the new shard whose
/// outbox holds it, and the message. `None`: nobody could receive it.
fn remap_message(mut o: Outgoing, map: &Map, to: u32) -> Option<(u32, Outgoing)> {
    let holder = match &mut o.msg {
        Message::ChildStart { parent, id, .. } => {
            // The parent's shard keeps resending it until the child
            // answers, and drops it when the child's result arrives there.
            *parent = map.parent(*parent);
            o.target = shard_of(id, to);
            parent.shard
        }
        Message::ChildResult { parent, .. } => {
            *parent = map.parent(*parent);
            if parent.run == 0 {
                // Its parent is gone: the receiver would drop it.
                return None;
            }
            o.target = parent.shard;
            o.target
        }
        Message::Signal { target, .. } => {
            o.target = shard_of(target, to);
            o.target
        }
        Message::Close { target, parent, .. } => {
            if let Some(p) = parent {
                *p = map.parent(*p);
            }
            o.target = shard_of(target, to);
            o.target
        }
    };
    Some((holder, o))
}

/// Writes the new shards under `<dir>/reshard.new`, synced.
fn build(
    dir: &Path,
    old: &Old,
    to: u32,
    method: FlushMethod,
    steps: &mut Steps,
    report: &mut ReshardReport,
) -> Result<()> {
    let root = dir.join(NEW);
    fs::create_dir_all(&root)?;
    for n in 0..to {
        fs::create_dir_all(root.join(shard_name(n)))?;
    }
    sync_dir(&root, method)?;
    sync_dir(dir, method)?;
    let (map, placed, next) = assign(old, to);
    // Every moved record is at or before this position, so the new shards'
    // logs start after it and nothing waits for a position never written.
    let lsn = old.shards.iter().map(|s| s.tail).max().unwrap_or(0);
    report.checkpoint_lsn = lsn;

    report.archived = rewrite_archives(
        &old.dir,
        &root,
        old.shards.len() as u32,
        to,
        lsn,
        &map,
        method,
    )?;
    steps.step("archives")?;

    let mut outboxes: Vec<Vec<Outgoing>> = (0..to).map(|_| Vec::new()).collect();
    for sh in &old.shards {
        for o in &sh.outbox {
            if let Some((holder, o)) = remap_message(o.clone(), &map, to) {
                outboxes[holder as usize].push(o);
                report.messages += 1;
            }
        }
    }

    let now = crate::local::unix_ms();
    for n in 0..to {
        let sdir = root.join(shard_name(n));
        let path = sdir.join(format!("runs-{GENERATION:010}.dat"));
        let file = File::create(&path)?;
        let mut buf = store_header();
        let mut written = 0u64;
        let mut cp_runs = Vec::new();
        for p in &placed[n as usize] {
            let r = old.shards[p.old_shard as usize].runs[p.index];
            let state = old.read(p.old_shard, &r)?;
            let records: Vec<RecordBuf> = state
                .records
                .iter()
                .map(|rec| remap(rec, p.old_shard, p.new_run, &map))
                .collect();
            let journal = encode_all(&records);
            let frame = match state.done {
                Some((status, finished_ms)) => {
                    report.finished += 1;
                    encode_entry(&StoreEntry::Done {
                        run: p.new_run,
                        last_lsn: state.last_lsn,
                        id_hash: r.id_hash,
                        status,
                        finished_ms,
                        journal,
                    })
                }
                None => encode_entry(&StoreEntry::Park {
                    run: p.new_run,
                    last_lsn: state.last_lsn,
                    id_hash: r.id_hash,
                    timers: state.timers.clone(),
                    journal,
                }),
            };
            let blob = BlobRef {
                offset: written + buf.len() as u64,
                len: u32::try_from(frame.len())
                    .map_err(|_| Error::Io(format!("run {}'s journal exceeds 4 GiB", r.run)))?,
            };
            buf.extend_from_slice(&frame);
            if state.done.is_none() {
                if r.parked.is_none() {
                    // Its code resumes at the next open, as after a checkpoint
                    // that stored its journal whole.
                    report.active += 1;
                    cp_runs.push(CheckpointRun {
                        run: p.new_run,
                        last_lsn: state.last_lsn,
                        blob: Some(blob),
                        journal: Vec::new(),
                    });
                } else {
                    report.suspended += 1;
                }
            }
            if p.new_run != r.run {
                report.renumbered += 1;
                if state.done.is_none() {
                    let tokens = pending_completions(&state.records)
                        .into_iter()
                        .map(|(seq, attempt)| {
                            (
                                format_token(&state.id, r.run, seq, attempt),
                                format_token(&state.id, p.new_run, seq, attempt),
                            )
                        })
                        .collect();
                    report.renumbered_unfinished.push(Renumbered {
                        workflow_id: state.id.clone(),
                        old_run: r.run,
                        new_run: p.new_run,
                        tokens,
                    });
                }
            }
            if buf.len() >= 8 << 20 {
                file.write_all_at(&buf, written)?;
                written += buf.len() as u64;
                buf.clear();
            }
        }
        file.write_all_at(&buf, written)?;
        let store_len = written + buf.len() as u64;
        flush_file(&file, method)?;
        drop(file);
        let cp = Checkpoint {
            shard: n,
            shards: to,
            lsn,
            next_run: next[n as usize],
            store_generation: GENERATION,
            store_len,
            created_ms: now,
            runs: cp_runs,
            outbox: encode_outbox(&outboxes[n as usize]),
            // The first batch of the new shared log.
            log_position: 1,
            // Recovery scans the store; the first checkpoint writes the index.
            index: None,
            obsolete: 0,
            index_runs: 0,
        };
        let cp_path = sdir.join(format!("checkpoint-{lsn:020}.fck"));
        let f = File::create(&cp_path)?;
        f.write_all_at(&cp.encode(), 0)?;
        flush_file(&f, method)?;
        sync_dir(&sdir, method)?;
        steps.step("a new shard")?;
    }
    sync_dir(&root, method)?;
    report
        .renumbered_unfinished
        .sort_by(|a, b| a.workflow_id.cmp(&b.workflow_id));
    Ok(())
}

/// Rewrites every old shard's history archives into the new shards: each
/// journal goes to the shard its ID hashes to, with its run's new number
/// (a run whose retention ended keeps its old one: nothing refers to it).
fn rewrite_archives(
    dir: &Path,
    root: &Path,
    from: u32,
    to: u32,
    lsn: Lsn,
    map: &Map,
    method: FlushMethod,
) -> Result<u64> {
    let mut pending: Vec<(Vec<(RunId, Vec<u8>)>, u64)> = (0..to).map(|_| (Vec::new(), 0)).collect();
    let mut count = 0u64;
    let flush = |n: usize, frames: &mut Vec<(RunId, Vec<u8>)>| -> Result<()> {
        let adir = root.join(shard_name(n as u32)).join("archive");
        crate::archive::write_frames(&adir, lsn, method, frames)?;
        frames.clear();
        Ok(())
    };
    for s in 0..from {
        let adir = dir.join(shard_name(s)).join("archive");
        let mut files: Vec<PathBuf> = match fs::read_dir(&adir) {
            Ok(d) => d
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|x| x == "arc"))
                .collect(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        files.sort();
        for path in files {
            for frame in crate::archive::read_frames(&path)? {
                let StoreEntry::Done {
                    run,
                    last_lsn,
                    id_hash,
                    status,
                    finished_ms,
                    journal,
                } = read_entry(&frame).map_err(decode_err)?
                else {
                    continue;
                };
                let n = (id_hash % u64::from(to)) as usize;
                let (new_run, journal) = match map.get(s, run) {
                    Some((_, new_run)) => {
                        let records = decode_canonical_all(&journal).map_err(decode_err)?;
                        let records: Vec<RecordBuf> =
                            records.iter().map(|r| remap(r, s, new_run, map)).collect();
                        (new_run, encode_all(&records))
                    }
                    None => (run, journal),
                };
                let frame = encode_entry(&StoreEntry::Done {
                    run: new_run,
                    last_lsn,
                    id_hash,
                    status,
                    finished_ms,
                    journal,
                });
                let slot = &mut pending[n];
                slot.1 += frame.len() as u64;
                slot.0.push((new_run, frame));
                count += 1;
                if slot.1 >= 8 << 20 {
                    flush(n, &mut slot.0)?;
                    slot.1 = 0;
                }
            }
        }
    }
    for (n, slot) in pending.iter_mut().enumerate() {
        flush(n, &mut slot.0)?;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start(run: RunId, id: &str, parent: Option<ParentRef>) -> RecordBuf {
        Record::Start {
            run,
            id: Arc::from(id),
            wtype: Arc::from("t"),
            version: 1,
            codec: 0,
            started_ms: 1,
            deadline_ms: 0,
            parent,
            policy: 0,
            from: None,
            copies: 0,
            update: None,
            input: Arc::from(&b""[..]),
            meta: None,
        }
    }

    #[test]
    fn records_follow_their_runs_and_parents() {
        let map = Map {
            shards: vec![vec![(1, 2, 7), (2, 0, 9)], vec![(1, 1, 1)]],
        };
        let child = start(
            5,
            "c",
            Some(ParentRef {
                shard: 0,
                run: 2,
                seq: 3,
            }),
        );
        match remap(&child, 1, 1, &map) {
            Record::Start { run, parent, .. } => {
                assert_eq!(run, 1);
                assert_eq!(
                    parent,
                    Some(ParentRef {
                        shard: 0,
                        run: 9,
                        seq: 3
                    })
                );
            }
            _ => unreachable!(),
        }
        // A parent that is gone matches no run.
        let orphan = start(
            5,
            "c",
            Some(ParentRef {
                shard: 1,
                run: 44,
                seq: 3,
            }),
        );
        let Record::Start { parent, .. } = remap(&orphan, 1, 1, &map) else {
            unreachable!()
        };
        assert_eq!(parent.unwrap().run, 0);
        // Continue-as-new: the next run's start inside the `Done` follows it.
        let mut next = Vec::new();
        encode_canonical(&start(2, "w", None).with_run(2), &mut next);
        let done: RecordBuf = Record::Done {
            run: 1,
            status: Status::ContinuedAsNew,
            finished_ms: 5,
            payload: Arc::from(next),
            by: None,
        };
        let Record::Done { run, payload, .. } = remap(&done, 0, 7, &map) else {
            unreachable!()
        };
        assert_eq!(run, 7);
        assert_eq!(decode_canonical_all(&payload).unwrap()[0].run(), 9);
    }

    #[test]
    fn completions_still_waiting() {
        let a = |seq, attempt| Record::Awaiting {
            run: 1,
            seq,
            name: Arc::from("a"),
            attempt,
        };
        let c = |seq| Record::Completed {
            run: 1,
            seq,
            attempt: 1,
            status: Status::Ok,
            payload: Arc::from(&b""[..]),
        };
        let j = vec![start(1, "w", None), a(1, 1), a(2, 1), c(1), a(2, 2)];
        assert_eq!(pending_completions(&j), vec![(2, 2)]);
    }
}
