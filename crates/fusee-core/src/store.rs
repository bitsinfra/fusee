//! Formats for the run store and checkpoints.
//!
//! **Run store.** An append-only file of checksummed entries. A `Park` entry
//! holds a suspended run's whole journal plus its timers, a `Delta` entry
//! (format 3) the records a run appended since an earlier frame of its own
//! plus its timers, a `Done` entry a finished run's start and result, and a
//! `Tombstone` marks a finished run whose retention ended. Later entries for
//! a run supersede earlier ones (a `Delta` keeps the frames it extends in
//! use). `Index` entries (format 3) hold the run table's persisted index.
//! The store is flushed before each checkpoint; the checkpoint names the
//! length that is valid, and recovery reads only that prefix.
//!
//! **Checkpoint.** Taken at log position `lsn`. It names the run store
//! generation and length, carries each loaded run's journal (or the part of
//! it not already in the run store), and (format 3) names the `Index` frames
//! in that prefix that list every parked run. Recovery loads the checkpoint,
//! reads those frames (or, without them, scans the whole prefix), then
//! applies log records after `lsn`. Log segments wholly at or before `lsn`
//! can then be deleted.

use crate::record::{Lsn, RunId, Seq, Status};
use crate::shard::{BlobRef, TimerKind};
use crate::wire::{DecodeError, DecodeResult, Reader, put_bytes, put_u32, put_u64, put_varint};

pub const STORE_MAGIC: &[u8; 8] = b"FUSEERUN";
/// Format 3 adds `Delta` and `Index` entries. Format 2 stores still open;
/// new entries go only to format 3 stores.
pub const STORE_FORMAT: u32 = 3;
pub const STORE_FORMAT_MIN: u32 = 2;
pub const STORE_HEADER: u64 = 16;
pub const CHECKPOINT_MAGIC: &[u8; 8] = b"FUSEECKP";
/// Format 3 adds the persisted index and the obsolete byte count. Format 2
/// checkpoints still open (recovery then scans the run store).
pub const CHECKPOINT_FORMAT: u32 = 3;
pub const CHECKPOINT_FORMAT_MIN: u32 = 2;

const PARK: u8 = 1;
const DONE: u8 = 2;
const TOMBSTONE: u8 = 3;
const DELTA: u8 = 4;
const INDEX: u8 = 5;

/// Most frames a `Delta` chain may have before its journal is written whole.
pub const MAX_CHAIN: u8 = 4;

/// The run store's 16-byte file header.
pub fn store_header() -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(STORE_MAGIC);
    put_u32(&mut out, STORE_FORMAT);
    let crc = crc32c::crc32c(&out);
    put_u32(&mut out, crc);
    out
}

pub fn check_store_header(bytes: &[u8]) -> DecodeResult<()> {
    store_format(bytes).map(|_| ())
}

/// The format of a run store from its header.
pub fn store_format(bytes: &[u8]) -> DecodeResult<u32> {
    let head = bytes
        .get(..16)
        .ok_or(DecodeError("short run store header"))?;
    if &head[..8] != STORE_MAGIC {
        return Err(DecodeError("not a run store"));
    }
    let format = u32::from_le_bytes([head[8], head[9], head[10], head[11]]);
    let crc = u32::from_le_bytes([head[12], head[13], head[14], head[15]]);
    if crc != crc32c::crc32c(&head[..12]) {
        return Err(DecodeError("damaged run store header"));
    }
    if !(STORE_FORMAT_MIN..=STORE_FORMAT).contains(&format) {
        return Err(DecodeError("run store format is not one this build reads"));
    }
    Ok(format)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreEntry {
    Park {
        run: RunId,
        last_lsn: Lsn,
        id_hash: u64,
        timers: Vec<(Seq, TimerKind, u64)>,
        journal: Vec<u8>,
    },
    Done {
        run: RunId,
        last_lsn: Lsn,
        id_hash: u64,
        status: Status,
        finished_ms: u64,
        journal: Vec<u8>,
    },
    Tombstone {
        run: RunId,
    },
    /// A suspended run parked again: the records appended since `prev`, the
    /// run's previous frame, which holds the rest of its journal.
    Delta {
        run: RunId,
        last_lsn: Lsn,
        id_hash: u64,
        timers: Vec<(Seq, TimerKind, u64)>,
        prev: BlobRef,
        /// Frames before this one in the chain (1 when `prev` is whole).
        depth: u8,
        /// Bytes of the frames before this one in the chain.
        chain: u64,
        journal: Vec<u8>,
    },
    /// Part of the run table's persisted index: [`crate::table::RECORD`]-byte
    /// records (see [`crate::table::Entry::record`]).
    Index {
        records: Vec<u8>,
    },
}

impl StoreEntry {
    /// The run the entry is about (0 for an `Index` entry).
    pub fn run(&self) -> RunId {
        match self {
            Self::Park { run, .. }
            | Self::Done { run, .. }
            | Self::Delta { run, .. }
            | Self::Tombstone { run } => *run,
            Self::Index { .. } => 0,
        }
    }

    /// Bytes of the run's frames this entry keeps in use, given its own
    /// frame length.
    pub fn chain_bytes(&self, frame_len: u32) -> u64 {
        match self {
            Self::Delta { chain, .. } => chain + u64::from(frame_len),
            _ => u64::from(frame_len),
        }
    }
}

/// A store entry with borrowed parts, encoded by [`encode_entry_into`]
/// without copying them into a [`StoreEntry`] first.
#[derive(Clone, Copy, Debug)]
pub enum EntryRef<'a> {
    Park {
        run: RunId,
        last_lsn: Lsn,
        id_hash: u64,
        timers: &'a [(Seq, TimerKind, u64)],
        journal: &'a [u8],
    },
    Done {
        run: RunId,
        last_lsn: Lsn,
        id_hash: u64,
        status: Status,
        finished_ms: u64,
        journal: &'a [u8],
    },
    Tombstone {
        run: RunId,
    },
    Delta {
        run: RunId,
        last_lsn: Lsn,
        id_hash: u64,
        timers: &'a [(Seq, TimerKind, u64)],
        prev: BlobRef,
        depth: u8,
        chain: u64,
        journal: &'a [u8],
    },
    Index {
        records: &'a [u8],
    },
}

impl StoreEntry {
    pub fn as_entry_ref(&self) -> EntryRef<'_> {
        match self {
            Self::Park {
                run,
                last_lsn,
                id_hash,
                timers,
                journal,
            } => EntryRef::Park {
                run: *run,
                last_lsn: *last_lsn,
                id_hash: *id_hash,
                timers,
                journal,
            },
            Self::Done {
                run,
                last_lsn,
                id_hash,
                status,
                finished_ms,
                journal,
            } => EntryRef::Done {
                run: *run,
                last_lsn: *last_lsn,
                id_hash: *id_hash,
                status: *status,
                finished_ms: *finished_ms,
                journal,
            },
            Self::Tombstone { run } => EntryRef::Tombstone { run: *run },
            Self::Delta {
                run,
                last_lsn,
                id_hash,
                timers,
                prev,
                depth,
                chain,
                journal,
            } => EntryRef::Delta {
                run: *run,
                last_lsn: *last_lsn,
                id_hash: *id_hash,
                timers,
                prev: *prev,
                depth: *depth,
                chain: *chain,
                journal,
            },
            Self::Index { records } => EntryRef::Index { records },
        }
    }
}

/// Encodes one framed entry. Returns the frame; the journal blob of a `Park`
/// or `Done` entry follows the entry metadata.
pub fn encode_entry(entry: &StoreEntry) -> Vec<u8> {
    let mut frame = Vec::new();
    encode_entry_into(entry.as_entry_ref(), &mut frame);
    frame
}

fn put_timers(body: &mut Vec<u8>, timers: &[(Seq, TimerKind, u64)]) {
    put_varint(body, timers.len() as u64);
    for (seq, kind, at) in timers {
        put_varint(body, u64::from(*seq));
        body.push(*kind as u8);
        put_varint(body, *at);
    }
}

/// Appends one framed entry to `out`: the bytes [`encode_entry`] returns,
/// written in place.
pub fn encode_entry_into(entry: EntryRef<'_>, out: &mut Vec<u8>) {
    let blob = match entry {
        EntryRef::Park { journal, .. }
        | EntryRef::Done { journal, .. }
        | EntryRef::Delta { journal, .. } => journal.len(),
        EntryRef::Index { records } => records.len(),
        EntryRef::Tombstone { .. } => 0,
    };
    out.reserve(blob + 64);
    let start = out.len();
    // Length and checksum, filled in once the body is written.
    out.extend_from_slice(&[0; 8]);
    let body = &mut *out;
    match entry {
        EntryRef::Park {
            run,
            last_lsn,
            id_hash,
            timers,
            journal,
        } => {
            body.push(PARK);
            put_varint(body, run);
            put_varint(body, last_lsn);
            put_u64(body, id_hash);
            put_timers(body, timers);
            put_bytes(body, journal);
        }
        EntryRef::Done {
            run,
            last_lsn,
            id_hash,
            status,
            finished_ms,
            journal,
        } => {
            body.push(DONE);
            put_varint(body, run);
            put_varint(body, last_lsn);
            put_u64(body, id_hash);
            body.push(status as u8);
            put_varint(body, finished_ms);
            put_bytes(body, journal);
        }
        EntryRef::Tombstone { run } => {
            body.push(TOMBSTONE);
            put_varint(body, run);
        }
        EntryRef::Delta {
            run,
            last_lsn,
            id_hash,
            timers,
            prev,
            depth,
            chain,
            journal,
        } => {
            body.push(DELTA);
            put_varint(body, run);
            put_varint(body, last_lsn);
            put_u64(body, id_hash);
            put_timers(body, timers);
            put_varint(body, prev.offset);
            put_varint(body, u64::from(prev.len));
            body.push(depth);
            put_varint(body, chain);
            put_bytes(body, journal);
        }
        EntryRef::Index { records } => {
            body.push(INDEX);
            put_bytes(body, records);
        }
    }
    let len = (out.len() - start - 8) as u32;
    let crc = crc32c::crc32c(&out[start + 8..]);
    out[start..start + 4].copy_from_slice(&len.to_le_bytes());
    out[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
}

fn decode_timers(r: &mut Reader<'_>) -> DecodeResult<Vec<(Seq, TimerKind, u64)>> {
    let n = r.varint()?;
    if n > 1_000_000 {
        return Err(DecodeError("too many timers"));
    }
    let mut timers = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let seq = r.varint_u32()?;
        let kind = TimerKind::from_u8(r.u8()?).ok_or(DecodeError("bad timer kind"))?;
        timers.push((seq, kind, r.varint()?));
    }
    Ok(timers)
}

pub fn decode_entry_body(body: &[u8]) -> DecodeResult<StoreEntry> {
    let mut r = Reader::new(body);
    let e = match r.u8()? {
        PARK => {
            let run = r.varint()?;
            let last_lsn = r.varint()?;
            let id_hash = r.u64()?;
            let timers = decode_timers(&mut r)?;
            StoreEntry::Park {
                run,
                last_lsn,
                id_hash,
                timers,
                journal: r.blob()?.to_vec(),
            }
        }
        DELTA => {
            let run = r.varint()?;
            let last_lsn = r.varint()?;
            let id_hash = r.u64()?;
            let timers = decode_timers(&mut r)?;
            let prev = BlobRef {
                offset: r.varint()?,
                len: r.varint_u32()?,
            };
            let depth = r.u8()?;
            if depth == 0 || depth > MAX_CHAIN {
                return Err(DecodeError("bad delta chain depth"));
            }
            StoreEntry::Delta {
                run,
                last_lsn,
                id_hash,
                timers,
                prev,
                depth,
                chain: r.varint()?,
                journal: r.blob()?.to_vec(),
            }
        }
        INDEX => {
            let records = r.blob()?.to_vec();
            if !records.len().is_multiple_of(crate::table::RECORD) {
                return Err(DecodeError("index entry is not whole records"));
            }
            StoreEntry::Index { records }
        }
        DONE => StoreEntry::Done {
            run: r.varint()?,
            last_lsn: r.varint()?,
            id_hash: r.u64()?,
            status: Status::from_u8(r.u8()?)?,
            finished_ms: r.varint()?,
            journal: r.blob()?.to_vec(),
        },
        TOMBSTONE => StoreEntry::Tombstone { run: r.varint()? },
        _ => return Err(DecodeError("unknown run store entry")),
    };
    if !r.is_empty() {
        return Err(DecodeError("trailing bytes in run store entry"));
    }
    Ok(e)
}

/// Reads entries from `bytes` (a run store prefix, header included), calling
/// `visit(offset_of_frame, frame_len, entry)`. Every byte must be valid: the
/// checkpoint vouches for this prefix having been flushed.
pub fn scan_store(bytes: &[u8], mut visit: impl FnMut(u64, u32, StoreEntry)) -> DecodeResult<()> {
    check_store_header(bytes)?;
    let mut r = Reader::new(&bytes[STORE_HEADER as usize..]);
    let mut offset = STORE_HEADER;
    while !r.is_empty() {
        let len = r.u32()? as usize;
        let crc = r.u32()?;
        let body = r.take(len)?;
        if crc32c::crc32c(body) != crc {
            return Err(DecodeError("run store entry checksum mismatch"));
        }
        let entry = decode_entry_body(body)?;
        visit(offset, (len + 8) as u32, entry);
        offset += (len + 8) as u64;
    }
    Ok(())
}

/// Reads the single entry at `blob` (as returned by [`scan_store`]).
pub fn read_entry(frame: &[u8]) -> DecodeResult<StoreEntry> {
    let mut r = Reader::new(frame);
    let len = r.u32()? as usize;
    let crc = r.u32()?;
    let body = r.take(len)?;
    if crc32c::crc32c(body) != crc {
        return Err(DecodeError("run store entry checksum mismatch"));
    }
    decode_entry_body(body)
}

/// The run, last LSN and journal of a `Park` or `Done` frame, borrowed from
/// `frame` and checked as [`read_entry`] checks it; `None` for other
/// entries (a `Delta` needs [`chain_journal`]).
pub fn read_whole_journal(frame: &[u8]) -> DecodeResult<Option<(RunId, Lsn, &[u8])>> {
    let mut r = Reader::new(frame);
    let len = r.u32()? as usize;
    let crc = r.u32()?;
    let body = r.take(len)?;
    if crc32c::crc32c(body) != crc {
        return Err(DecodeError("run store entry checksum mismatch"));
    }
    let mut r = Reader::new(body);
    let (run, last_lsn) = match r.u8()? {
        PARK => {
            let run = r.varint()?;
            let last_lsn = r.varint()?;
            r.u64()?;
            let n = r.varint()?;
            if n > 1_000_000 {
                return Err(DecodeError("too many timers"));
            }
            for _ in 0..n {
                r.varint_u32()?;
                TimerKind::from_u8(r.u8()?).ok_or(DecodeError("bad timer kind"))?;
                r.varint()?;
            }
            (run, last_lsn)
        }
        DONE => {
            let run = r.varint()?;
            let last_lsn = r.varint()?;
            r.u64()?;
            Status::from_u8(r.u8()?)?;
            r.varint()?;
            (run, last_lsn)
        }
        _ => return Ok(None),
    };
    let journal = r.blob()?;
    if !r.is_empty() {
        return Err(DecodeError("trailing bytes in run store entry"));
    }
    Ok(Some((run, last_lsn, journal)))
}

/// A finished run's status and end time when `journal` (canonical
/// records) ends with its `Done` record: a `Delta` frame that finishes a
/// run that was loaded from its stored frames.
pub fn journal_done(journal: &[u8]) -> Option<(Status, u64)> {
    match crate::record::decode_canonical_all(journal).ok()?.last()? {
        crate::record::Record::Done {
            status,
            finished_ms,
            ..
        } => Some((*status, *finished_ms)),
        _ => None,
    }
}

/// A run's journal from its newest frame, reading the frames a `Delta`
/// extends with `read`. Returns the journal (canonical records, oldest
/// first), the frames' total length and how many frames precede the newest.
pub fn chain_journal(
    newest: StoreEntry,
    frame_len: u32,
    read: &mut dyn FnMut(BlobRef) -> DecodeResult<StoreEntry>,
) -> DecodeResult<(Vec<u8>, u64, u8)> {
    let run = newest.run();
    match newest {
        StoreEntry::Park { journal, .. } | StoreEntry::Done { journal, .. } => {
            Ok((journal, u64::from(frame_len), 0))
        }
        StoreEntry::Tombstone { .. } | StoreEntry::Index { .. } => {
            Ok((Vec::new(), u64::from(frame_len), 0))
        }
        StoreEntry::Delta {
            prev,
            depth,
            chain,
            journal,
            ..
        } => {
            let mut parts = vec![journal];
            let mut at = prev;
            let mut left = depth;
            loop {
                let e = read(at)?;
                if e.run() != run {
                    return Err(DecodeError("delta frame extends another run's frame"));
                }
                left = left
                    .checked_sub(1)
                    .ok_or(DecodeError("delta chain is longer than it says"))?;
                match e {
                    StoreEntry::Park { journal, .. } => {
                        parts.push(journal);
                        break;
                    }
                    StoreEntry::Delta { prev, journal, .. } => {
                        parts.push(journal);
                        at = prev;
                    }
                    _ => {
                        return Err(DecodeError(
                            "delta frame extends a frame that is not a park",
                        ));
                    }
                }
            }
            if left != 0 {
                return Err(DecodeError("delta chain is shorter than it says"));
            }
            let mut out = Vec::with_capacity(parts.iter().map(Vec::len).sum());
            for p in parts.iter().rev() {
                out.extend_from_slice(p);
            }
            Ok((out, chain + u64::from(frame_len), depth))
        }
    }
}

/// One loaded run in a checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointRun {
    pub run: RunId,
    pub last_lsn: Lsn,
    /// Journal prefix in the run store.
    pub blob: Option<BlobRef>,
    /// Canonical records after the prefix.
    pub journal: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    pub shard: u32,
    pub shards: u32,
    /// Every record at or before this LSN is reflected here or in the store.
    pub lsn: Lsn,
    pub next_run: RunId,
    pub store_generation: u64,
    pub store_len: u64,
    pub created_ms: u64,
    pub runs: Vec<CheckpointRun>,
    /// Messages to other shards not yet acknowledged (host-defined
    /// encoding). The log after `lsn` regenerates newer ones.
    pub outbox: Vec<u8>,
    /// Log format 3: the sequence of the first batch of the shared log that
    /// can hold this shard's records after `lsn`. 0 in format 2, where each
    /// shard's segments start after its checkpoint on their own.
    pub log_position: u64,
    /// Checkpoint format 3: the `Index` frames in the run store prefix that
    /// together list every parked run, oldest first (later records for a run
    /// supersede earlier ones). `None`: recovery scans the store.
    pub index: Option<Vec<BlobRef>>,
    /// Bytes of the store prefix no longer in use, for compaction.
    pub obsolete: u64,
    /// Format 3: how many runs the index lists (it may hold more records,
    /// superseded ones), so recovery sizes the table once.
    pub index_runs: u64,
}

impl Checkpoint {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(CHECKPOINT_MAGIC);
        put_u32(&mut out, CHECKPOINT_FORMAT);
        put_u32(&mut out, self.shard);
        put_u32(&mut out, self.shards);
        put_u64(&mut out, self.lsn);
        put_u64(&mut out, self.next_run);
        put_u64(&mut out, self.store_generation);
        put_u64(&mut out, self.store_len);
        put_u64(&mut out, self.created_ms);
        put_varint(&mut out, self.runs.len() as u64);
        for r in &self.runs {
            put_varint(&mut out, r.run);
            put_varint(&mut out, r.last_lsn);
            match r.blob {
                None => out.push(0),
                Some(b) => {
                    out.push(1);
                    put_varint(&mut out, b.offset);
                    put_varint(&mut out, u64::from(b.len));
                }
            }
            put_bytes(&mut out, &r.journal);
        }
        put_bytes(&mut out, &self.outbox);
        put_varint(&mut out, self.log_position);
        match &self.index {
            None => out.push(0),
            Some(frames) => {
                out.push(1);
                put_varint(&mut out, frames.len() as u64);
                for b in frames {
                    put_varint(&mut out, b.offset);
                    put_varint(&mut out, u64::from(b.len));
                }
            }
        }
        put_varint(&mut out, self.obsolete);
        put_varint(&mut out, self.index_runs);
        let crc = crc32c::crc32c(&out);
        put_u32(&mut out, crc);
        out
    }

    pub fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        if bytes.len() < 12 {
            return Err(DecodeError("short checkpoint"));
        }
        let (body, crc) = bytes.split_at(bytes.len() - 4);
        if crc32c::crc32c(body) != u32::from_le_bytes([crc[0], crc[1], crc[2], crc[3]]) {
            return Err(DecodeError("checkpoint checksum mismatch"));
        }
        let mut r = Reader::new(body);
        if r.take(8)? != CHECKPOINT_MAGIC {
            return Err(DecodeError("not a checkpoint"));
        }
        let format = r.u32()?;
        if !(CHECKPOINT_FORMAT_MIN..=CHECKPOINT_FORMAT).contains(&format) {
            return Err(DecodeError("checkpoint format is not one this build reads"));
        }
        let shard = r.u32()?;
        let shards = r.u32()?;
        let lsn = r.u64()?;
        let next_run = r.u64()?;
        let store_generation = r.u64()?;
        let store_len = r.u64()?;
        let created_ms = r.u64()?;
        let n = r.varint()?;
        let mut runs = Vec::with_capacity(n.min(1 << 20) as usize);
        for _ in 0..n {
            let run = r.varint()?;
            let last_lsn = r.varint()?;
            let blob = match r.u8()? {
                0 => None,
                1 => Some(BlobRef {
                    offset: r.varint()?,
                    len: r.varint_u32()?,
                }),
                _ => return Err(DecodeError("bad blob flag")),
            };
            runs.push(CheckpointRun {
                run,
                last_lsn,
                blob,
                journal: r.blob()?.to_vec(),
            });
        }
        let outbox = r.blob()?.to_vec();
        let (log_position, index, obsolete, index_runs) = if format == 2 {
            (if r.is_empty() { 0 } else { r.varint()? }, None, 0, 0)
        } else {
            let position = r.varint()?;
            let index = match r.u8()? {
                0 => None,
                1 => {
                    let n = r.varint()?;
                    let mut frames = Vec::with_capacity(n.min(1 << 16) as usize);
                    for _ in 0..n {
                        frames.push(BlobRef {
                            offset: r.varint()?,
                            len: r.varint_u32()?,
                        });
                    }
                    Some(frames)
                }
                _ => return Err(DecodeError("bad index flag")),
            };
            (position, index, r.varint()?, r.varint()?)
        };
        if !r.is_empty() {
            return Err(DecodeError("trailing bytes in checkpoint"));
        }
        Ok(Self {
            shard,
            shards,
            lsn,
            next_run,
            store_generation,
            store_len,
            created_ms,
            runs,
            outbox,
            log_position,
            index,
            obsolete,
            index_runs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A run's journal is as long as its history: a frame or a checkpoint
    /// holding more than one payload's bound reads back (the store wrote
    /// such a frame and then refused to read it).
    #[test]
    fn journals_longer_than_a_payload_read_back() {
        let journal = vec![5u8; crate::wire::MAX_PAYLOAD + 4096];
        let park = StoreEntry::Park {
            run: 3,
            last_lsn: 99,
            id_hash: 42,
            timers: vec![(2, TimerKind::Sleep, 1000)],
            journal: journal.clone(),
        };
        let frame = encode_entry(&park);
        assert_eq!(read_entry(&frame).unwrap(), park);
        let (run, last_lsn, whole) = read_whole_journal(&frame).unwrap().unwrap();
        assert_eq!((run, last_lsn, whole.len()), (3, 99, journal.len()));
        let done = StoreEntry::Done {
            run: 4,
            last_lsn: 100,
            id_hash: 43,
            status: Status::Ok,
            finished_ms: 5,
            journal: journal.clone(),
        };
        assert_eq!(read_entry(&encode_entry(&done)).unwrap(), done);
        let delta = StoreEntry::Delta {
            run: 3,
            last_lsn: 120,
            id_hash: 42,
            timers: Vec::new(),
            prev: BlobRef {
                offset: 16,
                len: 30,
            },
            depth: 1,
            chain: 60,
            journal: journal.clone(),
        };
        assert_eq!(read_entry(&encode_entry(&delta)).unwrap(), delta);
        let cp = Checkpoint {
            shard: 0,
            shards: 1,
            lsn: 7,
            next_run: 5,
            store_generation: 1,
            store_len: 16,
            created_ms: 9,
            runs: vec![CheckpointRun {
                run: 3,
                last_lsn: 7,
                blob: None,
                journal,
            }],
            outbox: vec![1; crate::wire::MAX_PAYLOAD + 1],
            log_position: 2,
            index: None,
            obsolete: 0,
            index_runs: 0,
        };
        assert_eq!(Checkpoint::decode(&cp.encode()).unwrap(), cp);
    }

    #[test]
    fn store_round_trip() {
        let entries = vec![
            StoreEntry::Park {
                run: 3,
                last_lsn: 99,
                id_hash: 42,
                timers: vec![(2, TimerKind::Sleep, 1000)],
                journal: vec![1, 2, 3],
            },
            StoreEntry::Done {
                run: 4,
                last_lsn: 100,
                id_hash: 43,
                status: Status::Ok,
                finished_ms: 5,
                journal: vec![9],
            },
            StoreEntry::Tombstone { run: 4 },
            StoreEntry::Delta {
                run: 3,
                last_lsn: 120,
                id_hash: 42,
                timers: vec![(5, TimerKind::Retry, 2000), (0, TimerKind::Deadline, 9)],
                prev: BlobRef {
                    offset: 16,
                    len: 30,
                },
                depth: 1,
                chain: 60,
                journal: vec![4, 5],
            },
            StoreEntry::Index {
                records: vec![7; 64],
            },
        ];
        let mut file = store_header();
        for e in &entries {
            file.extend_from_slice(&encode_entry(e));
        }
        let mut seen = Vec::new();
        scan_store(&file, |off, len, e| {
            assert_eq!(
                read_entry(&file[off as usize..(off + u64::from(len)) as usize]).unwrap(),
                e
            );
            seen.push(e)
        })
        .unwrap();
        assert_eq!(seen, entries);
        let mut bad = file.clone();
        let last = bad.len() - 1;
        bad[last] ^= 1;
        assert!(scan_store(&bad, |_, _, _| {}).is_err());
        // A format 2 store still opens; format 4 does not.
        let mut old = file.clone();
        old[8..12].copy_from_slice(&2u32.to_le_bytes());
        let crc = crc32c::crc32c(&old[..12]);
        old[12..16].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(store_format(&old), Ok(2));
        old[8..12].copy_from_slice(&4u32.to_le_bytes());
        let crc = crc32c::crc32c(&old[..12]);
        old[12..16].copy_from_slice(&crc.to_le_bytes());
        assert!(store_format(&old).is_err());
    }

    #[test]
    fn delta_chains_read_oldest_first() {
        let mut file = store_header();
        let put = |e: &StoreEntry, file: &mut Vec<u8>| {
            let f = encode_entry(e);
            let at = BlobRef {
                offset: file.len() as u64,
                len: f.len() as u32,
            };
            file.extend_from_slice(&f);
            at
        };
        let base = put(
            &StoreEntry::Park {
                run: 1,
                last_lsn: 3,
                id_hash: 9,
                timers: vec![],
                journal: vec![1, 2],
            },
            &mut file,
        );
        let mid = put(
            &StoreEntry::Delta {
                run: 1,
                last_lsn: 5,
                id_hash: 9,
                timers: vec![],
                prev: base,
                depth: 1,
                chain: 0,
                journal: vec![3],
            },
            &mut file,
        );
        let top = StoreEntry::Delta {
            run: 1,
            last_lsn: 7,
            id_hash: 9,
            timers: vec![],
            prev: mid,
            depth: 2,
            chain: 99,
            journal: vec![4, 5],
        };
        let frame_at = |b: BlobRef| {
            read_entry(&file[b.offset as usize..(b.offset + u64::from(b.len)) as usize])
        };
        let (journal, chain, depth) = chain_journal(top.clone(), 10, &mut |b| frame_at(b)).unwrap();
        assert_eq!((journal, chain, depth), (vec![1, 2, 3, 4, 5], 109, 2));
        // A chain that claims more frames than it has is refused.
        let long = StoreEntry::Delta {
            run: 1,
            last_lsn: 7,
            id_hash: 9,
            timers: vec![],
            prev: mid,
            depth: 3,
            chain: 99,
            journal: vec![],
        };
        assert!(chain_journal(long, 10, &mut |b| frame_at(b)).is_err());
        let other = StoreEntry::Delta {
            run: 2,
            last_lsn: 7,
            id_hash: 9,
            timers: vec![],
            prev: base,
            depth: 1,
            chain: 1,
            journal: vec![],
        };
        assert!(chain_journal(other, 10, &mut |b| frame_at(b)).is_err());
    }

    fn sample() -> Checkpoint {
        Checkpoint {
            shard: 1,
            shards: 4,
            lsn: 77,
            next_run: 10,
            store_generation: 2,
            store_len: 4096,
            created_ms: 1,
            runs: vec![CheckpointRun {
                run: 9,
                last_lsn: 70,
                blob: Some(BlobRef {
                    offset: 16,
                    len: 40,
                }),
                journal: vec![5, 6],
            }],
            log_position: 1234,
            outbox: vec![1, 2, 3],
            index: Some(vec![
                BlobRef {
                    offset: 100,
                    len: 70,
                },
                BlobRef {
                    offset: 170,
                    len: 38,
                },
            ]),
            obsolete: 555,
            index_runs: 77,
        }
    }

    #[test]
    fn checkpoint_round_trip() {
        for c in [
            sample(),
            Checkpoint {
                index: None,
                log_position: 0,
                ..sample()
            },
        ] {
            let bytes = c.encode();
            assert_eq!(Checkpoint::decode(&bytes).unwrap(), c);
            for cut in 0..bytes.len() {
                assert!(Checkpoint::decode(&bytes[..cut]).is_err());
            }
        }
    }

    /// Format 2, as the previous build wrote it.
    fn encode_v2(c: &Checkpoint) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(CHECKPOINT_MAGIC);
        put_u32(&mut out, 2);
        put_u32(&mut out, c.shard);
        put_u32(&mut out, c.shards);
        put_u64(&mut out, c.lsn);
        put_u64(&mut out, c.next_run);
        put_u64(&mut out, c.store_generation);
        put_u64(&mut out, c.store_len);
        put_u64(&mut out, c.created_ms);
        put_varint(&mut out, c.runs.len() as u64);
        for r in &c.runs {
            put_varint(&mut out, r.run);
            put_varint(&mut out, r.last_lsn);
            match r.blob {
                None => out.push(0),
                Some(b) => {
                    out.push(1);
                    put_varint(&mut out, b.offset);
                    put_varint(&mut out, u64::from(b.len));
                }
            }
            put_bytes(&mut out, &r.journal);
        }
        put_bytes(&mut out, &c.outbox);
        if c.log_position != 0 {
            put_varint(&mut out, c.log_position);
        }
        let crc = crc32c::crc32c(&out);
        put_u32(&mut out, crc);
        out
    }

    #[test]
    fn format_2_checkpoints_open_without_an_index() {
        for position in [0, 1234] {
            let c = Checkpoint {
                index: None,
                obsolete: 0,
                index_runs: 0,
                log_position: position,
                ..sample()
            };
            assert_eq!(Checkpoint::decode(&encode_v2(&c)).unwrap(), c);
        }
    }
}
