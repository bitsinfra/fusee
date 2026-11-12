//! The backup's state: what every object holds, the restore points, which
//! objects a point needs, and what retention may drop. Pure: no I/O.
//!
//! On the target the state is a snapshot (`state/<version>.json`) plus the
//! events since it, both named by the root manifest ([`Root`]). The
//! shipper applies each event to its own copy as it records it, and a
//! restore replays the same events onto the snapshot.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Version of the manifest, snapshot and object layout.
pub const FORMAT: u32 = 1;

/// Objects (and merged chunks) are at most this large.
pub const MAX_OBJECT: u64 = 16 << 20;
/// Chunks of one level merged into one of the next.
pub const FANOUT: usize = 8;

/// Points newer than this are all kept; older ones one per minute.
pub const FINE_MS: u64 = 10 * 60_000;
/// Points older than this keep using a checkpoint up to an hour old.
pub const COARSE_MS: u64 = 3_600_000;

/// A restore point: the log through batch `.1`, exactly what had been
/// acknowledged at Unix time `.0` (milliseconds).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Point(pub u64, pub u64);

impl Point {
    pub fn ms(&self) -> u64 {
        self.0
    }
    pub fn seq(&self) -> u64 {
        self.1
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct State {
    pub shards: u32,
    pub timelines: Vec<Timeline>,
}

/// One history of one data directory. A directory restored from a backup
/// starts a new one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Timeline {
    pub id: String,
    pub created_ms: u64,
    #[serde(default)]
    pub segments: Vec<Segment>,
    #[serde(default)]
    pub checkpoints: Vec<CheckpointEntry>,
    #[serde(default)]
    pub stores: Vec<StoreChunk>,
    #[serde(default)]
    pub archives: Vec<ArchiveEntry>,
    #[serde(default)]
    pub points: Vec<Point>,
}

/// One shared-log segment: its uploaded prefix, in chunks from offset 0
/// (the header block) through whole batches.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Segment {
    pub number: u64,
    pub base_seq: u64,
    /// The file's length, which a restore gives it again.
    pub len: u64,
    /// The chain has moved past it and all of it is uploaded.
    pub sealed: bool,
    pub chunks: Vec<LogChunk>,
}

impl Segment {
    pub fn end(&self) -> u64 {
        self.chunks.last().map_or(0, |c| c.end)
    }

    /// The last batch uploaded.
    pub fn last_seq(&self) -> Option<u64> {
        self.chunks.last().map(|c| c.last_seq)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogChunk {
    pub key: String,
    pub offset: u64,
    pub end: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    /// Chain checksum of the last batch.
    pub last_checksum: u64,
    pub crc: u32,
    #[serde(default)]
    pub level: u32,
    /// The blobs the chunk's batches reference (offloaded records), in
    /// objects of their own; a merged chunk keeps its parts' objects.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blobs: Vec<BlobPack>,
}

/// An object holding blobs of the log's blob area, each as `file:u64
/// offset:u64 len:u64` (little-endian) and its bytes: what a restore writes
/// back to `wal/blobs/<file>.blob` at `offset`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BlobPack {
    pub key: String,
    pub bytes: u64,
    pub crc: u32,
}

impl LogChunk {
    /// The chunk's object and its blob objects.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.key.as_str()).chain(self.blobs.iter().map(|b| b.key.as_str()))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CheckpointEntry {
    pub shard: u32,
    pub key: String,
    pub lsn: u64,
    /// The first batch that can hold the shard's records after `lsn`.
    pub log_position: u64,
    /// The log's acknowledged position when the backup took the
    /// checkpoint: at or past the batch holding `lsn`, and past every
    /// acknowledgment the checkpoint reflects. Usable for restore points
    /// from this batch on.
    pub usable_from: u64,
    pub created_ms: u64,
    pub bytes: u64,
    pub crc: u32,
    pub store_generation: u64,
    pub store_len: u64,
    /// Which upload of that generation holds its prefix.
    pub store_epoch: u64,
}

/// A byte range of one shard's run store generation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoreChunk {
    pub shard: u32,
    pub generation: u64,
    /// Uploads of one generation are numbered: if the file turns out to
    /// differ from what was uploaded (it was rewritten after a restore or a
    /// damaged checkpoint), the next upload starts a new epoch from 0.
    pub epoch: u64,
    pub key: String,
    pub offset: u64,
    pub end: u64,
    pub crc: u32,
    #[serde(default)]
    pub level: u32,
}

/// An immutable history archive (`shard-NNN/archive/*.arc`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ArchiveEntry {
    pub shard: u32,
    pub name: String,
    /// The checkpoint LSN it was written with: a checkpoint at or after it
    /// covers it.
    pub through: u64,
    pub key: String,
    pub bytes: u64,
    pub crc: u32,
}

/// One change to the state, as recorded in the manifest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Event {
    Timeline {
        id: String,
        created_ms: u64,
    },
    /// A log chunk; the first one of a segment adds the segment.
    Chunk {
        tl: String,
        number: u64,
        base_seq: u64,
        len: u64,
        chunk: LogChunk,
    },
    /// A chunk replacing the consecutive chunks of the segment it covers.
    MergeLog {
        tl: String,
        number: u64,
        chunk: LogChunk,
    },
    Seal {
        tl: String,
        number: u64,
    },
    Checkpoint {
        tl: String,
        entry: CheckpointEntry,
    },
    Store {
        tl: String,
        chunk: StoreChunk,
    },
    MergeStore {
        tl: String,
        chunk: StoreChunk,
    },
    Archive {
        tl: String,
        entry: ArchiveEntry,
    },
    Point {
        tl: String,
        point: Point,
    },
}

/// `manifest.json`: the snapshot and the events after it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Root {
    pub format: u32,
    pub shards: u32,
    /// Increments with every manifest written.
    pub version: u64,
    pub updated_ms: u64,
    pub snapshot: Option<String>,
    #[serde(default)]
    pub events: Vec<Event>,
}

/// `state/<version>.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub format: u32,
    pub version: u64,
    pub state: State,
}

pub fn check_format(format: u32) -> Result<(), String> {
    if format == FORMAT {
        Ok(())
    } else {
        Err(format!(
            "backup format {format} is not the one this build reads ({FORMAT}); use the Fusee \
             version that wrote it"
        ))
    }
}

/// Replaces the consecutive chunks exactly covering `[offset, end)`.
fn merge_into<T>(
    chunks: &mut Vec<T>,
    offset: u64,
    end: u64,
    range: impl Fn(&T) -> (u64, u64),
    merged: T,
) -> Result<(), String> {
    let first = chunks.iter().position(|c| range(c).0 == offset);
    let last = chunks.iter().position(|c| range(c).1 == end);
    match (first, last) {
        (Some(a), Some(b)) if a <= b => {
            chunks.splice(a..=b, std::iter::once(merged));
            Ok(())
        }
        _ => Err(format!(
            "merged chunk {offset}..{end} does not cover whole chunks"
        )),
    }
}

impl State {
    pub fn timeline(&self, id: &str) -> Option<&Timeline> {
        self.timelines.iter().find(|t| t.id == id)
    }

    fn timeline_mut(&mut self, id: &str) -> Result<&mut Timeline, String> {
        self.timelines
            .iter_mut()
            .find(|t| t.id == id)
            .ok_or_else(|| format!("event for unknown timeline {id}"))
    }

    /// Applies one event; fails on an event that does not fit (a damaged
    /// or foreign manifest).
    pub fn apply(&mut self, e: &Event) -> Result<(), String> {
        match e {
            Event::Timeline { id, created_ms } => {
                if self.timeline(id).is_some() {
                    return Err(format!("timeline {id} exists"));
                }
                self.timelines.push(Timeline {
                    id: id.clone(),
                    created_ms: *created_ms,
                    segments: Vec::new(),
                    checkpoints: Vec::new(),
                    stores: Vec::new(),
                    archives: Vec::new(),
                    points: Vec::new(),
                });
            }
            Event::Chunk {
                tl,
                number,
                base_seq,
                len,
                chunk,
            } => {
                let t = self.timeline_mut(tl)?;
                if t.segments.last().is_none_or(|s| s.number != *number) {
                    if t.segments.last().is_some_and(|s| {
                        s.number >= *number || s.last_seq().is_some_and(|l| l >= *base_seq)
                    }) {
                        return Err(format!("segment {number} is out of order"));
                    }
                    t.segments.push(Segment {
                        number: *number,
                        base_seq: *base_seq,
                        len: *len,
                        sealed: false,
                        chunks: Vec::new(),
                    });
                }
                let s = t.segments.last_mut().expect("pushed");
                let expect_seq = s.last_seq().map_or(s.base_seq, |l| l + 1);
                if s.sealed
                    || chunk.offset != s.end()
                    || chunk.end <= chunk.offset
                    || chunk.end > s.len
                    || chunk.first_seq != expect_seq
                    || chunk.last_seq < chunk.first_seq
                {
                    return Err(format!(
                        "log chunk {}..{} does not continue segment {number}",
                        chunk.offset, chunk.end
                    ));
                }
                s.chunks.push(chunk.clone());
            }
            Event::MergeLog { tl, number, chunk } => {
                let t = self.timeline_mut(tl)?;
                let s = t
                    .segments
                    .iter_mut()
                    .find(|s| s.number == *number)
                    .ok_or_else(|| format!("merge into unknown segment {number}"))?;
                merge_into(
                    &mut s.chunks,
                    chunk.offset,
                    chunk.end,
                    |c| (c.offset, c.end),
                    chunk.clone(),
                )?;
            }
            Event::Seal { tl, number } => {
                let t = self.timeline_mut(tl)?;
                let s = t
                    .segments
                    .iter_mut()
                    .find(|s| s.number == *number)
                    .ok_or_else(|| format!("seal of unknown segment {number}"))?;
                s.sealed = true;
            }
            Event::Checkpoint { tl, entry } => {
                let t = self.timeline_mut(tl)?;
                if !t.checkpoints.iter().any(|c| c.key == entry.key) {
                    t.checkpoints.push(entry.clone());
                }
            }
            Event::Store { tl, chunk } => {
                let t = self.timeline_mut(tl)?;
                let end = store_end(&t.stores, chunk.shard, chunk.generation, chunk.epoch);
                if chunk.offset != end || chunk.end <= chunk.offset {
                    return Err(format!(
                        "store chunk {}..{} does not continue shard {} generation {}",
                        chunk.offset, chunk.end, chunk.shard, chunk.generation
                    ));
                }
                t.stores.push(chunk.clone());
            }
            Event::MergeStore { tl, chunk } => {
                let t = self.timeline_mut(tl)?;
                let mut mine: Vec<StoreChunk> = Vec::new();
                let mut rest = Vec::new();
                for c in std::mem::take(&mut t.stores) {
                    if (c.shard, c.generation, c.epoch)
                        == (chunk.shard, chunk.generation, chunk.epoch)
                    {
                        mine.push(c);
                    } else {
                        rest.push(c);
                    }
                }
                let r = merge_into(
                    &mut mine,
                    chunk.offset,
                    chunk.end,
                    |c| (c.offset, c.end),
                    chunk.clone(),
                );
                rest.extend(mine);
                t.stores = rest;
                r?;
            }
            Event::Archive { tl, entry } => {
                let t = self.timeline_mut(tl)?;
                if !t.archives.iter().any(|a| a.key == entry.key) {
                    t.archives.push(entry.clone());
                }
            }
            Event::Point { tl, point } => {
                let t = self.timeline_mut(tl)?;
                if t.points
                    .last()
                    .is_some_and(|p| p.0 > point.0 || p.1 >= point.1)
                {
                    return Err(format!("restore point {point:?} is out of order"));
                }
                t.points.push(*point);
            }
        }
        Ok(())
    }

    /// Every object the state names.
    pub fn keys(&self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for t in &self.timelines {
            out.extend(t.keys());
        }
        out
    }
}

/// End of the chunks uploaded for one store generation and epoch.
pub fn store_end(stores: &[StoreChunk], shard: u32, generation: u64, epoch: u64) -> u64 {
    stores
        .iter()
        .filter(|c| (c.shard, c.generation, c.epoch) == (shard, generation, epoch))
        .map(|c| c.end)
        .max()
        .unwrap_or(0)
}

/// Which consecutive chunks to merge, if any: the last [`FANOUT`] chunks
/// when they share a level and together are at most [`MAX_OBJECT`]
/// bytes. `chunks` are `(offset, end, level)`, in order.
pub fn merge_plan(chunks: &[(u64, u64, u32)]) -> Option<std::ops::Range<usize>> {
    if chunks.len() < FANOUT {
        return None;
    }
    let tail = &chunks[chunks.len() - FANOUT..];
    let level = tail[0].2;
    let bytes = tail[FANOUT - 1].1 - tail[0].0;
    (tail.iter().all(|c| c.2 == level) && bytes <= MAX_OBJECT)
        .then(|| chunks.len() - FANOUT..chunks.len())
}

/// What a restore of one point needs.
#[derive(Clone, Debug)]
pub struct Plan {
    pub timeline: String,
    pub point: Point,
    /// Segments in chain order. The last one is cut after the point's batch.
    pub segments: Vec<PlanSegment>,
    /// Per shard.
    pub shards: Vec<PlanShard>,
}

#[derive(Clone, Debug)]
pub struct PlanSegment {
    pub number: u64,
    pub base_seq: u64,
    pub len: u64,
    pub chunks: Vec<LogChunk>,
    /// Keep batches through this one only.
    pub cut: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct PlanShard {
    /// `None`: the shard replays the log from its first record.
    pub checkpoint: Option<CheckpointEntry>,
    pub store: Vec<StoreChunk>,
    pub archives: Vec<ArchiveEntry>,
}

impl Plan {
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.segments
            .iter()
            .flat_map(|s| s.chunks.iter().flat_map(LogChunk::keys))
            .chain(self.shards.iter().flat_map(|s| {
                s.checkpoint
                    .iter()
                    .map(|c| c.key.as_str())
                    .chain(s.store.iter().map(|c| c.key.as_str()))
                    .chain(s.archives.iter().map(|a| a.key.as_str()))
            }))
    }

    pub fn bytes(&self) -> u64 {
        let log: u64 = self
            .segments
            .iter()
            .flat_map(|s| &s.chunks)
            .map(|c| c.end - c.offset + c.blobs.iter().map(|b| b.bytes).sum::<u64>())
            .sum();
        let shards: u64 = self
            .shards
            .iter()
            .map(|s| {
                s.checkpoint.as_ref().map_or(0, |c| c.bytes)
                    + s.store.iter().map(|c| c.end - c.offset).sum::<u64>()
                    + s.archives.iter().map(|a| a.bytes).sum::<u64>()
            })
            .sum();
        log + shards
    }
}

impl Timeline {
    /// The last batch uploaded.
    pub fn uploaded_through(&self) -> u64 {
        self.segments
            .iter()
            .filter_map(|s| s.last_seq())
            .max()
            .unwrap_or(0)
    }

    pub fn keys(&self) -> impl Iterator<Item = String> + '_ {
        self.segments
            .iter()
            .flat_map(|s| s.chunks.iter().flat_map(|c| c.keys().map(str::to_owned)))
            .chain(self.checkpoints.iter().map(|c| c.key.clone()))
            .chain(self.stores.iter().map(|c| c.key.clone()))
            .chain(self.archives.iter().map(|a| a.key.clone()))
    }

    /// The uploaded log through batch `seq`, as segment indexes: the one
    /// holding it, and the first of the run of segments before it with no
    /// gap between them.
    fn run(&self, seq: u64) -> Option<(usize, usize)> {
        let k = self.segments.iter().rposition(|s| s.base_seq <= seq)?;
        if self.segments[k].last_seq().is_none_or(|l| l < seq) {
            return None;
        }
        let mut f = k;
        while f > 0 {
            let prev = &self.segments[f - 1];
            if prev.sealed
                && prev.chunks.first().is_some_and(|c| c.offset == 0)
                && prev.last_seq() == Some(self.segments[f].base_seq - 1)
            {
                f -= 1;
            } else {
                break;
            }
        }
        Some((f, k))
    }

    /// Checkpoints of `shard` usable for a point at `seq` whose log run
    /// starts at batch `first`, newest first.
    fn usable(&self, shard: u32, seq: u64, first: u64) -> impl Iterator<Item = &CheckpointEntry> {
        let mut v: Vec<&CheckpointEntry> = self
            .checkpoints
            .iter()
            .filter(|c| {
                c.shard == shard && c.usable_from <= seq && (first <= 1 || c.log_position >= first)
            })
            .collect();
        v.sort_by_key(|c| std::cmp::Reverse((c.lsn, c.created_ms)));
        v.into_iter()
    }

    /// What restoring `point` needs, with the newest usable checkpoint of
    /// each shard.
    pub fn plan(&self, shards: u32, point: Point) -> Result<Plan, String> {
        let (f, _) = self
            .run(point.1)
            .ok_or_else(|| format!("the log through batch {} is not in the backup", point.1))?;
        let first = self.segments[f].base_seq;
        let chosen = (0..shards)
            .map(|s| self.usable(s, point.1, first).next())
            .collect::<Vec<_>>();
        self.plan_with(point, &chosen)
    }

    /// What restoring `point` needs with these checkpoints.
    pub fn plan_with(
        &self,
        point: Point,
        chosen: &[Option<&CheckpointEntry>],
    ) -> Result<Plan, String> {
        let seq = point.1;
        if seq == 0 {
            return Err("the restore point holds no batch".into());
        }
        let (f, k) = self
            .run(seq)
            .ok_or_else(|| format!("the log through batch {seq} is not in the backup"))?;
        let first = self.segments[f].base_seq;
        let mut start = seq;
        let mut out = Vec::new();
        for (shard, cp) in chosen.iter().enumerate() {
            let shard = shard as u32;
            let mut ps = PlanShard::default();
            match cp {
                Some(cp) => {
                    start = start.min(cp.log_position.max(1));
                    if cp.store_generation != 0 {
                        let mut store: Vec<&StoreChunk> = self
                            .stores
                            .iter()
                            .filter(|c| {
                                (c.shard, c.generation, c.epoch)
                                    == (shard, cp.store_generation, cp.store_epoch)
                                    && c.offset < cp.store_len
                            })
                            .collect();
                        store.sort_by_key(|c| c.offset);
                        let mut at = 0;
                        for c in &store {
                            if c.offset != at {
                                break;
                            }
                            at = c.end;
                        }
                        if at < cp.store_len {
                            return Err(format!(
                                "shard {shard}: run store generation {} is uploaded through \
                                 byte {at} of {}",
                                cp.store_generation, cp.store_len
                            ));
                        }
                        ps.store = store.into_iter().cloned().collect();
                    }
                    ps.archives = self
                        .archives
                        .iter()
                        .filter(|a| a.shard == shard && a.through <= cp.lsn)
                        .cloned()
                        .collect();
                    ps.checkpoint = Some((*cp).clone());
                }
                None if first <= 1 => start = 1,
                None => {
                    return Err(format!(
                        "shard {shard} has no checkpoint usable at batch {seq} that the log \
                         from batch {first} can continue"
                    ));
                }
            }
            out.push(ps);
        }
        let i0 = (f..=k)
            .rev()
            .find(|&i| self.segments[i].base_seq <= start)
            .unwrap_or(f);
        let segments = (i0..=k)
            .map(|i| {
                let s = &self.segments[i];
                PlanSegment {
                    number: s.number,
                    base_seq: s.base_seq,
                    len: s.len,
                    chunks: s
                        .chunks
                        .iter()
                        .filter(|c| i < k || c.first_seq <= seq)
                        .cloned()
                        .collect(),
                    cut: (i == k).then_some(seq),
                }
            })
            .collect();
        Ok(Plan {
            timeline: self.id.clone(),
            point,
            segments,
            shards: out,
        })
    }

    /// Restore points with a complete plan.
    pub fn restorable(&self, shards: u32) -> impl Iterator<Item = Point> + '_ {
        self.points
            .iter()
            .copied()
            .filter(move |p| self.plan(shards, *p).is_ok())
    }
}

/// The newest restore point at or before `at_ms` (any time if `None`),
/// across timelines (only `timeline` if given), that can be restored.
pub fn choose(state: &State, at_ms: Option<u64>, timeline: Option<&str>) -> Result<Plan, String> {
    let mut candidates: Vec<(Point, usize)> = state
        .timelines
        .iter()
        .enumerate()
        .filter(|(_, t)| timeline.is_none_or(|id| t.id == id))
        .flat_map(|(i, t)| t.points.iter().map(move |p| (*p, i)))
        .filter(|(p, _)| at_ms.is_none_or(|at| p.0 <= at))
        .collect();
    if let Some(id) = timeline
        && state.timeline(id).is_none()
    {
        return Err(format!("the backup has no timeline {id}"));
    }
    candidates.sort_by_key(|(p, i)| std::cmp::Reverse((p.0, *i, p.1)));
    let mut last_err = None;
    for (p, i) in candidates {
        match state.timelines[i].plan(state.shards, p) {
            Ok(plan) => return Ok(plan),
            Err(e) => {
                last_err.get_or_insert(e);
            }
        }
    }
    Err(match (last_err, at_ms) {
        (Some(e), _) => format!("no restore point can be restored: {e}"),
        (None, Some(at)) => format!(
            "the backup has no restore point at or before {}",
            super::format_time(at)
        ),
        (None, None) => "the backup has no restore point yet".into(),
    })
}

/// What [`State::retain`] keeps of the timeline the shipper is writing.
pub struct Current<'a> {
    pub timeline: &'a str,
    /// The last batch uploaded: a point there is about to be possible.
    pub uploaded: u64,
    /// `(shard, generation, epoch)` the shipper is appending to.
    pub stores: &'a [(u32, u64, u64)],
}

impl State {
    /// Drops restore points older than `window_ms` (and thins old ones),
    /// then every entry no remaining point needs. Returns the keys of the
    /// objects dropped. The timeline being written keeps what its next
    /// restore point needs.
    pub fn retain(
        &mut self,
        now_ms: u64,
        window_ms: u64,
        current: Option<Current<'_>>,
    ) -> Vec<String> {
        let shards = self.shards;
        let mut removed = Vec::new();
        let cutoff = now_ms.saturating_sub(window_ms);
        let mut keep_timelines = Vec::new();
        for mut t in std::mem::take(&mut self.timelines) {
            let cur = current.as_ref().filter(|c| c.timeline == t.id);
            // Points: drop expired, thin old ones (the last point of each
            // minute), drop those that cannot be restored.
            let mut points: Vec<Point> = Vec::new();
            let n = t.points.len();
            for (i, p) in t.points.iter().enumerate() {
                if p.0 < cutoff {
                    continue;
                }
                let next_same_minute = t
                    .points
                    .get(i + 1)
                    .is_some_and(|q| q.0 / 60_000 == p.0 / 60_000);
                if i + 1 < n && p.0 + FINE_MS < now_ms && next_same_minute {
                    continue;
                }
                points.push(*p);
            }
            // Checkpoints per point, reusing an older one for old points.
            let mut need_cps: BTreeSet<String> = BTreeSet::new();
            let mut need_segments: BTreeSet<u64> = BTreeSet::new();
            let mut prev: Vec<Option<CheckpointEntry>> = vec![None; shards as usize];
            let mut kept_points = Vec::new();
            let mut all: Vec<(Point, bool)> = points.iter().map(|p| (*p, false)).collect();
            if let Some(c) = cur
                && c.uploaded > 0
                && all.last().is_none_or(|(p, _)| p.1 < c.uploaded)
            {
                all.push((Point(now_ms, c.uploaded), true));
            }
            // Whether the newest point (real or the one about to be
            // possible) can be restored.
            let mut last_ok = false;
            for (p, virtual_point) in all {
                last_ok = false;
                let Some((f, _)) = t.run(p.1) else { continue };
                let first = t.segments[f].base_seq;
                let old = p.0 + COARSE_MS < now_ms && !virtual_point;
                let chosen: Vec<Option<&CheckpointEntry>> = (0..shards)
                    .map(|s| {
                        let reuse = prev[s as usize].as_ref().and_then(|c| {
                            let ok = old
                                && c.usable_from <= p.1
                                && (first <= 1 || c.log_position >= first)
                                && p.0.saturating_sub(c.created_ms) < COARSE_MS;
                            ok.then(|| t.checkpoints.iter().find(|x| x.key == c.key))
                                .flatten()
                        });
                        reuse.or_else(|| t.usable(s, p.1, first).next())
                    })
                    .collect();
                let Ok(plan) = t.plan_with(p, &chosen) else {
                    continue;
                };
                for (s, c) in chosen.iter().enumerate() {
                    prev[s] = c.cloned();
                }
                for c in chosen.iter().flatten() {
                    need_cps.insert(c.key.clone());
                }
                need_segments.extend(plan.segments.iter().map(|s| s.number));
                last_ok = true;
                if !virtual_point {
                    kept_points.push(p);
                }
            }
            if cur.is_some() && !last_ok {
                // The timeline being written has no restorable point yet
                // (its first checkpoints are on their way): keep all of it
                // but expired points.
                t.points.retain(|p| p.0 >= cutoff);
                keep_timelines.push(t);
                continue;
            }
            if cur.is_none() && kept_points.is_empty() {
                removed.extend(t.keys());
                continue;
            }
            t.points = kept_points;
            if let Some(c) = cur {
                // Checkpoints newer than the upload, and the newest of each
                // shard, serve the next points.
                for s in 0..shards {
                    if let Some(cp) = t
                        .checkpoints
                        .iter()
                        .filter(|x| x.shard == s)
                        .max_by_key(|x| (x.lsn, x.created_ms))
                    {
                        need_cps.insert(cp.key.clone());
                    }
                }
                for cp in &t.checkpoints {
                    if cp.usable_from > c.uploaded {
                        need_cps.insert(cp.key.clone());
                    }
                }
                // Everything from the oldest needed segment on.
                if let Some(min) = need_segments.first().copied() {
                    need_segments.extend(t.segments.iter().map(|s| s.number).filter(|n| *n >= min));
                }
            }
            let (keep, drop): (Vec<_>, Vec<_>) = std::mem::take(&mut t.segments)
                .into_iter()
                .partition(|s| need_segments.contains(&s.number));
            removed.extend(
                drop.iter()
                    .flat_map(|s| s.chunks.iter().flat_map(|c| c.keys().map(str::to_owned))),
            );
            t.segments = keep;
            let (keep, drop): (Vec<_>, Vec<_>) = std::mem::take(&mut t.checkpoints)
                .into_iter()
                .partition(|c| need_cps.contains(&c.key));
            removed.extend(drop.into_iter().map(|c| c.key));
            t.checkpoints = keep;
            // Store chunks the kept checkpoints name, and those being
            // appended to.
            let mut need_store: BTreeMap<(u32, u64, u64), u64> = BTreeMap::new();
            for c in &t.checkpoints {
                let e = need_store
                    .entry((c.shard, c.store_generation, c.store_epoch))
                    .or_default();
                *e = (*e).max(c.store_len);
            }
            if let Some(c) = cur {
                for k in c.stores {
                    need_store.insert(*k, u64::MAX);
                }
            }
            let (keep, drop): (Vec<_>, Vec<_>) =
                std::mem::take(&mut t.stores).into_iter().partition(|c| {
                    need_store
                        .get(&(c.shard, c.generation, c.epoch))
                        .is_some_and(|len| c.offset < *len)
                });
            removed.extend(drop.into_iter().map(|c| c.key));
            t.stores = keep;
            // Archives: the timeline being written keeps them all (a later
            // checkpoint covers every one); another keeps those its
            // checkpoints cover.
            if cur.is_none() {
                let mut through: BTreeMap<u32, u64> = BTreeMap::new();
                for c in &t.checkpoints {
                    let e = through.entry(c.shard).or_default();
                    *e = (*e).max(c.lsn);
                }
                let (keep, drop): (Vec<_>, Vec<_>) = std::mem::take(&mut t.archives)
                    .into_iter()
                    .partition(|a| through.get(&a.shard).is_some_and(|l| a.through <= *l));
                removed.extend(drop.into_iter().map(|a| a.key));
                t.archives = keep;
            }
            keep_timelines.push(t);
        }
        self.timelines = keep_timelines;
        removed
    }
}

/// Object names. `tl` is a timeline ID.
pub mod keys {
    pub fn log(tl: &str, segment: u64, offset: u64, end: u64) -> String {
        format!("{tl}/log/{segment:020}/{offset:012}-{end:012}.wal")
    }
    /// The blobs referenced by the log chunk with the same name.
    pub fn blobs(tl: &str, segment: u64, offset: u64, end: u64) -> String {
        format!("{tl}/log/{segment:020}/{offset:012}-{end:012}.blobs")
    }
    pub fn checkpoint(tl: &str, shard: u32, lsn: u64, created_ms: u64) -> String {
        format!("{tl}/shard-{shard:03}/checkpoint-{lsn:020}-{created_ms}.fck")
    }
    pub fn store(
        tl: &str,
        shard: u32,
        generation: u64,
        epoch: u64,
        offset: u64,
        end: u64,
    ) -> String {
        format!("{tl}/shard-{shard:03}/runs-{generation:010}-e{epoch}/{offset:016}-{end:016}.dat")
    }
    pub fn archive(tl: &str, shard: u32, name: &str) -> String {
        format!("{tl}/shard-{shard:03}/archive/{name}")
    }
    pub fn snapshot(version: u64) -> String {
        format!("state/{version:020}.json")
    }
}
