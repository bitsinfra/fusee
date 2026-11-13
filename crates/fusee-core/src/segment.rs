//! Log segment format, the in-memory log writer and the recovery scanner.
//!
//! A shard's log is a sequence of segment files. Each segment starts with one
//! 4 KiB header block, followed by frames:
//!
//! ```text
//! frame  = len:u32le  crc:u32le  lsn:u64le  batch:u32le  body[len]
//! crc    = crc32c(lsn ++ batch ++ body)
//! ```
//!
//! `batch` is the distance from the first record of the flush batch the
//! frame was written in. Only one flush is in flight per log, so a batch is
//! written only after the previous one was durable. That lets recovery tell
//! a torn, never-acknowledged batch (whose writes the OS may have completed
//! in any order) from a damaged record that had been acknowledged.
//!
//! `lsn` is the record's log sequence number: consecutive across the shard's
//! whole log, starting at the segment header's `base_lsn`. The body is a
//! record in segment encoding (see [`crate::record`]); names are interned per
//! segment, so a segment can be read without any other file.
//!
//! The writer here does no I/O. It turns records into addressed byte
//! [`Chunk`]s, and the I/O layer (real files or the simulator's disk) writes
//! them and flushes. Every open of a log starts a fresh segment file, so bytes
//! left behind by a torn write are never followed by new records in the same
//! file. (An open may reuse the number of a trailing segment that holds no
//! valid record, after deleting that file.)
//!
//! Recovery reads each segment with [`SegmentScanner`] over a growing prefix
//! of the file, stopping just past the valid records ([`scan_segment`] is
//! the same scan over a whole image). A frame that fails its
//! checksum, carries the wrong LSN or does not decode ends the valid prefix.
//! If a later frame in the segment is still valid, a durable record was lost
//! in the middle of the log ([`Tail::Corrupt`]); otherwise the segment simply
//! ends in a torn write that was never acknowledged ([`Tail::Torn`]).

use crate::record::{
    Lsn, Name, NameDecoder, NameEncoder, Record, RecordBuf, decode_body, encode_body,
};
use crate::wire::{DecodeError, DecodeResult, Reader, put_varint};
use rustc_hash::FxHashMap;
use std::sync::Arc;

pub const MAGIC: &[u8; 8] = b"FUSEESEG";
/// The on-disk format version written into every segment header. A reader
/// refuses segments with a newer version.
pub const FORMAT_VERSION: u32 = 2;
pub const HEADER_BYTES: u64 = 4096;
pub const FRAME_HEADER: usize = 20;
pub const DEFAULT_SEGMENT_BYTES: u64 = 64 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentHeader {
    pub format: u32,
    pub shard: u32,
    pub segment: u64,
    pub base_lsn: Lsn,
    pub created_ms: u64,
}

impl SegmentHeader {
    pub fn new(shard: u32, segment: u64, base_lsn: Lsn, created_ms: u64) -> Self {
        Self {
            format: FORMAT_VERSION,
            shard,
            segment,
            base_lsn,
            created_ms,
        }
    }

    /// The full header block, zero padded to [`HEADER_BYTES`].
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_BYTES as usize);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.format.to_le_bytes());
        out.extend_from_slice(&self.shard.to_le_bytes());
        out.extend_from_slice(&self.segment.to_le_bytes());
        out.extend_from_slice(&self.base_lsn.to_le_bytes());
        out.extend_from_slice(&self.created_ms.to_le_bytes());
        let crc = crc32c::crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out.resize(HEADER_BYTES as usize, 0);
        out
    }

    /// `Ok(None)` for an all-zero (preallocated, never activated) header.
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>, ScanError> {
        let Some(head) = bytes.get(..44) else {
            return if bytes.iter().all(|b| *b == 0) {
                Ok(None)
            } else {
                Err(ScanError::BadHeader)
            };
        };
        if head.iter().all(|b| *b == 0) {
            return Ok(None);
        }
        if &head[..8] != MAGIC {
            return Err(ScanError::BadHeader);
        }
        let mut r = Reader::new(&head[8..]);
        let header = (|| -> DecodeResult<(Self, u32)> {
            let format = r.u32()?;
            let shard = r.u32()?;
            let segment = r.u64()?;
            let base_lsn = r.u64()?;
            let created_ms = r.u64()?;
            let crc = r.u32()?;
            Ok((
                Self {
                    format,
                    shard,
                    segment,
                    base_lsn,
                    created_ms,
                },
                crc,
            ))
        })()
        .map_err(|_| ScanError::BadHeader)?;
        if crc32c::crc32c(&head[..40]) != header.1 {
            return Err(ScanError::BadHeader);
        }
        if header.0.format != FORMAT_VERSION {
            return Err(ScanError::UnsupportedFormat(header.0.format));
        }
        Ok(Some(header.0))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanError {
    BadHeader,
    UnsupportedFormat(u32),
}

impl std::fmt::Display for ScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadHeader => write!(f, "segment header is damaged"),
            Self::UnsupportedFormat(v) => write!(
                f,
                "segment format version {v} is not the one this build reads ({FORMAT_VERSION}); \
                 open the data directory with the Fusee version that wrote it"
            ),
        }
    }
}

impl std::error::Error for ScanError {}

/// Bytes to write at `offset` of segment `segment`. A chunk that starts a
/// segment begins with its header block at offset 0.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    pub segment: u64,
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub starts_segment: bool,
}

/// Everything appended since the previous [`LogWriter::take`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Batch {
    pub chunks: Vec<Chunk>,
    pub first_lsn: Lsn,
    pub last_lsn: Lsn,
    pub records: u32,
    pub bytes: u64,
    /// Log format 3: the shard's records `first_lsn..=last_lsn` as
    /// canonical encodings (one section of a shared-log batch). Empty in
    /// format 2, where `chunks` carry the segment bytes.
    pub section: Vec<u8>,
}

/// Buffers of dropped batches, kept per thread for the next batch a writer
/// on that thread starts. A batch flushed on its writer's thread (inline,
/// or in `memory` mode) gives its buffers back to the next one, so a flush
/// of a small batch allocates nothing; one dropped on a flusher thread
/// leaves at most [`SPARE_MAX`] buffers of each kind there.
struct Spare {
    bytes: Vec<Vec<u8>>,
    lists: Vec<Vec<Chunk>>,
}

thread_local! {
    static SPARE: std::cell::RefCell<Spare> = const {
        std::cell::RefCell::new(Spare {
            bytes: Vec::new(),
            lists: Vec::new(),
        })
    };
}

/// Spare buffers kept per kind and thread.
const SPARE_MAX: usize = 4;
/// Chunk bytes of the usual size, and the largest buffer kept.
const CHUNK_BYTES: usize = 4096;
const SPARE_BYTES_MAX: usize = 64 << 10;

/// An empty byte buffer with room for at least `min` bytes, reused when
/// this thread has one.
pub fn spare_bytes(min: usize) -> Vec<u8> {
    SPARE
        .try_with(|s| {
            let mut s = s.try_borrow_mut().ok()?;
            let at = s.bytes.iter().rposition(|b| b.capacity() >= min)?;
            Some(s.bytes.swap_remove(at))
        })
        .ok()
        .flatten()
        .unwrap_or_else(|| Vec::with_capacity(min))
}

fn spare_list() -> Vec<Chunk> {
    SPARE
        .try_with(|s| s.try_borrow_mut().ok()?.lists.pop())
        .ok()
        .flatten()
        .unwrap_or_default()
}

/// Keeps `bytes` for a later [`spare_bytes`] on this thread.
pub fn recycle_bytes(mut bytes: Vec<u8>) {
    if bytes.capacity() == 0 || bytes.capacity() > SPARE_BYTES_MAX {
        return;
    }
    bytes.clear();
    let _ = SPARE.try_with(|s| {
        if let Ok(mut s) = s.try_borrow_mut()
            && s.bytes.len() < SPARE_MAX
        {
            s.bytes.push(bytes);
        }
    });
}

impl Drop for Batch {
    fn drop(&mut self) {
        let mut chunks = std::mem::take(&mut self.chunks);
        for c in chunks.drain(..) {
            recycle_bytes(c.bytes);
        }
        recycle_bytes(std::mem::take(&mut self.section));
        if chunks.capacity() != 0 && chunks.capacity() <= 64 {
            let _ = SPARE.try_with(|s| {
                if let Ok(mut s) = s.try_borrow_mut()
                    && s.lists.len() < SPARE_MAX
                {
                    s.lists.push(chunks);
                }
            });
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TooLarge;

struct Interned<'a>(&'a FxHashMap<Box<str>, u32>);

impl NameEncoder for Interned<'_> {
    #[inline]
    fn put(&mut self, out: &mut Vec<u8>, name: &str) {
        // Every name is defined before the body is encoded.
        let id = self.0.get(name).copied().unwrap_or(u32::MAX);
        put_varint(out, u64::from(id));
    }
}

/// Appends records to a shard log in memory. Assigns LSNs, interns names per
/// segment and decides segment boundaries. Pure: no files.
pub struct LogWriter {
    shard: u32,
    segment_bytes: u64,
    active_bytes: u64,
    segment: u64,
    offset: u64,
    next_lsn: Lsn,
    interns: FxHashMap<Box<str>, u32>,
    chunks: Vec<Chunk>,
    batch_first: Lsn,
    batch_records: u32,
    batch_bytes: u64,
    body: Vec<u8>,
    defines: Vec<Box<str>>,
    segment_first_lsn: FxHashMap<u64, Lsn>,
}

impl LogWriter {
    /// Starts a new segment `segment` whose first record will be `next_lsn`.
    pub fn new(shard: u32, segment: u64, next_lsn: Lsn, segment_bytes: u64, now_ms: u64) -> Self {
        let mut w = Self {
            shard,
            segment_bytes: segment_bytes.max(HEADER_BYTES * 4),
            active_bytes: 0,
            segment,
            offset: 0,
            next_lsn,
            interns: FxHashMap::default(),
            chunks: Vec::new(),
            batch_first: next_lsn,
            batch_records: 0,
            batch_bytes: 0,
            body: Vec::with_capacity(256),
            defines: Vec::new(),
            segment_first_lsn: FxHashMap::default(),
        };
        w.start_segment(segment, now_ms);
        w
    }

    fn start_segment(&mut self, segment: u64, now_ms: u64) {
        self.active_bytes = self.segment_bytes;
        self.segment = segment;
        self.interns.clear();
        let header = SegmentHeader::new(self.shard, segment, self.next_lsn, now_ms);
        self.chunks.push(Chunk {
            segment,
            offset: 0,
            bytes: header.encode(),
            starts_segment: true,
        });
        self.offset = HEADER_BYTES;
        self.batch_bytes += HEADER_BYTES;
        self.segment_first_lsn.insert(segment, self.next_lsn);
    }

    pub fn next_lsn(&self) -> Lsn {
        self.next_lsn
    }

    pub fn last_lsn(&self) -> Lsn {
        self.next_lsn - 1
    }

    pub fn segment(&self) -> u64 {
        self.segment
    }

    pub fn segment_bytes(&self) -> u64 {
        self.segment_bytes
    }

    /// The segment opened at recovery may be smaller. Later segments retain
    /// their configured capacity and the maximum record size is unchanged.
    pub fn limit_first_segment(&mut self, bytes: u64) {
        self.active_bytes = bytes.clamp(HEADER_BYTES * 4, self.segment_bytes);
    }

    /// Bytes used in the current segment.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Whether records or a new segment's header wait for a flush. The
    /// header of the segment an open starts is flushed on its own: replicated
    /// mode anchors a checkpoint on the batch that ends at the checkpoint's
    /// position, which must lie in a segment the checkpoint keeps. A header
    /// alone holds no record, so the next open may replace that segment.
    pub fn has_pending(&self) -> bool {
        self.batch_records > 0 || self.chunks.iter().any(|c| c.starts_segment)
    }

    pub fn pending_records(&self) -> u32 {
        self.batch_records
    }

    fn encode_into_body<S: AsRef<str>, B: AsRef<[u8]>>(&mut self, rec: &Record<S, B>) {
        self.defines.clear();
        let interns = &mut self.interns;
        let defines = &mut self.defines;
        rec.for_each_name(|name| {
            if !interns.contains_key(name) && !defines.iter().any(|d| &**d == name) {
                defines.push(Box::from(name));
            }
        });
        for d in self.defines.iter() {
            let id = self.interns.len() as u32;
            self.interns.insert(d.clone(), id);
        }
        self.body.clear();
        let defs: Vec<&str> = self.defines.iter().map(|d| &**d).collect();
        encode_body(rec, &defs, &mut Interned(&self.interns), &mut self.body);
    }

    /// Appends one record and returns its LSN.
    pub fn append<S: AsRef<str>, B: AsRef<[u8]>>(
        &mut self,
        rec: &Record<S, B>,
        now_ms: u64,
    ) -> Result<Lsn, TooLarge> {
        self.encode_into_body(rec);
        let mut frame_len = (FRAME_HEADER + self.body.len()) as u64;
        if frame_len > self.segment_bytes - HEADER_BYTES {
            // Undo the definitions so the segment stays consistent.
            for d in self.defines.drain(..) {
                self.interns.remove(&d);
            }
            return Err(TooLarge);
        }
        if self.offset + frame_len > self.active_bytes {
            for d in self.defines.drain(..) {
                self.interns.remove(&d);
            }
            self.start_segment(self.segment + 1, now_ms);
            self.encode_into_body(rec);
            frame_len = (FRAME_HEADER + self.body.len()) as u64;
            if frame_len > self.segment_bytes - HEADER_BYTES {
                for d in self.defines.drain(..) {
                    self.interns.remove(&d);
                }
                return Err(TooLarge);
            }
        }
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        if self.batch_records == 0 {
            self.batch_first = lsn;
        }
        let delta = u32::try_from(lsn - self.batch_first).unwrap_or(u32::MAX);
        let needs_new_chunk = match self.chunks.last() {
            Some(c) => c.segment != self.segment || c.offset + c.bytes.len() as u64 != self.offset,
            None => true,
        };
        if needs_new_chunk {
            self.chunks.push(Chunk {
                segment: self.segment,
                offset: self.offset,
                bytes: spare_bytes(CHUNK_BYTES),
                starts_segment: false,
            });
        }
        let lsn_bytes = lsn.to_le_bytes();
        let delta_bytes = delta.to_le_bytes();
        let mut crc = crc32c::crc32c(&lsn_bytes);
        crc = crc32c::crc32c_append(crc, &delta_bytes);
        crc = crc32c::crc32c_append(crc, &self.body);
        if let Some(chunk) = self.chunks.last_mut() {
            chunk
                .bytes
                .extend_from_slice(&(self.body.len() as u32).to_le_bytes());
            chunk.bytes.extend_from_slice(&crc.to_le_bytes());
            chunk.bytes.extend_from_slice(&lsn_bytes);
            chunk.bytes.extend_from_slice(&delta_bytes);
            chunk.bytes.extend_from_slice(&self.body);
        }
        self.offset += frame_len;
        self.batch_records += 1;
        self.batch_bytes += frame_len;
        Ok(lsn)
    }

    /// Takes everything appended since the last call.
    pub fn take(&mut self) -> Option<Batch> {
        if self.chunks.is_empty() {
            return None;
        }
        let chunks = std::mem::replace(&mut self.chunks, spare_list());
        let batch = Batch {
            chunks,
            first_lsn: if self.batch_records == 0 {
                self.next_lsn
            } else {
                self.batch_first
            },
            last_lsn: self.next_lsn - 1,
            records: self.batch_records,
            bytes: self.batch_bytes,
            section: Vec::new(),
        };
        self.batch_records = 0;
        self.batch_bytes = 0;
        self.batch_first = self.next_lsn;
        Some(batch)
    }

    /// Ends the current segment; the next append starts a new one.
    pub fn roll(&mut self, now_ms: u64) {
        self.start_segment(self.segment + 1, now_ms);
    }

    /// First LSN of each segment written by this writer.
    pub fn segment_first_lsn(&self, segment: u64) -> Option<Lsn> {
        self.segment_first_lsn.get(&segment).copied()
    }
}

/// How a scanned segment ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tail {
    /// The valid prefix is followed by zeros.
    Clean,
    /// The valid prefix is followed by bytes that never formed a valid frame:
    /// a write that never completed. Safe to discard.
    Torn { at: usize },
    /// A frame after the damaged one is valid: records that were durable have
    /// been lost from the middle of the log.
    Corrupt { at: usize, later_lsn: Lsn },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scanned {
    pub header: SegmentHeader,
    /// End offset of the valid prefix.
    pub end: usize,
    /// LSN of the last valid record, or `base_lsn - 1` when there is none.
    pub last_lsn: Lsn,
    pub records: u64,
    pub tail: Tail,
    /// Highest "first LSN of the frame's flush batch" among valid frames.
    pub max_batch_start: Lsn,
}

/// Decodes interned names while scanning one segment.
#[derive(Default)]
pub struct SegmentNames {
    names: Vec<Name>,
}

impl NameDecoder for SegmentNames {
    fn define(&mut self, r: &mut Reader<'_>) -> DecodeResult<()> {
        let n = r.varint()?;
        if n > 4096 {
            return Err(DecodeError("too many name definitions"));
        }
        for _ in 0..n {
            self.names.push(Arc::from(r.str()?));
        }
        Ok(())
    }

    fn get(&mut self, r: &mut Reader<'_>) -> DecodeResult<Name> {
        let id = r.varint()?;
        usize::try_from(id)
            .ok()
            .and_then(|i| self.names.get(i))
            .cloned()
            .ok_or(DecodeError("unknown interned name"))
    }
}

/// Reads one frame at `pos`: `(lsn, first LSN of its batch, body)`.
/// `Ok(None)` means the frame header is all zeros.
fn read_frame(bytes: &[u8], pos: usize) -> Result<Option<(Lsn, Lsn, &[u8])>, ()> {
    let Some(head) = bytes.get(pos..pos + FRAME_HEADER) else {
        return if bytes.get(pos..).is_none_or(|r| r.iter().all(|b| *b == 0)) {
            Ok(None)
        } else {
            Err(())
        };
    };
    if head.iter().all(|b| *b == 0) {
        return Ok(None);
    }
    let len = u32::from_le_bytes([head[0], head[1], head[2], head[3]]) as usize;
    let crc = u32::from_le_bytes([head[4], head[5], head[6], head[7]]);
    if len == 0 || len > crate::wire::MAX_PAYLOAD + 1024 {
        return Err(());
    }
    let body = bytes
        .get(pos + FRAME_HEADER..pos + FRAME_HEADER + len)
        .ok_or(())?;
    let actual = crc32c::crc32c_append(crc32c::crc32c(&head[8..20]), body);
    if actual != crc {
        return Err(());
    }
    let mut lsn_bytes = [0u8; 8];
    lsn_bytes.copy_from_slice(&head[8..16]);
    let lsn = u64::from_le_bytes(lsn_bytes);
    let delta = u64::from(u32::from_le_bytes([head[16], head[17], head[18], head[19]]));
    Ok(Some((lsn, lsn.saturating_sub(delta), body)))
}

/// For a segment whose header is unreadable: whether its body proves the
/// segment had been activated and flushed (a valid frame from a batch after
/// the first). If not, the header was torn by the segment's first, never
/// acknowledged flush.
pub fn headerless_was_flushed(bytes: &[u8], base_lsn: Lsn) -> bool {
    let start = (HEADER_BYTES as usize).saturating_sub(1).min(bytes.len());
    matches!(find_later(bytes, start, base_lsn), Tail::Corrupt { .. })
}

/// Scans a segment, calling `visit` for each valid record in order. Returns
/// `Ok(None)` for a never-activated (all-zero header) segment.
pub fn scan_segment(
    bytes: &[u8],
    mut visit: impl FnMut(Lsn, RecordBuf),
) -> Result<Option<Scanned>, ScanError> {
    let Some(mut scanner) = SegmentScanner::start(bytes, true)? else {
        return Ok(None);
    };
    match scanner.advance(bytes, true, &mut visit) {
        Progress::Done(s) => Ok(Some(s)),
        Progress::NeedMore => unreachable!("a complete image never needs more bytes"),
    }
}

/// What [`SegmentScanner::advance`] concluded from the bytes it was given.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Progress {
    /// The segment's valid prefix and tail are known: the same result a scan
    /// of the whole file returns.
    Done(Scanned),
    /// The verdict depends on bytes past the end of the prefix read so far.
    NeedMore,
}

/// Scans a segment from a growing prefix of its bytes, so recovery reads
/// only the valid records plus the first zero block after them instead of
/// the whole preallocated file. Fed the complete image, it returns exactly
/// what [`scan_segment`] returns.
pub struct SegmentScanner {
    header: SegmentHeader,
    names: SegmentNames,
    pos: usize,
    expected: Lsn,
    records: u64,
    max_batch_start: Lsn,
}

impl SegmentScanner {
    /// Reads the header. `Ok(None)` for a never-activated segment. A prefix
    /// shorter than the whole file must hold at least the header block.
    pub fn start(bytes: &[u8], complete: bool) -> Result<Option<Self>, ScanError> {
        debug_assert!(complete || bytes.len() >= HEADER_BYTES as usize);
        let Some(header) = SegmentHeader::decode(bytes)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            header,
            names: SegmentNames::default(),
            pos: HEADER_BYTES as usize,
            expected: header.base_lsn,
            records: 0,
            max_batch_start: 0,
        }))
    }

    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    /// Continues the scan over `bytes`, the segment's first `bytes.len()`
    /// bytes (each call passes a longer prefix of the same file). `complete`
    /// says `bytes` is the whole file. Records already visited are not
    /// visited again.
    pub fn advance(
        &mut self,
        bytes: &[u8],
        complete: bool,
        visit: &mut dyn FnMut(Lsn, RecordBuf),
    ) -> Progress {
        let tail = loop {
            match read_frame_prefix(bytes, self.pos, complete) {
                Frame::More => return Progress::NeedMore,
                Frame::Zero => break Tail::Clean,
                Frame::Valid(lsn, batch_start, body) if lsn == self.expected => {
                    match decode_body(body, &mut self.names) {
                        Ok(rec) => {
                            self.max_batch_start = self.max_batch_start.max(batch_start);
                            visit(lsn, rec);
                            self.records += 1;
                            self.expected += 1;
                            self.pos += FRAME_HEADER + body.len();
                        }
                        Err(_) => match find_later_prefix(bytes, self.pos, self.expected, complete)
                        {
                            Some(t) => break t,
                            None => return Progress::NeedMore,
                        },
                    }
                }
                Frame::Valid(..) | Frame::Bad => {
                    match find_later_prefix(bytes, self.pos, self.expected, complete) {
                        Some(t) => break t,
                        None => return Progress::NeedMore,
                    }
                }
            }
        };
        Progress::Done(Scanned {
            header: self.header,
            end: self.pos,
            last_lsn: self.expected - 1,
            records: self.records,
            tail,
            max_batch_start: self.max_batch_start,
        })
    }
}

enum Frame<'a> {
    /// The frame header is all zeros: nothing was ever written here.
    Zero,
    Valid(Lsn, Lsn, &'a [u8]),
    Bad,
    /// The frame continues past the prefix.
    More,
}

/// [`read_frame`] over a prefix of the file: answers `More` wherever the
/// whole-file answer could depend on bytes not read yet.
fn read_frame_prefix(bytes: &[u8], pos: usize, complete: bool) -> Frame<'_> {
    if complete {
        return match read_frame(bytes, pos) {
            Ok(None) => Frame::Zero,
            Ok(Some((lsn, batch, body))) => Frame::Valid(lsn, batch, body),
            Err(()) => Frame::Bad,
        };
    }
    let Some(head) = bytes.get(pos..pos + FRAME_HEADER) else {
        return Frame::More;
    };
    if head.iter().all(|b| *b == 0) {
        return Frame::Zero;
    }
    let len = u32::from_le_bytes([head[0], head[1], head[2], head[3]]) as usize;
    if len == 0 || len > crate::wire::MAX_PAYLOAD + 1024 {
        return Frame::Bad;
    }
    if bytes.len() < pos + FRAME_HEADER + len {
        return Frame::More;
    }
    match read_frame(bytes, pos) {
        Ok(None) => Frame::Zero,
        Ok(Some((lsn, batch, body))) => Frame::Valid(lsn, batch, body),
        Err(()) => Frame::Bad,
    }
}

/// [`find_later`] over a prefix: `None` until the prefix reaches the first
/// all-zero block after `at`, the limit a whole-file scan searches to.
fn find_later_prefix(bytes: &[u8], at: usize, expected: Lsn, complete: bool) -> Option<Tail> {
    if complete || zero_block_after(bytes, at).is_some() {
        Some(find_later(bytes, at, expected))
    } else {
        None
    }
}

/// The first all-zero, 4 KiB aligned block at or after `at`.
fn zero_block_after(bytes: &[u8], at: usize) -> Option<usize> {
    let mut block = at.div_ceil(4096) * 4096;
    while block + 4096 <= bytes.len() {
        if bytes[block..block + 4096].iter().all(|b| *b == 0) {
            return Some(block);
        }
        block += 4096;
    }
    None
}

/// After an invalid frame at `at` (where record `expected` should be),
/// looks for a later valid frame, up to the first all-zero 4 KiB block (no
/// write ever reached past it). A later frame from a *later* batch proves
/// `expected`'s batch had been flushed, so a durable record was lost. A
/// later frame from the *same* batch only shows the OS completed the torn
/// batch's writes out of order.
fn find_later(bytes: &[u8], at: usize, expected: Lsn) -> Tail {
    let limit = zero_block_after(bytes, at).unwrap_or(bytes.len());
    let region = &bytes[..limit];
    let mut pos = at + 1;
    while pos + FRAME_HEADER <= region.len() {
        if let Ok(Some((lsn, batch_start, _))) = read_frame(region, pos)
            && lsn >= expected
            && batch_start > expected
        {
            return Tail::Corrupt { at, later_lsn: lsn };
        }
        pos += 1;
    }
    Tail::Torn { at }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{RecordRef, Status};

    fn step(run: u64, seq: u32, name: &str) -> RecordRef<'_> {
        Record::Step {
            run,
            seq,
            name,
            attempt: 1,
            status: Status::Ok,
            payload: b"null",
            time: None,
        }
    }

    /// Lays a batch's chunks onto in-memory segment images.
    fn lay(images: &mut FxHashMap<u64, Vec<u8>>, batch: &Batch, size: u64) {
        for c in &batch.chunks {
            let img = images
                .entry(c.segment)
                .or_insert_with(|| vec![0; size as usize]);
            img[c.offset as usize..c.offset as usize + c.bytes.len()].copy_from_slice(&c.bytes);
        }
    }

    fn scan_all(img: &[u8]) -> (Scanned, Vec<(Lsn, RecordBuf)>) {
        let mut out = Vec::new();
        let s = scan_segment(img, |l, r| out.push((l, r))).unwrap().unwrap();
        (s, out)
    }

    #[test]
    fn write_then_scan() {
        let size = 64 * 1024;
        let mut w = LogWriter::new(0, 1, 1, size, 0);
        for i in 0..100u32 {
            assert_eq!(w.append(&step(1, i, "a"), 0).unwrap(), u64::from(i) + 1);
        }
        let batch = w.take().unwrap();
        assert_eq!(
            (batch.first_lsn, batch.last_lsn, batch.records),
            (1, 100, 100)
        );
        let mut images = FxHashMap::default();
        lay(&mut images, &batch, size);
        let (s, recs) = scan_all(&images[&1]);
        assert_eq!(s.tail, Tail::Clean);
        assert_eq!(recs.len(), 100);
        assert_eq!(recs[99].0, 100);
        assert_eq!(recs[5].1, step(1, 5, "a").to_buf());
    }

    #[test]
    fn a_large_first_record_rolls_to_a_full_size_segment() {
        let max = 128 * 1024;
        let mut w = LogWriter::new(0, 1, 1, max, 0);
        w.limit_first_segment(16 * 1024);
        let large = "a".repeat(32 * 1024);
        assert_eq!(w.append(&step(1, 1, &large), 0).unwrap(), 1);
        assert_eq!(w.segment(), 2);
        assert_eq!(w.segment_bytes(), max);
        assert_eq!(w.append(&step(1, 2, "tail"), 0).unwrap(), 2);
        let mut images = FxHashMap::default();
        lay(&mut images, &w.take().unwrap(), max);
        let (first, records) = scan_all(&images[&1]);
        assert_eq!(first.last_lsn, 0);
        assert!(records.is_empty());
        let (second, records) = scan_all(&images[&2]);
        assert_eq!(second.tail, Tail::Clean);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].1, step(1, 1, &large).to_buf());
    }

    #[test]
    fn interning_shrinks_frames_and_resets_per_segment() {
        let size = 16 * 1024;
        let mut w = LogWriter::new(0, 1, 1, size, 0);
        w.append(&step(1, 0, "a-long-step-name"), 0).unwrap();
        let first = w.offset();
        w.append(&step(1, 1, "a-long-step-name"), 0).unwrap();
        let second = w.offset() - first;
        assert!(second < first - HEADER_BYTES);
        for i in 2..2000 {
            w.append(&step(1, i, "a-long-step-name"), 0).unwrap();
        }
        assert!(w.segment() > 1);
        let batch = w.take().unwrap();
        let mut images = FxHashMap::default();
        lay(&mut images, &batch, size);
        let mut total = 0;
        let mut next = 1;
        let mut segs: Vec<_> = images.keys().copied().collect();
        segs.sort();
        for seg in segs {
            let (s, recs) = scan_all(&images[&seg]);
            assert_eq!(s.header.base_lsn, next);
            next = s.last_lsn + 1;
            total += recs.len();
        }
        assert_eq!(total, 2000);
    }

    #[test]
    fn torn_tail_is_distinguished_from_corruption() {
        let size = 64 * 1024;
        let mut w = LogWriter::new(0, 1, 1, size, 0);
        let mut images = FxHashMap::default();
        // Batch 1 (records 1..=30) was flushed; batch 2 (31..=50) was not.
        for i in 0..30 {
            w.append(&step(1, i, "a"), 0).unwrap();
        }
        lay(&mut images, &w.take().unwrap(), size);
        let mid = images[&1].clone();
        let (first, _) = scan_all(&mid);
        for i in 30..50 {
            w.append(&step(1, i, "a"), 0).unwrap();
        }
        lay(&mut images, &w.take().unwrap(), size);
        let clean = images[&1].clone();
        let (full, _) = scan_all(&clean);
        assert_eq!(full.last_lsn, 50);

        // Tear the last frame: garbage in its final bytes.
        let mut torn = clean.clone();
        torn[full.end - 3] ^= 0xff;
        let (s, recs) = scan_all(&torn);
        assert!(matches!(s.tail, Tail::Torn { .. }));
        assert_eq!(recs.len(), 49);

        // Batch 2 written out of order: its first record is missing but
        // later ones landed. Still a torn, unacknowledged batch.
        let mut holes = clean.clone();
        holes[first.end + 25] ^= 0xff;
        let (s, recs) = scan_all(&holes);
        assert!(matches!(s.tail, Tail::Torn { .. }), "{:?}", s.tail);
        assert_eq!(recs.len(), 30);

        // Damage inside the flushed batch 1 while batch 2 is intact: an
        // acknowledged record was lost.
        let mut corrupt = clean.clone();
        corrupt[HEADER_BYTES as usize + 200] ^= 0xff;
        let (s, recs) = scan_all(&corrupt);
        assert!(matches!(s.tail, Tail::Corrupt { .. }), "{:?}", s.tail);
        assert!(recs.len() < 30);
    }

    /// Scans `img` by feeding growing prefixes the way recovery reads a file.
    fn scan_by_prefix(img: &[u8], first: usize) -> (Option<Scanned>, Vec<(Lsn, RecordBuf)>, usize) {
        let mut out = Vec::new();
        let mut len = first.min(img.len());
        let mut scanner = match SegmentScanner::start(&img[..len], len == img.len()).unwrap() {
            Some(s) => s,
            None => return (None, out, len),
        };
        loop {
            let complete = len == img.len();
            match scanner.advance(&img[..len], complete, &mut |l, r| out.push((l, r))) {
                Progress::Done(s) => return (Some(s), out, len),
                Progress::NeedMore => {
                    assert!(!complete);
                    len = (len * 2).min(img.len());
                }
            }
        }
    }

    #[test]
    fn prefix_scans_match_whole_file_scans() {
        let size = 256 * 1024;
        let mut w = LogWriter::new(0, 1, 1, size, 0);
        let mut images = FxHashMap::default();
        for i in 0..40 {
            w.append(&step(1, i, "a"), 0).unwrap();
        }
        lay(&mut images, &w.take().unwrap(), size);
        let first_end = scan_all(&images[&1]).0.end;
        let big = "z".repeat(20_000);
        for i in 40..60 {
            w.append(&step(1, i, &big), 0).unwrap();
        }
        // A large payload of zeros: frames may contain all-zero blocks.
        let zeros = vec![0u8; 12_000];
        w.append(
            &Record::Step {
                run: 1,
                seq: 60,
                name: "zeros",
                attempt: 1,
                status: Status::Ok,
                payload: &zeros[..],
                time: None,
            },
            0,
        )
        .unwrap();
        lay(&mut images, &w.take().unwrap(), size);
        let clean = images[&1].clone();
        let full = scan_all(&clean).0;
        let mut variants = vec![clean.clone()];
        let mut torn = clean.clone();
        torn[full.end - 3] ^= 0xff;
        variants.push(torn);
        let mut holes = clean.clone();
        holes[first_end + 25] ^= 0xff;
        variants.push(holes);
        let mut corrupt = clean.clone();
        corrupt[HEADER_BYTES as usize + 200] ^= 0xff;
        variants.push(corrupt);
        let mut garbage_tail = clean.clone();
        for b in &mut garbage_tail[full.end + 9000..full.end + 9100] {
            *b = 0x5a;
        }
        variants.push(garbage_tail);
        for img in &variants {
            let mut want = Vec::new();
            let expected = scan_segment(img, |l, r| want.push((l, r))).unwrap();
            for first in [4096, 5000, 8192, 65536, size as usize] {
                let (got, records, read) = scan_by_prefix(img, first);
                assert_eq!(got, expected, "first read {first}");
                assert_eq!(records, want, "first read {first}");
                if expected.as_ref().is_some_and(|s| s.tail == Tail::Clean) && first <= 8192 {
                    // A clean segment is read only a little past its end.
                    assert!(read <= (full.end + FRAME_HEADER).next_power_of_two().max(first) * 2);
                }
            }
        }
    }

    #[test]
    fn spare_and_bad_headers() {
        assert_eq!(scan_segment(&vec![0; 8192], |_, _| {}).unwrap(), None);
        let mut h = SegmentHeader::new(0, 1, 1, 0).encode();
        h[20] ^= 1;
        assert_eq!(scan_segment(&h, |_, _| {}), Err(ScanError::BadHeader));
        let mut newer = SegmentHeader::new(0, 1, 1, 0);
        newer.format = FORMAT_VERSION + 1;
        assert_eq!(
            scan_segment(&newer.encode(), |_, _| {}),
            Err(ScanError::UnsupportedFormat(FORMAT_VERSION + 1))
        );
    }
}
