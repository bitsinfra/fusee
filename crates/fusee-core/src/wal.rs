//! Log format 3: one write-ahead log per device, shared by every shard.
//!
//! A log group is a sequence of segment files. Each segment begins with one
//! 4 KiB header block and holds batches back to back:
//!
//! ```text
//! batch   = header  section*  padding
//! header  = len:u32  crc:u32  body_len:u32  flags:u32
//!           seq:u64  prev:u64  acked:u64  life:u64  sections:u32  reserved:u32
//! section = shard:u32  count:u32  first_lsn:u64  bytes:u32  reserved:u32  records
//! records = (varint length, canonical record body)*
//! crc     = crc32c(salt ++ batch[8 .. body_len])
//! ```
//!
//! `len` is the distance to the next batch (a multiple of 8, past any
//! padding); `body_len` is what the checksum covers. `seq` numbers batches
//! across the whole group. `prev` is the [`batch_checksum`] of the batch
//! before it, so batches form a chain that stale bytes cannot join. `acked`
//! is the highest sequence the writer had acknowledged when it submitted the
//! batch. `life` is random per open of the log: two lives never write
//! identical batches, so a stale batch's checksum never matches a new one.
//!
//! `salt` is in the segment header and is new for every life of a segment
//! file (a recycled file gets a new header and salt), so bytes from a
//! segment's earlier life never pass the checksum.
//!
//! Each shard's records keep their own LSN sequence: a section carries one
//! shard's consecutive records starting at `first_lsn`. Records use the
//! canonical encoding ([`crate::record::encode_canonical`]), the same bytes
//! the run's journal and replication use.
//!
//! **Acceptance** (`spec/FuseeLog.tla`). Recovery accepts batches from a
//! known position while each passes its checksum, has the next sequence and
//! names the accepted batch before it. Where the chain ends, a later batch
//! of the same segment life whose `acked` reaches past the end proves an
//! acknowledged batch was lost: [`End::Damaged`]. Otherwise the tail was in
//! flight and never acknowledged, and the next open resumes in place after
//! it ([`End::Clean`] or [`End::Torn`]). Up to several writes may be in
//! flight at once: the device may complete them in any order, which is why
//! a valid batch after an invalid one proves nothing unless its `acked` says
//! the invalid one had been acknowledged.
//!
//! Batches of writes that may be in flight together never share a 4 KiB
//! block: the writer pads a batch to the next block boundary when the next
//! write may start before this one completes, so one write landing late can
//! never overwrite another's bytes.

use crate::record::{Lsn, RecordBuf, decode_body, encode_canonical};
use crate::wire::{DecodeError, DecodeResult, Reader};

pub const WAL_MAGIC: &[u8; 8] = b"FUSEEWAL";
pub const WAL_FORMAT: u32 = 3;
pub const BLOCK: u64 = 4096;
pub const WAL_HEADER_BYTES: u64 = BLOCK;
pub const BATCH_HEADER: usize = 56;
pub const SECTION_HEADER: usize = 24;
/// A batch is at most this large (a segment must hold it).
pub const MAX_BATCH: usize = 64 << 20;
/// How far past the end of the chain recovery looks for a batch proving
/// that an acknowledged batch was lost. A proof is written within a few
/// batches of the loss; damage with no proof that close is indistinguishable
/// from a torn tail, as with any log.
pub const DAMAGE_WINDOW: usize = 1 << 20;

/// Flag: the first batch of a writer life.
pub const FLAG_LIFE_START: u32 = 1;

pub use crate::wire::xxh64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalError {
    BadHeader,
    UnsupportedFormat(u32),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadHeader => write!(f, "log segment header is damaged"),
            Self::UnsupportedFormat(v) => write!(
                f,
                "log format version {v} is not the one this build reads ({WAL_FORMAT}); open \
                 the data directory with the Fusee version that wrote it"
            ),
        }
    }
}

impl std::error::Error for WalError {}

/// The header block of one life of a segment file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalHeader {
    pub group: u32,
    pub segment: u64,
    /// New for every life of the file; seeds every batch checksum in it.
    pub salt: u64,
    /// Sequence of the first batch in this segment.
    pub base_seq: u64,
    /// Checksum of the batch before `base_seq` (the chain continues across
    /// segments), or 0 for the first batch of the group.
    pub base_prev: u64,
    pub created_ms: u64,
}

impl WalHeader {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(WAL_HEADER_BYTES as usize);
        out.extend_from_slice(WAL_MAGIC);
        out.extend_from_slice(&WAL_FORMAT.to_le_bytes());
        out.extend_from_slice(&self.group.to_le_bytes());
        out.extend_from_slice(&self.segment.to_le_bytes());
        out.extend_from_slice(&self.salt.to_le_bytes());
        out.extend_from_slice(&self.base_seq.to_le_bytes());
        out.extend_from_slice(&self.base_prev.to_le_bytes());
        out.extend_from_slice(&self.created_ms.to_le_bytes());
        let crc = crc32c::crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out.resize(WAL_HEADER_BYTES as usize, 0);
        out
    }

    /// `Ok(None)` for an all-zero (never activated) header.
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>, WalError> {
        const LEN: usize = 8 + 4 + 4 + 8 * 5;
        let Some(head) = bytes.get(..LEN + 4) else {
            return if bytes.iter().all(|b| *b == 0) {
                Ok(None)
            } else {
                Err(WalError::BadHeader)
            };
        };
        if head.iter().all(|b| *b == 0) {
            return Ok(None);
        }
        if &head[..8] != WAL_MAGIC {
            return Err(WalError::BadHeader);
        }
        let crc = u32::from_le_bytes(head[LEN..LEN + 4].try_into().unwrap());
        if crc32c::crc32c(&head[..LEN]) != crc {
            return Err(WalError::BadHeader);
        }
        let format = u32::from_le_bytes(head[8..12].try_into().unwrap());
        if format != WAL_FORMAT {
            return Err(WalError::UnsupportedFormat(format));
        }
        let u64_at = |i: usize| u64::from_le_bytes(head[i..i + 8].try_into().unwrap());
        Ok(Some(Self {
            group: u32::from_le_bytes(head[12..16].try_into().unwrap()),
            segment: u64_at(16),
            salt: u64_at(24),
            base_seq: u64_at(32),
            base_prev: u64_at(40),
            created_ms: u64_at(48),
        }))
    }
}

/// One shard's consecutive records in a batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SectionRef<'a> {
    pub shard: u32,
    pub first_lsn: Lsn,
    pub count: u32,
    /// Concatenated canonical records.
    pub records: &'a [u8],
}

/// The fields of a batch header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchHeader {
    pub seq: u64,
    pub prev: u64,
    pub acked: u64,
    pub life: u64,
    pub flags: u32,
}

/// The chain checksum of a batch: its covered bytes, salt excluded, so the
/// chain continues across segments.
pub fn batch_checksum(covered: &[u8]) -> u64 {
    xxh64(covered, 0x4655_5345_4557_414c)
}

/// Appends one batch to `out`, padded to a multiple of `align` (8 or
/// [`BLOCK`]). Returns its chain checksum.
pub fn encode_batch(
    out: &mut Vec<u8>,
    salt: u64,
    header: &BatchHeader,
    sections: &[SectionRef<'_>],
    align: usize,
) -> u64 {
    debug_assert!(align.is_power_of_two() && align >= 8);
    let start = out.len();
    out.extend_from_slice(&[0; 8]); // len, crc
    let body_len = BATCH_HEADER
        + sections
            .iter()
            .map(|s| SECTION_HEADER + s.records.len())
            .sum::<usize>();
    out.extend_from_slice(&(body_len as u32).to_le_bytes());
    out.extend_from_slice(&header.flags.to_le_bytes());
    out.extend_from_slice(&header.seq.to_le_bytes());
    out.extend_from_slice(&header.prev.to_le_bytes());
    out.extend_from_slice(&header.acked.to_le_bytes());
    out.extend_from_slice(&header.life.to_le_bytes());
    out.extend_from_slice(&(sections.len() as u32).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    for s in sections {
        out.extend_from_slice(&s.shard.to_le_bytes());
        out.extend_from_slice(&s.count.to_le_bytes());
        out.extend_from_slice(&s.first_lsn.to_le_bytes());
        out.extend_from_slice(&(s.records.len() as u32).to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(s.records);
    }
    debug_assert_eq!(out.len() - start, body_len);
    let len = body_len.div_ceil(align) * align;
    out.resize(start + len, 0);
    out[start..start + 4].copy_from_slice(&(len as u32).to_le_bytes());
    let covered = &out[start + 8..start + body_len];
    let crc = crc32c::crc32c_append(crc32c::crc32c(&salt.to_le_bytes()), covered);
    let checksum = batch_checksum(covered);
    out[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
    checksum
}

/// Encoded size of a batch carrying `sections`, before padding.
pub fn batch_body_len(sections: &[SectionRef<'_>]) -> usize {
    BATCH_HEADER
        + sections
            .iter()
            .map(|s| SECTION_HEADER + s.records.len())
            .sum::<usize>()
}

/// How far the next batch starts from a batch of `body_len` bytes placed
/// at `offset` (a multiple of 8): the body rounded up to 8 bytes, or, when
/// `pipelined`, up to the end of the block it ends in, so that the next
/// batch starts on a block boundary even when this one did not. Writes in
/// flight together then never share a block, which `O_DIRECT` (whole-block
/// writes) needs. A batch that would grow past [`MAX_BATCH`] that way keeps
/// the 8-byte rounding.
pub fn batch_span(offset: u64, body_len: usize, pipelined: bool) -> u64 {
    let packed = (body_len as u64).div_ceil(8) * 8;
    if !pipelined {
        return packed;
    }
    let span = (offset + body_len as u64).div_ceil(BLOCK) * BLOCK - offset;
    if span > MAX_BATCH as u64 {
        packed
    } else {
        span
    }
}

/// Pads an encoded batch with zeros to `span` bytes and records that as
/// its distance to the next batch. The length field is outside the
/// checksums, so the checksums stay valid.
fn set_span(batch: &mut Vec<u8>, span: u64) {
    debug_assert!(span as usize >= batch.len() && span.is_multiple_of(8));
    batch.resize(span as usize, 0);
    batch[0..4].copy_from_slice(&(span as u32).to_le_bytes());
}

/// A batch that passed its checksum.
#[derive(Clone, Copy, Debug)]
pub struct BatchView<'a> {
    pub header: BatchHeader,
    /// Distance to the next batch.
    pub len: usize,
    pub checksum: u64,
    body: &'a [u8],
    sections: u32,
}

impl<'a> BatchView<'a> {
    /// Sections in order. Each is validated when the batch is read.
    pub fn sections(&self) -> impl Iterator<Item = SectionRef<'a>> + '_ {
        let body = self.body;
        let mut pos = 0;
        (0..self.sections).map(move |_| {
            let h = &body[pos..pos + SECTION_HEADER];
            let bytes = u32::from_le_bytes(h[16..20].try_into().unwrap()) as usize;
            let records = &body[pos + SECTION_HEADER..pos + SECTION_HEADER + bytes];
            pos += SECTION_HEADER + bytes;
            SectionRef {
                shard: u32::from_le_bytes(h[0..4].try_into().unwrap()),
                count: u32::from_le_bytes(h[4..8].try_into().unwrap()),
                first_lsn: u64::from_le_bytes(h[8..16].try_into().unwrap()),
                records,
            }
        })
    }
}

/// Decodes a section's records with their LSNs.
pub fn section_records(s: &SectionRef<'_>) -> DecodeResult<Vec<(Lsn, RecordBuf)>> {
    let mut r = Reader::new(s.records);
    let mut names = crate::record::InlineDecoder::default();
    let mut out = Vec::with_capacity(s.count as usize);
    while !r.is_empty() {
        let body = r.bytes()?;
        if is_offloaded(body) {
            return Err(DecodeError(
                "the section holds an offloaded record: read it with the log's blob area",
            ));
        }
        out.push((
            s.first_lsn + out.len() as u64,
            decode_body(body, &mut names)?,
        ));
    }
    if out.len() != s.count as usize {
        return Err(DecodeError("section record count mismatch"));
    }
    Ok(out)
}

/// Encodes records for a section.
pub fn encode_records<S: AsRef<str>, B: AsRef<[u8]>>(
    records: &[crate::record::Record<S, B>],
    out: &mut Vec<u8>,
) {
    for r in records {
        encode_canonical(r, out);
    }
}

enum Read<'a> {
    /// The batch header is all zeros: nothing was written here.
    Zero,
    Valid(BatchView<'a>),
    Bad,
    /// The batch continues past the prefix read so far.
    More,
}

/// Reads the batch at `pos` of a segment image prefix (`complete`: the whole
/// file). Checks length fields, the salted checksum and section structure.
fn read_batch(bytes: &[u8], pos: usize, salt: u64, complete: bool) -> Read<'_> {
    let Some(head) = bytes.get(pos..pos + BATCH_HEADER) else {
        if !complete {
            return Read::More;
        }
        return if bytes.get(pos..).is_none_or(|r| r.iter().all(|b| *b == 0)) {
            Read::Zero
        } else {
            Read::Bad
        };
    };
    if head.iter().all(|b| *b == 0) {
        return Read::Zero;
    }
    let u32_at = |i: usize| u32::from_le_bytes(head[i..i + 4].try_into().unwrap());
    let u64_at = |i: usize| u64::from_le_bytes(head[i..i + 8].try_into().unwrap());
    let len = u32_at(0) as usize;
    let body_len = u32_at(8) as usize;
    if !len.is_multiple_of(8) || body_len < BATCH_HEADER || body_len > len || len > MAX_BATCH {
        return Read::Bad;
    }
    let Some(covered) = bytes.get(pos + 8..pos + body_len) else {
        return if complete { Read::Bad } else { Read::More };
    };
    let crc = crc32c::crc32c_append(crc32c::crc32c(&salt.to_le_bytes()), covered);
    if crc != u32_at(4) {
        return Read::Bad;
    }
    let sections = u32_at(48);
    let body = &bytes[pos + BATCH_HEADER..pos + body_len];
    // Structure: sections exactly fill the body.
    let mut at = 0usize;
    for _ in 0..sections {
        let Some(h) = body.get(at..at + SECTION_HEADER) else {
            return Read::Bad;
        };
        let n = u32::from_le_bytes(h[16..20].try_into().unwrap()) as usize;
        at = match at.checked_add(SECTION_HEADER + n) {
            Some(a) if a <= body.len() => a,
            _ => return Read::Bad,
        };
    }
    if at != body.len() {
        return Read::Bad;
    }
    Read::Valid(BatchView {
        header: BatchHeader {
            seq: u64_at(16),
            prev: u64_at(24),
            acked: u64_at(32),
            life: u64_at(40),
            flags: u32_at(12),
        },
        len,
        checksum: batch_checksum(covered),
        body,
        sections,
    })
}

/// The end of the chain in a segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum End {
    /// Zeros follow the last batch.
    Clean,
    /// Bytes that are not the next batch follow: a write in flight that was
    /// never acknowledged, or stale bytes of an earlier life.
    Torn,
    /// A later batch says the batch at `at` had been acknowledged.
    Damaged { at: usize, later_seq: u64 },
    /// The segment is full: the chain continues in the next segment.
    Full,
}

/// The accepted chain through one segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Scanned {
    pub header: WalHeader,
    /// Offset just past the last accepted batch.
    pub end: usize,
    /// Sequence and checksum of the last accepted batch (the segment's base
    /// position when it holds none).
    pub last_seq: u64,
    pub last_checksum: u64,
    pub batches: u64,
    pub tail: End,
}

/// What [`WalScanner::advance`] concluded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Progress {
    Done(Scanned),
    /// The verdict depends on bytes past the prefix read so far.
    NeedMore,
}

/// Follows the chain through one segment from a growing prefix of its
/// bytes, like [`crate::segment::SegmentScanner`].
pub struct WalScanner {
    header: WalHeader,
    pos: usize,
    seq: u64,
    checksum: u64,
    batches: u64,
}

impl WalScanner {
    /// Starts at the segment's base. `expect` is the chain position the
    /// previous segment ended at (`None` for the first segment read, which
    /// must start at or before the position recovery needs). `Ok(None)` for a
    /// never-activated segment.
    pub fn start(bytes: &[u8], complete: bool) -> Result<Option<Self>, WalError> {
        debug_assert!(complete || bytes.len() >= WAL_HEADER_BYTES as usize);
        let Some(header) = WalHeader::decode(bytes)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            header,
            pos: WAL_HEADER_BYTES as usize,
            seq: header.base_seq.saturating_sub(1),
            checksum: header.base_prev,
            batches: 0,
        }))
    }

    pub fn header(&self) -> &WalHeader {
        &self.header
    }

    /// Continues over `bytes`, the segment's first `bytes.len()` bytes;
    /// `segment_len` is the file's length (batches never extend past it).
    /// Accepted batches are visited once each, in order.
    pub fn advance(
        &mut self,
        bytes: &[u8],
        complete: bool,
        segment_len: usize,
        visit: &mut dyn FnMut(&BatchView<'_>),
    ) -> Progress {
        let salt = self.header.salt;
        let tail = loop {
            if self.pos + BATCH_HEADER > segment_len {
                break End::Full;
            }
            match read_batch(bytes, self.pos, salt, complete) {
                Read::More => return Progress::NeedMore,
                Read::Zero => break End::Clean,
                Read::Valid(b)
                    if b.header.seq == self.seq + 1
                        && b.header.prev == self.checksum
                        && self.pos + b.len <= segment_len =>
                {
                    visit(&b);
                    self.seq = b.header.seq;
                    self.checksum = b.checksum;
                    self.batches += 1;
                    self.pos += b.len;
                }
                Read::Valid(_) | Read::Bad => match self.find_proof(bytes, complete, segment_len) {
                    Some(t) => break t,
                    None => return Progress::NeedMore,
                },
            }
        };
        Progress::Done(Scanned {
            header: self.header,
            end: self.pos,
            last_seq: self.seq,
            last_checksum: self.checksum,
            batches: self.batches,
            tail,
        })
    }

    /// After the chain's end at `self.pos`, looks for a batch of this
    /// segment life whose `acked` reaches past the end, within
    /// [`DAMAGE_WINDOW`] bytes or up to the first all-zero block.
    fn find_proof(&self, bytes: &[u8], complete: bool, segment_len: usize) -> Option<End> {
        let limit = (self.pos + DAMAGE_WINDOW).min(segment_len);
        let mut stop = limit;
        let mut block = self.pos.div_ceil(BLOCK as usize) * BLOCK as usize;
        while block + BLOCK as usize <= limit.min(bytes.len()) {
            if bytes[block..block + BLOCK as usize].iter().all(|b| *b == 0) {
                stop = block;
                break;
            }
            block += BLOCK as usize;
        }
        if stop == limit && !complete && bytes.len() < limit {
            return None;
        }
        let region = &bytes[..stop.min(bytes.len())];
        let mut at = self.pos + 8;
        while at + BATCH_HEADER <= region.len() {
            if let Read::Valid(b) = read_batch(region, at, self.header.salt, true)
                && b.header.seq > self.seq + 1
                && b.header.acked > self.seq
            {
                return Some(End::Damaged {
                    at: self.pos,
                    later_seq: b.header.seq,
                });
            }
            at += 8;
        }
        // Only reached past bytes that are not a zero header: a write in
        // flight or an earlier life's bytes.
        Some(End::Torn)
    }
}

/// Where the next batch goes. Pure: the I/O layer writes the bytes.
#[derive(Clone, Debug)]
pub struct WalWriter {
    group: u32,
    life: u64,
    segment: u64,
    salt: u64,
    segment_bytes: u64,
    offset: u64,
    next_seq: u64,
    prev: u64,
    started: bool,
}

/// Bytes to write at `offset` of `segment`; `header` is set when the batch
/// starts a new segment (write the header block first, or together).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Placed {
    pub segment: u64,
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub header: Option<WalHeader>,
    pub seq: u64,
}

impl WalWriter {
    /// Continues the chain after recovery: the next batch is `next_seq`, its
    /// `prev` is `prev`, and it goes at `offset` of `segment`, right after
    /// the accepted prefix (so the chain has one possible next position).
    /// Bytes a torn write left there are overwritten; any beyond the new
    /// batch can never join the chain.
    #[allow(clippy::too_many_arguments)]
    pub fn resume(
        group: u32,
        life: u64,
        segment: u64,
        salt: u64,
        segment_bytes: u64,
        offset: u64,
        next_seq: u64,
        prev: u64,
    ) -> Self {
        Self {
            group,
            life,
            segment,
            salt,
            segment_bytes,
            offset: offset.div_ceil(8) * 8,
            next_seq,
            prev,
            started: false,
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    pub fn segment(&self) -> u64 {
        self.segment
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Moves to a new segment with no batch to carry there: the header to
    /// write at its start, durably, before anything else uses it. The
    /// chain continues there exactly as after a roll (the header names the
    /// next batch and the checksum the chain ended with), so the segments
    /// before it can be retired once nothing needs their batches.
    pub fn roll(&mut self, segment: u64, salt: u64, segment_bytes: u64, now_ms: u64) -> WalHeader {
        self.segment = segment;
        self.salt = salt;
        self.segment_bytes = segment_bytes;
        self.offset = WAL_HEADER_BYTES;
        WalHeader {
            group: self.group,
            segment,
            salt,
            base_seq: self.next_seq,
            base_prev: self.prev,
            created_ms: now_ms,
        }
    }

    /// Encodes the next batch. `pipelined`: another write may start before
    /// this one completes, so the batch is padded to the end of its last
    /// block (see [`batch_span`]) and the next one starts in a block of its
    /// own. Without it, the next write shares this batch's last block and
    /// must not start before this one completed.
    /// `next_segment` supplies `(number, salt, size)` when the batch does not
    /// fit in the current segment.
    pub fn place(
        &mut self,
        sections: &[SectionRef<'_>],
        acked: u64,
        pipelined: bool,
        now_ms: u64,
        next_segment: impl FnOnce() -> (u64, u64, u64),
    ) -> Placed {
        let header = BatchHeader {
            seq: self.next_seq,
            prev: self.prev,
            acked,
            life: self.life,
            flags: if self.started { 0 } else { FLAG_LIFE_START },
        };
        let body = batch_body_len(sections);
        debug_assert!(body <= MAX_BATCH, "recovery refuses a longer batch");
        let mut bytes = Vec::with_capacity(body + BLOCK as usize);
        let mut checksum = encode_batch(&mut bytes, self.salt, &header, sections, 8);
        set_span(&mut bytes, batch_span(self.offset, body, pipelined));
        let mut new_header = None;
        if self.offset + bytes.len() as u64 > self.segment_bytes {
            let (segment, salt, size) = next_segment();
            self.segment = segment;
            self.salt = salt;
            self.segment_bytes = size;
            self.offset = WAL_HEADER_BYTES;
            new_header = Some(WalHeader {
                group: self.group,
                segment,
                salt,
                base_seq: self.next_seq,
                base_prev: self.prev,
                created_ms: now_ms,
            });
            bytes.clear();
            checksum = encode_batch(&mut bytes, salt, &header, sections, 8);
            set_span(&mut bytes, batch_span(self.offset, body, pipelined));
        }
        let placed = Placed {
            segment: self.segment,
            offset: self.offset,
            bytes,
            header: new_header,
            seq: self.next_seq,
        };
        self.offset += placed.bytes.len() as u64;
        self.prev = checksum;
        self.next_seq += 1;
        self.started = true;
        placed
    }
}

/// One shard's side of a shared log: assigns the shard's LSNs and encodes
/// its records, once, into the section its next batch carries. Pure.
#[derive(Clone, Debug)]
pub struct SectionWriter {
    next_lsn: Lsn,
    first: Lsn,
    count: u32,
    buf: Vec<u8>,
    /// Where the records not taken yet start in `buf`: past the ones a
    /// capped take ([`take_up_to`](Self::take_up_to)) copied out.
    start: usize,
    /// Where the last record's encoding starts in `buf`.
    last: usize,
}

/// Room a new section starts with (most sections are a few records).
const SECTION_BYTES: usize = 1024;

/// The most bytes of records a shard hands to its log at a time (one
/// record at least, however long). A shard that fell behind (a stalled
/// flush, a burst of starts) then writes its backlog as sections of this
/// size, so no batch, replicated entry or protocol frame built from
/// sections grows with the backlog; a flush that keeps up never comes near
/// it.
pub const MAX_SECTION: usize = 8 << 20;

impl SectionWriter {
    /// The shard's next record will be `next_lsn`.
    pub fn new(next_lsn: Lsn) -> Self {
        Self {
            next_lsn,
            first: next_lsn,
            count: 0,
            buf: Vec::new(),
            start: 0,
            last: 0,
        }
    }

    pub fn next_lsn(&self) -> Lsn {
        self.next_lsn
    }

    pub fn last_lsn(&self) -> Lsn {
        self.next_lsn - 1
    }

    pub fn has_pending(&self) -> bool {
        self.count > 0
    }

    /// Appends one record and returns its LSN.
    pub fn append<S: AsRef<str>, B: AsRef<[u8]>>(
        &mut self,
        rec: &crate::record::Record<S, B>,
    ) -> Result<Lsn, crate::segment::TooLarge> {
        if self.buf.capacity() == 0 {
            self.buf = crate::segment::spare_bytes(SECTION_BYTES);
        }
        // Encoded once, in place; the runtime's journal copies these bytes.
        let at = self.buf.len();
        encode_canonical(rec, &mut self.buf);
        if self.buf.len() - at + BATCH_HEADER + SECTION_HEADER > MAX_BATCH {
            self.buf.truncate(at);
            return Err(crate::segment::TooLarge);
        }
        if self.count == 0 {
            self.first = self.next_lsn;
        }
        self.last = at;
        self.count += 1;
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        Ok(lsn)
    }

    /// The canonical encoding of the record most recently appended (empty
    /// once its section was taken).
    pub fn last_encoded(&self) -> &[u8] {
        self.buf.get(self.last..).unwrap_or_default()
    }

    /// Forgets the records appended since the last call, keeping the
    /// buffer, for storage that holds nothing (`memory` mode): their count,
    /// bytes and last LSN.
    pub fn discard(&mut self) -> Option<(u32, u64, Lsn)> {
        if self.count == 0 {
            return None;
        }
        let out = (
            self.count,
            (self.buf.len() - self.start) as u64,
            self.next_lsn - 1,
        );
        self.buf.clear();
        self.start = 0;
        self.last = 0;
        self.count = 0;
        self.first = self.next_lsn;
        Some(out)
    }

    /// Takes every record not taken yet as one section.
    pub fn take(&mut self) -> Option<crate::segment::Batch> {
        self.take_up_to(usize::MAX)
    }

    /// Takes the oldest records not taken yet as one section of at most
    /// `max` bytes (one record at least); the rest stay for the next call.
    /// When everything fits, which is every call of a shard that keeps up,
    /// the section is the writer's buffer itself: nothing is copied.
    pub fn take_up_to(&mut self, max: usize) -> Option<crate::segment::Batch> {
        if self.count == 0 {
            return None;
        }
        let pending = self.buf.len() - self.start;
        let (section, records) = if pending <= max {
            // Records a capped take copied out are still at the front.
            if self.start > 0 {
                self.buf.drain(..self.start);
            }
            self.start = 0;
            self.last = 0;
            (std::mem::take(&mut self.buf), self.count)
        } else {
            // The leading records that fit, found by their length prefixes
            // and copied out; the buffer keeps the rest in place, so a long
            // backlog is copied once in all, not once per section.
            let mut r = Reader::new(&self.buf[self.start..]);
            let (mut end, mut records) = (0usize, 0u32);
            while records < self.count && r.blob().is_ok() {
                let at = pending - r.remaining();
                if records > 0 && at > max {
                    break;
                }
                end = at;
                records += 1;
            }
            let from = self.start;
            let section = self.buf[from..from + end].to_vec();
            self.start += end;
            // Give the copied prefix back once it is most of the buffer
            // (each byte moves at most once more).
            if self.start >= self.buf.len() / 2 {
                self.buf.drain(..self.start);
                self.last = match self.last.checked_sub(self.start) {
                    Some(last) => last,
                    None => self.buf.len(),
                };
                self.start = 0;
            }
            (section, records)
        };
        let first = self.first;
        self.first += u64::from(records);
        self.count -= records;
        Some(crate::segment::Batch {
            chunks: Vec::new(),
            first_lsn: first,
            last_lsn: self.first - 1,
            records,
            bytes: section.len() as u64,
            section,
        })
    }
}

// Payload offload (docs/durability.md, "Large payloads").
//
// A record whose canonical body is longer than the log's threshold is
// written to the log's blob area instead of into its batch; the section
// carries a reference in its place:
//
// ```text
// reference = OFFLOADED  kind:u8  file:varint  offset:varint  len:varint  crc:u32
// ```
//
// `kind` is the record's own kind (for tools), `file`, `offset` and `len`
// name the body's bytes in the blob area, `crc` is their CRC-32C. A batch
// holding a reference is written only after the bytes it names are
// durable, so recovery never meets a reference to bytes that were not
// written; the checksum catches damage. Everything above the log (journals,
// run stores, checkpoints, replication, server mode, SDKs) sees the
// record's canonical encoding: references exist only in log sections.

/// Kind byte of a reference to an offloaded record. Never a record kind.
pub const OFFLOADED: u8 = 0x7f;

/// Blob offsets are multiples of this, so each blob starts on a block of
/// its own (whole-block writes never touch another blob).
pub const BLOB_ALIGN: u64 = BLOCK;

/// Where an offloaded record's canonical body lives in the blob area.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OffloadRef {
    /// The record's kind ([`crate::record::kind`]).
    pub kind: u8,
    pub file: u64,
    pub offset: u64,
    pub len: u64,
    /// CRC-32C of the body.
    pub crc: u32,
}

impl OffloadRef {
    /// Appends the reference as a section record (with its length prefix,
    /// like [`encode_canonical`]).
    pub fn encode(&self, out: &mut Vec<u8>) {
        let mut body = Vec::with_capacity(32);
        body.push(OFFLOADED);
        body.push(self.kind);
        crate::wire::put_varint(&mut body, self.file);
        crate::wire::put_varint(&mut body, self.offset);
        crate::wire::put_varint(&mut body, self.len);
        body.extend_from_slice(&self.crc.to_le_bytes());
        crate::wire::put_bytes(out, &body);
    }

    /// Decodes a section record body that starts with [`OFFLOADED`].
    pub fn decode(body: &[u8]) -> DecodeResult<Self> {
        let mut r = Reader::new(body);
        if r.u8()? != OFFLOADED {
            return Err(DecodeError("not an offload reference"));
        }
        let kind = r.u8()?;
        let file = r.varint()?;
        let offset = r.varint()?;
        let len = r.varint()?;
        let crc = r.u32()?;
        if !r.is_empty() {
            return Err(DecodeError("trailing bytes after offload reference"));
        }
        if len as usize > crate::wire::MAX_PAYLOAD + 64 || !offset.is_multiple_of(BLOB_ALIGN) {
            return Err(DecodeError("bad offload reference"));
        }
        Ok(Self {
            kind,
            file,
            offset,
            len,
            crc,
        })
    }

    /// Whether `body` is the bytes this reference names.
    pub fn matches(&self, body: &[u8]) -> bool {
        body.len() as u64 == self.len
            && crc32c::crc32c(body) == self.crc
            && body.first() == Some(&self.kind)
    }
}

/// Whether a section record body is a reference to an offloaded record.
#[inline]
pub fn is_offloaded(body: &[u8]) -> bool {
    body.first() == Some(&OFFLOADED)
}

/// One record of a section: where its length prefix starts, and its body.
#[derive(Clone, Copy, Debug)]
pub struct SectionRecord<'a> {
    pub at: usize,
    pub body: &'a [u8],
}

/// The records of a section's bytes, in order, with their positions.
pub fn section_spans(records: &[u8]) -> impl Iterator<Item = DecodeResult<SectionRecord<'_>>> {
    let mut r = Reader::new(records);
    let total = records.len();
    std::iter::from_fn(move || {
        if r.is_empty() {
            return None;
        }
        let at = total - r.remaining();
        Some(r.bytes().map(|body| SectionRecord { at, body }))
    })
}

/// Calls `f` with the index in the section and the reference of every
/// offloaded record in a section's bytes.
pub fn for_each_offload(records: &[u8], mut f: impl FnMut(u32, OffloadRef)) -> DecodeResult<()> {
    for (i, rec) in section_spans(records).enumerate() {
        let rec = rec?;
        if is_offloaded(rec.body) {
            f(i as u32, OffloadRef::decode(rec.body)?);
        }
    }
    Ok(())
}

/// A record over the offload threshold, found in a section by
/// [`find_large`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Large {
    /// Index of the record in the section.
    pub index: u32,
    /// Where its length prefix starts, and its body, in the section's bytes.
    pub at: usize,
    pub body: std::ops::Range<usize>,
    pub crc: u32,
}

/// The records of a section's bytes whose canonical body is longer than
/// `threshold`, with their checksums. Pure and lock-free: the shard that
/// wrote the section runs it, so the checksum of a large payload costs only
/// that shard.
pub fn find_large(records: &[u8], threshold: usize) -> DecodeResult<Vec<Large>> {
    let mut out = Vec::new();
    if records.len() <= threshold {
        return Ok(out);
    }
    for (i, rec) in section_spans(records).enumerate() {
        let rec = rec?;
        if rec.body.len() > threshold && !is_offloaded(rec.body) {
            let start = rec.body.as_ptr() as usize - records.as_ptr() as usize;
            out.push(Large {
                index: i as u32,
                at: rec.at,
                body: start..start + rec.body.len(),
                crc: crc32c::crc32c(rec.body),
            });
        }
    }
    Ok(out)
}

/// Why an offloaded record could not be read back.
#[derive(Debug)]
pub enum ResolveError {
    Decode(DecodeError),
    /// The blob area does not hold what the reference names.
    Blob {
        at: Lsn,
        r: OffloadRef,
        why: String,
    },
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(e) => write!(f, "{e}"),
            Self::Blob { at, r, why } => write!(
                f,
                "record {at} references bytes {}..{} of blob file {} ({why})",
                r.offset,
                r.offset + r.len,
                r.file
            ),
        }
    }
}

impl From<DecodeError> for ResolveError {
    fn from(e: DecodeError) -> Self {
        Self::Decode(e)
    }
}

/// [`section_records`] for a section that may hold references, keeping
/// only the records after `after` (the others are neither decoded nor
/// read back): `fetch` reads the bytes a reference names (they are checked
/// here).
pub fn section_records_resolved(
    s: &SectionRef<'_>,
    after: Lsn,
    fetch: &mut dyn FnMut(&OffloadRef) -> std::io::Result<Vec<u8>>,
) -> Result<Vec<(Lsn, RecordBuf)>, ResolveError> {
    let mut r = Reader::new(s.records);
    let mut names = crate::record::InlineDecoder::default();
    let mut out = Vec::with_capacity(s.count as usize);
    let mut n = 0u64;
    while !r.is_empty() {
        let body = r.bytes()?;
        let lsn = s.first_lsn + n;
        n += 1;
        if lsn <= after {
            continue;
        }
        let rec = if is_offloaded(body) {
            let reference = OffloadRef::decode(body)?;
            let fail = |why: String| ResolveError::Blob {
                at: lsn,
                r: reference,
                why,
            };
            let bytes = fetch(&reference).map_err(|e| fail(e.to_string()))?;
            if !reference.matches(&bytes) {
                return Err(fail(if bytes.len() as u64 != reference.len {
                    format!("{} bytes there", bytes.len())
                } else {
                    "checksum mismatch".into()
                }));
            }
            decode_body(&bytes, &mut names)?
        } else {
            decode_body(body, &mut names)?
        };
        out.push((lsn, rec));
    }
    if n != u64::from(s.count) {
        return Err(DecodeError("section record count mismatch").into());
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
