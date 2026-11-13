//! The server-mode wire protocol, carried in [`fusee_core::frame`] frames.
//!
//! A worker (an application process running workflow code) leases the
//! server's shards and drives each shard's runtime against the server's
//! storage. Every request carries the lease epoch; the server refuses stale
//! epochs, so a worker that lost its lease can never append again.

use fusee_core::segment::{Batch, Chunk};
use fusee_core::wire::{
    DecodeError, DecodeResult, Reader, put_bytes, put_u32, put_u64, put_varint,
};

pub mod req {
    pub const HELLO: u8 = 1;
    pub const SUBMIT: u8 = 2;
    pub const STORE_APPEND: u8 = 3;
    pub const STORE_READ: u8 = 4;
    pub const CHECKPOINT: u8 = 5;
    pub const LOAD_CHECKPOINT: u8 = 6;
    pub const LOAD_STORE: u8 = 7;
    pub const SEGMENTS: u8 = 8;
    pub const READ_SEGMENT: u8 = 9;
    pub const SYNC_RECOVERED: u8 = 10;
    pub const DISCARD: u8 = 11;
    pub const OPEN_LOG: u8 = 12;
    pub const PING: u8 = 13;
    /// A limit operation from the current lease holder.
    pub const LIMIT: u8 = 14;
    /// A client (not a worker) connects: it never takes the lease.
    pub const CLIENT_HELLO: u8 = 15;
    /// A client's call for the worker: `method, JSON arguments`.
    pub const CALL: u8 = 16;
    /// The worker's answer to a relayed call: `relay u64, ok u8, bytes`.
    pub const CALL_REPLY: u8 = 17;
    /// Begin (0), copy reference page (1), or finish (2) a run-store rewrite.
    pub const COMPACT_STORE: u8 = 18;
    /// Bounded candidate IDs for a secondary-index lookup.
    pub const VISIBILITY: u8 = 19;
    pub const SEGMENT_EXTENT: u8 = 20;
    pub const SEGMENT_RANGE: u8 = 21;
    pub const QUORUM_PROBE: u8 = 22;
    /// A run-store append at an offset the worker computed:
    /// `expected u64, frame bytes[, run u64, lsn u64]`. Used only after the
    /// lease granted [`STORE_OFFSETS`](super::STORE_OFFSETS).
    pub const STORE_APPEND_AT: u8 = 23;
    /// Log format 3 ([`SHARED_LOG`](super::SHARED_LOG)): one shard's
    /// records as a section of the server's shared log, see
    /// [`encode_section`](super::encode_section). Not answered by itself:
    /// the shard's position arrives in a `DURABLE` frame once a batch
    /// holding it is durable, or `IO_FAILED`.
    pub const APPEND: u8 = 24;
    /// Log format 3: `after u64, start u32`. Answered with the shard's
    /// sections whose records reach past `after`, from section `start`:
    /// `last u64, total u32, count u32, sections` (see
    /// [`decode_sections`](super::decode_sections)); the worker asks again
    /// from `start + count` until it has `total`.
    pub const RECOVER_LOG: u8 = 25;
    /// [`MESSAGE_RELAY`](super::MESSAGE_RELAY): messages from the head's
    /// shard (under the head's epoch) to a shard another worker holds:
    /// `target u32, entries bytes` (the sender's outbox encoding). Not
    /// answered; the server forwards them to the target's holder as
    /// `MESSAGES`, or keeps them until the target has one.
    pub const RELAY: u8 = 26;
    /// [`MESSAGE_RELAY`](super::MESSAGE_RELAY): the head's shard applied
    /// relayed messages: `count u32, then shard u32, epoch u64, id u64`
    /// each, naming the sender's shard, lease epoch and outbox entry. Not
    /// answered; forwarded to the holder of that shard under that epoch
    /// only, else dropped.
    pub const RELAY_ACKS: u8 = 27;
    /// [`INVOKED`](super::INVOKED): stop sending this worker calls and
    /// messages for these shards (`count u32, then shard u32` each); they
    /// wait at the server for the next holder. Answered with `OK` after
    /// every call routed to the worker before it.
    pub const DRAIN: u8 = 28;
    /// [`INVOKED`](super::INVOKED): what the shards have left to do, sent
    /// once before the worker closes the connection: `count u32`, then
    /// per shard `shard u32, epoch u64, flags u8 (bit 0: busy, bit 1: the
    /// budget ended), due_in_ms u64 (u64::MAX: nothing due), steps u64,
    /// open_us u64, types u32, then each type as bytes` (workflow types
    /// with runs this worker had no code for). Not answered.
    pub const IDLE: u8 = 29;
}

// Capability bits. `HELLO` carries the worker's offer after its ID; `LEASE`
// carries the bits the server grants, then each granted bit's parameters in
// bit order. A peer ignores bits it does not know.
pub const PAGED_RECOVERY: u32 = 1;
pub const FIRST_SEGMENT_LIMIT: u32 = 2;
/// Every `STORE_APPEND_AT` is answered with `STORE_APPENDED` carrying the
/// offset the server wrote at, or with `IO_FAILED`. The server refuses an
/// append whose expected offset is not its store's length and then stops the
/// shard for the rest of the lease.
pub const STORE_OFFSETS: u32 = 4;
/// The worker sends `PING` at least every interval while it holds the lease;
/// the server revokes the lease after the timeout without traffic. The lease
/// parameters are `ping_interval_ms u32, timeout_ms u32`. Granted only to a
/// worker that offered it.
pub const LEASE_LIVENESS: u32 = 8;
/// The worker's engine is the only writer of the shard's segment files, so
/// its recovery may delete trailing segments without a valid record and
/// reuse their numbers. Not granted over replicated storage, where the
/// replica's applier writes segments too. No parameters.
pub const EXCLUSIVE_LOG: u32 = 16;
/// Log format 3: the server keeps one log for every shard. The worker sends
/// sections (`APPEND`), gets cumulative `DURABLE` positions, and recovers
/// with `RECOVER_LOG`; the segment requests are unused. No parameters. A
/// server with a shared log refuses a worker that does not offer it.
pub const SHARED_LOG: u32 = 32;
/// Shards are leased one at a time, each with its own epoch. `HELLO`
/// carries, after the offer, the most shards the worker takes at once (`0`:
/// all). The lease parameter is the shards granted now: `count u32, then
/// shard u32, epoch u64` each; later grants arrive as `GRANT` frames, and
/// every shard request carries its shard's epoch. A worker without it
/// waits until every shard is free and gets them all under one epoch.
pub const SHARD_LEASES: u32 = 64;
/// Messages between workflows on shards held by different workers go
/// through the server (`RELAY`, `RELAY_ACKS`, `MESSAGES`, `MESSAGE_ACKS`).
/// Granted with [`SHARD_LEASES`]. No parameters.
pub const MESSAGE_RELAY: u32 = 128;
/// Same-host shared memory over a Unix socket (`unix:/path?shm=1`, see
/// [`crate::shm`]): the lease frame carries the mapping and the server's
/// doorbell page as descriptors, and its parameter is `ring u64`, the
/// ring's size. Log sections then go through the ring and durable
/// positions come back in the mapping; everything else stays on the
/// socket. The worker's heartbeat in the mapping replaces `PING`. Granted
/// with [`SHARD_LEASES`] and [`SHARED_LOG`] only.
pub const SHARED_MEMORY: u32 = 256;
/// An invoked worker ([`crate::invoke`]): `HELLO` carries, after the most
/// shards to hold, the invocation's ID `u64`. The server grants that
/// invocation's free shards at once, never more and never later (no
/// standby), or refuses the connection. The lease parameter is
/// `remaining_ms u32`, the invocation's budget left. Granted with
/// [`SHARD_LEASES`] only. The worker drains (`DRAIN`) and reports
/// (`IDLE`) before it leaves.
pub const INVOKED: u32 = 512;
/// Bit 0 of an `IDLE` entry's flags: the shard still had work.
pub const IDLE_BUSY: u8 = 1;
/// Bit 1 of an `IDLE` entry's flags: the invocation's budget ended.
pub const IDLE_EXPIRED: u8 = 2;
pub const RECOVERY_PAGE: usize = 1 << 20;

pub mod resp {
    /// Lease granted: `shards u32, epoch u64`.
    pub const LEASE: u8 = 101;
    /// Success; body is request-specific.
    pub const OK: u8 = 102;
    /// Failure; body is a UTF-8 message.
    pub const ERR: u8 = 103;
    /// An asynchronous submit completed: `shard u32, lsn u64`.
    pub const FLUSHED: u8 = 104;
    /// An asynchronous checkpoint completed: `shard u32, lsn u64`.
    pub const CHECKPOINTED: u8 = 105;
    /// An asynchronous submit or checkpoint failed: `shard u32, message`.
    pub const IO_FAILED: u8 = 106;
    /// A client's call relayed to the worker: `relay u64, method, args`.
    pub const CALL: u8 = 107;
    /// A limit acquire was answered: `ticket u64, granted u8, retry_at u64`.
    pub const LIMITED: u8 = 108;
    pub const QUORUM_READY: u8 = 109;
    /// A `STORE_APPEND_AT` was written: `shard u32, offset u64`.
    pub const STORE_APPENDED: u8 = 110;
    /// Log format 3: shards' durable positions after a batch, cumulative
    /// (a later frame never lowers one): `id u64 (0), position u64, segment
    /// u64, count u32, then per shard: shard u32, lsn u64, flags u8` (bit 0:
    /// the shard alone holds back retiring the oldest log segment).
    /// `position` is the next batch's sequence and `segment` the active
    /// one, for checkpoints.
    pub const DURABLE: u8 = 111;
    /// More shards for a [`SHARD_LEASES`](super::SHARD_LEASES) worker:
    /// `id u64 (0), count u32, then shard u32, epoch u64` each.
    pub const GRANT: u8 = 112;
    /// Relayed messages for a shard this worker holds: `id u64 (0), to
    /// u32, from u32, epoch u64 (the sender's lease), entries bytes`.
    pub const MESSAGES: u8 = 113;
    /// Relayed acknowledgments for shards this worker holds: `id u64 (0),
    /// count u32, then shard u32, epoch u64, outbox id u64` each.
    pub const MESSAGE_ACKS: u8 = 114;
}

/// A `DURABLE` flag: one more checkpoint of the shard lets the log retire
/// its oldest segment.
pub const DURABLE_PINS: u8 = 1;

/// The body of an `APPEND` after the head: `first_lsn varint, last_lsn
/// varint, records varint, section bytes`.
pub fn encode_section(out: &mut Vec<u8>, b: &Batch) {
    put_varint(out, b.first_lsn);
    put_varint(out, b.last_lsn);
    put_varint(out, u64::from(b.records));
    put_bytes(out, &b.section);
}

pub fn decode_section(r: &mut Reader<'_>) -> DecodeResult<Batch> {
    let first_lsn = r.varint()?;
    let last_lsn = r.varint()?;
    let records = r.varint_u32()?;
    let section = r.blob()?.to_vec();
    if last_lsn < first_lsn || last_lsn - first_lsn + 1 != u64::from(records) {
        return Err(DecodeError("section LSNs do not match its record count"));
    }
    Ok(Batch {
        chunks: Vec::new(),
        first_lsn,
        last_lsn,
        records,
        bytes: section.len() as u64,
        section,
    })
}

/// Sections in a `RECOVER_LOG` answer: `first_lsn varint, count varint,
/// bytes` each.
pub fn encode_sections(out: &mut Vec<u8>, sections: &[fusee::wal::RawSection]) {
    for (first, count, bytes) in sections {
        put_varint(out, *first);
        put_varint(out, u64::from(*count));
        put_bytes(out, bytes);
    }
}

pub fn decode_sections(
    r: &mut Reader<'_>,
    count: u32,
    out: &mut Vec<fusee::wal::RawSection>,
) -> DecodeResult<()> {
    for _ in 0..count {
        let first = r.varint()?;
        let n = r.varint_u32()?;
        if n == 0 {
            return Err(DecodeError("empty log section"));
        }
        out.push((first, n, r.blob()?.to_vec()));
    }
    Ok(())
}

/// Header of every request after the kind byte: `id, shard, epoch`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Head {
    pub id: u64,
    pub shard: u32,
    pub epoch: u64,
}

pub fn head(out: &mut Vec<u8>, h: Head) {
    put_u64(out, h.id);
    put_u32(out, h.shard);
    put_u64(out, h.epoch);
}

pub fn read_head(r: &mut Reader<'_>) -> DecodeResult<Head> {
    Ok(Head {
        id: r.u64()?,
        shard: r.u32()?,
        epoch: r.u64()?,
    })
}

pub fn encode_batch(out: &mut Vec<u8>, b: &Batch) {
    put_varint(out, b.first_lsn);
    put_varint(out, b.last_lsn);
    put_varint(out, u64::from(b.records));
    put_varint(out, b.bytes);
    put_varint(out, b.chunks.len() as u64);
    for c in &b.chunks {
        put_varint(out, c.segment);
        put_varint(out, c.offset);
        out.push(u8::from(c.starts_segment));
        put_bytes(out, &c.bytes);
    }
}

pub fn decode_batch(r: &mut Reader<'_>) -> DecodeResult<Batch> {
    let first_lsn = r.varint()?;
    let last_lsn = r.varint()?;
    let records = r.varint_u32()?;
    let bytes = r.varint()?;
    let n = r.varint()?;
    if n > 1 << 16 {
        return Err(DecodeError("too many chunks"));
    }
    let mut chunks = Vec::with_capacity(n as usize);
    for _ in 0..n {
        chunks.push(Chunk {
            segment: r.varint()?,
            offset: r.varint()?,
            starts_segment: r.u8()? != 0,
            bytes: r.blob()?.to_vec(),
        });
    }
    Ok(Batch {
        chunks,
        first_lsn,
        last_lsn,
        records,
        bytes,
        section: Vec::new(),
    })
}
