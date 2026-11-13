//! The I/O boundary of a shard.
//!
//! A [`ShardRuntime`](crate::runtime::ShardRuntime) touches the outside
//! world only through [`Env`]: the clock, randomness, the log, the run store
//! and checkpoints. Production uses [`LocalEnv`](crate::local::LocalEnv)
//! (files, a flusher thread) or [`MemoryEnv`](crate::local::MemoryEnv); the
//! simulator supplies one whose disk tears writes and loses unflushed data
//! under a seeded scheduler. Everything above this trait is deterministic.

use fusee_core::segment::Batch;
use fusee_core::shard::BlobRef;
use fusee_core::store::Checkpoint;
use fusee_core::{Lsn, RunId};
use std::io;
use std::sync::Arc;

/// Called from any thread when background I/O completes, to wake the shard.
pub type Notify = Arc<dyn Fn() + Send + Sync>;

/// See [`Env::log_stats`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogStats {
    pub writes: u64,
    pub device_bytes: u64,
    pub rolls: u64,
    /// The log owner thread writes (Linux, `io_uring`; see `wal/owner.rs`).
    pub owner: bool,
}

pub trait Env {
    /// Unix time in milliseconds.
    fn now_ms(&self) -> u64;

    /// A clock for elapsed-time measurements. Virtual environments use their own clock.
    fn monotonic_us(&self) -> u64 {
        self.now_ms().saturating_mul(1000)
    }

    /// A random 64-bit value (deterministic under simulation).
    fn random(&mut self) -> u64;

    /// Installs the callback used to wake the shard when a background flush
    /// or checkpoint completes.
    fn set_notify(&mut self, notify: Notify);

    /// Writes `batch` and makes it durable in the background. The runtime
    /// keeps at most [`max_in_flight`](Self::max_in_flight) batches in
    /// flight, each following the last, and reports the next batch only
    /// while fewer are outstanding.
    fn submit(&mut self, batch: Batch);

    /// Records are durable as soon as they are appended (`memory` mode):
    /// the runtime then builds no batch and calls no flush for them.
    fn durable_on_append(&self) -> bool {
        false
    }

    /// How many submitted batches may be outstanding at once. With more
    /// than one, [`poll_flushed`](Self::poll_flushed) reports each advance
    /// of the durable position, which may cover only the earlier batches.
    fn max_in_flight(&self) -> usize {
        1
    }

    /// Whether [`flush_inline`](Self::flush_inline) is available. The runtime
    /// flushes inline when it has nothing else to do, which saves two thread
    /// handoffs per flush at low load.
    fn supports_inline(&self) -> bool {
        false
    }

    /// Writes `batch` and makes it durable on the calling thread.
    fn flush_inline(&mut self, batch: Batch) -> io::Result<Lsn> {
        let last = batch.last_lsn;
        self.submit(batch);
        Ok(last)
    }

    /// Duration of the most recent flush in microseconds (0 if unknown).
    /// The runtime flushes inline only on fast disks.
    fn last_flush_us(&self) -> u64 {
        0
    }

    /// The outcome of the in-flight background flush, once it completed:
    /// the position now durable. With several batches in flight, any
    /// advance of it (see [`max_in_flight`](Self::max_in_flight)).
    fn poll_flushed(&mut self) -> Option<io::Result<Lsn>>;

    /// Appends one run store frame (not yet durable). Returns its offset.
    fn store_append(&mut self, frame: &[u8]) -> io::Result<u64>;

    /// Append a full terminal journal selected for immutable archival.
    fn store_append_history(&mut self, frame: &[u8], _run: RunId, _lsn: Lsn) -> io::Result<u64> {
        self.store_append(frame)
    }

    /// Reads `len` bytes of the run store at `offset`.
    fn store_read(&mut self, offset: u64, len: u32) -> io::Result<Vec<u8>>;

    fn store_len(&self) -> u64;

    fn store_generation(&self) -> u64;

    /// Copy these live frames in order into a fresh generation. A successful
    /// replacement starts with the store header. The preceding generation
    /// remains recoverable until its last checkpoint is safely superseded.
    fn compact_store(&mut self, _live: &[(RunId, BlobRef)]) -> io::Result<bool> {
        Ok(false)
    }

    /// Secondary-index candidates for a checkpointed store prefix. Callers
    /// reconcile newer frames and validate matches against current state.
    fn visibility_candidates(
        &mut self,
        _filter: &crate::visibility::ListFilter,
        _limit: usize,
    ) -> Option<crate::visibility::Candidates> {
        None
    }

    /// Makes the run store durable through its current length, writes the
    /// checkpoint atomically, then deletes log segments numbered at or below
    /// `delete_through_segment`. May run in the background; completion is
    /// reported by [`poll_checkpoint`](Self::poll_checkpoint).
    fn checkpoint(&mut self, checkpoint: Checkpoint, delete_through_segment: Option<u64>);

    /// `Some` once the last checkpoint finished.
    fn poll_checkpoint(&mut self) -> Option<io::Result<Lsn>>;

    /// The newest valid checkpoint, if any.
    fn load_checkpoint(&mut self) -> io::Result<Option<Checkpoint>>;

    /// Reads the run store prefix named by a checkpoint (header included).
    /// With no checkpoint, the whole store is ignored and a fresh one begun.
    fn load_store(&mut self, generation: u64, len: u64) -> io::Result<Vec<u8>>;

    /// Visit the checkpoint's committed store prefix. File and remote
    /// implementations can stream it without materializing every journal.
    ///
    /// When the checkpoint [`load_checkpoint`](Self::load_checkpoint) last
    /// returned names a persisted run index ([`Checkpoint::index`]), an
    /// implementation may select the prefix for reads and appends without
    /// visiting anything: recovery then reads only the index frames. It
    /// must visit every frame on a second call for the same prefix.
    fn recover_store(
        &mut self,
        generation: u64,
        len: u64,
        visit: &mut dyn FnMut(u64, u32, fusee_core::store::StoreEntry),
    ) -> io::Result<()> {
        let bytes = self.load_store(generation, len)?;
        fusee_core::store::scan_store(&bytes, visit).map_err(io::Error::other)
    }

    /// Whether a later open recovers what this environment stores (false
    /// for memory mode). Without that, the runtime writes nothing only
    /// recovery reads, such as the persisted run index.
    fn recovers(&self) -> bool {
        true
    }

    /// Log segment numbers present, ascending.
    fn segments(&mut self) -> io::Result<Vec<u64>>;

    fn read_segment(&mut self, segment: u64) -> io::Result<Vec<u8>>;

    /// Reads about `len` bytes of segment `segment` starting at `offset`,
    /// fewer at the end of the file; the flag says the bytes reach the end.
    /// Recovery reads a segment's valid prefix this way instead of the whole
    /// preallocated file. The default reads everything at once.
    fn read_segment_from(
        &mut self,
        segment: u64,
        offset: u64,
        len: usize,
    ) -> io::Result<(Vec<u8>, bool)> {
        let _ = len;
        let mut bytes = self.read_segment(segment)?;
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        Ok((bytes.split_off(start), true))
    }

    /// The segment's header block (or the whole file, if shorter). Recovery
    /// reads only headers of segments that lie wholly behind the checkpoint,
    /// and does not count them as recovered.
    fn read_segment_header(&mut self, segment: u64) -> io::Result<Vec<u8>> {
        let mut bytes = self.read_segment(segment)?;
        bytes.truncate(fusee_core::segment::HEADER_BYTES as usize);
        Ok(bytes)
    }

    /// Makes everything read during recovery durable (it may have been in
    /// the page cache only when the process died).
    fn sync_recovered(&mut self) -> io::Result<()>;

    /// Removes a segment that recovery found was never activated (its first
    /// flush tore its header), so later recoveries never mistake it for a
    /// damaged segment in the middle of the log.
    fn discard_segment(&mut self, segment: u64) -> io::Result<()>;

    /// Log format 3: records go to a log shared with other shards, recovery
    /// gets them from [`recover_log`](Self::recover_log), and the segment
    /// methods are unused.
    fn shared_log(&self) -> bool {
        false
    }

    /// Log format 3: this shard's records after `after`, in order, and its
    /// last LSN in the log (`after` if none is later).
    fn recover_log(
        &mut self,
        _after: Lsn,
    ) -> io::Result<(Vec<(Lsn, fusee_core::record::RecordBuf)>, Lsn)> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "this storage has per-shard segments",
        ))
    }

    /// Log format 3: this shard alone holds back retiring a segment of the
    /// shared log; one more checkpoint (even with nothing new) frees it.
    fn checkpoint_wanted(&self) -> bool {
        false
    }

    /// After the log rolled to a new segment: whether this (active) shard
    /// should checkpoint for it. Per-shard logs: always. The shared log:
    /// only a shard whose oldest retained record is in the segment it
    /// retires next.
    fn checkpoint_on_roll(&self) -> bool {
        true
    }

    /// At a clean close: this shard holds any segment of the shared log
    /// older than the active one. Every such shard checkpoints, not only
    /// the one holding the oldest (shards close at once, so the next
    /// holder may already be gone when the oldest one lets go).
    fn checkpoint_wanted_on_close(&self) -> bool {
        self.checkpoint_wanted()
    }

    /// Log format 3: the shared log's active segment (checkpoints follow
    /// segment rolls). 0 for per-shard logs.
    fn log_segment(&self) -> u64 {
        0
    }

    /// The shared log's own counters, reported by one shard of the log so
    /// that a sum over shards counts them once: device writes, bytes those
    /// writes put on the device (headers and padding included), and
    /// segment rolls. Zeros elsewhere.
    fn log_stats(&self) -> LogStats {
        LogStats::default()
    }

    /// Log format 3 position for a checkpoint taken now: the sequence of the
    /// first shared-log batch that can hold this shard's later records. 0
    /// for per-shard (format 2) logs.
    fn log_position(&self) -> u64 {
        0
    }

    /// Whether this shard's engine is the only writer of its segment files,
    /// so recovery may delete trailing segments that hold no valid record
    /// and reuse their numbers. Replicated storage, where an applier also
    /// writes segments, must say no.
    fn exclusive_log(&self) -> bool {
        false
    }

    /// Whether a segment file is being prepared in the background (stall
    /// tracing reports it).
    fn preparing_segment(&self) -> bool {
        false
    }

    /// Capacity of the first segment after opening a shard. Full-size
    /// successors can be prepared in the background as the shard fills.
    fn first_segment_bytes(&self, maximum: u64) -> u64 {
        maximum
    }

    /// Starts writing at segment `segment`; earlier segments are read-only.
    fn open_log(&mut self, segment: u64) -> io::Result<()>;

    /// Server mode with per-shard leases: another worker holds this shard
    /// now. The engine then opens without waiting for it; the shard thread
    /// waits in [`wait_lease`](Self::wait_lease) before it recovers the
    /// shard, and calls to the shard queue meanwhile.
    fn standby(&self) -> bool {
        false
    }

    /// Waits up to `timeout` for this shard's lease: `Ok(true)` once this
    /// worker holds it, `Ok(false)` if not yet, an error if it never will
    /// (the connection is gone).
    fn wait_lease(&mut self, _timeout: std::time::Duration) -> io::Result<bool> {
        Ok(true)
    }

    /// Server mode: carries messages between this shard and shards another
    /// process holds. `None` (the default): every shard is in this process.
    fn relay(&self) -> Option<Arc<dyn Relay>> {
        None
    }

    /// Server mode's invoked workers: the host asks what the shard has
    /// left to do. When this returns true at the end of a tick, the runtime
    /// answers with [`report_due`](Self::report_due); it also does once
    /// when it closes. Checked once per tick, so it must be cheap.
    fn due_wanted(&self) -> bool {
        false
    }

    /// What the shard has left to do (see [`due_wanted`](Self::due_wanted));
    /// `closed` for the report a clean close makes last.
    fn report_due(&mut self, _due: crate::runtime::Due, _closed: bool) {}
}

/// Carries messages (child starts and results, signals and cancels between
/// workflows) between shards held by different processes, and their
/// acknowledgments. A Fusee server relays them in server mode.
///
/// Delivery is at least once: the sender's outbox is durable and resends
/// every unacknowledged entry, the receiver absorbs duplicates, and an
/// acknowledgment names the epoch of the sender's lease, so one that
/// outlives that lease (whose outbox IDs a new holder renumbers) is
/// dropped instead of acknowledging another entry.
pub trait Relay: Send + Sync {
    /// Whether this process holds `shard` now; messages to it go to its
    /// thread directly.
    fn holds(&self, shard: u32) -> bool;
    /// Messages from shard `from` to shard `target`, held elsewhere: the
    /// entries encoded by [`encode_outbox`](crate::runtime::encode_outbox).
    /// Lost if the connection is; the outbox resends them.
    fn send(&self, from: u32, target: u32, entries: &[u8]);
    /// Shard `from` applied relayed messages durably: `(sender shard,
    /// sender's lease epoch, outbox ID)` each.
    fn ack(&self, from: u32, acks: &[(u32, u64, u64)]);
    /// Relayed messages and acknowledgments for `shard` go to `inbound`
    /// from now on.
    fn attach(&self, shard: u32, inbound: Box<dyn RelayInbound>);
}

/// Where a [`Relay`] delivers what arrives for one shard.
pub trait RelayInbound: Send + Sync {
    /// Messages from shard `from` under its lease `epoch`, encoded by
    /// [`encode_outbox`](crate::runtime::encode_outbox).
    fn messages(&self, from: u32, epoch: u64, entries: &[u8]);
    /// The receiver applied this shard's outbox entry `id`.
    fn acked(&self, id: u64);
}

/// Lets one runtime type hold either real or memory I/O.
impl Env for Box<dyn Env> {
    fn now_ms(&self) -> u64 {
        (**self).now_ms()
    }
    fn monotonic_us(&self) -> u64 {
        (**self).monotonic_us()
    }
    fn random(&mut self) -> u64 {
        (**self).random()
    }
    fn set_notify(&mut self, notify: Notify) {
        (**self).set_notify(notify)
    }
    fn submit(&mut self, batch: Batch) {
        (**self).submit(batch)
    }
    fn max_in_flight(&self) -> usize {
        (**self).max_in_flight()
    }
    fn durable_on_append(&self) -> bool {
        (**self).durable_on_append()
    }
    fn supports_inline(&self) -> bool {
        (**self).supports_inline()
    }
    fn flush_inline(&mut self, batch: Batch) -> io::Result<Lsn> {
        (**self).flush_inline(batch)
    }
    fn last_flush_us(&self) -> u64 {
        (**self).last_flush_us()
    }
    fn poll_flushed(&mut self) -> Option<io::Result<Lsn>> {
        (**self).poll_flushed()
    }
    fn store_append(&mut self, frame: &[u8]) -> io::Result<u64> {
        (**self).store_append(frame)
    }
    fn store_append_history(&mut self, frame: &[u8], run: RunId, lsn: Lsn) -> io::Result<u64> {
        (**self).store_append_history(frame, run, lsn)
    }
    fn store_read(&mut self, offset: u64, len: u32) -> io::Result<Vec<u8>> {
        (**self).store_read(offset, len)
    }
    fn store_len(&self) -> u64 {
        (**self).store_len()
    }
    fn store_generation(&self) -> u64 {
        (**self).store_generation()
    }
    fn compact_store(&mut self, live: &[(RunId, BlobRef)]) -> io::Result<bool> {
        (**self).compact_store(live)
    }
    fn recovers(&self) -> bool {
        (**self).recovers()
    }
    fn visibility_candidates(
        &mut self,
        filter: &crate::visibility::ListFilter,
        limit: usize,
    ) -> Option<crate::visibility::Candidates> {
        (**self).visibility_candidates(filter, limit)
    }
    fn checkpoint(&mut self, checkpoint: Checkpoint, delete_through_segment: Option<u64>) {
        (**self).checkpoint(checkpoint, delete_through_segment)
    }
    fn poll_checkpoint(&mut self) -> Option<io::Result<Lsn>> {
        (**self).poll_checkpoint()
    }
    fn load_checkpoint(&mut self) -> io::Result<Option<Checkpoint>> {
        (**self).load_checkpoint()
    }
    fn load_store(&mut self, generation: u64, len: u64) -> io::Result<Vec<u8>> {
        (**self).load_store(generation, len)
    }
    fn recover_store(
        &mut self,
        generation: u64,
        len: u64,
        visit: &mut dyn FnMut(u64, u32, fusee_core::store::StoreEntry),
    ) -> io::Result<()> {
        (**self).recover_store(generation, len, visit)
    }
    fn segments(&mut self) -> io::Result<Vec<u64>> {
        (**self).segments()
    }
    fn read_segment(&mut self, segment: u64) -> io::Result<Vec<u8>> {
        (**self).read_segment(segment)
    }
    fn sync_recovered(&mut self) -> io::Result<()> {
        (**self).sync_recovered()
    }
    fn discard_segment(&mut self, segment: u64) -> io::Result<()> {
        (**self).discard_segment(segment)
    }
    fn read_segment_from(
        &mut self,
        segment: u64,
        offset: u64,
        len: usize,
    ) -> io::Result<(Vec<u8>, bool)> {
        (**self).read_segment_from(segment, offset, len)
    }
    fn read_segment_header(&mut self, segment: u64) -> io::Result<Vec<u8>> {
        (**self).read_segment_header(segment)
    }
    fn first_segment_bytes(&self, maximum: u64) -> u64 {
        (**self).first_segment_bytes(maximum)
    }
    fn preparing_segment(&self) -> bool {
        (**self).preparing_segment()
    }
    fn exclusive_log(&self) -> bool {
        (**self).exclusive_log()
    }
    fn log_position(&self) -> u64 {
        (**self).log_position()
    }
    fn shared_log(&self) -> bool {
        (**self).shared_log()
    }
    fn recover_log(
        &mut self,
        after: Lsn,
    ) -> io::Result<(Vec<(Lsn, fusee_core::record::RecordBuf)>, Lsn)> {
        (**self).recover_log(after)
    }
    fn log_segment(&self) -> u64 {
        (**self).log_segment()
    }
    fn log_stats(&self) -> LogStats {
        (**self).log_stats()
    }
    fn checkpoint_wanted(&self) -> bool {
        (**self).checkpoint_wanted()
    }
    fn checkpoint_on_roll(&self) -> bool {
        (**self).checkpoint_on_roll()
    }
    fn checkpoint_wanted_on_close(&self) -> bool {
        (**self).checkpoint_wanted_on_close()
    }
    fn open_log(&mut self, segment: u64) -> io::Result<()> {
        (**self).open_log(segment)
    }
    fn standby(&self) -> bool {
        (**self).standby()
    }
    fn wait_lease(&mut self, timeout: std::time::Duration) -> io::Result<bool> {
        (**self).wait_lease(timeout)
    }
    fn relay(&self) -> Option<Arc<dyn Relay>> {
        (**self).relay()
    }
    fn due_wanted(&self) -> bool {
        (**self).due_wanted()
    }
    fn report_due(&mut self, due: crate::runtime::Due, closed: bool) {
        (**self).report_due(due, closed)
    }
}

/// Read up to one MiB per storage request, including from servers whose
/// maximum wire frame is smaller than the entire store.
pub fn scan_store_pages(
    env: &mut dyn Env,
    length: u64,
    visit: &mut dyn FnMut(u64, u32, fusee_core::store::StoreEntry),
) -> io::Result<()> {
    use std::io::Read;
    struct Pages<'a> {
        env: &'a mut dyn Env,
        offset: u64,
        length: u64,
    }
    impl Read for Pages<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = (self.length - self.offset)
                .min(buf.len() as u64)
                .min(1 << 20) as u32;
            if n == 0 {
                return Ok(0);
            }
            let bytes = self.env.store_read(self.offset, n)?;
            if bytes.len() != n as usize {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "short run-store page",
                ));
            }
            buf[..bytes.len()].copy_from_slice(&bytes);
            self.offset += bytes.len() as u64;
            Ok(bytes.len())
        }
    }
    let mut reader = io::BufReader::with_capacity(
        1 << 20,
        Pages {
            env,
            offset: 0,
            length,
        },
    );
    scan_store_reader(&mut reader, 0, length, &mut |offset, len, entry| {
        visit(offset, len, entry);
        Ok(())
    })
}

/// Verify and visit a bounded run-store prefix from an already open reader.
pub fn scan_store_reader(
    reader: &mut dyn std::io::Read,
    mut offset: u64,
    length: u64,
    visit: &mut dyn FnMut(u64, u32, fusee_core::store::StoreEntry) -> io::Result<()>,
) -> io::Result<()> {
    use fusee_core::store::{STORE_HEADER, check_store_header, decode_entry_body};
    if offset == 0 {
        let mut header = [0; STORE_HEADER as usize];
        reader.read_exact(&mut header)?;
        check_store_header(&header).map_err(io::Error::other)?;
        offset = STORE_HEADER;
    }
    while offset < length {
        let mut prefix = [0u8; 8];
        reader.read_exact(&mut prefix)?;
        let len = u32::from_le_bytes(prefix[..4].try_into().unwrap());
        let frame_len = len
            .checked_add(8)
            .ok_or_else(|| io::Error::other("oversized run-store entry"))?;
        if u64::from(frame_len) > length - offset {
            return Err(io::Error::other(
                "run-store frame exceeds checkpoint prefix",
            ));
        }
        let mut body = Vec::new();
        body.try_reserve_exact(len as usize)
            .map_err(io::Error::other)?;
        body.resize(len as usize, 0);
        reader.read_exact(&mut body)?;
        if crc32c::crc32c(&body).to_le_bytes() != prefix[4..] {
            return Err(io::Error::other("run-store checksum mismatch"));
        }
        let entry = decode_entry_body(&body).map_err(io::Error::other)?;
        visit(offset, frame_len, entry)?;
        offset += u64::from(frame_len);
    }
    Ok(())
}

/// Validate the whole frame, including its identity, before copying it into
/// a generation whose checkpoint may eventually be the only durable copy.
pub fn check_live_frame(frame: &[u8], run: RunId) -> io::Result<()> {
    let len = frame
        .get(..4)
        .ok_or_else(|| io::Error::other("short live frame"))?;
    if u32::from_le_bytes(len.try_into().unwrap()) as usize != frame.len().saturating_sub(8) {
        return Err(io::Error::other("live frame length mismatch"));
    }
    let entry = fusee_core::store::read_entry(frame).map_err(io::Error::other)?;
    if entry.run() != run || matches!(entry, fusee_core::store::StoreEntry::Tombstone { .. }) {
        return Err(io::Error::other(
            "live frame points to another run or a tombstone",
        ));
    }
    Ok(())
}
