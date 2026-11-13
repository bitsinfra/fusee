//! The log owner: on Linux, one thread writes
//! every sealed batch of the shared log through an `io_uring`, keeping up
//! to the log's limit of writes in flight, and reaps their completions in
//! the same `io_uring_enter`. Where it runs it takes the place of the log
//! thread and the I/O threads that each block in one write: a batch does
//! not wait for a thread to wake and take it, and a completion is published
//! by the thread that submitted it.
//!
//! What it writes is what [`Wal::seal`] places; whatever seals
//! a batch (a shard's inline lead, a completion handing on, a kick) queues
//! it for the owner. Writes carry `RWF_DSYNC` on the segment's `O_DIRECT`
//! file, so a completion is durability. Batches that start a segment are
//! written by the owner without the ring (the header goes first), and a
//! write the kernel refuses with `EINVAL` is written again buffered, as
//! [`FileDevice::write`] does. A lone shard's inline write is unchanged.
//! The threads stay on tmpfs (see [`wanted`]).

use super::*;
use io_uring::{IoUring, opcode, types};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

/// The eventfd read's completion, apart from writes'.
const WAKE: u64 = u64::MAX;

/// Ring entries: writes in flight are a handful.
const RING: u32 = 64;

/// How long the owner polls for a write in flight at most, for a foreign
/// host's pipelined flush (see `run`).
const POLL_CAP_US: u64 = 60;

/// Completions closer together than this make the log busy for
/// [`poll_busy`].
const BUSY_GAP: std::time::Duration = std::time::Duration::from_micros(100);

/// `FUSEE_LOG_POLL=busy` (opt-in): the owner also polls
/// for its writes' completions, instead of sleeping, while writes are in
/// flight and completions come less than [`BUSY_GAP`] apart, which a busy
/// log does (at 1.64 M steps/s about every 21 µs) and a log at 100 K
/// steps/s, where shards mostly write inline, does not.
fn poll_busy() -> bool {
    static BUSY: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *BUSY.get_or_init(|| std::env::var("FUSEE_LOG_POLL").is_ok_and(|v| v == "busy"))
}

/// A write the ring carries.
struct Write {
    w: InFlight,
    /// Kept for a buffered rewrite: the segment's bytes and where.
    offset: u64,
    bytes: Vec<u8>,
    buf: AlignedBuf,
    /// Held until the completion: the descriptor is resolved only when
    /// the ring is entered, and must not be closed (or reused) before.
    _file: Arc<File>,
    /// Its partial last block, noted once it completed.
    tail: DirectTail,
    started: Instant,
}

/// Whether this log should have an owner thread: its ring and wake
/// eventfd, made here so that a kernel without `io_uring` (or one that
/// forbids it, or a locked-memory limit too low for the ring) keeps the
/// log's threads instead of failing the log. `FUSEE_LOG_OWNER=0` keeps the
/// threads; `1` takes the owner on tmpfs too.
pub(super) fn wanted(dev: &FileDevice, local: &LocalOptions) -> Option<(IoUring, OwnedFd)> {
    let asked = std::env::var("FUSEE_LOG_OWNER").ok();
    if asked.as_deref() == Some("0")
        || local.flush != FlushMethod::Dsync
        || !local.emulated_flush.is_zero()
        || !dev.direct_io()
    {
        return None;
    }
    // On tmpfs the kernel runs each write on an `io-wq` worker and, as
    // tmpfs does not take parallel direct writes, one at a time per file:
    // at a million steps a second in a container the owner made half the
    // writes of the threads, which keep two in flight there.
    if asked.as_deref() != Some("1") && on_tmpfs(&dev.dir) {
        return None;
    }
    let ring = IoUring::new(RING).ok()?;
    // SAFETY: plain system call; a negative result is handled.
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    // SAFETY: a new descriptor this function owns.
    (fd >= 0).then(|| (ring, unsafe { OwnedFd::from_raw_fd(fd) }))
}

fn on_tmpfs(dir: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let Ok(path) = std::ffi::CString::new(dir.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: `s` is a writable statfs and `path` a NUL-terminated string.
    let mut s: libc::statfs = unsafe { std::mem::zeroed() };
    unsafe {
        libc::statfs(path.as_ptr(), &mut s) == 0
            && i128::from(s.f_type) == i128::from(libc::TMPFS_MAGIC)
    }
}

/// Wakes the owner if it waits (one system call only then).
pub(super) fn wake(log: &SharedWal) {
    if log.owner_waiting.load(Ordering::SeqCst) {
        let one: u64 = 1;
        // SAFETY: writes 8 bytes from a live u64 to the eventfd the log owns.
        unsafe { libc::write(log.owner_fd, (&raw const one).cast(), 8) };
    }
}

/// The owner's loop. `efd` is held for the thread's life: a read armed on
/// it is resolved when the ring is entered, after the log may have gone.
pub(super) fn run(log: Weak<SharedWal>, mut ring: IoUring, efd: Arc<OwnedFd>) {
    let efd_raw = efd.as_raw_fd();
    let mut wake_buf = Box::new([0u8; 8]);
    let mut wake_armed = false;
    let mut writes: Vec<Option<Write>> = Vec::new();
    let mut in_ring = 0usize;
    // When a write's completion was last reaped.
    let mut reaped_at: Option<std::time::Instant> = None;
    let poll_busy = poll_busy();
    loop {
        let Some(shared) = log.upgrade() else { break };
        if shared.stop.load(Ordering::Acquire) {
            break;
        }
        // Seal what shards staged since the last pass.
        if shared.kicked.swap(false, Ordering::AcqRel) {
            let mut wal = shared.wal.lock().unwrap_or_else(|e| e.into_inner());
            shared.start_writes(&mut wal);
        }
        // Submit what is sealed.
        loop {
            let job = shared
                .jobs
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .queue
                .pop_front();
            let Some((mut w, started)) = job else { break };
            shared.queued.fetch_sub(1, Ordering::AcqRel);
            match prepare(&shared, &mut w) {
                Ok(Some((offset, bytes, file, buf, tail))) => {
                    let slot = writes.iter().position(Option::is_none).unwrap_or_else(|| {
                        writes.push(None);
                        writes.len() - 1
                    });
                    let sqe = opcode::Write::new(
                        types::Fd(file.as_raw_fd()),
                        buf.as_ref().as_ptr(),
                        buf.as_ref().len() as u32,
                    )
                    .offset(offset / DIRECT_ALIGN as u64 * DIRECT_ALIGN as u64)
                    .rw_flags(libc::RWF_DSYNC)
                    .build()
                    .user_data(slot as u64);
                    // SAFETY: the buffer lives in `writes[slot]` until the
                    // write's completion is reaped; the file stays open
                    // through the device's cache and the `Arc` dropped
                    // only after.
                    if unsafe { ring.submission().push(&sqe) }.is_err() {
                        shared.fail(io::Error::other("log owner ring full"));
                        break;
                    }
                    writes[slot] = Some(Write {
                        w,
                        offset,
                        bytes,
                        buf,
                        _file: file,
                        tail,
                        started,
                    });
                    in_ring += 1;
                }
                // Written already (a new segment's first batch, or no
                // direct file): publish as an I/O thread would.
                Ok(None) => finish(&shared, w, started, Ok(())),
                Err(e) => finish(&shared, w, started, Err(e)),
            }
        }
        if !wake_armed {
            let sqe = opcode::Read::new(types::Fd(efd_raw), wake_buf.as_mut_ptr(), 8)
                .build()
                .user_data(WAKE);
            // SAFETY: the buffer is boxed and lives as long as the ring.
            if unsafe { ring.submission().push(&sqe) }.is_ok() {
                wake_armed = true;
            }
        }
        // A foreign host pipelining its flushes (`direct`) submits its next
        // batch soon after each completion, then waits for that write:
        // with a core to spare, poll for both instead of sleeping, as the
        // log's I/O threads spin in that mode (`DIRECT_SPIN_US`): the two
        // wake-ups, the eventfd's and the completion's, can be a large part
        // of an SDK step's flush on a fast device.
        let busy = poll_busy && in_ring > 0 && reaped_at.is_some_and(|t| t.elapsed() < BUSY_GAP);
        let spun = if shared.spin_cap_us > 0
            && (busy || (shared.direct.load(Ordering::Relaxed) && crate::spin::spare_core()))
        {
            match ring.submit() {
                Err(e) if e.raw_os_error() != Some(libc::EINTR) => {
                    shared.fail(e);
                    break;
                }
                _ => {}
            }
            let budget = if in_ring > 0 {
                let last = shared.last_flush_us.load(Ordering::Relaxed);
                (last * 2 + 10).min(POLL_CAP_US)
            } else {
                DIRECT_SPIN_US
            };
            crate::spin::spin_until(std::time::Duration::from_micros(budget), || {
                !ring.completion().is_empty()
                    || shared.kicked.load(Ordering::Acquire)
                    || shared.queued.load(Ordering::Acquire) > 0
                    || shared.stop.load(Ordering::Acquire)
            })
        } else {
            false
        };
        // Wait for a completion: a write's, or the eventfd's (a kick or a
        // queued batch while nothing is in flight).
        let waited = if spun {
            drop(shared);
            Ok(0)
        } else {
            shared.owner_waiting.store(true, Ordering::SeqCst);
            let idle = in_ring == 0
                && !shared.kicked.load(Ordering::SeqCst)
                && shared.queued.load(Ordering::SeqCst) == 0
                && !shared.stop.load(Ordering::SeqCst);
            // Waiting holds no strong reference: the log may drop meanwhile.
            drop(shared);
            if idle || in_ring > 0 {
                ring.submit_and_wait(1)
            } else {
                ring.submit()
            }
        };
        if let Some(s) = log.upgrade() {
            s.owner_waiting.store(false, Ordering::SeqCst);
        }
        if let Err(e) = waited
            && e.raw_os_error() != Some(libc::EINTR)
        {
            if let Some(s) = log.upgrade() {
                s.fail(e);
            }
            break;
        }
        let done: Vec<(u64, i32)> = ring
            .completion()
            .map(|c| (c.user_data(), c.result()))
            .collect();
        let Some(shared) = log.upgrade() else { break };
        for (id, res) in done {
            if id == WAKE {
                wake_armed = false;
                shared.kicked.store(true, Ordering::Release);
                continue;
            }
            let Some(write) = writes.get_mut(id as usize).and_then(Option::take) else {
                continue;
            };
            in_ring -= 1;
            if poll_busy {
                reaped_at = Some(std::time::Instant::now());
            }
            let r = if res == write.buf.as_ref().len() as i32 {
                shared.dev.direct_wrote(write.tail);
                Ok(())
            } else if res == -libc::EINVAL {
                // The kernel refused the direct write itself: write the
                // bytes again buffered, as the device does from now on.
                shared
                    .dev
                    .write(write.w.segment, write.offset, &write.bytes)
            } else if res < 0 {
                Err(io::Error::from_raw_os_error(-res))
            } else {
                Err(io::Error::new(io::ErrorKind::WriteZero, "short log write"))
            };
            finish(&shared, write.w, write.started, r);
        }
    }
    // Writes still in the ring finish before their buffers go, and so does
    // the eventfd read (completed here).
    if wake_armed {
        let one: u64 = 1;
        // SAFETY: writes 8 bytes from a live u64 to the eventfd this
        // thread holds.
        unsafe { libc::write(efd_raw, (&raw const one).cast(), 8) };
    }
    while in_ring > 0 || wake_armed {
        match ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(_) => break,
        }
        for c in ring.completion() {
            if c.user_data() == WAKE {
                wake_armed = false;
            } else {
                in_ring = in_ring.saturating_sub(1);
            }
        }
    }
    if in_ring > 0 || wake_armed {
        // The ring failed with requests in it: the kernel may still use
        // their buffers, so they are never freed.
        std::mem::forget(writes);
        std::mem::forget(wake_buf);
    }
    drop(ring);
    drop(efd);
}

/// The direct write of a sealed batch: `(offset, bytes, file, aligned
/// blocks)`, or `None` when it was written here without the ring (it starts
/// a segment, or the device writes buffered).
#[allow(clippy::type_complexity)]
fn prepare(
    shared: &SharedWal,
    w: &mut InFlight,
) -> io::Result<Option<(u64, Vec<u8>, Arc<File>, AlignedBuf, DirectTail)>> {
    let Some((header, offset, bytes)) = w.io.take() else {
        return Ok(None);
    };
    if header.is_some() {
        w.io = Some((header, offset, bytes));
        Wal::perform(&shared.dev, w)?;
        return Ok(None);
    }
    match shared.dev.direct_buffer(w.segment, offset, &bytes)? {
        Some((file, buf, tail)) => Ok(Some((offset, bytes, file, buf, tail))),
        None => {
            shared.dev.write(w.segment, offset, &bytes)?;
            Ok(None)
        }
    }
}

/// A write completed (or failed): publish it and start what follows, as an
/// I/O thread did.
fn finish(shared: &SharedWal, w: InFlight, started: Instant, r: io::Result<()>) {
    shared
        .last_flush_us
        .store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
    let mut wakes = Vec::new();
    {
        let mut wal = shared.wal.lock().unwrap_or_else(|e| e.into_inner());
        match r {
            Ok(()) => {
                let shards = wal.complete(w);
                shared.publish(&wal, &shards, None, &mut wakes);
                shared.next_write(&mut wal, None, &mut wakes);
            }
            Err(e) => shared.fail(e),
        }
    }
    for w in wakes {
        w.run();
    }
}
