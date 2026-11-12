//! Same-host shared memory between a worker and the server
//! (`unix:/path.sock?shm=1`).
//!
//! The worker offers [`proto::SHARED_MEMORY`](crate::proto::SHARED_MEMORY)
//! over a Unix socket; the server creates the mapping (a memfd on Linux, an
//! unlinked POSIX shared-memory object on macOS) and passes it, with the
//! server's doorbell page, alongside the lease frame (`SCM_RIGHTS`). It
//! learns the worker's process from the socket's peer credentials.
//!
//! ```text
//! worker mapping
//!   header      magic, version, shards, ring size, worker pid, server pid;
//!               heartbeat (worker), closed (server), log position and
//!               active segment (server), each on its own cache line
//!   watermarks  one cache line per shard: durable LSN, lease epoch, a
//!               futex word bumped after every store, parked flag, pins,
//!               notify (worker: also send `DURABLE` on the socket), failed
//!   ring        head (server), tail (worker), then the bytes: frames of
//!               `len u32, crc32c u32, payload`, 8-byte aligned
//! doorbell page (one per server, shared by its workers)
//!   seq         futex word the server's log thread sleeps on
//!   sleeping    set while it sleeps; a producer wakes it only then
//! ```
//!
//! The ring carries only log sections; it is volatile, and only the
//! watermarks acknowledge. A frame is published by a release store of the
//! tail after its bytes, so the server never sees a partial frame; one that
//! fails its length or checksum checks means the worker broke the protocol,
//! and the server revokes its lease.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};
use std::time::Duration;

pub(crate) const MAGIC: u64 = u64::from_le_bytes(*b"FUSEESHM");
pub(crate) const VERSION: u32 = 1;
/// A cache line on every host this runs on (128 bytes on Apple silicon).
pub(crate) const LINE: usize = 128;
const PAGE: usize = 4096;
/// Ring bytes per worker. A section larger than a quarter of it goes over
/// the socket.
pub(crate) const RING_BYTES: usize = 4 << 20;
const FRAME_HEADER: usize = 8;

fn last_error(what: &str) -> io::Error {
    let e = io::Error::last_os_error();
    io::Error::new(e.kind(), format!("{what}: {e}"))
}

/// A shared mapping, unmapped when dropped.
pub(crate) struct Mapping {
    ptr: *mut u8,
    len: usize,
    fd: OwnedFd,
}

// The mapping is plain shared memory, accessed through atomics or under the
// ring's head and tail protocol.
unsafe impl Send for Mapping {}
unsafe impl Sync for Mapping {}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: `ptr` and `len` are the mapping made in `map`.
        unsafe {
            libc::munmap(self.ptr.cast(), self.len);
        }
    }
}

impl Mapping {
    /// New zeroed shared memory of `len` bytes, not reachable by name.
    pub(crate) fn create(len: usize) -> io::Result<Self> {
        let fd = anonymous_fd()?;
        // SAFETY: a valid descriptor we own.
        if unsafe { libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) } != 0 {
            return Err(last_error("sizing shared memory"));
        }
        Self::map(fd, len)
    }

    /// Maps a received descriptor, which must be at least `len` bytes.
    pub(crate) fn open(fd: OwnedFd, len: usize) -> io::Result<Self> {
        // SAFETY: fstat into a zeroed struct.
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd.as_raw_fd(), &mut st) } != 0 {
            return Err(last_error("inspecting shared memory"));
        }
        if (st.st_size as u64) < len as u64 {
            return Err(io::Error::other(
                "the shared memory is smaller than its layout",
            ));
        }
        Self::map(fd, len)
    }

    fn map(fd: OwnedFd, len: usize) -> io::Result<Self> {
        // SAFETY: mapping a descriptor of at least `len` bytes.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(last_error("mapping shared memory"));
        }
        Ok(Self {
            ptr: ptr.cast(),
            len,
            fd,
        })
    }

    pub(crate) fn fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    fn u64_at(&self, off: usize) -> &AtomicU64 {
        assert!(off.is_multiple_of(8) && off + 8 <= self.len);
        // SAFETY: in bounds, aligned, and only ever accessed atomically.
        unsafe { &*self.ptr.add(off).cast::<AtomicU64>() }
    }

    fn u32_at(&self, off: usize) -> &AtomicU32 {
        assert!(off.is_multiple_of(4) && off + 4 <= self.len);
        // SAFETY: in bounds, aligned, and only ever accessed atomically.
        unsafe { &*self.ptr.add(off).cast::<AtomicU32>() }
    }

    /// Copies `data` to `off`, which the caller owns under the ring protocol.
    fn write(&self, off: usize, data: &[u8]) {
        assert!(off + data.len() <= self.len);
        // SAFETY: in bounds; no other party reads these bytes until a later
        // release store publishes them.
        unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.add(off), data.len()) }
    }

    fn read(&self, off: usize, out: &mut [u8]) {
        assert!(off + out.len() <= self.len);
        // SAFETY: in bounds; published by an acquire load of the tail and
        // not reused before the head moves past them.
        unsafe { std::ptr::copy_nonoverlapping(self.ptr.add(off), out.as_mut_ptr(), out.len()) }
    }
}

#[cfg(target_os = "linux")]
fn anonymous_fd() -> io::Result<OwnedFd> {
    // SAFETY: a static name; the flags are valid.
    let fd = unsafe { libc::memfd_create(c"fusee-shm".as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(last_error("memfd_create"));
    }
    // SAFETY: a fresh descriptor we own.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(not(target_os = "linux"))]
fn anonymous_fd() -> io::Result<OwnedFd> {
    use std::sync::atomic::AtomicU64;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    // macOS names are at most 31 bytes; the object is unlinked at once, so
    // only the descriptor (and whoever receives it) reaches it.
    for _ in 0..16 {
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let name = format!("/fsm{}.{n}", std::process::id());
        let c = std::ffi::CString::new(name).map_err(io::Error::other)?;
        // SAFETY: a NUL-terminated name; standard flags.
        let fd = unsafe {
            libc::shm_open(
                c.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                0o600 as libc::c_uint,
            )
        };
        if fd < 0 {
            if io::Error::last_os_error().raw_os_error() == Some(libc::EEXIST) {
                continue;
            }
            return Err(last_error("shm_open"));
        }
        // SAFETY: the name we just created.
        unsafe {
            libc::shm_unlink(c.as_ptr());
        }
        // SAFETY: a fresh descriptor we own.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        // SAFETY: valid descriptor and flag.
        unsafe {
            libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
        }
        return Ok(fd);
    }
    Err(io::Error::other("could not name a shared-memory object"))
}

/// Waits while `word` holds `expected`, up to `timeout`, unless woken.
/// Works across processes on a shared mapping. Returns early on spurious
/// wake-ups; callers re-check their condition.
pub(crate) fn wait(word: &AtomicU32, expected: u32, timeout: Duration) {
    sys_wait(word, expected, timeout)
}

/// Wakes every thread of any process waiting on `word`.
pub(crate) fn wake(word: &AtomicU32) {
    sys_wake(word)
}

#[cfg(target_os = "linux")]
fn sys_wait(word: &AtomicU32, expected: u32, timeout: Duration) {
    let ts = libc::timespec {
        tv_sec: timeout.as_secs() as libc::time_t,
        tv_nsec: timeout.subsec_nanos() as libc::c_long,
    };
    // SAFETY: a shared (not private) futex on a valid aligned word.
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            word.as_ptr(),
            libc::FUTEX_WAIT,
            expected,
            &ts as *const libc::timespec,
            std::ptr::null::<u32>(),
            0,
        );
    }
}

#[cfg(target_os = "linux")]
fn sys_wake(word: &AtomicU32) {
    // SAFETY: a shared futex on a valid aligned word.
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            word.as_ptr(),
            libc::FUTEX_WAKE,
            i32::MAX,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0,
        );
    }
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn __ulock_wait(operation: u32, addr: *mut libc::c_void, value: u64, timeout_us: u32) -> i32;
    fn __ulock_wake(operation: u32, addr: *mut libc::c_void, wake_value: u64) -> i32;
}

/// `UL_COMPARE_AND_WAIT_SHARED`: a 32-bit word in memory shared between
/// processes. Stable since macOS 10.12 and what libc++ and Chromium use.
#[cfg(target_os = "macos")]
const UL_SHARED: u32 = 3;
#[cfg(target_os = "macos")]
const ULF_NO_ERRNO: u32 = 0x0100_0000;
#[cfg(target_os = "macos")]
const ULF_WAKE_ALL: u32 = 0x0000_0100;

#[cfg(target_os = "macos")]
fn sys_wait(word: &AtomicU32, expected: u32, timeout: Duration) {
    let us = timeout.as_micros().clamp(1, u128::from(u32::MAX)) as u32;
    // SAFETY: a valid aligned word in shared memory.
    unsafe {
        __ulock_wait(
            UL_SHARED | ULF_NO_ERRNO,
            word.as_ptr().cast(),
            u64::from(expected),
            us,
        );
    }
}

#[cfg(target_os = "macos")]
fn sys_wake(word: &AtomicU32) {
    // SAFETY: a valid aligned word in shared memory.
    unsafe {
        __ulock_wake(
            UL_SHARED | ULF_NO_ERRNO | ULF_WAKE_ALL,
            word.as_ptr().cast(),
            0,
        );
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn sys_wait(_word: &AtomicU32, _expected: u32, timeout: Duration) {
    std::thread::sleep(timeout.min(Duration::from_micros(50)));
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn sys_wake(_word: &AtomicU32) {}

/// Whether this host can run the shared-memory transport.
pub(crate) const SUPPORTED: bool = cfg!(any(target_os = "linux", target_os = "macos"));

/// The process at the other end of a Unix socket.
pub(crate) fn peer_pid(fd: RawFd) -> io::Result<u32> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: getsockopt into a zeroed ucred of the right size.
        let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        let r = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut cred as *mut libc::ucred).cast(),
                &mut len,
            )
        };
        if r != 0 {
            return Err(last_error("reading the worker's credentials"));
        }
        Ok(cred.pid as u32)
    }
    #[cfg(target_os = "macos")]
    {
        let mut pid: libc::pid_t = 0;
        let mut len = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
        // SAFETY: getsockopt into a pid_t of the right size.
        let r = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_LOCAL,
                libc::LOCAL_PEERPID,
                (&mut pid as *mut libc::pid_t).cast(),
                &mut len,
            )
        };
        if r != 0 {
            return Err(last_error("reading the worker's process"));
        }
        Ok(pid as u32)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = fd;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "peer credentials",
        ))
    }
}

/// Sends `data` with `fds` attached on a Unix socket; may write only part
/// of `data` (the descriptors go with the first byte).
pub(crate) fn send_with_fds(sock: RawFd, data: &[u8], fds: &[RawFd]) -> io::Result<usize> {
    let bytes = std::mem::size_of_val(fds) as u32;
    // SAFETY: CMSG_SPACE is a pure size computation.
    let space = unsafe { libc::CMSG_SPACE(bytes) } as usize;
    let mut control = vec![0u8; space];
    let mut iov = libc::iovec {
        iov_base: data.as_ptr() as *mut libc::c_void,
        iov_len: data.len(),
    };
    // SAFETY: a zeroed msghdr filled with valid buffers.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = space as _;
    // SAFETY: the control buffer holds one header of `bytes` descriptors.
    unsafe {
        let c = libc::CMSG_FIRSTHDR(&msg);
        (*c).cmsg_level = libc::SOL_SOCKET;
        (*c).cmsg_type = libc::SCM_RIGHTS;
        (*c).cmsg_len = libc::CMSG_LEN(bytes) as _;
        std::ptr::copy_nonoverlapping(
            fds.as_ptr().cast::<u8>(),
            libc::CMSG_DATA(c),
            bytes as usize,
        );
    }
    // SAFETY: a valid socket and message.
    let n = unsafe { libc::sendmsg(sock, &msg, 0) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(n as usize)
}

/// Receives into `buf` on a blocking Unix socket, collecting descriptors
/// passed alongside (closed on exec).
pub(crate) fn recv_with_fds(
    sock: RawFd,
    buf: &mut [u8],
    fds: &mut Vec<OwnedFd>,
) -> io::Result<usize> {
    let mut control = [0u8; 256];
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };
    // SAFETY: a zeroed msghdr filled with valid buffers.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = control.len() as _;
    #[cfg(target_os = "linux")]
    let flags = libc::MSG_CMSG_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let flags = 0;
    let n = loop {
        // SAFETY: a valid socket and message.
        let n = unsafe { libc::recvmsg(sock, &mut msg, flags) };
        if n >= 0 {
            break n as usize;
        }
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::Interrupted {
            return Err(e);
        }
    };
    // SAFETY: walking the control headers the kernel filled in.
    unsafe {
        let mut c = libc::CMSG_FIRSTHDR(&msg);
        while !c.is_null() {
            if (*c).cmsg_level == libc::SOL_SOCKET && (*c).cmsg_type == libc::SCM_RIGHTS {
                let data = libc::CMSG_DATA(c);
                let len = (*c).cmsg_len as usize - (data as usize - c as usize);
                for i in 0..len / std::mem::size_of::<RawFd>() {
                    let fd = std::ptr::read_unaligned(data.cast::<RawFd>().add(i));
                    let fd = OwnedFd::from_raw_fd(fd);
                    #[cfg(not(target_os = "linux"))]
                    libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
                    fds.push(fd);
                }
            }
            c = libc::CMSG_NXTHDR(&msg, c);
        }
    }
    Ok(n)
}

/// Where each part of a worker's mapping lies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Layout {
    pub shards: u32,
    pub ring: usize,
}

impl Layout {
    pub(crate) fn new(shards: u32, ring: usize) -> io::Result<Self> {
        if shards == 0 || shards > 1 << 16 || !ring.is_power_of_two() || ring < 4096 {
            return Err(io::Error::other("invalid shared-memory layout"));
        }
        Ok(Self { shards, ring })
    }

    fn line(&self, shard: u32) -> usize {
        PAGE + shard as usize * LINE
    }

    fn ring_offset(&self) -> usize {
        PAGE + (self.shards as usize * LINE).div_ceil(PAGE) * PAGE
    }

    fn data(&self) -> usize {
        self.ring_offset() + 2 * LINE
    }

    pub(crate) fn len(&self) -> usize {
        self.data() + self.ring
    }
}

// Header offsets.
const H_MAGIC: usize = 0;
const H_VERSION: usize = 8;
const H_SHARDS: usize = 12;
const H_RING: usize = 16;
const H_WORKER_PID: usize = 24;
const H_SERVER_PID: usize = 28;
const H_HEARTBEAT: usize = LINE;
const H_CLOSED: usize = 2 * LINE;
const H_POSITION: usize = 3 * LINE;
const H_SEGMENT: usize = 3 * LINE + 8;

// Watermark line offsets.
const W_DURABLE: usize = 0;
const W_EPOCH: usize = 8;
const W_SEQ: usize = 16;
const W_PARKED: usize = 20;
const W_PINS: usize = 24;
const W_NOTIFY: usize = 28;
const W_FAILED: usize = 32;

/// A worker's mapping.
pub(crate) struct Shared {
    map: Mapping,
    layout: Layout,
}

/// Why a ring cannot be read further: the worker broke the protocol.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Corrupt(pub &'static str);

impl Shared {
    /// Creates a mapping for a worker (server side).
    pub(crate) fn create(layout: Layout, worker_pid: u32) -> io::Result<Self> {
        let s = Self {
            map: Mapping::create(layout.len())?,
            layout,
        };
        let m = &s.map;
        m.u64_at(H_MAGIC).store(MAGIC, Ordering::Relaxed);
        m.u32_at(H_VERSION).store(VERSION, Ordering::Relaxed);
        m.u32_at(H_SHARDS).store(layout.shards, Ordering::Relaxed);
        m.u64_at(H_RING)
            .store(layout.ring as u64, Ordering::Relaxed);
        m.u32_at(H_WORKER_PID).store(worker_pid, Ordering::Relaxed);
        m.u32_at(H_SERVER_PID)
            .store(std::process::id(), Ordering::Release);
        Ok(s)
    }

    /// Maps what the server sent (worker side) and checks its header.
    pub(crate) fn attach(fd: OwnedFd, layout: Layout) -> io::Result<Self> {
        let s = Self {
            map: Mapping::open(fd, layout.len())?,
            layout,
        };
        let m = &s.map;
        if m.u64_at(H_MAGIC).load(Ordering::Acquire) != MAGIC
            || m.u32_at(H_VERSION).load(Ordering::Acquire) != VERSION
            || m.u32_at(H_SHARDS).load(Ordering::Acquire) != layout.shards
            || m.u64_at(H_RING).load(Ordering::Acquire) != layout.ring as u64
        {
            return Err(io::Error::other(
                "the server's shared memory does not match its layout",
            ));
        }
        Ok(s)
    }

    pub(crate) fn fd(&self) -> RawFd {
        self.map.fd()
    }

    pub(crate) fn layout(&self) -> Layout {
        self.layout
    }

    #[cfg(test)]
    pub(crate) fn worker_pid(&self) -> u32 {
        self.map.u32_at(H_WORKER_PID).load(Ordering::Relaxed)
    }

    pub(crate) fn heartbeat(&self) -> &AtomicU64 {
        self.map.u64_at(H_HEARTBEAT)
    }

    /// Set by the server when the worker's lease ended.
    pub(crate) fn closed(&self) -> &AtomicU32 {
        self.map.u32_at(H_CLOSED)
    }

    pub(crate) fn position(&self) -> &AtomicU64 {
        self.map.u64_at(H_POSITION)
    }

    pub(crate) fn segment(&self) -> &AtomicU64 {
        self.map.u64_at(H_SEGMENT)
    }

    pub(crate) fn line(&self, shard: u32) -> Option<Line<'_>> {
        (shard < self.layout.shards).then(|| Line {
            map: &self.map,
            base: self.layout.line(shard),
        })
    }

    fn head(&self) -> &AtomicU64 {
        self.map.u64_at(self.layout.ring_offset())
    }

    fn tail(&self) -> &AtomicU64 {
        self.map.u64_at(self.layout.ring_offset() + LINE)
    }

    /// The largest payload [`push`](Self::push) takes.
    pub(crate) fn max_payload(&self) -> usize {
        self.layout.ring / 4
    }

    fn copy_in(&self, pos: u64, data: &[u8]) {
        let cap = self.layout.ring;
        let off = (pos as usize) & (cap - 1);
        let first = data.len().min(cap - off);
        self.map.write(self.layout.data() + off, &data[..first]);
        if first < data.len() {
            self.map.write(self.layout.data(), &data[first..]);
        }
    }

    fn copy_out(&self, pos: u64, out: &mut [u8]) {
        let cap = self.layout.ring;
        let off = (pos as usize) & (cap - 1);
        let first = out.len().min(cap - off);
        self.map.read(self.layout.data() + off, &mut out[..first]);
        if first < out.len() {
            let rest = out.len() - first;
            self.map
                .read(self.layout.data(), &mut out[first..first + rest]);
        }
    }

    /// Producer (the worker, one writer at a time): appends one frame.
    /// `false` if it does not fit now; the caller uses the socket instead.
    pub(crate) fn push(&self, payload: &[u8]) -> bool {
        if payload.len() > self.max_payload() {
            return false;
        }
        let size = (FRAME_HEADER + payload.len()).next_multiple_of(8) as u64;
        let tail = self.tail().load(Ordering::Relaxed);
        let head = self.head().load(Ordering::Acquire);
        if tail - head + size > self.layout.ring as u64 {
            return false;
        }
        let mut header = [0u8; FRAME_HEADER];
        header[..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        header[4..].copy_from_slice(&crc32c::crc32c(payload).to_le_bytes());
        self.copy_in(tail, &header);
        self.copy_in(tail + FRAME_HEADER as u64, payload);
        // The frame's bytes before the tail that publishes them.
        self.tail().store(tail + size, Ordering::Release);
        true
    }

    /// Consumer (the server): the next frame at `head` into `out`, if one
    /// is published; `head` moves past it. Call [`release`](Self::release)
    /// to hand the space back.
    pub(crate) fn pop(&self, head: &mut u64, out: &mut Vec<u8>) -> Result<bool, Corrupt> {
        let tail = self.tail().load(Ordering::Acquire);
        if tail == *head {
            return Ok(false);
        }
        let cap = self.layout.ring as u64;
        if tail < *head || tail - *head > cap || !(tail - *head).is_multiple_of(8) {
            return Err(Corrupt("ring tail out of range"));
        }
        if tail - *head < FRAME_HEADER as u64 {
            return Err(Corrupt("ring tail inside a frame header"));
        }
        let mut header = [0u8; FRAME_HEADER];
        self.copy_out(*head, &mut header);
        let len = u32::from_le_bytes(header[..4].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(header[4..].try_into().expect("4 bytes"));
        let size = (FRAME_HEADER + len).next_multiple_of(8) as u64;
        if len > self.max_payload() {
            return Err(Corrupt("ring frame too large"));
        }
        if size > tail - *head {
            return Err(Corrupt("ring tail inside a frame"));
        }
        out.clear();
        out.resize(len, 0);
        self.copy_out(*head + FRAME_HEADER as u64, out);
        if crc32c::crc32c(out) != crc {
            return Err(Corrupt("ring frame checksum mismatch"));
        }
        *head += size;
        Ok(true)
    }

    /// The consumer is done with everything before `head`.
    pub(crate) fn release(&self, head: u64) {
        self.head().store(head, Ordering::Release);
    }

    /// The consumer's position, for a new consumer (the server keeps its
    /// own copy; the worker never moves it).
    pub(crate) fn consumed(&self) -> u64 {
        self.head().load(Ordering::Acquire)
    }

    /// Whether frames wait at `head`.
    pub(crate) fn pending(&self, head: u64) -> bool {
        self.tail().load(Ordering::Acquire) != head
    }

    /// Test hook: publishes `tail` as is.
    #[cfg(test)]
    pub(crate) fn force_tail(&self, tail: u64) {
        self.tail().store(tail, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn raw_write(&self, pos: u64, data: &[u8]) {
        self.copy_in(pos, data);
    }

    #[cfg(test)]
    pub(crate) fn tail_now(&self) -> u64 {
        self.tail().load(Ordering::Acquire)
    }
}

/// One shard's watermark line.
pub(crate) struct Line<'a> {
    map: &'a Mapping,
    base: usize,
}

impl Line<'_> {
    pub(crate) fn durable(&self) -> &AtomicU64 {
        self.map.u64_at(self.base + W_DURABLE)
    }
    /// The shard's lease epoch while this worker holds it (server writes).
    pub(crate) fn epoch(&self) -> &AtomicU64 {
        self.map.u64_at(self.base + W_EPOCH)
    }
    /// Bumped after every store the worker may wait for.
    pub(crate) fn seq(&self) -> &AtomicU32 {
        self.map.u32_at(self.base + W_SEQ)
    }
    pub(crate) fn parked(&self) -> &AtomicU32 {
        self.map.u32_at(self.base + W_PARKED)
    }
    pub(crate) fn pins(&self) -> &AtomicU32 {
        self.map.u32_at(self.base + W_PINS)
    }
    pub(crate) fn notify(&self) -> &AtomicU32 {
        self.map.u32_at(self.base + W_NOTIFY)
    }
    pub(crate) fn failed(&self) -> &AtomicU32 {
        self.map.u32_at(self.base + W_FAILED)
    }

    /// Server: the shard's position advanced (or it failed, or the lease
    /// ended); wakes the worker if it parked.
    pub(crate) fn publish(&self) {
        self.seq().fetch_add(1, Ordering::Release);
        fence(Ordering::SeqCst);
        if self.parked().load(Ordering::Relaxed) != 0 {
            wake(self.seq());
        }
    }

    /// Worker: waits until `done` holds, spinning for `spin`, then parking
    /// on the line's word, at most `limit` in all. Returns whether `done`.
    pub(crate) fn wait_until(
        &self,
        spin: Duration,
        limit: Duration,
        done: &dyn Fn() -> bool,
    ) -> bool {
        if crate::spin::spin_until(spin, done) {
            return true;
        }
        let deadline = std::time::Instant::now() + limit;
        loop {
            let seen = self.seq().load(Ordering::Acquire);
            self.parked().store(1, Ordering::Relaxed);
            fence(Ordering::SeqCst);
            if done() {
                self.parked().store(0, Ordering::Relaxed);
                return true;
            }
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                self.parked().store(0, Ordering::Relaxed);
                return done();
            }
            wait(self.seq(), seen, left.min(Duration::from_millis(100)));
            self.parked().store(0, Ordering::Relaxed);
            if done() {
                return true;
            }
        }
    }
}

/// The server's doorbell page: its log thread sleeps on `seq` while
/// `sleeping`; workers ring it after publishing frames.
pub(crate) struct Doorbell {
    map: Mapping,
}

impl Doorbell {
    pub(crate) fn create() -> io::Result<Self> {
        Ok(Self {
            map: Mapping::create(PAGE)?,
        })
    }

    pub(crate) fn attach(fd: OwnedFd) -> io::Result<Self> {
        Ok(Self {
            map: Mapping::open(fd, PAGE)?,
        })
    }

    pub(crate) fn fd(&self) -> RawFd {
        self.map.fd()
    }

    fn seq(&self) -> &AtomicU32 {
        self.map.u32_at(0)
    }

    fn sleeping(&self) -> &AtomicU32 {
        self.map.u32_at(LINE)
    }

    /// Producer: frames were published; wakes the log thread if it sleeps.
    pub(crate) fn ring(&self) {
        // Pairs with `sleep`: either it sees the frames, or this sees it
        // sleeping.
        fence(Ordering::SeqCst);
        if self.sleeping().load(Ordering::Relaxed) != 0 {
            self.seq().fetch_add(1, Ordering::Release);
            wake(self.seq());
        }
    }

    /// Wakes the log thread whether or not it sleeps (its list of rings
    /// changed).
    pub(crate) fn ring_always(&self) {
        self.seq().fetch_add(1, Ordering::Release);
        wake(self.seq());
    }

    /// Consumer: sleeps up to `timeout` unless `ready` (checked after the
    /// flag is visible) or a producer rings.
    pub(crate) fn sleep(&self, ready: &dyn Fn() -> bool, timeout: Duration) {
        let seen = self.seq().load(Ordering::Acquire);
        self.sleeping().store(1, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        if !ready() {
            wait(self.seq(), seen, timeout);
        }
        self.sleeping().store(0, Ordering::Relaxed);
    }
}

/// Watches a process: [`exited`](ProcWatch::exited) resolves when it ends.
/// A pidfd on Linux, a kqueue with `EVFILT_PROC` on macOS.
pub(crate) struct ProcWatch {
    fd: tokio::io::unix::AsyncFd<OwnedFd>,
}

impl ProcWatch {
    #[cfg(target_os = "linux")]
    pub(crate) fn new(pid: u32) -> io::Result<Self> {
        // SAFETY: pidfd_open with no flags.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) };
        if fd < 0 {
            return Err(last_error("pidfd_open"));
        }
        // SAFETY: a fresh descriptor we own.
        let fd = unsafe { OwnedFd::from_raw_fd(fd as RawFd) };
        Ok(Self {
            // SAFETY: the descriptor is owned by the `AsyncFd` and stays
            // open as long as it.
            fd: unsafe {
                tokio::io::unix::AsyncFd::register_with_interest(fd, tokio::io::Interest::READABLE)
            }?,
        })
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn new(pid: u32) -> io::Result<Self> {
        // SAFETY: kqueue takes no arguments.
        let kq = unsafe { libc::kqueue() };
        if kq < 0 {
            return Err(last_error("kqueue"));
        }
        // SAFETY: a fresh descriptor we own.
        let kq = unsafe { OwnedFd::from_raw_fd(kq) };
        // SAFETY: valid descriptor and flag.
        unsafe {
            libc::fcntl(kq.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
        }
        let change = libc::kevent {
            ident: pid as usize,
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_EXIT,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        // SAFETY: registers one change, receives none.
        let r = unsafe {
            libc::kevent(
                kq.as_raw_fd(),
                &change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if r < 0 {
            return Err(last_error("watching the worker process"));
        }
        Ok(Self {
            // SAFETY: the descriptor is owned by the `AsyncFd` and stays
            // open as long as it.
            fd: unsafe {
                tokio::io::unix::AsyncFd::register_with_interest(kq, tokio::io::Interest::READABLE)
            }?,
        })
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub(crate) fn new(_pid: u32) -> io::Result<Self> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "process watch"))
    }

    /// Resolves once the process has exited.
    pub(crate) async fn exited(&self) {
        loop {
            match self.fd.readable().await {
                Ok(mut guard) => {
                    if self.has_exited() {
                        return;
                    }
                    guard.clear_ready();
                }
                Err(_) => std::future::pending::<()>().await,
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn has_exited(&self) -> bool {
        // A pidfd is readable exactly once its process has exited.
        true
    }

    #[cfg(target_os = "macos")]
    fn has_exited(&self) -> bool {
        // SAFETY: a zeroed event; zero timeout polls.
        let mut ev: libc::kevent = unsafe { std::mem::zeroed() };
        let ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let n = unsafe {
            libc::kevent(
                self.fd.get_ref().as_raw_fd(),
                std::ptr::null(),
                0,
                &mut ev,
                1,
                &ts,
            )
        };
        n > 0 && ev.fflags & libc::NOTE_EXIT != 0
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn has_exited(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests;
