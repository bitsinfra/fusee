//! Segment lifecycle across process lives: what recovery reads, what each
//! open leaves behind, and what a clean shutdown trims.
//!
//! Dropping a `ShardRuntime` without `close` is a crash that keeps the page
//! cache (the same as SIGKILL): nothing is checkpointed and nothing pending is
//! flushed.

use fusee::core::segment::Batch;
use fusee::core::shard::BlobRef;
use fusee::core::store::Checkpoint;
use fusee::core::{Lsn, RunId};
use fusee::env::{Env, Notify};
use fusee::local::{LocalEnv, LocalOptions};
use fusee::runtime::{RuntimeConfig, ShardRuntime, StartRequest};
use fusee::{FlushMethod, IdPolicy};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

fn tmp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-lifecycle-{name}-{}-{}",
        std::process::id(),
        fusee::local::unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn options(segment_bytes: u64) -> LocalOptions {
    LocalOptions {
        flush: FlushMethod::Fsync,
        segment_bytes,
        ..LocalOptions::default()
    }
}

fn config(segment_bytes: u64) -> RuntimeConfig {
    RuntimeConfig {
        segment_bytes,
        ..RuntimeConfig::default()
    }
}

/// Forwards to a `LocalEnv`, counting segment bytes recovery reads.
struct Counting {
    inner: LocalEnv,
    read: Arc<AtomicU64>,
}

impl Env for Counting {
    fn now_ms(&self) -> u64 {
        self.inner.now_ms()
    }
    fn monotonic_us(&self) -> u64 {
        self.inner.monotonic_us()
    }
    fn random(&mut self) -> u64 {
        self.inner.random()
    }
    fn set_notify(&mut self, notify: Notify) {
        self.inner.set_notify(notify)
    }
    fn submit(&mut self, batch: Batch) {
        self.inner.submit(batch)
    }
    fn supports_inline(&self) -> bool {
        self.inner.supports_inline()
    }
    fn flush_inline(&mut self, batch: Batch) -> io::Result<Lsn> {
        self.inner.flush_inline(batch)
    }
    fn last_flush_us(&self) -> u64 {
        self.inner.last_flush_us()
    }
    fn poll_flushed(&mut self) -> Option<io::Result<Lsn>> {
        self.inner.poll_flushed()
    }
    fn store_append(&mut self, frame: &[u8]) -> io::Result<u64> {
        self.inner.store_append(frame)
    }
    fn store_append_history(&mut self, frame: &[u8], run: RunId, lsn: Lsn) -> io::Result<u64> {
        self.inner.store_append_history(frame, run, lsn)
    }
    fn store_read(&mut self, offset: u64, len: u32) -> io::Result<Vec<u8>> {
        self.inner.store_read(offset, len)
    }
    fn store_len(&self) -> u64 {
        self.inner.store_len()
    }
    fn store_generation(&self) -> u64 {
        self.inner.store_generation()
    }
    fn compact_store(&mut self, live: &[(RunId, BlobRef)]) -> io::Result<bool> {
        self.inner.compact_store(live)
    }
    fn checkpoint(&mut self, checkpoint: Checkpoint, delete: Option<u64>) {
        self.inner.checkpoint(checkpoint, delete)
    }
    fn poll_checkpoint(&mut self) -> Option<io::Result<Lsn>> {
        self.inner.poll_checkpoint()
    }
    fn load_checkpoint(&mut self) -> io::Result<Option<Checkpoint>> {
        self.inner.load_checkpoint()
    }
    fn load_store(&mut self, generation: u64, len: u64) -> io::Result<Vec<u8>> {
        self.inner.load_store(generation, len)
    }
    fn recover_store(
        &mut self,
        generation: u64,
        len: u64,
        visit: &mut dyn FnMut(u64, u32, fusee::core::store::StoreEntry),
    ) -> io::Result<()> {
        self.inner.recover_store(generation, len, visit)
    }
    fn segments(&mut self) -> io::Result<Vec<u64>> {
        self.inner.segments()
    }
    fn read_segment(&mut self, segment: u64) -> io::Result<Vec<u8>> {
        let bytes = self.inner.read_segment(segment)?;
        self.read.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }
    fn read_segment_from(
        &mut self,
        segment: u64,
        offset: u64,
        len: usize,
    ) -> io::Result<(Vec<u8>, bool)> {
        let (bytes, end) = self.inner.read_segment_from(segment, offset, len)?;
        self.read.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok((bytes, end))
    }
    fn read_segment_header(&mut self, segment: u64) -> io::Result<Vec<u8>> {
        let bytes = self.inner.read_segment_header(segment)?;
        self.read.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }
    fn sync_recovered(&mut self) -> io::Result<()> {
        self.inner.sync_recovered()
    }
    fn discard_segment(&mut self, segment: u64) -> io::Result<()> {
        self.inner.discard_segment(segment)
    }
    fn first_segment_bytes(&self, maximum: u64) -> u64 {
        self.inner.first_segment_bytes(maximum)
    }
    fn exclusive_log(&self) -> bool {
        self.inner.exclusive_log()
    }
    fn open_log(&mut self, segment: u64) -> io::Result<()> {
        self.inner.open_log(segment)
    }
}

fn open(dir: &Path, segment_bytes: u64, read: &Arc<AtomicU64>) -> ShardRuntime<Counting> {
    let env = Counting {
        inner: LocalEnv::open(dir, options(segment_bytes)).unwrap(),
        read: read.clone(),
    };
    // No registry: a foreign host would run the code. Starts still write
    // their records, which is all these tests need.
    ShardRuntime::open(env, config(segment_bytes), None, Arc::new(|| {})).unwrap()
}

fn start(rt: &ShardRuntime<Counting>, id: &str) {
    let input: fusee::core::record::Payload = Arc::from(&b"null"[..]);
    rt.start(StartRequest::new(id, "job", input, IdPolicy::default()))
        .unwrap();
    rt.flush_all().unwrap();
}

fn segment_files(dir: &Path) -> Vec<u64> {
    let mut v: Vec<u64> = std::fs::read_dir(dir.join("log"))
        .unwrap()
        .filter_map(|e| {
            e.ok()?
                .file_name()
                .to_str()?
                .strip_suffix(".seg")?
                .parse()
                .ok()
        })
        .collect();
    v.sort_unstable();
    v
}

#[test]
fn an_idle_crash_loop_does_not_add_segments() {
    let dir = tmp("idle");
    let read = Arc::new(AtomicU64::new(0));
    for _ in 0..20 {
        let rt = open(&dir, 1 << 20, &read);
        rt.tick();
        drop(rt);
        assert!(segment_files(&dir).len() <= 1, "{:?}", segment_files(&dir));
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_busy_crash_loop_keeps_the_log_short_and_loses_nothing() {
    let dir = tmp("busy");
    let read = Arc::new(AtomicU64::new(0));
    for life in 0..20 {
        let rt = open(&dir, 1 << 20, &read);
        for earlier in 0..life {
            assert!(
                rt.find(&format!("job-{earlier}")).is_some(),
                "life {life} lost job-{earlier}"
            );
        }
        start(&rt, &format!("job-{life}"));
        // The open checkpointed a long recovered tail in the background;
        // the next ticks collect it. Dropping waits for the thread.
        for _ in 0..50 {
            rt.tick();
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        drop(rt);
        let files = segment_files(&dir);
        assert!(files.len() <= 3, "life {life}: {files:?}");
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_clean_shutdown_checkpoints_and_trims_the_log() {
    let dir = tmp("clean");
    let read = Arc::new(AtomicU64::new(0));
    for life in 0..10 {
        let rt = open(&dir, 1 << 20, &read);
        start(&rt, &format!("job-{life}"));
        rt.close().unwrap();
        drop(rt);
        assert!(segment_files(&dir).len() <= 2, "{:?}", segment_files(&dir));
    }
    // Everything is behind the last checkpoint: the next open reads the
    // older segment's header and the start of the active one (the
    // checkpoint does not say where in it the covered records end).
    read.store(0, Ordering::Relaxed);
    let rt = open(&dir, 1 << 20, &read);
    for life in 0..10 {
        assert!(rt.find(&format!("job-{life}")).is_some());
    }
    let bytes = read.load(Ordering::Relaxed);
    assert!(
        bytes <= (256 << 10) + 2 * 4096,
        "read {bytes} bytes of a fully checkpointed log"
    );
    drop(rt);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn recovery_reads_the_valid_prefix_not_the_whole_segment() {
    let dir = tmp("prefix");
    let read = Arc::new(AtomicU64::new(0));
    let segment = 64 << 20;
    {
        let rt = open(&dir, segment, &read);
        for i in 0..100 {
            start(&rt, &format!("job-{i}"));
        }
        drop(rt);
    }
    read.store(0, Ordering::Relaxed);
    let rt = open(&dir, segment, &read);
    for i in 0..100 {
        assert!(rt.find(&format!("job-{i}")).is_some());
    }
    let bytes = read.load(Ordering::Relaxed);
    assert!(
        bytes <= 4 << 20,
        "read {bytes} bytes to recover a few KiB from a {segment}-byte segment"
    );
    drop(rt);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn stall_tracing_records_slow_ticks_and_flushes_with_context() {
    // Process-wide: other tests in this binary may trace too, harmlessly.
    fusee::runtime::set_stall_threshold_us(1);
    let dir = tmp("stalls");
    let read = Arc::new(AtomicU64::new(0));
    let rt = open(&dir, 1 << 20, &read);
    for i in 0..5 {
        start(&rt, &format!("job-{i}"));
    }
    let stalls = rt.take_stalls();
    assert!(!stalls.is_empty());
    assert!(
        stalls
            .iter()
            .any(|s| s.kind != fusee::runtime::StallKind::Tick && s.records > 0 && s.bytes > 0)
    );
    let m = rt.metrics();
    assert!(m.stalls >= stalls.len() as u64);
    assert!(m.max_flush_us > 0 && m.max_tick_us > 0);
    fusee::runtime::set_stall_threshold_us(0);
    drop(rt);
    std::fs::remove_dir_all(dir).unwrap();
}
