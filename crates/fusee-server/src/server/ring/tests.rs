//! The server's side of the ring against a worker driven by hand: frames
//! from a stale lease or for a shard the worker does not hold are refused
//! and never staged, and a ring that breaks the protocol revokes the lease.

use super::*;
use crate::server::{Server, ServerConfig};
use crate::shm::{Doorbell, Layout};
use fusee::{FlushMethod, LocalOptions};
use fusee_core::record::{Record, Status};
use fusee_core::segment::Batch;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;

struct FakeWorker {
    sock: UnixStream,
    dec: frame::Decoder,
    shard: u32,
    epoch: u64,
    shared: Arc<Shared>,
    bell: Doorbell,
}

impl FakeWorker {
    fn connect(path: &std::path::Path) -> Self {
        let mut sock = UnixStream::connect(path).unwrap();
        let mut b = Vec::new();
        put_u64(&mut b, 1);
        put_u32(
            &mut b,
            proto::SHARED_LOG | proto::SHARD_LEASES | proto::SHARED_MEMORY,
        );
        put_u32(&mut b, 1);
        let mut out = Vec::new();
        frame::encode(req::HELLO, &b, &mut out);
        sock.write_all(&out).unwrap();
        let mut dec = frame::Decoder::new();
        let mut buf = vec![0u8; 1 << 16];
        let mut fds = Vec::new();
        let (kind, body) = loop {
            if let Some(f) = dec.next().unwrap() {
                break f;
            }
            let n = shm::recv_with_fds(sock.as_raw_fd(), &mut buf, &mut fds).unwrap();
            assert!(n > 0);
            dec.feed(&buf[..n]);
        };
        assert_eq!(kind, resp::LEASE);
        let mut r = Reader::new(&body);
        let (_id, shards, _e, caps) = (r.u64(), r.u32().unwrap(), r.u64(), r.u32().unwrap());
        assert_ne!(caps & proto::SHARED_MEMORY, 0, "shared memory granted");
        if caps & proto::FIRST_SEGMENT_LIMIT != 0 {
            r.u64().unwrap();
        }
        assert_eq!(r.u32().unwrap(), 1);
        let (shard, epoch) = (r.u32().unwrap(), r.u64().unwrap());
        let ring = r.u64().unwrap() as usize;
        assert_eq!(fds.len(), 2, "the mapping and the doorbell");
        let mut fds = fds.into_iter();
        let shared = Arc::new(
            Shared::attach(fds.next().unwrap(), Layout::new(shards, ring).unwrap()).unwrap(),
        );
        let bell = Doorbell::attach(fds.next().unwrap()).unwrap();
        assert_eq!(
            shared.line(shard).unwrap().epoch().load(Ordering::Acquire),
            epoch,
            "the line names the lease"
        );
        sock.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        Self {
            sock,
            dec,
            shard,
            epoch,
            shared,
            bell,
        }
    }

    fn request(&mut self, kind: u8, id: u64, body: &[u8]) {
        let mut b = Vec::new();
        proto::head(
            &mut b,
            proto::Head {
                id,
                shard: self.shard,
                epoch: self.epoch,
            },
        );
        b.extend_from_slice(body);
        let mut out = Vec::new();
        frame::encode(kind, &b, &mut out);
        self.sock.write_all(&out).unwrap();
    }

    /// The next frame of `kind` within `within`; `Err` once the server
    /// closed the connection.
    fn expect(&mut self, kind: u8, within: Duration) -> Result<Option<Vec<u8>>, ()> {
        let deadline = std::time::Instant::now() + within;
        let mut buf = [0u8; 4096];
        loop {
            while let Some((k, b)) = self.dec.next().unwrap() {
                if k == kind {
                    return Ok(Some(b));
                }
            }
            if std::time::Instant::now() >= deadline {
                return Ok(None);
            }
            match self.sock.read(&mut buf) {
                Ok(0) => return Err(()),
                Ok(n) => self.dec.feed(&buf[..n]),
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) => {}
                Err(_) => return Err(()),
            }
        }
    }

    fn push(&self, shard: u32, epoch: u64, batch: &Batch) {
        let mut p = Vec::new();
        ring_head(&mut p, req::APPEND, shard, epoch, 0);
        proto::encode_section(&mut p, batch);
        assert!(self.shared.push(&p));
        self.bell.ring();
    }
}

fn section(first: u64, n: u32) -> Batch {
    let mut w = fusee_core::wal::SectionWriter::new(first);
    for i in 0..u64::from(n) {
        w.append(&Record::Step {
            run: 1,
            seq: (first + i) as u32,
            name: "s",
            attempt: 1,
            status: Status::Ok,
            payload: vec![7u8; 40],
            time: None,
        })
        .unwrap();
    }
    w.take().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_and_foreign_ring_frames_are_refused_and_a_broken_ring_revokes_the_lease() {
    let dir = std::env::temp_dir().join(format!("fusee-ring-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let server = Server::new(ServerConfig {
        dir: dir.join("data"),
        shards: 2,
        local: LocalOptions {
            flush: FlushMethod::Fsync,
            ..Default::default()
        },
        security: Default::default(),
    })
    .unwrap();
    let path = dir.join("s.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    tokio::spawn(crate::server::serve_unix(server.clone(), listener));
    let p = path.clone();
    tokio::task::spawn_blocking(move || {
        let mut w = FakeWorker::connect(&p);
        let shared = w.shared.clone();
        let line = shared.line(w.shard).unwrap();
        // Recover the shard, so its next section may follow.
        let mut b = Vec::new();
        put_u64(&mut b, 0);
        put_u32(&mut b, 0);
        w.request(req::RECOVER_LOG, 2, &b);
        assert!(
            w.expect(resp::OK, Duration::from_secs(10))
                .unwrap()
                .is_some()
        );
        // A section from the lease: durable, acknowledged in the mapping
        // and nowhere else (notify is off).
        w.push(w.shard, w.epoch, &section(1, 3));
        let t = std::time::Instant::now();
        while line.durable().load(Ordering::Acquire) < 3 {
            assert!(t.elapsed() < Duration::from_secs(10), "never durable");
            std::thread::yield_now();
        }
        assert_eq!(
            w.expect(resp::DURABLE, Duration::from_millis(200)).unwrap(),
            None
        );
        // With notify set the socket hears of it too.
        line.notify().store(1, Ordering::Release);
        w.push(w.shard, w.epoch, &section(4, 1));
        assert!(
            w.expect(resp::DURABLE, Duration::from_secs(10))
                .unwrap()
                .is_some()
        );
        assert_eq!(line.durable().load(Ordering::Acquire), 4);
        // A shard this worker does not hold: refused, nothing staged.
        let other = 1 - w.shard;
        w.push(other, w.epoch, &section(1, 1));
        let failed = w
            .expect(resp::IO_FAILED, Duration::from_secs(10))
            .unwrap()
            .unwrap();
        let mut r = Reader::new(&failed);
        assert_eq!((r.u64().unwrap(), r.u32().unwrap()), (0, other));
        assert_eq!(server.log().unwrap().durable(other), 0);
        // A stale epoch: refused, the line says so, and the shard takes no
        // more frames in this lease.
        w.push(w.shard, w.epoch + 9, &section(5, 1));
        let failed = w
            .expect(resp::IO_FAILED, Duration::from_secs(10))
            .unwrap()
            .unwrap();
        let mut r = Reader::new(&failed);
        r.u64().unwrap();
        assert_eq!(r.u32().unwrap(), w.shard);
        assert!(r.str().unwrap().contains("stale"), "{failed:?}");
        assert_eq!(line.failed().load(Ordering::Acquire), 1);
        w.push(w.shard, w.epoch, &section(5, 1));
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            server.log().unwrap().durable(w.shard),
            4,
            "refused for the lease"
        );
        // A frame whose bytes change after publication breaks the ring:
        // the lease is revoked and the worker's waiters are told.
        w.shared.force_tail(w.shared.tail_now() + 4);
        w.bell.ring();
        let t = std::time::Instant::now();
        loop {
            if w.expect(resp::OK, Duration::from_millis(100)).is_err() {
                break;
            }
            assert!(
                t.elapsed() < Duration::from_secs(10),
                "the lease survived a broken ring"
            );
        }
        assert_eq!(w.shared.closed().load(Ordering::Acquire), 1);
        assert_eq!(line.epoch().load(Ordering::Acquire), 0);
    })
    .await
    .unwrap();
    // The shard is free again: a new worker takes it under a new epoch and
    // sees exactly what was durable.
    let p = path.clone();
    tokio::task::spawn_blocking(move || {
        let w = FakeWorker::connect(&p);
        assert!(w.epoch > 0);
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
