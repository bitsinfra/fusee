//! Deterministic tests of the shared-memory ring, watermarks and doorbell,
//! and of passing a mapping over a Unix socket.

use super::*;
use std::sync::Arc;

fn shared(ring: usize) -> Shared {
    Shared::create(Layout::new(4, ring).unwrap(), std::process::id()).unwrap()
}

fn payload(n: usize, seed: u8) -> Vec<u8> {
    (0..n)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

#[test]
fn frames_arrive_whole_and_in_order_across_wraparound() {
    let s = shared(4096);
    let mut head = s.consumed();
    let mut out = Vec::new();
    let mut seed = 0u8;
    let mut sent = std::collections::VecDeque::new();
    // Sizes that do not divide the ring, so frames straddle its end.
    for round in 0..2000usize {
        let n = 1 + (round * 37) % 900;
        let p = payload(n, seed);
        seed = seed.wrapping_add(1);
        while !s.push(&p) {
            // Full: the consumer catches up.
            assert!(s.pop(&mut head, &mut out).unwrap());
            assert_eq!(out, sent.pop_front().unwrap());
            s.release(head);
        }
        sent.push_back(p);
        if round % 3 == 0 {
            while s.pop(&mut head, &mut out).unwrap() {
                assert_eq!(out, sent.pop_front().unwrap());
            }
            s.release(head);
        }
    }
    while s.pop(&mut head, &mut out).unwrap() {
        assert_eq!(out, sent.pop_front().unwrap());
    }
    assert!(sent.is_empty());
    assert!(head > 20 * 4096, "the ring wrapped many times");
}

#[test]
fn a_full_ring_refuses_until_the_consumer_releases() {
    let s = shared(4096);
    let p = payload(1000, 1);
    let mut pushed = 0;
    while s.push(&p) {
        pushed += 1;
    }
    assert_eq!(pushed, 4, "four 1008-byte frames fill 4 KiB");
    let mut head = s.consumed();
    let mut out = Vec::new();
    assert!(s.pop(&mut head, &mut out).unwrap());
    assert!(!s.push(&p), "space is reused only once released");
    s.release(head);
    assert!(s.push(&p));
    // Larger than a quarter of the ring: never in the ring.
    assert!(!s.push(&payload(1025, 0)));
}

#[test]
fn a_frame_being_written_is_invisible_until_its_tail_is_published() {
    // The worker dies while copying a frame: its tail never moved, so the
    // server never sees it, and earlier frames stay intact.
    let s = shared(4096);
    assert!(s.push(b"whole"));
    let tail = s.tail_now();
    let p = payload(300, 9);
    let mut header = [0u8; FRAME_HEADER];
    header[..4].copy_from_slice(&(p.len() as u32).to_le_bytes());
    header[4..].copy_from_slice(&crc32c::crc32c(&p).to_le_bytes());
    s.raw_write(tail, &header);
    s.raw_write(tail + FRAME_HEADER as u64, &p[..100]);
    let mut head = s.consumed();
    let mut out = Vec::new();
    assert!(s.pop(&mut head, &mut out).unwrap());
    assert_eq!(out, b"whole");
    assert!(!s.pop(&mut head, &mut out).unwrap());
    assert!(!s.pending(head));
}

#[test]
fn torn_partial_and_out_of_range_frames_are_refused() {
    let p = payload(200, 3);
    let size = (FRAME_HEADER + p.len()).next_multiple_of(8) as u64;
    let cases: Vec<(&str, Box<dyn Fn(&Shared)>)> = vec![
        (
            "checksum",
            Box::new(|s: &Shared| {
                let t = s.tail_now();
                assert!(s.push(&payload(200, 3)));
                // A byte of the payload changed after the checksum.
                s.raw_write(t + FRAME_HEADER as u64 + 17, &[0xAA]);
            }),
        ),
        (
            "inside a frame",
            Box::new(move |s: &Shared| {
                let t = s.tail_now();
                assert!(s.push(&payload(200, 3)));
                s.force_tail(t + size - 8);
            }),
        ),
        (
            "too large",
            Box::new(|s: &Shared| {
                let t = s.tail_now();
                let mut h = [0u8; FRAME_HEADER];
                h[..4].copy_from_slice(&(3000u32).to_le_bytes());
                s.raw_write(t, &h);
                s.force_tail(t + 3008);
            }),
        ),
    ];
    // A tail that is not on a frame boundary at all is refused at once.
    let s = shared(4096);
    let mut head = s.consumed();
    let mut out = Vec::new();
    assert!(s.push(b"before"));
    s.force_tail(s.tail_now() + 4);
    assert!(s.pop(&mut head, &mut out).is_err());
    // Nor is one more than a ring ahead of the head.
    let s = shared(4096);
    let mut head = s.consumed();
    s.force_tail(4096 * 2);
    assert!(s.pop(&mut head, &mut out).is_err());
    for (name, corrupt) in cases {
        let s = shared(4096);
        let mut head = s.consumed();
        let mut out = Vec::new();
        assert!(s.push(b"before"));
        corrupt(&s);
        assert!(s.pop(&mut head, &mut out).unwrap(), "{name}");
        assert_eq!(out, b"before");
        let e = s.pop(&mut head, &mut out).unwrap_err();
        assert!(!e.0.is_empty(), "{name}: {e:?}");
    }
}

#[test]
fn a_parked_worker_wakes_when_its_watermark_is_published() {
    let s = Arc::new(shared(4096));
    let s2 = s.clone();
    let waiter = std::thread::spawn(move || {
        let line = s2.line(2).unwrap();
        let done = || line.durable().load(Ordering::Acquire) >= 7;
        line.wait_until(Duration::ZERO, Duration::from_secs(20), &done)
    });
    let line = s.line(2).unwrap();
    while line.parked().load(Ordering::Acquire) == 0 {
        std::thread::yield_now();
    }
    let t = std::time::Instant::now();
    line.durable().store(7, Ordering::Release);
    line.publish();
    assert!(waiter.join().unwrap());
    assert!(t.elapsed() < Duration::from_secs(5), "woken, not timed out");
    assert!(s.line(4).is_none());
}

#[test]
fn the_doorbell_wakes_a_sleeping_consumer_and_only_rings_when_it_sleeps() {
    let bell = Arc::new(Doorbell::create().unwrap());
    let s = Arc::new(shared(4096));
    let (b2, s2) = (bell.clone(), s.clone());
    let consumer = std::thread::spawn(move || {
        let head = s2.consumed();
        let t = std::time::Instant::now();
        while !s2.pending(head) {
            b2.sleep(&|| s2.pending(head), Duration::from_secs(20));
        }
        t.elapsed()
    });
    while bell.sleeping().load(Ordering::Acquire) == 0 {
        std::thread::yield_now();
    }
    let before = bell.seq().load(Ordering::Acquire);
    assert!(s.push(b"x"));
    bell.ring();
    assert!(consumer.join().unwrap() < Duration::from_secs(5));
    assert_eq!(bell.seq().load(Ordering::Acquire), before + 1);
    // Nobody sleeps: ringing costs no wake-up.
    bell.ring();
    assert_eq!(bell.seq().load(Ordering::Acquire), before + 1);
}

#[test]
fn a_mapping_passed_over_a_unix_socket_is_the_same_memory() {
    let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
    assert_eq!(peer_pid(a.as_raw_fd()).unwrap(), std::process::id());
    let layout = Layout::new(3, 8192).unwrap();
    let server = Shared::create(layout, 77).unwrap();
    let bell = Doorbell::create().unwrap();
    let n = send_with_fds(a.as_raw_fd(), b"lease", &[server.fd(), bell.fd()]).unwrap();
    assert_eq!(n, 5);
    let mut buf = [0u8; 16];
    let mut fds = Vec::new();
    let got = recv_with_fds(b.as_raw_fd(), &mut buf, &mut fds).unwrap();
    assert_eq!(&buf[..got], b"lease");
    assert_eq!(fds.len(), 2);
    let mut fds = fds.into_iter();
    let worker = Shared::attach(fds.next().unwrap(), layout).unwrap();
    let wbell = Doorbell::attach(fds.next().unwrap()).unwrap();
    assert_eq!(worker.worker_pid(), 77);
    // The worker's frames reach the server's view, and the server's
    // watermarks the worker's, through two separate mappings.
    assert!(worker.push(b"section"));
    let mut head = server.consumed();
    let mut out = Vec::new();
    assert!(server.pop(&mut head, &mut out).unwrap());
    assert_eq!(out, b"section");
    server
        .line(1)
        .unwrap()
        .durable()
        .store(42, Ordering::Release);
    assert_eq!(
        worker.line(1).unwrap().durable().load(Ordering::Acquire),
        42
    );
    wbell.ring();
    // A layout the server did not make is refused.
    let wrong = Layout::new(4, 8192).unwrap();
    let (c, d) = std::os::unix::net::UnixStream::pair().unwrap();
    send_with_fds(c.as_raw_fd(), b"x", &[server.fd()]).unwrap();
    let mut fds = Vec::new();
    recv_with_fds(d.as_raw_fd(), &mut buf, &mut fds).unwrap();
    assert!(Shared::attach(fds.pop().unwrap(), wrong).is_err());
}

#[tokio::test]
async fn a_watched_process_is_seen_to_exit() {
    let mut child = std::process::Command::new("sleep")
        .arg("0.2")
        .spawn()
        .unwrap();
    let watch = ProcWatch::new(child.id()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), watch.exited())
        .await
        .expect("the exit was not seen");
    child.wait().unwrap();
}
