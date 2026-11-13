//! A stable-toolchain stand-in for the cargo-fuzz targets in `fuzz/`:
//! random and mutated inputs through every decoder, which must never panic.

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn inputs(seed: u64, n: usize) -> Vec<Vec<u8>> {
    let mut r = Rng(seed | 1);
    let mut w = fusee_core::segment::LogWriter::new(0, 1, 1, 64 * 1024, 0);
    for i in 0..20u32 {
        let rec: fusee_core::RecordRef<'_> = fusee_core::Record::Step {
            run: 1,
            seq: i,
            name: "s",
            attempt: 1,
            status: fusee_core::Status::Ok,
            payload: b"42",
            time: None,
        };
        let _ = w.append(&rec, 0);
    }
    let mut seg = vec![0u8; 64 * 1024];
    if let Some(b) = w.take() {
        for c in &b.chunks {
            seg[c.offset as usize..c.offset as usize + c.bytes.len()].copy_from_slice(&c.bytes);
        }
    }
    let mut frames = Vec::new();
    fusee_core::frame::encode(3, b"hello", &mut frames);
    let mut out = Vec::new();
    for _ in 0..n {
        let base = match r.next() % 3 {
            0 => seg.clone(),
            1 => frames.clone(),
            _ => (0..(r.next() % 200)).map(|_| r.next() as u8).collect(),
        };
        let mut m = base;
        for _ in 0..(r.next() % 8) {
            if m.is_empty() {
                break;
            }
            let i = (r.next() as usize) % m.len();
            m[i] ^= r.next() as u8;
        }
        if r.next().is_multiple_of(4) && !m.is_empty() {
            let cut = (r.next() as usize) % m.len();
            m.truncate(cut);
        }
        out.push(m);
    }
    out
}

#[test]
fn decoders_never_panic() {
    for input in inputs(7, 3000) {
        let _ = fusee_core::record::decode_canonical_all(&input);
        let _ = fusee_core::segment::scan_segment(&input, |_, _| {});
        let _ = fusee_core::store::scan_store(&input, |_, _, _| {});
        let _ = fusee_core::store::Checkpoint::decode(&input);
        let mut d = fusee_core::frame::Decoder::new();
        d.feed(&input);
        while let Ok(Some(_)) = d.next() {}
    }
}

/// The journal matcher driven by random operations over random journals
/// (the `journal` fuzz target, on the stable toolchain).
#[test]
fn journal_machine_never_panics() {
    use fusee_core::record::{Record, RecordBuf, Status, start_record};
    use fusee_core::run::{Completion, Next, RunMachine};
    use std::sync::Arc;
    let p = |b: &[u8]| -> Arc<[u8]> { Arc::from(b) };
    let mut r = Rng(99);
    for _ in 0..3000 {
        let mut recs: Vec<RecordBuf> = vec![start_record(
            1,
            Arc::from("f"),
            Arc::from("t"),
            1,
            0,
            0,
            None,
            0,
            p(b""),
        )];
        for _ in 0..(r.next() % 12) {
            let seq = (r.next() % 6) as u32;
            let name = ["a", "b", "c"][(r.next() % 3) as usize];
            let rec: RecordBuf = match r.next() % 8 {
                0 => Record::Step {
                    run: 1,
                    seq,
                    name: Arc::from(name),
                    attempt: 1,
                    status: Status::Ok,
                    payload: p(b"1"),
                    time: None,
                },
                1 => Record::Timer {
                    run: 1,
                    seq,
                    name: Arc::from(name),
                    fire_at_ms: 3,
                },
                2 => Record::Fired { run: 1, seq },
                3 => Record::Update {
                    run: 1,
                    name: Arc::from(name),
                    id: Arc::from("u"),
                    payload: p(b"2"),
                    caller: None,
                },
                4 => Record::Awaiting {
                    run: 1,
                    seq,
                    name: Arc::from(name),
                    attempt: 1,
                },
                5 => Record::Completed {
                    run: 1,
                    seq,
                    attempt: 1,
                    status: Status::Ok,
                    payload: p(b"3"),
                },
                6 => Record::Patch {
                    run: 1,
                    at: seq,
                    id: Arc::from(name),
                },
                _ => Record::UpdateDone {
                    run: 1,
                    seq,
                    ordinal: 0,
                    status: Status::Ok,
                    payload: p(b"4"),
                    state: p(b""),
                },
            };
            recs.push(rec);
        }
        let Ok(mut m) = RunMachine::new(&recs, 1) else {
            continue;
        };
        for _ in 0..(r.next() % 30) {
            let b = r.next();
            let name = ["a", "b", "c"][(b % 3) as usize];
            let seq = ((b >> 8) % 8) as u32;
            match (b >> 4) % 13 {
                0 => {
                    let _ = m.begin_step(name);
                }
                1 => {
                    let _ = m.begin_sleep(name, 5);
                }
                2 => {
                    let _ = m.begin_wait(name, 0);
                }
                3 => {
                    m.complete(
                        seq,
                        Completion::Step {
                            status: Status::Ok,
                            payload: p(b"1"),
                        },
                    );
                }
                4 => m.request_cancel(p(b"x")),
                5 => {
                    if let Next::Update {
                        name,
                        id,
                        payload,
                        live: Some(_),
                        ..
                    } = m.next()
                    {
                        let _ = m.accept_update(name, id, payload);
                    }
                }
                6 => {
                    let _ = m.forget(seq);
                }
                7 => {
                    let _ = m.begin_await(name);
                }
                8 => {
                    let _ = m.awaiting(seq);
                }
                9 => {
                    m.complete(
                        seq,
                        Completion::Completed {
                            attempt: 1,
                            status: Status::Ok,
                            payload: p(b"2"),
                        },
                    );
                }
                10 => m.offer_update(b, Arc::from(name), Arc::from("u"), p(b"3")),
                11 => {
                    let _ = m.patched(name);
                    let _ = m.heartbeat(seq, p(b"4"));
                }
                _ => {
                    let _ = m.update_done(b % 3, Status::Ok, p(b"5"));
                }
            }
        }
    }
}
