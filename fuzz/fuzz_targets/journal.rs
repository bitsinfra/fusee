#![no_main]
//! The journal matcher: a machine built from any decodable journal, driven
//! by arbitrary operations, must never panic.
use fusee_core::record::{Record, RecordBuf, Status, start_record};
use fusee_core::run::{Completion, Next, RunMachine};
use libfuzzer_sys::fuzz_target;
use std::sync::Arc;
fuzz_target!(|data: &[u8]| {
    let (journal, ops) = data.split_at(data.len() / 2);
    let p = |b: &[u8]| -> Arc<[u8]> { Arc::from(b) };
    let mut recs: Vec<RecordBuf> =
        vec![start_record(1, Arc::from("f"), Arc::from("t"), 1, 0, 0, None, 0, p(b""))];
    if let Ok(more) = fusee_core::record::decode_canonical_all(journal) {
        recs.extend(more.into_iter().filter(|r| !matches!(r, Record::Start { .. })));
    }
    let Ok(mut m) = RunMachine::new(&recs, 1) else { return };
    for b in ops {
        let name = ["a", "b", "c"][usize::from(*b % 3)];
        let seq = u32::from(b / 13);
        match b % 13 {
            0 => { let _ = m.begin_step(name); }
            1 => { let _ = m.begin_sleep(name, 5); }
            2 => { let _ = m.begin_wait(name, 0); }
            3 => { m.complete(seq, Completion::Step { status: Status::Ok, payload: p(b"1") }); }
            4 => { m.request_cancel(p(b"x")); }
            5 => {
                if let Next::Update { name, id, payload, live: Some(_), .. } = m.next() {
                    let _ = m.accept_update(name, id, payload);
                }
            }
            6 => { let _ = m.forget(seq); }
            7 => { let _ = m.begin_await(name); }
            8 => { let _ = m.awaiting(seq); }
            9 => { m.complete(seq, Completion::Completed { attempt: u32::from(*b % 3), status: Status::Ok, payload: p(b"2") }); }
            10 => { m.offer_update(u64::from(*b), Arc::from(name), Arc::from("u"), p(b"3")); }
            11 => { let _ = m.patched(name); let _ = m.heartbeat(seq, p(b"4")); }
            _ => { let _ = m.update_done(u64::from(*b % 4), Status::Ok, p(b"5")); }
        }
    }
});
