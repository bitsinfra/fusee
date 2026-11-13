//! Drives a memory engine through the C interface as an SDK would: start a
//! run, take its Start event, run one step, finish, and read the result.

use super::*;
use std::sync::atomic::AtomicU32;

fn s(v: &str) -> fusee_str {
    fusee_str::of(v.as_bytes())
}

unsafe fn view<'a>(v: fusee_str) -> &'a [u8] {
    unsafe { bytes(v) }.unwrap()
}

static NOTIFIED: AtomicU32 = AtomicU32::new(0);

unsafe extern "C" fn notify(_: *mut c_void) {
    NOTIFIED.fetch_add(1, Ordering::Relaxed);
}

fn options() -> fusee_options {
    fusee_options {
        size: std::mem::size_of::<fusee_options>() as u32,
        durability: c"memory".as_ptr(),
        dir: std::ptr::null(),
        address: std::ptr::null(),
        replica_id: -1,
        n_peers: 0,
        peers: std::ptr::null(),
        shards: 2,
        keep_history: 1,
        archive_history: 0,
        inline_flush: -1,
        park_after_ms: -1,
        retention_ms: -1,
        checkpoint_interval_ms: -1,
        segment_bytes: -1,
        flush: std::ptr::null(),
        n_codec_keys: 0,
        codec_keys: std::ptr::null(),
        notify: Some(notify),
        notify_user: std::ptr::null_mut(),
        notify_fd: -1,
    }
}

/// Ticks until `want` returns something, at most a second.
unsafe fn tick_until<T>(
    e: *mut fusee_engine,
    mut want: impl FnMut(&[u64], &[fusee_str]) -> Option<T>,
) -> T {
    let until = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut t = fusee_tick {
            events: std::ptr::null(),
            n_events: 0,
            extras: std::ptr::null(),
            n_extras: 0,
            next_wake_ms: -1,
        };
        assert_eq!(unsafe { fusee_engine_tick(e, &mut t) }, FUSEE_OK);
        let ev = unsafe { slice(t.events, t.n_events * FUSEE_EVENT_STRIDE) }.unwrap();
        let ex = unsafe { slice(t.extras, t.n_extras) }.unwrap();
        if let Some(v) = want(ev, ex) {
            return v;
        }
        assert!(std::time::Instant::now() < until, "nothing came");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn a_step_through_the_c_interface() {
    unsafe {
        let mut e: *mut fusee_engine = std::ptr::null_mut();
        let mut err = fusee_buf {
            ptr: std::ptr::null_mut(),
            len: 0,
            cap: 0,
        };
        assert_eq!(fusee_engine_open(&options(), &mut e, &mut err), FUSEE_OK);
        let io = fusee_engine_io(e);
        assert_eq!((*io).shards, 2);
        let req = fusee_start {
            id: s("w-1"),
            wtype: s("w"),
            input: s("{\"n\":1}"),
            version: 1,
            policy: 0,
            deadline_ms: 0,
            token: 1,
            has_update: 0,
            _reserved: 0,
            update_name: EMPTY,
            update_id: EMPTY,
            update_arg: EMPTY,
            update_token: -1,
        };
        let mut existed = 9;
        assert_eq!(fusee_engine_start(e, req, &mut existed), FUSEE_OK);
        assert_eq!(existed, 0);
        let mut acked = false;
        let (shard, run) = tick_until(e, |ev, ex| {
            acked |= ev
                .chunks(FUSEE_EVENT_STRIDE)
                .any(|r| r[0] == FUSEE_EV_ACK && r[1] == 1);
            ev.chunks(FUSEE_EVENT_STRIDE).find_map(|r| {
                (r[0] == FUSEE_EV_START).then(|| {
                    let x = r[5] as usize;
                    assert_eq!(view(ex[x]), b"w-1");
                    assert_eq!(view(ex[x + 1]), b"w");
                    assert_eq!(view(ex[x + 2]), b"{\"n\":1}");
                    (r[1] as u32, r[2])
                })
            })
        });
        let mut step = std::mem::zeroed::<fusee_step>();
        assert_eq!(
            fusee_engine_begin_step(e, shard, run, s("a"), FUSEE_STEP_INLINE, &mut step),
            FUSEE_OK
        );
        assert_eq!(step.replay, 0);
        if step.open == 0 {
            tick_until(e, |_, _| {
                let mut open = 0;
                let rc = fusee_engine_step_gate(
                    e,
                    shard,
                    run,
                    step.gate,
                    step.not_before_ms,
                    0,
                    &mut open,
                );
                assert_eq!(rc, FUSEE_OK);
                (open == 1).then_some(())
            });
        }
        assert_eq!(
            fusee_engine_step_done(
                e,
                shard,
                run,
                step.seq,
                FUSEE_DONE_OK | FUSEE_DONE_KEEP,
                s("2")
            ),
            FUSEE_OK
        );
        let mut d = no_delivery();
        assert_eq!(fusee_engine_quiescent(e, shard, run, &mut d), FUSEE_OK);
        assert_eq!(d.kind, FUSEE_DELIVERY_HELD);
        assert_eq!(d.seq, step.seq);
        // A recorded value.
        let mut nums = [0u64; 5];
        let mut data = EMPTY;
        let cand = 7u64.to_le_bytes();
        assert_eq!(
            fusee_engine_host(
                e,
                shard,
                run,
                FUSEE_OP_VALUE,
                0,
                0,
                0,
                fusee_str::of(&cand),
                EMPTY,
                EMPTY,
                nums.as_mut_ptr(),
                &mut data
            ),
            FUSEE_OK
        );
        assert_eq!(view(data), &cand);
        // An unknown operation fails with a message.
        assert_eq!(
            fusee_engine_host(
                e,
                shard,
                run,
                999,
                0,
                0,
                0,
                EMPTY,
                EMPTY,
                EMPTY,
                std::ptr::null_mut(),
                std::ptr::null_mut()
            ),
            FUSEE_INVALID
        );
        assert!(String::from_utf8_lossy(view(fusee_engine_error(e))).contains("999"));
        assert_eq!(
            fusee_engine_finish(e, shard, run, 0, s("\"done\"")),
            FUSEE_OK
        );
        tick_until(e, |ev, _| {
            acked |= ev
                .chunks(FUSEE_EVENT_STRIDE)
                .any(|r| r[0] == FUSEE_EV_ACK && r[1] == 1);
            acked.then_some(())
        });
        let mut r = std::mem::zeroed::<fusee_result>();
        tick_until(e, |_, _| {
            assert_eq!(fusee_engine_result(e, s("w-1"), &mut r), FUSEE_OK);
            (r.state == 2).then_some(())
        });
        assert_eq!(r.status, 0);
        assert_eq!(view(r.payload), b"\"done\"");
        let mut json = EMPTY;
        let mut found = 0;
        assert_eq!(
            fusee_engine_inspect(e, s("w-1"), &mut json, &mut found),
            FUSEE_OK
        );
        assert_eq!(found, 1);
        let v: serde_json::Value = serde_json::from_slice(view(json)).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["journal"][1]["kind"], "step");
        // A missing run is an error with its own code.
        let mut point = (0u32, 0u64, 0u64, 0u32);
        assert_eq!(
            fusee_engine_query_point(
                e,
                s("nope"),
                &mut point.0,
                &mut point.1,
                &mut point.2,
                &mut point.3
            ),
            FUSEE_NOT_FOUND
        );
        let mut out = [
            fusee_buf {
                ptr: std::ptr::null_mut(),
                len: 0,
                cap: 0,
            },
            fusee_buf {
                ptr: std::ptr::null_mut(),
                len: 0,
                cap: 0,
            },
            fusee_buf {
                ptr: std::ptr::null_mut(),
                len: 0,
                cap: 0,
            },
            fusee_buf {
                ptr: std::ptr::null_mut(),
                len: 0,
                cap: 0,
            },
        ];
        assert_eq!(
            fusee_queue_request(
                1,
                s("collect"),
                s("k"),
                s("e1"),
                s("\"a\""),
                f64::NAN,
                f64::NAN,
                f64::NAN,
                out.as_mut_ptr(),
                &mut err
            ),
            FUSEE_OK
        );
        assert_eq!(
            std::slice::from_raw_parts(out[0].ptr, out[0].len),
            b"fusee.batch"
        );
        for b in &mut out {
            fusee_buf_free(b);
        }
        assert_eq!(fusee_engine_close(e, &mut err), FUSEE_OK);
        fusee_buf_free(&mut err);
    }
}

/// One journal operation with a name and a writer; its numbers and data.
unsafe fn op(
    e: *mut fusee_engine,
    shard: u32,
    run: u64,
    code: u32,
    nums: [u64; 2],
    name: &str,
    chunk: &[u8],
) -> ([u64; 5], Vec<u8>) {
    let mut out = [0u64; 5];
    let mut data = EMPTY;
    let rc = unsafe {
        fusee_engine_host(
            e,
            shard,
            run,
            code,
            nums[0],
            nums[1],
            0,
            s(name),
            fusee_str::of(chunk),
            EMPTY,
            out.as_mut_ptr(),
            &mut data,
        )
    };
    assert_eq!(rc, FUSEE_OK, "{}", unsafe {
        String::from_utf8_lossy(view(fusee_engine_error(e)))
    });
    (out, unsafe { view(data) }.to_vec())
}

/// A run's stream through the C interface: workflow code's own stream, a
/// step's stream (JSON and bytes, completed by the step's result), the
/// chunks a step's retry finds recorded, and reads of both by offset.
#[test]
fn streams_through_the_c_interface() {
    unsafe {
        let mut e: *mut fusee_engine = std::ptr::null_mut();
        let mut err = fusee_buf {
            ptr: std::ptr::null_mut(),
            len: 0,
            cap: 0,
        };
        assert_eq!(fusee_engine_open(&options(), &mut e, &mut err), FUSEE_OK);
        assert_eq!(fusee_abi_version(), 4);
        let req = fusee_start {
            id: s("s-1"),
            wtype: s("w"),
            input: s("null"),
            version: 1,
            policy: 0,
            deadline_ms: 0,
            token: 1,
            has_update: 0,
            _reserved: 0,
            update_name: EMPTY,
            update_id: EMPTY,
            update_arg: EMPTY,
            update_token: -1,
        };
        let mut existed = 0;
        assert_eq!(fusee_engine_start(e, req, &mut existed), FUSEE_OK);
        let (shard, run) = tick_until(e, |ev, _| {
            ev.chunks(FUSEE_EVENT_STRIDE)
                .find_map(|r| (r[0] == FUSEE_EV_START).then_some((r[1] as u32, r[2])))
        });
        // Workflow code's stream.
        let (o, _) = op(
            e,
            shard,
            run,
            FUSEE_OP_STREAM_OPEN,
            [FUSEE_NO_SEQ, 0],
            "progress",
            &[],
        );
        assert_eq!(o[..2], [0, 0]);
        op(
            e,
            shard,
            run,
            FUSEE_OP_STREAM_APPEND,
            [FUSEE_NO_SEQ, 0],
            "progress",
            b"\"start\"",
        );
        // A step's stream: its first attempt appends and fails.
        let mut step = std::mem::zeroed::<fusee_step>();
        assert_eq!(
            fusee_engine_begin_step(e, shard, run, s("gen"), FUSEE_STEP_INLINE, &mut step),
            FUSEE_OK
        );
        let open = |step: &fusee_step| {
            if step.open == 0 {
                tick_until(e, |_, _| {
                    let mut open = 0;
                    let rc = fusee_engine_step_gate(
                        e,
                        shard,
                        run,
                        step.gate,
                        step.not_before_ms,
                        0,
                        &mut open,
                    );
                    assert_eq!(rc, FUSEE_OK);
                    (open == 1).then_some(())
                });
            }
        };
        open(&step);
        let seq = u64::from(step.seq);
        let (o, _) = op(e, shard, run, FUSEE_OP_STREAM_OPEN, [seq, 0], "tokens", &[]);
        assert_eq!(o[..2], [0, 0]);
        op(
            e,
            shard,
            run,
            FUSEE_OP_STREAM_APPEND,
            [seq, 0],
            "tokens",
            b"\"a\"",
        );
        op(
            e,
            shard,
            run,
            FUSEE_OP_STREAM_APPEND,
            [seq, 0],
            "tokens",
            b"\0\x01\x02",
        );
        let mut gate = 0;
        assert_eq!(
            fusee_engine_step_retry(e, shard, run, step.seq, 0, s("cut off"), &mut gate),
            FUSEE_OK
        );
        step.gate = gate;
        step.open = 0;
        step.not_before_ms = 0;
        open(&step);
        // The retry finds both chunks and continues after them.
        let (o, _) = op(e, shard, run, FUSEE_OP_STREAM_OPEN, [seq, 0], "tokens", &[]);
        assert_eq!(o[..2], [2, 0]);
        let (o, framed) = op(
            e,
            shard,
            run,
            FUSEE_OP_STREAM_RECORDED,
            [seq, 2],
            "tokens",
            &[],
        );
        assert_eq!(o[0], 2);
        assert_eq!(framed, b"\x03\0\0\0\"a\"\x03\0\0\0\0\x01\x02");
        // Workflow code cannot write a step's stream.
        let mut out = [0u64; 5];
        let rc = fusee_engine_host(
            e,
            shard,
            run,
            FUSEE_OP_STREAM_APPEND,
            FUSEE_NO_SEQ,
            0,
            0,
            s("tokens"),
            s("1"),
            EMPTY,
            out.as_mut_ptr(),
            std::ptr::null_mut(),
        );
        assert_ne!(rc, FUSEE_OK);
        op(
            e,
            shard,
            run,
            FUSEE_OP_STREAM_APPEND,
            [seq, 0],
            "tokens",
            b"\"b\"",
        );
        assert_eq!(
            fusee_engine_step_done(
                e,
                shard,
                run,
                step.seq,
                FUSEE_DONE_OK | FUSEE_DONE_KEEP,
                s("3")
            ),
            FUSEE_OK
        );
        // Its delivery records the result, and just before it the end of
        // the stream the step opened.
        let mut d = no_delivery();
        assert_eq!(fusee_engine_quiescent(e, shard, run, &mut d), FUSEE_OK);
        assert_eq!((d.kind, d.seq), (FUSEE_DELIVERY_HELD, step.seq));
        op(
            e,
            shard,
            run,
            FUSEE_OP_STREAM_CLOSE,
            [FUSEE_NO_SEQ, 0],
            "progress",
            &[],
        );
        // Reads from an offset until the end: (chunks, which are bytes, end).
        let mut token = 10;
        let mut read = |name: &str, mut from: u64| {
            let mut chunks = Vec::new();
            let mut raw = Vec::new();
            loop {
                token += 1;
                assert_eq!(
                    fusee_engine_stream_read(e, s("s-1"), -1, s(name), from, 5_000, token),
                    FUSEE_OK
                );
                let b: serde_json::Value = tick_until(e, |ev, ex| {
                    ev.chunks(FUSEE_EVENT_STRIDE).find_map(|r| {
                        (r[0] == FUSEE_EV_REPLY && r[1] == token).then(|| {
                            assert_eq!(r[2], 0);
                            serde_json::from_slice(view(ex[r[5] as usize])).unwrap()
                        })
                    })
                });
                assert_eq!(b["run"], run);
                assert_eq!(b["from"].as_u64(), Some(from));
                let got = b["chunks"].as_array().unwrap();
                for i in b["bytes"].as_array().unwrap() {
                    raw.push(i.as_u64().unwrap() + from);
                }
                chunks.extend(got.iter().cloned());
                from = b["next"].as_u64().unwrap();
                if !b["end"].is_null() {
                    return (chunks, raw, from, b["end"].clone());
                }
            }
        };
        // The step's stream from offset 1, ended by its result.
        let (chunks, raw, next, end) = read("tokens", 1);
        assert_eq!(chunks, [serde_json::json!("AQI="), serde_json::json!("b")]);
        assert_eq!((raw, next), (vec![1], 3));
        assert_eq!(end["status"], "ok");
        let (chunks, _, _, end) = read("progress", 0);
        assert_eq!(chunks, [serde_json::json!("start")]);
        assert_eq!(end["closed"], true);
        // No such run.
        assert_eq!(
            fusee_engine_stream_read(e, s("nobody"), -1, s("tokens"), 0, 0, 12),
            FUSEE_NOT_FOUND
        );
        assert_eq!(fusee_engine_finish(e, shard, run, 0, s("3")), FUSEE_OK);
        assert_eq!(fusee_engine_close(e, &mut err), FUSEE_OK);
        fusee_buf_free(&mut err);
    }
}

/// The next update delivered to a run (deciding it when it asks), as
/// `(ordinal, name, id, argument)`; ticks until there is one.
unsafe fn next_update(
    e: *mut fusee_engine,
    shard: u32,
    run: u64,
) -> (u64, String, String, Vec<u8>) {
    unsafe {
        tick_until(e, |_, _| {
            let mut d = no_delivery();
            assert_eq!(fusee_engine_quiescent(e, shard, run, &mut d), FUSEE_OK);
            if d.kind != FUSEE_DELIVERY_UPDATE {
                assert_eq!(d.kind, FUSEE_DELIVERY_NONE);
                return None;
            }
            let name = String::from_utf8_lossy(view(d.name)).into_owned();
            let id = String::from_utf8_lossy(view(d.id)).into_owned();
            let arg = view(d.payload).to_vec();
            let mut ordinal = d.ordinal as i64;
            if d.token >= 0 {
                assert_eq!(
                    fusee_engine_decide_update(e, shard, d.token as u64, 1, EMPTY, &mut ordinal),
                    FUSEE_OK
                );
            }
            assert!(ordinal >= 0);
            Some((ordinal as u64, name, id, arg))
        })
    }
}

/// An update handler returned `result`.
unsafe fn update_done(e: *mut fusee_engine, shard: u32, run: u64, ordinal: u64, result: &str) {
    let mut out = [0u64; 5];
    let rc = unsafe {
        fusee_engine_host(
            e,
            shard,
            run,
            FUSEE_OP_UPDATE_DONE,
            ordinal,
            0,
            0,
            s(result),
            EMPTY,
            EMPTY,
            out.as_mut_ptr(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(rc, FUSEE_OK);
}

/// A run's START event with this workflow ID: `(shard, run)`.
unsafe fn started(e: *mut fusee_engine, id: &str) -> (u32, u64) {
    unsafe {
        tick_until(e, |ev, ex| {
            ev.chunks(FUSEE_EVENT_STRIDE).find_map(|r| {
                (r[0] == FUSEE_EV_START && view(ex[r[5] as usize]) == id.as_bytes())
                    .then_some((r[1] as u32, r[2]))
            })
        })
    }
}

/// A key of an object type through the C interface: a call from outside
/// (update-with-start), state reads and writes, the committed state in
/// `inspect`, listings and the state operation, and calls from a workflow
/// (one whose result resolves as a child's, and a one-way one).
#[test]
fn objects_through_the_c_interface() {
    unsafe {
        let mut e: *mut fusee_engine = std::ptr::null_mut();
        let mut err = fusee_buf {
            ptr: std::ptr::null_mut(),
            len: 0,
            cap: 0,
        };
        assert_eq!(fusee_engine_open(&options(), &mut e, &mut err), FUSEE_OK);
        let shared = [s("get")];
        assert_eq!(
            fusee_engine_register_object(e, s("tally"), shared.as_ptr(), 1),
            FUSEE_OK
        );
        assert_eq!(
            fusee_engine_register_object(e, s("bad/name"), std::ptr::null(), 0),
            FUSEE_REJECTED
        );
        let call = fusee_start {
            id: s("tally/t"),
            wtype: s("tally"),
            input: s("null"),
            version: 1,
            policy: 1, // FUSEE_RUNNING_USE_EXISTING
            deadline_ms: 0,
            token: -1,
            has_update: 1,
            _reserved: 0,
            update_name: s("add"),
            update_id: s("c1"),
            update_arg: s("2"),
            update_token: 5,
        };
        let mut existed = 0;
        assert_eq!(fusee_engine_start(e, call, &mut existed), FUSEE_OK);
        let (shard, run) = started(e, "tally/t");
        let (ordinal, name, id, arg) = next_update(e, shard, run);
        assert_eq!(
            (name.as_str(), id.as_str(), &arg[..]),
            ("add", "c1", &b"2"[..])
        );
        let (o, v) = op(e, shard, run, FUSEE_OP_OBJECT_GET, [1, 0], "sum", &[]);
        assert_eq!((o[0], v.len()), (0, 0));
        op(e, shard, run, FUSEE_OP_OBJECT_SET, [1, 0], "sum", b"2");
        // The handler sees its own write; the committed state has not got it.
        let (o, v) = op(e, shard, run, FUSEE_OP_OBJECT_GET, [1, 0], "sum", &[]);
        assert_eq!((o[0], &v[..]), (1, &b"2"[..]));
        let (o, _) = op(e, shard, run, FUSEE_OP_OBJECT_GET, [0, 0], "sum", &[]);
        assert_eq!(o[0], 0);
        let (o, keys) = op(e, shard, run, FUSEE_OP_OBJECT_KEYS, [1, 0], "", &[]);
        assert_eq!((o[0], &keys[..]), (1, &b"[\"sum\"]"[..]));
        let (_, st) = op(e, shard, run, FUSEE_OP_OBJECT_STATE, [0, 0], "", &[]);
        assert_eq!(st, b"{}");
        update_done(e, shard, run, ordinal, "2");
        let reply = tick_until(e, |ev, ex| {
            ev.chunks(FUSEE_EVENT_STRIDE).find_map(|r| {
                (r[0] == FUSEE_EV_REPLY && r[1] == 5)
                    .then(|| (r[2], view(ex[r[5] as usize]).to_vec()))
            })
        });
        assert_eq!(reply, (0, b"2".to_vec()));
        let (_, st) = op(e, shard, run, FUSEE_OP_OBJECT_STATE, [0, 0], "", &[]);
        assert_eq!(st, b"{\"sum\":2}");
        // inspect shows the committed state; listings mark the key.
        let mut j = EMPTY;
        let mut found = 0;
        assert_eq!(
            fusee_engine_inspect(e, s("tally/t"), &mut j, &mut found),
            FUSEE_OK
        );
        let d: serde_json::Value = serde_json::from_slice(view(j)).unwrap();
        assert_eq!(d["object"]["state"], serde_json::json!({"sum": 2}));
        assert_eq!(d["object"]["running"], serde_json::json!([]));
        assert_eq!(fusee_engine_list(e, EMPTY, 10, &mut j), FUSEE_OK);
        let l: serde_json::Value = serde_json::from_slice(view(j)).unwrap();
        assert_eq!(l[0]["object"], true);
        // A call from a workflow: a journal operation whose seq resolves
        // with the handler's result.
        let req = fusee_start {
            id: s("caller"),
            wtype: s("w"),
            input: s("null"),
            version: 1,
            policy: 0,
            deadline_ms: 0,
            token: 7,
            has_update: 0,
            _reserved: 0,
            update_name: EMPTY,
            update_id: EMPTY,
            update_arg: EMPTY,
            update_token: -1,
        };
        assert_eq!(fusee_engine_start(e, req, &mut existed), FUSEE_OK);
        let (cs, cr) = started(e, "caller");
        assert_eq!(
            fusee_engine_inspect(e, s("caller"), &mut j, &mut found),
            FUSEE_OK
        );
        let d: serde_json::Value = serde_json::from_slice(view(j)).unwrap();
        assert!(d.get("object").is_none(), "{d}");
        let mut out = [0u64; 5];
        let mut begin = |target: &str, one_way: u64| {
            fusee_engine_host(
                e,
                cs,
                cr,
                FUSEE_OP_BEGIN_CALL,
                one_way,
                0,
                0,
                fusee_str::of(target.as_bytes()),
                s("add"),
                s("3"),
                out.as_mut_ptr(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(begin("no-type", 0), FUSEE_INVALID);
        assert_eq!(begin("tally/t", 0), FUSEE_OK);
        assert_eq!(out[1], 1);
        let seq = out[0] as u32;
        let (ordinal, name, _, arg) = next_update(e, shard, run);
        assert_eq!((name.as_str(), &arg[..]), ("add", &b"3"[..]));
        let (_, v) = op(e, shard, run, FUSEE_OP_OBJECT_GET, [1, 0], "sum", &[]);
        assert_eq!(v, b"2");
        op(e, shard, run, FUSEE_OP_OBJECT_SET, [1, 0], "sum", b"5");
        update_done(e, shard, run, ordinal, "5");
        let got = tick_until(e, |_, _| {
            let mut d = no_delivery();
            assert_eq!(fusee_engine_quiescent(e, cs, cr, &mut d), FUSEE_OK);
            (d.kind == FUSEE_DELIVERY_RESOLVE).then(|| (d.seq, d.status, view(d.payload).to_vec()))
        });
        assert_eq!(got, (seq, 0, b"5".to_vec()));
        // A one-way call has no seq to wait for.
        let mut out = [0u64; 5];
        assert_eq!(
            fusee_engine_host(
                e,
                cs,
                cr,
                FUSEE_OP_BEGIN_CALL,
                1,
                0,
                0,
                s("tally/t"),
                s("add"),
                s("1"),
                out.as_mut_ptr(),
                std::ptr::null_mut(),
            ),
            FUSEE_OK
        );
        assert_eq!(out[1], 0);
        assert_eq!(fusee_engine_close(e, &mut err), FUSEE_OK);
        fusee_buf_free(&mut err);
    }
}

#[test]
fn bad_options_report_why() {
    unsafe {
        let mut o = options();
        o.durability = c"bogus".as_ptr();
        let mut e: *mut fusee_engine = std::ptr::null_mut();
        let mut err = fusee_buf {
            ptr: std::ptr::null_mut(),
            len: 0,
            cap: 0,
        };
        assert_eq!(fusee_engine_open(&o, &mut e, &mut err), FUSEE_INVALID);
        assert!(e.is_null());
        let msg = String::from_utf8_lossy(std::slice::from_raw_parts(err.ptr, err.len));
        assert!(msg.contains("bogus"), "{msg}");
        fusee_buf_free(&mut err);
        let mut o = options();
        o.size -= 8;
        assert_eq!(fusee_engine_open(&o, &mut e, &mut err), FUSEE_INVALID);
        fusee_buf_free(&mut err);
    }
}
