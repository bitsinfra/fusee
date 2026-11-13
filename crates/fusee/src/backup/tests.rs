//! Unit tests: manifest events, restore plans, retention, merging, the
//! chain walk, the local target and time parsing.

use super::manifest::{
    self, CheckpointEntry, Current, Event, LogChunk, Point, Root, State, StoreChunk, keys,
    merge_plan,
};
use super::ship::{Walk, walk};
use super::target::{LocalTarget, Target};
use fusee_core::wal::{SectionRef, WAL_HEADER_BYTES, WalHeader, WalWriter};

const TL: &str = "t1";

fn chunk(number: u64, offset: u64, end: u64, first: u64, last: u64) -> LogChunk {
    LogChunk {
        key: keys::log(TL, number, offset, end),
        offset,
        end,
        first_seq: first,
        last_seq: last,
        last_checksum: last * 7,
        crc: 0,
        level: 0,
        blobs: Vec::new(),
    }
}

fn cp(shard: u32, lsn: u64, position: u64, usable_from: u64, created_ms: u64) -> CheckpointEntry {
    CheckpointEntry {
        shard,
        key: keys::checkpoint(TL, shard, lsn, created_ms),
        lsn,
        log_position: position,
        usable_from,
        created_ms,
        bytes: 10,
        crc: 0,
        store_generation: 1,
        store_len: 100,
        store_epoch: 0,
    }
}

fn store(shard: u32, offset: u64, end: u64) -> StoreChunk {
    StoreChunk {
        shard,
        generation: 1,
        epoch: 0,
        key: keys::store(TL, shard, 1, 0, offset, end),
        offset,
        end,
        crc: 0,
        level: 0,
    }
}

fn ev_chunk(number: u64, base: u64, c: LogChunk) -> Event {
    Event::Chunk {
        tl: TL.into(),
        number,
        base_seq: base,
        len: 1 << 20,
        chunk: c,
    }
}

/// Two shards; segments 1 (batches 1..=10), 2 (11..=20), 3 (21..=25, open).
fn sample() -> State {
    let mut s = State {
        shards: 2,
        timelines: Vec::new(),
    };
    let events = vec![
        Event::Timeline {
            id: TL.into(),
            created_ms: 0,
        },
        ev_chunk(1, 1, chunk(1, 0, 8192, 1, 5)),
        ev_chunk(1, 1, chunk(1, 8192, 16384, 6, 10)),
        Event::Seal {
            tl: TL.into(),
            number: 1,
        },
        ev_chunk(2, 11, chunk(2, 0, 8192, 11, 20)),
        Event::Seal {
            tl: TL.into(),
            number: 2,
        },
        ev_chunk(3, 21, chunk(3, 0, 8192, 21, 25)),
        Event::Store {
            tl: TL.into(),
            chunk: store(0, 0, 100),
        },
        Event::Store {
            tl: TL.into(),
            chunk: store(1, 0, 100),
        },
        Event::Checkpoint {
            tl: TL.into(),
            entry: cp(0, 4, 3, 6, 1_000),
        },
        Event::Checkpoint {
            tl: TL.into(),
            entry: cp(1, 2, 2, 6, 1_000),
        },
        Event::Checkpoint {
            tl: TL.into(),
            entry: cp(0, 9, 15, 18, 2_000),
        },
        Event::Checkpoint {
            tl: TL.into(),
            entry: cp(1, 8, 14, 18, 2_000),
        },
        Event::Point {
            tl: TL.into(),
            point: Point(1_000, 7),
        },
        Event::Point {
            tl: TL.into(),
            point: Point(2_000, 19),
        },
        Event::Point {
            tl: TL.into(),
            point: Point(3_000, 24),
        },
    ];
    for e in &events {
        s.apply(e).unwrap();
    }
    s
}

#[test]
fn events_must_continue_what_they_extend() {
    let mut s = sample();
    // A gap in a segment, a chunk out of sequence, a sealed segment.
    assert!(
        s.apply(&ev_chunk(3, 21, chunk(3, 9000, 9100, 26, 26)))
            .is_err()
    );
    assert!(
        s.apply(&ev_chunk(3, 21, chunk(3, 8192, 9000, 27, 28)))
            .is_err()
    );
    assert!(
        s.apply(&ev_chunk(2, 11, chunk(2, 8192, 9000, 21, 21)))
            .is_err()
    );
    // An older segment after a newer one.
    assert!(s.apply(&ev_chunk(1, 1, chunk(1, 0, 10, 1, 1))).is_err());
    assert!(
        s.apply(&Event::Point {
            tl: TL.into(),
            point: Point(2_500, 30),
        })
        .is_err(),
        "time goes backwards"
    );
    assert!(
        s.apply(&Event::Store {
            tl: TL.into(),
            chunk: store(0, 50, 150),
        })
        .is_err()
    );
    s.apply(&ev_chunk(3, 21, chunk(3, 8192, 9000, 26, 27)))
        .unwrap();
    assert_eq!(s.timelines[0].uploaded_through(), 27);
}

#[test]
fn merges_replace_exactly_the_chunks_they_cover() {
    let mut s = sample();
    let mut m = chunk(1, 0, 16384, 1, 10);
    m.level = 1;
    s.apply(&Event::MergeLog {
        tl: TL.into(),
        number: 1,
        chunk: m.clone(),
    })
    .unwrap();
    assert_eq!(s.timelines[0].segments[0].chunks, vec![m]);
    // Not on chunk boundaries.
    assert!(
        s.apply(&Event::MergeLog {
            tl: TL.into(),
            number: 2,
            chunk: chunk(2, 0, 4000, 11, 12),
        })
        .is_err()
    );
    s.apply(&Event::Store {
        tl: TL.into(),
        chunk: store(0, 100, 200),
    })
    .unwrap();
    let mut ms = store(0, 0, 200);
    ms.level = 1;
    s.apply(&Event::MergeStore {
        tl: TL.into(),
        chunk: ms.clone(),
    })
    .unwrap();
    let mine: Vec<_> = s.timelines[0]
        .stores
        .iter()
        .filter(|c| c.shard == 0)
        .collect();
    assert_eq!(mine, vec![&ms]);
}

#[test]
fn merge_plan_merges_eight_of_a_level() {
    let level0: Vec<(u64, u64, u32)> = (0..8).map(|i| (i * 10, i * 10 + 10, 0)).collect();
    assert_eq!(merge_plan(&level0[..7]), None);
    assert_eq!(merge_plan(&level0), Some(0..8));
    let mut mixed = vec![(0, 80, 1)];
    mixed.extend((8..15).map(|i| (i * 10, i * 10 + 10, 0)));
    assert_eq!(merge_plan(&mixed), None);
    let big: Vec<(u64, u64, u32)> = (0..8).map(|i| (i << 22, (i + 1) << 22, 0)).collect();
    assert_eq!(
        merge_plan(&big),
        None,
        "a merge stays within the object size"
    );
}

#[test]
fn a_plan_takes_the_newest_usable_checkpoints_and_the_log_after_them() {
    let s = sample();
    let t = &s.timelines[0];
    // At batch 7 only the first checkpoints are usable (usable from 6).
    let p = t.plan(2, Point(1_000, 7)).unwrap();
    assert_eq!(
        p.shards
            .iter()
            .map(|s| s.checkpoint.as_ref().unwrap().lsn)
            .collect::<Vec<_>>(),
        vec![4, 2]
    );
    // The log from the oldest position (2) through the point: segment 1,
    // both chunks, cut at 7.
    assert_eq!(p.segments.len(), 1);
    assert_eq!(p.segments[0].chunks.len(), 2);
    assert_eq!(p.segments[0].cut, Some(7));
    // At 24 the newer ones are; positions 14 and 15 start in segment 2.
    let p = t.plan(2, Point(3_000, 24)).unwrap();
    assert_eq!(p.shards[0].checkpoint.as_ref().unwrap().lsn, 9);
    assert_eq!(
        p.segments.iter().map(|s| s.number).collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(p.segments[1].cut, Some(24));
    assert_eq!(p.shards[0].store.len(), 1);
    // Beyond what is uploaded.
    assert!(t.plan(2, Point(4_000, 26)).is_err());
}

#[test]
fn a_shard_without_a_checkpoint_needs_the_log_from_its_start() {
    let mut s = sample();
    s.timelines[0].checkpoints.retain(|c| c.shard == 0);
    let t = &s.timelines[0];
    let p = t.plan(2, Point(3_000, 24)).unwrap();
    assert!(p.shards[1].checkpoint.is_none());
    assert_eq!(
        p.segments.iter().map(|s| s.number).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    // Without segment 1 the log has no start for shard 1.
    s.timelines[0].segments.remove(0);
    assert!(s.timelines[0].plan(2, Point(3_000, 24)).is_err());
}

#[test]
fn a_gap_in_the_log_needs_checkpoints_after_it() {
    let mut s = sample();
    // Segment 2 never made it: batches 11..=20 are missing.
    s.timelines[0].segments.remove(1);
    let t = &s.timelines[0];
    let err = t.plan(2, Point(3_000, 24)).unwrap_err();
    assert!(
        err.contains("not in the backup") || err.contains("checkpoint"),
        "{err}"
    );
    assert!(t.plan(2, Point(1_000, 7)).is_ok());
}

#[test]
fn a_store_upload_must_cover_the_prefix_a_checkpoint_names() {
    let mut s = sample();
    s.timelines[0].checkpoints[0].store_len = 150;
    let err = s.timelines[0].plan(2, Point(1_000, 7)).unwrap_err();
    assert!(err.contains("run store"), "{err}");
}

#[test]
fn choose_picks_the_newest_point_at_or_before_the_time() {
    let s = sample();
    assert_eq!(
        manifest::choose(&s, Some(2_500), None).unwrap().point,
        Point(2_000, 19)
    );
    assert_eq!(
        manifest::choose(&s, Some(2_000), None).unwrap().point,
        Point(2_000, 19)
    );
    assert_eq!(
        manifest::choose(&s, None, None).unwrap().point,
        Point(3_000, 24)
    );
    assert!(manifest::choose(&s, Some(999), None).is_err());
    assert!(manifest::choose(&s, None, Some("nope")).is_err());
}

#[test]
fn retention_drops_expired_points_and_what_only_they_needed() {
    let mut s = sample();
    let stores = [(0u32, 1u64, 0u64), (1, 1, 0)];
    // Window of 1.5 s at time 3.4 s: the point at 1 s expires, and with it
    // the first checkpoints and segment 1.
    let removed = s.retain(
        3_400,
        1_500,
        Some(Current {
            timeline: TL,
            uploaded: 25,
            stores: &stores,
        }),
    );
    let t = &s.timelines[0];
    assert_eq!(t.points, vec![Point(2_000, 19), Point(3_000, 24)]);
    assert_eq!(
        t.segments.iter().map(|s| s.number).collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert!(t.checkpoints.iter().all(|c| c.lsn >= 8));
    assert!(
        removed
            .iter()
            .any(|k| k.contains("/log/00000000000000000001/"))
    );
    assert!(removed.contains(&keys::checkpoint(TL, 0, 4, 1_000)));
    // Every remaining point still restores.
    for p in &t.points {
        t.plan(2, *p).unwrap();
    }
    // Nothing removed is named any more.
    let named = s.keys();
    assert!(removed.iter().all(|k| !named.contains(k)));
}

#[test]
fn retention_keeps_what_the_next_point_needs_on_the_current_timeline() {
    let mut s = sample();
    let stores = [(0u32, 1u64, 0u64), (1, 1, 0)];
    // Every point expired, but the shipper is about to record one at 25.
    s.retain(
        100_000,
        1_000,
        Some(Current {
            timeline: TL,
            uploaded: 25,
            stores: &stores,
        }),
    );
    let t = &s.timelines[0];
    assert!(t.points.is_empty());
    assert!(t.plan(2, Point(100_000, 25)).is_ok());
    // Another timeline in the same state with nothing left goes entirely.
    let mut s = sample();
    let removed = s.retain(100_000, 1_000, None);
    assert!(s.timelines.is_empty());
    assert_eq!(removed.len(), sample().keys().len());
}

#[test]
fn old_points_are_thinned_to_one_a_minute_and_reuse_checkpoints() {
    let mut s = State {
        shards: 1,
        timelines: Vec::new(),
    };
    s.apply(&Event::Timeline {
        id: TL.into(),
        created_ms: 0,
    })
    .unwrap();
    s.apply(&Event::Store {
        tl: TL.into(),
        chunk: store(0, 0, 100),
    })
    .unwrap();
    // One batch and one point a second for two hours; a checkpoint every
    // 30 seconds.
    let hours = 2 * 3600;
    let mut offset = 0;
    for i in 1..=hours as u64 {
        let ms = i * 1000;
        s.apply(&ev_chunk(1, 1, chunk(1, offset, offset + 64, i, i)))
            .unwrap();
        offset += 64;
        if i % 30 == 0 {
            let mut c = cp(0, i, i, i, ms);
            c.store_len = 100;
            s.apply(&Event::Checkpoint {
                tl: TL.into(),
                entry: c,
            })
            .unwrap();
        }
        s.apply(&Event::Point {
            tl: TL.into(),
            point: Point(ms, i),
        })
        .unwrap();
    }
    let now = hours as u64 * 1000;
    let stores = [(0u32, 1u64, 0u64)];
    s.retain(
        now,
        24 * 3_600_000,
        Some(Current {
            timeline: TL,
            uploaded: hours as u64,
            stores: &stores,
        }),
    );
    let t = &s.timelines[0];
    let fine = t
        .points
        .iter()
        .filter(|p| p.0 + manifest::FINE_MS >= now)
        .count();
    let coarse = t.points.len() - fine;
    assert!((599..=601).contains(&fine), "{fine}");
    // Two hours less ten minutes, one a minute.
    assert!((108..=111).contains(&coarse), "{coarse}");
    // Points older than an hour share a checkpoint per hour; newer ones
    // take the newest usable (one per 30 s for the last hour).
    let old_cps = t
        .checkpoints
        .iter()
        .filter(|c| c.created_ms + manifest::COARSE_MS < now)
        .count();
    assert!(old_cps <= 3, "{old_cps}");
    for p in &t.points {
        t.plan(1, *p).unwrap();
    }
}

#[test]
fn the_walk_follows_the_chain_and_stops_at_what_is_not_whole() {
    let salt = 0x5a17;
    let header = WalHeader {
        group: 0,
        segment: 1,
        salt,
        base_seq: 1,
        base_prev: 0,
        created_ms: 0,
    };
    let mut image = header.encode();
    let mut w = WalWriter::resume(0, 9, 1, salt, 1 << 20, WAL_HEADER_BYTES, 1, 0);
    let records = vec![1u8, 2, 3, 4];
    let mut ends = Vec::new();
    for i in 0..5u64 {
        let s = [SectionRef {
            shard: 0,
            first_lsn: i + 1,
            count: 1,
            records: &records,
        }];
        let placed = w.place(&s, i, i % 2 == 0, 0, || unreachable!());
        assert_eq!(placed.offset, image.len() as u64);
        image.extend_from_slice(&placed.bytes);
        ends.push(image.len() as u64);
    }
    let start = Walk {
        end: WAL_HEADER_BYTES,
        seq: 0,
        checksum: 0,
    };
    let all = walk(&image, 0, salt, start, 5, u64::MAX).unwrap();
    assert_eq!((all.seq, all.end), (5, ends[4]));
    let three = walk(&image, 0, salt, start, 3, u64::MAX).unwrap();
    assert_eq!((three.seq, three.end), (3, ends[2]));
    // Resumes from where it stopped, over bytes from that offset on.
    let rest = walk(
        &image[three.end as usize..],
        three.end,
        salt,
        three,
        5,
        u64::MAX,
    )
    .unwrap();
    assert_eq!(rest, all);
    // A batch cut short: stops before it, without an error.
    let cut = walk(&image[..ends[3] as usize - 8], 0, salt, start, 5, u64::MAX).unwrap();
    assert_eq!(cut.seq, 3);
    // Another salt (a recycled file's earlier life), a flipped byte, or a
    // wrong predecessor: an error.
    assert!(walk(&image, 0, salt + 1, start, 5, u64::MAX).is_err());
    let mut bad = image.clone();
    bad[ends[1] as usize + 60] ^= 1;
    assert!(walk(&bad, 0, salt, start, 5, u64::MAX).is_err());
    let wrong = Walk {
        checksum: 1,
        ..start
    };
    assert!(walk(&image, 0, salt, wrong, 5, u64::MAX).is_err());
    // Stops at an offset limit after the batch that crosses it.
    let limited = walk(&image, 0, salt, start, 5, ends[0] + 1).unwrap();
    assert_eq!(limited.seq, 2);
}

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "fusee-backup-unit-{name}-{}-{}",
        std::process::id(),
        crate::local::unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&d);
    d
}

#[test]
fn the_local_target_stores_objects_and_replaces_the_manifest() {
    let dir = tmp("target");
    let t = LocalTarget::new(&dir).unwrap();
    assert!(t.get_manifest().unwrap().is_none());
    t.put("a/b/one.wal", b"1").unwrap();
    t.put("a/two.dat", b"22").unwrap();
    assert_eq!(t.get("a/b/one.wal").unwrap().unwrap(), b"1");
    assert!(t.get("a/missing").unwrap().is_none());
    for bad in [
        "",
        "/abs",
        "a/../b",
        "a//b",
        "x.tmp",
        "manifest.json",
        "sp ace",
    ] {
        assert!(t.put(bad, b"x").is_err(), "{bad}");
    }
    t.put_manifest(b"{}").unwrap();
    t.put_manifest(b"{\"v\":2}").unwrap();
    assert_eq!(t.get_manifest().unwrap().unwrap(), b"{\"v\":2}");
    // Debris of an interrupted write is not an object.
    std::fs::write(dir.join("a/b/three.wal.tmp"), b"partial").unwrap();
    assert_eq!(t.list("").unwrap(), vec!["a/b/one.wal", "a/two.dat"]);
    assert_eq!(t.list("a/b").unwrap(), vec!["a/b/one.wal"]);
    t.cleanup().unwrap();
    assert!(!dir.join("a/b/three.wal.tmp").exists());
    t.delete("a/two.dat").unwrap();
    t.delete("a/two.dat").unwrap();
    assert_eq!(t.list("").unwrap(), vec!["a/b/one.wal"]);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_snapshot_and_its_events_load_back_to_the_same_state() {
    let dir = tmp("load");
    let target = LocalTarget::new(&dir).unwrap();
    let full = sample();
    // Snapshot after the first eight events, the rest in the root.
    let mut first = State {
        shards: 2,
        timelines: Vec::new(),
    };
    let events: Vec<Event> = {
        // Re-derive the sample's events by replaying it into a log of
        // events: the sample is built from them in order.
        let mut v = Vec::new();
        v.push(Event::Timeline {
            id: TL.into(),
            created_ms: 0,
        });
        for seg in &full.timelines[0].segments {
            for c in &seg.chunks {
                v.push(Event::Chunk {
                    tl: TL.into(),
                    number: seg.number,
                    base_seq: seg.base_seq,
                    len: seg.len,
                    chunk: c.clone(),
                });
            }
            if seg.sealed {
                v.push(Event::Seal {
                    tl: TL.into(),
                    number: seg.number,
                });
            }
        }
        for c in &full.timelines[0].stores {
            v.push(Event::Store {
                tl: TL.into(),
                chunk: c.clone(),
            });
        }
        for c in &full.timelines[0].checkpoints {
            v.push(Event::Checkpoint {
                tl: TL.into(),
                entry: c.clone(),
            });
        }
        for p in &full.timelines[0].points {
            v.push(Event::Point {
                tl: TL.into(),
                point: *p,
            });
        }
        v
    };
    for e in &events[..8] {
        first.apply(e).unwrap();
    }
    let snap = keys::snapshot(3);
    target
        .put(
            &snap,
            &serde_json::to_vec(&manifest::Snapshot {
                format: manifest::FORMAT,
                version: 3,
                state: first,
            })
            .unwrap(),
        )
        .unwrap();
    let root = Root {
        format: manifest::FORMAT,
        shards: 2,
        version: 4,
        updated_ms: 0,
        snapshot: Some(snap),
        events: events[8..].to_vec(),
    };
    target
        .put_manifest(&serde_json::to_vec(&root).unwrap())
        .unwrap();
    let (r, loaded) = super::load_state(&target).unwrap().unwrap();
    assert_eq!(r.version, 4);
    assert_eq!(loaded, full);
    // A manifest from a newer format is refused.
    let mut newer = root.clone();
    newer.format = manifest::FORMAT + 1;
    target
        .put_manifest(&serde_json::to_vec(&newer).unwrap())
        .unwrap();
    let err = super::load_state(&target).unwrap_err().to_string();
    assert!(err.contains("format"), "{err}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn times_parse_as_rfc3339_milliseconds_or_before_now() {
    let now = 1_762_689_600_000;
    assert_eq!(super::parse_time("2025-11-09T12:00:00Z", now).unwrap(), now);
    assert_eq!(super::parse_time("1762689600000", now).unwrap(), now);
    assert_eq!(super::parse_time("-15m", now).unwrap(), now - 900_000);
    assert_eq!(super::parse_time("-2h", now).unwrap(), now - 7_200_000);
    assert_eq!(super::parse_time("-90s", now).unwrap(), now - 90_000);
    assert!(super::parse_time("-3w", now).is_err());
    assert!(super::parse_time("yesterday", now).is_err());
    assert_eq!(super::format_time(now), "2025-11-09T12:00:00.000Z");
}

/// A manifest's archive names are plain file names: restore refuses a path.
#[test]
fn restore_takes_only_plain_archive_names() {
    use super::restore::archive_file_name;
    let real = "00000000000000000042-00000000000000000007-0a1b2c3d-4096.arc";
    assert_eq!(archive_file_name(real).unwrap(), real);
    for bad in [
        "../x.arc",
        "a/b.arc",
        "/etc/x.arc",
        ".arc",
        "x.dat",
        "",
        "x\\y.arc",
    ] {
        assert!(archive_file_name(bad).is_err(), "{bad:?}");
    }
}
