//! What a clean close leaves for the next open: a log with one segment
//! and no batch in it, and two checkpoint files of each shard's end, so
//! that one damaged file still loses nothing.

use fusee::core::store::Checkpoint;
use fusee::core::wal::{Progress, WalHeader, WalScanner};
use fusee::{Config, Ctx, Engine, Error};
use std::path::{Path, PathBuf};
use std::time::Duration;

const SHARDS: u32 = 3;
const CHAINS: u32 = 120;
const WAITERS: u32 = 30;

fn tmp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-short-log-{name}-{}-{}",
        std::process::id(),
        fusee::local::unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cfg(dir: &Path) -> Config {
    let mut c = Config::local(dir).shards(SHARDS);
    c.local.flush = fusee::FlushMethod::Fsync;
    c
}

/// Twenty steps with payloads large enough that the runs of one test fill
/// the log's first segments.
fn chain(engine: &Engine) -> fusee::WorkflowRef<u32, u32> {
    engine.register("chain", |ctx: Ctx, n: u32| async move {
        let mut sum = 0;
        for i in 0..20u32 {
            let text = ctx
                .step("s", move || async move {
                    Ok::<_, Error>(format!("{:0>600}", n + i))
                })
                .await?;
            sum += text.parse::<u32>().unwrap_or(0);
        }
        Ok(sum)
    })
}

fn waiter(engine: &Engine) -> fusee::WorkflowRef<u32, u32> {
    engine.register("waiter", |ctx: Ctx, n: u32| async move {
        let add: u32 = ctx.wait_for("go").await?.unwrap_or(0);
        ctx.step("add", move || async move { Ok::<_, Error>(n + add) })
            .await
    })
}

fn chain_sum(n: u32) -> u32 {
    (0..20).map(|i| n + i).sum()
}

/// Chains that finish and waiters that stay open, then a clean close.
async fn fill_and_close(dir: &Path) {
    let engine = Engine::open(cfg(dir)).await.unwrap();
    let (chain, waiter) = (chain(&engine), waiter(&engine));
    for n in 0..WAITERS {
        engine.start(&waiter, &format!("w-{n}"), &n).await.unwrap();
    }
    for n in 0..CHAINS {
        let sum = engine.execute(&chain, &format!("c-{n}"), &n).await.unwrap();
        assert_eq!(sum, chain_sum(n));
    }
    engine.shutdown().await.unwrap();
}

/// Every chain's result is there, and every waiter finishes at its signal.
async fn check_and_finish(dir: &Path, round: &str) {
    let engine = Engine::open(cfg(dir)).await.unwrap();
    let (_chain, _waiter) = (chain(&engine), waiter(&engine));
    for n in 0..CHAINS {
        let sum = engine.run::<u32>(&format!("c-{n}")).result().await;
        assert_eq!(sum.unwrap(), chain_sum(n), "{round}: c-{n}");
    }
    for n in 0..WAITERS {
        let run = engine.run::<u32>(&format!("w-{n}"));
        run.signal("go", &7u32).await.unwrap();
        assert_eq!(run.result().await.unwrap(), n + 7, "{round}: w-{n}");
    }
    engine.shutdown().await.unwrap();
}

/// The log's chain as recovery reads it: `(segments, batches)`.
fn chain_of(dir: &Path) -> (usize, u64) {
    let (mut segments, mut batches) = (0, 0);
    for entry in std::fs::read_dir(dir.join("wal")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|x| x != "wal") {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        if !matches!(WalHeader::decode(&bytes), Ok(Some(_))) {
            // A spare: never activated, or retired.
            continue;
        }
        segments += 1;
        let mut scanner = WalScanner::start(&bytes, true).unwrap().unwrap();
        match scanner.advance(&bytes, true, bytes.len(), &mut |_| {}) {
            Progress::Done(s) => batches += s.batches,
            Progress::NeedMore => panic!("a whole segment was read"),
        }
    }
    (segments, batches)
}

/// A shard's checkpoint files, oldest name first, with what each covers
/// (`None` for one that does not decode).
fn checkpoints(dir: &Path, shard: u32) -> Vec<(PathBuf, Option<u64>)> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir.join(format!("shard-{shard:03}")))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "fck"))
        .collect();
    files.sort();
    files
        .into_iter()
        .map(|p| {
            let lsn = Checkpoint::decode(&std::fs::read(&p).unwrap())
                .ok()
                .map(|c| c.lsn);
            (p, lsn)
        })
        .collect()
}

fn assert_short_and_covered_twice(dir: &Path, when: &str) {
    assert_eq!(
        chain_of(dir),
        (1, 0),
        "{when}: the log's (segments, batches)"
    );
    for shard in 0..SHARDS {
        let cps = checkpoints(dir, shard);
        assert_eq!(cps.len(), 2, "{when}: shard {shard} has {cps:?}");
        assert!(
            cps[0].1.is_some() && cps[0].1 == cps[1].1,
            "{when}: shard {shard}'s two checkpoints cover {cps:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clean_close_leaves_the_log_short_and_every_run_in_place() {
    let dir = tmp("close");
    fill_and_close(&dir).await;
    assert_short_and_covered_twice(&dir, "first close");
    // An open and a close with nothing in between change nothing.
    Engine::open(cfg(&dir))
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    assert_short_and_covered_twice(&dir, "idle reopen");
    check_and_finish(&dir, "second open").await;
    assert_short_and_covered_twice(&dir, "second close");
    // What the second life finished is there in the third.
    let engine = Engine::open(cfg(&dir)).await.unwrap();
    let _defs = (chain(&engine), waiter(&engine));
    for n in 0..WAITERS {
        let r = engine.run::<u32>(&format!("w-{n}")).result().await;
        assert_eq!(r.unwrap(), n + 7, "third open: w-{n}");
    }
    engine.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The log kept nothing after the close, so each shard's state is in its
/// two checkpoint files alone: with either one damaged, the other loads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn either_checkpoint_of_a_clean_close_is_enough() {
    for damaged in 0..2usize {
        let dir = tmp(&format!("damage-{damaged}"));
        fill_and_close(&dir).await;
        assert_short_and_covered_twice(&dir, "close");
        for shard in 0..SHARDS {
            let (path, _) = &checkpoints(&dir, shard)[damaged];
            let mut bytes = std::fs::read(path).unwrap();
            let at = bytes.len() / 2;
            bytes[at] ^= 0x5a;
            std::fs::write(path, bytes).unwrap();
            assert!(checkpoints(&dir, shard)[damaged].1.is_none());
        }
        // An open that changes nothing still closes on two good files.
        Engine::open(cfg(&dir))
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        assert_short_and_covered_twice(&dir, "idle close after damage");
        for shard in 0..SHARDS {
            let (path, _) = &checkpoints(&dir, shard)[damaged];
            std::fs::write(path, b"not a checkpoint").unwrap();
        }
        check_and_finish(&dir, &format!("checkpoint {damaged} damaged")).await;
        // The close after it stands on two good files again.
        assert_short_and_covered_twice(&dir, "close after damage");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A process that dies without closing keeps its log: the next open
/// replays it, and the close after that shortens it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_log_that_was_not_closed_is_replayed_then_shortened() {
    let dir = tmp("unclosed");
    {
        let engine = Engine::open(cfg(&dir)).await.unwrap();
        let (chain, waiter) = (chain(&engine), waiter(&engine));
        for n in 0..WAITERS {
            engine.start(&waiter, &format!("w-{n}"), &n).await.unwrap();
        }
        for n in 0..CHAINS {
            engine.execute(&chain, &format!("c-{n}"), &n).await.unwrap();
        }
        // No shutdown: the directory is copied as a crash would leave it.
        let copy = tmp("unclosed-copy");
        copy_dir(&dir, &copy);
        engine.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::rename(&copy, &dir).unwrap();
    }
    let (_, batches) = chain_of(&dir);
    assert!(batches > 0, "the copy holds the log");
    let _ = std::fs::remove_file(dir.join("fusee.pid"));
    check_and_finish(&dir, "after a crash").await;
    assert_short_and_covered_twice(&dir, "close after a crash");
    let _ = std::fs::remove_dir_all(&dir);
    tokio::time::sleep(Duration::from_millis(1)).await;
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// A shard that only waits while another keeps the log rolling is asked to
/// checkpoint again with nothing new, so that the log can retire the
/// segments holding its records. That checkpoint covers the position the
/// one before it covers: it must be a second file, not a rewrite of the
/// first, since the log then holds none of the shard's records and either
/// file may be the only good one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiting_shard_stands_on_two_checkpoint_files_once_the_log_moved_on() {
    use fusee::core::shard::shard_of;
    let dir = tmp("idle");
    let small = |dir: &Path| {
        let mut c = Config::local(dir)
            .shards(2)
            .park_after(Duration::from_secs(600))
            .checkpoint_interval(Duration::from_millis(20));
        c.local.flush = fusee::FlushMethod::Fsync;
        c.local.segment_bytes = 256 << 10;
        c
    };
    let ids = |shard: u32, n: usize| -> Vec<(u32, String)> {
        (0u32..)
            .map(|k| (k, format!("i-{k}")))
            .filter(|(_, id)| shard_of(id, 2) == shard)
            .take(n)
            .collect()
    };
    let quiet = ids(1, 9);
    let (waiting, finished) = quiet.split_last().unwrap();
    let covered_twice = |dir: &Path| {
        let cps = checkpoints(dir, 1);
        cps.len() == 2 && cps[0].1.is_some() && cps[0].1 == cps[1].1
    };
    let copy = tmp("idle-copy");
    {
        let engine = Engine::open(small(&dir)).await.unwrap();
        let (chain, waiter) = (chain(&engine), waiter(&engine));
        for (n, id) in finished {
            assert_eq!(engine.execute(&chain, id, n).await.unwrap(), chain_sum(*n));
        }
        // A run that stays loaded, waiting: shard 1 records nothing more.
        engine.start(&waiter, &waiting.1, &waiting.0).await.unwrap();
        // Shard 0 fills segment after segment until shard 1 has
        // checkpointed its end a second time.
        for (n, id) in ids(0, 400) {
            engine.execute(&chain, &id, &n).await.unwrap();
            if covered_twice(&dir) {
                break;
            }
        }
        for _ in 0..200 {
            if covered_twice(&dir) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            covered_twice(&dir),
            "shard 1's checkpoints: {:?}",
            checkpoints(&dir, 1)
        );
        // As a crash would leave it.
        copy_dir(&dir, &copy);
        engine.shutdown().await.unwrap();
    }
    let _ = std::fs::remove_file(copy.join("fusee.pid"));
    for damaged in 0..2usize {
        let trial = tmp(&format!("idle-{damaged}"));
        copy_dir(&copy, &trial);
        let (path, _) = &checkpoints(&trial, 1)[damaged];
        std::fs::write(path, b"not a checkpoint").unwrap();
        let engine = Engine::open(small(&trial)).await.unwrap();
        let _defs = (chain(&engine), waiter(&engine));
        for (n, id) in finished {
            let sum = engine.run::<u32>(id).result().await;
            assert_eq!(
                sum.unwrap(),
                chain_sum(*n),
                "checkpoint {damaged} damaged: {id}"
            );
        }
        let run = engine.run::<u32>(&waiting.1);
        run.signal("go", &7u32).await.unwrap();
        assert_eq!(
            run.result().await.unwrap(),
            waiting.0 + 7,
            "checkpoint {damaged} damaged"
        );
        engine.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(&trial);
    }
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&copy);
}
