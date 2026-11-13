//! Continuous backup end to end: workflows run while the backup ships the
//! log and checkpoints, the directory is deleted, and restores to several
//! points in time each hold exactly what was acknowledged by then, with
//! suspended runs resuming.

use fusee::backup::{self, BackupOptions, LocalTarget, RestoreOptions, Target};
use fusee::{Config, Ctx, Engine, Error};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

fn tmp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fusee-backup-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn options() -> BackupOptions {
    BackupOptions {
        upload_interval: Some(Duration::from_millis(50)),
        resolution: Duration::from_millis(50),
        retention: Duration::from_secs(3600),
    }
}

fn cfg(dir: &Path) -> Config {
    let mut c = Config::local(dir)
        .shards(2)
        // Small segments: the log rolls, seals and is retired while the
        // backup runs; frequent checkpoints and parking exercise the store.
        .segment_bytes(256 << 10)
        .checkpoint_interval(Duration::from_millis(100))
        .park_after(Duration::from_millis(20));
    c.local.flush = fusee::FlushMethod::Fsync;
    c
}

fn backed_up(dir: &Path, target: &Arc<dyn Target>) -> Config {
    cfg(dir).backup(target.clone(), options())
}

/// Registers the test's workflow types.
fn register(engine: &Engine) {
    engine.register("work", |ctx: Ctx, n: u32| async move {
        let mut acc = 0u64;
        for i in 0..3u32 {
            let page: Vec<u8> = ctx
                .step(&format!("page-{i}"), move || async move {
                    Ok::<_, Error>(vec![(n % 251) as u8; 2048])
                })
                .await?;
            acc += page.len() as u64;
        }
        Ok(u64::from(n) * 2 + acc)
    });
    engine.register("waiter", |ctx: Ctx, base: u32| async move {
        let v: u32 = ctx.wait_for("go").await?.unwrap_or(0);
        Ok(base + v)
    });
}

fn expected(n: u32) -> u64 {
    u64::from(n) * 2 + 3 * 2048
}

async fn phase(engine: &Engine, prefix: &str, count: u32) {
    for i in 0..count {
        engine
            .start_raw(
                "work",
                &format!("{prefix}-{i}"),
                serde_json::to_vec(&i).unwrap().into(),
                Default::default(),
            )
            .await
            .unwrap();
    }
    for i in 0..count {
        let id = format!("{prefix}-{i}");
        assert_eq!(engine.run::<u64>(&id).result().await.unwrap(), expected(i));
    }
}

async fn start_waiter(engine: &Engine, id: &str, base: u32) {
    engine
        .start_raw(
            "waiter",
            id,
            serde_json::to_vec(&base).unwrap().into(),
            Default::default(),
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restores_each_point_in_time_with_exactly_what_was_acknowledged() {
    let dir = tmp("dir");
    let store = tmp("target");
    let target: Arc<dyn Target> = Arc::new(LocalTarget::new(&store).unwrap());
    let engine = Engine::open(backed_up(&dir, &target)).await.unwrap();
    assert!(
        target.get_manifest().unwrap().is_some(),
        "a new backup must be recognizable before its first upload"
    );
    register(&engine);
    let b = engine.backup().expect("backup");

    phase(&engine, "a", 40).await;
    start_waiter(&engine, "wa", 100).await;
    let p1 = b.sync().await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    phase(&engine, "b", 40).await;
    start_waiter(&engine, "wb", 200).await;
    let p2 = b.sync().await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    phase(&engine, "c", 40).await;
    let p3 = b.sync().await.unwrap();
    assert!(
        p1.time_ms < p2.time_ms && p2.time_ms < p3.time_ms,
        "{p1:?} {p2:?} {p3:?}"
    );
    let status = b.status().unwrap();
    assert!(
        status.objects_uploaded > 0 && status.manifest_writes > 0,
        "{status:?}"
    );
    engine.shutdown().await.unwrap();
    // The disk is gone.
    std::fs::remove_dir_all(&dir).unwrap();

    let checked = backup::verify(&*target).unwrap();
    assert!(
        checked.missing.is_empty() && checked.damaged.is_empty(),
        "{checked:?}"
    );

    // (restore at, prefixes present, waiters present)
    let cases: Vec<(Option<u64>, Vec<&str>, Vec<(&str, u32)>, Option<u64>)> = vec![
        (Some(p1.time_ms), vec!["a"], vec![("wa", 100)], Some(p1.seq)),
        // After p1 and before the next phase began: still p1.
        (
            Some(p1.time_ms + 10),
            vec!["a"],
            vec![("wa", 100)],
            Some(p1.seq),
        ),
        (
            Some(p2.time_ms),
            vec!["a", "b"],
            vec![("wa", 100), ("wb", 200)],
            Some(p2.seq),
        ),
        (
            Some(p3.time_ms),
            vec!["a", "b", "c"],
            vec![("wa", 100), ("wb", 200)],
            None,
        ),
        (
            None,
            vec!["a", "b", "c"],
            vec![("wa", 100), ("wb", 200)],
            None,
        ),
    ];
    for (k, (at, present, waiters, seq)) in cases.into_iter().enumerate() {
        let out = tmp(&format!("restored-{k}"));
        let r = backup::restore(
            &*target,
            &out,
            &RestoreOptions {
                at_ms: at,
                timeline: None,
            },
        )
        .unwrap();
        if let Some(seq) = seq {
            assert_eq!(r.seq, seq, "case {k}: {r:?}");
        }
        let engine = Engine::open(cfg(&out)).await.unwrap();
        register(&engine);
        for prefix in ["a", "b", "c"] {
            for i in 0..40 {
                let id = format!("{prefix}-{i}");
                let status = engine.status(&id).await.unwrap();
                if present.contains(&prefix) {
                    assert_eq!(
                        engine.run::<u64>(&id).result().await.unwrap(),
                        expected(i),
                        "case {k}: {id}"
                    );
                } else {
                    assert!(
                        status.is_none(),
                        "case {k}: {id} should not exist: {status:?}"
                    );
                }
            }
        }
        for w in ["wa", "wb"] {
            let want = waiters.iter().find(|(id, _)| *id == w);
            match want {
                Some((_, base)) => {
                    // Suspended at the restore point; resumes now.
                    engine.run::<u32>(w).signal("go", &5u32).await.unwrap();
                    assert_eq!(engine.run::<u32>(w).result().await.unwrap(), base + 5);
                }
                None => assert!(engine.status(w).await.unwrap().is_none(), "case {k}: {w}"),
            }
        }
        engine.shutdown().await.unwrap();
        std::fs::remove_dir_all(&out).unwrap();
    }
    std::fs::remove_dir_all(&store).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backup_continues_across_restarts_and_a_restored_copy_starts_a_new_timeline() {
    let dir = tmp("restart");
    let store = tmp("restart-target");
    let target: Arc<dyn Target> = Arc::new(LocalTarget::new(&store).unwrap());
    let mut first_round = None;
    for (round, prefix) in ["a", "b", "c"].iter().enumerate() {
        let engine = Engine::open(backed_up(&dir, &target)).await.unwrap();
        register(&engine);
        phase(&engine, prefix, 20).await;
        if round == 0 {
            start_waiter(&engine, "w", 1).await;
        }
        let p = engine.backup().unwrap().sync().await.unwrap();
        first_round.get_or_insert(p);
        engine.shutdown().await.unwrap();
    }
    let (_, state) = backup::load_state(&*target).unwrap().unwrap();
    assert_eq!(state.timelines.len(), 1, "one history across restarts");
    let first_round = first_round.unwrap();

    // Restore the newest point: everything, and it continues the same
    // timeline when backed up again.
    let copy = tmp("restart-copy");
    backup::restore(&*target, &copy, &RestoreOptions::default()).unwrap();
    let engine = Engine::open(backed_up(&copy, &target)).await.unwrap();
    register(&engine);
    for prefix in ["a", "b", "c"] {
        for i in 0..20 {
            assert_eq!(
                engine
                    .run::<u64>(&format!("{prefix}-{i}"))
                    .result()
                    .await
                    .unwrap(),
                expected(i)
            );
        }
    }
    phase(&engine, "d", 5).await;
    engine.backup().unwrap().sync().await.unwrap();
    engine.shutdown().await.unwrap();
    let (_, state) = backup::load_state(&*target).unwrap().unwrap();
    assert_eq!(
        state.timelines.len(),
        1,
        "the newest point continues its timeline"
    );

    // Restore an older point and back that up: its future differs, so it
    // is a new timeline, and the old one stays restorable.
    let older = tmp("restart-older");
    backup::restore(
        &*target,
        &older,
        &RestoreOptions {
            at_ms: Some(first_round.time_ms),
            timeline: None,
        },
    )
    .unwrap();
    let engine = Engine::open(backed_up(&older, &target)).await.unwrap();
    register(&engine);
    assert!(engine.status("b-0").await.unwrap().is_none());
    phase(&engine, "e", 5).await;
    engine.run::<u32>("w").signal("go", &1u32).await.unwrap();
    assert_eq!(engine.run::<u32>("w").result().await.unwrap(), 2);
    engine.backup().unwrap().sync().await.unwrap();
    engine.shutdown().await.unwrap();
    let (_, state) = backup::load_state(&*target).unwrap().unwrap();
    assert_eq!(
        state.timelines.len(),
        2,
        "{:?}",
        state.timelines.iter().map(|t| &t.id).collect::<Vec<_>>()
    );
    let checked = backup::verify(&*target).unwrap();
    assert!(
        checked.missing.is_empty() && checked.damaged.is_empty(),
        "{checked:?}"
    );
    assert!(checked.timelines.iter().all(|t| t.2 > 0), "{checked:?}");

    // The newest point is the new timeline's.
    let latest = tmp("restart-latest");
    backup::restore(&*target, &latest, &RestoreOptions::default()).unwrap();
    let engine = Engine::open(cfg(&latest)).await.unwrap();
    register(&engine);
    assert!(engine.status("e-0").await.unwrap().is_some());
    assert!(engine.status("d-0").await.unwrap().is_none());
    engine.shutdown().await.unwrap();

    for d in [dir, copy, older, latest, store] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restore_refuses_a_directory_in_use_and_finishes_after_an_interrupted_attempt() {
    let dir = tmp("refuse");
    let store = tmp("refuse-target");
    let target: Arc<dyn Target> = Arc::new(LocalTarget::new(&store).unwrap());
    let engine = Engine::open(backed_up(&dir, &target)).await.unwrap();
    register(&engine);
    phase(&engine, "a", 5).await;
    engine.backup().unwrap().sync().await.unwrap();
    engine.shutdown().await.unwrap();
    let err = backup::restore(&*target, &dir, &RestoreOptions::default()).unwrap_err();
    assert!(err.to_string().contains("not empty"), "{err}");
    // An earlier attempt died halfway: its staging directory is replaced.
    let out = tmp("refuse-out");
    let staging = out.parent().unwrap().join(format!(
        ".{}.restoring",
        out.file_name().unwrap().to_string_lossy()
    ));
    std::fs::create_dir_all(staging.join("wal")).unwrap();
    std::fs::write(staging.join("wal/junk"), b"x").unwrap();
    backup::restore(&*target, &out, &RestoreOptions::default()).unwrap();
    assert!(!staging.exists());
    let engine = Engine::open(cfg(&out)).await.unwrap();
    register(&engine);
    assert_eq!(
        engine.run::<u64>("a-3").result().await.unwrap(),
        expected(3)
    );
    engine.shutdown().await.unwrap();
    for d in [dir, out, store] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// An S3 bucket in memory behind the S3 target's transport: the backup
/// and restore go through request signing, addressing, listing pages and
/// error handling as they would against S3, without a network.
#[cfg(feature = "s3")]
mod s3 {
    use super::*;
    use fusee::backup::s3::{Credentials, Request, Response, S3Options, S3Target, Transport};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Bucket {
        objects: Mutex<BTreeMap<String, Vec<u8>>>,
        requests: Mutex<usize>,
    }

    fn decode(s: &str) -> String {
        let b = s.as_bytes();
        let mut out = Vec::new();
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'%' {
                out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
                i += 3;
            } else {
                out.push(b[i]);
                i += 1;
            }
        }
        String::from_utf8(out).unwrap()
    }

    struct Shared(Arc<Bucket>);

    impl Transport for Shared {
        fn send(&self, req: &Request) -> std::io::Result<Response> {
            let this = &self.0;
            *this.requests.lock().unwrap() += 1;
            let auth = req.headers.iter().find(|(k, _)| k == "authorization");
            assert!(
                auth.is_some_and(|(_, v)| v.starts_with("AWS4-HMAC-SHA256 Credential=AK/")),
                "unsigned request {req:?}"
            );
            let key = decode(req.path.strip_prefix("/bucket/").unwrap_or(""));
            let mut objects = this.objects.lock().unwrap();
            let ok = |body: Vec<u8>| Response {
                status: 200,
                headers: Vec::new(),
                body,
            };
            Ok(match req.method.as_str() {
                "PUT" => {
                    objects.insert(key, req.body.clone());
                    ok(Vec::new())
                }
                "DELETE" => {
                    objects.remove(&key);
                    Response {
                        status: 204,
                        ..Default::default()
                    }
                }
                "GET" if req.path == "/bucket/" => {
                    let q = |name: &str| {
                        req.query
                            .iter()
                            .find(|(k, _)| k == name)
                            .map(|(_, v)| v.clone())
                    };
                    let prefix = q("prefix").unwrap_or_default();
                    let after = q("continuation-token").unwrap_or_default();
                    // Pages of three keys, to exercise continuation.
                    let keys: Vec<&String> = objects
                        .keys()
                        .filter(|k| k.starts_with(&prefix) && **k > after)
                        .collect();
                    let page = &keys[..keys.len().min(3)];
                    let mut xml = format!(
                        "<ListBucketResult><IsTruncated>{}</IsTruncated>",
                        keys.len() > 3
                    );
                    for k in page {
                        xml.push_str(&format!("<Contents><Key>{k}</Key></Contents>"));
                    }
                    if keys.len() > 3 {
                        xml.push_str(&format!(
                            "<NextContinuationToken>{}</NextContinuationToken>",
                            page[2]
                        ));
                    }
                    xml.push_str("</ListBucketResult>");
                    ok(xml.into_bytes())
                }
                "GET" => match objects.get(&key) {
                    Some(b) => ok(b.clone()),
                    None => Response {
                        status: 404,
                        body: b"<Error><Code>NoSuchKey</Code></Error>".to_vec(),
                        ..Default::default()
                    },
                },
                m => panic!("unexpected {m}"),
            })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_backup_to_s3_restores() {
        let bucket = Arc::new(Bucket::default());
        let target: Arc<dyn Target> = Arc::new(
            S3Target::with_transport(
                S3Options {
                    bucket: "bucket".into(),
                    prefix: "fusee/test".into(),
                    region: "us-east-1".into(),
                    endpoint: Some("http://127.0.0.1:9".into()),
                    path_style: true,
                    credentials: Credentials {
                        access_key: "AK".into(),
                        secret_key: "SK".into(),
                        session_token: None,
                    },
                    ca_file: None,
                    timeout: Duration::from_secs(1),
                },
                Box::new(Shared(bucket.clone())),
                Box::new(std::time::SystemTime::now),
            )
            .unwrap(),
        );
        let dir = tmp("s3");
        let engine = Engine::open(backed_up(&dir, &target)).await.unwrap();
        register(&engine);
        phase(&engine, "a", 30).await;
        start_waiter(&engine, "w", 3).await;
        let p = engine.backup().unwrap().sync().await.unwrap();
        phase(&engine, "b", 10).await;
        engine.shutdown().await.unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(
            bucket
                .objects
                .lock()
                .unwrap()
                .contains_key("fusee/test/manifest.json")
        );
        let checked = backup::verify(&*target).unwrap();
        assert!(checked.missing.is_empty() && checked.damaged.is_empty());
        for (at, b_present) in [(Some(p.time_ms), false), (None, true)] {
            let out = tmp("s3-out");
            backup::restore(
                &*target,
                &out,
                &RestoreOptions {
                    at_ms: at,
                    timeline: None,
                },
            )
            .unwrap();
            let engine = Engine::open(cfg(&out)).await.unwrap();
            register(&engine);
            assert_eq!(
                engine.run::<u64>("a-29").result().await.unwrap(),
                expected(29)
            );
            assert_eq!(engine.status("b-0").await.unwrap().is_some(), b_present);
            engine.run::<u32>("w").signal("go", &1u32).await.unwrap();
            assert_eq!(engine.run::<u32>("w").result().await.unwrap(), 4);
            engine.shutdown().await.unwrap();
            std::fs::remove_dir_all(&out).unwrap();
        }
        assert!(*bucket.requests.lock().unwrap() > 10);
    }
}

/// Offloaded records (results and signals over the payload offload
/// threshold) are shipped with the log chunks that reference them, and a
/// restore that replays the log reads them back whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_payloads_in_the_blob_area_are_backed_up_and_restored() {
    let dir = tmp("blobs-dir");
    let store = tmp("blobs-target");
    let target: Arc<dyn Target> = Arc::new(LocalTarget::new(&store).unwrap());
    // No checkpoint before the restore point: the restore replays the whole
    // log, references and all.
    let config = backed_up(&dir, &target).checkpoint_interval(Duration::from_secs(3600));
    let engine = open_big(config).await;
    for n in 0..4u32 {
        engine
            .start_raw(
                "big",
                &format!("big-{n}"),
                serde_json::to_vec(&n).unwrap().into(),
                Default::default(),
            )
            .await
            .unwrap();
        engine
            .run::<(bool, usize)>(&format!("big-{n}"))
            .signal("note", &"x".repeat(200_000))
            .await
            .unwrap();
    }
    for n in 0..4u32 {
        let r = engine
            .run::<(bool, usize)>(&format!("big-{n}"))
            .result()
            .await
            .unwrap();
        assert_eq!(r, (true, 200_000));
    }
    assert!(dir.join("wal/blobs").is_dir());
    let point = engine.backup().unwrap().sync().await.unwrap();
    let packs = files_under(&store)
        .iter()
        .filter(|p| p.to_string_lossy().ends_with(".blobs"))
        .count();
    assert!(packs > 0, "blob objects in the target");
    let v = backup::verify(&*target).unwrap();
    assert!(v.missing.is_empty() && v.damaged.is_empty(), "{v:?}");
    let restored = tmp("blobs-restored");
    backup::restore(
        &*target,
        &restored,
        &RestoreOptions {
            at_ms: Some(point.time_ms),
            timeline: None,
        },
    )
    .unwrap();
    assert!(restored.join("wal/blobs").is_dir());
    engine.shutdown().await.unwrap();
    let engine = open_big(cfg(&restored)).await;
    for n in 0..4u32 {
        let r = engine
            .run::<(bool, usize)>(&format!("big-{n}"))
            .result()
            .await
            .unwrap();
        assert_eq!(r, (true, 200_000), "big-{n} after the restore");
    }
    engine.shutdown().await.unwrap();
    for d in [dir, store, restored] {
        let _ = std::fs::remove_dir_all(d);
    }
}

fn big_page(n: u32) -> String {
    format!("{n:08}").repeat(40_000)
}

async fn open_big(c: Config) -> Engine {
    let engine = Engine::open(c).await.unwrap();
    engine.register("big", move |ctx: Ctx, n: u32| async move {
        let p: String = ctx
            .step("page", move || async move { Ok::<_, Error>(big_page(n)) })
            .await?;
        let note: String = ctx.wait_for("note").await?.unwrap_or_default();
        Ok((p == big_page(n), note.len()))
    });
    engine
}

fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(files_under(&p));
            } else {
                out.push(p);
            }
        }
    }
    out
}
