//! Crash campaign: a child process runs workflows with a continuous backup
//! and is killed (SIGKILL) at random moments, many times; after each kill
//! the backup's manifest names only complete, intact objects. A last clean
//! run, then a restore that is itself killed at random moments and rerun:
//! the restored directory holds every acknowledged workflow.
//!
//! The child is this test binary run again with `FUSEE_BACKUP_CHILD` set.

use fusee::backup::{self, BackupOptions, LocalTarget, RestoreOptions, Target};
use fusee::{Config, Ctx, Engine, Error};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn cfg(dir: &Path) -> Config {
    let mut c = Config::local(dir)
        .shards(2)
        .segment_bytes(256 << 10)
        .checkpoint_interval(Duration::from_millis(100))
        .park_after(Duration::from_millis(20));
    c.local.flush = fusee::FlushMethod::Fsync;
    c
}

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
}

fn expected(n: u32) -> u64 {
    u64::from(n) * 2 + 3 * 2048
}

/// Child: `work` runs workflows one after another, logging each
/// acknowledged result, until killed (or, with `clean`, for a while and
/// then shuts down); `restore` restores the newest point.
#[test]
fn child_entry() {
    let Ok(mode) = std::env::var("FUSEE_BACKUP_CHILD") else {
        return;
    };
    let dir = PathBuf::from(std::env::var("FUSEE_BACKUP_DIR").unwrap());
    let target_dir = PathBuf::from(std::env::var("FUSEE_BACKUP_TARGET").unwrap());
    let life: u32 = std::env::var("FUSEE_BACKUP_LIFE").unwrap().parse().unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let target: Arc<dyn Target> = Arc::new(LocalTarget::new(&target_dir).unwrap());
    match mode.as_str() {
        "work" | "clean" => rt.block_on(async {
            let opts = BackupOptions {
                upload_interval: Some(Duration::from_millis(20)),
                resolution: Duration::from_millis(20),
                retention: Duration::from_secs(3600),
            };
            let engine = Engine::open(cfg(&dir).backup(target, opts)).await.unwrap();
            register(&engine);
            let mut log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.with_extension("acked"))
                .unwrap();
            let until = Instant::now() + Duration::from_millis(400);
            let mut i = 0u32;
            while mode == "work" || Instant::now() < until {
                let n = life * 100_000 + i;
                let id = format!("r-{n}");
                engine
                    .start_raw(
                        "work",
                        &id,
                        serde_json::to_vec(&n).unwrap().into(),
                        Default::default(),
                    )
                    .await
                    .unwrap();
                assert_eq!(engine.run::<u64>(&id).result().await.unwrap(), expected(n));
                // A kill can interrupt the append. Frame both ends so a
                // later child cannot finish a partial ID with its own.
                log.write_all(format!("\n{n} ok\n").as_bytes()).unwrap();
                i += 1;
            }
            engine.backup().unwrap().sync().await.unwrap();
            engine.shutdown().await.unwrap();
        }),
        "restore" => {
            backup::restore(&*target, &dir, &RestoreOptions::default()).unwrap();
        }
        other => panic!("unknown child mode {other}"),
    }
}

fn spawn(mode: &str, dir: &Path, target: &Path, life: u32) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_entry",
            "--nocapture",
            "--test-threads",
            "1",
        ])
        .env("FUSEE_BACKUP_CHILD", mode)
        .env("FUSEE_BACKUP_DIR", dir)
        .env("FUSEE_BACKUP_TARGET", target)
        .env("FUSEE_BACKUP_LIFE", life.to_string())
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap()
}

fn xorshift(x: &mut u64) -> u64 {
    *x ^= *x << 13;
    *x ^= *x >> 7;
    *x ^= *x << 17;
    *x
}

fn walk_tmp(dir: &Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|entries| {
        entries.filter_map(|e| e.ok()).any(|e| {
            let p = e.path();
            if p.is_dir() {
                walk_tmp(&p)
            } else {
                p.to_string_lossy().ends_with(".tmp")
            }
        })
    })
}

fn check_backup(target: &dyn Target, when: &str) {
    let v = backup::verify(target).unwrap();
    assert!(
        v.missing.is_empty() && v.damaged.is_empty(),
        "{when}: the manifest names incomplete objects: {v:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_uploads_and_restores_leave_a_consistent_backup_and_a_complete_restore() {
    let base = std::env::temp_dir().join(format!(
        "fusee-backup-crash-{}-{}",
        std::process::id(),
        fusee::local::unix_ms()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let dir = base.join("data");
    let store = base.join("target");
    let target: Arc<dyn Target> = Arc::new(LocalTarget::new(&store).unwrap());
    let mut rng = 0x9e37_79b9_7f4a_7c15u64 ^ fusee::local::unix_ms();
    let lives: u32 = std::env::var("FUSEE_BACKUP_CRASH_LIVES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let mut killed = 0;
    // Kills that left objects no manifest names, or half-written files:
    // they landed in the middle of an upload.
    let mut mid_upload = 0;
    let mut saw_manifest = false;
    for life in 0..lives {
        let mut child = spawn("work", &dir, &store, life);
        let wait = 150 + xorshift(&mut rng) % 500;
        std::thread::sleep(Duration::from_millis(wait));
        if child.try_wait().unwrap().is_none() {
            child.kill().unwrap();
            killed += 1;
        }
        child.wait().unwrap();
        if let Some((root, state)) = backup::load_state(&*target).unwrap() {
            saw_manifest = true;
            check_backup(&*target, &format!("after kill {life}"));
            let mut named = state.keys();
            named.extend(root.snapshot);
            let debris =
                target.list("").unwrap().iter().any(|k| !named.contains(k)) || walk_tmp(&store);
            mid_upload += usize::from(debris);
        } else {
            // The first process can be killed before it opens the engine.
            assert!(!saw_manifest, "the manifest disappeared after kill {life}");
            assert!(
                target.list("").unwrap().is_empty(),
                "objects before the first manifest"
            );
        }
    }
    assert!(
        killed >= lives / 2,
        "only {killed} of {lives} children were killed"
    );
    let mut clean = spawn("clean", &dir, &store, lives);
    assert!(clean.wait().unwrap().success(), "the clean run failed");
    check_backup(&*target, "after the clean run");
    let (_, state) = backup::load_state(&*target).unwrap().unwrap();
    assert_eq!(
        state.timelines.len(),
        1,
        "every life continued the same history"
    );

    // How long a restore takes (process start included), then restores
    // killed at random moments within that, then one left to finish.
    let out = base.join("restored");
    let staging = base.join(".restored.restoring");
    let started = Instant::now();
    assert!(spawn("restore", &out, &store, 0).wait().unwrap().success());
    let took = started.elapsed().as_millis() as u64;
    std::fs::remove_dir_all(&out).unwrap();
    let mut restore_kills = 0;
    let mut mid_restore = 0;
    for attempt in 0..10 {
        // Marks what an earlier attempt left, to tell it from this one's.
        if staging.exists() {
            std::fs::write(staging.join("stale"), b"").unwrap();
        }
        let mut child = spawn("restore", &out, &store, 0);
        let wait = xorshift(&mut rng) % took.max(1);
        std::thread::sleep(Duration::from_millis(wait));
        if child.try_wait().unwrap().is_none() {
            child.kill().unwrap();
            restore_kills += 1;
        }
        child.wait().unwrap();
        mid_restore += usize::from(staging.exists() && !staging.join("stale").exists());
        check_backup(&*target, &format!("after restore attempt {attempt}"));
        // Either nothing is in place yet, or the whole directory is.
        if out.exists() {
            assert!(
                out.join("fusee.meta").exists(),
                "a partial restore is in place"
            );
            std::fs::remove_dir_all(&out).unwrap();
        }
    }
    let mut last = spawn("restore", &out, &store, 0);
    assert!(
        last.wait().unwrap().success(),
        "the rerun of the restore failed"
    );
    assert!(!staging.exists());

    let acked: Vec<u32> = std::fs::read_to_string(dir.with_extension("acked"))
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_suffix(" ok"))
        .map(|id| id.parse().unwrap())
        .collect();
    assert!(acked.len() > 50, "only {} workflows ran", acked.len());
    let engine = Engine::open(cfg(&out)).await.unwrap();
    register(&engine);
    for n in &acked {
        assert_eq!(
            engine.run::<u64>(&format!("r-{n}")).result().await.unwrap(),
            expected(*n),
            "r-{n}"
        );
    }
    engine.shutdown().await.unwrap();
    eprintln!(
        "{lives} lives ({killed} killed, {mid_upload} in the middle of an upload); restore takes \
         {took} ms, {restore_kills} of 10 restores killed ({mid_restore} in the middle); {} \
         workflows acknowledged, all restored",
        acked.len()
    );
    std::fs::remove_dir_all(&base).unwrap();
}
