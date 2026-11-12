//! Paired startup measurements with the same previously acknowledged data.
use super::*;
use fusee::foreign::ForeignEngine;
use fusee::local::Preallocate;
use fusee::runtime::ResultState;
use std::fs;
use std::path::Path;

const SHARDS: u32 = 8;
fn scoped_config(path: &Path, policy: Preallocate) -> Config {
    let mut cfg = Config::local(path).shards(SHARDS);
    cfg.local.preallocate = policy;
    cfg
}

fn copy_fixture(from: &Path, to: &Path) -> std::io::Result<()> {
    fs::create_dir(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_fixture(&entry.path(), &target)?;
        } else if entry.file_type()?.is_file() {
            fs::copy(entry.path(), target)?;
        } else {
            return Err(std::io::Error::other("unexpected recovery fixture entry"));
        }
    }
    Ok(())
}

fn allocated(path: &Path) -> Result<Vec<u64>, Error> {
    (0..SHARDS)
        .map(|i| {
            let files = fs::read_dir(path.join(format!("shard-{i:03}/log")))?
                .collect::<Result<Vec<_>, _>>()?;
            let newest = files
                .into_iter()
                .filter(|f| f.path().extension().is_some_and(|e| e == "seg"))
                .max_by_key(|f| f.file_name())
                .ok_or_else(|| Error::Failed("startup did not prepare a segment".into()))?;
            Ok(newest.metadata()?.len())
        })
        .collect()
}

pub(super) async fn run(dir: &Path) -> Result<Value, Error> {
    fs::create_dir_all(dir)?;
    let lab = dir.join(run_prefix());
    fs::create_dir(&lab)?;
    let seed = lab.join("seed");
    let engine = Engine::open(scoped_config(&seed, Preallocate::Zero)).await?;
    let wf = engine.register("witness", |ctx: Ctx, n: u32| async move {
        ctx.step("inc", || async move { Ok::<_, Error>(n + 1) })
            .await
    });
    let mut ids = std::collections::BTreeMap::new();
    for index in 0.. {
        let id = format!("segment-open-{index}");
        ids.entry(fusee::core::shard::shard_of(&id, SHARDS))
            .or_insert(id);
        if ids.len() == SHARDS as usize {
            break;
        }
    }
    for id in ids.values() {
        if engine.start(&wf, id, &41).await?.result().await? != 42 {
            return Err(Error::Failed("bad seed value".into()));
        }
    }
    engine.shutdown().await?;
    drop(wf);
    drop(engine);
    let mut samples = Vec::new();
    for trial in 0..5 {
        let mut policies = [
            ("full", Preallocate::Zero),
            ("short-first", Preallocate::ShortFirst),
        ];
        if trial % 2 != 0 {
            policies.reverse();
        }
        for (label, policy) in policies {
            for recovery in [false, true] {
                let attempt = lab.join(format!("{trial}-{label}-{recovery}"));
                if recovery {
                    copy_fixture(&seed, &attempt)?;
                }
                let cfg = scoped_config(&attempt, policy);
                let start = Instant::now();
                let host = ForeignEngine::open(&cfg, Arc::new(|| {}))?;
                let elapsed_us = start.elapsed().as_micros();
                let initial_files = allocated(&attempt)?;
                let mut recovered = Vec::new();
                if recovery {
                    for (shard, id) in &ids {
                        match host.result(id)? {
                            (
                                owner,
                                ResultState::Done {
                                    status: fusee::core::record::Status::Ok,
                                    payload,
                                    ..
                                },
                            ) if owner == *shard
                                && serde_json::from_slice::<u32>(&payload)? == 42 =>
                            {
                                recovered.push(id)
                            }
                            _ => return Err(Error::Failed("missing recovered witness".into())),
                        }
                    }
                }
                host.flush_all()?;
                drop(host);
                samples.push(json!({"trial":trial,"policy":label,"recovery":recovery,"open_us":elapsed_us,
                                    "initial_segment_bytes":initial_files,"acknowledged_values_checked":recovered}));
                fs::remove_dir_all(attempt)?;
            }
        }
    }
    fs::remove_dir_all(lab)?;
    Ok(json!({"machine":machine(),"shards":SHARDS,"workload":"segment-open","samples":samples}))
}
