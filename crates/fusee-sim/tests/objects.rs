//! Keyed objects under the simulator: client calls retried across crashes,
//! calls and one-way sends from workflows, shared reads, compaction, and a
//! checker that every key's history is linearizable.
//!
//! Each deposit adds a distinct power of two, so a balance names exactly
//! the deposits it includes. Per key the checker requires:
//!
//! - every exclusive call's answer includes its own deposit;
//! - the answers (exclusive and shared) form one chain under inclusion: a
//!   sequential history exists in which each answer is the state right
//!   after (or, for a read, at) its call;
//! - real time is respected: a call answered before another was first sent
//!   is ordered before it;
//! - the final balance is every deposit exactly once.

use fusee::object::ObjectHandlers;
use fusee::{Ctx, Error, ObjectCtx};
use fusee_sim::scenario::{Outcome, Scenario};
use std::time::Duration;

const KEYS: u64 = 2;
const CLIENT_DEPOSITS: u64 = 12;
const WORKFLOWS: u64 = 4;
const READS: u64 = 5;

/// Steps per deposit: enough that a key reaches the compaction threshold
/// within one seed.
const STEPS: u32 = 6;

fn bit(key: u64, n: u64) -> u64 {
    let _ = key;
    1u64 << n
}

fn check(out: &Outcome) -> Result<(), String> {
    for key in 0..KEYS {
        // (answer, sent, answered, own deposit) for the client's calls.
        let mut answers: Vec<(u64, Option<(u64, u64)>, u64)> = Vec::new();
        let mut all = 0u64;
        for i in 0..CLIENT_DEPOSITS {
            let id = format!("dep-{key}-{i}");
            let own = bit(key, i);
            all |= own;
            let r = out
                .call::<u64>(&id)
                .ok_or(format!("{id} was never answered"))??;
            let c = out.calls().find(|(c, _)| *c == id).map(|(_, c)| c.clone());
            answers.push((r, c.map(|c| (c.sent_ms, c.answered_ms)), own));
        }
        for w in 0..WORKFLOWS {
            let n = CLIENT_DEPOSITS + 2 * w;
            all |= bit(key, n) | bit(key, n + 1);
            let id = format!("payer-{key}-{w}");
            let r = out
                .result::<u64>(&id)
                .ok_or(format!("{id} has no result"))??;
            answers.push((r, None, bit(key, n)));
        }
        for i in 0..READS {
            let id = format!("read-{key}-{i}");
            let r = out
                .call::<u64>(&id)
                .ok_or(format!("{id} was never answered"))??;
            let c = out.calls().find(|(c, _)| *c == id).map(|(_, c)| c.clone());
            answers.push((r, c.map(|c| (c.sent_ms, c.answered_ms)), 0));
        }
        let final_id = format!("final-{key}");
        let fin = out
            .call::<u64>(&final_id)
            .ok_or(format!("{final_id} was never answered"))??;
        if fin != all {
            return Err(format!(
                "key {key}: final balance {fin:#x}, expected every deposit once: {all:#x}"
            ));
        }
        for (b, _, own) in &answers {
            if b & own != *own {
                return Err(format!(
                    "key {key}: the answer {b:#x} misses its own deposit {own:#x}"
                ));
            }
            if b & !all != 0 {
                return Err(format!(
                    "key {key}: the answer {b:#x} holds deposits never made"
                ));
            }
        }
        let mut sorted: Vec<u64> = answers.iter().map(|a| a.0).collect();
        sorted.sort_unstable();
        for w in sorted.windows(2) {
            if w[0] & w[1] != w[0] {
                return Err(format!(
                    "key {key}: answers {:#x} and {:#x} are not one history",
                    w[0], w[1]
                ));
            }
        }
        for a in &answers {
            for b in &answers {
                if let (Some((_, a_done)), Some((b_sent, _))) = (a.1, b.1)
                    && a_done < b_sent
                    && a.0 & b.0 != a.0
                {
                    return Err(format!(
                        "key {key}: {:#x} was answered at {a_done} ms, before a call sent at \
                         {b_sent} ms answered {:#x} without it",
                        a.0, b.0
                    ));
                }
            }
        }
    }
    Ok(())
}

fn scenario() -> Scenario {
    Scenario::new(|sim| {
        sim.object(
            "acct",
            ObjectHandlers::new()
                .exclusive("deposit", |ctx: ObjectCtx, n: u64| async move {
                    // A handler that spans operations, so crashes land
                    // inside calls.
                    let mut got = 0;
                    for s in 0..STEPS {
                        got = ctx
                            .step(&format!("s{s}"), move || async move {
                                fusee_sim::sleep(Duration::from_millis(1)).await;
                                Ok::<_, Error>(n)
                            })
                            .await?;
                    }
                    let b: u64 = ctx.state().get("balance")?.unwrap_or(0);
                    ctx.state().set("balance", &(b + got))?;
                    Ok(b + got)
                })
                .shared("balance", |ctx: ObjectCtx, _: ()| async move {
                    Ok(ctx.state().get::<u64>("balance")?.unwrap_or(0))
                }),
        );
        sim.register("payer", |ctx: Ctx, (key, n): (String, u64)| async move {
            let b: u64 = ctx
                .object("acct", &key)
                .call("deposit", &(1u64 << n))
                .await?;
            ctx.object("acct", &key)
                .send("deposit", &(1u64 << (n + 1)))?;
            Ok(b)
        });
        for key in 0..KEYS {
            let k = format!("k{key}");
            for i in 0..CLIENT_DEPOSITS {
                let at = Duration::from_millis(sim.random(1_500));
                sim.call(
                    at,
                    "acct",
                    &k,
                    "deposit",
                    &format!("dep-{key}-{i}"),
                    &bit(key, i),
                );
            }
            for w in 0..WORKFLOWS {
                let at = Duration::from_millis(sim.random(1_500));
                let n = CLIENT_DEPOSITS + 2 * w;
                sim.start(at, &format!("payer-{key}-{w}"), "payer", &(k.clone(), n));
            }
            for i in 0..READS {
                let at = Duration::from_millis(sim.random(2_000));
                sim.call(at, "acct", &k, "balance", &format!("read-{key}-{i}"), &());
            }
            // Long after the faults stopped and every call finished.
            sim.call(
                Duration::from_secs(1_200),
                "acct",
                &k,
                "balance",
                &format!("final-{key}"),
                &(),
            );
        }
        sim.check(check);
    })
}

#[test]
fn object_histories_are_linearizable_across_crashes() {
    let s = scenario()
        .run_seeds(
            0..std::env::var("FUSEE_OBJECT_SEEDS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1_000),
        )
        .unwrap_or_else(|f| panic!("{f}"));
    assert!(s.crashes > 0, "the seeds crashed: {s:?}");
}

fn caught(sabotage: fusee::runtime::Sabotage) -> (u64, String) {
    for seed in 0..400 {
        let mut cfg = scenario().config(seed);
        cfg.sabotage = sabotage;
        cfg.crash_every_ms = cfg.crash_every_ms.max(150);
        if let Err(f) = fusee_sim::run_seed(cfg) {
            return (seed, f.message);
        }
    }
    panic!("no seed caught {sabotage:?}");
}

#[test]
fn the_checker_catches_calls_run_twice() {
    let (seed, msg) = caught(fusee::runtime::Sabotage::DuplicateUpdates);
    eprintln!("DuplicateUpdates caught by seed {seed}: {msg}");
}

#[test]
fn the_checker_catches_exclusive_calls_that_overlap() {
    let (seed, msg) = caught(fusee::runtime::Sabotage::UnlockedObjects);
    eprintln!("UnlockedObjects caught by seed {seed}: {msg}");
}

#[test]
fn the_checker_catches_answers_released_before_durability() {
    let (seed, msg) = caught(fusee::runtime::Sabotage::EarlyRelease);
    eprintln!("EarlyRelease caught by seed {seed}: {msg}");
}
