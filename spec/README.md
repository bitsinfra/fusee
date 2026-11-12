# Specifications

| Module | What it states |
| --- | --- |
| `FuseeOutputs.tla` | The output-release rule on one shard: a step function runs only on durable inputs, an output leaves only after its record is durable, and a crash loses everything not durable. Invariant: nothing outside the process depends on a record a crash took back. |
| `FuseeReplication.tla` | The safety argument of the replication protocol in `crates/fusee-core/src/vsr.rs`: quorum commits in the current view, view changes that pick the durable log with the highest (last normal view, length), atomic log installation, crashes that keep only the durable prefix and never let a restarted primary lead its old view. Invariants: a committed entry never changes, and a quorum always holds it. |
| `FuseeCheckpoint.tla` | Crash ordering for installing a durable checkpoint and suffix. The old generation remains until directory sync makes the new pointer durable. Every previously acknowledged position must be present. |
| `FuseeLease.tla` | Read leases (`docs/replication.md`, Step-down and read leases): pings, a backup's promise not to take part in a view change, the primary's lease from a quorum's answers, crashes that forget promises, and per-replica clocks advancing at bounded, independent rates. Invariants: a read under a lease sees every committed write, and no replica is normal in a newer view while a lease holds. |
| `FuseeReconfig.tla` | Membership change on top of the replication model: one reconfiguration entry that seals its epoch, the next epoch formed by a quorum of new members that hold it durably, members without it copying the committed prefix, state transfer, retired members. Invariants: a committed entry never changes, and a quorum of the newest epoch holds every committed entry. |
| `FuseeLog.tla` | The acceptance rule of log format 3 (`crates/fusee-core/src/wal.rs`): batches chained by checksum, each carrying the sequence its writer had acknowledged when it was submitted, with up to K writes in flight completing in any order, crashes that keep, lose or tear each write in flight, and every reopen resuming in place over stale batches of earlier lives. Invariants: recovery never misses an acknowledged batch, a crash never looks like damage, and a stale batch of an older life is never spliced after a newer one. |

Check them with TLC:

```bash
java -cp tla2tools.jar tlc2.TLC -config FuseeOutputs.cfg FuseeOutputs.tla
java -cp tla2tools.jar tlc2.TLC -config MC.cfg MC.tla
```

With TLC 2.19 the output model has 7,272 distinct reachable states. The replication model has 5,815,792
distinct reachable states with three replicas, three operations, three view
changes and two crashes. Both searches exhaust their queues and satisfy
the specified safety invariants. The replication model abstracts network
delivery and storage I/O. Real replica recovery and fault tests exercise those
implementation details.

Reproduce with the [TLA+ tools release](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.4).
The SHA-256 of that `tla2tools.jar` is
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.

Commit on any durable quorum (`docs/replication.md`) is part of the
replication model: its `Commit` action accepts any quorum, counting the
primary's own durable length only when the primary is in it.
`MC-witness.cfg` adds `PrimaryCommitsOnlyWhatItHolds` (defined in `MC.tla`:
a primary never commits past its own durable prefix) and is expected to
fail; TLC finds the nine-step counterexample in which a primary commits an
entry only its two backups hold, so the state space that `MC.cfg` proves
safe contains that behavior.

```bash
java -cp tla2tools.jar tlc2.TLC -config MC-witness.cfg MC.tla
```

**Leases** (`MCLease.tla`). Three replicas,
one view change, one write, three ticks of real time, promise 2, clocks
advancing one or two units per tick (a 50 % drift bound, so the safe lease
is half the promise, 1). A ping's answer arrives with it: its delay would
only shorten the lease it grants. `FuseeLease-nocrash.cfg` (no crash):
28,105,017 distinct states, queue exhausted, no violation of
`LeaseReadsAreCurrent` or `LeaseExcludesNewerViews`. `FuseeLease.cfg` adds
one crash (and the restart abstention): 105,112,688 distinct states,
queue exhausted, no violation. Expected to fail, and
failing: `FuseeLease-nopromise.cfg` (backups ignore their promises) violates
`LeaseExcludesNewerViews` in six steps; `FuseeLease-drift.cfg` (lease 2,
the promise itself) in seven.

```bash
java -cp tla2tools.jar tlc2.TLC -deadlock -config FuseeLease-nocrash.cfg MCLease.tla
java -cp tla2tools.jar tlc2.TLC -deadlock -config FuseeLease.cfg MCLease.tla
java -cp tla2tools.jar tlc2.TLC -deadlock -config FuseeLease-nopromise.cfg MCLease.tla
java -cp tla2tools.jar tlc2.TLC -deadlock -config FuseeLease-drift.cfg MCLease.tla
```

**Membership change** (`MCReconfig.tla`). Four
replicas, three operations (the reconfiguration entry among them), views 0
and 1 in epoch 0 and 2 and 3 in epoch 1, one crash. Replacing member r3
with the new member r4 (`FuseeReconfig.cfg`): 1,662,865 distinct states;
growing from three members to four (`FuseeReconfig-grow.cfg`): 1,515,475;
both exhaust their queues with no violation of `CommittedNeverChanges`
or `CommittedSurvives`. Negative controls: letting epoch 0 propose after
the reconfiguration entry (`FuseeReconfig-noseal.cfg`) violates
`CommittedSurvives` in 14 steps; letting members without the entry vote in
the new epoch (`FuseeReconfig-nohold.cfg`) in 10. `-deadlock` because the
bounded models stop.

```bash
java -cp tla2tools.jar tlc2.TLC -deadlock -config FuseeReconfig.cfg MCReconfig.tla
java -cp tla2tools.jar tlc2.TLC -deadlock -config FuseeReconfig-grow.cfg MCReconfig.tla
java -cp tla2tools.jar tlc2.TLC -deadlock -config FuseeReconfig-noseal.cfg MCReconfig.tla
java -cp tla2tools.jar tlc2.TLC -deadlock -config FuseeReconfig-nohold.cfg MCReconfig.tla
```

The checkpoint model uses three pages and three acknowledged suffix positions. TLC explored 22 distinct states and exhausted its queue. A control run that allows pointer publication before file sync violates the recovery invariant.

The log model (`FuseeLog.tla`) is checked with five batch slots per life, two writes in flight and four lives
(`FuseeLog.cfg`: 67,823 distinct states) and with seven slots and three
writes in flight (`FuseeLog-k3.cfg`: 1,619,811 distinct states). Both
searches exhaust their queues with no violation. Slots are indexed by
sequence, so a stale batch with the expected sequence always sits where
recovery looks next and only the chain can reject it. Two negative controls
fail as they should: accepting batches by sequence alone splices a stale
batch of an older life (`ChainCheck = FALSE`, `NoStaleSplice`), and treating
any valid batch after the end as proof of loss, the rule of log format 2,
reports damage after an ordinary crash with two writes in flight
(`RelativeAck = FALSE`, `NoFalseDamage`).
