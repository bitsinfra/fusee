# Replication

## Protocol

`crates/fusee-core/src/vsr.rs`, a pure state machine in the style of
Viewstamped Replication Revisited:

- 1 to 9 replicas per group (3 or 5 in practice), with member IDs below 32.
  Membership is fixed within an *epoch*; a view carries its epoch in its
  high 32 bits, and the primary of view `v` is the epoch's member list at
  position `(v - first view of the epoch) % n`. A group that never changed
  is epoch 0 with members `0..n`, whose primary of `v` is `v % n`
  ([Membership change](#membership-change)).
- The primary appends entries and sends them to backups at once (before its
  own flush). Backups flush and acknowledge how far they are durable.
- An entry commits when any quorum holds it durably **in the current view**;
  the primary need not be one of them. Outputs and step gates use the
  commit position.
- Entries form a hash chain, so a replica can verify entries from any peer.
- View change: `StartViewChange`, then `DoViewChange` with each replica's
  last normal view, op and uncommitted suffix; the new primary takes the log
  with the highest (last normal view, op), fetches committed entries it
  lacks, and sends `StartView`.
- Protocol-aware recovery: a replica whose log has a damaged record in the
  middle does not truncate committed work. It takes no part in view changes,
  fetches the range from peers, verifies it against the chain, and rewrites
  its log; if its later entries belong to another history it takes the
  primary's log from below the damage.

Rules the protocol keeps, each covered by the simulator's checkers:

1. A replica that restarts during a view change comes back in view-change
   status, not normal.
2. A restarted primary never leads its old view again: entries a quorum of
   backups committed may be missing from its disk.
3. Responses that extend a log are accepted only from the current view's
   primary (or, for a new primary, from the replica whose log it adopts).
4. A backup keeps everything through its own commit; only later entries are
   replaced.
5. A backup missing committed entries of a new view stays recovering (does
   not vote) until a full state transfer fills the gap.
6. A log is only ever replaced atomically, once the complete new log is in
   hand; and the saved "last normal view" is a barrier behind the log it
   vouches for.
7. Only a replica with holes in its log abstains from voting.
8. A backup that answered its primary's ping takes part in no view change
   (starting, joining, voting or leading one) for the promise time after,
   and a restarted replica abstains for that long from its start
   ([read leases](#step-down-and-read-leases)).
9. A primary proposes nothing after a reconfiguration entry: it is the last
   entry of its epoch ([membership change](#membership-change)).
10. Only a replica that holds the log through the entry that began its
    epoch votes in it; one that does not first fetches that committed
    prefix from any member, verified against the entry.
11. A replica that restarted with a hole in its log installs a checkpoint
    image only with a suffix that reaches everything it holds after the
    hole: it may have acknowledged those entries, and be the only member of
    their quorum still holding them.

A primary-elect missing part of its selected donor’s committed prefix pulls verified pages and retries dropped continuation requests. A replica catching up from a primary keeps its old durable log until it has verified the entire replacement. The preferred page limit is 1 MiB or 4,096 operations. A single admissible entry may exceed the preferred byte limit. Full-log rewriting streams to a temporary file before the durable rename.

## Failover timing

`Timing { heartbeat_ms, election_ms }` (`ReplicaConfig::timing`; `fusee
server --heartbeat-ms --election-ms`), in milliseconds of each member's
clock. Defaults: **heartbeat 5 ms, election 40 ms**. An idle primary sends a
heartbeat every heartbeat interval; a backup that hears nothing from its
primary for the election timeout plus a jitter drawn again for every
election from `0..election_ms` starts a view change, so a silent primary is
suspected after 40 to 80 ms. The election timeout must be at least four
heartbeats. The I/O thread ticks at half the heartbeat (1 to 5 ms), and
before it starts an election it lets its readers run once, so heartbeats
that waited in a socket while the thread was descheduled count.

- **A crashed process is noticed sooner.** Its sockets close at once; a
  backup whose primary's stream closes (with nothing received since)
  elects as soon as its promise to the primary lapses (half the election
  timeout, 20 ms) instead of waiting out the timeout and jitter.
- **View changes repeat themselves.** A replica in a view change it started
  or joined resends its StartViewChange (and its DoViewChange) every 10 ms,
  so a member that was restarting when they were first sent joins the same
  view instead of waiting for another timeout; a primary that hears one for
  its own view sends that member its StartView again (it missed it, often
  because the primary's stream to it was not up yet), and a dialer retries
  a refused connection every 10 ms. A replica that restarted as the primary
  of a view never leads that view again (rule 2).
- **SDK members settle first.** A TypeScript or Python member opens its
  engine once it has been the ready primary of one view for 150 ms: while
  three processes start, the first primary can hand the role on within a
  few election timeouts.
- **Resends wait for a stall.** A primary resends a backup's missing suffix
  only once that backup's position has stalled for 10 ms with nothing new
  proposed.

**Lossy or distant networks.** A lost TCP segment stalls a stream until
it is retransmitted, at least 200 ms later on Linux; an election timeout
shorter than that turns every loss burst into an election. The defaults
suit a datacenter or loopback network; raise `--election-ms` (to 150 ms or
more, as the partition tests below do for their 2 % loss and 20 ms
delay) where packets are lost or round trips take tens of milliseconds.
Scheduling gaps count too: on a host with several times more runnable
threads than cores, a member can miss heartbeats for longer than the
election timeout, so raise `--election-ms` there as well.

**Primary kill to resumed work.** On three i4i.large hosts in one
placement group, the failovers that completed before the next of 30
primary kills had the next member ready p50 25.0 ms after the kill (at
most 30.3 ms) and committing new work p50 30.3 ms after it (at most
35.3 ms). `fusee-crashtest replicated-driver`
with `stream` measures the same on any machine: every primary also starts a
workflow every 2 ms, and the driver SIGKILLs the primary 30 to 230 ms after
it opened its engine and times, from the kill, the next member becoming
ready, opening its engine, running workflow code and committing a new
workflow. A kill that comes before the previous victim has restarted and
caught up is a second failure within the restart window, more than a
three-member group tolerates at once: the new primary has only a
recovering backup, and its first commit waits for that member's restart.
Opening the engine takes 3 to 9 ms of a failover (eight shards, each
reopening its directory and replaying its records since the checkpoint);
hot standby removes it.

**Hot standby.** `ReplicaNode::standby_config()` gives an engine that opens
at once on any member: its shard threads, runtimes and open run stores
exist and its workflows are registered while the member is a backup, and
its shards stand by (calls to them wait). The moment the member becomes
the ready primary, each shard takes the committed log batches from the I/O
thread's memory (one barrier round trip, 50 to 150 µs per shard) and
replays its records since its checkpoint; nothing is opened, except a
shard's run store when a checkpoint image installed while it stood by
moved the engine directory. A standby engine serves one primary tenure:
when the member loses the role it stops like any engine, and the
application opens the next one. `fusee-crashtest replica` uses it unless
`FUSEE_REPLICA_STANDBY=0`. With it, ready to first new commit takes about
1.5 ms, and kill to commit is dominated by detection and the view change.
A promoted shard still replays the records since its checkpoint (from
memory).

## Step-down and read leases

**The assumption.** Every member's clock advances at a rate within 1 % of
real time (`fusee_core::vsr::MAX_CLOCK_DRIFT_PPM`). Delays of messages and
of threads are unbounded; only rates are assumed. The protocol clock counts
time a host spends suspended: it advances with the monotonic clock plus
however far `CLOCK_BOOTTIME` has run ahead of it on Linux (macOS's
`CLOCK_MONOTONIC` already counts sleep), and never runs back, even when a
tool such as libfaketime moves the boot clock back. A clock that stops
while time passes, or a VM restored from a memory snapshot, breaks the
assumption.

**The rule.** With every heartbeat the primary sends each backup a ping
stamped with its clock. A backup normal in the primary's view answers and
*promises* not to take part in any view change (start, join, vote in or
lead one) until its own clock has advanced `P` (the promise, half the
election timeout: 20 ms) past the moment it received the ping; a request to
change views that arrives meanwhile waits and is joined once the promise
lapses, unless the primary renews it. A restarted replica, which cannot
remember its promises, abstains for `P` from its start. The primary holds a
**read lease** while its clock is less than `L` past the stamp of the
oldest ping among a quorum's freshest answers (itself included), where

    L = (P - 1)(1 - ρ)/(1 + ρ) - 1   (17 ms for the default timing)

the promise as the slowest allowed backup clock measures it, as the fastest
allowed primary clock measures it, less a millisecond at each end for
millisecond clocks. Every view change needs a quorum that intersects the
lease's quorum in a member bound by its promise, or in the primary itself,
which loses the lease the moment it leaves its view (before anything of
the view change is sent). So while a primary holds a lease no other member
can lead a newer view, and its state has every acknowledged write.

**Step-down.** A primary that hears from no quorum (acknowledgments or ping
answers) for four election timeouts, and at least 200 ms
(`Timing::step_down_ms`), starts a view change: its engine stops instead of
waiting on a commit that cannot come. Not sooner: a backup blocked in a slow
flush, or a process still starting, answers late, not never; by then a
backup that lost the primary has elected another, and the lease lapsed
long ago.

**Reads.** `ReplicaNode::lease_valid()` (also on a cloned `ReadGuard`) says
whether reads of the engine are current; `confirm_reads()` returns at once
under a lease, else after a no-op commits through a quorum (a read started
after it returns sees every write the group acknowledged before the call),
and fails without a quorum. The TypeScript and Python SDKs check the lease
before every engine call (queries, results, inspect and list included) and,
without one, confirm through the protocol or refuse the call. A dedicated
server's quorum check (`QUORUM_PROBE` and its idle fence) answers from the
lease when it can. A Rust application reading its embedded engine calls
`confirm_reads()` before a read that must not be stale. Under a load spike a
lease can lapse briefly; reads then pay one round trip.

**Checked.** `spec/FuseeLease.tla` models pings, promises, leases, view
changes, crashes and per-replica clocks advancing one or two units per tick
(drift of 50 %, so the safe lease is `P·1/2`). TLC checks that a lease read
sees every committed write and that no replica is normal in a newer view
while a lease holds (28,105,017 distinct states without crashes,
exhaustive; see `spec/README.md` for the run with a crash); two negative
controls fail: without the promise rule (six steps) and with a lease that
ignores drift (`L = P`, seven steps). With one crash, TLC exhausted
105,112,688 distinct states with no violation.
The simulator gives every member a clock rate within ±0.9 % and checks the
same two properties every simulated millisecond for a ready primary under
its lease (5,000 seeds, half with the default timing: 21,756,164 ms under
a lease, no violation); its negative controls find violations when backups
ignore promises (first at seed 74) and when clocks drift up to 90 % (seed
298). The process test
`a_cut_off_primary_loses_its_read_lease_then_steps_down` holds the lease in
every sample of 500 ms in steady state, and after both backups are killed
sees it lapse in 15 ms and the primary step down.

## Membership change

**The choice: epochs, a single committed reconfiguration entry per change,
no joint consensus.** Joint consensus (Raft §6) needs every decision during
the change to count two quorums, and single-server changes (Raft's
dissertation) need configurations to take effect when appended, with a
fix for a known bug; both change how every view change in VSR picks its
log, and VSR's deterministic primary per view would have to be computed
from a configuration that differs between members mid-change. Instead,
membership is fixed within an epoch, and a change
switches epochs at a known log position, as Viewstamped Replication
Revisited's reconfiguration does:

1. The primary proposes a reconfiguration entry naming the next epoch's
   members and addresses, as the last entry of its epoch: it proposes
   nothing after it (sealed). Shards' sections wait meanwhile; a view
   change that drops an uncommitted entry abandons the change.
2. The entry commits like any other, on a quorum of the old members.
3. A member that learns it committed enters the next epoch. A member of the
   new configuration that holds the log through the entry changes to the
   epoch's first view by an ordinary view change among the new members.
   Its first view is led by the first member of the new list: the old
   primary when it stays. Members not in it retire.
4. A member of the new configuration without that prefix (a new member, or
   one that lagged) fetches it from any member, or a member the change
   removed, verified against the entry's checksum, or takes a checkpoint
   image from any of them; it votes only once it holds it.
5. A member that missed the change learns it from any message of a later
   epoch (it asks), and a member answers a message of an earlier epoch
   with the entry, so a removed member retires even if it heard nothing.

**Restriction.** A quorum of the new members must be members now, so one
step adds or removes members (3 → 5, 5 → 3), or replaces a minority; a dead
member is replaced by naming its successor in its place. A change also
needs a quorum of the old members up (to commit the entry) and, to start
the new epoch, a quorum of the new members holding the log, which a removed
member can serve. Keep removed members running until the new members
report the new epoch. Like a failover, a change stops the primary's engine
and reopens it on the first member of the new list.

**Why it is safe.** Within an epoch nothing changes. Across the boundary:
every committed entry of the old epoch is at or before the reconfiguration
entry (rule 9), every voter in the new epoch holds the log through it
(rule 10), and the new epoch's first view starts from a quorum of the new
members, so its log contains every committed entry; later views of the new
epoch are ordinary.

**Checked.** `spec/FuseeReconfig.tla` adds the epoch handoff to the
replication model (replace a member: 1,662,865 distinct states; grow 3 →
4: 1,515,475; no violation of `CommittedNeverChanges` or
`CommittedSurvives`); dropping the seal rule fails in 14 steps, dropping
the hold rule in 10. The simulator test
`membership_changes_survive_crashes_during_reconfiguration` changes
membership under crashes, partitions, lost and duplicated messages, damage
and clock drift, with leases checked throughout: 3 → 5 → 3 (5 → 7 → 5 for a
third of the seeds), then a member lost for good and replaced, with
requests sent to the primary directly or through a backup and idle periods
in which only heartbeats run. Every change must complete after the faults
stop, removed members retire and every final member holds every committed
entry. The process test
`membership_changes_three_to_five_to_three_and_replace_a_lost_member` does
the same on real nodes, with workflows checked across all four epochs.

**Using it.** Start each new member with an empty directory and
`--join` (`ReplicaConfig::join`), giving `--replicas` the members'
addresses (`id=host:port,...`). Then, on any member:

```bash
fusee replicas --dir /var/lib/fusee status
fusee replicas --dir /var/lib/fusee set 0=10.0.1.10:7400,1=10.0.1.11:7400,2=10.0.1.12:7400,3=10.0.1.13:7400,4=10.0.1.14:7400
```

or `ReplicaNode::change_members(members, timeout)`. The membership is saved
in `vsr/members`; once a directory has one, it wins over `--replicas`.
Under mutual TLS a member's certificate must name the host of its new
address, as for the initial members.

## Commit on any durable quorum

The primary's own log write runs in the background, and it releases an
entry (opens the step gate, lets outputs leave) as soon as the entry
commits: when any quorum of the view holds it durably, with or without the
primary itself, so a slow primary disk does not slow commits. What the
primary keeps to `min(commit, durable)` is materialization: the
checkpoint images it builds and, in a dedicated server's groups, the
segment files it writes.

**Why this is safe.** A committed entry must survive every later view.

1. *Quorum intersection.* The entry is durable on a quorum Q of view v.
   Any later view starts from a view-change quorum Q', which intersects Q
   in at least one replica, r. Until v ends, r's log is a prefix-compatible
   copy of the primary's (rule 3); r is normal in v, so its last normal view
   is v or later, and the new primary adopts the log with the highest
   (last normal view, length) among Q', which is at least as long as r's in
   view v and contains the entry (rule 6 makes the adoption atomic; rule 4
   keeps every backup's committed prefix). Induction over views carries the
   entry forward. Nothing in this argument uses the primary's own
   durability: the commit quorum may exclude it.
2. *The primary's lost tail.* What the primary released but did not flush
   is not on its disk after a crash. Rule 2 covers exactly this case: a
   restarted primary never leads its old view again. It rejoins in
   view-change status, cannot become primary of a view without a
   view-change quorum (which, by 1, brings it the entry), and as a backup
   it fetches what its log lacks. Its recovered commit hint never exceeds
   what its log held when the hint was written, so it never claims an entry
   it lost.
3. *Materialization.* A checkpoint image covers only entries that are
   committed, released and durable on the replica that builds it, so an
   image never anchors an entry the builder's own log could lose.

**Checked.** In `spec/FuseeReplication.tla` the `Commit` action takes any
quorum, with the primary's own durable length counted only if the primary
is in it. TLC checks it (`MC.cfg`, three replicas, three operations, three
views, two crashes): no violation of `CommittedNeverChanges` or
`CommittedSurvives`. A witness configuration (`MC-witness.cfg`) adds the
invariant that a primary never commits past its own durable prefix; TLC
violates it in nine steps, a primary committing an entry only its two
backups hold, so the checked state space does contain the behavior.
The simulator test
`a_slow_primary_disk_never_loses_entries_a_backup_quorum_committed` runs
every seed with the primary flushing 10 to 100 times less often than its
backups: 5,000 seeds committed 12,843,442 entries, 5,519,235 of them before
the primary held them durably, with 66,046 crashes and no violation.
`a_slow_primary_disk_does_not_slow_commits_and_loses_nothing` (process
test) gives the primary a 40 ms flush: steps still commit in about a
millisecond, the primary is killed with its log behind, and nothing
acknowledged is lost.

## Where a step's time goes

With timings enabled (`ReplicaObserver::enable_timings`; the benchmark
harness enables them in replicated mode and adds the primary's to every
result line as `replica_stages`), each member records the stages of a
step's critical path (`replicated/timings.rs`), and
`bench/aws/check_cluster.py` lays them out per trial:

| Stage | Member |
| --- | --- |
| `shard_stage_to_take`: a shard hands its section to the I/O thread, which takes it | primary |
| `take_to_prepare_sent`: encode, write the Prepare to every backup's socket | primary |
| `prepare_sent_to_ack_read`: until the read that brings the acknowledgment completing a quorum | primary |
| `prepare_read_to_write`, `protocol_log_append` (write; with `RWF_DSYNC` the flush too), `protocol_log_sync`, `durable_to_ack_sent`; `prepare_read_to_ack_sent` the whole turnaround | backup |
| `ack_read_to_release`: commit published to the shards | primary |
| `release_to_shard_resume`: the shard sees its records committed | primary |
| `shard_resume_to_stage`: workflow code and the next record (its p50; the mean includes gaps between workflows) | primary |
| `io_thread_tick`: the I/O thread's timer work (heartbeats, timeouts, checkpoint images); `io_thread_lock_hold`: each hold of the protocol lock by its main task | any |

Network time is `prepare_sent_to_ack_read` less the faster backup's
turnaround.

**How the path is kept short.**

- **The lone shard proposes its own section.** When nothing else is
  staged or queued and at most two shards worked in the last 2 ms
  (`fusee::spin::few_busy`), a shard that stages a section takes the
  protocol state without waiting (it is behind a mutex the I/O thread
  holds while it works) and runs the round itself: propose, send the
  Prepare, hand the primary's write to its helper. The I/O thread, woken
  by the acknowledgment, releases the commit.
- **The Prepare goes out before the primary's own write is handed off**
  (the helper's wake-up is not on the commit path).
- **A finished primary write wakes the I/O thread only if a quorum waits
  for it** (`Replica::commit_waits_on_own_durability`) or more frames wait
  to be written; otherwise the next acknowledgment or tick finds it.
- **Group commit.** With an entry proposed and uncommitted, staged
  sections wait and go out together in the next entry when it commits; an
  idle group proposes at once. A second entry (up to four) goes out only
  while the oldest has waited one and a half times a usual round, at least
  40 µs after the last, and a round that reads the acknowledgment committing
  an entry proposes what staged meanwhile in the same round. Each shard
  keeps up to four sections in flight, so under load its next records join
  the next entry instead of waiting for the commit.
- **The checkpoint check is off the commit path.** An index grows as
  entries commit (`log3::Uncovered`) and tells the I/O thread where each
  shard's records start past its checkpoint; the image check runs when a
  checkpoint is pinned, a build finishes, or every 25 ms, so
  `io_thread_tick` stays short however long the log since the last image.
- **`O_DIRECT` and `RWF_DSYNC` for `vsr/log`** (Linux, `direct_io`): one
  system call per write and no page cache.
- **Busy-polled receive** (`FUSEE_REPLICA_BUSY_POLL_US`, off by default):
  the I/O thread keeps polling its sockets that long after any activity.
  On hosts whose network interface moderates receive interrupts
  adaptively, turning that moderation off (`ethtool -C <nic> adaptive-rx
  off`) lowers the step's p99 more.

**Throughput.** On three i8g.2xlarge in one placement group, a group
sustains 1.3 million steps a second with step p99 under 1 ms
([benchmark](benchmark.md#replicated)).

## The protocol log on disk

`vsr/log` holds checksummed entry frames in prepared 1 MiB extents. On
Linux with `LocalOptions::direct_io` (the default) it is written through
`O_DIRECT`, with `RWF_DSYNC` under `FlushMethod::Dsync` (no separate
flush), in whole 4 KiB blocks from aligned memory, the shared log's way:
the log keeps its last partial block and the next write starts at that
block and writes it again, with the same bytes, ahead of its own; two
writes in flight never share a block, because a write that shares the
previous one's last block starts only after it completed
(`replicated/writes.rs`, the shared log's `batch_span` rule). Extents are
zero-filled through `O_DIRECT` and flushed when prepared, so writes on the
commit path overwrite allocated blocks and change no metadata. A
filesystem that refuses `O_DIRECT` (`EINVAL` at the open or at a write)
moves the log to buffered writes and `fdatasync` for good, with one line
on stderr; the refused write is written again buffered (it transferred
nothing or a prefix of the same bytes). A torn rewrite of a shared block
can only leave old or new bytes in each sector, and its earlier frames
are the same in both. The superblock, commit hint and membership file
stay buffered.

**Checked.** `writes_keep_every_durable_entry_in_any_crash_state` (unit
test) writes 400-entry logs packed and in whole blocks, with up to two
writes in flight as the primary's background writes run them, and crashes
each of 600 runs at a random point with every write in flight landed,
lost, or torn at random 512-byte sectors: every entry of a completed write
reads back and nothing read back differs from what was written, including
padding shorter than a frame's length field before a block boundary.
`a_refused_direct_write_falls_back_to_buffered_writes` and
`the_protocol_log_writes_through_o_direct_where_the_filesystem_allows`
(which asserts the `O_DIRECT` descriptor on Linux) cover the rest; the
replicated tests run with it on Linux.

## Storage and verification

- **Simulator** (`FUSEE_VSR_SEEDS=5000 cargo test --release -p fusee-core
  vsr`, eleven tests, 5,000 seeds each where they take seeds): 12,794,098
  entries committed, 65,743 crashes, 13,142 damaged-entry repairs and
  585,387 checkpoint publications or installations, with message drops,
  duplicates, reordering, partitions and torn unflushed writes; slow
  primary 12,775,055 entries, 3,938,552 of them committed ahead of the
  primary's own flush; default timing 13,551,619 entries over 112,034
  views; drift 21,756,164 ms under a lease; membership changes 9,179,100
  entries, 15,000 epochs entered, 8,138 requests through a backup, 47,646
  crashes; no violation.
- **TLA+:** TLC checked 5,815,792 distinct states of the bounded
  replication model, and the lease and membership models above.
  [Configuration and results](../spec/README.md).
- **Storage:** `fusee_server::replicated`. The log is replicated, not a
  shard: one protocol entry is one shared-log batch (`replicated/log3.rs`),
  a section of canonical records from every shard of the primary's engine
  that had records, behind a small header naming the group's shard count
  (the entry's op, view and chain checksum stand in for the batch header).
  One group therefore serves all of the primary's shards: one per core up
  to eight for a new group, and afterwards the count its log was written
  with (`ReplicaNode::engine_config` sets it; keep it). The primary's
  engine stores each shard through `ReplicatedEnv`: a shard's flush
  completes once the entry holding its section commits. **One copy on
  disk:** the protocol log is the only log. A new primary's engine recovers
  each shard from the committed entries after its checkpoint, as a local
  engine recovers from the shared log; no replica materializes segment
  files. Each replica keeps `vsr/log` (checksummed entries in prepared
  1 MiB extents, replaced atomically during repair), `vsr/superblock`
  (saved after the log it vouches for), `vsr/commit` (a 12-byte commit
  hint rewritten in the background every 200 ms, so a damaged committed
  tail is repaired rather than truncated) and `engine/shard-NNN/` (each
  shard's run store and checkpoints).
- **Dedicated servers' groups** keep one shard per group, with flush
  batches materialized into `engine/shard-000/log`, a periodic synced hint
  and a checkpoint barrier, because their remote workers recover by
  reading those segments. A group's first engine decides its kind, and a
  dedicated server refuses a group that an embedded engine already wrote.
- **I/O thread:** one current-thread runtime per member reads every peer
  socket, runs the protocol and owns the protocol log. An inbound frame is
  handled on the task that read it; there is no inbound channel. Every
  round ends with at most one socket write per peer (frames coalesced; a
  Prepare encoded once for all backups) and one log write. Shards hand the
  I/O thread their sections (`log3::Batcher`) and wake it only if it is
  parked, or a lone shard runs the round itself
  ([Where a step's time goes](#where-a-steps-time-goes)); the round
  proposes everything staged as one entry, several entries in one Prepare
  when permit commands are waiting too, waiting for the entry in flight to
  commit unless its round runs slow (group commit, above). On commit it
  publishes each shard's committed LSN and wakes the shard, which spins
  briefly before it parks (the engine's spin-then-park). Right after such a
  release the I/O thread spins in turn for the shards' next section,
  polling its sockets without blocking meanwhile, within the budget of
  `fusee::spin::Budget` (twice how soon sections followed recent releases,
  at most 50 µs, never on hosts with two or fewer cores), then parks; a
  wait for acknowledgments parks at once and the reactor wakes the reader.
  The primary's own log write and flush run on two helper threads
  (`replicated/writes.rs`); while the log is busy every write is padded to
  the end of its 4 KiB block (`fusee_core::wal::batch_span`, the shared
  log's rule), so the next can start at once and two writes in flight never
  share a block. Backups write inline: their flush is on the commit path.
  Permit commands and a dedicated server's batches and checkpoint barriers
  reach the I/O thread through bounded queues (1,024 and 16 entries); the
  I/O thread never sends into them and drains them without waiting for a
  producer, a peer or a commit, so no cycle of waits exists. Status
  receivers are woken only when the role, view, readiness or stop state
  changes; progress fields update in place and are read by polling.
- **End-to-end test** (`crates/fusee-server/tests/replicated.rs`): three
  replicas; the primary runs workflows and is killed; a backup becomes
  primary, opens the engine from its own log, and the workflows finish with
  their recorded steps not run again; the old primary restarts and catches
  up as a backup. Other tests there: every shard fails over from the only
  copy of the log (no segment file anywhere), a slow primary disk (above),
  an engine reopened by the same primary, checkpoint images of every shard
  installed by a lagging member.
- **Linux partitions** (`bench/faults/partitions.py`, three containers,
  primary isolated, ±90 s clock skew, 20 ms delay, 2 % loss, 1 %
  duplication): all 1,000 acknowledged workflows completed with 3,000
  consistent external keys, and every member installed a checkpoint image;
  under a two-permit limit all 60 completed with at most two concurrent
  effects across all three members.
- **Real processes** (`fusee-crashtest replicated-driver <dir> <kills>
  <workflows> stream`): three replica processes; the primary runs workflows
  whose steps call an external-service log, and starts another every 2 ms;
  the driver SIGKILLs whichever process is primary, restarts it, and
  repeats. 50 runs of 20 kills, 1,000 primary SIGKILLs: 5,000 acknowledged
  workflows, all finished; 152,085 service calls, no key called with two
  different inputs.
- **Limits:** one group per primary; a dedicated server's groups are
  single-shard with materialized segments; hot standby keeps a promoted
  backup's engine open but its shards still replay the records since their
  checkpoint (from memory, see [Failover timing](#failover-timing));
  membership changes one quorum-preserving step at a time, and the
  TypeScript and Python SDKs do not expose them (the CLI and
  `ReplicaNode::change_members` do); a group's first checkpoint waits for
  every reachable configured member to advertise support (see below); a
  group without TLS and a token ([Security](#security)) requires an
  isolated trusted network.

```rust
let mut node = ReplicaNode::start(ReplicaConfig { id, peers, join: false, dir, local, timing, security }).await?;
loop {
    node.wait_ready().await;                       // primary, caught up
    let engine = Engine::open(node.engine_config()).await?;
    register_workflows(&engine);
    node.wait_lost().await;                        // deposed: stop work
    let _ = engine.shutdown().await;
}
```

## Security

Members authenticate each other with mutual TLS 1.3, a shared token, or both
(`ReplicaConfig::security`, a `fusee_server::security::Security`; the
default is neither, which keeps the plain protocol and prints a warning for a
non-loopback replication address). With the CLI:

```bash
fusee server --dir /var/lib/fusee --listen 10.0.1.10:7600 \
  --replicas 10.0.1.10:7400,10.0.1.11:7400,10.0.1.12:7400 --replica-id 0 \
  --tls-cert member-0.pem --tls-key member-0.key --replica-tls-ca group-ca.pem \
  --token-file /etc/fusee/token
```

`--tls-cert` and `--tls-key` also secure the worker listener; `--tls-ca`
(instead of `--replica-tls-ca`) also requires worker certificates.
`--replica-token-file` gives the replicas their own token; it defaults to
`--token-file`. An embedded member (the TypeScript and Python `replicated`
durability) takes the same settings as the `address` option:
`?cert=PATH&key=PATH&ca=PATH&token-file=PATH`.

- **Mutual TLS.** Every stream is TLS 1.3 with a certificate in both
  directions, verified against the group CA. A member's certificate must be
  valid for the host of its own replication address (its DNS name or IP
  address) and, if it lists extended key usages, allow both server and
  client authentication. A member that dials a peer verifies that host. A
  member that accepts a stream admits it only for the members whose hosts
  the certificate names, so a certificate the same CA issued to a worker, or
  for any other name, never speaks for a member.
- **Token.** Before its first frame, every stream runs the token exchange of
  [server mode](server-mode.md#security) with purpose 2 and the dialler's
  member ID in its hello. The listener then accepts only frames carrying that
  ID, and under TLS only an ID the certificate names. A proof made for the
  worker protocol is never accepted here, nor the reverse.
- A stream that fails either check is closed before one frame reaches the
  protocol. A frame that claims another member than its stream's closes the
  stream. A dialler whose handshake is refused retries every 500 ms; failed
  handshakes are logged at most once every ten seconds.
- TLS handshakes of inbound streams run on the node's general runtime, not
  on the I/O thread, so connection attempts never take its time.
  Established streams are read on the I/O thread. Replication
  traffic counters count plaintext bytes.

Members holding the group's credentials are trusted alike: a member's key
can speak for every member its certificate names, and a token alone for any
member. A token without TLS authenticates each stream's start but does not
encrypt or protect the traffic after it.

## Replica checkpoints

After a workflow checkpoint is durable, the primary associates its last LSN with a committed protocol entry. It builds an immutable image containing the workflow checkpoint, its run-store prefix, retained history archives, the boundary segment through that entry, and the permit table at the same operation. Image copies and SHA-256 validation run off the protocol thread. A generation pointer selects the image, verified log suffix and applied watermark together. Recovery rebuilds mutable engine files from the image and replays the suffix.

A lagging member fetches pages from the established primary or the donor selected by the view-change quorum. It keeps its old generation until the image and every locally committed position above it are accounted for. A healthy backup also keeps prepares it acknowledged in the current view. A member with a damaged image retains its saved view and commit watermark and refrains from voting until it receives a verified replacement. A group that loses every copy of a committed value cannot reconstruct it.

Compaction never waits for a member's progress: a member below the new base installs the image. Only a group's first checkpoint waits: the primary waits until every configured member has advertised checkpoint support in its Hello (sent every 500 ms). A member that has neither sent a frame nor kept the primary's connection open for 30 seconds is down or cut off and no longer holds the log; when it returns it takes the offered image and a verified suffix like any lagging member. A reachable member that never says Hello still holds the first checkpoint back. Once a replica holds a generation, its compaction waits for nobody. This rule decides when compaction may start, never what it drops: an image covers only committed entries and is durable before the log is trimmed.

A long run of permit or connection records can also advance the checkpoint when no workflow executes. The primary copies the previous immutable engine image, verifies the original SHA-256 while reading it, and substitutes the lease state and protocol position for the new committed prefix. With no earlier workflow records, the image contains only the lease state. A primary creates this empty-engine format only after every member that has said Hello advertises support, under the same reachability rule. An application reopening such a group starts with an empty engine and its previously committed limits. Workflow batches after the checkpoint replay normally.
