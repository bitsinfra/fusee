# Operations

## Data directory

```
<dir>/fusee.meta                      format, shard count and log format
<dir>/wal/<segment>.wal                the shared log (log format 3): segments
                                       from 1 MiB doubling to 64 MiB, reused
<dir>/shard-000/log/<segment>.seg      per-shard log segments (log format 2)
<dir>/shard-000/runs-<gen>.dat         run store (parked and finished runs)
<dir>/shard-000/checkpoint-<lsn>.fck   the two newest checkpoints (a second one
                                       of the same position: <lsn>b.fck)
<dir>/shard-000/visibility.redb        rebuildable search-attribute index
<dir>/shard-000/archive/              immutable retained journals, when enabled
<dir>/reshard.new/, reshard.old/       only while `fusee reshard` runs
```

The shard count is set when the directory is created. An engine opened with
another count refuses the directory and names the command that changes it
(see [Changing the shard count](#changing-the-shard-count)); the TypeScript,
Python and Go SDKs pass their `shards` option to the same check.

## Tools

- `fusee disk-check <dir> [seconds]`: flush latency of the disk under `dir`
  (`--json` for machine-readable output).
- `fusee inspect <dir>`: log segments (the chain's batch ranges, records per
  shard, torn tails, spares), checkpoints and the log position each covers.
- `fusee upgrade <dir>`: converts a closed log-format-2 directory to the
  shared log (see Format versions).
- `fusee reshard <dir> <shards>`: changes a closed local directory's shard
  count (see Changing the shard count).
- `fusee restore <target> <dir> [--at <time>]`, `fusee backups <target>
  [--verify]`, `fusee backup <dir> <target>`: continuous backup (see Backup
  and restore).
- `fusee server ...`: server mode. Workers on the server's host can connect
  to its Unix socket with `?shm=1` to send log records through shared
  memory ([server-mode.md](server-mode.md#shared-memory)); each such worker
  takes about 4 MiB of shared memory (unnamed, gone with both processes),
  and the server needs to read the socket's peer credentials.

## Recovery

On open, each shard loads its newest valid checkpoint, reads the run index
it names (32 bytes per parked run, written into the run store with each
checkpoint: a base, then what changed), and replays log records after it.
Once the deltas outweigh the base (half its bytes, or at least 64 delta
frames and eight per 256 KiB of it), a new base is written between checkpoints, a few frames per
100 ms sweep, and the first checkpoint after it is complete names it with
the deltas written meanwhile; the shard thread never rewrites the whole
index at once, except after a run-store compaction.
A checkpoint from before the index (format 2) makes the open scan the whole
run store prefix instead; the first checkpoint after it rewrites the store
in the current format. The shared
log is read once, from its oldest retained segment, and each shard decodes
only its records after its checkpoint. A torn tail (a flush that never
completed) is discarded and the next write resumes in place after the last
valid batch; damage to acknowledged records stops the open with an error
naming the batch. Segments every shard's two retained checkpoints cover are
retired: their header is zeroed and the file is kept as a spare that a later
roll renames and reuses, so a steady workload creates and zero-fills no
files. A clean shutdown checkpoints every shard (`Config::checkpoint_on_shutdown`)
and keeps a second copy of that checkpoint; with every shard's records then
covered by two files, the log moves to an empty segment and retires the
rest, so the next open reads the log's header and no batches, however large
the log had grown (a directory with a continuous backup keeps its segments).
An open that replays a tail checkpoints soon after, so a crash loop does
not grow the log. A failed flush stops the
shard (Linux may already have dropped the dirty pages); restart the process.

## Replicated groups

A replica's directory (see [replication.md](replication.md)):

```
<dir>/vsr/log, vsr/superblock, vsr/commit   protocol log, view, commit hint
<dir>/vsr/members                           the membership, once it has changed
<dir>/vsr/admin.sock                        local admin socket (fusee server)
<dir>/engine/shard-NNN/                     each shard's run store and checkpoints
```

**Failure detection.** `fusee server --heartbeat-ms 5 --election-ms 40`
(the defaults). A backup suspects a silent primary after 40 to 80 ms, and
a crashed one (its sockets closed) after about 20 ms. Raise the election
timeout on hosts whose scheduling or disks stall for tens of milliseconds:
an election timeout shorter than such stalls causes needless elections. A
primary that hears from no quorum for four election timeouts
(at least 200 ms) steps down.

**Read leases** assume each host's clock rate stays within 1 % of real
time; the clock used counts suspended time. Do not restore a replica's VM
from a memory snapshot while it may still be primary.

**Hot standby.** An embedded member that opens its engine with
`ReplicaNode::standby_config()` while it is a backup takes over without
opening an engine when it is promoted; open the next one when it loses
the role (see replication.md, Failover timing).

**`vsr/log` on Linux** is written with `O_DIRECT` and `RWF_DSYNC` when
`direct_io` is on (the default); a filesystem that refuses `O_DIRECT`
gets buffered writes and `fdatasync`, with one line on stderr. Put
`<dir>` on a filesystem that supports it (ext4 or XFS on the device).

**Stage timings.** `ReplicaObserver::enable_timings` records where each
replicated step's time goes on every member (replication.md, Where a
step's time goes); `fusee-bench` enables them in replicated mode.
`FUSEE_REPLICA_BUSY_POLL_US` (off by default) keeps the I/O thread
polling its sockets that long after any activity: measure it before using
it, and only on hosts with cores to spare.

**Membership.** `fusee replicas --dir <dir> status` prints a member's role,
view, epoch, commit and the members; `fusee replicas --dir <dir> set
id=host:port,...` changes the membership from any member (it answers once
that member has entered the new epoch, or after 30 seconds). One change
adds or removes members or replaces a minority: a quorum of the new list
must be members now.

- *Add members* (3 → 5): start each new member with an empty `--dir`,
  `--join`, its `--replica-id` and `--replicas` listing at least the
  current members (`0=a:7400,1=b:7400,2=c:7400,3=d:7400,4=e:7400`). Then
  `fusee replicas --dir <dir> set` with all five on any member. A joining
  member's status says `joining` until it is added.
- *Remove members* (5 → 3): `set` with the three that stay. The removed
  members report `retired`; stop them once `status` on the others shows the
  new epoch with their commit caught up.
- *Replace a dead member*: start its successor with a new ID and `--join`,
  then `set` the surviving members plus the successor. The surviving
  members must be a quorum of the new list (two of three).

The primary's engine stops at each change and reopens on the first member
of the new list (the current primary if it stays), as after a failover.
Under mutual TLS, a new member's certificate must name the host of its
address. Once `vsr/members` exists it overrides `--replicas`.

## Format versions

Segment headers, the run store and checkpoints carry format versions. A
build refuses data newer than it understands with a message saying so. A
release that changes a format converts old data with a command or refuses
it clearly.

New local directories use log format 3 (`log=3` in `fusee.meta`): one
shared log for every shard. `Config::shared_log(false)` creates a directory
with a log per shard (format 2) instead. To convert a
closed format-2 directory (a local one, or a server-mode one with its
server stopped), run `fusee upgrade <dir>` (or call
`fusee::upgrade::upgrade_to_shared_log`). It copies every valid record of
every shard into a new shared log under `wal.tmp`, syncs and renames it to
`wal`, switches `fusee.meta` to `log=3`, and only then deletes the old
segments, so a crash at any point leaves a directory that opens: before the
switch in format 2 (the next attempt removes the partial `wal`), after it in
format 3 (the next attempt removes leftover segments). Run stores,
checkpoints and archives are kept as they are. The tool refuses a log with
damaged acknowledged records instead of copying around them. New server-mode directories
use log format 3 too (see [server-mode.md](server-mode.md#server-directories));
replicated directories keep their own logs and are not converted.

## Changing the shard count

```
fusee reshard <dir> <shards>
```

(or `fusee::reshard::reshard(dir, shards, &ReshardOptions::default())`)
changes the shard count of a local directory in log format 3 while nothing
has it open; the tool takes the directory's lock. It grows and shrinks.
Opening the directory afterwards continues every workflow where it was. It
writes a copy of every run store and history archive, so it needs that much
free space, and takes time in proportion to them. Resharding is offline:
stop the engine, run `fusee reshard`, and open it with the new count; the
stop lasts as long as the tool takes to copy the run stores.

**What moves.** Every run the old shards know goes to the shard its workflow
ID hashes to under the new count: running runs, suspended (parked) runs and
finished runs still within retention, each with its whole journal. Timers,
buffered signals, accepted updates and their results, search attributes and
async steps waiting for a completion are journal records, so they move with
their run. A run that was in memory at its shard's last checkpoint is in
memory in the new shard's checkpoint and its code resumes at the next open (a
step that was running when the engine stopped runs again, as after any
restart); a parked run stays parked with its timers. Messages between shards
that were not acknowledged, including those a log tail regenerates at
recovery, move to the new shards' outboxes and are sent again; receivers drop
repeats as they always do (child starts attach, signals keep their
deduplication keys, child results for a finished wait are acknowledged).
History archives are rewritten into the shards their IDs hash to.
Schedules, debounce and batch coordinators are workflows and move like any
other. The search-attribute index is rebuilt, and the new shards start a new
shared log.

**Run numbers.** Run numbers count per shard, so runs from different old
shards can share one. In each new shard a run keeps its number unless
another run took it first (runs whose workflow is unfinished are placed
first) or an older run of the same workflow ID had to move; the others get
numbers above every number any old shard had used, in their old order, and
later starts number from there. Growing to a multiple of the old count
renumbers nothing, because each new shard draws from one old shard. Every
reference to a run follows it: a child's parent, a run's predecessor, the
next run a continue-as-new names, the parent in an unacknowledged message. A
reference to a run that no longer exists (its retention ended) is rewritten
to one that matches no run. Two things outside the directory name a run
number and change for a renumbered unfinished run: its `Ctx::key` values
(`<workflow ID>/<run>/<position>`, so a step that runs again after the
change offers a service a different idempotency key) and its completion
tokens. The command lists each renumbered unfinished run, and for every
async step still waiting, the token handed out before and the one the step
answers to now (`ReshardReport::renumbered_unfinished`); an old token is
refused ("the workflow run this token belongs to is gone").

**Crash safety.** The new shards are built under `<dir>/reshard.new` and
synced; until then the old directory is untouched except by the engine's
own recovery, which reads it. The switch is `fusee.meta`, replaced
atomically by one with `format=reshard` and the new count, which no build
opens: an engine refuses it with a message to run the command again. Then the
old `wal` and shard directories move into `<dir>/reshard.old`, the new ones
move in, a final `fusee.meta` names the new count, and `reshard.old` is
deleted. A crash before the switch leaves the directory as it was (the next
run deletes `reshard.new`); a crash during it leaves a directory the next
`fusee reshard <dir> <n>` finishes, every step checking what is done; a
crash after the final `fusee.meta` leaves leftovers the next run deletes.
`fusee reshard` with the count the directory has only does that cleanup.

**Invariants** (checked by `crates/fusee/tests/reshard.rs`):

1. Every run in an old shard's run table is in exactly one new shard, the
   one its ID hashes to, with the same journal except for run numbers, and
   in the same state: in memory if it was at the old checkpoint and has not
   finished, else parked with the same timers (a finished run is parked,
   as any open parks it).
2. Run numbers are unique within each new shard; among the runs of one
   workflow ID the newest has the highest number (the one its ID finds);
   every new shard's next number is above every number an old shard used,
   so a stale token or key never names a different run.
3. Every reference to a run names the same run after the change, or no run.
4. Every unacknowledged message is in exactly one new outbox: a child start
   in its parent's shard (which drops it when the child's result arrives),
   any other message in its target's shard. Deduplication keys of signals
   are unchanged.
5. Every moved record is at or before the new shards' checkpoint position
   (the end of the longest old log), so nothing waits for a position that is
   never written.
6. Before the switch the directory opens with the old count and holds
   everything it held; during it nothing opens it; after it, it opens with
   the new count.

| Failure | Result |
| --- | --- |
| Crash or error while reading or building | The directory opens as before; the next run deletes `reshard.new` and starts again |
| Crash during the switch | Engines refuse the directory, naming the command; running it again (with any count) finishes the switch first |
| Crash after the final `fusee.meta` | The directory opens with the new count; the next run deletes `reshard.old` |
| An old shard does not recover (damaged checkpoint or log) | Refused before the switch, with recovery's error |
| Disk full while building | Refused before the switch |
| The directory is open elsewhere | Refused: the directory lock is held |

Not covered: directories in log format 2 (refused: run `fusee upgrade`
first), replicated directories (refused), and server-mode directories, which
have the same layout but are untested with the tool; stop the server first
in any case. A continuous backup refuses a target written for another shard
count: give a resharded directory a new target. The tool reads the old log
without a backup attached, so like any open without one it may retire
segments every checkpoint covers; if a reshard is abandoned and the old
directory goes back to its backup, the backup continues in a new timeline.

## Backup and restore

A local directory in log format 3 can be backed up continuously to object
storage, and restored to a point in time on another machine. Server-mode
and replicated directories are not covered (a replicated directory's
other replicas are its copies).

```rust
use fusee::backup::{BackupOptions, LocalTarget};
let target = std::sync::Arc::new(LocalTarget::new("/mnt/backup/orders")?);
// or, with the `s3` feature:
// let target = Arc::new(fusee::backup::s3::S3Target::from_url("s3://bucket/orders")?);
let engine = Engine::open(Config::local("./data").backup(target, BackupOptions::default())).await?;
let point = engine.backup().unwrap().sync().await?; // everything acknowledged so far
```

A background thread uploads every sealed segment of the shared log as soon
as the chain moves past it, the active segment's acknowledged prefix every
`upload_interval` (1 s by default), and every completed checkpoint with the
part of the run store it names that is not uploaded yet and the history
archives it covers. The write path does not wait for any of it: the backup
reads files that are already durable, and the log keeps a segment the
backup has not uploaded from being retired (`fusee inspect` shows it as part
of the chain). While the target is unreachable, sealed segments therefore
stay on local disk; the backup retries with backoff, says so on stderr and in
`Backup::status()`, and catches up when the target returns.
`Engine::shutdown` makes a last upload of everything and waits for it.

| Option | Default | Meaning |
| --- | --- | --- |
| `upload_interval` | 1 s | Upload the active segment's acknowledged prefix this often. `None`: only sealed segments |
| `resolution` | 1 s | Record a restore point this often while the log is written |
| `retention` | 7 days | Keep restore points, and the objects they need, this long |

**What can be lost.** With `upload_interval` set, the log acknowledged since
the last upload: about one interval plus the time an upload takes. With
`None`, at most the active segment (up to the configured segment size, 64
MiB by default). A clean shutdown loses nothing.

**Restore points.** Every `resolution`, the backup reads the log's
acknowledged position together with the time; once the log is uploaded
through it, that pair is a restore point. Restoring it rebuilds exactly the
batches acknowledged at that time: every workflow start, step and signal
acknowledged by then is there, nothing acknowledged later is. `fusee
restore <target> <dir> --at <time>` (or `fusee::backup::restore`) picks the
newest restore point at or before the time (RFC 3339, Unix milliseconds, or
`-15m` before now; without `--at`, the newest) and writes a data directory
that opens and recovers like one after a crash at that moment: runs that
were suspended resume, timers fire, finished results are readable. The
directory is built under a hidden sibling (`.<dir>.restoring`) and renamed
into place when complete, so an interrupted restore leaves no partial
directory and a rerun starts over. `<dir>` must not exist or be empty.
Points older than ten minutes are thinned to one per minute; for points
older than an hour the backup keeps one checkpoint per shard per hour, so
restoring one replays up to an hour of log.

**Targets.** A directory (`LocalTarget`, or `file:///path` and a plain path
on the command line), or with the cargo feature `s3` (on in the `fusee`
command) an S3-compatible bucket: `s3://bucket/prefix?region=eu-west-1`, plus
`endpoint=http://127.0.0.1:9000&path_style=1` for MinIO and other stores and
`ca=/path/ca.pem` for a private CA. Requests are signed with Signature
Version 4 from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and
`AWS_SESSION_TOKEN`; HTTPS uses TLS 1.3 and the system's CA bundle (or
`SSL_CERT_FILE`). Other stores implement `fusee::backup::Target`: put, get,
list and delete of immutable named objects, and an atomically replaced
manifest. One directory writes to a target (prefix) at a time: a backup
whose manifest write finds a manifest it did not write stops.

**Layout.**

```
manifest.json                                    replaced atomically after each upload
state/<version>.json                             snapshot of the backup's state
<timeline>/log/<segment>/<offset>-<end>.wal      whole batches of one log segment
<timeline>/shard-NNN/checkpoint-<lsn>-<ms>.fck   a checkpoint, as written
<timeline>/shard-NNN/runs-<gen>-e<epoch>/<offset>-<end>.dat   run store bytes
<timeline>/shard-NNN/archive/<name>.arc          history archives
```

The manifest names a snapshot and the events since it (log chunks,
checkpoints, store chunks, archives, restore points, with their CRC-32C);
a new snapshot is written every 128 events and after retention. Small log
and store chunks are merged as they accumulate (eight of one size into one
of the next, up to 16 MiB), so a quiet log does not leave an object per
upload. A checkpoint is usable for a restore point when it had completed
before the point's batch was acknowledged; a point is restorable when every
shard has a usable checkpoint (or the log reaches back to its first record)
and the log from the oldest one is uploaded without a gap.

**Crashes.** Objects are uploaded before a manifest names them and deleted
only after a manifest that no longer names them is in place, so the
manifest names only complete objects whenever the process dies. The next
open deletes objects no manifest names and continues right after the last
batch the manifest has, after checking that the local log still holds that
batch with the same checksum. A directory restored from a backup and backed
up to the same target again starts a new timeline (unless it was restored
from the newest point, which its history simply continues); restore
considers every timeline's restore points by time, and `--timeline <id>`
selects one. `fusee backups <target> --verify` lists timelines and restore
points and reads every object the manifest names.

## Memory

Parked runs take a 32-byte table entry each plus their share of two hash
indexes (about 44 bytes in all); loaded runs are held to
`Config::memory_budget` (128 MiB by default across shards). On an
i8g.2xlarge, ten million parked runs over eight shards reopen in 252 ms
with 447 MB resident (46.9 bytes per run, everything included).

A process that has just parked millions of runs holds more than that:
glibc keeps what the loaded runs used. Two things keep it from staying. The
engine fixes glibc's mmap and trim thresholds once per process (256 KiB and
1 MiB) at the first shard open; glibc otherwise raises both each time a
large block is freed (a run index rebuilt into a bigger one), after which
each shard's arena keeps up to twice that block resident for good, and
`malloc_trim` cannot return it. Setting `MALLOC_MMAP_THRESHOLD_` or
`MALLOC_TRIM_THRESHOLD_` in the environment leaves glibc's own values. And
after a mass park, a background thread calls `malloc_trim` once a second
until the memory stops falling. With two million parked runs in a Linux
container, the two took the resident size from 203 MB to 147 MB, against
105 MB that malloc counts in use.

Do not set `MALLOC_ARENA_MAX=2` for this: it saves memory, but the shards
then share an allocator lock, and on an i8g.2xlarge it made creating ten
million runs three times slower and waking them 2.7 times slower.
jemalloc or mimalloc as the global allocator are fine.

## Web UI and HTTP API

The HTTP API (`/v1`, JSON; `GET /v1/openapi.json` describes it) serves
the web UI at `/` and Prometheus metrics at `GET /metrics`, one series per
counter per shard (`fusee_<counter>{shard="N"}`). It reads what the
engine's own entry points answer, on a task of its own, so it adds
nothing to a step. It is served by:

- the dev profile, at `FUSEE_HTTP` or else `127.0.0.1:7601` (`fusee dev`
  prints the address);
- `Config::http(addr)` or `fusee::http::serve(engine, addr, HttpOptions)`
  in Rust, `engine.serve()` in TypeScript, Python and Go;
- a server, with `fusee server --http host:port`.

It listens on loopback unless told otherwise, and off loopback it needs a
token (`Authorization: Bearer …`; the UI asks for it once). A read token
(`--read-token-file`) reaches lists, runs, results, events and metrics and
nothing that changes a run; namespace tokens (`--namespaces-file`) reach
the runs under one workflow-ID prefix, and cannot start the engine's own
`fusee.*` types (schedules, delayed starts), which name other runs in their
input. Behind a proxy that signs people in,
`--identity-header` names the person in the records of the signals,
cancellations, terminations and resets they send (see
[features.md](features.md#audit)). A request that changes something must
carry a JSON body or `X-Fusee-Action: 1`, which a page on another site
cannot send through a browser, and a browser `Origin` must match the host.
Without a token, a loopback API answers only requests for this machine's
names (`localhost`, `127.0.0.1`, `[::1]`): a page on another site that
points its own name at 127.0.0.1 (DNS rebinding) is refused.
`cargo run -p fusee --example ui` serves a populated memory engine on
http://127.0.0.1:7601.

Under the dev profile a run's step list is also a timeline: each step
record carries when the step began and how long it took (a `STEP_TIMED`
record, which production steps never write), and `GET /v1/runs/{id}`
returns them as `stepTimes`.

## Metrics

Rust exposes `Engine::shard_metrics().await` and an aggregate `Engine::metrics().await`. TypeScript exposes `engine.shardMetrics()` and Python exposes `engine.shard_metrics()`. The shard number is the position in the returned list. Counters cover records and bytes, flushes, flushed records, flush time, gated waits and wait time, steps executed and replayed, checkpoints, parking, deterministic replay errors and busy rejections. Durations are in microseconds. A delta in `flushed_records` divided by the corresponding delta in `flushes` gives the mean batch size.

Applications that install an OpenTelemetry meter provider can enable the Rust crate feature `opentelemetry` and open `Config::local(dir).opentelemetry(false)`. Instruments include per-shard append and executed-step counters and histograms for batch size, flush duration and gate wait duration. The rate of `fusee.steps.executed` gives steps per second. Passing `true` also records flush and gate-wait spans through the application's tracer provider. The application owns the exporter and destination. Fusee’s default configuration opens no telemetry connection.

## Traces

A run joins the trace of the request that started it. With telemetry on
(`telemetry: openTelemetry(api)` in TypeScript, `telemetry=fusee.open_telemetry()`
in Python, the `opentelemetry` feature in Rust), a start records the active
span's W3C `traceparent` in the run's `Start` record. The run's code then
runs under a `fusee.workflow` span in that trace, with a `fusee.step` span
per attempt below it, in every process that runs it: after a crash the
resumed code gets a new `fusee.workflow` span in the same trace, and
replayed steps make none. Children and continue-as-new keep the trace.
The HTTP API takes a `traceparent` header on starts; Rust takes
`.trace(traceparent)` on the start builder, and workflow code reads the
recorded one with `ctx.traceparent()` to parent the spans of the calls its
steps make. `fusee runs show` and the journal show it as the start's
`trace`. A start without one records nothing.

## Signed releases

The release workflow verifies an annotated `v*` tag against the repository's configured public signers. Set the Actions variable `FUSEE_RELEASE_SIGNING_KEYS` to ASCII-armored public GPG keys, or set `FUSEE_RELEASE_SSH_SIGNERS` to an OpenSSH allowed-signers file containing the approved principals and public keys. Run `scripts/verify-release-tag.sh v1.0.0` with the same environment locally. Tag verification precedes the jobs that build and upload registry artifacts.
