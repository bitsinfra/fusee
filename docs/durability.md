# Durability modes

## `local`

Every shard of a data directory writes to one shared log (log format 3, in
`<dir>/wal/`): whoever writes next takes every shard's pending records and
writes them as one batch with one durable flush, so steps on different
shards share writes instead of each paying for one. Directories created
before log format 3 keep a log per shard (`Config::shared_log(false)`
creates one). A batch is written and made durable with one call:

| Platform | Flush | Notes |
| --- | --- | --- |
| Linux | `O_DIRECT` aligned writes, each carrying `RWF_DSYNC` (default, for the shared log and per-shard logs); `fdatasync`; or `io_uring` (feature `io-uring`, per-shard logs, buffered writes) | Where the filesystem refuses `O_DIRECT` (ramfs, tmpfs before Linux 6.6), or takes the open and then rejects a write with `EINVAL`, the log writes buffered from then on, rewriting that write exactly, and says so once on stderr: a per-shard log then flushes with `fdatasync`, and the shared log keeps `RWF_DSYNC` on its buffered writes. `LocalOptions::direct_io = false` selects buffered writes. `RWF_DSYNC` needs Linux 4.7 or later |
| macOS | `F_FULLFSYNC` | plain `fsync` does not flush the drive cache |

On a fast disk (last flush at most 200 µs) an idle shard writes the batch on
its own thread when no other write is in flight, saving the thread handoffs;
otherwise the log thread writes while code keeps running, with up to two
writes in flight (each padded to blocks of its own, positions published in
order). A shard may have a section in each of them: a busy shard submits
its next records while its previous section is still being written, and
learns of each position as it becomes durable. A shard writing on its own
thread that finds a padded write in flight starts a second one beside it
once its records have waited a quarter of a flush, instead of waiting for
the first to hand it the next write.

On Linux with `O_DIRECT` and `RWF_DSYNC` (and the default `io-uring`
feature), the log thread and its I/O threads are replaced by one owner
thread: whatever seals a batch queues it, and the owner submits each write
through an `io_uring`, up to the in-flight limit, and reaps completions in
the same system call, publishing each durable position as it lands. A
batch that starts a segment is written after its header, without the ring;
a write the kernel refuses with `EINVAL` is written again buffered, as
above. Where `io_uring` is unavailable, or with `FUSEE_LOG_OWNER=0`, the
threads stay. They stay on tmpfs too, where the kernel runs each such
write on a worker thread, one at a time per file (`FUSEE_LOG_OWNER=1` takes
the owner there anyway). Docker's default seccomp profile refuses
`io_uring`, so a container keeps the threads unless its profile allows the
three `io_uring` calls. What is written, and in what order positions publish, is the same
either way.

A roll to a new segment asks for a checkpoint only from shards whose oldest
retained record is in the segment the log retires next (the others
checkpoint on their interval), and while the log fills a segment at over
100 MB/s the next segments grow to 256 MiB, so a busy log neither rolls nor
checkpoints every shard as often.

**What recovery accepts.** Each batch carries its sequence, the checksum of
the batch before it, the sequence its writer had acknowledged when it was
submitted, and a value unique to that open of the log. Recovery follows the
chain from the oldest segment and stops at the first batch that does not
continue it; the next open writes right after that point. Bytes left by a
torn write, by a write in flight at a crash, or by a segment's earlier life
can never join the chain. A valid batch after the end whose acknowledged
sequence reaches past the end proves that an acknowledged batch was lost,
and the directory refuses to open. The rule is model-checked in
`spec/FuseeLog.tla`. On macOS the default is one shard, because
`F_FULLFSYNC` flushes the whole drive and shards' flushes serialize.

**Batch size.** A batch carries at most 64 MiB, which is also the most
recovery reads as one batch. A shard hands the log at most 8 MiB of records
at a time (one record at least, whatever its size), and when more than a
batch is staged before a write, because a flush stalled or many shards
wrote large records at once, the log writes several batches: whole sections
while they fit, then the leading records of the next section, cut at a
record boundary, with the shards taking turns to go first. A backlog is
therefore written and acknowledged in order, in bounded pieces, in `local`,
server and replicated mode alike (a replicated entry takes at most 16 MiB of
staged sections).

**Large payloads.** A record whose canonical encoding is longer than
`Config::payload_offload_bytes` (64 KiB by default for `Config::local`; 0
turns it off) does not go into a log batch. Its bytes go to the log's blob
area (`<dir>/wal/blobs/`), and the batch carries a reference in its place:
the record's kind, a blob file, an offset, a length and a CRC-32C of the
bytes. A large step result, signal, update argument, start input or stream
chunk therefore does not hold up the small records of every other shard
behind one long write.

- **Ordering.** The shard's own thread finds and checksums its large
  records before handing the section over, so below the threshold the
  cost is one comparison. The blob thread writes the bytes and syncs their
  file. The section that references them, and every later section of that
  shard, enters a batch only after that sync. The chain therefore never
  accepts a batch that names bytes that are not durable, and other shards'
  batches keep going out meanwhile.
- **Layout.** Blob files are append-only and never reused. Each open of the
  log appends to a new file, and a file rolls over past a fixed size. Every
  blob starts on a 4 KiB block of its own, so whole-block `O_DIRECT` writes
  never touch another blob.
- **Recovery.** Each reference is resolved and checked against its length,
  checksum and kind. A mismatch refuses to open, naming the record and the
  blob file. Blob files that no batch of the chain references are deleted
  at open.
- **Retirement.** Blob files go with the log segments that reference them.
  A segment is retired once every shard's checkpoints (and the backup)
  cover it, and its header is zeroed durably before any blob file that
  only retired batches reference is deleted.
- **What stays the same.** References exist only in log sections.
  Journals, run stores, checkpoints, archives, exports, `fusee inspect`
  and every SDK see the record's canonical encoding, so conformance
  journals are byte-identical with offload on or off.
- **Where it applies.** A continuous backup uploads the blobs that a log
  chunk's batches reference, as one pack object per chunk, before any
  manifest names the chunk. A restore writes them back to
  `wal/blobs/`. `fusee reshard` reads the log with its references
  resolved. Server and replicated mode refuse a nonzero threshold, because
  their logs carry large records inside batches; `memory` mode ignores it.

**What a backup adds.** `local` survives a process crash and power loss;
it does not survive losing the disk. A continuous backup
([operations.md](operations.md#backup-and-restore)) copies the shared log
and the checkpoints to object storage as they become durable, so a lost
disk (or a bad deploy, or a deleted directory) costs at most what was
acknowledged since the last upload: about `upload_interval` (1 s by
default) plus the upload's own time, or the active log segment when
`upload_interval` is `None`, and nothing after a clean shutdown. A restore
to a point in time brings back exactly what was acknowledged at that
moment, with the same guarantees as recovery after a crash then: nothing
acknowledged by then is missing and nothing acknowledged later is there.
What a backup does not protect:

- Effects outside Fusee after the restore point. A step whose result was
  acknowledged after the point runs again after a restore (its idempotency
  key from `Ctx::key` is the same, so a service that honours it sees a
  retry).
- Acknowledgments within the loss window when the disk is lost.
- Directories in log format 2 (`fusee upgrade` converts them), server-mode
  directories and replicated directories.
- The visibility index (`visibility.redb`): it is rebuilt after a restore.
- A target written by two directories at once: a manifest write finds the
  other's manifest and that backup stops.

## Server

`fusee server --dir <dir>` holds the logs; applications open an engine with
`fusee_server::remote_config("host:port")`. See [server-mode.md](server-mode.md).

## `memory`

Nothing is written. For tests.

## `replicated`

A primary-backup protocol (Viewstamped Replication style) with
protocol-aware recovery: outputs and step gates wait until a quorum holds the
records durably. Embedded: the application instances are the replicas and
workflows run on the primary (`fusee_server::replicated::ReplicaNode`). See
[replication.md](replication.md).
