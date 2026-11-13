# Server mode

The same engine runs as `fusee server`. The server holds one log for every
shard (log format 3, as in local mode) and each shard's run store and
checkpoints on its disk; workflow code still runs in the application. The
application's shard runtimes use a remote storage backend, so a flush is
durable once the server flushed it: one pipelined round trip plus the
server's flush.

```bash
fusee server --dir /var/lib/fusee --listen 0.0.0.0:7600 --shards 4
```

```rust
let engine = Engine::open(fusee_server::remote_config("10.0.0.5:7600")?).await?;
```

On one host, add `--unix /run/fusee.sock` and connect workers to
`unix:/run/fusee.sock` to skip the TCP stack, or to
`unix:/run/fusee.sock?shm=1` to send log records through shared memory
(see [Shared memory](#shared-memory)).

**Leases.** Each shard is leased on its own, with an epoch of its own. A
worker takes every free shard unless its address says how many it wants at
most (`host:port?max-shards=2`); for the shards it does not get it waits as
a standby. When a holder's connection closes or goes silent, its shards go
to the waiting workers with new epochs; each recovers them from the
server's disk and resumes their workflows. Every request carries its
shard's epoch; the server refuses stale epochs, so a deposed process can
never append again. With `max-shards`, several workers run workflows
against one server at once and a hung worker loses only its shards:

```rust
// Two processes, each running two of the server's four shards.
let (cfg, remote) = fusee_server::remote_worker("10.0.0.5:7600?max-shards=2")?;
```

A worker's engine opens without waiting for the shards another worker
holds; calls to workflows on those shards wait until this worker takes the
shard over. Send them through a [`Client`](#calls-from-other-machines),
which the server routes to the shard's holder. Workers from before
per-shard leases wait until every shard is free and take them all.

**Messages between workers.** Child starts and results, signals and
cancels from one workflow to another, and their acknowledgments, go
through the server when the two shards are held by different workers. The
server forwards them to the target shard's holder, or keeps them (up to
1,024 frames or 8 MiB per shard, the oldest dropped first) until a worker
takes the shard. Delivery is exactly once in effect: the sender's outbox is
durable and resends every unacknowledged message each second, the receiver
absorbs duplicates as it does after a restart, and a receiver acknowledges
only once the message's effect is durable. An acknowledgment names the
epoch of the sender's lease; the server forwards it only to that lease, and
the worker checks it again, so an acknowledgment that outlives a failover
(whose new holder renumbers its outbox) is dropped instead of removing
another message; the new holder resends. A worker crash on either side, or
a target lease moving while a message is in flight, therefore costs at most
a resend.

**Liveness.** A worker holding the lease sends `PING` every second from its
own thread (over shared memory, it advances a heartbeat counter in the
mapping instead); any other request counts too. The server revokes the lease once
it has spent five seconds waiting to read from the worker without receiving
a byte, or once a write to the worker has made no progress for five seconds.
It closes the connection and hands the lease to the next standby with a
higher epoch, exactly as when the holder disconnects, so the revoked worker
can never append again. The connection's reader does no disk I/O, so a long
`fdatasync` or a large recovery read on a shard thread never delays it.
Time the server does not read is not silence: while a stalled runtime holds
the reader up, or while a shard has more than 32 MiB of requests waiting
and the server stops reading until it drains, the worker's pings wait in
the socket, and the server reads them before it checks the timeout.
`Server::set_liveness` sets the interval and timeout for leases
granted afterwards, or turns revocation off. The worker offers the
capability in its hello and the lease carries both values; a worker that
does not offer it keeps its lease until its connection closes.

**Run-store offsets.** Run-store appends are pipelined, and the worker
computes each frame's offset itself. Each append carries that offset. The
server refuses an append whose offset is not its store's length, then
refuses every later write to that shard for the rest of the lease;
otherwise it answers with the offset it wrote at, and the worker checks the
answer when it arrives. Either mismatch stops the shard with an error before
any flush or checkpoint sent after the append is acknowledged. A worker
uses this only when the lease advertises it, and otherwise sends the
original unverified append.

**The shared log.** A worker sends each shard's records as a section of the
server's log and does not wait for an answer before it sends the next
section of another shard: appends are pipelined. Whoever writes next takes
the sections every connection has staged and writes them as one batch with
one write (`O_DIRECT` and `RWF_DSYNC` on Linux, as in local mode), so
workers and shards share flushes. When a batch is durable, each
connection holding one of its shards gets one `DURABLE` frame with the
cumulative durable position of each, and the waiting shard spins, then
parks, until its position arrives. When nothing is in flight and the disk
is fast (last write at most 200 µs), the batch is written by the thread
that received it; otherwise by the log's I/O threads, with up to two writes
in flight, each padded to the end of its last 4 KiB block so that two writes
in flight never share a block. The shards a write made durable usually send
their next sections within a round trip of each other, so on that slow path
the next batch waits for them: until all have arrived, or three times their
usual turnaround (measured, at least 20 µs and at most a quarter of a
flush), whichever comes first, so eight shards do not split into groups
taking turns on the two writes (with a 25 ms flush, eight shards take 1.1
to 1.2 times one shard's time, as in local mode). A lone shard never waits:
it is the whole group. A shard granted to a new worker has what its
previous holder staged dropped (none of it was acknowledged), and its
recovery waits until that holder's writes in flight are durable, so its
records can never follow the new holder's.

**Recovery.** The server reads the log and ships each shard its records
after its checkpoint (`RECOVER_LOG`, in pages of about 1 MiB); the worker no
longer pulls whole segment files. The log is read once when the server
opens, and once per failover for every shard granted since the last read.
Run-store reads that continue a scan (a mass wake of runs parked one after
another) read further ahead each time, up to 1 MiB, so most of them need no
round trip.

**Threads.** Each shard of a lease has its own server thread. It owns the
shard's run store and checkpoints, handles the shard's requests in arrival
order, does their blocking I/O and writes the replies to the socket itself,
so the server's async runtime never waits on the disk for them. Each
shard's replies leave in the order its requests were handled, and every
reply to a run-store append leaves before the `DURABLE` frame of a section
sent after it. A section whose shard thread has nothing pending is staged
(and on a fast disk written) by the connection's reader itself, saving a
thread handoff. Between requests a shard thread spins for up to twice the
last gap while requests arrive within 100 µs of each other, and never on
hosts with two or fewer cores. A directory with a log per shard (format 2)
is served as before: each shard thread flushes its own log, inline on a
fast disk or through the shard's flusher thread.

**Waiting for a flush.** When a worker's shard has nothing else to do and
its last flush round trip took at most 200 µs, the shard thread sends the
batch and waits for the reply itself: it spins for about twice the recent
round trip (at most 200 µs, and never on hosts with two or fewer cores),
then parks. The connection's reader thread hands the reply to it directly,
without waking the shard's runtime. Otherwise the flush runs in the
background and the reader wakes the runtime when it completes. Both paths
measure the round trip, so a slow server is never waited for inline.

## Calls from other machines

Any process can connect as a client and
send calls to the worker holding the lease:

```rust
let c = fusee_server::Client::connect("10.0.0.5:7600")?;
c.complete(&token, &serde_json::json!(true))?;
let n = c.update("cart-1", "addItem", "req-42", &item)?;
```

The worker opts in by serving them next to its engine:

```rust
let (cfg, remote) = fusee_server::remote_worker("10.0.0.5:7600")?;
let engine = Engine::open(cfg).await?;
tokio::spawn(fusee_server::serve_calls(remote, engine.clone()));
```

`complete`, `fail`, `update`, `query`, `signal`, `start`, `result`, `cancel`,
`terminate` and `streamRead` are relayed, with JSON arguments, to the worker
holding the workflow's shard. `list` and `list_where` go to every worker holding shards,
each listing its own, and come back merged in shard order (up to the limit
per shard, as an engine's own listing). A call made while no worker holds the
shard it needs (for a listing: any shard) fails at once; the client retries.

**Objects from other machines.** A call to a key of an
[object type](features.md#objects-keyed-state) is an `updateWithStart` on
the key's run (`<type>/<key>`), relayed to the worker holding its shard,
and its state is the `fusee.state` query, so `Client::object_call(type,
key, handler, arg, id)`, `object_send` and `object_state` (and
`client.objectRef(type, key)` in TypeScript, `client.object_ref` in
Python) need nothing new from the server. A workflow's call to a key on
another worker's shard is a child-start message through the server, and
its result comes back as a child result.

**Streams from other machines.** `streamRead` is one read of a
[stream](features.md#streams): the worker answers with the durable chunks
from the offset, or waits for one (a long poll of up to a minute; 30 s by
default) and answers with none. Readers loop on it, which the clients do:

```rust
for chunk in client.stream("q-1", "tokens", 0) {
    print!("{}", chunk?.json::<String>()?);
}
```

In TypeScript `client.run(id).stream(name, { from })` and in Python
`client.run(id).stream(name, from_=n)` are async iterators. A read in
flight when its worker goes away fails ("the worker went away before
answering"); the reader connects again at its offset (`ClientStream::offset`,
`reader.offset`), and once a standby holds the shard it reads on from
there: chunks are only ever handed out once the server's log holds them,
so a failover never takes back one a reader saw, and the step that runs
again on the new worker continues after what was recorded. Each waiting
read holds one of the server's pending calls (at most 4,096).

**Limits.** The server persists named concurrency and rate definitions and
renews held permits every second. Leases expire after three seconds without
renewal. A restarted server waits out outstanding leases before reissuing
permits. The bound applies to valid permits. An external operation that
already received its request can continue after the issuing worker loses
its lease. Replicated groups record lease decisions in their quorum log.
The server also holds the wait queue, with each waiting step's priority and
fairness key ([priority and fairness](features.md#priority-and-fairness)),
so the order is the same whichever worker asks.

**Covers:** container fleets without local disks, failover between
application instances, and several instances sharing one server's shards
(`max-shards`).

**Security:** off by default. Turn on TLS and a token (see
[Security](#security)) on any network you do not fully trust.

**Measured** on an AWS i8g.2xlarge (instance-store NVMe, one shard,
`cargo run --release -p fusee-server --example server_step_cost -- 500 7`,
rounds that alternate local, Unix socket, shared memory and TCP): a
sequential step over shared memory takes 1.0 µs more than in local mode
(15.9 against 14.9 µs at p50), with one voluntary context switch per step
(the flush, as in local mode) and almost no socket messages; over a Unix
socket a step takes 12 to 18 µs more and over loopback TCP 20 to 21 µs
more, with three to four voluntary context switches per step.

## Shared memory

A worker on the server's host can send its log records through shared
memory instead of the socket: connect to `unix:/path.sock?shm=1`. The
worker asks for it in its hello; the server creates the mapping (a memfd on
Linux, an unlinked POSIX shared-memory object on macOS) and passes it, with
its doorbell page, alongside the lease frame (`SCM_RIGHTS`), and learns the
worker's process from the socket's peer credentials. Off by default; a
server that cannot set it up (no shared log, another platform) leases over
the socket alone, and the worker uses that.

```text
mapping   header: version, shards, ring size, worker and server pids,
          heartbeat (worker), closed (server), log position and segment
          one cache line per shard: durable LSN, lease epoch, a futex word
          bumped after every store, parked, pins, notify, failed
          ring: head (server), tail (worker), frames of length, CRC32C,
          payload (kind, shard, epoch, flags, section)
doorbell  one page per server: the futex word its log thread sleeps on
```

- **Appends.** A shard thread copies its section into the ring and
  publishes it with a release store of the tail; if the log thread sleeps,
  it rings the doorbell. One server thread polls every worker's ring,
  stages what it finds with the same epoch and LSN checks as the socket
  path, writes the batch (inline on a fast disk, else on the I/O threads),
  stores each shard's durable LSN in its line and wakes only waiters that
  parked. It spins for twice the longer of the recent flush and the
  workers' recent turnaround, from publishing a position to that shard's
  next section (at most 100 µs, never on hosts with two or fewer cores),
  before it sleeps on the doorbell: the next frame is about that far away.
  (Spinning for the flush time alone left it asleep on a fast disk, and
  every step paid a futex wake-up.)
- **Waiting.** A shard waiting for its own flush spins on its line for
  about twice the recent round trip (at most 200 µs), then parks on the
  line's futex word (`futex` on Linux, `__ulock_wait` on macOS). A
  background flush sets the line's notify flag, and the server then also
  sends a `DURABLE` frame on the socket, which wakes the shard's runtime.
- **What stays on the socket.** Recovery reads, run-store appends and
  reads, checkpoints, grants, limits, relayed calls and messages. A
  section is sent through the ring only while no run-store append of its
  shard awaits its answer, so the server has written every frame a
  section may reference before it takes the section; otherwise, or when
  the ring is full, it goes over the socket.
- **The ring is volatile.** Only watermarks acknowledge. A worker that
  dies mid-frame never published it; a frame published and not yet taken
  when its lease ends is refused by its epoch. A frame that fails its
  length or checksum checks, or names a shard the ring cannot carry,
  breaks the protocol: the server revokes the lease.
- **Fencing.** The server refuses ring frames with a stale epoch and
  reports it (`IO_FAILED`, and the line's failed flag), and the shard takes
  no more ring frames in that lease. When a lease ends, the server stops
  reading the ring, clears each line's epoch, sets `closed` and wakes every
  waiter. It also watches the worker's process (a pidfd on Linux, kqueue
  `EVFILT_PROC` on macOS) and revokes the lease as soon as it exits, even
  if another process kept the socket open; the heartbeat in the mapping
  replaces `PING` for liveness.

Measured with `cargo run --release -p fusee-server --example server_step_cost --
500 7` on an AWS i8g.2xlarge (instance-store NVMe): 500 sequential steps,
seven rounds that alternate local mode, the Unix socket, shared memory and
TCP, per-run medians.

| Path | Step p50 | Over local |
| --- | ---: | ---: |
| Local mode | 14.9 µs | |
| Shared memory | 15.9 µs | +1.0 µs |
| Unix socket | 27.0–32.5 µs | +12 to +18 µs |
| Loopback TCP | 35.1–36.2 µs | +20 to +21 µs |

A step over shared memory makes no socket call; it copies its section twice
(into the ring, out of it) where the socket path copies it four times (into
the frame, into and out of the kernel, out of the decoder), and the
voluntary context switch that remains is the flush itself, on the server's
log thread instead of the shard's.

## Invoked workers

A server can run workflows on workers that exist only while there is work:
functions on Lambda, Cloud Run or Vercel, or any HTTP server. When a shard
that no worker holds has work due, the server sends an HTTP `POST` to an
endpoint; the process that receives it opens an engine in server mode for
that shard, runs it until it is idle or its time budget ends, and returns.
No process stays up while workflows sleep.

```bash
fusee server --dir /var/lib/fusee --listen 0.0.0.0:7600 --token-file /etc/fusee/token \
  --invoke https://fusee-worker.example.com/invoke \
  --invoke-secret-file /etc/fusee/invoke-secret --invoke-concurrency 16 --invoke-budget-ms 60000
```

```rust
// In the function: any HTTP framework hands the request to the handler.
let handler = fusee_server::invoke::Handler::new(
    "fusee.example.com:7600?ca=ca.pem&token-file=token",
    std::fs::read("invoke-secret")?,
    |engine| { engine.workflow("order", order); },
)?;
let reply = handler.handle(signature_header, &body).await; // status and JSON body
```

The TypeScript and Python handlers are in their READMEs; each takes the
same address, secret and registration function. In Go,
`fusee.NewInvocationHandler(fusee.InvocationOptions{Address, Secret,
Register})` is an `http.Handler` (and `Invoke(ctx, signature, body)` for
other frameworks); in Java, `new InvocationHandler(address, secret, options,
register)` serves the JDK's `HttpServer` through `httpHandler()` and any
framework through `invoke(signature, body)`.

**When the server invokes.** A shard is *due* when no worker holds it and
one of these holds:

- a client call for it arrived: `start`, `signal`, `update`, `query`,
  `result`, `complete`, `fail`, `cancel`, `terminate`, `reset`,
  `streamRead`, or a listing's share of it;
- a message from another shard waits for it at the server (a child start or
  result, a signal, a cancel);
- the time its last holder reported comes: a timer (a sleep, a deadline, a
  scheduled start, a debounce window), a retry, or the resend of a message
  another shard has not acknowledged yet;
- its last holder left without reporting: it crashed, its lease was revoked,
  or the server restarted. A server starting with endpoints invokes every
  shard once, because what its shards' next due times are lives only in the
  memory of the server that last heard them.

Calls wait at the server (up to 60 seconds) until a worker holds their
shard, then go to it as usual; a call that waits longer fails and the
client retries. Messages wait as they do for any shard without a holder
(see *Messages between workers* above).

**Which endpoint.** Each endpoint takes every workflow type, or the types it
lists. A `start` goes to an endpoint for its type. For other work the
server uses the endpoint the shard's last holder named for the types it had
runs of and no code for, else the endpoint that last ran the shard, else the
first endpoint that takes every type. A shard with work for two endpoints is
invoked on one, then on the other.

**The request.** `POST` to the endpoint's URL with `content-type:
application/json`, `fusee-signature: t=<unix ms>,v1=<hex>` where `v1` is
HMAC-SHA256 with the endpoint's secret over `<t>.<body>`, and the body

```json
{"version":1,"invocation":"8f3c0e5a1b2d4c6e","shards":[2,5],"budgetMs":60000,"reasons":["call","timer"]}
```

Over `https://` the server verifies the endpoint's certificate against the
CA file given for it (or the system bundle) and can present a client
certificate (mutual TLS, `--invoke-cert` and `--invoke-key`). The handler
answers 401 to a request without a valid signature before it connects to
anything, so nobody but the server can make it open a connection; an
endpoint configured without a secret (the server warns) must check the
server's client certificate instead. A handler refuses a signature older
than five minutes, and a replayed request is harmless besides: its
invocation ID is no longer valid (see below).

**What the invoked process does.** The handler checks the signature, then
connects to the server as a worker with `invocation=<id>` among its address
options. The server grants exactly that invocation's shards, those of them
no other worker holds by now, each under a new lease epoch; an invoked
worker never waits as a standby, and a connection naming an unknown or
finished invocation is refused. Its engine recovers those shards from the
server (the checkpoint, the run store's index and the log after the
checkpoint: a few round trips per shard, no file transfer) and leaves every
other shard closed. Messages to other shards go through the server's relay,
calls are relayed to it as to any worker, and it pings to keep its lease.
It parks runs 50 ms after they suspend (instead of 5 s), runs until idle or
until its budget ends, then closes its engine (flushing, then
checkpointing, so the next open replays almost nothing),
reports each shard's next due time on the lease connection (`IDLE`), closes
the connection and answers `200` with a summary. It keeps nothing between
invocations; each one opens a fresh engine.

**Idle.** A shard is idle when nothing can make progress there: every
loaded run waits on a timer, a signal, a child, or an outside completion
(no step is running and no code is replaying), no records or checkpoint
wait for the server, every message to another shard has been acknowledged,
no relayed call is being answered (waits for a result or for stream chunks
do not count), and its next due time is at least 250 ms away. Messages to
another shard that are not acknowledged yet keep the shard for up to 3 s:
an acknowledgment names the sender's lease, so one that arrives after the
sender left is dropped and the message is sent again from its next
invocation. Once every shard the worker holds has been idle for 100 ms (so a
trickle of calls shares one invocation instead of paying a cold start each),
it asks the server to stop sending it work for them (`DRAIN`): calls and
messages that arrive from then on wait at the server and make the shard due
again the moment the worker leaves. The worker answers what was already on
its way and closes once all its shards are idle again. Calls waiting for a
result or for stream chunks that are still open when it leaves are not
answered: they wait at the server for the shard's next holder and do not
invoke one by themselves (a stream read waits up to two minutes there).

**Everything due, not a batch.** An invocation runs everything due on its
shards, not a bounded number of runs: the shard is the unit of leases,
recovery and fencing, and splitting a shard's runs across processes would
need leases per run. The server spreads due shards across the endpoint's
free concurrency (an invocation carries at most `--invoke-shards` shards,
default all of them, divided evenly between the free slots), so a shard is
the unit of parallelism, as for long-lived workers. The time budget bounds
an invocation's length.

**The time budget.** Each endpoint has a budget (`--invoke-budget-ms`,
default 60 s, at most the platform's execution limit), sent in the request
and in the lease. The worker stops at the budget less a margin (a tenth of
it, at most 5 s): it closes its engine without waiting for steps still
running and reports those shards as due now, handing their runs back; the
server invokes again at once. A step abandoned this way runs again in the
next invocation, as after a crash: its effects are recorded once, and its
function runs at least once, as always. A step therefore has to fit in one
budget; longer work belongs in an [async completion](features.md) that an
outside system finishes with `complete`. An invocation that reaches its
budget without finishing a step on a shard puts the shard in backoff (1 s,
doubling to 5 min) and logs it, so a step that can never fit costs one
invocation per backoff period instead of a loop.

**Exactly once.** Unchanged. An invoked worker holds ordinary per-shard
leases with epochs and the liveness timeout, and every request carries its
shard's epoch. The server waits for the HTTP response for the budget plus
a grace (30 s by default, `graceMs`); when the request fails or times out it
revokes the invocation's lease
if it still holds one, and the invocation ID stops being valid, so a
process that wakes up later can neither connect with it nor append under
the old epoch. Its shards are due again with backoff. A process the platform
kills mid-invocation is a crash: its lease ends (connection closed, or five
seconds of silence), and the next invocation recovers the shard from the
server's log.

**Backoff, retries and concurrency.** An endpoint runs at most
`--invoke-concurrency` invocations at once (default 16). An invocation that
fails (no connection, a status other than 2xx, a timeout, or a 2xx after
which the shards were never leased) leaves its shards due and backs the
endpoint off: 100 ms, doubling to 30 s, with a fifth of jitter, reset by the
next success. A slow endpoint holds its concurrency slots, so at most that
many invocations wait on it. A shard whose invoked holder left without
reporting is due again with the same backoff, per shard (a long-lived
worker's shards are due at once).

**Long-lived workers alongside.** A long-lived worker (one that connected
without `invocation=`) waiting as a standby takes any shard that frees up,
before the server would invoke for it; the server only invokes for shards
that no worker holds or waits for. When a long-lived worker leaves, its
shards are due at once and invoked workers take over.

**Configuration.** Each `--invoke URL` starts an endpoint; the `--invoke-*`
options after it apply to it: `--invoke-secret-file`, `--invoke-types a,b`,
`--invoke-concurrency`, `--invoke-budget-ms`, `--invoke-shards`,
`--invoke-ca`, `--invoke-cert` and `--invoke-key`. `--invoke-config FILE`
reads `{"endpoints": [{"url": ..., "secretFile": ..., "types": [...],
"concurrency": 16, "budgetMs": 60000, "shards": 0, "graceMs": 30000, "ca":
..., "cert": ..., "key": ...}]}`; unknown keys are refused. In Rust,
`Server::set_invokers(vec![invoke::Endpoint::new(url, secret)...])`, or
`Endpoint::from_json`.

**Metrics.** `Server::invocation_metrics()`, and the `invocations` call any
client can make (`client.call("invocations", {})`), give per endpoint the
invocations sent and how they ended (succeeded, failed, timed out, answered
without taking the lease), those in flight, the backoff left, the time from
sending to the answer, shards invoked, steps they ran and the engines'
open time; and overall the shards due, invoking and in backoff, handbacks,
leases that ended without a report, invocations without progress and calls
waiting at the server. `FUSEE_INVOKE_TRACE=1` prints a line per invocation
event on standard error, on the server and in the worker.

**Protocol.** The worker offers `INVOKED` (512) in its hello, followed by
the invocation ID; the lease carries the budget left. `DRAIN` (28) names
shards to route nothing more for, answered with `OK` behind every call
already routed to the worker. `IDLE` (29) carries, per shard and lease
epoch, whether work is left, whether the budget ended, the next due time
(relative, so clocks need not agree), steps run, the engine's open time and
the workflow types the worker had runs of and no code for. A shard's report
counts only under the epoch it names.

**Measured** on an Apple M-series laptop (plain `fsync`, 4 shards, server,
endpoint and client in one process over loopback, a fresh engine per
invocation: `cargo run --release -p fusee-server --example invoke_cost --
100 200 100 10000`):

| What | Result |
| --- | --- |
| Start for a shard nobody holds, call to the first step's body | p50 2.0 ms, p90 2.6 ms, max 2.9 ms |
| Of which: until the engine is open and the workflows registered | p50 1.6 ms (the shard's own recovery 0.8 ms) |
| 200 workflows of 4 steps on 4 shards, started at once | 4 invocations, 200 steps each |
| The same started one after another | 4 invocations, 200 steps each (the 100 ms idle grace) |
| Step after a 1 s sleep, invoked vs long-lived worker | late by 0 ms p50 in both; engine open 0.1 ms and invocation time 15 ms per resumed run (100 runs, 8 invocations) |
| Start on a shard holding 10,000 runs asleep for an hour | p50 165 ms, of which recovery 159 ms |

The cold start is a few round trips: the server's dispatch, the HTTP
request, the lease, the shard's recovery and the step. Timers are invoked
slightly early (the server wakes at the shard's earliest timer, which may
be early) and the worker waits for them, so the step after a sleep is no
later than on a long-lived worker. An invocation stays at least 100 ms
after its last work, so invocation time per resumed run falls as more runs
share one. Recovery grows with the runs a shard holds, about 16 µs each
(the open reads the shard's run store from the server): more shards keep
each invocation's open small, since an invocation opens only its own.
Platform cold starts (a function's process and runtime) come on top.

**Limits.** The next due times live in the server's memory, not on
its disk: a restarted server invokes every shard once. A listing invokes
every shard nobody holds. A step waiting for a concurrency permit or a
retry backoff longer than the budget keeps its invocation open until the
budget ends; the shard then backs off as an invocation without progress.


## Security

Workers and clients authenticate with TLS 1.3, a shared token, or both.
Without either, connections carry the plain protocol, and
the server prints a warning at startup when it listens on a non-loopback
address.

```bash
openssl rand -hex 32 > /etc/fusee/token && chmod 600 /etc/fusee/token
fusee server --dir /var/lib/fusee --listen 0.0.0.0:7600 \
  --tls-cert server.pem --tls-key server.key --tls-ca workers-ca.pem \
  --token-file /etc/fusee/token
```

Workers and clients put connection options after the address:

```rust
let addr = "db1.internal:7600?ca=/etc/fusee/ca.pem&cert=w1.pem&key=w1.key&token-file=/etc/fusee/token";
let engine = Engine::open(fusee_server::remote_config(addr)?).await?;
let client = fusee_server::Client::connect(addr)?;
```

The same string works as the TypeScript and Python `address`, after
`unix:/path.sock` (token only) and after `replicas://a,b,c`.

| Option | Meaning |
| --- | --- |
| `ca=PATH` | Turns TLS on: the CA certificates that sign the server's certificate. |
| `cert=PATH`, `key=PATH` | This worker's certificate chain and key (PKCS#8, PKCS#1 or SEC1 PEM), for a server started with `--tls-ca`. |
| `name=NAME` | The name to verify in the server's certificate instead of the address's host. |
| `token-file=PATH` | The shared token. |
| `max-shards=N` | Not a security option: a worker holds at most `N` shards at once (see Leases). |
| `shm=1` | Not a security option: a worker on a `unix:` address sends log records through shared memory (see [Shared memory](#shared-memory)). Off by default. |

An unknown or repeated option is an error, so a misspelt option never
silently turns security off. In Rust, `fusee_server::security::Security`
holds the same settings as values: `remote_worker_with`,
`Remote::connect_with` and `Client::connect_with` on the worker side,
`ServerConfig::security` and `Server::set_security` on the server.

**TLS.** TLS 1.3 only (rustls with ring), without session resumption. The
server presents `--tls-cert`. A worker verifies it against `ca=` and the host
it dialled (or `name=`), so the certificate must carry that DNS name or IP
address. With `--tls-ca` the server also requires a client certificate signed
by that CA (mutual TLS); any such certificate is accepted. Unix sockets never
use TLS: the socket file's permissions decide who connects, and the token
still applies.

**Token.** A file of at least 16 bytes (surrounding whitespace is not part of
it; use 32 random bytes). The server and workers warn when other users can
read it. Before its first protocol frame each connection proves the token
with HMAC-SHA256, in both directions:

```text
worker → HELLO      0xF0  version 1, purpose (1 server, 2 replica), sender u8, nonce [32]
server → CHALLENGE  0xF1  version 1, nonce [32]
worker → PROOF      0xF2  HMAC(token, "fusee auth v1 initiator" ‖ T)
server → ACCEPT     0xF3  HMAC(token, "fusee auth v1 responder" ‖ T)
            or REFUSED  0xF4  reason (UTF-8); the server closes the connection
T = hello body ‖ challenge body ‖ binding length u8 ‖ binding
```

They are ordinary frames (`FSE1` header, CRC32C) of at most 1 KiB. Under TLS,
`binding` is 32 bytes of TLS exporter output (RFC 8446 section 7.5, label
`EXPORTER-fusee-auth-v1`); without TLS it is empty. The token never crosses
the wire. Both nonces are fresh, so a recorded exchange never verifies again,
and under TLS a proof cannot be relayed into another session. The server
answers only after the worker's proof verifies, and the worker sends no
protocol frame before it has verified the server's, so neither side talks to
a party without the token. MACs are compared in constant time. TLS and the
token exchange must finish within 10 seconds. A worker without a token that
reaches a server with one, or the reverse, gets an error naming the option
to change; failed handshakes are logged at most once every ten seconds.

**A token without TLS** authenticates the start of each connection only:
anyone on the network path can still read the traffic and inject into the
stream. Use TLS wherever the network is not trusted.

**Limits:** workers are not told apart, so any worker with the
credentials can take the lease and any client can call the lease holder;
there are no revocation lists; certificates and tokens are read at startup,
so rotating them means a restart; and a flood of connections can still
exhaust the server.

## Server directories

A server directory keeps one log for every shard (`<dir>/wal/`, log
format 3); its layout is that of a local data directory, so `fusee inspect`
reads it. `Server::open(cfg, false)` creates a directory with a log per
shard (format 2) instead; `fusee upgrade <dir>`, with the server stopped,
converts such a directory to the shared log. A server with a shared log
refuses workers that only speak per-shard logs, with a message saying to
upgrade them.

## Reopening a worker

With a log per shard (format 2), a replacement worker negotiates paged segment recovery with the server. The server reports the file length and scans backwards for the last nonzero byte. The worker fetches the prefix in bounded pages and reconstructs the exact padding before passing the segment to its normal corruption and torn-tail checks. With the shared log, the server sends the shard's records instead (see Recovery above).

## Dedicated replicated servers

Run three or five servers, each with its own directory. List the replication addresses in the same order on every member. Give each server its index in that list and a separate listener for applications. One replication group owns one shard.

```bash
fusee server --dir /var/lib/fusee --listen 10.0.1.10:7600 \
  --replicas 10.0.1.10:7400,10.0.1.11:7400,10.0.1.12:7400 --replica-id 0
```

Start members 1 and 2 with their own listen addresses. `--tls-cert`, `--tls-key`, `--replica-tls-ca` and `--token-file` secure both the worker listener and the replica protocol ([replication security](replication.md#security)). An application passes the worker listeners to its ordinary server-mode API.

```ts
const engine = await Engine.open({
  durability: "server",
  address: "replicas://10.0.1.10:7600,10.0.1.11:7600,10.0.1.12:7600",
});
```

Rust `remote_worker`, the Python `Engine.open` API and standalone `Client.connect` accept the same address. A new connection probes the members until one commits a quorum record. That primary leases the shard to one worker. Other application processes wait as standbys. Each workflow flush waits for a durable majority. Checkpoints, store compaction, indexed listing and immutable history use the primary’s materialized shard files and its replicated checkpoint mechanism.

A replacement worker first commits a new permit session. Outstanding permits from its predecessor keep their expiry time. On a view change or a failed quorum check the server closes its worker and client sockets. Reconnect clients to discover the new primary. A call interrupted before its reply may already have committed. Reuse its workflow or update ID when retrying. Users of a worker engine that lost its connection must reopen it or let a connected standby recover. Replication-group membership is fixed.
