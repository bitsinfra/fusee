/*
 * fusee.h: the C interface to the Fusee engine (libfusee_c).
 *
 * For SDKs whose workflow code runs in the host language (Go, Java). The
 * host runs and replays workflow code; the engine records the journal,
 * decides step gates and durability, and hands the host events and
 * deliveries. Every durability mode (memory, local, server, replicated)
 * goes through this one interface: in server mode the engine speaks the
 * log service protocol to `fusee server` itself.
 *
 * Driving an engine
 *   1. fusee_engine_open with a notify callback (and/or a descriptor).
 *   2. Whenever notified, when the time fusee_tick.next_wake_ms names
 *      passes, after a request, and while fusee_io.owed is set: call
 *      fusee_engine_tick and handle its events in order:
 *        START  run the workflow's code (it replays what its journal holds)
 *        WAKE   re-check the run's waiting step gates, then deliver
 *        STUCK / ENDED  drop the run's code
 *        ACK / REJECT / REPLY / REFUSE  answer the request with that token
 *        RESULT the run's result is durable
 *   3. Deliver to a run with fusee_engine_quiescent until it returns
 *      FUSEE_DELIVERY_NONE, letting the run's code run until it is blocked
 *      between two deliveries (that is what makes replay deterministic).
 *   4. Workflow code calls fusee_engine_begin_step / step_done / host for
 *      its journal operations, and fusee_engine_finish when it returns.
 *
 * Threads
 *   - An engine handle is not thread-safe and not bound to a thread: calls
 *     on one engine must not overlap, and each must happen after the
 *     previous one returned (one owning thread, a lock, or a scheduler that
 *     hands the engine from thread to thread with a synchronizing handoff,
 *     such as a channel). Separate engines are independent.
 *   - The notify callback is called from any thread (log threads, network
 *     threads, the calling thread), at most once between two ticks. It must
 *     only wake the host's loop: never call into the engine from it, never
 *     block. A wake raised on the calling thread inside a call does not
 *     notify; it sets fusee_io.owed instead.
 *   - fusee_io may be read between calls, without a call, by the thread
 *     that owns the engine at that moment.
 *   - A client handle (fusee_client_*) is thread-safe: calls may overlap.
 *
 * Memory
 *   - Inputs (fusee_str, strings in fusee_options) are borrowed for the
 *     length of the call; the engine copies what it keeps.
 *   - fusee_str outputs point into the engine. They stay valid until the
 *     next call on that engine, except fusee_tick's events and extras,
 *     which stay valid until the next fusee_engine_tick.
 *   - fusee_buf outputs are owned by the caller: free each with
 *     fusee_buf_free (a zeroed buffer is fine to free).
 *
 * Errors
 *   Every fallible function returns FUSEE_OK (0) or an error code. The
 *   message of an engine call's failure is fusee_engine_error (valid until
 *   the next call on that engine); functions without an engine handle
 *   write their message to a fusee_buf. No panic crosses the boundary: one
 *   is reported as FUSEE_PANIC, after which the engine refuses every call
 *   but fusee_engine_close.
 *
 * Payloads
 *   Payloads cross in their recorded form: JSON text, or a zero byte
 *   followed by raw bytes (no JSON text starts with a zero byte). An empty
 *   payload means none.
 */
#ifndef FUSEE_H
#define FUSEE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Bumped when a struct layout or a function signature changes, or when
 * functions or journal operations are added (a host that needs them checks
 * fusee_abi_version for at least the version that added them).
 *   2  durable streams: FUSEE_OP_STREAM_*, fusee_engine_stream_read
 *   3  keyed objects: fusee_engine_register_object, FUSEE_OP_OBJECT_*,
 *      FUSEE_OP_BEGIN_CALL; listings and inspect carry "object";
 *      fusee_engine_invocation_state for invoked workers
 *   4  paused steps: fusee_engine_resume, FUSEE_OP_STEP_PAUSED, a retry
 *      time of UINT64_MAX; state edits: FUSEE_OP_OBJECT_EDIT */
#define FUSEE_ABI_VERSION 4

/* Status codes. */
#define FUSEE_OK 0
#define FUSEE_FAILED 1
#define FUSEE_BUSY 2             /* a shard's queue is full: retry later */
#define FUSEE_NOT_FOUND 3
#define FUSEE_ALREADY_EXISTS 4
#define FUSEE_UNKNOWN_WORKFLOW 5
#define FUSEE_DETERMINISM 6      /* the code diverged from its journal */
#define FUSEE_REJECTED 7
#define FUSEE_IO 8
#define FUSEE_CLOSED 9
#define FUSEE_INVALID 10         /* a bad argument: NULL, not UTF-8, bad JSON */
#define FUSEE_PANIC 11

/* Tick events: FUSEE_EVENT_STRIDE numbers each, the first the kind and the
 * last the index of the event's first extra:
 *   WAKE   [kind, shard, run, 0, 0, 0]
 *   ACK    [kind, token, 0, 0, 0, 0]          the request succeeded
 *   REJECT [kind, token, 0, 0, 0, 0]          the run finished, or its ID is in use
 *   START  [kind, shard, run, version, started_ms, x]  extras x: ID, type, input
 *   STUCK  [kind, shard, run, 0, 0, x]        extra x: the determinism error
 *   ENDED  [kind, shard, run, 0, 0, 0]
 *   RESULT [kind, shard, run, status, 0, x]   extra x: the payload
 *   REPLY  [kind, token, status, 0, 0, x]     extra x: the payload (update, query, stream read)
 *   REFUSE [kind, token, 0, 0, 0, x]          extra x: why (a validator refused) */
#define FUSEE_EVENT_STRIDE 6
#define FUSEE_EV_WAKE 1
#define FUSEE_EV_ACK 2
#define FUSEE_EV_REJECT 3
#define FUSEE_EV_START 4
#define FUSEE_EV_STUCK 5
#define FUSEE_EV_ENDED 6
#define FUSEE_EV_RESULT 7
#define FUSEE_EV_REPLY 8
#define FUSEE_EV_REFUSE 9

/* Statuses of steps, runs and replies. */
#define FUSEE_STATUS_OK 0
#define FUSEE_STATUS_FAILED 1
#define FUSEE_STATUS_CANCELLED 2
#define FUSEE_STATUS_TIMED_OUT 3
#define FUSEE_STATUS_TERMINATED 4
#define FUSEE_STATUS_CONTINUED 5 /* the ID continued as new: follow it */
#define FUSEE_STATUS_RESET 6     /* the run was replaced by a reset: follow it */

/* Start policy bits (fusee_start.policy, and FUSEE_OP_BEGIN_CHILD flags):
 * when the ID is running, bits 0-1; when it finished, bits 2-3; a child's
 * parent-close policy, bits 4-5; a detached child, bit 6. */
#define FUSEE_RUNNING_FAIL 0
#define FUSEE_RUNNING_USE_EXISTING 1
#define FUSEE_RUNNING_TERMINATE_EXISTING 2
#define FUSEE_FINISHED_ALLOW (0 << 2)
#define FUSEE_FINISHED_ALLOW_IF_FAILED (1 << 2)
#define FUSEE_FINISHED_REFUSE (2 << 2)
#define FUSEE_FINISHED_USE_EXISTING (3 << 2)
#define FUSEE_CLOSE_TERMINATE (0 << 4)
#define FUSEE_CLOSE_REQUEST_CANCEL (1 << 4)
#define FUSEE_CLOSE_ABANDON (2 << 4)
#define FUSEE_CHILD_DETACHED (1 << 6)

typedef struct fusee_engine fusee_engine;
typedef struct fusee_client fusee_client;

/* Borrowed bytes (see Memory). */
typedef struct fusee_str {
    const uint8_t *ptr;
    size_t len;
} fusee_str;

/* Bytes the caller owns; free with fusee_buf_free. */
typedef struct fusee_buf {
    uint8_t *ptr;
    size_t len;
    size_t cap;
} fusee_buf;

typedef struct fusee_key {
    const char *id;
    const uint8_t *key; /* 32 bytes */
} fusee_key;

typedef void (*fusee_notify_fn)(void *user);

typedef struct fusee_options {
    uint32_t size;               /* sizeof(fusee_options) */
    const char *durability;      /* "local" (NULL), "memory", "server", "replicated" */
    const char *dir;             /* local: the data directory (./fusee-data); replicated: this member's */
    const char *address;         /* server: host:port; replicated: ?cert=..&key=..&ca=..&token-file=.. */
    int32_t replica_id;          /* replicated: this member's index in peers; else -1 */
    uint32_t n_peers;
    const char *const *peers;    /* replicated: three or five addresses */
    uint32_t shards;             /* 0: the default (1 for a new engine; the server's otherwise) */
    uint32_t keep_history;       /* keep finished runs' journals (inspect, export) */
    uint32_t archive_history;
    int32_t inline_flush;        /* -1 default (on), 0: every flush on the log thread */
    int64_t park_after_ms;       /* -1 for each default */
    int64_t retention_ms;
    int64_t checkpoint_interval_ms;
    int64_t segment_bytes;
    const char *flush;           /* "fsync", "fdatasync", "fullfsync", "dsync", or NULL */
    uint32_t n_codec_keys;
    const fusee_key *codec_keys; /* AES-256-GCM payload encryption; the first key encrypts */
    fusee_notify_fn notify;      /* see Threads; may be NULL with notify_fd */
    void *notify_user;
    int32_t notify_fd;           /* -1, or a descriptor to write one byte to (duplicated) */
} fusee_options;

/* Shared with the host; read it between calls. */
typedef struct fusee_io {
    uint32_t owed;               /* tick soon; the host may clear it */
    uint32_t shards;
    const uint64_t *commits;     /* per shard: a gate at or below it is open */
} fusee_io;

typedef struct fusee_step {
    uint32_t seq;
    uint32_t replay;             /* 1: recorded; the function does not run */
    uint64_t gate;
    uint32_t attempt;
    uint32_t open;               /* 1: run this attempt now */
    uint64_t not_before_ms;
} fusee_step;

#define FUSEE_DELIVERY_NONE 0
#define FUSEE_DELIVERY_RESOLVE 1 /* operation seq settled with status and payload */
#define FUSEE_DELIVERY_HELD 2    /* a step result the host recorded with KEEP: use the kept value */
#define FUSEE_DELIVERY_CANCEL 3  /* operations seqs cancelled; payload: the reason */
#define FUSEE_DELIVERY_UPDATE 4  /* start an update's handler (validate first if token >= 0) */

typedef struct fusee_delivery {
    uint32_t kind;
    uint32_t seq;
    uint32_t status;
    uint32_t n_seqs;
    fusee_str payload;
    const uint32_t *seqs;
    uint64_t ordinal;            /* update */
    int64_t token;               /* update: -1 recorded; else validate and decide */
    fusee_str name;              /* update */
    fusee_str id;                /* update */
} fusee_delivery;

typedef struct fusee_tick {
    const uint64_t *events;      /* n_events * FUSEE_EVENT_STRIDE numbers */
    size_t n_events;
    const fusee_str *extras;
    size_t n_extras;
    int64_t next_wake_ms;        /* Unix ms to tick again; -1 none; at or before now: at once */
} fusee_tick;

typedef struct fusee_start {
    fusee_str id;
    fusee_str wtype;
    fusee_str input;
    uint32_t version;            /* at least 1 */
    uint32_t policy;             /* FUSEE_RUNNING_* | FUSEE_FINISHED_* */
    uint64_t deadline_ms;        /* absolute Unix ms; 0 none */
    int64_t token;               /* ACK when durable (REJECT when refused); -1 none */
    uint32_t has_update;         /* update-with-start: */
    uint32_t _reserved;
    fusee_str update_name;
    fusee_str update_id;
    fusee_str update_arg;        /* empty: null */
    int64_t update_token;        /* REPLY with the handler's result, or REFUSE */
} fusee_start;

typedef struct fusee_result {
    uint32_t state;              /* 0 not found, 1 running, 2 done */
    uint32_t shard;
    uint64_t run;
    uint32_t status;
    uint32_t _reserved;
    fusee_str payload;
} fusee_result;

/* ---------------------------------------------------------------- library */

uint32_t fusee_abi_version(void);
const char *fusee_version(void);
void fusee_buf_free(fusee_buf *buf);

/* --------------------------------------------------------------- lifecycle */

/* Opens an engine (in server and replicated mode this connects, and may
 * wait for the previous worker's lease or for this member to lead). */
int32_t fusee_engine_open(const fusee_options *options, fusee_engine **engine, fusee_buf *error);
/* Flushes, checkpoints and frees the engine (also after a panic). */
int32_t fusee_engine_close(fusee_engine *engine, fusee_buf *error);
/* The last failed call's message. */
fusee_str fusee_engine_error(const fusee_engine *engine);
fusee_io *fusee_engine_io(fusee_engine *engine);

/* -------------------------------------------------------------- driving */

int32_t fusee_engine_tick(fusee_engine *engine, fusee_tick *tick);
/* Server mode: the next call a client sent through the server, as JSON
 * {"relay": "<number>", "method": ..., "args": {...}}; *found 0 if none. */
int32_t fusee_engine_next_call(fusee_engine *engine, fusee_str *json, uint32_t *found);
/* Answers a relayed call: {"value": ...} or {"error": "...", "kind": "not_found"}. */
int32_t fusee_engine_reply_call(fusee_engine *engine, uint64_t relay, fusee_str json);
/* Serves the HTTP API and the UI at `addr` ("127.0.0.1:0" for any port) on a
 * thread of its own; an empty `token` serves without one (loopback only).
 * Its calls arrive through fusee_engine_next_call, as a server's do, and the
 * engine's notify callback fires when one is waiting. *bound is the URL it
 * listens on (http://host:port). */
int32_t fusee_engine_serve(fusee_engine *engine, fusee_str addr, fusee_str token, fusee_str *bound);
/* An invoked worker (an address with invocation=<id>, docs/server-mode.md,
 * "Invoked workers"): *state is "running" until every held shard is idle
 * ("idle"), the time budget ends ("expired") or the server is lost ("lost");
 * empty for any other engine. `awaiting`: the workflow types the host has
 * runs of and no code for. Poll every few milliseconds; once it is not
 * "running", close the engine, which reports to the server. */
int32_t fusee_engine_invocation_state(fusee_engine *engine, const fusee_str *awaiting, uint32_t n_awaiting,
                                      fusee_str *state);

/* ------------------------------------------------------------- requests
 * Answered later by tick events carrying `token` (choose unique tokens). */

/* The request is by value (its strings are borrowed for the call). */
int32_t fusee_engine_start(fusee_engine *engine, fusee_start request, uint32_t *existed);
int32_t fusee_engine_signal(fusee_engine *engine, fusee_str id, fusee_str name, fusee_str payload,
                            uint64_t token);
/* Cancels (terminate 0) or terminates (1) a run. */
int32_t fusee_engine_cancel(fusee_engine *engine, fusee_str id, fusee_str reason, uint32_t terminate,
                            uint64_t token);
/* Resumes a run's paused steps: each runs again with a fresh retry budget.
 * Refused (rejected) if nothing is paused. A retry time of UINT64_MAX in
 * `fusee_engine_step_retry` pauses a step. */
int32_t fusee_engine_resume(fusee_engine *engine, fusee_str id, uint64_t token);
int32_t fusee_engine_update(fusee_engine *engine, fusee_str id, fusee_str name, fusee_str update_id,
                            fusee_str arg, uint64_t token);
/* Finishes an async completion: `completion` is the token text. */
int32_t fusee_engine_complete(fusee_engine *engine, fusee_str completion, uint32_t ok, fusee_str payload,
                              uint64_t token);
int32_t fusee_engine_reset(fusee_engine *engine, fusee_str id, fusee_str before_step,
                           uint32_t reapply_signals, uint32_t version, uint64_t token);
/* A query's answer is released (ACK) once position lsn is durable. */
int32_t fusee_engine_ack_at(fusee_engine *engine, uint32_t shard, uint64_t lsn, uint64_t token);
/* A query against built-in code (schedules, queue coordinators): REPLY. */
int32_t fusee_engine_query_native(fusee_engine *engine, fusee_str id, fusee_str name, fusee_str arg,
                                  uint64_t token);
/* Reads stream `name` of workflow `id` from offset `from` (of run `run` when
 * it is not negative: the "run" an earlier read answered, to stay on it when
 * the ID moves on). REPLY carries the answer as JSON {"run", "from", "next",
 * "chunks", "bytes", "end"} (see docs/features.md#streams) at once when
 * durable chunks or the stream's end are there, else once some are, or with
 * no chunks after wait_ms (a long poll: ask again from "next"). Only durable
 * chunks are handed out. FUSEE_NOT_FOUND if there is no such run. In server
 * mode a client's "streamRead" call arrives through fusee_engine_next_call
 * with args {"id", "run", "name", "from", "waitMs"}: answer it with this
 * read's JSON as the value. */
int32_t fusee_engine_stream_read(fusee_engine *engine, fusee_str id, int64_t run, fusee_str name,
                                 uint64_t from, uint64_t wait_ms, uint64_t token);

/* ---------------------------------------------------------------- reads */

int32_t fusee_engine_result(fusee_engine *engine, fusee_str id, fusee_result *result);
/* {"id","type","version","status","startedMs","finishedMs","error","shard","run",
 *  "attributes","journal": [normalized records]}, and for a key of an object
 *  type "object": {"state": {...}, "running": [{"handler","id"}],
 *  "waiting": [{"handler","id"}]}; *found 0 if no such run. */
int32_t fusee_engine_inspect(fusee_engine *engine, fusee_str id, fusee_str *json, uint32_t *found);
/* A JSON array of run summaries ({"id", ..., "attributes", "object": true for a
 * key of an object type}); `filter` is a JSON object, or empty for all. */
int32_t fusee_engine_list(fusee_engine *engine, fusee_str filter, uint32_t limit, fusee_str *json);
/* A run's journal as an export file (.fjnl). */
int32_t fusee_engine_export_journal(fusee_engine *engine, fusee_str id, fusee_str *file, uint32_t *found);
int32_t fusee_engine_shard_metrics(fusee_engine *engine, fusee_str *json);
/* Where a query's answer comes from: foreign 1 if host code answers it. */
int32_t fusee_engine_query_point(fusee_engine *engine, fusee_str id, uint32_t *shard, uint64_t *run,
                                 uint64_t *lsn, uint32_t *foreign);
/* 0: no code slot; else 1 | replaying << 1 | live work pending << 2. */
int32_t fusee_engine_code_state(fusee_engine *engine, uint32_t shard, uint64_t run, uint32_t *state);
int32_t fusee_engine_limit(fusee_engine *engine, fusee_str name, uint32_t concurrent, double per_second);
int32_t fusee_engine_limit_usage(fusee_engine *engine, fusee_str name, uint32_t *held, uint64_t *waiting);
/* Marks `name` as an object type (docs/features.md#objects-keyed-state) whose
 * handlers named in `shared` are shared (every other one is exclusive): a
 * start of the type creates a key's run (ID "<type>/<key>"), and its calls,
 * each an update of that run, take turns. Register it right after the engine
 * opens, then register the type's code as a workflow (version 1 unless the
 * host versions it): code that registers every handler as an update (looked
 * up when a call is delivered: a recovered key can be delivered a recorded
 * call as soon as its code starts), answers the query "fusee.state" with
 * FUSEE_OP_OBJECT_STATE, and never returns. A call from outside is an
 * update-with-start of "<type>/<key>" with FUSEE_RUNNING_USE_EXISTING, input
 * null and the update's ID; a one-way call's ID starts with "fusee.send/". */
int32_t fusee_engine_register_object(fusee_engine *engine, fusee_str name, const fusee_str *shared,
                                     uint32_t n_shared);

/* --------------------------------------------------------- workflow code
 * Called by a run's code while it runs (the run is shard/run of its START). */

#define FUSEE_STEP_READ_ONLY 1   /* gate on nothing earlier */
#define FUSEE_STEP_INLINE 2      /* a lone workflow may flush its gate here */
/* Begins a step; for one that runs, checks its gate. A shut gate wakes the
 * run (WAKE) when it may open: call fusee_engine_step_gate again then. */
int32_t fusee_engine_begin_step(fusee_engine *engine, uint32_t shard, uint64_t run, fusee_str name,
                                uint32_t flags, fusee_step *step);
int32_t fusee_engine_step_gate(fusee_engine *engine, uint32_t shard, uint64_t run, uint64_t gate,
                               uint64_t not_before_ms, uint32_t inline_flush, uint32_t *open);

#define FUSEE_DONE_OK 1          /* else the step failed for good; payload: the message */
#define FUSEE_DONE_KEEP 2        /* the host keeps the value: its delivery is HELD */
int32_t fusee_engine_step_done(fusee_engine *engine, uint32_t shard, uint64_t run, uint32_t seq,
                               uint32_t flags, fusee_str payload);
/* Records a failed attempt that will be retried at retry_at_ms; *gate is the next attempt's gate. */
int32_t fusee_engine_step_retry(fusee_engine *engine, uint32_t shard, uint64_t run, uint32_t seq,
                                uint64_t retry_at_ms, fusee_str error, uint64_t *gate);

/* Every other journal operation: n0..n2 and s0..s2 are its arguments (unused
 * ones zero / empty), result (room for five numbers, may be NULL) and data
 * (may be NULL) its outputs. Arguments are by value so a host can pass
 * borrowed strings without building an array that points at them.
 *   op                        n0, n1, n2              s0, s1, s2              result / data
 *   BEGIN_AWAIT              -                       name                    seq, kind (0 run, 1 replay, 2 waiting), gate, attempt, not_before
 *   BEGIN_SLEEP              fire_at_ms              name                    seq
 *   BEGIN_WAIT               deadline_ms (0 none)    name                    seq
 *   BEGIN_CHILD              flags, deadline_ms      type, id, input         seq, attached (0: detached, no seq)
 *   SEND                     -                       target, name, payload   -
 *   AWAITING                 seq                     -                       -  (the token went out)
 *   HEARTBEAT                seq                     progress                -
 *   PATCHED                  deprecated              id                      patched
 *   SET_ATTRIBUTES           -                       attributes (JSON)       -
 *   UPDATE_DONE              ordinal, status         payload                 -
 *   CONTINUE_AS_NEW          version (0 same)        input                   -
 *   JOURNAL_SIZE             -                       -                       size
 *   CANCEL_WORKFLOW          -                       target, reason          -
 *   FORGET                   seq                     -                       -  (a race's loser)
 *   LIMIT_TICKET             -                       -                       ticket
 *   ACQUIRE                  ticket, priority (i64), weight   limit, fairness key   0 granted, 1 retry at result[1], 2 wait for a wake
 *   RELEASE                  ticket                  limit                   -
 *   WAKE_AT                  at_ms                   -                       -
 *   VALUE                    kind (0 now, 1 random, 2 uuid)   candidate      data: the recorded value
 *   KEY                      seq (0xFFFFFFFF none)   -                       data: the idempotency key
 *   TOKEN                    seq                     -                       data: the completion token
 *   PROGRESS                 seq                     -                       found; data: the progress
 *   STREAM_OPEN              writer                  name                    chunks recorded, closed
 *   STREAM_APPEND            writer                  name, chunk             -
 *   STREAM_CLOSE             writer                  name                    -
 *   STREAM_RECORDED          writer, limit           name                    count; data: the chunks
 *   OBJECT_GET               exclusive               key                     found; data: the value (JSON)
 *   OBJECT_SET               1 set, 0 clear          key, value (JSON)       -
 *   OBJECT_KEYS              exclusive               -                       count; data: the keys (a JSON array)
 *   OBJECT_STATE             -                       -                       data: the committed state (a JSON object)
 *   BEGIN_CALL               one-way                 type/key, handler, arg  seq, attached (0: one-way, no seq)
 *
 * Streams (docs/features.md#streams): writer is FUSEE_NO_SEQ for workflow
 * code, else the seq of the step whose function appends (from its own
 * thread, the host hands the call to the engine's owner). Workflow code
 * opens a stream of the run, which its steps may append to as well; a
 * step's function opens one of the step, which the step's final result
 * completes. A chunk is a payload in its recorded form (JSON, or a zero
 * byte and raw bytes); appending never waits for the disk, and workflow
 * code replaying an append records nothing. STREAM_OPEN in a step that runs
 * again says how many chunks its earlier attempts recorded (after a crash,
 * the durable ones) and whether one of them completed the stream (its
 * appends are then dropped); STREAM_RECORDED returns the first `limit` of
 * those chunks, each a 4-byte little-endian length and then the payload.
 *
 * Objects (docs/features.md#objects-keyed-state), from a key's handlers:
 * OBJECT_GET reads a value of the key's state (an exclusive handler,
 * exclusive 1, sees its own writes; a shared one the committed state);
 * OBJECT_SET writes one from the exclusive handler running (committed with
 * its UPDATE_DONE; a failed handler's writes are dropped). BEGIN_CALL, from
 * any workflow code, calls handler `handler` of the key whose workflow ID is
 * s0 ("<type>/<key>"; a type's name has no '/'): a journal operation sent
 * through the durable outbox and run once; a two-way call's result resolves
 * its seq as a child's does. */
#define FUSEE_OP_BEGIN_AWAIT 1
#define FUSEE_OP_BEGIN_SLEEP 2
#define FUSEE_OP_BEGIN_WAIT 3
#define FUSEE_OP_BEGIN_CHILD 4
#define FUSEE_OP_SEND 5
#define FUSEE_OP_AWAITING 6
#define FUSEE_OP_HEARTBEAT 7
#define FUSEE_OP_PATCHED 8
#define FUSEE_OP_SET_ATTRIBUTES 9
#define FUSEE_OP_UPDATE_DONE 10
#define FUSEE_OP_CONTINUE_AS_NEW 11
#define FUSEE_OP_JOURNAL_SIZE 12
#define FUSEE_OP_CANCEL_WORKFLOW 13
#define FUSEE_OP_FORGET 14
#define FUSEE_OP_LIMIT_TICKET 15
#define FUSEE_OP_ACQUIRE 16
#define FUSEE_OP_RELEASE 17
#define FUSEE_OP_WAKE_AT 18
#define FUSEE_OP_VALUE 19
#define FUSEE_OP_KEY 20
#define FUSEE_OP_TOKEN 21
#define FUSEE_OP_PROGRESS 22
#define FUSEE_OP_STREAM_OPEN 23
#define FUSEE_OP_STREAM_APPEND 24
#define FUSEE_OP_STREAM_CLOSE 25
#define FUSEE_OP_STREAM_RECORDED 26
#define FUSEE_OP_OBJECT_GET 27
#define FUSEE_OP_OBJECT_SET 28
#define FUSEE_OP_OBJECT_KEYS 29
#define FUSEE_OP_OBJECT_STATE 30
#define FUSEE_OP_BEGIN_CALL 31
/* STEP_PAUSED: o0 is 1 while step n0 is paused (its retries ran out and no
 * resume has come; the engine wakes the run when one does), else 0. */
#define FUSEE_OP_STEP_PAUSED 32
/* OBJECT_EDIT, from the handler "fusee.state.edit" every object type
 * registers: applies the state edit s0 (JSON) as that exclusive handler's
 * writes; the bytes are the edit's answer, its JSON. */
#define FUSEE_OP_OBJECT_EDIT 33
#define FUSEE_NO_SEQ 0xFFFFFFFFu /* a stream operation's writer: workflow code */
int32_t fusee_engine_host(fusee_engine *engine, uint32_t shard, uint64_t run, uint32_t op,
                          uint64_t n0, uint64_t n1, uint64_t n2, fusee_str s0, fusee_str s1,
                          fusee_str s2, uint64_t *result, fusee_str *data);

/* The run's next delivery (kind NONE when there is none). */
int32_t fusee_engine_quiescent(fusee_engine *engine, uint32_t shard, uint64_t run,
                               fusee_delivery *delivery);
/* A validator's decision on an UPDATE delivery with a token: *ordinal is
 * the update's ordinal to run the handler under, or -1 (refused). */
int32_t fusee_engine_decide_update(fusee_engine *engine, uint32_t shard, uint64_t token, uint32_t ok,
                                   fusee_str message, int64_t *ordinal);
/* The run's code returned: status FUSEE_STATUS_*, payload the result or message. */
int32_t fusee_engine_finish(fusee_engine *engine, uint32_t shard, uint64_t run, uint32_t status,
                            fusee_str payload);
/* The code cannot go on (a recorded update with no handler): stop the run. */
int32_t fusee_engine_stuck(fusee_engine *engine, uint32_t shard, uint64_t run, fusee_str message);

/* ------------------------------------------------------- free functions */

/* One debounced start (kind 0: a window, b max wait) or batched event
 * (kind 1: a max events, b max wait) as the update-with-start that carries
 * it; c is the dedup window; NaN takes the default; times in ms. result
 * receives four buffers: coordinator type, coordinator ID, start input
 * (JSON) and update argument (JSON). */
int32_t fusee_queue_request(uint32_t kind, fusee_str workflow, fusee_str key, fusee_str event_id,
                            fusee_str input, double a, double b, double c, fusee_buf result[4],
                            fusee_buf *error);
/* FUSEE_OK, or FUSEE_INVALID with the problem. */
int32_t fusee_validate_schedule(fusee_str spec, fusee_buf *problem);

/* ----------------------------------------------------------------- client */

int32_t fusee_client_connect(fusee_str address, fusee_client **client, fusee_buf *error);
/* Blocks until the worker answers: result is its JSON value, or the error message. */
int32_t fusee_client_call(const fusee_client *client, fusee_str method, fusee_str args,
                          fusee_buf *result);
/* Shuts the connection (waking calls in progress); the handle stays valid. */
int32_t fusee_client_close(const fusee_client *client);
/* Frees the handle once no call is running on it. */
void fusee_client_free(fusee_client *client);

#ifdef __cplusplus
}
#endif

#endif /* FUSEE_H */
