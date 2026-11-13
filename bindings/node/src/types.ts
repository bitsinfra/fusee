// Options and shapes the public API takes.
import { Codec, Serializer, json } from "./codec.js";
import { WorkflowDefinition, workflow } from "./definitions.js";
import { FuseeError, RejectedError, call } from "./errors.js";
import { Ctx } from "./execution.js";
import { native } from "./native.js";
import { Telemetry } from "./telemetry.js";

// ---------------------------------------------------------------- public types

export type Duration = number | string;

export interface RetryPolicy {
  /** Attempts including the first (default 3). */
  attempts?: number;
  /** "fixed": every delay is `initial` (the same as `factor: 1`). */
  backoff?: "exponential" | "fixed";
  /** First delay (default 100ms). */
  initial?: Duration;
  /** Each delay is the last times this (default 2). */
  factor?: number;
  /** Longest delay (default 10s). */
  max?: Duration;
  /** Each delay is drawn between half and all of it (default true), so retries spread out. */
  jitter?: boolean;
  /** No attempt starts once this long has passed since the first failure. */
  maxElapsed?: Duration;
  /** Errors never retried: class names or classes (a `FatalError`, or `retryable = false`, never is). */
  nonRetryable?: (string | (new (...args: any[]) => Error))[];
  /** Retried only while this returns true. */
  retryIf?: (error: unknown) => boolean;
  /**
   * When the retries run out on an error that could be retried: `"fail"`
   * (the default) fails the step; `"pause"` pauses the run, with its parent
   * and callers waiting, until `run.resume()` runs the step again with a
   * fresh set of attempts. Unset: the engine's `onExhausted`.
   */
  onExhausted?: "fail" | "pause";
}

/** What `ctx.awaitCompletion`'s `send` gets: the token, and the URL that completes it over HTTP when the engine serves its API. */
export interface Completion {
  token: string;
  /** `POST` the value as JSON here (`…/v1/fail/<token>` fails it). */
  url?: string;
}

export interface StepOptions {
  retry?: RetryPolicy;
  /** Each attempt fails (and may be retried) after this long. */
  timeout?: Duration;
  /** An attempt fails (and may be retried) if this long passes without `ctx.heartbeat`. */
  heartbeatTimeout?: Duration;
  /** Take a permit of this limit (see `engine.limit`) before each attempt. */
  limit?: string;
  /**
   * While waiting for the limit's permit, steps with a higher priority go
   * first (an integer, default 0). Not journaled.
   */
  priority?: number;
  /**
   * Steps waiting at one priority take turns by this key (a tenant ID, say),
   * so a key with a deep backlog cannot starve the others. Default: the
   * shared group of steps without a key.
   */
  fairnessKey?: string;
  /** Permits the fairness key takes per turn (default 1). */
  fairnessWeight?: number;
  /**
   * The step only reads: its function changes nothing outside the process,
   * so it runs without waiting for the records before it to be durable. A
   * crash can make it run again with different inputs, which is harmless for
   * a read and wrong for anything else. Later steps and outputs still wait
   * for its result. Ignored by `awaitCompletion`.
   */
  readOnly?: boolean;
}

export type ParentClose = "terminate" | "request-cancel" | "abandon";

export interface ChildOptions {
  /** What happens to the child when this workflow closes (default terminate). */
  parentClose?: ParentClose;
  timeout?: Duration;
}

/** Observe one replica, including while it waits to become primary. */
export class ReplicaMetrics {
  /** @internal */
  readonly handle = new native.NativeReplicaMetrics();
  snapshot(): { view: number; primary: boolean; ready: boolean; stopped: boolean; op: number; commit: number; applied: number; sentBytes: number; receivedBytes: number } | null {
    const value=this.handle.snapshot();
    return value == null ? null : JSON.parse(value);
  }
}

export interface OpenOptions {
  /** Workflow definitions this engine runs (`workflow(name, fn)`). */
  workflows?: readonly WorkflowDefinition<any, any>[];
  /** How payloads become JSON and back: `rich` from `@bitsinfra/fusee/rich` keeps dates, maps, sets and bigints. */
  serializer?: Serializer;
  /**
   * `"dev"` (or `FUSEE_PROFILE=dev`, which `fusee dev` sets; never inferred):
   * says so on stderr, keeps every run's history, flushes with `fsync`
   * rather than `F_FULLFSYNC` on macOS (a process crash loses nothing; power
   * loss may), and serves the UI.
   */
  profile?: "dev" | "production";
  /**
   * Serve the HTTP API and UI here once open (`"off"`: never). Under the dev
   * profile the default is `FUSEE_HTTP`, else 127.0.0.1:7601, else a free port.
   */
  http?: string;
  /** Failures record their stack trace (default true). */
  recordStacks?: boolean;
  /**
   * What a step does when its retries run out, for every step whose retry
   * policy does not say: `"pause"` pauses its run until `run.resume()`.
   * Default `"fail"`.
   */
  onExhausted?: "fail" | "pause";
  dir?: string;
  durability?: "local" | "memory" | "server" | "replicated";
  /** Server socket, e.g. 127.0.0.1:7600 or unix:/path/to/socket. */
  address?: string;
  /** Member index in peers for replicated mode. */
  replicaId?: number;
  replicaMetrics?: ReplicaMetrics;
  peers?: string[];
  shards?: number;
  retention?: Duration;
  parkAfter?: Duration;
  checkpointInterval?: Duration;
  /** Override the flush call. Default: F_FULLFSYNC on macOS, fdatasync on Linux. */
  flush?: "fullfsync" | "fsync" | "fdatasync" | "dsync";
  segmentBytes?: number;
  /** Keep finished runs' whole journals (audit, inspect, export, reset). */
  keepHistory?: boolean;
  /** Archive full journals on disk after hot retention expires. */
  archiveHistory?: boolean;
  /** Payload encryption, e.g. `aesGcm({ k1: key })`. */
  codec?: Codec;
  /** OpenTelemetry spans and metrics, e.g. `openTelemetry(await import("@opentelemetry/api"))`. */
  telemetry?: Telemetry;
  /**
   * Flush on the JavaScript thread when nothing else is pending (default
   * true). False sends every flush to the log thread, so the event loop
   * never waits for the disk.
   */
  inlineFlush?: boolean;
  /**
   * "native" (default on Node): a libuv handle runs the engine's turns back
   * to back in one event-loop callback, each entered so that Node drains
   * `process.nextTick` callbacks and microtasks when it returns, as a
   * macrotask would. "macrotask" (always on Bun): each turn is its own
   * event-loop callback.
   */
  driver?: "native" | "macrotask";
  /**
   * Run workflow code on this many worker threads (local or memory
   * durability). Each worker owns a set of shards (shard `s` belongs to
   * worker `s % workers`), loads `module` to register its own copy of the
   * workflows, and appends to the one shared log; this thread routes each
   * call to the worker that owns the workflow ID's shard. A new directory
   * gets one shard per worker unless `shards` says otherwise; there are at
   * most as many workers as shards. Values crossing to a worker (query
   * arguments and answers) are copied with the structured clone algorithm.
   */
  workers?: number;
  /**
   * With `workers`: the ES module (a path or file URL) each worker imports.
   * Its default export (or `register`) is called with the worker's engine
   * and registers workflows on it with `engine.workflow(name, fn)`.
   */
  module?: string | URL;
}

export interface IdPolicy {
  /** When the ID is running: fail (default), return it, or terminate it and start anew. */
  running?: "fail" | "use-existing" | "terminate-existing";
  /** When its last run finished within retention: start anew (default), only if it failed, refuse, or return it. */
  finished?: "allow" | "allow-if-failed" | "refuse" | "use-existing";
}

export interface StartOptions {
  /** Select a registered workflow version. Defaults to the newest. */
  version?: number;
  /** The run ends TimedOut after this long. */
  timeout?: Duration;
  idPolicy?: IdPolicy;
}

export interface UpdateOptions {
  /** A retried call with the same ID gets the recorded result. */
  id?: string;
}

export interface UpdateHandler<A, R> {
  /** Runs first; throw (or return a string) to refuse. A refusal records nothing. */
  validate?: (arg: A) => void | boolean | string;
  /** Runs once per accepted update; may run steps. */
  handle: (arg: A) => R | Promise<R>;
}

export interface ScheduleOptions {
  /** Five-field cron (`0 2 * * *`) or `@daily`, `@hourly`... */
  cron?: string;
  /** A fixed interval instead of cron. */
  every?: Duration;
  offset?: Duration;
  /** IANA time zone for cron (default UTC). */
  timezone?: string;
  overlap?: "skip" | "buffer-one" | "allow-all" | "cancel-other";
  jitter?: Duration;
  catchupWindow?: Duration;
  paused?: boolean;
}

export interface DebounceOptions {
  /** The run starts once this long passes without another start under the key. */
  window: Duration;
  /** ...or this long after the window's first start, whichever is first (default: no bound). */
  maxWait?: Duration;
  /** How long the key remembers event IDs after its last event (default 10m). */
  dedupWindow?: Duration;
  /** The event ID: a retried call with the same ID joins once and gets the same run. */
  id?: string;
}

export interface BatchOptions {
  /** The run starts as soon as the batch holds this many events (default 100, at most 10,000). */
  maxEvents?: number;
  /** ...or this long after the batch's first event (default 1s). */
  maxWait?: Duration;
  /** How long the key remembers event IDs after its last event (default 10m). */
  dedupWindow?: Duration;
  /** The event ID: a retried call with the same ID adds the event once and gets the same run. */
  id?: string;
}

export type WorkflowFn<I, O> = (ctx: Ctx, input: I) => Promise<O>;

export interface Pending {
  kind: "step" | "sleep" | "wait" | "child" | "await";
  name: string;
  resolve: (v: unknown) => void;
  reject: (e: unknown) => void;
}

export function idPolicyBits(p: IdPolicy = {}): number {
  const r = { fail: 0, "use-existing": 1, "terminate-existing": 2 }[p.running ?? "fail"];
  const f = { allow: 0, "allow-if-failed": 1, refuse: 2, "use-existing": 3 }[p.finished ?? "allow"];
  return r | (f << 2);
}

export function closeBits(c: ParentClose = "terminate"): number {
  return { terminate: 0, "request-cancel": 1, abandon: 2 }[c] << 4;
}

export const DETACHED = 1 << 6;
/** Deliveries at completion allowed in one tick, across runs, and for how long after it began (ms). */
export const INLINE_PER_TICK = 256;
export const INLINE_TICK_MS = 2;
export const SCHEDULE_TYPE = "fusee.schedule";
/** The coordinator type of delayed starts (crates/fusee/src/delay.rs). */
export const DELAY_TYPE = "fusee.delay";
/** A run waiting for its turn (see `Concurrency`). */
export const QUEUED_TYPE = "fusee.queued";
export const CONTROL = "fusee.schedule.control";
/** The update debounce and batch coordinators take events on (`fusee::queue::EVENT`). */
export const EVENT = "fusee.event";
/** A coordinator ended before its handler took the event (`fusee::queue::ENDED`): send it again. */
const ENDED = "the workflow ended before the update finished";
export const USE_EXISTING = idPolicyBits({ running: "use-existing" });

/**
 * Sends an event (`send`: the update-with-start) until a coordinator takes
 * it: one that was ending refused it with `ENDED` (through a server, behind
 * "workflow failed: " prefixes) before its handler ran. Resolves with the
 * receipt's run ID.
 */
export async function untilTaken(coordinator: string, send: () => Promise<unknown>): Promise<string> {
  for (let i = 0; i < 100; i++) {
    try {
      return ((await send()) as { id: string }).id;
    } catch (e) {
      if (!(e instanceof Error) || e.message.replace(/^(?:workflow failed: )+/, "") !== ENDED) throw e;
    }
  }
  throw new FuseeError(`coordinator '${coordinator}' kept ending before taking the event`);
}

/** One debounced start or batched event as `fusee::queue::request` builds it: `[type, coordinator ID, start input, update argument]`. */
export function queueRequest(
  kind: "debounce" | "batch", workflow: string, key: string, eventId: string, input: unknown,
  a: number | undefined, b: number | undefined, c: number | undefined,
): [string, string, string, string] {
  if (input instanceof Uint8Array) throw new RejectedError(`a ${kind === "debounce" ? "debounced input" : "batched event"} is JSON, not bytes`);
  const [wtype = "", id = "", state = "", arg = ""] = call(() => native.queueRequest(kind, workflow, key, eventId, json(input), a, b, c));
  return [wtype, id, state, arg];
}
