// errors
export declare class AlreadyExistsError extends FuseeError {
    readonly id?: string | undefined;
    constructor(message: string, id?: string | undefined);
}
export declare class BusyError extends FuseeError {
    readonly retryable = true;
    constructor(message?: string);
}
export declare class CancelledError extends FuseeError {
    readonly reason: string;
    constructor(reason: string);
}
export declare class ChildError extends FuseeError {
    readonly workflowId: string;
    readonly cause: Failure;
    readonly retryable: boolean;
    constructor(workflowId: string, cause: Failure);
}
export declare class ClosedError extends FuseeError {
    constructor(message?: string);
}
export declare class CodecError extends FuseeError {
    constructor(message: string);
}
export declare class ConnectionError extends FuseeError {
    readonly retryable = true;
    constructor(message: string);
}
/** Where each error code's page lives. */
export declare const DOCS_BASE = "https://github.com/bitsinfra/fusee/blob/main/docs/errors.md";
export declare class DeterminismError extends FuseeError {
    readonly workflowId?: string | undefined;
    constructor(message: string, workflowId?: string | undefined);
}
/**
 * Throw (or extend) to fail a step without retries:
 *
 *     class CardDeclined extends FatalError { code = "card_declined" }
 *     throw new CardDeclined("card declined", { details: { last4: "4242" } });
 *
 * Any error with `retryable = false` is final as well.
 */
export declare class FatalError extends Error {
    readonly retryable = false;
    code?: string;
    details?: unknown;
    constructor(message?: string, opts?: {
        code?: string;
        details?: unknown;
        cause?: unknown;
    });
}
/** Every error Fusee throws. `code` is the same in every SDK; `docs` links to its page. */
export declare class FuseeError extends Error {
    readonly code: string;
    /** The failure this error carries (a step's, a child's, a workflow's). */
    readonly cause?: Failure;
    readonly retryable: boolean;
    constructor(message: string, code?: string, cause?: Failure);
    get docs(): string;
}
export declare class IoError extends FuseeError {
    constructor(message: string);
}
export declare class NotFoundError extends FuseeError {
    readonly id?: string | undefined;
    constructor(message: string, id?: string | undefined);
}
/** A request was refused and nothing was recorded: an update's validator, a stale token, a bad argument. */
export declare class RejectedError extends FuseeError {
    constructor(message: string);
}
/**
 * A step failed after its last attempt (or with a final failure). The
 * step's own failure is `cause`: `e.cause.type`, `e.cause.code`, `e.cause.details`.
 */
export declare class StepError extends FuseeError {
    readonly step: string;
    readonly cause: Failure;
    readonly retryable: boolean;
    constructor(step: string, cause: Failure | string);
}
export declare class TerminatedError extends FuseeError {
    constructor(message?: string);
}
export declare class TimeoutError extends FuseeError {
    readonly retryable = true;
    constructor(message?: string);
}
export declare class UnknownWorkflowError extends FuseeError {
    constructor(message: string);
}
/** A workflow, update or object handler failed; its failure is `cause`. */
export declare class WorkflowError extends FuseeError {
    readonly workflowId?: string | undefined;
    readonly cause: Failure;
    constructor(cause: Failure, workflowId?: string | undefined);
}
/** The page for an error code. */
export declare const docsLink: (code: string) => string;
/** The error a caller sees for a recorded failure: a step's as StepError, a child's as ChildError, anything else as WorkflowError. */
export declare function errorOf(f: Failure, workflowId?: string): FuseeError;
/** The envelope of a thrown value: type, code, details, retryability, stack and causes. Runs on failures only. */
export declare function failureOf(e: unknown, stacks?: boolean, depth?: number): Failure;
/** A recorded failure: its envelope, or old plain text read as `{ message }`. */
export declare function parseFailure(text: string): Failure;
/**
 * A failure as recorded: what a failed step, update, object call or workflow
 * carries, in every SDK. `type` is the error's class name (absent for a plain
 * `Error`); `code` and `details` are the application's; `retryable: false`
 * marks it final.
 */
export interface Failure {
    message: string;
    type?: string;
    code?: string;
    details?: unknown;
    retryable?: boolean;
    stack?: string;
    cause?: Failure;
}

// codec
/** `aesGcm({ k2: newKey, k1: oldKey })`: 32-byte keys (Buffer, or hex / base64 text). */
export declare function aesGcm(keys: Record<string, Buffer | Uint8Array | string>): Codec;
/** Payload encryption: AES-256-GCM. The first key encrypts; every key decrypts. */
export interface Codec {
    kind: "aes-gcm";
    keys: {
        id: string;
        key: Buffer;
    }[];
}
/**
 * How payloads become JSON text and back: `Engine.open({ serializer })`.
 * The default is JSON as it is; `@bitsinfra/fusee/rich` keeps dates, maps, sets and
 * bigints. One serializer per process: every engine in it reads the
 * others' payloads.
 */
export interface Serializer {
    stringify(value: unknown): string;
    parse(text: string): unknown;
}

// types
/** Observe one replica, including while it waits to become primary. */
export declare class ReplicaMetrics {
    snapshot(): {
        view: number;
        primary: boolean;
        ready: boolean;
        stopped: boolean;
        op: number;
        commit: number;
        applied: number;
        sentBytes: number;
        receivedBytes: number;
    } | null;
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
export interface ChildOptions {
    /** What happens to the child when this workflow closes (default terminate). */
    parentClose?: ParentClose;
    timeout?: Duration;
}
/** What `ctx.awaitCompletion`'s `send` gets: the token, and the URL that completes it over HTTP when the engine serves its API. */
export interface Completion {
    token: string;
    /** `POST` the value as JSON here (`…/v1/fail/<token>` fails it). */
    url?: string;
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
export type Duration = number | string;
export interface IdPolicy {
    /** When the ID is running: fail (default), return it, or terminate it and start anew. */
    running?: "fail" | "use-existing" | "terminate-existing";
    /** When its last run finished within retention: start anew (default), only if it failed, refuse, or return it. */
    finished?: "allow" | "allow-if-failed" | "refuse" | "use-existing";
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
export type ParentClose = "terminate" | "request-cancel" | "abandon";
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
export interface StartOptions {
    /** Select a registered workflow version. Defaults to the newest. */
    version?: number;
    /** The run ends TimedOut after this long. */
    timeout?: Duration;
    idPolicy?: IdPolicy;
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
export interface UpdateHandler<A, R> {
    /** Runs first; throw (or return a string) to refuse. A refusal records nothing. */
    validate?: (arg: A) => void | boolean | string;
    /** Runs once per accepted update; may run steps. */
    handle: (arg: A) => R | Promise<R>;
}
export interface UpdateOptions {
    /** A retried call with the same ID gets the recorded result. */
    id?: string;
}
export type WorkflowFn<I, O> = (ctx: Ctx, input: I) => Promise<O>;

// telemetry
/**
 * Instruments from the application's `@opentelemetry/api` module (pass the
 * module itself): a `fusee.step` span per step attempt, nested under
 * whatever is active, and the metrics `fusee.steps.executed`,
 * `fusee.step.duration`, `fusee.workflows.started` and
 * `fusee.workflows.finished`. Replayed steps record nothing.
 */
export declare function openTelemetry(api: {
    trace: {
        getTracer(name: string, version?: string): NonNullable<Telemetry["tracer"]>;
        setSpan(ctx: unknown, span: SpanLike): unknown;
        getSpanContext?(ctx: unknown): {
            traceId: string;
            spanId: string;
            traceFlags: number;
        } | undefined;
        setSpanContext?(ctx: unknown, sc: {
            traceId: string;
            spanId: string;
            traceFlags: number;
            isRemote?: boolean;
        }): unknown;
    };
    metrics: {
        getMeter(name: string, version?: string): NonNullable<Telemetry["meter"]>;
    };
    context: {
        active(): unknown;
        with<T>(ctx: unknown, fn: () => T): T;
    };
}): Telemetry;
/** The parts of an OpenTelemetry span Fusee uses. */
export interface SpanLike {
    setAttribute(key: string, value: string | number | boolean): unknown;
    setStatus(status: {
        code: number;
        message?: string;
    }): unknown;
    end(): void;
}
/**
 * OpenTelemetry instruments for an engine, structurally typed so Fusee has
 * no dependency on `@opentelemetry/api`. Build one with `openTelemetry(api)`.
 */
export interface Telemetry {
    /** `context`: the parent (from `spanIn` or `remote`); the active one when absent. */
    tracer?: {
        startSpan(name: string, options?: {
            attributes?: Attrs;
        }, context?: unknown): SpanLike;
    };
    /** The active span's W3C traceparent: a run started here joins its trace. */
    traceparent?(): string | undefined;
    /** A context whose parent is the span a traceparent names. */
    remote?(traceparent: string): unknown;
    /** A context with `span` as the active span, below `parent` (the active one when absent). */
    spanIn?(span: SpanLike, parent?: unknown): unknown;
    meter?: {
        createCounter(name: string, options?: {
            description?: string;
            unit?: string;
        }): {
            add(n: number, attrs?: Attrs): void;
        };
        createHistogram(name: string, options?: {
            description?: string;
            unit?: string;
        }): {
            record(n: number, attrs?: Attrs): void;
        };
    };
    /** Runs `fn` with `span` as the active span, so the step's own spans nest under it. */
    withSpan?<T>(span: SpanLike, fn: () => T): T;
}

// execution
/** The workflow context. */
export declare class Ctx {
    private readonly x;
    get workflowId(): string;
    get workflowType(): string;
    get version(): number;
    get runId(): number;
    private stepSeq;
    /**
     * A key stable across retries and recovery for the step whose function is
     * running (distinct for each run of a workflow ID, including a reset one).
     * Pass it to services that accept idempotency keys.
     */
    key(): string;
    /**
     * Runs `fn` as a durable step. `fn` runs only once everything this run
     * recorded before is durable (at once with `readOnly`); its result is
     * recorded and returned without waiting for the disk. On replay the
     * recorded result is returned and `fn` does not run.
     */
    step<T>(name: string, fn: ((s: StepCtx) => T | Promise<T>) | (() => T | Promise<T>), opts?: StepOptions): Promise<T>;
    /**
     * A step completed from outside: `send(token)` starts work elsewhere and
     * returns; the workflow waits until a process holding the token calls
     * `engine.complete(token, value)` or `engine.fail(token, error)`. `send`
     * is retried under the retry policy, each attempt with a new token.
     */
    awaitCompletion<T>(name: string, send: (completion: Completion) => unknown, opts?: StepOptions): Promise<T>;
    /** Progress of the running step. Coalesced; never waits for the disk. */
    heartbeat<T>(progress: T): void;
    /** The latest progress the running step reported, also from before a crash. */
    progress<T>(): T | undefined;
    /**
     * Opens the run's stream `name` for appending (readers follow it with
     * `run.stream(name)`). In workflow code it opens a stream of the run: its
     * appends are recorded once (a replay appends nothing), and `close()` or
     * the run's end completes it; steps may append to it too. Inside a step's
     * function it opens a stream of the step, which the step's result
     * completes; an attempt that runs again finds what earlier attempts
     * recorded in `recorded()` and continues after it.
     */
    stream(name: string): StreamWriter;
    /** Sleeps durably; the run is parked out of memory while it waits. */
    sleep(name: string, duration: Duration): Promise<void>;
    /** Waits for the next signal `name`. Resolves `undefined` on timeout. */
    waitFor<T>(sig: SignalDefinition<T> | string, opts?: {
        timeout?: Duration;
    }): Promise<T | undefined>;
    /** Starts a child workflow and waits for its result. */
    child<O, I = unknown>(wf: WorkflowDefinition<I, O> | string, id: string, input: I, opts?: ChildOptions): Promise<O>;
    /**
     * Starts a child workflow without waiting for it. By default it is
     * terminated when this workflow closes; `parentClose: "abandon"` lets it
     * outlive it.
     */
    startChild<I>(wf: WorkflowDefinition<I, unknown> | string, id: string, input: I, opts?: ChildOptions): void;
    /** @deprecated Use `startChild`. */
    spawnChild<I>(wtype: string, id: string, input: I, opts?: ChildOptions): void;
    /** Sends a signal to another workflow once this run is durable through here. */
    signal<T>(target: string, sig: SignalDefinition<T> | string, value: T): void;
    /** `signal` under the name every SDK uses. */
    signalWorkflow<T>(target: string, sig: SignalDefinition<T> | string, value: T): void;
    /** Sleeps durably until `at`; the run is parked out of memory while it waits. */
    sleepUntil(name: string, at: Date | number): Promise<void>;
    /**
     * Registers a step that undoes what the workflow did so far: if the
     * workflow fails or is cancelled, every registered compensation runs as a
     * step named `name`, newest first, then the failure is thrown. Register it
     * after the step it undoes succeeded:
     *
     *     const hold = await ctx.step("reserve", () => inventory.reserve(sku));
     *     ctx.compensate("release", () => inventory.release(hold));
     */
    compensate(name: string, fn: () => unknown, opts?: StepOptions): void;
    /** Asks another workflow to cancel, once this point is durable. */
    cancelWorkflow(target: string, reason?: string): void;
    /**
     * Key `key` of object type `type`, called from this workflow or handler:
     * each call is a journal operation, sent through the durable outbox and
     * run exactly once; `call` resolves with the handler's recorded result.
     */
    object(type: string, key: string): ObjectCall;
    /** Registers a read-only query handler (`run.query(name, arg)`). */
    onQuery<A, R>(name: string, fn: (arg: A) => R): void;
    /** Registers an update handler (`run.update(name, arg)`). */
    onUpdate<A, R>(name: string, handler: UpdateHandler<A, R> | ((arg: A) => R | Promise<R>)): void;
    /** Ends this run and starts a new run of the same ID with `input`, in one record. Never resolves. */
    continueAsNew<I>(input: I, opts?: {
        version?: number;
    }): Promise<never>;
    /** Journal operations this run made so far (deterministic). */
    journalSize(): number;
    /** True for new runs, and for running ones the first time they get here after the deploy. */
    patched(id: string): boolean;
    /** Marks a patch whose old branch was removed. */
    deprecatePatch(id: string): void;
    /** Records search attributes (`null` removes a key). They are never encrypted. */
    setAttributes(attributes: Record<string, unknown>): void;
    /** The current time in Unix ms, recorded. */
    now(): number;
    /** A random number in [0, 1), recorded. */
    random(): number;
    /** A random UUID, recorded. */
    uuid(): string;
    /** Like `Promise.all`, for journal operations. */
    all<T extends readonly unknown[]>(ops: T): Promise<{
        -readonly [K in keyof T]: Awaited<T[K]>;
    }>;
    /**
     * Like `Promise.race`; the winner is the first result delivered, which
     * replay reproduces. The losers are dropped (a dropped wait no longer
     * takes signals).
     */
    race<T>(ops: Promise<T>[]): Promise<T>;
}
/**
 * What a step's function gets when it declares a parameter:
 * `ctx.step("charge", (s) => pay({ idempotencyKey: s.key, signal: s.signal }))`.
 */
export declare class StepCtx {
    private readonly x;
    private readonly seq;
    /** The step's name. */
    readonly name: string;
    /** 1 on the first attempt, then 2, 3, ... */
    readonly attempt: number;
    /**
     * A key stable across retries and recovery, distinct for each step of
     * each run: pass it to services that accept idempotency keys.
     */
    get key(): string;
    get workflowId(): string;
    /**
     * Aborted when this attempt times out (or misses its heartbeat), or the
     * run is cancelled, terminated or closed. Pass it to `fetch` and the like.
     */
    get signal(): AbortSignal;
    /** Reports progress; resets the heartbeat timeout. Coalesced; never waits for the disk. */
    heartbeat<T>(progress?: T): void;
    /** The latest progress this step reported, also from an attempt before a crash. */
    progress<T>(): T | undefined;
    /** Opens this step's stream `name` (see `ctx.stream`). */
    stream(name: string): StreamWriter;
}
/**
 * Names who asks for the signals, cancellations, terminations and resets
 * made inside `fn`: their journal records carry it as `by` (audit). The
 * HTTP API names its token or the proxy's signed-in user the same way.
 *
 * ```ts
 * await asRequester("job:nightly", () => engine.run("order-1").cancel("expired"));
 * ```
 */
export declare function asRequester<T>(by: string, fn: () => T): T;

// objects
/** A key called from workflow code (`ctx.object(type, key)`). */
export declare class ObjectCall {
    private readonly x;
    readonly type: string;
    readonly key: string;
    /** Calls a handler and waits for its result: a journal operation, run exactly once. */
    call<R = unknown, A = unknown>(handler: string, arg?: A): Promise<R>;
    /** Calls a handler without waiting: the outbox delivers the call once this point is durable. */
    send<A = unknown>(handler: string, arg?: A): void;
}
/** The context of an object's handler: the workflow context of the key's run, its state and its key. */
export declare class ObjectCtx extends Ctx {
    /** Whether this handler is exclusive (it may change state). */
    readonly exclusive: boolean;
    /** The key's state. */
    readonly state: ObjectState;
    /** The object's key. */
    get objectKey(): string;
}
/** A key of an object type, called from outside workflow code. */
export declare class ObjectRef {
    private readonly engine;
    readonly type: string;
    readonly key: string;
    /** The key's workflow ID (`<type>/<key>`). */
    get id(): string;
    /** Calls a handler and waits for its durable result. */
    call<R = unknown, A = unknown>(handler: string, arg?: A, opts?: CallOptions): Promise<R>;
    /** Calls a handler without waiting for its result: resolves once the call is accepted. */
    send<A = unknown>(handler: string, arg?: A, opts?: CallOptions): Promise<void>;
    /**
     * Edits the state from outside its handlers: an exclusive call, so it
     * waits for the key and commits as a handler's writes do.
     */
    editState(edit: StateEdit, opts?: EditOptions): Promise<EditResult>;
    /** Sets one field of the state. */
    setState(field: string, value: unknown, opts?: EditOptions): Promise<EditResult>;
    /** Applies a JSON Patch (RFC 6902) to the state as one JSON object. */
    patchState(ops: JsonPatchOp[], opts?: EditOptions): Promise<EditResult>;
    /** Clears one field, or (none named) the whole state; the key remains. */
    clearState(field?: string, opts?: EditOptions): Promise<EditResult>;
    /** The committed state (`{}` for a key never called), once it is durable. */
    state<T = Record<string, unknown>>(): Promise<T>;
    /** The key's run: its journal, and `object` with its state and pending calls. */
    inspect(): any;
    /** Deletes the key: its run ends and its state with it; the next call starts empty. */
    delete(): Promise<void>;
}
/** A key's state as a handler sees it: JSON values under string keys. */
export declare class ObjectState {
    private readonly x;
    private readonly exclusive;
    /** The value, or `undefined` when the key has none. An exclusive handler sees its own writes. */
    get<T = unknown>(key: string): T | undefined;
    /** Sets a value (`null` or `undefined` clears it); committed when the handler returns. */
    set(key: string, value: unknown): void;
    clear(key: string): void;
    /** Clears every key. */
    clearAll(): void;
    /** The keys that hold a value, sorted. */
    keys(): string[];
    private writable;
}
/** An object type, to address its keys. */
export declare class ObjectType {
    private readonly engine;
    readonly name: string;
    /** The object with key `key`. */
    key(key: string): ObjectRef;
}
/**
 * A reader of a run's stream: yields each chunk once it is durable, in
 * order, and ends once the stream is complete. `offset` is where a reader
 * that reconnects continues; `status` says how the stream ended.
 */
export declare class StreamReader<T = unknown> implements AsyncIterable<T> {
    private readonly read;
    /** The offset of the next chunk. */
    offset: number;
    /** The run being read, once the first read found it (pass it back to keep reading it). */
    run: number | undefined;
    /** How the stream ended: `ok`, a failed step's `failed`, or the run's status if it ended first. */
    status: string | undefined;
    private readonly waitMs;
    [Symbol.asyncIterator](): AsyncIterator<T>;
}
/** A writer of one of the run's streams (`ctx.stream`). */
export declare class StreamWriter {
    private readonly x;
    readonly name: string;
    private readonly step;
    /** Chunks the step's earlier attempts recorded (0 on a first attempt and in workflow code). */
    readonly recordedCount: number;
    /** The stream was complete when it was opened (an earlier attempt of the step completed it: appends are dropped). */
    readonly closed: boolean;
    private readonly seq;
    /** Appends a chunk: a `Uint8Array` as bytes, anything else as JSON. Never waits for the disk. */
    append(value: unknown): void;
    /** Completes the stream: readers end once they have every chunk. */
    close(): void;
    /**
     * The chunks the step's earlier attempts recorded (after a crash, the
     * durable ones): continue after them, so readers see no chunk twice.
     */
    recorded<T = unknown>(): T[];
}
/**
 * Marks a handler as shared: it runs as soon as its call arrives, next to
 * other calls (an exclusive one included), and only reads state.
 */
export declare function shared<A, R>(handle: ObjectHandler<A, R>): SharedHandler<A, R>;
/** Options of a call from outside workflow code. */
export interface CallOptions {
    /** An idempotency ID: calls with the same ID run once, and each gets the recorded result. */
    id?: string;
}
/** Options of a state edit. */
export interface EditOptions extends CallOptions {
    /** Refuse the edit unless the state is still at this version (`inspect().object.version`). */
    ifVersion?: string;
}
/** What a state edit answers: the state's new version and the fields it changed. */
export interface EditResult {
    version: string;
    changed: string[];
}
/** One operation of a JSON Patch (RFC 6902). */
export type JsonPatchOp = {
    op: "add" | "replace" | "test";
    path: string;
    value: unknown;
} | {
    op: "remove";
    path: string;
} | {
    op: "move" | "copy";
    from: string;
    path: string;
};
/** An object's handler: workflow code with the key's state. */
export type ObjectHandler<A = any, R = any> = (ctx: ObjectCtx, arg: A) => R | Promise<R>;
/** An object type's handlers by name: exclusive unless marked with `shared`. */
export type ObjectHandlers = Record<string, ObjectHandler | SharedHandler>;
/** A handler marked with `shared`. */
export interface SharedHandler<A = any, R = any> {
    readonly shared: true;
    readonly handle: ObjectHandler<A, R>;
}
/**
 * An edit of a key's state from outside its handlers, applied in this
 * order: `clearAll`, `clear`, `set` (a `null` value removes the field),
 * `patch` (a JSON Patch over the state as one object).
 */
export interface StateEdit {
    clearAll?: boolean;
    clear?: string[];
    set?: Record<string, unknown>;
    patch?: JsonPatchOp[];
}
export interface StreamOptions {
    /** The offset to start at (default 0): where a reader that reconnects continues. */
    from?: number;
    /** How long one read waits for a chunk before it asks again (default 30 s). */
    wait?: Duration;
}

// definitions
export declare function query<A = unknown, R = unknown>(name: string): QueryDefinition<A, R>;
export declare function signal<T = unknown>(name: string): SignalDefinition<T>;
export declare function update<A = unknown, R = unknown>(name: string): UpdateDefinition<A, R>;
/**
 * Defines a workflow type:
 *
 *     export const checkout = workflow("checkout", async (ctx, order: Order) => { ... });
 *     export const nightly = workflow("nightly", { version: 2, timeout: "1h" }, async (ctx) => { ... });
 */
export declare function workflow<I = unknown, O = unknown>(name: string, fn: WorkflowFn<I, O>): WorkflowDefinition<I, O>;
/**
 * Runs at once: at most `limit` runs of the type run at a time, or of each
 * key (`key(input)`, a tenant say); the others wait, in the order they were
 * started, as `fusee.queued/<id>` runs, and `result()` waits through the
 * queue. The turns are durable: a run keeps its turn while it sleeps and
 * across crashes. Starts from outside a workflow (an engine, a client, the
 * HTTP API) take turns; a child started by a workflow does not.
 */
export interface Concurrency<I = unknown> {
    limit: number;
    key?: (input: I) => string;
}
/** A query's name, argument and result types. */
export interface QueryDefinition<A = unknown, R = unknown> {
    readonly kind: "query";
    readonly name: string;
}
/** A signal's name and payload type. A plain string works wherever a definition does. */
export interface SignalDefinition<T = unknown> {
    readonly kind: "signal";
    readonly name: string;
}
/** An update's name, argument and result types. */
export interface UpdateDefinition<A = unknown, R = unknown> {
    readonly kind: "update";
    readonly name: string;
}
/**
 * A workflow type: its name and code, a plain value apart from any engine.
 * Pass it to `Engine.open({ workflows })`, `engine.start`, `ctx.child`; a
 * client imports it to start runs with typed input and output.
 */
export interface WorkflowDefinition<I = unknown, O = unknown> {
    readonly kind: "workflow";
    readonly name: string;
    readonly version: number;
    /** The run's deadline, unless a start gives one. */
    readonly timeout?: Duration;
    /** At most `limit` runs at once (per `key(input)`); the others wait their turn. */
    readonly concurrency?: Concurrency<I>;
    /** The code (absent in a definition that only names a type another process runs). */
    readonly fn?: WorkflowFn<I, O>;
}

// runs
export declare class Run<O> {
    private readonly engine;
    readonly id: string;
    constructor(engine: Engine, id: string);
    /** Sends a signal (a definition or its name); resolves once it is durable. */
    signal<T>(sig: SignalDefinition<T> | string, value: T): Promise<void>;
    /** Waits for the run's result (following continue-as-new and reset). */
    result(): Promise<O>;
    /**
     * Waits until a run with this ID exists. A debounced or batched run
     * starts only when its window closes; until then `result` fails with
     * NotFound.
     */
    started(): Promise<void>;
    /** Sends an update and waits for its handler's durable result. */
    update<A, R>(upd: UpdateDefinition<A, R> | string, arg: A, opts?: UpdateOptions): Promise<R>;
    /** Calls a query handler; the answer waits until the state it read is durable. */
    query<R, A = unknown>(q: QueryDefinition<A, R> | string, arg?: A): Promise<R>;
    cancel(reason?: string): Promise<void>;
    /** Ends the run at once; children follow their parent close policy. */
    terminate(reason?: string): Promise<void>;
    /**
     * Resumes a run that paused because a step ran out of retries
     * (`retry: { onExhausted: "pause" }`): the step runs again at once, with
     * a fresh set of attempts, on the code this process has now. Rejected if
     * nothing is paused.
     */
    resume(): Promise<void>;
    /** Starts a new run whose journal is this one's cut just before `beforeStep`. */
    reset(opts: {
        beforeStep: string;
        reapplySignals?: boolean;
        version?: number;
    }): Promise<void>;
    /** The run's summary: status, type, version, times, error. */
    status(): Promise<RunSummary>;
    /**
     * The run and its journal (each record with its `kind`), and for a key of
     * an object type its state. Throws NotFoundError for an unknown ID.
     */
    describe(): Promise<RunDetail>;
    /** Runs a failed run again from its failed step (a reset before it, with its signals). */
    retry(): Promise<void>;
    /** @deprecated Use `describe()`. */
    inspect(): any;
    /** A reader of the run's stream `name` (`for await (const chunk of run.stream("tokens"))`). */
    stream<T = unknown>(name: string, opts?: StreamOptions): StreamReader<T>;
}
/** A schedule, addressed by name. */
export declare class Schedule {
    private readonly engine;
    readonly name: string;
    constructor(engine: Engine, name: string);
    get id(): string;
    private send;
    pause(): Promise<void>;
    resume(): Promise<void>;
    /** Starts one run now. */
    trigger(): Promise<void>;
    /** Starts a run for every scheduled time in [from, to]. */
    backfill(from: Date | number, to: Date | number): Promise<void>;
    /** The schedule's state: spec, next time, running run, counters. */
    describe(): Promise<any>;
    /** Stops the schedule; runs it started keep running. */
    delete(): Promise<void>;
}
export declare class Workflow<I, O> {
    private readonly engine;
    readonly name: string;
    readonly version: number;
    constructor(engine: Engine, name: string, version: number);
    /** Starts a run. Resolves once the start is durable. */
    start(id: string, input: I, opts?: StartOptions): Promise<Run<O>>;
    /**
     * Starts the workflow if it is not running and delivers an update in the
     * same record; if it is running, delivers the update to it. Returns the
     * update's result once it is durable.
     */
    updateWithStart<A, R>(id: string, input: I, update: string, arg: A, opts?: StartOptions & UpdateOptions): Promise<{
        run: Run<O>;
        result: R;
    }>;
    /**
     * Debounces a start under `key`: starts inside the window collapse into
     * one run, which begins with the latest start's input when the window
     * closes (`window` after the latest start, or `maxWait` after the first).
     * Resolves once this start is durable, with the handle of that run; the
     * run exists once the window closes (`run.started()`).
     */
    debounce(key: string, input: I, opts: DebounceOptions): Promise<Run<O>>;
    /**
     * Adds an event to the batch under `key`: once the batch holds
     * `maxEvents` events or `maxWait` has passed since its first, one run
     * starts with all of them, in the order they were accepted, as its input
     * (an array). Resolves once the event is durable, with the handle of that
     * run.
     */
    batch(key: string, event: I extends readonly (infer E)[] ? E : unknown, opts?: BatchOptions): Promise<Run<O>>;
}
/** One journal record: its `kind` (start, step, attempt, timer, signal, wait, child, done, ...) and fields. */
export interface JournalRecord {
    kind: string;
    [field: string]: unknown;
}
/** A run and its journal (`run.describe()`). */
export interface RunDetail extends RunSummary {
    journal: JournalRecord[];
    /** For a key of an object type: its state and pending calls. */
    objectDetail?: unknown;
    /**
     * From a server or the HTTP API, under the dev profile: when each step
     * began (Unix ms) and how long it took (ms), by sequence number.
     */
    stepTimes?: Record<string, [number, number]>;
}
/** A run as lists and `run.status()` show it. */
export interface RunSummary {
    id: string;
    type: string;
    version: number;
    /** running, suspended, parked, paused, stuck, waiting_for_code, ok, failed, cancelled, timed_out, terminated. */
    status: string;
    startedMs: number;
    finishedMs: number | null;
    /** The failure (its envelope as JSON) or the stuck reason. */
    error: string | null;
    run: number;
    shard: number;
    attributes?: Record<string, unknown>;
    object?: boolean;
}

// remote
/** Sends calls to the worker holding a Fusee server's shards. */
export declare class Client {
    private readonly transport;
    private constructor();
    static connect(address: string): Promise<Client>;
    call<T = unknown>(method: string, args: object): Promise<T>;
    /** As `Engine.start`: a definition or a type name; `delay`/`at` start it later. */
    start<I, O = unknown>(wf: WorkflowDefinition<I, O> | string, id: string, input: I, opts?: StartOptions & {
        version?: number;
        delay?: Duration;
        at?: Date | number;
    }): Promise<RemoteRun<O>>;
    /** As `Engine.signalWithStart`. */
    signalWithStart<I, O, T>(wf: WorkflowDefinition<I, O> | string, id: string, input: I, sig: SignalDefinition<T> | string, value: T, opts?: StartOptions & {
        version?: number;
    }): Promise<RemoteRun<O>>;
    /** As `Engine.updateWithStart`: the run and the update's result. */
    updateWithStart<I, O, A, R>(wf: WorkflowDefinition<I, O> | string, id: string, input: I, upd: UpdateDefinition<A, R> | string, arg: A, opts?: StartOptions & UpdateOptions & {
        version?: number;
    }): Promise<{
        run: RemoteRun<O>;
        result: R;
    }>;
    /** Debounces a start of workflow type `type` under `key` (see `Workflow.debounce`). */
    debounce<I, O = unknown>(wf: WorkflowDefinition<I, O> | string, key: string, input: I, opts: DebounceOptions): Promise<RemoteRun<O>>;
    /** Adds an event to the batch of workflow type `type` under `key` (see `Workflow.batch`). */
    batch<E, O = unknown>(wf: WorkflowDefinition<any, O> | string, key: string, event: E, opts?: BatchOptions): Promise<RemoteRun<O>>;
    private enqueue;
    run<O = unknown>(id: string): RemoteRun<O>;
    /** Key `key` of object type `type`, called through the server. */
    objectRef(type: string, key: string): RemoteObjectRef;
    list(where?: Record<string, unknown>, limit?: number): Promise<Record<string, unknown>[]>;
    complete(token: string, value?: unknown): Promise<void>;
    fail(token: string, error: string): Promise<void>;
    close(): void;
}
/** A key of an object type, called through a server (see `ObjectRef`). */
export declare class RemoteObjectRef {
    private readonly client;
    readonly type: string;
    readonly key: string;
    constructor(client: Client, type: string, key: string);
    get id(): string;
    call<R = unknown, A = unknown>(handler: string, arg?: A, opts?: CallOptions): Promise<R>;
    send<A = unknown>(handler: string, arg?: A, opts?: CallOptions): Promise<void>;
    editState(edit: StateEdit, opts?: EditOptions): Promise<EditResult>;
    setState(field: string, value: unknown, opts?: EditOptions): Promise<EditResult>;
    patchState(ops: JsonPatchOp[], opts?: EditOptions): Promise<EditResult>;
    clearState(field?: string, opts?: EditOptions): Promise<EditResult>;
    state<T = Record<string, unknown>>(): Promise<T>;
}
export declare class RemoteRun<O = unknown> {
    private readonly client;
    readonly id: string;
    constructor(client: Client, id: string);
    result(): Promise<O>;
    /** Waits until a run with this ID exists (a debounced or batched run starts when its window closes). */
    started(): Promise<void>;
    signal<T>(sig: SignalDefinition<T> | string, value: T): Promise<void>;
    query<R, A = unknown>(q: QueryDefinition<A, R> | string, arg?: A): Promise<R>;
    update<A, R>(upd: UpdateDefinition<A, R> | string, arg: A, opts?: UpdateOptions): Promise<R>;
    cancel(reason?: string): Promise<void>;
    terminate(reason?: string): Promise<void>;
    /** Resumes a paused run: its step runs again with a fresh set of attempts. */
    resume(): Promise<void>;
    /** As `Run.reset`: `{ beforeStep, reapplySignals }` (a step name alone works too). */
    reset(opts: {
        beforeStep: string;
        reapplySignals?: boolean;
    } | string, reapplySignals?: boolean): Promise<void>;
    /** The run and its journal (as `Run.describe`). */
    describe(): Promise<RunDetail>;
    /** The run's summary (as `Run.status`). */
    status(): Promise<RunSummary>;
    /** Runs a failed run again from its failed step. */
    retry(): Promise<void>;
    /** A reader of the run's stream `name`, answered by the worker holding its shard. */
    stream<T = unknown>(name: string, opts?: StreamOptions): StreamReader<T>;
}
/** Cumulative counters since this shard opened. Durations are microseconds. */
export interface ShardMetrics {
    records: number;
    log_bytes: number;
    flushes: number;
    inline_flushes: number;
    flushed_records: number;
    flush_us_total: number;
    steps_run: number;
    steps_replayed: number;
    runs_started: number;
    runs_finished: number;
    runs_parked: number;
    parked_entries: number;
    runs_loaded: number;
    checkpoints: number;
    /** Run index bases written a frame at a time between checkpoints. */
    index_bases_built: number;
    determinism_errors: number;
    busy: number;
    gate_waits: number;
    gate_wait_us_total: number;
}

// engine
export declare class Engine extends CodeHost {
    private readonly tokens;
    private readonly waiters;
    private readonly be;
    private nextToken;
    private scheduledTick?;
    /** Holds the process open while a native kick is pending (see `kick`). */
    protected readonly alive: NodeJS.Timeout;
    /** A native driver runs the turns (see `OpenOptions.driver`). */
    private readonly driving;
    private pumps;
    /** An emptied set of `pumps` kept for the next turn (`takePumps`). */
    private sparePumps;
    private timer;
    private timerAt;
    protected closed: boolean;
    private turnBegan;
    private inlineThisTurn;
    /** Turns only for deliveries since the last full one (see `deliveryTurn`). */
    private deliveryTurns;
    /** Something other than a delivery asked for the next tick (see `kick`). */
    private fullTick;
    protected backend(): Backend;
    private serveRemoteCalls;
    /**
     * The URL this engine's HTTP API is reached at from outside (behind a
     * proxy): `awaitCompletion`'s `url` is built from it.
     */
    setCallbackUrl(url: string): void;
    /** One process, no server and no worker threads: a type no code is registered for cannot run anywhere. */
    private readonly embedded;
    private readonly workerTimer?;
    /** In a worker thread: the link to the thread that routes calls (see `Workers`). */
    private parent;
    static open(opts?: OpenOptions): Promise<Engine>;
    private static openWith;
    /**
     * Registers a workflow type (version 1 unless given). Without a function,
     * returns a handle to start runs of a type registered elsewhere (with
     * `workers`, in the workers' module).
     */
    workflow<I, O>(name: string, fn?: WorkflowFn<I, O>, opts?: {
        version?: number;
    }): Workflow<I, O>;
    run<O>(id: string): Run<O>;
    /**
     * Registers an object type: its handlers by name, exclusive unless marked
     * with `shared`. Without handlers, returns a handle to call a type
     * registered elsewhere (with `workers`, in the workers' module).
     */
    object(name: string, handlers?: ObjectHandlers, opts?: {
        version?: number;
    }): ObjectType;
    /** Key `key` of object type `type`. */
    objectRef(type: string, key: string): ObjectRef;
    /** Keys of object type `type` (up to `limit` per shard). */
    objects(type: string, limit?: number): string[];
    /** Runs, optionally filtered: `{ where: { status: "failed", amount: { gte: 100 } } }`. */
    /** Current counters for each shard, in shard-number order. */
    shardMetrics(): ShardMetrics[];
    list(opts?: number | {
        where?: Record<string, unknown>;
        limit?: number;
    }): any[];
    /** Defines (or resizes) a concurrency and rate limit for steps. */
    limit(name: string, spec: {
        concurrent?: number;
        perSecond?: number;
    }): void;
    /** Every limit this engine defined, by name, with its permits now. */
    limits(): {
        name: string;
        concurrent: number;
        perSecond: number;
        inUse: number;
        waiting: number;
    }[];
    /**
     * Serves the HTTP API and the web UI on a thread of its own (they call
     * this engine through its event loop; nothing rides the step path). Off
     * loopback a `token` is required. Returns the URL:
     *
     *     console.log(engine.serve());   // http://127.0.0.1:7601
     */
    serve(opts?: {
        addr?: string;
        token?: string;
        insecure?: boolean;
    }): string;
    /** The engine's counters summed over its shards. */
    metrics(): Record<string, number>;
    /** Registers a workflow definition's code (as `Engine.open({ workflows })` does). */
    register<I, O>(def: WorkflowDefinition<I, O>): WorkflowDefinition<I, O>;
    /**
     * Starts a run of `wf` (a definition or a type name) under workflow ID
     * `id`; resolves once the start is durable. `delay` or `at` starts it
     * later: a durable timer (the run `fusee.delay/<id>`), then the start;
     * `result()` waits through the delay.
     */
    start<I, O>(wf: WorkflowDefinition<I, O> | string, id: string, input: I, opts?: StartOptions & {
        delay?: Duration;
        at?: Date | number;
    }): Promise<Run<O>>;
    /**
     * Signals the run `id`, starting it first with `input` if it is not
     * running: both reach the engine before its next tick, so the signal is
     * recorded before a new run's code first runs.
     */
    signalWithStart<I, O, T>(wf: WorkflowDefinition<I, O> | string, id: string, input: I, sig: SignalDefinition<T> | string, value: T, opts?: StartOptions): Promise<Run<O>>;
    /** Starts `wf` if it is not running and delivers an update in the same record; returns the run and the update's result. */
    updateWithStart<I, O, A, R>(wf: WorkflowDefinition<I, O> | string, id: string, input: I, upd: UpdateDefinition<A, R> | string, arg: A, opts?: StartOptions & UpdateOptions): Promise<{
        run: Run<O>;
        result: R;
    }>;
    /** Debounces a start of `wf` under `key` (see `DebounceOptions`). */
    debounce<I, O>(wf: WorkflowDefinition<I, O> | string, key: string, input: I, opts: DebounceOptions): Promise<Run<O>>;
    /** Adds an event to the batch of `wf` under `key` (see `BatchOptions`). */
    batch<I, O>(wf: WorkflowDefinition<I, O> | string, key: string, event: unknown, opts?: BatchOptions): Promise<Run<O>>;
    /** Permits of a limit held right now. */
    limitInUse(name: string): number;
    /** This process's steps waiting for a permit of a limit right now. */
    limitWaiting(name: string): number;
    /** Finishes an async completion from its token; resolves once recorded durably. */
    complete<T>(token: string, value: T): Promise<void>;
    /** Fails an async completion. */
    fail(token: string, error: string): Promise<void>;
    /** Creates a schedule (or updates its spec) that starts `workflow` on its cron or interval. */
    schedule<I>(name: string, opts: ScheduleOptions, workflow: string | Workflow<I, unknown>, input?: I): Promise<Schedule>;
    /** A handle to an existing schedule. */
    scheduleHandle(name: string): Schedule;
    /** The schedules: `get(name)`, and `list()` of every schedule that exists. */
    get schedules(): {
        get(name: string): Schedule;
        list(): Schedule[];
    };
    /** A run's journal as an export file (`.fjnl`) for `Replayer`, or null. */
    exportJournal(id: string): Buffer | null;
    /** Flushes and closes. */
    close(): Promise<void>;
    /** Throws unless this thread holds `id` (for calls that answer synchronously). */
    private here;
    private handleRemote;
    /**
     * A turn of the native driver asked for only to make deliveries: each due
     * run takes its next one, and nothing else runs (no engine tick, events
     * or timers). The driver runs each turn in its own callback scope, so the
     * microtasks a delivery starts all run before the next turn's: a fan-out
     * delivers branch after branch within one libuv callback at a fraction
     * of a full turn's cost. A full turn follows the burst,
     * or every `DELIVERY_TURNS`, so the records the deliveries appended and
     * any background wake are seen.
     */
    private deliveryTurn;
    /** The runs due for a delivery, leaving an empty set for the next ones. */
    private takePumps;
    /** Keeps a set `takePumps` returned, once it is gone through. */
    private givePumps;
    private tick;
    /** One event: `ev[o]` is its kind (see `Ev`), its strings and payloads are in `extra`. */
    private handle;
}

// replay
/**
 * Runs workflow code against journals exported with `fusee export`, without
 * running step functions or timers, and reports the first difference.
 */
export declare class Replayer extends CodeHost {
    private readonly opts;
    private current;
    constructor(opts?: {
        codec?: Codec;
        workflows?: readonly WorkflowDefinition<any, any>[];
    });
    protected backend(): Backend;
    workflow<I, O>(name: string, fn: WorkflowFn<I, O>, opts?: {
        version?: number;
    }): this;
    replay(file: Buffer): Promise<ReplayReport>;
    replayFile(path: string): Promise<ReplayReport>;
    /** Replays every `.fjnl` file in a directory. */
    replayDir(dir: string): Promise<ReplayReport[]>;
}
export interface ReplayReport {
    file?: string;
    workflowId: string;
    workflowType: string;
    /** The code made the same journal operations, in the same order. */
    ok: boolean;
    /** The first difference, naming the step. */
    error?: string;
    /** The code reached its end within the journal. */
    finished: boolean;
}

// invoke
/**
 * The worker side of invocations over HTTP (suspend to zero): when a Fusee
 * server started with `--invoke URL` has work for shards no worker holds, it
 * POSTs to that URL; each request opens a fresh engine for the invocation's
 * shards, runs them until they are idle or the budget ends, and closes it.
 * Mount `node` on `node:http` or Express (before any body parser, or after
 * `express.raw()`), export `lambda` as an AWS Lambda handler, or use `fetch`
 * with the Fetch API (Bun, Deno, Vercel, Cloudflare).
 */
export declare class InvocationHandler {
    private readonly opts;
    constructor(opts: InvocationHandlerOptions);
    /** Verifies, runs the invocation, and answers: 200 with a summary, 401, 400 or 503. */
    invoke(req: {
        headers?: Headers;
        body: string | Uint8Array;
    }): Promise<InvocationReply>;
    /** For `node:http`, Express and other `(req, res)` frameworks. */
    get node(): (req: any, res: any) => void;
    /** An AWS Lambda handler (function URL, API Gateway or ALB proxy events). */
    get lambda(): (event: {
        headers?: Record<string, string | undefined>;
        body?: string | null;
        isBase64Encoded?: boolean;
    }) => Promise<InvocationReply>;
    /** For the Fetch API: `(request: Request) => Promise<Response>`. */
    get fetch(): (request: Request) => Promise<Response>;
}
/** The header a Fusee server signs invocations in. */
export declare const SIGNATURE_HEADER = "fusee-signature";
/** See {@link InvocationHandler}. */
export declare function invocationHandler(opts: InvocationHandlerOptions): InvocationHandler;
/** Checks a `fusee-signature` value (`t=<unix ms>,v1=<hex HMAC-SHA256>`) within five minutes. */
export declare function verifySignature(secret: string | Uint8Array, header: string, body: Uint8Array, nowMs?: number): string | null;
export interface InvocationHandlerOptions extends Omit<OpenOptions, "durability" | "address" | "workers" | "module"> {
    /** The server, with this worker's connection options: `host:port?ca=...&token-file=...`. */
    address: string;
    /**
     * The endpoint's signing secret (the server's `--invoke-secret-file`).
     * Leave it out only behind mutual TLS or on a network only the server reaches.
     */
    secret?: string | Uint8Array;
    /** Registers the workflows on each invocation's fresh engine. */
    register: (engine: Engine) => void | Promise<void>;
}
/** An HTTP answer: a status and a JSON body. */
export interface InvocationReply {
    statusCode: number;
    headers: Record<string, string>;
    body: string;
}

// telemetry (referenced)
type Attrs = Record<string, string | number | boolean>;

// host (referenced)
/** Runs workflow code: shared by the engine and the replayer. */
export declare abstract class CodeHost {
    /** Failures record their stack trace (OpenOptions.recordStacks). */
    protected recordStacks: boolean;
    /** What a step does when its retries run out (OpenOptions.onExhausted). */
    protected onExhausted: "fail" | "pause";
    protected readonly types: Map<string, Map<number, Registered>>;
    protected readonly runs: RunTable<RunExec>;
    protected readonly awaiting: Map<string, StartEvent[]>;
    protected abstract backend(): Backend;
    protected onStart(ev: StartEvent): void;
    protected onEnded(shard: number, run: number, stuck?: string): void;
    protected instantiate(ev: StartEvent): boolean;
}

// execution (referenced)
/** Where a run's journal operations go: the engine or a replayer. */
export interface Backend {
    /** Outputs of `beginStep`: `[seq, replay, gate, attempt, notBefore]`. */
    readonly out: Float64Array;
    /** 2 replay, 1 run now (the gate is open), 0 wait. */
    beginStep(shard: number, run: number, name: string, readOnly: boolean): number;
    stepGate(shard: number, run: number, gate: number, notBefore: number): boolean;
    /** Whether a gate is open by what the last tick reported, without a call. */
    gateOpen?(shard: number, gate: number, notBefore: number): boolean;
    finishStep(shard: number, run: number, seq: number, ok: boolean, value: string | Uint8Array): void;
    /** `finishStep`, then `quiescent`: one native call where a run takes its delivery at once. */
    finishStepDeliver?(shard: number, run: number, seq: number, ok: boolean, value: string): Delivery | null;
    call(shard: number, op: string, run: number, args: (string | number)[], bufs?: Buffer[]): number[];
    bytes(shard: number, op: string, run: number, args: (string | number)[], bufs?: Buffer[]): Buffer | null;
    /** `ctx.now()` as a number, without the buffers of `bytes("value")` (absent: that path). */
    valueNow?(shard: number, run: number, liveMs: number): number;
    /** `ctx.sleep`'s sequence number, without `call("beginSleep")`'s strings (absent: that path). */
    beginSleep?(shard: number, run: number, name: string, atMs: number): number;
    scheduleRun?(execution: RunExec): void;
    /** The engine's tick count (see `deliverNow`); absent on replay. */
    turn?(): number;
    /** Whether a run whose code is blocked may take a delivery now (the fairness budget). */
    deliverNow?(): boolean;
    quiescent(shard: number, run: number): Delivery | null;
    decideUpdate(shard: number, token: number, ok: boolean, message: string): number;
    finish(shard: number, run: number, status: number, value: string | Uint8Array): void;
    stuck(shard: number, run: number, message: string): void;
    /** `full` false: only a delivery needs the next turn (see `Engine.kick`). */
    kick(full?: boolean): void;
}

// invoke (referenced)
type Headers = Record<string, string | string[] | undefined> | {
    get(name: string): string | null;
};

// host (referenced)
type Registered = {
    fn: WorkflowFn<unknown, unknown>;
    version: number;
};
/** Values keyed by (shard, run), without building a string key. */
export declare class RunTable<T> {
    private readonly shards;
    size: number;
    get(shard: number, run: number): T | undefined;
    has(shard: number, run: number): boolean;
    set(shard: number, run: number, v: T): void;
    delete(shard: number, run: number): boolean;
    values(): IterableIterator<T>;
    clear(): void;
}

// execution (referenced)
declare class RunExec {
    readonly be: Backend;
    readonly shard: number;
    readonly run: number;
    readonly id: string;
    readonly wtype: string;
    readonly version: number;
    readonly startedMs: number;
    readonly pending: Map<number, Pending>;
    gated: Map<number, () => void>;
    beats: Map<number, number>;
    queries: Map<string, (arg: unknown) => unknown>;
    updates: Map<string, UpdateHandler<unknown, unknown>>;
    /** JSON of step results this code recorded and not yet had delivered. */
    held: Map<number, string>;
    current: number | undefined;
    pumping: boolean;
    ended: boolean;
    /** The tick in which something last resumed this run's code (see `deliverNow`). */
    resumedAt: number;
    readonly ctx: Ctx;
    /** The code waits with nothing pending by design (an object, continue-as-new). */
    idle: boolean;
    /** The run's `fusee.workflow` span and the context it is active in (telemetry). */
    wfSpan: SpanLike | undefined;
    wfCtx: unknown;
    constructor(be: Backend, shard: number, run: number, id: string, wtype: string, version: number, startedMs: number, ctxClass?: new (x: RunExec) => Ctx);
    /** Failures record their stack (OpenOptions.recordStacks). */
    recordStacks: boolean;
    /** What a step does when its retries run out, unless its policy says. */
    onExhausted: "fail" | "pause";
    /** Where the engine's HTTP API is reached (completion URLs), if it serves one. */
    callbackBase: (() => string | undefined) | undefined;
    /** Each running step's abort controller, made when its `s.signal` is read. */
    aborts: Map<number, AbortController> | undefined;
    /** Aborts the signals of running steps (the run was cancelled, ended or closed). */
    abortSteps(reason: Error): void;
    /** Compensations, newest last (ctx.compensate); made on first use. */
    undo: {
        name: string;
        fn: () => unknown;
        opts: StepOptions;
    }[] | undefined;
    h(op: string, args?: (string | number)[], bufs?: Buffer[]): number[];
    hb(op: string, args?: (string | number)[], bufs?: Buffer[]): Buffer | null;
    /**
     * Something changed: re-check gates, then deliver. A wake arrives in a
     * tick, its own macrotask, so unless something resumed this run's code in
     * this tick already, the code is blocked and can take a delivery at once
     * (see `deliverSoon`): resuming after a signal costs no extra turn.
     */
    wake(): void;
    schedulePump(): void;
    pumpScheduled(): void;
    /** Workflow code may run from here on (a start, a delivery, a rejection). */
    resumed(): void;
    /**
     * Deliver at completion. Replay hands a run its recorded results one at
     * a time, and between two deliveries the run's code runs until it is
     * blocked: every microtask the previous delivery started has run first.
     * Live delivery matches that by waiting a macrotask before each delivery
     * (`schedulePump`), because JavaScript cannot see whether its microtask
     * queue has drained.
     *
     * The wait is not needed when nothing has resumed the run's code since
     * the engine's current tick began (`resumedAt` is from an earlier tick).
     * Each tick runs as its own macrotask (on Bun, native wakes defer to one,
     * see the constructor), so whatever resumed the run before it (a start,
     * a delivery, a rejection) has finished its microtasks, and the code is
     * blocked on ctx operations. Only the SDK resumes it, and each of those
     * sets `resumedAt`. So a step that settles now (its function ran in this
     * tick, or it was asynchronous) is delivered in the same microtask, and
     * the run sees exactly what a delivery in the next macrotask would show
     * it: `quiescent` picks the same next item and appends the same record.
     * Once that delivery resumes the run, a second completion in the same
     * tick waits for the next macrotask: the continuation may still
     * have microtasks queued, which would run after the second delivery's and
     * not before it as on replay.
     *
     * Fairness: a run takes at most one such delivery per tick, so a chain
     * yields to libuv at every tick. Across runs, the engine stops after a
     * budget per tick (`INLINE_PER_TICK` deliveries or `INLINE_TICK_MS`
     * since the tick began) and the rest take the macrotask path.
     */
    deliverSoon(): void;
    /** Whether a delivery may be made at once (see `deliverSoon`); takes from the tick's budget. */
    mayDeliverNow(): boolean;
    pump(): void;
    /** Makes a delivery `quiescent` (or `finishStepDeliver`) returned. */
    take(d: Delivery | null): void;
    /** The code awaits something outside ctx (see `take`): the run stops, as on a determinism error. */
    awaitedElsewhere(): void;
    /** An update reached its turn: validate a new request, then start its handler. */
    startUpdate(d: Delivery): void;
    resolve(seq: number, status: number, payload: Wire): void;
    fail(e: FuseeError): void;
    op<T>(kind: Pending["kind"], name: string, seq: number): Promise<T>;
    /** The code dropped an operation (the losing side of a race). */
    forget(seq: number): void;
    /**
     * Runs a step's function (or an async completion's send function) under
     * its gate, permit, timeouts and retry policy. `done(true, prepare(v))`
     * records the result; `done(false, msg)` the final failure. When
     * `prepare` throws (a step result JSON cannot encode), the attempt fails
     * like one whose function threw. `open`: what `beginStep` found for the
     * first attempt's gate (true open, false shut and registered for a wake),
     * or undefined when nobody asked yet.
     */
    runAttempts(seq: number, gate: number, attempt: number, notBefore: number, fn: (attempt: number) => unknown, opts: StepOptions, 
    /**
     * Records the outcome. `now`: the run takes its next delivery at once;
     * a callback that made it with the record returns it (or null for
     * none), otherwise undefined.
     */
    done: (ok: boolean, v: unknown, now: boolean) => Delivery | null | undefined | void, prepare?: (v: unknown) => unknown, name?: string, open?: boolean): void;
}

// native (referenced)
/** A run's code starts (or resumes, replaying its journal). */
export interface StartEvent {
    shard: number;
    run: number;
    id: string;
    wtype: string;
    version: number;
    startedMs: number;
    input: Wire;
    /** The W3C traceparent the run joins, if its start was traced. */
    trace?: string;
}
/** The next item for a run whose code is blocked. */
export interface Delivery {
    kind: "resolve" | "cancel" | "update";
    seq: number;
    status: number;
    /** For a result: its payload, unless `held` (the run's code recorded it and keeps it). */
    payload: Wire;
    held: boolean;
    seqs?: number[];
    ordinal?: number;
    name?: string;
    id?: string;
    token?: number;
}

// types (referenced)
interface Pending {
    kind: "step" | "sleep" | "wait" | "child" | "await";
    name: string;
    resolve: (v: unknown) => void;
    reject: (e: unknown) => void;
}

// native (referenced)
/** A payload as it crosses from native code: JSON or text as a string, raw bytes as a Buffer. */
export type Wire = string | Buffer | null | undefined;
