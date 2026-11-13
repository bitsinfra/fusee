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

// definitions
/**
 * Defines a workflow type:
 *
 *     export const checkout = workflow("checkout", async (ctx, order: Order) => { ... });
 *     export const nightly = workflow("nightly", { version: 2, timeout: "1h" }, async (ctx) => { ... });
 */
export declare function workflow<I = unknown, O = unknown>(name: string, fn: WorkflowFn<I, O>): WorkflowDefinition<I, O>;
export declare function signal<T = unknown>(name: string): SignalDefinition<T>;
export declare function query<A = unknown, R = unknown>(name: string): QueryDefinition<A, R>;
export declare function update<A = unknown, R = unknown>(name: string): UpdateDefinition<A, R>;

// errors
/** Every error Fusee throws. `code` is the same in every SDK; `docs` links to its page. */
export declare class FuseeError extends Error {
    readonly code: string;
    /** The failure this error carries (a step's, a child's, a workflow's). */
    readonly cause?: Failure;
    readonly retryable: boolean;
    constructor(message: string, code?: string, cause?: Failure);
    get docs(): string;
}
export declare class NotFoundError extends FuseeError {
    readonly id?: string | undefined;
    constructor(message: string, id?: string | undefined);
}
export declare class AlreadyExistsError extends FuseeError {
    readonly id?: string | undefined;
    constructor(message: string, id?: string | undefined);
}
export declare class UnknownWorkflowError extends FuseeError {
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
/** A workflow, update or object handler failed; its failure is `cause`. */
export declare class WorkflowError extends FuseeError {
    readonly workflowId?: string | undefined;
    readonly cause: Failure;
    constructor(cause: Failure, workflowId?: string | undefined);
}
export declare class ChildError extends FuseeError {
    readonly workflowId: string;
    readonly cause: Failure;
    readonly retryable: boolean;
    constructor(workflowId: string, cause: Failure);
}
export declare class CancelledError extends FuseeError {
    readonly reason: string;
    constructor(reason: string);
}
export declare class TimeoutError extends FuseeError {
    readonly retryable = true;
    constructor(message?: string);
}
/** A request was refused and nothing was recorded: an update's validator, a stale token, a bad argument. */
export declare class RejectedError extends FuseeError {
    constructor(message: string);
}

// definitions
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
/** A signal's name and payload type. A plain string works wherever a definition does. */
export interface SignalDefinition<T = unknown> {
    readonly kind: "signal";
    readonly name: string;
}
/** A query's name, argument and result types. */
export interface QueryDefinition<A = unknown, R = unknown> {
    readonly kind: "query";
    readonly name: string;
}
/** An update's name, argument and result types. */
export interface UpdateDefinition<A = unknown, R = unknown> {
    readonly kind: "update";
    readonly name: string;
}

// types
export interface StartOptions {
    /** Select a registered workflow version. Defaults to the newest. */
    version?: number;
    /** The run ends TimedOut after this long. */
    timeout?: Duration;
    idPolicy?: IdPolicy;
}

// errors
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

// types (referenced)
type Duration = number | string;
interface UpdateOptions {
    /** A retried call with the same ID gets the recorded result. */
    id?: string;
}
interface DebounceOptions {
    /** The run starts once this long passes without another start under the key. */
    window: Duration;
    /** ...or this long after the window's first start, whichever is first (default: no bound). */
    maxWait?: Duration;
    /** How long the key remembers event IDs after its last event (default 10m). */
    dedupWindow?: Duration;
    /** The event ID: a retried call with the same ID joins once and gets the same run. */
    id?: string;
}
interface BatchOptions {
    /** The run starts as soon as the batch holds this many events (default 100, at most 10,000). */
    maxEvents?: number;
    /** ...or this long after the batch's first event (default 1s). */
    maxWait?: Duration;
    /** How long the key remembers event IDs after its last event (default 10m). */
    dedupWindow?: Duration;
    /** The event ID: a retried call with the same ID adds the event once and gets the same run. */
    id?: string;
}

// runs (referenced)
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

// objects (referenced)
interface StreamOptions {
    /** The offset to start at (default 0): where a reader that reconnects continues. */
    from?: number;
    /** How long one read waits for a chunk before it asks again (default 30 s). */
    wait?: Duration;
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
/** Options of a call from outside workflow code. */
export interface CallOptions {
    /** An idempotency ID: calls with the same ID run once, and each gets the recorded result. */
    id?: string;
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

// types (referenced)
type WorkflowFn<I, O> = (ctx: Ctx, input: I) => Promise<O>;

// definitions (referenced)
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

// types (referenced)
interface IdPolicy {
    /** When the ID is running: fail (default), return it, or terminate it and start anew. */
    running?: "fail" | "use-existing" | "terminate-existing";
    /** When its last run finished within retention: start anew (default), only if it failed, refuse, or return it. */
    finished?: "allow" | "allow-if-failed" | "refuse" | "use-existing";
}

// runs (referenced)
/** One journal record: its `kind` (start, step, attempt, timer, signal, wait, child, done, ...) and fields. */
export interface JournalRecord {
    kind: string;
    [field: string]: unknown;
}

// execution (referenced)
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

// types (referenced)
interface StepOptions {
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
/** What `ctx.awaitCompletion`'s `send` gets: the token, and the URL that completes it over HTTP when the engine serves its API. */
export interface Completion {
    token: string;
    /** `POST` the value as JSON here (`…/v1/fail/<token>` fails it). */
    url?: string;
}

// objects (referenced)
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

// types (referenced)
interface ChildOptions {
    /** What happens to the child when this workflow closes (default terminate). */
    parentClose?: ParentClose;
    timeout?: Duration;
}

// objects (referenced)
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

// types (referenced)
interface UpdateHandler<A, R> {
    /** Runs first; throw (or return a string) to refuse. A refusal records nothing. */
    validate?: (arg: A) => void | boolean | string;
    /** Runs once per accepted update; may run steps. */
    handle: (arg: A) => R | Promise<R>;
}
interface RetryPolicy {
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
type ParentClose = "terminate" | "request-cancel" | "abandon";
