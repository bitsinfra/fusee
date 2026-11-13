// The engine.
import { resolve as resolvePath } from "node:path";
import { pathToFileURL } from "node:url";
import { type MessagePort } from "node:worker_threads";
import { St, encode, fromWire, json, ms, toWire, useSerializer, wire, wireText } from "./codec.js";
import { Concurrency, QUIET, SignalDefinition, UpdateDefinition, WorkflowDefinition, nameOf, update, workflow } from "./definitions.js";
import { ChildError, ClosedError, FuseeError, NotFoundError, RejectedError, StepError, UnknownWorkflowError, WorkflowError, call, failureOf, failureText, nativeError, nearest } from "./errors.js";
import { AUDITED, Backend, RunExec, delay, requesterStore } from "./execution.js";
import { CodeHost, EngineBackend, RunTable, Waiter } from "./host.js";
import { DELIVERY_TURNS, DRIVER, EV_STRIDE, Ev, IO_DRIVING, IO_EV, IO_EVENTS, IO_IDLE, IO_KICK, IO_LEN, IO_LIGHT, IO_NEXT, IO_OWED, IO_TURN, IS_BUN, KICK_NATIVE, NativeEngine, Wire, bindHot, native, nowMs } from "./native.js";
import { ObjectHandlers, ObjectRef, ObjectType, objectMain } from "./objects.js";
import { ShardMetrics } from "./remote.js";
import { settle } from "./replay.js";
import { Run, Schedule, Workflow } from "./runs.js";
import { Instruments } from "./telemetry.js";
import { BatchOptions, CONTROL, DELAY_TYPE, DebounceOptions, Duration, EVENT, INLINE_PER_TICK, INLINE_TICK_MS, OpenOptions, QUEUED_TYPE, SCHEDULE_TYPE, ScheduleOptions, StartOptions, USE_EXISTING, UpdateOptions, WorkflowFn, idPolicyBits, queueRequest, untilTaken } from "./types.js";
import { Link, Workers, started } from "./workers.js";

export class Engine extends CodeHost {
  /** @internal */ readonly native: NativeEngine;
  /** @internal The scratch array shared with the native engine. */
  readonly io = new Float64Array(IO_LEN);
  private readonly tokens = new Map<
    number,
    { resolve: (v: unknown) => void; reject: (e: unknown) => void; value: boolean }
  >();
  private readonly waiters = new RunTable<Waiter[]>();
  private readonly be: Backend = new EngineBackend(this);
  private nextToken = 1;
  private scheduledTick?: ReturnType<typeof setImmediate> | true;
  /** Holds the process open while a native kick is pending (see `kick`). */
  protected readonly alive = setInterval(() => {}, 2 ** 30).unref();
  /** A native driver runs the turns (see `OpenOptions.driver`). */
  private readonly driving: boolean;
  private pumps = new Set<RunExec>();
  /** An emptied set of `pumps` kept for the next turn (`takePumps`). */
  private sparePumps: Set<RunExec> | undefined;
  private timer: NodeJS.Timeout | undefined;
  private timerAt = Infinity;
  protected closed = false;
  /** @internal Ticks so far; each runs as its own macrotask (see `RunExec.deliverSoon`). */
  turn = 0;
  private turnBegan = 0;
  private inlineThisTurn = 0;
  /** Turns only for deliveries since the last full one (see `deliveryTurn`). */
  private deliveryTurns = 0;
  /** Something other than a delivery asked for the next tick (see `kick`). */
  private fullTick = true;

  /** @internal The fairness budget for deliveries at completion in this tick. */
  deliverNow(): boolean {
    if (this.inlineThisTurn >= INLINE_PER_TICK || performance.now() - this.turnBegan > INLINE_TICK_MS) return false;
    this.inlineThisTurn += 1;
    return true;
  }

  protected backend(): Backend {
    return this.be;
  }

  private serveRemoteCalls: boolean;
  /** @internal Under the dev profile. */
  dev = false;
  /** @internal The URL completions are reached at (`setCallbackUrl`, or what `serve` bound). */
  private callbackUrl: string | undefined;

  /**
   * The URL this engine's HTTP API is reached at from outside (behind a
   * proxy): `awaitCompletion`'s `url` is built from it.
   */
  setCallbackUrl(url: string): void {
    this.callbackUrl = url.replace(/\/+$/, "");
    this.callbackBase ??= () => this.callbackUrl;
  }
  /** One process, no server and no worker threads: a type no code is registered for cannot run anywhere. */
  private readonly embedded: boolean;
  private readonly workerTimer?: ReturnType<typeof setInterval>;
  /** In a worker thread: the link to the thread that routes calls (see `Workers`). */
  private parent: Link | undefined;

  /** @internal `part`: open one worker's shards of a group; `routed`: open nothing (see `Workers`). */
  protected constructor(opts: OpenOptions, connection?: object, part?: { group: number; worker: number }, routed = false) {
    super();
    if (opts.telemetry) this.tel = new Instruments(opts.telemetry);
    this.serveRemoteCalls = opts.durability === "server";
    this.embedded = !connection && !part && !routed;
    if (routed) {
      this.native = undefined as unknown as NativeEngine;
      this.driving = false;
      return;
    }
    // Bun 1.3 aborts on libuv handles ("unsupported uv function"), and its
    // napi_make_callback does not drain microtasks: macrotasks only.
    const driver = IS_BUN ? undefined : (opts.driver ?? DRIVER);
    this.native = call(() => bindHot(
      native.NativeEngine.open(
        part ? { group: part.group, worker: part.worker, driver } : { ...nativeOptions(opts), driver },
        // The native wake already runs in its own libuv callback, after the
        // microtask queue drained, so it can tick at once. Bun runs queued
        // threadsafe-function calls back to back without draining microtasks
        // between them, so a tick there could deliver to a run whose code is
        // still running: take the next macrotask instead.
        IS_BUN ? () => this.kick() : () => this.tick(),
        connection,
        this.io,
      ),
    ));
    this.driving = this.native.driving();
    // A process serving remote work must remain alive between client calls.
    if (connection) this.workerTimer = setInterval(() => {}, 2 ** 30);
  }

  static async open(opts: OpenOptions = {}): Promise<Engine> {
    const release = useSerializer(opts.serializer);
    try {
      const e = await Engine.openWith(opts);
      e.releaseSerializer = release;
      return e;
    } catch (err) {
      release?.();
      throw err;
    }
  }

  /** @internal The serializer this engine took (see `useSerializer`). */
  releaseSerializer: (() => void) | undefined;

  private static async openWith(opts: OpenOptions): Promise<Engine> {
    const profile = opts.profile ?? process.env.FUSEE_PROFILE ?? "production";
    if (profile !== "dev" && profile !== "production") throw new RejectedError(`profile is "dev" or "production", not '${profile}'`);
    const dev = profile === "dev";
    if (opts.dir === undefined && opts.durability === undefined && opts.address === undefined) {
      // FUSEE_DIR (fusee dev sets it), else a server's worker at FUSEE_URL, else ./fusee-data.
      if (process.env.FUSEE_DIR) opts = { ...opts, dir: process.env.FUSEE_DIR };
      else if (process.env.FUSEE_URL) opts = { ...opts, durability: "server", address: process.env.FUSEE_URL };
      else opts = { ...opts, dir: "./fusee-data" };
    }
    if (dev) {
      const local = (opts.durability ?? "local") === "local";
      const fsync = local && process.platform === "darwin" && opts.flush === undefined;
      opts = { ...opts, keepHistory: opts.keepHistory ?? true, ...(fsync ? { flush: "fsync" as const } : {}) };
      if (!QUIET.on)
        process.stderr.write(
          `fusee: dev profile (${opts.dir ?? opts.address ?? opts.durability}): every run's history is kept` +
            (fsync ? ", flushes use fsync (a process crash loses nothing; power loss may)" : "") +
            ". Not for production.\n",
        );
    }
    if (opts.workers !== undefined) return Workers.start(opts);
    const mode = opts.durability;
    const connection = mode === "server" || mode === "replicated"
      ? await native.prepareConnection(mode, opts.address, opts.replicaId, opts.peers, opts.dir, opts.flush, opts.replicaMetrics?.handle)
      : undefined;
    const e = new Engine(opts, connection);
    e.dev = dev;
    if (opts.recordStacks === false) e.recordStacks = false;
    if (opts.onExhausted) e.onExhausted = opts.onExhausted;
    for (const d of opts.workflows ?? []) e.register(d);
    e.tick();
    // The HTTP API and UI: asked for, or under the dev profile.
    const http = opts.http ?? (dev ? process.env.FUSEE_HTTP ?? "" : "off");
    if (http !== "off") {
      let url: string;
      if (http) url = e.serve({ addr: http }); // asked for: a busy address is an error
      else {
        try {
          url = e.serve({ addr: "127.0.0.1:7601" });
        } catch {
          url = e.serve({ addr: "127.0.0.1:0" }); // 7601 is taken: any free port
        }
      }
      if (!QUIET.on) process.stderr.write(`fusee: UI and HTTP API at ${url}\n`);
    }
    return e;
  }

  /** @internal A worker thread's part of a group; `port` reaches the routing thread. */
  static openPart(group: number, worker: number, opts: OpenOptions, port: MessagePort): Engine {
    const e = new Engine(opts, undefined, { group, worker });
    e.parent = new Link(port, (op, a) => e.serveRouted(op, a));
    e.tick();
    return e;
  }

  /**
   * Registers a workflow type (version 1 unless given). Without a function,
   * returns a handle to start runs of a type registered elsewhere (with
   * `workers`, in the workers' module).
   */
  workflow<I, O>(name: string, fn?: WorkflowFn<I, O>, opts: { version?: number } = {}): Workflow<I, O> {
    const version = opts.version ?? 1;
    if (fn) this.registerCode(name, fn as WorkflowFn<unknown, unknown>, version);
    return new Workflow<I, O>(this, name, version);
  }

  run<O>(id: string): Run<O> {
    return new Run<O>(this, id);
  }

  /**
   * Registers an object type: its handlers by name, exclusive unless marked
   * with `shared`. Without handlers, returns a handle to call a type
   * registered elsewhere (with `workers`, in the workers' module).
   */
  object(name: string, handlers?: ObjectHandlers, opts: { version?: number } = {}): ObjectType {
    if (handlers) this.registerObject(name, handlers, opts.version ?? 1);
    return new ObjectType(this, name);
  }

  /** @internal */
  protected registerObject(name: string, handlers: ObjectHandlers, version: number): void {
    const sharedNames = Object.entries(handlers)
      .filter(([, h]) => typeof h !== "function" && h.shared)
      .map(([n]) => n);
    call(() => this.native.registerObject(name, sharedNames));
    this.registerCode(name, objectMain(handlers), version);
  }

  /** Key `key` of object type `type`. */
  objectRef(type: string, key: string): ObjectRef {
    return new ObjectRef(this, type, key);
  }

  /** Keys of object type `type` (up to `limit` per shard). */
  objects(type: string, limit = 1000): string[] {
    return this.list({ where: { type }, limit })
      .filter((r) => r.object && r.finishedMs == null)
      .map((r) => String(r.id).slice(type.length + 1));
  }

  /** Runs, optionally filtered: `{ where: { status: "failed", amount: { gte: 100 } } }`. */
  /** Current counters for each shard, in shard-number order. */
  shardMetrics(): ShardMetrics[] {
    return JSON.parse(call(() => this.native.shardMetrics()));
  }

  list(opts: number | { where?: Record<string, unknown>; limit?: number } = 100): any[] {
    if (typeof opts === "number") return JSON.parse(call(() => this.native.list(opts)));
    return JSON.parse(call(() => this.native.listWhere(JSON.stringify(opts.where ?? {}), opts.limit ?? 1000)));
  }

  /** Defines (or resizes) a concurrency and rate limit for steps. */
  limit(name: string, spec: { concurrent?: number; perSecond?: number }): void {
    call(() => this.native.limit(name, spec.concurrent ?? 0, spec.perSecond ?? 0));
    this.limitSpecs.set(name, { concurrent: spec.concurrent ?? 0, perSecond: spec.perSecond ?? 0 });
  }

  /** @internal Limits defined through this engine (`limits()`). */
  private readonly limitSpecs = new Map<string, { concurrent: number; perSecond: number }>();

  /** Every limit this engine defined, by name, with its permits now. */
  limits(): { name: string; concurrent: number; perSecond: number; inUse: number; waiting: number }[] {
    return [...this.limitSpecs.entries()]
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([name, s]) => {
        const [inUse = 0, waiting = 0] = this.limitUsage(name);
        return { name, ...s, inUse, waiting };
      });
  }

  /**
   * Serves the HTTP API and the web UI on a thread of its own (they call
   * this engine through its event loop; nothing rides the step path). Off
   * loopback a `token` is required. Returns the URL:
   *
   *     console.log(engine.serve());   // http://127.0.0.1:7601
   */
  serve(opts: { addr?: string; token?: string; insecure?: boolean } = {}): string {
    if (this.parent !== undefined || this.native === undefined)
      throw new RejectedError("serve the HTTP API from the engine Engine.open returned");
    const bound = call(() => this.native.serve(opts.addr ?? "127.0.0.1:7601", opts.token ?? null, opts.insecure ?? false, this.dev ? "dev" : null));
    this.serveRemoteCalls = true;
    this.kick();
    const url = `http://${bound}`;
    if (this.callbackUrl === undefined) this.callbackUrl = url;
    this.callbackBase ??= () => this.callbackUrl;
    return url;
  }

  /** The engine's counters summed over its shards. */
  metrics(): Record<string, number> {
    const total: Record<string, number> = {};
    for (const m of this.shardMetrics()) {
      for (const [k, v] of Object.entries(m)) if (typeof v === "number") total[k] = (total[k] ?? 0) + v;
    }
    return total;
  }

  // ------------------------------------------------ the client interface, on definitions

  /** Registers a workflow definition's code (as `Engine.open({ workflows })` does). */
  register<I, O>(def: WorkflowDefinition<I, O>): WorkflowDefinition<I, O> {
    if (!def.fn) throw new RejectedError(`workflow '${def.name}' has no code to register: define it with workflow(name, fn)`);
    this.workflow(def.name, def.fn, { version: def.version });
    if (def.concurrency) this.runLimits.set(def.name, def.concurrency as Concurrency);
    return def;
  }

  /** @internal Runs at once per type (from definitions with `concurrency`). */
  readonly runLimits = new Map<string, Concurrency>();

  /**
   * Starts a run of `wf` (a definition or a type name) under workflow ID
   * `id`; resolves once the start is durable. `delay` or `at` starts it
   * later: a durable timer (the run `fusee.delay/<id>`), then the start;
   * `result()` waits through the delay.
   */
  async start<I, O>(
    wf: WorkflowDefinition<I, O> | string,
    id: string,
    input: I,
    opts: StartOptions & { delay?: Duration; at?: Date | number } = {},
  ): Promise<Run<O>> {
    const name = nameOf(wf);
    const def = typeof wf === "string" ? undefined : wf;
    const o = def?.timeout !== undefined && opts.timeout === undefined ? { ...opts, timeout: def.timeout } : opts;
    if (opts.delay !== undefined || opts.at !== undefined) {
      this.checkType(name);
      const at = opts.at !== undefined ? +opts.at : nowMs() + (ms(opts.delay) ?? 0);
      const t = ms(o.timeout);
      const coordinator = { type: name, id, input: input ?? null, atMs: at, ...(t !== undefined ? { timeoutMs: t } : {}) };
      await this.startRaw(DELAY_TYPE, 1, `${DELAY_TYPE}/${id}`, coordinator, { idPolicy: opts.idPolicy });
      return new Run<O>(this, id);
    }
    // A type with runs at once: a slot waits for the turn, then starts it.
    const c = (def?.concurrency as Concurrency | undefined) ?? this.runLimits.get(name);
    if (c) {
      this.checkType(name);
      const t = ms(o.timeout);
      const key = c.key ? String(c.key(input)) : undefined;
      const slot = { type: name, id, input: input ?? null, limit: c.limit, ...(key !== undefined ? { key } : {}), ...(t !== undefined ? { timeoutMs: t } : {}) };
      await this.startRaw(QUEUED_TYPE, 1, `${QUEUED_TYPE}/${id}`, slot, { idPolicy: opts.idPolicy });
      return new Run<O>(this, id);
    }
    await this.startRaw(name, def?.version ?? 1, id, input, o);
    return new Run<O>(this, id);
  }

  /**
   * Signals the run `id`, starting it first with `input` if it is not
   * running: both reach the engine before its next tick, so the signal is
   * recorded before a new run's code first runs.
   */
  async signalWithStart<I, O, T>(
    wf: WorkflowDefinition<I, O> | string,
    id: string,
    input: I,
    sig: SignalDefinition<T> | string,
    value: T,
    opts: StartOptions = {},
  ): Promise<Run<O>> {
    const name = nameOf(wf);
    this.checkType(name);
    const policy = { ...opts.idPolicy };
    if ((policy.running ?? "fail") === "fail") policy.running = "use-existing";
    if (this.elsewhere(id)) {
      await this.startWire(name, typeof wf === "string" ? 1 : wf.version, id, wire(input), opts.version, 0, idPolicyBits(policy));
      await this.signalWire(id, nameOf(sig), wire(value));
      return new Run<O>(this, id);
    }
    const t = ms(opts.timeout);
    const latest = opts.version ?? this.latest(name, typeof wf === "string" ? 1 : wf.version);
    const trace = this.tel?.traceparent();
    const started = this.request((tok) => this.native.start(id, name, latest, wire(input), t === undefined ? 0 : nowMs() + t, tok, idPolicyBits(policy), null, null, null, null, trace));
    const by = requesterStore.getStore();
    const signalled = this.request((tok) => this.native.signal(id, nameOf(sig), wire(value), tok, by));
    await started;
    await signalled;
    return new Run<O>(this, id);
  }

  /** Starts `wf` if it is not running and delivers an update in the same record; returns the run and the update's result. */
  updateWithStart<I, O, A, R>(
    wf: WorkflowDefinition<I, O> | string,
    id: string,
    input: I,
    upd: UpdateDefinition<A, R> | string,
    arg: A,
    opts: StartOptions & UpdateOptions = {},
  ): Promise<{ run: Run<O>; result: R }> {
    const name = nameOf(wf);
    this.checkType(name);
    return this.workflow<I, O>(name).updateWithStart<A, R>(id, input, nameOf(upd), arg, opts);
  }

  /** Debounces a start of `wf` under `key` (see `DebounceOptions`). */
  debounce<I, O>(wf: WorkflowDefinition<I, O> | string, key: string, input: I, opts: DebounceOptions): Promise<Run<O>> {
    return this.workflow<I, O>(nameOf(wf)).debounce(key, input, opts);
  }

  /** Adds an event to the batch of `wf` under `key` (see `BatchOptions`). */
  batch<I, O>(wf: WorkflowDefinition<I, O> | string, key: string, event: unknown, opts: BatchOptions = {}): Promise<Run<O>> {
    return this.workflow<I, O>(nameOf(wf)).batch(key, event as never, opts);
  }

  /** Permits of a limit held right now. */
  limitInUse(name: string): number {
    return this.limitUsage(name)[0] ?? 0;
  }

  /** This process's steps waiting for a permit of a limit right now. */
  limitWaiting(name: string): number {
    return this.limitUsage(name)[1] ?? 0;
  }

  /** @internal `[permits held, steps waiting]`. */
  protected limitUsage(name: string): number[] {
    return call(() => this.native.limitUsage(name));
  }

  /** Finishes an async completion from its token; resolves once recorded durably. */
  complete<T>(token: string, value: T): Promise<void> {
    return this.completeWire(token, true, wire(value));
  }

  /** Fails an async completion. */
  fail(token: string, error: string): Promise<void> {
    return this.completeWire(token, false, String(error));
  }

  /** Creates a schedule (or updates its spec) that starts `workflow` on its cron or interval. */
  async schedule<I>(name: string, opts: ScheduleOptions, workflow: string | Workflow<I, unknown>, input?: I): Promise<Schedule> {
    const spec = {
      cron: opts.cron ?? null,
      everyMs: opts.every === undefined ? null : ms(opts.every),
      offsetMs: ms(opts.offset) ?? 0,
      timezone: opts.timezone ?? "UTC",
      overlap: opts.overlap ?? "skip",
      jitterMs: ms(opts.jitter) ?? 0,
      catchupWindowMs: ms(opts.catchupWindow) ?? 365 * 864e5,
      paused: opts.paused ?? false,
      workflow: typeof workflow === "string" ? workflow : workflow.name,
      input: input === undefined ? null : input,
    };
    await this.scheduleSpec(name, JSON.parse(json(spec)));
    return new Schedule(this, name);
  }

  /** @internal Creates or updates schedule `name` from its spec (JSON). */
  async scheduleSpec(name: string, spec: Record<string, unknown>): Promise<void> {
    const id = new Schedule(this, name).id;
    if (this.elsewhere(id)) return this.forward("schedule", [name, spec]) as Promise<void>;
    const problem = this.native.validateSchedule(JSON.stringify(spec));
    if (problem) throw new FuseeError(problem);
    await this.startRaw(SCHEDULE_TYPE, 1, id, { name, spec }, { idPolicy: { running: "use-existing" } });
    await this.run(id).signal(CONTROL, { op: "update", spec });
  }

  /** A handle to an existing schedule. */
  scheduleHandle(name: string): Schedule {
    return new Schedule(this, name);
  }

  /** The schedules: `get(name)`, and `list()` of every schedule that exists. */
  get schedules(): { get(name: string): Schedule; list(): Schedule[] } {
    return {
      get: (name) => new Schedule(this, name),
      list: () =>
        this.list({ where: { type: SCHEDULE_TYPE }, limit: 10_000 })
          .filter((r: { id: string; finishedMs: number | null }) => r.id.startsWith(`${SCHEDULE_TYPE}/`) && r.finishedMs == null)
          .map((r: { id: string }) => new Schedule(this, r.id.slice(SCHEDULE_TYPE.length + 1))),
    };
  }

  /** A run's journal as an export file (`.fjnl`) for `Replayer`, or null. */
  exportJournal(id: string): Buffer | null {
    this.here(id);
    return call(() => this.native.exportJournal(id));
  }

  /**
   * @internal An invoked worker's invocation: "running" until every held
   * shard is idle ("idle"), the budget ends ("expired") or the server is
   * lost ("lost"); null for other engines.
   */
  invocationState(): string | null {
    const awaiting = [...this.awaiting].filter(([, runs]) => runs.length).map(([t]) => t);
    return this.native.invocationState(awaiting);
  }

  /** Flushes and closes. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.releaseSerializer?.();
    if (this.scheduledTick && this.scheduledTick !== true) clearImmediate(this.scheduledTick);
    clearInterval(this.alive);
    if (this.timer) clearTimeout(this.timer);
    if (this.workerTimer) clearInterval(this.workerTimer);
    const reason = new FuseeError("engine closed", "Closed");
    for (const { reject } of this.tokens.values()) reject(reason);
    for (const list of this.waiters.values()) for (const waiter of list) waiter.reject(reason);
    this.tokens.clear();
    this.waiters.clear();
    this.pumps.clear();
    for (const r of this.runs.values()) {
      r.ended = true;
      r.abortSteps(new ClosedError());
    }
    call(() => this.native.close());
  }

  /** @internal */
  latest(wtype: string, version: number): number {
    return Math.max(version, ...[...(this.types.get(wtype)?.keys() ?? [])]);
  }

  /** @internal */
  async startRaw(wtype: string, version: number, id: string, input: unknown, opts: StartOptions): Promise<void> {
    this.checkType(wtype);
    const t = ms(opts.timeout);
    await this.startWire(wtype, version, id, wire(input), opts.version, t === undefined ? 0 : nowMs() + t, idPolicyBits(opts.idPolicy));
  }

  // Operations on one workflow ID. Each runs on the thread that owns the
  // ID's shard: with `workers`, the routing thread forwards it (`forward`),
  // and so does a worker for an ID another worker owns.

  /**
   * @internal A start of a type this process runs no code for: refused here
   * when no other process could run it (embedded, no worker threads).
   */
  checkType(wtype: string): void {
    if (!this.embedded || wtype.startsWith("fusee.") || this.types.has(wtype)) return;
    const names = [...this.types.keys()].filter((n) => !n.startsWith("fusee.")).sort();
    const near = nearest(wtype, names);
    let m = `no code is registered for workflow type '${wtype}'`;
    if (near) m += ` (did you mean '${near}'?)`;
    m += names.length
      ? `. Registered types: ${names.join(", ")}`
      : ". No workflow types are registered: register them with engine.workflow(name, fn)";
    throw new UnknownWorkflowError(m);
  }

  /** @internal Whether an operation on `id` runs on another thread. */
  protected elsewhere(id: string): boolean {
    return this.parent !== undefined && !this.native.ownsId(id);
  }

  /** @internal Sends an operation to the thread that owns its ID. */
  protected forward(op: string, args: unknown[]): Promise<unknown> {
    return this.parent!.call(op, args);
  }

  /** Throws unless this thread holds `id` (for calls that answer synchronously). */
  private here(id: string): void {
    if (this.elsewhere(id))
      throw new FuseeError(`workflow '${id}' belongs to another worker: call this on the engine Engine.open returned`);
  }

  /** @internal */
  async startWire(
    wtype: string, version: number, id: string, input: string | Uint8Array,
    explicit: number | undefined, deadlineMs: number, policy: number,
  ): Promise<void> {
    if (this.elsewhere(id)) return this.forward("start", [wtype, version, id, input, explicit, deadlineMs, policy]) as Promise<void>;
    const latest = wtype === SCHEDULE_TYPE ? 1 : explicit ?? this.latest(wtype, version);
    const trace = this.tel?.traceparent();
    await this.request((tok) => this.native.start(id, wtype, latest, input, deadlineMs, tok, policy, null, null, null, null, trace));
    this.tel?.start(wtype);
  }

  /** @internal */
  updateWithStartWire(
    wtype: string, version: number, id: string, input: string | Uint8Array,
    explicit: number | undefined, deadlineMs: number, policy: number,
    update: string, updateId: string, arg: string | Uint8Array,
  ): Promise<unknown> {
    const args = [wtype, version, id, input, explicit, deadlineMs, policy, update, updateId, arg];
    if (this.elsewhere(id)) return this.forward("updateWithStart", args);
    const latest = explicit ?? this.latest(wtype, version);
    return this.requestValue((tok) =>
      this.native.start(id, wtype, latest, input, deadlineMs, -1, policy, update, updateId, arg, tok, this.tel?.traceparent()),
    );
  }

  /**
   * @internal One debounced start or batched event (see `fusee::queue`):
   * update-with-start on the key's coordinator, sent again while the
   * coordinator it reached was ending. Resolves with the run it joined.
   */
  async enqueue<O>(
    kind: "debounce" | "batch", workflow: string, key: string, eventId: string | undefined, input: unknown,
    a: number | undefined, b: number | undefined, c: number | undefined,
  ): Promise<Run<O>> {
    const id = eventId ?? "";
    const [wtype, coordinator, state, arg] = queueRequest(kind, workflow, key, id, input, a, b, c);
    const run = await untilTaken(coordinator, () =>
      this.updateWithStartWire(wtype, 1, coordinator, state, 1, 0, USE_EXISTING, EVENT, id, arg),
    );
    return new Run<O>(this, run);
  }

  /** @internal Whether a run with this ID exists (running, parked, or finished within retention). */
  runExists(id: string): Promise<boolean> {
    if (this.elsewhere(id)) return this.forward("exists", [id]) as Promise<boolean>;
    try {
      return Promise.resolve(call(() => this.native.result(id)).state !== "notfound");
    } catch (e) {
      return Promise.reject(e);
    }
  }

  /** @internal */
  signalWire(id: string, name: string, value: string | Uint8Array, by = requesterStore.getStore()): Promise<void> {
    if (this.elsewhere(id)) return this.forward("signal", [id, name, value, by]) as Promise<void>;
    return this.request((t) => this.native.signal(id, name, value, t, by));
  }

  /** @internal */
  updateWire(id: string, name: string, updateId: string, arg: string | Uint8Array): Promise<unknown> {
    if (this.elsewhere(id)) return this.forward("update", [id, name, updateId, arg]);
    return this.requestValue((t) => this.native.update(id, name, updateId, arg, t));
  }

  /** @internal Cancels or terminates a run. */
  closeRun(id: string, reason: string, terminate: boolean, by = requesterStore.getStore()): Promise<void> {
    if (this.elsewhere(id)) return this.forward("close-run", [id, reason, terminate, by]) as Promise<void>;
    return this.request((t) => (terminate ? this.native.terminate(id, reason, t, by) : this.native.cancel(id, reason, t, by)));
  }

  /** @internal Resumes a paused run. */
  resumeRun(id: string, by = requesterStore.getStore()): Promise<void> {
    if (this.elsewhere(id)) return this.forward("resume", [id, by]) as Promise<void>;
    return this.request((t) => this.native.resume(id, t, by));
  }

  /** @internal */
  resetRun(id: string, beforeStep: string, reapplySignals: boolean, version: number, by = requesterStore.getStore()): Promise<void> {
    if (this.elsewhere(id)) return this.forward("reset", [id, beforeStep, reapplySignals, version, by]) as Promise<void>;
    return this.request((t) => this.native.reset(id, beforeStep, reapplySignals, version, t, by));
  }

  /** @internal */
  completeWire(token: string, ok: boolean, value: string | Uint8Array): Promise<void> {
    // A malformed token is refused wherever it goes.
    if (this.elsewhere(tokenId(token) ?? "")) return this.forward("complete", [token, ok, value]) as Promise<void>;
    return this.request((t) => this.native.complete(token, ok, value, t));
  }

  /** @internal */
  describeSchedule(id: string): Promise<unknown> {
    if (this.elsewhere(id)) return this.forward("describe", [id]);
    return this.requestValue((t) => this.native.queryNative(id, "describe", encode(null), t));
  }

  /** @internal One read of a stream: the durable chunks from `from`, or those appended within `waitMs`. */
  streamRead(id: string, run: number | undefined, name: string, from: number, waitMs: number): Promise<unknown> {
    if (this.elsewhere(id)) return this.forward("streamRead", [id, run ?? -1, name, from, waitMs]);
    return this.requestValue((t) => this.native.streamRead(id, run ?? -1, name, from, waitMs, t));
  }

  /** @internal The run's summary and journal as JSON, or null. */
  inspectRun(id: string): string | null {
    this.here(id);
    return call(() => this.native.inspect(id));
  }

  /** @internal An operation another thread routed here (see `forward`). */
  serveRouted(op: string, a: any[]): unknown {
    switch (op) {
      case "start":
        return this.startWire(a[0], a[1], a[2], a[3], a[4], a[5], a[6]);
      case "updateWithStart":
        return this.updateWithStartWire(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9]);
      case "signal":
        return this.signalWire(a[0], a[1], a[2], a[3] ?? undefined);
      case "update":
        return this.updateWire(a[0], a[1], a[2], a[3]);
      case "query":
        return this.query(a[0], a[1], a[2]);
      case "close-run":
        return this.closeRun(a[0], a[1], a[2], a[3] ?? undefined);
      case "resume":
        return this.resumeRun(a[0], a[1] ?? undefined);
      case "reset":
        return this.resetRun(a[0], a[1], a[2], a[3], a[4] ?? undefined);
      case "result":
        return this.result(a[0]);
      case "exists":
        return this.runExists(a[0]);
      case "complete":
        return this.completeWire(a[0], a[1], a[2]);
      case "describe":
        return this.describeSchedule(a[0]);
      case "streamRead":
        return this.streamRead(a[0], a[1] < 0 ? undefined : a[1], a[2], a[3], a[4]);
      case "schedule":
        return this.scheduleSpec(a[0], a[1]);
      case "close":
        return this.close();
    }
    throw new FuseeError(`unknown operation '${op}'`);
  }

  /** @internal */
  request(f: (token: number) => void): Promise<void> {
    return this.requestValue<unknown>(f, false) as Promise<void>;
  }

  /** @internal Resolves with the reply's value. */
  requestValue<T>(f: (token: number) => void, value = true): Promise<T> {
    if (this.closed) return Promise.reject(new ClosedError());
    const token = this.nextToken++;
    return new Promise<T>((resolve, reject) => {
      this.tokens.set(token, { resolve: resolve as (v: unknown) => void, reject, value });
      try {
        f(token);
      } catch (e) {
        this.tokens.delete(token);
        reject(nativeError(e));
        return;
      }
      this.kick();
    });
  }

  /** @internal */
  async query<R>(id: string, name: string, arg: unknown): Promise<R> {
    if (this.elsewhere(id)) return this.forward("query", [id, name, arg]) as Promise<R>;
    for (let i = 0; i < 2000; i++) {
      const [shard = 0, run = 0, lsn = 0, foreign] = call(() => this.native.queryPoint(id));
      const x = this.runs.get(shard, run);
      const state = call(() => this.native.codeState(shard, run));
      if (!x && !foreign) {
        // Native code (a schedule) answers itself.
        if (!state.length) throw new FuseeError(`workflow '${id}' is not running code`, "Failed");
        return this.requestValue<R>((t) => this.native.queryNative(id, name, encode(arg), t));
      }
      if (!x || state[0]) {
        // A parked execution has to register its handlers and replay first.
        this.kick();
        await new Promise((r) => setTimeout(r, 1));
        continue;
      }
      const h = x.queries.get(name);
      if (!h) throw new RejectedError(`no handler for query '${name}'`);
      const answer = h(arg) as R;
      await this.request((t) => this.native.ackAt(shard, lsn, t));
      return answer;
    }
    throw new FuseeError("the workflow did not finish replaying", "Failed");
  }

  /** @internal */
  result<O>(id: string): Promise<O> {
    if (this.elsewhere(id)) return this.forward("result", [id]) as Promise<O>;
    const r = call(() => this.native.result(id));
    if (r.state === "notfound") {
      const nf = new NotFoundError(`workflow '${id}' not found`, id);
      if (id.startsWith("fusee.")) return Promise.reject(nf);
      // A delayed start, or a run waiting for its turn: the run that will
      // start it gives its result.
      const waiting = [`${DELAY_TYPE}/${id}`, `${QUEUED_TYPE}/${id}`];
      const elsewhere = waiting.filter((w) => this.elsewhere(w));
      for (const w of waiting) {
        if (!this.elsewhere(w) && call(() => this.native.result(w)).state !== "notfound") return this.result<O>(w);
      }
      if (elsewhere.length > 0) {
        const tryNext = (i: number): Promise<O> =>
          i >= elsewhere.length
            ? Promise.reject(nf)
            : (this.forward("result", [elsewhere[i]]) as Promise<O>).catch((e: unknown) => {
                if (e instanceof FuseeError && e.code === "NotFound") return tryNext(i + 1);
                throw e;
              });
        return tryNext(0);
      }
      return Promise.reject(nf);
    }
    if (r.state === "done") return settle<O>(r.status, toWire(r.payload));
    return new Promise<O>((resolve, reject) => {
      const list = this.waiters.get(r.shard, r.run);
      const w = { id, resolve: resolve as (v: unknown) => void, reject };
      if (list) list.push(w);
      else this.waiters.set(r.shard, r.run, [w]);
      this.kick();
    });
  }

  /** @internal Deliver at most once per workflow per event-loop turn. */
  scheduleRun(execution: RunExec): void {
    this.pumps.add(execution);
    this.kick(false);
  }

  /**
   * @internal Schedules a tick soon (coalesced). `full` false: the turn is
   * for deliveries only, and its tick may skip the engine (`IO_LIGHT`).
   */
  kick(full = true): void {
    if (full) this.fullTick = true;
    // Inside a turn the native driver runs: it runs another once this one
    // and its microtasks are done; one only for deliveries (2) unless
    // anything asked for a full one (1).
    if (this.driving && this.io[IO_DRIVING] !== 0) {
      this.io[IO_KICK] = full || this.io[IO_KICK] === 1 ? 1 : 2;
      return;
    }
    if (this.scheduledTick || this.closed) return;
    if (KICK_NATIVE || this.driving) {
      // The native callback does not hold the process open: a timer that
      // never fires does, until the tick runs.
      this.scheduledTick = true;
      this.alive.ref();
      this.native.wake();
      return;
    }
    this.scheduledTick = setImmediate(() => {
      this.scheduledTick = undefined;
      this.tick();
    });
  }

  private async handleRemote(relay: string, method: string, a: Record<string, any>): Promise<void> {
    // Who asked, as the API that took the call named them (audit).
    const by = AUDITED.includes(method) && typeof a.by === "string" && a.by !== "" ? a.by : undefined;
    if (by !== undefined && requesterStore.getStore() !== by) {
      return requesterStore.run(by, () => this.handleRemote(relay, method, a));
    }
    // A call's options as the protocol carries them: null means unset.
    if (a.options && typeof a.options === "object")
      a.options = Object.fromEntries(Object.entries(a.options).filter(([, v]) => v !== null));
    const run = this.run(a.id);
    const workflow = new Workflow(this, a.type, a.version ?? 1);
    let response: object;
    try {
      let value: unknown;
      switch (method) {
        case "start": {
          const { delay: d, at, ...o } = a.options ?? {};
          if (d != null || at != null) await this.start(a.type, a.id, a.input, { ...o, delay: d ?? undefined, at: at ?? undefined });
          else await workflow.start(a.id, a.input, { ...o, version: a.version });
          break;
        }
        case "signalWithStart":
          await this.signalWithStart(a.type, a.id, a.input, a.name, a.value, { ...a.options, version: a.version });
          break;
        case "describe": value = await run.describe(); break;
        case "types":
          value = [...this.types.entries()]
            .filter(([n]) => !n.startsWith("fusee."))
            .sort(([x], [y]) => (x < y ? -1 : 1))
            .map(([name, vs]) => ({ name, versions: [...vs.keys()].sort((x, y) => x - y), object: false }));
          break;
        case "metrics": value = { total: this.metrics(), shards: this.shardMetrics() }; break;
        case "limits": value = this.limits(); break;
        case "info": value = { engine: "fusee", sdk: "typescript", shards: this.shardMetrics().length }; break;
        case "updateWithStart":
          value = (await workflow.updateWithStart(a.id, a.input, a.name, a.arg, { ...a.options, id: a.updateId, version: a.version })).result; break;
        case "signal": await run.signal(a.name, a.value); break;
        case "update": value = await run.update(a.name, a.arg, { id: a.updateId }); break;
        case "query": value = await run.query(a.name, a.arg); break;
        case "result": value = await run.result(); break;
        case "complete": await this.complete(a.token, a.value); break;
        case "fail": await this.fail(a.token, a.error); break;
        case "cancel": await run.cancel(a.reason); break;
        case "resume": await run.resume(); break;
        case "terminate": await run.terminate(a.reason); break;
        case "reset": await run.reset({ beforeStep: a.beforeStep, reapplySignals: a.reapplySignals, version: a.version }); break;
        case "list": value = this.list({ where: a.where, limit: a.limit }); break;
        case "streamRead":
          value = await this.streamRead(a.id, a.run ?? undefined, a.name, a.from ?? 0, Math.min(a.waitMs ?? 30_000, 60_000));
          break;
        default: throw new FuseeError(`unknown remote call '${method}'`);
      }
      response = { value: value ?? null };
    } catch (e) {
      const code = e instanceof FuseeError ? e.code : "WorkflowFailed";
      // A step's, child's or workflow's failure goes as its envelope, so the
      // caller gets the same error, cause and all.
      const failure = e instanceof StepError || e instanceof ChildError || e instanceof WorkflowError || !(e instanceof FuseeError);
      response = {
        error: failure ? failureText(failureOf(e, this.recordStacks)) : e instanceof Error ? e.message : String(e),
        kind: code.replace(/([a-z])([A-Z])/g, "$1_$2").toLowerCase(),
      };
    }
    if (!this.closed) this.native.replyCall(relay, JSON.stringify(response));
  }

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
  private deliveryTurn(): boolean {
    if (
      !this.driving ||
      this.io[IO_TURN] !== 2 ||
      this.fullTick ||
      this.pumps.size === 0 ||
      this.deliveryTurns >= DELIVERY_TURNS ||
      this.timerAt <= nowMs()
    )
      return false;
    this.deliveryTurns += 1;
    this.turn += 1;
    this.turnBegan = performance.now();
    this.inlineThisTurn = 0;
    const due = this.takePumps();
    for (const execution of due) execution.pumpScheduled();
    this.givePumps(due);
    // Nothing more to deliver: a full turn (the engine flushes what the
    // deliveries appended).
    if (this.io[IO_KICK] === 0) this.io[IO_KICK] = 1;
    return true;
  }

  /** The runs due for a delivery, leaving an empty set for the next ones. */
  private takePumps(): Set<RunExec> {
    const due = this.pumps;
    this.pumps = this.sparePumps ?? new Set();
    this.sparePumps = undefined;
    return due;
  }

  /** Keeps a set `takePumps` returned, once it is gone through. */
  private givePumps(due: Set<RunExec>): void {
    due.clear();
    this.sparePumps = due;
  }

  private tick(): void {
    if (this.closed) return;
    if (this.deliveryTurn()) return;
    this.deliveryTurns = 0;
    if (this.scheduledTick) {
      if (this.scheduledTick !== true) clearImmediate(this.scheduledTick);
      else this.alive.unref();
      this.scheduledTick = undefined;
    }
    this.turn += 1;
    this.turnBegan = performance.now();
    this.inlineThisTurn = 0;
    // Only work queued before this turn can deliver here. Promise callbacks
    // created by a delivery always run before another delivery to that run.
    const due = this.pumps.size ? this.takePumps() : undefined;
    // A turn only for deliveries skips the engine's tick (unless it asked
    // for one): a fan-out's deliveries then cost one tick, not one each.
    const light = due !== undefined && !this.fullTick && !(this.timerAt <= nowMs());
    this.fullTick = false;
    let extra: (Float64Array | Wire)[] | null;
    try {
      this.io[IO_IDLE] = due ? 0 : 1;
      this.io[IO_LIGHT] = light ? 1 : 0;
      extra = this.native.tick();
      if (this.serveRemoteCalls) {
        let msg: string | null;
        while ((msg = this.native.nextCall()) != null) {
          const { relay, method, args } = JSON.parse(msg);
          void this.handleRemote(relay, method, args).catch(() => this.close().catch(() => {}));
        }
      }
    } catch (e) {
      const reason = nativeError(e);
      for (const { reject } of this.tokens.values()) reject(reason);
      for (const ws of this.waiters.values()) for (const w of ws) w.reject(reason);
      void this.close().catch(() => {});
      return;
    }
    const io = this.io;
    const n = io[IO_EVENTS]!;
    const next = io[IO_NEXT]!;
    const nextWakeMs = next < 0 ? null : next;
    if (io[IO_OWED] !== 0) {
      io[IO_OWED] = 0;
      this.kick();
    }
    if (n) {
      // Events beyond the shared array's room come in their own array.
      // Nothing ticks while they are handled (ticks are macrotasks), so the
      // shared rows stay as they are.
      const big = extra?.[0] as Float64Array | null | undefined;
      const ev = big ?? io;
      const base = big ? 0 : IO_EV;
      for (let o = base; o < base + n * EV_STRIDE; o += EV_STRIDE) this.handle(ev, o, extra);
    }
    if (due) {
      for (const execution of due) execution.pumpScheduled();
      this.givePumps(due);
    }
    const awaited = this.tokens.size > 0 || this.waiters.size > 0;
    const now = nowMs();
    if (nextWakeMs != null && nextWakeMs <= now) {
      // A busy shard (or a deadline already passed) needs the next turn. A
      // zero setTimeout would wait at least 1 ms.
      this.kick();
    } else if (nextWakeMs != null || awaited) {
      const latest = Math.min(nextWakeMs ?? Infinity, now + (awaited ? 50 : 2 ** 31 - 1));
      if (!this.timer || latest < this.timerAt) {
        if (this.timer) clearTimeout(this.timer);
        this.timerAt = latest;
        this.timer = setTimeout(() => {
          this.timer = undefined;
          this.timerAt = Infinity;
          this.tick();
        }, Math.max(0, latest - now));
      }
      if (awaited) this.timer.ref?.();
      else this.timer.unref?.();
    } else if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
      this.timerAt = Infinity;
    }
    if (n) this.kick();
    // After a light turn that asked for no other, a full one: the records
    // its deliveries appended reach the disk.
    else if (light && !this.scheduledTick && this.io[IO_KICK] === 0) this.kick();
  }

  /** One event: `ev[o]` is its kind (see `Ev`), its strings and payloads are in `extra`. */
  private handle(ev: Float64Array, o: number, extra: (Float64Array | Wire)[] | null): void {
    const kind = ev[o]!;
    const x = 1 + ev[o + 5]!;
    switch (kind) {
      case Ev.Wake:
        this.runs.get(ev[o + 1]!, ev[o + 2]!)?.wake();
        return;
      case Ev.Ack:
      case Ev.Reject:
      case Ev.Reply:
      case Ev.Refuse: {
        const token = ev[o + 1]!;
        const t = this.tokens.get(token);
        if (!t) return;
        this.tokens.delete(token);
        if (kind === Ev.Ack) t.resolve(null);
        else if (kind === Ev.Reply) {
          const payload = extra![x] as Wire;
          if (ev[o + 2] === St.Ok) {
            let v: unknown;
            try {
              v = fromWire(payload);
            } catch (e) {
              t.reject(nativeError(e));
              return;
            }
            t.resolve(v);
          } else t.reject(new FuseeError(wireText(payload)));
        } else if (kind === Ev.Refuse) t.reject(new RejectedError(wireText(extra![x] as Wire)));
        else t.reject(new FuseeError("the workflow has already finished, or its ID is in use", t.value ? "AlreadyExists" : "Rejected"));
        return;
      }
      case Ev.Result: {
        const shard = ev[o + 1]!;
        const run = ev[o + 2]!;
        const status = ev[o + 3]!;
        const ws = this.waiters.get(shard, run);
        if (!ws) return;
        this.waiters.delete(shard, run);
        const payload = extra![x] as Wire;
        for (const w of ws) {
          if (status === St.ContinuedAsNew || status === St.Reset) {
            // Follow the ID to the run that took over.
            this.result(w.id).then(w.resolve, w.reject);
          } else settle(status, payload).then(w.resolve, w.reject);
        }
        return;
      }
      case Ev.Start:
        this.onStart({
          shard: ev[o + 1]!,
          run: ev[o + 2]!,
          version: ev[o + 3]!,
          startedMs: ev[o + 4]!,
          id: extra![x] as string,
          wtype: extra![x + 1] as string,
          input: extra![x + 2] as Wire,
          trace: (extra![x + 3] as string) || undefined,
        });
        return;
      case Ev.Stuck:
        this.onEnded(ev[o + 1]!, ev[o + 2]!, wireText(extra![x] as Wire));
        return;
      case Ev.Ended:
        this.onEnded(ev[o + 1]!, ev[o + 2]!);
        return;
    }
  }
}

/** The engine options the native side reads. */
export function nativeOptions(opts: OpenOptions): Record<string, unknown> {
  return {
    dir: opts.dir,
    durability: opts.durability ?? "local",
    shards: opts.shards,
    retentionMs: ms(opts.retention),
    parkAfterMs: ms(opts.parkAfter),
    checkpointIntervalMs: ms(opts.checkpointInterval),
    flush: opts.flush,
    segmentBytes: opts.segmentBytes,
    keepHistory: opts.keepHistory,
    archiveHistory: opts.archiveHistory,
    codecKeys: opts.codec?.keys,
    inlineFlush: opts.inlineFlush,
  };
}

/** The workflow ID inside a completion token (`fusee:<run>:<seq>:<attempt>:<hex ID>`). */
function tokenId(token: string): string | undefined {
  const m = /^fusee:\d+:\d+:\d+:((?:[0-9a-fA-F]{2})*)$/.exec(token);
  return m ? Buffer.from(m[1]!, "hex").toString("utf8") : undefined;
}

/** A path (relative to the working directory) or URL, as a URL `import()` takes. */
export function moduleUrl(m: string | URL): string {
  if (m instanceof URL) return m.href;
  return /^[a-z][a-z0-9+.-]+:/i.test(m) ? m : pathToFileURL(resolvePath(m)).href;
}
