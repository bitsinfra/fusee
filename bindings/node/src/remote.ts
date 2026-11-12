// A client of a Fusee server, and the runs and objects it addresses.
import { ms } from "./codec.js";
import { QueryDefinition, SignalDefinition, UpdateDefinition, WorkflowDefinition, nameOf, workflow } from "./definitions.js";
import { FuseeError, RejectedError, nativeError } from "./errors.js";
import { delay } from "./execution.js";
import { NativeClient, native } from "./native.js";
import { CallOptions, EditOptions, EditResult, JsonPatchOp, SEND_PREFIX, STATE_QUERY, StateEdit, StreamOptions, StreamReader, editArg } from "./objects.js";
import { RunDetail, RunSummary } from "./runs.js";
import { BatchOptions, DebounceOptions, Duration, EVENT, StartOptions, UpdateOptions, queueRequest, untilTaken } from "./types.js";

/** Sends calls to the worker holding a Fusee server's shards. */
export class Client {
  private constructor(private readonly transport: NativeClient) {}

  static async connect(address: string): Promise<Client> {
    try { return new Client(await native.connectClient(address)); }
    catch (e) { throw nativeError(e); }
  }

  async call<T = unknown>(method: string, args: object): Promise<T> {
    try { return JSON.parse(await this.transport.call(method, JSON.stringify(args))); }
    catch (e) { throw nativeError(e); }
  }

  /** As `Engine.start`: a definition or a type name; `delay`/`at` start it later. */
  async start<I, O = unknown>(
    wf: WorkflowDefinition<I, O> | string,
    id: string,
    input: I,
    opts: StartOptions & { version?: number; delay?: Duration; at?: Date | number } = {},
  ): Promise<RemoteRun<O>> {
    const { delay: d, at, ...rest } = opts;
    const options: Record<string, unknown> = { ...rest, timeout: ms(rest.timeout ?? (typeof wf === "string" ? undefined : wf.timeout)) };
    if (d !== undefined) options.delay = ms(d);
    if (at !== undefined) options.at = +at;
    await this.call("start", { type: nameOf(wf), id, input, version: opts.version, options });
    return this.run<O>(id);
  }

  /** As `Engine.signalWithStart`. */
  async signalWithStart<I, O, T>(
    wf: WorkflowDefinition<I, O> | string,
    id: string,
    input: I,
    sig: SignalDefinition<T> | string,
    value: T,
    opts: StartOptions & { version?: number } = {},
  ): Promise<RemoteRun<O>> {
    const idPolicy = { ...opts.idPolicy };
    if ((idPolicy.running ?? "fail") === "fail") idPolicy.running = "use-existing";
    await this.call("signalWithStart", { type: nameOf(wf), id, input, name: nameOf(sig), value, version: opts.version,
      options: { ...opts, idPolicy, timeout: ms(opts.timeout) } });
    return this.run<O>(id);
  }

  /** As `Engine.updateWithStart`: the run and the update's result. */
  async updateWithStart<I, O, A, R>(
    wf: WorkflowDefinition<I, O> | string,
    id: string,
    input: I,
    upd: UpdateDefinition<A, R> | string,
    arg: A,
    opts: StartOptions & UpdateOptions & { version?: number } = {},
  ): Promise<{ run: RemoteRun<O>; result: R }> {
    const result = await this.call<R>("updateWithStart", { type: nameOf(wf), id, input, name: nameOf(upd), arg,
      updateId: opts.id ?? "", version: opts.version, options: { ...opts, timeout: ms(opts.timeout) } });
    return { run: this.run<O>(id), result };
  }

  /** Debounces a start of workflow type `type` under `key` (see `Workflow.debounce`). */
  async debounce<I, O = unknown>(wf: WorkflowDefinition<I, O> | string, key: string, input: I, opts: DebounceOptions): Promise<RemoteRun<O>> {
    const type = nameOf(wf);
    const window = ms(opts.window);
    if (window === undefined) throw new FuseeError("debounce needs a window");
    return this.enqueue<O>("debounce", type, key, opts.id ?? "", input, window, ms(opts.maxWait), ms(opts.dedupWindow));
  }

  /** Adds an event to the batch of workflow type `type` under `key` (see `Workflow.batch`). */
  async batch<E, O = unknown>(wf: WorkflowDefinition<any, O> | string, key: string, event: E, opts: BatchOptions = {}): Promise<RemoteRun<O>> {
    return this.enqueue<O>("batch", nameOf(wf), key, opts.id ?? "", event, opts.maxEvents, ms(opts.maxWait), ms(opts.dedupWindow));
  }

  private async enqueue<O>(
    kind: "debounce" | "batch", workflow: string, key: string, eventId: string, input: unknown,
    a: number | undefined, b: number | undefined, c: number | undefined,
  ): Promise<RemoteRun<O>> {
    const [type, id, state, arg] = queueRequest(kind, workflow, key, eventId, input, a, b, c);
    const args = { type, id, input: JSON.parse(state), name: EVENT, arg: JSON.parse(arg), updateId: eventId, version: 1,
      options: { idPolicy: { running: "use-existing", finished: "allow" } } };
    return this.run<O>(await untilTaken(id, () => this.call("updateWithStart", args)));
  }

  run<O = unknown>(id: string): RemoteRun<O> { return new RemoteRun(this, id); }
  /** Key `key` of object type `type`, called through the server. */
  objectRef(type: string, key: string): RemoteObjectRef { return new RemoteObjectRef(this, type, key); }
  list(where: Record<string, unknown> = {}, limit = 100): Promise<Record<string, unknown>[]> {
    return this.call("list", { where, limit });
  }
  async complete(token: string, value?: unknown): Promise<void> { await this.call("complete", { token, value }); }
  async fail(token: string, error: string): Promise<void> { await this.call("fail", { token, error }); }
  close(): void { this.transport.close(); }
}

/** A key of an object type, called through a server (see `ObjectRef`). */
export class RemoteObjectRef {
  constructor(private readonly client: Client, readonly type: string, readonly key: string) {}
  get id(): string { return `${this.type}/${this.key}`; }
  call<R = unknown, A = unknown>(handler: string, arg?: A, opts: CallOptions = {}): Promise<R> {
    return this.client.call("updateWithStart", { type: this.type, id: this.id, input: null, name: handler, arg: arg ?? null,
      updateId: opts.id ?? "", options: { idPolicy: { running: "use-existing" } } });
  }
  async send<A = unknown>(handler: string, arg?: A, opts: CallOptions = {}): Promise<void> {
    await this.client.call("updateWithStart", { type: this.type, id: this.id, input: null, name: handler, arg: arg ?? null,
      updateId: SEND_PREFIX + (opts.id ?? ""), options: { idPolicy: { running: "use-existing" } } });
  }
  editState(edit: StateEdit, opts: EditOptions = {}): Promise<EditResult> {
    return this.client.call("editState", { id: this.id, edit: editArg(edit, opts), updateId: opts.id ?? "" });
  }
  setState(field: string, value: unknown, opts: EditOptions = {}): Promise<EditResult> {
    return this.editState({ set: { [field]: value } }, opts);
  }
  patchState(ops: JsonPatchOp[], opts: EditOptions = {}): Promise<EditResult> { return this.editState({ patch: ops }, opts); }
  clearState(field?: string, opts: EditOptions = {}): Promise<EditResult> {
    return this.editState(field === undefined ? { clearAll: true } : { clear: [field] }, opts);
  }
  async state<T = Record<string, unknown>>(): Promise<T> {
    try { return await this.client.call("query", { id: this.id, name: STATE_QUERY, arg: null }); }
    catch (e) { if (e instanceof FuseeError && /not ?found/i.test(e.code + e.message)) return {} as T; throw e; }
  }
}

export class RemoteRun<O = unknown> {
  constructor(private readonly client: Client, readonly id: string) {}
  result(): Promise<O> { return this.client.call("result", { id: this.id }); }
  /** Waits until a run with this ID exists (a debounced or batched run starts when its window closes). */
  async started(): Promise<void> {
    for (let pause = 1; !(await this.client.list({ id: this.id }, 1)).length; pause = Math.min(pause * 2, 50)) await delay(pause);
  }
  async signal<T>(sig: SignalDefinition<T> | string, value: T): Promise<void> {
    await this.client.call("signal", { id: this.id, name: nameOf(sig), value });
  }
  query<R, A = unknown>(q: QueryDefinition<A, R> | string, arg?: A): Promise<R> {
    return this.client.call("query", { id: this.id, name: nameOf(q), arg });
  }
  update<A, R>(upd: UpdateDefinition<A, R> | string, arg: A, opts: UpdateOptions = {}): Promise<R> {
    return this.client.call("update", { id: this.id, name: nameOf(upd), arg, updateId: opts.id ?? "" });
  }
  async cancel(reason = "cancelled"): Promise<void> { await this.client.call("cancel", { id: this.id, reason }); }
  async terminate(reason = "terminated"): Promise<void> { await this.client.call("terminate", { id: this.id, reason }); }
  /** Resumes a paused run: its step runs again with a fresh set of attempts. */
  async resume(): Promise<void> { await this.client.call("resume", { id: this.id }); }
  /** As `Run.reset`: `{ beforeStep, reapplySignals }` (a step name alone works too). */
  async reset(opts: { beforeStep: string; reapplySignals?: boolean } | string, reapplySignals = true): Promise<void> {
    const o = typeof opts === "string" ? { beforeStep: opts, reapplySignals } : opts;
    await this.client.call("reset", { id: this.id, beforeStep: o.beforeStep, reapplySignals: o.reapplySignals ?? true });
  }
  /** The run and its journal (as `Run.describe`). */
  describe(): Promise<RunDetail> { return this.client.call("describe", { id: this.id }); }
  /** The run's summary (as `Run.status`). */
  async status(): Promise<RunSummary> {
    const { journal: _j, objectDetail: _o, stepTimes: _t, ...summary } = await this.describe();
    return summary;
  }
  /** Runs a failed run again from its failed step. */
  async retry(): Promise<void> {
    const d = await this.describe();
    const failed = [...d.journal].reverse().find((r) => r.kind === "step" && r.status === "failed");
    if (!failed) throw new RejectedError(`workflow '${this.id}' has no failed step to retry (status ${d.status})`);
    await this.reset({ beforeStep: String(failed.name) });
  }
  /** A reader of the run's stream `name`, answered by the worker holding its shard. */
  stream<T = unknown>(name: string, opts: StreamOptions = {}): StreamReader<T> {
    return new StreamReader<T>((run, from, waitMs) => this.client.call("streamRead", { id: this.id, run, name, from, waitMs }), opts);
  }
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
