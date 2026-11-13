// Handles: a workflow type on an engine, a run, a schedule.
import { ms, wire } from "./codec.js";
import { QueryDefinition, SignalDefinition, UpdateDefinition, nameOf, update } from "./definitions.js";
import { Engine } from "./engine.js";
import { FuseeError, NotFoundError, RejectedError } from "./errors.js";
import { delay } from "./execution.js";
import { nowMs } from "./native.js";
import { StreamOptions, StreamReader } from "./objects.js";
import { BatchOptions, CONTROL, DebounceOptions, StartOptions, UpdateOptions, idPolicyBits } from "./types.js";

export class Workflow<I, O> {
  constructor(
    private readonly engine: Engine,
    readonly name: string,
    readonly version: number,
  ) {}

  /** Starts a run. Resolves once the start is durable. */
  async start(id: string, input: I, opts: StartOptions = {}): Promise<Run<O>> {
    await this.engine.startRaw(this.name, this.version, id, input, opts);
    return new Run<O>(this.engine, id);
  }

  /**
   * Starts the workflow if it is not running and delivers an update in the
   * same record; if it is running, delivers the update to it. Returns the
   * update's result once it is durable.
   */
  async updateWithStart<A, R>(
    id: string,
    input: I,
    update: string,
    arg: A,
    opts: StartOptions & UpdateOptions = {},
  ): Promise<{ run: Run<O>; result: R }> {
    const p = { ...opts.idPolicy };
    if ((p.running ?? "fail") === "fail") p.running = "use-existing";
    const t = ms(opts.timeout);
    const result = (await this.engine.updateWithStartWire(
      this.name,
      this.version,
      id,
      wire(input),
      opts.version,
      t === undefined ? 0 : nowMs() + t,
      idPolicyBits(p),
      update,
      opts.id ?? "",
      wire(arg),
    )) as R;
    return { run: new Run<O>(this.engine, id), result };
  }

  /**
   * Debounces a start under `key`: starts inside the window collapse into
   * one run, which begins with the latest start's input when the window
   * closes (`window` after the latest start, or `maxWait` after the first).
   * Resolves once this start is durable, with the handle of that run; the
   * run exists once the window closes (`run.started()`).
   */
  async debounce(key: string, input: I, opts: DebounceOptions): Promise<Run<O>> {
    const window = ms(opts.window);
    if (window === undefined) throw new FuseeError("debounce needs a window");
    return this.engine.enqueue<O>("debounce", this.name, key, opts.id, input, window, ms(opts.maxWait), ms(opts.dedupWindow));
  }

  /**
   * Adds an event to the batch under `key`: once the batch holds
   * `maxEvents` events or `maxWait` has passed since its first, one run
   * starts with all of them, in the order they were accepted, as its input
   * (an array). Resolves once the event is durable, with the handle of that
   * run.
   */
  async batch(key: string, event: I extends readonly (infer E)[] ? E : unknown, opts: BatchOptions = {}): Promise<Run<O>> {
    return this.engine.enqueue<O>("batch", this.name, key, opts.id, event, opts.maxEvents, ms(opts.maxWait), ms(opts.dedupWindow));
  }
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

export class Run<O> {
  constructor(
    private readonly engine: Engine,
    readonly id: string,
  ) {}

  /** Sends a signal (a definition or its name); resolves once it is durable. */
  signal<T>(sig: SignalDefinition<T> | string, value: T): Promise<void> {
    return this.engine.signalWire(this.id, nameOf(sig), wire(value));
  }

  /** Waits for the run's result (following continue-as-new and reset). */
  result(): Promise<O> {
    return this.engine.result<O>(this.id);
  }

  /**
   * Waits until a run with this ID exists. A debounced or batched run
   * starts only when its window closes; until then `result` fails with
   * NotFound.
   */
  async started(): Promise<void> {
    for (let pause = 1; !(await this.engine.runExists(this.id)); pause = Math.min(pause * 2, 50)) await delay(pause);
  }

  /** Sends an update and waits for its handler's durable result. */
  update<A, R>(upd: UpdateDefinition<A, R> | string, arg: A, opts: UpdateOptions = {}): Promise<R> {
    return this.engine.updateWire(this.id, nameOf(upd), opts.id ?? "", wire(arg)) as Promise<R>;
  }

  /** Calls a query handler; the answer waits until the state it read is durable. */
  query<R, A = unknown>(q: QueryDefinition<A, R> | string, arg?: A): Promise<R> {
    return this.engine.query<R>(this.id, nameOf(q), arg);
  }

  cancel(reason = "cancelled"): Promise<void> {
    return this.engine.closeRun(this.id, reason, false);
  }

  /** Ends the run at once; children follow their parent close policy. */
  terminate(reason = "terminated"): Promise<void> {
    return this.engine.closeRun(this.id, reason, true);
  }

  /**
   * Resumes a run that paused because a step ran out of retries
   * (`retry: { onExhausted: "pause" }`): the step runs again at once, with
   * a fresh set of attempts, on the code this process has now. Rejected if
   * nothing is paused.
   */
  resume(): Promise<void> {
    return this.engine.resumeRun(this.id);
  }

  /** Starts a new run whose journal is this one's cut just before `beforeStep`. */
  reset(opts: { beforeStep: string; reapplySignals?: boolean; version?: number }): Promise<void> {
    return this.engine.resetRun(this.id, opts.beforeStep, opts.reapplySignals ?? true, opts.version ?? 0);
  }

  /** The run's summary: status, type, version, times, error. */
  async status(): Promise<RunSummary> {
    const { journal: _j, objectDetail: _o, stepTimes: _t, ...summary } = await this.describe();
    return summary;
  }

  /**
   * The run and its journal (each record with its `kind`), and for a key of
   * an object type its state. Throws NotFoundError for an unknown ID.
   */
  async describe(): Promise<RunDetail> {
    const j = this.engine.inspectRun(this.id);
    if (!j) throw new NotFoundError(`workflow '${this.id}' not found`, this.id);
    return JSON.parse(j) as RunDetail;
  }

  /** Runs a failed run again from its failed step (a reset before it, with its signals). */
  async retry(): Promise<void> {
    const d = await this.describe();
    const failed = [...d.journal].reverse().find((r) => r.kind === "step" && r.status === "failed");
    if (!failed) throw new RejectedError(`workflow '${this.id}' has no failed step to retry (status ${d.status})`);
    await this.reset({ beforeStep: String(failed.name) });
  }

  /** @deprecated Use `describe()`. */
  inspect(): any {
    const j = this.engine.inspectRun(this.id);
    return j ? JSON.parse(j) : null;
  }

  /** A reader of the run's stream `name` (`for await (const chunk of run.stream("tokens"))`). */
  stream<T = unknown>(name: string, opts: StreamOptions = {}): StreamReader<T> {
    return new StreamReader<T>((run, from, waitMs) => this.engine.streamRead(this.id, run, name, from, waitMs), opts);
  }
}

/** A schedule, addressed by name. */
export class Schedule {
  constructor(
    private readonly engine: Engine,
    readonly name: string,
  ) {}

  get id(): string {
    return `fusee.schedule/${this.name}`;
  }

  private send(cmd: object): Promise<void> {
    return this.engine.run(this.id).signal(CONTROL, cmd);
  }
  pause(): Promise<void> {
    return this.send({ op: "pause" });
  }
  resume(): Promise<void> {
    return this.send({ op: "resume" });
  }
  /** Starts one run now. */
  trigger(): Promise<void> {
    return this.send({ op: "trigger" });
  }
  /** Starts a run for every scheduled time in [from, to]. */
  backfill(from: Date | number, to: Date | number): Promise<void> {
    return this.send({ op: "backfill", fromMs: +from, toMs: +to });
  }
  /** The schedule's state: spec, next time, running run, counters. */
  describe(): Promise<any> {
    return this.engine.describeSchedule(this.id);
  }
  /** Stops the schedule; runs it started keep running. */
  delete(): Promise<void> {
    return this.engine.run(this.id).terminate("schedule deleted");
  }
}
