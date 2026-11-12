// Worker threads: an engine split across threads.
import { type MessagePort, Worker, parentPort } from "node:worker_threads";
import { Engine, moduleUrl, nativeOptions } from "./engine.js";
import { AlreadyExistsError, BusyError, CancelledError, ChildError, ClosedError, CodecError, ConnectionError, DeterminismError, Failure, FuseeError, IoError, NotFoundError, RejectedError, StepError, TerminatedError, TimeoutError, UnknownWorkflowError, WorkflowError, call, nativeError } from "./errors.js";
import { NativeGroup, native } from "./native.js";
import { ShardMetrics } from "./remote.js";
import { Schedule, Workflow } from "./runs.js";
import { OpenOptions, WorkflowFn } from "./types.js";

// ---------------------------------------------------------------- worker threads

/** An error as it crosses threads. */
interface ErrorInfo {
  name: string;
  message: string;
  code?: string;
  reason?: string;
  step?: string;
  workflowId?: string;
  id?: string;
  /** The failure envelope a step, child or workflow error carries. */
  failure?: Failure;
}

/** Fusee's error classes, by name, as they cross threads. */
const ERRORS: Record<string, Function> = {
  FuseeError,
  NotFoundError,
  AlreadyExistsError,
  UnknownWorkflowError,
  StepError,
  WorkflowError,
  ChildError,
  CancelledError,
  TimeoutError,
  TerminatedError,
  DeterminismError,
  RejectedError,
  CodecError,
  ConnectionError,
  IoError,
  BusyError,
  ClosedError,
};

/** What crosses a thread of an error: its class, message and Fusee's fields. */
function errorInfo(e: unknown): ErrorInfo {
  if (!(e instanceof Error)) return { name: "Error", message: String(e) };
  const x = e as Error & Record<string, unknown>;
  const str = (v: unknown) => (typeof v === "string" ? v : undefined);
  const i: ErrorInfo = { name: e.name, message: e.message, code: str(x.code), reason: str(x.reason), step: str(x.step) };
  if (typeof x.workflowId === "string") i.workflowId = x.workflowId;
  if (typeof x.id === "string") i.id = x.id;
  if (e instanceof FuseeError && e.cause !== undefined) i.failure = e.cause;
  return i;
}

/** The error `errorInfo` describes, of the same class where it is one of Fusee's. */
function revive(i: ErrorInfo): Error {
  if (i.name === "StepError" && i.step !== undefined && i.failure) return new StepError(i.step, i.failure);
  if (i.name === "ChildError" && i.workflowId !== undefined && i.failure) return new ChildError(i.workflowId, i.failure);
  if (i.name === "WorkflowError" && i.failure) return new WorkflowError(i.failure, i.workflowId);
  const cls = Object.hasOwn(ERRORS, i.name) ? ERRORS[i.name]! : Error;
  const e = Reflect.construct(Error, [i.message], cls) as Error & Record<string, unknown>;
  e.name = i.name;
  if (i.code !== undefined) Object.defineProperty(e, "code", { value: i.code, enumerable: true });
  if (i.reason !== undefined) e.reason = i.reason;
  if (i.step !== undefined) e.step = i.step;
  if (i.id !== undefined) e.id = i.id;
  if (i.workflowId !== undefined) e.workflowId = i.workflowId;
  return e;
}

/**
 * Calls between two threads over a port, batched per microtask. Each
 * message is an array of calls `[id, op, args]` and replies
 * `[-id, ok, value]`; `serve` answers the other side's calls.
 */
export class Link {
  private nextId = 1;
  private readonly waiting = new Map<number, { resolve: (v: unknown) => void; reject: (e: unknown) => void }>();
  private queue: unknown[][] = [];
  private broken: Error | undefined;

  /** `hold`: keep the process alive (ref the port) only while a call waits for its reply. */
  constructor(
    private readonly port: MessagePort | Worker,
    private readonly serve: (op: string, args: any[]) => unknown,
    private readonly hold = false,
  ) {
    port.on("message", (m: unknown) => {
      if (Array.isArray(m)) this.receive(m as unknown[][]);
    });
  }

  call(op: string, args: unknown[]): Promise<unknown> {
    if (this.broken) return Promise.reject(this.broken);
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.waiting.set(id, { resolve, reject });
      if (this.hold && this.waiting.size === 1) this.port.ref();
      this.send([id, op, args]);
    });
  }

  /** The other side is gone: every waiting and later call fails. */
  fail(e: Error): void {
    this.broken ??= e;
    for (const w of this.waiting.values()) w.reject(this.broken);
    this.waiting.clear();
    if (this.hold) this.port.unref();
  }

  private send(m: unknown[]): void {
    if (this.queue.push(m) === 1) queueMicrotask(() => this.flush());
  }

  private flush(): void {
    const q = this.queue;
    this.queue = [];
    if (!q.length || this.broken) return;
    try {
      this.port.postMessage(q);
      return;
    } catch {
      // Something in the batch cannot be copied: send the rest one by one.
    }
    for (const m of q) {
      try {
        this.port.postMessage([m]);
      } catch (e) {
        const info = errorInfo(new FuseeError(`a value cannot be copied to another thread: ${e instanceof Error ? e.message : String(e)}`));
        const id = m[0] as number;
        if (id > 0) this.settle(id, false, info);
        else this.port.postMessage([[id, 0, info]]);
      }
    }
  }

  private receive(batch: unknown[][]): void {
    for (const m of batch) {
      const id = m[0] as number;
      if (id < 0) {
        this.settle(-id, m[1] === 1, m[2]);
        continue;
      }
      let r: unknown;
      try {
        r = this.serve(m[1] as string, m[2] as any[]);
      } catch (e) {
        this.send([-id, 0, errorInfo(e)]);
        continue;
      }
      Promise.resolve(r).then(
        (v) => this.send([-id, 1, v]),
        (e) => this.send([-id, 0, errorInfo(e)]),
      );
    }
  }

  private settle(id: number, ok: boolean, v: unknown): void {
    const w = this.waiting.get(id);
    if (!w) return;
    this.waiting.delete(id);
    if (this.hold && !this.waiting.size) this.port.unref();
    if (!ok) w.reject(revive(v as ErrorInfo));
    // Bytes arrive as a plain Uint8Array; the SDK hands out Buffers.
    else w.resolve(v instanceof Uint8Array && !Buffer.isBuffer(v) ? Buffer.from(v.buffer, v.byteOffset, v.byteLength) : v);
  }
}

export const WORKER_KEY = "fusee.worker";

/**
 * The engine `Engine.open({ workers })` returns. Workflow code runs on the
 * worker threads, each holding its own engine part (the shards it owns);
 * this thread holds no shards and routes each call on a workflow ID to the
 * worker that owns its shard. Reads across workers (`list`, `inspect`,
 * `shardMetrics`) block until each worker's thread answers.
 */
export class Workers extends Engine {
  private readonly threads: Worker[] = [];
  private readonly links: Link[] = [];

  private constructor(
    opts: OpenOptions,
    private readonly group: NativeGroup,
  ) {
    super(opts, undefined, undefined, true);
  }

  static async start(opts: OpenOptions): Promise<Engine> {
    const n = opts.workers!;
    if (!Number.isInteger(n) || n < 1) throw new RejectedError(`workers must be a whole number of at least 1, not ${n}`);
    if (opts.module === undefined) throw new FuseeError("workers need `module`: the module that registers workflows in each worker");
    if (opts.telemetry) throw new FuseeError("telemetry does not cross to worker threads");
    const mode = opts.durability ?? "local";
    if (mode !== "local" && mode !== "memory") throw new RejectedError(`workers need local or memory durability, not ${mode}`);
    const group = call(() => native.NativeGroup.open(nativeOptions(opts), n));
    const [id = 0, , workers = 1] = group.info();
    const e = new Workers(opts, group);
    const href = moduleUrl(opts.module);
    try {
      for (let w = 0; w < workers; w++) {
        // The package's entry, so its modules load in their order (index.ts).
        const t = new Worker(new URL("./index.js", import.meta.url), {
          workerData: { [WORKER_KEY]: { group: id, worker: w, module: href, driver: opts.driver } },
        });
        const link = new Link(t, (op, a) => e.dispatch(op, a), true);
        t.on("error", (err) => link.fail(err instanceof Error ? err : new FuseeError(String(err))));
        t.on("exit", (code) => link.fail(new ClosedError(`worker ${w} exited (code ${code})`)));
        e.threads.push(t);
        e.links.push(link);
      }
      await Promise.all(e.threads.map((t, w) => started(t, w)));
    } catch (err) {
      e.closed = true;
      clearInterval(e.alive);
      await Promise.all(e.threads.map((t) => t.terminate()));
      group.close();
      throw err;
    }
    for (const t of e.threads) t.unref();
    return e;
  }

  /** @internal Sends an operation to the worker that owns its ID's shard. */
  dispatch(op: string, a: any[]): Promise<unknown> {
    if (this.closed) return Promise.reject(new ClosedError());
    let w: number;
    try {
      w =
        op === "start" || op === "updateWithStart"
          ? this.group.route(a[2], false)
          : op === "complete"
            ? this.group.route(a[0], true)
            : op === "schedule"
              ? this.group.route(new Schedule(this, a[0]).id, false)
              : this.group.route(a[0], false);
    } catch (e) {
      return Promise.reject(nativeError(e));
    }
    if (w < 0) return Promise.reject(new RejectedError("not a completion token"));
    return this.links[w]!.call(op, a);
  }

  protected override elsewhere(): boolean {
    return true;
  }

  protected override forward(op: string, args: unknown[]): Promise<unknown> {
    return this.dispatch(op, args);
  }

  /** A handle to start runs of a workflow type the workers' module registers. */
  override workflow<I, O>(name: string, fn?: WorkflowFn<I, O>, opts: { version?: number } = {}): Workflow<I, O> {
    if (fn) throw new RejectedError(`with workers, register '${name}' in the workers' module`);
    return super.workflow<I, O>(name, undefined, opts);
  }

  /** @internal Objects are registered in the workers' module. */
  protected override registerObject(name: string): void {
    throw new RejectedError(`with workers, register object '${name}' in the workers' module`);
  }

  override list(opts: number | { where?: Record<string, unknown>; limit?: number } = 100): any[] {
    if (typeof opts === "number") return JSON.parse(call(() => this.group.list(opts)));
    return JSON.parse(call(() => this.group.listWhere(JSON.stringify(opts.where ?? {}), opts.limit ?? 1000)));
  }

  override shardMetrics(): ShardMetrics[] {
    return JSON.parse(call(() => this.group.shardMetrics()));
  }

  override limit(name: string, spec: { concurrent?: number; perSecond?: number }): void {
    call(() => this.group.limit(name, spec.concurrent ?? 0, spec.perSecond ?? 0));
  }

  protected override limitUsage(name: string): number[] {
    return call(() => this.group.limitUsage(name));
  }

  override exportJournal(id: string): Buffer | null {
    return call(() => this.group.exportJournal(id));
  }

  /** @internal */
  override inspectRun(id: string): string | null {
    return call(() => this.group.inspect(id));
  }

  /** Closes every worker's engine (flushing and checkpointing its shards), then stops the workers. */
  override async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    clearInterval(this.alive);
    await Promise.all(this.links.map((l) => l.call("close", []).catch(() => {})));
    await Promise.all(this.threads.map((t) => t.terminate()));
    this.group.close();
  }
}

/** Resolves once worker `w` has opened its part and registered its workflows. */
export function started(t: Worker, w: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const done = () => {
      t.off("message", onMessage);
      t.off("error", onError);
      t.off("exit", onExit);
    };
    const onMessage = (m: { t?: string; e?: ErrorInfo }) => {
      if (m?.t === "ready") {
        done();
        resolve();
      } else if (m?.t === "failed") {
        done();
        reject(revive(m.e!));
      }
    };
    const onError = (e: unknown) => {
      done();
      reject(e);
    };
    const onExit = (code: number) => {
      done();
      reject(new ClosedError(`worker ${w} exited (code ${code}) while starting`));
    };
    t.on("message", onMessage);
    t.on("error", onError);
    t.on("exit", onExit);
  });
}

/** A worker thread: open this worker's part, register the module's workflows, then serve. */
export async function bootWorker(d: { group: number; worker: number; module: string; driver?: "native" | "macrotask" }): Promise<void> {
  const port = parentPort!;
  let engine: Engine | undefined;
  try {
    engine = Engine.openPart(d.group, d.worker, { driver: d.driver }, port);
    const mod = await import(d.module);
    const register = typeof mod.default === "function" ? mod.default : mod.register;
    if (typeof register !== "function")
      throw new FuseeError(`${d.module} exports no function to register workflows (export default (engine) => { ... })`);
    await register(engine);
    port.postMessage({ t: "ready" });
  } catch (e) {
    // Let go of this worker's shards before the open fails.
    await engine?.close().catch(() => {});
    port.postMessage({ t: "failed", e: errorInfo(e) });
  }
}
