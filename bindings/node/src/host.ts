// What hosts runs' code: the backend over the native engine, and the code host.
import { encoded, fromWire, text, toWire } from "./codec.js";
import { Engine } from "./engine.js";
import { CancelledError, DeterminismError } from "./errors.js";
import { Backend, Ctx, RunExec, outcomeOf } from "./execution.js";
import { Delivery, IO_COMMIT, IO_OWED, NativeDelivery, NativeEvent, StartEvent, nowMs } from "./native.js";
import { Instruments } from "./telemetry.js";
import { WorkflowFn } from "./types.js";

type Registered = { fn: WorkflowFn<unknown, unknown>; version: number };

export type Waiter = { id: string; resolve: (v: unknown) => void; reject: (e: unknown) => void };

/** Values keyed by (shard, run), without building a string key. */
export class RunTable<T> {
  private readonly shards: (Map<number, T> | undefined)[] = [];
  size = 0;
  get(shard: number, run: number): T | undefined {
    return this.shards[shard]?.get(run);
  }
  has(shard: number, run: number): boolean {
    return this.shards[shard]?.has(run) ?? false;
  }
  set(shard: number, run: number, v: T): void {
    let m = this.shards[shard];
    if (!m) this.shards[shard] = m = new Map();
    const before = m.size;
    m.set(run, v);
    this.size += m.size - before;
  }
  delete(shard: number, run: number): boolean {
    const m = this.shards[shard];
    if (!m?.delete(run)) return false;
    this.size -= 1;
    return true;
  }
  *values(): IterableIterator<T> {
    for (const m of this.shards) if (m) yield* m.values();
  }
  clear(): void {
    this.shards.length = 0;
    this.size = 0;
  }
}

/** Step names an engine keeps by number (`EngineBackend.beginStep`). */
const STEP_NAMES = 4096;

export class EngineBackend implements Backend {
  readonly out: Float64Array;
  constructor(private readonly e: Engine) {
    this.out = e.io;
  }
  /** A wake raised during the last call was not sent: tick soon instead. */
  private owed(): void {
    if (this.out[IO_OWED] !== 0) {
      this.out[IO_OWED] = 0;
      this.e.kick();
    }
  }
  scheduleRun(execution: RunExec): void { this.e.scheduleRun(execution); }
  turn(): number { return this.e.turn; }
  deliverNow(): boolean { return this.e.deliverNow(); }
  beginStep(shard: number, run: number, name: string, readOnly: boolean): number {
    // A name the engine already holds goes by number: converting the string
    // on every step cost an allocation and a copy. At most STEP_NAMES are
    // kept (names made per run, such as with an ID in them, take the string).
    let id = this.stepNames.get(name);
    if (id === undefined && this.stepNames.size < STEP_NAMES) {
      id = this.e.native.stepName(name);
      this.stepNames.set(name, id);
    }
    const go = id === undefined
      ? this.e.native.beginStep(shard, run, name, readOnly)
      : this.e.native.beginStepNamed(shard, run, id, readOnly);
    this.owed();
    return go;
  }
  private readonly stepNames = new Map<string, number>();
  stepGate(shard: number, run: number, gate: number, notBefore: number): boolean {
    const open = this.e.native.stepGate(shard, run, gate, notBefore, true);
    this.owed();
    return open;
  }
  gateOpen(shard: number, gate: number, notBefore: number): boolean {
    return gate <= this.out[IO_COMMIT + shard]! && (notBefore === 0 || notBefore <= nowMs());
  }
  finishStep(shard: number, run: number, seq: number, ok: boolean, value: string | Uint8Array): void {
    if (typeof value === "string") this.e.native.finishStep(shard, run, seq, ok, value);
    else this.e.native.finishStepBytes(shard, run, seq, value);
    this.owed();
  }
  call(shard: number, op: string, run: number, args: (string | number)[], bufs: Buffer[] = []): number[] {
    const r = this.e.native.hostCall(shard, op, run, args.map(String), bufs);
    this.owed();
    return r;
  }
  bytes(shard: number, op: string, run: number, args: (string | number)[], bufs: Buffer[] = []): Buffer | null {
    const r = this.e.native.hostBytes(shard, op, run, args.map(String), bufs);
    this.owed();
    return r;
  }
  valueNow(shard: number, run: number, liveMs: number): number {
    const r = this.e.native.valueNow(shard, run, liveMs);
    this.owed();
    return r;
  }
  beginSleep(shard: number, run: number, name: string, atMs: number): number {
    const r = this.e.native.beginSleep(shard, run, name, atMs);
    this.owed();
    return r;
  }
  finishStepDeliver(shard: number, run: number, seq: number, ok: boolean, value: string): Delivery | null {
    const r = this.e.native.finishStepDeliver(shard, run, seq, ok, value);
    this.owed();
    return this.delivery(r);
  }
  quiescent(shard: number, run: number): Delivery | null {
    const r = this.e.native.quiescent(shard, run);
    this.owed();
    return this.delivery(r);
  }
  private delivery(r: string | Buffer | NativeDelivery | null): Delivery | null {
    const o = this.out;
    switch (o[0]) {
      case 0:
        return null;
      case 1:
        return { kind: "resolve", seq: o[1]!, status: o[2]!, payload: r as string | Buffer | null, held: o[3] === 2 };
      default: {
        const d = r as NativeDelivery;
        return { ...d, payload: d.kind === "cancel" ? text(d.payload) : toWire(d.payload), held: false };
      }
    }
  }
  decideUpdate(shard: number, token: number, ok: boolean, message: string): number {
    return this.e.native.decideUpdate(shard, token, ok, message);
  }
  finish(shard: number, run: number, status: number, value: string | Uint8Array): void {
    this.e.native.finish(shard, run, status, value);
    this.owed();
  }
  stuck(shard: number, run: number, message: string): void {
    this.e.native.stuck(shard, run, message);
  }
  kick(full = true): void {
    this.e.kick(full);
  }
}

/** Runs workflow code: shared by the engine and the replayer. */
export abstract class CodeHost {
  /** Failures record their stack trace (OpenOptions.recordStacks). */
  protected recordStacks = true;
  /** What a step does when its retries run out (OpenOptions.onExhausted). */
  protected onExhausted: "fail" | "pause" = "fail";
  /** @internal Where the HTTP API is reached, for completion URLs. */
  callbackBase: (() => string | undefined) | undefined;
  /** @internal The context class each run gets (`fusee/testing` extends it). */
  ctxClass: new (x: RunExec) => Ctx = Ctx;
  protected readonly types = new Map<string, Map<number, Registered>>();
  protected readonly runs = new RunTable<RunExec>();
  protected readonly awaiting = new Map<string, StartEvent[]>();
  /** @internal */
  protected tel: Instruments | undefined;
  protected abstract backend(): Backend;

  /** @internal Registers a type's code by name. */
  registerCode(name: string, fn: WorkflowFn<unknown, unknown>, version: number): void {
    let m = this.types.get(name);
    if (!m) this.types.set(name, (m = new Map()));
    m.set(version, { fn, version });
    const waiting = this.awaiting.get(name) ?? [];
    this.awaiting.set(
      name,
      waiting.filter((ev) => !this.instantiate(ev)),
    );
  }

  protected onStart(ev: StartEvent): void {
    if (!this.instantiate(ev)) {
      const list = this.awaiting.get(ev.wtype) ?? [];
      list.push(ev);
      this.awaiting.set(ev.wtype, list);
    }
  }

  protected onEnded(shard: number, run: number, stuck?: string): void {
    const r = this.runs.get(shard, run);
    this.runs.delete(shard, run);
    if (r) {
      r.ended = true;
      this.tel?.endWorkflow(r, stuck !== undefined ? 1 : 4);
      r.abortSteps(new CancelledError("the run ended"));
      if (stuck !== undefined) r.fail(new DeterminismError(stuck || "determinism error"));
    }
  }

  protected instantiate(ev: StartEvent): boolean {
    const reg = this.types.get(ev.wtype)?.get(ev.version);
    if (!reg) return false;
    const { shard, run } = ev;
    if (this.runs.has(shard, run)) return true;
    const be = this.backend();
    const x = new RunExec(be, shard, run, ev.id, ev.wtype, ev.version, ev.startedMs, this.ctxClass);
    x.recordStacks = this.recordStacks;
    x.onExhausted = this.onExhausted;
    x.callbackBase = this.callbackBase;
    x.tel = this.tel;
    this.tel?.workflow(x, ev.trace);
    this.runs.set(shard, run, x);
    x.resumed();
    let input: unknown;
    try {
      input = fromWire(ev.input);
    } catch {
      input = undefined;
    }
    let p: Promise<unknown>;
    try {
      p = Promise.resolve(reg.fn(x.ctx, input));
    } catch (e) {
      p = Promise.reject(e);
    }
    const failed = (e: unknown): void => {
      if (x.ended || e instanceof DeterminismError) return;
      x.ended = true;
      this.runs.delete(shard, run);
      const [status, msg] = outcomeOf(e, this.recordStacks);
      be.finish(shard, run, status, msg);
      this.tel?.finish(x.wtype, status);
      this.tel?.endWorkflow(x, status);
      be.kick();
    };
    // One reaction for both outcomes: a separate `catch` for the
    // compensations cost every run another promise, an async function and
    // a microtask, though few runs register any.
    p.then(
      (out) => {
        if (x.ended) return;
        x.ended = true;
        this.runs.delete(shard, run);
        const [status, data] = encoded(out, "result");
        be.finish(shard, run, status, data);
        this.tel?.finish(x.wtype, status);
        this.tel?.endWorkflow(x, status);
        be.kick();
      },
      (e: unknown) => {
        // Failed or cancelled: the compensations run as steps, newest
        // first, then the run ends with its failure (ctx.compensate); a
        // determinism error in one of them ends it instead.
        const undo = x.undo;
        if (undo?.length && !x.ended && !(e instanceof DeterminismError)) {
          x.undo = undefined;
          compensate(x, undo).then(() => failed(e), failed);
        } else failed(e);
      },
    );
    x.wake();
    return true;
  }
}

/** Runs a failed run's compensations as steps, newest first. */
async function compensate(x: RunExec, undo: NonNullable<RunExec["undo"]>): Promise<void> {
  for (let k = undo.pop(); k; k = undo.pop()) {
    try {
      await x.ctx.step(k.name, k.fn, k.opts);
    } catch (err) {
      if (err instanceof DeterminismError) throw err;
      console.error(`fusee: workflow '${x.id}': compensation '${k.name}' failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
}

/** A start event as the replayer's native side describes it. */
export function startEvent(ev: NativeEvent): StartEvent {
  return {
    shard: ev.shard,
    run: ev.run,
    id: ev.id ?? "",
    wtype: ev.wtype ?? "",
    version: ev.version,
    startedMs: ev.startedMs,
    input: toWire(ev.input),
  };
}
