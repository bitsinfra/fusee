// Replaying exported journals against code.
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { Codec, St, fromWire, tagged, text, toWire, wireText } from "./codec.js";
import { WorkflowDefinition } from "./definitions.js";
import { CancelledError, TerminatedError, TimeoutError, call, errorOf, nativeError, parseFailure } from "./errors.js";
import { Backend } from "./execution.js";
import { CodeHost, startEvent } from "./host.js";
import { Delivery, NativeReplay, Wire, native } from "./native.js";
import { WorkflowFn } from "./types.js";

class ReplayBackend implements Backend {
  readonly out = new Float64Array(8);
  constructor(private readonly r: NativeReplay) {}
  beginStep(shard: number, run: number, name: string): number {
    const [seq = 0, replay = 0, gate = 0, attempt = 1, notBefore = 0] = this.call(shard, "beginStep", run, [name]);
    this.out.set([seq, replay, gate, attempt, notBefore]);
    // Step functions never run on replay: the gate stays shut.
    return replay ? 2 : 0;
  }
  stepGate(shard: number, run: number, gate: number, notBefore: number): boolean {
    return Boolean(this.call(shard,"mayRun",run,[gate,notBefore])[0]);
  }
  finishStep(shard: number, run: number, seq: number, ok: boolean, value: string | Uint8Array): void {
    this.call(shard,"stepDone",run,[seq,ok?"1":"0"],[tagged(value)]);
  }
  call(_shard: number, op: string, run: number, args: (string | number)[], bufs: Buffer[] = []): number[] {
    return this.r.hostCall(op, run, args.map(String), bufs);
  }
  bytes(_shard: number, op: string, run: number, args: (string | number)[], bufs: Buffer[] = []): Buffer | null {
    return this.r.hostBytes(op, run, args.map(String), bufs);
  }
  quiescent(_shard: number, run: number): Delivery | null {
    const d = this.r.quiescent(run);
    if (!d) return null;
    return { ...d, payload: d.kind === "cancel" ? text(d.payload) : toWire(d.payload), held: false };
  }
  decideUpdate(_shard: number, token: number, ok: boolean, message: string): number {
    return this.r.decideUpdate(token, ok, message);
  }
  finish(_shard: number, run: number, status: number, value: string | Uint8Array): void {
    this.r.finish(run, status, tagged(value));
  }
  stuck(): void {}
  kick(): void {}
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

/**
 * Runs workflow code against journals exported with `fusee export`, without
 * running step functions or timers, and reports the first difference.
 */
export class Replayer extends CodeHost {
  private current: ReplayBackend | undefined;
  constructor(private readonly opts: { codec?: Codec; workflows?: readonly WorkflowDefinition<any, any>[] } = {}) {
    super();
    for (const d of opts.workflows ?? []) {
      if (d.fn) this.workflow(d.name, d.fn, { version: d.version });
    }
  }

  protected backend(): Backend {
    return this.current!;
  }

  workflow<I, O>(name: string, fn: WorkflowFn<I, O>, opts: { version?: number } = {}): this {
    this.types.set(name, this.types.get(name) ?? new Map());
    this.types.get(name)!.set(opts.version ?? 1, { fn: fn as WorkflowFn<unknown, unknown>, version: opts.version ?? 1 });
    return this;
  }

  async replay(file: Buffer): Promise<ReplayReport> {
    const r = call(() => native.NativeReplay.open(this.opts.codec?.keys ?? null));
    this.current = new ReplayBackend(r);
    this.runs.clear();
    this.awaiting.clear();
    const head = JSON.parse(call(() => r.import(file)));
    const report: ReplayReport = { workflowId: head.workflowId, workflowType: head.workflowType, ok: false, finished: false };
    try {
      if (!head.complete) {
        report.error = "the export holds only the run's start and result (export from an engine with keepHistory)";
        return report;
      }
      if (!this.types.get(head.workflowType)?.get(head.version)) {
        report.error = `no code registered for workflow type '${head.workflowType}' version ${head.version}`;
        return report;
      }
      let quiet = 0;
      while (quiet < 20) {
        const { events } = r.tick();
        for (const ev of events) {
          if (ev.kind === "start") this.onStart(startEvent(ev));
          else if (ev.kind === "wake") this.runs.get(ev.shard, ev.run)?.wake();
          else if (ev.kind === "stuck") this.onEnded(ev.shard, ev.run, ev.message ?? "");
          else if (ev.kind === "ended") this.onEnded(ev.shard, ev.run);
        }
        quiet = events.length ? 0 : quiet + 1;
        await new Promise((res) => setImmediate(res));
      }
      const [stuck = "", replaying = "false", live = "false", finished = "false"] = r.state(head.run);
      report.finished = finished === "true";
      if (stuck) report.error = stuck;
      else if (replaying === "true" && live === "false") report.error = "the code stopped before reaching the end of its journal";
      else report.ok = true;
      return report;
    } finally {
      for (const x of this.runs.values()) x.ended = true;
      r.close();
    }
  }

  replayFile(path: string): Promise<ReplayReport> {
    return this.replay(readFileSync(path)).then((rep) => ({ ...rep, file: path }));
  }

  /** Replays every `.fjnl` file in a directory. */
  async replayDir(dir: string): Promise<ReplayReport[]> {
    const out = [];
    for (const f of readdirSync(dir).filter((f) => f.endsWith(".fjnl")).sort()) out.push(await this.replayFile(join(dir, f)));
    return out;
  }
}

export function settle<O>(status: number, payload: Wire): Promise<O> {
  switch (status) {
    case St.Ok:
      try {
        return Promise.resolve(fromWire<O>(payload));
      } catch (e) {
        return Promise.reject(nativeError(e));
      }
    case St.Cancelled:
      return Promise.reject(new CancelledError(wireText(payload)));
    case St.TimedOut:
      return Promise.reject(new TimeoutError(wireText(payload)));
    case St.Terminated:
    case St.ContinuedAsNew:
    case St.Reset:
      return Promise.reject(new TerminatedError(wireText(payload)));
    default:
      return Promise.reject(errorOf(parseFailure(wireText(payload))));
  }
}
