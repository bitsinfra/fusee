/**
 * The test kit: an engine for tests, with step mocks and checks.
 *
 *     import { TestEngine } from "@bitsinfra/fusee/testing";
 *
 *     const t = await TestEngine.open({ workflows: [checkout] });
 *     t.mockStep(checkout, "charge", (attempt) => ({ id: "ch_1" }));
 *     const run = await t.start(checkout, "order-1", order);
 *     assert.equal((await run.result()).charge, "ch_1");
 *     assert.deepEqual(await t.steps("order-1"), ["quote", "charge"]);
 *     await t.check();   // strict replay of every run
 *     await t.close();
 *
 * The engine is in memory, under the dev profile, and serves nothing. A mock
 * replaces a step's function by workflow type and step name; one that throws
 * fails that attempt, which is retried under the step's policy.
 */
import { isDeepStrictEqual } from "node:util";
import {
  Ctx,
  DeterminismError,
  Engine,
  Replayer,
  type Duration,
  type JournalRecord,
  type Run,
  type StartOptions,
  type StepCtx,
  type StepOptions,
  type WorkflowDefinition,
} from "./index.js";
// The kit's internals, from their own modules (after the index, which sets
// the load order): the index does not export them, so its declarations
// name only public things.
import { QUIET } from "./definitions.js";
import { TEST_CLOCK } from "./native.js";

type Mock = (attempt: number) => unknown;

const ms = (d: Duration): number => {
  if (typeof d === "number") return d;
  const m = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)$/.exec(d.trim());
  if (!m) throw new Error(`bad duration '${d}'`);
  return Number(m[1]) * { ms: 1, s: 1e3, m: 6e4, h: 36e5, d: 864e5 }[m[2] as "ms" | "s" | "m" | "h" | "d"];
};

export class TestEngine {
  /** Step functions running now (time skips only when none is). */
  private inflight = 0;
  private readonly mocks = new Map<string, Mock>();

  private constructor(
    /** The engine under test (a new one after `crash()`). */
    public engine: Engine,
    private readonly workflows: readonly WorkflowDefinition<any, any>[],
    private readonly shards: number,
  ) {}

  private async openEngine(): Promise<Engine> {
    QUIET.on = true;
    let engine: Engine;
    try {
      engine = await Engine.open({ durability: "memory", profile: "dev", http: "off", workflows: [], shards: this.shards });
    } finally {
      QUIET.on = false;
    }
    const mocks = this.mocks;
    const self = this;
    engine.ctxClass = class MockingCtx extends Ctx {
      override step<T>(name: string, fn: ((s: StepCtx) => T | Promise<T>) | (() => T | Promise<T>), o: StepOptions = {}): Promise<T> {
        const m = mocks.get(`${this.workflowType}/${name}`);
        const target = (m ? (s: StepCtx) => m(s.attempt) : fn) as (s: StepCtx) => unknown;
        const counted = async (s: StepCtx): Promise<T> => {
          self.inflight += 1;
          try {
            return (await target(s)) as T;
          } finally {
            self.inflight -= 1;
          }
        };
        return super.step(name, counted, o);
      }
    };
    for (const d of this.workflows) engine.register(d);
    return engine;
  }

  static async open(opts: { workflows?: readonly WorkflowDefinition<any, any>[]; shards?: number } = {}): Promise<TestEngine> {
    const t = new TestEngine(undefined as unknown as Engine, opts.workflows ?? [], opts.shards ?? 1);
    t.engine = await t.openEngine();
    return t;
  }

  /** The virtual time, Unix ms. */
  now(): number {
    return Date.now() + TEST_CLOCK.offset;
  }

  /** Moves the clock forward: timers that fall due fire. */
  async advance(by: Duration): Promise<void> {
    TEST_CLOCK.set(TEST_CLOCK.offset + ms(by));
    this.engine.kick();
    await new Promise((r) => setImmediate(r));
  }

  /** While no step function runs, moves the clock to the next timer. */
  private async skip(): Promise<void> {
    if (this.inflight) return;
    const at = this.engine.native.nextTimerMs();
    if (at == null) return;
    const now = this.now();
    if (at > now) await this.advance(at - now);
    else this.engine.kick();
  }

  /**
   * The run's result. While every run waits on a timer, time skips to it,
   * so a workflow that sleeps a day finishes at once.
   */
  async result<O = unknown>(id: string): Promise<O> {
    const p = this.engine.run<O>(id).result();
    let done = false;
    p.then(() => (done = true), () => (done = true));
    while (!done) {
      await new Promise((r) => setTimeout(r, 2));
      if (!done) await this.skip();
    }
    return p;
  }

  /**
   * Drops the engine's memory as a crash would and recovers: a new engine
   * loads every run from its journal, the code replays, and runs continue.
   * Recorded steps do not run again.
   */
  async crash(opts: { cut?: { id: string; beforeStep: number } } = {}): Promise<void> {
    const old = this.engine;
    const files: [number, Buffer, number | null][] = [];
    for (const r of old.list(10_000) as { id: string; type: string; startedMs: number }[]) {
      if (r.type.startsWith("fusee.")) continue;
      const f = old.exportJournal(r.id);
      if (f) files.push([r.startedMs, f, opts.cut?.id === r.id ? opts.cut.beforeStep : null]);
    }
    await old.close();
    const engine = await this.openEngine();
    for (const [, f, before] of files.sort((a, b) => a[0] - b[0])) engine.native.importJournal(f, before);
    engine.kick();
    this.engine = engine;
  }

  /** Replaces a step's function: `fn(attempt)` returns its result, or throws to fail that attempt. */
  mockStep(wf: WorkflowDefinition<any, any> | string, step: string, fn: Mock): void {
    this.mocks.set(`${typeof wf === "string" ? wf : wf.name}/${step}`, fn);
  }

  start<I, O>(wf: WorkflowDefinition<I, O> | string, id: string, input: I, opts: StartOptions = {}): Promise<Run<O>> {
    return this.engine.start(wf, id, input, opts);
  }

  run<O = unknown>(id: string): Run<O> {
    return this.engine.run<O>(id);
  }

  /** The run's journal without times or run IDs, for snapshot tests. */
  async snapshot(id: string): Promise<JournalRecord[]> {
    return (await this.engine.run(id).describe()).journal;
  }

  /** The names of the steps the run recorded, in order. */
  async steps(id: string): Promise<string[]> {
    return (await this.snapshot(id)).filter((r) => r.kind === "step").map((r) => String(r.name));
  }

  /**
   * Strict replay of every run against the registered code: throws
   * DeterminismError naming each run and step whose code differs from its journal.
   */
  async check(): Promise<void> {
    const found: string[] = [];
    for (const r of this.engine.list(10_000) as { id: string; type: string }[]) {
      if (r.type.startsWith("fusee.")) continue;
      const file = this.engine.exportJournal(r.id);
      if (!file) continue;
      const rep = await new Replayer({ workflows: this.workflows }).replay(file);
      const err = rep.error;
      if (err && !err.startsWith("the export holds only") && !err.startsWith("no code registered")) found.push(`${r.id} (${r.type}): ${err}`);
    }
    if (found.length) throw new DeterminismError(found.join("; "));
  }

  async close(): Promise<void> {
    await this.engine.close();
    TEST_CLOCK.set(0);
  }
}

/** One run for `crashEverywhere`: the workflows (with fresh fakes), the run, and the outside effects. */
export interface Case {
  workflows: readonly WorkflowDefinition<any, any>[];
  workflow: WorkflowDefinition<any, any> | string;
  id: string;
  input?: unknown;
  /** The outside effects of the run's steps (a fake service's calls), compared with deepStrictEqual. */
  effects: () => unknown;
}

/**
 * Runs a workflow once per step boundary with a crash at each: the step's
 * function ran, its result was not recorded, the engine recovered. Each time
 * the result and the outside effects must equal those of a run without a
 * crash; a step that is not idempotent (charges twice when run again) is
 * found and named.
 *
 *     await crashEverywhere(() => {
 *       const payments = new FakePayments();
 *       return { workflows: [checkoutWith(payments)], workflow: "checkout", id: "o-1", input: order, effects: () => payments.snapshot() };
 *     });
 */
export async function crashEverywhere(build: () => Case): Promise<void> {
  const same = (a: unknown, b: unknown): boolean => isDeepStrictEqual(a, b);
  let c = build();
  let t = await TestEngine.open({ workflows: c.workflows });
  await t.start(c.workflow, c.id, c.input);
  const expected = await t.result(c.id);
  const effects = c.effects();
  const steps = await t.steps(c.id);
  await t.close();
  for (const [k, step] of steps.entries()) {
    c = build();
    t = await TestEngine.open({ workflows: c.workflows });
    let got: unknown;
    let error: unknown;
    let now: unknown;
    try {
      await t.start(c.workflow, c.id, c.input);
      await t.result(c.id);
      await t.crash({ cut: { id: c.id, beforeStep: k } });
      try {
        got = await t.result(c.id);
      } catch (e) {
        error = e;
      }
      now = c.effects();
    } finally {
      await t.close();
    }
    const at = `a crash after step '${step}' ran and before its result was recorded`;
    if (error !== undefined) throw new Error(`${at} failed the run: ${error instanceof Error ? error.message : String(error)}`);
    if (!same(got, expected)) throw new Error(`${at} changed the result: ${JSON.stringify(expected)} without a crash, ${JSON.stringify(got)} with one`);
    if (!same(now, effects))
      throw new Error(
        `${at} changed the outside effects: ${JSON.stringify(effects)} without a crash, ${JSON.stringify(now)} with one. ` +
          "A step that runs again must not repeat its effect: pass s.key to the service as an idempotency key",
      );
  }
}
