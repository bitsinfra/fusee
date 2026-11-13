import { Engine, type Duration, type JournalRecord, type Run, type StartOptions, type WorkflowDefinition } from "./index.js";
type Mock = (attempt: number) => unknown;
export declare class TestEngine {
    /** The engine under test (a new one after `crash()`). */
    engine: Engine;
    private readonly workflows;
    private readonly shards;
    /** Step functions running now (time skips only when none is). */
    private inflight;
    private readonly mocks;
    private constructor();
    private openEngine;
    static open(opts?: {
        workflows?: readonly WorkflowDefinition<any, any>[];
        shards?: number;
    }): Promise<TestEngine>;
    /** The virtual time, Unix ms. */
    now(): number;
    /** Moves the clock forward: timers that fall due fire. */
    advance(by: Duration): Promise<void>;
    /** While no step function runs, moves the clock to the next timer. */
    private skip;
    /**
     * The run's result. While every run waits on a timer, time skips to it,
     * so a workflow that sleeps a day finishes at once.
     */
    result<O = unknown>(id: string): Promise<O>;
    /**
     * Drops the engine's memory as a crash would and recovers: a new engine
     * loads every run from its journal, the code replays, and runs continue.
     * Recorded steps do not run again.
     */
    crash(opts?: {
        cut?: {
            id: string;
            beforeStep: number;
        };
    }): Promise<void>;
    /** Replaces a step's function: `fn(attempt)` returns its result, or throws to fail that attempt. */
    mockStep(wf: WorkflowDefinition<any, any> | string, step: string, fn: Mock): void;
    start<I, O>(wf: WorkflowDefinition<I, O> | string, id: string, input: I, opts?: StartOptions): Promise<Run<O>>;
    run<O = unknown>(id: string): Run<O>;
    /** The run's journal without times or run IDs, for snapshot tests. */
    snapshot(id: string): Promise<JournalRecord[]>;
    /** The names of the steps the run recorded, in order. */
    steps(id: string): Promise<string[]>;
    /**
     * Strict replay of every run against the registered code: throws
     * DeterminismError naming each run and step whose code differs from its journal.
     */
    check(): Promise<void>;
    close(): Promise<void>;
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
export declare function crashEverywhere(build: () => Case): Promise<void>;
export {};
