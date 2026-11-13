// Definitions apart from any engine: workflow, signal, query, update.
import { RejectedError } from "./errors.js";
import { Duration, WorkflowFn } from "./types.js";

// ---------------------------------------------------------------- definitions

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

/** A signal's name and payload type. A plain string works wherever a definition does. */
export interface SignalDefinition<T = unknown> {
  readonly kind: "signal";
  readonly name: string;
  /** @internal The payload type. */
  readonly __type?: T;
}
/** A query's name, argument and result types. */
export interface QueryDefinition<A = unknown, R = unknown> {
  readonly kind: "query";
  readonly name: string;
  /** @internal */
  readonly __type?: (arg: A) => R;
}
/** An update's name, argument and result types. */
export interface UpdateDefinition<A = unknown, R = unknown> {
  readonly kind: "update";
  readonly name: string;
  /** @internal */
  readonly __type?: (arg: A) => R;
}

/**
 * Defines a workflow type:
 *
 *     export const checkout = workflow("checkout", async (ctx, order: Order) => { ... });
 *     export const nightly = workflow("nightly", { version: 2, timeout: "1h" }, async (ctx) => { ... });
 */
export function workflow<I = unknown, O = unknown>(name: string, fn: WorkflowFn<I, O>): WorkflowDefinition<I, O>;
export function workflow<I = unknown, O = unknown>(
  name: string,
  opts: { version?: number; timeout?: Duration; concurrency?: Concurrency<I> },
  fn: WorkflowFn<I, O>,
): WorkflowDefinition<I, O>;
/** A definition that only names a type (another process runs its code). */
export function workflow<I = unknown, O = unknown>(name: string): WorkflowDefinition<I, O>;
export function workflow<I, O>(
  name: string,
  a?: WorkflowFn<I, O> | { version?: number; timeout?: Duration; concurrency?: Concurrency<I> },
  b?: WorkflowFn<I, O>,
): WorkflowDefinition<I, O> {
  const opts = typeof a === "object" && a !== null ? a : {};
  const fn = typeof a === "function" ? a : b;
  const version = opts.version ?? 1;
  if (!Number.isInteger(version) || version < 1) throw new RejectedError(`workflow '${name}': version is a whole number from 1, not ${version}`);
  const c = (opts as { concurrency?: Concurrency<I> }).concurrency;
  if (c !== undefined && (!Number.isInteger(c.limit) || c.limit < 1))
    throw new RejectedError(`workflow '${name}': concurrency.limit is a whole number from 1, not ${c.limit}`);
  return Object.freeze({
    kind: "workflow" as const,
    name,
    version,
    ...(opts.timeout !== undefined ? { timeout: opts.timeout } : {}),
    ...(c !== undefined ? { concurrency: Object.freeze({ ...c }) } : {}),
    ...(fn ? { fn } : {}),
  });
}
export function signal<T = unknown>(name: string): SignalDefinition<T> {
  return Object.freeze({ kind: "signal" as const, name });
}
export function query<A = unknown, R = unknown>(name: string): QueryDefinition<A, R> {
  return Object.freeze({ kind: "query" as const, name });
}
export function update<A = unknown, R = unknown>(name: string): UpdateDefinition<A, R> {
  return Object.freeze({ kind: "update" as const, name });
}

/** @internal The test kit opens engines without the dev profile's announcement. */
export const QUIET = { on: false };

/** A definition or its name. */
export const nameOf = (d: string | { readonly name: string }): string => (typeof d === "string" ? d : d.name);
