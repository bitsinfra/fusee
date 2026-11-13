// Errors and the failure envelope every SDK reads.
import { text } from "./codec.js";
import { RetryPolicy } from "./types.js";

// ---------------------------------------------------------------- errors

/** Where each error code's page lives. */
export const DOCS_BASE = "https://github.com/bitsinfra/fusee/blob/main/docs/errors.md";
const STACK_LIMIT = 4096;

/** The page for an error code. */
export const docsLink = (code: string): string => `${DOCS_BASE}#${code.toLowerCase()}`;

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

/** A recorded failure: its envelope, or old plain text read as `{ message }`. */
export function parseFailure(text: string): Failure {
  if (text.startsWith("{")) {
    try {
      const v = JSON.parse(text);
      if (v && typeof v === "object" && typeof v.message === "string") return v as Failure;
    } catch {
      /* plain text */
    }
  }
  return { message: text };
}

export function failureText(f: Failure): string {
  try {
    return JSON.stringify(f);
  } catch {
    // Details JSON cannot encode: keep the rest.
    return JSON.stringify({ ...f, details: String(f.details) });
  }
}

/** Every error Fusee throws. `code` is the same in every SDK; `docs` links to its page. */
export class FuseeError extends Error {
  /** The failure this error carries (a step's, a child's, a workflow's). */
  readonly cause?: Failure;
  readonly retryable: boolean = false;
  constructor(
    message: string,
    readonly code: string = "WorkflowFailed",
    cause?: Failure,
  ) {
    super(message);
    this.name = "FuseeError";
    if (cause !== undefined) this.cause = cause;
  }
  get docs(): string {
    return docsLink(this.code);
  }
}
export class NotFoundError extends FuseeError {
  constructor(message: string, readonly id?: string) {
    super(message, "NotFound");
    this.name = "NotFoundError";
  }
}
export class AlreadyExistsError extends FuseeError {
  constructor(message: string, readonly id?: string) {
    super(message, "AlreadyExists");
    this.name = "AlreadyExistsError";
  }
}
export class UnknownWorkflowError extends FuseeError {
  constructor(message: string) {
    super(message, "UnknownWorkflow");
    this.name = "UnknownWorkflowError";
  }
}
/**
 * A step failed after its last attempt (or with a final failure). The
 * step's own failure is `cause`: `e.cause.type`, `e.cause.code`, `e.cause.details`.
 */
export class StepError extends FuseeError {
  declare readonly cause: Failure;
  override readonly retryable: boolean;
  constructor(
    readonly step: string,
    cause: Failure | string,
  ) {
    const c = typeof cause === "string" ? parseFailure(cause) : cause;
    super(`step '${step}' failed: ${c.message}`, "StepFailed", c);
    this.name = "StepError";
    this.retryable = c.retryable !== false;
  }
}
/** A workflow, update or object handler failed; its failure is `cause`. */
export class WorkflowError extends FuseeError {
  declare readonly cause: Failure;
  constructor(cause: Failure, readonly workflowId?: string) {
    super(workflowId ? `workflow '${workflowId}' failed: ${cause.message}` : cause.message, "WorkflowFailed", cause);
    this.name = "WorkflowError";
  }
}
export class ChildError extends FuseeError {
  declare readonly cause: Failure;
  override readonly retryable: boolean;
  constructor(readonly workflowId: string, cause: Failure) {
    super(`child '${workflowId}' failed: ${cause.message}`, "ChildFailed", cause);
    this.name = "ChildError";
    this.retryable = cause.retryable !== false;
  }
}
export class CancelledError extends FuseeError {
  constructor(readonly reason: string) {
    super(`cancelled: ${reason}`, "Cancelled");
    this.name = "CancelledError";
  }
}
export class TimeoutError extends FuseeError {
  override readonly retryable = true;
  constructor(message = "timed out") {
    super(message, "TimedOut");
    this.name = "TimeoutError";
  }
}
export class TerminatedError extends FuseeError {
  constructor(message = "terminated") {
    super(message, "Terminated");
    this.name = "TerminatedError";
  }
}
export class DeterminismError extends FuseeError {
  constructor(message: string, readonly workflowId?: string) {
    super(message, "Determinism");
    this.name = "DeterminismError";
  }
}
/** A request was refused and nothing was recorded: an update's validator, a stale token, a bad argument. */
export class RejectedError extends FuseeError {
  constructor(message: string) {
    super(message.startsWith("rejected: ") ? message : `rejected: ${message}`, "Rejected");
    this.name = "RejectedError";
  }
}
export class CodecError extends FuseeError {
  constructor(message: string) {
    super(message, "Codec");
    this.name = "CodecError";
  }
}
export class ConnectionError extends FuseeError {
  override readonly retryable = true;
  constructor(message: string) {
    super(message, "Connection");
    this.name = "ConnectionError";
  }
}
export class IoError extends FuseeError {
  constructor(message: string) {
    super(message, "Io");
    this.name = "IoError";
  }
}
export class BusyError extends FuseeError {
  override readonly retryable = true;
  constructor(message = "the engine is busy: try again shortly") {
    super(message, "Busy");
    this.name = "BusyError";
  }
}
export class ClosedError extends FuseeError {
  constructor(message = "the engine is closed") {
    super(message, "Closed");
    this.name = "ClosedError";
  }
}

/**
 * Throw (or extend) to fail a step without retries:
 *
 *     class CardDeclined extends FatalError { code = "card_declined" }
 *     throw new CardDeclined("card declined", { details: { last4: "4242" } });
 *
 * Any error with `retryable = false` is final as well.
 */
export class FatalError extends Error {
  readonly retryable = false;
  code?: string;
  details?: unknown;
  constructor(message = "", opts: { code?: string; details?: unknown; cause?: unknown } = {}) {
    super(message, opts.cause === undefined ? undefined : { cause: opts.cause });
    this.name = new.target.name;
    if (opts.code !== undefined) this.code = opts.code;
    if (opts.details !== undefined) this.details = opts.details;
  }
}

/** Plain error types whose name is not recorded as the failure's type. */
const PLAIN = new Set(["Error", "FatalError"]);

/** The envelope of a thrown value: type, code, details, retryability, stack and causes. Runs on failures only. */
export function failureOf(e: unknown, stacks = true, depth = 0): Failure {
  if (e instanceof StepError) return { message: e.message, type: "StepFailed", details: { step: e.step }, ...(e.cause.retryable === false ? { retryable: false } : {}), cause: e.cause };
  if (e instanceof ChildError) return { message: e.message, type: "ChildFailed", details: { workflowId: e.workflowId }, ...(e.cause.retryable === false ? { retryable: false } : {}), cause: e.cause };
  if (e instanceof WorkflowError) return e.cause;
  if (e instanceof FuseeError) {
    if (e.code === "WorkflowFailed") return { message: e.message };
    return { message: e.message, type: e.code, ...(e.retryable ? {} : { retryable: false }) };
  }
  if (!(e instanceof Error)) return { message: typeof e === "string" ? e : JSON.stringify(e) ?? String(e) };
  const name = e.constructor?.name && e.constructor.name !== "Object" ? e.constructor.name : e.name;
  const f: Failure = { message: e.message || name };
  if (!PLAIN.has(name)) f.type = name;
  const x = e as Error & { code?: unknown; details?: unknown; retryable?: unknown };
  if (typeof x.code === "string") f.code = x.code;
  if (x.details !== undefined) f.details = x.details;
  if (typeof x.retryable === "boolean") f.retryable = x.retryable;
  if (stacks && e.stack) {
    const b = Buffer.from(e.stack);
    f.stack = b.length > STACK_LIMIT ? b.subarray(0, STACK_LIMIT).toString("utf8") : e.stack;
  }
  if ((e as { cause?: unknown }).cause !== undefined && depth < 8) f.cause = failureOf((e as { cause?: unknown }).cause, stacks, depth + 1);
  return f;
}

/** Where in `v` JSON fails (a BigInt, a cycle), as `$.a[1].b`. */
function badPath(v: unknown, path = "$", seen = new Set<unknown>()): string | null {
  if (typeof v === "bigint") return `${path} (a BigInt)`;
  if (v === null || typeof v !== "object") return null;
  if (seen.has(v)) return `${path} (a cycle)`;
  seen.add(v);
  if (typeof (v as { toJSON?: unknown }).toJSON === "function") return null;
  if (Array.isArray(v)) {
    for (let i = 0; i < v.length; i++) {
      const p = badPath(v[i], `${path}[${i}]`, seen);
      if (p) return p;
    }
    return null;
  }
  for (const [k, x] of Object.entries(v)) {
    const p = badPath(x, `${path}.${k}`, seen);
    if (p) return p;
  }
  return null;
}

/** A value JSON cannot encode: a final Codec failure naming where. */
export function codecError(what: string, v: unknown, e: unknown): CodecError {
  const where = badPath(v);
  const why = e instanceof Error ? e.message : String(e);
  return new CodecError(`${what} cannot be encoded as JSON${where ? `: ${where}` : ""} (${why})`);
}

/** The registered name closest to `name`, if it looks like a typo of it. */
export function nearest(name: string, names: string[]): string | undefined {
  const lev = (a: string, b: string): number => {
    let prev = Array.from({ length: b.length + 1 }, (_, i) => i);
    for (let i = 1; i <= a.length; i++) {
      const cur = [i];
      for (let j = 1; j <= b.length; j++) cur[j] = Math.min(prev[j]! + 1, cur[j - 1]! + 1, prev[j - 1]! + (a[i - 1] === b[j - 1] ? 0 : 1));
      prev = cur;
    }
    return prev[b.length]!;
  };
  let best: [number, string] | undefined;
  for (const n of names) {
    const d = lev(name.toLowerCase(), n.toLowerCase());
    if (d <= Math.max(1, Math.floor(n.length / 3)) && (!best || d < best[0])) best = [d, n];
  }
  return best?.[1];
}

/** Whether a thrown value is final under `policy`: never retried. */
export function isFinal(e: unknown, policy: RetryPolicy | undefined): boolean {
  const x = e as { retryable?: unknown; name?: string; constructor?: { name?: string } } | null;
  if (x && typeof x === "object" && x.retryable === false) return true;
  if (policy?.nonRetryable && e instanceof Error) {
    const name = e.constructor?.name ?? e.name;
    if (policy.nonRetryable.some((n) => (typeof n === "string" ? n === name || n === e.name : e instanceof n))) return true;
  }
  if (policy?.retryIf && !policy.retryIf(e)) return true;
  return false;
}

/** The error a caller sees for a recorded failure: a step's as StepError, a child's as ChildError, anything else as WorkflowError. */
export function errorOf(f: Failure, workflowId?: string): FuseeError {
  const d = (f.details && typeof f.details === "object" ? f.details : {}) as Record<string, unknown>;
  if (f.type === "StepFailed" && f.cause && typeof d.step === "string") return new StepError(d.step, f.cause);
  if (f.type === "ChildFailed" && f.cause && typeof d.workflowId === "string") return new ChildError(d.workflowId, f.cause);
  return new WorkflowError(f, workflowId);
}

const BY_CODE: Record<string, (m: string) => FuseeError> = {
  NotFound: (m) => new NotFoundError(m),
  AlreadyExists: (m) => new AlreadyExistsError(m),
  UnknownWorkflow: (m) => new UnknownWorkflowError(m),
  Cancelled: (m) => new CancelledError(m.replace(/^cancelled: /, "")),
  TimedOut: (m) => new TimeoutError(m),
  Terminated: (m) => new TerminatedError(m),
  Determinism: (m) => new DeterminismError(m),
  Rejected: (m) => new RejectedError(m),
  Codec: (m) => new CodecError(m),
  Connection: (m) => new ConnectionError(m),
  Io: (m) => new IoError(m),
  Busy: (m) => new BusyError(m),
  Closed: (m) => new ClosedError(m),
};

/** An error from the native module (`Code: message`) as its class. */
export function nativeError(e: unknown): FuseeError {
  if (e instanceof FuseeError) return e;
  const msg = e instanceof Error ? e.message : String(e);
  const m = /^(\w+): (.*)$/s.exec(msg);
  const code = m?.[1] ?? "Io";
  const text = m?.[2] ?? msg;
  if (code === "StepFailed" || code === "WorkflowFailed" || code === "ChildFailed" || code === "Failed") return errorOf(parseFailure(text));
  const make = BY_CODE[code];
  return make ? make(text) : new FuseeError(text, code);
}

export function call<T>(f: () => T): T {
  try {
    return f();
  } catch (e) {
    throw nativeError(e);
  }
}
