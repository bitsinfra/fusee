// A run's code: the context, steps, waits, children, updates, deliveries, and who asked for a change (audit).
import { AsyncLocalStorage } from "node:async_hooks";
import { randomBytes, randomUUID } from "node:crypto";
import { St, decode, encode, encoded, fromWire, ms, tagged, text, utf8, wire, wireText } from "./codec.js";
import { SignalDefinition, WorkflowDefinition, nameOf } from "./definitions.js";
import { CancelledError, CodecError, DeterminismError, FuseeError, RejectedError, StepError, TerminatedError, TimeoutError, call, codecError, errorOf, failureOf, failureText, isFinal, nativeError, parseFailure } from "./errors.js";
import { Delivery, Wire, nowMs } from "./native.js";
import { ObjectCall, StreamWriter } from "./objects.js";
import { Instruments, SpanLike } from "./telemetry.js";
import { ChildOptions, Completion, DETACHED, Duration, Pending, StepOptions, UpdateHandler, closeBits } from "./types.js";

export const delay = (t: number): Promise<void> => new Promise((r) => setTimeout(r, t));

/** The step whose function is running, across awaits. */
const stepStore = new AsyncLocalStorage<number>();

/** Who asks for the signals, cancellations, terminations and resets made here (audit). */
export const requesterStore = new AsyncLocalStorage<string>();
export const AUDITED = ["signal", "signalWithStart", "cancel", "resume", "terminate", "reset"];

/**
 * A retry time that means "until resumed": the native layer's `u64::MAX`,
 * passed as text (a number cannot hold it exactly) and read back as a
 * number at least `PAUSED_AT`.
 */
const PAUSED_ARG = "18446744073709551615";
const PAUSED_AT = 2 ** 63;

/**
 * Names who asks for the signals, cancellations, terminations and resets
 * made inside `fn`: their journal records carry it as `by` (audit). The
 * HTTP API names its token or the proxy's signed-in user the same way.
 *
 * ```ts
 * await asRequester("job:nightly", () => engine.run("order-1").cancel("expired"));
 * ```
 */
export function asRequester<T>(by: string, fn: () => T): T {
  return requesterStore.run(by, fn);
}
/**
 * `FUSEE_STEP_CONTEXT=lean` (off by default): a step's
 * function runs inside `stepStore` only if it is an `async` function or
 * code has called `ctx.key()` or `ctx.heartbeat()` before; otherwise only
 * its synchronous part knows its step. Saves about 150 ns a step, but a
 * plain function that returns a promise and calls `ctx.key()` in a
 * continuation, before anything called it, gets the workflow's key there.
 */
const LEAN_CONTEXT = process.env.FUSEE_STEP_CONTEXT === "lean";
const AsyncFunction = (async () => {}).constructor;
/** Some code has asked which step it runs in (see `LEAN_CONTEXT`). */
let stepAsked = false;

// ---------------------------------------------------------------- execution

/** Where a run's journal operations go: the engine or a replayer. */
export interface Backend {
  /** Outputs of `beginStep`: `[seq, replay, gate, attempt, notBefore]`. */
  readonly out: Float64Array;
  /** 2 replay, 1 run now (the gate is open), 0 wait. */
  beginStep(shard: number, run: number, name: string, readOnly: boolean): number;
  stepGate(shard: number, run: number, gate: number, notBefore: number): boolean;
  /** Whether a gate is open by what the last tick reported, without a call. */
  gateOpen?(shard: number, gate: number, notBefore: number): boolean;
  finishStep(shard: number, run: number, seq: number, ok: boolean, value: string | Uint8Array): void;
  /** `finishStep`, then `quiescent`: one native call where a run takes its delivery at once. */
  finishStepDeliver?(shard: number, run: number, seq: number, ok: boolean, value: string): Delivery | null;
  call(shard: number, op: string, run: number, args: (string | number)[], bufs?: Buffer[]): number[];
  bytes(shard: number, op: string, run: number, args: (string | number)[], bufs?: Buffer[]): Buffer | null;
  /** `ctx.now()` as a number, without the buffers of `bytes("value")` (absent: that path). */
  valueNow?(shard: number, run: number, liveMs: number): number;
  /** `ctx.sleep`'s sequence number, without `call("beginSleep")`'s strings (absent: that path). */
  beginSleep?(shard: number, run: number, name: string, atMs: number): number;
  scheduleRun?(execution: RunExec): void;
  /** The engine's tick count (see `deliverNow`); absent on replay. */
  turn?(): number;
  /** Whether a run whose code is blocked may take a delivery now (the fairness budget). */
  deliverNow?(): boolean;
  quiescent(shard: number, run: number): Delivery | null;
  decideUpdate(shard: number, token: number, ok: boolean, message: string): number;
  finish(shard: number, run: number, status: number, value: string | Uint8Array): void;
  stuck(shard: number, run: number, message: string): void;
  /** `full` false: only a delivery needs the next turn (see `Engine.kick`). */
  kick(full?: boolean): void;
}

/**
 * The shared, always-empty map a run's optional maps start as: reads and
 * deletes work on it, and the first `set` goes through `own`, which gives
 * the run a map of its own. A woken run that only replays to its end
 * allocates none of them.
 */
const NO_ENTRIES: Map<any, any> = new Map();

function own<K, V>(m: Map<K, V>): Map<K, V> {
  return m === NO_ENTRIES ? new Map<K, V>() : m;
}

export class RunExec {
  readonly pending = new Map<number, Pending>();
  gated: Map<number, () => void> = NO_ENTRIES;
  beats: Map<number, number> = NO_ENTRIES;
  queries: Map<string, (arg: unknown) => unknown> = NO_ENTRIES;
  updates: Map<string, UpdateHandler<unknown, unknown>> = NO_ENTRIES;
  /** JSON of step results this code recorded and not yet had delivered. */
  held: Map<number, string> = NO_ENTRIES;
  current: number | undefined;
  pumping = false;
  ended = false;
  /** The tick in which something last resumed this run's code (see `deliverNow`). */
  resumedAt = -1;
  readonly ctx: Ctx;
  /** @internal */
  tel: Instruments | undefined;
  /** The code waits with nothing pending by design (an object, continue-as-new). */
  idle = false;
  /** The run's `fusee.workflow` span and the context it is active in (telemetry). */
  wfSpan: SpanLike | undefined;
  wfCtx: unknown;

  constructor(
    readonly be: Backend,
    readonly shard: number,
    readonly run: number,
    readonly id: string,
    readonly wtype: string,
    readonly version: number,
    readonly startedMs: number,
    ctxClass: new (x: RunExec) => Ctx = Ctx,
  ) {
    this.ctx = new ctxClass(this);
  }

  /** Failures record their stack (OpenOptions.recordStacks). */
  recordStacks = true;
  /** What a step does when its retries run out, unless its policy says. */
  onExhausted: "fail" | "pause" = "fail";
  /** Where the engine's HTTP API is reached (completion URLs), if it serves one. */
  callbackBase: (() => string | undefined) | undefined;

  /** Each running step's abort controller, made when its `s.signal` is read. */
  aborts: Map<number, AbortController> | undefined;

  /** Aborts the signals of running steps (the run was cancelled, ended or closed). */
  abortSteps(reason: Error): void {
    if (!this.aborts) return;
    for (const c of this.aborts.values()) c.abort(reason);
    this.aborts.clear();
  }

  /** Compensations, newest last (ctx.compensate); made on first use. */
  undo: { name: string; fn: () => unknown; opts: StepOptions }[] | undefined;

  h(op: string, args: (string | number)[] = [], bufs: Buffer[] = []): number[] {
    if (this.ended && op !== "release") throw new FuseeError("workflow execution has ended");
    return call(() => this.be.call(this.shard, op, this.run, args, bufs));
  }

  hb(op: string, args: (string | number)[] = [], bufs: Buffer[] = []): Buffer | null {
    if (this.ended) throw new FuseeError("workflow execution has ended");
    return call(() => this.be.bytes(this.shard, op, this.run, args, bufs));
  }

  /**
   * Something changed: re-check gates, then deliver. A wake arrives in a
   * tick, its own macrotask, so unless something resumed this run's code in
   * this tick already, the code is blocked and can take a delivery at once
   * (see `deliverSoon`): resuming after a signal costs no extra turn.
   */
  wake(): void {
    if (this.gated.size) for (const [, retry] of [...this.gated]) retry();
    this.deliverSoon();
  }

  schedulePump(): void {
    if (this.pumping || this.ended) return;
    this.pumping = true;
    // One delivery per macrotask: every microtask the previous delivery
    // started runs first, exactly as on replay.
    if (this.be.scheduleRun) this.be.scheduleRun(this);
    else setImmediate(() => this.pumpScheduled());
  }

  pumpScheduled(): void {
    this.pumping = false;
    // Something in this tick already resumed the code (a wake delivered at
    // once): its microtasks have not run yet, so wait another turn.
    if (this.be.turn && this.resumedAt === this.be.turn()) this.schedulePump();
    else this.pump();
  }

  /** Workflow code may run from here on (a start, a delivery, a rejection). */
  resumed(): void {
    this.resumedAt = this.be.turn?.() ?? -1;
  }

  /**
   * Deliver at completion. Replay hands a run its recorded results one at
   * a time, and between two deliveries the run's code runs until it is
   * blocked: every microtask the previous delivery started has run first.
   * Live delivery matches that by waiting a macrotask before each delivery
   * (`schedulePump`), because JavaScript cannot see whether its microtask
   * queue has drained.
   *
   * The wait is not needed when nothing has resumed the run's code since
   * the engine's current tick began (`resumedAt` is from an earlier tick).
   * Each tick runs as its own macrotask (on Bun, native wakes defer to one,
   * see the constructor), so whatever resumed the run before it (a start,
   * a delivery, a rejection) has finished its microtasks, and the code is
   * blocked on ctx operations. Only the SDK resumes it, and each of those
   * sets `resumedAt`. So a step that settles now (its function ran in this
   * tick, or it was asynchronous) is delivered in the same microtask, and
   * the run sees exactly what a delivery in the next macrotask would show
   * it: `quiescent` picks the same next item and appends the same record.
   * Once that delivery resumes the run, a second completion in the same
   * tick waits for the next macrotask: the continuation may still
   * have microtasks queued, which would run after the second delivery's and
   * not before it as on replay.
   *
   * Fairness: a run takes at most one such delivery per tick, so a chain
   * yields to libuv at every tick. Across runs, the engine stops after a
   * budget per tick (`INLINE_PER_TICK` deliveries or `INLINE_TICK_MS`
   * since the tick began) and the rest take the macrotask path.
   */
  deliverSoon(): void {
    if (this.mayDeliverNow()) this.pump();
    else this.schedulePump();
  }

  /** Whether a delivery may be made at once (see `deliverSoon`); takes from the tick's budget. */
  mayDeliverNow(): boolean {
    return !this.ended && this.be.turn !== undefined && this.resumedAt !== this.be.turn() && this.be.deliverNow!();
  }

  pump(): void {
    if (this.ended) return;
    let d: Delivery | null;
    try {
      d = this.be.quiescent(this.shard, this.run);
    } catch (e) {
      this.fail(nativeError(e));
      return;
    }
    this.take(d);
  }

  /** Makes a delivery `quiescent` (or `finishStepDeliver`) returned. */
  take(d: Delivery | null): void {
    // The record a delivery appended waits for a full tick (or the next
    // gate): a burst of deliveries shares one flush.
    this.be.kick(false);
    if (!d) {
      // Nothing to deliver, every microtask has run, and the code awaits no
      // ctx operation yet has not returned: it awaits something else (a
      // timer, I/O, a promise nothing here settles), which a replay could
      // not give it. One comparison per turn.
      if (this.pending.size === 0 && !this.ended && !this.idle) this.awaitedElsewhere();
      return;
    }
    this.resumed();
    if (d.kind === "resolve") {
      let payload = d.payload;
      const held = this.held.get(d.seq);
      if (held !== undefined) this.held.delete(d.seq);
      if (d.held) {
        if (held === undefined) {
          this.fail(new FuseeError(`the result of step ${d.seq} was not kept`));
          return;
        }
        payload = held;
      }
      this.resolve(d.seq, d.status, payload);
    } else if (d.kind === "cancel") {
      const reason = wireText(d.payload);
      for (const s of d.seqs ?? []) {
        const p = this.pending.get(s);
        this.pending.delete(s);
        this.gated.delete(s);
        this.held.delete(s);
        p?.reject(new CancelledError(reason));
      }
    } else {
      this.startUpdate(d);
    }
    this.schedulePump();
  }

  /** The code awaits something outside ctx (see `take`): the run stops, as on a determinism error. */
  awaitedElsewhere(): void {
    this.ended = true;
    const msg =
      `workflow code may only await ctx operations (step, sleep, waitFor, child, all, race); ` +
      `workflow '${this.id}' awaited something else (a timer, I/O, or a promise no ctx operation settles). ` +
      `Put other awaits inside a step: await ctx.step(name, fn)`;
    call(() => this.be.stuck(this.shard, this.run, msg));
    this.abortSteps(new DeterminismError(msg));
  }

  /** An update reached its turn: validate a new request, then start its handler. */
  startUpdate(d: Delivery): void {
    const name = d.name ?? "";
    const handler = this.updates.get(name);
    const arg = fromWire<unknown>(d.payload);
    let ordinal = d.ordinal ?? 0;
    if (d.token !== undefined && d.token >= 0) {
      let refusal: string | undefined;
      if (!handler) refusal = `no handler for update '${name}'`;
      else if (handler.validate) {
        try {
          const v = handler.validate(arg);
          if (v === false) refusal = "refused by the validator";
          else if (typeof v === "string") refusal = v;
        } catch (e) {
          refusal = e instanceof Error ? e.message : String(e);
        }
      }
      const o = call(() => this.be.decideUpdate(this.shard, d.token!, refusal === undefined, refusal ?? ""));
      if (refusal !== undefined || o < 0) return;
      ordinal = o;
    } else if (!handler) {
      this.ended = true;
      const msg = `determinism error: the journal delivers update '${name}' (id '${d.id}') but the code registered no handler for it`;
      call(() => this.be.stuck(this.shard, this.run, msg));
      this.fail(new DeterminismError(msg));
      return;
    }
    let p: Promise<unknown>;
    try {
      p = Promise.resolve(handler!.handle(arg));
    } catch (e) {
      p = Promise.reject(e);
    }
    p.then(
      (v) => {
        if (this.ended) return;
        const [status, data] = encoded(v, "update result");
        this.h("updateDone", [ordinal, status], [tagged(data)]);
        this.be.kick();
      },
      (e) => {
        if (this.ended) return;
        const [status, msg] = outcomeOf(e, this.recordStacks);
        this.h("updateDone", [ordinal, status], [utf8(msg)]);
        this.be.kick();
      },
    );
  }

  resolve(seq: number, status: number, payload: Wire): void {
    const p = this.pending.get(seq);
    if (!p) return;
    this.pending.delete(seq);
    switch (status) {
      case St.Ok: {
        let v: unknown;
        try {
          v = p.kind === "sleep" ? undefined : fromWire(payload);
        } catch (e) {
          p.reject(new CodecError(`a recorded result is not JSON: ${e instanceof Error ? e.message : String(e)}`));
          break;
        }
        p.resolve(v);
        break;
      }
      case St.Failed: {
        const f = parseFailure(wireText(payload));
        p.reject(p.kind === "step" || p.kind === "await" ? new StepError(p.name, f) : errorOf(f));
        break;
      }
      case St.Cancelled: {
        const e = new CancelledError(wireText(payload));
        this.aborts?.get(seq)?.abort(e);
        p.reject(e);
        break;
      }
      case St.TimedOut:
        if (p.kind === "wait") p.resolve(undefined);
        else p.reject(new TimeoutError(wireText(payload) || "timed out"));
        break;
      default:
        p.reject(new TerminatedError(wireText(payload)));
    }
  }

  fail(e: FuseeError): void {
    if (this.pending.size) this.resumed();
    for (const [, p] of this.pending) p.reject(e);
    this.pending.clear();
  }

  op<T>(kind: Pending["kind"], name: string, seq: number): Promise<T> {
    const p = new Promise<T>((resolve, reject) => {
      this.pending.set(seq, { kind, name, resolve: resolve as (v: unknown) => void, reject });
    });
    seqOf.set(p, seq);
    return p;
  }

  /** The code dropped an operation (the losing side of a race). */
  forget(seq: number): void {
    if (!this.pending.has(seq)) return;
    this.pending.delete(seq);
    this.gated.delete(seq);
    this.held.delete(seq);
    this.h("forget", [seq]);
  }

  /**
   * Runs a step's function (or an async completion's send function) under
   * its gate, permit, timeouts and retry policy. `done(true, prepare(v))`
   * records the result; `done(false, msg)` the final failure. When
   * `prepare` throws (a step result JSON cannot encode), the attempt fails
   * like one whose function threw. `open`: what `beginStep` found for the
   * first attempt's gate (true open, false shut and registered for a wake),
   * or undefined when nobody asked yet.
   */
  runAttempts(
    seq: number,
    gate: number,
    attempt: number,
    notBefore: number,
    fn: (attempt: number) => unknown,
    opts: StepOptions,
    /**
     * Records the outcome. `now`: the run takes its next delivery at once;
     * a callback that made it with the record returns it (or null for
     * none), otherwise undefined.
     */
    done: (ok: boolean, v: unknown, now: boolean) => Delivery | null | undefined | void,
    prepare: (v: unknown) => unknown = (v) => v,
    name = "",
    open?: boolean,
  ): void {
    const attempts = Math.max(1, opts.retry?.attempts ?? 3);
    const initial = ms(opts.retry?.initial) ?? 100;
    const max = ms(opts.retry?.max) ?? 10_000;
    const factor = opts.retry?.backoff === "fixed" ? 1 : (opts.retry?.factor ?? 2);
    const jitter = opts.retry?.jitter !== false;
    const maxElapsed = ms(opts.retry?.maxElapsed);
    // When the step first failed (read on the failure path only).
    let firstFailure = 0;
    const timeout = ms(opts.timeout);
    const hbTimeout = ms(opts.heartbeatTimeout);
    // The native acquire's arguments after the ticket: the step's place
    // among the limit's waiters (none: arrival order).
    const place: readonly (string | number)[] =
      opts.limit === undefined ||
      (opts.priority === undefined && opts.fairnessKey === undefined && opts.fairnessWeight === undefined)
        ? NO_PLACE
        : [opts.priority ?? 0, opts.fairnessKey ?? "", opts.fairnessWeight ?? 0];
    let ticket: number | undefined;
    // A retry timer and a wake can both call tryRun: only one attempt runs.
    let running = false;
    let settled = false;
    // The one pending timer that calls tryRun again: the retry delay, or the
    // rate limit's refill.
    let later: ReturnType<typeof setTimeout> | undefined;
    const cancelLater = (): void => {
      if (later === undefined) return;
      clearTimeout(later);
      later = undefined;
    };
    const retryAt = (at: number): void => {
      cancelLater();
      later = setTimeout(() => {
        later = undefined;
        tryRun();
      }, Math.max(0, at - nowMs()));
    };
    const tryRun = (): void => {
      if (running || settled) return;
      if (this.ended || !this.pending.has(seq)) {
        this.gated.delete(seq);
        cancelLater();
        if (ticket !== undefined && opts.limit) this.h("release", [opts.limit, ticket]);
        return;
      }
      // A paused step waits for its resume, which wakes the run. What
      // `beginStep` found for its gate is stale by then: ask again.
      if (notBefore >= PAUSED_AT) {
        open = undefined;
        if (this.h("stepPaused", [seq])[0]) {
          (this.gated = own(this.gated)).set(seq, tryRun);
          return;
        }
        // Resumed: a fresh set of attempts, counted from here.
        notBefore = 0;
        attempt = 1;
        firstFailure = 0;
      }
      // A wake during the retry delay cannot let the attempt run, and asking
      // the gate again would add a native timer each time: the delay's timer
      // calls back.
      if (later !== undefined && nowMs() < notBefore) return;
      // A read-only step waits for no earlier record (gate 0 is always durable).
      const g = opts.readOnly ? 0 : gate;
      const known = open;
      open = undefined;
      if (
        known === undefined
          ? !this.be.gateOpen?.(this.shard, g, notBefore) && !call(() => this.be.stepGate(this.shard, this.run, g, notBefore))
          : !known
      ) {
        (this.gated = own(this.gated)).set(seq, tryRun);
        if (notBefore > nowMs()) retryAt(notBefore);
        return;
      }
      if (opts.limit) {
        if (ticket === undefined) ticket = this.h("limitTicket")[0] ?? 0;
        const a = this.h("acquire", [opts.limit, ticket, ...place])[0] ?? 0;
        if (a !== 0) {
          // Woken when the permit may be ours (or at the rate limit's refill).
          (this.gated = own(this.gated)).set(seq, tryRun);
          if (a > 0) retryAt(a);
          return;
        }
      }
      this.gated.delete(seq);
      cancelLater();
      running = true;
      this.current = seq;
      // Only a heartbeat timeout reads when the attempt began.
      if (hbTimeout !== undefined) (this.beats = own(this.beats)).set(seq, nowMs());
      const span = this.tel?.attempt(this, name, attempt);
      let r: unknown;
      try {
        const scoped = !LEAN_CONTEXT || stepAsked || fn instanceof AsyncFunction;
        r = span
          ? span.run(() => (scoped ? stepStore.run(seq, fn, attempt) : fn(attempt)))
          : scoped
            ? stepStore.run(seq, fn, attempt)
            : fn(attempt);
      } catch (e) {
        r = Promise.reject(e);
      } finally {
        this.current = undefined;
      }
      const timers: NodeJS.Timeout[] = [];
      // A synchronous result with no timer to race settles in the next
      // microtask, where `Promise.resolve(r).then` would settle it, without
      // the promise.
      const lean =
        timeout === undefined &&
        hbTimeout === undefined &&
        (r === null || (typeof r !== "object" && typeof r !== "function") || typeof (r as { then?: unknown }).then !== "function");
      const races: Promise<unknown>[] = lean ? NO_RACES : [Promise.resolve(r)];
      if (timeout !== undefined) {
        races.push(
          new Promise((_, rej) =>
            timers.push(
              setTimeout(() => {
                const e = new TimeoutError(`step timed out after ${timeout} ms`);
                this.aborts?.get(seq)?.abort(e);
                rej(e);
              }, timeout),
            ),
          ),
        );
      }
      if (hbTimeout !== undefined) {
        races.push(
          new Promise((_, rej) => {
            const check = (): void => {
              const last = this.beats.get(seq) ?? 0;
              const left = last + hbTimeout - nowMs();
              if (left <= 0) {
                const e = new TimeoutError(`step sent no heartbeat for ${hbTimeout} ms`);
                this.aborts?.get(seq)?.abort(e);
                rej(e);
              }
              else timers.push(setTimeout(check, left));
            };
            timers.push(setTimeout(check, hbTimeout));
          }),
        );
      }
      const finished = (): void => {
        running = false;
        for (const t of timers) clearTimeout(t);
        this.beats.delete(seq);
        this.aborts?.delete(seq);
        if (ticket !== undefined && opts.limit) {
          this.h("release", [opts.limit, ticket]);
          ticket = undefined;
        }
      };
      const record = (ok: boolean, v: unknown): void => {
        settled = true;
        // Decided before the record, as `deliverSoon` would decide after
        // it: nothing in between resumes the run's code.
        const now = this.mayDeliverNow();
        let d: Delivery | null | undefined | void;
        try {
          d = done(ok, v, now);
        } catch (err) {
          this.fail(err instanceof FuseeError ? err : nativeError(err));
          return;
        }
        if (d !== undefined) this.take(d);
        else if (now) this.pump();
        else this.schedulePump();
        this.be.kick();
      };
      const failed = (e: unknown): void => {
        // The failure as its envelope: type, code, details, stack, causes.
        const msg = failureText(failureOf(e, this.recordStacks));
        const now = nowMs();
        if (!firstFailure) firstFailure = now;
        const base = Math.min(max, initial * factor ** (attempt - 1));
        const delay = jitter ? base / 2 + Math.random() * (base / 2) : base;
        const inTime = maxElapsed === undefined || now + delay - firstFailure <= maxElapsed;
        const final = isFinal(e, opts.retry);
        if (attempt < attempts && inTime && !final) {
          notBefore = now + Math.round(delay);
          try {
            gate = this.h("stepRetry", [seq, notBefore], [utf8(msg)])[0] ?? 0;
          } catch (err) {
            this.fail(nativeError(err));
            return;
          }
          attempt += 1;
          this.be.kick();
          tryRun();
        } else if (!final && (opts.retry?.onExhausted ?? this.onExhausted) === "pause") {
          // Out of retries: the run waits for a resume instead of failing.
          try {
            gate = this.h("stepRetry", [seq, PAUSED_ARG], [utf8(msg)])[0] ?? 0;
          } catch (err) {
            this.fail(nativeError(err));
            return;
          }
          notBefore = PAUSED_AT;
          attempt += 1;
          this.be.kick();
          tryRun();
        } else {
          record(false, msg);
        }
      };
      const succeeded = (v: unknown): void => {
        finished();
        if (this.ended) {
          span?.end();
          return;
        }
        let out: unknown;
        try {
          out = prepare(v);
        } catch (e) {
          span?.end(e instanceof Error ? e.message : String(e));
          // A result JSON cannot encode fails the same way every time: final.
          failed(codecError(`step '${name}''s result`, v, e));
          return;
        }
        span?.end();
        record(true, out);
      };
      if (lean) {
        queueMicrotask(() => succeeded(r));
        return;
      }
      (races.length === 1 ? races[0]! : Promise.race(races)).then(
        succeeded,
        (e) => {
          finished();
          span?.end(e instanceof Error ? e.message : String(e));
          if (this.ended) return;
          failed(e);
        },
      );
    };
    tryRun();
  }
}

const seqOf = new WeakMap<Promise<unknown>, number>();

/** A lean attempt's races: none, and nothing is added to them. */
const NO_RACES: Promise<unknown>[] = [];

/** A step without a priority or fairness key: plain arrival order at its limit. */
const NO_PLACE: readonly (string | number)[] = Object.freeze([]);

/** Throws for a priority or fairness weight a limit's queue cannot take. */
function checkPlace(o: StepOptions): void {
  const p = o.priority;
  const w = o.fairnessWeight;
  if (p !== undefined && !(Number.isInteger(p) && p >= -(2 ** 31) && p < 2 ** 31))
    throw new RejectedError(`priority must be a 32-bit integer, not ${p}`);
  if (w !== undefined && !(Number.isInteger(w) && w >= 0 && w < 2 ** 32))
    throw new RejectedError(`fairnessWeight must be a whole number, not ${w}`);
}

export function outcomeOf(e: unknown, stacks = true): [number, string] {
  return e instanceof CancelledError
    ? [St.Cancelled, e.reason]
    : e instanceof TimeoutError
      ? [St.TimedOut, e.message]
      : e instanceof TerminatedError
        ? [St.Terminated, e.message]
        : [St.Failed, failureText(failureOf(e, stacks))];
}

/**
 * What a step's function gets when it declares a parameter:
 * `ctx.step("charge", (s) => pay({ idempotencyKey: s.key, signal: s.signal }))`.
 */
export class StepCtx {
  /** @internal */
  constructor(
    private readonly x: RunExec,
    private readonly seq: number,
    /** The step's name. */
    readonly name: string,
    /** 1 on the first attempt, then 2, 3, ... */
    readonly attempt: number,
  ) {}

  /**
   * A key stable across retries and recovery, distinct for each step of
   * each run: pass it to services that accept idempotency keys.
   */
  get key(): string {
    return text(this.x.hb("key", [this.seq]) ?? undefined);
  }

  get workflowId(): string {
    return this.x.id;
  }

  /**
   * Aborted when this attempt times out (or misses its heartbeat), or the
   * run is cancelled, terminated or closed. Pass it to `fetch` and the like.
   */
  get signal(): AbortSignal {
    let c = this.x.aborts?.get(this.seq);
    if (!c) {
      c = new AbortController();
      if (this.x.ended) c.abort(new CancelledError("the run ended"));
      (this.x.aborts ??= new Map()).set(this.seq, c);
    }
    return c.signal;
  }

  /** Reports progress; resets the heartbeat timeout. Coalesced; never waits for the disk. */
  heartbeat<T>(progress?: T): void {
    (this.x.beats = own(this.x.beats)).set(this.seq, nowMs());
    this.x.h("heartbeat", [this.seq], [encode(progress ?? null)]);
    this.x.be.kick();
  }

  /** The latest progress this step reported, also from an attempt before a crash. */
  progress<T>(): T | undefined {
    const b = this.x.hb("progress", [this.seq]);
    return b ? decode<T>(b) : undefined;
  }

  /** Opens this step's stream `name` (see `ctx.stream`). */
  stream(name: string): StreamWriter {
    const [recorded = 0, closed = 0] = this.x.h("streamOpen", [name, this.seq]);
    return new StreamWriter(this.x, name, this.seq, recorded, closed === 1, () => this.seq);
  }
}

/** The workflow context. */
export class Ctx {
  /** @internal */
  constructor(private readonly x: RunExec) {}

  get workflowId(): string {
    return this.x.id;
  }
  get workflowType(): string {
    return this.x.wtype;
  }
  get version(): number {
    return this.x.version;
  }
  get runId(): number {
    return this.x.run;
  }

  private stepSeq(): number | undefined {
    stepAsked = true;
    return stepStore.getStore() ?? this.x.current;
  }

  /**
   * A key stable across retries and recovery for the step whose function is
   * running (distinct for each run of a workflow ID, including a reset one).
   * Pass it to services that accept idempotency keys.
   */
  key(): string {
    return text(this.x.hb("key", [this.stepSeq() ?? 0xffffffff]) ?? undefined);
  }

  /**
   * Runs `fn` as a durable step. `fn` runs only once everything this run
   * recorded before is durable (at once with `readOnly`); its result is
   * recorded and returned without waiting for the disk. On replay the
   * recorded result is returned and `fn` does not run.
   */
  step<T>(name: string, fn: ((s: StepCtx) => T | Promise<T>) | (() => T | Promise<T>), opts: StepOptions = {}): Promise<T> {
    const x = this.x;
    if (x.ended) throw new FuseeError("workflow execution has ended");
    if (opts.priority !== undefined || opts.fairnessWeight !== undefined) checkPlace(opts);
    const be = x.be;
    const go = call(() => be.beginStep(x.shard, x.run, name, opts.readOnly === true));
    const o = be.out;
    const seq = o[0]!;
    const p = x.op<T>("step", name, seq);
    if (go !== 2) {
      // A function that declares a parameter gets the step context (made
      // only then).
      const run: (attempt: number) => unknown =
        fn.length > 0
          ? (attempt) => (fn as (s: StepCtx) => unknown)(new StepCtx(x, seq, name, attempt))
          : (fn as () => unknown);
      x.runAttempts(
        seq,
        o[2]!,
        o[3]!,
        o[4]!,
        run,
        opts,
        (ok, v, now) => {
          // The code keeps the JSON it recorded: the delivery does not carry it back.
          if (ok && typeof v === "string") (x.held = own(x.held)).set(seq, v);
          const value = ok ? (v as string | Uint8Array) : String(v);
          // Recorded and delivered in one native call.
          if (now && typeof value === "string" && be.finishStepDeliver)
            return call(() => be.finishStepDeliver!(x.shard, x.run, seq, ok, value));
          call(() => be.finishStep(x.shard, x.run, seq, ok, value));
          return undefined;
        },
        wire,
        name,
        // 0: the gate is shut, and the engine wakes the run when it opens.
        go === 1,
      );
    }
    be.kick();
    return p;
  }

  /**
   * A step completed from outside: `send(token)` starts work elsewhere and
   * returns; the workflow waits until a process holding the token calls
   * `engine.complete(token, value)` or `engine.fail(token, error)`. `send`
   * is retried under the retry policy, each attempt with a new token.
   */
  awaitCompletion<T>(name: string, send: (completion: Completion) => unknown, opts: StepOptions = {}): Promise<T> {
    const x = this.x;
    if (opts.priority !== undefined || opts.fairnessWeight !== undefined) checkPlace(opts);
    const [seq = 0, kind = 0, gate = 0, attempt = 1, notBefore = 0] = x.h("beginAwait", [name]);
    const p = x.op<T>("await", name, seq);
    if (kind === 0) {
      x.runAttempts(
        seq,
        gate,
        attempt,
        notBefore,
        () => {
          const token = text(x.hb("token", [seq]) ?? undefined);
          const base = x.callbackBase?.();
          return send({ token, ...(base ? { url: `${base}/v1/complete/${encodeURIComponent(token)}` } : {}) });
        },
        // Only steps pause; a completion's send fails when out of retries.
        { ...opts, readOnly: false, retry: { ...opts.retry, onExhausted: "fail" } },
        (ok, v) => {
          if (ok) x.h("awaiting", [seq]);
          else x.h("stepDone", [seq, "0"], [utf8(String(v))]);
        },
        undefined,
        name,
      );
    }
    x.be.kick();
    return p;
  }

  /** Progress of the running step. Coalesced; never waits for the disk. */
  heartbeat<T>(progress: T): void {
    const seq = this.stepSeq();
    if (seq === undefined) throw new FuseeError("heartbeat called outside a step");
    (this.x.beats = own(this.x.beats)).set(seq, nowMs());
    this.x.h("heartbeat", [seq], [encode(progress)]);
    this.x.be.kick();
  }

  /** The latest progress the running step reported, also from before a crash. */
  progress<T>(): T | undefined {
    const seq = this.stepSeq();
    if (seq === undefined) return undefined;
    const b = this.x.hb("progress", [seq]);
    return b ? decode<T>(b) : undefined;
  }

  /**
   * Opens the run's stream `name` for appending (readers follow it with
   * `run.stream(name)`). In workflow code it opens a stream of the run: its
   * appends are recorded once (a replay appends nothing), and `close()` or
   * the run's end completes it; steps may append to it too. Inside a step's
   * function it opens a stream of the step, which the step's result
   * completes; an attempt that runs again finds what earlier attempts
   * recorded in `recorded()` and continues after it.
   */
  stream(name: string): StreamWriter {
    const step = this.stepSeq();
    const [recorded = 0, closed = 0] = this.x.h("streamOpen", [name, step ?? "w"]);
    return new StreamWriter(this.x, name, step, recorded, closed === 1, () => this.stepSeq());
  }

  /** Sleeps durably; the run is parked out of memory while it waits. */
  sleep(name: string, duration: Duration): Promise<void> {
    const x = this.x;
    const at = nowMs() + (ms(duration) ?? 0);
    const be = x.be;
    let seq: number;
    if (be.beginSleep) {
      if (x.ended) throw new FuseeError("workflow execution has ended");
      seq = call(() => be.beginSleep!(x.shard, x.run, name, at));
    } else {
      [seq = 0] = x.h("beginSleep", [name, at]);
    }
    be.kick();
    return x.op<void>("sleep", name, seq);
  }

  /** Waits for the next signal `name`. Resolves `undefined` on timeout. */
  waitFor<T>(sig: SignalDefinition<T> | string, opts: { timeout?: Duration } = {}): Promise<T | undefined> {
    const name = nameOf(sig);
    const x = this.x;
    const t = ms(opts.timeout);
    const deadline = t === undefined ? 0 : nowMs() + t;
    const [seq = 0] = x.h("beginWait", [name, deadline]);
    x.be.kick();
    return x.op<T | undefined>("wait", name, seq);
  }

  /** Starts a child workflow and waits for its result. */
  child<O, I = unknown>(wf: WorkflowDefinition<I, O> | string, id: string, input: I, opts: ChildOptions = {}): Promise<O> {
    const wtype = nameOf(wf);
    const x = this.x;
    const t = ms(opts.timeout);
    const [seq = 0] = x.h("beginChild", [wtype, id, closeBits(opts.parentClose), t === undefined ? 0 : nowMs() + t], [
      encode(input),
    ]);
    x.be.kick();
    return x.op<O>("child", wtype, seq);
  }

  /**
   * Starts a child workflow without waiting for it. By default it is
   * terminated when this workflow closes; `parentClose: "abandon"` lets it
   * outlive it.
   */
  startChild<I>(wf: WorkflowDefinition<I, unknown> | string, id: string, input: I, opts: ChildOptions = {}): void {
    this.spawnChild(nameOf(wf), id, input, opts);
  }

  /** @deprecated Use `startChild`. */
  spawnChild<I>(wtype: string, id: string, input: I, opts: ChildOptions = {}): void {
    const x = this.x;
    const t = ms(opts.timeout);
    x.h("beginChild", [wtype, id, closeBits(opts.parentClose) | DETACHED, t === undefined ? 0 : nowMs() + t], [
      encode(input),
    ]);
    x.be.kick();
  }

  /** Sends a signal to another workflow once this run is durable through here. */
  signal<T>(target: string, sig: SignalDefinition<T> | string, value: T): void {
    this.x.h("send", [target, nameOf(sig)], [encode(value)]);
    this.x.be.kick();
  }

  /** `signal` under the name every SDK uses. */
  signalWorkflow<T>(target: string, sig: SignalDefinition<T> | string, value: T): void {
    this.signal(target, sig, value);
  }

  /** Sleeps durably until `at`; the run is parked out of memory while it waits. */
  sleepUntil(name: string, at: Date | number): Promise<void> {
    const x = this.x;
    const [seq = 0] = x.h("beginSleep", [name, +at]);
    x.be.kick();
    return x.op<void>("sleep", name, seq);
  }

  /**
   * Registers a step that undoes what the workflow did so far: if the
   * workflow fails or is cancelled, every registered compensation runs as a
   * step named `name`, newest first, then the failure is thrown. Register it
   * after the step it undoes succeeded:
   *
   *     const hold = await ctx.step("reserve", () => inventory.reserve(sku));
   *     ctx.compensate("release", () => inventory.release(hold));
   */
  compensate(name: string, fn: () => unknown, opts: StepOptions = {}): void {
    (this.x.undo ??= []).push({ name, fn, opts });
  }

  /** Asks another workflow to cancel, once this point is durable. */
  cancelWorkflow(target: string, reason = "cancelled"): void {
    this.x.h("cancelWorkflow", [target, reason]);
    this.x.be.kick();
  }

  /**
   * Key `key` of object type `type`, called from this workflow or handler:
   * each call is a journal operation, sent through the durable outbox and
   * run exactly once; `call` resolves with the handler's recorded result.
   */
  object(type: string, key: string): ObjectCall {
    return new ObjectCall(this.x, type, key);
  }

  /** Registers a read-only query handler (`run.query(name, arg)`). */
  onQuery<A, R>(name: string, fn: (arg: A) => R): void {
    (this.x.queries = own(this.x.queries)).set(name, fn as (arg: unknown) => unknown);
  }

  /** Registers an update handler (`run.update(name, arg)`). */
  onUpdate<A, R>(name: string, handler: UpdateHandler<A, R> | ((arg: A) => R | Promise<R>)): void {
    const h = typeof handler === "function" ? { handle: handler } : handler;
    (this.x.updates = own(this.x.updates)).set(name, h as UpdateHandler<unknown, unknown>);
  }

  /** Ends this run and starts a new run of the same ID with `input`, in one record. Never resolves. */
  continueAsNew<I>(input: I, opts: { version?: number } = {}): Promise<never> {
    this.x.h("continueAsNew", [opts.version ?? 0], [encode(input)]);
    this.x.be.kick();
    this.x.idle = true;
    return new Promise<never>(() => {});
  }

  /** Journal operations this run made so far (deterministic). */
  journalSize(): number {
    return this.x.h("journalSize")[0] ?? 0;
  }

  /** True for new runs, and for running ones the first time they get here after the deploy. */
  patched(id: string): boolean {
    return (this.x.h("patched", [id, "0"])[0] ?? 0) === 1;
  }

  /** Marks a patch whose old branch was removed. */
  deprecatePatch(id: string): void {
    this.x.h("patched", [id, "1"]);
  }

  /** Records search attributes (`null` removes a key). They are never encrypted. */
  setAttributes(attributes: Record<string, unknown>): void {
    if (attributes === null || typeof attributes !== "object" || Array.isArray(attributes)) {
      throw new FuseeError("search attributes are an object");
    }
    this.x.h("setAttributes", [], [encode(attributes)]);
  }

  /** The current time in Unix ms, recorded. */
  now(): number {
    const x = this.x;
    const be = x.be;
    if (be.valueNow) {
      if (x.ended) throw new FuseeError("workflow execution has ended");
      return call(() => be.valueNow!(x.shard, x.run, nowMs()));
    }
    const b = Buffer.alloc(8);
    b.writeBigUInt64LE(BigInt(nowMs()));
    return Number((x.hb("value", [0], [b]) ?? b).readBigUInt64LE(0));
  }

  /** A random number in [0, 1), recorded. */
  random(): number {
    const v = this.x.hb("value", [1], [randomBytes(8)]) ?? Buffer.alloc(8);
    return Number(v.readBigUInt64LE(0) >> 11n) / 2 ** 53;
  }

  /** A random UUID, recorded. */
  uuid(): string {
    const candidate = Buffer.from(randomUUID().replace(/-/g, ""), "hex");
    const h = (this.x.hb("value", [2], [candidate]) ?? candidate).toString("hex");
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
  }

  /** Like `Promise.all`, for journal operations. */
  all<T extends readonly unknown[]>(ops: T): Promise<{ -readonly [K in keyof T]: Awaited<T[K]> }> {
    return Promise.all(ops) as Promise<{ -readonly [K in keyof T]: Awaited<T[K]> }>;
  }

  /**
   * Like `Promise.race`; the winner is the first result delivered, which
   * replay reproduces. The losers are dropped (a dropped wait no longer
   * takes signals).
   */
  race<T>(ops: Promise<T>[]): Promise<T> {
    const forget = (): void => {
      for (const o of ops) {
        const s = seqOf.get(o);
        if (s !== undefined) this.x.forget(s);
      }
    };
    return Promise.race(ops).then(
      (v) => {
        forget();
        return v;
      },
      (e) => {
        forget();
        throw e;
      },
    );
  }
}
