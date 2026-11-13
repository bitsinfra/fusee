// OpenTelemetry instruments: spans and metrics.
import { RunExec } from "./execution.js";

// ------------------------------------------------------------- telemetry

type Attrs = Record<string, string | number | boolean>;

/** The parts of an OpenTelemetry span Fusee uses. */
export interface SpanLike {
  setAttribute(key: string, value: string | number | boolean): unknown;
  setStatus(status: { code: number; message?: string }): unknown;
  end(): void;
}

/**
 * OpenTelemetry instruments for an engine, structurally typed so Fusee has
 * no dependency on `@opentelemetry/api`. Build one with `openTelemetry(api)`.
 */
export interface Telemetry {
  /** `context`: the parent (from `spanIn` or `remote`); the active one when absent. */
  tracer?: { startSpan(name: string, options?: { attributes?: Attrs }, context?: unknown): SpanLike };
  /** The active span's W3C traceparent: a run started here joins its trace. */
  traceparent?(): string | undefined;
  /** A context whose parent is the span a traceparent names. */
  remote?(traceparent: string): unknown;
  /** A context with `span` as the active span, below `parent` (the active one when absent). */
  spanIn?(span: SpanLike, parent?: unknown): unknown;
  meter?: {
    createCounter(name: string, options?: { description?: string; unit?: string }): { add(n: number, attrs?: Attrs): void };
    createHistogram(name: string, options?: { description?: string; unit?: string }): { record(n: number, attrs?: Attrs): void };
  };
  /** Runs `fn` with `span` as the active span, so the step's own spans nest under it. */
  withSpan?<T>(span: SpanLike, fn: () => T): T;
}

/**
 * Instruments from the application's `@opentelemetry/api` module (pass the
 * module itself): a `fusee.step` span per step attempt, nested under
 * whatever is active, and the metrics `fusee.steps.executed`,
 * `fusee.step.duration`, `fusee.workflows.started` and
 * `fusee.workflows.finished`. Replayed steps record nothing.
 */
export function openTelemetry(api: {
  trace: {
    getTracer(name: string, version?: string): NonNullable<Telemetry["tracer"]>;
    setSpan(ctx: unknown, span: SpanLike): unknown;
    getSpanContext?(ctx: unknown): { traceId: string; spanId: string; traceFlags: number } | undefined;
    setSpanContext?(ctx: unknown, sc: { traceId: string; spanId: string; traceFlags: number; isRemote?: boolean }): unknown;
  };
  metrics: { getMeter(name: string, version?: string): NonNullable<Telemetry["meter"]> };
  context: { active(): unknown; with<T>(ctx: unknown, fn: () => T): T };
}): Telemetry {
  return {
    tracer: api.trace.getTracer("fusee", VERSION),
    meter: api.metrics.getMeter("fusee", VERSION),
    withSpan: (span, fn) => api.context.with(api.trace.setSpan(api.context.active(), span), fn),
    spanIn: (span, parent) => api.trace.setSpan(parent ?? api.context.active(), span),
    traceparent: () => {
      const sc = api.trace.getSpanContext?.(api.context.active());
      if (!sc || /^0+$/.test(sc.traceId) || /^0+$/.test(sc.spanId)) return undefined;
      return `00-${sc.traceId}-${sc.spanId}-${(sc.traceFlags & 0xff).toString(16).padStart(2, "0")}`;
    },
    remote: (tp) => {
      const p = tp.split("-");
      if (!api.trace.setSpanContext || p.length !== 4) return undefined;
      return api.trace.setSpanContext(api.context.active(), {
        traceId: p[1]!, spanId: p[2]!, traceFlags: parseInt(p[3]!, 16), isRemote: true,
      });
    },
  };
}

const VERSION = "1.0.2";
/** OpenTelemetry's `SpanStatusCode`. */
const SPAN_OK = 1;
const SPAN_ERROR = 2;

/** @internal */
export class Instruments {
  private readonly steps;
  private readonly stepTime;
  private readonly started;
  private readonly finished;

  constructor(private readonly t: Telemetry) {
    const m = t.meter;
    this.steps = m?.createCounter("fusee.steps.executed", { description: "Step attempts run (not replayed)" });
    this.stepTime = m?.createHistogram("fusee.step.duration", { description: "Step attempt duration", unit: "ms" });
    this.started = m?.createCounter("fusee.workflows.started", { description: "Workflow starts accepted" });
    this.finished = m?.createCounter("fusee.workflows.finished", { description: "Workflow runs finished by this process" });
  }

  /** One attempt of a step; the returned function ends it. */
  attempt(x: RunExec, name: string, attempt: number): { run<T>(fn: () => T): T; end(error?: string): void } {
    const began = performance.now();
    const attrs: Attrs = {
      "fusee.workflow.id": x.id,
      "fusee.workflow.type": x.wtype,
      "fusee.step.name": name,
      "fusee.step.attempt": attempt,
    };
    const span = this.t.tracer?.startSpan("fusee.step", { attributes: attrs }, x.wfCtx);
    return {
      run: (fn) => (span && this.t.withSpan ? this.t.withSpan(span, fn) : fn()),
      end: (error) => {
        const labels: Attrs = { "fusee.workflow.type": x.wtype, outcome: error === undefined ? "ok" : "error" };
        this.steps?.add(1, labels);
        this.stepTime?.record(performance.now() - began, labels);
        if (span) {
          span.setStatus(error === undefined ? { code: SPAN_OK } : { code: SPAN_ERROR, message: error });
          span.end();
        }
      },
    };
  }

  start(wtype: string): void {
    this.started?.add(1, { "fusee.workflow.type": wtype });
  }

  /** The active span's traceparent, for a start made here. */
  traceparent(): string | undefined {
    return this.t.traceparent?.();
  }

  /**
   * A `fusee.workflow` span for a run's code in this process, below the
   * trace its start recorded: its steps' spans go under it.
   */
  workflow(x: RunExec, trace: string | undefined): void {
    const tracer = this.t.tracer;
    if (!tracer) return;
    const parent = trace ? this.t.remote?.(trace) : undefined;
    const span = tracer.startSpan(
      "fusee.workflow",
      { attributes: { "fusee.workflow.id": x.id, "fusee.workflow.type": x.wtype, "fusee.run": x.run } },
      parent,
    );
    x.wfSpan = span;
    x.wfCtx = this.t.spanIn?.(span, parent);
  }

  /** Ends a run's workflow span (`status` as recorded, or the run ended from outside). */
  endWorkflow(x: RunExec, status?: number): void {
    const span = x.wfSpan;
    if (!span) return;
    x.wfSpan = undefined;
    span.setStatus(status === undefined || status === 0 ? { code: SPAN_OK } : { code: SPAN_ERROR, message: STATUS_NAMES[status] ?? String(status) });
    span.end();
  }

  finish(wtype: string, status: number): void {
    this.finished?.add(1, { "fusee.workflow.type": wtype, status: STATUS_NAMES[status] ?? String(status) });
  }
}

const STATUS_NAMES: Record<number, string> = { 0: "ok", 1: "failed", 2: "cancelled", 3: "timed_out", 4: "terminated" };
