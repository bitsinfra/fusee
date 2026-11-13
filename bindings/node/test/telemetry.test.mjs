import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Engine, openTelemetry } from "../dist/index.js";

const tmp = () => mkdtempSync(join(tmpdir(), "fusee-node-otel-"));

const TP = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

/**
 * A stand-in for `@opentelemetry/api` that records what it is given. A
 * context is `{ span, trace, sc }`: the active span, the traceparent it is
 * in, and a remote span context.
 */
function fakeApi() {
  const spans = [];
  const points = [];
  let active = null;
  const instrument = (name) => ({
    add: (n, attrs) => points.push({ name, n, attrs }),
    record: (n, attrs) => points.push({ name, n, attrs }),
  });
  const api = {
    trace: {
      getTracer: () => ({
        startSpan: (name, opts, ctx) => {
          const parent = ctx === undefined ? active : ctx;
          const span = { name, attrs: { ...opts?.attributes }, parent: parent?.span ?? null, trace: parent?.trace, status: null, ended: false };
          spans.push(span);
          return {
            setAttribute: (k, v) => (span.attrs[k] = v),
            setStatus: (s) => (span.status = s),
            end: () => (span.ended = true),
            _span: span,
          };
        },
      }),
      setSpan: (ctx, span) => ({ span: span._span, trace: ctx?.trace }),
      getSpanContext: (ctx) => ctx?.sc,
      setSpanContext: (ctx, sc) => ({
        span: null,
        trace: `00-${sc.traceId}-${sc.spanId}-${sc.traceFlags.toString(16).padStart(2, "0")}`,
      }),
    },
    metrics: { getMeter: () => ({ createCounter: instrument, createHistogram: instrument }) },
    context: {
      active: () => active,
      with: (ctx, fn) => {
        const prev = active;
        active = ctx;
        try {
          return fn();
        } finally {
          active = prev;
        }
      },
    },
  };
  return { api, spans, points, current: () => active?.span ?? null, setActive: (c) => (active = c) };
}

test("steps report spans and metrics; replayed steps report nothing", async () => {
  const dir = tmp();
  const otel = fakeApi();
  let insideStep = null;
  let fails = 1;
  const open = () => Engine.open({ dir, flush: "fsync", shards: 1, telemetry: openTelemetry(otel.api) });
  let engine = await open();
  const register = (e) =>
    e.workflow("order", async (ctx, n) => {
      const a = await ctx.step("price", () => {
        insideStep = otel.current();
        return n * 2;
      });
      const b = await ctx.step(
        "charge",
        () => {
          if (fails-- > 0) throw new Error("card declined");
          return a + 1;
        },
        { retry: { attempts: 3, initial: "1ms" } },
      );
      await ctx.waitFor("go", { timeout: "30s" });
      return b;
    });
  const wf = register(engine);
  // Started inside a request's span: the run records its trace.
  otel.setActive({ span: { name: "request" }, trace: TP, sc: { traceId: "0af7651916cd43dd8448eb211c80319c", spanId: "b7ad6b7169203331", traceFlags: 1 } });
  const run = await wf.start("o-1", 20);
  otel.setActive(null);
  for (let i = 0; i < 200 && otel.spans.filter((s) => s.ended).length < 3; i++) await new Promise((r) => setTimeout(r, 5));
  const ended = otel.spans.filter((s) => s.ended);
  // The run's code has a workflow span in the starter's trace; the step
  // spans go under it.
  const [wfSpan] = otel.spans.filter((s) => s.name === "fusee.workflow");
  assert.equal(wfSpan.trace, TP);
  assert.equal(wfSpan.attrs["fusee.workflow.id"], "o-1");
  assert.ok(ended.every((s) => s.parent === wfSpan));
  assert.equal((await run.describe()).journal[0].trace, TP);
  assert.deepEqual(
    ended.map((s) => [s.name, s.attrs["fusee.step.name"], s.attrs["fusee.step.attempt"], s.status?.code]),
    [
      ["fusee.step", "price", 1, 1],
      ["fusee.step", "charge", 1, 2],
      ["fusee.step", "charge", 2, 1],
    ],
  );
  assert.equal(ended[1].status.message, "card declined");
  assert.equal(ended[0].attrs["fusee.workflow.id"], "o-1");
  assert.equal(ended[0].attrs["fusee.workflow.type"], "order");
  // The step function ran with its span active.
  assert.equal(insideStep, ended[0]);
  const executed = otel.points.filter((p) => p.name === "fusee.steps.executed");
  assert.deepEqual(executed.map((p) => p.attrs.outcome), ["ok", "error", "ok"]);
  assert.equal(otel.points.filter((p) => p.name === "fusee.step.duration").length, 3);
  assert.deepEqual(
    otel.points.filter((p) => p.name === "fusee.workflows.started").map((p) => p.attrs["fusee.workflow.type"]),
    ["order"],
  );
  await engine.close();

  // Reopen: the run replays its two steps without running them.
  const before = otel.spans.length;
  engine = await open();
  register(engine);
  await engine.run("o-1").signal("go", true);
  assert.equal(await engine.run("o-1").result(), 41);
  // Replayed steps make no spans; the resumed code has a workflow span in
  // the same trace, ended with the run.
  const after = otel.spans.slice(before);
  assert.deepEqual(after.map((s) => [s.name, s.trace, s.ended, s.status?.code]), [["fusee.workflow", TP, true, 1]]);
  const finished = otel.points.filter((p) => p.name === "fusee.workflows.finished");
  assert.deepEqual(finished.map((p) => p.attrs.status), ["ok"]);
  await engine.close();
  rmSync(dir, { recursive: true, force: true });
});

test("an engine without telemetry makes no spans", async () => {
  const dir = tmp();
  const engine = await Engine.open({ dir, flush: "fsync", shards: 1 });
  const wf = engine.workflow("one", async (ctx) => ctx.step("s", () => 1));
  assert.equal(await (await wf.start("x", null)).result(), 1);
  await engine.close();
  rmSync(dir, { recursive: true, force: true });
});
