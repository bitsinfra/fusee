import { test } from "node:test";
import assert from "node:assert/strict";
import { Engine, RejectedError, workflow } from "../dist/index.js";
import { rich } from "../dist/rich.js";

test("fusee/rich keeps dates, maps, sets and bigints; one serializer per process", async () => {
  const when = new Date("2026-11-11T10:00:00Z");
  const wf = workflow("rich", async (ctx, input) => {
    const v = await ctx.step("make", () => ({ when, tags: new Set(["a", "b"]), n: 10n ** 20n, m: new Map([["k", when]]) }));
    return { v, input, sameDate: v.when instanceof Date && input.at instanceof Date };
  });
  const engine = await Engine.open({ durability: "memory", serializer: rich, workflows: [wf] });
  const out = await (await engine.start(wf, "r-1", { at: when })).result();
  assert.ok(out.sameDate);
  assert.equal(out.v.when.getTime(), when.getTime());
  assert.deepEqual([...out.v.tags], ["a", "b"]);
  assert.equal(out.v.n, 10n ** 20n);
  assert.equal(out.v.m.get("k").getTime(), when.getTime());
  // One serializer per process while an engine uses it.
  const other = { stringify: JSON.stringify, parse: JSON.parse };
  await assert.rejects(Engine.open({ durability: "memory", serializer: other }), RejectedError);
  await engine.close();
  // Its last engine closed, the process may use another.
  await (await Engine.open({ durability: "memory", serializer: other })).close();
});
