// Payloads over the payload offload threshold (64 KiB by default) go to the
// shared log's blob area: a 1 MiB step result and a 1 MiB signal round trip
// exactly, before and after a reopen.
import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Engine } from "../dist/index.js";

const tmp = () => mkdtempSync(join(tmpdir(), "fusee-node-offload-"));
const MiB = 1 << 20;
const page = (seed, n) => {
  let x = seed | 1;
  let s = "";
  while (s.length < n) {
    x = (x * 1103515245 + 12345) >>> 0;
    s += String.fromCharCode(97 + (x % 26));
  }
  return s;
};

test("a 1 MiB step result and a 1 MiB signal go through the blob area", async () => {
  const dir = tmp();
  let fetched = 0;
  const register = (engine) =>
    engine.workflow("big", async (ctx, n) => {
      const body = await ctx.step("fetch", () => {
        fetched += 1;
        return page(7, n);
      });
      const note = await ctx.waitFor("note");
      return { body: body.length, same: body === page(7, n), note: note.length, noteSame: note === page(9, MiB) };
    });
  let engine = await Engine.open({ dir, flush: "fsync", shards: 2 });
  let wf = register(engine);
  const run = await wf.start("a", MiB);
  // A second run stays suspended across the reopen, its result recorded.
  await wf.start("b", MiB);
  await run.signal("note", page(9, MiB));
  const want = { body: MiB, same: true, note: MiB, noteSame: true };
  assert.deepEqual(await run.result(), want);
  assert.ok(existsSync(join(dir, "wal", "blobs")), "the blob area was written");
  assert.ok(readdirSync(join(dir, "wal", "blobs")).length > 0);
  await engine.close();

  engine = await Engine.open({ dir, flush: "fsync", shards: 2 });
  wf = register(engine);
  assert.deepEqual(await engine.run("a").result(), want);
  const b = engine.run("b");
  await b.signal("note", page(9, MiB));
  assert.deepEqual(await b.result(), want);
  assert.equal(fetched, 2, "recorded results replay, steps do not run again");
  await engine.close();
  rmSync(dir, { recursive: true, force: true });
});
