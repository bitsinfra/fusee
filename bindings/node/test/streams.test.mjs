// Durable streams in the TypeScript SDK: chunk order and completeness, a
// reader that reconnects at its offset, a restart in the middle of a
// streaming step, replays that append nothing, bytes chunks.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Engine, FuseeError } from "../dist/index.js";

const tmp = () => mkdtempSync(join(tmpdir(), "fusee-node-streams-"));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const token = (i) => `t${i} `;

async function readAll(run, name, from = 0) {
  const r = run.stream(name, { from });
  const out = [];
  for await (const c of r) out.push(c);
  return { chunks: out, status: r.status, offset: r.offset };
}

test("chunks arrive in order and the step's result ends the stream", async () => {
  const engine = await Engine.open({ durability: "memory" });
  try {
    const wf = engine.workflow("llm", async (ctx, n) =>
      ctx.step("generate", async () => {
        const out = ctx.stream("tokens");
        let text = "";
        for (let i = 0; i < n; i++) {
          out.append(token(i));
          text += token(i);
          if (i % 25 === 0) await sleep(1);
        }
        return text;
      }),
    );
    const run = await wf.start("q-1", 300);
    const { chunks, status } = await readAll(run, "tokens");
    assert.equal(chunks.length, 300);
    assert.equal(status, "ok");
    assert.equal(chunks.join(""), await run.result());
    // A reader that reconnects at an offset continues exactly there.
    const tail = await readAll(run, "tokens", 250);
    assert.deepEqual(tail.chunks, chunks.slice(250));
    assert.equal(tail.offset, 300);
    // A reader that stops early knows where it stopped.
    const r = run.stream("tokens");
    for await (const _ of r) if (r.offset === 10) break;
    assert.equal(r.offset, 10);
  } finally {
    await engine.close();
  }
});

test("bytes chunks, a workflow's own stream, and a long poll", async () => {
  const engine = await Engine.open({ durability: "memory" });
  try {
    const wf = engine.workflow("events", async (ctx) => {
      const out = ctx.stream("events");
      out.append({ stage: "start" });
      out.append(new Uint8Array([0, 1, 2, 255]));
      await ctx.waitFor("go");
      out.append("after");
      out.close();
      await ctx.waitFor("finish");
      return null;
    });
    const run = await wf.start("e-1", null);
    const r = run.stream("events", { wait: 50 });
    const it = r[Symbol.asyncIterator]();
    assert.deepEqual((await it.next()).value, { stage: "start" });
    const b = (await it.next()).value;
    assert.ok(Buffer.isBuffer(b));
    assert.deepEqual([...b], [0, 1, 2, 255]);
    let got;
    const pending = it.next().then((v) => (got = v));
    await sleep(150);
    assert.equal(got, undefined, "the read waits for the next chunk");
    await run.signal("go", 1);
    await pending;
    assert.equal(got.value, "after");
    assert.equal((await it.next()).done, true);
    assert.equal(r.status, "ok");
    await run.signal("finish", 1);
    await run.result();
    await assert.rejects(readAll(engine.run("nobody"), "events"), (e) => e instanceof FuseeError && e.code === "NotFound");
  } finally {
    await engine.close();
  }
});

test("a restart mid-stream keeps the recorded prefix; a replay appends nothing", async () => {
  const dir = tmp();
  const resumedAt = [];
  // Tokens each attempt generated (the first one's code may run on after
  // its engine closed, and its appends then fail).
  const generated = [];
  const register = (engine) =>
    engine.workflow("llm", async (ctx, n) => {
      const p = ctx.stream("progress");
      p.append("started");
      const text = await ctx.step("generate", async () => {
        const out = ctx.stream("tokens");
        const prefix = out.recorded();
        resumedAt.push(prefix.length);
        const attempt = generated.push(0) - 1;
        let text = prefix.join("");
        for (let i = prefix.length; i < n; i++) {
          generated[attempt]++;
          out.append(token(i));
          text += token(i);
          await sleep(2);
        }
        return text;
      });
      p.append("done");
      return text;
    });
  const n = 40;
  const seen = [];
  try {
    let engine = await Engine.open({ dir });
    register(engine);
    await engine.workflow("llm").start("c-1", n);
    const r = engine.run("c-1").stream("tokens");
    for await (const c of r) {
      seen.push(c);
      if (seen.length === 15) break;
    }
    // The process stops in the middle of the step.
    await engine.close();
    engine = await Engine.open({ dir });
    register(engine);
    // The reader comes back at its offset.
    const rest = await readAll(engine.run("c-1"), "tokens", r.offset);
    seen.push(...rest.chunks);
    const expected = Array.from({ length: n }, (_, i) => token(i));
    assert.deepEqual(seen, expected, "no token is missing, repeated or changed");
    assert.equal(await engine.run("c-1").result(), expected.join(""));
    assert.equal(resumedAt.length, 2);
    assert.ok(resumedAt[1] >= 15, `the retry continued after the recorded prefix: ${resumedAt}`);
    assert.equal(generated[1], n - resumedAt[1], "no recorded token was generated again");
    // The workflow's own appends were recorded once, across the replay.
    const progress = await readAll(engine.run("c-1"), "progress");
    assert.deepEqual(progress.chunks, ["started", "done"]);
    const kinds = engine.run("c-1").inspect().journal.filter((r) => r.kind === "chunk" && r.stream === "progress");
    assert.equal(kinds.length, 2);
    await engine.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("append cost", async () => {
  const engine = await Engine.open({ durability: "memory" });
  try {
    const n = 100_000;
    const wf = engine.workflow("bulk", async (ctx, n) =>
      ctx.step("generate", () => {
        const out = ctx.stream("tokens");
        const t = performance.now();
        for (let i = 0; i < n; i++) out.append(i);
        return performance.now() - t;
      }),
    );
    const run = await wf.start("b-1", n);
    const ms = await run.result();
    let count = 0;
    const t = performance.now();
    for await (const c of run.stream("tokens")) assert.equal(c, count++);
    assert.equal(count, n);
    console.log(
      `node append: ${Math.round(n / (ms / 1e3))} chunks/s (${Math.round((ms * 1e6) / n)} ns each); ` +
        `read back ${Math.round(n / ((performance.now() - t) / 1e3))} chunks/s`,
    );
  } finally {
    await engine.close();
  }
});
