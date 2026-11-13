import { test } from "node:test";
import assert from "node:assert/strict";
import { Engine, workflow } from "../dist/index.js";
import { modelCall } from "../dist/ai.js";

test("modelCall streams tokens and resumes after a failed attempt", async () => {
  const calls = [];
  const draft = workflow("draft", async (ctx, topic) =>
    modelCall(ctx, "draft", async function* (resume) {
      calls.push(resume);
      const words = ["Fusee ", "keeps ", "going ", "after ", "a ", "crash."];
      for (const w of words.slice(resume.split(" ").length - 1)) {
        yield w;
        if (calls.length === 1 && w === "going ") throw new Error("connection reset");
      }
    }, { retry: { attempts: 2, initial: 1 } }),
  );
  const engine = await Engine.open({ durability: "memory", workflows: [draft] });
  const run = await engine.start(draft, "d-1", "durability");
  const text = await run.result();
  assert.equal(text, "Fusee keeps going after a crash.");
  // The second attempt got what the first had streamed.
  assert.deepEqual(calls, ["", "Fusee keeps going "]);
  const tokens = [];
  for await (const c of run.stream("draft")) tokens.push(c);
  assert.equal(tokens.join(""), "Fusee keeps going after a crash.");
  await engine.close();
});
