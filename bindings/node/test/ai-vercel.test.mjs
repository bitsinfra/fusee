// fusee/ai/vercel with the Vercel AI SDK itself (a dev dependency): its
// generateText and streamText, its tool(), and its mock model.
import { test } from "node:test";
import assert from "node:assert/strict";
import { generateText, jsonSchema, stepCountIs, streamText, tool } from "ai";
import { MockLanguageModelV4 } from "ai/test";
import { Engine, workflow } from "../dist/index.js";
import { durableModel, durableTools } from "../dist/ai-vercel.js";
import { crashEverywhere } from "../dist/testing.js";

const usage = {
  inputTokens: { total: 3, noCache: 3, cacheRead: 0, cacheWrite: 0 },
  outputTokens: { total: 2, text: 2, reasoning: 0 },
};

/** A model that asks for one refund, then answers with the tool's output. */
function scriptedModel(calls = []) {
  return new MockLanguageModelV4({
    doGenerate: async ({ prompt }) => {
      calls.push(prompt.length);
      const result = prompt.find((m) => m.role === "tool");
      return {
        content: result
          ? [{ type: "text", text: `refunded ${result.content[0].output.value.refunded}` }]
          : [{ type: "tool-call", toolCallId: "call-1", toolName: "refund", input: JSON.stringify({ amount: 30 }) }],
        finishReason: { unified: result ? "stop" : "tool-calls", raw: undefined },
        usage,
        warnings: [],
        response: { id: "r", timestamp: new Date(0), modelId: "scripted" },
      };
    },
  });
}

/** A payments service that refunds once per idempotency key. */
function payments() {
  const done = new Map();
  return {
    refund(key, amount) {
      if (!done.has(key)) done.set(key, amount);
      return { refunded: done.get(key) };
    },
    snapshot: () => [...done.entries()],
  };
}

const refundTool = (svc) =>
  tool({
    description: "Refunds an order",
    inputSchema: jsonSchema({ type: "object", properties: { amount: { type: "number" } }, required: ["amount"] }),
    execute: async ({ amount }, { toolCallId }) => svc.refund(toolCallId, amount),
  });

const agentWith = (svc, calls) =>
  workflow("agent", async (ctx, prompt) => {
    const { text, steps } = await generateText({
      model: durableModel(ctx, scriptedModel(calls)),
      tools: durableTools(ctx, { refund: refundTool(svc) }),
      maxRetries: 0,
      stopWhen: stepCountIs(5),
      prompt,
    });
    return { text, steps: steps.length };
  });

test("generateText's model and tool calls are steps", async () => {
  const svc = payments();
  const calls = [];
  const agent = agentWith(svc, calls);
  const engine = await Engine.open({ durability: "memory", workflows: [agent], keepHistory: true });
  const run = await engine.start(agent, "a-1", "refund my order");
  assert.deepEqual(await run.result(), { text: "refunded 30", steps: 2 });
  const steps = engine.run("a-1").inspect().journal.filter((r) => r.kind === "step").map((r) => r.name);
  assert.deepEqual(steps, ["model", "tool:refund", "model"]);
  assert.equal(calls.length, 2);
  await engine.close();
});

test("a crash anywhere in generateText changes neither the answer nor the refunds", async () => {
  await crashEverywhere(() => {
    const svc = payments();
    return { workflows: [agentWith(svc, [])], workflow: "agent", id: "a-2", input: "refund my order", effects: () => svc.snapshot() };
  });
});

test("streamText's call is recorded whole and its text is a stream readers follow", async () => {
  const model = new MockLanguageModelV4({
    doStream: async () => ({
      stream: new ReadableStream({
        start(c) {
          for (const p of [
            { type: "stream-start", warnings: [] },
            { type: "response-metadata", id: "r", timestamp: new Date(0), modelId: "scripted" },
            { type: "text-start", id: "t" },
            { type: "text-delta", id: "t", delta: "Hello, " },
            { type: "text-delta", id: "t", delta: "world" },
            { type: "text-end", id: "t" },
            { type: "finish", finishReason: { unified: "stop", raw: undefined }, usage },
          ]) c.enqueue(p);
          c.close();
        },
      }),
    }),
  });
  const chat = workflow("chat", async (ctx) => {
    const result = streamText({ model: durableModel(ctx, model), prompt: "hi", maxRetries: 0 });
    return await result.text;
  });
  const engine = await Engine.open({ durability: "memory", workflows: [chat] });
  const run = await engine.start(chat, "c-1", null);
  assert.equal(await run.result(), "Hello, world");
  const chunks = [];
  for await (const c of run.stream("model#1")) chunks.push(c);
  assert.deepEqual(chunks, ["Hello, ", "world"]);
  await engine.close();
});

test("a tool that needs approval waits for a person, and a refusal is its result", async () => {
  const asked = [];
  let refunds = 0;
  const agent = workflow("approve", async (ctx, amount) => {
    const tools = durableTools(
      ctx,
      {
        refund: tool({
          inputSchema: jsonSchema({ type: "object", properties: { amount: { type: "number" } } }),
          execute: async ({ amount: a }) => ((refunds += 1), { refunded: a }),
        }),
      },
      { approve: { tools: ["refund"], ask: (cb, call) => asked.push({ token: cb.token, call }) } },
    );
    return tools.refund.execute({ amount }, { toolCallId: "call-9", messages: [] });
  });
  const engine = await Engine.open({ durability: "memory", workflows: [agent] });
  const yes = await engine.start(agent, "y-1", 30);
  for (let i = 0; i < 400 && asked.length < 1; i++) await new Promise((r) => setTimeout(r, 5));
  assert.deepEqual(asked[0].call, { tool: "refund", input: { amount: 30 }, toolCallId: "call-9" });
  assert.equal(refunds, 0, "nothing runs before the approval");
  await engine.complete(asked[0].token, true);
  assert.deepEqual(await yes.result(), { refunded: 30 });
  const no = await engine.start(agent, "n-1", 99);
  for (let i = 0; i < 400 && asked.length < 2; i++) await new Promise((r) => setTimeout(r, 5));
  await engine.complete(asked[1].token, { approved: false, reason: "over the limit" });
  assert.deepEqual(await no.result(), { approved: false, reason: "over the limit" });
  assert.equal(refunds, 1);
  await engine.close();
});
