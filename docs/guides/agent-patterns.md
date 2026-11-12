# Agent patterns

Each pattern below is built from what [agents](agents.mdx) already uses:
steps, streams, children, waits and objects. None needs anything else, and
each keeps the guarantee of a step: a crash resumes at the call that was
running, and nothing that finished runs again.

## The Vercel AI SDK

An agent written with the AI SDK becomes durable without changing its loop:
`durableModel` makes each model call a step and `durableTools` makes each
tool call one.

```ts
import { generateText, stepCountIs } from "ai";
import { openai } from "@ai-sdk/openai";
import { workflow } from "fusee";
import { durableModel, durableTools } from "fusee/ai/vercel";

const support = workflow("support", async (ctx, question: string) => {
  const { text } = await generateText({
    model: durableModel(ctx, openai("gpt-5")),
    tools: durableTools(ctx, { lookupOrder, refund }),
    maxRetries: 0, // the steps retry, and their retries are recorded
    stopWhen: stepCountIs(10),
    prompt: question,
  });
  return text;
});
```

- Model calls are steps named `model`, tool calls `tool:<name>`. Steps are
  matched by position, so a determinism error names the call that moved.
- A tool's `execute` gets the AI SDK's `toolCallId`: pass it to services
  that take an idempotency key, as a step passes `s.key`.
- `streamText` works too: each call's text is a stream (`model#1`,
  `model#2`, …) readers follow while it runs (see
  [streaming to a chat UI](#streaming-to-a-chat-ui)).
- Set the AI SDK's own retries to zero. The step's retry policy applies
  instead, `onExhausted: "pause"` included: an agent whose provider is down
  waits for it instead of losing the conversation.

## Pydantic AI

`FuseeDurability` makes a Pydantic AI agent's model requests and tool calls
steps; `run` runs the agent as part of the workflow.

```python
import fusee
from pydantic_ai import Agent
from fusee.ai.pydantic import FuseeDurability, run

agent = Agent(
    "openai:gpt-5",
    name="support",
    tools=[lookup_order, refund],
    capabilities=[FuseeDurability(model_options={"retry": fusee.Retry(on_exhausted="pause")})],
)

@fusee.workflow("support")
async def support(ctx, question: str) -> str:
    result = await run(ctx, lambda: agent.run(question))
    return result.output
```

- Steps are named after the agent and its toolsets
  (`support__model.request`,
  `support__function_toolset__<agent>.call_tool:refund`): name the agent and
  give each toolset an `id`.
- A tool's own step options are its metadata's `"fusee"` entry; `False`
  calls it without a step.
- Pydantic AI's waits between its own retries are durable sleeps.
- Outside `run` the agent is an ordinary agent, so the same agent serves
  tests and scripts.
- Install with `pip install 'fusee[pydantic-ai]'`.

## The OpenAI Agents SDK

`run` is `Runner.run` with the agent made durable: model calls are steps
named `model`, function tool calls `tool:<name>`, and the agents it hands
off to are made durable too.

```python
import fusee
from agents import Agent, function_tool
from agents.tool_context import ToolContext
from fusee.ai.openai import run

@function_tool
def refund(ctx: ToolContext, order: str, amount: int) -> str:
    return payments.refund(order, amount, idempotency_key=ctx.tool_call_id)

agent = Agent(name="support", instructions="Help with orders.", tools=[refund])

@fusee.workflow("support")
async def support(ctx, question: str) -> str:
    result = await run(ctx, agent, question, model_options={"retry": fusee.Retry(on_exhausted="pause")})
    return result.final_output
```

- `model_options` and `tool_options` are the steps' options (retries,
  timeout, limit). The OpenAI client's own retries happen inside one
  attempt; set them to zero to leave retrying to the step.
- `durable_agent(agent)` is the durable copy on its own, for code that
  calls `Runner` itself; outside a workflow it is an ordinary agent.
- Hosted tools (web search, file search) run inside the model call, so
  they are part of its step.
- Install with `pip install 'fusee[openai-agents]'`.

## Sessions: one conversation per key

An object keeps a conversation's history under its key, and its exclusive
handler runs one turn at a time, in the order the messages came.

```ts
import { modelCall } from "fusee/ai";

const chats = engine.object("chat", {
  say: async (ctx, text: string) => {
    const history = ctx.state.get("history") ?? [];
    history.push({ role: "user", content: text });
    const reply = await modelCall(ctx, "reply", (resume) => model.stream(history, { prefill: resume }));
    history.push({ role: "assistant", content: reply });
    ctx.state.set("history", history);
    return reply;
  },
  history: shared((ctx) => ctx.state.get("history") ?? []),
});

await chats.key("user-7").call("say", "Where is my order?");
```

```python
async def say(ctx, text):
    history = ctx.state.get("history") or []
    history.append({"role": "user", "content": text})
    reply = await model_call(ctx, "reply", lambda resume: model.stream(history, prefill=resume))
    history.append({"role": "assistant", "content": reply})
    ctx.state.set("history", history)
    return reply

chats = fusee.object("chat", {"say": say})
```

A support engineer can fix a session's history from outside with
[`fusee objects edit chat user-7`](../features.md#editing-state-from-outside).

## Approval before a tool runs

A tool that changes something a person must agree to waits for them. The
wait holds no process: the run parks until the answer comes.

```ts
const tools = durableTools(ctx, { refund }, {
  approve: { tools: ["refund"], ask: (cb, call) => slack.post(`Refund ${call.input.amount}? ${cb.url}`) },
});
```

Completing the URL with `true` runs the tool; `{ "approved": false,
"reason": "..." }` gives the model the refusal as the tool's result.
Without the adapter, the same is `ctx.awaitCompletion` before the step, or
`ctx.waitFor(approval, { timeout: "1d" })` on a signal (see
[waiting for people](human-approval.mdx)).

## Interrupt and regenerate

Run each turn as its own child so it can be stopped. Cancelling the run
aborts the model call's request (its step's `s.signal`), and the next turn
starts from the history as it was.

```ts
const turn = workflow("turn", async (ctx, history: Message[]) =>
  ctx.step("model", (s) => model.complete(history, { signal: s.signal })),
);

// The user pressed "stop", then "regenerate":
await engine.run("chat-7/turn-3").cancel("interrupted");
await engine.start(turn, "chat-7/turn-3b", history);
```

## Parallel tool calls

Calls that do not depend on each other run at once. Each is its own step,
so a crash keeps the ones that finished.

```ts
const [weather, flights] = await ctx.all([
  ctx.step("tool:weather", () => weatherApi.get(city)),
  ctx.step("tool:flights", () => flightsApi.search(city, date)),
]);
```

```python
weather, flights = await ctx.all([
    ctx.step("tool:weather", lambda: weather_api.get(city)),
    ctx.step("tool:flights", lambda: flights_api.search(city, date)),
])
```

## Racing models

Ask two models and take the first answer; the slower call is cancelled.

```ts
const answer = await ctx.race([
  ctx.step("model:fast", (s) => fast.complete(prompt, { signal: s.signal })),
  ctx.step("model:large", (s) => large.complete(prompt, { signal: s.signal })),
]);
```

## A workflow as a tool

A tool can be a whole workflow, with its own steps, retries and journal: the
agent waits for it as a child.

```ts
const research = workflow("research", async (ctx, topic: string) => { /* many steps */ });

const tools = {
  research: { description: "Researches a topic", execute: ({ topic }, { toolCallId }) =>
    ctx.child(research, `${ctx.workflowId}/${toolCallId}`, topic) },
};
```

The child's ID comes from the call, so a replay attaches to the same child
instead of starting another.

## Orchestrator and workers

A planner splits the work, children do the parts, and a final step joins
them.

```ts
const plan = await ctx.step("plan", () => planner.split(task));
const parts = await ctx.all(plan.parts.map((p, i) => ctx.child(writer, `${ctx.workflowId}/part-${i}`, p)));
return ctx.step("join", () => editor.merge(parts));
```

## A sequential pipeline

Each stage is its own model call, so a failure in a later stage does not
repeat the earlier ones.

```python
outline = await ctx.step("outline", lambda: model.complete(f"Outline: {topic}"))
draft = await ctx.step("draft", lambda: model.complete(f"Write from: {outline}"))
final = await ctx.step("edit", lambda: model.complete(f"Edit: {draft}"))
```

## Evaluate and retry

A judge scores each answer; a low score asks again, up to a bound the
journal enforces.

```ts
for (let round = 1; round <= 3; round++) {
  const answer = await ctx.step("model", () => model.complete(prompt));
  const score = await ctx.step("judge", () => judge.score(prompt, answer));
  if (score >= 0.8) return answer;
  prompt = `${prompt}\nA reviewer scored the last answer ${score}. Improve it.`;
}
throw new FatalError("no answer passed review in 3 rounds");
```

## Streaming to a chat UI

A model call's tokens are a stream of the run. A web handler in any process
follows it; a reader that reconnects passes its offset and misses nothing.

```ts
// In the workflow:
const reply = await modelCall(ctx, "reply", (resume) => model.stream(history, { prefill: resume }));

// In the web server, with server-sent events:
app.get("/chat/:id/stream", async (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  for await (const token of engine.run(req.params.id).stream("reply", { from: Number(req.query.from ?? 0) })) {
    res.write(`data: ${JSON.stringify(token)}\n\n`);
  }
  res.end();
});
```

The HTTP API serves the same stream without your server:
`GET /v1/runs/{id}/streams/reply` with `Accept: text/event-stream`.

## Notify when ready

A long task tells someone when it is done: the last step calls a webhook
with the step's key, so a crash after the call does not notify twice.

```ts
const report = await ctx.child(research, `${ctx.workflowId}/research`, topic);
await ctx.step("notify", (s) => fetch(webhookUrl, {
  method: "POST",
  headers: { "Idempotency-Key": s.key },
  body: JSON.stringify({ id: ctx.workflowId, report }),
}));
```

A caller that would rather wait asks for the result instead:
`await engine.run(id).result()`, or `GET /v1/runs/{id}/result`.

## Undo on failure

An agent that books, buys or sends registers how to undo each action. If
the run fails or is cancelled, the compensations run, newest first.

```ts
const booking = await ctx.step("tool:book", (s) => hotels.book(room, { idempotencyKey: s.key }));
ctx.compensate("tool:cancel-booking", () => hotels.cancel(booking.id));
```

## Long conversations

A run's journal grows by a few records per turn. After a few hundred turns,
summarize and continue as a new run with the summary.

```python
@fusee.workflow("chat")
async def chat(ctx, state):
    history = await run_turns(ctx, state)
    if ctx.journal_size() > 1000:
        summary = await ctx.step("summarize", lambda: model.summarize(history))
        return ctx.continue_as_new({"summary": summary})
```

## Rate and cost limits

Model calls take a permit of a limit, so many runs share a provider's quota
and wait their turn instead of failing. A workflow timeout bounds what a
runaway agent can spend.

```ts
engine.limit("openai", { perSecond: 50, concurrent: 20 });
await ctx.step("model", () => model.complete(history), { limit: "openai", fairnessKey: tenantId });
await engine.start(agent, id, question, { timeout: "15m" });
```

## When the provider is down

A model call that keeps failing can pause its run instead of failing it:
the conversation waits, with everything it has, until someone resumes it.

```ts
await ctx.step("model", () => model.complete(history), {
  retry: { attempts: 5, max: "30s", onExhausted: "pause" },
});
// Once the provider is back:
//   fusee runs bulk resume --type support --status paused
```

See [pausing when retries run out](../features.md#pausing-when-retries-run-out).
