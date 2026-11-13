// An AI agent loop with tool calls: every model and tool call is a step, so a
// crash resumes the conversation without calling the model again.
import { Engine, workflow } from "@bitsinfra/fusee";

const model = async (history) =>
  history.some((h) => h.startsWith("tool:"))
    ? { answer: `Pack an umbrella (${history.at(-1)}).` }
    : { tool: "weather", args: "Lisbon" };
const tools = { weather: async (city) => `tool: rain in ${city}` };

export const agent = workflow("agent", async (ctx, question) => {
  const history = [`user: ${question}`];
  for (let turn = 0; turn < 10; turn++) {
    const t = await ctx.step(`model-${turn}`, () => model(history));
    if (t.answer) return t.answer;
    history.push(await ctx.step(`tool-${turn}-${t.tool}`, () => tools[t.tool](t.args)));
  }
  throw new Error("no answer after 10 turns");
});

const engine = await Engine.open({ dir: "./data/agent-loop", workflows: [agent] });
const run = await engine.start(agent, "trip-question-1", "Do I need an umbrella in Lisbon?", { idPolicy: { finished: "use-existing" } });
console.log(await run.result());
await engine.close();
