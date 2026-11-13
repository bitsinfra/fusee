"""An AI agent loop with tool calls: every model and tool call is a step."""
import asyncio

import fusee
from fusee import Engine


async def model(history):
    if any(h.startswith("tool:") for h in history):
        return {"answer": f"Pack an umbrella ({history[-1]})."}
    return {"tool": "weather", "args": "Lisbon"}


TOOLS = {"weather": lambda city: f"tool: rain in {city}"}


@fusee.workflow("agent")
async def agent(ctx: fusee.Ctx, question: str) -> str:
    history = [f"user: {question}"]
    for turn in range(10):
        t = await ctx.step(f"model-{turn}", lambda: model(list(history)))
        if "answer" in t:
            return t["answer"]
        history.append(await ctx.step(f"tool-{turn}-{t['tool']}", lambda: TOOLS[t["tool"]](t["args"])))
    raise RuntimeError("no answer after 10 turns")


async def main():
    async with await Engine.open("./data/agent-loop", workflows=[agent]) as engine:
        run = await engine.start(agent, "trip-question-1", "Do I need an umbrella in Lisbon?",
                                 id_policy={"running": "use-existing", "finished": "use-existing"})
        print(await run.result())


asyncio.run(main())
