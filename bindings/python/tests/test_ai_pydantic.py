"""fusee.ai.pydantic with Pydantic AI itself (its Agent, tools, output types
and FunctionModel). Skipped when pydantic-ai is not installed."""

import asyncio
import shutil
import sys
import tempfile

try:
    import pydantic_ai  # noqa: F401
except ImportError:
    pydantic_ai = None

import fusee
from fusee import Engine, Retry
from fusee.testing import Case, crash_everywhere

if pydantic_ai is not None:
    import os

    os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")
    from pydantic import BaseModel
    from pydantic_ai import Agent
    from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, ToolReturnPart
    from pydantic_ai.models.function import AgentInfo, FunctionModel

    from fusee.ai.pydantic import FuseeDurability, run


class Payments:
    """Refunds once per idempotency key."""

    def __init__(self):
        self.done = {}

    def refund(self, key, amount):
        self.done.setdefault(key, amount)
        return {"refunded": self.done[key]}

    def snapshot(self):
        return sorted(self.done.items())


def scripted(calls, fail_first=0):
    """A model that asks for one refund, then answers with the tool's output."""
    state = {"failures": fail_first}

    def model(messages, info: AgentInfo):
        calls.append(len(messages))
        if state["failures"] > 0:
            state["failures"] -= 1
            raise RuntimeError("the provider is down")
        for m in messages:
            for p in m.parts:
                if isinstance(p, ToolReturnPart):
                    return ModelResponse(parts=[TextPart(f"refunded {p.content['refunded']}")])
        return ModelResponse(parts=[ToolCallPart("refund", {"amount": 30}, tool_call_id="call-1")])

    return FunctionModel(model)


def support_with(payments, calls, **durability):
    agent = Agent(scripted(calls, durability.pop("fail_first", 0)), name="support",
                  capabilities=[FuseeDurability(**durability)])

    @agent.tool
    def refund(ctx, amount: int) -> dict:
        return payments.refund(ctx.tool_call_id, amount)

    @fusee.workflow("support")
    async def support(ctx, prompt):
        result = await run(ctx, lambda: agent.run(prompt))
        return result.output

    return support


async def runs_as_steps():
    payments, calls = Payments(), []
    wf = support_with(payments, calls)
    engine = await Engine.open(durability="memory", workflows=[wf], keep_history=True)
    try:
        r = await engine.start(wf, "s-1", "refund my order")
        assert await r.result() == "refunded 30"
        steps = [x["name"] for x in (await engine.run("s-1").describe()).journal if x["kind"] == "step"]
        assert steps == ["support__model.request", "support__function_toolset__<agent>.call_tool:refund",
                         "support__model.request"], steps
        assert calls == [1, 3], calls
        assert payments.snapshot() == [("call-1", 30)]
    finally:
        await engine.close()


async def crash_anywhere():
    def build():
        payments = Payments()
        return Case([support_with(payments, [])], "support", "s-2", "refund my order", payments.snapshot)

    await crash_everywhere(build)


async def restart_resumes_without_asking_again():
    d = tempfile.mkdtemp(prefix="fusee-py-pydantic-")
    payments, calls = Payments(), []
    try:
        engine = await Engine.open(d, workflows=[support_with(payments, calls)])
        r = await engine.start("support", "s-3", "refund my order")
        assert await r.result() == "refunded 30"
        await engine.close()
        # The finished run's steps are recorded: reading it again calls nothing.
        engine = await Engine.open(d, workflows=[support_with(payments, calls)])
        assert await engine.run("s-3").result() == "refunded 30"
        assert calls == [1, 3], calls
        await engine.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


async def model_failures_are_step_retries_and_can_pause():
    payments, calls = Payments(), []
    wf = support_with(payments, calls, fail_first=1, model_options={"retry": Retry(attempts=2, initial=1)})
    engine = await Engine.open(durability="memory", workflows=[wf], keep_history=True)
    try:
        r = await engine.start(wf, "s-4", "refund my order")
        assert await r.result() == "refunded 30"
        kinds = [x["kind"] for x in (await engine.run("s-4").describe()).journal]
        assert kinds.count("attempt") == 1, kinds
    finally:
        await engine.close()
    payments, calls = Payments(), []
    wf = support_with(payments, calls, fail_first=2,
                      model_options={"retry": Retry(attempts=2, initial=1, on_exhausted="pause")})
    engine = await Engine.open(durability="memory", workflows=[wf])
    try:
        r = await engine.start(wf, "s-5", "refund my order")
        for _ in range(400):
            if (await r.status()).status == "paused":
                break
            await asyncio.sleep(0.005)
        assert (await r.status()).status == "paused"
        await r.resume()
        assert await r.result() == "refunded 30"
    finally:
        await engine.close()


async def structured_output_and_outside_a_workflow():
    class Refund(BaseModel):
        amount: int
        reason: str

    def model(messages, info: AgentInfo):
        tool = info.output_tools[0]
        return ModelResponse(parts=[ToolCallPart(tool.name, {"amount": 30, "reason": "late"})])

    agent = Agent(FunctionModel(model), name="classify", output_type=Refund, capabilities=[FuseeDurability()])

    @fusee.workflow("classify")
    async def classify(ctx, text):
        r = await run(ctx, lambda: agent.run(text))
        return r.output.model_dump()

    engine = await Engine.open(durability="memory", workflows=[classify])
    try:
        r = await engine.start(classify, "c-1", "my order was late")
        assert await r.result() == {"amount": 30, "reason": "late"}
    finally:
        await engine.close()
    # Outside a workflow the agent is an ordinary agent.
    assert (await agent.run("my order was late")).output == Refund(amount=30, reason="late")


TESTS = (runs_as_steps, crash_anywhere, restart_resumes_without_asking_again,
         model_failures_are_step_retries_and_can_pause, structured_output_and_outside_a_workflow)


def test_pydantic_ai():
    if pydantic_ai is None:
        print("skipped: pydantic-ai is not installed")
        return
    for t in TESTS:
        asyncio.run(t())


if __name__ == "__main__":
    if pydantic_ai is None:
        print("skipped: pydantic-ai is not installed")
        sys.exit(0)
    for t in TESTS:
        asyncio.run(t())
        print("ok", t.__name__)
