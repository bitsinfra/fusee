"""fusee.ai.openai with the OpenAI Agents SDK itself (its Agent, Runner,
function tools and model interface). Skipped when openai-agents is not
installed."""

import asyncio
import shutil
import sys
import tempfile

try:
    import agents
except ImportError:
    agents = None

import fusee
from fusee import Engine, Retry
from fusee.testing import Case, crash_everywhere

if agents is not None:
    from agents import Agent, ModelResponse, RunConfig, Usage, function_tool, set_tracing_disabled
    from agents.models.interface import Model
    from openai.types.responses import ResponseFunctionToolCall, ResponseOutputMessage, ResponseOutputText

    from fusee.ai.openai import durable_agent, run

    set_tracing_disabled(True)


class Payments:
    """Refunds once per idempotency key."""

    def __init__(self):
        self.done = {}

    def refund(self, key, amount):
        self.done.setdefault(key, amount)
        return f"refunded {self.done[key]}"

    def snapshot(self):
        return sorted(self.done.items())


if agents is not None:
    class Scripted(Model):
        """Asks for one refund, then answers with the tool's output."""

        def __init__(self, calls, fail_first=0):
            self.calls = calls
            self.failures = fail_first

        async def get_response(self, system_instructions, input, model_settings, tools, output_schema,
                               handoffs, tracing, *, previous_response_id, conversation_id, prompt):
            self.calls.append(len(input) if isinstance(input, list) else 1)
            if self.failures > 0:
                self.failures -= 1
                raise RuntimeError("the provider is down")
            outputs = [i for i in input if isinstance(i, dict) and i.get("type") == "function_call_output"] \
                if isinstance(input, list) else []
            if outputs:
                out = [ResponseOutputMessage(id="msg_1", type="message", role="assistant", status="completed",
                                             content=[ResponseOutputText(type="output_text", annotations=[],
                                                                         text=f"done: {outputs[0]['output']}")])]
            else:
                out = [ResponseFunctionToolCall(type="function_call", id="fc_1", call_id="call-1",
                                                name="refund", arguments='{"amount": 30}')]
            return ModelResponse(output=out, usage=Usage(requests=1), response_id="resp_1")

        def stream_response(self, *args, **kwargs):
            raise NotImplementedError

    class Replies(Model):
        """Answers each call with what ``reply(input)`` gives: a tool call
        ``(name, arguments)`` or a text."""

        def __init__(self, reply):
            self.reply = reply

        async def get_response(self, system_instructions, input, model_settings, tools, output_schema,
                               handoffs, tracing, *, previous_response_id, conversation_id, prompt):
            r = self.reply(input if isinstance(input, list) else [{"role": "user", "content": input}])
            if isinstance(r, tuple):
                out = [ResponseFunctionToolCall(type="function_call", id=f"fc_{r[0]}", call_id=f"call-{r[0]}",
                                                name=r[0], arguments=r[1])]
            else:
                out = [ResponseOutputMessage(id="msg_1", type="message", role="assistant", status="completed",
                                             content=[ResponseOutputText(type="output_text", annotations=[], text=r)])]
            return ModelResponse(output=out, usage=Usage(requests=1), response_id="resp_1")

        def stream_response(self, *args, **kwargs):
            raise NotImplementedError

    def tool_output(input, name):
        for i in input:
            if isinstance(i, dict) and i.get("type") == "function_call_output" and i.get("call_id") == f"call-{name}":
                return i["output"]
        return None


def support_with(payments, calls, fail_first=0, **options):
    @function_tool
    def refund(ctx: agents.RunContextWrapper, amount: int) -> str:
        return payments.refund(ctx.tool_call_id, amount)

    agent = Agent(name="support", instructions="Refund orders.", tools=[refund],
                  model=Scripted(calls, fail_first))

    @fusee.workflow("support")
    async def support(ctx, prompt):
        result = await run(ctx, agent, prompt, **options)
        return result.final_output

    return support


async def runs_as_steps():
    payments, calls = Payments(), []
    wf = support_with(payments, calls)
    engine = await Engine.open(durability="memory", workflows=[wf], keep_history=True)
    try:
        r = await engine.start(wf, "o-1", "refund my order")
        assert await r.result() == "done: refunded 30"
        steps = [x["name"] for x in (await engine.run("o-1").describe()).journal if x["kind"] == "step"]
        assert steps == ["model", "tool:refund", "model"], steps
        assert len(calls) == 2, calls
        assert payments.snapshot() == [("call-1", 30)]
    finally:
        await engine.close()


async def crash_anywhere():
    def build():
        payments = Payments()
        return Case([support_with(payments, [])], "support", "o-2", "refund my order", payments.snapshot)

    await crash_everywhere(build)


async def restart_and_retries():
    d = tempfile.mkdtemp(prefix="fusee-py-openai-")
    payments, calls = Payments(), []
    try:
        engine = await Engine.open(d, workflows=[support_with(payments, calls, fail_first=1,
                                                              model_options={"retry": Retry(attempts=3, initial=1)})])
        r = await engine.start("support", "o-3", "refund my order")
        assert await r.result() == "done: refunded 30"
        assert len(calls) == 3, calls
        await engine.close()
        engine = await Engine.open(d, workflows=[support_with(payments, calls)])
        assert await engine.run("o-3").result() == "done: refunded 30"
        assert len(calls) == 3, calls
        await engine.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


async def pauses_and_resumes():
    payments, calls = Payments(), []
    wf = support_with(payments, calls, fail_first=2,
                      model_options={"retry": Retry(attempts=2, initial=1, on_exhausted="pause")})
    engine = await Engine.open(durability="memory", workflows=[wf])
    try:
        r = await engine.start(wf, "o-4", "refund my order")
        for _ in range(400):
            if (await r.status()).status == "paused":
                break
            await asyncio.sleep(0.005)
        assert (await r.status()).status == "paused"
        await r.resume()
        assert await r.result() == "done: refunded 30"
    finally:
        await engine.close()


async def handoffs_structured_outputs_and_outside_a_workflow():
    looked_up = []

    @function_tool
    def lookup(order: str) -> agents.ToolOutputText:
        looked_up.append(order)
        return agents.ToolOutputText(text=f"{order} shipped")

    def support_reply(input):
        out = tool_output(input, "lookup")
        if out is None:
            return ("lookup", '{"order": "o-9"}')
        return f"support: {out[0]['text'] if isinstance(out, list) else out}"

    def triage_reply(input):
        return ("transfer_to_support", "{}")

    support = Agent(name="support", tools=[lookup], model=Replies(support_reply))
    triage = Agent(name="triage", handoffs=[support], model=Replies(triage_reply))

    @fusee.workflow("triage")
    async def wf(ctx, prompt):
        r = await run(ctx, triage, prompt)
        return [r.last_agent.name, r.final_output]

    engine = await Engine.open(durability="memory", workflows=[wf], keep_history=True)
    try:
        r = await engine.start(wf, "t-1", "where is o-9?")
        assert await r.result() == ["support", "support: o-9 shipped"]
        steps = [x["name"] for x in (await engine.run("t-1").describe()).journal if x["kind"] == "step"]
        assert steps == ["model", "model", "tool:lookup", "model"], steps
        assert looked_up == ["o-9"]
    finally:
        await engine.close()
    # Outside a workflow the agent is an ordinary agent.
    r = await agents.Runner.run(durable_agent(triage), "where is o-9?")
    assert r.final_output == "support: o-9 shipped"


TESTS = (runs_as_steps, crash_anywhere, restart_and_retries, pauses_and_resumes,
         handoffs_structured_outputs_and_outside_a_workflow)


def test_openai_agents():
    if agents is None:
        print("skipped: openai-agents is not installed")
        return
    for t in TESTS:
        asyncio.run(t())


if __name__ == "__main__":
    if agents is None:
        print("skipped: openai-agents is not installed")
        sys.exit(0)
    for t in TESTS:
        asyncio.run(t())
        print("ok", t.__name__)
