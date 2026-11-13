"""Durable agents on the OpenAI Agents SDK: every model call and every
function tool call is a step, so an agent resumes after a crash at the call it
was making, and nothing it finished runs again::

    from agents import Agent, function_tool
    from fusee.ai.openai import run

    agent = Agent(name="support", instructions="...", tools=[refund])

    @fusee.workflow("support")
    async def support(ctx, question: str) -> str:
        result = await run(ctx, agent, question)
        return result.final_output

``run`` is ``Runner.run`` with the agent (and the agents it hands off to)
made durable: model calls are steps named ``model``, function tool calls
``tool:<name>``. Pass the tool call's ID (``ToolContext.tool_call_id``) to
services that take an idempotency key, as a step passes ``s.key``.

The agent runs on the engine's event loop in a task of its own and asks the
workflow for each step, which the workflow makes in the order they come.
Tool calls run one at a time. A tool's result is recorded as JSON: the SDK's
structured outputs (``ToolOutputText``, ``ToolOutputImage``,
``ToolOutputFileContent``) as the dicts it also takes, anything else JSON
cannot hold as the text the SDK sends the model. Handoffs to ``Agent``
objects are made durable; ``Handoff`` objects keep their agents as they are.
Hosted tools (web search, file search) run inside the model call.

Needs ``openai-agents`` 0.24 or later (``pip install 'fusee[openai-agents]'``).
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import AsyncIterator
from typing import Any, Optional

try:
    import pydantic
    from agents import Agent, RunConfig, Runner
    from agents.items import ModelResponse, TResponseStreamEvent
    from agents.models.interface import Model, ModelProvider
    from agents.tool import FunctionTool, ToolOutputFileContent, ToolOutputImage, ToolOutputText
except ImportError as e:  # pragma: no cover - depends on the environment
    raise ImportError(
        "fusee.ai.openai needs openai-agents 0.24 or later: pip install 'fusee[openai-agents]'"
    ) from e

from .._runtime import Ctx
from . import _bridge

__all__ = ["DurableModel", "DurableModelProvider", "durable_agent", "run"]

_RESPONSE = pydantic.TypeAdapter(ModelResponse)
_EVENTS = pydantic.TypeAdapter(list[TResponseStreamEvent])


class DurableModel(Model):
    """A model whose calls are steps inside ``run``, and plain calls outside."""

    def __init__(self, inner: Model, options: Optional[dict] = None) -> None:
        self.inner = inner
        self.options = dict(options or {})

    async def get_response(self, system_instructions, input, model_settings, tools, output_schema,  # noqa: A002, ANN001
                           handoffs, tracing, *, previous_response_id, conversation_id, prompt) -> ModelResponse:
        def call():
            return self.inner.get_response(
                system_instructions, input, model_settings, tools, output_schema, handoffs, tracing,
                previous_response_id=previous_response_id, conversation_id=conversation_id, prompt=prompt)

        b = _bridge.current()
        if b is None:
            return await call()

        async def body() -> Any:
            return _RESPONSE.dump_python(await call(), mode="json")

        return _RESPONSE.validate_python(await b.step("model", body, self.options))

    async def stream_response(self, system_instructions, input, model_settings, tools, output_schema,  # noqa: A002, ANN001
                              handoffs, tracing, *, previous_response_id, conversation_id,
                              prompt) -> AsyncIterator[TResponseStreamEvent]:
        def call():
            return self.inner.stream_response(
                system_instructions, input, model_settings, tools, output_schema, handoffs, tracing,
                previous_response_id=previous_response_id, conversation_id=conversation_id, prompt=prompt)

        b = _bridge.current()
        if b is None:
            async for event in call():
                yield event
            return

        async def body() -> Any:
            return _EVENTS.dump_python([e async for e in call()], mode="json")

        for event in _EVENTS.validate_python(await b.step("model", body, self.options)):
            yield event

    def get_retry_advice(self, request):  # noqa: ANN001, ANN201
        return self.inner.get_retry_advice(request)

    async def close(self) -> None:
        await self.inner.close()


class DurableModelProvider(ModelProvider):
    """A provider whose models are ``DurableModel``s."""

    def __init__(self, inner: ModelProvider, options: Optional[dict] = None) -> None:
        self.inner = inner
        self.options = dict(options or {})

    def get_model(self, model_name: Optional[str]) -> Model:
        model = self.inner.get_model(model_name)
        return model if isinstance(model, DurableModel) else DurableModel(model, self.options)


def _encode(value: Any) -> Any:
    """A tool's result as JSON a step records, which the SDK reads as it reads
    the result itself."""
    if isinstance(value, (ToolOutputText, ToolOutputImage, ToolOutputFileContent)):
        return value.model_dump(mode="json", exclude_none=True)
    if isinstance(value, (list, tuple)) and value and all(
            isinstance(v, (ToolOutputText, ToolOutputImage, ToolOutputFileContent)) for v in value):
        return [_encode(v) for v in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (dict, list)):
        try:
            return json.loads(json.dumps(value))
        except (TypeError, ValueError):
            pass
    return str(value)


def _durable_tool(tool: FunctionTool, options: dict) -> FunctionTool:
    original = tool.on_invoke_tool
    name = f"tool:{tool.name}"

    async def invoke(tool_ctx, arguments: str) -> Any:  # noqa: ANN001
        b = _bridge.current()
        if b is None:
            return await original(tool_ctx, arguments)

        async def body() -> Any:
            return _encode(await original(tool_ctx, arguments))

        return await b.step(name, body, options)

    return dataclasses.replace(tool, on_invoke_tool=invoke)


def durable_agent(agent: Agent, *, model_options: Optional[dict] = None,
                  tool_options: Optional[dict] = None) -> Agent:
    """A copy of ``agent`` (and of the ``Agent`` objects it hands off to)
    whose function tools and model, when it is a ``Model`` object, make their
    calls as steps inside ``run``."""
    copies: dict[int, Agent] = {}

    def copy(a: Agent) -> Agent:
        if id(a) in copies:
            return copies[id(a)]
        model = a.model
        if isinstance(model, Model) and not isinstance(model, DurableModel):
            model = DurableModel(model, model_options)
        tools = [_durable_tool(t, dict(tool_options or {})) if isinstance(t, FunctionTool) else t for t in a.tools]
        c = a.clone(model=model, tools=tools)
        copies[id(a)] = c
        c.handoffs = [copy(h) if isinstance(h, Agent) else h for h in a.handoffs]
        return c

    return copy(agent)


async def run(ctx: Ctx, agent: Agent, input: Any, *, run_config: Optional[RunConfig] = None,  # noqa: A002
              model_options: Optional[dict] = None, tool_options: Optional[dict] = None, **kwargs: Any) -> Any:
    """``Runner.run(agent, input, ...)`` as part of the workflow, with the
    agent's model and function tool calls made as steps. ``model_options`` and
    ``tool_options`` are the steps' options, as ``ctx.step`` takes them:
    ``retry`` (a ``fusee.Retry``, e.g. with ``on_exhausted="pause"``),
    ``timeout``, ``limit``. Returns the ``RunResult``."""
    durable = durable_agent(agent, model_options=model_options, tool_options=tool_options)
    config = run_config or RunConfig()
    model = config.model
    if isinstance(model, Model) and not isinstance(model, DurableModel):
        model = DurableModel(model, model_options)
    config = dataclasses.replace(
        config, model=model, model_provider=DurableModelProvider(config.model_provider, model_options))
    return await _bridge.run(ctx, lambda: Runner.run(durable, input, run_config=config, **kwargs))
