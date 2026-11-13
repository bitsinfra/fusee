"""Durable Pydantic AI agents: every model request and every tool call is a
step, so an agent resumes after a crash at the call it was making, and
nothing it finished runs again::

    from pydantic_ai import Agent
    from fusee.ai.pydantic import FuseeDurability, run

    agent = Agent("openai:gpt-5", name="support", capabilities=[FuseeDurability()])

    @fusee.workflow("support")
    async def support(ctx, question: str) -> str:
        result = await run(ctx, lambda: agent.run(question))
        return result.output

The agent is Pydantic AI's own, with its tools, output types and models;
``FuseeDurability`` routes its I/O through steps as Pydantic AI's durable
engines do. Outside ``run`` the agent behaves as it always does.

The agent runs on the engine's event loop in a task of its own and asks the
workflow for each step, clock reading and sleep, which the workflow makes in
the order they come. Tool calls run one at a time, so a replay asks for the
same steps in the same order. Name the agent (``name=``) and give each
toolset an ``id``: they name the steps (``support__model.request``,
``support__function_toolset__<agent>.call_tool:refund``).

Needs ``pydantic-ai`` 2.68 or later (``pip install 'fusee[pydantic-ai]'``).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, TypeVar

try:
    from pydantic_ai import Agent
    from pydantic_ai.durable_exec import (
        JSON_CODEC,
        BaseDurabilityCapability,
        DurabilityEngineSpec,
        JournalCallableOperationBackend,
        RoleBasedOperationConfig,
        conversation_id_from_run_id,
    )
    from pydantic_ai.models import Model
except ImportError as e:  # pragma: no cover - depends on the environment
    raise ImportError(
        "fusee.ai.pydantic needs pydantic-ai 2.68 or later: pip install 'fusee[pydantic-ai]'"
    ) from e

from .._runtime import Ctx
from . import _bridge

__all__ = ["FuseeDurability", "run"]

T = TypeVar("T")


class _Backend(JournalCallableOperationBackend[dict]):
    async def execute(self, *, operation_id, name, body, cache_key, config) -> object:  # noqa: ANN001
        b = _bridge.current()
        if b is None:
            raise RuntimeError("a durable Pydantic AI operation outside fusee.ai.pydantic.run")
        return await b.step(name, body, config)


@dataclass(init=False, kw_only=True)
class FuseeDurability(BaseDurabilityCapability):
    """Makes an agent durable inside a Fusee workflow: its model requests, tool
    calls and toolset discovery are steps, named after the agent (``name=``)
    and each toolset's ``id``. Use it with ``run``.

    ``model_options`` and ``tool_options`` are the steps' options, as
    ``ctx.step`` takes them: ``retry`` (a ``fusee.Retry``, e.g. with
    ``on_exhausted="pause"``), ``timeout``, ``limit``. A tool's own options are
    its metadata's ``"fusee"`` entry (``False``: no step).
    """

    engine_spec = DurabilityEngineSpec(
        engine_name="Fusee",
        durable_unit_noun="step",
        durable_container_noun="workflow",
        codec=JSON_CODEC,
        wrapped_toolset_kinds=frozenset({"function", "mcp", "dynamic"}),
        toolset_lifecycles={"function": "enter-always", "mcp": "enter-in-durable-unit", "dynamic": "enter-never"},
        sequential_tools_in_durable_context=True,
        tool_config_key="fusee",
    )

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        models: Optional[Mapping[str, Model]] = None,
        model_options: Optional[dict] = None,
        tool_options: Optional[dict] = None,
    ) -> None:
        super().__init__(models=models, name=name)
        self._model_options = dict(model_options or {})
        self._tool_options = dict(tool_options or {})

    @property
    def in_durable_context(self) -> bool:
        return _bridge.current() is not None

    def get_default_run_id(self) -> Optional[str]:
        b = _bridge.current()
        if b is None:
            return None
        # A recorded value, so a replay gets the same run ID. The workflow
        # code is waiting for the agent, so this is its next operation.
        return f"{b.ctx.workflow_id}:{b.ctx.uuid()}"

    def get_default_conversation_id(self, run_id: str) -> Optional[str]:
        return conversation_id_from_run_id(run_id) if self.in_durable_context else None

    async def get_run_start_time(self, run_id: str) -> Optional[datetime]:
        b = _bridge.current()
        if b is None:
            return None
        return datetime.fromtimestamp(await b.ask("now") / 1000, timezone.utc)

    def get_durable_operation_backend(self) -> JournalCallableOperationBackend[dict]:
        def tool_options(_operation_id: object, tool: object, _tool_name: str) -> Any:
            meta = getattr(getattr(tool, "tool_def", None), "metadata", None) or {}
            own = meta.get("fusee") if isinstance(meta, dict) else None
            if own is False:
                return False
            return {**self._tool_options, **(own or {})}

        return _Backend(
            agent_name=self.name,
            default_model_id=self.default_model_id,
            config=RoleBasedOperationConfig(
                model=self._model_options,
                event={},
                capability={},
                tool=self._tool_options,
                resolve_tool=tool_options,
            ),
        )


async def run(ctx: Ctx, call: Callable[[], Awaitable[T]]) -> T:
    """Runs ``call`` (e.g. ``lambda: agent.run(prompt)``) as part of the
    workflow: the agents in it that have ``FuseeDurability`` make their I/O as
    steps, and their retry delays as durable sleeps. Returns what ``call``
    returns."""

    async def agent() -> T:
        async def sleep(seconds: float) -> None:
            b = _bridge.current()
            if b is not None:
                await b.ask("sleep", seconds)

        with Agent.using_sleep(sleep):
            return await call()

    return await _bridge.run(ctx, agent)
