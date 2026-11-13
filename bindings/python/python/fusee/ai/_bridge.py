"""Running an agent framework's code inside a workflow.

Workflow code may only await ``ctx`` operations, and frameworks like
Pydantic AI and the OpenAI Agents SDK need a real asyncio task (anyio, task
groups, locks). So the framework runs in a task of its own on the engine's
loop, and asks the workflow for each step, clock reading and sleep; the
workflow makes them in the order they come and hands back what they
recorded. A replay asks for the same operations in the same order, as long
as the framework's own code decides only on what they return.
"""

from __future__ import annotations

import asyncio
import contextvars
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any, Optional, TypeVar

from .._runtime import Ctx, _Op

T = TypeVar("T")


class Bridge:
    """Between the framework's task and the workflow's code."""

    def __init__(self, ctx: Ctx) -> None:
        self.ctx = ctx
        self.loop = asyncio.get_running_loop()
        self.asks: deque = deque()
        self.wake: Optional[_Op] = None
        self.sleeps = 0

    @property
    def ended(self) -> bool:
        return self.ctx._x.ended

    async def ask(self, kind: str, *args: Any) -> Any:
        """From the framework's task: has the workflow make an operation."""
        if self.ended:
            # The run ended (or its engine closed) under the framework.
            raise asyncio.CancelledError()
        fut = self.loop.create_future()
        self.asks.append((kind, args, fut, contextvars.copy_context()))
        self.notify()
        return await fut

    async def step(self, name: str, body: Callable[[], Awaitable[Any]], options: Optional[dict] = None) -> Any:
        """A step whose function is ``body``, run with the asker's context."""
        return await self.ask("step", name, body, dict(options or {}))

    def notify(self) -> None:
        w = self.wake
        if w is not None and not w.done:
            self.ctx._x.settle_local(w)

    async def serve(self, task: "asyncio.Task[T]") -> T:
        """From workflow code: answers the framework's asks until it returns."""
        task.add_done_callback(lambda _t: self.notify())
        try:
            while True:
                while self.asks:
                    kind, args, fut, context = self.asks.popleft()
                    try:
                        value = await self.answer(kind, args, context)
                    except BaseException as e:  # noqa: BLE001 - the framework's call fails with it
                        if not fut.done():
                            fut.set_exception(e)
                        if isinstance(e, (KeyboardInterrupt, SystemExit)):
                            raise
                    else:
                        if not fut.done():
                            fut.set_result(value)
                if task.done() and not self.asks:
                    return task.result()
                # Nothing to answer: wait for the framework to ask or finish.
                # The wait is not journaled; what the framework asks next is.
                self.wake = self.ctx._x.local_op()
                await self.wake
        finally:
            if not task.done():
                task.cancel()

    async def answer(self, kind: str, args: tuple, context: contextvars.Context) -> Any:
        ctx = self.ctx
        if kind == "step":
            name, body, options = args

            async def unit() -> Any:
                # The step's function runs as the asker's code would.
                return await context.run(asyncio.ensure_future, body())

            return await ctx.step(name, unit, **options)
        if kind == "now":
            return ctx.now()
        if kind == "uuid":
            return ctx.uuid()
        if kind == "sleep":
            (seconds,) = args
            self.sleeps += 1
            await ctx.sleep(f"agent.sleep#{self.sleeps}", seconds * 1000)
            return None
        raise ValueError(f"no such ask: {kind}")


BRIDGE: contextvars.ContextVar[Optional[Bridge]] = contextvars.ContextVar("fusee_ai_bridge", default=None)


def current() -> Optional[Bridge]:
    """The bridge of the workflow the calling code runs for, if any."""
    b = BRIDGE.get()
    return None if b is None or b.ended else b


async def run(ctx: Ctx, call: Callable[[], Awaitable[T]]) -> T:
    """Runs ``call`` in a task of its own as part of the workflow, answering
    its asks with ``ctx`` operations."""
    b = Bridge(ctx)
    token = BRIDGE.set(b)
    try:
        task = asyncio.ensure_future(call())
    finally:
        BRIDGE.reset(token)
    return await b.serve(task)
