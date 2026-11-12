"""The test kit: an engine for tests, with step mocks and checks.

::

    from fusee.testing import TestEngine

    async def test_checkout():
        t = await TestEngine.open(workflows=[checkout])
        t.mock_step(checkout, "charge", lambda attempt: {"id": "ch_1"})
        run = await t.engine.start(checkout, "order-1", order)
        assert (await run.result()).charge == "ch_1"
        assert await t.steps("order-1") == ["quote", "charge"]
        await t.check()          # strict replay of every run
        await t.close()

The engine is in memory, under the dev profile (a stuck run's result raises
``DeterminismError``), and serves nothing. Mocks replace a step's function
by workflow type and step name; a mock that raises fails that attempt, which
is retried under the step's policy.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

from . import _native
from ._codec import ms
from ._defs import name_of
from ._engine import _QUIET, Engine
from ._errors import DeterminismError
from ._replay import Replayer
from ._runtime import TEST_OFFSET as _CLOCK
from ._runtime import Ctx, takes_arg

__all__ = ["TestEngine", "Case", "crash_everywhere"]


class _MockingCtx(Ctx):
    """A workflow context whose steps look up the test engine's mocks, and
    count the step functions running (time skips only when none is)."""

    def step(self, name: str, fn: Callable[..., Any], **options: Any) -> Any:  # type: ignore[override]
        host = self._x.host
        mock = host._mocks.get((self._x.wtype, name))
        target = (lambda s: mock(s.attempt)) if mock is not None else fn
        wants = takes_arg(target)

        def counted(s):
            host._inflight += 1
            try:
                r = target(s) if wants else target()
            except BaseException:
                host._inflight -= 1
                raise
            if inspect.isawaitable(r):

                async def done():
                    try:
                        return await r
                    finally:
                        host._inflight -= 1

                return done()
            host._inflight -= 1
            return r

        return super().step(name, counted, **options)


class TestEngine:
    """An engine for tests (see the module documentation)."""

    __test__ = False  # not a pytest test class

    def __init__(self, engine: Engine, workflows: list, objects: list, shards: int, options: dict):
        self.engine = engine
        self._workflows = workflows
        self._objects = objects
        self._shards = shards
        self._options = options
        self._mocks: dict = {}
        self._prepare(engine)

    def _prepare(self, engine: Engine) -> None:
        engine._ctx_class = _MockingCtx
        engine._mocks = self._mocks
        engine._inflight = 0

    @staticmethod
    async def _open(workflows: list, objects: list, shards: int, options: dict) -> Engine:
        quiet = _QUIET.set(True)
        try:
            return await Engine.open(durability="memory", profile="dev", http="off", workflows=workflows,
                                     objects=objects, shards=shards, **options)
        finally:
            _QUIET.reset(quiet)

    @classmethod
    async def open(cls, *, workflows: Iterable[Any] = (), objects: Iterable[Any] = (), shards: int = 1,
                   **options: Any) -> "TestEngine":
        workflows, objects = list(workflows), list(objects)
        engine = await cls._open(workflows, objects, shards, options)
        return cls(engine, workflows, objects, shards, options)

    # -------------------------------------------------------------- time

    def now_ms(self) -> int:
        """The virtual time, Unix ms."""
        return int(time.time() * 1000) + _CLOCK[0]

    async def advance(self, duration: Any) -> None:
        """Moves the clock forward: timers that fall due fire."""
        _CLOCK[0] += int(ms(duration))
        _native.set_test_clock(_CLOCK[0])
        self.engine._kick()
        await asyncio.sleep(0)

    async def _skip(self) -> None:
        """While no step function runs, moves the clock to the next timer."""
        if self.engine._inflight:
            return
        at = self.engine._native.next_timer_ms()
        if at is None:
            return
        now = self.now_ms()
        if at > now:
            await self.advance(at - now)
        else:
            self.engine._kick()

    async def start(self, workflow: Any, id: str, input: Any = None, **options: Any) -> Any:
        return await self.engine.start(workflow, id, input, **options)

    def run(self, id: str, workflow: Any = None) -> Any:
        return self.engine.run(id, workflow)

    def mock_step(self, workflow: Any, step: str, fn: Callable[[int], Any]) -> None:
        """Replaces the step ``step`` of ``workflow`` (a definition or type
        name): ``fn(attempt)`` returns its result, or raises to fail that
        attempt (a ``fusee.Fatal`` is not retried)."""
        self.engine._mocks[(name_of(workflow), step)] = fn

    async def result(self, id: str, workflow: Any = None) -> Any:
        """The run's result (decoded as ``workflow``'s output type when given).
        While every run waits on a timer, time skips to it, so a workflow that
        sleeps a day finishes at once."""
        fut = asyncio.ensure_future(self.engine.run(id, workflow).result())
        while True:
            done, _ = await asyncio.wait({fut}, timeout=0.002)
            if done:
                return fut.result()
            await self._skip()

    async def crash(self, *, cut: Optional[tuple] = None) -> None:
        """Drops the engine's memory as a crash would and recovers: a new
        engine loads every run from its journal, the code replays, and runs
        continue. Recorded steps do not run again. (``cut=(id, k)``: that
        run's journal ends before its k-th step's record.)"""
        old = self.engine
        files = []
        async for r in old.list():
            if r.type.startswith("fusee."):
                continue
            data = old.export_journal(r.id)
            if data:
                files.append((r.started_ms, data, cut[1] if cut and cut[0] == r.id else None))
        await old.close()
        engine = await self._open(self._workflows, self._objects, self._shards, self._options)
        self._prepare(engine)
        for _, data, before in sorted(files, key=lambda f: f[0]):
            engine._native.import_journal(data, before)
        engine._kick()
        self.engine = engine

    async def steps(self, id: str) -> list:
        """The names of the steps the run recorded, in order."""
        return (await self.engine.run(id).describe()).steps

    async def snapshot(self, id: str) -> list:
        """The run's journal without times or run IDs, for snapshot tests."""
        return (await self.engine.run(id).describe()).journal

    async def check(self) -> None:
        """Strict replay of every run against the registered code: raises
        ``DeterminismError`` naming each run and step that differ."""
        found = []
        async for r in self.engine.list():
            if r.type.startswith("fusee."):
                continue
            data = self.engine.export_journal(r.id)
            if not data:
                continue
            rep = await Replayer(self._workflows).replay(data)
            err = rep.error
            if err and not err.startswith("the export holds only") and not err.startswith("no code registered"):
                found.append(f"{r.id} ({r.type}): {err}")
        if found:
            raise DeterminismError("; ".join(found))

    async def close(self) -> None:
        await self.engine.close()
        _CLOCK[0] = 0
        _native.set_test_clock(0)

    async def __aenter__(self) -> "TestEngine":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()



@dataclass
class Case:
    """One run for :func:`crash_everywhere`: the workflows (with fresh fakes),
    the run to start, and how to read the outside effects (a fake service's
    calls)."""

    workflows: list
    workflow: Any
    id: str
    input: Any = None
    effects: Callable[[], Any] = field(default=lambda: None)


async def crash_everywhere(build: Callable[[], Case]) -> None:
    """Runs a workflow once per step boundary with a crash at each: the
    step's function ran, its result was not recorded, the engine recovered.
    Each time the result and the outside effects must equal those of a run
    without a crash; a step that is not idempotent (charges twice when run
    again) is found and named (``AssertionError``).

    ``build()`` makes the workflows with fresh fakes and returns the Case::

        def build():
            payments = FakePayments()
            return Case([checkout_with(payments)], "checkout", "o-1", order, payments.snapshot)

        await crash_everywhere(build)
    """
    case = build()
    t = await TestEngine.open(workflows=case.workflows)
    await t.start(case.workflow, case.id, case.input)
    expected = await t.result(case.id)
    effects = case.effects()
    steps = await t.steps(case.id)
    await t.close()
    for k, step in enumerate(steps):
        case = build()
        t = await TestEngine.open(workflows=case.workflows)
        try:
            await t.start(case.workflow, case.id, case.input)
            await t.result(case.id)
            await t.crash(cut=(case.id, k))
            error = None
            try:
                got = await t.result(case.id)
            except Exception as e:  # noqa: BLE001 - reported below
                got, error = None, e
            now = case.effects()
        finally:
            await t.close()
        at = f"a crash after step '{step}' ran and before its result was recorded"
        if error is not None:
            raise AssertionError(f"{at} failed the run: {error}")
        if got != expected:
            raise AssertionError(f"{at} changed the result: {expected!r} without a crash, {got!r} with one")
        if now != effects:
            raise AssertionError(
                f"{at} changed the outside effects: {effects!r} without a crash, {now!r} with one. A step that "
                "runs again must not repeat its effect: pass s.key to the service as an idempotency key")
