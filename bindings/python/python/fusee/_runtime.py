"""Running workflow code: Fusee's deterministic scheduler, the workflow
context, the step context, objects and stream writers.

Workflow code runs under this scheduler, driven by the engine's tick: it
may only await ctx operations (or ``ctx.all`` / ``ctx.race`` of them and of
other workflow coroutines). Step functions run on the asyncio loop (or a
thread, with ``thread=True``) and may do anything.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import struct
import threading
import time
import uuid as _uuid
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional, TypeVar, Union, overload

from . import _codec
from ._codec import decode, encode, from_wire, ms, tagged, wire, wire_text
from ._defs import DEFAULT_RETRY, ObjectType, Query, Retry, Signal, Update, Workflow, _Shared, name_of
from ._errors import (
    CancelledError,
    ChildError,
    ClosedError,
    CodecError,
    DeterminismError,
    Failure,
    FuseeError,
    RejectedError,
    StepError,
    TerminatedError,
    TimeoutError,
    WorkflowError,
    error_of,
    failure_of,
    native_error,
)

if TYPE_CHECKING:
    from ._telemetry import Telemetry

T = TypeVar("T")
O = TypeVar("O")

OK, FAILED, CANCELLED, TIMED_OUT, TERMINATED, CONTINUED, RESET = 0, 1, 2, 3, 4, 5, 6
_DETACHED = 1 << 6

# ``step_done`` made no delivery (it returns a tuple, or None, when it did).
_UNDELIVERED = object()

#: A retry time meaning "until resumed" (the engine's ``u64::MAX``).
PAUSED = 2**64 - 1

# Delivery kinds the native quiescent returns first.
_D_RESOLVE, _D_HELD, _D_CANCEL, _D_UPDATE = 1, 2, 3, 4

#: The query every object answers with its committed state (``fusee::object::STATE_QUERY``).
STATE_QUERY = "fusee.state"
#: The handler every object type has that edits its state from outside
#: (``fusee::object::EDIT_HANDLER``).
EDIT_HANDLER = "fusee.state.edit"
#: Update IDs of one-way calls start with this (``fusee::object::SEND_PREFIX``).
SEND_PREFIX = "fusee.send/"

log = logging.getLogger("fusee")
# What `ctx.log` gives while a run replays its journal: the lines were
# written when the code first ran.
_replay_log = logging.getLogger("fusee.replaying")
_replay_log.disabled = True
_replay_log.propagate = False
_SILENT = logging.LoggerAdapter(_replay_log, {})


class TextFormatter(logging.Formatter):
    """Fusee's line format, as the Rust engine writes it: ``fusee: <message>``
    for the engine; for a workflow, the level, the workflow ID and step,
    then the message."""

    def format(self, record: logging.LogRecord) -> str:
        wid = getattr(record, "workflow_id", None)
        if wid is None:
            return f"fusee: {record.getMessage()}"
        step = getattr(record, "step", None)
        out = f"{record.levelname.lower().replace('warning', 'warn'):<5} {wid}"
        if step:
            out += f" step '{step}'"
        return f"{out}: {record.getMessage()}"


def show_logs() -> None:
    """Under the dev profile, when the application has not configured
    logging: workflow and engine records at INFO go to stderr."""
    if logging.getLogger().handlers or log.handlers:
        return
    h = logging.StreamHandler()
    h.setFormatter(TextFormatter())
    log.addHandler(h)
    log.setLevel(logging.INFO)


#: The test kit's virtual time: ms ahead of the wall clock (fusee.testing).
TEST_OFFSET = [0]


def now_ms() -> int:
    return int(time.time() * 1000) + TEST_OFFSET[0]


def key(shard: int, run: int) -> int:
    """One integer for (shard, run): shards are fewer than 1024."""
    return (run << 10) | shard


def close_bits(parent_close: str = "terminate") -> int:
    try:
        return {"terminate": 0, "request-cancel": 1, "abandon": 2}[parent_close] << 4
    except KeyError:
        raise RejectedError(f"parent_close is terminate, request-cancel or abandon, not {parent_close!r}") from None


def place(priority: Optional[int], fairness_key: Optional[str], fairness_weight: Optional[int]) -> tuple:
    """A step's ``(priority, fairness key, weight)`` among a limit's waiters."""
    p = 0 if priority is None else priority
    w = 0 if fairness_weight is None else fairness_weight
    if isinstance(p, bool) or not isinstance(p, int) or not -(2**31) <= p < 2**31:
        raise RejectedError(f"priority must be a 32-bit integer, not {priority!r}")
    if isinstance(w, bool) or not isinstance(w, int) or not 0 <= w < 2**32:
        raise RejectedError(f"fairness_weight must be a whole number, not {fairness_weight!r}")
    return (p, fairness_key or "", w)


def takes_arg(fn: Callable) -> bool:
    """Whether a step function declares a parameter for the step context:
    one without a default (``lambda i=i: ...`` declares none)."""
    code = getattr(fn, "__code__", None)
    if code is not None:
        n = code.co_argcount - len(getattr(fn, "__defaults__", None) or ())
        if getattr(fn, "__self__", None) is not None:
            n -= 1
        return n >= 1
    try:
        params = inspect.signature(fn).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(p.default is p.empty and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD) for p in params)


def outcome(error: BaseException, stacks: bool = True) -> tuple:
    """A run's (or update's) final ``(status, text)`` for an exception: a
    reason, or a failure's envelope."""
    if isinstance(error, CancelledError):
        return CANCELLED, error.reason
    if isinstance(error, TimeoutError):
        return TIMED_OUT, str(error)
    if isinstance(error, TerminatedError):
        return TERMINATED, str(error)
    return FAILED, failure_of(error, stacks).encode().decode()


# ------------------------------------------------------------------ the scheduler


class _Op:
    """A journal operation the workflow awaits."""

    __slots__ = ("kind", "name", "seq", "done", "value", "error", "waiters", "dec", "target")

    def __init__(self, kind: str, name: str, seq: int = -1, dec: Optional[Callable] = None,
                 target: Optional[str] = None):
        self.kind = kind
        self.name = name
        self.seq = seq
        self.done = False
        self.value: Any = None
        self.error: Optional[BaseException] = None
        self.waiters: list = []
        self.dec = dec
        self.target = target

    def __await__(self):
        if not self.done:
            yield self
        if self.error is not None:
            raise self.error
        return self.value


class _Never:
    """Awaited after continue-as-new: never resolves."""

    done = False

    def __await__(self):
        yield self


class _Group:
    """``ctx.all`` / ``ctx.race`` over ops and coroutines."""

    __slots__ = ("items", "race", "done", "value", "error", "waiters", "results", "left", "_started")

    def __init__(self, items: list, race: bool):
        self.items = items
        self.race = race
        self.done = False
        self.value: Any = None
        self.error: Optional[BaseException] = None
        self.waiters: list = []
        self.results: list = [None] * len(items)
        self.left = len(items)
        self._started = False

    def __await__(self):
        if not self.done:
            yield self
        if self.error is not None:
            raise self.error
        return self.value


class _Task:
    __slots__ = ("coro", "on_done")

    def __init__(self, coro, on_done: Callable[[Any, Optional[BaseException]], None]):
        self.coro = coro
        self.on_done = on_done


def _settle(target, value: Any, error: Optional[BaseException], ready: deque) -> None:
    if target.done:
        return
    target.done = True
    target.value = value
    target.error = error
    for t in target.waiters:
        ready.append(t)
    target.waiters.clear()


class Backend:
    """Where a run's journal operations go: the engine or a replayer."""

    def __init__(self, native, kick: Callable[[], None]):
        self.n = native
        self.kick = kick
        # Each shard's commit position as of the last tick (None on replay).
        self.commits: Optional[list] = None


_NO_CODE = "workflow code may only await ctx operations (step, sleep, wait_for, child, all, race)"


class RunExec:
    def __init__(self, host: "CodeHost", be: Backend, shard: int, run: int, ev: dict):
        self.host = host
        self.be = be
        self.n = be.n
        self.loop = host._loop
        self.shard = shard
        self.run = run
        self.id = ev["id"]
        self.wtype = ev["wtype"]
        self.version = ev["version"]
        self.started_ms = ev["started_ms"]
        self.pending: dict[int, _Op] = {}
        self.gated: dict[int, Callable[[], None]] = {}
        self.beats: dict[int, int] = {}
        self.queries: dict[str, tuple] = {}
        self.updates: Any = {}
        # Step functions running on the loop or a thread: cancelled when the
        # run is cancelled or ends.
        self.tasks: dict[int, asyncio.Future] = {}
        # JSON of step results this code recorded and not yet had delivered.
        self.held: dict[int, str] = {}
        # A step result recorded while the run's code ran, not yet given to
        # the engine: it goes with the delivery that follows,
        # or before any other call this run makes.
        self.held_done: Optional[tuple] = None
        self.ready: deque = deque()
        # A scheduler loop (run_ready or pump) is on the stack; and a
        # delivery was asked for meanwhile.
        self.scheduling = False
        self.deliver = False
        # Waiting in the engine tick's batch of deliveries (see Engine._pump_batch).
        self.batched = False
        self.ended = False
        # Waits for in-process work that is not journaled (``local_op``):
        # while there are any, the code takes no delivery, since what it
        # asks for next is not known yet.
        self.local_waits = 0
        # The run's `fusee.workflow` span and the context it is active in
        # (telemetry; see Telemetry._workflow).
        self.wf_span: Any = None
        self.wf_ctx: Any = None
        # The host's context class (the test kit's adds step mocks).
        self.ctx = host._ctx_class(self)

    def end(self) -> None:
        """The run's code stops: step functions still running are cancelled."""
        self.ended = True
        if self.tasks:
            for f in self.tasks.values():
                f.cancel()
            self.tasks.clear()

    def flush_done(self) -> None:
        """Gives the engine a held step result (see ``held_done``)."""
        held, self.held_done = self.held_done, None
        if held is not None:
            try:
                self.n.step_done(self.shard, self.run, held[0], True, held[1])
            except RuntimeError as e:
                raise native_error(e) from None

    def h(self, op: str, args: Optional[list] = None, bufs: Optional[list] = None) -> list:
        if threading.get_ident() != self.host._thread:
            # A step function running on a thread: the engine is the loop's.
            return self.host._on_loop(lambda: self.h(op, args, bufs))
        if self.held_done is not None:
            self.flush_done()
        if self.ended and op != "release":
            raise RejectedError("workflow execution has ended", code="Closed")
        a = [str(x) for x in (args or [])]
        try:
            return self.n.host_call(self.shard, op, self.run, a, list(bufs or []))
        except RuntimeError as e:
            raise native_error(e) from None

    def hb(self, op: str, args: Optional[list] = None, bufs: Optional[list] = None) -> Optional[bytes]:
        if threading.get_ident() != self.host._thread:
            return self.host._on_loop(lambda: self.hb(op, args, bufs))
        if self.held_done is not None:
            self.flush_done()
        if self.ended:
            raise ClosedError("workflow execution has ended")
        a = [str(x) for x in (args or [])]
        try:
            return self.n.host_bytes(self.shard, op, self.run, a, list(bufs or []))
        except RuntimeError as e:
            raise native_error(e) from None

    # Typed calls for the operations every step makes, in place of h().

    def begin_step(self, name: str, read_only: bool) -> tuple:
        """``(seq, replay, gate, attempt, not_before, open)``: the gate is
        checked here for a step that runs."""
        if self.ended:
            raise ClosedError("workflow execution has ended")
        if self.held_done is not None:
            self.flush_done()
        try:
            return self.n.begin_step(self.shard, self.run, name, read_only, self.host._inline_ok())
        except RuntimeError as e:
            raise native_error(e) from None

    def may_run(self, gate: int, not_before: int) -> bool:
        if self.ended:
            raise ClosedError("workflow execution has ended")
        # Open by the commit position the last tick reported: no call.
        commits = self.be.commits
        if commits is not None and gate <= commits[self.shard] and (not_before == 0 or not_before <= now_ms()):
            return True
        if self.held_done is not None:
            self.flush_done()
        try:
            return self.n.may_run(self.shard, self.run, gate, not_before, self.host._inline_ok())
        except RuntimeError as e:
            raise native_error(e) from None

    def step_done(self, seq: int, ok: bool, value: Union[str, bytes], deliver: bool = False):
        """Records a step's outcome. ``deliver``: the run takes its next
        delivery at once (see ``finished`` in ``run_attempts``); for a JSON
        result the native call makes it too and returns its tuple (or
        ``None``). Otherwise returns ``_UNDELIVERED``."""
        if self.ended:
            raise ClosedError("workflow execution has ended")
        if self.held_done is not None:
            self.flush_done()
        try:
            if ok and isinstance(value, str):
                # Kept: the delivery does not carry it back.
                self.held[seq] = value
                step_done_deliver = getattr(self.n, "step_done_deliver", None)
                if deliver and step_done_deliver is not None:
                    return step_done_deliver(self.shard, self.run, seq, True, value)
                if self.scheduling and hasattr(self.n, "quiescent_many_done"):
                    # The code is running: the delivery that follows this
                    # result comes once it blocks; record the result with it.
                    self.held_done = (seq, value)
                    return _UNDELIVERED
                self.n.step_done(self.shard, self.run, seq, True, value)
            elif ok:
                self.n.step_done_bytes(self.shard, self.run, seq, value)
            else:
                self.n.step_done(self.shard, self.run, seq, False,
                                 value if isinstance(value, str) else value.decode("utf-8", "replace"))
        except RuntimeError as e:
            raise native_error(e) from None
        return _UNDELIVERED

    def step_retry(self, seq: int, retry_at: int, error: bytes) -> int:
        if self.held_done is not None:
            self.flush_done()
        if self.ended:
            raise ClosedError("workflow execution has ended")
        try:
            return self.n.step_retry(self.shard, self.run, seq, retry_at, error)
        except RuntimeError as e:
            raise native_error(e) from None

    # The scheduler: run every runnable task until all are blocked.
    def run_ready(self) -> None:
        if self.scheduling:
            return  # the loop on the stack runs them
        self.scheduling = True
        try:
            self._drain()
        finally:
            self.scheduling = False
        if self.deliver:
            self.pump()

    def _drain(self) -> None:
        while self.ready and not self.ended:
            task = self.ready.popleft()
            self._resume(task)

    def _resume(self, task: _Task) -> None:
        exc: Optional[BaseException] = None
        while True:
            try:
                yielded = task.coro.throw(exc) if exc is not None else task.coro.send(None)
            except StopIteration as s:
                task.on_done(s.value, None)
                return
            except BaseException as e:  # noqa: BLE001 - delivered to the awaiting task
                task.on_done(None, e)
                return
            if isinstance(yielded, _Never):
                return
            if isinstance(yielded, (_Op, _Group)):
                if yielded.done:
                    self.ready.append(task)
                else:
                    yielded.waiters.append(task)
                    if isinstance(yielded, _Group) and not yielded._started:
                        self._start_group(yielded)
                return
            exc = FuseeError(_NO_CODE + f"; workflow '{self.id}' awaited {type(yielded).__name__}. Put other "
                             "awaits inside a step: await ctx.step(name, fn)", code="Determinism")

    def _forget_losers(self, g: _Group) -> None:
        for item in g.items:
            if isinstance(item, _Op) and not item.done and item.seq >= 0:
                self.forget(item.seq)

    def _start_group(self, g: _Group) -> None:
        g._started = True
        for i, item in enumerate(g.items):
            def done(v, e, i=i):
                if g.done:
                    return
                if g.race:
                    _settle(g, (i, v) if e is None else None, e, self.ready)
                    self._forget_losers(g)
                    return
                if e is not None:
                    _settle(g, None, e, self.ready)
                    return
                g.results[i] = v
                g.left -= 1
                if g.left == 0:
                    _settle(g, list(g.results), None, self.ready)

            if isinstance(item, (_Op, _Group)):
                async def wait(item=item):
                    return await item
                self.ready.append(_Task(wait(), done))
            elif inspect.iscoroutine(item):
                self.ready.append(_Task(item, done))
            else:
                done(item, None)
        if not g.items:
            _settle(g, [], None, self.ready)

    def forget(self, seq: int) -> None:
        if self.pending.pop(seq, None) is None:
            return
        self.gated.pop(seq, None)
        self.held.pop(seq, None)
        t = self.tasks.pop(seq, None)
        if t is not None:
            t.cancel()
        self.h("forget", [seq])

    def spawn_root(self, coro) -> None:
        def done(v, e):
            self.host._finished(self, v, e)

        self.ready.append(_Task(coro, done))
        self.run_ready()
        self.pump()

    def pump(self) -> None:
        """Delivers recorded or completed results one at a time; between
        deliveries the code runs until every task is blocked."""
        batch = self.host._batch
        if batch is not None and not self.scheduling:
            # Inside an engine tick: the tick delivers to every run that
            # asked, a round at a time (see Engine._pump_batch).
            if not self.batched:
                self.batched = True
                batch.append(self)
            return
        if self.scheduling:
            # Called from running code (a synchronous step finished inside
            # a task): the loop on the stack delivers once every task is
            # blocked, as replay does, instead of running other tasks
            # inside this one.
            self.deliver = True
            return
        self.scheduling = True
        try:
            self._pump()
        finally:
            self.scheduling = False
            self.deliver = False

    def _direct_pump(self) -> bool:
        """Whether ``pump`` would deliver at once: not inside a tick's
        batch, and no task of this run is running."""
        return self.host._batch is None and not self.scheduling and not self.ended

    def pump_from(self, first) -> None:
        """``pump``, its first delivery already made by ``step_done``."""
        self.scheduling = True
        try:
            self._pump(first)
        finally:
            self.scheduling = False
            self.deliver = False

    def local_op(self) -> _Op:
        """A wait for in-process work that is not journaled (an integration's
        task that asks the code for operations): settle it with
        ``settle_local``. Deliveries wait until it is settled."""
        self.local_waits += 1
        return _Op("local", "")

    def settle_local(self, op: _Op, value: Any = None, error: Optional[BaseException] = None) -> None:
        if op.done:
            return
        self.local_waits -= 1
        _settle(op, value, error, self.ready)
        self.run_ready()
        # Deliveries held back during the wait (a replay's recorded results).
        if not self.local_waits:
            self.pump()

    def _pump(self, first: Any = None) -> None:
        if first is None:
            first = _UNDELIVERED
        while not self.ended:
            if first is not _UNDELIVERED:
                d, first = first, _UNDELIVERED
            elif self.local_waits:
                break
            else:
                try:
                    held, self.held_done = self.held_done, None
                    d = (self.n.step_done_deliver(self.shard, self.run, held[0], True, held[1])
                         if held is not None else self.n.quiescent(self.shard, self.run))
                except RuntimeError as e:
                    if self._failed(e):
                        continue  # the code that ran finished a step
                    return
            if d is None:
                break
            self._apply(d)
            self._drain()
        self.be.kick()

    def _failed(self, e: RuntimeError) -> bool:
        """The engine refused a delivery: every pending operation fails.
        True if the code that then ran asked for another delivery."""
        err = native_error(e)
        for op in list(self.pending.values()):
            _settle(op, None, err, self.ready)
        self.pending.clear()
        self.deliver = False
        self._drain()
        return self.deliver

    def _apply(self, d: tuple) -> None:
        """One delivery: settle what it resolves (the code runs after)."""
        kind = d[0]
        if kind <= _D_HELD:
            seq = d[1]
            held = self.held.pop(seq, None)
            op = self.pending.pop(seq, None)
            if op is not None:
                if kind == _D_HELD:
                    if held is None:
                        _settle(op, None, FuseeError(f"the result of step {seq} was not kept"), self.ready)
                    else:
                        self._resolve(op, d[2], held)
                else:
                    self._resolve(op, d[2], d[3])
        elif kind == _D_CANCEL:
            reason = d[3]
            for s in d[4]:
                op = self.pending.pop(s, None)
                self.gated.pop(s, None)
                self.held.pop(s, None)
                t = self.tasks.pop(s, None)
                if t is not None:
                    t.cancel()
                if op is not None:
                    _settle(op, None, CancelledError(reason), self.ready)
        else:
            _, _, _, payload, ordinal, name, uid, token = d
            self._start_update(ordinal, name, uid, payload, token)

    def _start_update(self, ordinal: int, name: str, uid: str, payload, token: int) -> None:
        handler = self.updates.get(name)
        arg = from_wire(payload)
        if handler is not None and handler[2] is not None and arg is not None:
            try:
                arg = handler[2](arg)
            except Exception as e:  # noqa: BLE001 - refused below
                if token >= 0:
                    _call_decide(self, token, False, f"update '{name}': the argument does not match its type: {e}")
                    return
        if token >= 0:
            refusal = None
            if handler is None:
                refusal = f"workflow '{self.id}' has no handler for update '{name}'"
            elif handler[0] is not None:
                try:
                    v = handler[0](arg)
                    if v is False:
                        refusal = "refused by the validator"
                    elif isinstance(v, str):
                        refusal = v
                except Exception as e:  # noqa: BLE001 - the refusal reason
                    refusal = str(e) or type(e).__name__
            o = _call_decide(self, token, refusal is None, refusal or "")
            if refusal is not None or o < 0:
                return
            ordinal = o
        elif handler is None:
            msg = (
                f"determinism error: the journal delivers update '{name}' (id '{uid}') but the code "
                "registered no handler for it"
            )
            self.ended = True
            try:
                self.n.stuck(self.shard, self.run, msg)
            except RuntimeError as e:
                raise native_error(e) from None
            return
        stacks = self.host._stacks

        def done(v, e, ordinal=ordinal):
            if self.ended:
                return
            if e is None:
                try:
                    data, st = tagged(wire(v)), OK
                except (TypeError, ValueError) as err:
                    st, data = FAILED, Failure(str(_codec.codec_error(f"update '{name}''s result", v, err)),
                                               "Codec", retryable=False).encode()
            else:
                st, text = outcome(e, stacks)
                data = text.encode()
            self.h("updateDone", [ordinal, st], [data])
            self.be.kick()

        try:
            coro = handler[1](arg)
        except BaseException as e:  # noqa: BLE001 - the update fails
            done(None, e)
            return
        if not inspect.iscoroutine(coro):
            value = coro

            async def wrap():
                return value

            coro = wrap()
        self.ready.append(_Task(coro, done))

    def _resolve(self, op: _Op, status: int, payload: Union[None, str, bytes]) -> None:
        if status == OK:
            if op.kind == "sleep":
                _settle(op, None, None, self.ready)
                return
            try:
                value = from_wire(payload)
            except ValueError as e:
                _settle(op, None, CodecError(f"a recorded result is not JSON: {e}"), self.ready)
                return
            if op.dec is not None and value is not None:
                try:
                    value = op.dec(value)
                except Exception as e:  # noqa: BLE001 - the recorded value does not fit the type
                    _settle(op, None, CodecError(f"{op.kind} '{op.name}': the recorded value does not match "
                                                 f"its type: {e}"), self.ready)
                    return
            _settle(op, value, None, self.ready)
        elif status == FAILED:
            f = Failure.decode(payload)
            if op.kind in ("step", "await"):
                err: BaseException = StepError(op.name, f, workflow_id=self.id)
            elif op.kind == "child":
                err = ChildError(op.target or op.name, f)
            else:
                err = error_of(f)
            _settle(op, None, err, self.ready)
        elif status == CANCELLED:
            _settle(op, None, CancelledError(wire_text(payload)), self.ready)
        elif status == TIMED_OUT:
            if op.kind == "wait":
                _settle(op, None, None, self.ready)
            else:
                _settle(op, None, TimeoutError(wire_text(payload) or "timed out"), self.ready)
        else:
            _settle(op, None, TerminatedError(wire_text(payload)), self.ready)

    def wake(self) -> None:
        for retry in list(self.gated.values()):
            retry()
        self.pump()

    def run_attempts(self, seq: int, gate: int, attempt: int, not_before: int, fn, retry: Retry,
                     timeout, heartbeat_timeout, limit: Optional[str], done: Callable[[bool, Any], None],
                     read_only: bool = False, name: str = "", open_: Optional[bool] = None,
                     place: tuple = (), thread: bool = False, step_ctx: bool = False,
                     pausable: bool = True) -> None:
        """Runs a step's function (or an async completion's send function)
        under its gate, permit, timeouts and retry policy. A read-only step
        waits for no earlier record (gate 0 is always durable). ``open_``:
        what ``begin_step`` found for the first attempt's gate (``False``:
        shut, and the engine wakes the run when it opens). ``place``: the
        step's ``(priority, fairness key, weight)`` among the limit's
        waiters (empty: arrival order). ``step_ctx``: ``fn`` takes the step
        context."""
        loop = self.loop
        host = self.host
        # "later" is the one pending timer that calls try_run again: the
        # retry delay, or the rate limit's refill.
        if not_before < 0:
            # The native layer's "until resumed" arrives as a signed -1.
            not_before = PAUSED
        st = {"gate": gate, "attempt": attempt, "not_before": not_before, "ticket": None,
              "running": False, "settled": False, "timers": [], "later": None, "span": None,
              "known": open_, "first": None}
        tel = host._tel

        def end_span(error: Optional[BaseException]) -> None:
            a = st["span"]
            if a is not None:
                st["span"] = None
                a.end(None if error is None else (str(error) or type(error).__name__))

        def release() -> None:
            if st["ticket"] is not None and limit:
                self.h("release", [limit, st["ticket"]])
                st["ticket"] = None

        def cancel_later() -> None:
            if st["later"] is not None:
                st["later"].cancel()
                st["later"] = None

        def retry_at(at_ms: int) -> None:
            cancel_later()

            def fire() -> None:
                st["later"] = None
                try_run()

            st["later"] = loop.call_later(max(0.0, (at_ms - now_ms()) / 1000), fire)

        def finished(value: Any, error: Optional[BaseException]) -> None:
            st["running"] = False
            if st["timers"]:
                for t in st["timers"]:
                    t.cancel()
                st["timers"] = []
            if self.beats:
                self.beats.pop(seq, None)
            if self.tasks:
                self.tasks.pop(seq, None)
            release()
            if self.ended or seq not in self.pending or st["settled"]:
                end_span(error)
                return
            if error is None:
                try:
                    data = wire(value)
                except (TypeError, ValueError) as e:
                    # Every attempt would fail the same way: final.
                    error = _codec.codec_error(f"step '{name}''s result", value, e, step=name)
                else:
                    end_span(None)
                    st["settled"] = True
                    # Recorded and delivered in one native call when the
                    # pump would deliver at once anyway.
                    direct = self._direct_pump()
                    d = done(True, data, direct)
                    if not direct or d is _UNDELIVERED:
                        self.pump()
                    elif d is None:
                        self.be.kick()
                    else:
                        self.pump_from(d)
                    return
            end_span(error)
            f = failure_of(error, host._stacks)
            if isinstance(error, CodecError):
                f.retryable = False
            if retry.retryable(f, st["attempt"]):
                at = None
                if st["attempt"] < retry.attempts:
                    now = now_ms()
                    if st["first"] is None:
                        st["first"] = now
                    at = now + int(retry.delay_ms(st["attempt"]))
                    limit_ms = ms(retry.max_elapsed)
                    if limit_ms is not None and at - st["first"] > limit_ms:
                        at = None
                if at is None and pausable and (retry.on_exhausted or host._on_exhausted) == "pause":
                    # Out of retries: the run waits for a resume.
                    at = PAUSED
                if at is not None:
                    st["not_before"] = at
                    try:
                        st["gate"] = self.step_retry(seq, at, f.encode())
                    except FuseeError as e:
                        op = self.pending.pop(seq, None)
                        if op is not None:
                            _settle(op, None, e, self.ready)
                        self.run_ready()
                        return
                    if at == PAUSED:
                        emit = getattr(host, "_emit", None)
                        if emit is not None:
                            emit("paused", self.id, name, f)
                    st["attempt"] += 1
                    self.be.kick()
                    try_run()
                    return
            st["settled"] = True
            done(False, f.encode().decode())
            self.pump()

        def call():
            if step_ctx:
                return fn(StepCtx(self, seq, name, st["attempt"]))
            return fn()

        def try_run() -> None:
            # A retry timer and a wake can both get here: one attempt runs.
            if st["running"] or st["settled"]:
                return
            if self.ended or seq not in self.pending:
                self.gated.pop(seq, None)
                cancel_later()
                release()
                return
            # A paused step waits for its resume, which wakes the run. What
            # begin_step found for its gate is stale by then: ask again.
            if st["not_before"] >= PAUSED:
                st["known"] = None
                if self.h("stepPaused", [seq])[0]:
                    self.gated[seq] = try_run
                    return
                # Resumed: a fresh set of attempts, counted from here.
                st["not_before"] = 0
                st["attempt"] = 1
                st["first"] = None
            # A wake during the retry delay cannot let the attempt run: the
            # delay's timer calls back.
            if st["later"] is not None and now_ms() < st["not_before"]:
                return
            known = st["known"]
            st["known"] = None
            if not (self.may_run(0 if read_only else st["gate"], st["not_before"]) if known is None else known):
                self.gated[seq] = try_run
                if st["not_before"] > now_ms():
                    retry_at(st["not_before"])
                return
            if limit:
                if st["ticket"] is None:
                    st["ticket"] = self.h("limitTicket")[0]
                a = self.h("acquire", [limit, st["ticket"], *place])[0]
                if a != 0:
                    self.gated[seq] = try_run
                    if a > 0:
                        retry_at(a)
                    return
            if self.gated:
                self.gated.pop(seq, None)
            if st["later"] is not None:
                cancel_later()
            st["running"] = True
            if heartbeat_timeout is not None:
                self.beats[seq] = now_ms()
            if tel is not None:
                st["span"] = tel._attempt(self, name, st["attempt"])
            # The task created here copies the context, span included.
            with (st["span"].activate() if st["span"] is not None else _NO_SPAN):
                if thread and not inspect.iscoroutinefunction(fn):
                    r = loop.run_in_executor(None, call)
                else:
                    try:
                        if host._dev and not inspect.iscoroutinefunction(fn):
                            began = time.perf_counter()
                            r = call()
                            host._slow_step(self, name, time.perf_counter() - began, r)
                        else:
                            r = call()
                    except BaseException as e:  # noqa: BLE001 - becomes the step's failure
                        finished(None, e)
                        return
                    if not inspect.isawaitable(r):
                        finished(r, None)
                        return
                fut = asyncio.ensure_future(asyncio.wait_for(r, timeout / 1000) if timeout is not None else r)
            self.tasks[seq] = fut
            if heartbeat_timeout is not None:
                def check() -> None:
                    if fut.done():
                        return
                    left = self.beats.get(seq, 0) + heartbeat_timeout - now_ms()
                    if left <= 0:
                        st["silent"] = True
                        fut.cancel()
                    else:
                        st["timers"].append(loop.call_later(left / 1000, check))

                st["timers"].append(loop.call_later(heartbeat_timeout / 1000, check))

            def cb(f: asyncio.Future) -> None:
                if f.cancelled():
                    if st.pop("silent", False):
                        finished(None, TimeoutError(f"step sent no heartbeat for {int(heartbeat_timeout)} ms"))
                    else:
                        finished(None, CancelledError("step cancelled"))
                elif f.exception() is not None:
                    e = f.exception()
                    if isinstance(e, asyncio.TimeoutError):
                        e = TimeoutError(f"step '{name}' timed out after {int(timeout)} ms")
                    finished(None, e)
                else:
                    finished(f.result(), None)

            fut.add_done_callback(cb)

        try_run()


def _call_decide(x: RunExec, token: int, ok: bool, message: str) -> int:
    try:
        x.flush_done()
        return x.n.decide_update(x.shard, token, ok, message)
    except RuntimeError as e:
        raise native_error(e) from None


class _NoSpan:
    def __enter__(self):
        return None

    def __exit__(self, *exc):
        return False


_NO_SPAN = _NoSpan()


@dataclass
class Completion:
    """What ``ctx.await_completion``'s send function receives: the token to
    complete with (``engine.complete(token, value)``), and the callback URL
    when the engine serves its HTTP API with a public address."""

    token: str
    url: Optional[str] = None

    def __str__(self) -> str:
        return self.token


class StepCtx:
    """The step function's optional argument: ``ctx.step("charge", lambda s:
    pay(key=s.key))``. Built only for a function that declares it."""

    __slots__ = ("_x", "seq", "name", "attempt", "_key")

    def __init__(self, x: RunExec, seq: int, name: str, attempt: int):
        self._x = x
        self.seq = seq
        #: The step's name.
        self.name = name
        #: 1 for the first attempt, 2 for the first retry, ...
        self.attempt = attempt
        self._key: Optional[str] = None

    @property
    def key(self) -> str:
        """A key stable across retries and recovery for this step (distinct
        for each run of a workflow ID, including one that replaced it by
        reset). Pass it to services that accept idempotency keys."""
        if self._key is None:
            self._key = (self._x.hb("key", [self.seq]) or b"").decode()
        return self._key

    @property
    def cancelled(self) -> bool:
        """True once the run was cancelled, terminated or closed. An async
        step function is cancelled at its next ``await``; a function on a
        thread (``thread=True``) checks this to stop early."""
        return self._x.ended

    @property
    def workflow_id(self) -> str:
        return self._x.id

    def heartbeat(self, progress: Any = None) -> None:
        """Reports progress. Coalesced; never waits for the disk."""
        x = self._x
        if self.seq in x.beats:
            x.beats[self.seq] = now_ms()
        x.h("heartbeat", [self.seq], [encode(progress)])
        x.be.kick()

    def progress(self) -> Any:
        """The latest progress this step reported, also from before a crash."""
        return decode(self._x.hb("progress", [self.seq]))

    def stream(self, name: str) -> "StreamWriter":
        """Opens the step's stream ``name``: the step's result completes it,
        and an attempt that runs again finds what earlier attempts recorded
        in ``recorded()``."""
        recorded, closed = self._x.h("streamOpen", [name, self.seq])
        seq = self.seq
        return StreamWriter(self._x, name, seq, recorded, bool(closed), lambda: seq)

    @property
    def log(self) -> logging.LoggerAdapter:
        """A logger tagged with the workflow ID, run and step."""
        return logging.LoggerAdapter(log, {"workflow_id": self._x.id, "run_id": self._x.run, "step": self.name})


class Ctx:
    """The workflow context. Code between ctx calls must be deterministic:
    it runs again against the journal after a crash."""

    #: Compensations registered so far (made on first use).
    _undo: Optional[list] = None

    def __init__(self, x: RunExec):
        self._x = x

    def compensate(self, name: str, fn: Callable[..., Any]) -> None:
        """Registers a step that undoes what the workflow did so far: if the
        workflow fails or is cancelled, every registered compensation runs
        as a step named ``name``, newest first, then the failure is raised.
        Register it after the step it undoes succeeded::

            hold = await ctx.step("reserve", lambda: inventory.reserve(sku))
            ctx.compensate("release", lambda: inventory.release(hold))
            await ctx.step("charge", lambda: payments.charge(cents))  # fails: "release" runs
        """
        if self._undo is None:
            self._undo = []
        self._undo.append((name, fn))

    @property
    def workflow_id(self) -> str:
        return self._x.id

    @property
    def workflow_type(self) -> str:
        return self._x.wtype

    @property
    def version(self) -> int:
        return self._x.version

    @property
    def run_id(self) -> int:
        return self._x.run

    @property
    def log(self) -> logging.LoggerAdapter:
        """A logger tagged with the workflow ID and run (the ``fusee``
        logger). Silent while the run replays its journal, so a line is
        written once however often the code runs; not journaled."""
        if self._x.host._replaying(self._x):
            return _SILENT
        return logging.LoggerAdapter(log, {"workflow_id": self._x.id, "run_id": self._x.run})

    def _op(self, kind: str, name: str, seq: int, dec: Optional[Callable] = None,
            target: Optional[str] = None) -> _Op:
        op = _Op(kind, name, seq, dec, target)
        self._x.pending[seq] = op
        self._x.be.kick()
        return op

    def step(self, name: str, fn: Callable[..., Any], *, retry: Optional[Retry] = None, timeout=None,
             heartbeat_timeout=None, limit: Optional[str] = None, read_only: bool = False,
             priority: Optional[int] = None, fairness_key: Optional[str] = None,
             fairness_weight: Optional[int] = None, output: Any = None, thread: Optional[bool] = None) -> Any:
        """Runs ``fn`` (sync or async) as a durable step and returns its
        recorded result (as JSON decodes it, or as ``output``'s type).

        ``fn`` may take one argument, the step context ``s``: ``s.key`` (an
        idempotency key), ``s.attempt``, ``s.heartbeat(p)``, ``s.stream(n)``.
        It runs once everything the run recorded before is durable; with
        ``read_only=True`` at once (use it only for steps that change
        nothing outside). ``thread=True`` runs a synchronous ``fn`` on a
        thread so it does not hold the event loop.

        With ``limit``, each attempt first takes a permit of that limit
        (``engine.limit``); a higher ``priority`` goes first, and steps of
        one priority take turns by ``fairness_key``."""
        x = self._x
        pl: tuple = ()
        if priority is not None or fairness_key is not None or fairness_weight is not None:
            pl = place(priority, fairness_key, fairness_weight)
        seq, replay, gate, attempt, not_before, open_ = x.begin_step(name, read_only)
        op = self._op("step", name, seq, None if output is None else _codec.decoder_for(output))
        if not replay:
            def done(ok: bool, v: Any, deliver: bool = False) -> Any:
                return x.step_done(seq, ok, v, deliver)

            x.run_attempts(seq, gate, attempt, not_before, fn, retry or DEFAULT_RETRY, ms(timeout),
                           ms(heartbeat_timeout), limit, done, read_only, name, open_, pl,
                           x.host._sync_thread if thread is None else thread, takes_arg(fn))
        return op

    def await_completion(self, name: str, send: Callable[[Completion], Any], *, retry: Optional[Retry] = None,
                         timeout=None, output: Any = None) -> Any:
        """A step completed from outside: ``send(cb)`` starts work elsewhere
        with ``cb.token`` (or ``cb.url``); the workflow waits until a process
        calls ``engine.complete(token, value)`` or ``engine.fail(token,
        error)``. ``send`` is retried under the retry policy, each attempt
        with a new token."""
        x = self._x
        seq, kind, gate, attempt, not_before = x.h("beginAwait", [name])
        op = self._op("await", name, seq, None if output is None else _codec.decoder_for(output))
        if kind == 0:
            def done(ok: bool, v: Any, deliver: bool = False) -> Any:
                if ok:
                    x.h("awaiting", [seq])
                else:
                    x.step_done(seq, False, v)
                return _UNDELIVERED

            def run_send():
                tok = (x.hb("token", [seq]) or b"").decode()
                return send(Completion(tok, x.host._completion_url(tok)))

            x.run_attempts(seq, gate, attempt, not_before, run_send, retry or DEFAULT_RETRY, ms(timeout), None,
                           None, done, False, name, pausable=False)
        return op

    def stream(self, name: str) -> "StreamWriter":
        """Opens the run's stream ``name`` for appending (readers follow it
        with ``run.stream(name)``). Its appends are recorded once (a replay
        appends nothing), and ``close()`` or the run's end completes it. A
        step opens a stream of its own with ``s.stream(name)``."""
        recorded, closed = self._x.h("streamOpen", [name, "w"])
        return StreamWriter(self._x, name, None, recorded, bool(closed), lambda: None)

    def sleep(self, name: str, duration) -> Any:
        """Sleeps durably: the wake time is recorded and survives restarts."""
        x = self._x
        at = now_ms() + int(ms(duration) or 0)
        seq = x.h("beginSleep", [name, at])[0]
        return self._op("sleep", name, seq)

    def sleep_until(self, name: str, at) -> Any:
        """Sleeps until ``at`` (a ``datetime`` or Unix milliseconds)."""
        seq = self._x.h("beginSleep", [name, _codec.at_ms(at)])[0]
        return self._op("sleep", name, seq)

    @overload
    def wait_for(self, signal: Signal[T], *, timeout=None) -> Awaitable[Optional[T]]: ...
    @overload
    def wait_for(self, signal: str, *, timeout=None) -> Awaitable[Any]: ...

    def wait_for(self, signal: Any, *, timeout=None) -> Any:
        """Waits for the next signal; ``None`` on timeout."""
        x = self._x
        t = ms(timeout)
        deadline = 0 if t is None else now_ms() + int(t)
        n = name_of(signal)
        seq = x.h("beginWait", [n, deadline])[0]
        dec = signal.decode if isinstance(signal, Signal) and signal._decode is not None else None
        return self._op("wait", n, seq, dec)

    @overload
    def child(self, workflow: Workflow[Any, O], id: str, input: Any = None, *, parent_close: str = "terminate",
              timeout=None) -> Awaitable[O]: ...
    @overload
    def child(self, workflow: str, id: str, input: Any = None, *, parent_close: str = "terminate",
              timeout=None) -> Awaitable[Any]: ...

    def child(self, workflow: Any, id: str, input: Any = None, *, parent_close: str = "terminate",
              timeout=None) -> Any:
        """Starts a child workflow and waits for its result. The child's ID
        makes the start idempotent."""
        x = self._x
        t = ms(timeout)
        deadline = 0 if t is None else now_ms() + int(t)
        wtype = name_of(workflow)
        seq = x.h("beginChild", [wtype, id, close_bits(parent_close), deadline], [encode(input)])[0]
        dec = (lambda v: workflow.decode_output(v)) if isinstance(workflow, Workflow) else None
        return self._op("child", wtype, seq, dec, id)

    def start_child(self, workflow: Any, id: str, input: Any = None, *, parent_close: str = "terminate",
                    timeout=None) -> None:
        """Starts a child without waiting. By default it is terminated when
        this workflow closes; ``parent_close="abandon"`` lets it outlive it."""
        x = self._x
        t = ms(timeout)
        deadline = 0 if t is None else now_ms() + int(t)
        x.h("beginChild", [name_of(workflow), id, close_bits(parent_close) | _DETACHED, deadline], [encode(input)])
        x.be.kick()

    def signal_workflow(self, id: str, signal: Any, value: Any = None) -> None:
        """Signals another workflow, once this point is durable."""
        self._x.h("send", [id, name_of(signal)], [encode(value)])
        self._x.be.kick()

    def cancel_workflow(self, id: str, *, reason: str = "cancelled") -> None:
        """Asks another workflow to cancel, once this point is durable."""
        self._x.h("cancelWorkflow", [id, reason])
        self._x.be.kick()

    def object(self, type: Any, key: str) -> "ObjectCall":
        """Key ``key`` of object type ``type``, called from this workflow:
        each call is a journal operation, run exactly once."""
        return ObjectCall(self._x, name_of(type), key)

    def on_query(self, query: Any, handler: Callable[[Any], Any]) -> None:
        """Registers a read-only query handler (``run.query(query, arg)``)."""
        dec = query._decode_arg if isinstance(query, Query) else None
        self._x.queries[name_of(query)] = (handler, dec)

    def on_update(self, update: Any, handler: Callable[[Any], Any], *, validate: Optional[Callable] = None) -> None:
        """Registers an update. ``validate`` runs first (raise, or return a
        string, to refuse; a refusal records nothing); ``handler`` (sync or
        ``async``, may run steps) runs once per accepted update."""
        dec = update._decode_arg if isinstance(update, Update) else None
        self._x.updates[name_of(update)] = (validate, handler, dec)

    def continue_as_new(self, input: Any = None, *, version: int = 0) -> Any:
        """Ends this run and starts a new run of the same ID with ``input``,
        in one record. Await it; it never returns."""
        self._x.h("continueAsNew", [version], [encode(input)])
        self._x.be.kick()
        return _Never()

    def journal_size(self) -> int:
        """Journal operations this run made so far (deterministic)."""
        return self._x.h("journalSize")[0]

    def patched(self, id: str) -> bool:
        """True for new runs, and for running ones the first time they get
        here after the deploy; false while replaying an older journal."""
        return self._x.h("patched", [id, "0"])[0] == 1

    def deprecate_patch(self, id: str) -> None:
        self._x.h("patched", [id, "1"])

    def set_attributes(self, attributes: dict) -> None:
        """Records search attributes (``None`` removes a key). Never encrypted."""
        if not isinstance(attributes, dict):
            raise FuseeError("search attributes are a dict")
        self._x.h("setAttributes", [], [encode(attributes)])

    def now(self) -> int:
        """Unix milliseconds, recorded."""
        b = self._x.hb("value", [0], [struct.pack("<Q", now_ms())])
        return struct.unpack("<Q", b)[0]

    def random(self) -> float:
        """A float in [0, 1), recorded."""
        b = self._x.hb("value", [1], [os.urandom(8)])
        return (struct.unpack("<Q", b)[0] >> 11) / float(1 << 53)

    def uuid(self) -> str:
        b = self._x.hb("value", [2], [_uuid.uuid4().bytes])
        return str(_uuid.UUID(bytes=bytes(b)))

    def all(self, items: list) -> Any:
        """Runs ops and coroutines concurrently; results in argument order."""
        return _Group(list(items), race=False)

    def race(self, items: list) -> Any:
        """``(index, result)`` of the first to finish; replay picks the same
        one. Losing operations are dropped (a dropped wait takes no signal)."""
        return _Group(list(items), race=True)


# ---------------------------------------------------------------- objects


class ObjectState:
    """A key's state as a handler sees it: JSON values under string keys."""

    def __init__(self, x: RunExec, exclusive: bool):
        self._x = x
        self._exclusive = exclusive

    def get(self, key: str, default: Any = None) -> Any:
        """The value, or ``default`` when the key has none. An exclusive
        handler sees its own writes."""
        b = self._x.hb("objectGet", [key, "1" if self._exclusive else "0"])
        return json.loads(b) if b else default

    def set(self, key: str, value: Any) -> None:
        """Sets a value (``None`` clears it); committed when the handler returns."""
        if value is None:
            return self.clear(key)
        if isinstance(value, (bytes, bytearray, memoryview)):
            raise RejectedError("state values are JSON, not bytes")
        self._writable()
        self._x.h("objectSet", [key, "1"], [encode(value)])

    def clear(self, key: str) -> None:
        self._writable()
        self._x.h("objectSet", [key, "0"])

    def clear_all(self) -> None:
        """Clears every key."""
        for k in self.keys():
            self.clear(k)

    def keys(self) -> list:
        """The keys that hold a value, sorted."""
        b = self._x.hb("objectKeys", ["1" if self._exclusive else "0"])
        return json.loads(b) if b else []

    def _writable(self) -> None:
        if not self._exclusive:
            raise RejectedError("a shared handler cannot change state; make it exclusive")


class ObjectCtx(Ctx):
    """The context of an object's handler: the workflow context of the key's
    run, its ``state`` and its ``object_key``."""

    def __init__(self, x: RunExec, exclusive: bool):
        super().__init__(x)
        self.exclusive = exclusive
        self.state = ObjectState(x, exclusive)

    @property
    def object_key(self) -> str:
        return self.workflow_id[len(self.workflow_type) + 1:]


class ObjectCall:
    """A key called from workflow code (``ctx.object(type, key)``)."""

    def __init__(self, x: RunExec, type: str, key: str):
        self._x = x
        self.type = type
        self.key = key

    def call(self, handler: str, arg: Any = None) -> Any:
        """Calls a handler; await it for the result. A journal operation,
        run exactly once."""
        x = self._x
        seq = x.h("beginCall", [self.type, self.key, handler, "0"], [encode(arg)])[0]
        op = _Op("call", f"{self.type}.{handler}", seq)
        x.pending[seq] = op
        x.be.kick()
        return op

    def send(self, handler: str, arg: Any = None) -> None:
        """Calls a handler without waiting: the outbox delivers the call
        once this point is durable."""
        self._x.h("beginCall", [self.type, self.key, handler, "1"], [encode(arg)])
        self._x.be.kick()


class _ObjectUpdates(dict):
    """An object run's update handlers, looked up when a call is delivered:
    a handler may be added after the run's code started."""

    def __init__(self, handlers: dict, x: RunExec):
        super().__init__()
        self._handlers = handlers
        self._x = x

    def get(self, name, default=None):
        h = self._handlers.get(name)
        if h is None and name == EDIT_HANDLER:
            # An edit from outside, applied by the engine as this exclusive
            # handler's writes.
            x = self._x
            return (None, lambda edit: json.loads(x.hb("objectEdit", [], [json.dumps(edit).encode()]) or b"{}"), None)
        if h is None:
            return default
        exclusive = not isinstance(h, _Shared)
        fn = h.fn if isinstance(h, _Shared) else h
        octx = ObjectCtx(self._x, exclusive)
        return (None, lambda arg: fn(octx, arg), None)


def object_main(handlers: dict):
    """An object type's code: its handlers as updates of the key's run, and
    the state query. It never returns."""

    async def forever():
        await _Never()

    def main(ctx, _snapshot):
        # Set up before the run's first delivery, which may be a recorded call.
        x = ctx._x
        x.updates = _ObjectUpdates(handlers, x)
        ctx.on_query(STATE_QUERY, lambda _arg: json.loads(x.hb("objectState") or b"{}"))
        return forever()

    return main


class StreamWriter:
    """A writer of one of the run's streams (``ctx.stream`` / ``s.stream``)."""

    def __init__(self, x: RunExec, name: str, step: Optional[int], recorded: int, closed: bool,
                 seq: Callable[[], Optional[int]]):
        self._x, self.name, self._step, self._seq = x, name, step, seq
        #: Chunks the step's earlier attempts recorded (0 on a first attempt and in workflow code).
        self.recorded_count = recorded
        #: The stream was complete when it was opened (an earlier attempt of the step completed it).
        self.closed = closed

    def append(self, value: Any) -> None:
        """Appends a chunk: ``bytes`` as bytes, anything else as JSON. Never
        waits for the disk."""
        s = self._seq()
        self._x.h("streamAppend", [self.name, "w" if s is None else s], [tagged(wire(value))])
        self._x.be.kick()

    def close(self) -> None:
        """Completes the stream: readers end once they have every chunk."""
        s = self._seq()
        self._x.h("streamClose", [self.name, "w" if s is None else s])
        self._x.be.kick()

    def recorded(self) -> list:
        """The chunks the step's earlier attempts recorded (after a crash,
        the durable ones): continue after them, so readers see no chunk
        twice."""
        if self._step is None or not self.recorded_count:
            return []
        b = self._x.hb("streamRecorded", [self.name, self._step, self.recorded_count]) or b""
        out, at = [], 0
        while at + 4 <= len(b):
            n = int.from_bytes(b[at:at + 4], "little")
            out.append(decode(b[at + 4:at + 4 + n]))
            at += 4 + n
        return out


# ---------------------------------------------------------------- the code host


class CodeHost:
    """Runs workflow code: shared by the engine and the replayer."""

    #: The workflow context each run gets (``fusee.testing`` subclasses it).
    _ctx_class: type = Ctx

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop]):
        self._loop = loop
        #: name -> version -> the Workflow definition.
        self._types: dict[str, dict[int, Workflow]] = {}
        # Runs at once per type (definitions with concurrency=).
        self._run_limits: dict = {}
        self._objects: dict[str, ObjectType] = {}
        self._runs: dict[int, RunExec] = {}
        self._awaiting: dict[str, list] = {}
        self._tel: Optional["Telemetry"] = None
        # Runs asking for deliveries during an engine tick (see Engine._pump_batch).
        self._batch: Optional[list] = None
        self._stacks = True
        self._on_exhausted = "fail"
        self._sync_thread = False
        self._dev = False
        self._warned_slow: set = set()
        #: The thread the engine's calls must come from.
        self._thread = threading.get_ident()

    def _on_loop(self, fn: Callable[[], Any]) -> Any:
        """Runs ``fn`` on the loop's thread and waits for it (a step function
        on a thread calling the engine)."""
        done = threading.Event()
        out: list = [None, None]

        def run() -> None:
            try:
                out[0] = fn()
            except BaseException as e:  # noqa: BLE001 - raised in the calling thread
                out[1] = e
            done.set()

        assert self._loop is not None
        self._loop.call_soon_threadsafe(run)
        done.wait()
        if out[1] is not None:
            raise out[1]
        return out[0]

    def _backend(self) -> Backend:
        raise NotImplementedError

    def _inline_ok(self) -> bool:
        return False

    def _completion_url(self, token: str) -> Optional[str]:
        return None

    def _register(self, wf: Workflow) -> None:
        versions = self._types.setdefault(wf.name, {})
        old = versions.get(wf.version)
        if old is not None and old is not wf:
            raise RejectedError(
                f"workflow type '{wf.name}' (version {wf.version}) is registered twice: first "
                f"{_where(old.fn)}, again {_where(wf.fn)}. Register each type once; give a new version of its "
                "code a new version number")
        versions[wf.version] = wf
        waiting = self._awaiting.pop(wf.name, [])
        self._awaiting[wf.name] = [ev for ev in waiting if not self._instantiate(ev)]

    def _on_start(self, ev: dict) -> None:
        if not self._instantiate(ev):
            self._awaiting.setdefault(ev["wtype"], []).append(ev)
            self._waiting_for_code(ev)

    def _waiting_for_code(self, ev: dict) -> None:
        pass

    def _on_ended(self, shard: int, run: int) -> None:
        r = self._runs.pop(key(shard, run), None)
        if r is not None:
            if self._tel is not None:
                self._tel._end_workflow(r, 4)
            r.end()

    def _instantiate(self, ev: dict) -> bool:
        wf = self._types.get(ev["wtype"], {}).get(ev["version"])
        if wf is None:
            return False
        k = key(ev["shard"], ev["run"])
        if k in self._runs:
            return True
        x = RunExec(self, self._backend(), ev["shard"], ev["run"], ev)
        if self._tel is not None:
            self._tel._workflow(x, ev.get("trace"))
        self._runs[k] = x
        try:
            coro = wf.fn(x.ctx, wf.decode_input(from_wire(ev["input"])))
        except BaseException as e:  # noqa: BLE001 - the run fails
            self._finished(x, None, e)
            return True
        if not inspect.iscoroutine(coro):
            value = coro

            async def wrap():
                return value

            coro = wrap()
        x.spawn_root(coro)
        return True

    def _finished(self, x: RunExec, value: Any, error: Optional[BaseException]) -> None:
        if x.ended:
            return
        undo = x.ctx._undo
        if error is not None and undo and not isinstance(error, DeterminismError):
            # Failed or cancelled: the compensations run as steps, newest
            # first, then the run ends with its failure.
            x.ctx._undo = None

            async def compensate():
                while undo:
                    name, fn = undo.pop()
                    try:
                        await x.ctx.step(name, fn)
                    except Exception as e:  # noqa: BLE001 - the others still run
                        log.error("compensation %r failed: %s", name, e,
                                  extra={"workflow_id": x.id, "run_id": x.run})
                raise error

            x.spawn_root(compensate())
            return
        if isinstance(error, DeterminismError):
            x.end()
            return
        x.end()
        self._runs.pop(key(x.shard, x.run), None)
        if error is None:
            try:
                status, data = OK, wire(value)
            except (TypeError, ValueError) as e:
                status, data = FAILED, Failure(str(_codec.codec_error(f"workflow '{x.id}''s result", value, e)),
                                               "Codec", retryable=False).encode().decode()
        else:
            status, data = outcome(error, self._stacks)
        x.flush_done()
        x.n.finish(x.shard, x.run, status, data)
        if self._tel is not None:
            self._tel._finish(x.wtype, status)
            self._tel._end_workflow(x, status)
        x.be.kick()
        self._run_settled(x)

    def _run_settled(self, x: RunExec) -> None:
        """A run's code finished or parked (the engine's dev profile
        replays it)."""

    def _replaying(self, x: RunExec) -> bool:
        """Whether the run's code is replaying its journal (a replayer always
        is)."""
        return True

    def _slow_step(self, x: RunExec, name: str, seconds: float, r: Any) -> None:
        """Under the dev profile: a synchronous step that held the event loop."""
        if seconds < 0.05 or inspect.isawaitable(r) or (x.wtype, name) in self._warned_slow:
            return
        self._warned_slow.add((x.wtype, name))
        log.warning(
            "fusee: step '%s' of workflow type '%s' held the event loop for %d ms: every other run waited. "
            "Make it async, or run it on a thread: ctx.step(%r, fn, thread=True), or open the engine with "
            "sync_steps=\"thread\"", name, x.wtype, int(seconds * 1000), name)


def _where(fn: Any) -> str:
    code = getattr(fn, "__code__", None)
    if code is None:
        return f"as {fn!r}"
    return f"at {code.co_filename}:{code.co_firstlineno}"


def as_workflow(d: Any) -> Workflow:
    if isinstance(d, Workflow):
        return d
    raise RejectedError(f"expected a workflow definition (fusee.workflow), not {type(d).__name__}")


__all__ = ["Ctx", "StepCtx", "ObjectCtx", "ObjectState", "ObjectCall", "StreamWriter", "Completion"]

_ = (WorkflowError,)
