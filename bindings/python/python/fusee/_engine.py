"""The engine and the handles it gives out. ``Engine`` (embedded) and
``Client`` (remote) implement one interface: code that starts, signals and
reads runs does not know which it holds."""

from __future__ import annotations

import asyncio
import base64
import contextvars
import builtins
import contextlib
import dataclasses
import json
import os
import re
import sys
import time
from typing import Any, AsyncIterator, Callable, Generic, Iterable, Optional, TypeVar, Union, overload

from . import _codec, _native
from ._codec import encode, from_wire, ms, tagged_to_wire, wire, wire_text
from ._defs import IdPolicy, ObjectType, Query, Signal, Update, Workflow, _Shared, name_of
from ._defs import id_policy as _id_policy
from ._errors import (
    AlreadyExistsError,
    CancelledError,
    ClosedError,
    CodecError,
    DeterminismError,
    Failure,
    FuseeError,
    NotFoundError,
    RejectedError,
    TerminatedError,
    TimeoutError,
    UnknownWorkflowError,
    docs_link,
    error_of,
    failure_of,
    native_error,
)
from ._runtime import (
    CANCELLED,
    CONTINUED,
    FAILED,
    OK,
    RESET,
    EDIT_HANDLER,
    SEND_PREFIX,
    STATE_QUERY,
    TERMINATED,
    TIMED_OUT,
    Backend,
    CodeHost,
    RunExec,
    key,
    log,
    now_ms,
    show_logs,
    object_main,
)

O = TypeVar("O")
T = TypeVar("T")
R = TypeVar("R")

_SCHEDULE_TYPE = "fusee.schedule"
_DELAY_TYPE = "fusee.delay"
# A run waiting for its turn (see fusee.Concurrency).
_QUEUED_TYPE = "fusee.queued"

# Set by the test kit: its engines do not announce the dev profile.
_QUIET: contextvars.ContextVar[bool] = contextvars.ContextVar("fusee_quiet", default=False)

# Audit: who asks for the signals, cancellations, terminations and resets
# made in this context (see `as_requester`).
_BY: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("fusee_by", default=None)
_AUDITED = ("signal", "signalWithStart", "cancel", "resume", "terminate", "reset")


@contextlib.contextmanager
def as_requester(by: str):
    """Names who asks for the signals, cancellations, terminations and
    resets made inside the block: their journal records carry it as `by`
    (audit). The HTTP API names its token or the proxy's signed-in user
    the same way."""
    t = _BY.set(by or None)
    try:
        yield
    finally:
        _BY.reset(t)
_CONTROL = "fusee.schedule.control"
# The debounce and batch coordinators take events on this update (``fusee::queue::EVENT``).
_EVENT = "fusee.event"
# A coordinator ended before its handler took the event (``fusee::queue::ENDED``): send it again.
_ENDED = "the workflow ended before the update finished"

# Event kinds in the flat event list the native tick returns (see native/lib.rs).
_EV_WAKE, _EV_ACK, _EV_REJECT, _EV_START, _EV_STUCK, _EV_ENDED, _EV_RESULT, _EV_REPLY, _EV_REFUSE = range(1, 10)
# Gates a lone workflow may flush inline per tick before yielding to asyncio.
_INLINE_PER_TICK = 32
# Ticks the SDK asks for go through the wake pipe (FUSEE_KICK=call_soon: loop.call_soon).
_KICK_PIPE = os.environ.get("FUSEE_KICK") != "call_soon"
#: How long a run may wait for code before the engine says so.
_WAITING_FOR_CODE_WARN_S = 5.0


def _call(f: Callable[[], T]) -> T:
    try:
        return f()
    except RuntimeError as e:
        raise native_error(e) from None


# ------------------------------------------------------------- run summaries


@dataclasses.dataclass
class RunSummary:
    """One run, as listings and ``run.status()`` give it."""

    id: str
    type: str
    version: int
    #: running, suspended, parked, stuck, waiting_for_code, queued, or a
    #: final status: ok, failed, cancelled, timed_out, terminated.
    status: str
    started_ms: int
    finished_ms: Optional[int] = None
    error: Optional[str] = None
    run_id: int = 0
    shard: int = 0
    attributes: dict = dataclasses.field(default_factory=dict)
    object: bool = False

    @property
    def finished(self) -> bool:
        return self.finished_ms is not None

    @classmethod
    def from_json(cls, r: dict) -> "RunSummary":
        return cls(
            id=r["id"], type=r.get("type") or r.get("wtype", ""), version=r.get("version", 1),
            status=r.get("status", ""), started_ms=r.get("started_ms", r.get("startedMs", 0)),
            finished_ms=r.get("finished_ms", r.get("finishedMs")), error=r.get("error"),
            run_id=r.get("run", 0), shard=r.get("shard", 0), attributes=r.get("attributes") or {},
            object=bool(r.get("object", False)))


@dataclasses.dataclass
class LimitInfo:
    """A limit and its permits right now (``engine.limits()``)."""

    name: str
    #: Permits held at once; 0 means no concurrency limit.
    concurrent: int
    #: Permits started per second; 0 means no rate limit.
    per_second: float
    #: Permits held now.
    in_use: int
    #: Steps of this process waiting for a permit.
    waiting: int


@dataclasses.dataclass
class RunDetail:
    """A run and its journal: each record a dict with its ``kind`` (start,
    step, attempt, timer, signal, wait, child, child_done, send, update,
    update_done, completed, heartbeat, attributes, chunk, stream_end, done,
    ...), times and run IDs left out."""

    summary: RunSummary
    journal: list
    #: For a key of an object type: its ``state`` and pending calls.
    object: Optional[dict] = None

    @property
    def steps(self) -> list:
        """The names of the steps the run recorded, in order."""
        return [r["name"] for r in self.journal if r.get("kind") == "step"]

    @property
    def failed_step(self) -> Optional[str]:
        """The step whose failure ended the run, if one did."""
        for r in reversed(self.journal):
            if r.get("kind") == "step" and r.get("status") == "failed":
                return r["name"]
        return None


def _filter(type: Any, status: Optional[str], started_after, started_before, attributes: Optional[dict],
            id: Optional[str] = None) -> dict:
    where: dict = dict(attributes or {})
    if type is not None:
        where["type"] = name_of(type)
    if status is not None:
        where["status"] = status
    if id is not None:
        where["id"] = id
    span: dict = {}
    if started_after is not None:
        span["gte"] = _codec.at_ms(started_after)
    if started_before is not None:
        span["lt"] = _codec.at_ms(started_before)
    if span:
        where["startedMs"] = span
    return where


# ------------------------------------------------------------- handles


class StreamReader:
    """A reader of a run's stream: ``async for chunk in run.stream("tokens")``
    yields each chunk once it is durable, in order, and ends once the stream
    is complete. ``offset`` is where a reader that reconnects continues;
    ``status`` says how the stream ended (``ok``, a failed step's
    ``failed``, or the run's status if it ended first)."""

    def __init__(self, read: Callable[[Optional[int], int, int], Any], start: int = 0, wait=None,
                 run: Optional[int] = None):
        self._read = read
        self.offset = start
        self.run = run
        self.status: Optional[str] = None
        self._wait = int(ms(wait) if wait is not None else 30_000)

    def __aiter__(self):
        return self._chunks()

    async def _chunks(self):
        while self.status is None:
            b = await self._read(self.run, self.offset, self._wait)
            self.run = b["run"]
            raw = set(b.get("bytes") or ())
            for i, v in enumerate(b["chunks"]):
                self.offset = b["from"] + i + 1
                yield base64.b64decode(v) if i in raw else v
            self.offset = max(self.offset, b["next"])
            if b.get("end"):
                self.status = b["end"]["status"]


class Run(Generic[O]):
    """A handle to a run by workflow ID, from an engine or a client."""

    def __init__(self, ops: Any, id: str, workflow: Optional[Workflow] = None):
        self._ops = ops
        self.id = id
        self._wf = workflow

    def __repr__(self) -> str:
        return f"Run({self.id!r})"

    async def signal(self, signal: Any, value: Any = None) -> None:
        """Sends a signal: ``await run.signal(review, True)``."""
        await self._ops._signal(self.id, name_of(signal), value)

    @overload
    async def query(self, query: Query[Any, R], arg: Any = None) -> R: ...
    @overload
    async def query(self, query: str, arg: Any = None) -> Any: ...

    async def query(self, query: Any, arg: Any = None) -> Any:
        """Calls a query handler; the answer waits until the state it read is durable."""
        v = await self._ops._query(self.id, name_of(query), arg)
        if isinstance(query, Query) and query._decode_result is not None and v is not None:
            v = query._decode_result(v)
        return v

    @overload
    async def update(self, update: Update[Any, R], arg: Any = None, *, id: str = "") -> R: ...
    @overload
    async def update(self, update: str, arg: Any = None, *, id: str = "") -> Any: ...

    async def update(self, update: Any, arg: Any = None, *, id: str = "") -> Any:
        """Sends an update and waits for its handler's durable result. A
        retried call with the same ``id`` gets the recorded result."""
        v = await self._ops._update(self.id, name_of(update), arg, id)
        if isinstance(update, Update) and update._decode_result is not None and v is not None:
            v = update._decode_result(v)
        return v

    async def result(self) -> O:
        """The run's result (following continue-as-new and reset). A failed
        run raises its error: a step's as ``StepError`` (``e.cause`` is the
        step's failure), anything else as ``WorkflowError``."""
        v = await self._ops._result(self.id)
        if self._wf is not None and v is not None:
            v = self._wf.decode_output(v)
        return v

    async def status(self) -> RunSummary:
        """The run's summary. Raises ``NotFoundError`` if there is none."""
        s = await self._ops._status(self.id)
        if s is None:
            raise NotFoundError(f"workflow '{self.id}' not found", id=self.id)
        return s

    async def describe(self) -> RunDetail:
        """The run's summary and journal."""
        d = await self._ops._describe(self.id)
        if d is None:
            raise NotFoundError(f"workflow '{self.id}' not found", id=self.id)
        return d

    async def started(self) -> None:
        """Waits until a run with this ID exists (a debounced or batched run
        starts only when its window closes)."""
        pause = 0.001
        while not await self._ops._exists(self.id):
            await asyncio.sleep(pause)
            pause = min(pause * 2, 0.05)

    async def cancel(self, reason: str = "cancelled") -> None:
        """Delivers cancellation to the run's waits; its code may compensate."""
        await self._ops._close_run(self.id, reason, False)

    async def terminate(self, reason: str = "terminated") -> None:
        """Ends the run at once; children follow their parent close policy."""
        await self._ops._close_run(self.id, reason, True)

    async def resume(self) -> None:
        """Resumes a run that paused because a step ran out of retries
        (``Retry(on_exhausted="pause")``): the step runs again at once, with
        a fresh set of attempts, on the code this process has now. Raises
        :class:`RejectedError` if nothing is paused."""
        await self._ops._resume(self.id)

    async def reset(self, before_step: str, *, reapply_signals: bool = True, version: int = 0) -> None:
        """Starts a new run whose journal is this one's cut before ``before_step``."""
        await self._ops._reset(self.id, before_step, reapply_signals, version)

    async def retry(self) -> None:
        """Runs a failed run again from its failed step: a reset before it
        (the steps before keep their results)."""
        d = await self.describe()
        step = d.failed_step
        if step is None:
            raise RejectedError(f"workflow '{self.id}' has no failed step to retry (status {d.summary.status})")
        await self.reset(step)

    def stream(self, name: str, *, from_: int = 0, wait=None) -> StreamReader:
        """A reader of the run's stream ``name`` from offset ``from_``."""
        return StreamReader(lambda run, start, wait_ms: self._ops._stream_read(self.id, run, name, start, wait_ms),
                            from_, wait)


class ObjectRef:
    """A key of an object type, called from outside workflow code."""

    def __init__(self, ops: Any, type: str, key: str):
        self._ops = ops
        self.type = type
        self.key = key

    @property
    def id(self) -> str:
        return f"{self.type}/{self.key}"

    def __repr__(self) -> str:
        return f"ObjectRef({self.type!r}, {self.key!r})"

    async def call(self, handler: str, arg: Any = None, *, id: str = "") -> Any:
        """Calls a handler and waits for its durable result. Calls with the
        same ``id`` run once, and each gets the recorded result."""
        return await self._ops._object_call(self.type, self.id, handler, arg, id)

    async def send(self, handler: str, arg: Any = None, *, id: str = "") -> None:
        """Calls a handler without waiting for its result: returns once the
        call is accepted."""
        await self._ops._object_call(self.type, self.id, handler, arg, SEND_PREFIX + id)

    async def edit_state(self, edit: dict, *, if_version: Optional[str] = None, id: str = "") -> dict:
        """Edits the state from outside its handlers: an exclusive call, so
        it waits for the key and commits as a handler's writes do. ``edit``
        has ``clear_all``/``clearAll``, ``clear`` (fields), ``set`` (fields
        and values; ``None`` removes one) and ``patch`` (a JSON Patch over
        the state as one object), applied in that order. ``if_version``
        refuses the edit unless the state is still at that version (the
        ``version`` in ``describe()``'s ``object``). Returns ``{"version",
        "changed"}``."""
        e = dict(edit)
        if "clear_all" in e:
            e["clearAll"] = e.pop("clear_all")
        if if_version is not None:
            e["ifVersion"] = if_version
        return await self._ops._edit_state(self.type, self.id, e, id)

    async def set_state(self, field: str, value: Any, *, if_version: Optional[str] = None) -> dict:
        """Sets one field of the state."""
        return await self.edit_state({"set": {field: value}}, if_version=if_version)

    async def patch_state(self, ops: list, *, if_version: Optional[str] = None) -> dict:
        """Applies a JSON Patch (RFC 6902) to the state as one JSON object."""
        return await self.edit_state({"patch": ops}, if_version=if_version)

    async def clear_state(self, field: Optional[str] = None, *, if_version: Optional[str] = None) -> dict:
        """Clears one field, or (none named) the whole state; the key remains."""
        edit = {"clearAll": True} if field is None else {"clear": [field]}
        return await self.edit_state(edit, if_version=if_version)

    async def state(self) -> dict:
        """The committed state (``{}`` for a key never called), once durable."""
        try:
            return await self._ops._query(self.id, STATE_QUERY, None)
        except NotFoundError:
            return {}

    async def describe(self) -> Optional[RunDetail]:
        """The key's run: its journal, and ``object`` with its state and
        pending calls."""
        return await self._ops._describe(self.id)

    async def delete(self) -> None:
        """Deletes the key: its run ends and its state with it."""
        try:
            await self._ops._close_run(self.id, "object deleted", True)
        except NotFoundError:
            pass


@dataclasses.dataclass
class ScheduleSpec:
    """When a schedule fires: a cron expression or an interval."""

    cron: Optional[str] = None
    every: Any = None
    offset: Any = None
    timezone: str = "UTC"
    #: skip, buffer-one, buffer-all, cancel-other, terminate-other, allow-all
    overlap: str = "skip"
    jitter: Any = None
    catchup_window: Any = None
    paused: bool = False

    @classmethod
    def of(cls, spec: Any) -> "ScheduleSpec":
        if isinstance(spec, ScheduleSpec):
            return spec
        if isinstance(spec, str):
            return cls(cron=spec)
        if isinstance(spec, dict):
            return cls(**spec)
        raise RejectedError(f"a schedule's spec is a ScheduleSpec, a dict or a cron string, not {type(spec).__name__}")

    def wire(self, workflow: str, input: Any) -> dict:
        return {
            "cron": self.cron,
            "everyMs": None if self.every is None else int(ms(self.every)),
            "offsetMs": int(ms(self.offset) or 0),
            "timezone": self.timezone,
            "overlap": self.overlap,
            "jitterMs": int(ms(self.jitter) or 0),
            "catchupWindowMs": int(ms(self.catchup_window) or 365 * 864e5),
            "paused": self.paused,
            "workflow": workflow,
            "input": input,
        }


class Schedule:
    """A schedule, addressed by name."""

    def __init__(self, ops: Any, name: str):
        self._ops = ops
        self.name = name
        self.id = f"fusee.schedule/{name}"

    def __repr__(self) -> str:
        return f"Schedule({self.name!r})"

    async def _send(self, cmd: dict) -> None:
        await self._ops._signal(self.id, _CONTROL, cmd)

    async def pause(self) -> None:
        await self._send({"op": "pause"})

    async def resume(self) -> None:
        await self._send({"op": "resume"})

    async def trigger(self) -> None:
        """Starts one run now."""
        await self._send({"op": "trigger"})

    async def backfill(self, from_: Any, to: Any) -> None:
        """Starts a run for every scheduled time in ``[from_, to]`` (datetimes
        or Unix milliseconds)."""
        await self._send({"op": "backfill", "fromMs": _codec.at_ms(from_), "toMs": _codec.at_ms(to)})

    async def describe(self) -> dict:
        return await self._ops._query(self.id, "describe", None)

    async def delete(self) -> None:
        """Stops the schedule; runs it started keep running."""
        await self._ops._close_run(self.id, "schedule deleted", True)


class Schedules:
    """``engine.schedules.get(name)``, ``engine.schedules.list()``."""

    def __init__(self, ops: Any):
        self._ops = ops

    def get(self, name: str) -> Schedule:
        return Schedule(self._ops, name)

    async def list(self) -> builtins.list:
        out = []
        async for r in self._ops.list(type=_SCHEDULE_TYPE):
            if r.finished_ms is None and r.id.startswith("fusee.schedule/"):
                out.append(Schedule(self._ops, r.id[len("fusee.schedule/"):]))
        return out


def _settle_result(status: int, payload: Union[None, str, bytes], id: str) -> Any:
    if status == OK:
        return from_wire(payload)
    text = wire_text(payload)
    if status == CANCELLED:
        raise CancelledError(text)
    if status == TIMED_OUT:
        raise TimeoutError(text or f"workflow '{id}' timed out")
    if status in (TERMINATED, CONTINUED, RESET):
        raise TerminatedError(text)
    raise error_of(Failure.decode(text), workflow_id=id)


async def _until_taken(coordinator: str, send: Callable[[], Any]) -> str:
    """Sends an event until a coordinator takes it: one that was ending
    refused it with ``_ENDED`` before its handler ran. Returns the receipt's
    run ID."""
    for _ in range(100):
        try:
            return (await send())["id"]
        except FuseeError as e:
            msg = e.cause.message if e.cause is not None else str(e)
            if re.sub(r"^(?:workflow failed: )+", "", msg) != _ENDED:
                raise
    raise FuseeError(f"coordinator '{coordinator}' kept ending before taking the event")


def _queue_request(kind: str, workflow: str, key: str, event_id: str, value: Any,
                   a: Optional[float], b: Optional[float], c: Optional[float]) -> tuple:
    """One debounced start or batched event as ``fusee::queue::request``
    builds it: ``(type, coordinator ID, start input, update argument)``."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        what = "debounced input" if kind == "debounce" else "batched event"
        raise RejectedError(f"a {what} is JSON, not bytes")
    return _call(lambda: _native.queue_request(kind, workflow, key, event_id, _codec.json_text(value), a, b, c))


def _failure_text(failure: Any) -> str:
    if isinstance(failure, Failure):
        return failure.encode().decode()
    if isinstance(failure, BaseException):
        return failure_of(failure, False).encode().decode()
    return Failure(str(failure)).encode().decode()


class ReplicaMetrics:
    """Observe a replica, including while its worker waits to become primary."""

    def __init__(self):
        self._handle = _native.NativeReplicaMetrics()

    def snapshot(self):
        value = self._handle.snapshot()
        return None if value is None else json.loads(value)


def _profile(profile: Optional[str]) -> Optional[str]:
    p = profile if profile is not None else os.environ.get("FUSEE_PROFILE") or None
    if p not in (None, "dev", "production"):
        raise RejectedError(f"profile is 'dev' or 'production', not {p!r}")
    return None if p == "production" else p


class Engine(CodeHost):
    """An embedded engine: it records runs in a directory (or memory) and
    runs the workflows it was opened with."""

    def __init__(self, loop: asyncio.AbstractEventLoop, **opts):
        super().__init__(loop)
        self._serve_remote_calls = opts.get("durability") == "server"
        # A server's worker: other workers may hold code this one does not.
        self._server_mode = self._serve_remote_calls
        self._rfd, self._wfd = os.pipe()
        os.set_blocking(self._rfd, False)
        os.set_blocking(self._wfd, False)
        try:
            self._native = _call(lambda: _native.NativeEngine(notify_fd=self._wfd, **opts))
        except BaseException:
            os.close(self._rfd)
            os.close(self._wfd)
            raise
        self._be = Backend(self._native, self._kick)
        self._tokens: dict[int, tuple] = {}
        self._waiters: dict[int, list] = {}
        self._next_token = 1
        self._kicked = False
        self._timer: Optional[asyncio.TimerHandle] = None
        self._timer_at = 0  # when self._timer fires (ms)
        self._closed = False
        self._failure: Optional[FuseeError] = None
        self._inline_left = _INLINE_PER_TICK
        self._handlers: dict[str, list] = {}
        self._where = opts.get("dir") or opts.get("durability", "memory")
        loop.add_reader(self._rfd, self._on_notify)

    def __repr__(self) -> str:
        return f"Engine({self._where!r})"

    def _backend(self) -> Backend:
        return self._be

    @classmethod
    async def open(
        cls,
        dir: Optional[str] = None,
        *,
        workflows: Iterable[Any] = (),
        objects: Iterable[ObjectType] = (),
        durability: Optional[str] = None,
        address: Optional[str] = None,
        profile: Optional[str] = None,
        replica_id: Optional[int] = None,
        replica_metrics: Optional[ReplicaMetrics] = None,
        peers: Optional[builtins.list[str]] = None,
        shards: Optional[int] = None,
        retention=None,
        park_after=None,
        checkpoint_interval=None,
        flush: Optional[str] = None,
        segment_bytes: Optional[int] = None,
        keep_history: Optional[bool] = None,
        archive_history: bool = False,
        record_stacks: bool = True,
        on_exhausted: str = "fail",
        sync_steps: str = "inline",
        codec: Optional[list] = None,
        telemetry: Any = None,
        inline_flush: Optional[bool] = None,
        http: Optional[str] = None,
    ) -> "Engine":
        """Opens an engine that runs ``workflows`` (and ``objects``).

        With no directory: ``FUSEE_DIR``; else ``FUSEE_URL`` (a worker of a
        Fusee server); else ``./fusee-data``. ``durability`` is ``local``
        (the default with a directory), ``memory``, ``server`` or
        ``replicated``.

        ``profile="dev"`` (or ``FUSEE_PROFILE=dev``) is for development: it
        says so on stderr, keeps every run's history, flushes with ``fsync``
        rather than ``F_FULLFSYNC`` on macOS (it survives a process crash,
        not power loss), warns when a synchronous step holds the event loop,
        replays each run against the code when it parks or ends and logs any
        difference, makes ``result()`` of a stuck run raise
        ``DeterminismError``, shows ``ctx.log`` records on stderr when logging
        is not configured, and serves the UI.

        ``http="127.0.0.1:7601"`` serves the HTTP API and UI from the engine
        (``"off"``: never; under the dev profile the default is
        ``FUSEE_HTTP``, else 7601, else a free port).

        ``record_stacks=False`` keeps stack traces out of recorded failures.
        ``on_exhausted="pause"`` pauses a run whose step runs out of retries
        (unless the step's ``Retry`` says otherwise) until ``run.resume()``.
        ``sync_steps="thread"`` runs synchronous step functions on threads.
        ``codec=aes_gcm({...})`` encrypts payloads; ``telemetry=
        open_telemetry()`` reports spans and metrics."""
        if dir is None and durability in (None, "local"):
            if os.environ.get("FUSEE_DIR"):
                dir = os.environ["FUSEE_DIR"]
            elif os.environ.get("FUSEE_URL") and durability is None and address is None:
                durability, address = "server", os.environ["FUSEE_URL"]
            elif durability is None:
                dir = "./fusee-data"
        if durability is None:
            durability = "local" if dir is not None else "memory"
        if sync_steps not in ("inline", "thread"):
            raise RejectedError(f"sync_steps is 'inline' or 'thread', not {sync_steps!r}")
        prof = _profile(profile)
        dev = prof == "dev"
        if dev:
            show_logs()
            if keep_history is None:
                keep_history = True
            if flush is None and sys.platform == "darwin" and durability == "local":
                flush = "fsync"
            where = dir or address or durability
            if not _QUIET.get():
                print(f"fusee: dev profile ({where}): every run's history is kept, each run is replayed "
                      "against the code when it parks or ends"
                      + (", flushes use fsync (a process crash loses nothing; power loss may)"
                         if flush == "fsync" and sys.platform == "darwin" else "")
                      + ". Not for production.", file=sys.stderr)

        def opt(v):
            m = ms(v)
            return None if m is None else int(m)

        prepared = None
        if durability in ("server", "replicated"):
            prepared = await asyncio.to_thread(_native.prepare_connection, durability, address, replica_id, peers, dir,
                                               flush, replica_metrics._handle if replica_metrics else None)
        e = cls(
            asyncio.get_running_loop(),
            prepared=prepared,
            dir=dir,
            durability=durability,
            shards=shards,
            park_after_ms=opt(park_after),
            retention_ms=opt(retention),
            checkpoint_interval_ms=opt(checkpoint_interval),
            flush=flush,
            segment_bytes=segment_bytes,
            keep_history=bool(keep_history),
            archive_history=archive_history,
            codec_keys=codec,
            inline_flush=inline_flush,
        )
        e._tel = telemetry
        e._stacks = record_stacks
        if on_exhausted not in ("fail", "pause"):
            raise RejectedError(f"on_exhausted is 'fail' or 'pause', not {on_exhausted!r}")
        e._on_exhausted = on_exhausted
        e._sync_thread = sync_steps == "thread"
        e._dev = dev
        e._limit_specs: dict = {}
        e._codec_keys = codec
        e._strict_pending: list = []
        e._strict_task = None
        e._strict_seen: set = set()
        e._address = address
        e._durability = durability
        e._dir = dir
        try:
            for d in workflows:
                if isinstance(d, ObjectType):
                    e._add_object(d)
                else:
                    e._add_workflow(d)
            for o in objects:
                e._add_object(o)
        except BaseException:
            await e.close()
            raise
        e._tick()
        # ``http``: serve the API and UI there (any profile; "off": never).
        # Under the dev profile without it: FUSEE_HTTP, else 7601, else any.
        if http is None:
            http = os.environ.get("FUSEE_HTTP", "") if dev else "off"
        if http != "off":
            if http:
                # Asked for (`fusee dev` sets it): a busy address is an error.
                url = e.serve(http)
            else:
                try:
                    url = e.serve("127.0.0.1:7601")
                except FuseeError:
                    url = e.serve("127.0.0.1:0")  # 7601 is taken: any free port
            print(f"fusee: UI and HTTP API at {url}", file=sys.stderr)
        return e

    def serve(self, addr: str = "127.0.0.1:7601", *, token: Optional[str] = None,
              insecure: bool = False) -> str:
        """Serves the HTTP API and the web UI on a thread of their own (they
        call this engine through its event loop; nothing rides the step
        path). Off loopback a ``token`` is required. Returns the URL::

            print(engine.serve())   # http://127.0.0.1:7601
        """
        bound = _call(lambda: self._native.serve(addr, token, insecure, "dev" if self._dev else None))
        self._serve_remote_calls = True
        self._kick()
        self._url = f"http://{bound}"
        return self._url

    def _completion_url(self, token: str) -> Optional[str]:
        url = getattr(self, "_url", None)
        return None if url is None else f"{url}/v1/complete/{token}"

    async def __aenter__(self) -> "Engine":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    # -------------------------------------------------------------- registration

    def _add_workflow(self, wf: Any) -> None:
        if not isinstance(wf, Workflow):
            raise RejectedError(
                f"workflows=[...] takes definitions made with fusee.workflow, not {type(wf).__name__}")
        self._register(wf)
        if wf.concurrency is not None:
            self._run_limits[wf.name] = wf.concurrency

    def _add_object(self, o: ObjectType) -> None:
        self._objects[o.name] = o
        if self not in o._engines:
            o._engines.append(self)
        self._register_object_def(o.name, o.handlers)
        self._register(Workflow(o.name, object_main(o.handlers), version=o.version))

    def register(self, *definitions: Any) -> None:
        """Adds workflows or object types after the engine opened (prefer
        ``Engine.open(workflows=[...])``)."""
        for d in definitions:
            if isinstance(d, ObjectType):
                self._add_object(d)
            else:
                self._add_workflow(d)

    def _register_object_def(self, name: str, handlers: dict) -> None:
        names = [n for n, h in handlers.items() if isinstance(h, _Shared)]
        _call(lambda: self._native.register_object(name, names))

    def on(self, event: str, handler: Callable[..., Any]) -> None:
        """Calls ``handler`` on an engine event: ``"stuck"`` (``workflow_id,
        message``) when a run stops on a determinism error, ``"paused"``
        (``workflow_id, step, failure``) when a step of this process runs out
        of retries and pauses its run, and ``"waiting_for_code"``
        (``workflow_id, type``) when a run has waited five seconds for code
        no process registered."""
        if event not in ("stuck", "paused", "waiting_for_code"):
            raise RejectedError(f"engine events are 'stuck', 'paused' and 'waiting_for_code', not {event!r}")
        self._handlers.setdefault(event, []).append(handler)

    def _emit(self, event: str, *args: Any) -> None:
        for h in self._handlers.get(event, ()):
            try:
                h(*args)
            except Exception:  # noqa: BLE001 - a handler's failure is the application's
                log.exception("fusee: the %s handler raised", event)

    def _waiting_for_code(self, ev: dict) -> None:
        wtype, wid = ev["wtype"], ev["id"]

        def warn() -> None:
            if any(x is ev for x in self._awaiting.get(wtype, ())):
                log.warning("fusee: workflow '%s' has waited %d s for code of type '%s' (version %d), which this "
                            "process does not register. Register it with Engine.open(workflows=[...]) in a worker "
                            "that serves this shard", wid, _WAITING_FOR_CODE_WARN_S, wtype, ev["version"])
                self._emit("waiting_for_code", wid, wtype)

        if self._loop is not None and not self._closed:
            self._loop.call_later(_WAITING_FOR_CODE_WARN_S, warn)

    def _check_type(self, wtype: str) -> None:
        """A start of a type this process runs no code for: refused here when
        no other process could run it (embedded, not a server's worker)."""
        if wtype in self._types or wtype.startswith("fusee.") or self._server_mode:
            return
        names = sorted(self._types)
        near = _nearest(wtype, names)
        msg = f"no code is registered for workflow type '{wtype}'"
        if near:
            msg += f" (did you mean '{near}'?)"
        msg += (f". Registered types: {', '.join(names)}" if names
                else ". No workflow types are registered: pass them to Engine.open(workflows=[...])")
        raise UnknownWorkflowError(msg, type=wtype, registered=names)

    # -------------------------------------------------------------- the client interface

    def run(self, id: str, workflow: Optional[Workflow[Any, O]] = None) -> Run[O]:
        """A handle to a run by workflow ID; ``workflow`` types its result."""
        return Run(self, id, workflow)

    @overload
    async def start(self, workflow: Workflow[Any, O], id: str, input: Any = None, *, id_policy: Any = None,
                    timeout: Any = None, version: Optional[int] = None, delay: Any = None,
                    at: Any = None) -> Run[O]: ...
    @overload
    async def start(self, workflow: str, id: str, input: Any = None, *, id_policy: Any = None,
                    timeout: Any = None, version: Optional[int] = None, delay: Any = None,
                    at: Any = None) -> Run[Any]: ...

    async def start(self, workflow: Any, id: str, input: Any = None, *, id_policy: Any = None, timeout: Any = None,
                    version: Optional[int] = None, delay: Any = None, at: Any = None) -> Any:
        """Starts a run of ``workflow`` (a definition or a type name) under
        workflow ID ``id``; returns once it is durable. ``id_policy`` (an
        :class:`IdPolicy` or ``{"running": ..., "finished": ...}``) says
        what happens when the ID is taken. ``delay="10m"`` or ``at=datetime``
        starts it later: a durable timer (the run ``fusee.delay/<id>``), then
        the start; ``result()`` waits through the delay."""
        wtype = name_of(workflow)
        self._check_type(wtype)
        wf = workflow if isinstance(workflow, Workflow) else None
        if wf is not None:
            wf.check_input(input)
            if timeout is None:
                timeout = wf.timeout
        t = ms(timeout)
        deadline = 0 if t is None else now_ms() + int(t)
        policy = _id_policy(id_policy)
        latest = version if version is not None else self._latest(wtype, 1)
        data = self._input(wtype, input)
        if delay is not None or at is not None:
            when = _codec.at_ms(at) if at is not None else now_ms() + int(ms(delay))
            coord = {"type": wtype, "id": id, "input": json.loads(data) if isinstance(data, str) else input,
                     "atMs": when, **({"timeoutMs": int(t)} if t is not None else {})}
            cid, cdata = f"{_DELAY_TYPE}/{id}", wire(coord)
            try:
                await self._request(lambda tok: self._native.start(cid, _DELAY_TYPE, 1, cdata, 0, tok, policy.bits()),
                                    start=True)
            except AlreadyExistsError:
                raise await self._already_exists(cid) from None
            return Run(self, id, wf)
        # A type with runs at once: a slot waits for the turn, then starts it.
        c = (wf.concurrency if wf is not None else None) or self._run_limits.get(wtype)
        if c is not None:
            slot = {"type": wtype, "id": id, "input": json.loads(data) if isinstance(data, str) else input,
                    "limit": c.limit, **({"key": str(c.key(input))} if c.key is not None else {}),
                    **({"timeoutMs": int(t)} if t is not None else {})}
            sid, sdata = f"{_QUEUED_TYPE}/{id}", wire(slot)
            try:
                await self._request(lambda tok: self._native.start(sid, _QUEUED_TYPE, 1, sdata, 0, tok, policy.bits()),
                                    start=True)
            except AlreadyExistsError:
                raise await self._already_exists(sid) from None
            return Run(self, id, wf)
        trace = self._tel._current_traceparent() if self._tel is not None else None
        try:
            await self._request(lambda tok: self._native.start(id, wtype, latest, data, deadline, tok, policy.bits(),
                                                               None, trace), start=True)
        except AlreadyExistsError:
            raise await self._already_exists(id) from None
        if self._tel is not None:
            self._tel._start(wtype)
        return Run(self, id, wf)

    async def signal_with_start(self, workflow: Any, id: str, input: Any, signal: Any, value: Any = None, *,
                                id_policy: Any = None, timeout: Any = None, version: Optional[int] = None) -> Any:
        """Signals the run ``id``, starting it first with ``input`` if it is
        not running: the signal is recorded before the new run's code first
        runs. Returns the run::

            await engine.signal_with_start(cart, "cart-7", Cart(), add, item)
        """
        wtype = name_of(workflow)
        self._check_type(wtype)
        wf = workflow if isinstance(workflow, Workflow) else None
        if wf is not None:
            wf.check_input(input)
            if timeout is None:
                timeout = wf.timeout
        t = ms(timeout)
        deadline = 0 if t is None else now_ms() + int(t)
        policy = _id_policy(id_policy, running="use-existing")
        if policy.running == "fail":
            policy = IdPolicy("use-existing", policy.finished)
        latest = version if version is not None else self._latest(wtype, 1)
        data = self._input(wtype, input)
        sig, payload = name_of(signal), wire(value)
        # Both reach the engine before its next tick: the run's code sees
        # the signal however soon it runs.
        started = asyncio.ensure_future(self._request(
            lambda tok: self._native.start(id, wtype, latest, data, deadline, tok, policy.bits()), start=True))
        by = _BY.get()
        signalled = asyncio.ensure_future(self._request(lambda tok: self._native.signal(id, sig, payload, tok, by)))
        try:
            await started
        except AlreadyExistsError:
            signalled.cancel()
            raise await self._already_exists(id) from None
        except BaseException:
            signalled.cancel()
            raise
        await signalled
        return Run(self, id, wf)

    async def update_with_start(self, workflow: Any, id: str, input: Any, update: Any, arg: Any = None, *,
                                update_id: str = "", id_policy: Any = None, timeout: Any = None,
                                version: Optional[int] = None) -> tuple:
        """Starts the workflow if it is not running and delivers an update in
        the same record. Returns ``(run, result)``."""
        wtype = name_of(workflow)
        self._check_type(wtype)
        t = ms(timeout)
        deadline = 0 if t is None else now_ms() + int(t)
        policy = _id_policy(id_policy, running="use-existing")
        if policy.running == "fail":
            policy = IdPolicy("use-existing", policy.finished)
        latest = version if version is not None else self._latest(wtype, 1)
        data = self._input(wtype, input)
        result = await self._request_value(lambda tok: self._native.start(
            id, wtype, latest, data, deadline, -1, policy.bits(), (name_of(update), update_id, wire(arg), tok)))
        if isinstance(update, Update) and update._decode_result is not None and result is not None:
            result = update._decode_result(result)
        return Run(self, id, workflow if isinstance(workflow, Workflow) else None), result

    def object(self, type: Any, key: str) -> ObjectRef:
        """Key ``key`` of object type ``type`` (a definition or a name)."""
        return ObjectRef(self, name_of(type), key)

    @property
    def schedules(self) -> Schedules:
        return Schedules(self)

    async def schedule(self, name: str, spec: Any, workflow: Any, input: Any = None) -> Schedule:
        """Creates a schedule (or updates its spec) that starts ``workflow``
        with ``input`` on ``spec``: a cron string, a dict
        (``{"every": "1h"}``) or a :class:`ScheduleSpec`."""
        s = ScheduleSpec.of(spec).wire(name_of(workflow), input)
        problem = self._native.validate_schedule(json.dumps(s))
        if problem:
            raise RejectedError(f"schedule '{name}': {re.sub(r'^(rejected|codec error): ', '', problem)}")
        handle = Schedule(self, name)
        policy = IdPolicy("use-existing", "allow")
        await self._request(lambda tok: self._native.start(
            handle.id, _SCHEDULE_TYPE, 1, wire({"name": name, "spec": s}), 0, tok, policy.bits()))
        await self._signal(handle.id, _CONTROL, {"op": "update", "spec": s})
        return handle

    async def list(self, *, type: Any = None, status: Optional[str] = None, started_after: Any = None,
                   started_before: Any = None, attributes: Optional[dict] = None,
                   limit: int = 1000) -> AsyncIterator[RunSummary]:
        """Runs, newest first per shard, filtered by type, status, start time
        and search attributes (``{"amount": {"gte": 100}}``)::

            async for r in engine.list(status="failed"):
                print(r.id, r.error)
        """
        where = _filter(type, status, started_after, started_before, attributes)
        rows = json.loads(_call(lambda: self._native.list_where(json.dumps(where), limit)))
        for r in rows:
            yield RunSummary.from_json(r)

    async def complete(self, token: str, value: Any = None) -> None:
        """Finishes an async completion with a value; returns once recorded durably."""
        await self._request(lambda tok: self._native.complete(token, True, wire(value), tok))

    async def fail(self, token: str, failure: Any) -> None:
        """Fails an async completion (a message, an exception or a Failure)."""
        await self._request(lambda tok: self._native.complete(token, False, _failure_text(failure), tok))

    async def debounce(self, workflow: Any, key: str, input: Any = None, *, window: Any, max_wait: Any = None,
                       dedup_window: Any = None, event_id: str = "") -> Run[Any]:
        """Debounces a start under ``key``: starts inside the window collapse
        into one run, which begins with the latest start's input when the
        window closes (``window`` after the latest start, or ``max_wait``
        after the first). Returns once this start is durable, with the
        handle of that run (``await run.started()``). ``event_id`` makes a
        retried call join once."""
        w = ms(window)
        if w is None:
            raise RejectedError("debounce needs a window")
        return await self._enqueue("debounce", name_of(workflow), key, event_id, input, w, ms(max_wait),
                                   ms(dedup_window))

    async def batch(self, workflow: Any, key: str, event: Any = None, *, max_events: Optional[int] = None,
                    max_wait: Any = None, dedup_window: Any = None, event_id: str = "") -> Run[Any]:
        """Adds an event to the batch under ``key``: once it holds
        ``max_events`` events (default 100) or ``max_wait`` (default 1s) has
        passed since its first, one run starts with all of them as its
        input (a list)."""
        return await self._enqueue("batch", name_of(workflow), key, event_id, event,
                                   None if max_events is None else float(max_events), ms(max_wait), ms(dedup_window))

    def limit(self, name: str, *, concurrent: int = 0, per_second: float = 0.0) -> None:
        """Defines (or resizes) a concurrency and rate limit for steps."""
        _call(lambda: self._native.limit(name, concurrent, float(per_second)))
        self._limit_specs[name] = (concurrent, float(per_second))

    def limits(self) -> builtins.list[LimitInfo]:
        """Every limit this engine defined, with its permits now, by name
        (``fusee limits``, the UI's Limits page)."""
        out = []
        for name, (concurrent, per_second) in sorted(self._limit_specs.items()):
            in_use, waiting = self.limit_usage(name)
            out.append(LimitInfo(name, concurrent, per_second, in_use, waiting))
        return out

    def limit_usage(self, name: str) -> tuple:
        """``(in use, waiting here)`` permits of a limit right now."""
        return tuple(_call(lambda: self._native.limit_usage(name)))

    async def metrics(self) -> dict:
        """Counters summed over shards (as ``GET /v1/metrics``'s ``total``),
        with the runs stuck and waiting for code."""
        total: dict = {}
        for m in self.shard_metrics():
            for k, v in m.items():
                if isinstance(v, (int, float)):
                    total[k] = total.get(k, 0) + v
        for status, k in (("stuck", "runs_stuck"), ("paused", "runs_paused"), ("waiting_for_code", "runs_waiting_for_code")):
            rows = json.loads(_call(lambda: self._native.list_where(json.dumps({"status": status}), 1 << 30)))
            total[k] = len(rows)
        return total

    def shard_metrics(self) -> builtins.list[dict]:
        """Per-shard counters in shard-number order, durations in microseconds."""
        return json.loads(_call(self._native.shard_metrics))

    def export_journal(self, id: str) -> Optional[bytes]:
        """A run's journal as an export file (``.fjnl``) for ``Replayer``."""
        return _call(lambda: self._native.export_journal(id))

    async def close(self) -> None:
        self._close(self._failure or ClosedError())

    def _close(self, error: FuseeError) -> None:
        if self._closed:
            return
        self._closed = True
        for fut, _ in self._tokens.values():
            if not fut.done():
                fut.set_exception(error)
                # Nobody may await it any more: no "never retrieved" noise.
                fut.exception()
        for waiters in self._waiters.values():
            for fut in waiters:
                if not fut.done():
                    fut.set_exception(error)
                    fut.exception()
        self._tokens.clear()
        self._waiters.clear()
        if self._timer:
            self._timer.cancel()
        for r in list(self._runs.values()):
            r.end()
        self._loop.remove_reader(self._rfd)
        try:
            _call(self._native.close)
        finally:
            os.close(self._rfd)
            os.close(self._wfd)

    # -------------------------------------------------------------- the operations handles use

    def _input(self, wtype: str, input: Any) -> Any:
        try:
            return wire(input)
        except (TypeError, ValueError) as e:
            raise _codec.codec_error(f"the input of '{wtype}'", input, e) from None

    async def _already_exists(self, id: str) -> FuseeError:
        s = await self._status(id)
        if s is None:
            return AlreadyExistsError(f"workflow '{id}' already exists. Start it with "
                                      "id_policy={'running': 'use-existing'} to attach to it", id=id)
        if s.finished_ms is None:
            state = "" if s.status == "running" else f", {s.status}"
            return AlreadyExistsError(
                f"workflow '{id}' is already running (run {s.run_id}{state}). Start it with "
                "id_policy={'running': 'use-existing'} to attach to that run, or use another ID",
                id=id, run_id=s.run_id)
        return AlreadyExistsError(
            f"workflow '{id}' already ran (run {s.run_id}, {s.status}) and this start refuses a finished ID. "
            "Start it with id_policy={'finished': 'allow'} to run it again", id=id, run_id=s.run_id)

    async def _signal(self, id: str, name: str, value: Any) -> None:
        by = _BY.get()
        await self._request(lambda tok: self._native.signal(id, name, wire(value), tok, by))

    async def _update(self, id: str, name: str, arg: Any, uid: str) -> Any:
        return await self._request_value(lambda tok: self._native.update(id, name, uid, wire(arg), tok))

    async def _close_run(self, id: str, reason: str, terminate: bool) -> None:
        by = _BY.get()
        if terminate:
            await self._request(lambda tok: self._native.terminate(id, reason, tok, by))
        else:
            await self._request(lambda tok: self._native.cancel(id, reason, tok, by))

    async def _reset(self, id: str, before_step: str, reapply_signals: bool, version: int) -> None:
        by = _BY.get()
        await self._request(lambda tok: self._native.reset(id, before_step, reapply_signals, version, tok, by))

    async def _resume(self, id: str) -> None:
        by = _BY.get()
        await self._request(lambda tok: self._native.resume(id, tok, by))

    async def _status(self, id: str) -> Optional[RunSummary]:
        rows = json.loads(_call(lambda: self._native.list_where(json.dumps({"id": id}), 1)))
        return RunSummary.from_json(rows[0]) if rows else None

    async def _describe(self, id: str) -> Optional[RunDetail]:
        j = _call(lambda: self._native.inspect(id))
        if not j:
            return None
        d = json.loads(j)
        journal = d.pop("journal", [])
        obj = d.pop("object", None)
        return RunDetail(RunSummary.from_json(d), journal, obj)

    async def _object_call(self, type: str, id: str, handler: str, arg: Any, update_id: str) -> Any:
        policy = IdPolicy("use-existing", "allow")
        latest = self._latest(type, 1)
        return await self._request_value(lambda tok: self._native.start(
            id, type, latest, "null", 0, -1, policy.bits(), (handler, update_id, wire(arg), tok)))

    async def _edit_state(self, type: str, id: str, edit: dict, update_id: str) -> Any:
        return await self._object_call(type, id, EDIT_HANDLER, edit, update_id)

    def _latest(self, wtype: str, version: int) -> int:
        return max([version, *self._types.get(wtype, {}).keys()])

    def _on_notify(self) -> None:
        try:
            while os.read(self._rfd, 4096):
                pass
        except (BlockingIOError, OSError):
            pass
        if _KICK_PIPE:
            self._kicked = False
        self._tick()

    async def _request(self, f: Callable[[int], Any], start: bool = False) -> None:
        await self._request_value(f, value=False, start=start)

    async def _stream_read(self, id: str, run: Optional[int], name: str, start: int, wait_ms: int) -> dict:
        """One read of a stream: the durable chunks from ``start``, or those
        appended within ``wait_ms``."""
        return await self._request_value(lambda tok: self._native.stream_read(id, run, name, start, wait_ms, tok))

    async def _request_value(self, f: Callable[[int], Any], value: bool = True, start: bool = False) -> Any:
        if self._closed:
            raise self._failure or ClosedError()
        tok = self._next_token
        self._next_token += 1
        fut = self._loop.create_future()
        # A refusal of a start means its ID is taken; of anything else, that
        # the run has finished.
        self._tokens[tok] = (fut, start)
        try:
            f(tok)
        except RuntimeError as e:
            self._tokens.pop(tok, None)
            raise native_error(e) from None
        self._kick()
        return await fut

    async def _query(self, id: str, name: str, arg: Any) -> Any:
        for _ in range(4000):
            shard, run, lsn, foreign = _call(lambda: self._native.query_point(id))
            x = self._runs.get(key(shard, run))
            state = _call(lambda: self._native.code_state(shard, run))
            if x is None and not foreign:
                if state is None:
                    raise RejectedError(f"workflow '{id}' is not running code: it finished, or it waits for code")
                return await self._request_value(lambda tok: self._native.query_native(id, name, encode(arg), tok))
            if x is None or (state is not None and state[0]):
                self._kick()
                await asyncio.sleep(0.001)
                continue
            h = x.queries.get(name)
            if h is None:
                raise RejectedError(f"workflow '{id}' has no handler for query '{name}'"
                                    + (f"; it has: {', '.join(sorted(x.queries))}" if x.queries else ""))
            fn, dec = h
            answer = fn(dec(arg) if dec is not None and arg is not None else arg)
            await self._request(lambda tok: self._native.ack_at(shard, lsn, tok))
            return answer
        raise FuseeError("the workflow did not finish replaying")

    async def _enqueue(self, kind: str, workflow: str, key: str, event_id: str, value: Any,
                       a: Optional[float], b: Optional[float], c: Optional[float]) -> Run[Any]:
        """One debounced start or batched event (see ``fusee::queue``):
        update-with-start on the key's coordinator, sent again while the
        coordinator it reached was ending. Returns the run it joined."""
        wtype, cid, state, arg = _queue_request(kind, workflow, key, event_id, value, a, b, c)
        policy = IdPolicy("use-existing", "allow")
        run = await _until_taken(cid, lambda: self._request_value(lambda tok: self._native.start(
            cid, wtype, 1, state, 0, -1, policy.bits(), (_EVENT, event_id, arg, tok))))
        return Run(self, run)

    async def _exists(self, id: str) -> bool:
        """Whether a run with this ID exists (running, parked, or finished
        within retention)."""
        return _call(lambda: self._native.result(id))[0] != "notfound"

    async def _result(self, id: str) -> Any:
        while True:
            state, shard, run, status, payload = _call(lambda: self._native.result(id))
            if state == "notfound":
                if not id.startswith("fusee."):
                    # A delayed start, or a run waiting for its turn: the run
                    # that will start it gives its result.
                    for waiting in (f"{_DELAY_TYPE}/{id}", f"{_QUEUED_TYPE}/{id}"):
                        try:
                            return await self._result(waiting)
                        except NotFoundError:
                            pass
                raise NotFoundError(f"workflow '{id}' not found", id=id)
            if state == "done":
                return _settle_result(status, tagged_to_wire(payload), id)
            fut = self._loop.create_future()
            self._waiters.setdefault(key(shard, run), []).append(fut)
            self._kick()
            status, payload = await fut
            if status in (CONTINUED, RESET):
                continue  # follow the ID to the run that took over
            return _settle_result(status, payload, id)

    def _kick(self) -> None:
        if self._kicked or self._closed:
            return
        self._kicked = True
        if _KICK_PIPE:
            # Through the wake pipe, as a background wake: the loop's next
            # select returns at once on a readable descriptor.
            self._native.wake()
        else:
            self._loop.call_soon(self._tick_soon)

    def _tick_soon(self) -> None:
        self._kicked = False
        self._tick()

    async def _handle_remote(self, call: dict) -> None:
        a = call["args"]
        method = call["method"]
        rid = a.get("id", "")
        value = None
        # Who asked, as the API that took the call named them (audit).
        by_token = _BY.set(a.get("by") or None) if method in _AUDITED else None
        try:
            options = a.get("options") or {}
            policy = options.get("idPolicy") or {}
            if method == "start":
                await self.start(a["type"], rid, a.get("input"), id_policy=policy, timeout=options.get("timeout"),
                                 version=a.get("version"))
            elif method == "updateWithStart":
                _, value = await self.update_with_start(a["type"], rid, a.get("input"), a["name"], a.get("arg"),
                                                        update_id=a.get("updateId", ""), id_policy=policy,
                                                        timeout=options.get("timeout"), version=a.get("version"))
            elif method == "signalWithStart":
                await self.signal_with_start(a["type"], rid, a.get("input"), a["name"], a.get("value"),
                                             id_policy=policy, timeout=options.get("timeout"),
                                             version=a.get("version"))
            elif method == "signal":
                await self._signal(rid, a["name"], a.get("value"))
            elif method == "query":
                value = await self._query(rid, a["name"], a.get("arg"))
            elif method == "update":
                value = await self._update(rid, a["name"], a.get("arg"), a.get("updateId", ""))
            elif method == "result":
                value = await self._result(rid)
            elif method == "complete":
                await self.complete(a["token"], a.get("value"))
            elif method == "fail":
                await self.fail(a["token"], Failure.decode(a.get("error", "")))
            elif method == "cancel":
                await self._close_run(rid, a["reason"], False)
            elif method == "resume":
                await self._resume(rid)
            elif method == "terminate":
                await self._close_run(rid, a["reason"], True)
            elif method == "reset":
                await self._reset(rid, a["beforeStep"], a.get("reapplySignals", True), a.get("version", 0))
            elif method == "describe":
                d = await self._describe(rid)
                if d is None:
                    raise NotFoundError(f"workflow '{rid}' not found", id=rid)
                value = {**_summary_wire(d.summary), "journal": d.journal, "objectDetail": d.object}
            elif method == "types":
                value = [{"name": n, "versions": sorted(vs), "object": n in self._objects}
                         for n, vs in sorted(self._types.items()) if not n.startswith("fusee.")]
            elif method == "metrics":
                shards = self.shard_metrics()
                for status, k in (("stuck", "runs_stuck"), ("paused", "runs_paused"), ("waiting_for_code", "runs_waiting_for_code")):
                    for m in shards:
                        m[k] = 0
                    rows = json.loads(_call(lambda: self._native.list_where(json.dumps({"status": status}), 1 << 30)))
                    for row in rows:
                        if 0 <= row.get("shard", -1) < len(shards):
                            shards[row["shard"]][k] += 1
                total: dict = {}
                for m in shards:
                    for k, v in m.items():
                        if isinstance(v, (int, float)):
                            total[k] = total.get(k, 0) + v
                value = {"total": total, "shards": shards}
            elif method == "limits":
                value = [{"name": x.name, "concurrent": x.concurrent, "perSecond": x.per_second,
                          "inUse": x.in_use, "waiting": x.waiting} for x in self.limits()]
            elif method == "info":
                value = {"engine": "fusee", "sdk": "python", "durability": self._durability, "dir": self._dir,
                         "shards": len(self.shard_metrics()), "profile": "dev" if self._dev else None}
            elif method == "export":
                b = self.export_journal(rid)
                if b is None:
                    raise NotFoundError(f"workflow '{rid}' not found", id=rid)
                value = {"file": base64.b64encode(b).decode(), "name": f"{rid}.fjnl"}
            elif method == "list":
                value = json.loads(_call(lambda: self._native.list_where(json.dumps(a.get("where") or {}),
                                                                      a.get("limit", 100))))
                for row in value:
                    row["startedMs"] = row.pop("started_ms")
                    row["finishedMs"] = row.pop("finished_ms")
            elif method == "streamRead":
                value = await self._stream_read(rid, a.get("run"), a["name"], int(a.get("from") or 0),
                                                min(int(a.get("waitMs") or 30_000), 60_000))
            else:
                raise RejectedError(f"unknown remote call '{method}'")
            response = {"value": value}
        except Exception as exc:  # noqa: BLE001 - answered to the client
            code = getattr(exc, "code", "WorkflowFailed")
            msg = str(exc)
            cause = getattr(exc, "cause", None)
            if code in ("StepFailed", "WorkflowFailed", "ChildFailed"):
                msg = failure_of(exc, False).encode().decode() if not isinstance(cause, Failure) or \
                    code != "WorkflowFailed" else cause.encode().decode()
            response = {"error": msg, "kind": _snake(code)}
        finally:
            if by_token is not None:
                _BY.reset(by_token)
        if not self._closed:
            try:
                self._native.reply_call(int(call["relay"]), json.dumps(response))
            except (OSError, RuntimeError) as exc:
                self._stop_after_failure(exc)

    def _stop_after_failure(self, exc: Exception) -> None:
        self._failure = native_error(exc)
        try:
            self._close(self._failure)
        except FuseeError:
            # Failed storage may also fail its final flush. Pending and later
            # calls receive the original error that stopped this engine.
            pass

    def _tick(self) -> None:
        if self._closed:
            return
        self._inline_left = _INLINE_PER_TICK
        try:
            ev, extras, next_ms, commits, owed = self._native.tick()
            if self._serve_remote_calls:
                while (message := self._native.next_call()) is not None:
                    self._loop.create_task(self._handle_remote(json.loads(message)))
        except (OSError, RuntimeError) as exc:
            self._stop_after_failure(exc)
            return
        self._be.commits = commits
        if ev:
            self._batch = batch = []
            try:
                for i in range(0, len(ev), 6):
                    self._handle(ev, i, extras)
            finally:
                self._batch = None
            if batch:
                self._pump_batch(batch)
        now = now_ms()
        at = next_ms if next_ms is not None else (now + 50 if self._tokens or self._waiters else None)
        if at is None:
            if self._timer:
                self._timer.cancel()
                self._timer = None
        elif at <= now:
            self._kick()  # due now: the next loop turn
        elif self._timer is None or at < self._timer_at:
            # A timer that fires no later than needed stays: an early tick
            # only looks again.
            if self._timer:
                self._timer.cancel()
            self._timer_at = at
            self._timer = self._loop.call_later((at - now) / 1000, self._on_timer)
        if ev or owed:
            self._kick()

    def _on_timer(self) -> None:
        self._timer = None
        self._tick()

    def _handle(self, ev: list, i: int, extras: Optional[list]) -> None:
        """One event of the flat list: ``ev[i]`` is its kind, its strings and
        payloads are in ``extras`` from index ``ev[i + 5]``."""
        kind = ev[i]
        if kind == _EV_WAKE:
            r = self._runs.get(key(ev[i + 1], ev[i + 2]))
            if r is not None:
                r.wake()
        elif kind in (_EV_ACK, _EV_REJECT, _EV_REPLY, _EV_REFUSE):
            entry = self._tokens.pop(ev[i + 1], None)
            if entry is None:
                return
            fut, start = entry
            if fut.done():
                return
            if kind == _EV_ACK:
                fut.set_result(None)
            elif kind == _EV_REPLY:
                payload = extras[ev[i + 5]]
                if ev[i + 2] == OK:
                    try:
                        fut.set_result(from_wire(payload))
                    except ValueError as e:
                        fut.set_exception(CodecError(f"a reply is not JSON: {e}"))
                else:
                    fut.set_exception(error_of(Failure.decode(wire_text(payload))))
            elif kind == _EV_REFUSE:
                fut.set_exception(RejectedError(extras[ev[i + 5]]))
            elif start:
                fut.set_exception(AlreadyExistsError("the workflow ID is in use"))
            else:
                fut.set_exception(RejectedError("the workflow has already finished"))
        elif kind == _EV_RESULT:
            futs = self._waiters.pop(key(ev[i + 1], ev[i + 2]), None)
            if futs:
                outcome = (ev[i + 3], extras[ev[i + 5]])
                for fut in futs:
                    if not fut.done():
                        fut.set_result(outcome)
        elif kind == _EV_START:
            x = ev[i + 5]
            self._on_start({"shard": ev[i + 1], "run": ev[i + 2], "version": ev[i + 3], "started_ms": ev[i + 4],
                            "id": extras[x], "wtype": extras[x + 1], "input": extras[x + 2],
                            "trace": extras[x + 3]})
        elif kind == _EV_STUCK:
            k = key(ev[i + 1], ev[i + 2])
            r = self._runs.get(k)
            message = extras[ev[i + 5]] if extras is not None and ev[i + 5] >= 0 else ""
            if r is not None:
                self._emit("stuck", r.id, message)
            if self._dev:
                # Under the dev profile a stuck run's result fails at once.
                for fut in self._waiters.pop(k, ()):
                    if not fut.done():
                        fut.set_exception(DeterminismError(message, workflow_id=r.id if r else None))
            self._on_ended(ev[i + 1], ev[i + 2])
        elif kind == _EV_ENDED:
            r = self._runs.get(key(ev[i + 1], ev[i + 2]))
            if r is not None:
                self._run_settled(r)
            self._on_ended(ev[i + 1], ev[i + 2])

    # -- strict replay (dev profile) ------------------------------------------

    def _replaying(self, x: RunExec) -> bool:
        state = self._native.code_state(x.shard, x.run)
        return bool(state and state[0])

    def _run_settled(self, x: RunExec) -> None:
        if self._dev:
            self._strict_replay(x.id)

    def _strict_replay(self, id: str) -> None:
        """Queues a replay of the run's journal against the code, off the
        step path, once its code has parked or ended."""
        self._strict_pending.append(id)
        if self._strict_task is None or self._strict_task.done():
            self._strict_task = self._loop.create_task(self._strict_drain())

    async def _strict_drain(self) -> None:
        from ._replay import Replayer

        # The run's last records reach its journal with the next flush.
        await asyncio.sleep(0.02)
        while self._strict_pending and not self._closed:
            id = self._strict_pending.pop(0)
            try:
                data = self.export_journal(id)
            except FuseeError:
                continue
            if not data:
                continue
            workflows = [wf for versions in self._types.values() for wf in versions.values()]
            try:
                rep = await Replayer(workflows, codec=self._codec_keys).replay(data)
            except Exception as ex:  # noqa: BLE001 - a check, never a failure of the run
                log.debug("strict replay of %r could not run: %s", id, ex)
                continue
            err = rep.error
            if not err or err.startswith("no code registered") or err.startswith("the export holds only"):
                continue
            if (id, err) in self._strict_seen:
                continue
            self._strict_seen.add((id, err))
            log.warning(
                "strict replay: run %r (%s) does not replay against its code: %s. Code outside steps "
                "must make the same calls in the same order every time: read the clock, random numbers "
                "and unordered collections inside a step. %s",
                id, rep.workflow_type, err, docs_link("Determinism"))

    def _pump_batch(self, runs: list) -> None:
        """Delivers to the runs a tick woke, a round at a time with one
        native call per round: each run gets at most one delivery per round,
        then its code runs until every task is blocked, as on replay between
        two deliveries."""
        for x in runs:
            x.batched = False
        n = self._native
        while runs:
            live = [x for x in runs if not x.ended and not x.scheduling and not x.local_waits]
            if not live:
                break
            try:
                # Step results the runs' code held go with their deliveries.
                held = [x.held_done for x in live]
                if any(h is not None for h in held) and hasattr(n, "quiescent_many_done"):
                    for x in live:
                        x.held_done = None
                    ds = n.quiescent_many_done(
                        [x.shard for x in live], [x.run for x in live],
                        [-1 if h is None else h[0] for h in held], [None if h is None else h[1] for h in held])
                else:
                    ds = n.quiescent_many([x.shard for x in live], [x.run for x in live])
            except RuntimeError:
                # One at a time, so each run fails with the engine's error.
                for x in live:
                    x.pump()
                return
            runs = []
            for x, d in zip(live, ds):
                if d is None:
                    continue
                x.scheduling = True
                try:
                    x._apply(d)
                    x._drain()
                finally:
                    x.scheduling = False
                    x.deliver = False
                runs.append(x)
        self._kick()

    def _inline_ok(self) -> bool:
        """Whether a lone workflow may flush at its gate now (a budget per
        tick, so a chain of steps yields to asyncio)."""
        if self._inline_left <= 0:
            return False
        self._inline_left -= 1
        return True


def _snake(code: str) -> str:
    return re.sub(r"([a-z])([A-Z])", r"\1_\2", code).lower()


def _summary_wire(s: RunSummary) -> dict:
    return {"id": s.id, "type": s.type, "version": s.version, "status": s.status, "startedMs": s.started_ms,
            "finishedMs": s.finished_ms, "error": s.error, "run": s.run_id, "shard": s.shard,
            "attributes": s.attributes, "object": s.object}


def _nearest(name: str, names: builtins.list) -> Optional[str]:
    limit = max(1, min(3, len(name) // 3))
    best = None
    for c in names:
        d = _levenshtein(name, c)
        if d <= limit and (best is None or d < best[0]):
            best = (d, c)
    return best[1] if best else None


def _levenshtein(a: str, b: str) -> int:
    row = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        prev, row[0] = row[0], i + 1
        for j, cb in enumerate(b):
            cur = row[j + 1]
            row[j + 1] = prev if ca == cb else 1 + min(prev, row[j], row[j + 1])
            prev = cur
    return row[len(b)]


_ = (time, base64, Signal)
