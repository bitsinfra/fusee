"""Definitions: workflows, signals, queries, updates and object types as
plain values, apart from any engine. A client may import them for their
names and types alone; an engine runs the workflows it is opened with.

    review = fusee.signal("review", bool)

    @fusee.workflow("checkout")
    async def checkout(ctx: fusee.Ctx, order: Order) -> str: ...

    engine = await fusee.Engine.open("./data", workflows=[checkout])
    run = await engine.start(checkout, "order-123", order)    # Run[str]
"""

from __future__ import annotations

import dataclasses
import inspect
import json
import random as _random
import typing
from typing import Any, Awaitable, Callable, Generic, Optional, TypeVar, Union, overload

from ._codec import Decoder, decoder_for, json_text, ms
from ._errors import Failure, FuseeError, RejectedError

I = TypeVar("I")
O = TypeVar("O")
T = TypeVar("T")
A = TypeVar("A")
R = TypeVar("R")


class Workflow(Generic[I, O]):
    """A workflow type. Calling it runs the function directly (for tests)."""

    def __init__(self, name: str, fn: Callable[..., Awaitable[O]], *, version: int = 1, timeout=None,
                 input: Any = None, output: Any = None, concurrency: Optional["Concurrency"] = None):
        if not name:
            raise RejectedError("a workflow needs a name")
        if not isinstance(version, int) or version < 1:
            raise RejectedError(f"workflow '{name}': version is a whole number from 1, not {version!r}")
        if concurrency is not None and (not isinstance(concurrency.limit, int) or concurrency.limit < 1):
            raise RejectedError(f"workflow '{name}': concurrency.limit is a whole number from 1, not {concurrency.limit!r}")
        self.name = name
        self.version = version
        self.concurrency = concurrency
        self.fn = fn
        self.timeout = ms(timeout)
        self.__wrapped__ = fn
        # Annotations are resolved once, at first use (a forward reference
        # may name a class defined after the workflow).
        self._types: Optional[tuple] = None if input is None and output is None else (input, output)
        self._input: Optional[Decoder] = None
        self._output: Optional[Decoder] = None
        if self._types is not None:
            self._input = decoder_for(input)
            self._output = decoder_for(output)

    def _resolve(self) -> None:
        if self._types is not None:
            return
        tin = tout = None
        try:
            hints = typing.get_type_hints(self.fn)
            params = list(inspect.signature(self.fn).parameters)
            if len(params) >= 2:
                tin = hints.get(params[1])
            tout = hints.get("return")
        except Exception:  # noqa: BLE001 - unresolvable annotations: plain data
            pass
        self._types = (tin, tout)
        self._input = decoder_for(tin)
        self._output = decoder_for(tout)

    def decode_input(self, v: Any) -> Any:
        self._resolve()
        return v if self._input is None or v is None else self._input(v)

    def decode_output(self, v: Any) -> Any:
        self._resolve()
        return v if self._output is None or v is None else self._output(v)

    def check_input(self, v: Any) -> None:
        """Validates an input from outside before it is recorded (a refusal
        records nothing). Only for annotated types that validate."""
        self._resolve()
        if self._input is None or v is None:
            return
        try:
            self._input(json.loads(json_text(v)))
        except FuseeError:
            raise
        except Exception as e:  # noqa: BLE001 - the validator's reason
            raise RejectedError(f"workflow '{self.name}': the input does not match its type: {e}") from None

    def __call__(self, ctx: Any, input: I) -> Awaitable[O]:
        return self.fn(ctx, input)

    def __repr__(self) -> str:
        return f"Workflow({self.name!r}, version={self.version})"


@overload
def workflow(name: Callable[[Any, I], Awaitable[O]], /) -> Workflow[I, O]: ...
@overload
def workflow(name: str, /, *, version: int = 1, timeout: Any = None, input: Any = None,
             output: Any = None, concurrency: Optional["Concurrency"] = None
             ) -> Callable[[Callable[[Any, I], Awaitable[O]]], Workflow[I, O]]: ...
@overload
def workflow(name: str, fn: Callable[[Any, I], Awaitable[O]], /, *, version: int = 1, timeout: Any = None,
             input: Any = None, output: Any = None, concurrency: Optional["Concurrency"] = None) -> Workflow[I, O]: ...


@dataclasses.dataclass(frozen=True)
class Concurrency:
    """Runs at once: at most ``limit`` runs of the type run at a time, or of
    each key (``key(input)``, a tenant say); the others wait, in the order
    they were started, as ``fusee.queued/<id>`` runs, and ``result()`` waits
    through the queue. The turns are durable: a run keeps its turn while it
    sleeps and across crashes. Starts from outside a workflow take turns; a
    child started by a workflow does not."""

    limit: int
    key: Optional[Callable[[Any], str]] = None


def workflow(name: Any = None, fn: Any = None, /, *, version: int = 1, timeout: Any = None, input: Any = None,
             output: Any = None, concurrency: Optional[Concurrency] = None) -> Any:
    """Defines a workflow type::

        @fusee.workflow("checkout")
        async def checkout(ctx: fusee.Ctx, order: Order) -> str: ...

    ``@fusee.workflow`` alone names it after the function. The input's and
    result's annotations (a dataclass, pydantic model, msgspec struct,
    ``TypedDict``, ``datetime``, ...) decide what the code receives and what
    ``run.result()`` returns; ``input=`` and ``output=`` override them.
    ``timeout`` is the workflow's deadline from its start."""
    if callable(name) and fn is None:
        return Workflow(name.__name__, name, version=version, timeout=timeout, input=input, output=output,
                        concurrency=concurrency)
    if fn is not None:
        return Workflow(name, fn, version=version, timeout=timeout, input=input, output=output,
                        concurrency=concurrency)

    def define(f: Callable[[Any, I], Awaitable[O]]) -> Workflow[I, O]:
        return Workflow(name or f.__name__, f, version=version, timeout=timeout, input=input, output=output,
                        concurrency=concurrency)

    return define


class Signal(Generic[T]):
    """A signal's name and payload type: ``review = fusee.signal("review", bool)``."""

    __slots__ = ("name", "type", "_decode")

    def __init__(self, name: str, type: Any = None):
        self.name = name
        self.type = type
        self._decode = decoder_for(type)

    def decode(self, v: Any) -> Any:
        return v if self._decode is None or v is None else self._decode(v)

    def __repr__(self) -> str:
        return f"Signal({self.name!r})"


class Query(Generic[A, R]):
    __slots__ = ("name", "arg", "result", "_decode_arg", "_decode_result")

    def __init__(self, name: str, *, arg: Any = None, result: Any = None):
        self.name = name
        self.arg = arg
        self.result = result
        self._decode_arg = decoder_for(arg)
        self._decode_result = decoder_for(result)

    def __repr__(self) -> str:
        return f"Query({self.name!r})"


class Update(Generic[A, R]):
    __slots__ = ("name", "arg", "result", "_decode_arg", "_decode_result")

    def __init__(self, name: str, *, arg: Any = None, result: Any = None):
        self.name = name
        self.arg = arg
        self.result = result
        self._decode_arg = decoder_for(arg)
        self._decode_result = decoder_for(result)

    def __repr__(self) -> str:
        return f"Update({self.name!r})"


def signal(name: str, type: Optional[typing.Type[T]] = None) -> Signal[T]:
    """Defines a signal. A plain string works wherever a signal does."""
    return Signal(name, type)


def query(name: str, *, arg: Any = None, result: Any = None) -> Query[Any, Any]:
    """Defines a query: ``status = fusee.query("status", result=Status)``."""
    return Query(name, arg=arg, result=result)


def update(name: str, *, arg: Any = None, result: Any = None) -> Update[Any, Any]:
    """Defines an update: ``approve = fusee.update("approve", arg=Approval, result=bool)``."""
    return Update(name, arg=arg, result=result)


def name_of(d: Any) -> str:
    """The name a definition or a plain string gives."""
    if isinstance(d, str):
        return d
    n = getattr(d, "name", None)
    if isinstance(n, str):
        return n
    raise RejectedError(f"expected a name or a definition, not {type(d).__name__}")


class _Shared:
    __slots__ = ("fn",)

    def __init__(self, fn):
        self.fn = fn


def shared(fn: Callable[..., Any]) -> Any:
    """Marks an object handler as shared: it runs as soon as its call
    arrives, next to other calls, and only reads state."""
    return _Shared(fn)


class ObjectType:
    """A keyed object type. Handlers are exclusive (one call at a time per
    key, in arrival order; they may change state) unless marked shared::

        cart = fusee.object("cart", {"add": add, "items": fusee.shared(items)})

    or with decorators::

        cart = fusee.object("cart")

        @cart.exclusive
        async def add(ctx, item): ...
    """

    def __init__(self, name: str, handlers: Optional[dict] = None, *, version: int = 1):
        if not name or "/" in name:
            raise RejectedError(f"an object type's name is not empty and has no '/': '{name}'")
        self.name = name
        self.version = version
        self.handlers: dict = dict(handlers or {})
        # Engines this type is registered with: a handler added later reaches them.
        self._engines: list = []

    def _add(self, fn, name: Optional[str], is_shared: bool):
        self.handlers[name or fn.__name__] = _Shared(fn) if is_shared else fn
        for e in self._engines:
            e._register_object_def(self.name, self.handlers)
        return fn

    def exclusive(self, fn=None, *, name: Optional[str] = None):
        """Adds an exclusive handler (``async def h(ctx, arg)``)."""
        if fn is None:
            return lambda f: self._add(f, name, False)
        return self._add(fn, name, False)

    def shared(self, fn=None, *, name: Optional[str] = None):
        """Adds a shared handler: it runs at once, next to other calls, and
        only reads state."""
        if fn is None:
            return lambda f: self._add(f, name, True)
        return self._add(fn, name, True)

    def __repr__(self) -> str:
        return f"ObjectType({self.name!r})"


def object(name: str, handlers: Optional[dict] = None, *, version: int = 1) -> ObjectType:  # noqa: A001
    """Defines a keyed object type (see :class:`ObjectType`)."""
    return ObjectType(name, handlers, version=version)


# ------------------------------------------------------------- options

_RUNNING = {"fail": 0, "use-existing": 1, "terminate-existing": 2}
_FINISHED = {"allow": 0, "allow-if-failed": 1, "refuse": 2, "use-existing": 3}


@dataclasses.dataclass(frozen=True)
class IdPolicy:
    """What a start does when its workflow ID is taken. ``running``: fail,
    use-existing, terminate-existing. ``finished``: allow, allow-if-failed,
    refuse, use-existing."""

    running: str = "fail"
    finished: str = "allow"

    def bits(self) -> int:
        try:
            return _RUNNING[self.running] | (_FINISHED[self.finished] << 2)
        except KeyError as e:
            raise RejectedError(
                f"unknown ID policy value {e.args[0]!r}: running is one of {', '.join(_RUNNING)}; "
                f"finished is one of {', '.join(_FINISHED)}") from None


def id_policy(p: Union[None, IdPolicy, dict], running: str = "fail", finished: str = "allow") -> IdPolicy:
    if p is None:
        return IdPolicy(running, finished)
    if isinstance(p, IdPolicy):
        return p
    if isinstance(p, dict):
        return IdPolicy(p.get("running", running), p.get("finished", finished))
    raise RejectedError(f"id_policy is an IdPolicy or a dict, not {type(p).__name__}")


@dataclasses.dataclass
class Retry:
    """How a failing step is retried. ``attempts`` includes the first.

    A failure marked final (:class:`fusee.Fatal`, or any exception with
    ``retryable = False``) is never retried. ``non_retryable`` names
    exception types that are final; ``retry_if(failure, attempt)`` decides
    each time. Each delay is drawn between half of it and all of it unless
    ``jitter=False``; no retry starts later than ``max_elapsed`` after the
    first failure. ``on_exhausted="pause"``: when the retries run out on a
    failure that could be retried, the run pauses (its parent and callers
    keep waiting) until ``run.resume()`` runs the step again with a fresh
    set of attempts; ``"fail"`` fails the step; ``None`` takes the engine's
    ``on_exhausted``."""

    attempts: int = 3
    initial: Union[int, float, str] = 100
    factor: float = 2.0
    max: Union[int, float, str] = "10s"
    jitter: bool = True
    max_elapsed: Union[None, int, float, str] = None
    non_retryable: tuple = ()
    retry_if: Optional[Callable[[Failure, int], bool]] = None
    on_exhausted: Optional[str] = None

    def delay_ms(self, failed_attempt: int) -> float:
        initial = ms(self.initial) or 0.0
        cap = ms(self.max) or 0.0
        d = min(cap, initial * self.factor ** (failed_attempt - 1))
        if self.jitter and d > 1:
            d = d / 2 + _random.random() * (d / 2)
        return d

    def allows(self, f: Failure, attempt: int) -> bool:
        return attempt < self.attempts and self.retryable(f, attempt)

    def retryable(self, f: Failure, attempt: int) -> bool:
        """Whether the policy retries ``f`` at all (it is not final),
        whatever attempts remain."""
        if f.retryable is False:
            return False
        if any(f.has_type(t if isinstance(t, str) else t.__name__) for t in self.non_retryable):
            return False
        return self.retry_if is None or bool(self.retry_if(f, attempt))


#: The policy when none is given: three attempts, about 100 ms and 200 ms apart.
DEFAULT_RETRY = Retry()
