"""A blocking client, for code without an event loop (Django views, Flask
handlers, scripts)::

    from fusee.sync import Client

    client = Client.connect("127.0.0.1:7600")      # or FUSEE_URL
    run = client.start(checkout, "order-1", order)
    receipt = run.result(timeout=30)
    client.close()

It runs the async :class:`fusee.Client` on a thread of its own; each call
blocks until the server answers. It is safe to share between threads.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from typing import Any, Generic, Optional, TypeVar

from ._client import Client as AsyncClient
from ._engine import Run as AsyncRun
from ._engine import RunDetail, RunSummary
from ._errors import TimeoutError

__all__ = ["Client", "Run"]

O = TypeVar("O")


class _Loop:
    """An event loop on a daemon thread."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, name="fusee-sync", daemon=True)
        self.thread.start()

    def run(self, coro, timeout: Optional[float] = None) -> Any:
        fut = asyncio.run_coroutine_threadsafe(coro, self.loop)
        try:
            return fut.result(timeout)
        except concurrent.futures.TimeoutError:
            fut.cancel()
            raise TimeoutError(f"no answer within {timeout} s") from None

    def stop(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5)


class Run(Generic[O]):
    """A run, by workflow ID (the blocking form of :class:`fusee.Run`)."""

    def __init__(self, loop: _Loop, run: AsyncRun):
        self._loop = loop
        self._run = run

    @property
    def id(self) -> str:
        return self._run.id

    def __repr__(self) -> str:
        return f"fusee.sync.Run({self.id!r})"

    def result(self, timeout: Optional[float] = None) -> O:
        """Waits for the result; ``timeout`` in seconds raises
        ``fusee.TimeoutError`` (the run goes on)."""
        return self._loop.run(self._run.result(), timeout)

    def signal(self, signal: Any, value: Any = None) -> None:
        self._loop.run(self._run.signal(signal, value))

    def query(self, query: Any, arg: Any = None) -> Any:
        return self._loop.run(self._run.query(query, arg))

    def update(self, update: Any, arg: Any = None, *, id: str = "") -> Any:
        return self._loop.run(self._run.update(update, arg, id=id))

    def status(self) -> RunSummary:
        return self._loop.run(self._run.status())

    def describe(self) -> RunDetail:
        return self._loop.run(self._run.describe())

    def cancel(self, reason: str = "cancelled") -> None:
        self._loop.run(self._run.cancel(reason))

    def terminate(self, reason: str = "terminated") -> None:
        self._loop.run(self._run.terminate(reason))

    def resume(self) -> None:
        """Resumes a paused run (see :meth:`fusee.Run.resume`)."""
        self._loop.run(self._run.resume())

    def retry(self) -> None:
        self._loop.run(self._run.retry())


class Client:
    """The blocking form of :class:`fusee.Client` (see the module)."""

    def __init__(self, loop: _Loop, client: AsyncClient):
        self._loop = loop
        self._client = client

    @classmethod
    def connect(cls, address: Optional[str] = None, **options: Any) -> "Client":
        """As ``fusee.Client.connect``: ``address`` defaults to ``FUSEE_URL``."""
        loop = _Loop()
        try:
            client = loop.run(AsyncClient.connect(address, **options))
        except BaseException:
            loop.stop()
            raise
        return cls(loop, client)

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def run(self, id: str, workflow: Any = None) -> Run[Any]:
        """A handle; makes no call."""
        return Run(self._loop, self._client.run(id, workflow))

    def start(self, workflow: Any, id: str, input: Any = None, **options: Any) -> Run[Any]:
        return Run(self._loop, self._loop.run(self._client.start(workflow, id, input, **options)))

    def signal_with_start(self, workflow: Any, id: str, input: Any, signal: Any, value: Any = None,
                          **options: Any) -> Run[Any]:
        return Run(self._loop, self._loop.run(
            self._client.signal_with_start(workflow, id, input, signal, value, **options)))

    def update_with_start(self, workflow: Any, id: str, input: Any, update: Any, arg: Any = None,
                          **options: Any) -> tuple:
        run, result = self._loop.run(self._client.update_with_start(workflow, id, input, update, arg, **options))
        return Run(self._loop, run), result

    def list(self, **filters: Any) -> list:
        """Runs matching the filters of ``fusee.Client.list``, as a list."""

        async def collect():
            return [r async for r in self._client.list(**filters)]

        return self._loop.run(collect())

    def complete(self, token: str, value: Any = None) -> None:
        self._loop.run(self._client.complete(token, value))

    def fail(self, token: str, failure: Any) -> None:
        self._loop.run(self._client.fail(token, failure))

    def close(self) -> None:
        try:
            self._loop.run(self._client.close())
        finally:
            self._loop.stop()
