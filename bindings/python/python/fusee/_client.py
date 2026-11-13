"""A client of a Fusee server: the same interface as ``Engine``, with calls
sent to whichever worker holds each run's shard."""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any, AsyncIterator, Optional, TypeVar, overload
from urllib.parse import quote

from . import _codec, _native
from ._codec import ms
from ._defs import IdPolicy, Update, Workflow, name_of
from ._defs import id_policy as _id_policy
from ._engine import (
    _CONTROL,
    _EVENT,
    _SCHEDULE_TYPE,
    ObjectRef,
    Run,
    RunDetail,
    RunSummary,
    Schedule,
    Schedules,
    ScheduleSpec,
    _failure_text,
    _filter,
    _queue_request,
    _until_taken,
)
from ._errors import (
    AlreadyExistsError,
    CancelledError,
    ConnectionError,
    Failure,
    FuseeError,
    NotFoundError,
    RejectedError,
    TerminatedError,
    TimeoutError,
    error_of,
    native_error,
)

O = TypeVar("O")

_KINDS = {
    "not_found": NotFoundError,
    "already_exists": AlreadyExistsError,
    "rejected": RejectedError,
    "busy": None,
}


def connection_url(address: str, *, tls: Optional[dict] = None, token_file: Optional[str] = None,
                   max_shards: Optional[int] = None, shared_memory: bool = False) -> str:
    """A server address with its options in the form the native layer reads."""
    opts = []
    if tls:
        for k in ("ca", "cert", "key"):
            if tls.get(k):
                opts.append(f"{k}={quote(str(tls[k]))}")
        unknown = set(tls) - {"ca", "cert", "key"}
        if unknown:
            raise RejectedError(f"tls takes ca, cert and key, not {', '.join(sorted(unknown))}")
    if token_file:
        opts.append(f"token-file={quote(str(token_file))}")
    if max_shards:
        opts.append(f"max-shards={int(max_shards)}")
    if shared_memory:
        opts.append("shm=1")
    if not opts:
        return address
    return address + ("&" if "?" in address else "?") + "&".join(opts)


class Client:
    """Starts, signals and reads runs through a Fusee server. The same
    interface as ``Engine``::

        client = await fusee.Client.connect("db1:7600", token_file="/etc/fusee/token")
        run = await client.start(checkout, "order-123", order)
    """

    def __init__(self, transport, address: str = ""):
        self._transport = transport
        self._address = address

    def __repr__(self) -> str:
        return f"Client({self._address!r})"

    @classmethod
    async def connect(cls, address: Optional[str] = None, *, tls: Optional[dict] = None,
                      token_file: Optional[str] = None) -> "Client":
        """Connects to a server: ``address`` (default ``FUSEE_URL``) and, as
        options, ``tls={"ca": ..., "cert": ..., "key": ...}`` and
        ``token_file``."""
        address = address or os.environ.get("FUSEE_URL")
        if not address:
            raise ConnectionError("no server address: pass one, or set FUSEE_URL")
        url = connection_url(address, tls=tls, token_file=token_file)
        try:
            transport = await asyncio.to_thread(_native.NativeClient, url)
        except RuntimeError as e:
            raise native_error(e) from None
        return cls(transport, address)

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def call(self, method: str, args: dict) -> Any:
        """Sends one protocol call without blocking the event loop."""
        try:
            response = await asyncio.to_thread(self._transport.call, method, json.dumps(args))
        except RuntimeError as e:
            err = native_error(e)
            raise self._remote_error(err) from None
        return json.loads(response)

    def _remote_error(self, e: FuseeError) -> FuseeError:
        """A worker's error: cancellation and timeouts cross as text."""
        msg = str(e)
        if e.code in ("WorkflowFailed",) and e.cause is not None:
            msg = e.cause.message
            if e.cause.type == "Cancelled" or msg.startswith("cancelled: "):
                return CancelledError(msg[len("cancelled: "):] if msg.startswith("cancelled: ") else msg)
            if e.cause.type == "TimedOut" or msg == "timed out":
                return TimeoutError(msg)
            if e.cause.type == "Terminated" or msg == "terminated":
                return TerminatedError(msg)
        return e

    # -------------------------------------------------------------- the client interface

    def run(self, id: str, workflow: Optional[Workflow[Any, O]] = None) -> Run[O]:
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
        """See ``Engine.start`` (``delay=``/``at=`` start it later)."""
        wf = workflow if isinstance(workflow, Workflow) else None
        if wf is not None:
            wf.check_input(input)
            if timeout is None:
                timeout = wf.timeout
        p = _id_policy(id_policy)
        options: dict = {"timeout": None if timeout is None else int(ms(timeout)),
                         "idPolicy": {"running": p.running, "finished": p.finished}}
        if delay is not None:
            options["delay"] = int(ms(delay))
        if at is not None:
            options["at"] = _codec.at_ms(at)
        await self.call("start", {"type": name_of(workflow), "id": id, "input": input, "version": version,
                                  "options": options})
        return Run(self, id, wf)

    async def signal_with_start(self, workflow: Any, id: str, input: Any, signal: Any, value: Any = None, *,
                                id_policy: Any = None, timeout: Any = None, version: Optional[int] = None) -> Any:
        """See ``Engine.signal_with_start``."""
        wf = workflow if isinstance(workflow, Workflow) else None
        if wf is not None:
            wf.check_input(input)
            if timeout is None:
                timeout = wf.timeout
        p = _id_policy(id_policy, running="use-existing")
        await self.call("signalWithStart", {
            "type": name_of(workflow), "id": id, "input": input, "name": name_of(signal), "value": value,
            "version": version, "options": {"timeout": None if timeout is None else int(ms(timeout)),
                                            "idPolicy": {"running": p.running, "finished": p.finished}}})
        return Run(self, id, wf)

    async def update_with_start(self, workflow: Any, id: str, input: Any, update: Any, arg: Any = None, *,
                                update_id: str = "", id_policy: Any = None, timeout: Any = None,
                                version: Optional[int] = None) -> tuple:
        p = _id_policy(id_policy, running="use-existing")
        result = await self.call("updateWithStart", {
            "type": name_of(workflow), "id": id, "input": input, "name": name_of(update), "arg": arg,
            "updateId": update_id, "version": version,
            "options": {"timeout": None if timeout is None else int(ms(timeout)),
                        "idPolicy": {"running": p.running, "finished": p.finished}}})
        if isinstance(update, Update) and update._decode_result is not None and result is not None:
            result = update._decode_result(result)
        return Run(self, id, workflow if isinstance(workflow, Workflow) else None), result

    def object(self, type: Any, key: str) -> ObjectRef:
        return ObjectRef(self, name_of(type), key)

    @property
    def schedules(self) -> Schedules:
        return Schedules(self)

    async def schedule(self, name: str, spec: Any, workflow: Any, input: Any = None) -> Schedule:
        s = ScheduleSpec.of(spec).wire(name_of(workflow), input)
        handle = Schedule(self, name)
        await self.call("start", {"type": _SCHEDULE_TYPE, "id": handle.id, "input": {"name": name, "spec": s},
                                  "version": 1, "options": {"timeout": None,
                                                            "idPolicy": {"running": "use-existing",
                                                                         "finished": "allow"}}})
        await self._signal(handle.id, _CONTROL, {"op": "update", "spec": s})
        return handle

    async def list(self, *, type: Any = None, status: Optional[str] = None, started_after: Any = None,
                   started_before: Any = None, attributes: Optional[dict] = None,
                   limit: int = 1000) -> AsyncIterator[RunSummary]:
        where = _filter(type, status, started_after, started_before, attributes)
        for r in await self.call("list", {"limit": limit, "where": where}):
            yield RunSummary.from_json(r)

    async def complete(self, token: str, value: Any = None) -> None:
        await self.call("complete", {"token": token, "value": value})

    async def fail(self, token: str, failure: Any) -> None:
        await self.call("fail", {"token": token, "error": _failure_text(failure)})

    async def debounce(self, workflow: Any, key: str, input: Any = None, *, window: Any, max_wait: Any = None,
                       dedup_window: Any = None, event_id: str = "") -> Run[Any]:
        w = ms(window)
        if w is None:
            raise RejectedError("debounce needs a window")
        return await self._enqueue("debounce", name_of(workflow), key, event_id, input, w, ms(max_wait),
                                   ms(dedup_window))

    async def batch(self, workflow: Any, key: str, event: Any = None, *, max_events: Optional[int] = None,
                    max_wait: Any = None, dedup_window: Any = None, event_id: str = "") -> Run[Any]:
        return await self._enqueue("batch", name_of(workflow), key, event_id, event,
                                   None if max_events is None else float(max_events), ms(max_wait), ms(dedup_window))

    async def close(self) -> None:
        try:
            self._transport.close()
        except (RuntimeError, OSError):
            pass

    # -------------------------------------------------------------- the operations handles use

    async def _enqueue(self, kind, workflow, key, event_id, value, a, b, c) -> Run[Any]:
        wtype, cid, state, arg = _queue_request(kind, workflow, key, event_id, value, a, b, c)
        args = {"type": wtype, "id": cid, "input": json.loads(state), "name": _EVENT, "arg": json.loads(arg),
                "updateId": event_id, "version": 1,
                "options": {"timeout": None, "idPolicy": {"running": "use-existing", "finished": "allow"}}}
        return Run(self, await _until_taken(cid, lambda: self.call("updateWithStart", args)))

    async def _signal(self, id: str, name: str, value: Any) -> None:
        await self.call("signal", {"id": id, "name": name, "value": value})

    async def _query(self, id: str, name: str, arg: Any) -> Any:
        return await self.call("query", {"id": id, "name": name, "arg": arg})

    async def _update(self, id: str, name: str, arg: Any, uid: str) -> Any:
        return await self.call("update", {"id": id, "name": name, "arg": arg, "updateId": uid})

    async def _result(self, id: str) -> Any:
        return await self.call("result", {"id": id})

    async def _close_run(self, id: str, reason: str, terminate: bool) -> None:
        await self.call("terminate" if terminate else "cancel", {"id": id, "reason": reason})

    async def _resume(self, id: str) -> None:
        await self.call("resume", {"id": id})

    async def _reset(self, id: str, before_step: str, reapply_signals: bool, version: int) -> None:
        await self.call("reset", {"id": id, "beforeStep": before_step, "reapplySignals": reapply_signals,
                                  "version": version})

    async def _status(self, id: str) -> Optional[RunSummary]:
        rows = await self.call("list", {"limit": 1, "where": {"id": id}})
        return RunSummary.from_json(rows[0]) if rows else None

    async def _describe(self, id: str) -> Optional[RunDetail]:
        d = await self.call("describe", {"id": id})
        if d is None:
            return None
        journal = d.pop("journal", [])
        obj = d.pop("objectDetail", None)
        return RunDetail(RunSummary.from_json(d), journal, obj)

    async def _exists(self, id: str) -> bool:
        return await self._status(id) is not None

    async def _stream_read(self, id: str, run: Optional[int], name: str, start: int, wait_ms: int) -> dict:
        return await self.call("streamRead", {"id": id, "run": run, "name": name, "from": start, "waitMs": wait_ms})

    async def _edit_state(self, type: str, id: str, edit: dict, update_id: str) -> Any:
        return await self.call("editState", {"id": id, "edit": edit, "updateId": update_id})

    async def _object_call(self, type: str, id: str, handler: str, arg: Any, update_id: str) -> Any:
        return await self.call("updateWithStart", {
            "type": type, "id": id, "input": None, "name": handler, "arg": arg, "updateId": update_id, "version": None,
            "options": {"timeout": None, "idPolicy": {"running": "use-existing", "finished": "allow"}}})


_ = (IdPolicy, Failure, _KINDS)
