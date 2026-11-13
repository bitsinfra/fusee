"""Replay testing: workflow code run against exported journals."""

from __future__ import annotations

import asyncio
import os
import threading
from dataclasses import dataclass
from typing import Iterable, Optional

from . import _native
from ._codec import tagged_to_wire
from ._defs import Workflow
from ._errors import native_error
from ._runtime import Backend, CodeHost, as_workflow, key


@dataclass
class ReplayReport:
    workflow_id: str
    workflow_type: str
    ok: bool = False
    error: Optional[str] = None
    finished: bool = False
    file: Optional[str] = None


class Replayer(CodeHost):
    """Runs workflow code against journals exported with ``fusee export``
    (or ``engine.export_journal``), without running step functions or
    timers, and reports the first difference by step name::

        report = await fusee.Replayer(workflows=[checkout]).replay_file("order-1.fjnl")
        assert report.ok, report.error
    """

    def __init__(self, workflows: Iterable[Workflow] = (), *, codec: Optional[list] = None):
        super().__init__(None)
        self._codec = codec
        self._current: Optional[Backend] = None
        for wf in workflows:
            self._register(as_workflow(wf))

    def _backend(self) -> Backend:
        return self._current  # type: ignore[return-value]

    async def replay(self, file: bytes) -> ReplayReport:
        self._loop = asyncio.get_running_loop()
        self._thread = threading.get_ident()
        try:
            r = _native.NativeReplay(self._codec)
            head_json = r.import_journal(file)
        except RuntimeError as e:
            raise native_error(e) from None
        import json

        self._current = Backend(r, lambda: None)
        self._runs.clear()
        self._awaiting.clear()
        head = json.loads(head_json)
        rep = ReplayReport(head["workflow_id"], head["workflow_type"])
        try:
            if not head["complete"]:
                rep.error = "the export holds only the run's start and result (export from an engine with keep_history)"
                return rep
            if head["version"] not in self._types.get(head["workflow_type"], {}):
                rep.error = f"no code registered for workflow type '{head['workflow_type']}' version {head['version']}"
                return rep
            quiet = 0
            while quiet < 20:
                events = r.tick()
                for ev in events:
                    k = ev["kind"]
                    if k == "start":
                        self._on_start({**ev, "input": tagged_to_wire(ev["input"])})
                    elif k == "wake":
                        x = self._runs.get(key(ev["shard"], ev["run"]))
                        if x is not None:
                            x.wake()
                    elif k in ("stuck", "ended"):
                        self._on_ended(ev["shard"], ev["run"])
                quiet = 0 if events else quiet + 1
                await asyncio.sleep(0)
            stuck, replaying, live, finished = r.state(head["run"])
            rep.finished = finished
            if stuck:
                rep.error = stuck
            elif replaying and not live:
                rep.error = "the code stopped before reaching the end of its journal"
            else:
                rep.ok = True
            return rep
        finally:
            for x in self._runs.values():
                x.end()
            r.close()

    async def replay_file(self, path: str) -> ReplayReport:
        with open(path, "rb") as f:
            rep = await self.replay(f.read())
        rep.file = path
        return rep

    async def replay_dir(self, dir: str) -> list:
        """Replays every ``.fjnl`` file in a directory."""
        out = []
        for name in sorted(os.listdir(dir)):
            if name.endswith(".fjnl"):
                out.append(await self.replay_file(os.path.join(dir, name)))
        return out
