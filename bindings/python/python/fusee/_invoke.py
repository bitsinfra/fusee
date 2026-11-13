"""Invoked workers: the worker side of invocations over HTTP (suspend to zero)."""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import re
import time
from typing import Any, Callable, Optional

from ._engine import Engine
from ._errors import FuseeError, RejectedError
from ._runtime import now_ms as _now_ms

SIGNATURE_HEADER = "fusee-signature"


def verify_signature(secret, header: str, body: bytes, now_ms: Optional[int] = None) -> Optional[str]:
    """Checks a ``fusee-signature`` value (``t=<unix ms>,v1=<hex HMAC-SHA256>``)
    within five minutes. Returns why it fails, or ``None``."""
    import hashlib
    import hmac

    key = secret.encode() if isinstance(secret, str) else bytes(secret)
    t = None
    sigs = []
    for part in header.split(","):
        k, _, v = part.strip().partition("=")
        if k == "t" and v.isdigit():
            t = int(v)
        elif k == "v1" and v:
            sigs.append(v)
    if t is None:
        return "the signature has no timestamp"
    now = _now_ms() if now_ms is None else now_ms
    if abs(now - t) > 300_000:
        return "the signature is too old (or the clocks differ by over five minutes)"
    mac = hmac.new(key, f"{t}.".encode() + body, hashlib.sha256).hexdigest()
    if any(hmac.compare_digest(mac, s) for s in sigs):
        return None
    return "the signature does not match"


class InvocationHandler:
    """The worker side of invocations over HTTP (suspend to zero).

    When a Fusee server started with ``--invoke URL`` has work for shards no
    worker holds, it POSTs to that URL; each request opens a fresh engine
    for the invocation's shards, runs them until they are idle or the
    budget ends, and closes it. The handler is an ASGI app (``uvicorn
    module:app``); ``lambda_handler`` is an AWS Lambda handler; ``invoke``
    serves any other framework. An engine belongs to the thread that opened
    it: run invocations on one thread (Lambda and ASGI servers do)::

        app = invocation_handler("fusee.internal:7600?token-file=/run/token",
                                 workflows=[order],
                                 secret=os.environ["FUSEE_INVOKE_SECRET"])

    ``register(engine)``, if given, runs after the engine opens (for setup
    beyond the definitions).
    """

    def __init__(self, address: str, *, workflows=(), objects=(), register: Optional[Callable[["Engine"], Any]] = None,
                 secret=None, **engine_options):
        if secret is not None and len(secret.encode() if isinstance(secret, str) else secret) < 16:
            raise RejectedError("an invocation secret needs at least 16 bytes; use 32 random bytes")
        self.address = address
        self.secret = secret
        self.workflows = list(workflows)
        self.objects = list(objects)
        self.register = register
        self.engine_options = engine_options

    async def invoke(self, headers: Optional[dict], body: bytes) -> tuple:
        """Verifies and runs one invocation: ``(status, headers, body)``."""

        def reply(status: int, v: Any) -> tuple:
            return status, {"content-type": "application/json"}, json.dumps(v).encode()

        if self.secret is not None:
            sig = None
            for k, v in (headers or {}).items():
                if k.lower() == SIGNATURE_HEADER:
                    sig = v
            if not sig:
                return reply(401, {"error": "unsigned request"})
            problem = verify_signature(self.secret, sig, body)
            if problem:
                return reply(401, {"error": problem})
        try:
            request = json.loads(body)
        except ValueError as e:
            return reply(400, {"error": str(e)})
        inv = request.get("invocation") if isinstance(request, dict) else None
        if not isinstance(inv, str) or not re.fullmatch(r"[0-9a-f]{16}", inv) or set(inv) == {"0"}:
            return reply(400, {"error": "invalid invocation ID"})
        began = time.monotonic()
        sep = "&" if "?" in self.address else "?"
        try:
            # Runs park soon after they suspend, so the next invocation's open stays cheap.
            options = {"park_after": 50, **self.engine_options}
            engine = await Engine.open(durability="server", address=f"{self.address}{sep}invocation={inv}",
                                       workflows=self.workflows, objects=self.objects, **options)
        except Exception as e:  # noqa: BLE001 - answered as a status
            if "refused this worker" in str(e):
                return reply(200, {"invocation": inv, "skipped": str(e)})
            return reply(503, {"error": str(e)})
        open_ms = (time.monotonic() - began) * 1e3
        outcome = "running"
        error = None
        try:
            if self.register is not None:
                r = self.register(engine)
                if inspect.isawaitable(r):
                    await r
            while True:
                awaiting = [t for t, runs in engine._awaiting.items() if runs]
                outcome = engine._native.invocation_state(awaiting) or "idle"
                if outcome != "running":
                    break
                await asyncio.sleep(0.005)
        except Exception as e:  # noqa: BLE001 - reported in the summary
            error = str(e)
        try:
            steps = sum(m.get("steps_run", 0) for m in engine.shard_metrics())
        except FuseeError:
            steps = 0  # the engine closed itself: its connection is gone
        try:
            await engine.close()
        except Exception as e:  # noqa: BLE001
            error = error or str(e)
        # The native engine belongs to this thread: release it here.
        for r in engine._runs.values():
            r.n = None
        engine._runs.clear()
        engine._be.n = None
        engine._native = None
        return reply(200, {"invocation": inv, "outcome": outcome, "steps": steps, "openMs": open_ms,
                           "ms": (time.monotonic() - began) * 1e3, "error": error})

    async def __call__(self, scope, receive, send):
        """The ASGI app."""
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        if scope["type"] != "http":
            return
        body = b""
        while True:
            message = await receive()
            body += message.get("body", b"")
            if not message.get("more_body"):
                break
        if scope.get("method") != "POST":
            status, headers, out = 405, {"content-type": "application/json"}, b'{"error":"POST only"}'
        else:
            hdrs = {k.decode("latin-1"): v.decode("latin-1") for k, v in scope.get("headers", [])}
            status, headers, out = await self.invoke(hdrs, body)
        await send({"type": "http.response.start", "status": status,
                    "headers": [(k.encode(), v.encode()) for k, v in headers.items()]})
        await send({"type": "http.response.body", "body": out})

    def lambda_handler(self, event: dict, context=None) -> dict:
        """An AWS Lambda handler (function URL, API Gateway or ALB proxy events)."""
        raw = event.get("body") or ""
        body = base64.b64decode(raw) if event.get("isBase64Encoded") else raw.encode()
        status, headers, out = asyncio.run(self.invoke(event.get("headers") or {}, body))
        return {"statusCode": status, "headers": headers, "body": out.decode()}


def invocation_handler(address: str, *, workflows=(), objects=(),
                       register: Optional[Callable[["Engine"], Any]] = None, secret=None,
                       **engine_options) -> InvocationHandler:
    """See :class:`InvocationHandler`."""
    return InvocationHandler(address, workflows=workflows, objects=objects, register=register, secret=secret,
                             **engine_options)
