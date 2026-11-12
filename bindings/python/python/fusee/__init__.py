"""Fusee for Python: durable execution at the speed of the disk.

Definitions are plain values, apart from any engine::

    import fusee

    review = fusee.signal("review", bool)

    @fusee.workflow("checkout")
    async def checkout(ctx: fusee.Ctx, order: Order) -> str:
        quote = await ctx.step("quote", lambda: pricing.quote(order))
        charge = await ctx.step("charge", lambda s: payments.charge(quote, key=s.key),
                                retry=fusee.Retry(attempts=5))
        approved = await ctx.wait_for(review, timeout="1d")
        return charge["id"] if approved else await ctx.step("refund", lambda: payments.refund(charge))

    engine = await fusee.Engine.open("./data", workflows=[checkout])
    run = await engine.start(checkout, "order-123", order)
    await run.signal(review, True)
    print(await run.result())

Workflow code runs under Fusee's deterministic scheduler, so it may only
``await`` ctx operations (or ``ctx.all`` / ``ctx.race`` of them and of other
workflow coroutines). Step functions run on the asyncio loop (or a thread,
with ``thread=True``) and may do anything.
"""

from ._client import Client, connection_url
from ._codec import ms as duration_ms
from ._defs import (
    Concurrency,
    IdPolicy,
    ObjectType,
    Query,
    Retry,
    Signal,
    Update,
    Workflow,
    object,
    query,
    shared,
    signal,
    update,
    workflow,
)
from ._engine import (
    Engine,
    as_requester,
    LimitInfo,
    ObjectRef,
    ReplicaMetrics,
    Run,
    RunDetail,
    RunSummary,
    Schedule,
    Schedules,
    ScheduleSpec,
    StreamReader,
)
from ._errors import (
    DOCS_BASE,
    AlreadyExistsError,
    BusyError,
    CancelledError,
    ChildError,
    ClosedError,
    CodecError,
    ConnectionError,
    DeterminismError,
    Failure,
    Fatal,
    FuseeError,
    IoError,
    NotFoundError,
    RejectedError,
    StepError,
    TerminatedError,
    TimeoutError,
    UnknownWorkflowError,
    WorkflowError,
)
from ._invoke import SIGNATURE_HEADER, InvocationHandler, invocation_handler, verify_signature
from ._replay import Replayer, ReplayReport
from ._runtime import Completion, Ctx, ObjectCall, ObjectCtx, ObjectState, StepCtx, StreamWriter
from ._telemetry import Telemetry, open_telemetry


def aes_gcm(keys: dict) -> list:
    """Payload encryption with AES-256-GCM: ``aes_gcm({"k2": new_key, "k1":
    old_key})``. Keys are 32 bytes (or hex / base64 text). The first key
    encrypts; every key decrypts."""
    import base64
    import re

    out = []
    for kid, k in keys.items():
        if isinstance(k, str):
            k = bytes.fromhex(k) if re.fullmatch(r"[0-9a-fA-F]{64}", k) else base64.b64decode(k)
        k = bytes(k)
        if len(k) != 32:
            raise RejectedError(f"key {kid!r} is not 32 bytes")
        out.append((kid, k))
    if not out:
        raise RejectedError("a codec needs at least one key")
    return out


__all__ = [
    "Concurrency",
    "as_requester",
    # definitions
    "workflow",
    "signal",
    "query",
    "update",
    "object",
    "shared",
    "Workflow",
    "Signal",
    "Query",
    "Update",
    "ObjectType",
    "Retry",
    "IdPolicy",
    "ScheduleSpec",
    # engine and client
    "Engine",
    "Client",
    "connection_url",
    "Run",
    "RunSummary",
    "RunDetail",
    "LimitInfo",
    "ObjectRef",
    "Schedule",
    "Schedules",
    "StreamReader",
    "ReplicaMetrics",
    # workflow code
    "Ctx",
    "StepCtx",
    "ObjectCtx",
    "ObjectState",
    "ObjectCall",
    "StreamWriter",
    "Completion",
    # errors
    "Failure",
    "Fatal",
    "FuseeError",
    "NotFoundError",
    "AlreadyExistsError",
    "UnknownWorkflowError",
    "StepError",
    "WorkflowError",
    "ChildError",
    "CancelledError",
    "TimeoutError",
    "TerminatedError",
    "DeterminismError",
    "RejectedError",
    "CodecError",
    "ConnectionError",
    "IoError",
    "BusyError",
    "ClosedError",
    "DOCS_BASE",
    # tools
    "Replayer",
    "ReplayReport",
    "Telemetry",
    "open_telemetry",
    "aes_gcm",
    "duration_ms",
    "InvocationHandler",
    "invocation_handler",
    "verify_signature",
    "SIGNATURE_HEADER",
]
