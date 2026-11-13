"""Errors: one set in every SDK, named by code, and the failure envelope a
failed step, update or workflow records (see docs/errors.md)."""

from __future__ import annotations

import json
import re
import traceback
from dataclasses import dataclass, field
from typing import Any, Optional

#: Where each error code's page lives (an open decision; one place to change).
DOCS_BASE = "https://github.com/bitsinfra/fusee/blob/main/docs/errors.md"
#: Stacks longer than this are cut.
STACK_LIMIT = 4096


def docs_link(code: str) -> str:
    return f"{DOCS_BASE}#{code.lower()}"


@dataclass
class Failure:
    """A failure as recorded: what a failed step, update, object call or
    workflow carries, in every SDK. ``type`` is the error's class name
    (absent for a plain ``Exception``); ``code`` and ``details`` are the
    application's; ``retryable=False`` marks it final."""

    message: str
    type: Optional[str] = None
    code: Optional[str] = None
    details: Any = None
    retryable: Optional[bool] = None
    stack: Optional[str] = None
    cause: Optional["Failure"] = None

    def to_json(self) -> dict:
        out: dict = {"message": self.message}
        for k in ("type", "code", "details", "retryable", "stack"):
            v = getattr(self, k)
            if v is not None:
                out[k] = v
        if self.cause is not None:
            out["cause"] = self.cause.to_json()
        return out

    def encode(self) -> bytes:
        try:
            return json.dumps(self.to_json(), separators=(",", ":"), ensure_ascii=False).encode()
        except (TypeError, ValueError):
            # Details JSON cannot encode: keep the rest.
            self.details = repr(self.details)
            return json.dumps(self.to_json(), separators=(",", ":"), ensure_ascii=False).encode()

    @classmethod
    def from_json(cls, v: Any) -> "Failure":
        if not isinstance(v, dict):
            return cls(str(v))
        cause = v.get("cause")
        return cls(
            message=str(v.get("message", "")),
            type=v.get("type"),
            code=v.get("code"),
            details=v.get("details"),
            retryable=v.get("retryable"),
            stack=v.get("stack"),
            cause=None if cause is None else cls.from_json(cause),
        )

    @classmethod
    def decode(cls, payload: Any) -> "Failure":
        """A recorded failure: an envelope, or plain text as its message."""
        if payload is None:
            return cls("")
        if isinstance(payload, (bytes, bytearray, memoryview)):
            payload = bytes(payload).decode("utf-8", "replace")
        if isinstance(payload, str):
            if payload.startswith("{"):
                try:
                    return cls.from_json(json.loads(payload))
                except ValueError:
                    pass
            return cls(payload)
        return cls.from_json(payload)

    @property
    def root(self) -> "Failure":
        """The innermost cause (this failure if it has none)."""
        f = self
        while f.cause is not None:
            f = f.cause
        return f

    def has_type(self, name: str) -> bool:
        f: Optional[Failure] = self
        while f is not None:
            if f.type == name:
                return True
            f = f.cause
        return False

    def __str__(self) -> str:
        return self.message


class FuseeError(Exception):
    """Every error Fusee raises. ``code`` is the same in every SDK; ``docs``
    links to its page."""

    code: str = "WorkflowFailed"
    #: The failure this error carries (a step's, a child's, a workflow's).
    cause: Optional[Failure] = None
    retryable: bool = False

    def __init__(self, message: str, *, code: Optional[str] = None, cause: Optional[Failure] = None):
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        if cause is not None:
            self.cause = cause

    @property
    def docs(self) -> str:
        return docs_link(self.code)


class NotFoundError(FuseeError):
    code = "NotFound"

    def __init__(self, message: str, *, id: Optional[str] = None):
        super().__init__(message)
        self.id = id


class AlreadyExistsError(FuseeError):
    code = "AlreadyExists"

    def __init__(self, message: str, *, id: Optional[str] = None, run_id: Optional[int] = None):
        super().__init__(message)
        self.id = id
        self.run_id = run_id


class UnknownWorkflowError(FuseeError):
    code = "UnknownWorkflow"

    def __init__(self, message: str, *, type: Optional[str] = None, registered: Optional[list] = None):
        super().__init__(message)
        self.type = type
        self.registered = registered or []


class StepError(FuseeError):
    """A step failed after its last attempt (or with a final failure). The
    step's own failure is ``cause``: ``e.cause.type``, ``e.cause.code``,
    ``e.cause.details``."""

    code = "StepFailed"

    def __init__(self, step: str, cause: Failure, *, workflow_id: Optional[str] = None,
                 attempts: Optional[int] = None):
        super().__init__(f"step '{step}' failed: {cause.message}", cause=cause)
        self.step = step
        self.workflow_id = workflow_id
        self.attempts = attempts
        self.retryable = cause.retryable is not False


class WorkflowError(FuseeError):
    """A workflow, update or object handler failed; its failure is ``cause``."""

    code = "WorkflowFailed"

    def __init__(self, cause: Failure, *, workflow_id: Optional[str] = None):
        where = f"workflow '{workflow_id}' failed: " if workflow_id else ""
        super().__init__(f"{where}{cause.message}", cause=cause)
        self.workflow_id = workflow_id


class ChildError(FuseeError):
    code = "ChildFailed"

    def __init__(self, workflow_id: str, cause: Failure):
        super().__init__(f"child '{workflow_id}' failed: {cause.message}", cause=cause)
        self.workflow_id = workflow_id
        self.retryable = cause.retryable is not False


class CancelledError(FuseeError):
    code = "Cancelled"
    retryable = True

    def __init__(self, reason: str = "cancelled"):
        super().__init__(f"cancelled: {reason}")
        self.reason = reason


class TimeoutError(FuseeError):  # noqa: A001 - the same name in every SDK
    code = "TimedOut"
    retryable = True

    def __init__(self, message: str = "timed out"):
        super().__init__(message)


class TerminatedError(FuseeError):
    code = "Terminated"

    def __init__(self, reason: str = "terminated"):
        super().__init__(reason)
        self.reason = reason


class DeterminismError(FuseeError):
    code = "Determinism"

    def __init__(self, message: str, *, workflow_id: Optional[str] = None):
        super().__init__(message)
        self.workflow_id = workflow_id


class RejectedError(FuseeError):
    """A request was refused and nothing was recorded."""

    code = "Rejected"

    def __init__(self, message: str):
        super().__init__(message)
        self.reason = message


class CodecError(FuseeError):
    code = "Codec"

    def __init__(self, message: str, *, step: Optional[str] = None, path: Optional[str] = None):
        super().__init__(message)
        self.step = step
        self.path = path


class ConnectionError(FuseeError):  # noqa: A001 - the same name in every SDK
    code = "Connection"
    retryable = True

    def __init__(self, message: str, *, address: Optional[str] = None):
        super().__init__(message)
        self.address = address


class IoError(FuseeError):
    code = "Io"
    retryable = True


class BusyError(FuseeError):
    code = "Busy"
    retryable = True


class ClosedError(FuseeError):
    code = "Closed"

    def __init__(self, message: str = "engine closed"):
        super().__init__(message)


class Fatal(Exception):
    """Raise (or subclass) to fail a step without retries::

        class CardDeclined(fusee.Fatal):
            code = "card_declined"

        raise CardDeclined("card declined", details={"last4": "4242"})

    Any exception with ``retryable = False`` is final as well."""

    retryable = False
    code: Optional[str] = None
    details: Any = None

    def __init__(self, message: str = "", *, code: Optional[str] = None, details: Any = None):
        super().__init__(message)
        if code is not None:
            self.code = code
        if details is not None:
            self.details = details


_BY_CODE = {
    c.code: c
    for c in (NotFoundError, AlreadyExistsError, UnknownWorkflowError, StepError, WorkflowError, ChildError,
              CancelledError, TimeoutError, TerminatedError, DeterminismError, RejectedError, CodecError,
              ConnectionError, IoError, BusyError, ClosedError)
}
#: Plain exception types whose name is not recorded as the failure's type.
_PLAIN = {"Exception", "Fatal"}


def failure_of(e: BaseException, stacks: bool = True, depth: int = 0) -> Failure:
    """The envelope of an exception: its type, code, details, retryability,
    stack and cause chain. Runs on failures only."""
    if isinstance(e, StepError) and e.cause is not None:
        return Failure(str(e), "StepFailed", details={"step": e.step}, retryable=e.cause.retryable, cause=e.cause)
    if isinstance(e, ChildError) and e.cause is not None:
        return Failure(str(e), "ChildFailed", details={"workflowId": e.workflow_id}, retryable=e.cause.retryable,
                       cause=e.cause)
    if isinstance(e, WorkflowError) and e.cause is not None:
        return e.cause
    if isinstance(e, FuseeError):
        if e.code == "WorkflowFailed":
            # Fusee's plain failure: the message alone, as in every SDK.
            return Failure(e.message)
        return Failure(e.message, e.code, retryable=None if e.retryable else False)
    name = type(e).__name__
    f = Failure(str(e) or name, None if name in _PLAIN else name)
    code = getattr(e, "code", None)
    if isinstance(code, str):
        f.code = code
    details = getattr(e, "details", None)
    if details is not None:
        f.details = details
    retryable = getattr(e, "retryable", None)
    if isinstance(retryable, bool):
        f.retryable = retryable
    if stacks and e.__traceback__ is not None:
        s = "".join(traceback.format_exception(type(e), e, e.__traceback__, chain=False))
        b = s.encode()
        if len(b) > STACK_LIMIT:
            s = b[:STACK_LIMIT].decode("utf-8", "ignore")
        f.stack = s
    if e.__cause__ is not None and depth < 8:
        f.cause = failure_of(e.__cause__, stacks, depth + 1)
    return f


def error_of(f: Failure, workflow_id: Optional[str] = None) -> FuseeError:
    """The error a caller sees for a recorded failure: a step's as
    :class:`StepError`, a child's as :class:`ChildError`, anything else as
    :class:`WorkflowError`."""
    details = f.details if isinstance(f.details, dict) else {}
    if f.type == "StepFailed" and f.cause is not None and isinstance(details.get("step"), str):
        return StepError(details["step"], f.cause, workflow_id=workflow_id)
    if f.type == "ChildFailed" and f.cause is not None and isinstance(details.get("workflowId"), str):
        return ChildError(details["workflowId"], f.cause)
    return WorkflowError(f, workflow_id=workflow_id)


def native_error(e: BaseException) -> FuseeError:
    """An error from the native module (``Code: message``) as its class."""
    msg = str(e)
    m = re.match(r"^(\w+): (.*)$", msg, re.S)
    code, text = (m.group(1), m.group(2)) if m else ("Io", msg)
    if code in ("StepFailed", "WorkflowFailed", "ChildFailed", "Failed"):
        return error_of(Failure.decode(text))
    cls = _BY_CODE.get(code)
    if cls is None:
        return FuseeError(text, code=code)
    if cls is CancelledError:
        return CancelledError(re.sub(r"^cancelled: ", "", text))
    return cls(text)
