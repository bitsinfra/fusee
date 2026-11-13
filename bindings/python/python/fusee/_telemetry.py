"""OpenTelemetry instruments: a ``fusee.step`` span per step attempt and
step and workflow counters."""

from __future__ import annotations

import time
from typing import Any, Optional

from ._runtime import _NO_SPAN

_STATUS_NAMES = {0: "ok", 1: "failed", 2: "cancelled", 3: "timed_out", 4: "terminated"}


class Telemetry:
    """OpenTelemetry instruments for an engine: a ``fusee.step`` span per
    step attempt, current while the step's function runs, and the metrics
    ``fusee.steps.executed``, ``fusee.step.duration`` (ms),
    ``fusee.workflows.started`` and ``fusee.workflows.finished``. Replayed
    steps record nothing. Build one with :func:`open_telemetry`, or pass any
    ``tracer`` and ``meter`` with the OpenTelemetry API's methods."""

    def __init__(self, tracer=None, meter=None, use_span=None, status=None,
                 traceparent=None, remote=None, span_in=None):
        self._tracer = tracer
        self._use_span = use_span
        self._status = status
        # Trace context: the current span's traceparent (a start made here
        # records it), a context for a recorded one, and a context with a
        # span active in it.
        self._traceparent = traceparent
        self._remote = remote
        self._span_in = span_in
        self._steps = meter.create_counter("fusee.steps.executed", description="Step attempts run (not replayed)") if meter else None
        self._step_time = meter.create_histogram("fusee.step.duration", unit="ms", description="Step attempt duration") if meter else None
        self._started = meter.create_counter("fusee.workflows.started", description="Workflow starts accepted") if meter else None
        self._finished = meter.create_counter("fusee.workflows.finished", description="Workflow runs finished by this process") if meter else None

    def _attempt(self, x: Any, name: str, attempt: int) -> "_Attempt":
        span = None
        if self._tracer is not None:
            span = self._tracer.start_span("fusee.step", context=getattr(x, "wf_ctx", None), attributes={
                "fusee.workflow.id": x.id,
                "fusee.workflow.type": x.wtype,
                "fusee.step.name": name,
                "fusee.step.attempt": attempt,
            })
        return _Attempt(self, x.wtype, span)

    def _current_traceparent(self) -> Optional[str]:
        return self._traceparent() if self._traceparent is not None else None

    def _workflow(self, x: Any, trace: Optional[str]) -> None:
        """A ``fusee.workflow`` span for a run's code in this process, below
        the trace its start recorded; its steps' spans go under it."""
        if self._tracer is None:
            return
        parent = self._remote(trace) if trace and self._remote is not None else None
        span = self._tracer.start_span("fusee.workflow", context=parent, attributes={
            "fusee.workflow.id": x.id, "fusee.workflow.type": x.wtype, "fusee.run": x.run})
        x.wf_span = span
        x.wf_ctx = self._span_in(span, parent) if self._span_in is not None else None

    def _end_workflow(self, x: Any, status: Optional[int]) -> None:
        span = getattr(x, "wf_span", None)
        if span is None:
            return
        x.wf_span = None
        if self._status is not None:
            ok = status is None or status == 0
            span.set_status(self._status(None if ok else _STATUS_NAMES.get(status, str(status))))
        span.end()

    def _start(self, wtype: str) -> None:
        if self._started is not None:
            self._started.add(1, {"fusee.workflow.type": wtype})

    def _finish(self, wtype: str, status: int) -> None:
        if self._finished is not None:
            self._finished.add(1, {"fusee.workflow.type": wtype, "status": _STATUS_NAMES.get(status, str(status))})


class _Attempt:
    __slots__ = ("t", "wtype", "span", "began", "done")

    def __init__(self, t: Telemetry, wtype: str, span):
        self.t = t
        self.wtype = wtype
        self.span = span
        self.began = time.perf_counter()
        self.done = False

    def activate(self):
        """A context manager making the span current (or doing nothing)."""
        if self.span is not None and self.t._use_span is not None:
            return self.t._use_span(self.span, end_on_exit=False)
        return _NO_SPAN

    def end(self, error: Optional[str] = None) -> None:
        if self.done:
            return
        self.done = True
        labels = {"fusee.workflow.type": self.wtype, "outcome": "ok" if error is None else "error"}
        if self.t._steps is not None:
            self.t._steps.add(1, labels)
        if self.t._step_time is not None:
            self.t._step_time.record((time.perf_counter() - self.began) * 1000, labels)
        if self.span is not None:
            if self.t._status is not None:
                self.span.set_status(self.t._status(error))
            self.span.end()


def open_telemetry() -> Telemetry:
    """Instruments from the application's ``opentelemetry`` package (the
    API; the application configures providers and exporters)."""
    from opentelemetry import metrics, trace
    from opentelemetry.trace import NonRecordingSpan, SpanContext, Status, StatusCode, TraceFlags

    def status(error: Optional[str]):
        return Status(StatusCode.OK) if error is None else Status(StatusCode.ERROR, error)

    def traceparent() -> Optional[str]:
        sc = trace.get_current_span().get_span_context()
        if not sc.is_valid:
            return None
        return f"00-{sc.trace_id:032x}-{sc.span_id:016x}-{int(sc.trace_flags):02x}"

    def remote(tp: str):
        try:
            _, t, s, f = tp.split("-")
            sc = SpanContext(int(t, 16), int(s, 16), is_remote=True, trace_flags=TraceFlags(int(f, 16)))
        except ValueError:
            return None
        return trace.set_span_in_context(NonRecordingSpan(sc))

    def span_in(span, parent=None):
        return trace.set_span_in_context(span, parent)

    return Telemetry(trace.get_tracer("fusee"), metrics.get_meter("fusee"), trace.use_span, status,
                     traceparent, remote, span_in)


