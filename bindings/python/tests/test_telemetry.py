import asyncio
import contextlib
import contextvars
import shutil
import tempfile

from fusee import Engine, Retry, Telemetry
from _helpers import journal, on, status

_active = contextvars.ContextVar("active", default=None)


TP = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


class FakeSpan:
    def __init__(self, name, attributes, context=None):
        self.name = name
        self.attrs = dict(attributes or {})
        # A context is {"trace": traceparent, "span": parent span}.
        self.parent = context["span"] if context else _active.get()
        self.trace = context["trace"] if context else None
        self.status = "unset"
        self.ended = False

    def set_status(self, s):
        self.status = s

    def end(self):
        self.ended = True


class FakeTracer:
    def __init__(self):
        self.spans = []

    def start_span(self, name, context=None, attributes=None):
        s = FakeSpan(name, attributes, context)
        self.spans.append(s)
        return s


class FakeMeter:
    def __init__(self):
        self.points = []

    def _inst(self, name):
        points = self.points

        class I:
            def add(self, n, attrs=None):
                points.append((name, n, attrs))

            def record(self, n, attrs=None):
                points.append((name, n, attrs))

        return I()

    def create_counter(self, name, **_):
        return self._inst(name)

    def create_histogram(self, name, **_):
        return self._inst(name)


@contextlib.contextmanager
def use_span(span, end_on_exit=False):
    tok = _active.set(span)
    try:
        yield span
    finally:
        _active.reset(tok)


async def spans_and_metrics():
    d = tempfile.mkdtemp(prefix="fusee-py-otel-")
    tracer, meter = FakeTracer(), FakeMeter()
    tel = Telemetry(tracer, meter, use_span, lambda err: ("ok", None) if err is None else ("error", err),
                    traceparent=lambda: TP,
                    remote=lambda tp: {"trace": tp, "span": None},
                    span_in=lambda span, parent=None: {"trace": (parent or {}).get("trace"), "span": span})
    seen = {}
    fails = [1]

    def register(engine):
        @on(engine, "order")
        async def order(ctx, n):
            def price():
                seen["sync"] = _active.get()
                return n * 2

            a = await ctx.step("price", price)

            async def charge():
                await asyncio.sleep(0)
                seen["async"] = _active.get()
                if fails[0] > 0:
                    fails[0] -= 1
                    raise RuntimeError("card declined")
                return a + 1

            b = await ctx.step("charge", charge, retry=Retry(attempts=3, initial="1ms"))
            await ctx.wait_for("go", timeout="30s")
            return b

        return order

    engine = await Engine.open(d, flush="fsync", shards=1, telemetry=tel)
    wf = register(engine)
    await engine.start(wf, "o-1", 20)
    for _ in range(400):
        if sum(s.ended for s in tracer.spans) >= 3:
            break
        await asyncio.sleep(0.005)
    ended = [s for s in tracer.spans if s.ended]
    # The run's code has its workflow span, below the trace its start
    # recorded; the step spans go under it.
    [wf_span] = [s for s in tracer.spans if s.name == "fusee.workflow"]
    assert wf_span.trace == TP and wf_span.attrs["fusee.workflow.id"] == "o-1", vars(wf_span)
    assert all(s.parent is wf_span for s in ended), [(s.name, s.parent) for s in ended]
    assert [r.get("trace") for r in await journal(engine, "o-1") if r["kind"] == "start"] == [TP]
    got = [(s.attrs["fusee.step.name"], s.attrs["fusee.step.attempt"], s.status) for s in ended]
    assert got == [("price", 1, ("ok", None)), ("charge", 1, ("error", "card declined")), ("charge", 2, ("ok", None))], got
    assert ended[0].attrs["fusee.workflow.id"] == "o-1"
    assert ended[0].attrs["fusee.workflow.type"] == "order"
    # The step function ran with its span current, synchronous or not.
    assert seen["sync"] is ended[0], seen
    assert seen["async"] is ended[2], seen
    outcomes = [p[2]["outcome"] for p in meter.points if p[0] == "fusee.steps.executed"]
    assert outcomes == ["ok", "error", "ok"], outcomes
    assert len([p for p in meter.points if p[0] == "fusee.step.duration"]) == 3
    assert [p[2]["fusee.workflow.type"] for p in meter.points if p[0] == "fusee.workflows.started"] == ["order"]
    await engine.close()

    # Reopen: replayed steps make no spans; the resumed code has a workflow
    # span in the same trace, ended with the run.
    before = len(tracer.spans)
    engine = await Engine.open(d, flush="fsync", shards=1, telemetry=tel)
    register(engine)
    await engine.run("o-1").signal("go", True)
    assert await engine.run("o-1").result() == 41
    [resumed] = tracer.spans[before:]
    assert resumed.name == "fusee.workflow" and resumed.trace == TP and resumed.ended, vars(resumed)
    assert resumed.status == ("ok", None)
    assert [p[2]["status"] for p in meter.points if p[0] == "fusee.workflows.finished"] == ["ok"]
    await engine.close()
    shutil.rmtree(d, ignore_errors=True)


def test_spans_and_metrics():
    asyncio.run(spans_and_metrics())


if __name__ == "__main__":
    test_spans_and_metrics()
    print("ok test_spans_and_metrics")
