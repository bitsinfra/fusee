"""Developer-experience checks for Python, and the definitions-first
interface: each mistake fails where it is wrong, with a message that names
the thing and the fix."""

import asyncio
import dataclasses
import datetime
import io
import logging
import os
import shutil
import tempfile
from typing import Optional

import fusee
from fusee import Ctx


def tmp():
    return tempfile.mkdtemp(prefix="fusee-dx-")


@dataclasses.dataclass
class Order:
    sku: str
    cents: int
    placed: datetime.datetime


@dataclasses.dataclass
class Receipt:
    order: Order
    charge: str


review = fusee.signal("review", bool)
status = fusee.query("status")


class CardDeclined(fusee.Fatal):
    code = "card_declined"


attempts = {"n": 0}


@fusee.workflow("checkout")
async def checkout(ctx: Ctx, order: Order) -> Receipt:
    assert isinstance(order, Order), type(order)
    assert isinstance(order.placed, datetime.datetime)
    keys = []

    def charge(s: fusee.StepCtx):
        keys.append(s.key)
        return f"ch_{order.cents}_{s.attempt}"

    ch = await ctx.step("charge", charge)
    ctx.on_query(status, lambda _: {"phase": "review", "keys": len(keys)})
    approved = await ctx.wait_for(review, timeout="5s")
    assert approved is True
    return Receipt(order, ch)


@fusee.workflow("declined")
async def declined(ctx: Ctx, _) -> None:
    def pay():
        attempts["n"] += 1
        raise CardDeclined("card declined", details={"last4": "4242"})

    await ctx.step("pay", pay)


@fusee.workflow("flaky_policy")
async def flaky_policy(ctx: Ctx, _) -> None:
    def call():
        attempts["n"] += 1
        raise KeyError("missing")

    await ctx.step("call", call, retry=fusee.Retry(attempts=5, initial=1, non_retryable=(KeyError,)))


@fusee.workflow("unencodable")
async def unencodable(ctx: Ctx, _) -> None:
    def load():
        attempts["n"] += 1
        return {"items": [1, {"bad": {1, 2}}]}

    await ctx.step("load", load)


@fusee.workflow("fails")
async def fails(ctx: Ctx, _) -> None:
    await ctx.step("one", lambda: 1)
    await ctx.step("two", lambda: 1 / 0, retry=fusee.Retry(attempts=1))


@fusee.workflow("blocking")
async def blocking(ctx: Ctx, _) -> int:
    import time

    def slow():
        time.sleep(0.08)
        return 1

    a = await ctx.step("slow", slow)
    b = await ctx.step("threaded", slow, thread=True)
    return a + b


async def definitions_and_typed_values():
    engine = await fusee.Engine.open(durability="memory", workflows=[checkout])
    order = Order("sku-1", 4200, datetime.datetime(2026, 11, 10, 12, 0, tzinfo=datetime.timezone.utc))
    run = await engine.start(checkout, "order-1", order)
    for _ in range(200):
        try:
            s = await run.query(status)
            break
        except fusee.RejectedError:
            await asyncio.sleep(0.005)
    assert s == {"phase": "review", "keys": 1}, s
    await run.signal(review, True)
    r = await run.result()
    # The result comes back as its annotated type.
    assert isinstance(r, Receipt) and isinstance(r.order, Order), r
    assert r.order == order and r.charge == "ch_4200_1", r
    summary = await run.status()
    assert summary.status == "ok" and summary.type == "checkout", summary
    rows = [x async for x in engine.list(type=checkout)]
    assert [x.id for x in rows] == ["order-1"], rows
    await engine.close()


async def probes():
    engine = await fusee.Engine.open(durability="memory",
                                     workflows=[declined, flaky_policy, unencodable, fails, checkout])

    # Finding 1: an unregistered type fails at once, with the nearest name.
    try:
        await engine.start("chekout", "x-1", None)
        raise AssertionError("expected UnknownWorkflowError")
    except fusee.UnknownWorkflowError as e:
        assert "did you mean 'checkout'?" in str(e), e
        assert e.code == "UnknownWorkflow" and e.docs.endswith("#unknownworkflow")

    # Findings 4 and 5: the caller sees the step, type, code and details,
    # and a Fatal error is not retried.
    attempts["n"] = 0
    try:
        await (await engine.start(declined, "d-1")).result()
        raise AssertionError("expected StepError")
    except fusee.StepError as e:
        assert e.step == "pay"
        assert e.cause.type == "CardDeclined" and e.cause.code == "card_declined", e.cause
        assert e.cause.details == {"last4": "4242"}
        assert e.cause.retryable is False
        assert "CardDeclined" in (e.cause.stack or ""), e.cause.stack
    assert attempts["n"] == 1, attempts

    # A policy's non_retryable list.
    attempts["n"] = 0
    try:
        await (await engine.start(flaky_policy, "f-1")).result()
        raise AssertionError("expected StepError")
    except fusee.StepError as e:
        assert e.cause.type == "KeyError", e.cause
    assert attempts["n"] == 1, attempts

    # Finding 6: a result JSON cannot encode is a codec failure naming the
    # path, never retried.
    attempts["n"] = 0
    try:
        await (await engine.start(unencodable, "u-1")).result()
        raise AssertionError("expected StepError")
    except fusee.StepError as e:
        assert e.cause.type == "Codec", e.cause
        assert "set at $.items[1].bad" in e.cause.message, e.cause.message
    assert attempts["n"] == 1, attempts

    # Finding 10: a failed run keeps its steps.
    run = await engine.start(fails, "fl-1")
    try:
        await run.result()
    except fusee.StepError as e:
        assert e.cause.type == "ZeroDivisionError"
    d = await run.describe()
    assert d.steps == ["one", "two"] and d.failed_step == "two", d.journal

    # Finding 15: a duplicate start names the run and the policy.
    await engine.start(checkout, "o-dup", Order("s", 1, datetime.datetime.now(datetime.timezone.utc)))
    try:
        await engine.start(checkout, "o-dup", Order("s", 1, datetime.datetime.now(datetime.timezone.utc)))
        raise AssertionError("expected AlreadyExistsError")
    except fusee.AlreadyExistsError as e:
        assert "is already running (run " in str(e) and "use-existing" in str(e), e

    # Registering a type twice names both places.
    @fusee.workflow("checkout")
    async def checkout2(ctx, _):
        return None

    try:
        engine.register(checkout2)
        raise AssertionError("expected a refusal")
    except fusee.RejectedError as e:
        assert "registered twice" in str(e) and "test_dx.py" in str(e), e

    # A bad cron names the schedule.
    try:
        await engine.schedule("nightly", "not cron", checkout)
        raise AssertionError("expected RejectedError")
    except fusee.RejectedError as e:
        assert str(e) == "schedule 'nightly': a cron expression has five fields, 'not cron' has 2", str(e)
    await engine.close()


async def key_outside_a_step():
    # Finding 3: there is no ctx.key(); the step context has the key.
    assert not hasattr(fusee.Ctx, "key")

    @fusee.workflow("keys")
    async def keys(ctx, _):
        a = await ctx.step("a", lambda s: s.key)
        b = await ctx.step("b", lambda s: s.key)
        return [a, b]

    engine = await fusee.Engine.open(durability="memory", workflows=[keys])
    a, b = await (await engine.start(keys, "k-1")).result()
    assert a != b and a.startswith("k"), (a, b)
    await engine.close()


async def blocking_steps():
    # Finding 11: under the dev profile a step that holds the loop warns
    # once, naming both fixes; thread=True runs it off the loop.
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    logging.getLogger("fusee").addHandler(handler)
    try:
        engine = await fusee.Engine.open(durability="memory", profile="dev", workflows=[blocking])
        assert await (await engine.start(blocking, "b-1")).result() == 2
        await engine.close()
    finally:
        logging.getLogger("fusee").removeHandler(handler)
    out = buf.getvalue()
    assert "step 'slow'" in out and "thread=True" in out, out
    assert "'threaded'" not in out, out


flips = {"n": 0}


@fusee.workflow("nondeterministic")
async def nondeterministic(ctx: Ctx, _) -> str:
    # Wrong on purpose: a choice outside a step that differs on replay.
    flips["n"] += 1
    name = "first" if flips["n"] % 2 else "second"
    await ctx.step(name, lambda: 1)
    return name


async def strict_replay():
    """Under the dev profile a finished run is replayed against its code,
    and a difference is logged at once with the step name."""
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    logging.getLogger("fusee").addHandler(handler)
    os.environ["FUSEE_HTTP"] = "off"
    try:
        engine = await fusee.Engine.open(durability="memory", profile="dev", workflows=[nondeterministic, checkout])
        assert await (await engine.start(nondeterministic, "nd-1")).result() == "first"
        for _ in range(200):
            if "strict replay" in buf.getvalue():
                break
            await asyncio.sleep(0.01)
        await engine.close()
    finally:
        del os.environ["FUSEE_HTTP"]
        logging.getLogger("fusee").removeHandler(handler)
    out = buf.getvalue()
    assert "strict replay: run 'nd-1' (nondeterministic)" in out, out
    assert "'first'" in out and "'second'" in out, out
    assert "#determinism" in out, out


go = fusee.signal("go", bool)


@fusee.workflow("logged")
async def logged(ctx: Ctx, _) -> None:
    ctx.log.info("started")

    def charge(s: fusee.StepCtx):
        s.log.info("charging")
        return 1

    await ctx.step("charge", charge)
    await ctx.wait_for(go)
    ctx.log.info("done")


async def logs_once():
    """ctx.log is tagged with the run and silent while the run replays."""
    records = []

    class Keep(logging.Handler):
        def emit(self, r):
            if getattr(r, "workflow_id", None) == "lg-1":
                records.append((r.getMessage(), getattr(r, "step", None)))

    handler = Keep()
    logger = logging.getLogger("fusee")
    logger.addHandler(handler)
    level = logger.level
    logger.setLevel(logging.INFO)
    d = tmp()
    try:
        engine = await fusee.Engine.open(d, workflows=[logged])
        await engine.start(logged, "lg-1")
        for _ in range(200):
            if len(records) == 2:
                break
            await asyncio.sleep(0.01)
        await engine.close()
        # Reopened: the signal replays the code up to the wait.
        engine = await fusee.Engine.open(d, workflows=[logged])
        await engine.run("lg-1").signal(go, True)
        await engine.run("lg-1").result()
        await engine.close()
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)
        shutil.rmtree(d, ignore_errors=True)
    assert records == [("started", None), ("charging", "charge"), ("done", None)], records


add = fusee.signal("add", str)


@fusee.workflow("cart")
async def cart(ctx: Ctx, items: list) -> list:
    # The first signal is there however soon the code runs.
    while len(items) < 2:
        items.append(await ctx.wait_for(add))
    return items


async def signal_with_start():
    engine = await fusee.Engine.open(durability="memory", workflows=[cart])
    run = await engine.signal_with_start(cart, "cart-1", [], add, "apple")
    again = await engine.signal_with_start(cart, "cart-1", [], add, "pear")
    assert run.id == again.id == "cart-1"
    assert await run.result() == ["apple", "pear"]
    await engine.close()


undone = []


@fusee.workflow("trip")
async def trip(ctx: Ctx, fail: bool) -> None:
    for what in ("flight", "hotel"):
        await ctx.step(f"book {what}", lambda: None)
        ctx.compensate(f"cancel {what}", lambda what=what: undone.append(what))
    if fail:
        def pay():
            raise fusee.Fatal("card declined")
        await ctx.step("pay", pay)
    else:
        await ctx.wait_for("never")


async def compensations():
    engine = await fusee.Engine.open(durability="memory", workflows=[trip])
    try:
        await (await engine.start(trip, "t-1", True)).result()
        raise AssertionError("expected StepError")
    except fusee.StepError as e:
        assert e.step == "pay", e
    assert undone == ["hotel", "flight"], undone
    d = await engine.run("t-1").describe()
    assert d.steps == ["book flight", "book hotel", "pay", "cancel hotel", "cancel flight"], d.steps
    undone.clear()
    run = await engine.start(trip, "t-2", False)
    for _ in range(200):
        if (await run.status()).status in ("suspended", "parked"):
            break
        await asyncio.sleep(0.005)
    await run.cancel("changed plans")
    try:
        await run.result()
        raise AssertionError("expected CancelledError")
    except fusee.CancelledError:
        pass
    assert undone == ["hotel", "flight"], undone
    await engine.close()


stops = []


@fusee.workflow("long_steps")
async def long_steps(ctx: Ctx, threaded: bool) -> None:
    import time

    async def on_loop():
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            stops.append("coroutine cancelled")
            raise

    def on_thread(s: fusee.StepCtx):
        for _ in range(500):
            if s.cancelled:
                stops.append("thread stopped")
                return
            time.sleep(0.01)

    if threaded:
        await ctx.step("work", on_thread, thread=True)
    else:
        await ctx.step("work", on_loop)


async def cancellation_reaches_steps():
    engine = await fusee.Engine.open(durability="memory", workflows=[long_steps])
    for i, threaded in enumerate((False, True)):
        run = await engine.start(long_steps, f"ls-{i}", threaded)
        await asyncio.sleep(0.05)
        await run.cancel("stop")
        try:
            await run.result()
            raise AssertionError("expected CancelledError")
        except fusee.CancelledError:
            pass
    for _ in range(100):
        if len(stops) == 2:
            break
        await asyncio.sleep(0.01)
    assert stops == ["coroutine cancelled", "thread stopped"], stops
    await engine.close()


@fusee.workflow("remind")
async def remind(ctx: Ctx, who: str) -> list:
    return [f"hi {who}", ctx.now()]


async def delayed_start():
    import time
    engine = await fusee.Engine.open(durability="memory", workflows=[remind])
    began = time.time()
    run = await engine.start(remind, "rm-1", "ana", delay="300ms")
    assert run.id == "rm-1"
    s = await engine.run("fusee.delay/rm-1").status()
    assert s.finished_ms is None and s.type == "fusee.delay", s
    msg, at = await run.result()
    # The delay is counted in whole milliseconds from the start's request.
    assert msg == "hi ana" and time.time() - began >= 0.29, (msg, time.time() - began)
    try:
        await engine.start("remnd", "rm-2", "x", delay="1m")
        raise AssertionError("expected UnknownWorkflowError")
    except fusee.UnknownWorkflowError:
        pass
    await engine.close()


async def bad_directory_and_connection():
    try:
        await fusee.Engine.open("/dev/null/fusee")
        raise AssertionError("expected IoError")
    except fusee.FuseeError as e:
        assert "/dev/null/fusee" in str(e), e
    try:
        await fusee.Client.connect("127.0.0.1:1")
        raise AssertionError("expected ConnectionError")
    except fusee.ConnectionError as e:
        assert "no fusee server at 127.0.0.1:1" in str(e) and "fusee server --dir" in str(e), e
    d = tmp()
    e1 = await fusee.Engine.open(d)
    try:
        await fusee.Engine.open(d)
        raise AssertionError("expected IoError")
    except fusee.IoError as e:
        assert f"is open in another process (pid {os.getpid()})" in str(e), e
    await e1.close()
    shutil.rmtree(d, ignore_errors=True)


async def default_directory():
    d = tmp()
    os.environ["FUSEE_DIR"] = d
    try:
        engine = await fusee.Engine.open()
        assert os.path.exists(os.path.join(d, "fusee.meta")), os.listdir(d)
        await engine.close()
    finally:
        del os.environ["FUSEE_DIR"]
        shutil.rmtree(d, ignore_errors=True)


async def http_api():
    """engine.serve(): the HTTP API answers through the engine's loop."""
    import json as _json
    import urllib.request

    engine = await fusee.Engine.open(durability="memory", workflows=[checkout, declined])
    url = engine.serve("127.0.0.1:0")

    def call(method, path, body=None):
        data = None if body is None else _json.dumps(body).encode()
        req = urllib.request.Request(url + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, _json.loads(r.read() or b"null")
        except urllib.error.HTTPError as e:
            return e.code, _json.loads(e.read() or b"null")

    order = {"sku": "s", "cents": 7, "placed": "2026-11-10T12:00:00+00:00"}
    assert await asyncio.to_thread(call, "POST", "/v1/runs", {"type": "checkout", "id": "h-1", "input": order}) == \
        (200, {"id": "h-1"})
    status, v = await asyncio.to_thread(call, "POST", "/v1/runs", {"type": "nope", "id": "h-2"})
    assert status == 400 and v["error"]["code"] == "UnknownWorkflow", v
    for _ in range(200):
        status, v = await asyncio.to_thread(call, "POST", "/v1/runs/h-1/queries/status", None)
        if status == 200:
            break
        await asyncio.sleep(0.01)
    assert v == {"phase": "review", "keys": 1}, (status, v)
    assert (await asyncio.to_thread(call, "POST", "/v1/runs/h-1/signals/review", True))[0] == 200
    status, v = await asyncio.to_thread(call, "GET", "/v1/runs/h-1/result")
    assert status == 200 and v["status"] == "ok" and v["value"]["charge"] == "ch_7_1", v
    await asyncio.to_thread(call, "POST", "/v1/runs", {"type": "declined", "id": "h-3"})
    status, v = await asyncio.to_thread(call, "GET", "/v1/runs/h-3/result")
    assert v["status"] == "failed" and v["error"]["cause"]["type"] == "CardDeclined", v
    status, types = await asyncio.to_thread(call, "GET", "/v1/types")
    assert [t["name"] for t in types] == ["checkout", "declined"], types
    status, info = await asyncio.to_thread(call, "GET", "/v1/info")
    assert info["sdk"] == "python", info
    status, rows = await asyncio.to_thread(call, "GET", "/v1/runs")
    assert {r["id"] for r in rows} == {"h-1", "h-3"}, rows
    status, d = await asyncio.to_thread(call, "GET", "/v1/runs/h-3")
    assert d["journal"][-1]["kind"] == "done", d
    engine.limit("stripe", concurrent=2, per_second=10)
    assert [x.name for x in engine.limits()] == ["stripe"]
    status, limits = await asyncio.to_thread(call, "GET", "/v1/limits")
    assert limits == [{"name": "stripe", "concurrent": 2, "perSecond": 10.0, "inUse": 0, "waiting": 0}], limits
    await engine.close()


async def audit():
    """Signals, cancellations and resets record who asked: `as_requester` in
    code, the HTTP API's own name for a call it relays."""
    import json as _json
    import urllib.request

    engine = await fusee.Engine.open(durability="memory", keep_history=True, workflows=[cart])
    url = engine.serve("127.0.0.1:0")
    run = await engine.start(cart, "aud-1", [])
    with fusee.as_requester("job:nightly"):
        await run.signal(add, "apple")

    def post(path, body):
        req = urllib.request.Request(url + path, data=_json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status

    assert await asyncio.to_thread(post, "/v1/runs/aud-1/signals/add", "pear") == 200
    assert await run.result() == ["apple", "pear"]
    journal = (await run.describe()).journal
    assert [r.get("by") for r in journal if r["kind"] == "signal"] == ["job:nightly", "http"], journal
    # A signal made outside `as_requester` names nobody.
    run2 = await engine.start(cart, "aud-2", [])
    await run2.signal(add, "x")
    with fusee.as_requester("ops:ana"):
        await run2.cancel("not needed")
    with pytest_raises(fusee.CancelledError):
        await run2.result()
    journal = (await run2.describe()).journal
    assert [r.get("by") for r in journal if r["kind"] == "signal"] == [None], journal
    assert [r.get("by") for r in journal if r["kind"] in ("cancel", "done")][0] == "ops:ana", journal
    await engine.close()


class pytest_raises:
    def __init__(self, exc):
        self.exc = exc

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        assert t is not None and issubclass(t, self.exc), f"expected {self.exc.__name__}, got {t}"
        return True


go = fusee.signal("go", int)


@fusee.workflow("report", concurrency=fusee.Concurrency(1, key=lambda input: input["tenant"]))
async def tenant_report(ctx: Ctx, input: dict) -> str:
    n = await ctx.wait_for(go)
    return f"{input['tenant']}/{input['n']}:{n}"


async def runs_at_once():
    """At most one run per tenant at a time; the others wait, in order."""
    engine = await fusee.Engine.open(durability="memory", workflows=[tenant_report])
    runs = [await engine.start(tenant_report, f"r-{i}", {"tenant": t, "n": i}) for i, t in enumerate("aaba")]

    async def live():
        rows = [r async for r in engine.list(type="report")]
        return sorted(r.id for r in rows if r.finished_ms is None)

    async def until(want):
        for _ in range(400):
            got = await live()
            if got == want:
                return got
            await asyncio.sleep(0.005)
        return await live()

    assert await until(["r-0", "r-2"]) == ["r-0", "r-2"]
    await runs[0].signal(go, 1)
    assert await runs[0].result() == "a/0:1"
    assert await until(["r-1", "r-2"]) == ["r-1", "r-2"]
    for i in (1, 2):
        await runs[i].signal(go, i)
    assert await until(["r-3"]) == ["r-3"]
    await runs[3].signal(go, 3)
    assert [await r.result() for r in runs] == ["a/0:1", "a/1:1", "b/2:2", "a/3:3"]
    await engine.close()


async def main():
    await runs_at_once()
    await audit()
    await definitions_and_typed_values()
    await http_api()
    await probes()
    await key_outside_a_step()
    await blocking_steps()
    await strict_replay()
    await logs_once()
    await signal_with_start()
    await compensations()
    await cancellation_reaches_steps()
    await delayed_start()
    await bad_directory_and_connection()
    await default_directory()
    print("dx ok")


if __name__ == "__main__":
    asyncio.run(main())

_ = Optional
