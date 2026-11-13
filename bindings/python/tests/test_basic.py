import asyncio
import shutil
import tempfile
import time

import fusee
from _helpers import journal, on, status
from fusee import CancelledError, Engine, Replayer, Retry, StepError


def tmp():
    return tempfile.mkdtemp(prefix="fusee-py-")


async def scenario():
    d = tmp()
    engine = await Engine.open(d, flush="fsync", shards=2)
    charged = []

    @on(engine, "ship")
    async def ship(ctx, x):
        return await ctx.step("label", lambda: f"label-{x}")

    @on(engine, "checkout")
    async def checkout(ctx, order):
        quote = await ctx.step("quote", lambda: order["amount"] * 2)

        def charge(s: fusee.StepCtx):
            charged.append(s.key)
            return {"id": f"ch_{quote}"}

        c = await ctx.step("charge", charge, retry=Retry(attempts=3))
        await ctx.sleep("cool-off", "5ms")
        approved = await ctx.wait_for("review", timeout="2s")
        label = await ctx.child("ship", f"{ctx.workflow_id}/ship", quote)

        async def slow():
            await asyncio.sleep(0.001)
            return 2

        a, b = await ctx.all([ctx.step("a", lambda: 1), ctx.step("b", slow)])
        return {"charge": c["id"], "approved": approved, "label": label, "sum": a + b,
                "t": ctx.now() > 0, "u": len(ctx.uuid())}

    run = await engine.start(checkout, "order-123", {"amount": 21})
    await run.signal("review", True)
    out = await run.result()
    assert out == {"charge": "ch_42", "approved": True, "label": "label-42", "sum": 3, "t": True, "u": 36}, out
    assert len(charged) == 1
    again = await engine.start(checkout, "order-123", {"amount": 99}, id_policy={"running": "use-existing", "finished": "use-existing"})
    assert await again.result() == out
    await engine.close()
    shutil.rmtree(d, ignore_errors=True)


async def retries_and_cancel():
    engine = await Engine.open(durability="memory")
    n = {"c": 0}

    @on(engine, "flaky")
    async def flaky(ctx, fail_always):
        def call():
            n["c"] += 1
            if fail_always or n["c"] < 3:
                raise ValueError(f"attempt {n['c']}")
            return n["c"]

        return await ctx.step("call", call, retry=Retry(attempts=4, initial="1ms"))

    assert await (await engine.start(flaky, "f1", False)).result() == 3
    n["c"] = 0
    try:
        await (await engine.start(flaky, "f2", True)).result()
        raise AssertionError("expected failure")
    except fusee.StepError as e:
        assert e.step == "call" and e.cause.message == "attempt 4" and e.cause.type == "ValueError", e

    @on(engine, "saga")
    async def saga(ctx, _):
        await ctx.step("reserve", lambda: True)
        try:
            await ctx.wait_for("confirm")
            return "confirmed"
        except CancelledError as e:
            await ctx.step("release", lambda: True)
            raise CancelledError(f"compensated: {e.reason}")

    run = await engine.start(saga, "s1", None)
    await asyncio.sleep(0.01)
    await run.cancel("customer left")
    try:
        await run.result()
        raise AssertionError("expected cancellation")
    except CancelledError as e:
        assert e.reason == "compensated: customer left", e.reason

    @on(engine, "bad")
    async def bad(ctx, _):
        try:
            await ctx.step("boom", lambda: 1 / 0, retry=Retry(attempts=1))
        except StepError as e:
            return e.step

    assert await (await engine.start(bad, "b1", None)).result() == "boom"

    @on(engine, "racer")
    async def racer(ctx, _):
        i, v = await ctx.race([ctx.sleep("slow", "2s"), ctx.step("fast", lambda: "f")])
        return [i, v]

    assert await (await engine.start(racer, "r1", None)).result() == [1, "f"]
    await engine.close()


async def recovery():
    d = tmp()
    calls = []

    @fusee.workflow("pausing")
    async def pausing(ctx, _):
        a = await ctx.step("a", lambda: calls.append(1) or 7)
        v = await ctx.wait_for("go")
        return a + v

    engine = await Engine.open(d, flush="fsync", workflows=[pausing])
    await engine.start(pausing, "p1", None)
    while not calls:
        await asyncio.sleep(0.002)
    await asyncio.sleep(0.02)
    await engine.close()

    engine = await Engine.open(d, flush="fsync", workflows=[pausing])
    run = engine.run("p1")
    await run.signal("go", 5)
    assert await run.result() == 12
    assert len(calls) == 1, calls
    await engine.close()
    shutil.rmtree(d, ignore_errors=True)


async def synchronous_steps():
    """Synchronous steps complete without growing the stack, and in the
    order replay reproduces."""
    engine = await Engine.open(durability="memory", keep_history=True)
    n = 5000

    @on(engine, "chain")
    async def chain(ctx, read_only):
        total = 0
        for i in range(n):
            total += await ctx.step(f"s{i}", lambda i=i: i, read_only=read_only)
        return total

    @on(engine, "wide")
    async def wide(ctx, args):
        width, length, read_only = args

        async def branch(j):
            total = 0
            for i in range(length):
                total += await ctx.step(f"b{j}-{i}", lambda: 1, read_only=read_only)
            return total

        return sum(await ctx.all([branch(j) for j in range(width)]))

    assert await (await engine.start(chain, "chain-ro", True)).result() == n * (n - 1) // 2
    assert await (await engine.start(chain, "chain", False)).result() == n * (n - 1) // 2
    assert await (await engine.start(wide, "wide-ro", [2000, 1, True])).result() == 2000
    assert await (await engine.start(wide, "wide", [2000, 1, False])).result() == 2000

    # Each branch's next step begins only after every branch has blocked,
    # as on replay.
    await (await engine.start(wide, "order-ro", [3, 2, True])).result()
    names = (await engine.run("order-ro").describe()).steps
    assert names == ["b0-0", "b1-0", "b2-0", "b0-1", "b1-1", "b2-1"], names
    replayer = Replayer(workflows=[wide])
    report = await replayer.replay(engine.export_journal("order-ro"))
    assert report.ok and report.finished, report
    await engine.close()


async def retry_delay_wakes():
    """Wakes during a retry delay neither ask the gate again nor arm more
    timers: the delay's one timer runs the next attempt."""
    engine = await Engine.open(durability="memory")
    delay = 600
    tries = []

    @on(engine, "backoff")
    async def backoff(ctx, _):
        def flaky():
            tries.append(1)
            if len(tries) == 1:
                raise ValueError("first attempt fails")
            return len(tries)

        async def ticks():
            n = 0
            # Each signal wakes the run while the flaky step waits out its delay.
            for _ in range(20):
                n += await ctx.wait_for("tick")
            return n

        return await ctx.all([ctx.step("flaky", flaky, retry=Retry(attempts=2, initial=delay)), ticks()])

    # Count the gate checks of the retry (the only ones with a not-before
    # time) and the timers armed for its delay.
    counts = {"gates": 0, "timers": 0}
    from fusee._runtime import RunExec

    may_run = RunExec.may_run
    loop = asyncio.get_running_loop()
    call_later = loop.call_later

    def counted_may_run(self, gate, not_before):
        if not_before > 0:
            counts["gates"] += 1
        return may_run(self, gate, not_before)

    def counted_call_later(seconds, *args, **kwargs):
        if delay / 2000 < seconds <= delay / 1000:
            counts["timers"] += 1
        return call_later(seconds, *args, **kwargs)

    RunExec.may_run = counted_may_run
    loop.call_later = counted_call_later
    try:
        run = await engine.start(backoff, "r-1", None)
        while not tries:
            await asyncio.sleep(0.001)
        began = time.monotonic()
        for _ in range(20):
            await run.signal("tick", 1)
        assert time.monotonic() - began < delay / 1000, "the signals arrived during the delay"
        assert await run.result() == [2, 20]
    finally:
        RunExec.may_run = may_run
        del loop.call_later
    # Once when the delay began, once when its timer fired.
    assert counts["gates"] <= 2, counts
    assert counts["timers"] == 1, counts
    await engine.close()


async def raw_bytes():
    """Steps, signals and results that are bytes cross without JSON and
    come back as bytes, live and after a restart."""
    d = tmp()
    try:
        ran = []

        @fusee.workflow("blob")
        async def blob(ctx, inp):
            def make():
                ran.append(1)
                return b"\x00\xffraw" + inp
            a = await ctx.step("make", make)
            b = await ctx.step("json", lambda: {"n": len(a)})
            sig = await ctx.wait_for("more")
            return a + sig + str(b["n"]).encode()

        engine = await Engine.open(d, flush="fsync", workflows=[blob])
        run = await engine.start(blob, "b-1", b"in")
        await run.signal("more", b"!")
        out = await run.result()
        assert out == b"\x00\xffrawin!7", out
        assert isinstance(out, bytes)
        await engine.close()
        # Recovery replays the recorded bytes; the step does not run again.
        engine = await Engine.open(d, flush="fsync", workflows=[blob])
        assert await engine.run("b-1").result() == b"\x00\xffrawin!7"
        assert ran == [1], ran
        await engine.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_raw_bytes():
    asyncio.run(raw_bytes())


def test_scenario():
    asyncio.run(scenario())


def test_retry_delay_wakes():
    asyncio.run(retry_delay_wakes())


def test_synchronous_steps():
    asyncio.run(synchronous_steps())


def test_retries_and_cancel():
    asyncio.run(retries_and_cancel())


def test_recovery():
    asyncio.run(recovery())


if __name__ == "__main__":
    for t in (test_scenario, test_retries_and_cancel, test_recovery, test_synchronous_steps, test_retry_delay_wakes,
              test_raw_bytes):
        t()
        print("ok", t.__name__)
