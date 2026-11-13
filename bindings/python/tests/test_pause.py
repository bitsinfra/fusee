"""A step whose retries run out pauses its run; a resume runs it again with
a fresh set of attempts."""

import asyncio
import inspect
import shutil
import tempfile

from _helpers import journal, on, status
import fusee
from fusee import Engine, RejectedError, Retry


def tmp():
    return tempfile.mkdtemp(prefix="fusee-py-pause-")


async def eventually(what, f):
    for _ in range(2000):
        v = f()
        if inspect.isawaitable(v):
            v = await v
        if v:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"timed out waiting for {what}")


class Service:
    def __init__(self):
        self.broken = True
        self.attempts = []

    def charge(self, n, attempt):
        self.attempts.append(attempt)
        if self.broken:
            raise RuntimeError("payments are down")
        return n * 10


def register(engine, svc, retry=Retry(attempts=2, initial=1, on_exhausted="pause")):
    @on(engine, "pay")
    async def pay(ctx, n):
        return await ctx.step("charge", lambda s: svc.charge(n, s.attempt), retry=retry) + 1

    return pay


async def a_paused_child_keeps_its_parent_waiting():
    d = tmp()
    svc = Service()
    engine = await Engine.open(d, keep_history=True)
    paused = []
    engine.on("paused", lambda wid, step, failure: paused.append((wid, step, failure.message)))
    try:
        pay = register(engine, svc)

        @on(engine, "order")
        async def order(ctx, n):
            return await ctx.child(pay, "pay-1", n)

        run = await engine.start(order, "order-1", 4)
        await eventually("the pause", lambda: _is(engine, "pay-1", "paused"))
        assert len(svc.attempts) == 2
        assert paused == [("pay-1", "charge", "payments are down")], paused
        await asyncio.sleep(0.05)
        assert len(svc.attempts) == 2, "nothing retries while paused"
        assert await status(engine, "order-1") in ("suspended", "parked")

        svc.broken = False
        await engine.run("pay-1").resume()
        assert await run.result() == 41
        assert svc.attempts == [1, 2, 1], svc.attempts
        kinds = [r["kind"] for r in await journal(engine, "pay-1")]
        assert kinds == ["start", "attempt", "attempt", "resume", "step", "done"], kinds
        try:
            await engine.run("pay-1").resume()
            raise AssertionError("resumed twice")
        except RejectedError:
            pass
    finally:
        await engine.close()
        shutil.rmtree(d, ignore_errors=True)


async def _is(engine, id, want):
    return await status(engine, id) == want


async def stays_paused_across_a_restart():
    d = tmp()
    svc = Service()
    plain = Retry(attempts=2, initial=1)
    engine = await Engine.open(d, on_exhausted="pause")
    register(engine, svc, plain)
    await engine.start("pay", "pay-2", 1)
    await eventually("the pause", lambda: _is(engine, "pay-2", "paused"))
    await engine.close()
    engine = await Engine.open(d, on_exhausted="pause")
    try:
        register(engine, svc, plain)
        await eventually("the paused run", lambda: _is(engine, "pay-2", "paused"))
        await asyncio.sleep(0.05)
        assert len(svc.attempts) == 2
        await engine.run("pay-2").resume()

        async def paused_again():
            return len(svc.attempts) == 4 and await _is(engine, "pay-2", "paused")

        await eventually("two more attempts and a pause", paused_again)
        svc.broken = False
        await engine.run("pay-2").resume()
        assert await engine.run("pay-2").result() == 11
        assert svc.attempts == [1, 2, 1, 2, 1], svc.attempts
    finally:
        await engine.close()
        shutil.rmtree(d, ignore_errors=True)


async def cancel_reaches_a_paused_run():
    d = tmp()
    svc = Service()
    engine = await Engine.open(d)
    try:
        register(engine, svc)
        run = await engine.start("pay", "pay-c", 1)
        await eventually("the pause", lambda: _is(engine, "pay-c", "paused"))
        await run.cancel("not needed")
        try:
            await run.result()
            raise AssertionError("a cancelled run returned")
        except fusee.CancelledError:
            pass
        assert len(svc.attempts) == 2
    finally:
        await engine.close()
        shutil.rmtree(d, ignore_errors=True)


def test_a_paused_child_keeps_its_parent_waiting():
    asyncio.run(a_paused_child_keeps_its_parent_waiting())


def test_stays_paused_across_a_restart():
    asyncio.run(stays_paused_across_a_restart())


def test_cancel_reaches_a_paused_run():
    asyncio.run(cancel_reaches_a_paused_run())


if __name__ == "__main__":
    for t in (test_a_paused_child_keeps_its_parent_waiting, test_stays_paused_across_a_restart,
              test_cancel_reaches_a_paused_run):
        t()
        print("ok", t.__name__)
