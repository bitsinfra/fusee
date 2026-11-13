"""The workflow features (docs/features.md), in the Python SDK."""

import asyncio
import inspect
import os
import shutil
import tempfile

from _helpers import journal, on, status
import fusee
from fusee import (
    Engine,
    FuseeError,
    RejectedError,
    Replayer,
    Retry,
    TerminatedError,
    aes_gcm,
)


def tmp():
    return tempfile.mkdtemp(prefix="fusee-py-feat-")


async def eventually(what, f):
    for _ in range(2000):
        v = f()
        if inspect.isawaitable(v):
            v = await v
        if v:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"timed out waiting for {what}")


async def queries_and_updates():
    d = tmp()
    priced = []

    @fusee.workflow("cart")
    async def cart(ctx, _):
        items = []
        ctx.on_query("items", lambda _: list(items))

        async def add(item):
            price = await ctx.step("price", lambda: priced.append(1) or len(item) * 10)
            items.append(item)
            return price

        ctx.on_update("add", add, validate=lambda item: None if item else "empty item")
        await ctx.wait_for("checkout")
        return items

    engine = await Engine.open(d, flush="fsync", keep_history=True, park_after=10, workflows=[cart])
    run = await engine.start(cart, "cart-1")
    assert await run.update("add", "apple", id="u-1") == 50
    assert await run.update("add", "apple", id="u-1") == 50
    assert len(priced) == 1
    await eventually("foreign workflow parks", lambda: any(m["runs_parked"] for m in engine.shard_metrics()))
    assert await run.query("items") == ["apple"]
    assert any(m["runs_loaded"] for m in engine.shard_metrics())
    assert len(priced) == 1
    try:
        await run.update("add", "")
        raise AssertionError("expected a rejection")
    except RejectedError as e:
        assert "empty" in str(e), e
    assert await run.query("items") == ["apple"]
    try:
        await run.query("nope")
        raise AssertionError("expected a rejection")
    except RejectedError:
        pass
    kinds = [r["kind"] for r in (await run.describe()).journal]
    assert kinds.count("update") == 1, kinds
    await engine.close()

    engine = await Engine.open(d, flush="fsync", keep_history=True, workflows=[cart])
    run = engine.run("cart-1")
    assert await run.update("add", "apple", id="u-1") == 50
    assert await run.update("add", "pear") == 40
    assert await run.query("items") == ["apple", "pear"]
    await run.signal("checkout")
    assert await run.result() == ["apple", "pear"]
    assert len(priced) == 2
    await engine.close()
    shutil.rmtree(d, ignore_errors=True)


async def update_with_start():
    engine = await Engine.open(durability="memory", keep_history=True)

    @on(engine, "counter")
    async def counter(ctx, start):
        n = {"v": start}

        def inc(by):
            n["v"] += by
            return n["v"]

        ctx.on_update("inc", inc)
        await ctx.wait_for("stop")
        return n["v"]

    run, result = await engine.update_with_start(counter, "c-1", 10, "inc", 5)
    assert result == 15
    assert (await run.describe()).journal[0]["update"]["name"] == "inc"
    _, result = await engine.update_with_start(counter, "c-1", 99, "inc", 1)
    assert result == 16
    await run.signal("stop")
    assert await run.result() == 16
    await engine.close()


async def heartbeats():
    d = tmp()
    started = []

    @fusee.workflow("export")
    async def export(ctx, _):
        async def work(s):
            start = s.progress() or 0
            started.append(start)
            for p in range(start, 6):
                s.heartbeat(p + 1)
                await asyncio.sleep(0.015)
            return 6

        return await ctx.step("export", work, retry=Retry(attempts=3, initial=1), heartbeat_timeout="1s")

    engine = await Engine.open(d, flush="fsync", workflows=[export])
    await engine.start(export, "exp-1")
    await asyncio.sleep(0.06)
    await engine.close()

    engine = await Engine.open(d, flush="fsync", workflows=[export])
    assert await engine.run("exp-1").result() == 6
    assert started[0] == 0 and started[1] > 0, started
    await engine.close()

    engine = await Engine.open(durability="memory")
    attempts = []

    @on(engine, "slow")
    async def slow(ctx, _):
        async def work(s):
            attempts.append(1)
            start = s.progress() or 0
            s.heartbeat(start + 1)
            if len(attempts) == 1:
                await asyncio.sleep(10)
            return start

        return await ctx.step("slow", work, retry=Retry(attempts=3, initial=1), heartbeat_timeout=100)

    assert await (await engine.start(slow, "s-1")).result() == 1
    assert len(attempts) == 2, attempts
    await engine.close()
    shutil.rmtree(d, ignore_errors=True)


async def async_completion():
    engine = await Engine.open(durability="memory")
    tokens = []

    @on(engine, "approve")
    async def approve(ctx, _):
        def send(cb):
            tokens.append(cb.token)
            if len(tokens) == 1:
                raise RuntimeError("mail server down")

        return await ctx.await_completion("approval", send, retry=Retry(attempts=3, initial=1))

    run = await engine.start(approve, "a-1")
    await eventually("two tokens", lambda: len(tokens) == 2)
    try:
        await engine.complete(tokens[0], True)
        raise AssertionError("a stale token was accepted")
    except RejectedError:
        pass
    await engine.complete(tokens[1], True)
    assert await run.result() is True
    try:
        await engine.complete(tokens[1], False)
        raise AssertionError("a used token was accepted")
    except RejectedError:
        pass
    await engine.close()


async def can_patch_reset():
    engine = await Engine.open(durability="memory", keep_history=True)

    @on(engine, "loop")
    async def loop(ctx, state):
        left, acc = state
        v = await ctx.step("work", lambda: left)
        assert ctx.journal_size() <= 2
        if left == 0:
            return acc + v
        await ctx.continue_as_new([left - 1, acc + v])

    run = await engine.start(loop, "loop-1", [5, 0])
    assert await run.result() == 15
    assert (await run.describe()).journal[0]["from"] == "continued"

    @on(engine, "patchy")
    async def patchy(ctx, _):
        return "new" if ctx.patched("fix") else "old"

    assert await (await engine.start(patchy, "p-1")).result() == "new"

    charged = []

    @on(engine, "pay")
    async def pay(ctx, n):
        quote = await ctx.step("quote", lambda: n)
        key = await ctx.step("charge", lambda s: charged.append(1) or s.key)
        go = await ctx.wait_for("go")
        return f"{quote}/{key}/{go}"

    p = await engine.start(pay, "pay-1", 7)
    await p.signal("go", 1)
    first = await p.result()
    await p.reset("charge")
    second = await p.result()
    assert len(charged) == 2
    assert first != second and second.startswith("7/") and second.endswith("/1"), second
    await engine.close()


async def close_policy():
    engine = await Engine.open(durability="memory")

    @on(engine, "kid")
    async def kid(ctx, _):
        return await ctx.wait_for("never")

    @on(engine, "parent")
    async def parent(ctx, _):
        ctx.start_child("kid", "kid-terminate")
        ctx.start_child("kid", "kid-cancel", parent_close="request-cancel")
        ctx.start_child("kid", "kid-abandon", parent_close="abandon")
        return await ctx.wait_for("never")

    run = await engine.start(parent, "parent-1")
    async def all_started():
        return all([await status(engine, i) for i in ("kid-terminate", "kid-cancel", "kid-abandon")])

    await eventually("children", all_started)
    await run.terminate("stop")
    try:
        await run.result()
        raise AssertionError("expected termination")
    except TerminatedError:
        pass

    async def closed():
        return await status(engine, "kid-terminate") == "terminated" and await status(engine, "kid-cancel") == "cancelled"

    await eventually("children closed", closed)
    assert await status(engine, "kid-abandon") in ("suspended", "running", "parked")
    await engine.close()


async def schedules_attributes_limits():
    engine = await Engine.open(durability="memory")
    runs = []

    @on(engine, "report")
    async def report(ctx, _):
        runs.append(ctx.workflow_id)
        return ctx.workflow_id

    s = await engine.schedule("nightly", {"every": "100ms"}, report)
    await eventually("three runs", lambda: len(runs) >= 3)
    assert (await s.describe())["started"] >= 3
    await s.pause()
    await asyncio.sleep(0.05)
    n = len(runs)
    await asyncio.sleep(0.3)
    assert len(runs) == n
    ids = [r.id async for r in engine.list(type=report)]
    assert len(set(ids)) == len(ids) and all(i.startswith("nightly@") for i in ids), ids
    await s.delete()

    @on(engine, "order")
    async def order(ctx, amount):
        ctx.set_attributes({"customer": "c-1", "amount": amount})
        if amount > 100:
            raise ValueError("too big")
        ctx.set_attributes({"stage": "paid"})
        return amount

    for i, a in enumerate([50, 150, 80]):
        try:
            await (await engine.start(order, f"o-{i}", a)).result()
        except FuseeError:
            pass

    async def q(**filters):
        return sorted([r.id async for r in engine.list(**filters)])

    assert await q(attributes={"customer": "c-1"}) == ["o-0", "o-1", "o-2"]
    assert await q(status="failed") == ["o-1"]
    assert await q(attributes={"amount": {"gte": 60, "lt": 200}, "stage": "paid"}) == ["o-2"]

    engine.limit("stripe", concurrent=2)
    load = {"now": 0, "max": 0}

    @on(engine, "charge")
    async def charge(ctx, _):
        async def work():
            load["now"] += 1
            load["max"] = max(load["max"], load["now"])
            await asyncio.sleep(0.02)
            load["now"] -= 1

        return await ctx.step("charge", work, limit="stripe")

    started = [await engine.start(charge, f"c-{i}") for i in range(8)]
    for r in started:
        await r.result()
    assert load["max"] == 2, load
    await engine.close()


async def encryption():
    d = tmp()
    codec = aes_gcm({"k1": bytes([3]) * 32})
    engine = await Engine.open(d, flush="fsync", codec=codec)

    @on(engine, "secret")
    async def secret(ctx, card):
        masked = await ctx.step("mask", lambda: f"{card[:4]}-SECRETRESULT")
        note = await ctx.wait_for("note")
        return f"{masked}/{note}"

    run = await engine.start(secret, "s-1", "4242SECRETCARD")
    await run.signal("note", "SECRETSIGNAL")
    assert await run.result() == "4242-SECRETRESULT/SECRETSIGNAL"
    await engine.close()
    for root, _, files in os.walk(d):
        for f in files:
            with open(os.path.join(root, f), "rb") as fh:
                b = fh.read()
            for needle in (b"SECRETCARD", b"SECRETRESULT", b"SECRETSIGNAL"):
                assert needle not in b, f"{f} holds {needle}"
    engine = await Engine.open(d, flush="fsync", codec=codec)
    assert await engine.run("s-1").result() == "4242-SECRETRESULT/SECRETSIGNAL"
    await engine.close()
    shutil.rmtree(d, ignore_errors=True)


async def replay_and_id_policies():
    engine = await Engine.open(durability="memory", keep_history=True)

    @on(engine, "pay")
    async def pay(ctx, _):
        q = await ctx.step("quote", lambda: 3)
        return await ctx.step("charge", lambda: q * 2)

    await (await engine.start(pay, "p-1")).result()
    file = engine.export_journal("p-1")

    @fusee.workflow("pay")
    async def pay_same(ctx, _):
        q = await ctx.step("quote", lambda: 0)
        return await ctx.step("charge", lambda: q)

    rep = await Replayer(workflows=[pay_same]).replay(file)
    assert rep.ok and rep.finished, rep

    @fusee.workflow("pay")
    async def pay_changed(ctx, _):
        q = await ctx.step("quote", lambda: 3)
        return await ctx.step("refund", lambda: q)

    rep = await Replayer(workflows=[pay_changed]).replay(file)
    assert not rep.ok and "'charge'" in rep.error and "'refund'" in rep.error, rep

    @on(engine, "w")
    async def w(ctx, fail):
        await ctx.wait_for("go", timeout=50)
        if fail:
            raise ValueError("no")
        return ctx.run_id

    a = await engine.start(w, "id-1", False)

    async def refused(**kw):
        try:
            await engine.start(w, "id-1", False, id_policy=kw or None)
        except fusee.AlreadyExistsError:
            return True
        return False

    assert await refused()
    same_run = await engine.start(w, "id-1", False, id_policy={"running": "use-existing"})
    first = await a.result()
    assert await same_run.result() == first
    try:
        await (await engine.start(w, "id-1", True)).result()
    except FuseeError:
        pass
    c = await engine.start(w, "id-1", False, id_policy={"finished": "allow-if-failed"})
    assert await c.result() > first
    assert await refused(finished="allow-if-failed")
    assert await refused(finished="refuse")
    await engine.close()


def test_queries_and_updates():
    asyncio.run(queries_and_updates())


def test_update_with_start():
    asyncio.run(update_with_start())


def test_heartbeats():
    asyncio.run(heartbeats())


def test_async_completion():
    asyncio.run(async_completion())


def test_can_patch_reset():
    asyncio.run(can_patch_reset())


def test_close_policy():
    asyncio.run(close_policy())


def test_schedules_attributes_limits():
    asyncio.run(schedules_attributes_limits())


def test_encryption():
    asyncio.run(encryption())


def test_replay_and_id_policies():
    asyncio.run(replay_and_id_policies())


if __name__ == "__main__":
    for t in (
        test_queries_and_updates,
        test_update_with_start,
        test_heartbeats,
        test_async_completion,
        test_can_patch_reset,
        test_close_policy,
        test_schedules_attributes_limits,
        test_encryption,
        test_replay_and_id_policies,
    ):
        t()
        print("ok", t.__name__)
