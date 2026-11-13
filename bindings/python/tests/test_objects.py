"""Keyed objects in the Python SDK: exclusive order, shared reads, state
across restarts, exactly-once calls, calls from workflows and between
objects, and compaction."""

import asyncio
import shutil
import tempfile

import fusee
from fusee import Engine, FuseeError, shared
from _helpers import journal, on, status


def tmp():
    return tempfile.mkdtemp(prefix="fusee-py-objects-")


async def eventually(what, f):
    for _ in range(4000):
        v = f()
        if hasattr(v, "__await__"):
            v = await v
        if v:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"timed out waiting for {what}")


def define_cart(counters):
    cart = fusee.object("cart")

    @cart.exclusive
    async def add(ctx, item):
        def price():
            counters["ran"] += 1
            return f"{item}$"

        priced = await ctx.step("price", price)
        items = ctx.state.get("items", [])
        items.append(priced)
        ctx.state.set("items", items)
        return len(items)

    @cart.shared
    async def get(ctx, _):
        return ctx.state.get("items", [])

    @cart.exclusive
    async def fail(ctx, _):
        ctx.state.set("items", ["lost"])
        raise FuseeError("out of stock")

    @cart.shared(name="key")
    async def key_of(ctx, _):
        return ctx.object_key

    return cart


async def calls_and_restarts():
    d = tmp()
    counters = {"ran": 0}
    try:
        cart = define_cart(counters)
        engine = await Engine.open(dir=d, flush="fsync", objects=[cart])
        k = engine.object(cart, "u-1")
        assert await k.call("add", "apple", id="a") == 1
        assert await k.call("add", "pear", id="b") == 2
        assert await k.call("add", "pear", id="b") == 2
        assert counters["ran"] == 2
        assert await k.call("get") == ["apple$", "pear$"]
        try:
            await k.call("fail")
            raise AssertionError("fail should fail")
        except FuseeError as e:
            assert "out of stock" in str(e), e
        assert await k.state() == {"items": ["apple$", "pear$"]}
        assert await k.call("key") == "u-1"
        assert await engine.object("cart", "nobody").state() == {}
        info = await k.describe()
        assert info.object["state"] == {"items": ["apple$", "pear$"]}
        assert len([r for r in info.journal if r["kind"] == "update_done" and "state" in r]) == 2
        await k.send("add", "fig", id="c")
        assert [r.id async for r in engine.list(type=cart) if r.object] == ["cart/u-1"]
        await engine.close()

        engine = await Engine.open(dir=d, flush="fsync", objects=[cart])
        k = engine.object(cart, "u-1")
        await eventually("nothing", lambda: True)
        assert len((await k.state())["items"]) == 3
        assert await k.call("add", "pear", id="b") == 2
        assert counters["ran"] == 3
        await engine.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


async def turns_and_shared_reads():
    engine = await Engine.open(durability="memory")
    try:
        state = {"busy": False, "overlaps": 0}
        gate = asyncio.get_running_loop().create_future()
        doc = fusee.object("doc")

        @doc.exclusive
        async def bump(ctx, who):
            if state["busy"]:
                state["overlaps"] += 1
            state["busy"] = True

            async def work():
                await asyncio.sleep(0.001)

            await ctx.step("work", work)
            seen = ctx.state.get("seen", [])
            seen.append(who)
            ctx.state.set("seen", seen)
            state["busy"] = False
            return len(seen)

        @doc.exclusive
        async def hold(ctx, _):
            ctx.state.set("held", True)

            async def wait():
                await gate

            await ctx.step("hold", wait)

        @doc.shared
        async def read(ctx, _):
            return ctx.state.get("held", False)

        engine.register(doc)
        k = engine.object(doc, "d")
        places = await asyncio.gather(*[k.call("bump", who) for who in range(20)])
        assert state["overlaps"] == 0
        seen = (await k.state())["seen"]
        for who, place in enumerate(places):
            assert seen[place - 1] == who, (seen, places)
        held = asyncio.ensure_future(k.call("hold"))
        async def running():
            return len((await k.describe()).object["running"]) == 1

        async def waiting():
            return len((await k.describe()).object["waiting"]) == 1

        await eventually("hold to start", running)
        assert await k.call("read") is False
        nxt = asyncio.ensure_future(k.call("bump", 99))
        await eventually("bump to wait", waiting)
        gate.set_result(None)
        await held
        assert await nxt == 21
        assert await k.call("read") is True
    finally:
        await engine.close()


async def calls_from_workflows():
    engine = await Engine.open(durability="memory", keep_history=True)
    try:
        counters = {"ran": 0}
        engine.register(define_cart(counters))

        async def place(ctx, items):
            n = 0
            for i in items:
                n = await ctx.object("cart", ctx.object_key).call("add", i)
            ctx.state.set("placed", n)
            return n

        engine.register(fusee.object("order", {"place": place}))

        @on(engine, "shopper")
        async def shopper(ctx, key):
            a = await ctx.object("cart", key).call("add", "tea")
            ctx.object("cart", key).send("add", "milk")
            placed = await ctx.object("order", key).call("place", ["jam", "bread"])
            items = await ctx.object("cart", key).call("get")
            return {"a": a, "placed": placed, "items": items}

        run = await engine.start(shopper, "shop-1", "s1")
        out = await run.result()
        assert out["a"] == 1
        assert sorted(out["items"]) == ["bread$", "jam$", "milk$", "tea$"], out
        assert counters["ran"] == 4
        assert len([r for r in (await run.describe()).journal if r["kind"] == "call"]) == 4
    finally:
        await engine.close()


async def compaction():
    engine = await Engine.open(durability="memory")
    try:
        ran = {"n": 0}

        async def add(ctx, n):
            ran["n"] += 1
            s = ctx.state.get("sum", 0) + n
            ctx.state.set("sum", s)
            return s

        tally = fusee.object("tally", {"add": add, "peek": shared(lambda ctx, _: ctx.state.get("sum"))})
        engine.register(tally)
        k = engine.object(tally, "t")
        for n in range(1, 301):
            assert await k.call("add", n, id=f"id-{n}") == n * (n + 1) // 2
        info = await k.describe()
        assert len(info.journal) <= 2 * 128 + 4, len(info.journal)
        assert info.object["state"] == {"sum": 45150}
        before = ran["n"]
        assert await k.call("add", 5, id="id-5") == 15
        assert ran["n"] == before
        assert await k.call("peek") == 45150
    finally:
        await engine.close()


def test_calls_and_restarts():
    asyncio.run(calls_and_restarts())


def test_turns_and_shared_reads():
    asyncio.run(turns_and_shared_reads())


def test_calls_from_workflows():
    asyncio.run(calls_from_workflows())


def test_compaction():
    asyncio.run(compaction())


if __name__ == "__main__":
    for t in (test_calls_and_restarts, test_turns_and_shared_reads, test_calls_from_workflows, test_compaction):
        t()
        print("ok", t.__name__)
