"""Conformance runner (Python SDK). See conformance/README.md."""
import asyncio
import json

import fusee
from fusee import CancelledError, Engine, FuseeError, Retry, StepError


def on(engine, name):
    """Defines a workflow and registers it with ``engine``."""

    def define(fn):
        wf = fusee.workflow(name)(fn)
        engine.register(wf)
        return wf

    return define


async def journal(engine, id):
    return (await engine.run(id).describe()).journal


async def status(engine, id):
    try:
        return (await engine.run(id).status()).status
    except fusee.NotFoundError:
        return None


async def main():
    engine = await Engine.open(durability="memory", shards=2, keep_history=True)
    r3 = Retry(attempts=3, initial=1, max=1)

    @on(engine, "seq")
    async def seq(ctx, inp):
        a = await ctx.step("a", lambda: inp["n"] + 1)
        b = await ctx.step("b", lambda: a * 2)
        return await ctx.step("c", lambda: {"a": a, "b": b})

    @on(engine, "parallel")
    async def parallel(ctx, n):
        x, y = await ctx.all([ctx.step("x", lambda: n), ctx.step("y", lambda: n * 10)])
        return await ctx.step("z", lambda: x + y)

    tries = {}

    @on(engine, "retry")
    async def retry(ctx, _):
        def flaky():
            tries[ctx.workflow_id] = tries.get(ctx.workflow_id, 0) + 1
            if tries[ctx.workflow_id] < 3:
                raise Exception(f"try {tries[ctx.workflow_id]}")
            return "ok"

        return await ctx.step("flaky", flaky, retry=r3)

    @on(engine, "read_only")
    async def read_only(ctx, _):
        def fetch():
            tries[ctx.workflow_id] = tries.get(ctx.workflow_id, 0) + 1
            if tries[ctx.workflow_id] < 2:
                raise Exception(f"try {tries[ctx.workflow_id]}")
            return "data"

        v = await ctx.step("fetch", fetch, retry=r3, read_only=True)
        return await ctx.step("store", lambda: f"{v}!")

    @on(engine, "signal")
    async def signal(ctx, _):
        v = await ctx.wait_for("go")
        return await ctx.step("use", lambda: f"{v}!")

    @on(engine, "double")
    async def double(ctx, x):
        return await ctx.step("mul", lambda: x * 2)

    @on(engine, "child")
    async def child(ctx, _):
        return await ctx.child("double", f"{ctx.workflow_id}/c", 21)

    @on(engine, "sleep")
    async def sleep(ctx, _):
        await ctx.sleep("nap", 1)
        return await ctx.step("after", lambda: "woke")

    @on(engine, "values")
    async def values(ctx, _):
        ctx.now()
        ctx.random()
        ctx.uuid()
        return "values"

    @on(engine, "cancel")
    async def cancel(ctx, _):
        try:
            return await ctx.wait_for("never")
        except CancelledError as e:
            await ctx.step("undo", lambda: e.reason)
            raise CancelledError(f"undone: {e.reason}")

    @on(engine, "fail")
    async def fail(ctx, _):
        def boom():
            raise Exception("boom")

        try:
            await ctx.step("boom", boom, retry=Retry(attempts=1))
            return "unexpected"
        except StepError:
            return await ctx.step("recover", lambda: "recovered")

    @on(engine, "listener")
    async def listener(ctx, _):
        return (await ctx.wait_for("ping")) or ""

    @on(engine, "send")
    async def send(ctx, target):
        ctx.signal_workflow(target, "ping", "hello")
        return "sent"

    @on(engine, "timeout_wait")
    async def timeout_wait(ctx, _):
        v = await ctx.wait_for("never", timeout=5)
        return await ctx.step("after", lambda: "none" if v is None else "some")

    await engine.start(seq, "seq", {"n": 3})
    await engine.start(parallel, "parallel", 5)
    await engine.start(retry, "retry", None)
    await engine.start(read_only, "read_only", None)
    await engine.start(signal, "signal", None)
    await engine.run("signal").signal("go", "hi")
    await engine.start(child, "child", None)
    await engine.start(sleep, "sleep", None)
    await engine.start(values, "values", None)
    await engine.start(cancel, "cancel", None)
    await engine.start(fail, "fail", None)
    await engine.start(listener, "listener", None)
    await engine.start(send, "send", "listener")
    await engine.start(timeout_wait, "timeout_wait", None)
    for _ in range(100):
        if await status(engine, "cancel") in ("suspended", "parked"):
            break
        await asyncio.sleep(0.001)
    await engine.run("cancel").cancel("stop")
    out = {}
    for id in ["seq", "parallel", "retry", "read_only", "signal", "child", "child/c", "sleep", "values", "cancel",
               "fail", "listener", "send", "timeout_wait"]:
        try:
            await engine.run(id).result()
        except Exception:  # noqa: BLE001 - failed/cancelled runs still have journals
            pass
        out[id] = await journal(engine, id)
    await features(engine, out)
    await streams(engine, out, r3)
    await queues(engine, out)
    await pause(engine, out)
    await engine.close()
    await objects(out)
    print(json.dumps(out, indent=2))


async def until(engine, id, states):
    for _ in range(2000):
        if await status(engine, id) in states:
            return
        await asyncio.sleep(0.001)
    raise RuntimeError(f"{id} never reached {states}")


async def features(engine, out):
    """The workflow features (docs/features.md)."""

    @on(engine, "updates")
    async def updates(ctx, _):
        total = {"v": 0}

        async def add(v):
            d = await ctx.step("apply", lambda: v * 2)
            total["v"] += d
            return total["v"]

        ctx.on_update("add", add, validate=lambda v: "negative" if v < 0 else None)
        ctx.on_query("total", lambda _: total["v"])
        await ctx.wait_for("done")
        return total["v"]

    @on(engine, "update_start")
    async def update_start(ctx, start):
        n = {"v": start}

        def inc(by):
            n["v"] += by
            return n["v"]

        ctx.on_update("inc", inc)
        await ctx.wait_for("stop")
        return n["v"]

    @on(engine, "heartbeat")
    async def heartbeat(ctx, _):
        async def work(s):
            s.heartbeat(1)
            await asyncio.sleep(0.03)
            return s.progress() or 0

        return await ctx.step("work", work)

    token = {}

    @on(engine, "await")
    async def await_(ctx, _):
        return await ctx.await_completion("approval", lambda cb: token.setdefault("t", cb.token) and None)

    @on(engine, "continued")
    async def continued(ctx, left):
        v = await ctx.step("work", lambda: left)
        if left == 0:
            return v
        await ctx.continue_as_new(left - 1)

    @on(engine, "patched")
    async def patched(ctx, _):
        ctx.deprecate_patch("old")
        label = "new" if ctx.patched("new") else "old"
        return await ctx.step("after", lambda: label)

    @on(engine, "attributes")
    async def attributes(ctx, _):
        ctx.set_attributes({"customer": "c-1", "amount": 5})
        ctx.set_attributes({"stage": "paid"})
        return "attributes"

    @on(engine, "kid")
    async def kid(ctx, _):
        return (await ctx.wait_for("go")) or ""

    @on(engine, "parent_close")
    async def parent_close(ctx, _):
        ctx.start_child("kid", "parent_close/terminate", None)
        ctx.start_child("kid", "parent_close/abandon", None, parent_close="abandon")
        await ctx.wait_for("never")
        return "unreachable"

    @on(engine, "reset")
    async def reset(ctx, _):
        a = await ctx.step("a", lambda: 1)
        b = await ctx.step("b", lambda: a + 1)
        return await ctx.step("c", lambda: b + 1)

    @on(engine, "victim")
    async def victim(ctx, _):
        try:
            await ctx.wait_for("never")
            return "unexpected"
        except CancelledError as e:
            await ctx.step("undo", lambda: e.reason)
            raise CancelledError("undone")

    @on(engine, "canceller")
    async def canceller(ctx, _):
        ctx.cancel_workflow("victim", reason="stop it")
        return "sent"

    @on(engine, "race_wait")
    async def race_wait(ctx, _):
        await ctx.race([ctx.wait_for("x"), ctx.sleep("nap", 1)])
        return (await ctx.wait_for("x")) or ""

    await engine.start(updates, "updates", None, id_policy={"running": "use-existing"})
    up = engine.run("updates")
    a = await up.update("add", 3, id="u1")
    b = await up.update("add", 3, id="u1")
    try:
        await up.update("add", -1)
        rejected = False
    except Exception:  # noqa: BLE001
        rejected = True
    total = await up.query("total")
    assert a == 6 and b == 6 and rejected and total == 6, (a, b, rejected, total)
    await up.signal("done", None)
    _, r = await engine.update_with_start(update_start, "update_start", 10, "inc", 5)
    assert r == 15, r
    await engine.run("update_start").signal("stop", None)
    await engine.start(heartbeat, "heartbeat", None)
    await engine.start(await_, "await", None)
    while "t" not in token:
        await asyncio.sleep(0.001)
    await until(engine, "await", ("suspended", "parked"))
    await engine.complete(token["t"], "approved")
    await engine.start(continued, "continued", 2)
    await engine.start(patched, "patched", None)
    await engine.start(attributes, "attributes", None)
    await engine.start(parent_close, "parent_close", None)
    await until(engine, "parent_close/terminate", ("suspended", "parked"))
    await until(engine, "parent_close/abandon", ("suspended", "parked"))
    await engine.run("parent_close").terminate("stop")
    await until(engine, "parent_close/terminate", ("terminated",))
    await engine.run("parent_close/abandon").signal("go", "free")
    await engine.start(reset, "reset", None)
    await engine.run("reset").result()
    await engine.run("reset").reset("b")
    await engine.start(victim, "victim", None)
    await until(engine, "victim", ("suspended", "parked"))
    await engine.start(canceller, "canceller", None)
    await engine.start(race_wait, "race_wait", None)
    for _ in range(2000):
        if len(await journal(engine, "race_wait")) >= 6:
            break
        await asyncio.sleep(0.001)
    await engine.run("race_wait").signal("x", "late")
    for id in ["updates", "update_start", "heartbeat", "await", "continued", "patched", "attributes",
               "parent_close", "parent_close/terminate", "parent_close/abandon", "reset", "victim",
               "canceller", "race_wait"]:
        try:
            await engine.run(id).result()
        except Exception:  # noqa: BLE001 - failed/cancelled runs still have journals
            pass
        out[id] = await journal(engine, id)


async def streams(engine, out, r3):
    """A workflow's own stream, a step's stream (JSON and bytes chunks), and
    a step whose retry continues after what its failed attempt recorded."""

    @on(engine, "streams")
    async def streams_wf(ctx, _):
        p = ctx.stream("progress")
        p.append("start")

        def generate(s):
            o = s.stream("tokens")
            for t in ["a", "b", "c"]:
                o.append(t)
            o.append(bytes([1, 2]))
            return "abc"

        text = await ctx.step("generate", generate)
        p.append({"text": text})
        p.close()
        return text

    @on(engine, "stream_retry")
    async def stream_retry(ctx, _):
        def generate(s):
            o = s.stream("tokens")
            prev = o.recorded()
            if not prev:
                o.append("x")
                o.append("y")
                raise Exception("cut off")
            o.append("z")
            return len(prev) + 1

        return await ctx.step("generate", generate, retry=r3)

    for wf, id in ((streams_wf, "streams"), (stream_retry, "stream_retry")):
        await engine.start(wf, id, None)
        try:
            await engine.run(id).result()
        except Exception:  # noqa: BLE001
            pass
        out[id] = await journal(engine, id)
    r = engine.run("streams").stream("progress")
    chunks = [c async for c in r]
    out["streams/read"] = {"chunks": chunks, "status": r.status}


async def objects(out):
    """Keyed objects: see ``objects`` in crates/fusee/examples/conformance.rs."""
    cart = fusee.object("conf_cart")

    @cart.exclusive
    async def add(ctx, item):
        priced = await ctx.step("price", lambda: f"{item}$")
        items = ctx.state.get("items", [])
        items.append(priced)
        ctx.state.set("items", items)
        ctx.state.set("last", items[-1])
        return len(items)

    @cart.exclusive
    async def remove(ctx, item):
        items = ctx.state.get("items", [])
        ctx.state.set("touched", True)
        if item not in items:
            raise FuseeError(f"missing {item}")
        items.remove(item)
        ctx.state.set("items", items)
        return len(items)

    @cart.shared
    async def get(ctx, _):
        return {"items": ctx.state.get("items"), "keys": ctx.state.keys()}

    @cart.exclusive
    async def clear(ctx, _):
        ctx.state.clear_all()
        return len(ctx.state.keys())

    async def tally_add(ctx, n):
        s = ctx.state.get("sum", 0) + n
        ctx.state.set("sum", s)
        return s

    tally = fusee.object("conf_tally", {"add": tally_add})

    @fusee.workflow("obj_caller")
    async def obj_caller(ctx, key):
        n = await ctx.object("conf_cart", key).call("add", "d")
        ctx.object("conf_cart", key).send("add", "e")
        return n

    engine = await Engine.open(durability="memory", shards=2, keep_history=True, park_after=3_600_000,
                               workflows=[obj_caller], objects=[cart, tally])
    k = engine.object("conf_cart", "k1")
    calls = [await k.call("add", "a", id="c1"), await k.call("add", "b", id="c2"),
             await k.call("add", "b", id="c2")]
    try:
        calls.append(await k.call("remove", "zz", id="c3"))
    except FuseeError:
        calls.append({"error": True})
    calls.append(await k.call("get", None, id="c4"))
    await k.send("add", "c", id="s1")
    run = await engine.start(obj_caller, "obj_caller", "k1")
    calls.append(await run.result())
    for _ in range(2000):
        if len((await k.state()).get("items", [])) == 5:
            break
        await asyncio.sleep(0.001)
    calls.append(await k.call("get", None, id="c5"))
    calls.append(await k.call("clear", None, id="c6"))
    calls.append(await k.call("add", "f", id="c7"))
    calls.append(await k.call("fusee.state.edit", {"set": {"owner": "ana"},
                                                   "patch": [{"op": "add", "path": "/items/-", "value": "g"}]}, id="c8"))
    calls.append(await k.state())
    out["obj/calls"] = calls
    out["obj/cart"] = (await k.describe()).journal
    out["obj/caller"] = await journal(engine, "obj_caller")
    t = engine.object("conf_tally", "t")
    sums = [await t.call("add", n, id=f"t-{n}") for n in range(1, 131)]
    sums.append(await t.call("add", 5, id="t-5"))
    out["obj/sums"] = [sums[0], sums[127], sums[129], sums[130]]
    out["obj/tally_state"] = await t.state()
    out["obj/tally"] = (await t.describe()).journal
    await engine.close()


async def pause(engine, out):
    """Pause: see ``pause`` in crates/fusee/examples/conformance.rs."""
    calls = [0]

    @on(engine, "pause")
    async def paused(ctx, _):
        def flaky():
            calls[0] += 1
            if calls[0] < 3:
                raise Exception(f"try {calls[0]}")
            return "ok"

        return await ctx.step("flaky", flaky, retry=Retry(attempts=2, initial=1, max=1, on_exhausted="pause"))

    await engine.start(paused, "pause", None)
    for _ in range(4000):
        if (await engine.run("pause").status()).status == "paused":
            break
        await asyncio.sleep(0.001)
    await engine.run("pause").resume()
    try:
        await engine.run("pause").result()
    except FuseeError:
        pass
    out["pause"] = (await engine.run("pause").describe()).journal


async def queues(engine, out):
    """Queues: see ``queues`` in crates/fusee/examples/conformance.rs."""
    engine.limit("conf", concurrent=1)

    @on(engine, "prio")
    async def prio(ctx, p):
        a = await ctx.step("first", lambda: p["level"], limit="conf", priority=p["level"],
                           fairness_key=p["key"], fairness_weight=p["weight"])
        return await ctx.step("second", lambda: f"{p['key']}:{a}", limit="conf", priority=-p["level"],
                              fairness_key="other")

    @on(engine, "deb_target")
    async def deb(ctx, v):
        return await ctx.step("got", lambda: v)

    @on(engine, "batch_target")
    async def bat(ctx, events):
        return await ctx.step("got", lambda: events)

    ids = []
    for i in range(4):
        await engine.start(prio, f"prio/{i}", {"level": i, "key": "odd" if i % 2 else "even", "weight": i + 1})
        ids.append(f"prio/{i}")
    a, b, c = await asyncio.gather(*(engine.debounce(deb, "k", n, window=500, event_id=f"d{n}") for n in (1, 2, 3)))
    again = await engine.debounce(deb, "k", 99, window=500, event_id="d2")
    assert all(r.id == a.id for r in (b, c, again)), "debounced starts joined different runs"
    await a.started()
    await a.result()
    out["debounce/run"] = await journal(engine, a.id)
    runs = []
    for eid, e in (("b1", "a"), ("b2", "b"), ("b2", "x"), ("b3", "c")):
        runs.append(await engine.batch(bat, "k", e, max_events=3, max_wait="60s", event_id=eid))
    assert all(r.id == runs[0].id for r in runs), "batched events joined different runs"
    await runs[0].started()
    await runs[0].result()
    out["batch/full"] = await journal(engine, runs[0].id)
    timed = await engine.batch(bat, "t", "z", max_events=100, max_wait=20)
    await timed.started()
    await timed.result()
    out["batch/timed"] = await journal(engine, timed.id)
    out["debounce/describe"] = await engine.run("fusee.debounce/deb_target/k").query("describe")
    out["batch/describe"] = await engine.run("fusee.batch/batch_target/k").query("describe")
    for id in ids:
        await engine.run(id).result()
        out[id] = await journal(engine, id)


asyncio.run(main())
