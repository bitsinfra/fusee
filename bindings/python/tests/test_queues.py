"""Queues in the Python SDK: priorities and fairness keys on limits,
debounced starts and batches."""

import asyncio
import re
import shutil
import tempfile
import time

from fusee import Engine, FuseeError, RejectedError
from _helpers import journal, on, status


def tmp():
    return tempfile.mkdtemp(prefix="fusee-py-queues-")


async def eventually(what, f):
    for _ in range(2000):
        if f():
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"timed out waiting for {what}")


async def serve_order(asks):
    """Holds the one permit of ``one``, queues a job per ask ``(level, key,
    weight)`` in that order, then lets them through: the order they ran in."""
    engine = await Engine.open(durability="memory")
    try:
        engine.limit("one", concurrent=1)
        gate = asyncio.get_running_loop().create_future()
        served = []

        @on(engine, "hold")
        async def hold(ctx, _):
            async def wait():
                await gate

            return await ctx.step("hold", wait, limit="one")

        @on(engine, "job")
        async def job(ctx, ask):
            level, key, weight = ask
            return await ctx.step("work", lambda: served.append(f"{key}@{level}"), limit="one",
                                  priority=level, fairness_key=key, fairness_weight=weight)

        blocker = await engine.start(hold, "hold")
        await eventually("the blocker's permit", lambda: engine.limit_usage("one")[0] == 1)
        # Each start waits until its step is queued, so the arrival order is fixed.
        runs = []
        for i, ask in enumerate(asks):
            runs.append(await engine.start(job, f"job-{i}", list(ask)))
            await eventually("the step to queue", lambda: engine.limit_usage("one")[1] == i + 1)
        gate.set_result(None)
        await blocker.result()
        for r in runs:
            await r.result()
        assert engine.limit_usage("one")[0] == 0
        assert engine.limit_usage("one")[1] == 0
        return served
    finally:
        await engine.close()


async def priority_then_fairness():
    # Tenant "big" queues six steps, then "a" and "b" two each, then one
    # urgent step: the deep key gets one permit per round, like the others.
    asks = [(0, "big", 0)] * 6 + [(0, "a", 0), (0, "b", 0), (0, "a", 0), (0, "b", 0), (5, "urgent", 0)]
    assert await serve_order(asks) == [
        "urgent@5", "big@0", "a@0", "b@0", "big@0", "a@0", "b@0", "big@0", "big@0", "big@0", "big@0",
    ]


async def fairness_weight():
    asks = [(0, "w2", 2), (0, "w1", 1)] * 4
    assert await serve_order(asks) == ["w2@0", "w2@0", "w1@0", "w2@0", "w2@0", "w1@0", "w1@0", "w1@0"]


async def bad_priorities():
    engine = await Engine.open(durability="memory")
    try:
        @on(engine, "bad")
        async def bad(ctx, opts):
            return await ctx.step("s", lambda: 1, limit="one", **opts)

        cases = (({"priority": 1.5}, "priority"), ({"priority": 2**31}, "priority"),
                 ({"priority": True}, "priority"), ({"fairness_weight": -1}, "fairness_weight"))
        for i, (opts, problem) in enumerate(cases):
            try:
                await (await engine.start(bad, f"bad-{i}", opts)).result()
                raise AssertionError(f"{opts} was taken")
            except FuseeError as e:
                assert problem in str(e), e
        assert await (await engine.start(bad, "ok", {"priority": -7, "fairness_key": "t", "fairness_weight": 3})).result() == 1
    finally:
        await engine.close()


async def debounce_collapse_and_max_wait():
    engine = await Engine.open(durability="memory", keep_history=True)
    try:
        starts = []

        @on(engine, "reindex")
        async def reindex(ctx, n):
            starts.append(n)
            return n

        a, b, other, c = await asyncio.gather(
            engine.debounce(reindex, "doc-1", 1, window=300),
            engine.debounce(reindex, "doc-1", 2, window=300),
            # Another key has its own window.
            engine.debounce(reindex, "doc-2", 20, window=300),
            engine.debounce(reindex, "doc-1", 3, window=300),
        )
        assert a.id == b.id == c.id
        assert a.id != other.id
        assert re.fullmatch(r"doc-1@[0-9a-f-]{36}", a.id), a.id
        # Nothing runs until the window closes.
        try:
            await a.result()
            raise AssertionError("the run existed before its window closed")
        except FuseeError as e:
            assert e.code == "NotFound", e
        await a.started()
        assert await a.result() == 3
        await other.started()
        assert await other.result() == 20
        assert len(starts) == 2
        # The run is a child the coordinator abandoned, with the latest input.
        first = (await a.describe()).journal[0]
        assert first["child"] is True and first["input"] == 3, first
        # After the window closed, a start opens a new window and a new run.
        d = await engine.debounce(reindex, "doc-1", 4, window=300)
        assert d.id != a.id
        await d.started()
        assert await d.result() == 4
        # A maximum wait bounds a window that starts keep extending.
        ids = set()
        begin = time.monotonic()
        n = 100
        while time.monotonic() - begin < 0.7:
            ids.add((await engine.debounce(reindex, "busy", n, window=80, max_wait=200)).id)
            n += 1
            await asyncio.sleep(0.02)
        assert len(ids) >= 3, ids
        for id in ids:
            run = engine.run(id)
            await run.started()
            await run.result()
        try:
            await engine.debounce(reindex, "doc-1", 1, window=None)
            raise AssertionError("a debounce without a window was taken")
        except FuseeError as e:
            assert "window" in str(e), e
    finally:
        await engine.close()


async def batches_by_count_and_time():
    engine = await Engine.open(durability="memory")
    try:
        @on(engine, "bulk")
        async def bulk(ctx, events):
            return events

        handles = [await engine.batch(bulk, "tenant-1", i, max_events=3, max_wait=500) for i in range(7)]
        # A retried event with the same ID is added once.
        again = await engine.batch(bulk, "tenant-1", 7, max_events=3, max_wait=500, event_id="evt-7")
        dup = await engine.batch(bulk, "tenant-1", 7, max_events=3, max_wait=500, event_id="evt-7")
        assert again.id == dup.id
        ids = [h.id for h in handles]
        assert ids[0] == ids[2] != ids[3] == ids[5] != ids[6] == again.id, ids
        results = []
        for id in (ids[0], ids[3], ids[6]):
            run = engine.run(id)
            await run.started()
            results.append(await run.result())
        # Two closed by their count, the last by its wait.
        assert results == [[0, 1, 2], [3, 4, 5], [6, 7]], results
        # A rule the coordinator cannot use is refused before anything is sent.
        for bad in (0, 10_001):
            try:
                await engine.batch(bulk, "tenant-1", 1, max_events=bad)
                raise AssertionError(f"max_events={bad} was taken")
            except RejectedError:
                pass
        try:
            await engine.batch(bulk, "tenant-1", b"x")
            raise AssertionError("bytes were taken")
        except FuseeError as e:
            assert "JSON, not bytes" in str(e), e
    finally:
        await engine.close()


async def refused_as_it_ended():
    engine = await Engine.open(durability="memory")
    try:
        @on(engine, "bulk")
        async def bulk(ctx, events):
            return events

        # The refusal as it comes from this engine, and through a server.
        refusals = ["the workflow ended before the update finished",
                    "workflow failed: the workflow ended before the update finished"]
        send = engine._request_value
        calls = []

        async def flaky(f, value=True):
            calls.append(1)
            if len(calls) <= len(refusals):
                raise FuseeError(refusals[len(calls) - 1])
            return await send(f, value)

        engine._request_value = flaky
        run = await engine.batch(bulk, "k", "e", max_events=1)
        assert len(calls) == 3
        await run.started()
        assert await run.result() == ["e"]

        # Any other failure is the caller's.
        async def broken(f, value=True):
            raise FuseeError("disk on fire")

        engine._request_value = broken
        try:
            await engine.batch(bulk, "k", "f", max_events=1)
            raise AssertionError("the failure was swallowed")
        except FuseeError as e:
            assert "disk on fire" in str(e), e
        engine._request_value = send
    finally:
        await engine.close()


async def open_window_across_a_restart():
    d = tmp()

    def register(engine):
        @on(engine, "notify")
        async def notify(ctx, n):
            return await ctx.step("x10", lambda: n * 10)

        @on(engine, "bulk")
        async def bulk(ctx, events):
            return await ctx.step("all", lambda: events)

        return notify, bulk

    try:
        engine = await Engine.open(d, flush="fsync")
        notify, bulk = register(engine)
        first = await engine.debounce(notify, "user-9", 1, window=300, event_id="e-1")
        second = await engine.debounce(notify, "user-9", 2, window=300, event_id="e-2")
        assert first.id == second.id
        run_id = first.id
        batch_id = (await engine.batch(bulk, "t", "a", max_events=3, max_wait="1m", event_id="a")).id
        assert (await engine.batch(bulk, "t", "b", max_events=3, max_wait="1m", event_id="b")).id == batch_id
        # The process stops before either window closes.
        await engine.close()

        engine = await Engine.open(d, flush="fsync")
        notify, bulk = register(engine)
        # A retry of an acknowledged event joins the same window once.
        assert (await engine.debounce(notify, "user-9", 2, window=300, event_id="e-2")).id == run_id
        run = engine.run(run_id)
        await run.started()
        assert await run.result() == 20
        # Once fired, the same event ID still answers with its run.
        assert (await engine.debounce(notify, "user-9", 2, window=300, event_id="e-2")).id == run_id
        # The batch kept its two events: the third fills it.
        assert (await engine.batch(bulk, "t", "b", max_events=3, max_wait="1m", event_id="b")).id == batch_id
        filled = await engine.batch(bulk, "t", "c", max_events=3, max_wait="1m", event_id="c")
        assert filled.id == batch_id
        await filled.started()
        assert await filled.result() == ["a", "b", "c"]
        assert len([r async for r in engine.list(type="notify")]) == 1
        await engine.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_priority_then_fairness():
    asyncio.run(priority_then_fairness())


def test_fairness_weight():
    asyncio.run(fairness_weight())


def test_bad_priorities():
    asyncio.run(bad_priorities())


def test_debounce_collapse_and_max_wait():
    asyncio.run(debounce_collapse_and_max_wait())


def test_batches_by_count_and_time():
    asyncio.run(batches_by_count_and_time())


def test_refused_as_it_ended():
    asyncio.run(refused_as_it_ended())


def test_open_window_across_a_restart():
    asyncio.run(open_window_across_a_restart())


if __name__ == "__main__":
    for t in (
        test_priority_then_fairness,
        test_fairness_weight,
        test_bad_priorities,
        test_debounce_collapse_and_max_wait,
        test_batches_by_count_and_time,
        test_refused_as_it_ended,
        test_open_window_across_a_restart,
    ):
        t()
        print("ok", t.__name__)
