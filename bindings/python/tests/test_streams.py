"""Durable streams in the Python SDK: chunk order and completeness, a reader
that reconnects at its offset, a restart in the middle of a streaming step,
replays that append nothing, bytes chunks."""

import asyncio
import shutil
import tempfile
import time

from fusee import Engine, FuseeError
from _helpers import journal, on, status


def token(i):
    return f"t{i} "


async def read_all(run, name, start=0):
    r = run.stream(name, from_=start)
    chunks = [c async for c in r]
    return chunks, r.status, r.offset


async def order_and_end():
    engine = await Engine.open(durability="memory")
    try:
        @on(engine, "llm")
        async def llm(ctx, n):
            async def generate(s):
                out = s.stream("tokens")
                text = ""
                for i in range(n):
                    out.append(token(i))
                    text += token(i)
                    if i % 25 == 0:
                        await asyncio.sleep(0.001)
                return text

            return await ctx.step("generate", generate)

        run = await engine.start(llm, "q-1", 300)
        chunks, status, _ = await read_all(run, "tokens")
        assert len(chunks) == 300
        assert status == "ok"
        assert "".join(chunks) == await run.result()
        tail, _, offset = await read_all(run, "tokens", 250)
        assert tail == chunks[250:]
        assert offset == 300
        r = run.stream("tokens")
        async for _ in r:
            if r.offset == 10:
                break
        assert r.offset == 10
    finally:
        await engine.close()


async def bytes_and_long_poll():
    engine = await Engine.open(durability="memory")
    try:
        @on(engine, "events")
        async def events(ctx, _):
            out = ctx.stream("events")
            out.append({"stage": "start"})
            out.append(bytes([0, 1, 2, 255]))
            await ctx.wait_for("go")
            out.append("after")
            out.close()
            await ctx.wait_for("finish")

        run = await engine.start(events, "e-1", None)
        r = run.stream("events", wait=50)
        it = r.__aiter__()
        assert await it.__anext__() == {"stage": "start"}
        assert await it.__anext__() == bytes([0, 1, 2, 255])
        pending = asyncio.ensure_future(it.__anext__())
        await asyncio.sleep(0.15)
        assert not pending.done(), "the read waits for the next chunk"
        await run.signal("go", 1)
        assert await pending == "after"
        try:
            await it.__anext__()
            raise AssertionError("the stream should have ended")
        except StopAsyncIteration:
            pass
        assert r.status == "ok"
        await run.signal("finish", 1)
        await run.result()
        try:
            await read_all(engine.run("nobody"), "events")
            raise AssertionError("expected NotFound")
        except FuseeError as e:
            assert e.code == "NotFound", e
    finally:
        await engine.close()


async def restart_mid_stream():
    d = tempfile.mkdtemp(prefix="fusee-py-streams-")
    resumed_at = []
    # Tokens each attempt generated (the first one's code may run on after
    # its engine closed, and its appends then fail).
    generated = []
    n = 40

    def register(engine):
        @on(engine, "llm")
        async def llm(ctx, n):
            p = ctx.stream("progress")
            p.append("started")

            async def generate(s):
                out = s.stream("tokens")
                prefix = out.recorded()
                resumed_at.append(len(prefix))
                generated.append(0)
                attempt = len(generated) - 1
                text = "".join(prefix)
                for i in range(len(prefix), n):
                    generated[attempt] += 1
                    out.append(token(i))
                    text += token(i)
                    await asyncio.sleep(0.002)
                return text

            text = await ctx.step("generate", generate)
            p.append("done")
            return text

        return llm

    seen = []
    try:
        engine = await Engine.open(dir=d)
        llm = register(engine)
        await engine.start(llm, "c-1", n)
        r = engine.run("c-1").stream("tokens")
        async for c in r:
            seen.append(c)
            if len(seen) == 15:
                break
        # The process stops in the middle of the step.
        await engine.close()
        engine = await Engine.open(dir=d)
        register(engine)
        rest, status, _ = await read_all(engine.run("c-1"), "tokens", r.offset)
        assert status == "ok"
        seen += rest
        expected = [token(i) for i in range(n)]
        assert seen == expected, "no token is missing, repeated or changed"
        assert await engine.run("c-1").result() == "".join(expected)
        assert len(resumed_at) == 2 and resumed_at[1] >= 15, resumed_at
        assert generated[1] == n - resumed_at[1], "no recorded token was generated again"
        progress, _, _ = await read_all(engine.run("c-1"), "progress")
        assert progress == ["started", "done"]
        await engine.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


async def append_cost():
    engine = await Engine.open(durability="memory")
    try:
        n = 100_000

        @on(engine, "bulk")
        async def bulk(ctx, n):
            def generate(s):
                out = s.stream("tokens")
                t = time.perf_counter()
                for i in range(n):
                    out.append(i)
                return time.perf_counter() - t

            return await ctx.step("generate", generate)

        run = await engine.start(bulk, "b-1", n)
        s = await run.result()
        t = time.perf_counter()
        count = 0
        async for c in run.stream("tokens"):
            assert c == count
            count += 1
        assert count == n
        print(f"python append: {n / s:.0f} chunks/s ({s * 1e9 / n:.0f} ns each); "
              f"read back {n / (time.perf_counter() - t):.0f} chunks/s")
    finally:
        await engine.close()


def test_order_and_end():
    asyncio.run(order_and_end())


def test_bytes_and_long_poll():
    asyncio.run(bytes_and_long_poll())


def test_restart_mid_stream():
    asyncio.run(restart_mid_stream())


def test_append_cost():
    asyncio.run(append_cost())


if __name__ == "__main__":
    for t in (test_order_and_end, test_bytes_and_long_poll, test_restart_mid_stream, test_append_cost):
        t()
        print("ok", t.__name__)
