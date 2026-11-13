"""Payloads over the payload offload threshold (64 KiB by default) go to the
shared log's blob area: a 1 MiB step result and a 1 MiB signal round trip
exactly, before and after a reopen."""
import asyncio
import os
import shutil
import tempfile

from fusee import Engine
from _helpers import journal, on, status

MIB = 1 << 20


def page(seed, n):
    x = seed | 1
    out = bytearray()
    while len(out) < n:
        x = (x * 1103515245 + 12345) & 0xFFFFFFFF
        out.append(97 + x % 26)
    return out.decode()


async def round_trip():
    d = tempfile.mkdtemp(prefix="fusee-py-offload-")
    fetched = []

    def register(engine):
        @on(engine, "big")
        async def big(ctx, n):
            def fetch():
                fetched.append(1)
                return page(7, n)

            body = await ctx.step("fetch", fetch)
            note = await ctx.wait_for("note")
            return {"body": len(body), "same": body == page(7, n), "note": len(note),
                    "note_same": note == page(9, MIB)}

        return big

    engine = await Engine.open(d, flush="fsync", shards=2)
    wf = register(engine)
    run = await engine.start(wf, "a", MIB)
    # A second run stays suspended across the reopen, its result recorded.
    await engine.start(wf, "b", MIB)
    await run.signal("note", page(9, MIB))
    want = {"body": MIB, "same": True, "note": MIB, "note_same": True}
    assert await run.result() == want
    blobs = os.path.join(d, "wal", "blobs")
    assert os.path.isdir(blobs) and os.listdir(blobs), "the blob area was written"
    await engine.close()

    engine = await Engine.open(d, flush="fsync", shards=2)
    register(engine)
    assert await engine.run("a").result() == want
    b = engine.run("b")
    await b.signal("note", page(9, MIB))
    assert await b.result() == want
    assert len(fetched) == 2, "recorded results replay, steps do not run again"
    await engine.close()
    shutil.rmtree(d, ignore_errors=True)


def test_large_payloads_round_trip():
    asyncio.run(round_trip())


if __name__ == "__main__":
    test_large_payloads_round_trip()
    print("ok test_large_payloads_round_trip")
