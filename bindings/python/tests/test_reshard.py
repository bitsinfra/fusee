"""A directory opened with another shard count is refused with a message
naming the command that changes it (`fusee reshard`); nothing reshards on
its own."""

import asyncio
import shutil
import tempfile

from fusee import Engine


async def another_count_is_refused():
    d = tempfile.mkdtemp(prefix="fusee-py-reshard-")
    try:
        engine = await Engine.open(d, flush="fsync", shards=2)
        await engine.close()
        try:
            await Engine.open(d, flush="fsync", shards=3)
        except Exception as e:  # noqa: BLE001 - the binding's error type
            assert "fusee reshard" in str(e), e
        else:
            raise AssertionError("opened with another shard count")
        again = await Engine.open(d, flush="fsync", shards=2)
        await again.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_another_count_is_refused():
    asyncio.run(another_count_is_refused())


if __name__ == "__main__":
    test_another_count_is_refused()
    print("ok", test_another_count_is_refused.__name__)
