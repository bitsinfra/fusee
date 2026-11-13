"""Edits of a key's state from outside its handlers, in the Python SDK."""

import asyncio
import shutil
import tempfile

import fusee
from fusee import Engine, FuseeError


def tmp():
    return tempfile.mkdtemp(prefix="fusee-py-edit-")


def cart():
    async def add(ctx, item):
        items = ctx.state.get("items") or []
        items.append(item)
        ctx.state.set("items", items)
        return len(items)

    async def items(ctx, _):
        return ctx.state.get("items") or []

    return fusee.object("cart", {"add": add, "items": fusee.shared(items)})


async def edits():
    d = tmp()
    carts = cart()
    engine = await Engine.open(d, objects=[carts])
    try:
        c = engine.object("cart", "u-1")
        assert await c.call("add", "apple") == 1
        assert (await c.set_state("owner", "ana"))["changed"] == ["owner"]
        await c.patch_state([{"op": "add", "path": "/items/-", "value": "pear"},
                             {"op": "test", "path": "/owner", "value": "ana"}])
        assert await c.state() == {"items": ["apple", "pear"], "owner": "ana"}
        assert await c.call("add", "fig") == 3
        try:
            await c.patch_state([{"op": "remove", "path": "/nope"}])
            raise AssertionError("a bad patch applied")
        except FuseeError as e:
            assert "JSON Patch operation 0" in str(e), e
        version = (await c.describe()).object["version"]
        ok = await c.clear_state("owner", if_version=version)
        assert ok["version"] != version
        try:
            await c.clear_state(if_version=version)
            raise AssertionError("a stale edit applied")
        except FuseeError as e:
            assert "changed since version" in str(e), e
        assert await c.call("items") == ["apple", "pear", "fig"]
    finally:
        await engine.close()
    engine = await Engine.open(d, objects=[carts])
    try:
        c = engine.object("cart", "u-1")
        assert await c.state() == {"items": ["apple", "pear", "fig"]}
        await c.clear_state()
        assert await c.state() == {}
    finally:
        await engine.close()
        shutil.rmtree(d, ignore_errors=True)


def test_edits():
    asyncio.run(edits())


if __name__ == "__main__":
    test_edits()
    print("ok test_edits")
