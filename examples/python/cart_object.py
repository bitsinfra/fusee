"""A cart object in Python: one object per user, with its own state; calls
to a key take turns, and a call sent again with the same ID runs once."""
import asyncio

import fusee
from fusee import Engine

cart = fusee.object("cart")


@cart.exclusive
async def add(ctx, item: dict) -> int:
    items = ctx.state.get("items") or []
    items.append(item)
    ctx.state.set("items", items)
    return len(items)


@cart.shared
async def total(ctx, _=None) -> int:
    return sum(i["cents"] for i in ctx.state.get("items") or [])


@cart.exclusive
async def checkout(ctx, _=None) -> str:
    items = ctx.state.get("items") or []
    cents = sum(i["cents"] for i in items)
    receipt = await ctx.step("charge", lambda s: f"charged {cents} cents for {len(items)} items")
    ctx.state.clear("items")
    return receipt


async def main():
    async with await Engine.open("./data/cart-object", objects=[cart]) as engine:
        user = engine.object(cart, "user-7")
        await user.call("add", {"sku": "tea", "cents": 450}, id="r1")
        print("items:", await user.call("add", {"sku": "tea", "cents": 450}, id="r1"))  # sent again: once
        print("items:", await user.call("add", {"sku": "milk", "cents": 120}, id="r2"))
        print("total:", await user.call("total"))
        print("receipt:", await user.call("checkout", id="c1"))


asyncio.run(main())
