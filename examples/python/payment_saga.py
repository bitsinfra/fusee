"""A payment saga in Python."""
import asyncio

import fusee
from fusee import CancelledError, Engine, Retry

shipped = fusee.signal("shipped", bool)


def short(key: str) -> str:
    """A short ID made from a step's idempotency key (as a service would): FNV-1a."""
    h = 0x811C9DC5
    for b in key.encode():
        h = ((h ^ b) * 0x01000193) & 0xFFFFFFFF
    return f"{h:08x}"


async def reserve(sku, key):
    return f"res-{sku}-{short(key)}"


class CardDeclined(fusee.Fatal):
    """Final: a declined card is not retried."""

    code = "card_declined"


async def charge(cents, key):
    if cents > 1_000_000:
        raise CardDeclined("card declined")
    return f"ch-{short(key)}"


@fusee.workflow("checkout")
async def checkout(ctx: fusee.Ctx, order: dict) -> str:
    reservation = await ctx.step("reserve", lambda s: reserve(order["sku"], s.key))
    try:
        charge_id = await ctx.step("charge", lambda s: charge(order["cents"], s.key),
                                   retry=Retry(attempts=5, initial="200ms", max="1m"))
    except fusee.StepError:
        await ctx.step("release", lambda: print("  released", reservation))
        raise
    try:
        ok = await ctx.wait_for(shipped, timeout="1d")
    except CancelledError:
        ok = False
    if ok:
        return charge_id
    await ctx.step("refund", lambda: print("  refunded", charge_id))
    await ctx.step("release", lambda: print("  released", reservation))
    raise CancelledError("not shipped; refunded")


async def main():
    async with await Engine.open("./data/payment-saga", workflows=[checkout]) as engine:
        run = await engine.start(checkout, "order-123", {"sku": "SKU-9", "cents": 4200},
                                 id_policy={"running": "use-existing", "finished": "use-existing"})
        await run.signal(shipped, True)
        print("charged:", await run.result())


asyncio.run(main())
