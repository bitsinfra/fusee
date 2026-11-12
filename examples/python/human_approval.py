"""Human approval in Python: a workflow that asks a person, waits up to
three days for the answer (durably: a crash meanwhile loses nothing), and
acts on it."""
import asyncio

import fusee
from fusee import Engine

decision = fusee.signal("decision", dict)


@fusee.workflow("expense")
async def expense(ctx: fusee.Ctx, claim: dict) -> str:
    await ctx.step("ask", lambda: print(f"  asked finance to approve {claim['cents']} cents for {claim['by']}"))
    answer = await ctx.wait_for(decision, timeout="3d")
    if answer is None:
        return "expired: nobody answered in three days"
    if not answer["approved"]:
        return f"rejected by {answer['by']}"
    paid = await ctx.step("pay", lambda: f"paid {claim['cents']} cents")
    return f"{paid} (approved by {answer['by']})"


async def main():
    async with await Engine.open("./data/human-approval", workflows=[expense]) as engine:
        run = await engine.start(expense, "expense-7", {"by": "ana", "cents": 12000},
                                 id_policy={"running": "use-existing", "finished": "use-existing"})
        await run.signal(decision, {"approved": True, "by": "dana"})
        print("result:", await run.result())


asyncio.run(main())
