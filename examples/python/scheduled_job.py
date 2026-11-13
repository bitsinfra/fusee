"""A scheduled job: a schedule starts the report workflow on a cron
expression; each scheduled time runs once, even across restarts."""
import asyncio

import fusee
from fusee import Engine

sent = []


@fusee.workflow("report")
async def report(ctx: fusee.Ctx, _) -> None:
    await ctx.step("send-report", lambda: print(f"  {ctx.workflow_id} sent"))
    sent.append(ctx.workflow_id)


async def main():
    async with await Engine.open("./data/scheduled-job", workflows=[report]) as engine:
        # A real job: {"cron": "0 2 * * *", "timezone": "Europe/London"}.
        schedule = await engine.schedule("nightly-report", {"every": "1s", "overlap": "skip"}, report)
        while len(sent) < 3:
            await asyncio.sleep(0.05)
        print("next:", (await schedule.describe())["nextMs"])
        await schedule.delete()


asyncio.run(main())
