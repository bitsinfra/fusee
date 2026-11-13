"""A worker that exists only while there is work: a Fusee server invokes this
ASGI app when a shard nobody holds has work due, and each request opens a
fresh engine, runs the shard until it is idle, and closes it.

    fusee server --dir ./data/invoked --listen 127.0.0.1:7600 \\
      --invoke http://127.0.0.1:8080/fusee --invoke-secret-file ./secret
    FUSEE_INVOKE_SECRET="$(cat ./secret)" uvicorn invoked_worker:app --port 8080

On AWS Lambda, point the function's handler at ``invoked_worker.lambda_handler``.
Run this file directly to start a reminder through the server.
"""

import asyncio
import os

import fusee
from fusee import Client, invocation_handler

ADDRESS = os.environ.get("FUSEE_ADDRESS", "127.0.0.1:7600")


@fusee.workflow("reminder")
async def reminder(ctx: fusee.Ctx, args: dict) -> str:
    # Sleeps with no process up, then sends.
    await ctx.step("schedule", lambda: print(f"reminding {args['to']} in {args['after']}"))
    await ctx.sleep("wait", args["after"])
    return await ctx.step("send", lambda: f"sent {args['text']!r} to {args['to']}")


app = invocation_handler(ADDRESS, workflows=[reminder], secret=os.environ.get("FUSEE_INVOKE_SECRET"))
lambda_handler = app.lambda_handler


async def main():
    async with await Client.connect(ADDRESS) as client:
        run = await client.start(reminder, "remind-ana-1", {"to": "ana", "text": "stand-up", "after": "2s"})
        print(await run.result())


if __name__ == "__main__":
    asyncio.run(main())
