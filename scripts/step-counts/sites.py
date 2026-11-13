"""python3 sites.py <package parent dir> <out>: allocation sites of a
2000-step chain (DYLD_INSERT_LIBRARIES=sitemalloc.dylib)."""
import asyncio, ctypes, sys
sys.path.insert(0, sys.argv[1])
import fusee
from fusee import Engine
lib = ctypes.CDLL(None)
async def body(ctx, k):
    for i in range(k):
        await ctx.step("noop", lambda: i)
    return k
async def main():
    if hasattr(fusee, "workflow"):
        chain = fusee.workflow("chain", body)
        e = await Engine.open(durability="memory", workflows=[chain]); run = lambda i, k: e.start(chain, i, k)
    else:
        e = await Engine.open(durability="memory"); chain = e.workflow("chain")(body); run = lambda i, k: chain.start(i, k)
    await (await run("w", 50)).result()
    lib.sitemalloc_on(1)
    await (await run("s", 2000)).result()
    lib.sitemalloc_on(0)
    await e.close()
    lib.sitemalloc_dump(sys.argv[2].encode())
asyncio.run(main())
