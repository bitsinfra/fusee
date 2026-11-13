"""Per-step overhead of a sequential no-op chain (closed loop)."""
import asyncio, os, shutil, sys, time
import fusee
from fusee import Engine

stamps = []


@fusee.workflow("chain")
async def chain(ctx, n):
    for i in range(n):
        await ctx.step("noop", lambda: stamps.append(time.perf_counter()) or i)
    return n


async def main(mode):
    d = os.path.join(os.environ.get("TMPDIR", "/tmp"), "fusee-py-bench")
    shutil.rmtree(d, ignore_errors=True)
    if mode == "memory":
        engine = await Engine.open(durability="memory", workflows=[chain])
    else:
        engine = await Engine.open(d, flush="fsync" if mode == "fsync" else None, workflows=[chain])
    n = 3000 if mode != "local" else 300
    await (await engine.start(chain, "solo", n)).result()
    gaps = sorted((b - a) * 1e6 for a, b in zip(stamps, stamps[1:]))
    p = lambda q: gaps[int((len(gaps) - 1) * q)]
    print(f"python {mode}: step-to-step p50 {p(0.5):.1f} µs  p99 {p(0.99):.1f} µs ({n} steps)")
    t0 = time.perf_counter()
    runs = [await engine.start(chain, f"w{i}", 20) for i in range(1000)]
    await asyncio.gather(*(r.result() for r in runs))
    el = time.perf_counter() - t0
    print(f"  20000 durable steps in {el:.2f} s = {20000/el:.0f} steps/s")
    await engine.close()

asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "memory"))
