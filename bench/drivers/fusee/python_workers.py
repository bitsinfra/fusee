"""Python on several cores: worker processes of one server, over shared memory.

Each worker is a Python process that opens its engine in server mode with
`unix:<socket>?shm=1&max-shards=<k>`: the server leases it `k` of the
shards, it runs their workflows on its own interpreter and asyncio loop,
and every worker appends to the server's one log through a shared-memory
ring. A shard's workflows run where its lease is; a start or a result for
another worker's shard goes through the server.

    python3 python_workers.py <dir> <workers,...> [chains per worker] [steps]

For each worker count it starts `fusee server` (`FUSEE_CLI`, default
target/release/fusee) with one shard per worker in a fresh directory,
starts the workers, and has each worker run `chains` chains of `steps`
no-op steps on its own shards, all at once (closed loop). One JSON line
per worker count: steps per second in all and per worker process, and the
step-to-step gap inside each chain (p50, p99). The gap is timed inside the
workers; the rate counts from the first start to the last result.

    python3 python_workers.py worker <address> <ready-file>

is one worker process (used by the above).
"""
import asyncio
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def pct(v, q):
    return v[min(len(v) - 1, int(round((len(v) - 1) * q)))] if v else None


async def worker(address, ready):
    import fusee
    from fusee import Engine
    e = await Engine.open(durability="server", address=address)
    gaps = []

    @fusee.workflow("chain")
    async def chain(ctx, n):
        marks = []
        value = 0

        def inc():
            a = time.perf_counter_ns()
            r = value + 1
            marks.append((a, time.perf_counter_ns()))
            return r

        for _ in range(n):
            value = await ctx.step("noop", inc)
        gaps.extend((b[0] - a[1]) / 1000 for a, b in zip(marks, marks[1:]))
        return value

    # A child on another worker's shard: the start and the result go through
    # the server's relay.
    @fusee.workflow("child")
    async def child(ctx, n):
        return await ctx.step("triple", lambda: n * 3)

    @fusee.workflow("parent")
    async def parent(ctx, kid):
        return await ctx.child("child", kid, 5) + 1

    @fusee.workflow("gaps")
    async def report(ctx, _):
        # The gaps this process measured, handed back through a step (the
        # client reads the workflow's result).
        out = sorted(gaps)
        gaps.clear()
        return await ctx.step("gaps", lambda: {"count": len(out), "p50": pct(out, .5), "p99": pct(out, .99),
                                               "pid": os.getpid()})

    e.register(chain, child, parent, report)
    stop = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    Path(ready).write_text("ready")
    await stop.wait()
    await e.close()


def shard_of(id_, shards):
    """`fusee_core::shard::shard_of`: FNV-1a, then a 64-bit finalizer."""
    m = (1 << 64) - 1
    h = 0xcbf29ce484222325
    for b in id_.encode():
        h = ((h ^ b) * 0x100000001b3) & m
    h ^= h >> 33
    h = (h * 0xff51afd7ed558ccd) & m
    h ^= h >> 33
    h = (h * 0xc4ceb9fe1a85ec53) & m
    return (h ^ (h >> 33)) % max(1, shards)


async def trial(base, workers, chains, steps):
    from fusee import Client
    cli = os.environ.get("FUSEE_CLI", str(ROOT / "target/release/fusee"))
    directory = Path(base) / f"w{workers}"
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)
    # Socket paths are short (SUN_LEN): not under the data directory.
    sock = Path("/tmp") / f"fusee-pyw-{os.getpid()}-{workers}.sock"
    sock.unlink(missing_ok=True)
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    flush = ["--flush", os.environ["FUSEE_SERVER_FLUSH"]] if os.environ.get("FUSEE_SERVER_FLUSH") else []
    server = subprocess.Popen([cli, "server", "--dir", str(directory / "data"), "--unix", str(sock),
                               "--listen", f"127.0.0.1:{port}", "--shards", str(workers), *flush],
                              stdout=subprocess.DEVNULL)
    procs = []
    try:
        for _ in range(200):
            if sock.exists():
                break
            await asyncio.sleep(0.05)
        address = f"unix:{sock}?shm=1&max-shards=1"
        env = dict(os.environ)
        for w in range(workers):
            ready = directory / f"ready-{w}"
            procs.append((subprocess.Popen([sys.executable, __file__, "worker", address, str(ready)], env=env), ready))
            # One at a time, so each takes its own shard.
            for _ in range(600):
                if ready.exists():
                    break
                await asyncio.sleep(0.05)
            else:
                raise RuntimeError(f"worker {w} did not take a shard")
        c = await Client.connect(f"unix:{sock}")
        try:
            # IDs by shard, so each worker runs `chains` of them.
            by_shard = {s: [] for s in range(workers)}
            k = 0
            while any(len(v) < chains for v in by_shard.values()):
                rid = f"c-{k}"
                k += 1
                s = shard_of(rid, workers)
                if len(by_shard[s]) < chains:
                    by_shard[s].append(rid)
            warm = [f"warm-{s}-{i}" for s in range(workers) for i in range(2)]
            await asyncio.gather(*[(await c.start("chain", w, 50)).result() for w in warm])
            ids = [i for v in by_shard.values() for i in v]
            t0 = time.perf_counter()
            runs = [await c.start("chain", i, steps) for i in ids]
            await asyncio.gather(*[r.result() for r in runs])
            seconds = time.perf_counter() - t0
            # Each worker's parent waits for a child another worker runs.
            relayed = 0
            if workers > 1:
                for s in range(workers):
                    parent = next(f"p-{s}-{j}" for j in range(10_000) if shard_of(f"p-{s}-{j}", workers) == s)
                    kid = next(f"k-{s}-{j}" for j in range(10_000)
                               if shard_of(f"k-{s}-{j}", workers) == (s + 1) % workers)
                    if await (await c.start("parent", parent, kid)).result() != 16:
                        raise RuntimeError("a child on another worker gave the wrong result")
                    relayed += 1
            reports = []
            for s in range(workers):
                rid = next(f"gaps-{s}-{j}" for j in range(10_000) if shard_of(f"gaps-{s}-{j}", workers) == s)
                reports.append(await (await c.start("gaps", rid, None)).result())
        finally:
            c.close()
        total = len(ids) * steps
        return {"workers": workers, "chains_per_worker": chains, "steps": total, "seconds": seconds,
                "rate": total / seconds, "rate_per_worker": total / seconds / workers,
                "children_across_workers": relayed,
                "gap_p50_us": [r["p50"] for r in reports], "gap_p99_us": [r["p99"] for r in reports],
                "pids": sorted({r["pid"] for r in reports})}
    finally:
        for p, _ in procs:
            p.send_signal(signal.SIGTERM)
        for p, _ in procs:
            try:
                p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                p.kill()
        server.send_signal(signal.SIGTERM)
        try:
            server.wait(timeout=20)
        except subprocess.TimeoutExpired:
            server.kill()
        shutil.rmtree(directory, ignore_errors=True)
        sock.unlink(missing_ok=True)


async def main(base, counts, chains, steps):
    for w in counts:
        r = await trial(base, w, chains, steps)
        r["machine"] = {"os": platform.system(), "arch": platform.machine(), "cores": os.cpu_count(),
                        "host_load": os.getloadavg()}
        print(json.dumps(r), flush=True)


if __name__ == "__main__":
    if sys.argv[1] == "worker":
        asyncio.run(worker(sys.argv[2], sys.argv[3]))
    else:
        counts = [int(x) for x in sys.argv[2].split(",")]
        asyncio.run(main(sys.argv[1], counts, int(sys.argv[3]) if len(sys.argv) > 3 else 16,
                         int(sys.argv[4]) if len(sys.argv) > 4 else 2000))
