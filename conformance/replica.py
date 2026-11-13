"""Run mixed Rust-backed SDK replicas, kill the primary and verify the recovered journal."""
import asyncio
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


async def peer(folder, index, peers):
    import fusee
    from fusee import Engine
    dir = Path(folder)
    @fusee.workflow("mixed")
    async def mixed(ctx, n):
        stage = 0
        ctx.on_query("stage",lambda _: stage)
        def effect(s, value):
            with (dir/"effects").open("a") as log:
                log.write(f"{s.key} {value}\n")
            return value+1
        first = await ctx.step("first",lambda s: effect(s, n))
        stage = first
        await ctx.wait_for("go")
        return await ctx.step("second",lambda s: effect(s, first))
    e = await Engine.open(durability="replicated", replica_id=index, peers=peers.split(","),
                          dir=str(dir/f"node-{index}"), workflows=[mixed])
    if not (dir/"committed").exists():
        run = await e.start(mixed,"known",41,id_policy={"running":"use-existing"})
        while await run.query("stage")!=42:
            await asyncio.sleep(.005)
        (dir/"committed").write_text(str(index))
    else:
        run=e.run("known")
        await run.signal("go",True)
        (dir/"finished").write_text(json.dumps({"id":index,"result":await run.result()}))
    await asyncio.Event().wait()


def wait_file(path,procs,deadline):
    while time.monotonic()<deadline:
        if path.exists() and path.stat().st_size:
            return path.read_text()
        for p in procs:
            if p.poll() not in (None,-signal.SIGKILL):
                raise RuntimeError(f"peer exited unexpectedly: {p.returncode}")
        time.sleep(.025)
    raise TimeoutError(f"peers failed to produce {path.name}")


def trial(primary_language):
    with tempfile.TemporaryDirectory(prefix="fusee-sdk-vsr-") as tmp:
        root=Path(tmp)
        sockets=[socket.socket() for _ in range(3)]
        for sock in sockets:
            sock.bind(("127.0.0.1",0))
        endpoints=",".join(f"127.0.0.1:{s.getsockname()[1]}" for s in sockets)
        for sock in sockets:
            sock.close()
        py=[sys.executable,__file__]; node=["node",str(ROOT/"conformance/replica.mjs")]
        groups=[node,py,node] if primary_language=="python" else [py,node,py]
        procs=[]
        try:
            for i,cli in enumerate(groups):
                procs.append(subprocess.Popen([*cli,tmp,str(i),endpoints],
                             env=dict(os.environ,PYTHONPATH=str(ROOT/"bindings/python/python"))))
            deadline=time.monotonic()+45
            holder=int(wait_file(root/"committed",procs,deadline))
            procs[holder].kill()
            procs[holder].wait()
            reply=json.loads(wait_file(root/"finished",procs,deadline))
            assert reply["id"]!=holder and reply["result"]==43
            seen={}
            for line in (root/"effects").read_text().splitlines():
                key,value=line.split()
                assert seen.setdefault(key,value)==value
            assert sorted(seen.values())==["41","42"]
            print(json.dumps({"members":[("node" if c==node else "python") for c in groups],
                              "old_primary":holder,"new_primary":reply["id"],"result":reply["result"],
                              "consistent_external_keys":len(seen)}),flush=True)
        finally:
            for p in procs:
                if p.poll() is None:
                    p.kill()
            for p in procs:
                p.wait()


if __name__=="__main__":
    if len(sys.argv)==1:
        trial("python")
        trial("node")
    else:
        asyncio.run(peer(sys.argv[1],int(sys.argv[2]),sys.argv[3]))
