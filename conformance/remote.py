"""Run each remote SDK client against a worker in another language and a real server."""
import asyncio
import os
import shutil
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


async def worker(address):
    import fusee
    from fusee import Engine

    @fusee.workflow("approval")
    async def approval(ctx, initial):
        count, token = initial, None
        def add(n):
            nonlocal count
            count += n
            return count
        async def send(cb):
            nonlocal token
            token = cb.token
        ctx.on_update("add", add)
        ctx.on_query("count", lambda _: count)
        ctx.on_query("token", lambda _: token)
        approved = await ctx.await_completion("decision", send)
        return [count, approved]
    # Debounced and batched runs, their steps queued for a limit by priority.
    @fusee.workflow("echo")
    async def echo(ctx, v):
        return await ctx.step("v", lambda: v, limit="remote", priority=2, fairness_key="t", fairness_weight=2)
    @fusee.workflow("collect")
    async def collect(ctx, events):
        return await ctx.step("all", lambda: events, limit="remote", priority=-1)
    # A step that streams tokens, then bytes.
    @fusee.workflow("talk")
    async def talk(ctx, n):
        async def generate(s):
            out = s.stream("tokens")
            for i in range(n):
                out.append(f"t{i}")
                await asyncio.sleep(0.005)
            out.append(b"\x01\x02")
            return n
        return await ctx.step("generate", generate)
    # A key of an object type, called by both clients.
    async def tally_add(ctx, n):
        s = ctx.state.get("sum", 0) + n
        ctx.state.set("sum", s)
        return s
    tally = fusee.object("tally", {"add": tally_add})
    e = await Engine.open(durability="server", address=address, workflows=[approval, echo, collect, talk],
                          objects=[tally])
    e.limit("remote", concurrent=1)
    stop = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    print("READY", flush=True)
    await stop.wait()
    await e.close()


async def client(address, tag=""):
    from fusee import Client
    c = await Client.connect(address)
    try:
        name = "from-python" + tag
        _, added = await c.update_with_start("approval", name, 4, "add", 3, update_id="once")
        assert added == 7, added
        run = c.run(name)
        assert await run.update("add", 3, id="once") == 7
        assert await run.query("count") == 7
        token = None
        for _ in range(100):
            token = await run.query("token")
            if token:
                break
            await asyncio.sleep(.01)
        assert isinstance(token, str)
        await c.complete(token, True)
        assert await run.result() == [7, True]
        assert any([r.id == name and isinstance(r.started_ms, int) async for r in c.list(status="ok")])
        # Two starts inside the window: one run, the latest input.
        first = await c.debounce("echo", f"d-{name}", 1, window=500, event_id="d1")
        latest = await c.debounce("echo", f"d-{name}", 2, window=500, event_id="d2")
        assert first.id == latest.id
        await first.started()
        assert await first.result() == 2
        # A repeated event ID adds nothing; the batch closes on its wait.
        events = [await c.batch("collect", f"b-{name}", e, max_events=3, max_wait=300, event_id=e)
                  for e in ("a", "b", "b")]
        assert len({r.id for r in events}) == 1
        await events[0].started()
        assert await events[0].result() == ["a", "b"]
        # A stream read through the server, then again from where it stopped.
        await c.start("talk", f"s-{name}", 10)
        r = c.run(f"s-{name}").stream("tokens")
        got = []
        async for chunk in r:
            got.append(chunk)
            if len(got) == 4:
                break
        rest = c.run(f"s-{name}").stream("tokens", from_=r.offset)
        got += [chunk async for chunk in rest]
        assert got == [f"t{i}" for i in range(10)] + [b"\x01\x02"], got
        assert rest.status == "ok" and rest.offset == 11
        # A key of an object type: a call, its retry, a one-way call, the state.
        k = c.object("tally", name)
        assert await k.call("add", 2, id="o1") == 2
        assert await k.call("add", 2, id="o1") == 2
        await k.send("add", 3, id="o2")
        for _ in range(200):
            if await k.state() == {"sum": 5}:
                break
            await asyncio.sleep(.01)
        assert await k.state() == {"sum": 5}
        print("PASS Python client", flush=True)
    finally:
        await c.close()


def stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def cross_language():
    import select
    js = ["node", str(ROOT/"conformance/remote.mjs")]
    py = [sys.executable, __file__]
    pairs = [(js, py), (py, js)]
    env = dict(os.environ, PYTHONPATH=str(ROOT/"bindings/python/python"))
    with tempfile.TemporaryDirectory(prefix="fusee-remote-") as directory:
        # The Go companion (bindings/go/cmd/remote), when a Go toolchain is
        # here: it works both ways with each of the others.
        go = os.environ.get("GO") or shutil.which("go")
        if go and os.environ.get("FUSEE_REMOTE_GO") != "0":
            exe = os.path.join(directory, "remote-go")
            subprocess.run([go, "build", "-o", exe, "./cmd/remote"], cwd=ROOT/"bindings/go", check=True)
            pairs += [([exe], py), (py, [exe]), ([exe], js), (js, [exe])]
        # The Java companion (bindings/java, dev.fusee.cmd.Remote), when its
        # classes are built (mvn test-compile) and a JDK is here.
        java = shutil.which("java")
        classes = ROOT/"bindings/java/target"
        if java and (classes/"test-classes").is_dir() and os.environ.get("FUSEE_REMOTE_JAVA") != "0":
            jv = [java, "-cp", f"{classes/'classes'}{os.pathsep}{classes/'test-classes'}", "dev.fusee.cmd.Remote"]
            others = [py, js] + ([[exe]] if go and os.environ.get("FUSEE_REMOTE_GO") != "0" else [])
            for other in others:
                pairs += [(jv, other), (other, jv)]
        with socket.socket() as sock:
            sock.bind(("127.0.0.1",0))
            port = sock.getsockname()[1]
        address = f"127.0.0.1:{port}"
        server = subprocess.Popen([os.environ.get("FUSEE_CLI",str(ROOT/"target/release/fusee")), "server",
                                   "--dir",directory,"--listen",address,"--shards","2"])
        try:
            for _ in range(100):
                try:
                    socket.create_connection(("127.0.0.1",port),timeout=.1).close()
                    break
                except OSError:
                    time.sleep(.02)
            for engine, caller in pairs:
                w = subprocess.Popen([*engine,"worker",address],env=env,stdout=subprocess.PIPE,text=True)
                try:
                    if not select.select([w.stdout],[],[],30)[0] or w.stdout.readline().strip()!="READY":
                        raise RuntimeError("worker did not register workflows")
                    # Each client run gets its own workflow IDs: a client
                    # meets workers in more than one language.
                    tag = "-on-" + {"remote.mjs": "js", "remote.py": "py", "dev.fusee.cmd.Remote": "java"}.get(
                        os.path.basename(engine[-1]), "go")
                    subprocess.run([*caller,"client",address,tag],env=env,check=True,timeout=45)
                finally:
                    stop(w)
        finally:
            stop(server)


if __name__=="__main__":
    if len(sys.argv)==1:
        cross_language()
    elif sys.argv[1]=="worker":
        asyncio.run(worker(sys.argv[2]))
    else:
        asyncio.run(client(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else ""))
