"""Fail over three dedicated server processes while different SDK workers share a durable workflow."""
import asyncio
from contextlib import ExitStack
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import sys
import tempfile

from remote import ROOT, stop


def addresses(count):
    with ExitStack() as cleanup:
        sockets = [cleanup.enter_context(socket.socket()) for _ in range(count)]
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [f"127.0.0.1:{sock.getsockname()[1]}" for sock in sockets]


def ready(worker):
    if not select.select([worker.stdout], [], [], 30)[0] or worker.stdout.readline().strip() != "READY":
        raise RuntimeError(f"worker did not acquire a ready primary (exit {worker.poll()})")


async def scenario(old, new):
    from fusee import Client
    env = dict(os.environ, PYTHONPATH=str(ROOT / "bindings/python/python"))
    commands = {"node": ["node", str(ROOT/"conformance/remote.mjs")],
                "python": [sys.executable, str(ROOT/"conformance/remote.py")]}
    listeners = addresses(6)
    peers = ",".join(listeners[:3])
    endpoints = listeners[3:]
    address = "replicas://" + ",".join(endpoints)
    with tempfile.TemporaryDirectory(prefix="fusee-dedicated-") as directory, ExitStack() as cleanup:
        nodes = []
        for idx, endpoint in enumerate(endpoints):
            server = subprocess.Popen([env.get("FUSEE_CLI", str(ROOT/"target/release/fusee")), "server",
                "--dir", str(Path(directory)/str(idx)), "--listen", endpoint,
                "--replicas", peers, "--replica-id", str(idx)], env=env)
            cleanup.callback(stop, server)
            nodes.append(server)

        def launch(language):
            proc = subprocess.Popen([*commands[language], "worker", address], env=env,
                                    stdout=subprocess.PIPE, text=True)
            cleanup.callback(stop, proc)
            return proc

        first = launch(old)
        await asyncio.to_thread(ready, first)
        c = await Client.connect(address)
        cleanup.callback(c._transport.close)
        name = "approval-through-failover"
        _, added = await c.update_with_start("approval", name, 4, "add", 3, update_id="once")
        assert added == 7, added
        run = c.run(name)
        token = None
        for _ in range(100):
            token = await run.query("token")
            if token:
                break
            await asyncio.sleep(.01)
        assert isinstance(token, str)

        async def holder(endpoint):
            try:
                direct = await Client.connect(endpoint)
            except Exception:
                return False
            try:
                return await asyncio.wait_for(direct.run(name).query("count"), 2) == 7
            except Exception:
                return False
            finally:
                await direct.close()

        serving = await asyncio.gather(*(holder(addr) for addr in endpoints))
        assert serving.count(True) == 1, serving
        primary = serving.index(True)
        standby = launch(new)
        # A standby connects to the current primary while the previous worker owns its lease.
        assert not select.select([standby.stdout], [], [], .5)[0]
        nodes[primary].kill()
        nodes[primary].wait(timeout=5)
        await asyncio.to_thread(ready, standby)
        after = await Client.connect(address)
        cleanup.callback(after._transport.close)
        recovered = after.run(name)
        assert await recovered.update("add", 3, id="once") == 7
        assert await recovered.query("count") == 7
        await after.complete(token, True)
        result = await recovered.result()
        assert result == [7, True], result
        assert any([r.id == name async for r in after.list(status="ok")])
        print(json.dumps(dict(old_worker=old, standby=new, killed_server=primary,
                              recovered=result, listed=True, replayed_update=7)), flush=True)
        await asyncio.to_thread(subprocess.run, [*commands[old], "client", address],
                                env=env, check=True, timeout=45)


async def main():
    for old, new in [("node", "python"), ("python", "node")]:
        await asyncio.wait_for(scenario(old, new), timeout=75)


if __name__ == "__main__":
    asyncio.run(main())
