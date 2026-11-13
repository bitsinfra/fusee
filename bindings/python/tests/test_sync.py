"""fusee.sync: the blocking client, against a server with a Python worker."""

import asyncio
import os
import shutil
import socket
import subprocess
import tempfile
import time

import fusee
from fusee import Ctx
from fusee.sync import Client

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
CLI = os.environ.get("FUSEE_CLI", os.path.join(ROOT, "target", "release", "fusee"))

approve = fusee.signal("approve", bool)


@fusee.workflow("greet")
async def greet(ctx: Ctx, name: str) -> str:
    ok = await ctx.wait_for(approve)
    return f"hello {name}" if ok else "no"


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def blocking_code(address: str) -> None:
    """What a Flask handler would do: no event loop in sight."""
    with Client.connect(address) as client:
        run = client.start(greet, "g-1", "world")
        assert run.id == "g-1"
        try:
            run.result(timeout=0.2)
            raise AssertionError("expected a timeout")
        except fusee.TimeoutError:
            pass
        run.signal(approve, True)
        assert run.result(timeout=10) == "hello world"
        assert [r.id for r in client.list(type=greet)] == ["g-1"]
        again = client.signal_with_start(greet, "g-2", "there", approve, True)
        assert again.result(timeout=10) == "hello there"


async def main():
    d = tempfile.mkdtemp(prefix="fusee-sync-")
    port = free_port()
    address = f"127.0.0.1:{port}"
    server = subprocess.Popen([CLI, "server", "--dir", os.path.join(d, "server"), "--listen", address])
    try:
        for _ in range(200):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
                break
            except OSError:
                time.sleep(0.025)
        worker = await fusee.Engine.open(durability="server", address=address, workflows=[greet])
        await asyncio.to_thread(blocking_code, address)
        await worker.close()
        print("ok sync")
    finally:
        server.terminate()
        server.wait(timeout=10)
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
