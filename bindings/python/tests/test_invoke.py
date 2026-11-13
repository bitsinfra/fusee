"""Invoked workers in the Python SDK: a Fusee server started with --invoke
POSTs to an HTTP endpoint that runs each invocation through the Lambda
handler shape (a fresh event loop and engine each time); workflows sleep,
take signals and finish with no engine open between invocations. The ASGI
app and the handler refuse bad signatures before connecting."""

import asyncio
import hashlib
import hmac
import json
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import fusee
from fusee import Client, invocation_handler, verify_signature

SECRET = "an invocation secret of 32 bytes"
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
CLI = os.environ.get("FUSEE_CLI", os.path.join(ROOT, "target", "release", "fusee"))


def sign(body: bytes, t=None) -> str:
    t = int(time.time() * 1000) if t is None else t
    return f"t={t},v1=" + hmac.new(SECRET.encode(), f"{t}.".encode() + body, hashlib.sha256).hexdigest()


def test_signatures():
    body = b'{"version":1}'
    assert verify_signature(SECRET, sign(body), body) is None
    assert "does not match" in verify_signature(SECRET, sign(body), b"{}")
    assert "too old" in verify_signature(SECRET, sign(body, int(time.time() * 1000) - 600_000), body)


def test_refused_before_connecting():
    opened = []
    h = invocation_handler("127.0.0.1:1", secret=SECRET, register=lambda e: opened.append(e))
    body = json.dumps({"version": 1, "invocation": "00000000000000aa", "shards": [0], "budgetMs": 1000})
    assert h.lambda_handler({"headers": {}, "body": body})["statusCode"] == 401
    bad = h.lambda_handler({"headers": {"Fusee-Signature": sign(body.encode())}, "body": body + " "})
    assert bad["statusCode"] == 401

    async def asgi(path_body: bytes, headers):
        sent = []

        async def receive():
            return {"type": "http.request", "body": path_body, "more_body": False}

        async def send(m):
            sent.append(m)

        scope = {"type": "http", "method": "POST", "headers": [(k.encode(), v.encode()) for k, v in headers.items()]}
        await h(scope, receive, send)
        return sent[0]["status"]

    assert asyncio.run(asgi(b"{}", {"fusee-signature": sign(b"{}")})) == 400
    assert asyncio.run(asgi(b"{}", {})) == 401
    assert not opened


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_sleep_signal_finish_with_no_engine_between_invocations():
    if not os.path.exists(CLI):
        print("skipped: no", CLI)
        return
    directory = tempfile.mkdtemp(prefix="fusee-py-invoke-")
    secret_file = os.path.join(directory, "secret")
    with open(secret_file, "w") as f:
        f.write(SECRET)
    os.chmod(secret_file, 0o600)
    port = free_port()
    address = f"127.0.0.1:{port}"
    opened = []
    steps = {}

    @fusee.workflow("sleeper")
    async def sleeper(ctx, n):
        def first():
            steps[f"{ctx.workflow_id}/first"] = steps.get(f"{ctx.workflow_id}/first", 0) + 1
            return n + 1

        a = await ctx.step("first", first)
        await ctx.sleep("nap", "1200ms")
        go = await ctx.wait_for("go")

        def second():
            steps[f"{ctx.workflow_id}/second"] = steps.get(f"{ctx.workflow_id}/second", 0) + 1
            return a * 10 + go

        return await ctx.step("second", second)

    handler = invocation_handler(address, workflows=[sleeper], secret=SECRET, register=lambda e: opened.append(1))

    class Endpoint(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("content-length", "0")))
            # Each invocation: a fresh event loop and engine (asyncio.run),
            # one at a time on this thread, as on a Lambda.
            r = handler.lambda_handler({"headers": dict(self.headers.items()), "body": body.decode()})
            out = r["body"].encode()
            self.send_response(r["statusCode"])
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(out)))
            self.end_headers()
            self.wfile.write(out)

        def log_message(self, *args):
            pass

    http = HTTPServer(("127.0.0.1", 0), Endpoint)
    http.timeout = 0.05
    url = f"http://127.0.0.1:{http.server_address[1]}/invoke"
    server = subprocess.Popen([CLI, "server", "--dir", os.path.join(directory, "server"), "--listen", address,
                               "--shards", "2", "--invoke", url, "--invoke-secret-file", secret_file])
    try:
        for _ in range(200):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
                break
            except OSError:
                time.sleep(0.025)

        async def run():
            client = await Client.connect(address)
            ids = ["s-0", "s-1", "s-2"]
            for k, id in enumerate(ids):
                await client.start(sleeper, id, k)
            await asyncio.sleep(2.5)
            quiet = await client.call("invocations", {})
            assert quiet["endpoints"][0]["inFlight"] == 0, quiet
            for k, id in enumerate(ids):
                await client.run(id).signal("go", k + 5)
                assert await client.run(id).result() == (k + 1) * 10 + k + 5
            m = await client.call("invocations", {})
            for _ in range(500):
                now = await client.call("invocations", {})
                if now["endpoints"][0]["inFlight"] == 0 and now["invokingShards"] == 0:
                    break
                await asyncio.sleep(0.02)
            await client.close()
            return m

        # The endpoint serves on this thread, which owns every engine; the
        # client runs beside it.
        out = {}

        def scenario():
            try:
                out["m"] = asyncio.run(run())
            except BaseException as e:  # noqa: BLE001 - re-raised below
                out["error"] = e

        t = threading.Thread(target=scenario)
        t.start()
        while t.is_alive():
            http.handle_request()
        if "error" in out:
            raise out["error"]
        m = out["m"]
        for id in ["s-0", "s-1", "s-2"]:
            assert steps[f"{id}/first"] == 1 and steps[f"{id}/second"] == 1, steps
        e = m["endpoints"][0]
        assert e["failed"] + e["timedOut"] + e["unleased"] == 0, m
        assert len(opened) >= 4, opened
        print(f"{len(opened)} engines for {e['invocations']} invocations, {e['steps']} steps, "
              f"mean open {e['openUsTotal'] / max(e['opens'], 1) / 1e3:.1f} ms")
    finally:
        server.kill()
        server.wait()
        http.server_close()
        shutil.rmtree(directory, ignore_errors=True)


if __name__ == "__main__":
    for t in (test_signatures, test_refused_before_connecting, test_sleep_signal_finish_with_no_engine_between_invocations):
        t()
        print("ok", t.__name__)
