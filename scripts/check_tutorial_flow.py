#!/usr/bin/env python3
"""Follows the tutorials (docs/tutorial-*.md) end to end in fresh projects,
as a reader would: `fusee init`, `fusee dev`, a hard kill of the app during
its pause, `fusee runs signal`, `fusee runs ls` and `runs show`, then the
project's test. Checks that the run continued from its journal (its
`compose` step recorded once) and finished with the approval.
scripts/check_tutorials.sh covers init and the tests only.

    python3 scripts/check_tutorial_flow.py [--fusee path/to/fusee] [py] [ts] [rust]

Each project gets its own port, so nothing else on 7601 is touched. The
Python project imports the binding from this tree (PYTHONPATH), the
TypeScript one links bindings/node (built: dist and the .node file), and
the Rust one depends on crates/fusee by path.
"""
import argparse
import json
import os
import queue
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def descendants(pid: int) -> list[tuple[int, str]]:
    rows = subprocess.run(["ps", "-Ao", "pid=,ppid=,command="], capture_output=True, text=True).stdout
    kids: dict[int, list[tuple[int, str]]] = {}
    for line in rows.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) == 3:
            kids.setdefault(int(parts[1]), []).append((int(parts[0]), parts[2]))
    out, todo = [], [pid]
    while todo:
        for child in kids.get(todo.pop(), []):
            out.append(child)
            todo.append(child[0])
    return out


class Dev:
    """`fusee dev` in a project, its output read line by line."""

    def __init__(self, fusee: str, cwd: Path, env: dict, http: str):
        self.p = subprocess.Popen([fusee, "dev", "--http", http], cwd=cwd, env=env, text=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
        self.lines: list[str] = []
        self.q: queue.Queue = queue.Queue()
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.p.stdout:
            self.lines.append(line.rstrip("\n"))
            self.q.put(line)

    def wait_for(self, pattern: str, timeout: float, after: int = 0) -> int:
        """Waits for the `after`+1-th line matching `pattern`; returns its index."""
        deadline, seen = time.time() + timeout, 0
        i = 0
        while time.time() < deadline:
            while i < len(self.lines):
                if re.search(pattern, self.lines[i]):
                    seen += 1
                    if seen > after:
                        return i
                i += 1
            if self.p.poll() is not None:
                break
            time.sleep(0.05)
        raise SystemExit(f"never printed /{pattern}/ (x{after + 1}):\n" + "\n".join(self.lines[-30:]))

    def stop(self):
        if self.p.poll() is None:
            os.killpg(self.p.pid, signal.SIGINT)
            try:
                self.p.wait(15)
            except subprocess.TimeoutExpired:
                os.killpg(self.p.pid, signal.SIGKILL)
                self.p.wait()


def runs(fusee: str, url: str, *args: str) -> str:
    r = subprocess.run([fusee, "runs", "--url", url, *args], capture_output=True, text=True, timeout=60)
    if r.returncode:
        raise SystemExit(f"fusee runs {' '.join(args)} failed:\n{r.stdout}{r.stderr}")
    return r.stdout


def flow(lang: str, fusee: str, work: Path) -> None:
    proj = work / lang
    proj.mkdir()
    env = dict(os.environ)
    env.pop("FUSEE_PROFILE", None)
    if lang == "py":
        subprocess.run([fusee, "init", "--lang", "python"], cwd=proj, check=True, capture_output=True)
        env["PYTHONPATH"] = str(ROOT / "bindings/python/python")
        app, test = "app.py", [sys.executable, "test_workflows.py"]
    elif lang == "ts":
        subprocess.run([fusee, "init", "--lang", "ts"], cwd=proj, check=True, capture_output=True)
        (proj / "node_modules/@bitsinfra").mkdir(parents=True)
        (proj / "node_modules/@bitsinfra/fusee").symlink_to(ROOT / "bindings/node")
        app, test = "app.ts", ["node", "--test", "workflows.test.ts"]
    else:
        subprocess.run([fusee, "init", "--lang", "rust"], cwd=proj, check=True, capture_output=True)
        cargo = proj / "Cargo.toml"
        cargo.write_text(re.sub(r'^fusee = ".*"', f'fusee = {{ path = "{ROOT / "crates/fusee"}" }}',
                                cargo.read_text(), flags=re.M))
        app, test = "/debug/", ["cargo", "test", "--quiet"]
    port = free_port()
    http, url = f"127.0.0.1:{port}", f"http://127.0.0.1:{port}"
    env["FUSEE_URL"] = url
    t0 = time.time()
    dev = Dev(fusee, proj, env, http)
    try:
        # 2. Run it: the run starts and waits in its pause.
        dev.wait_for(r"started greet-1", 600)
        with urllib.request.urlopen(f"{url}/v1/runs/greet-1", timeout=10) as r:
            d = json.load(r)
        assert d["status"] in ("running", "suspended"), d["status"]
        # 3. Kill it within the pause: a crash, no shutdown.
        apps = [p for p, cmd in descendants(dev.p.pid) if app in cmd and "cargo" not in cmd.split()[0]]
        assert apps, f"no {app} process under fusee dev: {descendants(dev.p.pid)}"
        for p in apps:
            os.kill(p, signal.SIGKILL)
        # fusee dev starts the app again; it attaches to the run.
        dev.wait_for(r"started greet-1", 300, after=1)
        # 4. Approve it.
        runs(fusee, url, "signal", "greet-1", "approve", "true")
        dev.wait_for(r"hello world", 60)
        listed = runs(fusee, url, "ls")
        assert re.search(r"greet-1\b.*\bok\b", listed), listed
        shown = runs(fusee, url, "show", "greet-1")
        compose = len(re.findall(r"\bcompose\b", shown))
        assert compose == 1, f"compose shown {compose} times:\n{shown}"
        with urllib.request.urlopen(f"{url}/v1/runs/greet-1", timeout=10) as r:
            d = json.load(r)
        steps = [j for j in d["journal"] if j.get("kind") == "step" and j.get("name") == "compose"]
        assert d["status"] == "ok" and len(steps) == 1, (d["status"], steps)
    finally:
        dev.stop()
    # 5. Test it.
    r = subprocess.run(test, cwd=proj, env=env, capture_output=True, text=True, timeout=900)
    if r.returncode:
        raise SystemExit(f"{lang}: the project's test failed:\n{r.stdout[-2000:]}{r.stderr[-2000:]}")
    print(f"ok  {lang}: init, dev, kill -9 during the pause, resumed, signal, ok, compose once, test "
          f"({time.time() - t0:.0f} s)")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--fusee", default=str(ROOT / "target/release/fusee"))
    ap.add_argument("langs", nargs="*", default=["py", "ts", "rust"])
    a = ap.parse_args()
    fusee = str(Path(a.fusee).resolve())
    with tempfile.TemporaryDirectory() as t:
        for lang in a.langs:
            flow(lang, fusee, Path(t))
    print("tutorial flows ok")


if __name__ == "__main__":
    main()
