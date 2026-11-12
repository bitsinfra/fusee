#!/usr/bin/env python3
"""Native calls and allocations per step of the Node and Python bindings:
a sequential no-op chain in `memory` mode, counted without changing either
binding.

    python3 scripts/step-counts/run.py [--node PKG ...] [--python DIR ...]

PKG is a Node package directory (with dist/ and the .node file); DIR holds
the `fusee` Python package (with its native module). Name a tree before a
change and after it to compare; with no arguments it measures this
checkout's builds. To measure an older tree, extract it (`git archive <ref>
| tar -x -C /tmp/base`), build its native modules with their own
CARGO_TARGET_DIR (cargo hashes path packages relative to the workspace, so
a shared target would mix the trees' artifacts), compile its dist, and
point --node / --python at it.

Native calls: the Node addon's exported functions and class methods are
wrapped as it loads (count-native.cjs); Python's C calls into the native
module are seen by sys.setprofile. Allocations: malloc, calloc,
posix_memalign and aligned_alloc in the whole process, counted by an
interposed library (countmalloc.c, macOS). Each figure is a 3,300-step
chain minus a 300-step one, over 3,000, the median of three processes.
`sites.py` (with sitemalloc.c) lists where the allocations come from.
"""
import argparse, json, os, statistics, subprocess, sys, tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def countmalloc(tmp: Path) -> Path:
    lib = tmp / "countmalloc.dylib"
    subprocess.run(["cc", "-O2", "-dynamiclib", "-o", str(lib), str(HERE / "countmalloc.c")], check=True)
    return lib


def run(cmd, lib: Path, out: Path):
    env = dict(os.environ, DYLD_INSERT_LIBRARIES=str(lib), COUNTMALLOC_OUT=str(out))
    r = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=600)
    if r.returncode:
        sys.exit(r.stderr[-2000:])
    j = json.loads(r.stdout.strip().splitlines()[-1])
    allocs, _ = map(int, out.read_text().split())
    return j, allocs


def measure(lang: str, pkg: str, lib: Path, tmp: Path):
    calls, allocs, by = [], [], {}
    for _ in range(3):
        got = {}
        for n in (300, 3300):
            cmd = (["node", "--require", str(HERE / "count-native.cjs"), str(HERE / "chain.mjs"), pkg, str(n)]
                   if lang == "node" else [sys.executable, str(HERE / "chain.py"), pkg, str(n)])
            got[n] = run(cmd, lib, tmp / f"{lang}-{n}.txt")
        calls.append((got[3300][0]["calls"] - got[300][0]["calls"]) / 3000)
        allocs.append((got[3300][1] - got[300][1]) / 3000)
        keys = set(got[3300][0]["by"]) | set(got[300][0]["by"])
        by = {k: round((got[3300][0]["by"].get(k, 0) - got[300][0]["by"].get(k, 0)) / 3000, 2) for k in keys}
    return statistics.median(calls), statistics.median(allocs), {k: v for k, v in sorted(by.items()) if v}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--node", action="append", default=[])
    ap.add_argument("--python", action="append", default=[])
    a = ap.parse_args()
    if not a.node and not a.python:
        a.node, a.python = [str(ROOT / "bindings/node")], [str(ROOT / "bindings/python/python")]
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        lib = countmalloc(tmp)
        for lang, pkgs in (("node", a.node), ("python", a.python)):
            for pkg in pkgs:
                c, al, by = measure(lang, pkg, lib, tmp)
                print(f"{lang:6} {pkg}\n       native calls/step {c:.2f}  allocations/step {al:.2f}  {json.dumps(by)}")


if __name__ == "__main__":
    main()
