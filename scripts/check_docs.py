#!/usr/bin/env python3
"""Checks the code blocks of the docs and READMEs (every code block is checked
in CI).

Most blocks are fragments, so each kind is checked as far as it can be on
its own:

- python: compiles (top-level `await` allowed);
- sh / bash: every `fusee <command> [<subcommand>]` names a command the
  CLI has (`fusee <command> --help` succeeds);
- json, toml: parse;
- rust, ts, go, java: the whole-file examples are built by their SDKs'
  test suites; here only their fences are checked to be closed.

The tutorials' projects (init and their tests) are checked by
scripts/check_tutorials.sh, and their whole flow (init, dev, a kill -9
during the pause, resume, signal, `runs ls`/`show`, test) by
scripts/check_tutorial_flow.py.

    python3 scripts/check_docs.py [--fusee path/to/fusee]
"""

from __future__ import annotations

import ast
import json
import os
import re
import subprocess
import sys
import tomllib

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILES = (
    [os.path.join("docs", f) for f in sorted(os.listdir(os.path.join(ROOT, "docs"))) if f.endswith((".md", ".mdx"))]
    + [os.path.join("docs", "guides", f) for f in sorted(os.listdir(os.path.join(ROOT, "docs", "guides")))
       if f.endswith((".md", ".mdx"))]
    + ["README.md", "deploy/README.md", "examples/README.md"]
    + [os.path.join("bindings", d, "README.md") for d in sorted(os.listdir(os.path.join(ROOT, "bindings")))
       if os.path.exists(os.path.join(ROOT, "bindings", d, "README.md"))]
)

FENCE = re.compile(r"^```([A-Za-z0-9_+-]*)\s*$")


def blocks(path: str):
    lang, start, body = None, 0, []
    for n, line in enumerate(open(os.path.join(ROOT, path), encoding="utf-8"), 1):
        m = FENCE.match(line.rstrip("\n"))
        if lang is None and m:
            lang, start, body = m.group(1) or "text", n, []
        elif lang is not None and line.rstrip("\n") == "```":
            yield lang, start, "".join(body)
            lang = None
        elif lang is not None:
            body.append(line)
    if lang is not None:
        yield "unclosed", start, ""


def check_python(src: str) -> str | None:
    try:
        compile(src, "<doc>", "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT, dont_inherit=True)
    except SyntaxError as e:
        return f"python does not compile: {e.msg} (line {e.lineno})"
    return None


def check_shell(src: str, fusee: str, seen: dict) -> str | None:
    for raw in src.splitlines():
        line = raw.strip()
        if not line.startswith("fusee "):
            continue
        words = line.split()
        # The command and, when the next word is a plain name, its subcommand.
        cmd = words[1:2]
        if len(words) > 2 and re.fullmatch(r"[a-z][a-z-]*", words[2]):
            cmd = words[1:3]
        key = " ".join(cmd)
        if key not in seen:
            r = subprocess.run([fusee, *cmd, "--help"], capture_output=True, text=True)
            seen[key] = r.returncode == 0 or "Usage" in (r.stdout + r.stderr)
            if not seen[key] and len(cmd) == 2:
                # The second word was an argument, not a subcommand.
                r = subprocess.run([fusee, cmd[0], "--help"], capture_output=True, text=True)
                seen[key] = r.returncode == 0
        if not seen[key]:
            return f"`{line}`: the CLI has no `fusee {key}`"
    return None


def main() -> int:
    fusee = "fusee"
    if "--fusee" in sys.argv:
        fusee = sys.argv[sys.argv.index("--fusee") + 1]
    seen: dict = {}
    problems, counts = [], {}
    for path in FILES:
        if not os.path.exists(os.path.join(ROOT, path)):
            continue
        for lang, line, src in blocks(path):
            counts[lang] = counts.get(lang, 0) + 1
            err = None
            if lang == "unclosed":
                err = "a code fence is not closed"
            elif lang == "python":
                err = check_python(src)
            elif lang in ("sh", "bash"):
                err = check_shell(src, fusee, seen)
            elif lang == "json":
                try:
                    json.loads(src)
                except ValueError as e:
                    err = f"json does not parse: {e}"
            elif lang == "toml":
                try:
                    tomllib.loads(src)
                except tomllib.TOMLDecodeError as e:
                    err = f"toml does not parse: {e}"
            if err:
                problems.append(f"{path}:{line}: {err}")
    for p in problems:
        print(p)
    print(", ".join(f"{n} {lang}" for lang, n in sorted(counts.items())) + f"; {len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
