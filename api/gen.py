#!/usr/bin/env python3
"""Writes docs/api.md from api/spec.json: every operation, with its name in
each language (the case rule, or the spelling api/check.py knows), so the
cross-language table cannot drift from the spec.

    python3 api/gen.py           # write docs/api.md
    python3 api/gen.py --check   # exit 1 if docs/api.md is not what the spec gives
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check  # noqa: E402

ROOT = check.ROOT
OUT = os.path.join(ROOT, "docs", "api.md")
LANGS = ["rust", "typescript", "python", "go", "java"]
TITLES = {
    "definitions": "Definitions",
    "client": "Engine and Client (one interface)",
    "engine": "Engine only",
    "run": "A run",
    "objectRef": "An object's key",
    "schedule": "A schedule",
    "ctx": "Workflow context",
    "stepCtx": "Step context",
}


def cell(key: str, op: dict, lang: str) -> str:
    if lang == "rust" and (key, op["name"]) in check.RUST_ALIASES:
        return f"`{check.RUST_ALIASES[(key, op['name'])]}`"
    if lang == "go" and (key, op["name"]) in check.GO_ALIASES:
        return f"`{check.GO_ALIASES[(key, op['name'])]}`"
    name = check.spelled(op["name"], lang)
    if key == "stepCtx" and lang == "rust":
        return f"`ctx.{name}`"
    return f"`{name}`"


def render() -> str:
    lines = [
        "# API at a glance",
        "",
        "Generated from [`api/spec.json`](../api/spec.json) by `api/gen.py`; do not edit.",
        "Every operation has one name, one argument order and one option set,",
        "written in each language's case. `api/check.py` checks the SDKs against it.",
        "",
    ]
    for key, ops in check.objects():
        lines += [f"## {TITLES[key]}", "", "| Operation | Arguments | Options | " + " | ".join(
            {"rust": "Rust", "typescript": "TypeScript", "python": "Python", "go": "Go", "java": "Java"}[l]
            for l in LANGS) + " |", "| --- | --- | --- | " + " | ".join("---" for _ in LANGS) + " |"]
        for op in ops:
            args = ", ".join(op.get("args", []))
            opts = ", ".join(op.get("options", []))
            row = [f"**{op['name']}**", args or "", opts or ""] + [cell(key, op, l) for l in LANGS]
            lines.append("| " + " | ".join(row) + " |")
        docs = [(op["name"], op["doc"]) for op in ops if op.get("doc")]
        if docs:
            lines.append("")
            for name, doc in docs:
                lines.append(f"- **{name}**: {doc}")
        lines.append("")
    errors = check.SPEC.get("errors")
    if errors:
        lines += ["## Errors", "", "The same codes in every SDK; each has a page in [errors.md](errors.md).", ""]
        names = errors if isinstance(errors, list) else list(errors)
        for e in names:
            code = e if isinstance(e, str) else e.get("code", e.get("name"))
            lines.append(f"- [`{code}`](errors.md#{str(code).lower()})")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    text = render()
    if "--check" in sys.argv:
        current = open(OUT).read() if os.path.exists(OUT) else ""
        if current != text:
            print("docs/api.md is not what api/spec.json gives: run python3 api/gen.py", file=sys.stderr)
            return 1
        return 0
    with open(OUT, "w") as f:
        f.write(text)
    print(f"wrote {os.path.relpath(OUT, ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
