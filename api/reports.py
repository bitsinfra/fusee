#!/usr/bin/env python3
"""API reports: the public surface of the TypeScript and Python SDKs, checked
in under api/reports/ so a change to it shows in review.

    python3 api/reports.py           # write the reports (build the Node package first)
    python3 api/reports.py --check   # exit 1 if a report is not what the code gives
"""

from __future__ import annotations

import inspect
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "api", "reports")


def typescript() -> dict:
    """The packages' declaration files (built with stripInternal)."""
    out = {}
    dist = os.path.join(ROOT, "bindings", "node", "dist")
    for name in ("index.d.ts", "testing.d.ts", "ai.d.ts", "ai-vercel.d.ts", "client.d.ts", "rich.d.ts"):
        if name in ("index.d.ts", "client.d.ts"):
            # Entry points that re-export from the modules: their surface is
            # the declarations they re-export, followed into each module.
            text = subprocess.run(["node", os.path.join(ROOT, "api", "ts-surface.mjs"), os.path.join(dist, name)],
                                  check=True, capture_output=True, text=True).stdout
        else:
            text = open(os.path.join(dist, name)).read()
        # Drop the source map comment; keep the rest as tsc wrote it.
        out[f"typescript-{name}"] = re.sub(r"//# sourceMappingURL=.*\n?", "", text)
    return out


def python() -> dict:
    sys.path.insert(0, os.path.join(ROOT, "bindings", "python", "python"))
    import fusee  # noqa: PLC0415
    import fusee.ai  # noqa: PLC0415
    import fusee.sync  # noqa: PLC0415
    import fusee.testing  # noqa: PLC0415

    lines = []

    def sig(obj) -> str:
        try:
            # Default values that print an address (a lambda) print the same each time.
            return re.sub(r" at 0x[0-9a-f]+", "", str(inspect.signature(obj)))
        except (TypeError, ValueError):
            return "(...)"

    for mod in (fusee, fusee.ai, fusee.sync, fusee.testing):
        lines.append(f"# {mod.__name__}")
        for name in sorted(getattr(mod, "__all__", [])):
            obj = getattr(mod, name)
            if inspect.isclass(obj):
                bases = ", ".join(b.__name__ for b in obj.__bases__ if b is not object)
                lines.append(f"class {name}({bases})")
                for m, v in sorted(vars(obj).items()):
                    if m.startswith("_") and m != "__init__":
                        continue
                    if isinstance(v, property):
                        lines.append(f"    {m}: property")
                    elif inspect.isfunction(v):
                        kind = "async def" if inspect.iscoroutinefunction(v) else "def"
                        lines.append(f"    {kind} {m}{sig(v)}")
                    elif isinstance(v, (classmethod, staticmethod)):
                        lines.append(f"    @{type(v).__name__} {m}{sig(v.__func__)}")
            elif inspect.isfunction(obj):
                kind = "async def" if inspect.iscoroutinefunction(obj) else "def"
                lines.append(f"{kind} {name}{sig(obj)}")
            else:
                lines.append(f"{name}: {type(obj).__name__}")
        lines.append("")
    return {"python.txt": "\n".join(lines)}


def main() -> int:
    reports = {**typescript(), **python()}
    if "--check" in sys.argv:
        stale = [n for n, t in reports.items() if not os.path.exists(os.path.join(OUT, n)) or open(os.path.join(OUT, n)).read() != t]
        for n in stale:
            print(f"api/reports/{n} is not what the code gives: run python3 api/reports.py and review the change", file=sys.stderr)
        return 1 if stale else 0
    os.makedirs(OUT, exist_ok=True)
    for n, t in reports.items():
        with open(os.path.join(OUT, n), "w") as f:
            f.write(t)
        print(f"wrote api/reports/{n} ({t.count(chr(10))} lines)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
