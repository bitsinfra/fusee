#!/usr/bin/env python3
"""Checks each SDK against api/spec.json (one model, five spellings).

Every operation of the spec must exist in each SDK under that language's
case. Python is checked by importing the package; Rust by reading the
public functions of the types' source files; TypeScript, Go and Java by
name in their sources.

    python3 api/check.py            # all SDKs; exit 1 if a conforming one lacks an operation
    python3 api/check.py --json     # the findings as JSON (api/gen.py uses them)
"""

from __future__ import annotations

import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPEC = json.load(open(os.path.join(ROOT, "api", "spec.json")))

#: SDKs on the spec. The others are reported, not failed.
CONFORMING = {"python", "rust", "java", "typescript", "go"}

#: Where each spec object lives, per language.
PYTHON_CLASSES = {
    "definitions": ["fusee"],
    "client": ["Engine", "Client"],
    "engine": ["Engine"],
    "run": ["Run"],
    "objectRef": ["ObjectRef"],
    "schedule": ["Schedule"],
    "ctx": ["Ctx"],
    "stepCtx": ["StepCtx"],
}

RUST_TYPES = {
    "definitions": ("lib", None),
    "client": ("engine.rs", "Engine"),
    "engine": ("engine.rs", "Engine"),
    "run": ("engine.rs", "RunHandle"),
    "objectRef": ("object.rs", "ObjectRef"),
    "schedule": ("engine.rs", "ScheduleHandle"),
    "ctx": ("ctx.rs", "Ctx"),
    # Rust keeps the step's operations on the context (ctx.key() knows the
    # running step without ambient state).
    "stepCtx": ("ctx.rs", "Ctx"),
}

#: Spellings that differ from the case rule, with the reason.
RUST_ALIASES = {
    ("definitions", "workflow"): "WorkflowRef::new",
    ("definitions", "signal"): "Signal::new",
    ("definitions", "query"): "Query::new",
    ("definitions", "update"): "Update::new",
    ("definitions", "object"): "Engine::object_type(name).exclusive(..)",
    ("definitions", "shared"): "ObjectType::shared",
    ("engine", "serve"): "Config::http / http::serve",
    ("engine", "on"): "log::set_hook",
    ("client", "list"): "Engine::list_where",
    ("client", "schedules"): "Engine::list_schedules",
    ("client", "complete"): "Engine::complete",
    ("ctx", "sleepUntil"): "Ctx::sleep_until",
    ("ctx", "all"): "futures::join! (Rust's own)",
    ("ctx", "race"): "futures::select! (Rust's own)",
    ("ctx", "now"): "Ctx::now_ms",
    ("ctx", "object"): "Ctx::object",
}

#: Go spells initialisms in capitals (Go's own rule).
GO_ALIASES = {
    ("ctx", "uuid"): "UUID",
    # Go cannot have a function and a type both named Workflow.
    ("definitions", "workflow"): "Define",
}

PYTHON_ALIASES = {
    ("engine", "serve"): "Engine.serve",
    ("engine", "on"): "Engine.on",
}


def snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def spelled(name: str, lang: str) -> str:
    case = SPEC["case"][lang]
    if case == "snake":
        return snake(name)
    if case == "pascal":
        return name[:1].upper() + name[1:]
    return name


def objects():
    for key in ("definitions", "client", "engine", "run", "objectRef", "schedule", "ctx", "stepCtx"):
        v = SPEC[key]
        ops = v if isinstance(v, list) else v.get("operations", [])
        yield key, ops


# ---------------------------------------------------------------- Python


def check_python() -> dict:
    sys.path.insert(0, os.path.join(ROOT, "bindings", "python", "python"))
    try:
        import fusee  # noqa: PLC0415
    except Exception as e:  # noqa: BLE001 - reported
        return {"error": f"cannot import the Python package: {e}"}
    missing = []
    for key, ops in objects():
        homes = [fusee if c == "fusee" else getattr(fusee, c, None) for c in PYTHON_CLASSES[key]]
        for op in ops:
            name = spelled(op["name"], "python")
            if (key, op["name"]) in PYTHON_ALIASES:
                continue
            for h in homes:
                if h is None or not hasattr(h, name):
                    missing.append(f"{key}.{op['name']} → {getattr(h, '__name__', h)}.{name}")
    return {"missing": missing}


# ---------------------------------------------------------------- Rust


def rust_fns(file: str, type_name: str | None) -> set:
    src = open(os.path.join(ROOT, "crates", "fusee", "src", file)).read()
    if type_name is None:
        return set(re.findall(r"pub (?:async )?fn ([a-z_0-9]+)", src))
    out = set()
    # Every `impl Type` / `impl<..> Type<..>` block's public functions.
    for m in re.finditer(r"\nimpl(?:<[^>]*>)? " + re.escape(type_name) + r"\b[^{]*\{", src):
        depth, i = 1, m.end()
        while depth and i < len(src):
            depth += {"{": 1, "}": -1}.get(src[i], 0)
            i += 1
        out |= set(re.findall(r"\n    pub (?:async )?fn ([a-z_0-9]+)", src[m.end():i]))
    return out


def check_rust() -> dict:
    missing = []
    for key, ops in objects():
        file, type_name = RUST_TYPES[key]
        if key == "definitions":
            continue  # constructors of definition types: see RUST_ALIASES
        have = rust_fns(file, type_name)
        for op in ops:
            if (key, op["name"]) in RUST_ALIASES:
                continue
            name = spelled(op["name"], "rust")
            if name not in have:
                missing.append(f"{key}.{op['name']} → {type_name}::{name}")
    return {"missing": missing}


# ---------------------------------------------------------------- the others


def check_text(lang: str, paths: list, pattern: str) -> dict:
    """A check by name in an SDK's sources."""
    text = ""
    for p in paths:
        full = os.path.join(ROOT, p)
        for dirpath, _, files in os.walk(full) if os.path.isdir(full) else [(os.path.dirname(full), [], [os.path.basename(full)])]:
            for f in files:
                if f.endswith((".ts", ".go", ".java")) and "node_modules" not in dirpath and "test" not in f.lower():
                    text += open(os.path.join(dirpath, f), errors="ignore").read()
    missing = []
    aliases = GO_ALIASES if lang == "go" else {}
    for key, ops in objects():
        for op in ops:
            name = aliases.get((key, op["name"])) or spelled(op["name"], lang)
            if not re.search(pattern.format(re.escape(name)), text):
                missing.append(f"{key}.{op['name']} → {name}")
    return {"missing": missing}


def main() -> int:
    found = {
        "python": check_python(),
        "rust": check_rust(),
        "typescript": check_text("typescript", ["bindings/node/src"], r"\b{}\s*[(<:]"),
        "go": check_text("go", ["bindings/go"], r"func (?:\([^)]*\) )?{}\b"),
        "java": check_text("java", ["bindings/java/src/main"], r"\b{}\s*\("),
    }
    if "--json" in sys.argv:
        print(json.dumps(found, indent=2))
    failed = False
    for lang, r in found.items():
        gaps = r.get("missing", [])
        err = r.get("error")
        state = "on the spec" if lang in CONFORMING else "reported only"
        if "--json" not in sys.argv:
            print(f"{lang} ({state}): {err or (str(len(gaps)) + ' missing')}")
            if lang in CONFORMING:
                for g in gaps:
                    print(f"  {g}")
        if lang in CONFORMING and (gaps or err):
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
