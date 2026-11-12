#!/usr/bin/env python3
"""Writes the docs for coding agents into a built site: ``llms.txt`` (every
page in the sidebar's order, with a one-line description), ``llms-full.txt``
(every page as Markdown, in one file) and a ``.md`` beside each page.

    python3 scripts/llms.py target/site     # after the site is built
    python3 scripts/llms.py --check         # every sidebar page has a source

The page order and the page IDs come from ``docs/tooling/navigation.mjs``,
which the site's sidebar uses, so the files list what the site shows.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SITE_URL = "https://fusee.bitsinfra.com"


def navigation() -> tuple[list[tuple[str, str]], dict[str, str]]:
    """The sidebar as ``(section, page ID)`` pairs, and the page ID of each
    README that is a page."""
    script = (
        "const m = await import(process.argv[1]);"
        "console.log(JSON.stringify({ sidebar: m.sidebar, external: m.externalPages }));"
    )
    out = subprocess.run(
        ["node", "--input-type=module", "-e", script, (ROOT / "docs/tooling/navigation.mjs").as_uri()],
        check=True, capture_output=True, text=True,
    ).stdout
    nav = json.loads(out)
    pages = []
    for group in nav["sidebar"]:
        for item in group["items"]:
            slug = item if isinstance(item, str) else item["slug"]
            pages.append((group["label"], slug))
    external = {page_id: source for source, page_id in nav["external"].items()}
    return pages, external


def source_of(page_id: str, external: dict[str, str]) -> Path | None:
    if page_id in external:
        return ROOT / external[page_id]
    if page_id == "reference/operations":
        return ROOT / "docs/api.md"
    for ext in (".md", ".mdx"):
        p = ROOT / "docs" / f"{page_id}{ext}"
        if p.is_file():
            return p
    p = ROOT / "docs" / page_id / "index.md"
    return p if p.is_file() else None


def markdown(path: Path) -> str:
    """The page as Markdown: MDX imports and components' tags dropped, their
    content kept."""
    text = path.read_text()
    lines = []
    fence = False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            fence = not fence
        if not fence:
            if re.match(r"^import\s.+\sfrom\s", line):
                continue
            if re.match(r"^\s*</?[A-Z][A-Za-z]*(\s[^>]*)?/?>\s*$", line):
                continue
        lines.append(line)
    return "\n".join(lines).strip() + "\n"


def title_and_summary(md: str, page_id: str) -> tuple[str, str]:
    title = page_id
    summary = ""
    paragraph: list[str] = []
    for line in md.splitlines():
        if line.startswith("# ") and title == page_id:
            title = line[2:].strip()
            continue
        if not line.strip():
            if paragraph:
                break
            continue
        if line.startswith(("#", "```", "|", "<", "- ", "* ", ">")) and not paragraph:
            continue
        paragraph.append(line.strip())
    if paragraph:
        text = " ".join(paragraph)
        text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
        text = re.sub(r"[*_`]", "", text)
        first = re.split(r"(?<=[.!?])\s", text, maxsplit=1)[0]
        summary = first if len(first) <= 200 else first[:197].rstrip() + "..."
    return title, summary


def build(site: Path | None) -> list[str]:
    pages, external = navigation()
    problems = []
    index = [
        "# Fusee",
        "",
        "> Durable execution at the speed of the disk: an embeddable engine for Rust, TypeScript, "
        "Python, Go and Java. Workflows are ordinary async code whose steps are journaled.",
        "",
        "Rules for writing Fusee code: " + f"{SITE_URL}/guides/coding-agents.md",
    ]
    full = ["# Fusee documentation", ""]
    section = None
    for group, page_id in pages:
        src = source_of(page_id, external)
        if src is None:
            problems.append(f"the sidebar names '{page_id}', which has no source page")
            continue
        md = markdown(src)
        title, summary = title_and_summary(md, page_id)
        if group != section:
            index += ["", f"## {group}", ""]
            section = group
        url = f"{SITE_URL}/{'' if page_id == 'index' else page_id}"
        index.append(f"- [{title}]({SITE_URL}/{page_id}.md)" + (f": {summary}" if summary else ""))
        full += [f"<!-- {url} -->", "", md, ""]
        if site is not None:
            target = site / ("index.md" if page_id == "index" else f"{page_id}.md")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(md)
    if site is not None:
        (site / "llms.txt").write_text("\n".join(index) + "\n")
        (site / "llms-full.txt").write_text("\n".join(full))
        for name in ("llms.txt", "llms-full.txt"):
            if not (site / name).stat().st_size:
                problems.append(f"{name} is empty")
        listed = (site / "llms-full.txt").read_text()
        for _, page_id in pages:
            url = f"{SITE_URL}/{'' if page_id == 'index' else page_id}"
            if f"<!-- {url} -->" not in listed:
                problems.append(f"llms-full.txt is missing '{page_id}'")
    return problems


def main() -> int:
    args = sys.argv[1:]
    site = None if args == ["--check"] else Path(args[0]) if args else None
    if site is None and args != ["--check"]:
        print(__doc__)
        return 2
    problems = build(site)
    for p in problems:
        print(f"llms: {p}", file=sys.stderr)
    if not problems and site is not None:
        print(f"llms: {site / 'llms.txt'}, {site / 'llms-full.txt'}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
