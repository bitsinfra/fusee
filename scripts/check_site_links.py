#!/usr/bin/env python3
"""Check local links, assets, and section anchors in the built documentation."""

from html.parser import HTMLParser
from pathlib import Path
import sys
from urllib.parse import unquote, urljoin, urlsplit


class Page(HTMLParser):
    def __init__(self, path):
        super().__init__()
        self.ids = set()
        self.links = set()
        self.feed(path.read_text(encoding="utf-8"))

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if "id" in attrs:
            self.ids.add(attrs["id"])
        for key in ("href", "src"):
            if attrs.get(key):
                self.links.add(attrs[key])


def main():
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "target/site").resolve()
    # Each SDK generator owns its HTML. Verify our links into those directories.
    pages = {
        file: Page(file) for file in root.rglob("*.html")
        if not (file.relative_to(root).parts[:2] in
                (("api", "rust"), ("api", "python"), ("api", "typescript")))
    }
    if root / "index.html" not in pages:
        sys.exit(f"No documentation homepage in {root}")
    broken = []
    for path, page in pages.items():
        for link in sorted(page.links):
            route = urlsplit(urljoin("/" + path.relative_to(root).as_posix(), link))
            if route.scheme or route.netloc:
                continue
            target = root / unquote(route.path).lstrip("/")
            if target.is_dir():
                target = target / "index.html"
            if not target.is_file() or (route.fragment and target in pages
                                        and unquote(route.fragment) not in pages[target].ids):
                broken.append(f"{path.relative_to(root)}: {link}")
    for error in broken:
        print(error)
    print(f"{len(pages)} HTML files checked; {len(broken)} broken link(s)")
    return bool(broken)


if __name__ == "__main__":
    sys.exit(main())
