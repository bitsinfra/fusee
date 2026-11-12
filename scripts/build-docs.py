"""Build the documentation site and the three API references into a local, browsable directory.

    python3 scripts/build-docs.py            # target/site/index.html

The guides use Fumadocs; see CONTRIBUTING.md for prerequisites.
"""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT/"target"/"site"

def run(*args, **kwargs):
    subprocess.run(list(map(str, args)), check=True, cwd=ROOT, **kwargs)


def main():
    OUT.parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="fusee-docs-", dir=OUT.parent) as tmp:
        site = Path(tmp)/"site"
        run("npm", "run", "build", "--prefix", "docs")
        shutil.copytree(ROOT/"docs"/"out", site)
        # The docs for coding agents: llms.txt, llms-full.txt, a .md beside each page.
        run(sys.executable, ROOT/"scripts"/"llms.py", site)
        api = site/"api"
        # Keep unrelated crates and source pages from previous Cargo builds out of the site.
        cargo = Path(tmp)/"cargo"
        run("cargo", "doc", "--locked", "--no-deps", "--target-dir", cargo,
            "-p", "fusee", "-p", "fusee-core", "-p", "fusee-server")
        shutil.copytree(cargo/"doc", api/"rust", ignore=shutil.ignore_patterns(".lock"))
        run(ROOT/"docs/node_modules/.bin/typedoc", "--tsconfig", "bindings/node/tsconfig.json",
            "--excludeInternal", "--excludePrivate", "--excludeProtected", "--disableSources", "--out", api/"typescript",
            "bindings/node/src/index.ts", "bindings/node/src/testing.ts", "bindings/node/src/ai.ts")
        run(sys.executable, "-m", "pdoc", "--no-show-source", "-o", api/"python", "fusee",
            env=dict(os.environ, PYTHONPATH=str(ROOT/"bindings/python/python")))
        for path in ["index.html", "guides/sagas/index.html", "errors/index.html", "api/index.html", "api/rust/fusee/index.html",
                     "api/typescript/index.html", "api/python/fusee.html", "llms.txt", "llms-full.txt", "guides/sagas.md"]:
            if not (site/path).is_file():
                raise RuntimeError(f"missing documentation page {path}")
        if OUT.exists():
            shutil.rmtree(OUT)
        shutil.move(site, OUT)
    print(f"documentation: {OUT/'index.html'}")


if __name__ == "__main__":
    main()
