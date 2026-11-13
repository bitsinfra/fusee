"""`fusee` on the command line (`uvx fusee dev`, `pipx run fusee dev`): runs
the fusee CLI that ships in this wheel (``fusee/bin/fusee``), so nothing
else needs installing. ``FUSEE_CLI`` points at another binary."""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path


def binary() -> str:
    env = os.environ.get("FUSEE_CLI")
    if env:
        return env
    bundled = Path(__file__).parent / "bin" / "fusee"
    if bundled.is_file():
        mode = bundled.stat().st_mode
        if not mode & stat.S_IXUSR:
            try:
                bundled.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            except OSError:
                pass
        return str(bundled)
    # In the repository: the CLI cargo built.
    local = Path(__file__).resolve().parents[4] / "target" / "release" / "fusee"
    if local.is_file():
        return str(local)
    sys.exit("fusee: this installation has no CLI binary (a wheel built without it). "
             "Set FUSEE_CLI to a fusee binary, or install one: cargo install fusee-cli")


def main() -> None:
    exe = binary()
    try:
        os.execv(exe, [exe, *sys.argv[1:]])
    except OSError as e:
        sys.exit(f"fusee: cannot run {exe}: {e}")


if __name__ == "__main__":
    main()
