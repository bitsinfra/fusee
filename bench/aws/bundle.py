"""Build a source bundle from the actual workspace, including local edits."""
import argparse
import hashlib
import io
from pathlib import Path
import subprocess
import tarfile

root = Path(__file__).resolve().parents[2]


def build(output, include_obelisk=False):
    names = subprocess.check_output(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard",
                                     "--", "Cargo.toml", "Cargo.lock", "crates", "bench", "bindings", "conformance", "spec", "README.md"], cwd=root).split(b"\0")
    if include_obelisk:
        for component in ("noop_activity.wasm", "chain_workflow.wasm"):
            path = Path("bench/drivers/obelisk/build") / component
            if not (root/path).is_file():
                raise ValueError(f"build the Obelisk components before bundling: {path}")
            names.append(str(path).encode())
    source_hash = hashlib.sha256()
    with tarfile.open(output, "w:gz") as archive:
        for name in sorted(set(names)):
            if not name or name.startswith(b"bench/results/"):
                continue
            path = root / name.decode()
            if not path.is_file() or path.is_symlink():
                continue
            contents = path.read_bytes()
            source_hash.update(name + b"\0" + hashlib.sha256(contents).digest())
            info = archive.gettarinfo(str(path), arcname=name.decode())
            archive.addfile(info, io.BytesIO(contents))
        revision = subprocess.check_output(["git","rev-parse","HEAD"],cwd=root).strip()
        metadata = revision + b"\n" + source_hash.hexdigest().encode() + b"\n"
        entry = tarfile.TarInfo("SOURCE-REVISION")
        entry.size = len(metadata)
        archive.addfile(entry,io.BytesIO(metadata))
    print(f"{output}: {source_hash.hexdigest()}")


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("output", type=Path)
    cli.add_argument("--include-obelisk", action="store_true")
    args = cli.parse_args()
    build(args.output, args.include_obelisk)
