"""Keep a guest result until a complete verified copy exists."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import tarfile
import tempfile

HEADROOM = 32 * 1024 * 1024


def sync_directory(directory):
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def download_archive(s3, bucket, key, destination):
    size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    if size <= 0:
        raise ValueError("empty guest archive")
    parent = destination.parent
    scratch = shutil.disk_usage(parent).free < size + HEADROOM
    if scratch:
        parent = Path(tempfile.mkdtemp(prefix="fusee-evidence-"))
        if shutil.disk_usage(parent).free < size + HEADROOM:
            parent.rmdir()
            raise OSError("no room to receive the archive on either volume")
    fd, name = tempfile.mkstemp(prefix=".receiving-", dir=parent)
    os.close(fd)
    partial = Path(name)
    try:
        s3.download_file(bucket, key, str(partial))
        if partial.stat().st_size != size:
            raise ValueError("S3 archive size mismatch")
        with partial.open("rb") as file:
            digest = hashlib.file_digest(file, "sha256").hexdigest()
            os.fsync(file.fileno())
        with gzip.open(partial, "rb") as stream:
            while stream.read(1024 * 1024):
                pass  # Validate the gzip checksum and length through EOF.
        with tarfile.open(partial) as archive:
            code = int(archive.extractfile("./exit-code").read())

        saved = parent / destination.name
        partial.replace(saved)
        sync_directory(parent)
        if scratch:
            pointer = destination.with_suffix(".receiving-link")
            pointer.unlink(missing_ok=True)
            pointer.symlink_to(saved)
            pointer.replace(destination)
            sync_directory(destination.parent)
        temporary = saved.resolve().is_relative_to(Path(tempfile.gettempdir()).resolve())
        return dict(sha256=digest, bytes=size, exit_code=code,
                    scratch=scratch or temporary, path=str(saved.resolve()))
    finally:
        partial.unlink(missing_ok=True)


def cleanup_bucket(s3, bucket, inputs, results, verified):
    # Each run has one source bundle, at most one peer list and up
    # to five guest archives. An unexpected object must remain untouched.
    page = s3.list_objects_v2(Bucket=bucket)
    existing = {item["Key"] for item in page.get("Contents", [])}
    unknown = existing - set(inputs) - set(results)
    if unknown or page.get("IsTruncated"):
        raise ValueError("unexpected objects in the benchmark bucket")
    keep = existing.intersection(results) - set(verified)
    remove = existing.intersection(set(inputs) | set(verified))
    if remove:
        deleted = s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in sorted(remove)]})
        if deleted.get("Errors"):
            raise OSError("benchmark object deletion failed")
    if not keep:
        s3.delete_bucket(Bucket=bucket)
    return sorted(keep)


def save_manifest(path, manifest):
    path = path.resolve()
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as file:
        tmp = Path(file.name)
        try:
            json.dump(manifest, file, indent=2)
            file.flush()
            os.fsync(file.fileno())
            tmp.replace(path)
            sync_directory(path.parent)
        finally:
            tmp.unlink(missing_ok=True)


def main():
    import boto3
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--receipt", action="append", default=[], metavar="S3_KEY=SHA256",
                        help="digest verified on a separate copy, after download")
    args = parser.parse_args()
    file = args.directory / "run.json"
    run = json.loads(file.read_text())
    region = (run["zone"] if "zone" in run else run["zones"][0])[:-1]
    s3 = boto3.client("s3", region_name=region)
    kept = run.get("retained_results", [])
    if run.get("bucket_removed") or not kept:
        raise SystemExit("no retained result in this manifest")
    archives = run.setdefault("archives", {})
    receipts = dict(part.split("=", 1) for part in args.receipt)
    for key in kept:
        # CloudShell scratch can disappear between sessions.
        previous = archives.get(key)
        if previous and (key in receipts or Path(previous["path"]).is_file()):
            continue
        received = download_archive(s3, run["bucket"], key, args.directory / Path(key).name)
        if previous and previous["sha256"] != received["sha256"]:
            raise ValueError("retained object has changed")
        archives[key] = received
    save_manifest(file, run)
    for key, digest in receipts.items():
        if key not in kept or digest != archives[key]["sha256"]:
            raise SystemExit("separate copy does not match the received archive")
    left = cleanup_bucket(s3, run["bucket"], [], kept, receipts)
    run.update(bucket_removed=not left, retained_results=left)
    save_manifest(file, run)
    print(json.dumps({"archives": archives, "retained": left}))


if __name__ == "__main__":
    main()
