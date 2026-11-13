"""Validate a returned floor archive and summarize its direct and buffered write floors."""
from array import array
import argparse
import json
from pathlib import Path
import sys
import tarfile


def check(directory):
    manifest = json.loads((directory / "run.json").read_text())
    if not manifest["instance_terminated"] or not manifest["bucket_removed"]:
        raise ValueError("incomplete resource cleanup")
    methods = {"pwrite+fdatasync", "pwritev2(RWF_DSYNC)", "io_uring linked write+fdatasync"}
    expected = {(method, block, depth) for method in methods for block in (4096,8192,16384,32768,65536)
                for depth in (1,2,4,8,16,32)}
    with tarfile.open(directory / "results.tgz") as archive:
        # Index via a single streaming pass. Re-seeking in a gzip archive for every
        # sample file would repeatedly decompress the preceding files.
        conf = {}
        samples = {}
        for entry in archive:
            if not entry.isfile():
                continue
            if entry.name.endswith(".ns"):
                values = array("Q")
                values.frombytes(archive.extractfile(entry).read())
                if sys.byteorder != "little":
                    values.byteswap()
                if not values or min(values) <= 0:
                    raise ValueError(f"empty or invalid timings: {entry.name}")
                samples[Path(entry.name).name] = len(values)
            elif Path(entry.name).name in {"exit-code", "disk-check.jsonl", "io-floors.jsonl", "source-revision.txt"}:
                conf[Path(entry.name).name] = archive.extractfile(entry).read().decode()
        if conf["exit-code"].strip() != "0":
            raise ValueError("benchmark exited with an error")
        rows = map(json.loads, conf["io-floors.jsonl"].splitlines())
        total = 0
        for row in rows:
            expected.remove((row["method"], row["bytes"], row["queue_depth"]))
            count = samples.pop(row["samples_u64le_ns"])
            if len(row["lane_counts"]) != row["queue_depth"] or any(n < 1 for n in row["lane_counts"]):
                raise ValueError("missing I/O lane")
            if count != sum(row["lane_counts"]) or count != row["latency"]["count"]:
                raise ValueError("raw and histogram counts differ")
            total += count
        if samples or expected:
            raise ValueError("incomplete floor matrix")
        disk = json.loads(conf["disk-check.jsonl"])
        floor = next(p for p in disk["result"]["disk"] if p["method"]=="fdatasync" and p["bytes"]==4096)
        if floor["samples"] <= 0:
            raise ValueError("missing buffered baseline")
    return {"instance":manifest["type"], "disk":manifest["disk"], "zone":manifest["zone"],
            "run":directory.name, "raw_direct_io_observations":total, "direct_io_cases":90,
            "buffered_4k_samples":floor["samples"], "buffered_4k_p50_us":round(floor["p50_us"],3),
            "buffered_4k_p99_us":round(floor["p99_us"],3)}


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("directories", type=Path, nargs="+")
    for directory in cli.parse_args().directories:
        print(json.dumps(check(directory)))
