"""The flush-gap experiment: why an inline flush is slower after interpreter
work than back to back.

    python3 flush_gap.py <dir> [writes] [gap µs]

Writes 4 KiB blocks with `O_DIRECT` and `RWF_DSYNC` (the shared log's
default write) to one preallocated file, timing each write, in three
interleaved modes: back to back; with `gap` µs of spinning between writes
(the device sees the same gap as under an SDK, the core stays warm); and
with `gap` µs of cache-thrashing work between writes (a sweep over 32 MiB,
as an interpreter's work leaves the caches). If only the thrashing mode is
slower, the inflation is the core's caches and a warm log thread avoids it;
if both are, it is the gap. One JSON line with each mode's percentiles.
Linux only (O_DIRECT, RWF_DSYNC).
"""
import json
import mmap
import os
import sys
import time


def pct(v, q):
    return v[min(len(v) - 1, int(round((len(v) - 1) * q)))]


def main(folder, writes, gap_us):
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, "flush-gap.bin")
    blocks = 4096
    with open(path, "wb") as f:
        f.truncate(blocks * 4096)
        os.fsync(f.fileno())
    fd = os.open(path, os.O_RDWR | os.O_DIRECT)
    buf = mmap.mmap(-1, 4096)
    buf.write(b"\x5a" * 4096)
    thrash = bytearray(32 << 20)
    gap_ns = int(gap_us * 1000)
    modes = {"back_to_back": [], "spin_gap": [], "thrash_gap": []}
    names = list(modes)
    k = 0
    try:
        for i in range(writes * len(names)):
            mode = names[i % len(names)]
            if mode == "spin_gap":
                until = time.perf_counter_ns() + gap_ns
                while time.perf_counter_ns() < until:
                    pass
            elif mode == "thrash_gap":
                until = time.perf_counter_ns() + gap_ns
                j = (i * 4099) % len(thrash)
                while time.perf_counter_ns() < until:
                    # Touch a cache line every 64 bytes, page-crossing.
                    for _ in range(64):
                        thrash[j] = (thrash[j] + 1) & 0xFF
                        j = (j + 4160) % len(thrash)
            a = time.perf_counter_ns()
            os.pwritev(fd, [buf], (k % blocks) * 4096, os.RWF_DSYNC)
            modes[mode].append((time.perf_counter_ns() - a) / 1000)
            k += 1
    finally:
        os.close(fd)
        os.unlink(path)
    out = {"workload": "flush-gap", "gap_us": gap_us, "writes_per_mode": writes, "method": "O_DIRECT + RWF_DSYNC write"}
    for mode, v in modes.items():
        v = sorted(v[len(v) // 20:])  # the first twentieth warms up
        out[mode] = {"p50_us": pct(v, .5), "p90_us": pct(v, .9), "p99_us": pct(v, .99), "count": len(v)}
    print(json.dumps(out))


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 20000, float(sys.argv[3]) if len(sys.argv) > 3 else 35)
