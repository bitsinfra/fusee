"""Verify acknowledged outputs after replaying real ext4 block writes up to crash markers."""
import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time


def command(*args, **options):
    return subprocess.run(list(map(str, args)), check=True, **options)


@contextmanager
def loop_file(path, megabytes):
    with path.open("xb") as stream:
        stream.truncate(megabytes << 20)
    dev = command("losetup", "--find", "--show", path, capture_output=True, text=True).stdout.strip()
    try:
        yield dev
    finally:
        command("losetup", "--detach", dev)


@contextmanager
def mounted(dev, mount):
    command("mount", "-t", "ext4", dev, mount)
    try:
        yield mount / "engine"
    finally:
        command("umount", mount)


def observed(path):
    acks, results = set(), {}
    for line in path.read_text().splitlines():
        parts = line.split()
        if parts[:1] == ["ACK"]:
            acks.add(parts[1])
        elif parts[:1] == ["RESULT"]:
            run, result = parts[1:]
            if results.setdefault(run, result) != result:
                raise RuntimeError(f"changing result for {run}")
    return acks, results


def effect_inputs(path):
    values = {}
    for line in path.read_text().splitlines():
        key, value = line.split()
        if values.setdefault(key, value) != value:
            raise RuntimeError(f"contradictory inputs for {key}")
    return values


def capture(binary, root, engine, mapping):
    stdout, effects = root / "stdout", root / "effects"
    cuts = []
    with stdout.open("xb") as output:
        proc = subprocess.Popen([binary, "child", engine, effects, "0", "3000"], stdout=output)
        try:
            for target in [8, 64, 180, 400]:
                deadline = time.monotonic() + 90
                while len(observed(stdout)[0]) < target:
                    if time.monotonic() >= deadline or proc.poll() is not None:
                        raise RuntimeError(f"workload failed before reaching {target} starts")
                    time.sleep(.002)
                proc.send_signal(signal.SIGSTOP)
                _, status = os.waitpid(proc.pid, os.WUNTRACED)
                if not os.WIFSTOPPED(status):
                    raise RuntimeError("workload exited before block cut")
                name = f"ack-{target}"
                command("dmsetup", "message", mapping, "0", "mark", name)
                acks, results = observed(stdout)
                snapshot = root / f"{name}.service"
                shutil.copyfile(effects, snapshot)
                cuts.append((name, acks, results, snapshot))
                proc.send_signal(signal.SIGCONT)
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait()
    return cuts


def check_cut(binary, replay, log, root, mount, cut, evidence):
    name, acks, old, service = cut
    with loop_file(root / f"{name}.img", 128) as replica:
        command(replay, "--log", log, "--replay", replica, "--end-mark", name)
        with mounted(replica, mount) as engine:
            output = root / f"{name}.stdout"
            with output.open("xb") as stream:
                command(binary, "child", engine, service, "0", "0", *sorted(acks), stdout=stream, timeout=90)
            _, new = observed(output)
            if set(new) != acks or any(new.get(k) != v for k, v in old.items()):
                raise RuntimeError(f"{name}: acknowledged starts or returned results lost")
            keys = effect_inputs(service)
            if not keys:
                raise RuntimeError("no external calls")
            evidence.mkdir(exist_ok=True, parents=True)
            (evidence / f"{name}.json").write_text(json.dumps({
                "acknowledged_ids": sorted(acks), "returned_before": old, "recovered_results": new,
                "external_inputs": keys
            }, sort_keys=True))
            print(json.dumps({"mark": name, "acknowledged": len(acks), "returned_before": len(old),
                              "completed": len(new), "external_keys": len(keys)}), flush=True)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("binary", type=Path)
    cli.add_argument("replay", type=Path)
    cli.add_argument("evidence", type=Path)
    args = cli.parse_args()
    with tempfile.TemporaryDirectory(prefix="fusee-dm-") as lab:
        root = Path(lab)
        mount = root / "mnt"
        mount.mkdir()
        with loop_file(root / "data.img", 128) as dev, loop_file(root / "log.img", 512) as log:
            mapping = f"fusee-log-{os.getpid()}"
            sectors = command("blockdev", "--getsz", dev, capture_output=True, text=True).stdout.strip()
            command("dmsetup", "create", mapping, "--table", f"0 {sectors} log-writes {dev} {log}")
            try:
                mapped = f"/dev/mapper/{mapping}"
                command("mkfs.ext4", "-q", "-E", "nodiscard,lazy_itable_init=0,lazy_journal_init=0", mapped)
                with mounted(mapped, mount) as engine:
                    engine.mkdir()
                    command("sync", "-f", mount)
                    cuts = capture(args.binary, root, engine, mapping)
            finally:
                command("dmsetup", "remove", mapping)
            for cut in cuts:
                check_cut(args.binary, args.replay, log, root, mount, cut, args.evidence)


if __name__ == "__main__":
    main()
