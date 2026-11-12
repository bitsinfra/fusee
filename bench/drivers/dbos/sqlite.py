"""DBOS on SQLite, one locally committed workflow step per call."""
import argparse
import importlib.metadata
import json
import os
import platform
import time
from pathlib import Path

from dbos import DBOS
from sqlalchemy import create_engine, event


def summary(samples):
    ordered = sorted(samples)
    def percentile(q):
        return ordered[int((len(ordered) - 1) * q + 0.5)] if ordered else 0
    return {"count": len(samples), "p50_us": percentile(.5), "p90_us": percentile(.9),
            "p99_us": percentile(.99), "p999_us": percentile(.999), "max_us": percentile(1)}


def configure_dbos(database):
    engine = create_engine(f"sqlite:///{database.resolve()}")
    @event.listens_for(engine, "connect")
    def configure(connection, _record):
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        if connection.execute("PRAGMA synchronous").fetchone() != (2,):
            raise RuntimeError("SQLite FULL durability could not be established")
    DBOS(config={"name":"fusee-bench", "system_database_engine":engine, "enable_otlp":False})


def main():
    cli = argparse.ArgumentParser()
    cli.add_argument("database", type=Path)
    cli.add_argument("steps", type=int)
    cli.add_argument("runs", type=int)
    args = cli.parse_args()
    configure_dbos(args.database)
    stamps = []
    @DBOS.step()
    def noop(value):
        begin = time.monotonic_ns()
        result = value + 1
        stamps.append((begin, time.monotonic_ns()))
        return result
    @DBOS.workflow()
    def chain(n):
        value = 0
        for _ in range(n):
            value = noop(value)
        return value
    DBOS.launch()
    gaps, whole = [], []
    try:
        if chain(args.steps) != args.steps:
            raise RuntimeError("incorrect warmup result")
        for _ in range(args.runs):
            stamps.clear()
            start = time.monotonic_ns()
            if chain(args.steps) != args.steps or len(stamps) != args.steps:
                raise RuntimeError("unexpected step count or result")
            whole.append((time.monotonic_ns() - start) / 1000)
            samples = [(begin - end) / 1000 for (_, end), (begin, _) in zip(stamps, stamps[1:])]
            if any(gap < 0 for gap in samples):
                raise RuntimeError("dependent step timestamps overlapped")
            gaps.append(samples)
    finally:
        DBOS.destroy()
    print(json.dumps({"machine":{"os":platform.system(),"arch":platform.machine(),"kernel":platform.release(),"cores":os.cpu_count(),"host_load":os.getloadavg()},
        "engine":"dbos-sqlite","version":importlib.metadata.version("dbos"),"durability":"SQLite WAL, synchronous=FULL on every writer connection",
        "workload":"chain","result":[{"steps":args.steps,"step_to_step":summary([g for trial in gaps for g in trial]),"workflow":summary(whole),
            "transition_samples_us":gaps,"workflow_samples_us":whole,"transition_boundary":"body-end-to-next-body-start"}]}))


if __name__ == "__main__":
    main()
