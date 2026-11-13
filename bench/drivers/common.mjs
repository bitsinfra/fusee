// Shared measurement for the other engines' drivers: identical to fusee-bench.
import { execSync } from "node:child_process";
import os from "node:os";
import { readFileSync, existsSync } from "node:fs";
import { dirname, join, parse } from "node:path";
import { createRequire } from "node:module";

export function sdkVersion(moduleUrl, name) {
  let dir = dirname(createRequire(moduleUrl).resolve(name));
  while (dir !== parse(dir).root) {
    const manifest = join(dir, "package.json");
    if (existsSync(manifest)) {
      const pkg = JSON.parse(readFileSync(manifest, "utf8"));
      if (pkg.name === name) return pkg.version;
    }
    dir = dirname(dir);
  }
  throw new Error(`could not identify installed version of ${name}`);
}

const stamps = [];
const transitions = [];
export function stamp(fn) {
  const start = process.hrtime.bigint();
  const value = fn();
  stamps.push([start, process.hrtime.bigint()]);
  return value;
}

function pct(sorted, q) {
  return sorted.length ? sorted[Math.min(sorted.length - 1, Math.round((sorted.length - 1) * q))] : 0;
}

export function finishRun(steps, start, wholeUs, warmup = false) {
  const elapsed = Number(process.hrtime.bigint() - start) / 1000;
  const gaps = recordRun(steps);
  if (!warmup) {
    transitions.push(gaps);
    wholeUs.push(elapsed);
  }
  return {workflow_us: elapsed, transition_samples_us: gaps};
}

function recordRun(steps) {
  if (!Number.isInteger(steps) || steps < 1 || stamps.length !== steps) {
    throw new Error(`expected ${steps} step timestamps in this run, got ${stamps.length}`);
  }
  const gaps = [];
  for (let i = 1; i < stamps.length; i++) {
    const gap = Number(stamps[i][0] - stamps[i - 1][1]) / 1000;
    if (gap < 0) throw new Error("timestamps went backwards");
    gaps.push(gap);
  }
  stamps.length = 0;
  return gaps;
}

export function report(engine, version, durability, steps, wholeUs, detail = {}) {
  if (stamps.length || !transitions.length || transitions.length !== wholeUs.length) {
    throw new Error("incomplete workflow measurements");
  }
  const gaps = transitions.flat().sort((a, b) => a - b);
  const s = (v) => ({ count: v.length, p50_us: pct(v, 0.5), p90_us: pct(v, 0.9), p99_us: pct(v, 0.99), p999_us: pct(v, 0.999), max_us: v.at(-1) ?? 0 });
  let load = "";
  try { load = execSync("uptime").toString().trim(); } catch {}
  console.log(JSON.stringify({
    machine: { os: process.platform, arch: process.arch, cpu: os.cpus()[0]?.model, cores: os.cpus().length, host_load: load, instance_type: process.env.FUSEE_INSTANCE_TYPE ?? null, zone: process.env.FUSEE_ZONE ?? null, revision: process.env.FUSEE_REVISION ?? null },
    engine, version, durability, workload: "chain",
    result: [{ steps, step_to_step: s(gaps), workflow: s([...wholeUs].sort((a,b) => a-b)),
      transition_samples_us: transitions, workflow_samples_us: wholeUs, ...detail }],
  }));
}
