#!/usr/bin/env node
// `npx fusee …`: runs the fusee CLI that ships in this platform's package
// (fusee-<os>-<cpu>[-<libc>]), so `npx fusee dev` works with nothing else
// installed. FUSEE_CLI points at another binary.
"use strict";
const { spawnSync } = require("node:child_process");
const { existsSync } = require("node:fs");
const { dirname, join } = require("node:path");

function platform() {
  const { platform: os, arch } = process;
  if (os === "darwin") return `darwin-${arch}`;
  if (os === "linux") {
    const glibc = typeof process.report?.getReport === "function" && process.report.getReport().header.glibcVersionRuntime;
    return `linux-${arch}-${glibc ? "gnu" : "musl"}`;
  }
  return `${os}-${arch}`;
}

function binary() {
  if (process.env.FUSEE_CLI) return process.env.FUSEE_CLI;
  const name = `fusee-${platform()}`;
  try {
    const pkg = dirname(require.resolve(`${name}/package.json`));
    const bin = join(pkg, "bin", "fusee");
    if (existsSync(bin)) return bin;
  } catch {
    // not installed: fall through
  }
  // In the repository: the CLI cargo built.
  const local = join(__dirname, "..", "..", "..", "target", "release", "fusee");
  if (existsSync(local)) return local;
  console.error(
    `fusee: no CLI for ${platform()}: the package ${name} is not installed. ` +
      `Install it (npm install ${name}), or set FUSEE_CLI to a fusee binary (cargo install fusee-cli).`,
  );
  process.exit(127);
}

const r = spawnSync(binary(), process.argv.slice(2), { stdio: "inherit" });
if (r.error) {
  console.error(`fusee: ${r.error.message}`);
  process.exit(127);
}
process.exit(r.status ?? (r.signal ? 128 : 1));
