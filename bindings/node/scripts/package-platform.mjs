// Build the same native package for a private install or a registry release:
//   node scripts/package-platform.mjs <platform> <native library> <package dir> [fusee CLI binary]
// With the CLI, the package carries it as bin/fusee, which `npx fusee`
// (bin/fusee.cjs in the main package) runs.
import { chmodSync, copyFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
const [target, binary, destination, cli] = process.argv.slice(2);
if (!/^(darwin-(arm64|x64)|linux-(arm64|x64)-(gnu|musl))$/.test(target) || !binary || !destination)
  throw new Error("expected a supported platform, native library and package directory");
const pkg = JSON.parse(readFileSync(fileURLToPath(new URL("../package.json", import.meta.url)), "utf8"));
const [os, cpu, abi] = target.split("-");
const name = `fusee-${target}`;
if (pkg.optionalDependencies[name] !== pkg.version) throw new Error(`native package version differs from ${pkg.version}`);
mkdirSync(destination, { recursive: true });
const main = `fusee.${target}.node`;
copyFileSync(binary, join(destination, main));
const files = [main];
if (cli) {
  mkdirSync(join(destination, "bin"), { recursive: true });
  copyFileSync(cli, join(destination, "bin", "fusee"));
  chmodSync(join(destination, "bin", "fusee"), 0o755);
  files.push("bin");
}
writeFileSync(
  join(destination, "package.json"),
  JSON.stringify(
    {
      name,
      version: pkg.version,
      main,
      files,
      license: pkg.license,
      repository: pkg.repository,
      os: [os],
      cpu: [cpu],
      ...(abi ? { libc: [abi === "gnu" ? "glibc" : "musl"] } : {}),
    },
    null,
    2,
  ) + "\n",
);
