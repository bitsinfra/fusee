import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const tarballs = process.argv.slice(2);
if (!tarballs.length) throw new Error("Pass the tested npm tarballs, native packages first.");

for (const tarball of tarballs) {
  const path = resolve(tarball);
  const { name, version } = JSON.parse(
    execFileSync("tar", ["-xOf", path, "package/package.json"], { encoding: "utf8" }),
  );
  const spec = `${name}@${version}`;
  const expected = "sha512-" + createHash("sha512").update(readFileSync(path)).digest("base64");
  const url = `https://registry.npmjs.org/${encodeURIComponent(name)}/${encodeURIComponent(version)}`;
  const response = await fetch(url);
  if (response.ok) {
    const published = await response.json();
    if (published.dist?.integrity !== expected) {
      throw new Error(`${spec} is already published with different bytes`);
    }
    console.log(`Verified the existing publication of ${spec}`);
  } else if (response.status === 404) {
    execFileSync("npm", ["publish", "--registry=https://registry.npmjs.org", "--provenance", "--access", "public", path], {
      stdio: "inherit",
    });
  } else {
    throw new Error(`Checking ${spec} on npm returned HTTP ${response.status}`);
  }
}
