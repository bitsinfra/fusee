import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

const tarballs = process.argv.slice(2);
if (!tarballs.length) throw new Error("Pass the tested npm tarballs, native packages first.");
// The release owner can also publish these exact artifacts with interactive
// npm authentication. In CI, give that publication time to appear on npm.
let ownerDeadline;

for (const tarball of tarballs) {
  const path = resolve(tarball);
  const { name, version } = JSON.parse(
    execFileSync("tar", ["-xOf", path, "package/package.json"], { encoding: "utf8" }),
  );
  const spec = `${name}@${version}`;
  const expected = "sha512-" + createHash("sha512").update(readFileSync(path)).digest("base64");
  const url = `https://registry.npmjs.org/${encodeURIComponent(name)}/${encodeURIComponent(version)}`;
  async function verified() {
    const response = await fetch(url + "?check=" + Date.now());
    if (response.status === 404) return false;
    if (!response.ok) throw new Error(`Checking ${spec} on npm returned HTTP ${response.status}`);
    const published = await response.json();
    if (published.dist?.integrity !== expected) {
      throw new Error(`${spec} is already published with different bytes`);
    }
    console.log(`Verified the existing publication of ${spec}`);
    return true;
  }
  if (await verified()) continue;
  if (!ownerDeadline) {
    try {
      execFileSync("npm", ["publish", "--registry=https://registry.npmjs.org", "--provenance", "--access", "public", path], {
        encoding: "utf8", stdio: ["ignore", "inherit", "pipe"],
      });
      continue;
    } catch (e) {
      process.stderr.write(e.stderr ?? "");
      if (!process.env.GITHUB_ACTIONS || !/\b(?:E404|E403|ENEEDAUTH|EOTP)\b/.test(e.stderr ?? "")) throw e;
      ownerDeadline = Date.now() + 30 * 60_000;
      console.log("npm did not authorize this workflow to publish. Waiting up to 30 minutes for the release owner to publish the tested npm artifact.");
    }
  }
  while (!(await verified())) {
    if (Date.now() >= ownerDeadline) throw new Error(`Timed out waiting for ${spec} on npm`);
    await sleep(15_000);
  }
}
