// Copies the cargo-built library next to the package as fusee.<platform>-<arch>.node.
import { copyFileSync, existsSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..", "..", "..");
const profile = process.env.PROFILE ?? "release";
const name = process.platform === "darwin" ? "libfusee_node.dylib" : process.platform === "win32" ? "fusee_node.dll" : "libfusee_node.so";
const src = join(root, "target", profile, name);
if (!existsSync(src)) throw new Error(`build the native library first: ${src} is missing`);
const dst = join(here, "..", `fusee.${process.platform}-${process.arch}.node`);
// A new file, not an overwrite: macOS kills a process that loads a signed
// library rewritten in place (its cached code signature no longer matches).
rmSync(dst, { force: true });
copyFileSync(src, dst);
console.log(`copied ${src} -> ${dst}`);
