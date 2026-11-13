#!/usr/bin/env bash
# Rehearses the release locally: builds each package from this checkout for
# this machine and uses it as a user would, publishing nothing. Each step reports ok, failed or skipped (a tool not
# installed here).
#
#   scripts/rehearse-release.sh
#
# Uses CARGO_TARGET_DIR (default ./target) for the release builds.
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
summary=()
step() { summary+=("$1  $2"); echo "== $2: $1"; }

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) PLATFORM=darwin-arm64 ;;
  Darwin-x86_64) PLATFORM=darwin-x64 ;;
  Linux-x86_64) PLATFORM=linux-x64-gnu ;;
  Linux-aarch64) PLATFORM=linux-arm64-gnu ;;
  *) echo "unsupported host"; exit 2 ;;
esac
LIBEXT=so; [ "$(uname -s)" = Darwin ] && LIBEXT=dylib

echo "== building the CLI and the native libraries (release)"
(cd "$ROOT" && cargo build --release --locked -p fusee-cli -p fusee-node -p fusee-python -p fusee-c) || exit 1
CLI=$TARGET/release/fusee

# npm: the main package and this platform's package, with the CLI in it.
(
  set -e
  cd "$ROOT/bindings/node"
  npx tsc -p tsconfig.json
  node scripts/package-platform.mjs "$PLATFORM" "$TARGET/release/libfusee_node.$LIBEXT" "$WORK/npm-$PLATFORM" "$CLI"
  sdk_tarball=$(npm pack --silent --pack-destination "$WORK")
  platform_tarball=$(cd "$WORK/npm-$PLATFORM" && npm pack --silent --pack-destination "$WORK")
  mkdir -p "$WORK/npm-app" && cd "$WORK/npm-app"
  npm init -y >/dev/null
  npm install --silent --offline --no-audit --no-fund "$WORK/$sdk_tarball" "$WORK/$platform_tarball" >/dev/null
  npx --no-install fusee version
  node --input-type=module -e "import { Engine, workflow } from '@bitsinfra/fusee'; const w = workflow('w', async (ctx) => ctx.step('s', () => 42)); const e = await Engine.open({ durability: 'memory', workflows: [w] }); console.log('npm: result', await (await e.start(w, 'r', null)).result()); await e.close();"
)
if [ "$?" = 0 ]; then step ok "npm (npx fusee, import fusee)"; else step failed "npm"; fi

# PyPI: a wheel with the CLI in it, installed in a fresh virtualenv.
if command -v maturin >/dev/null 2>&1; then
  (
    set -e
    mkdir -p "$ROOT/bindings/python/python/fusee/bin"
    cp "$CLI" "$ROOT/bindings/python/python/fusee/bin/fusee"
    trap 'rm -rf "$ROOT/bindings/python/python/fusee/bin"' EXIT
    (cd "$ROOT/bindings/python" && maturin build --release --out "$WORK/wheels" >/dev/null)
    python3 -m venv "$WORK/venv"
    "$WORK/venv/bin/pip" install --quiet "$WORK"/wheels/*.whl
    "$WORK/venv/bin/fusee" version
    "$WORK/venv/bin/python" -c "import asyncio, fusee
@fusee.workflow('w')
async def w(ctx, _): return await ctx.step('s', lambda: 42)
async def main():
    async with await fusee.Engine.open(durability='memory', workflows=[w]) as e:
        print('pypi: result', await (await e.start(w, 'r', None)).result())
asyncio.run(main())"
  )
  if [ "$?" = 0 ]; then step ok "PyPI wheel (fusee on PATH, import fusee)"; else step failed "PyPI wheel"; fi
else
  step skipped "PyPI wheel (maturin not installed: pip install maturin)"
fi

# crates.io: what each crate would upload.
(
  set -e
  cd "$ROOT"
  for c in fusee-core fusee fusee-server fusee-sim fusee-cli; do
    n=$(cargo package --list --allow-dirty -p "$c" 2>/dev/null | wc -l)
    [ "$n" -gt 0 ] || { echo "no files for $c"; exit 1; }
    echo "crates.io: $c: $n files"
  done
)
if [ "$?" = 0 ]; then step ok "crates.io (package lists)"; else step failed "crates.io (package lists)"; fi

# Maven: the jar with its JNI library.
if command -v mvn >/dev/null 2>&1; then
  (cd "$ROOT/bindings/java" && FUSEE_LIB_DIR="$TARGET/release" mvn -B -q -o -DskipTests package \
    && ls target/*.jar) && step ok "Maven (jar)" || step failed "Maven (jar)"
else
  step skipped "Maven (mvn not installed)"
fi

# Homebrew: the formula over this machine's CLI (as each platform's).
(
  set -e
  mkdir -p "$WORK/brew"
  for p in darwin-arm64 darwin-x64 linux-x64-musl linux-arm64-musl; do cp "$CLI" "$WORK/brew/fusee-$p"; done
  python3 "$ROOT/scripts/homebrew-formula.py" 0.0.0-rehearsal "$WORK/brew" > "$WORK/fusee.rb"
  if command -v ruby >/dev/null 2>&1; then ruby -c "$WORK/fusee.rb"; fi
)
if [ "$?" = 0 ]; then step ok "Homebrew formula"; else step failed "Homebrew formula"; fi

# The container image.
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  (cd "$ROOT" && docker build -q -t fusee:rehearsal . && docker run --rm --entrypoint fusee fusee:rehearsal version) \
    && step ok "container image" || step failed "container image"
else
  step skipped "container image (no docker daemon)"
fi

echo
printf '%s\n' "${summary[@]}"
! printf '%s\n' "${summary[@]}" | grep -q '^failed'
