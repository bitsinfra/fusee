#!/bin/sh
# The tutorials' projects, as `fusee init` writes them, against this
# checkout: the Python project's test runs; the Rust project builds with the
# fusee crate from the tree; the TypeScript project's test runs against the
# Node package built in the tree. (docs/tutorial-*.md)
#
#   scripts/check_tutorials.sh [path/to/fusee]
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
fusee=${1:-$root/target/release/fusee}
# Absolute: the checks below run in other directories.
fusee=$(cd "$(dirname "$fusee")" && pwd)/$(basename "$fusee")
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# The starters' tests skip their five-second pause on the virtual clock: one
# that takes five seconds waited for it in real time.
quick() {
  start=$(date +%s)
  "$@"
  took=$(($(date +%s) - start))
  if [ "$took" -ge 5 ]; then
    echo "$* took ${took}s: the test waited for its pause in real time" >&2
    exit 1
  fi
}

mkdir "$work/py" && cd "$work/py"
"$fusee" init --lang python >/dev/null
quick env PYTHONPATH="$root/bindings/python/python" python3 test_workflows.py

mkdir "$work/ts" && cd "$work/ts"
"$fusee" init --lang ts >/dev/null
mkdir -p node_modules/@bitsinfra && ln -s "$root/bindings/node" node_modules/@bitsinfra/fusee
quick node --test workflows.test.ts

mkdir "$work/rs" && cd "$work/rs"
"$fusee" init --lang rust >/dev/null
sed -i.bak "s|^fusee = \".*\"|fusee = { path = \"$root/crates/fusee\" }|" Cargo.toml
cargo build --quiet
echo "tutorial projects ok"
