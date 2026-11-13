#!/usr/bin/env bash
# Runs the five example applications (payment saga, agent loop, scheduled
# job, human approval, cart object) in all five languages, each in a fresh
# directory, and checks what each prints.
#
#   scripts/check_examples.sh            # every language
#   scripts/check_examples.sh py ts      # some
#
# Needs: the Python native module and the Node package built in-tree; for
# Rust and Go, CARGO_TARGET_DIR (default ./target) holding release builds
# of the examples and of libfusee_c; for Java, bindings/java/target/classes
# (mvn compile) and FUSEE_LIB_DIR.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
LANGS=${*:-py ts rs go java}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
failed=0

expect() { # name, output file, expected lines...
  local name=$1 out=$2
  shift 2
  for line in "$@"; do
    if ! grep -qxF -- "$line" "$out"; then
      echo "FAIL $name: no line '$line' in:"
      sed 's/^/    /' "$out"
      failed=1
      return
    fi
  done
  echo "ok   $name"
}

check() { # lang, example, command...
  local lang=$1 ex=$2
  shift 2
  local dir=$WORK/$lang-$ex out=$WORK/$lang-$ex.out
  mkdir -p "$dir"
  if ! (cd "$dir" && timeout 120 "$@") >"$out" 2>&1; then
    echo "FAIL $lang $ex: exited with an error:"
    sed 's/^/    /' "$out"
    failed=1
    return
  fi
  case $ex in
    payment_saga) expect "$lang $ex" "$out" "charged: ch-d1027e5a" ;;
    agent_loop) expect "$lang $ex" "$out" "Pack an umbrella (tool: rain in Lisbon)." ;;
    human_approval) expect "$lang $ex" "$out" "  asked finance to approve 12000 cents for ana" \
      "result: paid 12000 cents (approved by dana)" ;;
    cart_object) expect "$lang $ex" "$out" "items: 1" "items: 2" "total: 570" \
      "receipt: charged 570 cents for 2 items" ;;
    scheduled_job)
      if [ "$(grep -c ' sent$' "$out")" -ge 3 ] && grep -q '^next: [0-9]' "$out"; then
        echo "ok   $lang $ex"
      else
        echo "FAIL $lang $ex:"
        sed 's/^/    /' "$out"
        failed=1
      fi ;;
  esac
}

EXAMPLES="payment_saga agent_loop scheduled_job human_approval cart_object"
dashed() { echo "${1//_/-}"; }
camel() { echo "$1" | awk -F_ '{for (i = 1; i <= NF; i++) printf "%s", toupper(substr($i, 1, 1)) substr($i, 2)}'; }

for lang in $LANGS; do
  case $lang in
    py)
      for ex in $EXAMPLES; do
        check py "$ex" env PYTHONPATH="$ROOT/bindings/python/python" python3 "$ROOT/examples/python/$ex.py"
      done ;;
    ts)
      # The examples import "@bitsinfra/fusee": give them the in-tree package.
      mkdir -p "$WORK/node_modules/@bitsinfra"
      ln -sfn "$ROOT/bindings/node" "$WORK/node_modules/@bitsinfra/fusee"
      for ex in $EXAMPLES; do
        cp "$ROOT/examples/typescript/$(dashed "$ex").mjs" "$WORK/$ex.mjs"
        check ts "$ex" node "$WORK/$ex.mjs"
      done ;;
    rs)
      for ex in $EXAMPLES; do
        check rs "$ex" "$TARGET/release/examples/$ex"
      done ;;
    go)
      # The SDK links target/release/libfusee_c.a; another target directory
      # is linked as a system library from there.
      if [ "$TARGET" = "$ROOT/target" ]; then
        (cd "$ROOT/examples/go" && go build -o "$WORK/go/" ./...)
      else
        (cd "$ROOT/examples/go" && CGO_LDFLAGS="-L$TARGET/release" go build -tags fusee_system -o "$WORK/go/" ./...)
      fi
      for ex in $EXAMPLES; do
        check go "$ex" "$WORK/go/$ex"
      done ;;
    java)
      for ex in $EXAMPLES; do
        check java "$ex" env FUSEE_LIB_DIR="${FUSEE_LIB_DIR:-$TARGET/release}" \
          java -cp "$ROOT/bindings/java/target/classes" "$ROOT/examples/java/$(camel "$ex").java"
      done ;;
    *) echo "unknown language '$lang' (py ts rs go java)"; exit 2 ;;
  esac
done
exit $failed
