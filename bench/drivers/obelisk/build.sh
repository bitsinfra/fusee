#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p build
: "${CARGO_TARGET_DIR:=$PWD/build/cargo}"
export CARGO_TARGET_DIR
cargo build --manifest-path activity/Cargo.toml --target wasm32-wasip2 --release --locked
cargo build --manifest-path workflow/Cargo.toml --target wasm32-unknown-unknown --release --locked
cp "$CARGO_TARGET_DIR/wasm32-wasip2/release/noop_activity.wasm" build/noop_activity.wasm
wasm-tools component new "$CARGO_TARGET_DIR/wasm32-unknown-unknown/release/chain_workflow.wasm" -o build/chain_workflow.wasm
