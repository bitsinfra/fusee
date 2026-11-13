# Sourced by host.sh on the target eight-vCPU host.
#
# The SDK cells, checked by bench/aws/check_sdk.py:
#   - floor: `bench floors`, the log's O_DIRECT + RWF_DSYNC write (floor-*.jsonl);
#   - Node and Python step: the matching chains (rust-, node-, python-*.jsonl),
#     and, warmed up, eight live workflows on eight shards: one chain with
#     seven waiting, and eight chains at once (sdk-live-*.jsonl), with the
#     flush inline (default) or pipelined off the interpreter thread
#     (FUSEE_SDK_FLUSH=pipeline), and Node's turns driven natively (default)
#     or one macrotask each (FUSEE_DRIVER=macrotask), interleaved by trial;
#   - Node fan-out of 100 (sdk-fanout-*.jsonl);
#   - Node worker threads on this disk at 1, 2, 4 and 8 threads
#     (workers-local.jsonl), and Python worker processes of one server over
#     shared memory (pyworkers.jsonl);
#   - a million simultaneous wakes on one Node thread (wakes-*.jsonl);
#   - the gap between an inline and a pipelined flush (flush-gap.jsonl).
# FUSEE_SDK_QUICK=1 runs one trial of each, for a short lifetime.
set -euo pipefail
node_image=node:24.14.0-bookworm-slim
docker pull -q "$node_image"
docker run --name fusee-sdk-node "$node_image" node --version
docker cp fusee-sdk-node:/usr/local/bin/node /usr/local/bin/node
docker rm fusee-sdk-node
docker run --rm -v /opt/fusee/bindings/node:/sdk -w /sdk "$node_image" \
    sh -euc 'npm install --no-audit --no-fund --omit=optional --ignore-scripts; ./node_modules/.bin/tsc -p tsconfig.json'
docker run "${common[@]}" "$image" cargo build --release --locked -p fusee-node -p fusee-python --lib
built=$(docker volume inspect fusee-build --format '{{.Mountpoint}}')/release
export FUSEE_NATIVE="$built/libfusee_node.node"
cp "$built/libfusee_node.so" "$FUSEE_NATIVE"
cp "$built/lib_native.so" /opt/fusee/bindings/python/python/fusee/_native.abi3.so
export PYTHONPATH=/opt/fusee/bindings/python/python
export FUSEE_CLI="$built/fusee"
# Profile-guided native libraries (FUSEE_SDK_PGO=0 turns it off): the
# native libraries built instrumented, trained on the Node chain, eight live chains,
# the fan-out, 100,000 wakes and the Python chain, then rebuilt with the
# merged profile. The default build stays at $built for the comparison at
# the end (base-*.jsonl).
base_native=/mnt/fusee/base-native.node
cp "$FUSEE_NATIVE" "$base_native"
if [[ ${FUSEE_SDK_PGO:-1} == 1 ]]; then
    mkdir -p /mnt/fusee/sdkpgo/raw
    docker run "${common[@]}" -e CARGO_TARGET_DIR=/build/sdk-pgo-gen -e RUSTFLAGS="-Cprofile-generate=/data/sdkpgo/raw" \
        "$image" cargo build --release --locked -p fusee-node -p fusee-python --lib >"$FUSEE_RESULTS/sdk-pgo-gen-build.log" 2>&1
    gen_dir=$(docker volume inspect fusee-build --format '{{.Mountpoint}}')/sdk-pgo-gen/release
    cp "$gen_dir/libfusee_node.so" /mnt/fusee/sdkpgo/gen.node
    cp "$gen_dir/lib_native.so" /opt/fusee/bindings/python/python/fusee/_native.abi3.so
    export LLVM_PROFILE_FILE=/mnt/fusee/sdkpgo/raw/%m_%p.profraw
    FUSEE_NATIVE=/mnt/fusee/sdkpgo/gen.node node bench/drivers/fusee/node.mjs /mnt/fusee/pgo-node 1000 2 >/dev/null 2>>"$FUSEE_RESULTS/sdk-pgo-train.log" || true
    FUSEE_NATIVE=/mnt/fusee/sdkpgo/gen.node node bench/drivers/fusee/node_cells.mjs /mnt/fusee/pgo-cells live 8 8 0 300 1 >/dev/null 2>>"$FUSEE_RESULTS/sdk-pgo-train.log" || true
    FUSEE_NATIVE=/mnt/fusee/sdkpgo/gen.node node bench/drivers/fusee/node_cells.mjs /mnt/fusee/pgo-cells fanout 8 100 200 >/dev/null 2>>"$FUSEE_RESULTS/sdk-pgo-train.log" || true
    FUSEE_NATIVE=/mnt/fusee/sdkpgo/gen.node node bench/drivers/fusee/node_wakes.mjs /mnt/fusee/pgo-wakes 100000 8 >/dev/null 2>>"$FUSEE_RESULTS/sdk-pgo-train.log" || true
    python3 bench/drivers/fusee/python.py /mnt/fusee/pgo-py 1000 2 >/dev/null 2>>"$FUSEE_RESULTS/sdk-pgo-train.log" || true
    unset LLVM_PROFILE_FILE
    ls -la /mnt/fusee/sdkpgo/raw >"$FUSEE_RESULTS/sdk-pgo-raw.txt" 2>&1
    docker run "${common[@]}" "$image" sh -c 'rustup component add llvm-tools >/dev/null 2>&1;
            p=$(ls "$(rustc --print sysroot)"/lib/rustlib/*/bin/llvm-profdata | head -1);
            "$p" merge -o /data/sdkpgo/merged.profdata /data/sdkpgo/raw && ls -la /data/sdkpgo/merged.profdata' \
        >"$FUSEE_RESULTS/sdk-pgo-merge.log" 2>&1 || true
    if [[ -s /mnt/fusee/sdkpgo/merged.profdata ]]; then
        docker run "${common[@]}" -e CARGO_TARGET_DIR=/build/sdk-pgo-use -e RUSTFLAGS="-Cprofile-use=/data/sdkpgo/merged.profdata" \
            "$image" cargo build --release --locked -p fusee-node -p fusee-python --lib >"$FUSEE_RESULTS/sdk-pgo-use-build.log" 2>&1
        use_dir=$(docker volume inspect fusee-build --format '{{.Mountpoint}}')/sdk-pgo-use/release
        export FUSEE_NATIVE=/mnt/fusee/sdkpgo/use.node
        cp "$use_dir/libfusee_node.so" "$FUSEE_NATIVE"
        cp "$use_dir/lib_native.so" /opt/fusee/bindings/python/python/fusee/_native.abi3.so
        echo pgo >"$FUSEE_RESULTS/sdk-native.txt"
    else
        cp "$built/lib_native.so" /opt/fusee/bindings/python/python/fusee/_native.abi3.so
        echo default >"$FUSEE_RESULTS/sdk-native.txt"
    fi
    rm -rf /mnt/fusee/pgo-node /mnt/fusee/pgo-cells /mnt/fusee/pgo-wakes /mnt/fusee/pgo-py
fi
export FUSEE_BENCH_CHAIN_RUNS=2
# Cross-language and SDK failover checks. A failure is recorded (exit code
# in the .exit file, output in the log) and reported by check_sdk.py; the
# measurements below still run.
python3 conformance/remote.py > "$FUSEE_RESULTS/cross-language.log" 2>&1 \
    && echo 0 > "$FUSEE_RESULTS/cross-language.exit" || echo $? > "$FUSEE_RESULTS/cross-language.exit"
python3 conformance/replica.py > "$FUSEE_RESULTS/sdk-failover.log" 2>&1 \
    && echo 0 > "$FUSEE_RESULTS/sdk-failover.exit" || echo $? > "$FUSEE_RESULTS/sdk-failover.exit"
trials=(0 1 2 3 4)
if [[ ${FUSEE_SDK_QUICK:-0} == 1 ]]; then trials=(0); fi
engines=(rust node python)
if [[ ${FUSEE_SDK_PAIRED:-0} == 1 ]]; then
    python3 - <<'PYCONTROL'
from pathlib import Path
original = Path("bindings/node/dist/index.js")
source = original.read_text()
gate = "this.e.native.stepGate(shard, run, gate, notBefore, true)"
if source.count(gate) != 1:
    raise ValueError("cannot identify the Node comparison gate")
original.with_name("gate-control.js").write_text(source.replace(gate, gate.replace("true", "false")))
PYCONTROL
    sha256sum bindings/node/dist/{index,gate-control}.js "$FUSEE_NATIVE" > "$FUSEE_RESULTS/sdk-pair.sha256"
    cp bindings/node/dist/{index,gate-control}.js "$FUSEE_RESULTS/"
fi
for shards in 1 8; do
    export FUSEE_SDK_SHARDS=$shards
    for trial in "${trials[@]}"; do
        bench floors /data/sdk-floors > "$FUSEE_RESULTS/floor-$shards-$trial.jsonl"
        # Repeat one full trial with 20 measured workflows on the same device.
        populations=(2)
        if [[ $trial == 0 ]]; then populations+=(20); fi
        for runs in "${populations[@]}"; do
            export FUSEE_BENCH_CHAIN_RUNS=$runs
            for ((k=0;k<3;k++)); do
                engine=${engines[(trial+k)%3]}
                variants=("$engine")
                if [[ $engine == node && ${FUSEE_SDK_PAIRED:-0} == 1 ]]; then
                    if (( trial%2 )); then variants=(node node-control); else variants=(node-control node); fi
                fi
                for variant in "${variants[@]}"; do
                    tag="$variant-$shards-$runs-$trial"
                    unset FUSEE_BENCH_NODE_SDK
                    if [[ $variant == node-control ]]; then export FUSEE_BENCH_NODE_SDK=/opt/fusee/bindings/node/dist/gate-control.js; fi
                    if [[ $engine == rust ]]; then
                        "$built/fusee-bench" run chain --mode local --shards "$shards" --dir "/mnt/fusee/$tag" --trials 1 > "$FUSEE_RESULTS/$tag.jsonl"
                    else
                        for n in 1 10 100 1000; do
                            data="/mnt/fusee/$tag-$n"
                            if [[ $engine == node ]]; then
                                node bench/drivers/fusee/node.mjs "$data" "$n" "$runs"
                            else
                                python3 bench/drivers/fusee/python.py "$data" "$n" "$runs"
                            fi
                        done > "$FUSEE_RESULTS/$tag.jsonl"
                    fi
                done
            done
        done
    done
done
unset FUSEE_BENCH_NODE_SDK

# The SDK cells, interleaved: within a trial each cell runs every variant, in an
# order that rotates with the trial, so drift on the host spreads evenly.
export FUSEE_SDK_SHARDS=8
node_variants=(default pipeline macrotask inline)
variant_env() {
    case $1 in
        default) echo "" ;;
        pipeline) echo "FUSEE_SDK_FLUSH=pipeline" ;;
        macrotask) echo "FUSEE_DRIVER=macrotask" ;;
        inline) echo "FUSEE_SDK_FLUSH=inline" ;;
    esac
}
for trial in "${trials[@]}"; do
    bench floors /data/sdk-floors > "$FUSEE_RESULTS/floor-cells-$trial.jsonl"
    for ((k=0;k<${#node_variants[@]};k++)); do
        v=${node_variants[(trial+k)%${#node_variants[@]}]}
        e=$(variant_env "$v")
        # Eight live workflows: one chain with seven waiting, then eight chains.
        env $e node bench/drivers/fusee/node_cells.mjs "/mnt/fusee/cells-$v" live 8 1 7 1000 3 \
            > "$FUSEE_RESULTS/sdk-live-1-7-$v-$trial.jsonl"
        env $e node bench/drivers/fusee/node_cells.mjs "/mnt/fusee/cells-$v" live 8 8 0 1000 3 \
            > "$FUSEE_RESULTS/sdk-live-8-0-$v-$trial.jsonl"
        env $e node bench/drivers/fusee/node_cells.mjs "/mnt/fusee/cells-$v" fanout 8 100 500 \
            > "$FUSEE_RESULTS/sdk-fanout-$v-$trial.jsonl"
    done
    # The Python and Node 1,000-step chains with the flush pipelined, beside
    # the default cells above (python-8-2-*, node-8-2-*).
    for v in pipeline default; do
        FUSEE_SDK_FLUSH=$([[ $v == pipeline ]] && echo pipeline || echo inline) \
            python3 bench/drivers/fusee/python.py "/mnt/fusee/py-$v-$trial" 1000 2 > "$FUSEE_RESULTS/sdk-python-$v-$trial.jsonl"
        FUSEE_SDK_FLUSH=$([[ $v == pipeline ]] && echo pipeline || echo inline) \
            node bench/drivers/fusee/node.mjs "/mnt/fusee/node-$v-$trial" 1000 2 > "$FUSEE_RESULTS/sdk-node-$v-$trial.jsonl"
    done
done

# Node worker threads with the real disk: steps per second per thread and the
# step gap p99, closed loop, 1 to 256 chains per thread.
FUSEE_BENCH_DIR=/mnt/fusee FUSEE_JSON=1 FUSEE_STEPS=200000 \
    node bindings/node/bench-workers.mjs local 1,2,4,8 1,16,64,256 > "$FUSEE_RESULTS/workers-local.jsonl"
FUSEE_JSON=1 node bindings/node/bench-workers.mjs memory 1,2,4,8 16,64 > "$FUSEE_RESULTS/workers-memory.jsonl"
# Python worker processes of one server over shared memory, one shard each.
python3 bench/drivers/fusee/python_workers.py /mnt/fusee/pyworkers 1,2,4,8 16 4000 > "$FUSEE_RESULTS/pyworkers.jsonl"
# Simultaneous wakes on one Node thread (eight shards on its one engine).
for n in 100000 1000000; do
    node bench/drivers/fusee/node_wakes.mjs "/mnt/fusee/wakes-$n" "$n" 8 > "$FUSEE_RESULTS/wakes-$n.jsonl"
done
# The default native build on the Node cells, beside the run above
# (base-*.jsonl): with FUSEE_SDK_PGO=1 the run above used the
# profile-guided one.
if [[ -s "$base_native" && $(cat "$FUSEE_RESULTS/sdk-native.txt" 2>/dev/null) == pgo ]]; then
    for t in 0 1; do
        FUSEE_NATIVE="$base_native" node bench/drivers/fusee/node_cells.mjs /mnt/fusee/base-cells fanout 8 100 500 \
            > "$FUSEE_RESULTS/base-sdk-fanout-$t.jsonl"
    done
    FUSEE_NATIVE="$base_native" node bench/drivers/fusee/node_cells.mjs /mnt/fusee/base-cells live 8 8 0 1000 3 \
        > "$FUSEE_RESULTS/base-sdk-live-8-0.jsonl"
    FUSEE_NATIVE="$base_native" node bench/drivers/fusee/node_wakes.mjs /mnt/fusee/base-wakes 1000000 8 \
        > "$FUSEE_RESULTS/base-wakes-1000000.jsonl"
fi
# The gap between an inline and a pipelined flush on this device.
python3 bench/drivers/fusee/flush_gap.py /mnt/fusee/flush-gap 20000 35 > "$FUSEE_RESULTS/flush-gap.jsonl"

# Record a separate V8 CPU profile after all timing trials on the same host.
# Sampling perturbs performance; this is a diagnostic, not another trial.
mkdir -p "$FUSEE_RESULTS/node-profile"
FUSEE_SDK_SHARDS=8 node --cpu-prof --cpu-prof-interval=100 --cpu-prof-dir="$FUSEE_RESULTS/node-profile" \
    bench/drivers/fusee/node.mjs /mnt/fusee/node-profile 1000 12 >"$FUSEE_RESULTS/node-profile/chain.json"
