#!/usr/bin/env bash
# Runs exclusively on the new, disposable EC2 instance created by host_run.py or cluster_run.py.
set -euo pipefail
: "${FUSEE_DATA:?}" "${FUSEE_RESULTS:?}" "${FUSEE_CASE:?}" "${FUSEE_INDEX:?}"
cd /opt/fusee
# A cluster node learns its peers from a presigned URL. Fetch it first: the
# URL is signed with the controller's credentials, which may be short-lived
# (aws login issues 15-minute credentials), so it may not outlive the build.
if [[ $FUSEE_CASE == cluster ]]; then
    curl -fsS --retry 40 --retry-all-errors --retry-delay 3 --retry-max-time 600 "$FUSEE_PEER_URL" > "$FUSEE_RESULTS/peers.json"
fi
export DEBIAN_FRONTEND=noninteractive
apt-get -qq update
apt-get -qq install -y docker.io jq nvme-cli
systemctl enable --now docker
case "$FUSEE_DATA" in
    instance) model="Amazon EC2 NVMe Instance Storage" ;;
    gp3|io2) model="Amazon Elastic Block Store" ;;
    *) echo "unknown storage class $FUSEE_DATA"; exit 2 ;;
esac
mapfile -t candidates < <(lsblk -Jdnpo PATH,MODEL | jq -r --arg model "$model" '.blockdevices[]|select(.model!=null and (.model|rtrimstr(" ")|startswith($model)))|.path')
device=
for candidate in "${candidates[@]}"; do
    if ! lsblk -nrpo MOUNTPOINTS "$candidate" | grep -qE '[^[:space:]]'; then
        device=$candidate
        break
    fi
done
test -n "$device" || { echo "no empty $FUSEE_DATA benchmark disk"; lsblk; exit 1; }
# Refuse to format a disk carrying any prior filesystem or partition signature.
test -z "$(wipefs -n --noheadings --output TYPE "$device")"
mkfs.ext4 -q -E lazy_itable_init=0,lazy_journal_init=0 "$device"
mkdir -p /mnt/fusee
mount "$device" /mnt/fusee
lsblk -Jpo NAME,MODEL,SIZE,SERIAL,FSTYPE,MOUNTPOINTS >"$FUSEE_RESULTS/disks.json"
findmnt -J /mnt/fusee >"$FUSEE_RESULTS/filesystem.json"
lscpu --json >"$FUSEE_RESULTS/cpu.json"
uname -a >"$FUSEE_RESULTS/kernel.txt"
cp SOURCE-REVISION "$FUSEE_RESULTS/source-revision.txt"
if [[ $FUSEE_CASE == comparisons || $FUSEE_CASE == external ]]; then
    # Put every container database, including anonymous PostgreSQL volumes,
    # on the same measured device as Fusee and SQLite.
    systemctl stop docker docker.socket
    mkdir -p /etc/docker
    printf '{"data-root":"/mnt/fusee/docker"}\n' >/etc/docker/daemon.json
    systemctl start docker
    test "$(docker info -f '{{.DockerRootDir}}')" = /mnt/fusee/docker
fi
image=rust:1.95-trixie
docker pull -q "$image"
docker image inspect "$image" --format '{{json .RepoDigests}}' >"$FUSEE_RESULTS/build-image.json"
common=(--rm --network host --security-opt seccomp=unconfined --mount type=bind,source=/opt/fusee,target=/src,readonly
    --mount type=bind,source=/mnt/fusee,target=/data --mount type=bind,source="$FUSEE_RESULTS",target=/results
    --mount type=volume,source=fusee-build,target=/build -e CARGO_TARGET_DIR=/build -w /src
    -e FUSEE_INSTANCE_TYPE -e FUSEE_ZONE -e FUSEE_REVISION -e FUSEE_USD_PER_HOUR
    -e FUSEE_LOG_IN_FLIGHT -e FUSEE_LOG_PAD -e FUSEE_LOG_SECOND -e FUSEE_SPIN -e FUSEE_DRIVE_FLUSH_EVERY -e FUSEE_LOG_POLL -e FUSEE_SHARD_SPIN -e FUSEE_ADMIT_ROUND -e FUSEE_SHARD_NICE -e FUSEE_BENCH_INLINE)
docker run "${common[@]}" "$image" cargo build --release --locked -p fusee-bench -p fusee-cli -p fusee-sim
bench() { docker run "${common[@]}" "$image" /build/release/fusee-bench "$@"; }
# The engine's stall tracer, on by default at 1 ms (FUSEE_STALL_US=0 turns it
# off). Each cell's trace is archived beside its results as <cell>.stalls.jsonl
# and summarized in its JSON ("stalls").
stall=(--stall-us "${FUSEE_STALL_US:-1000}")
traced() {
    local label=$1
    shift
    bench "$@" "${stall[@]}" --stall-trace "/results/$label.stalls.jsonl" >"$FUSEE_RESULTS/$label.jsonl"
    # FUSEE_TRIM=1: each point leaves the disk as it found it. A point
    # writes gigabytes of log and the next deletes them; without a trim the
    # drive keeps those blocks as live data and its writes slow down point
    # after point. A running engine reuses its segments in place instead.
    if [[ ${FUSEE_TRIM:-0} == 1 ]]; then
        rm -rf /mnt/fusee/runs
        fstrim -v /mnt/fusee >>"$FUSEE_RESULTS/fstrim.log" 2>&1 || true
    fi
}
# Hardware counters and `perf` for the micro and local cases, best effort:
# a failure here only leaves those files out. perf_event_paranoid -1 lets
# the containers count (the step-cost example and the harness's per-thread
# counters open perf_event_open themselves) and sample kernel stacks; the
# host is disposable. `perf` comes from Debian's linux-perf in an image made
# from the build image, so it reads the binaries where they were built.
perfimg=
perf_setup() {
    sysctl -qw kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 || true
    cat /proc/sys/kernel/perf_event_paranoid >"$FUSEE_RESULTS/perf-event-paranoid.txt" 2>&1 || true
    docker rm -f fusee-perf-setup >/dev/null 2>&1 || true
    if docker run --name fusee-perf-setup "$image" sh -c \
        'apt-get -qq update >/dev/null && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y linux-perf >/dev/null && perf --version' \
        >"$FUSEE_RESULTS/perf-version.txt" 2>&1 && docker commit fusee-perf-setup fusee-perf >/dev/null; then
        perfimg=fusee-perf
    fi
    docker rm -f fusee-perf-setup >/dev/null 2>&1 || true
}
# Runs a command in the perf image; `PEX_TRACE=1 pex ...` also mounts
# tracefs, for the syscall tracepoints.
pex() {
    local -a trace=()
    if [[ ${PEX_TRACE:-0} == 1 && -d /sys/kernel/tracing/events ]]; then
        trace=(--mount type=bind,source=/sys/kernel/tracing,target=/sys/kernel/tracing)
    fi
    docker run "${common[@]}" "${trace[@]}" -e TMPDIR=/data/tmp "$perfimg" sh -c 'mkdir -p /data/tmp /data/perf && exec "$@"' pex "$@"
}
# Counters for `perf stat`: the PMU's (where it has them), then software.
perf_hw=instructions,cycles,branch-misses,L1-icache-load-misses,L1-dcache-load-misses,stalled-cycles-frontend,stalled-cycles-backend
perf_sw=task-clock,context-switches,cpu-migrations,page-faults
perf_sys=raw_syscalls:sys_enter,syscalls:sys_enter_futex,syscalls:sys_enter_pwritev2,syscalls:sys_enter_epoll_pwait,syscalls:sys_enter_write,syscalls:sys_enter_read,syscalls:sys_enter_pwrite64,syscalls:sys_enter_fdatasync,syscalls:sys_enter_io_uring_enter,syscalls:sys_enter_sched_yield
# Profiles one command: `perf stat` of counters (and, separately, of
# syscalls by kind), then `perf record` with call graphs reported as text.
# Each is its own run, since sampling and tracing perturb it. Usage:
# profile <label> <command...>; the command's stdout goes to <label>-*.out.
profile() {
    [[ -n $perfimg ]] || return 0
    local label=$1
    shift
    pex perf stat -x, -o "/results/$label-perf-stat.csv" -e "$perf_hw,$perf_sw" "$@" \
        >"$FUSEE_RESULTS/$label-perf-stat.out" 2>&1 || true
    PEX_TRACE=1 pex perf stat -x, -o "/results/$label-perf-syscalls.csv" -e "$perf_sw,$perf_sys" "$@" \
        >"$FUSEE_RESULTS/$label-perf-syscalls.out" 2>&1 || true
    pex sh -c 'perf record -F 2999 -g -o /data/perf/$0.data -- "$@" >/results/$0-perf-record.out 2>&1 \
        && perf report -i /data/perf/$0.data --stdio --no-children --sort comm,dso,sym --percent-limit 0.3 -g none \
            >/results/$0-perf-report.txt 2>/dev/null \
        && perf report -i /data/perf/$0.data --stdio --no-children --sort sym --percent-limit 1.5 -g caller,0.5,callee,function,percent \
            >/results/$0-perf-callgraph.txt 2>/dev/null; rm -f /data/perf/$0.data' "$label" "$@" || true
}
# The extra cells (FUSEE_CELLS, comma-separated), order rotated per trial.
run_cells() {
    local -a selected args
    IFS=, read -ra selected <<<"${FUSEE_CELLS:-fanout-1000,fanout-10000,payload,children,service-concurrent,saturate}"
    for ((trial=0; trial<${FUSEE_CELL_TRIALS:-3}; trial++)); do
        for ((i=0; i<${#selected[@]}; i++)); do
            name=${selected[(trial+i)%${#selected[@]}]}
            case $name in
                fanout-1000|fanout-10000|children) args=(--trials 1) ;;
                payload) args=(--trials 1 --steps 32) ;;
                service-concurrent) args=(--trials 1 --steps 10 --concurrency 10000) ;;
                saturate) args=(--steps 32 --rate 1600 --seconds 10) ;;
                *) echo "unknown cell $name"; return 2 ;;
            esac
            traced "$name-$trial" run "$name" --dir /data/runs --shards 8 "${args[@]}"
        done
    done
}
# A profile-guided build of the benchmark. An instrumented build runs the
# open loop (1.64 M and 6.55 M steps/s, and 100 K with
# ten-step workflows) and 200,000 sleepers' wake, and a second build uses
# the merged profile: /build/pgo-use/release/fusee-bench.
pgo_build() {
    mkdir -p /mnt/fusee/pgo/raw
    docker run "${common[@]}" -e CARGO_TARGET_DIR=/build/pgo-gen -e RUSTFLAGS="-Cprofile-generate=/data/pgo/raw" \
        "$image" cargo build --release --locked -p fusee-bench >"$FUSEE_RESULTS/pgo-gen-build.log" 2>&1
    gen() { docker run "${common[@]}" "$image" /build/pgo-gen/release/fusee-bench "$@" >/dev/null 2>>"$FUSEE_RESULTS/pgo-train.log"; }
    gen run open --dir /data/pgo-runs --shards 8 --steps 32 --trials 1 --rate 51200 --seconds 3
    gen run open --dir /data/pgo-runs --shards 8 --steps 32 --trials 1 --rate 204800 --seconds 3
    gen run open --dir /data/pgo-runs --shards 8 --steps 10 --trials 1 --rate 10000 --seconds 3
    gen run sleepers --dir /data/pgo-runs --shards 8 --rate 200000 --trials 1
    ls -la /mnt/fusee/pgo/raw >"$FUSEE_RESULTS/pgo-raw.txt" 2>&1
    docker run "${common[@]}" "$image" sh -c 'rustup component add llvm-tools >/dev/null 2>&1;
            p=$(ls "$(rustc --print sysroot)"/lib/rustlib/*/bin/llvm-profdata | head -1);
            "$p" merge -o /data/pgo/merged.profdata /data/pgo/raw && ls -la /data/pgo/merged.profdata' \
        >"$FUSEE_RESULTS/pgo-merge.log" 2>&1
    docker run "${common[@]}" -e CARGO_TARGET_DIR=/build/pgo-use -e RUSTFLAGS="-Cprofile-use=/data/pgo/merged.profdata" \
        "$image" cargo build --release --locked -p fusee-bench >"$FUSEE_RESULTS/pgo-use-build.log" 2>&1
    rm -rf /mnt/fusee/pgo-runs
    fstrim /mnt/fusee >/dev/null 2>&1 || true
}
# The profile-guided binary, as `traced` runs the default one.
pgo_traced() {
    local label=$1
    shift
    docker run "${common[@]}" "$image" /build/pgo-use/release/fusee-bench "$@" "${stall[@]}" \
        --stall-trace "/results/$label.stalls.jsonl" >"$FUSEE_RESULTS/$label.jsonl"
    if [[ ${FUSEE_TRIM:-0} == 1 ]]; then
        rm -rf /mnt/fusee/runs
        fstrim /mnt/fusee >/dev/null 2>&1 || true
    fi
}
case "$FUSEE_CASE" in
    pgo)
        # The published points with the profile-guided build, the disk
        # trimmed between points: 8 M steps/s three times, 7.2 M, 1.64 M
        # twice, CPU per step at 100 K, and a million sleepers twice.
        export FUSEE_TRIM=1
        fstrim -v /mnt/fusee >>"$FUSEE_RESULTS/fstrim.log" 2>&1 || true
        pgo_build
        if [[ -s /mnt/fusee/pgo/merged.profdata ]]; then
            for t in 0 1 2; do
                pgo_traced "open-8000000-$t" run open --dir /data/runs --shards 8 --steps 32 --trials 1 --rate 250000 --seconds 10
            done
            pgo_traced open-7200000 run open --dir /data/runs --shards 8 --steps 32 --trials 1 --rate 225000 --seconds 10
            for t in 0 1; do
                pgo_traced "open-1638400-$t" run open --dir /data/runs --shards 8 --steps 32 --trials 1 --rate 51200 --seconds 10
            done
            pgo_traced cost100k run cost --dir /data/runs --shards 8 --steps 10 --trials 1 --rate 10000 --seconds 5
            for t in 0 1; do
                docker run "${common[@]}" "$image" /build/pgo-use/release/fusee-bench run sleepers --dir /data/runs --shards 8 \
                    --rate 1000000 --trials 1 >"$FUSEE_RESULTS/sleepers-1000000-$t.jsonl" || true
                rm -rf /mnt/fusee/runs
                fstrim /mnt/fusee >/dev/null 2>&1 || true
            done
        fi
        ;;
    floors)
        bench io-floors /data/floors /results/floor-samples 2 >"$FUSEE_RESULTS/io-floors.jsonl"
        bench floors /data/floors >"$FUSEE_RESULTS/disk-check.jsonl"
        ;;
    gp3)
        # A chain against the floor of the same gp3 volume, with the
        # device's counters around each flush path: the default (a lone
        # gate flushes inline) and every idle flush inline.
        bench io-floors /data/floors /results/floor-samples 2 >"$FUSEE_RESULTS/io-floors.jsonl"
        # The same probe walking a log segment's worth of blocks, each
        # written once since the file was zero-filled, as the log's appends.
        docker run "${common[@]}" -e FUSEE_FLOOR_EXTENT_MIB=64 "$image" /build/release/fusee-bench \
            io-floors /data/floors /results/floor-samples-64 2 1 >"$FUSEE_RESULTS/io-floors-64.jsonl"
        dev=${device##*/}
        for variant in default inline; do
            envs=(-e FUSEE_BENCH_VARIANT="$variant")
            case $variant in
                inline) envs+=(-e FUSEE_BENCH_INLINE_MAX_US=100000) ;;
                fdatasync) envs+=(-e FUSEE_BENCH_FLUSH=fdatasync) ;;
            esac
            grep " $dev " /proc/diskstats >"$FUSEE_RESULTS/diskstats-$variant-before.txt"
            docker run "${common[@]}" "${envs[@]}" "$image" /build/release/fusee-bench run chain \
                --dir /data/runs --shards 8 --trials 2 --rate 1000 --seconds 5 --steps 32 "${stall[@]}" \
                --stall-trace "/results/chain-$variant.stalls.jsonl" >"$FUSEE_RESULTS/chain-$variant.jsonl"
            grep " $dev " /proc/diskstats >"$FUSEE_RESULTS/diskstats-$variant-after.txt"
        done
        ;;
    local)
        # On an EBS disk, the device's own floors first, from the same volume
        # as the cases.
        if [[ $FUSEE_DATA != instance ]]; then
            bench io-floors /data/floors /results/floor-samples 2 >"$FUSEE_RESULTS/io-floors.jsonl"
        fi
        # Rotate case order on each trial. The engine is reopened for every case.
        cases=(chain fanout service open signal)
        for trial in 0 1 2 3 4; do
            for ((i=0; i<${#cases[@]}; i++)); do
                name=${cases[(trial+i)%${#cases[@]}]}
                traced "$name-$trial" run "$name" --dir /data/runs --shards 8 --trials 1 --rate 1000 --seconds 5 --steps 32
            done
        done
        # Counters for the sweep's per-thread report (context switches,
        # instructions and cycles per step at each point).
        perf_setup
        traced max run max --dir /data/runs --shards 8 --steps 32 --rate 100 --seconds 10
        traced cost run cost --dir /data/runs --shards 8 --steps 10 --trials 1 --seconds 5 \
            --usd-per-hour "$FUSEE_USD_PER_HOUR"
        # The 1.64 M and 3.28 M steps/s points, profiled: counters,
        # syscalls by kind and call graphs by thread, each from its own run.
        for starts in 51200 102400; do
            profile "open-$starts" /build/release/fusee-bench run open --dir /data/runs --shards 8 --steps 32 \
                --trials 1 --rate "$starts" --seconds 5
        done
        docker run "${common[@]}" "$image" /build/release/fusee-crashtest driver /data/crash 20 500 \
            >"$FUSEE_RESULTS/crash.log"
        if [[ -n ${FUSEE_CELLS:-} ]]; then
            run_cells
        fi
        ;;
    cells)
        bench floors /data/floors >"$FUSEE_RESULTS/disk-check.jsonl"
        run_cells
        ;;
    soak)
        # Ten minutes per rate unless FUSEE_SOAK_SECONDS says otherwise; each
        # rate is its own process and engine, with a latency and stall series
        # per 10 s of scheduled arrivals.
        bench floors /data/floors >"$FUSEE_RESULTS/disk-check.jsonl"
        IFS=, read -ra rates <<<"${FUSEE_SOAK_RATES:-1600,12800,51200}"
        for rate in "${rates[@]}"; do
            traced "soak-$rate" run soak --dir /data/runs --shards 8 --steps 32 --rates "$rate" \
                --seconds "${FUSEE_SOAK_SECONDS:-600}" --window-seconds 10
        done
        ;;
    sleepers)
        # Each population must actually park and wake on the same host.
        for n in 1000000 10000000; do
            bench run sleepers --dir /data/runs --shards 8 --rate "$n" --trials 1 >"$FUSEE_RESULTS/sleepers-$n.jsonl"
        done
        # Ten million parked runs over eight shards, reopened in a fresh
        # process: open time, what the open read, resident size.
        docker run "${common[@]}" "$image" cargo build --release --locked -p fusee --example parked_open
        docker run "${common[@]}" "$image" /build/release/examples/parked_open \
            /data/parked 10000000 8 >"$FUSEE_RESULTS/parked-open-10000000.txt"
        cat /sys/kernel/mm/transparent_hugepage/enabled >"$FUSEE_RESULTS/thp.txt" 2>/dev/null || true
        ;;
    packages)
        source /opt/fusee/bench/aws/package-host.sh
        ;;
    sdk)
        source /opt/fusee/bench/aws/sdk.sh
        ;;
    comparisons|external)
        bench floors /data/comparison-floor >"$FUSEE_RESULTS/disk-check.jsonl"
        if [[ ${FUSEE_SEGMENT_PROBE:-0} == 1 ]]; then
            bench segment-open /data/segment-open > "$FUSEE_RESULTS/segment-open.json"
        fi
        source /opt/fusee/bench/aws/comparisons.sh
        ;;
    cluster)
        source /opt/fusee/bench/aws/cluster-host.sh
        ;;
    micro)
        # Costs measured by in-tree examples, each crate's built into
        # its own target directory (fusee's `step_cost`, fusee-server's
        # `server_step_cost`). Temporary directories go on the measured
        # disk.
        docker run "${common[@]}" -e CARGO_TARGET_DIR=/build/ex-fusee "$image" cargo build --release --locked -p fusee --example step_cost --example wake_cost
        docker run "${common[@]}" -e CARGO_TARGET_DIR=/build/ex-server "$image" cargo build --release --locked -p fusee-server --example server_step_cost
        # The same step-cost example with LSE atomics inline instead of the
        # outlined helpers the default aarch64 build calls (`__aarch64_ldadd8_*`,
        # which pick LSE at run time): how much those calls cost there.
        if [[ $(uname -m) == aarch64 ]]; then
            docker run "${common[@]}" -e CARGO_TARGET_DIR=/build/ex-lse -e RUSTFLAGS="-C target-feature=+lse" "$image" \
                cargo build --release --locked -p fusee --example step_cost || true
        fi
        ex() { docker run "${common[@]}" -e TMPDIR=/data/tmp "$image" sh -c 'mkdir -p /data/tmp && exec "$@"' ex "$@"; }
        perf_setup
        # What one small O_DIRECT durable write costs in CPU, by method:
        # the writer's user and system time, and every CPU's busy and
        # interrupt time, per write.
        bench write-cpu /data/write-cpu 3 >"$FUSEE_RESULTS/write-cpu.jsonl" 2>&1 || true
        # Memory-mode step cost and one-step start to result. The example reads instructions, cycles, branch and L1
        # misses and stall cycles per step itself (perf_event_open).
        lse=0
        ex test -x /build/ex-lse/release/examples/step_cost && lse=1
        for trial in 0 1 2; do
            ex /build/ex-fusee/release/examples/step_cost 200000 5 >"$FUSEE_RESULTS/step-cost-$trial.txt" 2>&1
            if [[ $lse == 1 ]]; then
                ex /build/ex-lse/release/examples/step_cost 200000 5 >"$FUSEE_RESULTS/step-cost-lse-$trial.txt" 2>&1 || true
            fi
        done
        # Where a memory-mode step's cycles go: a longer chain under perf.
        profile step-cost /build/ex-fusee/release/examples/step_cost 1000000 5
        # CPU per step at 100,000 steps/s (10,000 starts/s of 10 steps).
        # Each thread group reports context switches per step, and
        # instructions and cycles where the PMU counts.
        traced cost-100k run cost --dir /data/runs --shards 8 --steps 10 --trials 1 --rate 10000 --seconds 5 \
            --usd-per-hour "$FUSEE_USD_PER_HOUR"
        # The 100 K steps/s point alone, profiled: counters, syscalls by
        # kind, and a call-graph profile by thread.
        profile open-100k /build/release/fusee-bench run open --dir /data/runs --shards 8 --steps 10 --trials 1 \
            --rate 10000 --seconds 5
        # Mass wake at 1 M and 3 M parked, one shard, budget off and on.
        for n in 1000000 3000000; do
            ex /build/ex-fusee/release/examples/wake_cost create /data/wake-$n $n >"$FUSEE_RESULTS/wake-create-$n.txt" 2>&1
            for budget in 0 256; do
                rm -rf /mnt/fusee/wake-copy && cp -a /mnt/fusee/wake-$n /mnt/fusee/wake-copy
                ex /build/ex-fusee/release/examples/wake_cost wake /data/wake-copy $budget >"$FUSEE_RESULTS/wake-$n-budget$budget.txt" 2>&1
            done
            rm -rf /mnt/fusee/wake-$n /mnt/fusee/wake-copy
        done
        # Server-mode step cost: Unix socket, shared memory and TCP against
        # local, alternating rounds.
        for trial in 0 1 2; do
            ex /build/ex-server/release/examples/server_step_cost 500 7 >"$FUSEE_RESULTS/server-step-cost-$trial.txt" 2>&1
        done
        ;;
    dm-replay)
        bash /opt/fusee/bench/faults/dm-host.sh
        ;;
    *) echo "unsupported execution case $FUSEE_CASE"; exit 2 ;;
esac
