# Sourced by host.sh on the three fresh instances.
apt-get -qq install -y sockperf python3 ethtool
coord=/run/fusee-coordination
mkdir -p "$coord"
test -s "$FUSEE_RESULTS/peers.json" || curl -fsS --retry 16 --retry-all-errors "$FUSEE_PEER_URL" > "$FUSEE_RESULTS/peers.json"
mapfile -t ips < <(jq -r '.[].address' "$FUSEE_RESULTS/peers.json")
test "${#ips[@]}" -eq 3
self=${ips[FUSEE_INDEX]}
python3 -m http.server 39990 --bind "$self" --directory "$coord" >/var/tmp/fusee-coord.log 2>&1 &
http_pid=$!
sockperf server --tcp -i "$self" -p 11111 >"$FUSEE_RESULTS/sockperf-server.log" 2>&1 &
sock_pid=$!
tidy_cluster() {
    kill "$http_pid" "$sock_pid" 2>/dev/null || true
}
trap tidy_cluster EXIT
bench floors /data/local-floor >"$FUSEE_RESULTS/disk-check.jsonl"
# Interrupt coalescing: steps of about 64, 128 and 256 µs in a link's
# round trips come from adaptive receive moderation. Record the settings
# and counters of the member's interface.
nic=$(ip -o route get 1.1.1.1 2>/dev/null | sed -n 's/.* dev \([^ ]*\).*/\1/p')
nic=${nic:-$(ip -o -4 route show default | awk '{print $5; exit}')}
record_nic() {
    { echo "# $1 $(date -u +%FT%TZ) $nic"; ethtool -c "$nic" 2>&1 || true; } >>"$FUSEE_RESULTS/nic-coalesce-$FUSEE_INDEX.txt"
    { echo "# $1 $(date -u +%FT%TZ) $nic"; ethtool -S "$nic" 2>&1 || true; } >>"$FUSEE_RESULTS/nic-stats-$FUSEE_INDEX.txt"
}
record_nic start
sockperf --version >"$FUSEE_RESULTS/sockperf-version.txt"
touch "$coord/ready"
seen() { curl -fsS -o /dev/null --connect-timeout 1 --max-time 2 "http://${ips[$1]}:39990/$2" 2>/dev/null; }
await_flag() {
    local deadline=$((SECONDS+480))
    until seen "$1" "$2"; do
        ((SECONDS<deadline)) || { echo "timed out waiting for peer $1 flag $2"; return 1; }
        sleep .25
    done
}
for peer in 0 1 2; do await_flag "$peer" ready; done
# Only one sender/receiver pair is active. Save a separate full-RTT trace per trial.
for trial in 1 2 3 4 5; do
    for delta in 0 1 2; do
        from=$(((trial+delta)%3))
        for hop in 1 2; do
            to=$(((from+hop)%3))
            name="rtt-$trial-$from-$to"
            if (( FUSEE_INDEX==from )); then
                sockperf ping-pong --tcp --full-rtt -i "${ips[to]}" -p 11111 -m 64 -t 3 \
                    --full-log "$FUSEE_RESULTS/$name.csv" > "$FUSEE_RESULTS/$name.log"
                test -s "$FUSEE_RESULTS/$name.csv"
                touch "$coord/$name"
            fi
            await_flag "$from" "$name"
        done
    done
done
if [[ ${FUSEE_CLUSTER_SYSTEM:-fusee} == restate ]]; then
    source /opt/fusee/bench/aws/restate-cluster.sh
elif [[ ${FUSEE_CLUSTER_SYSTEM:-fusee} == temporal ]]; then
    source /opt/fusee/bench/aws/temporal-cluster.sh
else
export FUSEE_BENCH_HOLD_PRIMARY=1
peers=$(printf '%s:39170,' "${ips[@]}")
peers=${peers%,}
# FUSEE_CLUSTER_WORKLOADS (space-separated): chain and open (the default),
# saturate (the open loop of 10-step workflows from 1,000 starts/s, raised
# by 1.5 until step p99 passes 1 ms or throughput stops rising, then two
# points more; first FUSEE_SATURATE_TRIALS trials, default 2) and failover (primary kills on
# these hosts, once, after the other workloads; see failover_run).
read -ra workloads <<<"${FUSEE_CLUSTER_WORKLOADS:-chain open}"
# Busy-polled receive: every replica keeps polling its sockets this long
# after any activity. Off unless set.
export FUSEE_REPLICA_BUSY_POLL_US=${FUSEE_REPLICA_BUSY_POLL_US:-0}
workload_args() {
    case $1 in
        saturate) echo --steps 10 --rate 1000 --seconds 5 --growth 1.5 --past 2 ;;
        *) echo --steps 10 --seconds 5 --rate 100 ;;
    esac
}
# Primary kills on the real hosts. Every member runs the crash test's
# replica (fusee-crashtest replica: its engine opens as a hot standby, and
# every primary starts a workflow every 2 ms besides 100 acknowledged ones)
# under a loop that restarts it 100 ms after it dies. The member whose
# replica is the ready primary and has committed a new workflow, once the
# group has had FUSEE_FAILOVER_SETTLE_MS (default 1500) to become whole
# again, SIGKILLs its own replica and logs the kill; the other members'
# logs time the next member's readiness (READY), engine (PRIMARY), first
# workflow code (EXEC) and first new commit (SACK) from it. check_cluster.py
# merges the three logs (wall clocks under chrony; offsets archived).
failover_run() {
    local label=$1 kills=$2 settle_us=$((${FUSEE_FAILOVER_SETTLE_MS:-1500}*1000))
    local log="$FUSEE_RESULTS/$label-r$FUSEE_INDEX.log" name="fusee-$label"
    chronyc tracking >"$FUSEE_RESULTS/$label-chrony-$FUSEE_INDEX.txt" 2>&1 || true
    : >"$log"
    docker run -d --name "$name" "${common[@]}" -e FUSEE_REPLICA_FLUSH=default -e FUSEE_REPLICA_BUSY_POLL_US "$image" \
        bash -c 'while :; do /build/release/fusee-crashtest replica "$@"; sleep 0.1; done' bash \
        "$FUSEE_INDEX" "$peers" "/data/$label/r$FUSEE_INDEX" "/data/$label/service-$FUSEE_INDEX.log" 100 0 1 >/dev/null
    docker logs -f "$name" >>"$log" 2>&1 &
    local logs=$! deadline=$((SECONDS+900)) n=1 sack_us now_us
    touch "$coord/$label-started"
    for peer in 0 1 2; do await_flag "$peer" "$label-started"; done
    while ((n <= kills)); do
        ((SECONDS<deadline)) || { echo "failover timed out after $((n-1)) kills"; break; }
        local counted=0
        for peer in 0 1 2; do if seen "$peer" "$label-kill-$n"; then counted=1; fi; done
        if ((counted)); then n=$((n+1)); continue; fi
        # Ready primary since its last view change, with a new commit.
        local last
        last=$(grep -E '^(PRIMARY|VIEW|SACK) ' "$log" | tail -1)
        if [[ $last == SACK* ]]; then
            sack_us=${last##* }
            now_us=$(date +%s%6N)
            if ((now_us - sack_us >= settle_us + RANDOM % 300000)); then
                echo "KILL $FUSEE_INDEX $(date +%s%6N)" >>"$log"
                pkill -9 -f '^/build/release/fusee-crashtest replica' || true
                touch "$coord/$label-kill-$n"
                n=$((n+1))
            fi
        fi
        sleep .02
    done
    chronyc tracking >>"$FUSEE_RESULTS/$label-chrony-$FUSEE_INDEX.txt" 2>&1 || true
    touch "$coord/$label-done"
    for peer in 0 1 2; do await_flag "$peer" "$label-done"; done
    docker kill "$name" >/dev/null 2>&1 || true
    kill "$logs" 2>/dev/null || true
    wait "$logs" 2>/dev/null || true
}
# One measured trial of a workload on every member, labelled $2.
run_trial() {
        local workload=$1 label=$2
        result="$FUSEE_RESULTS/$label.jsonl"
        instance="fusee-$label"
        stats="$label-peer-$FUSEE_INDEX"
        export FUSEE_BENCH_METRICS="/results/$stats"
        read -ra args <<<"$(workload_args "$workload")"
        docker run --name "$instance" "${common[@]}" -e FUSEE_BENCH_HOLD_PRIMARY -e FUSEE_BENCH_METRICS \
            -e FUSEE_REPLICA_BUSY_POLL_US "$image" \
            /build/release/fusee-bench run "$workload" --mode replicated --peers "$peers" --replica-id "$FUSEE_INDEX" \
            --dir "/data/$label" --trials 1 "${args[@]}" --stall-us "${FUSEE_STALL_US:-1000}" \
            --stall-trace "/results/$stats.stalls.jsonl" > "$result" 2>"$FUSEE_RESULTS/$label.stderr" &
        client=$!
        deadline=$((SECONDS+900))
        while :; do
            if [[ -s "$result" ]] && jq -e --arg workload "$workload" '.workload==$workload and .mode=="replicated"' "$result" >/dev/null; then
                touch "$coord/$label"
            fi
            for peer in 0 1 2; do if seen "$peer" "$label"; then break 2; fi; done
            kill -0 "$client" 2>/dev/null || { cat "$FUSEE_RESULTS/$label.stderr"; exit 1; }
            ((SECONDS<deadline)) || { echo "replication trial timed out: $label"; exit 1; }
            sleep .25
        done
        # All members acknowledge the same completed trial before stopping any
        # replica, so loss of the primary cannot start another measured run.
        touch "$coord/$label-observed"
        for peer in 0 1 2; do await_flag "$peer" "$label-observed"; done
        touch "$FUSEE_RESULTS/$stats.end"
        until [[ -s "$FUSEE_RESULTS/$stats.json" ]]; do
            ((SECONDS<deadline)) || { echo "missing member resource counters"; exit 1; }
            sleep .05
        done
        rm "$FUSEE_RESULTS/$stats.end"
        touch "$coord/$label-counted"
        for peer in 0 1 2; do await_flag "$peer" "$label-counted"; done
        docker kill "$instance" >/dev/null
        wait "$client" || true
        touch "$coord/$label-stopped"
        for peer in 0 1 2; do await_flag "$peer" "$label-stopped"; done
}
for trial in 1 2 3 4 5; do
    for workload in "${workloads[@]}"; do
        if [[ $workload == failover ]]; then
            continue
        fi
        if [[ $workload == saturate ]] && ((trial > ${FUSEE_SATURATE_TRIALS:-2})); then
            continue
        fi
        run_trial "$workload" "$workload-$trial"
    done
done
record_nic after-trials
# The same chain with adaptive receive moderation off on every member
# (FUSEE_CLUSTER_RX_OFF=0 skips it). Each member restores its own setting.
# check_cluster.py reports these trials beside the default ones.
if ((${FUSEE_CLUSTER_RX_OFF:-1})) && [[ " ${workloads[*]} " == *" chain "* ]]; then
    adaptive=$(ethtool -c "$nic" 2>/dev/null | awk -F': *' '/^Adaptive RX/ {print $2}' | awk '{print $1}')
    usecs=$(ethtool -c "$nic" 2>/dev/null | awk -F': *' '/^rx-usecs:/ {print $2; exit}')
    if ethtool -C "$nic" adaptive-rx off rx-usecs 0 2>"$FUSEE_RESULTS/nic-rx-off-$FUSEE_INDEX.err"; then
        echo "adaptive-rx was ${adaptive:-unknown}, rx-usecs ${usecs:-unknown}; set off, 0" >"$FUSEE_RESULTS/nic-rx-off-$FUSEE_INDEX.txt"
    else
        echo "ethtool -C refused; trials ran with the default setting" >"$FUSEE_RESULTS/nic-rx-off-$FUSEE_INDEX.txt"
    fi
    record_nic rx-off
    for trial in 1 2 3 4 5; do
        run_trial chain "chain-rxoff-$trial"
    done
    ethtool -C "$nic" adaptive-rx "${adaptive:-on}" ${usecs:+rx-usecs "$usecs"} 2>/dev/null || true
    record_nic restored
fi
# Busy-polled receive in the same launch: the chain again with every
# replica polling its sockets FUSEE_CLUSTER_BUSY_POLL_TRIAL_US after any
# activity (0, the default, skips it).
if ((${FUSEE_CLUSTER_BUSY_POLL_TRIAL_US:-0} > 0)) && [[ " ${workloads[*]} " == *" chain "* ]]; then
    default_poll=$FUSEE_REPLICA_BUSY_POLL_US
    export FUSEE_REPLICA_BUSY_POLL_US=$FUSEE_CLUSTER_BUSY_POLL_TRIAL_US
    for trial in 1 2 3 4 5; do
        run_trial chain "chain-busypoll-$trial"
    done
    export FUSEE_REPLICA_BUSY_POLL_US=$default_poll
fi
for workload in "${workloads[@]}"; do
    if [[ $workload == failover ]]; then
        failover_run failover "${FUSEE_FAILOVER_KILLS:-12}"
    fi
done
fi
# Keep the coordination service alive until every peer has finished its last barrier.
touch "$coord/finished"
for peer in 0 1 2; do await_flag "$peer" finished; done
sleep 3
tidy_cluster
trap - EXIT
