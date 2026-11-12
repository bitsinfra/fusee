# Sourced after the three isolated hosts have measured each network pair.
set -euo pipefail
restate_image=docker.restate.dev/restatedev/restate:1.6.0
node_image=node:24.14.0-bookworm-slim
out=$FUSEE_RESULTS
node="restate-$FUSEE_INDEX"
addresses=$(printf '%s\n' "${ips[@]}" | jq -R ' "http://"+.+":5122" ' | jq -sc .)
mkdir -p /mnt/fusee/restate
docker pull -q "$restate_image"
docker image inspect "$restate_image" --format '{{json .RepoDigests}}' >"$out/restate-image.json"
docker run -d --name "$node" --network host -v /mnt/fusee/restate:/restate-data \
    -e RESTATE_AUTO_PROVISION=false -e RESTATE_CLUSTER_NAME=fusee-benchmark \
    -e RESTATE_ADVERTISED_ADDRESS="http://$self:5122" -e RESTATE_BIND_ADDRESS="$self:5122" \
    -e RESTATE_NODE_NAME="node-$FUSEE_INDEX" -e RESTATE_METADATA_CLIENT__ADDRESSES="$addresses" \
    -e RESTATE_LOG_SERVER__ROCKSDB_DISABLE_WAL_FSYNC=false -e RESTATE_LOG_SERVER__ROCKSDB_DISABLE_WAL=false \
    "$restate_image" --base-dir /restate-data >/dev/null
docker inspect "$node" --format '{{json .Config.Env}}' >"$out/restate-config.json"
touch "$coord/restate-started"
for peer in 0 1 2; do await_flag "$peer" restate-started; done
ctl() { docker exec "$node" restatectl --address "http://$self:5122" "$@"; }
if (( FUSEE_INDEX==0 )); then
    for _ in $(seq 1 120); do
        if ctl provision --num-partitions 8 --log-provider replicated --replication 2 --yes >"$out/restate-provision.log" 2>&1 &&
            grep -qiE 'successfully provisioned|already provisioned' "$out/restate-provision.log"; then break; fi
        sleep 1
    done
    until ctl logs describe 0 >"$out/restate-log-zero.log" 2>&1 &&
          grep -q 'replicated' "$out/restate-log-zero.log" && grep -q 'node: 2' "$out/restate-log-zero.log"; do
        sleep 1
    done
    ctl status >"$out/restate-status.log"
    ctl config get >"$out/restate-config.log"
    ctl logs list >"$out/restate-logs.log"
    touch "$coord/restate-ready"
fi
await_flag 0 restate-ready
docker pull -q "$node_image"
docker run --rm -v /opt/fusee/bench/drivers:/drivers -w /drivers/restate "$node_image" npm ci --no-audit --no-fund
for trial in 0 1 2 3 4; do
    worker=$((trial % 3))
    if (( FUSEE_INDEX==worker )); then
        for n in 1 10 100 1000; do
            docker run --rm --network host -v /opt/fusee/bench/drivers:/drivers:ro -w /drivers/restate \
                -e RESTATE_WORKER_URI="http://$self:9080" -e RESTATE_ADMIN="http://127.0.0.1:9070" \
                -e RESTATE_INGRESS="http://127.0.0.1:8080" -e FUSEE_ZONE -e FUSEE_INSTANCE_TYPE -e FUSEE_REVISION \
                "$node_image" node chain.mjs "$n" 2 >"$out/restate-$trial-$n.json"
        done
        touch "$coord/restate-$trial"
    fi
    await_flag "$worker" "restate-$trial"
done
ctl status >"$out/restate-final-status.log"
docker logs "$node" >"$out/restate-server.log" 2>&1
