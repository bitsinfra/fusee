# Three Temporal services on three hosts. PostgreSQL commits wait for one
# of two streaming standbys to report a durable WAL flush.
set -euo pipefail
out=$FUSEE_RESULTS
pg='postgres@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685'
temporal='temporalio/server@sha256:c3e752127759616bb1615e0f9ba0e21635aeb5fdeb922de4f371c350955f46ae'
admin='temporalio/admin-tools@sha256:a9f84fb9a374b2374fe2e67c8efc0468ff3f1c66c8a0b14597ec86e349e62bca'
node_image=node:24.14.0-bookworm-slim
primary=${ips[0]}
data=/mnt/fusee/pg
cfg=/run/fusee-temporal
mkdir -p "$data" "$cfg"
docker pull -q "$pg"
docker pull -q "$temporal"
docker pull -q "$admin"
docker image inspect "$pg" "$temporal" "$admin" --format '{{json .RepoDigests}}' >"$out/temporal-images.jsonl"
pgargs=(--network host -v "$data:/var/lib/postgresql/data" -e POSTGRES_PASSWORD=fusee-benchmark)
if (( FUSEE_INDEX==0 )); then
    docker run -d --name pg "${pgargs[@]}" "$pg" postgres -c listen_addresses="$self,localhost" -c max_connections=300 \
        -c shared_buffers=256MB -c wal_level=replica -c max_wal_senders=8 -c wal_keep_size=1GB -c fsync=on -c synchronous_commit=on >/dev/null
    until docker exec pg pg_isready -U postgres >/dev/null; do sleep 1; done
    for standby in "${ips[@]:1}"; do
        echo "host replication postgres $standby/32 scram-sha-256" | docker exec -i pg sh -c 'cat >> /var/lib/postgresql/data/pg_hba.conf'
    done
    docker exec pg psql -v ON_ERROR_STOP=1 -U postgres -Atc "SELECT pg_create_physical_replication_slot('standby1', true); SELECT pg_create_physical_replication_slot('standby2', true); SELECT pg_reload_conf()"
    touch "$coord/pg-primary"
else
    await_flag 0 pg-primary
    docker run --rm "${pgargs[@]}" -e PGPASSWORD=fusee-benchmark "$pg" sh -euc \
        'chown postgres:postgres /var/lib/postgresql/data; exec su-exec postgres pg_basebackup -h "$1" -U postgres -D /var/lib/postgresql/data -R -X stream --slot="$2"' sh "$primary" "standby$FUSEE_INDEX"
    printf "primary_conninfo = 'host=%s user=postgres password=fusee-benchmark application_name=standby%s'\n" \
        "$primary" "$FUSEE_INDEX" >> "$data/postgresql.auto.conf"
    docker run -d --name pg "${pgargs[@]}" "$pg" postgres -c listen_addresses="$self,localhost" \
        -c max_connections=300 -c shared_buffers=256MB -c fsync=on >/dev/null
    until docker exec pg pg_isready -U postgres >/dev/null; do sleep 1; done
    touch "$coord/pg-streaming"
fi
if (( FUSEE_INDEX==0 )); then
    await_flag 1 pg-streaming; await_flag 2 pg-streaming
    sql() { docker exec pg psql -v ON_ERROR_STOP=1 -U postgres -Atc "$1"; }
    sql "ALTER SYSTEM SET synchronous_standby_names = 'ANY 1 (standby1, standby2)'"
    sql 'SELECT pg_reload_conf()'
    until [[ $(sql "select count(*) from pg_stat_replication where state='streaming' and sync_state='quorum'") == 2 ]]; do sleep 1; done
    sql 'show fsync; show synchronous_commit; show synchronous_standby_names; SELECT application_name,state,sync_state,flush_lsn FROM pg_stat_replication ORDER BY 1' >"$out/pg-durability-before.txt"
    sql 'SELECT slot_name,active,restart_lsn FROM pg_replication_slots ORDER BY 1' >"$out/pg-replication-slots.txt"
    for pair in temporal:temporal temporal_visibility:visibility; do
        db=${pair%:*}; schema=${pair#*:}
        base=(docker run --rm --network host -e SQL_PASSWORD=fusee-benchmark "$admin" temporal-sql-tool --ep "$primary" -p 5432 -u postgres --pw fusee-benchmark --pl postgres12 --db "$db")
        "${base[@]}" create-database
        "${base[@]}" setup-schema -v 0.0
        "${base[@]}" update-schema -d "/etc/temporal/schema/postgresql/v12/$schema/versioned"
    done
    touch "$coord/schemas"
fi
await_flag 0 schemas
jq --arg pg "$primary:5432" --arg self "$self" --arg frontend "$primary:7233" \
  '.persistence.datastores[].sql |= (.connectAddr=$pg | .password="fusee-benchmark") |
   .global.membership.broadcastAddress=$self | .services[].rpc.bindOnIP=$self |
   .clusterMetadata.clusterInformation.active.rpcAddress=$frontend |
   .dynamicConfigClient.filepath="/config/tuned.yaml"' \
  /opt/fusee/bench/drivers/temporal/cluster-template.json >"$cfg/temporal.json"
cp /opt/fusee/bench/drivers/temporal/tuned.yaml "$cfg/tuned.yaml"
jq 'del(.persistence.datastores[].sql.password)' "$cfg/temporal.json" >"$out/temporal-config.json"
cp "$cfg/tuned.yaml" "$out/"
# Cloud-init uses umask 077. The pinned server runs as the temporal user.
# Grant its UID access only to this guest's read-only benchmark configuration.
server_uid=$(docker run --rm --entrypoint id "$temporal" -u)
server_gid=$(docker run --rm --entrypoint id "$temporal" -g)
chown "$server_uid:$server_gid" "$cfg" "$cfg/temporal.json" "$cfg/tuned.yaml"
chmod 700 "$cfg"
chmod 600 "$cfg/temporal.json" "$cfg/tuned.yaml"
docker run --rm -v "$cfg:/config:ro" --entrypoint sh "$temporal" \
    -c 'test -r /config/temporal.json && test -r /config/tuned.yaml'
docker run -d --name temporal --network host -v "$cfg:/config:ro" "$temporal" \
    temporal-server --config-file /config/temporal.json --allow-no-auth start >/dev/null
deadline=$((SECONDS+180))
until docker run --rm --network host "$admin" temporal operator cluster health --address "$self:7233" >"$out/temporal-health.log" 2>&1; do
    if [[ $(docker inspect -f '{{.State.Running}}' temporal) != true ]] || (( SECONDS >= deadline )); then
        docker logs temporal >"$out/temporal-server.log" 2>&1
        docker inspect --format '{{json .State}}' temporal >"$out/temporal-state.json"
        cat "$out/temporal-health.log"
        exit 1
    fi
    sleep 1
done
touch "$coord/temporal-healthy"
for peer in 0 1 2; do await_flag "$peer" temporal-healthy; done
if (( FUSEE_INDEX==0 )); then
    cli=(docker run --rm --network host "$admin" temporal operator namespace)
    "${cli[@]}" describe --address "$self:7233" --namespace default >/dev/null 2>&1 || "${cli[@]}" create --address "$self:7233" --namespace default
    touch "$coord/namespace"
fi
await_flag 0 namespace
docker run --rm -v /opt/fusee/bench/drivers:/drivers -w /drivers/temporal "$node_image" npm ci --no-audit --no-fund
for trial in 0 1 2 3 4; do
    worker=$((trial % 3))
    if (( FUSEE_INDEX==worker )); then
        for n in 1 10 100 1000; do
            docker run --rm --network host -v /opt/fusee/bench/drivers:/drivers:ro -w /drivers/temporal \
               -e TEMPORAL_ADDRESS="$self:7233" -e TEMPORAL_TUNED=1 -e FUSEE_ZONE -e FUSEE_INSTANCE_TYPE -e FUSEE_REVISION \
               "$node_image" node chain.mjs "$n" 2 >"$out/temporal-$trial-$n.json"
        done
        touch "$coord/temporal-$trial"
    fi
    await_flag "$worker" "temporal-$trial"
done
if (( FUSEE_INDEX==0 )); then
    sql 'show fsync; show synchronous_commit; show synchronous_standby_names; SELECT application_name,state,sync_state,flush_lsn FROM pg_stat_replication ORDER BY 1' >"$out/pg-durability-after.txt"
fi
docker logs temporal >"$out/temporal-server.log" 2>&1
