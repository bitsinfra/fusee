#!/usr/bin/env bash
# Mint a short-lived worker token inside the local benchmark stack.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
compose=(docker compose -p "${FUSEE_COMPOSE_PROJECT:-fusee-bench}" -f "$here/../docker-compose.yml")
tenant=$("${compose[@]}" exec -T postgres psql -U postgres -d hatchet -At -c 'select id from "Tenant" where slug = $$default$$')
test -n "$tenant"
HATCHET_CLIENT_TOKEN=$("${compose[@]}" exec -T hatchet ./hatchet-admin token create --config /config --tenant-id "$tenant" --name fusee-bench --expiresIn 75m | awk '/^eyJ/{print $1}')
test -n "$HATCHET_CLIENT_TOKEN"
export HATCHET_CLIENT_TOKEN HATCHET_CLIENT_TLS_STRATEGY=none HATCHET_CLIENT_HOST_PORT=127.0.0.1:7077
if [[ ${1:-} == --workloads ]]; then exec node "$here/workloads.mjs"; fi
if [[ ${1:-} == --sleepers ]]; then exec node "$here/sleepers.mjs"; fi
if [[ ${1:-} == --kill ]]; then shift; exec node "$here/../workloads/kill.mjs" hatchet "$@"; fi
exec node "$here/chain.mjs" "$@"
