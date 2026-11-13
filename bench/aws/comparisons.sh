# Prepare a single disposable EC2 machine for rotating engine comparisons.
# host.sh has already mounted the test device and placed Docker's root on it.
set -euo pipefail
apt-get -qq install -y docker-compose-v2 python3-venv
drivers=/opt/fusee/bench/drivers
arch=$(uname -m)
case "$arch" in
    x86_64) goarch=amd64; ob_hash=f51823294851447773c9a5ee547fe74313fe0b399305d9e9583a329a31835253; in_hash=7e8062e3f744ac95e24ae1ec4519195ec1a3a4e74edb416fb5225f6a78f382de ;;
    aarch64) goarch=arm64; ob_hash=c8cb65d817ffce24b24f0257711cc81b12295edd4ea8818ec6f5a0813fcae9d0; in_hash=ec29655cd9037514f0f67281fb32cee070a21b0e49784160b22868966fff6056 ;;
esac
fetch() {
    curl -fLsS --retry 4 --retry-all-errors "$1" -o "$3"
    echo "$2  $3" | sha256sum -c -
}
needs_obelisk=true
if [[ $FUSEE_CASE == external && ,${FUSEE_EXTERNAL_SYSTEMS:?}, != *,obelisk,* ]]; then
    needs_obelisk=false
fi
if "$needs_obelisk"; then
    for component in noop_activity chain_workflow; do
        [[ -s "$drivers/obelisk/build/$component.wasm" ]] || { echo "Missing Obelisk $component.wasm; create this bundle with --include-obelisk" >&2; exit 1; }
    done
    fetch "https://github.com/obeli-sk/obelisk/releases/download/v0.42.0/obelisk-$arch-unknown-linux-musl.tar.gz" "$ob_hash" /tmp/obelisk.tgz
    tar xzf /tmp/obelisk.tgz -C /usr/local/bin obelisk
    sha256sum "$drivers/obelisk/build/"*.wasm >"$FUSEE_RESULTS/wasm.sha256"
fi
fetch "https://github.com/inngest/inngest/releases/download/v1.51.0/inngest_1.51.0_linux_$goarch.tar.gz" "$in_hash" /tmp/inngest.tgz
tar xzf /tmp/inngest.tgz -C /usr/local/bin inngest
fetch "https://raw.githubusercontent.com/earendil-works/absurd/cdca473a40d9817c3987c55cb796f9dc4f4ad22e/sql/absurd.sql" "918694ced6591643b51c172d986709979b0ca6880896c6b2f5652e6ab64970c0" /tmp/absurd.sql
node_image=node:24.14.0-bookworm-slim
docker pull -q "$node_image"
docker run --name fusee-node-tools "$node_image" node --version
docker cp fusee-node-tools:/usr/local/bin/node /usr/local/bin/node
docker rm fusee-node-tools
docker run --rm -v "$drivers:/drivers" "$node_image" sh -euc '
    for engine in temporal restate dbos inngest hatchet absurd; do
        cd "/drivers/$engine"; npm ci --no-audit --no-fund
    done'
python3 -m venv /opt/fusee-python
/opt/fusee-python/bin/pip -q install -r "$drivers/dbos/requirements.txt"
compose=(docker compose -p fusee-bench -f "$drivers/docker-compose.yml")
"${compose[@]}" pull
"${compose[@]}" up -d --wait postgres
"${compose[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d absurd </tmp/absurd.sql
"${compose[@]}" exec -T postgres psql -At -U postgres -c 'show synchronous_commit; show fsync; show full_page_writes;' > "$FUSEE_RESULTS/postgres-settings.txt"
docker images --digests --format '{{.Repository}}:{{.Tag}} {{.Digest}}' >"$FUSEE_RESULTS/images.txt"
cp "$drivers/docker-compose.yml" "$drivers/temporal/tuned.yaml" "$drivers/obelisk/server.toml" "$FUSEE_RESULTS/"
docker volume inspect fusee-build --format '{{.Mountpoint}}' > /tmp/fusee-build-dir
export FUSEE_BENCH="$(cat /tmp/fusee-build-dir)/release/fusee-bench"
export FUSEE_CLI="$(cat /tmp/fusee-build-dir)/release/fusee"
export FUSEE_BENCH_CHAIN_RUNS=2
if [[ $FUSEE_CASE == external ]]; then
    if [[ $FUSEE_EXTERNAL_SYSTEMS == *fusee-* ]]; then
        docker run --rm -v /opt/fusee/bindings/node:/sdk -w /sdk "$node_image" \
            sh -euc 'npm install --no-audit --no-fund --omit=optional --ignore-scripts; ./node_modules/.bin/tsc -p tsconfig.json'
        docker run "${common[@]}" "$image" cargo build --release --locked -p fusee-node --lib
        export FUSEE_NATIVE="$(cat /tmp/fusee-build-dir)/release/libfusee_node.node"
        cp "$(cat /tmp/fusee-build-dir)/release/libfusee_node.so" "$FUSEE_NATIVE"
    fi
    suite=(python3 "$drivers/external_matrix.py" "$FUSEE_RESULTS" --systems "${FUSEE_EXTERNAL_SYSTEMS:?}" --trials "${FUSEE_EXTERNAL_TRIALS:-5}")
    if [[ -n ${FUSEE_EXTERNAL_WORKLOADS:-} ]]; then suite+=(--workloads "$FUSEE_EXTERNAL_WORKLOADS"); fi
    "${suite[@]}"
else
    python3 "$drivers/matrix.py" "$FUSEE_RESULTS"
fi
