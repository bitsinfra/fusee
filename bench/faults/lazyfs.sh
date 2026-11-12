#!/usr/bin/env bash
# Run inside the LazyFS test container with /dev/fuse and CAP_SYS_ADMIN.
set -euo pipefail
lab=$(mktemp -d)
mkdir "$lab/backing" "$lab/mount"
cat > "$lab/lazyfs.toml" <<CONFIG
[faults]
fifo_path="$lab/faults"
fifo_path_completed="$lab/completed"
[cache]
apply_eviction=false
[cache.simple]
custom_size="256mb"
blocks_per_page=1
[filesystem]
log_all_operations=false
logfile="$lab/lazyfs.log"
CONFIG
/opt/lazyfs/lazyfs/build/lazyfs "$lab/mount" --config-path "$lab/lazyfs.toml" -o modules=subdir -o "subdir=$lab/backing" -f >"$lab/mount.log" 2>&1 &
pid=$!
trap 'umount "$lab/mount" 2>/dev/null || true; kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true' EXIT
for ((i=0; i<100; i++)); do
    if mountpoint -q "$lab/mount" && [[ -p "$lab/completed" ]]; then break; fi
    kill -0 "$pid" 2>/dev/null || { cat "$lab/mount.log"; exit 1; }
    sleep .1
done
mountpoint -q "$lab/mount" || { cat "$lab/mount.log"; exit 1; }
"${FUSEE_CRASHTEST:-/build/release/fusee-crashtest}" lazyfs-driver "$lab/mount/engine" "$lab/service-oracle" "$lab/faults" "$lab/completed" "${1:-20}" "${2:-200}"
