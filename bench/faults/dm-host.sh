#!/usr/bin/env bash
# Disposable benchmark host only. All mapped block devices are fresh loop files.
set -euo pipefail
apt-get -qq install -y dmsetup build-essential git kmod
if ! modprobe dm-log-writes; then
    apt-get -qq install -y "linux-modules-extra-$(uname -r)" || true
fi
if ! modprobe dm-log-writes; then
    apt-get -qq install -y "linux-headers-$(uname -r)"
    release=$(uname -r | cut -d. -f1,2)
    mkdir -p /var/tmp/fusee-dm-module
    cd /var/tmp/fusee-dm-module
    curl -fsSLo dm-log-writes.c "https://raw.githubusercontent.com/torvalds/linux/v${release}/drivers/md/dm-log-writes.c"
    sha256sum dm-log-writes.c > "$FUSEE_RESULTS/dm-module.sha256"
    echo 'obj-m += dm-log-writes.o' > Makefile
    make -s -C "/lib/modules/$(uname -r)/build" M="$PWD" modules
    insmod ./dm-log-writes.ko
fi
work=/var/tmp/fusee-log-writes
git clone --quiet https://github.com/josefbacik/log-writes "$work"
git -C "$work" checkout -q 7b70d8a6863c5de30933d42a7672d35d01d2dc6c
make -s -C "$work" replay-log
binary=$(docker volume inspect fusee-build --format '{{.Mountpoint}}')/release/fusee-crashtest
TMPDIR=/mnt/fusee python3 /opt/fusee/bench/faults/dm_replay.py "$binary" "$work/replay-log" "$FUSEE_RESULTS/dm-cuts" | tee "$FUSEE_RESULTS/dm-replay.jsonl"
