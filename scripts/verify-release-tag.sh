#!/usr/bin/env bash
# Verify an annotated release tag using only the maintainer keys supplied by the repository.
set -euo pipefail
tag=${1:?supply a release tag}
[[ $tag == v* ]] || { echo 'Expected a version tag starting with v.' >&2; exit 1; }
[[ $(git cat-file -t "refs/tags/$tag") == tag ]] || { echo 'Release tag must be annotated.' >&2; exit 1; }
if [[ -z ${FUSEE_RELEASE_SIGNING_KEYS:-} && -z ${FUSEE_RELEASE_SSH_SIGNERS:-} ]]; then
    echo 'Set FUSEE_RELEASE_SIGNING_KEYS or FUSEE_RELEASE_SSH_SIGNERS to the authorized public release signers.' >&2
    exit 1
fi
umask 077
key_dir=$(mktemp -d)
trap 'rm -rf "$key_dir"' EXIT
export GNUPGHOME="$key_dir"
printf '%s\n' no-auto-key-retrieve no-auto-key-import >"$key_dir/gpg.conf"
if [[ -n ${FUSEE_RELEASE_SIGNING_KEYS:-} ]]; then
    printf '%s\n' "$FUSEE_RELEASE_SIGNING_KEYS" | gpg --batch --import
fi
printf '%s\n' "${FUSEE_RELEASE_SSH_SIGNERS:-}" >"$key_dir/allowed_signers"
git -c gpg.ssh.allowedSignersFile="$key_dir/allowed_signers" verify-tag "refs/tags/$tag"
