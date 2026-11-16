#!/usr/bin/env bash
# Docker exec does not inherit entrypoint's umask. Secure OAuth/keyring writes too.
set -euo pipefail
umask 077
if [[ $# == 1 && $1 == --version ]]; then
    # Upstream 0.8.0 has no --version flag; report its installed package metadata.
    exec python3 -c 'from importlib.metadata import version; print("google-colab-cli " + version("google-colab-cli"))'
fi
if [[ $# -ge 2 && $1 == auth && $2 == login ]]; then
    # Upstream's `auth` command authenticates a VM, not the local CLI account.
    shift 2
    exec python3 /app/docker/colab_login.py "$@"
fi
exec /usr/local/bin/colab-upstream "$@"
