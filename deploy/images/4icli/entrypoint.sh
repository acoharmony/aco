#!/usr/bin/env bash
# Seed 4icli's XDG config from the workspace source of truth.
#
# The runtime container is a read-only consumer of credentials. It never
# runs `4icli configure` or `4icli rotate` — those are bootstrap-time
# operations driven from outside the container, after a portal-issued
# key/secret rotation. See deploy/images/4icli/bootstrap.sh.
#
# This entrypoint copies $WORKSPACE_CONFIG into 4icli's XDG location and
# execs the container command. If $WORKSPACE_CONFIG is missing, it fails
# loudly so the operator knows to run bootstrap — silently regenerating
# from stale env vars is what poisoned bronze/config.txt in the past.
set -euo pipefail

CONFIG_FILE="${HOME}/.config/4icli/config.txt"
WORKSPACE_CONFIG="/opt/s3/data/workspace/bronze/config.txt"

if [ ! -s "$WORKSPACE_CONFIG" ]; then
    echo "[4icli-entrypoint] $WORKSPACE_CONFIG missing or empty." >&2
    echo "[4icli-entrypoint] Run bootstrap with fresh portal creds:" >&2
    echo "[4icli-entrypoint]   deploy/images/4icli/bootstrap.sh KEY SECRET" >&2
    exit 1
fi

mkdir -p "$(dirname "$CONFIG_FILE")"
cp "$WORKSPACE_CONFIG" "$CONFIG_FILE"
chmod 600 "$CONFIG_FILE"

exec "$@"
