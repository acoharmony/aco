#!/usr/bin/env bash
# Bootstrap 4icli config.txt on container start.
#
# Source of truth is $BRONZE/config.txt (generated once via
# `4icli configure` against the real API and committed alongside the
# workspace). We copy it into $HOME/.config/4icli/config.txt — 4icli's
# expected XDG location — so any subcommand resolves the creds.
#
# Fallback: if the workspace config is missing, bootstrap from
# FOURICLI_API_KEY/FOURICLI_API_SECRET via `4icli configure`. The
# env-var path used to be the primary route; it's been demoted because
# the credentials in deploy/.env are stale (the API rejects them), so
# the workspace-resident config is the only one that actually works.
set -euo pipefail

CONFIG_FILE="${HOME}/.config/4icli/config.txt"
WORKSPACE_CONFIG="/opt/s3/data/workspace/bronze/config.txt"

mkdir -p "$(dirname "$CONFIG_FILE")"

if [ -s "$WORKSPACE_CONFIG" ]; then
    cp "$WORKSPACE_CONFIG" "$CONFIG_FILE"
    chmod 600 "$CONFIG_FILE"
elif [ ! -s "$CONFIG_FILE" ]; then
    : "${FOURICLI_API_KEY:?FOURICLI_API_KEY must be set (or mount a valid config.txt at $WORKSPACE_CONFIG)}"
    : "${FOURICLI_API_SECRET:?FOURICLI_API_SECRET must be set (or mount a valid config.txt at $WORKSPACE_CONFIG)}"
    4icli configure --key "$FOURICLI_API_KEY" --secret "$FOURICLI_API_SECRET" >/dev/null
fi

exec "$@"
