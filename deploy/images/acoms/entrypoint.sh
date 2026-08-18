#!/usr/bin/env bash
# Seed ACOMS CLI config, verify authentication, then exec the requested command.
#
# Primary source of truth is deploy/.env, loaded into the compose service as:
#
#   ACOMS_API_KEY     -> configure --key
#   ACOMS_API_SECRET  -> configure --secret
#   ACOMS_API_ID      -> datahub --aco verification
#
# If env credentials are not present, a pre-seeded workspace config directory
# can still be consumed for operator debugging, but the normal service path
# fails loudly rather than staying up unauthenticated.
set -euo pipefail

WORKSPACE_CONFIG_DIR="${ACOMS_WORKSPACE_CONFIG_DIR:-/opt/s3/data/workspace/bronze/acoms/config}"
CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/acoms-cli"
ACOMS_ENV="${ACOMS_ENV:-prod}"
ACOMS_VERIFY_ON_START="${ACOMS_VERIFY_ON_START:-1}"
ACOMS_VERIFY_YEAR="${ACOMS_VERIFY_YEAR:-$(date -u +%Y)}"
ACOMS_READY_FILE="${ACOMS_READY_FILE:-$ACOMS_HOME/auth-ready}"

mkdir -p "$CONFIG_DIR" "$ACOMS_HOME"
rm -f "$ACOMS_READY_FILE"

require_env() {
    name="$1"
    if [ -z "${!name:-}" ]; then
        echo "[acoms-entrypoint] ${name} is missing." >&2
        echo "[acoms-entrypoint] Set ACOMS_API_KEY, ACOMS_API_SECRET, and ACOMS_API_ID in deploy/.env." >&2
        exit 1
    fi
}

if [ -n "${ACOMS_API_KEY:-}" ] || [ -n "${ACOMS_API_SECRET:-}" ] || [ -n "${ACOMS_API_ID:-}" ]; then
    require_env ACOMS_API_KEY
    require_env ACOMS_API_SECRET
    require_env ACOMS_API_ID

    echo "[acoms-entrypoint] configuring ACOMS credentials from deploy environment." >&2
    acoms configure \
        --key "$ACOMS_API_KEY" \
        --secret "$ACOMS_API_SECRET" \
        --env "$ACOMS_ENV" >/dev/null

    if [ "$ACOMS_VERIFY_ON_START" = "1" ]; then
        echo "[acoms-entrypoint] verifying ACOMS Datahub access." >&2
        acoms datahub \
            --view \
            --aco "$ACOMS_API_ID" \
            --year "$ACOMS_VERIFY_YEAR" >/dev/null
    fi
    printf 'ok\n' >"$ACOMS_READY_FILE"
elif [ -d "$WORKSPACE_CONFIG_DIR" ] && find "$WORKSPACE_CONFIG_DIR" -type f -print -quit | grep -q .; then
    echo "[acoms-entrypoint] seeding ACOMS config from workspace config directory." >&2
    cp -R "$WORKSPACE_CONFIG_DIR/." "$CONFIG_DIR/"
    chmod -R go-rwx "$CONFIG_DIR"
    printf 'ok\n' >"$ACOMS_READY_FILE"
else
    echo "[acoms-entrypoint] no ACOMS credentials found." >&2
    echo "[acoms-entrypoint] Set ACOMS_API_KEY, ACOMS_API_SECRET, and ACOMS_API_ID in deploy/.env." >&2
    exit 1
fi

exec "$@"
