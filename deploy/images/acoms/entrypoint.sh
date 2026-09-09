#!/usr/bin/env bash
# Seed ACOMS CLI config, verify authentication, then exec the requested command.
#
# The runtime container is a read-only consumer of persisted credentials.
# Rotation happens through `aco acoms setup` / deploy/images/acoms/bootstrap.sh,
# which writes a verified config directory under workspace/auth.
set -euo pipefail

WORKSPACE_CONFIG_DIR="${ACOMS_CONFIG_SOURCE_DIR:-/opt/s3/data/workspace/auth/secrets/acoms/config}"
LEGACY_WORKSPACE_CONFIG_DIR="${ACOMS_LEGACY_CONFIG_SOURCE_DIR:-/opt/s3/data/workspace/bronze/acoms/config}"
CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/acoms-cli"
ACOMS_ENV="${ACOMS_ENV:-prod}"
ACOMS_VERIFY_ON_START="${ACOMS_VERIFY_ON_START:-1}"
ACOMS_VERIFY_YEAR="${ACOMS_VERIFY_YEAR:-$(date -u +%Y)}"
ACOMS_READY_FILE="${ACOMS_READY_FILE:-$ACOMS_HOME/auth-ready}"
ACOMS_ALLOW_ENV_CONFIG_ON_START="${ACOMS_ALLOW_ENV_CONFIG_ON_START:-0}"

mkdir -p "$CONFIG_DIR" "$ACOMS_HOME"
rm -f "$ACOMS_READY_FILE"

if [ -n "${ACO_AUTH_CERT_BUNDLE:-}" ] && [ -s "$ACO_AUTH_CERT_BUNDLE" ]; then
    RUNTIME_CA_BUNDLE="${ACO_RUNTIME_CA_BUNDLE:-/tmp/acoharmony-ca-bundle.pem}"
    cat /etc/ssl/certs/ca-certificates.crt "$ACO_AUTH_CERT_BUNDLE" >"$RUNTIME_CA_BUNDLE"
    export NODE_EXTRA_CA_CERTS="$RUNTIME_CA_BUNDLE"
    export SSL_CERT_FILE="$RUNTIME_CA_BUNDLE"
    export REQUESTS_CA_BUNDLE="$RUNTIME_CA_BUNDLE"
fi

require_env() {
    name="$1"
    if [ -z "${!name:-}" ]; then
        echo "[acoms-entrypoint] ${name} is missing." >&2
        echo "[acoms-entrypoint] Run: aco acoms setup" >&2
        exit 1
    fi
}

config_dir_has_files() {
    [ -d "$1" ] && find "$1" -type f -print -quit | grep -q .
}

seed_config_dir() {
    source_dir="$1"
    echo "[acoms-entrypoint] seeding ACOMS config from $source_dir." >&2
    rm -rf "$CONFIG_DIR"
    mkdir -p "$CONFIG_DIR"
    cp -R "$source_dir/." "$CONFIG_DIR/"
    chmod -R go-rwx "$CONFIG_DIR"
}

if config_dir_has_files "$WORKSPACE_CONFIG_DIR"; then
    seed_config_dir "$WORKSPACE_CONFIG_DIR"
elif config_dir_has_files "$LEGACY_WORKSPACE_CONFIG_DIR"; then
    echo "[acoms-entrypoint] using legacy config source: $LEGACY_WORKSPACE_CONFIG_DIR" >&2
    echo "[acoms-entrypoint] migrate with: aco acoms setup" >&2
    seed_config_dir "$LEGACY_WORKSPACE_CONFIG_DIR"
elif [ "$ACOMS_ALLOW_ENV_CONFIG_ON_START" = "1" ]; then
    require_env ACOMS_API_KEY
    require_env ACOMS_API_SECRET
    require_env ACOMS_API_ID

    echo "[acoms-entrypoint] configuring ACOMS credentials from env fallback." >&2
    echo "[acoms-entrypoint] this fallback is disabled by default; prefer aco acoms setup." >&2
    acoms configure \
        --key "$ACOMS_API_KEY" \
        --secret "$ACOMS_API_SECRET" \
        --env "$ACOMS_ENV" >/dev/null
else
    echo "[acoms-entrypoint] no persisted ACOMS config found." >&2
    echo "[acoms-entrypoint] Expected: $WORKSPACE_CONFIG_DIR" >&2
    echo "[acoms-entrypoint] Run: aco acoms setup" >&2
    exit 1
fi

if [ "$ACOMS_VERIFY_ON_START" = "1" ]; then
    require_env ACOMS_API_ID
    echo "[acoms-entrypoint] verifying ACOMS Datahub access." >&2
    acoms datahub \
        --view \
        --aco "$ACOMS_API_ID" \
        --year "$ACOMS_VERIFY_YEAR" >/dev/null
fi

printf 'ok\n' >"$ACOMS_READY_FILE"

exec "$@"
