#!/usr/bin/env bash
# Bootstrap the persistent ACOMS config from a fresh portal-issued key/secret pair.
#
# Flow:
#   1. Spin up a throwaway ACOMS container with no entrypoint.
#   2. Run `acoms configure --key $KEY --secret $SECRET --env $ACOMS_ENV`.
#   3. Verify with `acoms datahub --view --aco $ACO_ID --year <year>`.
#   4. On success, atomically copy the in-container config directory to workspace/auth.
#   5. Tear down the throwaway container.
#
# Usage:
#   deploy/images/acoms/bootstrap.sh [KEY SECRET ACO_ID [ACOMS_ENV]]
#
# With no arguments, reads ACOMS_API_KEY / ACOMS_API_SECRET / ACOMS_API_ID /
# ACOMS_ENV from deploy/.env (relative to this script).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../../.env"

if [ "$#" -ge 3 ]; then
    KEY="$1"
    SECRET="$2"
    ACO_ID="$3"
    ACOMS_ENV="${4:-prod}"
elif [ "$#" -eq 0 ] && [ -f "$ENV_FILE" ]; then
    # shellcheck disable=SC1090
    set -a; . "$ENV_FILE"; set +a
    KEY="${ACOMS_API_KEY:?ACOMS_API_KEY missing from $ENV_FILE}"
    SECRET="${ACOMS_API_SECRET:?ACOMS_API_SECRET missing from $ENV_FILE}"
    ACO_ID="${ACOMS_API_ID:?ACOMS_API_ID missing from $ENV_FILE}"
    ACOMS_ENV="${ACOMS_ENV:-prod}"
else
    echo "usage: $0 [KEY SECRET ACO_ID [ACOMS_ENV]]" >&2
    echo "  with no args: reads ACOMS_API_KEY/SECRET/API_ID/ENV from $ENV_FILE" >&2
    echo "  KEY       - portal-issued ACOMS API key" >&2
    echo "  SECRET    - portal-issued ACOMS API secret" >&2
    echo "  ACO_ID    - ACO entity used for the verify call" >&2
    echo "  ACOMS_ENV - vendor environment (default: prod)" >&2
    exit 2
fi

YEAR="$(date -u +%Y)"
AUTH_ROOT="${ACO_AUTH_ROOT:-/opt/s3/data/workspace/auth}"
WORKSPACE_CONFIG_DIR="${ACOMS_CONFIG_SOURCE_DIR:-$AUTH_ROOT/secrets/acoms/config}"
LEGACY_WORKSPACE_CONFIG_DIR="${ACOMS_LEGACY_CONFIG_SOURCE_DIR:-/opt/s3/data/workspace/bronze/acoms/config}"
MANIFEST_FILE="$(dirname "$WORKSPACE_CONFIG_DIR")/manifest.json"
IMAGE="ghcr.io/acoharmony/acoms:latest"
NAME="acoms-bootstrap-$$"

mkdir -p "$(dirname "$WORKSPACE_CONFIG_DIR")"
chmod 700 "$AUTH_ROOT" "$AUTH_ROOT/secrets" "$(dirname "$WORKSPACE_CONFIG_DIR")" 2>/dev/null || true

cleanup() {
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    rm -f "${VERIFY_LOG:-}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[acoms-bootstrap] starting throwaway container $NAME"
docker run -d --rm \
    --name "$NAME" \
    --entrypoint /bin/sh \
    --user 1002:1002 \
    "$IMAGE" -c 'tail -f /dev/null' >/dev/null

echo "[acoms-bootstrap] running acoms configure"
docker exec "$NAME" acoms configure --key "$KEY" --secret "$SECRET" --env "$ACOMS_ENV" >/dev/null

VERIFY_LOG="$(mktemp)"
echo "[acoms-bootstrap] verifying creds against datahub (aco=$ACO_ID year=$YEAR env=$ACOMS_ENV)"
if ! docker exec "$NAME" acoms datahub --view --aco "$ACO_ID" --year "$YEAR" >"$VERIFY_LOG" 2>&1; then
    echo "[acoms-bootstrap] verify call failed - saved creds do not authenticate." >&2
    echo "[acoms-bootstrap] $WORKSPACE_CONFIG_DIR was NOT modified." >&2
    echo "[acoms-bootstrap] Check KEY/SECRET/ACO_ID and confirm the key is active." >&2
    sed -n '1,20p' "$VERIFY_LOG" >&2
    exit 1
fi

if grep -Eiq 'unauthorized|forbidden|(^|[^0-9])(401|403)([^0-9]|$)|invalid client|authentication failed' "$VERIFY_LOG"; then
    echo "[acoms-bootstrap] verify output looked like an auth failure." >&2
    echo "[acoms-bootstrap] $WORKSPACE_CONFIG_DIR was NOT modified." >&2
    sed -n '1,20p' "$VERIFY_LOG" >&2
    exit 1
fi

REMOTE_CONFIG_DIR=""
for candidate in /home/care/.config/acoms-cli /home/care/.config/acoms; do
    if docker exec "$NAME" sh -c "test -d '$candidate' && find '$candidate' -type f -print -quit | grep -q ."; then
        REMOTE_CONFIG_DIR="$candidate"
        break
    fi
done

if [ -z "$REMOTE_CONFIG_DIR" ]; then
    echo "[acoms-bootstrap] configure succeeded but no ACOMS config directory was found." >&2
    exit 1
fi

echo "[acoms-bootstrap] verify ok - copying config directory to $WORKSPACE_CONFIG_DIR"
tmp_dir="$(mktemp -d "$(dirname "$WORKSPACE_CONFIG_DIR")/.config.XXXXXX")"
docker cp "$NAME:$REMOTE_CONFIG_DIR/." "$tmp_dir/"
chmod -R go-rwx "$tmp_dir"

if [ -d "$WORKSPACE_CONFIG_DIR" ]; then
    backup="$WORKSPACE_CONFIG_DIR.bak.$(date -u +%Y%m%dT%H%M%SZ)"
    mv "$WORKSPACE_CONFIG_DIR" "$backup"
    chmod -R go-rwx "$backup"
    echo "[acoms-bootstrap] backed up previous config to $backup"
fi

mv "$tmp_dir" "$WORKSPACE_CONFIG_DIR"
chmod -R go-rwx "$WORKSPACE_CONFIG_DIR"

fingerprint="$(find "$WORKSPACE_CONFIG_DIR" -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}')"
cat >"$MANIFEST_FILE" <<EOF
{
  "service": "acoms",
  "entity_id": "$ACO_ID",
  "vendor_env": "$ACOMS_ENV",
  "config_dir": "$WORKSPACE_CONFIG_DIR",
  "legacy_config_dir": "$LEGACY_WORKSPACE_CONFIG_DIR",
  "credential_fingerprint": "sha256:$fingerprint",
  "verified_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
chmod 600 "$MANIFEST_FILE"

echo "[acoms-bootstrap] done. Restart the ACOMS service to pick up new creds:"
echo "[acoms-bootstrap]   docker compose -f deploy/docker-compose.yml restart acoms"
