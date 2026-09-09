#!/usr/bin/env bash
# Bootstrap the persistent 4icli config from a fresh portal-issued key/secret pair.
#
# When 4Innovation rotates credentials in the portal, the operator runs
# this script once to produce a new workspace auth config. The runtime 4icli
# container (deploy/services/4icli.yml) is read-only with respect to
# config.txt — it never calls `4icli configure` or `4icli rotate`.
#
# Flow:
#   1. Spin up a throwaway 4icli container with no entrypoint.
#   2. Run `4icli configure --key $KEY --secret $SECRET` inside it.
#   3. Verify with `4icli datahub -v -a $APM -y <year>`. If 401, abort
#      without touching the persisted config — fresh portal creds that
#      can't auth means a copy/paste error or a not-yet-active key.
#   4. On success, atomically copy the in-container config.txt to workspace/auth.
#   5. Tear down the throwaway container.
#
# Usage:
#   deploy/images/4icli/bootstrap.sh [KEY SECRET [APM_ID]]
#
# With no arguments, reads FOURICLI_API_KEY / FOURICLI_API_SECRET /
# FOURICLI_APM_ID from deploy/.env (relative to this script). Pass KEY
# and SECRET explicitly to override .env. APM_ID defaults to D0259
# (HarmonyCares); override for verification against a different APM
# entity.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../../.env"

if [ "$#" -ge 2 ]; then
    KEY="$1"
    SECRET="$2"
    APM_ID="${3:-D0259}"
elif [ "$#" -eq 0 ] && [ -f "$ENV_FILE" ]; then
    # shellcheck disable=SC1090
    set -a; . "$ENV_FILE"; set +a
    KEY="${FOURICLI_API_KEY:?FOURICLI_API_KEY missing from $ENV_FILE}"
    SECRET="${FOURICLI_API_SECRET:?FOURICLI_API_SECRET missing from $ENV_FILE}"
    APM_ID="${FOURICLI_APM_ID:-D0259}"
else
    echo "usage: $0 [KEY SECRET [APM_ID]]" >&2
    echo "  with no args: reads FOURICLI_API_KEY/SECRET/APM_ID from $ENV_FILE" >&2
    echo "  KEY     - portal-issued 4i API client ID" >&2
    echo "  SECRET  - portal-issued 4i API client secret" >&2
    echo "  APM_ID  - APM entity for verify call (default: D0259)" >&2
    exit 2
fi

YEAR="$(date -u +%Y)"

AUTH_ROOT="${ACO_AUTH_ROOT:-/opt/s3/data/workspace/auth}"
WORKSPACE_CONFIG="${FOURICLI_WORKSPACE_CONFIG:-$AUTH_ROOT/secrets/4icli/config.txt}"
LEGACY_WORKSPACE_CONFIG="${FOURICLI_LEGACY_WORKSPACE_CONFIG:-/opt/s3/data/workspace/bronze/config.txt}"
MANIFEST_FILE="$(dirname "$WORKSPACE_CONFIG")/manifest.json"
IMAGE="ghcr.io/acoharmony/4icli:latest"
NAME="4icli-bootstrap-$$"

mkdir -p "$(dirname "$WORKSPACE_CONFIG")"
chmod 700 "$AUTH_ROOT" "$AUTH_ROOT/secrets" "$(dirname "$WORKSPACE_CONFIG")" 2>/dev/null || true

cleanup() {
    docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[bootstrap] starting throwaway container $NAME"
docker run -d --rm \
    --name "$NAME" \
    --entrypoint /bin/sh \
    --user 1002:1002 \
    "$IMAGE" -c 'tail -f /dev/null' >/dev/null

echo "[bootstrap] running 4icli configure"
docker exec "$NAME" 4icli configure --key "$KEY" --secret "$SECRET" >/dev/null

echo "[bootstrap] verifying creds against datahub (apm=$APM_ID year=$YEAR)"
if ! docker exec "$NAME" 4icli datahub -v -a "$APM_ID" -y "$YEAR" >/dev/null 2>&1; then
    echo "[bootstrap] verify call failed — saved creds do not authenticate." >&2
    echo "[bootstrap] $WORKSPACE_CONFIG was NOT modified." >&2
    echo "[bootstrap] Check that KEY/SECRET match the portal exactly and the key is active." >&2
    exit 1
fi

echo "[bootstrap] verify ok — copying config.txt to $WORKSPACE_CONFIG"
tmp_config="$(mktemp "$(dirname "$WORKSPACE_CONFIG")/.config.txt.XXXXXX")"
docker cp "$NAME:/home/care/.config/4icli/config.txt" "$tmp_config"
chmod 600 "$tmp_config"

if [ -s "$WORKSPACE_CONFIG" ]; then
    backup="$WORKSPACE_CONFIG.bak.$(date -u +%Y%m%dT%H%M%SZ)"
    cp -p "$WORKSPACE_CONFIG" "$backup"
    chmod 600 "$backup"
    echo "[bootstrap] backed up previous config to $backup"
fi

mv "$tmp_config" "$WORKSPACE_CONFIG"
chmod 600 "$WORKSPACE_CONFIG"

fingerprint="$(sha256sum "$WORKSPACE_CONFIG" | awk '{print $1}')"
cat >"$MANIFEST_FILE" <<EOF
{
  "service": "4icli",
  "entity_id": "$APM_ID",
  "config_path": "$WORKSPACE_CONFIG",
  "legacy_config_path": "$LEGACY_WORKSPACE_CONFIG",
  "credential_fingerprint": "sha256:$fingerprint",
  "verified_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
chmod 600 "$MANIFEST_FILE"

echo "[bootstrap] done. Restart the 4icli service to pick up new creds:"
echo "[bootstrap]   docker compose -f deploy/docker-compose.yml restart 4icli"
