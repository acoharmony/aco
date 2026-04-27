#!/usr/bin/env bash
# Bootstrap $BRONZE/config.txt from a fresh portal-issued key/secret pair.
#
# When 4Innovation rotates credentials in the portal, the operator runs
# this script once to produce a new $BRONZE/config.txt. The runtime 4icli
# container (deploy/services/4icli.yml) is read-only with respect to
# config.txt — it never calls `4icli configure` or `4icli rotate`.
#
# Flow:
#   1. Spin up a throwaway 4icli container with no entrypoint.
#   2. Run `4icli configure --key $KEY --secret $SECRET` inside it.
#   3. Verify with `4icli datahub -v -a $APM -y <year>`. If 401, abort
#      without touching $BRONZE/config.txt — fresh portal creds that
#      can't auth means a copy/paste error or a not-yet-active key.
#   4. On success, copy the in-container config.txt to $BRONZE/config.txt.
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

WORKSPACE_CONFIG="/opt/s3/data/workspace/bronze/config.txt"
IMAGE="ghcr.io/acoharmony/4icli:latest"
NAME="4icli-bootstrap-$$"

if [ ! -d "$(dirname "$WORKSPACE_CONFIG")" ]; then
    echo "[bootstrap] $(dirname "$WORKSPACE_CONFIG") does not exist on host." >&2
    echo "[bootstrap] Bring up the workspace mount before bootstrapping." >&2
    exit 1
fi

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
docker cp "$NAME:/home/care/.config/4icli/config.txt" "$WORKSPACE_CONFIG"
chmod 644 "$WORKSPACE_CONFIG"

echo "[bootstrap] done. Restart the 4icli service to pick up new creds:"
echo "[bootstrap]   docker compose -f deploy/docker-compose.yml restart 4icli"
