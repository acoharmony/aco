#!/usr/bin/env bash
# If a config.txt is already present (e.g. bind-mounted from the host's
# XDG config dir), use it as-is. Otherwise bootstrap from env vars by
# driving `4icli configure`'s hidden TTY prompt with `expect`.
set -euo pipefail

CONFIG_FILE="${HOME}/.config/4icli/config.txt"

if [ ! -s "$CONFIG_FILE" ]; then
    : "${FOURICLI_API_KEY:?FOURICLI_API_KEY must be set}"
    : "${FOURICLI_API_SECRET:?FOURICLI_API_SECRET must be set}"

    expect <<EOF >/dev/null
log_user 0
set timeout 15
spawn 4icli configure
expect "API Client ID:"
send -- "$FOURICLI_API_KEY\r"
expect "API Client Secret:"
send -- "$FOURICLI_API_SECRET\r"
expect eof
EOF
fi

exec "$@"
