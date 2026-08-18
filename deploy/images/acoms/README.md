# ACOMS Docker Image

Containerized ACOMS CLI for operator-driven ACO-MS workflows.

This mirrors the deployment shape used by `4icli`: a small non-root Ubuntu
image, the same Zscaler CA trust setup, a long-running compose service for
`docker exec`, and a workspace mount at `/opt/s3/data/workspace`.

## Vendor zip

The ACOMS CLI zip is a local vendor artifact and is intentionally ignored by
git. Put exactly one zip here before building images:

```bash
deploy/images/acoms/src/<vendor-acoms-cli>.zip
```

During image build the zip is extracted to `/opt/acoms`, the first executable
named like `acoms`, `acoms-cli`, or `acomscli` is linked to
`/usr/local/bin/acoms`, and the container runs as user `care` (`uid=1002`).

## Runtime usage

Build the local image from the ignored vendor zip:

```bash
aco deploy acoms build
```

Start the long-running service:

```bash
aco deploy acoms start
```

Run the CLI:

```bash
docker exec -it acoms acoms --help
```

The working directory is `/opt/s3/data/workspace/bronze`, matching the 4icli
service convention so downloaded/exported files land in the shared workspace.

## Config model

No credentials are baked into the image. The normal runtime path reads these
three values from `deploy/.env` through Docker Compose:

```bash
ACOMS_API_KEY=...
ACOMS_API_SECRET=...
ACOMS_API_ID=...
```

On container start, `entrypoint.sh` runs `acoms configure` with the key/secret
and verifies Datahub access with `ACOMS_API_ID`. The container exits instead of
staying up if credentials are missing or invalid. After successful verification
it writes `/home/care/.acoms/auth-ready`, which backs the compose healthcheck.

For operator debugging, a pre-seeded config directory can still be mounted at:

```bash
/opt/s3/data/workspace/bronze/acoms/config/
```

If env credentials are absent and that directory exists, `entrypoint.sh` copies
it into `$HOME/.config/acoms/` with owner-only permissions.

## Files

- `src/*.zip` — local vendor zip, ignored by git
- `Dockerfile` — image definition
- `entrypoint.sh` — optional config seed + command exec
