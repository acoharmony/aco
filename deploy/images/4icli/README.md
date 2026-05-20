# 4icli Docker Image

Containerized 4Innovation CLI for ACO REACH data downloads.

## Credential model

The runtime container is a **read-only consumer** of credentials. It never
calls `4icli configure` or `4icli rotate`. Rotation happens in the
4Innovation portal; refresh of the local `config.txt` happens out-of-band
via `bootstrap.sh`.

Layout at runtime:

- **Source of truth:** `/opt/s3/data/workspace/bronze/config.txt` (host)
  → same path inside the container via the workspace volume mount.
- **Reader path:** `entrypoint.sh` copies `$BRONZE/config.txt` to
  `$HOME/.config/4icli/config.txt` (XDG) on every container start.
  If the source file is missing, the entrypoint exits non-zero —
  no env-var fallback, no auto-`configure`.

## Bootstrap (after portal rotation)

When 4Innovation issues a new key/secret in the portal:

```bash
deploy/images/4icli/bootstrap.sh KEY SECRET [APM_ID]
```

The script spins up a throwaway container, runs `4icli configure`,
verifies with a real `datahub -v` call, and only then copies the
resulting `config.txt` to `$BRONZE/config.txt`. If verify fails, the
source of truth is left untouched — protects against pasting a typo or
a not-yet-active key.

After bootstrap, restart the runtime service to pick up the new file:

```bash
docker compose -f deploy/docker-compose.yml restart 4icli
```

## Profile awareness

Storage paths come from the active profile in `aco.toml`:

- **dev profile:** `/opt/s3/data/workspace` → bronze at `/opt/s3/data/workspace/bronze`
- **prod profile:** uses profile-specific `storage.data_path`

The container's `working_dir` matches the profile's bronze directory, so
`4icli datahub -d` (which writes to `$PWD`) lands files in the right place.

## Files

- `4icli` — real Go binary (~70MB)
- `Dockerfile` — image definition
- `entrypoint.sh` — XDG seed + fail-loud-if-missing
- `bootstrap.sh` — operator script for refreshing `config.txt` after portal rotation
- `config.txt.example` — example shape only (not used by image)

## Security

- `config.txt` lives in the workspace volume and is never baked into the image.
- The runtime container has no `FOURICLI_API_KEY` / `FOURICLI_API_SECRET`
  env wiring — credentials only ever exist on disk in encrypted form.
- Container runs as non-root (`care`, uid 1002).
- Ubuntu 22.04 base.
