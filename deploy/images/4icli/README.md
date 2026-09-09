# 4icli Docker Image

Containerized 4Innovation CLI for ACO REACH data downloads.

## Credential model

The runtime container is a **read-only consumer** of credentials. It never
calls `4icli configure` or `4icli rotate`. Rotation happens in the
4Innovation portal; refresh of the local `config.txt` happens out-of-band
via `bootstrap.sh`.

Layout at runtime:

- **Source of truth:** `/opt/s3/data/workspace/auth/secrets/4icli/config.txt` (host)
  → same path inside the container via the workspace volume mount.
- **Reader path:** `entrypoint.sh` copies the workspace auth config to
  `$HOME/.config/4icli/config.txt` (XDG) on every container start.
  If the source file is missing, the entrypoint exits non-zero —
  no env-var fallback, no auto-`configure`.
- **Legacy fallback:** `/opt/s3/data/workspace/bronze/config.txt` is still
  consumed if the new auth path is absent, so existing installs can migrate
  by running `aco 4icli setup`.

## Bootstrap (after portal rotation)

When 4Innovation issues a new key/secret in the portal:

```bash
aco 4icli setup
```

The setup command updates `deploy/.env`, runs `bootstrap.sh`, verifies with a
real `datahub -v` call, and only then copies the resulting `config.txt` to the
workspace auth source of truth. If verify fails, the previous config is left
untouched.

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
- `bootstrap.sh` — verified config refresh after portal rotation
- `config.txt.example` — example shape only (not used by image)

## Security

- `config.txt` lives under `/opt/s3/data/workspace/auth/secrets/4icli/` and is
  never baked into the image.
- The runtime container has no `FOURICLI_API_KEY` / `FOURICLI_API_SECRET`
  env wiring — credentials only ever exist on disk in encrypted form.
- Container runs as non-root (`care`, uid 1002).
- Ubuntu 22.04 base.

## Diagnostics

Use the shared auth doctor before assuming a download failure is a vendor issue:

```bash
aco auth doctor --service 4icli
aco auth register-ip 4icli --ip <portal-registered-public-ip>
aco auth harden
```
