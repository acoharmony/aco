# 4icli Docker Image

Containerized 4Innovation CLI for ACO REACH data downloads.

## Build

```bash
cd compose/images/4icli
docker build -t acoharmony/4icli:latest .
```

## Usage

The Python client (`acoharmony._4icli.FourICLI`) automatically runs 4icli via Docker with:

- **Storage backend mount**: Profile-aware bronze tier mounted to `/workspace/bronze`
- **config.txt**: Bind-mounted from `working_dir/config.txt` to `/workspace/bronze/config.txt`
- **Working directory**: `/workspace/bronze` (where config.txt is expected)

### Manual Usage

```bash
# List categories
docker run --rm \
  -v /opt/s3/data/workspace/bronze:/workspace/bronze:rw \
  -v /opt/s3/data/workspace/bronze/config.txt:/workspace/bronze/config.txt:ro \
  -w /workspace/bronze \
  acoharmony/4icli:latest \
  datahub -a D0259 -l

# Download CCLF files
docker run --rm \
  -v /opt/s3/data/workspace/bronze:/workspace/bronze:rw \
  -v /opt/s3/data/workspace/bronze/config.txt:/workspace/bronze/config.txt:ro \
  -w /workspace/bronze \
  acoharmony/4icli:latest \
  datahub -a D0259 -c CCLF -d --createdWithinLastWeek
```

## Profile Awareness

Storage paths are derived from active profile in `config/profiles.yaml`:

- **dev profile**: `/opt/s3/data/workspace` → bronze at `/opt/s3/data/workspace/bronze`
- **prod profile**: Uses profile-specific `storage.data_path`

The container mounts the profile-specific bronze directory, ensuring downloads go to the correct storage backend.

## Files

- `4icli` - Binary executable (70MB)
- `Dockerfile` - Container image definition
- `config.txt.example` - Example credentials file (not included in image)

## Security

- `config.txt` is bind-mounted read-only at runtime (never baked into image)
- Container runs as non-root
- Minimal Ubuntu 22.04 base image
