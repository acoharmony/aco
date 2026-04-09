# 4Innovation CLI Configuration

Configuration directory for 4Innovation DataHub CLI integration.

## Contents

- `4icli` - 4Innovation CLI binary (70MB, v0.0.20)
- `config.txt` - API credentials (gitignored, example below)
- `docs/` - API documentation and mappings
- `logs/` - Legacy log directory (logs now in workspace/logs)
- `scripts/` - Helper scripts
- `src/` - Source documentation

## Purpose

This directory contains the 4icli binary and related configuration files for reference and non-Docker usage.

**Note**: The primary execution mode is now containerized via the Docker image at `compose/images/4icli/`.

## Credentials

Credentials are managed in two ways:

### 1. Environment Variables (Recommended)
```bash
# In .env
FOURICLI_API_KEY=your_api_key_here
FOURICLI_API_SECRET=your_api_secret_here
```

The Python client automatically creates `config.txt` from environment variables at runtime.

### 2. Manual config.txt
```
api_key:api_secret
```

Place in the working directory (typically `workspace/bronze/config.txt`).

## Docker Integration

The 4icli binary in `compose/images/4icli/4icli` is copied from this directory during Docker image builds:

```dockerfile
COPY 4icli /usr/local/bin/4icli
```

See `compose/images/4icli/README.md` for Docker usage.

## Python Module

The Python wrapper is in `src/acoharmony/_4icli/`. See:
- [Module README](../../../src/acoharmony/_4icli/README.md)
- [Test Suite](../../../tests/_4icli/)

## Security

- `config.txt` is gitignored and should never be committed
- Credentials are bind-mounted read-only into containers
- Environment variable approach keeps secrets out of filesystem
