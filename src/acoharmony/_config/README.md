# _config

Configuration management for all profiles and environments.

## Purpose

Manage configuration across profiles. Profile-based settings. Environment variables.

## Profiles

From `pyproject.toml`:

### local
- **Workers:** 2
- **Memory:** 4GB
- **Batch:** 1000
- **Storage:** Local filesystem
- **Database:** Disabled
- **Validation:** Disabled

### dev (default)
- **Workers:** 12
- **Memory:** 40GB
- **Batch:** 10000
- **Storage:** Local filesystem (`/opt/s3/data/workspace`)
- **Database:** PostgreSQL (enabled)
- **Validation:** Enabled

### staging
- **Workers:** 12
- **Memory:** 40GB
- **Batch:** 50000
- **Storage:** S3-compatible (s3api)
- **Database:** PostgreSQL
- **Backend:** DuckDB SQL

### prod
- **Workers:** 16
- **Memory:** 32GB
- **Batch:** 100000
- **Storage:** Databricks Unity Catalog
- **Database:** Production database
- **Backend:** Databricks SQL
- **Monitoring:** Enabled

## Usage

```python
from acoharmony._config import get_profile, get_config

# Get current profile
profile = get_profile()  # Returns "dev" by default

# Get configuration
config = get_config()
workers = config.processing.max_workers
```

## Configuration Sources

1. `pyproject.toml` - Base configuration
2. Environment variables - Override config
3. `.env` files - Local overrides

## Key Settings

- **ACO_PROFILE** - Active profile (local/dev/staging/prod)
- **Storage paths** - Data tier locations
- **Database connections** - EDW, tracking DB
- **Processing limits** - Workers, memory, batch size
- **4icli settings** - APM ID, config paths

## Integration

All modules use `_config/` for settings.
