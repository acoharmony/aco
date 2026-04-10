# © 2025 HarmonyCares
# All rights reserved.

"""
Storage configuration for ACO Harmony.

Provides a simple abstraction over different storage backends:

- Local filesystem at /opt/s3/data/workspace
- RustFS S3-compatible storage (staging profile)
- Databricks Unity Catalog (production profile)

The project uses medallion architecture (bronze/silver/gold), and profiles
map this to the appropriate backend storage.
"""

import os
from pathlib import Path
from typing import Any

from .medallion import MedallionLayer


class StorageBackend:
    """
    Storage backend configuration for profile-based data access.

         the actual storage backend (local filesystem, RustFS S3-compatible,
        or Databricks) based on the active profile (local, dev, staging, prod). It reads from
        YAML configuration files in src/acoharmony/_config/profiles/ to determine where and
        how to store data.
            'local'

        Get paths for different medallion layers:
            PosixPath('/opt/s3/data/workspace/bronze')
            PosixPath('/opt/s3/data/workspace/silver')
            PosixPath('/opt/s3/data/workspace/gold')
    """

    def __init__(self, profile: str | None = None):
        """
        Initialize storage configuration.

                Parameters

                profile : str, optional
                    Storage profile to use ('local', 'dev', 'staging', 'prod').
                    Defaults to ACO_PROFILE env var or 'local'.
                    'local'

                Use a specific profile:
                    'staging'
        """
        self.profile = profile or os.getenv("ACO_PROFILE", "local")
        self.config = self._load_config()
        self.project_root = Path(__file__).parent.parent.parent

    def _expand_env_vars(self, value):
        """Recursively expand environment variables in config values."""
        import re

        if isinstance(value, str):
            # Match ${VAR:-default} or ${VAR}
            def replacer(match):
                var_name = match.group(1)
                default = match.group(2) if match.group(2) else ""
                return os.getenv(var_name, default)

            return re.sub(r"\$\{([^:}]+)(?::-(.*?))?\}", replacer, value)
        elif isinstance(value, dict):
            return {k: self._expand_env_vars(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._expand_env_vars(item) for item in value]
        else:
            return value

    def _load_config(self) -> dict[str, Any]:
        """Load the active profile from the packaged aco.toml file."""
        from ._config_loader import load_aco_config

        config = load_aco_config()
        profiles = config.get("profiles", {})

        if self.profile not in profiles:
            raise ValueError(
                f"Profile '{self.profile}' not found. Available profiles: {list(profiles.keys())}"
            )

        # Expand environment variables in the profile block
        return self._expand_env_vars(profiles[self.profile])

    def get_data_path(self, subpath: str = "") -> Path | str:
        """
        Get the data path for the current profile.

                Args:
                    subpath: Optional subpath within the data directory
                            (e.g., "bronze", "silver", "gold")

                Returns:
                    Path or string (for S3/cloud paths) to the data location
                    '/opt/s3/data/workspace'

                Get specific medallion layer path:
                    '/opt/s3/data/workspace/bronze'
        """
        storage_config = self.config.get("storage", {})

        # Get the base data path from the profile
        data_path = storage_config.get("data_path")

        if not data_path:
            # Fallback to project-local /data if not specified
            data_path = str(self.project_root / "data")

        # Handle cloud storage paths (S3, Azure, etc.)
        if data_path.startswith(("s3://", "az://", "gs://")):
            # For cloud storage, append subpath with /
            if subpath:
                return f"{data_path.rstrip('/')}/{subpath}"
            return data_path

        # For local filesystem
        base_path = Path(data_path)
        if subpath:
            full_path = base_path / subpath
        else:
            full_path = base_path

        # Create directory if it doesn't exist (local only)
        full_path.mkdir(parents=True, exist_ok=True)

        return full_path

    def get_path(self, tier: str | MedallionLayer) -> Path | str:
        """
        Get path for a specific medallion layer.

                Args:
                    tier: Medallion layer string (bronze, silver, gold, tmp, logs, cites, cites/corpus, cites/raw) or
                          MedallionLayer enum (BRONZE, SILVER, GOLD)

                Returns:
                    Path or string to the storage location
                    '/opt/s3/data/workspace/bronze'
                    '/opt/s3/data/workspace/silver'

                Use MedallionLayer enum:
                    '/opt/s3/data/workspace/gold'

                Citation storage:
                    '/opt/s3/data/workspace/cites/corpus'
        """
        # Convert MedallionLayer to tier string if needed
        if isinstance(tier, MedallionLayer):
            tier_str = tier.data_tier
        else:
            tier_str = tier

        # Map tiers to subdirectories (medallion architecture)
        tier_mapping = {
            "bronze": "bronze",  # Raw, unprocessed data
            "silver": "silver",  # Cleaned, validated data
            "gold": "gold",  # Business-level aggregates
            "tmp": "tmp",
            "logs": "logs",
            "cites": "cites",  # Citation data storage
            "cites/corpus": "cites/corpus",  # Processed citation corpus
            "cites/raw": "cites/raw",  # Raw citation downloads
        }

        subpath = tier_mapping.get(tier_str.lower(), tier_str)
        return self.get_data_path(subpath)

    def get_storage_type(self) -> str:
        """
        Get the storage backend type.

                Returns:
                    Storage type: 'local', 's3', 's3api', 'databricks', 'duckdb'
                's3api'
                'local'
        """
        storage_config = self.config.get("storage", {})
        backend = storage_config.get("backend", "local")

        # Check data_path for cloud indicators
        data_path = storage_config.get("data_path", "")
        if data_path.startswith("s3://"):
            # Could be s3api (RustFS), s3, or databricks
            if backend in ["s3api", "s3", "databricks"]:
                return backend
            return "s3"

        return backend

    def get_environment(self) -> str:
        """Get current environment."""
        return self.config.get("environment", "local")

    def get_connection_params(self) -> dict[str, Any]:
        """
        Get connection parameters for the storage backend.

                Returns:
                    Dictionary with backend-specific connection parameters
                    '/opt/s3/data/workspace'

                S3-compatible storage (RustFS via s3api):
                    'http://s3api:10001'
        """
        storage_config = self.config.get("storage", {})
        backend = self.get_storage_type()

        if backend in ["s3", "s3api"]:
            return {
                "endpoint": storage_config.get("endpoint"),
                "access_key": storage_config.get("access_key"),
                "secret_key": storage_config.get("secret_key"),
                "bucket": storage_config.get("bucket"),
                "region": storage_config.get("region", "us-east-1"),
                "use_ssl": storage_config.get("use_ssl", True),
            }
        elif backend == "databricks":
            return {
                "host": storage_config.get("databricks_host"),
                "token": storage_config.get("databricks_token"),
                "catalog": storage_config.get("catalog", "main"),
                "schema": storage_config.get("schema", "aco_harmony"),
            }
        elif backend == "duckdb":
            return {
                "database": storage_config.get("database", "aco_harmony.db"),
                "read_only": storage_config.get("read_only", False),
            }
        else:
            return {"base_path": self.get_data_path()}
