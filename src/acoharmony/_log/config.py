# © 2025 HarmonyCares
# All rights reserved.

"""
Logging configuration for ACO Harmony.

Provides centralized logging configuration that integrates with storage backends.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class LogConfig:
    """
    Configuration for ACO Harmony logging.

        Integrates with the storage configuration and medallion architecture
        to ensure logs go to the appropriate backend (local filesystem, RustFS, Databricks).

        Attributes

        storage_config : StorageBackend
            Storage configuration instance for path resolution.
        namespace : str
            Namespace for logs (stored separately from medallion layers).
        level : str
            Default logging level.
        format : str
            Log message format.
        json_logs : bool
            Whether to write structured JSON logs.
    """

    storage_config: Any | None = None  # Will be StorageBackend instance
    namespace: str = "logs"
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    json_logs: bool = True
    _base_path: Path | str | None = None

    def __post_init__(self):
        """Initialize storage-aware logging."""
        if self.storage_config is None:
            # Try to create storage config with default profile
            try:
                import os

                from .._store import StorageBackend

                profile = os.getenv("ACO_PROFILE", "local")
                self.storage_config = StorageBackend(profile=profile)
            except ImportError:
                # Skinny install: no StorageBackend, use defaults
                pass
            except Exception as e:
                import os

                from .._exceptions import StorageBackendError

                profile = os.getenv("ACO_PROFILE")
                raise StorageBackendError.from_initialization_error(e, profile) from e

        # Ensure log directory exists for local storage
        if self.storage_config and self.storage_config.get_storage_type() == "local":
            log_path = self.get_base_path()
            if isinstance(log_path, Path):
                log_path.mkdir(parents=True, exist_ok=True)

    def get_base_path(self) -> Path | str:
        """
        Get the base path for logs using storage configuration.

                Returns

                Path or str
                    Base path for logs (local Path or cloud URL string)
        """
        if self._base_path is not None:
            return self._base_path

        if self.storage_config is not None:
            return self.storage_config.get_path("logs")

        # Skinny install fallback: use temp directory
        import tempfile
        return Path(tempfile.gettempdir()) / "acoharmony" / "logs"

    @classmethod
    def from_env(cls) -> LogConfig:
        """
        Create config from environment variables.

                Checks for:
                - ACO_PROFILE (for storage profile)
                - ACOHARMONY_LOG_LEVEL
                - ACOHARMONY_LOG_JSON

                Returns

                LogConfig
                    Configuration from environment.
        """
        import os

        try:
            from .._store import StorageBackend
        except ImportError:
            # Skinny install: no StorageBackend, use defaults
            level = os.getenv("ACOHARMONY_LOG_LEVEL", "INFO")
            json_logs = os.getenv("ACOHARMONY_LOG_JSON", "true").lower() == "true"
            return cls(level=level, json_logs=json_logs)

        # Create storage config from environment profile
        profile = os.getenv("ACO_PROFILE", "local")
        try:
            storage_config = StorageBackend(profile=profile)
        except Exception as e:
            from .._exceptions import StorageBackendError

            raise StorageBackendError.from_initialization_error(e, profile) from e

        level = os.getenv("ACOHARMONY_LOG_LEVEL", "INFO")
        json_logs = os.getenv("ACOHARMONY_LOG_JSON", "true").lower() == "true"

        return cls(storage_config=storage_config, level=level, json_logs=json_logs)

    @classmethod
    def from_storage(cls, storage_config) -> LogConfig:
        """
        Create config from storage configuration.

                Parameters

                storage_config : StorageBackend
                    Storage configuration instance.

                Returns

                LogConfig
                    Configuration using the storage config.
        """
        return cls(storage_config=storage_config)


# Global configuration instance
_config: LogConfig | None = None


def setup_logging(config: LogConfig | None = None, profile: str | None = None) -> LogConfig:
    """
    Setup global logging configuration.

        Parameters

        config : LogConfig, optional
            Configuration to use. If None, creates from environment.
        profile : str, optional
            Storage profile to use (overrides environment).

        Returns

        LogConfig
            The active configuration.
    """
    global _config

    # Check if already configured
    if _config is not None and config is None:
        return _config

    if config is None:
        if profile:
            # Create config with specific profile
            try:
                from .._store import StorageBackend

                storage_config = StorageBackend(profile=profile)
                config = LogConfig.from_storage(storage_config)
            except ImportError:
                # Skinny install: no StorageBackend, use defaults
                config = LogConfig()
        else:
            # Create from environment
            config = LogConfig.from_env()

    _config = config

    # Configure Python logging (only if not already configured)
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(level=getattr(logging, config.level), format=config.format)

    # Log the configuration
    logger = logging.getLogger("acoharmony.log")
    base_path = config.get_base_path()
    storage_type = config.storage_config.get_storage_type() if config.storage_config else "unknown"
    logger.info("Logging configured", extra={"backend": storage_type, "path": str(base_path)})

    return config


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.

        Parameters

        name : str
            Name of the logger.

        Returns

        Logger
            Configured logger instance.
    """
    # Ensure logging is setup
    global _config
    if _config is None:
        setup_logging()

    return logging.getLogger(f"acoharmony.{name}")
