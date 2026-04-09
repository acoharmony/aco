# © 2025 HarmonyCares
# All rights reserved.

"""
Unified configuration management for ACO Harmony.

 centralized configuration management for the entire ACOHarmony
system, eliminating hardcoded values and ensuring consistency across all components.
It implements a hierarchical configuration system with multiple sources and intelligent
defaults based on data patterns.

Configuration Hierarchy:
    1. Default values (defined in dataclasses)
    2. Configuration file (YAML format)
    3. Environment variables (highest priority)
    4. Table-specific overrides
    5. Pattern-based adjustments

Environment Variables:
    - ACO_BASE_PATH: Base directory for all data operations
    - ACO_TRACKING: Enable/disable transformation tracking (true/false)
    - ACO_INCREMENTAL: Enable/disable incremental processing (true/false)
    - ACO_CHUNK_SIZE: Number of rows to process in each chunk
    - ACO_LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR)

Configuration Files:
    The system searches for configuration files in the following order:
    1. ./config.yml or ./config.yaml (current directory)
    2. ~/.acoharmony/config.yml (user home)
    3. /etc/acoharmony/config.yml (system-wide)

Example YAML Configuration:
    transform:
        enable_tracking: true
        incremental: true
        chunk_size: 100000
        compression: zstd

    storage:
        base_path: /opt/s3/data/workspace
        bronze_dir: bronze
        silver_dir: silver
        gold_dir: gold

    logging:
        level: INFO
        backend: local

    tables:
        cclf1:
            chunk_size: 50000
            temp_write: true
        bar:
            temp_write: false

"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl
import yaml

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # Python < 3.11


@dataclass
class ProcessingConfig:
    """
    Processing configuration for performance tuning.

        Controls parallel processing, memory limits, and batch sizes.
        Loaded from profile-specific settings in pyproject.toml.

        Attributes:
            batch_size: Number of rows per batch for processing
            max_workers: Maximum number of parallel workers
            memory_limit: Memory limit as string (e.g., "48GB")
    """

    batch_size: int = 10000
    max_workers: int = 4
    memory_limit: str = "16GB"


@dataclass
class TransformConfig:
    """
    Transform-specific configuration for data processing operations.

        This class defines all configuration parameters related to data transformation,
        including processing strategies, performance tuning, and output formatting.
        These settings directly impact how data flows through the transformation pipeline.

        Attributes:
            enable_tracking (bool): Enable state tracking for idempotent transformations.
                When True, the system maintains state files to avoid reprocessing.
                Default: True.

            incremental (bool): Enable incremental processing mode.
                When True, only new/modified files are processed.
                Default: True.

            streaming (bool): Enable streaming mode for large files.
                When True, data is processed in a streaming fashion to minimize memory.
                Default: False (batch processing is more efficient for most cases).

            chunk_size (int): Number of rows to process in each chunk.
                Larger values use more memory but may be faster.
                Default: 100000.

            temp_write (bool): Write to temporary file first, then rename.
                Ensures atomic writes and prevents partial file corruption.
                Default: True.

            max_retries (int): Maximum number of retry attempts for failed operations.
                Applies to file I/O and transformation operations.
                Default: 3.

            row_group_size (int): Parquet row group size for output files.
                Affects file size and query performance.
                Default: 50000.

            compression (str): Compression algorithm for output files.
                Options: 'snappy', 'gzip', 'lz4', 'zstd' (recommended - fastest with best compression).
                Default: 'zstd'.

            compression_level (Optional[int]): Compression level (algorithm-specific).
                Higher values mean better compression but slower processing.
                Default: None (use algorithm default).

        Notes:
            - Chunk size affects memory usage: memory ≈ chunk_size * average_row_size
            - Row group size affects query performance: smaller = better for selective queries
            - Compression choice impacts storage vs speed tradeoff
    """

    enable_tracking: bool = True
    incremental: bool = True
    streaming: bool = False
    chunk_size: int = 100000
    temp_write: bool = True
    max_retries: int = 3
    row_group_size: int = 50000
    compression: str = "zstd"
    compression_level: int | None = None


@dataclass
class StoragePaths:
    """
    Storage paths configuration for medallion architecture.

        This class defines the directory structure and storage locations used throughout
        the ACOHarmony system. It implements the medallion architecture with bronze,
        silver, and gold layers for different data quality tiers.

        Attributes:
            base_path (Path): Root directory for all data operations.
                All other paths are relative to this unless absolute.
                Default: /opt/s3/data/workspace.

            bronze_dir (str): Directory for bronze layer (raw input files).
                Contains original, unmodified source data.
                Default: 'bronze'.

            silver_dir (str): Directory for silver layer (cleaned, validated data).
                Contains data after initial transformations and cleaning.
                Default: 'silver'.

            gold_dir (str): Directory for gold layer (business-level aggregates).
                Contains fully processed, analysis-ready data.
                Default: 'gold'.

            temp_dir (str): Directory for temporary files.
                Used for atomic writes and intermediate operations.
                Default: 'temp'.

            archive_dir (str): Directory for archived/backup data.
                Contains historical versions and backups.
                Default: 'archive'.

            logs_dir (str): Directory for log files.
                Contains application and processing logs.
                Default: 'logs'.

            tracking_dir (str): Directory for tracking state files.
                Contains transformation state for idempotency.
                Default: 'logs/tracking'.

        Directory Structure (Medallion Architecture):
            base_path/
            ├── bronze/          # Raw, unprocessed data
            ├── silver/          # Cleaned, validated data
            ├── gold/            # Business-level aggregates
            ├── temp/            # Temporary working files
            ├── archive/         # Historical backups
            ├── cites/           # Citation data storage
            │   ├── corpus/      # Processed citation corpus
            │   └── raw/         # Raw citation downloads
            └── logs/            # Application logs
                └── tracking/    # State tracking files

        Notes:
            - Directories are created automatically when needed
            - Medallion architecture ensures clear data quality tiers
            - Path resolution handles both relative and absolute paths
    """

    base_path: Path = field(default_factory=lambda: Path("/opt/s3/data/workspace"))
    bronze_dir: str = "bronze"
    silver_dir: str = "silver"
    gold_dir: str = "gold"
    temp_dir: str = "temp"
    archive_dir: str = "archive"
    logs_dir: str = "logs"
    tracking_dir: str = "logs/tracking"
    cites_dir: str = "cites"
    cites_corpus_dir: str = "cites/corpus"
    cites_raw_dir: str = "cites/raw"


@dataclass
class LogConfig:
    """
    Logging configuration for application monitoring and debugging.

        This class configures the logging system used throughout ACOHarmony for
        tracking operations, debugging issues, and monitoring performance.

        Attributes:
            backend (str): Logging backend implementation.
                Options: 'local' (file-based), 'console', 'syslog'.
                Default: 'local'.

            level (str): Minimum logging level to capture.
                Options: 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'.
                Default: 'INFO'.

            format (str): Log message format string.
                Python logging format with placeholders for timestamp, level, etc.
                Default: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'.

            max_file_size (int): Maximum size of a single log file in bytes.
                When exceeded, log rotation occurs.
                Default: 10MB (10 * 1024 * 1024).

            backup_count (int): Number of rotated log files to keep.
                Older files are automatically deleted.
                Default: 5.

        Log Levels:
            - DEBUG: Detailed diagnostic information
            - INFO: General informational messages
            - WARNING: Warning messages for potential issues
            - ERROR: Error messages for recoverable failures
            - CRITICAL: Critical errors requiring immediate attention

        Notes:
            - Log rotation prevents disk space exhaustion
            - Higher log levels include all lower levels
            - Production systems typically use INFO or WARNING
    """

    backend: str = "local"
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5


@dataclass
class AppConfig:
    """
    Main application configuration container.

        This is the top-level configuration class that aggregates all subsystem
        configurations and provides methods for loading from various sources.
        It implements a flexible configuration system with multiple override levels.

        Attributes:
            transform (TransformConfig): Configuration for data transformations.
                Controls processing behavior and performance.

            storage (StoragePaths): Configuration for storage paths.
                Defines directory structure and paths.

            logging (LogConfig): Configuration for logging system.
                Controls log output and retention.

            processing (ProcessingConfig): Configuration for processing performance.
                Controls workers, batch sizes, memory limits from active profile.

            table_configs (Dict[str, Dict[str, Any]]): Table-specific overrides.
                Allows per-table customization of transform settings.
                Keys are table names, values are config overrides.

        Configuration Sources:
            1. Defaults: Built-in values in dataclass definitions
            2. File: YAML configuration files from standard locations
            3. Environment: Environment variables (highest priority)
            4. Table-specific: Per-table overrides and patterns
            5. Profile: Profile-specific settings from pyproject.toml

        Notes:
            - Configuration is loaded lazily on first access
            - Environment variables override file settings
            - Table patterns provide intelligent defaults
    """

    transform: TransformConfig = field(default_factory=TransformConfig)
    storage: StoragePaths = field(default_factory=StoragePaths)
    logging: LogConfig = field(default_factory=LogConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)

    # Table-specific overrides
    table_configs: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_file(cls, config_path: Path | None = None) -> "AppConfig":
        """
        Load configuration from a YAML file.

                Searches for configuration files in standard locations if no path is
                provided. The first found file is used. If no file is found, returns
                default configuration.

                Args:
                    config_path (Optional[Path]): Explicit path to configuration file.
                        If None, searches standard locations in order:
                        1. ./config.yml or ./config.yaml (current directory)
                        2. ~/.acoharmony/config.yml (user home)
                        3. /etc/acoharmony/config.yml (system-wide)

                Returns:
                    AppConfig: Configuration loaded from file or defaults if no file found.

                Notes:
                    - YAML parsing errors will raise exceptions
                    - Missing files return default configuration
                    - First matching file in search path is used
        """
        if config_path is None:
            # Look for config in standard locations
            search_paths = [
                Path.cwd() / "config.yml",
                Path.cwd() / "config.yaml",
                Path.home() / ".acoharmony" / "config.yml",
                Path("/etc/acoharmony/config.yml"),
            ]
            for path in search_paths:
                if path.exists():
                    config_path = path
                    break

        if config_path and config_path.exists():
            with open(config_path) as f:
                data = yaml.safe_load(f)
                return cls.from_dict(data)

        # Return default config
        return cls()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AppConfig":
        """
        Create configuration from a dictionary.

                Parses a dictionary (typically from YAML) into typed configuration
                objects. Handles nested configurations and type conversions.

                Args:
                    data: Dictionary containing configuration data with keys:
                        - transform (dict): Transform configuration parameters
                        - storage (dict): Storage paths and directories
                        - logging (dict): Logging configuration
                        - tables (dict): Table-specific overrides

                Returns:
                    AppConfig: Typed configuration object created from dictionary.

                Notes:
                    - Missing sections use default values
                    - Invalid keys in sections are ignored
                    - base_path is converted to Path object
                    - Table configs are stored as-is for flexibility
        """
        config = cls()

        # Parse transform config
        if "transform" in data:
            config.transform = TransformConfig(**data["transform"])

        # Parse storage config
        if "storage" in data:
            storage_data = data["storage"]
            if "base_path" in storage_data:
                storage_data["base_path"] = Path(storage_data["base_path"])
            config.storage = StoragePaths(**storage_data)

        # Parse logging config
        if "logging" in data:
            config.logging = LogConfig(**data["logging"])

        # Parse table-specific configs
        if "tables" in data:
            config.table_configs = data["tables"]

        return config

    @classmethod
    def from_env(cls) -> "AppConfig":
        """
        Create configuration from environment variables.

                Reads ACO-specific environment variables and creates a configuration
                object with those values. Only specified variables are set; others
                use defaults. This is typically used for overrides.

                Environment Variables:
                    - ACO_BASE_PATH: Base directory path for data operations
                    - ACO_TRACKING: Enable tracking ('true'/'false')
                    - ACO_INCREMENTAL: Enable incremental mode ('true'/'false')
                    - ACO_CHUNK_SIZE: Chunk size for processing (integer)
                    - ACO_LOG_LEVEL: Logging level (DEBUG/INFO/WARNING/ERROR)

                Returns:
                    AppConfig: Configuration with environment variable overrides.

                Notes:
                    - Boolean values are case-insensitive ('true', 'True', 'TRUE')
                    - Invalid values are ignored (defaults used)
                    - This is typically used with from_file() for overrides
        """
        config = cls()

        # Override from environment
        if os.getenv("ACO_BASE_PATH"):
            config.storage.base_path = Path(os.getenv("ACO_BASE_PATH"))

        if os.getenv("ACO_TRACKING"):
            config.transform.enable_tracking = os.getenv("ACO_TRACKING").lower() == "true"

        if os.getenv("ACO_INCREMENTAL"):
            config.transform.incremental = os.getenv("ACO_INCREMENTAL").lower() == "true"

        if os.getenv("ACO_CHUNK_SIZE"):
            config.transform.chunk_size = int(os.getenv("ACO_CHUNK_SIZE"))

        if os.getenv("ACO_LOG_LEVEL"):
            config.logging.level = os.getenv("ACO_LOG_LEVEL")

        return config

    def get_table_config(self, table_name: str) -> TransformConfig:
        """
        Get configuration for a specific table with intelligent defaults.

                Returns a TransformConfig tailored for the given table, applying:
                1. Base transform configuration
                2. Table-specific overrides from config
                3. Pattern-based adjustments (e.g., CCLF files)
                4. Data type optimizations

                Args:
                    table_name: Name of the table to get configuration for.
                        Examples: 'cclf1', 'bar', 'consolidated_enrollment'.

                Returns:
                    TransformConfig: Configuration optimized for the table.

                Pattern-Based Adjustments:
                    - CCLF files: Reduced chunk size (50000), temp writes enabled
                    - Alignment files (bar, alr, etc.): Temp writes disabled
                    - Final outputs: Larger row groups, zstd compression

                Notes:
                    - Creates a new TransformConfig instance (no mutation)
                    - Pattern matching is case-sensitive
                    - Table-specific overrides take precedence over patterns
                    - Unknown tables get default configuration
        """
        # Start with default transform config
        config = TransformConfig(
            enable_tracking=self.transform.enable_tracking,
            incremental=self.transform.incremental,
            streaming=self.transform.streaming,
            chunk_size=self.transform.chunk_size,
            temp_write=self.transform.temp_write,
            max_retries=self.transform.max_retries,
            row_group_size=self.transform.row_group_size,
            compression=self.transform.compression,
            compression_level=self.transform.compression_level,
        )

        # Apply table-specific overrides
        if table_name in self.table_configs:
            overrides = self.table_configs[table_name]
            for key, value in overrides.items():
                if hasattr(config, key):
                    setattr(config, key, value)

        # Special cases based on table patterns
        if table_name.startswith("cclf"):
            # CCLF files need special handling
            config.chunk_size = min(config.chunk_size, 50000)
            config.temp_write = True
            config.streaming = False

        elif table_name in ["bar", "alr", "palmr", "pbvar", "sva"]:
            # Alignment files
            config.temp_write = False

        elif "final" in table_name:
            # Final outputs
            config.row_group_size = 100000
            config.compression = "zstd"
            config.compression_level = 3

        return config


# Singleton instance
_config: AppConfig | None = None


def get_config() -> AppConfig:
    """
    Get the global configuration instance (singleton pattern).

        This function implements lazy loading of configuration, creating it on
        first access and returning the cached instance on subsequent calls.
        The configuration is loaded from files and then overridden with
        environment variables and profile settings.

        Configuration Loading Order:
            1. Load from configuration file (searches standard locations)
            2. Apply profile settings from pyproject.toml
            3. Apply environment variable overrides
            4. Cache the result for future calls

        Returns:
            AppConfig: The global configuration instance.

        Notes:
            - Thread-safe due to GIL (Global Interpreter Lock)
            - Configuration is immutable after loading
            - Use reset_config() to force reload (testing only)
            - Environment variables take precedence over file settings
            - Profile settings from pyproject.toml override defaults

        Side Effects:
            - Creates configuration files/directories on first access
            - Caches configuration in global variable _config
    """
    global _config
    if _config is None:
        # Load from file, then override with environment
        _config = AppConfig.from_file()
        env_config = AppConfig.from_env()

        # Load processing config from profile
        profile_config = load_profile_config_all()
        if "processing" in profile_config:
            proc = profile_config["processing"]
            _config.processing = ProcessingConfig(
                batch_size=proc.get("batch_size", _config.processing.batch_size),
                max_workers=proc.get("max_workers", _config.processing.max_workers),
                memory_limit=proc.get("memory_limit", _config.processing.memory_limit),
            )

        # Merge environment overrides
        if env_config.storage.base_path != _config.storage.base_path:
            _config.storage.base_path = env_config.storage.base_path

        # Apply other env overrides as needed
        if os.getenv("ACO_TRACKING"):
            _config.transform.enable_tracking = env_config.transform.enable_tracking

    return _config


def load_profile_config_all(profile: str | None = None) -> dict[str, Any]:
    """
    Load complete profile configuration from pyproject.toml.

        This is similar to load_polars_config_from_profile but returns
        the entire profile configuration, not just Polars settings.

        Args:
            profile: Profile name (dev, local, staging, prod).
                    If None, uses ACO_PROFILE env var or 'dev' default.

        Returns:
            Dictionary with complete profile configuration.
            Returns empty dict if no profile config found.
    """
    try:
        # Find pyproject.toml in the package root
        package_root = Path(__file__).parent.parent.parent
        pyproject_path = package_root / "pyproject.toml"

        if not pyproject_path.exists():
            return {}

        with open(pyproject_path, "rb") as f:
            pyproject_data = tomllib.load(f)

        # Get ACO Harmony configuration
        acoharmony_config = pyproject_data.get("tool", {}).get("acoharmony", {})
        profiles = acoharmony_config.get("profiles", {})

        # Get active profile
        active_profile = (
            profile or os.getenv("ACO_PROFILE") or acoharmony_config.get("default_profile", "dev")
        )

        if active_profile not in profiles:
            return {}

        # Return entire profile config
        return profiles[active_profile]

    except Exception:
        # If anything fails, return empty dict (graceful degradation)
        return {}


def reset_config():
    """
    Reset the global configuration instance.

        This function clears the cached configuration, forcing it to be reloaded
        on the next call to get_config(). This is primarily intended for testing
        scenarios where configuration needs to be changed between test cases.

        Warning:
            This function should NOT be used in production code as it can cause
            inconsistent configuration state across different parts of the application.

        Side Effects:
            - Clears global _config variable
            - Next get_config() call will reload from files/environment
            - May cause inconsistent state if called during processing

        Notes:
            - Only use in test fixtures and setup/teardown
            - Not thread-safe if called while other threads access config
            - Does not affect already-instantiated configuration objects
    """
    global _config
    _config = None


def load_polars_config_from_profile(profile: str | None = None) -> dict[str, Any]:
    """
    Load Polars configuration from pyproject.toml profile.

        Args:
            profile: Profile name (dev, local, staging, prod).
                    If None, uses ACO_PROFILE env var or 'dev' default.

        Returns:
            Dictionary with Polars configuration settings from the profile.
            Returns empty dict if no Polars config found.
    """
    try:
        # Find pyproject.toml in the package root
        package_root = Path(__file__).parent.parent.parent
        pyproject_path = package_root / "pyproject.toml"

        if not pyproject_path.exists():
            return {}

        with open(pyproject_path, "rb") as f:
            pyproject_data = tomllib.load(f)

        # Get ACO Harmony configuration
        acoharmony_config = pyproject_data.get("tool", {}).get("acoharmony", {})
        profiles = acoharmony_config.get("profiles", {})

        # Get active profile
        active_profile = (
            profile or os.getenv("ACO_PROFILE") or acoharmony_config.get("default_profile", "dev")
        )

        if active_profile not in profiles:
            return {}

        # Get Polars config from profile
        return profiles[active_profile].get("polars", {})

    except Exception:
        # If anything fails, return empty dict (graceful degradation)
        return {}


def apply_polars_config(polars_config: dict[str, Any] | None = None) -> None:
    """
    Apply Polars configuration settings for optimal performance.

        This function configures Polars with settings from the active profile
        in pyproject.toml, optimizing thread usage, chunk sizes, and PyArrow integration.

        Args:
            polars_config: Optional dictionary of Polars settings. If None, loads from
                          active profile in pyproject.toml.

        Configuration applied:
            - max_threads: Number of threads for parallel operations (via POLARS_MAX_THREADS env)
            - streaming_chunk_size: Chunk size for streaming operations
            - string_cache: Enable string caching for categorical data
            - use_pyarrow: Use PyArrow for better Arrow integration (applied during I/O)
            - arrow_large_utf8: Enable large UTF8 strings in Arrow (applied during I/O)

        Notes:
            - Called automatically on import when acoharmony is loaded
            - Settings are global and affect all Polars operations
            - Thread count should match or be less than CPU cores
            - Thread pool size must be set via environment variable before import
    """
    if polars_config is None:
        polars_config = load_polars_config_from_profile()

    if not polars_config:
        # No config found, use sensible defaults
        return

    # Apply thread pool configuration via environment variable
    # Note: This must be set BEFORE polars is imported to take effect
    # We set it here for subprocess/future imports, but it won't affect
    # the already-imported Polars instance
    if "max_threads" in polars_config and "POLARS_MAX_THREADS" not in os.environ:
        os.environ["POLARS_MAX_THREADS"] = str(polars_config["max_threads"])

    # Apply streaming chunk size - controls memory usage in streaming operations
    # Smaller chunks = less memory but potentially slower
    if "streaming_chunk_size" in polars_config:
        chunk_size = polars_config["streaming_chunk_size"]
        pl.Config.set_streaming_chunk_size(chunk_size)

    # CRITICAL: Set flag for execute_stage to use smaller row groups
    # This ensures sink_parquet writes in small batches to prevent memory spikes
    if polars_config.get("force_streaming", False):
        os.environ["ACO_FORCE_STREAMING"] = "1"

    # CRITICAL: NEVER enable string cache - it accumulates memory across stages
    # String cache is explicitly disabled in execute_stage after each stage completes
    # The config option exists for documentation but should ALWAYS be false
    pl.disable_string_cache()

    # Note: use_pyarrow and arrow_large_utf8 are PyArrow settings
    # They're documented in the profile config but are applied during
    # DataFrame I/O operations, not as global Polars settings
