# © 2025 HarmonyCares
# All rights reserved.

"""Configuration management for 4icli integration."""

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


def get_current_year() -> int:
    """
    Get the current year.

    Returns:
        Current year as an integer
    """
    return datetime.now().year


def load_profile_config(profile: str | None = None) -> dict[str, Any]:
    """Load 4icli configuration from the packaged aco.toml."""
    from .._config_loader import load_aco_config

    config = load_aco_config()
    profiles = config.get("profiles", {})

    active_profile = (
        profile or os.getenv("ACO_PROFILE") or config.get("default_profile", "dev")
    )

    if active_profile not in profiles:
        raise ValueError(
            f"Profile '{active_profile}' not found. Available profiles: {list(profiles.keys())}"
        )

    return profiles[active_profile]


@dataclass
class FourICLIConfig:
    """
    Configuration for 4icli binary and operations.

        Profile-aware and storage-aware configuration that reads from
        acoharmony/config profiles to determine storage paths.
    """

    # Binary location
    binary_path: Path

    # Working directory - CRITICAL: 4icli reads config.txt from current directory
    # Must contain config.txt with credentials
    working_dir: Path

    # Storage paths (profile-aware)
    data_path: Path  # Base storage path from profile
    bronze_dir: Path  # Bronze tier - where 4icli downloads files
    archive_dir: Path  # Archive tier - where processed ZIPs are moved
    silver_dir: Path  # Silver tier - processed data
    gold_dir: Path  # Gold tier - final outputs
    log_dir: Path  # Log directory (profile-aware)
    tracking_dir: Path  # State tracking directory

    # API Credentials (from environment)
    api_key: str | None = None
    api_secret: str | None = None

    # Default query parameters
    default_year: int = get_current_year()
    default_apm_id: str | None = None

    # Logging
    enable_logging: bool = True

    # Timeouts (in seconds)
    command_timeout: int = 3600  # 1 hour for downloads
    list_timeout: int = 60  # 1 minute for listing operations

    # Rate limiting
    request_delay: float = 2.0  # Seconds to wait between requests (API rate limit)

    @classmethod
    def from_profile(cls, profile: str | None = None) -> "FourICLIConfig":
        """Create config from acoharmony profile configuration."""
        profile_config = load_profile_config(profile)

        # Get storage path from profile
        storage_config = profile_config.get("storage", {})
        data_path = Path(storage_config.get("data_path", "/opt/s3/data/workspace"))

        # Storage tiers based on medallion architecture
        bronze_dir = data_path / "bronze"
        archive_dir = data_path / "archive"
        silver_dir = data_path / "silver"
        gold_dir = data_path / "gold"

        # Get 4icli settings from profile
        fouricli_config = profile_config.get("fouricli", {})

        # 4icli binary path - profile-aware
        binary_path = Path(
            os.getenv("FOURICLI_BINARY_PATH")
            or fouricli_config.get("binary_path")
            or "/usr/local/bin/4icli"
        )

        # Working directory must contain config.txt for 4icli credentials
        # Defaults to bronze tier
        working_dir = Path(os.getenv("FOURICLI_WORKING_DIR", str(bronze_dir)))

        # Log directory - profile-aware, uses workspace logs
        log_dir = data_path / "logs"

        # State tracking directory - should be under logs for organization
        tracking_dir = data_path / "logs" / "tracking"

        # Config path from profile (profile-aware)
        config_path_str = fouricli_config.get("config_path")
        if config_path_str:
            # If relative path, make it relative to project root
            if not config_path_str.startswith("/"):
                project_root = Path(__file__).parent.parent.parent.parent
                config_path = project_root / config_path_str
            else:
                config_path = Path(config_path_str)
        else:
            # Fallback: check common locations
            config_path = None

        instance = cls(
            binary_path=binary_path,
            working_dir=working_dir,
            data_path=data_path,
            bronze_dir=bronze_dir,
            archive_dir=archive_dir,
            silver_dir=silver_dir,
            gold_dir=gold_dir,
            log_dir=log_dir,
            tracking_dir=tracking_dir,
            api_key=os.getenv("FOURICLI_API_KEY"),
            api_secret=os.getenv("FOURICLI_API_SECRET"),
            default_apm_id=os.getenv("FOURICLI_APM_ID")
            or fouricli_config.get("default_apm_id")
            or os.getenv("ACO_APM_ID"),
            default_year=int(
                os.getenv("FOURICLI_DEFAULT_YEAR")
                or fouricli_config.get("default_year")
                or get_current_year()
            ),
        )

        # Store the profile config path for later use
        instance._profile_config_path = config_path

        return instance

    def validate(self) -> None:
        """
        Validate configuration settings.

                For containerized execution, we don't validate binary path since
                it runs in Docker. We do validate storage paths and check for
                pre-configured credentials.

                Note: Directory creation is now lazy - directories are created only when
                actually needed by operations, not during config initialization.
        """
        # Ensure working directory exists (always needed)
        self.working_dir.mkdir(parents=True, exist_ok=True)

        # Check for config.txt using profile-aware lookup
        try:
            self.ensure_config_file()
        except FileNotFoundError:
            # Re-raise with additional context
            raise

    def ensure_storage_directories(self) -> None:
        """
        Create storage directories if they don't exist.

                Call this explicitly when you need the directories, not during initialization.
                This prevents creating directories when just loading config for introspection.
        """
        # Create storage directories if they don't exist
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        self.tracking_dir.mkdir(parents=True, exist_ok=True)

        if self.enable_logging:
            self.log_dir.mkdir(parents=True, exist_ok=True)

    def sync_config_to_deployment(self) -> None:
        """
        Sync config.txt from profile's config_path to deploy/compose/conf/4icli/

                This ensures the Docker container has the latest credentials from the
                profile-specified location (e.g., /usr/local/bin/config.txt for local/dev).
        """
        if not hasattr(self, "_profile_config_path") or not self._profile_config_path:
            return  # No profile config path specified

        source_config = self._profile_config_path
        if not source_config.exists():
            return  # Source doesn't exist, skip sync

        # Target is always deploy/compose/conf/4icli/config.txt
        project_root = Path(__file__).parent.parent.parent.parent
        target_config = project_root / "deploy" / "compose" / "conf" / "4icli" / "config.txt"
        target_config.parent.mkdir(parents=True, exist_ok=True)

        # Only copy if source is newer or target doesn't exist
        if (
            not target_config.exists()
            or source_config.stat().st_mtime > target_config.stat().st_mtime
        ):
            import shutil

            shutil.copy2(source_config, target_config)

    def ensure_config_file(self) -> Path:
        """
        Ensure config.txt exists and is accessible.

                Priority order (profile-aware):
                1. Profile-specified config_path (from profile YAML)
                2. Working directory (bronze) - may be symlink
                3. deploy/compose/conf/4icli/config.txt (fallback)

                For local/dev profiles: Uses /usr/local/bin/config.txt
                For staging/prod profiles: Uses deploy/compose/conf/4icli/config.txt

                Returns path to config.txt.

                Note: config.txt is a hashed file created by '4icli configure' command,
                not a plain text file. It cannot be auto-generated from env vars.
        """
        # 1. Check profile-specified config path first (profile-aware)
        if hasattr(self, "_profile_config_path") and self._profile_config_path:
            if self._profile_config_path.exists():
                # Sync to deploy directory for containers
                self.sync_config_to_deployment()
                return self._profile_config_path

        # 2. Check working directory (bronze) - may be symlink to profile config
        config_file = self.working_dir / "config.txt"
        if config_file.exists():
            return config_file

        # 3. Check deploy/compose/conf/4icli (fallback for containers)
        project_root = Path(__file__).parent.parent.parent.parent
        compose_config = project_root / "deploy" / "compose" / "conf" / "4icli" / "config.txt"

        if compose_config.exists():
            return compose_config

        # No config.txt found
        profile_path = (
            self._profile_config_path if hasattr(self, "_profile_config_path") else "not configured"
        )
        raise FileNotFoundError(
            f"4icli config.txt not found. Checked:\n"
            f"1. Profile config: {profile_path}\n"
            f"2. Working dir: {config_file}\n"
            f"3. Deployment: {compose_config}\n"
            f"Run '4icli configure' to create credentials file."
        )

    def get_alignment_dir(self, alignment_type: str) -> Path:
        """Get directory for alignment files - returns bronze (no nesting)."""
        return self.bronze_dir

    def get_cclf_dir(self, cclf_number: str | None = None) -> Path:
        """Get directory for CCLF files - returns bronze (no nesting)."""
        return self.bronze_dir

    def get_report_dir(self, report_type: str) -> Path:
        """Get directory for report files - returns bronze (no nesting)."""
        return self.bronze_dir
