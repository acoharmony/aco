# © 2025 HarmonyCares
# All rights reserved.

"""Configuration management for ACOMS CLI integration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


def get_current_year() -> int:
    """Return the current calendar year."""
    return datetime.now().year


def load_profile_config(profile: str | None = None) -> dict[str, Any]:
    """Load ACO Harmony profile configuration."""
    from .._config_loader import load_aco_config

    config = load_aco_config()
    profiles = config.get("profiles", {})
    active_profile = profile or os.getenv("ACO_PROFILE") or config.get("default_profile", "dev")

    if active_profile not in profiles:
        raise ValueError(
            f"Profile '{active_profile}' not found. Available profiles: {list(profiles.keys())}"
        )

    return profiles[active_profile]


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _parse_env_value(raw: str) -> str:
    value = raw.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def load_deploy_env(deploy_dir: Path | None = None) -> dict[str, str]:
    """
    Load simple KEY=VALUE entries from deploy/.env without exporting them.

    Only values needed to configure ACOMS are read by callers; nothing here logs
    or prints secrets.
    """
    env_path = (deploy_dir or (_project_root() / "deploy")) / ".env"
    if not env_path.exists():
        return {}

    loaded: dict[str, str] = {}
    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        key = key.strip()
        if key:
            loaded[key] = _parse_env_value(value)

    return loaded


@dataclass
class AcomsConfig:
    """Profile-aware configuration for ACOMS CLI operations."""

    binary_path: Path
    working_dir: Path
    data_path: Path
    bronze_dir: Path
    archive_dir: Path
    silver_dir: Path
    gold_dir: Path
    log_dir: Path
    tracking_dir: Path
    container_name: str = "acoms"
    api_key: str | None = None
    api_secret: str | None = None
    default_aco_id: str | None = None
    default_year: int = get_current_year()
    enable_logging: bool = True
    command_timeout: int = 3600
    list_timeout: int = 120
    request_delay: float = 1.0

    @classmethod
    def from_profile(cls, profile: str | None = None) -> AcomsConfig:
        """Create config from the active ACO Harmony profile and deploy/.env."""
        profile_config = load_profile_config(profile)
        deploy_env = load_deploy_env()

        storage_config = profile_config.get("storage", {})
        data_path = Path(storage_config.get("data_path", "/opt/s3/data/workspace"))

        bronze_dir = data_path / "bronze"
        archive_dir = data_path / "archive"
        silver_dir = data_path / "silver"
        gold_dir = data_path / "gold"
        log_dir = data_path / "logs"
        tracking_dir = log_dir / "tracking"

        acoms_config = profile_config.get("acoms", {})

        binary_path = Path(
            os.getenv("ACOMS_BINARY_PATH")
            or deploy_env.get("ACOMS_BINARY_PATH")
            or acoms_config.get("binary_path")
            or "/usr/local/bin/acoms"
        )
        working_dir = Path(
            os.getenv("ACOMS_WORKING_DIR")
            or deploy_env.get("ACOMS_WORKING_DIR")
            or acoms_config.get("working_dir")
            or bronze_dir
        )

        default_year_raw = (
            os.getenv("ACOMS_DEFAULT_YEAR")
            or deploy_env.get("ACOMS_DEFAULT_YEAR")
            or acoms_config.get("default_year")
            or get_current_year()
        )

        return cls(
            binary_path=binary_path,
            working_dir=working_dir,
            data_path=data_path,
            bronze_dir=bronze_dir,
            archive_dir=archive_dir,
            silver_dir=silver_dir,
            gold_dir=gold_dir,
            log_dir=log_dir,
            tracking_dir=tracking_dir,
            container_name=(
                os.getenv("ACOMS_CONTAINER_NAME")
                or deploy_env.get("ACOMS_CONTAINER_NAME")
                or acoms_config.get("container_name")
                or "acoms"
            ),
            api_key=os.getenv("ACOMS_API_KEY") or deploy_env.get("ACOMS_API_KEY"),
            api_secret=os.getenv("ACOMS_API_SECRET") or deploy_env.get("ACOMS_API_SECRET"),
            default_aco_id=(
                os.getenv("ACOMS_API_ID")
                or deploy_env.get("ACOMS_API_ID")
                or acoms_config.get("default_aco_id")
                or os.getenv("ACO_ENTITY_ID")
            ),
            default_year=int(default_year_raw),
            command_timeout=int(
                os.getenv("ACOMS_COMMAND_TIMEOUT")
                or deploy_env.get("ACOMS_COMMAND_TIMEOUT")
                or acoms_config.get("command_timeout")
                or 3600
            ),
            list_timeout=int(
                os.getenv("ACOMS_LIST_TIMEOUT")
                or deploy_env.get("ACOMS_LIST_TIMEOUT")
                or acoms_config.get("list_timeout")
                or 120
            ),
            request_delay=float(
                os.getenv("ACOMS_REQUEST_DELAY")
                or deploy_env.get("ACOMS_REQUEST_DELAY")
                or acoms_config.get("request_delay")
                or 1.0
            ),
        )

    def validate(self) -> None:
        """Ensure local storage paths used by ACOMS workflows exist."""
        self.working_dir.mkdir(parents=True, exist_ok=True)
        self.ensure_storage_directories()

    def ensure_storage_directories(self) -> None:
        """Create storage and tracking directories lazily."""
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        self.tracking_dir.mkdir(parents=True, exist_ok=True)
        if self.enable_logging:
            self.log_dir.mkdir(parents=True, exist_ok=True)

    def resolve_aco_id(self, aco_id: str | None = None) -> str:
        """Return an explicit or configured ACO Entity ID."""
        resolved = aco_id or self.default_aco_id
        if not resolved:
            raise ValueError(
                "ACOMS ACO Entity ID is not configured. Set ACOMS_API_ID in "
                "the environment or deploy/.env."
            )
        return resolved
