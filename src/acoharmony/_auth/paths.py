"""Shared filesystem paths for persisted CLI authentication material."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _profile_data_path(profile: str | None = None) -> Path:
    """Return the active profile's storage data path."""
    from .._config_loader import load_aco_config

    config: dict[str, Any] = load_aco_config()
    profiles = config.get("profiles", {})
    active_profile = profile or os.getenv("ACO_PROFILE") or config.get("default_profile", "dev")
    profile_config = profiles.get(active_profile, {})
    storage_config = profile_config.get("storage", {})
    return Path(storage_config.get("data_path", "/opt/s3/data/workspace"))


@dataclass(frozen=True)
class AuthPaths:
    """Concrete paths used by auth setup, runtime containers, and diagnostics."""

    data_path: Path
    root: Path
    registry: Path
    secrets_dir: Path
    logs_dir: Path
    certs_dir: Path
    cert_bundle: Path
    fouricli_config: Path
    acoms_config_dir: Path
    legacy_fouricli_config: Path
    legacy_acoms_config_dir: Path


def get_auth_paths(profile: str | None = None) -> AuthPaths:
    """Resolve auth paths from profile storage and optional environment overrides."""
    data_path = _profile_data_path(profile)
    root = Path(os.getenv("ACO_AUTH_ROOT", str(data_path / "auth")))
    secrets_dir = Path(os.getenv("ACO_AUTH_SECRETS_DIR", str(root / "secrets")))
    logs_dir = Path(os.getenv("ACO_AUTH_LOGS_DIR", str(root / "logs")))
    certs_dir = Path(os.getenv("ACO_AUTH_CERTS_DIR", str(root / "certs")))

    return AuthPaths(
        data_path=data_path,
        root=root,
        registry=Path(os.getenv("ACO_AUTH_REGISTRY", str(root / "registry.yaml"))),
        secrets_dir=secrets_dir,
        logs_dir=logs_dir,
        certs_dir=certs_dir,
        cert_bundle=Path(os.getenv("ACO_AUTH_CERT_BUNDLE", str(certs_dir / "zscaler-bundle.pem"))),
        fouricli_config=Path(
            os.getenv(
                "FOURICLI_WORKSPACE_CONFIG",
                str(secrets_dir / "4icli" / "config.txt"),
            )
        ),
        acoms_config_dir=Path(
            os.getenv(
                "ACOMS_CONFIG_SOURCE_DIR",
                str(secrets_dir / "acoms" / "config"),
            )
        ),
        legacy_fouricli_config=data_path / "bronze" / "config.txt",
        legacy_acoms_config_dir=data_path / "bronze" / "acoms" / "config",
    )
