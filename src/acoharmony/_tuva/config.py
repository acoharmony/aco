# © 2025 HarmonyCares
# All rights reserved.

"""
Tuva configuration and path management.

Handles locating the Tuva dbt project and managing configuration variables.
"""

import os
from pathlib import Path
from typing import Any

import yaml


class TuvaConfig:
    """
    Configuration manager for Tuva Health integration.

    Handles locating the Tuva dbt project directory and loading
    configuration from dbt_project.yml.
    """

    def __init__(self, tuva_root: Path | None = None):
        """
        Initialize Tuva configuration.

        Args:
            tuva_root: Optional path to Tuva project root.
                      Defaults to bundled Tuva repo in _depends
        """
        if tuva_root is None:
            # Try bundled repo first
            bundled_tuva = Path(__file__).parent / "_depends" / "repos" / "tuva"
            if bundled_tuva.exists():
                tuva_root = bundled_tuva
            else:
                # Fallback to environment or default
                tuva_root = Path(os.getenv("TUVA_ROOT", "/home/care/tuva"))

        self.tuva_root = tuva_root

        if not self.tuva_root.exists():
            raise FileNotFoundError(f"Tuva project not found at {self.tuva_root}")

        self.dbt_project_file = self.tuva_root / "dbt_project.yml"
        if not self.dbt_project_file.exists():
            raise FileNotFoundError(f"dbt_project.yml not found at {self.dbt_project_file}")

        self._config = self._load_config()

    def _load_config(self) -> dict[str, Any]:
        """Load dbt_project.yml configuration."""
        with open(self.dbt_project_file) as f:
            return yaml.safe_load(f)

    @property
    def project_name(self) -> str:
        """Get Tuva project name."""
        return self._config.get("name", "the_tuva_project")

    @property
    def version(self) -> str:
        """Get Tuva version."""
        return self._config.get("version", "unknown")

    @property
    def models_dir(self) -> Path:
        """Get path to models directory."""
        return self.tuva_root / "models"

    @property
    def macros_dir(self) -> Path:
        """Get path to macros directory."""
        return self.tuva_root / "macros"

    @property
    def seeds_dir(self) -> Path:
        """Get path to seeds directory."""
        return self.tuva_root / "seeds"

    def get_vars(self) -> dict[str, Any]:
        """Get dbt project variables."""
        return self._config.get("vars", {})

    def get_model_config(self, model_path: str) -> dict[str, Any]:
        """
        Get configuration for a specific model path.

        Args:
            model_path: Model path (e.g., 'cms_hcc', 'quality_measures')

        Returns:
            Model configuration dictionary
        """
        models_config = self._config.get("models", {})

        # Navigate through the model path
        parts = model_path.split(".")
        current = models_config

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part, {})
            else:
                return {}

        return current if isinstance(current, dict) else {}

    def list_model_directories(self) -> list[str]:
        """List all model directories in Tuva project."""
        if not self.models_dir.exists():
            return []

        return [
            d.name for d in self.models_dir.iterdir() if d.is_dir() and not d.name.startswith(".")
        ]

    def __repr__(self) -> str:
        return f"TuvaConfig(root={self.tuva_root}, version={self.version})"
