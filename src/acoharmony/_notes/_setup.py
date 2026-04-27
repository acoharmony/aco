# © 2025 HarmonyCares
# All rights reserved.

"""Notebook environment setup."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Literal

from ._base import PluginRegistry

MedallionTier = Literal["bronze", "silver", "gold"]


class SetupPlugins(PluginRegistry):
    """One-call notebook bootstrap: sys.path, storage, catalog, medallion paths."""

    def setup_project_path(self) -> Path:
        project_root = Path("/home/care/acoharmony")
        if project_root.exists() and str(project_root / "src") not in sys.path:
            sys.path.insert(0, str(project_root / "src"))
        return project_root

    def initialize(self, setup_path: bool = True) -> dict[str, Any]:
        """Bootstrap a notebook session and return all the handles it needs."""
        if setup_path:
            self.setup_project_path()
        return {
            "storage": self.storage,
            "catalog": self.catalog,
            "gold_path": self.get_medallion_path("gold"),
            "silver_path": self.get_medallion_path("silver"),
            "bronze_path": self.get_medallion_path("bronze"),
        }

    def get_medallion_path(self, tier: MedallionTier) -> Path:
        try:
            return Path(self.storage.get_path(tier))
        except Exception:  # ALLOWED: notebook bootstrap fallback for offline tests
            return Path(f"/opt/s3/data/workspace/{tier}")
