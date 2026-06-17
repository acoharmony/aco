"""Tests for Databricks UC volume helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from acoharmony._databricks._uc_volume import (
    build_copy_commands,
    normalize_uc_path,
    selected_layers,
)
from acoharmony._store import MedallionLayer


class _Storage:
    def __init__(self, root: Path):
        self.root = root

    def get_path(self, tier: str | MedallionLayer) -> Path:
        tier_name = tier.data_tier if isinstance(tier, MedallionLayer) else tier
        return self.root / tier_name

    def get_uc_volume_path(self, tier: str | MedallionLayer) -> str:
        tier_name = tier.data_tier if isinstance(tier, MedallionLayer) else tier
        return f"/Volumes/uat_sandbox/gov_programs/{tier_name}"


class TestUcVolumeHelpers:
    @pytest.mark.unit
    def test_normalize_uc_path_adds_dbfs_scheme(self):
        assert normalize_uc_path("/Volumes/a/b/c") == "dbfs:/Volumes/a/b/c"
        assert normalize_uc_path("dbfs:/Volumes/a/b/c") == "dbfs:/Volumes/a/b/c"

    @pytest.mark.unit
    def test_selected_layers_all(self):
        assert selected_layers("all") == ["bronze", "silver", "gold"]

    @pytest.mark.unit
    def test_selected_layers_rejects_unknown(self):
        with pytest.raises(ValueError, match="Unknown layer"):
            selected_layers("platinum")

    @pytest.mark.unit
    def test_build_copy_commands_uses_storage_defaults(self, tmp_path):
        storage = _Storage(tmp_path)
        commands = build_copy_commands(
            storage=storage,
            layer="silver",
            source=None,
            destination=None,
            databricks_bin="databricks",
            databricks_profile="uat",
            target=None,
            concurrency=4,
            overwrite=True,
        )

        assert len(commands) == 1
        command = commands[0]
        assert command.source == tmp_path / "silver"
        assert command.destination == "/Volumes/uat_sandbox/gov_programs/silver"
        assert command.command == [
            "databricks",
            "--profile",
            "uat",
            "fs",
            "cp",
            str(tmp_path / "silver"),
            "dbfs:/Volumes/uat_sandbox/gov_programs/silver",
            "--recursive",
            "--concurrency",
            "4",
            "--overwrite",
        ]
