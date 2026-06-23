"""Tests for Databricks UC volume helpers."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pytest

import acoharmony._databricks._uc_volume as uc_volume
from acoharmony._databricks._uc_tables import FsEntry
from acoharmony._databricks._uc_volume import (
    build_copy_commands,
    copy_to_uc_volumes,
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


class _FakeDatabricksCli:
    entries: list[FsEntry] = []

    def __init__(self, **kwargs):
        pass

    @property
    def available(self) -> bool:
        return True

    def fs_ls(self, path: str) -> list[FsEntry]:
        return self.entries


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

    @pytest.mark.unit
    def test_copy_to_uc_volumes_skips_unchanged_source(self, tmp_path, monkeypatch, capsys):
        storage = _Storage(tmp_path)
        source = tmp_path / "bronze"
        source.mkdir()
        raw_file = source / "raw.csv"
        raw_file.write_text("a,b\n1,2\n", encoding="utf-8")

        monkeypatch.setattr(uc_volume, "StorageBackend", lambda profile=None: storage)
        monkeypatch.setattr(uc_volume.shutil, "which", lambda value: value)

        calls: list[list[str]] = []

        def fake_run_command(command: list[str], *, dry_run: bool) -> None:
            calls.append(command)

        monkeypatch.setattr(uc_volume, "run_command", fake_run_command)

        args = argparse.Namespace(
            aco_profile=None,
            databricks_bin="databricks",
            destination=None,
            dry_run=False,
            force=False,
            layer="bronze",
            overwrite=True,
            profile=None,
            skip_mkdir=True,
            source=None,
            state_file=str(tmp_path / "state.json"),
            target=None,
            concurrency=4,
        )

        assert copy_to_uc_volumes(args) == 0
        assert len(calls) == 1

        assert copy_to_uc_volumes(args) == 0
        assert len(calls) == 1
        assert "Skipping bronze" in capsys.readouterr().out

        time.sleep(0.01)
        raw_file.write_text("a,b\n3,4\n", encoding="utf-8")

        assert copy_to_uc_volumes(args) == 0
        assert len(calls) == 2

    @pytest.mark.unit
    def test_copy_to_uc_volumes_bootstraps_from_matching_destination_volume(
        self, tmp_path, monkeypatch, capsys
    ):
        storage = _Storage(tmp_path)
        source = tmp_path / "bronze"
        source.mkdir()
        raw_file = source / "raw.csv"
        raw_file.write_text("a,b\n1,2\n", encoding="utf-8")

        _FakeDatabricksCli.entries = [
            FsEntry(
                path="/Volumes/uat_sandbox/gov_programs/bronze/raw.csv",
                name="raw.csv",
                is_dir=False,
                size=raw_file.stat().st_size,
                modification_time=1,
            )
        ]

        monkeypatch.setattr(uc_volume, "StorageBackend", lambda profile=None: storage)
        monkeypatch.setattr(uc_volume, "DatabricksCli", _FakeDatabricksCli)
        monkeypatch.setattr(uc_volume.shutil, "which", lambda value: value)

        calls: list[list[str]] = []
        monkeypatch.setattr(
            uc_volume,
            "run_command",
            lambda command, *, dry_run: calls.append(command),
        )

        args = argparse.Namespace(
            aco_profile=None,
            databricks_bin="databricks",
            destination=None,
            dry_run=False,
            force=False,
            layer="bronze",
            overwrite=True,
            profile=None,
            skip_mkdir=True,
            source=None,
            state_file=str(tmp_path / "state.json"),
            target=None,
            concurrency=4,
        )

        assert copy_to_uc_volumes(args) == 0
        assert calls == []
        assert "destination volume already matches source" in capsys.readouterr().out
        assert (tmp_path / "state.json").exists()
