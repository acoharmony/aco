"""Tests for the Databricks publishing pipeline."""

from __future__ import annotations

import argparse
from unittest.mock import patch

import pytest

import acoharmony._databricks._pipeline as databricks_pipeline
import acoharmony.cli


class _LogWriter:
    def __init__(self):
        self.entries = []

    def log(self, level, message, **kwargs):
        self.entries.append({"level": level, "message": message, **kwargs})

    def info(self, message, **kwargs):
        self.log("INFO", message, **kwargs)

    def warning(self, message, **kwargs):
        self.log("WARNING", message, **kwargs)

    def error(self, message, **kwargs):
        self.log("ERROR", message, **kwargs)


def _args(**overrides):
    defaults = {
        "aco_profile": None,
        "concurrency": 8,
        "copy_only": False,
        "databricks_bin": "databricks",
        "dry_run": False,
        "force": False,
        "layers": None,
        "no_recurse": False,
        "poll_interval_seconds": 2.0,
        "profile": None,
        "skip_missing_roots": False,
        "state_file": None,
        "table_mode": "managed-delta",
        "tables_only": False,
        "target": None,
        "wait_timeout_seconds": 30,
        "warehouse_id": "a198dca258996e5a",
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class TestDatabricksPipeline:
    @pytest.mark.unit
    def test_pipeline_defaults_to_local_profile_and_runs_copy_then_tables(self, monkeypatch):
        copy_calls = []
        table_calls = []
        log_writer = _LogWriter()
        monkeypatch.setattr(databricks_pipeline, "logger", log_writer)

        monkeypatch.setattr(
            databricks_pipeline,
            "copy_to_uc_volumes",
            lambda args: copy_calls.append(args) or 0,
        )
        monkeypatch.setattr(
            databricks_pipeline,
            "create_tables",
            lambda args: table_calls.append(args) or 0,
        )

        assert databricks_pipeline.run_databricks_pipeline(_args()) == 0

        assert len(copy_calls) == 1
        assert copy_calls[0].aco_profile == "local"
        assert copy_calls[0].layer == "all"
        assert copy_calls[0].overwrite is True
        assert copy_calls[0].force is False

        assert len(table_calls) == 1
        assert table_calls[0].aco_profile == "local"
        assert table_calls[0].layers is None
        assert table_calls[0].replace_existing is True
        assert table_calls[0].warehouse_id == "a198dca258996e5a"
        assert table_calls[0].log_writer is log_writer
        assert [entry["action"] for entry in log_writer.entries] == [
            "start_pipeline",
            "start_step",
            "complete_step",
            "start_step",
            "complete_step",
            "complete_pipeline",
        ]

    @patch("acoharmony._databricks._pipeline.cmd_databricks_pipeline", return_value=7)
    @patch(
        "sys.argv",
        [
            "aco",
            "pipeline",
            "databricks",
            "--aco-profile",
            "staging",
            "--warehouse-id",
            "warehouse-123",
            "--layer",
            "bronze",
            "--layer",
            "gold",
            "--tables-only",
            "--dry-run",
        ],
    )
    @pytest.mark.unit
    def test_cli_dispatches_databricks_pipeline(self, mock_databricks_pipeline):
        result = acoharmony.cli.main()

        assert result == 7
        mock_databricks_pipeline.assert_called_once()
        args = mock_databricks_pipeline.call_args.args[0]
        assert args.name == "databricks"
        assert args.aco_profile == "staging"
        assert args.warehouse_id == "warehouse-123"
        assert args.layers == ["bronze", "gold"]
        assert args.tables_only is True
        assert args.dry_run is True

    @pytest.mark.unit
    def test_pipeline_copies_each_selected_layer_and_updates_tables_once(self, monkeypatch):
        copy_calls = []
        table_calls = []

        monkeypatch.setattr(
            databricks_pipeline,
            "copy_to_uc_volumes",
            lambda args: copy_calls.append(args) or 0,
        )
        monkeypatch.setattr(
            databricks_pipeline,
            "create_tables",
            lambda args: table_calls.append(args) or 0,
        )

        assert (
            databricks_pipeline.run_databricks_pipeline(
                _args(aco_profile="dev", layers=["bronze", "gold"])
            )
            == 0
        )

        assert [args.layer for args in copy_calls] == ["bronze", "gold"]
        assert all(args.aco_profile == "dev" for args in copy_calls)
        assert len(table_calls) == 1
        assert table_calls[0].layers == ["bronze", "gold"]

    @pytest.mark.unit
    def test_pipeline_stops_when_copy_fails(self, monkeypatch):
        table_calls = []

        monkeypatch.setattr(databricks_pipeline, "copy_to_uc_volumes", lambda args: 2)
        monkeypatch.setattr(
            databricks_pipeline,
            "create_tables",
            lambda args: table_calls.append(args) or 0,
        )

        assert databricks_pipeline.run_databricks_pipeline(_args()) == 2
        assert table_calls == []

    @pytest.mark.unit
    def test_tables_only_skips_volume_copy(self, monkeypatch):
        copy_calls = []
        table_calls = []

        monkeypatch.setattr(
            databricks_pipeline,
            "copy_to_uc_volumes",
            lambda args: copy_calls.append(args) or 0,
        )
        monkeypatch.setattr(
            databricks_pipeline,
            "create_tables",
            lambda args: table_calls.append(args) or 0,
        )

        assert databricks_pipeline.run_databricks_pipeline(_args(tables_only=True)) == 0
        assert copy_calls == []
        assert len(table_calls) == 1

    @pytest.mark.unit
    def test_copy_only_skips_table_update(self, monkeypatch):
        copy_calls = []
        table_calls = []
        monkeypatch.setattr(databricks_pipeline, "logger", _LogWriter())
        monkeypatch.setattr(
            databricks_pipeline,
            "copy_to_uc_volumes",
            lambda args: copy_calls.append(args) or 0,
        )
        monkeypatch.setattr(
            databricks_pipeline,
            "create_tables",
            lambda args: table_calls.append(args) or 0,
        )

        assert databricks_pipeline.run_databricks_pipeline(_args(copy_only=True)) == 0
        assert len(copy_calls) == 1
        assert table_calls == []

    @pytest.mark.unit
    def test_pipeline_rejects_copy_only_with_tables_only(self, monkeypatch, capsys):
        log_writer = _LogWriter()
        monkeypatch.setattr(databricks_pipeline, "logger", log_writer)

        assert (
            databricks_pipeline.run_databricks_pipeline(_args(copy_only=True, tables_only=True))
            == 2
        )

        assert "Choose only one" in capsys.readouterr().out
        assert any(entry.get("action") == "validate_pipeline" for entry in log_writer.entries)

    @pytest.mark.unit
    def test_pipeline_uses_environment_warehouse_id(self, monkeypatch):
        table_calls = []
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "env-warehouse")
        monkeypatch.setattr(databricks_pipeline, "logger", _LogWriter())
        monkeypatch.setattr(
            databricks_pipeline,
            "create_tables",
            lambda args: table_calls.append(args) or 0,
        )

        assert (
            databricks_pipeline.run_databricks_pipeline(_args(tables_only=True, warehouse_id=None))
            == 0
        )

        assert len(table_calls) == 1
        assert table_calls[0].warehouse_id == "env-warehouse"

    @pytest.mark.unit
    def test_pipeline_warns_when_warehouse_id_is_missing(self, monkeypatch, capsys):
        table_calls = []
        monkeypatch.delenv("DATABRICKS_WAREHOUSE_ID", raising=False)
        monkeypatch.setattr(databricks_pipeline, "logger", _LogWriter())
        monkeypatch.setattr(
            databricks_pipeline,
            "create_tables",
            lambda args: table_calls.append(args) or 0,
        )

        assert (
            databricks_pipeline.run_databricks_pipeline(_args(tables_only=True, warehouse_id=None))
            == 0
        )

        assert table_calls[0].warehouse_id is None
        assert "DATABRICKS_WAREHOUSE_ID is not set" in capsys.readouterr().err

    @pytest.mark.unit
    def test_pipeline_returns_error_when_table_update_raises(self, monkeypatch, capsys):
        log_writer = _LogWriter()
        monkeypatch.setattr(databricks_pipeline, "logger", log_writer)

        def raise_table_error(args):
            raise RuntimeError("spark_catalog requires a single-part namespace")

        monkeypatch.setattr(databricks_pipeline, "create_tables", raise_table_error)

        assert databricks_pipeline.run_databricks_pipeline(_args(tables_only=True)) == 1
        assert "ERROR: Databricks table update failed" in capsys.readouterr().err
        assert any(
            entry.get("action") == "complete_step"
            and entry.get("step") == "create-tables"
            and entry.get("success") is False
            for entry in log_writer.entries
        )

    @pytest.mark.unit
    def test_pipeline_returns_error_when_volume_sync_raises(self, monkeypatch, capsys):
        log_writer = _LogWriter()
        table_calls = []
        monkeypatch.setattr(databricks_pipeline, "logger", log_writer)

        def raise_copy_error(args):
            raise RuntimeError("source path does not exist")

        monkeypatch.setattr(databricks_pipeline, "copy_to_uc_volumes", raise_copy_error)
        monkeypatch.setattr(
            databricks_pipeline,
            "create_tables",
            lambda args: table_calls.append(args) or 0,
        )

        assert databricks_pipeline.run_databricks_pipeline(_args()) == 1
        assert table_calls == []
        assert "ERROR: Databricks volume sync failed" in capsys.readouterr().err
        assert any(
            entry.get("action") == "complete_step"
            and entry.get("step") == "copy-volume"
            and entry.get("success") is False
            for entry in log_writer.entries
        )

    @pytest.mark.unit
    def test_cmd_databricks_pipeline_delegates(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            databricks_pipeline,
            "run_databricks_pipeline",
            lambda args: calls.append(args) or 23,
        )
        args = _args()

        assert databricks_pipeline.cmd_databricks_pipeline(args) == 23
        assert calls == [args]
