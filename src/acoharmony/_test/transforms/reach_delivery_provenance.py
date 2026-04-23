# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.reach_delivery_provenance module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import json
import logging
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class _MockStorageConfig:
    """Minimal storage stub that routes every tier under a single base dir."""

    def __init__(self, base: Path):
        self._base = base

    def get_path(self, tier):
        tier_name = getattr(tier, "data_tier", None) or str(tier).lower()
        # Accept MedallionLayer enum or plain "silver" / "logs" strings.
        sub = {
            "bronze": "bronze",
            "silver": "silver",
            "gold": "gold",
            "logs": "logs",
            "raw": "bronze",
            "processed": "silver",
            "curated": "gold",
        }.get(tier_name, tier_name)
        path = self._base / sub
        path.mkdir(parents=True, exist_ok=True)
        return path


class _MockExecutor:
    def __init__(self, base: Path):
        self.storage_config = _MockStorageConfig(base)
        self.logger = logging.getLogger("test_reach_delivery_provenance")


@pytest.fixture
def base_dir(tmp_path: Path) -> Path:
    return tmp_path / "workspace"


@pytest.fixture
def executor(base_dir: Path) -> _MockExecutor:
    return _MockExecutor(base_dir)


def _write_calendar(base_dir: Path) -> None:
    """Write a minimal reach_calendar.parquet with two schedulable reports."""
    df = pl.DataFrame(
        {
            "type": ["Report", "Report", "Event"],
            "description": [
                "Monthly Expenditure Report - April",
                "Quarterly Benchmark Report - Q2",
                "Some event",
            ],
            "category": ["Finance", "Finance", "Alignment"],
            "start_date": [date(2024, 4, 25), date(2024, 7, 15), date(2024, 4, 1)],
            "file_date": ["2026-04-01", "2026-04-01", "2026-04-01"],
            "py": [2024, 2024, None],
        }
    )
    silver = base_dir / "silver"
    silver.mkdir(parents=True, exist_ok=True)
    df.write_parquet(silver / "reach_calendar.parquet")


def _write_state(base_dir: Path) -> None:
    """Write a minimal 4icli_state.json matching one of the calendar rows."""
    state = {
        "mexpr_april.xlsx": {
            "filename": "REACH.D0259.MEXPR.04.PY2024.D240425.T1.xlsx",
            "file_type_code": 214,
            "category": "Reports",
            "remote_metadata": {"created": "2024-04-25T12:00:00.000Z"},
            "download_timestamp": "2024-04-26T00:00:00",
        }
    }
    logs = base_dir / "logs" / "tracking"
    logs.mkdir(parents=True, exist_ok=True)
    (logs / "4icli_state.json").write_text(json.dumps(state))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestResolveStateFile:
    @pytest.mark.unit
    def test_resolves_under_logs_tracking(self, executor, base_dir):
        resolved = _resolve_state_file(executor.storage_config)
        assert resolved == base_dir / "logs" / "tracking" / "4icli_state.json"

    @pytest.mark.unit
    def test_resolved_path_parent_is_logs(self, executor):
        resolved = _resolve_state_file(executor.storage_config)
        assert resolved.parent.name == "tracking"
        assert resolved.parent.parent.name == "logs"


class TestExecute:
    @pytest.mark.unit
    def test_missing_state_file_produces_empty_frame(self, executor, base_dir):
        """The transform is delivery-centric: with no state file there are
        no deliveries, so the output is empty. Calendar-only rows never
        appear — we don't have anything to attribute them to."""
        _write_calendar(base_dir)
        # Deliberately do NOT write state.

        df = execute(executor).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_happy_path_matches_calendar_to_delivery(self, executor, base_dir):
        """State file with a matching delivery produces an on-time row."""
        _write_calendar(base_dir)
        _write_state(base_dir)

        df = execute(executor).collect()
        mexpr = df.filter(
            (pl.col("schema_name") == "mexpr") & (pl.col("period") == "M04")
        )
        assert mexpr.height == 1
        row = mexpr.row(0, named=True)
        assert row["actual_delivery_date"] == date(2024, 4, 25)
        assert row["actual_delivery_source"] == "remote_created"
        assert row["delivery_status"] == "on_time"
        assert row["delivery_diff_days"] == 0

    @pytest.mark.unit
    def test_execute_returns_lazyframe(self, executor, base_dir):
        _write_calendar(base_dir)
        result = execute(executor)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_scheduled_but_undelivered_calendar_rows_are_excluded(
        self, executor, base_dir
    ):
        """The calendar schedules a Q2 reach_bnmr, but the state file only
        has the mexpr delivery. Since we didn't receive reach_bnmr, it must
        NOT appear in the output — delivery-centric."""
        _write_calendar(base_dir)
        _write_state(base_dir)

        df = execute(executor).collect()
        assert "reach_bnmr" not in df["schema_name"].to_list()
        # And no row in the entire output should be flagged "missing".
        assert "missing" not in df["delivery_status"].to_list()

    @pytest.mark.unit
    def test_executor_from_simple_namespace_works(self, base_dir):
        """Consumers of execute() pass a SimpleNamespace — the transform
        should be agnostic as long as storage_config has get_path()."""
        _write_calendar(base_dir)
        _write_state(base_dir)
        ns_executor = SimpleNamespace(storage_config=_MockStorageConfig(base_dir))
        df = execute(ns_executor).collect()
        assert df.height >= 1
