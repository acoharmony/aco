# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.readmissions_summary_deduped module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
import logging
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
import acoharmony


class _MockMedallionStorage:
    """Mock medallion storage for transform tests."""

    def __init__(self, silver_path=None, gold_path=None):
        if silver_path is None:
            silver_path = Path(".")
        self.silver_path = silver_path
        self.gold_path = gold_path or silver_path

    def get_path(self, layer: str = "silver"):
        if layer == "silver":
            return self.silver_path
        if layer == "gold":
            return self.gold_path
        return self.silver_path


class _MockExecutor:
    """Mock executor for transform tests."""

    def __init__(self, base=None, storage_config=None):
        if storage_config is not None:
            self.storage_config = storage_config
        elif base is not None:
            self.storage_config = _MockMedallionStorage(silver_path=base)
        else:
            self.storage_config = _MockMedallionStorage()
        self.logger = logging.getLogger(__name__)


def _write(df: pl.DataFrame, path: Path) -> None:
    """Write a DataFrame as parquet, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _get_inner_fn(decorated):
    """Walk decorator chain to find original function named 'execute'."""
    visited = set()

    def _search(obj):
        if obj is None or id(obj) in visited:
            return None
        visited.add(id(obj))
        if callable(obj) and hasattr(obj, "__code__") and obj.__code__.co_name == "execute":
            return obj
        for attr in ("func", "__wrapped__"):
            found = _search(getattr(obj, attr, None))
            if found:
                return found
        if hasattr(obj, "__closure__") and obj.__closure__:
            for cell in obj.__closure__:
                try:
                    found = _search(cell.cell_contents)
                    if found:
                        return found
                except ValueError:
                    pass
        return None

    return _search(decorated)


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


@pytest.fixture
def executor(tmp_base: Path) -> _MockExecutor:
    return _MockExecutor(tmp_base)


def _make_executor(silver_dir: Path) -> _MockExecutor:
    return _MockExecutor(storage_config=_MockMedallionStorage(silver_path=silver_dir))


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestReadmissionsSummaryDeduped:
    """Tests for readmissions_summary_deduped executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import readmissions_summary_deduped
        assert readmissions_summary_deduped is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        from acoharmony._transforms.readmissions_summary_deduped import execute
        assert callable(execute)


class TestReadmissionsSummaryDedupedV2:
    """Tests for readmissions_summary_deduped transform."""

    @staticmethod
    def _make_readmissions_summary(path: Path):
        df = pl.DataFrame(
            {
                "patient_id": ["P1", "P1", "P1", "P2"],
                "index_encounter_id": ["C1", "C1", "C1", "C4"],
                "index_admission_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 3, 1),
                ],
                "index_discharge_date": [
                    date(2024, 1, 5),
                    date(2024, 1, 5),
                    date(2024, 1, 5),
                    date(2024, 3, 10),
                ],
                "readmission_encounter_id": ["C2", "C2", "C3", "C5"],
                "readmission_admission_date": [
                    date(2024, 1, 20),
                    date(2024, 1, 20),
                    date(2024, 1, 28),
                    date(2024, 3, 25),
                ],
                "days_to_readmission": [15, 15, 23, 15],
            }
        )
        _write(df, path)

    @pytest.mark.unit
    def test_execute_with_data(self, tmp_path):
        """Cover lines 38-78: normal deduplication path."""
        from acoharmony._transforms import readmissions_summary_deduped

        gold = tmp_path / "gold"
        gold.mkdir()
        self._make_readmissions_summary(gold / "readmissions_summary.parquet")

        executor = _MockExecutor(gold)
        inner = _get_inner_fn(readmissions_summary_deduped.execute)
        assert inner is not None
        result = inner(executor)
        df = result.collect()

        # 4 rows with one exact duplicate -> 3 unique
        assert len(df) == 3

    @pytest.mark.unit
    def test_execute_missing_file(self, tmp_path):
        """Cover lines 45-59: missing file returns empty schema."""
        from acoharmony._transforms import readmissions_summary_deduped

        gold = tmp_path / "gold"
        gold.mkdir()

        executor = _MockExecutor(gold)
        inner = _get_inner_fn(readmissions_summary_deduped.execute)
        result = inner(executor)
        df = result.collect()

        assert len(df) == 0
        assert "patient_id" in df.columns


class TestReadmissionsSummaryDedupedExecute:
    """Tests for readmissions_summary_deduped.execute."""

    def _make_executor(self, gold_path: Path):
        executor = MagicMock()
        storage = MagicMock()
        storage.get_path.return_value = gold_path
        executor.storage_config = storage
        executor.logger = MagicMock()
        return executor

    @pytest.mark.unit
    def test_missing_parquet_returns_empty(self, tmp_path):
        from acoharmony._transforms.readmissions_summary_deduped import execute

        executor = self._make_executor(tmp_path)
        result = execute(executor)
        df = result.collect()
        assert df.shape[0] == 0
        assert "patient_id" in df.columns
        assert "days_to_readmission" in df.columns

    @pytest.mark.unit
    def test_deduplicates_rows(self, tmp_path):
        from acoharmony._transforms.readmissions_summary_deduped import execute

        # Create readmissions_summary.parquet with duplicates
        readmissions = pl.DataFrame(
            {
                "patient_id": ["P1", "P1", "P1", "P2"],
                "index_encounter_id": ["C1", "C1", "C1", "C3"],
                "index_admission_date": [
                    _date(2024, 1, 1),
                    _date(2024, 1, 1),
                    _date(2024, 1, 1),
                    _date(2024, 3, 1),
                ],
                "index_discharge_date": [
                    _date(2024, 1, 5),
                    _date(2024, 1, 5),
                    _date(2024, 1, 5),
                    _date(2024, 3, 5),
                ],
                "readmission_encounter_id": ["C2", "C2", "C2", "C4"],
                "readmission_admission_date": [
                    _date(2024, 1, 20),
                    _date(2024, 1, 20),
                    _date(2024, 1, 20),
                    _date(2024, 3, 20),
                ],
                "days_to_readmission": [15, 15, 15, 15],
            }
        )
        readmissions.write_parquet(tmp_path / "readmissions_summary.parquet")

        executor = self._make_executor(tmp_path)
        result = execute(executor)
        df = result.collect()
        # Should have 2 unique rows (one for P1, one for P2)
        assert df.shape[0] == 2

    @pytest.mark.unit
    def test_no_duplicates(self, tmp_path):
        from acoharmony._transforms.readmissions_summary_deduped import execute

        readmissions = pl.DataFrame(
            {
                "patient_id": ["P1", "P2"],
                "index_encounter_id": ["C1", "C3"],
                "index_admission_date": [_date(2024, 1, 1), _date(2024, 3, 1)],
                "index_discharge_date": [_date(2024, 1, 5), _date(2024, 3, 5)],
                "readmission_encounter_id": ["C2", "C4"],
                "readmission_admission_date": [_date(2024, 1, 20), _date(2024, 3, 20)],
                "days_to_readmission": [15, 15],
            }
        )
        readmissions.write_parquet(tmp_path / "readmissions_summary.parquet")

        executor = self._make_executor(tmp_path)
        result = execute(executor)
        df = result.collect()
        assert df.shape[0] == 2
