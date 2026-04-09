# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.readmissions_summary module."""

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
class TestReadmissionsSummary:
    """Tests for readmissions_summary executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import readmissions_summary
        assert readmissions_summary is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        from acoharmony._transforms.readmissions_summary import execute
        assert callable(execute)


class TestReadmissionsSummaryV2:
    """Tests for readmissions_summary transform."""

    @staticmethod
    def _make_medical_claim(path: Path):
        df = pl.DataFrame(
            {
                "person_id": ["P1", "P1", "P1", "P2"],
                "claim_id": ["C1", "C2", "C3", "C4"],
                "bill_type_code": ["111", "111", "111", "121"],
                "admission_date": [
                    date(2024, 1, 1),
                    date(2024, 1, 20),
                    date(2024, 6, 1),
                    date(2024, 3, 1),
                ],
                "discharge_date": [
                    date(2024, 1, 5),
                    date(2024, 1, 25),
                    date(2024, 6, 5),
                    date(2024, 3, 10),
                ],
            }
        )
        _write(df, path)

    @pytest.mark.unit
    def test_execute_with_claims(self, tmp_path):
        """Cover lines 50-94: normal execution with claims data."""
        from acoharmony._transforms import readmissions_summary

        gold = tmp_path / "gold"
        gold.mkdir()
        self._make_medical_claim(gold / "medical_claim.parquet")

        executor = _MockExecutor(gold)
        # The module-level `execute` is wrapped by @composable + other decorators
        # Get the inner function
        inner = _get_inner_fn(readmissions_summary.execute)
        assert inner is not None, "Could not find inner execute function"
        result = inner(executor)
        df = result.collect()

        assert "patient_id" in df.columns
        assert "days_to_readmission" in df.columns
        # P1 has C1->C2 readmission (15 days)
        assert len(df) >= 1

    @pytest.mark.unit
    def test_execute_missing_file(self, tmp_path):
        """Cover lines 57-71: missing medical_claim.parquet returns empty schema."""
        from acoharmony._transforms import readmissions_summary

        gold = tmp_path / "gold"
        gold.mkdir()

        executor = _MockExecutor(gold)
        inner = _get_inner_fn(readmissions_summary.execute)
        result = inner(executor)
        df = result.collect()

        assert len(df) == 0
        assert "patient_id" in df.columns
        assert "days_to_readmission" in df.columns


class TestReadmissionsSummaryExecute:
    """Tests for readmissions_summary.execute."""

    def _make_executor(self, gold_path: Path):
        executor = MagicMock()
        storage = MagicMock()
        storage.get_path.return_value = gold_path
        executor.storage_config = storage
        executor.logger = MagicMock()
        return executor

    @pytest.mark.unit
    def test_missing_parquet_returns_empty(self, tmp_path):
        from acoharmony._transforms.readmissions_summary import execute

        executor = self._make_executor(tmp_path)
        result = execute(executor)
        df = result.collect()
        assert df.shape[0] == 0
        assert "patient_id" in df.columns
        assert "index_encounter_id" in df.columns
        assert "days_to_readmission" in df.columns

    @pytest.mark.unit
    def test_with_parquet_data(self, tmp_path):
        from acoharmony._transforms.readmissions_summary import execute

        # Create a medical_claim.parquet with inpatient claims
        medical_claim = pl.DataFrame(
            {
                "person_id": ["P1", "P1", "P2"],
                "claim_id": ["C1", "C2", "C3"],
                "admission_date": [_date(2024, 1, 1), _date(2024, 1, 20), _date(2024, 3, 1)],
                "discharge_date": [_date(2024, 1, 5), _date(2024, 1, 25), _date(2024, 3, 5)],
                "bill_type_code": ["111", "111", "111"],
            }
        )
        medical_claim.write_parquet(tmp_path / "medical_claim.parquet")

        executor = self._make_executor(tmp_path)
        result = execute(executor)
        df = result.collect()
        # P1 has a readmission within 30 days
        assert df.shape[0] >= 1
        assert "patient_id" in df.columns
        assert "days_to_readmission" in df.columns

    @pytest.mark.unit
    def test_no_readmissions(self, tmp_path):
        from acoharmony._transforms.readmissions_summary import execute

        # Claims too far apart for readmission
        medical_claim = pl.DataFrame(
            {
                "person_id": ["P1", "P1"],
                "claim_id": ["C1", "C2"],
                "admission_date": [_date(2024, 1, 1), _date(2024, 6, 1)],
                "discharge_date": [_date(2024, 1, 5), _date(2024, 6, 5)],
                "bill_type_code": ["111", "111"],
            }
        )
        medical_claim.write_parquet(tmp_path / "medical_claim.parquet")

        executor = self._make_executor(tmp_path)
        result = execute(executor)
        df = result.collect()
        assert df.shape[0] == 0
