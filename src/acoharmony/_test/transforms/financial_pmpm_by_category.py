# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.financial_pmpm_by_category module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from datetime import date, datetime  # noqa: F811
from pathlib import Path

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

    def get_path(self, layer="silver"):
        layer_str = layer.value if hasattr(layer, "value") else str(layer)
        base = self.gold_path if layer_str == "gold" else self.silver_path
        return base / layer_str


class _MockExecutor:
    """Mock executor for transform tests."""

    def __init__(self, base=None, storage_config=None):
        if storage_config is not None:
            self.storage_config = storage_config
        elif base is not None:
            self.storage_config = _MockMedallionStorage(silver_path=base)
        else:
            self.storage_config = _MockMedallionStorage()


def _write(df: pl.DataFrame, path: Path) -> None:
    """Write a DataFrame as parquet, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


def _lazy(df: pl.DataFrame) -> pl.LazyFrame:
    return df.lazy()


def _write_parquet(path: Path, df: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


@pytest.fixture
def tmp_base(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture
def executor(tmp_base: Path) -> _MockExecutor:
    return _MockExecutor(tmp_base)


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestFinancialPmpmByCategory:
    """Tests for financial_pmpm_by_category executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import financial_pmpm_by_category
        assert financial_pmpm_by_category is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        from acoharmony._transforms.financial_pmpm_by_category import execute
        assert callable(execute)


class TestFinancialPmpmByCategoryV2:
    """Tests for financial_pmpm_by_category.execute."""

    @staticmethod
    def _service_category_df() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "person_id": ["P1", "P1", "P2", "P2"],
                "claim_start_date": [
                    date(2024, 1, 15),
                    date(2024, 1, 20),
                    date(2024, 1, 10),
                    date(2024, 2, 5),
                ],
                "service_category_2": ["inpatient", "outpatient", "inpatient", "pharmacy"],
                "paid": [5000.0, 1500.0, 3000.0, 250.0],
            }
        )

    @staticmethod
    def _consolidated_alignment_df() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "current_mbi": ["P1", "P2"],
                "ym_202401_reach": [True, True],
                "ym_202402_reach": [True, True],
                "ym_202401_mssp": [False, False],
                "ym_202402_mssp": [False, False],
            }
        )

    @pytest.mark.unit
    def test_basic_pmpm_calculation(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(gold / "service_category.parquet", self._service_category_df())
        _write_parquet(
            gold / "consolidated_alignment.parquet",
            self._consolidated_alignment_df(),
        )

        from acoharmony._transforms.financial_pmpm_by_category import execute

        result = execute(executor).collect()
        assert len(result) > 0
        expected_cols = {
            "month",
            "year_month",
            "program",
            "category",
            "total_spend",
            "member_months",
            "pmpm",
        }
        assert expected_cols.issubset(set(result.columns))

    @pytest.mark.unit
    def test_pmpm_values(self, executor: _MockExecutor, tmp_base: Path) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(gold / "service_category.parquet", self._service_category_df())
        _write_parquet(
            gold / "consolidated_alignment.parquet",
            self._consolidated_alignment_df(),
        )

        from acoharmony._transforms.financial_pmpm_by_category import execute

        result = execute(executor).collect()
        # All PMPM values should be >= 0
        assert (result["pmpm"] >= 0).all()
        # Total spend should be > 0 where we have data
        assert (result["total_spend"] > 0).all()

    @pytest.mark.unit
    def test_month_format(self, executor: _MockExecutor, tmp_base: Path) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(gold / "service_category.parquet", self._service_category_df())
        _write_parquet(
            gold / "consolidated_alignment.parquet",
            self._consolidated_alignment_df(),
        )

        from acoharmony._transforms.financial_pmpm_by_category import execute

        result = execute(executor).collect()
        months = result["month"].to_list()
        # Should be YYYY-MM format
        for m in months:
            assert len(m) == 7
            assert m[4] == "-"

    @pytest.mark.unit
    def test_returns_lazyframe(self, executor: _MockExecutor, tmp_base: Path) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(gold / "service_category.parquet", self._service_category_df())
        _write_parquet(
            gold / "consolidated_alignment.parquet",
            self._consolidated_alignment_df(),
        )

        from acoharmony._transforms.financial_pmpm_by_category import execute

        result = execute(executor)
        assert isinstance(result, pl.LazyFrame)
