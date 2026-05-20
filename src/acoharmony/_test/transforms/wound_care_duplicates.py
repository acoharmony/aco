# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.wound_care_duplicates module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from datetime import date, datetime  # noqa: F811
from pathlib import Path
from typing import Any

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


def _write_parquet(path: Path, df: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


@pytest.fixture
def tmp_base(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture
def executor(tmp_base: Path) -> _MockExecutor:
    return _MockExecutor(tmp_base)


def _skin_substitute_claims_df() -> pl.DataFrame:
    """Skin substitute claims used by high cost, duplicates, frequency, clustered, identical."""
    rows: list[dict[str, Any]] = []
    # NPI1 treats M1 heavily (20 claims, clustered)
    for i in range(20):
        rows.append(
            {
                "member_id": "M1",
                "rendering_npi": "NPI1",
                "hcpcs_code": "Q4158",
                "paid_amount": 60000.0,
                "claim_end_date": date(2024, 1, 1 + (i % 28)),
            }
        )
    # NPI1 treats M2 (15 claims, same amount/code = identical pattern)
    for i in range(15):
        rows.append(
            {
                "member_id": "M2",
                "rendering_npi": "NPI1",
                "hcpcs_code": "Q4158",
                "paid_amount": 60000.0,
                "claim_end_date": date(2024, 2, 1 + (i % 28)),
            }
        )
    # NPI1 treats M3 (12 claims, same pattern)
    for i in range(12):
        rows.append(
            {
                "member_id": "M3",
                "rendering_npi": "NPI1",
                "hcpcs_code": "Q4158",
                "paid_amount": 60000.0,
                "claim_end_date": date(2024, 3, 1 + (i % 28)),
            }
        )
    # NPI2 treats M4 (2 claims, not high frequency)
    rows.append(
        {
            "member_id": "M4",
            "rendering_npi": "NPI2",
            "hcpcs_code": "Q4161",
            "paid_amount": 200.0,
            "claim_end_date": date(2024, 4, 1),
        }
    )
    rows.append(
        {
            "member_id": "M4",
            "rendering_npi": "NPI2",
            "hcpcs_code": "Q4161",
            "paid_amount": 200.0,
            "claim_end_date": date(2024, 4, 15),
        }
    )
    return pl.DataFrame(rows)


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestWoundCareDuplicates:
    """Tests for Wound Care Duplicates transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import _wound_care_duplicates
        assert acoharmony._transforms._wound_care_duplicates is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        from acoharmony._transforms._wound_care_duplicates import execute
        assert callable(execute)


class TestWoundCareDuplicatesV2:
    """Tests for _wound_care_duplicates.execute."""

    @pytest.mark.unit
    def test_returns_dict_with_expected_keys(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(
            gold / "skin_substitute_claims.parquet", _skin_substitute_claims_df()
        )

        from acoharmony._transforms._wound_care_duplicates import execute

        result = execute(executor)
        assert isinstance(result, dict)
        assert "duplicate_details" in result
        assert "npi_summary" in result

    @pytest.mark.unit
    def test_detects_same_day_duplicates(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        # Create explicit same-day duplicates
        df = pl.DataFrame(
            {
                "rendering_npi": ["NPI1", "NPI1", "NPI1", "NPI2"],
                "member_id": ["M1", "M1", "M1", "M2"],
                "claim_end_date": [
                    date(2024, 1, 5),
                    date(2024, 1, 5),
                    date(2024, 1, 5),
                    date(2024, 2, 1),
                ],
                "hcpcs_code": ["Q4158", "Q4158", "Q4158", "Q4161"],
                "paid_amount": [1000.0, 1000.0, 1000.0, 200.0],
            }
        )
        _write_parquet(gold / "skin_substitute_claims.parquet", df)

        from acoharmony._transforms._wound_care_duplicates import execute

        result = execute(executor)
        details = result["duplicate_details"].collect()
        # NPI1/M1/2024-01-05/Q4158 appears 3 times -> duplicate (claim_count > 1)
        assert len(details) == 1
        assert details["claim_count"][0] == 3

        npi_summary = result["npi_summary"].collect()
        assert len(npi_summary) == 1
        assert npi_summary["rendering_npi"][0] == "NPI1"

    @pytest.mark.unit
    def test_no_duplicates(self, executor: _MockExecutor, tmp_base: Path) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        df = pl.DataFrame(
            {
                "rendering_npi": ["NPI1", "NPI2"],
                "member_id": ["M1", "M2"],
                "claim_end_date": [date(2024, 1, 1), date(2024, 2, 1)],
                "hcpcs_code": ["Q4158", "Q4161"],
                "paid_amount": [1000.0, 500.0],
            }
        )
        _write_parquet(gold / "skin_substitute_claims.parquet", df)

        from acoharmony._transforms._wound_care_duplicates import execute

        result = execute(executor)
        details = result["duplicate_details"].collect()
        assert len(details) == 0

    @pytest.mark.unit
    def test_duplicate_details_columns(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        df = pl.DataFrame(
            {
                "rendering_npi": ["NPI1", "NPI1"],
                "member_id": ["M1", "M1"],
                "claim_end_date": [date(2024, 1, 5), date(2024, 1, 5)],
                "hcpcs_code": ["Q4158", "Q4158"],
                "paid_amount": [1000.0, 2000.0],
            }
        )
        _write_parquet(gold / "skin_substitute_claims.parquet", df)

        from acoharmony._transforms._wound_care_duplicates import execute

        result = execute(executor)
        details = result["duplicate_details"].collect()
        expected_cols = {
            "rendering_npi",
            "member_id",
            "claim_end_date",
            "hcpcs_code",
            "claim_count",
            "total_paid",
            "avg_paid_per_claim",
        }
        assert expected_cols.issubset(set(details.columns))
