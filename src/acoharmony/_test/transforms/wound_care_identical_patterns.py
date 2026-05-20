# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.wound_care_identical_patterns module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from datetime import date
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
class TestWoundCareIdenticalPatterns:
    """Tests for Wound Care Identical Patterns transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import _wound_care_identical_patterns
        assert acoharmony._transforms._wound_care_identical_patterns is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        from acoharmony._transforms._wound_care_identical_patterns import execute
        assert callable(execute)


class TestWoundCareIdenticalPatternsV2:
    """Tests for _wound_care_identical_patterns.execute."""

    @pytest.mark.unit
    def test_returns_dict(self, executor: _MockExecutor, tmp_base: Path) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(
            gold / "skin_substitute_claims.parquet", _skin_substitute_claims_df()
        )

        from acoharmony._transforms._wound_care_identical_patterns import execute

        result = execute(executor)
        assert isinstance(result, dict)
        assert "pattern_details" in result
        assert "npi_summary" in result

    @pytest.mark.unit
    def test_detects_identical_patterns(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(
            gold / "skin_substitute_claims.parquet", _skin_substitute_claims_df()
        )

        from acoharmony._transforms._wound_care_identical_patterns import execute

        # NPI1 + Q4158 + 60000.0 has 47 claims across 3 patients
        result = execute(executor)
        pattern_details = result["pattern_details"].collect()
        assert len(pattern_details) > 0
        # Should include the NPI1/Q4158/60000 pattern
        npi1_patterns = pattern_details.filter(pl.col("rendering_npi") == "NPI1")
        assert len(npi1_patterns) > 0

    @pytest.mark.unit
    def test_custom_thresholds(self, executor: _MockExecutor, tmp_base: Path) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(
            gold / "skin_substitute_claims.parquet", _skin_substitute_claims_df()
        )

        from acoharmony._transforms._wound_care_identical_patterns import execute

        # With very high thresholds, nothing should match
        result = execute(executor, min_identical_claims=100, min_patients=50)
        pattern_details = result["pattern_details"].collect()
        assert len(pattern_details) == 0

    @pytest.mark.unit
    def test_pattern_details_columns(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(
            gold / "skin_substitute_claims.parquet", _skin_substitute_claims_df()
        )

        from acoharmony._transforms._wound_care_identical_patterns import execute

        result = execute(executor)
        pattern_details = result["pattern_details"].collect()
        expected_cols = {
            "rendering_npi",
            "hcpcs_code",
            "paid_amount",
            "claim_count",
            "unique_patients",
            "unique_dates",
            "first_claim_date",
            "last_claim_date",
            "pattern_span_days",
        }
        assert expected_cols.issubset(set(pattern_details.columns))

    @pytest.mark.unit
    def test_npi_summary_columns(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(
            gold / "skin_substitute_claims.parquet", _skin_substitute_claims_df()
        )

        from acoharmony._transforms._wound_care_identical_patterns import execute

        result = execute(executor)
        npi_summary = result["npi_summary"].collect()
        expected_cols = {
            "rendering_npi",
            "unique_billing_patterns",
            "total_identical_claims",
            "total_patients_affected",
            "max_identical_claims",
            "unique_payment_amounts",
        }
        assert expected_cols.issubset(set(npi_summary.columns))
