# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.wound_care_high_frequency module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

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
class TestWoundCareHighFrequency:
    """Tests for Wound Care High Frequency transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import _wound_care_high_frequency
        assert acoharmony._transforms._wound_care_high_frequency is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        from acoharmony._transforms._wound_care_high_frequency import execute
        assert callable(execute)


class TestWoundCareHighFrequencyV2:
    """Tests for _wound_care_high_frequency.execute."""

    @pytest.mark.unit
    def test_returns_dict(self, executor: _MockExecutor, tmp_base: Path) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(
            gold / "skin_substitute_claims.parquet", _skin_substitute_claims_df()
        )

        from acoharmony._transforms._wound_care_high_frequency import execute

        result = execute(executor)
        assert isinstance(result, dict)
        assert "patient_level" in result
        assert "npi_summary" in result

    @pytest.mark.unit
    def test_detects_high_frequency(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(
            gold / "skin_substitute_claims.parquet", _skin_substitute_claims_df()
        )

        from acoharmony._transforms._wound_care_high_frequency import execute

        result = execute(executor)
        patient_level = result["patient_level"].collect()
        # NPI1/M1 has 20 applications >= 15 threshold
        # NPI1/M2 has 15 applications >= 15
        # NPI1/M3 has 12 applications < 15
        npi_member_pairs = set(
            zip(
                patient_level["rendering_npi"].to_list(),
                patient_level["member_id"].to_list(), strict=False,
            )
        )
        assert ("NPI1", "M1") in npi_member_pairs
        assert ("NPI1", "M2") in npi_member_pairs
        assert ("NPI1", "M3") not in npi_member_pairs

    @pytest.mark.unit
    def test_custom_threshold(self, executor: _MockExecutor, tmp_base: Path) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(
            gold / "skin_substitute_claims.parquet", _skin_substitute_claims_df()
        )

        from acoharmony._transforms._wound_care_high_frequency import execute

        result = execute(executor, min_applications=10)
        patient_level = result["patient_level"].collect()
        npi_member_pairs = set(
            zip(
                patient_level["rendering_npi"].to_list(),
                patient_level["member_id"].to_list(), strict=False,
            )
        )
        # Now M3 (12) should also be included
        assert ("NPI1", "M3") in npi_member_pairs

    @pytest.mark.unit
    def test_npi_summary_columns(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(
            gold / "skin_substitute_claims.parquet", _skin_substitute_claims_df()
        )

        from acoharmony._transforms._wound_care_high_frequency import execute

        result = execute(executor)
        npi_summary = result["npi_summary"].collect()
        expected_cols = {
            "rendering_npi",
            "patients_with_high_frequency",
            "total_applications",
            "max_apps_single_patient",
            "avg_apps_per_patient",
        }
        assert expected_cols.issubset(set(npi_summary.columns))

    @pytest.mark.unit
    def test_patient_level_columns(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(
            gold / "skin_substitute_claims.parquet", _skin_substitute_claims_df()
        )

        from acoharmony._transforms._wound_care_high_frequency import execute

        result = execute(executor)
        patient_level = result["patient_level"].collect()
        expected_cols = {
            "rendering_npi",
            "member_id",
            "application_count",
            "first_application",
            "last_application",
            "unique_products",
            "span_days",
        }
        assert expected_cols.issubset(set(patient_level.columns))
