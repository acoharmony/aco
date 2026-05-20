# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.home_visit_claims module."""

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
class TestHomeVisitClaims:
    """Tests for home_visit_claims executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import home_visit_claims
        assert home_visit_claims is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        from acoharmony._transforms.home_visit_claims import execute
        assert callable(execute)


class TestHomeVisitClaimsV2:
    """Tests for home_visit_claims.execute."""

    @staticmethod
    def _physician_claims_df() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "rendering_tin": ["TIN1", "TIN2", "TIN3"],
                "rendering_npi": ["NPI1", "NPI2", "NPI3"],
                "person_id": ["P1", "P2", "P3"],
                "member_id": ["M1", "M2", "M3"],
                "claim_id": ["C1", "C2", "C3"],
                "claim_line_number": [1, 1, 1],
                "hcpcs_code": ["99348", "99213", "G2211"],
                "hcpcs_modifier_1": [None, None, None],
                "hcpcs_modifier_2": [None, None, None],
                "hcpcs_modifier_3": [None, None, None],
                "hcpcs_modifier_4": [None, None, None],
                "hcpcs_modifier_5": [None, None, None],
                "claim_start_date": [date(2024, 1, 1)] * 3,
                "claim_end_date": [date(2024, 1, 1)] * 3,
                "claim_line_start_date": [date(2024, 1, 1)] * 3,
                "claim_line_end_date": [date(2024, 1, 1)] * 3,
                "place_of_service_code": ["12", "11", "12"],
                "service_unit_quantity": [1, 1, 1],
                "paid_amount": [200.0, 150.0, 100.0],
                "allowed_amount": [250.0, 200.0, 120.0],
                "charge_amount": [300.0, 250.0, 150.0],
                "diagnosis_code_type": ["icd-10-cm"] * 3,
                "diagnosis_code_1": ["Z00.00", "Z00.01", "Z00.00"],
                "diagnosis_code_2": [None, None, None],
                "diagnosis_code_3": [None, None, None],
                "diagnosis_code_4": [None, None, None],
                "diagnosis_code_5": [None, None, None],
                "data_source": ["cclf"] * 3,
                "source_filename": ["f1.csv", "f2.csv", "f3.csv"],
                "ingest_datetime": [datetime(2024, 1, 15)] * 3,
            }
        )

    @pytest.mark.unit
    def test_filters_home_visit_codes(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        silver = tmp_base / "silver"
        silver.mkdir(parents=True, exist_ok=True)
        _write_parquet(
            silver / "int_physician_claim_deduped.parquet",
            self._physician_claims_df(),
        )

        from acoharmony._transforms.home_visit_claims import execute

        result = execute(executor).collect()
        codes = result["hcpcs_code"].to_list()
        # 99348 is home visit, G2211 is home visit add-on, 99213 is office
        assert "99348" in codes
        assert "G2211" in codes
        assert "99213" not in codes

    @pytest.mark.unit
    def test_renames_columns(self, executor: _MockExecutor, tmp_base: Path) -> None:
        silver = tmp_base / "silver"
        silver.mkdir(parents=True, exist_ok=True)
        _write_parquet(
            silver / "int_physician_claim_deduped.parquet",
            self._physician_claims_df(),
        )

        from acoharmony._transforms.home_visit_claims import execute

        result = execute(executor).collect()
        assert "tin" in result.columns
        assert "npi" in result.columns

    @pytest.mark.unit
    def test_empty_result_when_no_home_visits(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        silver = tmp_base / "silver"
        silver.mkdir(parents=True, exist_ok=True)
        df = self._physician_claims_df().filter(pl.col("hcpcs_code") == "99213")
        _write_parquet(silver / "int_physician_claim_deduped.parquet", df)

        from acoharmony._transforms.home_visit_claims import execute

        result = execute(executor).collect()
        assert len(result) == 0
