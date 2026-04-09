# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.skin_substitute_claims module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date
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


def _medical_claim_df() -> pl.DataFrame:
    """Medical claims with a mix of wound care, skin substitute and other codes."""
    return pl.DataFrame(
        {
            "member_id": ["M1", "M2", "M3", "M4", "M5", "M6"],
            "rendering_npi": ["NPI1", "NPI1", "NPI2", "NPI2", "NPI3", "NPI3"],
            "hcpcs_code": ["11042", "Q4158", "99213", "15271", "97597", "99348"],
            "paid_amount": [500.0, 1200.0, 150.0, 800.0, 350.0, 200.0],
            "claim_end_date": [
                date(2024, 1, 10),
                date(2024, 1, 15),
                date(2024, 2, 1),
                date(2024, 2, 5),
                date(2024, 3, 1),
                date(2024, 3, 10),
            ],
            "claim_start_date": [
                date(2024, 1, 10),
                date(2024, 1, 15),
                date(2024, 2, 1),
                date(2024, 2, 5),
                date(2024, 3, 1),
                date(2024, 3, 10),
            ],
            "claim_id": ["C1", "C2", "C3", "C4", "C5", "C6"],
        }
    )
class TestSkinSubstituteClaimsTransform:
    """Tests for Skin Substitute Claims transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import _skin_substitute_claims
        assert acoharmony._transforms._skin_substitute_claims is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        from acoharmony._transforms._skin_substitute_claims import execute
        assert callable(execute)


class TestSkinSubstituteClaims:
    """Tests for _skin_substitute_claims.execute."""

    @pytest.mark.unit
    def test_filters_skin_substitute_codes(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(gold / "medical_claim.parquet", _medical_claim_df())

        from acoharmony._transforms._skin_substitute_claims import execute

        result = execute(executor).collect()
        codes = result["hcpcs_code"].to_list()
        # Q4158 and 15271 are skin substitutes
        assert "Q4158" in codes
        assert "15271" in codes
        # 11042, 97597 are wound care but NOT skin substitutes
        assert "11042" not in codes
        assert "97597" not in codes

    @pytest.mark.unit
    def test_returns_lazyframe(self, executor: _MockExecutor, tmp_base: Path) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(gold / "medical_claim.parquet", _medical_claim_df())

        from acoharmony._transforms._skin_substitute_claims import execute

        result = execute(executor)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_empty_input(self, executor: _MockExecutor, tmp_base: Path) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        empty = pl.DataFrame(
            {
                "member_id": pl.Series([], dtype=pl.Utf8),
                "hcpcs_code": pl.Series([], dtype=pl.Utf8),
            }
        )
        _write_parquet(gold / "medical_claim.parquet", empty)

        from acoharmony._transforms._skin_substitute_claims import execute

        result = execute(executor).collect()
        assert len(result) == 0
