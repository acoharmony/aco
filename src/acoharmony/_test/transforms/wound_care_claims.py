# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.wound_care_claims module."""

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


def _write(df: pl.DataFrame, path: Path) -> None:
    """Write a DataFrame as parquet, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


def _lazy(df: pl.DataFrame) -> pl.LazyFrame:
    return df.lazy()


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestWoundCareClaimsTransform:
    """Tests for Wound Care Claims transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import _wound_care_claims
        assert acoharmony._transforms._wound_care_claims is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        from acoharmony._transforms._wound_care_claims import execute
        assert callable(execute)


class _MockStorageConfig:
    """Mock storage config that returns tmp paths for medallion layers."""

    def __init__(self, base: Path) -> None:
        self._base = base

    def get_path(self, layer: Any) -> Path:
        p = self._base / layer.value
        p.mkdir(parents=True, exist_ok=True)
        return p


class _MockExecutor:
    """Mock executor wired to a tmp directory."""

    def __init__(self, base: Path) -> None:
        self.storage_config = _MockStorageConfig(base)


def _write_parquet(path: Path, df: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _call_execute(execute_fn: Any, executor: Any) -> pl.LazyFrame:
    """Call a transform's execute function, handling decorator chains.

    Recursively searches through decorator wrappers (.func, __wrapped__,
    __closure__) to find and call the original execute function.
    """
    try:
        result = execute_fn(executor)
        if isinstance(result, pl.LazyFrame):
            return result
    except TypeError:
        pass

    # Recursively search for callable that returns LazyFrame
    visited: set[int] = set()

    def _search(obj: Any, depth: int = 0) -> pl.LazyFrame | None:
        """Recursively search through decorator chain for the original function."""
        if obj is None or id(obj) in visited or depth > 10:
            return None
        visited.add(id(obj))

        # Try calling directly if it looks like the original function
        # (has 'execute' in its name or matches expected signature)
        if callable(obj) and hasattr(obj, "__code__"):
            co_name = obj.__code__.co_name
            # Only try calling if it looks like the real execute function
            # (not intermediate wrappers like 'wrapper' or 'decorator')
            if co_name == "execute" or co_name not in ("wrapper", "decorator"):
                try:
                    result = obj(executor)
                    if isinstance(result, pl.LazyFrame):
                        return result
                except TypeError:
                    pass
                except Exception:
                    # If it's the real function and it errors, propagate
                    if co_name == "execute":
                        raise

        # Search .func and __wrapped__ attributes
        for attr in ("func", "__wrapped__"):
            found = _search(getattr(obj, attr, None), depth + 1)
            if found is not None:
                return found

        # Search closure cells
        closure = getattr(obj, "__closure__", None)
        if closure:
            for cell in closure:
                try:
                    val = cell.cell_contents
                except ValueError:
                    continue
                if callable(val):
                    found = _search(val, depth + 1)
                    if found is not None:
                        return found

        return None

    result = _search(execute_fn)
    if result is not None:
        return result

    raise RuntimeError(
        f"Could not obtain a LazyFrame from {execute_fn!r} — "
        "check the decorator chain on the source transform."
    )


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


class TestWoundCareClaims:
    """Tests for _wound_care_claims.execute."""

    @pytest.mark.unit
    def test_filters_wound_care_codes(self, executor: _MockExecutor, tmp_base: Path) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(gold / "medical_claim.parquet", _medical_claim_df())

        from acoharmony._transforms._wound_care_claims import execute

        result = execute(executor).collect()
        codes = result["hcpcs_code"].to_list()
        # 11042, Q4158, 15271, 97597 are wound care; 99213 (office) and 99348 (home) are not
        assert "11042" in codes
        assert "Q4158" in codes
        assert "15271" in codes
        assert "97597" in codes
        assert "99213" not in codes

    @pytest.mark.unit
    def test_returns_lazyframe(self, executor: _MockExecutor, tmp_base: Path) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        _write_parquet(gold / "medical_claim.parquet", _medical_claim_df())

        from acoharmony._transforms._wound_care_claims import execute

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
                "paid_amount": pl.Series([], dtype=pl.Float64),
                "claim_end_date": pl.Series([], dtype=pl.Date),
            }
        )
        _write_parquet(gold / "medical_claim.parquet", empty)

        from acoharmony._transforms._wound_care_claims import execute

        result = execute(executor).collect()
        assert len(result) == 0

    @pytest.mark.unit
    def test_no_matching_codes(self, executor: _MockExecutor, tmp_base: Path) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        df = pl.DataFrame(
            {
                "member_id": ["M1"],
                "hcpcs_code": ["99999"],
                "paid_amount": [100.0],
                "claim_end_date": [date(2024, 1, 1)],
            }
        )
        _write_parquet(gold / "medical_claim.parquet", df)

        from acoharmony._transforms._wound_care_claims import execute

        result = execute(executor).collect()
        assert len(result) == 0
