# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.member_medical_claims_with_match module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

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


def _lazy(df: pl.DataFrame) -> pl.LazyFrame:
    return df.lazy()


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
class TestMemberMedicalClaimsWithMatch:
    """Tests for member_medical_claims_with_match executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import member_medical_claims_with_match
        assert member_medical_claims_with_match is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        from acoharmony._transforms.member_medical_claims_with_match import execute
        assert callable(execute)


class TestMemberMedicalClaimsWithMatchV2:
    """Tests for member_medical_claims_with_match.execute."""

    @pytest.mark.unit
    def test_adds_match_flag_both_present(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        df = pl.DataFrame(
            {
                "claim_id": ["C1", "C2", "C3"],
                "hdai_claim_id": ["H1", None, "H3"],
                "cclf_claim_id": ["CL1", "CL2", None],
                "paid_amount": [100.0, 200.0, 300.0],
            }
        )
        _write_parquet(gold / "member_medical_claims_results.parquet", df)

        from acoharmony._transforms.member_medical_claims_with_match import execute

        result = _call_execute(execute, executor).collect()
        assert "claim_id_match" in result.columns
        matches = result["claim_id_match"].cast(pl.Utf8).to_list()
        assert matches[0] == "yes"
        assert matches[1] == "missing_hdai"
        assert matches[2] == "missing_cclf"

    @pytest.mark.unit
    def test_creates_columns_if_missing(
        self, executor: _MockExecutor, tmp_base: Path
    ) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        df = pl.DataFrame(
            {
                "claim_id": ["C1", "C2"],
                "paid_amount": [100.0, 200.0],
            }
        )
        _write_parquet(gold / "member_medical_claims_results.parquet", df)

        from acoharmony._transforms.member_medical_claims_with_match import execute

        result = _call_execute(execute, executor).collect()
        assert "claim_id_match" in result.columns
        # Both columns were null -> hdai_claim_id is null -> "missing_hdai"
        matches = result["claim_id_match"].cast(pl.Utf8).to_list()
        assert all(m == "missing_hdai" for m in matches)

    @pytest.mark.unit
    def test_returns_lazyframe(self, executor: _MockExecutor, tmp_base: Path) -> None:
        gold = tmp_base / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        df = pl.DataFrame(
            {
                "claim_id": ["C1"],
                "hdai_claim_id": ["H1"],
                "cclf_claim_id": ["CL1"],
            }
        )
        _write_parquet(gold / "member_medical_claims_results.parquet", df)

        from acoharmony._transforms.member_medical_claims_with_match import execute

        result = _call_execute(execute, executor)
        assert isinstance(result, pl.LazyFrame)
