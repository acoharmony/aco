# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.reach_hedr module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
import logging
from datetime import date, datetime  # noqa: F811
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
import acoharmony


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


@pytest.fixture
def executor(tmp_base: Path) -> _MockExecutor:
    return _MockExecutor(tmp_base)


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestReachHedr:
    """Tests for reach_hedr executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import reach_hedr
        assert reach_hedr is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        from acoharmony._transforms.reach_hedr import execute
        assert callable(execute)

from acoharmony.medallion import MedallionLayer  # noqa: E402


class _MockStorageConfig:
    def __init__(self, base: Path):
        self._base = base

    def get_path(self, layer):
        return self._base


class _MockExecutor:
    def __init__(self, base: Path):
        self.storage_config = _MockStorageConfig(base)
        self.logger = logging.getLogger("test_transforms_coverage_gaps")


def _write(df: pl.DataFrame, path: Path):
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


class TestReachHedrV2:
    """Tests for the REACH HEDR eligibility transform."""

    @staticmethod
    def _make_bar(tmp_path: Path):
        """Create minimal BAR parquet for silver."""
        df = pl.DataFrame(
            {
                "bene_mbi": ["MBI001", "MBI002", "MBI003"],
                "file_date": ["2025-01", "2025-01", "2025-01"],
                "source_filename": [
                    "P.D0259.ALGR25.RP.D250301.T1200.xlsx",
                    "P.D0259.ALGR25.RP.D250301.T1200.xlsx",
                    "P.D0259.ALGR25.RP.D250301.T1200.xlsx",
                ],
                "start_date": [
                    date(2025, 1, 1),
                    date(2025, 3, 1),
                    date(2025, 1, 1),
                ],
                "end_date": [None, date(2025, 9, 30), None],
                "bene_date_of_death": [None, None, date(2025, 6, 15)],
                "bene_date_of_birth": [
                    date(1950, 5, 1),
                    date(1945, 8, 20),
                    date(1960, 1, 1),
                ],
                "bene_first_name": ["Alice", "Bob", "Charlie"],
                "bene_last_name": ["Smith", "Jones", "Brown"],
                "bene_address_line_1": ["123 Main", "456 Oak", ""],
                "bene_city": ["Troy", "Dayton", "Lima"],
                "bene_state": ["OH", "OH", "OH"],
                "bene_zip_5": ["45373", "45402", "45801"],
                "bene_race_ethnicity": ["White", "Black", None],
            }
        )
        _write(df, tmp_path / "bar.parquet")

    @pytest.mark.unit
    def test_execute_no_alignment_no_sdoh(self, tmp_path):
        """Cover lines 52-388 — no alignment, no SDOH data."""
        from acoharmony._transforms import reach_hedr

        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        silver.mkdir()
        gold.mkdir()

        self._make_bar(silver)

        # Build executor that returns silver/gold paths for respective layers
        class _Storage:
            def get_path(self, layer):
                if layer == MedallionLayer.SILVER:
                    return silver
                return gold

        executor = MagicMock()
        executor.storage_config = _Storage()
        executor.logger = logging.getLogger("test_reach_hedr")

        result = reach_hedr.execute(executor)
        df = result.collect()

        assert "hedr_denominator" in df.columns
        assert "hedr_numerator" in df.columns
        assert "hedr_status" in df.columns
        assert "missing_data_fields" in df.columns
        assert "performance_year" in df.columns
        assert len(df) > 0

    @pytest.mark.unit
    def test_execute_with_alignment(self, tmp_path):
        """Cover alignment branch — with temporal alignment data."""
        from acoharmony._transforms import reach_hedr

        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        silver.mkdir()
        gold.mkdir()

        self._make_bar(silver)

        # Create alignment data with month columns
        alignment = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002"],
                "months_in_reach": [10, 5],
                "first_reach_date": [date(2025, 1, 1), date(2025, 3, 1)],
                "last_reach_date": [None, date(2025, 9, 30)],
                "observable_start": [date(2025, 1, 1), date(2025, 1, 1)],
                "observable_end": [date(2025, 12, 31), date(2025, 12, 31)],
                "ym_202501_reach": [True, False],
                "ym_202502_reach": [True, False],
                "ym_202503_reach": [True, True],
                "ym_202504_reach": [True, True],
                "ym_202505_reach": [True, True],
                "ym_202506_reach": [True, True],
                "ym_202507_reach": [True, True],
                "ym_202508_reach": [True, False],
                "ym_202509_reach": [True, False],
                "ym_202510_reach": [True, False],
            }
        )
        _write(alignment, gold / "aco_alignment.parquet")

        class _Storage:
            def get_path(self, layer):
                if layer == MedallionLayer.SILVER:
                    return silver
                return gold

        executor = MagicMock()
        executor.storage_config = _Storage()
        executor.logger = logging.getLogger("test_reach_hedr_align")

        result = reach_hedr.execute(executor)
        df = result.collect()
        assert "reach_months_2025" in df.columns

    @pytest.mark.unit
    def test_execute_with_alignment_no_month_cols(self, tmp_path):
        """Cover alignment branch without ym_ columns — fallback to months_in_reach."""
        from acoharmony._transforms import reach_hedr

        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        silver.mkdir()
        gold.mkdir()

        self._make_bar(silver)

        # Alignment data WITHOUT ym_ columns
        alignment = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002"],
                "months_in_reach": [10, 5],
                "first_reach_date": [date(2025, 1, 1), date(2025, 3, 1)],
                "last_reach_date": [None, date(2025, 9, 30)],
                "observable_start": [date(2025, 1, 1), date(2025, 1, 1)],
                "observable_end": [date(2025, 12, 31), date(2025, 12, 31)],
            }
        )
        _write(alignment, gold / "aco_alignment.parquet")

        class _Storage:
            def get_path(self, layer):
                if layer == MedallionLayer.SILVER:
                    return silver
                return gold

        executor = MagicMock()
        executor.storage_config = _Storage()
        executor.logger = logging.getLogger("test_reach_hedr_no_ym")

        result = reach_hedr.execute(executor)
        df = result.collect()
        assert "reach_months_2025" in df.columns

    @pytest.mark.unit
    def test_execute_with_sdoh(self, tmp_path):
        """Cover SDOH branch."""
        from acoharmony._transforms import reach_hedr

        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        silver.mkdir()
        gold.mkdir()

        self._make_bar(silver)

        sdoh = pl.DataFrame(
            {
                "mbi": ["MBI001", "MBI002"],
                "date_assessment_complete": [date(2025, 3, 1), None],
                "assessment_declined": ["No", "Yes"],
            }
        )
        _write(sdoh, silver / "reach_sdoh.parquet")

        class _Storage:
            def get_path(self, layer):
                if layer == MedallionLayer.SILVER:
                    return silver
                return gold

        executor = MagicMock()
        executor.storage_config = _Storage()
        executor.logger = logging.getLogger("test_reach_hedr_sdoh")

        result = reach_hedr.execute(executor)
        df = result.collect()
        assert "hedr_numerator" in df.columns

    @pytest.mark.unit
    def test_execute_non_runout_bar(self, tmp_path):
        """Cover the else branch: no runout files, use regular BAR."""
        from acoharmony._transforms import reach_hedr

        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        silver.mkdir()
        gold.mkdir()

        # BAR without .RP. in filename
        df = pl.DataFrame(
            {
                "bene_mbi": ["MBI001"],
                "file_date": ["2025-01"],
                "source_filename": ["P.D0259.ALGR25.D250301.T1200.xlsx"],
                "start_date": [date(2025, 1, 1)],
                "end_date": [None],
                "bene_date_of_death": [None],
                "bene_date_of_birth": [date(1950, 5, 1)],
                "bene_first_name": ["Alice"],
                "bene_last_name": ["Smith"],
                "bene_address_line_1": ["123 Main"],
                "bene_city": ["Troy"],
                "bene_state": ["OH"],
                "bene_zip_5": ["45373"],
                "bene_race_ethnicity": ["White"],
            }
        )
        _write(df, silver / "bar.parquet")

        class _Storage:
            def get_path(self, layer):
                if layer == MedallionLayer.SILVER:
                    return silver
                return gold

        executor = MagicMock()
        executor.storage_config = _Storage()
        executor.logger = logging.getLogger("test_reach_hedr_no_runout")

        result = reach_hedr.execute(executor)
        collected = result.collect()
        assert len(collected) == 1


class TestReachHedrExecute:
    """Tests for reach_hedr.execute function."""

    @pytest.mark.unit
    def test_execute_with_bar_no_runout(self, tmp_path):
        """Test execute with BAR data but no runout files and no alignment."""
        from acoharmony._expressions._reach_hedr_eligible import (
            build_reach_hedr_denominator_expr,
        )

        # Test the expression builders directly since execute() requires the full executor
        df_schema = [
            "death_date",
            "months_in_reach_2025",
            "first_reach_date",
            "last_reach_date",
        ]
        expr = build_reach_hedr_denominator_expr(
            performance_year=2025, df_schema=df_schema
        )
        assert expr is not None

    @pytest.mark.unit
    def test_denominator_expr_with_months(self):
        from acoharmony._expressions._reach_hedr_eligible import (
            build_reach_hedr_denominator_expr,
        )

        df = pl.DataFrame(
            {
                "death_date": [None, None, date(2025, 5, 1)],
                "months_in_reach_2025": [8, 4, 10],
                "first_reach_date": [
                    date(2025, 1, 1),
                    date(2025, 6, 1),
                    date(2025, 1, 1),
                ],
                "last_reach_date": [None, None, date(2025, 11, 1)],
            }
        )

        expr = build_reach_hedr_denominator_expr(
            performance_year=2025,
            df_schema=df.columns,
        )

        result = df.with_columns(expr.alias("hedr_denominator"))
        # P1: alive, 8 months >= 6, started before Oct 1, still enrolled -> True
        assert result["hedr_denominator"][0] is True
        # P2: alive, 4 months < 6 -> False
        assert result["hedr_denominator"][1] is False
        # P3: deceased -> False
        assert result["hedr_denominator"][2] is False

    @pytest.mark.unit
    def test_numerator_expr_with_required_columns(self):
        from acoharmony._expressions._reach_hedr_eligible import (
            build_reach_hedr_numerator_expr,
        )

        df = pl.DataFrame(
            {
                "death_date": [None, None],
                "months_in_reach_2025": [8, 8],
                "first_reach_date": [date(2025, 1, 1), date(2025, 1, 1)],
                "last_reach_date": [None, None],
                "race": ["White", None],
                "ethnicity": ["Non-Hispanic", "Hispanic"],
            }
        )

        expr = build_reach_hedr_numerator_expr(
            performance_year=2025,
            required_data_columns=["race", "ethnicity"],
            df_schema=df.columns,
        )

        result = df.with_columns(expr.alias("hedr_numerator"))
        # P1: all required data present -> True
        assert result["hedr_numerator"][0] is True
        # P2: race is null -> False
        assert result["hedr_numerator"][1] is False

    @pytest.mark.unit
    def test_rate_expr(self):
        from acoharmony._expressions._reach_hedr_eligible import (
            build_reach_hedr_rate_expr,
        )

        schema = ["death_date", "months_in_reach_2025", "first_reach_date", "last_reach_date"]
        exprs = build_reach_hedr_rate_expr(
            performance_year=2025,
            df_schema=schema,
        )

        assert "hedr_denominator" in exprs
        assert "hedr_numerator" in exprs
        assert "hedr_eligible" in exprs
        assert "hedr_complete" in exprs

    @pytest.mark.unit
    def test_calculate_rate(self):
        from acoharmony._expressions._reach_hedr_eligible import (
            calculate_reach_hedr_rate,
        )

        assert calculate_reach_hedr_rate(1000, 850) == pytest.approx(85.0)
        assert calculate_reach_hedr_rate(0, 0) == 0.0
        assert calculate_reach_hedr_rate(100, 100) == pytest.approx(100.0)

    @pytest.mark.unit
    def test_denominator_with_temporal_matrix(self):
        from acoharmony._expressions._reach_hedr_eligible import (
            build_reach_hedr_denominator_expr,
        )

        # Test with ym_ columns instead of reach_months
        df = pl.DataFrame(
            {
                "death_date": [None],
                "first_reach_date": [date(2025, 1, 1)],
                "last_reach_date": [None],
                "ym_202501_reach": [True],
                "ym_202502_reach": [True],
                "ym_202503_reach": [True],
                "ym_202504_reach": [True],
                "ym_202505_reach": [True],
                "ym_202506_reach": [True],
                "ym_202507_reach": [True],
                "ym_202508_reach": [False],
                "ym_202509_reach": [False],
                "ym_202510_reach": [False],
            }
        )

        expr = build_reach_hedr_denominator_expr(
            performance_year=2025,
            df_schema=df.columns,
        )

        result = df.with_columns(expr.alias("hedr_denominator"))
        # 7 months through Oct >= 6 -> True
        assert result["hedr_denominator"][0] is True

    @pytest.mark.unit
    def test_denominator_insufficient_months_temporal(self):
        from acoharmony._expressions._reach_hedr_eligible import (
            build_reach_hedr_denominator_expr,
        )

        df = pl.DataFrame(
            {
                "death_date": [None],
                "first_reach_date": [date(2025, 6, 1)],
                "last_reach_date": [None],
                "ym_202501_reach": [False],
                "ym_202502_reach": [False],
                "ym_202503_reach": [False],
                "ym_202504_reach": [False],
                "ym_202505_reach": [False],
                "ym_202506_reach": [True],
                "ym_202507_reach": [True],
                "ym_202508_reach": [True],
                "ym_202509_reach": [True],
                "ym_202510_reach": [True],
            }
        )

        expr = build_reach_hedr_denominator_expr(
            performance_year=2025,
            df_schema=df.columns,
        )

        result = df.with_columns(expr.alias("hedr_denominator"))
        # 5 months through Oct < 6 -> False
        assert result["hedr_denominator"][0] is False


class TestReachHedrV3:
    """Tests for REACH HEDR module."""

    @pytest.mark.unit
    def test_module_imports(self):
        from acoharmony._transforms.reach_hedr import execute

        assert execute is not None

    @pytest.mark.unit
    def test_expression_builders_importable(self):
        from acoharmony._expressions._reach_hedr_eligible import (
            build_reach_hedr_denominator_expr,
            build_reach_hedr_numerator_expr,
        )

        assert build_reach_hedr_denominator_expr is not None
        assert build_reach_hedr_numerator_expr is not None
