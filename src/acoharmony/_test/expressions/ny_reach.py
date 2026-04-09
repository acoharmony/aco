# © 2025 HarmonyCares
# All rights reserved.





# =============================================================================
# Tests for ny_reach
# =============================================================================












# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest


class TestNyReach:
    """Test cases for expression builders."""

    @pytest.mark.unit
    def test_build_ny_reach_expr_with_office(self):

        schema = ["office_location", "death_date"]
        df = pl.DataFrame({
            "office_location": ["NY", "IL", "NY"],
            "death_date": pl.Series([None, None, None], dtype=pl.Date),
        })
        result = df.select(build_ny_reach_expr(df_schema=schema))
        # Without REACH-specific columns, just checks living + NY
        assert result[0, 0] is True
        assert result[1, 0] is False










    @pytest.mark.unit
    def test_build_ny_reach_expr_without_office(self):

        schema = ["death_date"]
        df = pl.DataFrame({
            "death_date": pl.Series([None], dtype=pl.Date),
        })
        result = df.select(build_ny_reach_expr(df_schema=schema))
        # Without office column, just checks living
        assert result[0, 0] is True










    @pytest.mark.unit
    def test_count_ny_reach_patients(self):

        lf = pl.LazyFrame({
            "office_location": ["NY", "IL", "NY"],
            "death_date": pl.Series([None, None, None], dtype=pl.Date),
        })
        count = count_ny_reach_patients(lf)
        assert count == 2










    @pytest.mark.unit
    def test_build_ny_reach_lazyframe_active(self):

        lf = pl.LazyFrame({
            "office_location": ["NY", "IL", "NY"],
            "death_date": pl.Series([None, None, None], dtype=pl.Date),
        })
        result = build_ny_reach_lazyframe(lf).collect()
        assert result.height == 2










    @pytest.mark.unit
    def test_build_ny_reach_lazyframe_include_inactive(self):

        lf = pl.LazyFrame({
            "office_location": ["NY", "IL", "NY"],
            "death_date": pl.Series([None, None, None], dtype=pl.Date),
            "first_reach_date": [date(2024, 1, 1), date(2024, 1, 1), None],
        })
        result = build_ny_reach_lazyframe(lf, include_inactive=True).collect()
        # NY + ever REACH
        assert result.height == 1










    @pytest.mark.unit
    def test_build_ny_reach_with_bar_expr(self):

        schema = ["office_location", "death_date", "source_filename"]
        pl.DataFrame({
            "office_location": ["NY"],
            "death_date": pl.Series([None], dtype=pl.Date),
            "source_filename": ["P.D0259.ALGC25.RP.D251118"],
        })
        expr = build_ny_reach_with_bar_expr(df_schema=schema)
        # Just verify it returns an expression without error
        assert expr is not None










    @pytest.mark.unit
    def test_build_ny_reach_expr_empty_schema(self):
        """build_ny_reach_expr with empty schema (line 54 - no office column)."""

        expr = build_ny_reach_expr(df_schema=[])
        assert expr is not None










    @pytest.mark.unit
    def test_build_ny_reach_lazyframe_inclusive_no_reach_history(self):
        """build_ny_reach_lazyframe with include_inactive but no reach history (line 133)."""

        df = pl.DataFrame({
            "current_mbi": ["A", "B"],
            "office_location": ["NY", "CT"],
        }).lazy()

        result = build_ny_reach_lazyframe(df, include_inactive=True)
        collected = result.collect()
        # NY office should be included
        assert collected.height >= 1










    @pytest.mark.unit
    def test_build_ny_reach_with_bar_expr_no_office(self):
        """build_ny_reach_with_bar_expr without office column (line 182)."""

        expr = build_ny_reach_with_bar_expr(df_schema=[])
        assert expr is not None










    @pytest.mark.unit
    def test_build_ny_reach_with_bar_expr_with_office(self):
        """build_ny_reach_with_bar_expr with office column (line 165)."""

        expr = build_ny_reach_with_bar_expr(df_schema=["office_location", "source_file"])
        assert expr is not None










    @pytest.mark.unit
    def test_build_ny_reach_expr_default_schema(self):
        """Line 54: df_schema defaults to []."""

        expr = build_ny_reach_expr(df_schema=None)
        assert expr is not None










    @pytest.mark.unit
    def test_build_ny_reach_with_bar_expr_default_schema(self):
        """Line 165: df_schema defaults to []."""

        expr = build_ny_reach_with_bar_expr(df_schema=None)
        assert expr is not None

    @pytest.mark.unit
    def test_build_ny_reach_lazyframe_inactive_no_office_location(self):
        """Line 124->135: include_inactive=True but no office_location in schema skips to return."""
        lf = pl.LazyFrame({
            "current_mbi": ["A", "B", "C"],
            "death_date": pl.Series([None, None, None], dtype=pl.Date),
        })
        # include_inactive=True but schema has no office_location column
        result = build_ny_reach_lazyframe(lf, include_inactive=True)
        collected = result.collect()
        # Without office_location, no filtering in the else branch -> all rows returned
        assert "is_ny_reach" in collected.columns
        assert collected.height == 3


















