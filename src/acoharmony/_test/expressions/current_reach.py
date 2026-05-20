# © 2025 HarmonyCares
# All rights reserved.





# =============================================================================
# Tests for current_reach
# =============================================================================












# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest


class TestCurrentReach:
    """Test cases for expression builders."""

    @pytest.mark.unit
    def test_build_current_reach_expr_minimal(self):

        df = pl.DataFrame({"death_date": pl.Series([None], dtype=pl.Date)})
        result = df.select(build_current_reach_expr(df_schema=["death_date"]))
        assert result[0, 0] is True










    @pytest.mark.unit
    def test_build_current_reach_expr_with_observable(self):

        schema = ["death_date", "observable_start", "observable_end", "last_reach_date", "ever_reach"]
        df = pl.DataFrame({
            "death_date": pl.Series([None], dtype=pl.Date),
            "observable_start": [date(2024, 1, 1)],
            "observable_end": [date(2025, 12, 31)],
            "last_reach_date": [date(2025, 1, 15)],
            "ever_reach": [True],
        })
        ref = date(2025, 6, 1)
        result = df.select(build_current_reach_expr(reference_date=ref, df_schema=schema))
        assert result[0, 0] is True










    @pytest.mark.unit
    def test_build_reach_attribution_window_expr(self):

        schema = ["death_date", "first_reach_date", "last_reach_date"]
        df = pl.DataFrame({
            "death_date": pl.Series([None, None], dtype=pl.Date),
            "first_reach_date": [date(2024, 1, 1), date(2025, 7, 1)],
            "last_reach_date": [date(2025, 6, 30), date(2025, 12, 31)],
        })
        expr = build_reach_attribution_window_expr(
            date(2025, 1, 1), date(2025, 6, 30), df_schema=schema
        )
        result = df.select(expr)
        assert result[0, 0] is True
        assert result[1, 0] is False  # started after window










    @pytest.mark.unit
    def test_build_current_reach_expr_default_schema(self):
        """Line 45: df_schema defaults to []."""

        expr = build_current_reach_expr(df_schema=None)
        assert expr is not None










    @pytest.mark.unit
    def test_build_current_reach_with_bar_expr_default_schema(self):
        """Line 100: df_schema defaults to []."""

        expr = build_current_reach_with_bar_expr(df_schema=None)
        assert expr is not None

    @pytest.mark.unit
    def test_build_current_reach_with_bar_expr_explicit_reference_date(self):
        """Branch 94->98: reference_date is not None, skips today() default."""
        ref = date(2025, 6, 1)
        schema = ["source_filename"]
        df = pl.DataFrame({
            "source_filename": [
                "P.D0259.ALGC25.RP.D251118",
                "P.D0259.ALGR24.RP.D240601",
            ],
        })
        expr = build_current_reach_with_bar_expr(reference_date=ref, df_schema=schema)
        result = df.select(expr)
        # First row is ALGC and is max ALGC filename -> True
        assert result[0, 0] is True
        # Second row is ALGR -> False
        assert result[1, 0] is False

    @pytest.mark.unit
    def test_build_current_reach_with_bar_expr_program_filter(self):
        """Branch 128->129: 'program' in df_schema adds program == REACH filter."""
        ref = date(2025, 6, 1)
        schema = ["source_filename", "program"]
        df = pl.DataFrame({
            "source_filename": [
                "P.D0259.ALGC25.RP.D251118",
                "P.D0259.ALGC25.RP.D251118",
            ],
            "program": ["REACH", "ACO"],
        })
        expr = build_current_reach_with_bar_expr(reference_date=ref, df_schema=schema)
        result = df.select(expr)
        assert result[0, 0] is True   # REACH program matches
        assert result[1, 0] is False  # ACO program does not match

    @pytest.mark.unit
    def test_build_current_reach_with_bar_expr_file_date_parsed(self):
        """Branch 132->134: 'file_date_parsed' in df_schema adds recency check."""
        ref = date(2025, 6, 1)
        schema = ["source_filename", "file_date_parsed"]
        df = pl.DataFrame({
            "source_filename": [
                "P.D0259.ALGC25.RP.D251118",
                "P.D0259.ALGC25.RP.D251118",
            ],
            "file_date_parsed": [
                date(2025, 5, 1),   # within 180 days of ref
                date(2024, 1, 1),   # older than 180 days from ref
            ],
        })
        expr = build_current_reach_with_bar_expr(reference_date=ref, df_schema=schema)
        result = df.select(expr)
        assert result[0, 0] is True   # recent file date
        assert result[1, 0] is False  # too old file date

    @pytest.mark.unit
    def test_build_reach_attribution_window_expr_default_schema(self):
        """Branch 157->158: df_schema defaults to [] when None."""
        df = pl.DataFrame({
            "death_date": pl.Series([None], dtype=pl.Date),
        })
        expr = build_reach_attribution_window_expr(
            date(2025, 1, 1), date(2025, 12, 31), df_schema=None
        )
        result = df.select(expr)
        # With empty schema, only living_expr applies (no reach date columns)
        assert result[0, 0] is True

    @pytest.mark.unit
    def test_build_reach_attribution_window_expr_no_reach_dates_in_schema(self):
        """Branch 166->176: schema lacks first/last_reach_date, skips overlap check."""
        df = pl.DataFrame({
            "death_date": pl.Series([None], dtype=pl.Date),
        })
        expr = build_reach_attribution_window_expr(
            date(2025, 1, 1), date(2025, 12, 31), df_schema=["death_date"]
        )
        result = df.select(expr)
        # Without reach date columns, only living check applies
        assert result[0, 0] is True


















