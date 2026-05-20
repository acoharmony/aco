# © 2025 HarmonyCares
# All rights reserved.





# =============================================================================
# Tests for current_mssp
# =============================================================================












# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest


class TestCurrentMssp:
    """Test cases for expression builders."""

    @pytest.mark.unit
    def test_build_current_mssp_expr_minimal(self):

        df = pl.DataFrame({"death_date": pl.Series([None], dtype=pl.Date)})
        result = df.select(build_current_mssp_expr(df_schema=["death_date"]))
        assert result[0, 0] is True










    @pytest.mark.unit
    def test_build_current_mssp_expr_reach_precedence(self):

        schema = ["death_date", "last_mssp_date", "last_reach_date", "ever_mssp"]
        df = pl.DataFrame({
            "death_date": pl.Series([None, None], dtype=pl.Date),
            "last_mssp_date": [date(2024, 6, 1), date(2025, 6, 1)],
            "last_reach_date": [date(2025, 1, 1), date(2024, 1, 1)],
            "ever_mssp": [True, True],
        })
        ref = date(2025, 6, 1)
        result = df.select(build_current_mssp_expr(reference_date=ref, df_schema=schema))
        assert result[0, 0] is False  # REACH is more recent
        assert result[1, 0] is True   # MSSP is more recent










    @pytest.mark.unit
    def test_build_mssp_attribution_window_expr(self):

        schema = ["death_date", "first_mssp_date", "last_mssp_date"]
        df = pl.DataFrame({
            "death_date": pl.Series([None], dtype=pl.Date),
            "first_mssp_date": [date(2024, 1, 1)],
            "last_mssp_date": [date(2025, 6, 30)],
        })
        result = df.select(
            build_mssp_attribution_window_expr(date(2025, 1, 1), date(2025, 6, 30), df_schema=schema)
        )
        assert result[0, 0] is True










    @pytest.mark.unit
    def test_build_current_mssp_expr_with_none_schema(self):
        """Test build_current_mssp_expr with None schema defaults to []."""

        expr = build_current_mssp_expr(df_schema=None)
        assert expr is not None










    @pytest.mark.unit
    def test_build_current_mssp_expr_empty_schema(self):
        """Test with empty schema (line 52)."""

        expr = build_current_mssp_expr(df_schema=[])
        assert expr is not None










    @pytest.mark.unit
    def test_build_current_mssp_expr_only_last_reach(self):
        """When only last_reach_date is in schema (line 94)."""

        expr = build_current_mssp_expr(df_schema=["last_reach_date"])
        assert expr is not None










    @pytest.mark.unit
    def test_build_current_mssp_with_alr_none_schema(self):
        """Test build_current_mssp_with_alr_expr with None schema."""

        expr = build_current_mssp_with_alr_expr(df_schema=None)
        assert expr is not None










    @pytest.mark.unit
    def test_build_current_mssp_with_alr_program_col(self):
        """Test with program column in schema (line 132)."""

        expr = build_current_mssp_with_alr_expr(df_schema=["program", "source_file"])
        assert expr is not None










    @pytest.mark.unit
    def test_build_mssp_attribution_window_none_schema(self):
        """Test build_mssp_attribution_window_expr with None schema (line 178)."""

        expr = build_mssp_attribution_window_expr(date(2024, 1, 1), date(2024, 12, 31), df_schema=None)
        assert expr is not None

    @pytest.mark.unit
    def test_build_current_mssp_expr_observable_window(self):
        """Test branch 61->63: observable_start and observable_end columns filter correctly."""
        schema = ["death_date", "observable_start", "observable_end"]
        ref = date(2025, 3, 15)
        df = pl.DataFrame({
            "death_date": pl.Series([None, None, None], dtype=pl.Date),
            "observable_start": [date(2025, 1, 1), date(2025, 4, 1), date(2025, 1, 1)],
            "observable_end": [date(2025, 6, 30), date(2025, 6, 30), date(2025, 2, 28)],
        })
        result = df.select(build_current_mssp_expr(reference_date=ref, df_schema=schema))
        # Row 0: ref is within window -> True
        assert result[0, 0] is True
        # Row 1: ref is before observable_start -> False
        assert result[1, 0] is False
        # Row 2: ref is after observable_end -> False
        assert result[2, 0] is False

    @pytest.mark.unit
    def test_build_current_mssp_with_alr_expr_none_reference_date(self):
        """Test branch 116->120: reference_date defaults to today when None."""
        expr = build_current_mssp_with_alr_expr(reference_date=None, df_schema=["source_filename"])
        assert expr is not None

    @pytest.mark.unit
    def test_build_current_mssp_with_alr_expr_file_date_parsed(self):
        """Test branch 136->138: file_date_parsed filtering for stale files."""
        ref = date(2025, 6, 1)
        schema = ["source_filename", "file_date_parsed"]
        df = pl.DataFrame({
            "source_filename": ["report_2025.csv", "report_2025.csv"],
            "file_date_parsed": [date(2025, 5, 1), date(2024, 10, 1)],
        })
        result = df.select(build_current_mssp_with_alr_expr(reference_date=ref, df_schema=schema))
        # Row 0: file date within 180 days -> True
        assert result[0, 0] is True
        # Row 1: file date older than 180 days -> False
        assert result[1, 0] is False

    @pytest.mark.unit
    def test_build_current_mssp_with_alr_expr_reach_and_mssp(self):
        """Test branch 145->147: last_reach_date and last_mssp_date both in schema."""
        ref = date(2025, 6, 1)
        schema = ["source_filename", "last_reach_date", "last_mssp_date"]
        df = pl.DataFrame({
            "source_filename": ["report.csv", "report.csv", "report.csv"],
            "last_reach_date": pl.Series([None, date(2025, 5, 1), date(2024, 1, 1)], dtype=pl.Date),
            "last_mssp_date": [date(2025, 5, 1), date(2025, 3, 1), date(2025, 5, 1)],
        })
        result = df.select(build_current_mssp_with_alr_expr(reference_date=ref, df_schema=schema))
        # Row 0: reach is null -> True
        assert result[0, 0] is True
        # Row 1: reach > mssp -> False (REACH takes precedence)
        assert result[1, 0] is False
        # Row 2: mssp > reach -> True
        assert result[2, 0] is True

    @pytest.mark.unit
    def test_build_current_mssp_with_alr_expr_only_reach(self):
        """Test branch 151->153: only last_reach_date in schema (no last_mssp_date)."""
        ref = date(2025, 6, 1)
        schema = ["source_filename", "last_reach_date"]
        df = pl.DataFrame({
            "source_filename": ["report.csv", "report.csv"],
            "last_reach_date": pl.Series([None, date(2025, 1, 1)], dtype=pl.Date),
        })
        result = df.select(build_current_mssp_with_alr_expr(reference_date=ref, df_schema=schema))
        # Row 0: reach is null -> True
        assert result[0, 0] is True
        # Row 1: reach is not null -> False (conservative exclusion)
        assert result[1, 0] is False

    @pytest.mark.unit
    def test_build_mssp_attribution_window_expr_with_reach(self):
        """Test branch 199->201: REACH exclusion in attribution window."""
        schema = [
            "death_date", "first_mssp_date", "last_mssp_date",
            "first_reach_date", "last_reach_date",
        ]
        start = date(2025, 1, 1)
        end = date(2025, 6, 30)
        df = pl.DataFrame({
            "death_date": pl.Series([None, None, None], dtype=pl.Date),
            "first_mssp_date": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1)],
            "last_mssp_date": [date(2025, 6, 30), date(2025, 3, 1), date(2025, 6, 30)],
            "first_reach_date": pl.Series([None, date(2025, 2, 1), date(2025, 2, 1)], dtype=pl.Date),
            "last_reach_date": pl.Series([None, date(2025, 6, 30), date(2025, 4, 1)], dtype=pl.Date),
        })
        result = df.select(
            build_mssp_attribution_window_expr(start, end, df_schema=schema)
        )
        # Row 0: no REACH at all -> True
        assert result[0, 0] is True
        # Row 1: REACH overlaps window AND last_reach > last_mssp -> False
        assert result[1, 0] is False
        # Row 2: REACH overlaps window BUT last_mssp > last_reach -> True
        assert result[2, 0] is True


















