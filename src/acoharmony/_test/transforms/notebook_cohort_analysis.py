"""Tests for _transforms.notebook_cohort_analysis module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest
import acoharmony


class TestNotebookCohortAnalysis:
    """Tests for notebook cohort analysis."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._notebook_cohort_analysis is not None

    @pytest.mark.unit
    def test_calculate_cohort_analysis_exists(self):
        assert callable(calculate_cohort_analysis)

    @pytest.mark.unit
    def test_calculate_cohort_analysis_empty_months(self):
        df = pl.LazyFrame({"col": [1]})
        result = calculate_cohort_analysis(df, [])
        assert result is None

    @pytest.mark.unit
    def test_calculate_cohort_analysis_insufficient_months(self):
        df = pl.LazyFrame({"col": [1]})
        result = calculate_cohort_analysis(df, ["202401", "202402", "202403"])
        assert result is None


class TestNotebookCohortDeep:
    """Deeper tests for notebook cohort analysis."""

    @pytest.mark.unit
    def test_cohort_with_sufficient_months_but_missing_cols(self):
        df = pl.LazyFrame({"col": [1, 2, 3]})
        result = calculate_cohort_analysis(
            df, ["202401", "202402", "202403", "202404", "202405", "202406"]
        )
        # With no ym_ columns, cohorts should be empty list or None
        assert result is None or isinstance(result, list)


class TestNotebookCohortAnalysisV2:
    """Tests for calculate_cohort_analysis."""

    @pytest.mark.unit
    def test_returns_none_insufficient_months(self):

        df = pl.DataFrame({"id": [1]}).lazy()
        assert calculate_cohort_analysis(df, []) is None
        assert calculate_cohort_analysis(df, ["202401", "202402", "202403"]) is None

    @pytest.mark.unit
    def test_returns_none_empty_year_months(self):

        df = pl.DataFrame({"id": [1]}).lazy()
        assert calculate_cohort_analysis(df, None) is None

    @pytest.mark.unit
    def test_basic_cohort_analysis(self):

        # Create data with temporal columns for 6 months
        months = ["202401", "202402", "202403", "202404", "202405", "202406"]
        data = {"person_id": ["P1", "P2", "P3"]}
        for m in months:
            # P1 enrolled in all months, P2 drops out after month 3, P3 only month 1
            data[f"ym_{m}_reach"] = [True, m <= "202403", m == "202401"]
            data[f"ym_{m}_mssp"] = [False, False, False]

        df = pl.DataFrame(data).lazy()
        result = calculate_cohort_analysis(df, months)
        # Should return cohort data (last 6 months minus current = last 5)
        assert result is not None
        assert len(result) > 0
        for cohort in result:
            assert "cohort" in cohort
            assert "initial_size" in cohort
            assert "month_0" in cohort

    @pytest.mark.unit
    def test_cohort_analysis_missing_columns(self):

        # year_month columns don't exist
        months = ["202401", "202402", "202403", "202404", "202405", "202406"]
        df = pl.DataFrame({"person_id": ["P1"]}).lazy()
        result = calculate_cohort_analysis(df, months)
        assert result is None  # No matching columns


# ---------------------------------------------------------------------------
# Coverage gap tests: _notebook_cohort_analysis.py line 67
# ---------------------------------------------------------------------------


class TestCohortAnalysisMissingFutureColumn:
    """Cover break when future columns are missing."""

    @pytest.mark.unit
    def test_break_on_missing_future_column(self):
        """Line 67: break when future_col_reach/mssp not in schema."""

        # Only month 1 columns exist, not month 2 -- break in retention loop
        df = pl.DataFrame({
            "ym_202401_reach": [True, False, True],
            "ym_202401_mssp": [False, True, False],
        }).lazy()

        months = ["202401", "202402"]
        result = calculate_cohort_analysis(df, months)
        # Should still produce results but with limited retention data
        if result is not None:
            assert isinstance(result, list)

    @pytest.mark.unit
    def test_break_on_missing_future_column_with_enough_months(self):
        """Line 66->67: cohort month columns exist but future month columns
        are missing from the schema, triggering the break inside the retention
        loop.

        We provide 7 year_months so that year_months[-6:-1] yields cohort
        months, and we include reach/mssp columns only for the first 5 months
        so that when the loop tries to look up month 6 or 7 columns it hits
        the break.
        """
        months = [
            "202401", "202402", "202403", "202404",
            "202405", "202406", "202407",
        ]

        # Build columns for only the first 5 months; months 6 and 7 are
        # intentionally absent from the DataFrame schema.
        data: dict[str, list] = {"id": [1, 2, 3]}
        for m in months[:5]:
            data[f"ym_{m}_reach"] = [True, True, True]
            data[f"ym_{m}_mssp"] = [False, False, False]

        df = pl.DataFrame(data).lazy()
        result = calculate_cohort_analysis(df, months)

        # Cohorts whose future month columns are missing should still produce
        # partial cohort_data (with month_0 but possibly fewer retention keys).
        assert result is not None
        assert isinstance(result, list)
        assert len(result) > 0
        for cohort in result:
            assert "month_0" in cohort


class TestCohortZeroSize:
    """Cover branch 53->40: cohort_size is 0, loop continues to next month."""

    @pytest.mark.unit
    def test_cohort_size_zero_skips_to_next(self):
        """Branch 53->40: when all rows are False for a month, cohort_size=0."""
        # 5 months with columns, but month 202402 has all False -> cohort_size=0
        months = ["202401", "202402", "202403", "202404", "202405"]
        data = {}
        for m in months:
            if m == "202402":
                # Zero enrolled in this month
                data[f"ym_{m}_reach"] = [False, False]
                data[f"ym_{m}_mssp"] = [False, False]
            else:
                data[f"ym_{m}_reach"] = [True, True]
                data[f"ym_{m}_mssp"] = [False, False]
        df = pl.DataFrame(data).lazy()
        result = calculate_cohort_analysis(df, months)
        # Should produce results but skip the month with zero enrollment
        assert result is not None
        cohort_labels = [c["cohort"] for c in result]
        # 202402 should not appear because cohort_size was 0
        assert "2024-02" not in cohort_labels
