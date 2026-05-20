"""Tests for _transforms.notebook_transitions module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

import acoharmony
import acoharmony._transforms._notebook_transitions as _notebook_transitions
from acoharmony._transforms._notebook_transitions import (  # noqa: E402
    analyze_enrollment_patterns,
    calculate_alignment_transitions,
    calculate_month_over_month_comparison,
)


class TestNotebookTransitions:
    """Tests for notebook transitions."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._notebook_transitions is not None

    @pytest.mark.unit
    def test_calculate_alignment_transitions_exists(self):
        assert callable(calculate_alignment_transitions)

    @pytest.mark.unit
    def test_calculate_alignment_transitions_empty(self):
        df = pl.LazyFrame({"col": [1]})
        result = calculate_alignment_transitions(df, None, [])
        assert result == (None, None, None)



class TestCalculateAlignmentTransitions:
    """Tests for calculate_alignment_transitions."""

    def _make_enriched_df(self):
        """Create a minimal enriched DataFrame with temporal columns."""
        return pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002", "MBI003", "MBI004"],
                "ym_202401_reach": [False, True, True, False],
                "ym_202401_mssp": [True, False, False, False],
                "ym_202401_ffs": [False, False, False, True],
                "ym_202402_reach": [True, True, False, False],
                "ym_202402_mssp": [False, False, True, False],
                "ym_202402_ffs": [False, False, False, True],
            }
        ).lazy()

    @pytest.mark.unit
    def test_transitions_basic(self):
        """Calculate transitions between two months."""
        df = self._make_enriched_df()
        year_months = ["202401", "202402"]
        result, prev_ym, curr_ym = calculate_alignment_transitions(df, "202402", year_months)

        assert result is not None
        assert prev_ym == "202401"
        assert curr_ym == "202402"
        assert "transition_type" in result.columns
        assert "count" in result.columns
        assert result["count"].sum() == 4

    @pytest.mark.unit
    def test_transitions_none_when_no_selected_ym(self):
        """Returns None when selected_ym is None."""
        df = self._make_enriched_df()
        result, _, _ = calculate_alignment_transitions(df, None, ["202401"])
        assert result is None

    @pytest.mark.unit
    def test_transitions_none_when_no_year_months(self):
        """Returns None when year_months is empty."""
        df = self._make_enriched_df()
        result, _, _ = calculate_alignment_transitions(df, "202401", [])
        assert result is None

    @pytest.mark.unit
    def test_transitions_none_when_first_month(self):
        """Returns None when selected_ym is the first month."""
        df = self._make_enriched_df()
        result, _, _ = calculate_alignment_transitions(df, "202401", ["202401", "202402"])
        assert result is None

    @pytest.mark.unit
    def test_transitions_invalid_selected_ym(self):
        """Returns None when selected_ym not in year_months."""
        df = self._make_enriched_df()
        result, _, _ = calculate_alignment_transitions(df, "202412", ["202401", "202402"])
        assert result is None

    @pytest.mark.unit
    def test_transitions_missing_columns(self):
        """Returns None when required columns don't exist."""
        df = pl.DataFrame({"current_mbi": ["MBI001"]}).lazy()
        result, _, _ = calculate_alignment_transitions(df, "202402", ["202401", "202402"])
        assert result is None


class TestCalculateMonthOverMonthComparison:
    """Tests for calculate_month_over_month_comparison."""

    def _make_enriched_df(self):
        return pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002", "MBI003"],
                "ym_202401_reach": [True, False, False],
                "ym_202401_mssp": [False, True, False],
                "ym_202401_ffs": [False, False, True],
                "ym_202402_reach": [True, True, False],
                "ym_202402_mssp": [False, False, False],
                "ym_202402_ffs": [False, False, True],
            }
        ).lazy()

    @pytest.mark.unit
    def test_basic_comparison(self):
        """Calculate month-over-month comparison."""
        df = self._make_enriched_df()
        result = calculate_month_over_month_comparison(df, "202402", ["202401", "202402"])

        assert result is not None
        assert "reach_change" in result
        assert "mssp_change" in result
        assert "ffs_change" in result
        assert result["reach_change"] == 1  # 1 -> 2
        assert result["mssp_change"] == -1  # 1 -> 0

    @pytest.mark.unit
    def test_none_when_no_selected_ym(self):
        """Returns None when selected_ym is None."""
        df = self._make_enriched_df()
        result = calculate_month_over_month_comparison(df, None, ["202401"])
        assert result is None

    @pytest.mark.unit
    def test_none_when_first_month(self):
        """Returns None for first month."""
        df = self._make_enriched_df()
        result = calculate_month_over_month_comparison(df, "202401", ["202401", "202402"])
        assert result is None

    @pytest.mark.unit
    def test_none_when_invalid_month(self):
        """Returns None when selected_ym not in list."""
        df = self._make_enriched_df()
        result = calculate_month_over_month_comparison(df, "202412", ["202401", "202402"])
        assert result is None


class TestAnalyzeEnrollmentPatterns:
    """Tests for analyze_enrollment_patterns."""

    @pytest.mark.unit
    def test_basic_enrollment_patterns(self):
        """Calculate enrollment pattern metrics."""
        df = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002"],
                "months_in_reach": [6, 3],
                "months_in_mssp": [2, 9],
                "enrollment_gaps": [0, 1],
            }
        ).lazy()

        df_enriched = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002"],
                "months_in_reach": [6, 3],
                "months_in_mssp": [2, 9],
                "enrollment_gaps": [0, 1],
            }
        ).lazy()

        historical, current = analyze_enrollment_patterns(df, df_enriched, None)
        assert historical is not None
        assert current is None  # No selected_ym

    @pytest.mark.unit
    def test_with_selected_ym(self):
        """Calculate patterns with selected month."""
        df = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002"],
                "months_in_reach": [6, 3],
                "months_in_mssp": [2, 9],
                "enrollment_gaps": [0, 1],
            }
        ).lazy()

        df_enriched = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002"],
                "ym_202402_reach": [True, False],
                "ym_202402_mssp": [False, True],
                "months_in_reach": [6, 3],
                "months_in_mssp": [2, 9],
                "enrollment_gaps": [0, 1],
            }
        ).lazy()

        historical, current = analyze_enrollment_patterns(df, df_enriched, "202402")
        assert historical is not None
        assert current is not None


# ===================== Coverage gap: lines 159, 261-288 (missing schema columns, enrollment fallbacks) =====================

class TestCompareMonthlyEnrollmentMissingColumns:
    """Test calculate_month_over_month_comparison when columns are missing (line 159)."""

    @pytest.mark.unit
    def test_returns_none_when_required_columns_missing(self):
        """calculate_month_over_month_comparison returns None when year-month columns missing."""
        df_enriched = pl.DataFrame({
            "current_mbi": ["A", "B"],
        }).lazy()
        result = calculate_month_over_month_comparison(df_enriched, "202402", ["202401", "202402"])
        assert result is None


class TestAnalyzeEnrollmentPatternsFallbacks:
    """Test analyze_enrollment_patterns fallback expressions (lines 261-288)."""

    @pytest.mark.unit
    def test_fallback_when_columns_missing(self):
        """analyze_enrollment_patterns uses fallback expressions when columns missing."""


        # Minimal df without optional columns
        df = pl.DataFrame({
            "current_mbi": ["A", "B", "C"],
        }).lazy()

        df_enriched = pl.DataFrame({
            "current_mbi": ["A", "B", "C"],
        }).lazy()

        historical, current = analyze_enrollment_patterns(df, df_enriched, None)
        assert historical is not None
        assert current is None

    @pytest.mark.unit
    def test_fallback_continuous_enrollment_missing(self):
        """Fallback when has_continuous_enrollment column is missing."""


        df = pl.DataFrame({
            "current_mbi": ["A", "B"],
            "has_program_transition": [True, False],
        }).lazy()

        df_enriched = pl.DataFrame({
            "current_mbi": ["A", "B"],
        }).lazy()

        historical, current = analyze_enrollment_patterns(df, df_enriched, None)
        assert historical is not None
        # continuous_count should be 0 (fallback)
        assert historical["continuous_count"][0] == 0

    @pytest.mark.unit
    def test_fallback_months_columns_missing(self):
        """Fallback when months_in_reach/mssp/total columns missing."""


        df = pl.DataFrame({
            "current_mbi": ["A"],
        }).lazy()
        df_enriched = df.clone()

        historical, current = analyze_enrollment_patterns(df, df_enriched, None)
        assert historical is not None
        assert historical["avg_reach_months"][0] == 0.0
        assert historical["avg_mssp_months"][0] == 0.0
        assert historical["avg_total_months"][0] == 0.0
        assert historical["avg_gaps"][0] == 0.0


# ---------------------------------------------------------------------------
# Coverage gap tests: _notebook_transitions.py lines 261, 281
# ---------------------------------------------------------------------------


class TestAnalyzeEnrollmentPatternsSchemaCheck:
    """Cover schema presence check branches."""

    @pytest.mark.unit
    def test_has_continuous_enrollment_in_schema(self):
        """Line 261: 'has_continuous_enrollment' present uses real column."""

        df = pl.DataFrame({
            "current_mbi": ["A"],
            "has_continuous_enrollment": [True],
            "total_aligned_months": [12],
        }).lazy()

        historical, current = analyze_enrollment_patterns(df, df.clone(), None)
        assert historical is not None
        assert historical["continuous_count"][0] == 1

    @pytest.mark.unit
    def test_total_aligned_months_in_schema(self):
        """Line 281: 'total_aligned_months' present uses real column."""

        df = pl.DataFrame({
            "current_mbi": ["A"],
            "total_aligned_months": [6],
        }).lazy()

        historical, current = analyze_enrollment_patterns(df, df.clone(), None)
        assert historical is not None
        assert historical["avg_total_months"][0] == 6.0


class TestAnalyzeEnrollmentPatternsSelectedYmMissingCols:
    """Cover branch 301->312: selected_ym is set but reach/mssp cols missing in enriched schema."""

    @pytest.mark.unit
    def test_current_enrollment_none_when_ym_cols_missing(self):
        """Line 301->312: reach_col/mssp_col NOT in enriched_schema => current_enrollment stays None."""
        df = pl.DataFrame({
            "current_mbi": ["A", "B"],
        }).lazy()

        # enriched df does NOT have ym_202402_reach / ym_202402_mssp columns
        df_enriched = pl.DataFrame({
            "current_mbi": ["A", "B"],
            "some_other_col": [1, 2],
        }).lazy()

        historical, current = analyze_enrollment_patterns(df, df_enriched, "202402")
        assert historical is not None
        assert current is None  # Branch 301->312: columns missing so current stays None
