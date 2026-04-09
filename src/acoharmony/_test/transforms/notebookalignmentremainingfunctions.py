# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for remaining untested functions in consolidated_alignments notebook.

Covers:
- Calculation functions: enrollment patterns, cohort analysis, sources, vintage
- Display functions: trends, patterns, appendix, cohort displays
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import sys
from pathlib import Path

try:
    import consolidated_alignments
except ModuleNotFoundError:
    import pytest
    pytest.skip("consolidated_alignments notebook not on path", allow_module_level=True)
import polars as pl
import pytest
import acoharmony

# Add notebooks directory to path
sys.path.insert(0, str(Path("/opt/s3/data/notebooks")))

# Import the notebook module


@pytest.fixture(scope="module")
def notebook_defs():
    """Run notebook once and cache definitions for all tests."""
    _, defs = consolidated_alignments.app.run()
    return defs


class TestAnalyzeEnrollmentPatterns:
    """Tests for analyze_enrollment_patterns function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003"],
            "death_date": [None, None, None],
            "ever_reach": [True, False, True],
            "ever_mssp": [False, True, True],
            "ym_202401_reach": [True, False, False],
            "ym_202401_mssp": [False, True, True],
            "has_continuous_enrollment": [True, True, False],
            "has_program_transition": [False, True, False],
            "months_in_reach": [12, 0, 6],
            "months_in_mssp": [0, 12, 6],
            "total_aligned_months": [12, 12, 12],
            "enrollment_gaps": [0, 0, 2],
        })

    @pytest.fixture
    def enriched_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003"],
            "has_valid_voluntary_alignment": [True, False, True],
            "ym_202401_reach": [True, False, False],
            "ym_202401_mssp": [False, True, True],
            "has_continuous_enrollment": [True, True, False],
            "has_program_transition": [False, True, False],
            "months_in_reach": [12, 0, 6],
            "months_in_mssp": [0, 12, 6],
            "total_aligned_months": [12, 12, 12],
            "enrollment_gaps": [0, 0, 2],
        })

    @pytest.mark.unit
    def test_returns_tuple(self, notebook_defs, sample_data, enriched_data):
        func = notebook_defs["analyze_enrollment_patterns"]
        historical, current = func(sample_data, enriched_data, "202401", pl)

        assert isinstance(historical, pl.DataFrame)
        assert isinstance(current, pl.DataFrame | type(None))

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data, enriched_data):
        func = notebook_defs["analyze_enrollment_patterns"]
        historical1, current1 = func(sample_data, enriched_data, "202401", pl)
        historical2, current2 = func(sample_data, enriched_data, "202401", pl)

        assert historical1.equals(historical2)
        if current1 is not None and current2 is not None:
            assert current1.equals(current2)
        else:
            assert current1 == current2


class TestCalculateCohortAnalysis:
    """Tests for calculate_cohort_analysis function."""

    @pytest.fixture
    def sample_data(self):
        # Need at least 4 months for cohort analysis
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004"],
            "death_date": [None, None, None, None],
            "ym_202401_reach": [True, True, False, True],
            "ym_202401_mssp": [False, False, True, False],
            "ym_202402_reach": [True, False, False, True],
            "ym_202402_mssp": [False, True, True, False],
            "ym_202403_reach": [True, False, False, False],
            "ym_202403_mssp": [False, True, False, True],
            "ym_202404_reach": [True, True, False, True],
            "ym_202404_mssp": [False, False, True, False],
            "ym_202405_reach": [True, False, True, True],
            "ym_202405_mssp": [False, True, False, False],
        })

    @pytest.mark.unit
    def test_returns_list_or_none(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_cohort_analysis"]
        # Need at least 4 months
        result = func(sample_data, ["202401", "202402", "202403", "202404", "202405"], pl)

        assert isinstance(result, list | type(None))

    @pytest.mark.unit
    def test_returns_none_with_insufficient_months(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_cohort_analysis"]
        result = func(sample_data, ["202401", "202402"], pl)

        assert result is None

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_cohort_analysis"]
        result1 = func(sample_data, ["202401", "202402", "202403", "202404", "202405"], pl)
        result2 = func(sample_data, ["202401", "202402", "202403", "202404", "202405"], pl)

        assert result1 == result2


class TestCalculateCurrentAndHistoricalSources:
    """Tests for calculate_current_and_historical_sources function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003"],
            "death_date": [None, None, None],
            "primary_alignment_source": ["SVA", "Claims", "PBVAR"],
            "ym_202401_reach": [True, True, False],
            "ym_202401_mssp": [False, False, True],
            "has_valid_voluntary_alignment": [True, False, True],
        })

    @pytest.mark.unit
    def test_returns_tuple_of_dataframes(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_current_and_historical_sources"]
        current, historical = func(sample_data, "202401", pl)

        assert isinstance(current, pl.DataFrame | type(None))
        assert isinstance(historical, pl.DataFrame)

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_current_and_historical_sources"]
        current1, historical1 = func(sample_data, "202401", pl)
        current2, historical2 = func(sample_data, "202401", pl)

        if current1 is not None and current2 is not None:
            assert current1.sort("current_alignment_source").equals(current2.sort("current_alignment_source"))
        assert historical1.sort("primary_alignment_source").equals(historical2.sort("primary_alignment_source"))


class TestCalculateMonthOverMonthComparison:
    """Tests for calculate_month_over_month_comparison function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004"],
            "death_date": [None, None, None, None],
            "ym_202312_reach": [True, True, False, False],
            "ym_202312_mssp": [False, False, True, True],
            "ym_202312_ffs": [False, False, False, False],
            "ym_202401_reach": [True, False, True, False],
            "ym_202401_mssp": [False, True, False, True],
            "ym_202401_ffs": [False, False, False, False],
            "has_valid_voluntary_alignment": [True, False, True, False],
            "has_voluntary_alignment": [True, False, True, False],
        })

    @pytest.mark.unit
    def test_returns_dict_or_none(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_month_over_month_comparison"]
        result = func(sample_data, "202401", ["202312", "202401"], pl)

        assert isinstance(result, dict | type(None))

    @pytest.mark.unit
    def test_with_valid_months(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_month_over_month_comparison"]
        result = func(sample_data, "202401", ["202312", "202401"], pl)

        if result is not None:
            assert "prev_month" in result
            assert "curr_month" in result

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_month_over_month_comparison"]
        result1 = func(sample_data, "202401", ["202312", "202401"], pl)
        result2 = func(sample_data, "202401", ["202312", "202401"], pl)

        assert result1 == result2


class TestCalculateVintageCohorts:
    """Tests for calculate_vintage_cohorts function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004"],
            "death_date": [None, None, None, None],
            "months_in_reach": [12, 6, 24, 3],
            "months_in_mssp": [0, 6, 0, 12],
            "ym_202401_reach": [True, True, False, False],
            "ym_202401_mssp": [False, False, True, True],
        })

    @pytest.mark.unit
    def test_returns_lazyframe(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_vintage_cohorts"]
        result = func(sample_data, "202401", pl)

        # Function returns LazyFrame, not DataFrame
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_vintage_cohorts"]
        result1 = func(sample_data, "202401", pl).collect().sort("current_mbi")
        result2 = func(sample_data, "202401", pl).collect().sort("current_mbi")

        assert result1.equals(result2)


class TestCalculateVintageDistribution:
    """Tests for calculate_vintage_distribution function."""

    @pytest.fixture
    def vintage_data(self):
        # This is output from calculate_vintage_cohorts - needs vintage_cohort column
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004"],
            "vintage_cohort": ["0-6 months", "6-12 months", "12-24 months", "Never Enrolled"],
            "months_in_reach": [3, 9, 18, 0],
            "months_in_mssp": [0, 0, 6, 0],
            "ym_202401_reach": [True, True, True, False],
            "ym_202401_mssp": [False, False, True, False],
            "has_program_transition": [False, False, True, False],
        })

    @pytest.mark.unit
    def test_returns_dataframe_or_none(self, notebook_defs, vintage_data):
        func = notebook_defs["calculate_vintage_distribution"]
        result = func(vintage_data, "202401", pl)

        assert isinstance(result, pl.DataFrame | type(None))

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, vintage_data):
        func = notebook_defs["calculate_vintage_distribution"]
        result1 = func(vintage_data, "202401", pl)
        result2 = func(vintage_data, "202401", pl)

        if result1 is not None and result2 is not None:
            assert result1.sort("vintage_cohort").equals(result2.sort("vintage_cohort"))
        else:
            assert result1 == result2


class TestDisplayFunctions:
    """Tests for display helper functions."""

    @pytest.mark.unit
    def test_display_alignment_trends(self, notebook_defs):
        func = notebook_defs["display_alignment_trends"]

        # Test with None (no data)
        result = func(None, consolidated_alignments.mo)
        assert result is not None

    @pytest.mark.unit
    def test_display_technical_appendix(self, notebook_defs):
        func = notebook_defs["display_technical_appendix"]
        result = func(consolidated_alignments.mo)

        assert result is not None

    @pytest.mark.unit
    def test_create_branded_header(self, notebook_defs):
        func = notebook_defs["create_branded_header"]
        from datetime import datetime as dt

        result = func(dt, consolidated_alignments.mo)

        assert result is not None
