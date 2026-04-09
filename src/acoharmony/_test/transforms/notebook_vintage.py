"""Tests for _transforms.notebook_vintage module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest
import acoharmony

from acoharmony._transforms._notebook_vintage import (  # noqa: E402
    calculate_office_vintage_distribution,
    calculate_vintage_cohorts,
    calculate_vintage_distribution,
)


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()
class TestNotebookVintage:
    """Tests for notebook vintage."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._notebook_vintage is not None

    @pytest.mark.unit
    def test_calculate_vintage_cohorts_exists(self):
        assert callable(calculate_vintage_cohorts)



class TestCalculateVintageCohorts:
    """Tests for calculate_vintage_cohorts."""

    @pytest.mark.unit
    def test_no_temporal_columns(self):
        """Returns 'Never Enrolled' when no temporal columns exist."""
        df = pl.DataFrame(
            {"current_mbi": ["MBI001", "MBI002"]}
        ).lazy()

        result = calculate_vintage_cohorts(df).collect()
        assert "vintage_cohort" in result.columns
        assert all(v == "Never Enrolled" for v in result["vintage_cohort"].to_list())

    @pytest.mark.unit
    def test_with_temporal_columns_no_most_recent(self):
        """Marks enrolled vs not when no most_recent_ym provided."""
        df = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002"],
                "ym_202401_reach": [True, False],
                "ym_202402_reach": [True, False],
            }
        ).lazy()

        result = calculate_vintage_cohorts(df).collect()
        assert "first_enrollment_ym" in result.columns
        assert "vintage_cohort" in result.columns
        cohorts = result["vintage_cohort"].to_list()
        assert "Enrolled" in cohorts
        assert "Never Enrolled" in cohorts

    @pytest.mark.unit
    def test_with_temporal_columns_and_most_recent(self):
        """Calculates proper vintage cohorts when most_recent_ym is provided."""
        df = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002", "MBI003"],
                "ym_202301_reach": [True, False, False],
                "ym_202401_reach": [True, True, False],
                "ym_202402_reach": [True, True, False],
            }
        ).lazy()

        result = calculate_vintage_cohorts(df, most_recent_ym="202402").collect()
        assert "months_since_first_enrollment" in result.columns
        assert "vintage_cohort" in result.columns

        # MBI001 enrolled since 202301 -> 13 months -> "12-24 months"
        mbi001 = result.filter(pl.col("current_mbi") == "MBI001")
        assert mbi001["vintage_cohort"][0] == "12-24 months"

        # MBI003 never enrolled
        mbi003 = result.filter(pl.col("current_mbi") == "MBI003")
        assert mbi003["vintage_cohort"][0] == "Never Enrolled"


class TestCalculateVintageDistribution:
    """Tests for calculate_vintage_distribution."""

    @pytest.mark.unit
    def test_returns_none_without_most_recent_ym(self):
        """Returns None when most_recent_ym is not provided."""
        df = pl.DataFrame({"vintage_cohort": ["0-6 months"]}).lazy()
        result = calculate_vintage_distribution(df, None)
        assert result is None

    @pytest.mark.unit
    def test_returns_none_missing_temporal_columns(self):
        """Returns None when temporal columns are missing."""
        df = pl.DataFrame({"vintage_cohort": ["0-6 months"]}).lazy()
        result = calculate_vintage_distribution(df, "202401")
        assert result is None

    @pytest.mark.unit
    def test_basic_distribution(self):
        """Calculates distribution with proper columns."""
        df = pl.DataFrame(
            {
                "vintage_cohort": ["0-6 months", "0-6 months", "Never Enrolled"],
                "ym_202401_reach": [True, True, False],
                "ym_202401_mssp": [False, False, False],
                "months_in_reach": [3, 5, 0],
                "months_in_mssp": [0, 0, 0],
            }
        ).lazy()

        result = calculate_vintage_distribution(df, "202401")
        assert result is not None
        assert "vintage_cohort" in result.columns
        assert "count" in result.columns
        assert "current_reach" in result.columns


class TestCalculateOfficeVintageDistribution:
    """Tests for calculate_office_vintage_distribution."""

    @pytest.mark.unit
    def test_returns_none_without_most_recent_ym(self):
        """Returns None when most_recent_ym is not provided."""
        df = pl.DataFrame({"vintage_cohort": ["0-6 months"]}).lazy()
        result = calculate_office_vintage_distribution(df, None)
        assert result is None

    @pytest.mark.unit
    def test_returns_none_without_office_column(self):
        """Returns None when office column doesn't exist."""
        df = pl.DataFrame(
            {
                "vintage_cohort": ["0-6 months"],
                "ym_202401_reach": [True],
                "ym_202401_mssp": [False],
            }
        ).lazy()
        result = calculate_office_vintage_distribution(df, "202401")
        assert result is None

    @pytest.mark.unit
    def test_basic_office_distribution(self):
        """Calculates office-level vintage distribution."""
        df = pl.DataFrame(
            {
                "office_name": ["Office A", "Office A", "Office B"],
                "vintage_cohort": ["0-6 months", "6-12 months", "0-6 months"],
                "ym_202401_reach": [True, True, True],
                "ym_202401_mssp": [False, False, False],
                "months_in_reach": [3, 10, 5],
                "months_in_mssp": [0, 0, 0],
            }
        ).lazy()

        result = calculate_office_vintage_distribution(df, "202401")
        assert result is not None
        assert "office_name" in result.columns
        assert "vintage_cohort" in result.columns


class TestVintageDistributionFallbackBranches:
    """Cover fallback branches for missing optional columns."""

    @pytest.mark.unit
    def test_distribution_without_months_columns(self):
        """Lines 172, 177, 182, 187: Fallback when months_in_* columns missing."""
        df = pl.DataFrame(
            {
                "vintage_cohort": ["0-6 months", "Never Enrolled"],
                "ym_202401_reach": [True, False],
                "ym_202401_mssp": [False, False],
            }
        ).lazy()

        result = calculate_vintage_distribution(df, "202401")
        assert result is not None
        assert "avg_months_reach" in result.columns
        assert "avg_months_mssp" in result.columns
        assert "avg_total_months" in result.columns
        assert "transitions" in result.columns

    @pytest.mark.unit
    def test_distribution_with_months_and_transition_columns(self):
        """Lines 169-186: Full metrics with all optional columns."""
        df = pl.DataFrame(
            {
                "vintage_cohort": ["0-6 months", "0-6 months"],
                "ym_202401_reach": [True, True],
                "ym_202401_mssp": [False, False],
                "months_in_reach": [3, 5],
                "months_in_mssp": [1, 2],
                "has_program_transition": [True, False],
            }
        ).lazy()

        result = calculate_vintage_distribution(df, "202401")
        assert result is not None
        assert "avg_months_reach" in result.columns
        assert "transitions" in result.columns

    @pytest.mark.unit
    def test_office_distribution_auto_detect_office_location(self):
        """Line 245: Auto-detect office_location column."""
        df = pl.DataFrame(
            {
                "office_location": ["Loc A", "Loc B"],
                "vintage_cohort": ["0-6 months", "6-12 months"],
                "ym_202401_reach": [True, True],
                "ym_202401_mssp": [False, False],
            }
        ).lazy()

        result = calculate_office_vintage_distribution(df, "202401")
        assert result is not None
        assert "office_location" in result.columns

    @pytest.mark.unit
    def test_office_distribution_explicit_column(self):
        """Lines 250-251: explicit office_column that exists."""
        df = pl.DataFrame(
            {
                "office_name": ["Office A"],
                "vintage_cohort": ["0-6 months"],
                "ym_202401_reach": [True],
                "ym_202401_mssp": [False],
            }
        ).lazy()

        result = calculate_office_vintage_distribution(df, "202401", office_column="office_name")
        assert result is not None

    @pytest.mark.unit
    def test_office_distribution_explicit_column_not_found(self):
        """Lines 250-251: explicit office_column that doesn't exist."""
        df = pl.DataFrame(
            {
                "vintage_cohort": ["0-6 months"],
                "ym_202401_reach": [True],
                "ym_202401_mssp": [False],
            }
        ).lazy()

        result = calculate_office_vintage_distribution(df, "202401", office_column="nonexistent")
        assert result is None

    @pytest.mark.unit
    def test_office_distribution_missing_temporal(self):
        """Lines 257-258: missing temporal columns."""
        df = pl.DataFrame(
            {
                "office_name": ["Office A"],
                "vintage_cohort": ["0-6 months"],
            }
        ).lazy()

        result = calculate_office_vintage_distribution(df, "202401")
        assert result is None

    @pytest.mark.unit
    def test_office_distribution_without_months_columns(self):
        """Lines 272, 277, 282, 287: Fallback when months_in_* columns missing."""
        df = pl.DataFrame(
            {
                "office_name": ["Office A", "Office A"],
                "vintage_cohort": ["0-6 months", "Never Enrolled"],
                "ym_202401_reach": [True, False],
                "ym_202401_mssp": [False, False],
            }
        ).lazy()

        result = calculate_office_vintage_distribution(df, "202401")
        assert result is not None
        assert "avg_months_reach" in result.columns

    @pytest.mark.unit
    def test_office_distribution_with_office_location_as_secondary(self):
        """Lines 291-292, 305-306: Both office_name and office_location in schema."""
        df = pl.DataFrame(
            {
                "office_name": ["Office A", "Office A"],
                "office_location": ["Loc 1", "Loc 1"],
                "vintage_cohort": ["0-6 months", "Never Enrolled"],
                "ym_202401_reach": [True, False],
                "ym_202401_mssp": [False, False],
            }
        ).lazy()

        result = calculate_office_vintage_distribution(df, "202401")
        assert result is not None
        assert "office_location" in result.columns

    @pytest.mark.unit
    def test_office_distribution_with_all_optional_columns(self):
        """Full coverage of office distribution with all optional columns."""
        df = pl.DataFrame(
            {
                "office_name": ["Office A", "Office A"],
                "vintage_cohort": ["0-6 months", "0-6 months"],
                "ym_202401_reach": [True, True],
                "ym_202401_mssp": [False, False],
                "months_in_reach": [3, 5],
                "months_in_mssp": [1, 2],
                "has_program_transition": [True, False],
            }
        ).lazy()

        result = calculate_office_vintage_distribution(df, "202401")
        assert result is not None
        assert "transitions" in result.columns
