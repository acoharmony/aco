"""Tests for _transforms.notebook_utilities module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime

import polars as pl
import pytest

import acoharmony
import acoharmony._transforms._notebook_utilities as _notebook_utilities
from acoharmony._transforms._notebook_utilities import (
    analyze_sva_action_categories,
    calculate_basic_stats,
    calculate_current_and_historical_sources,
    calculate_current_program_distribution,
    calculate_historical_program_distribution,
    enrich_with_outreach_data,
    extract_year_months,
    prepare_voluntary_outreach_data,
)


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestNotebookUtilities:
    """Tests for notebook utilities."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._notebook_utilities is not None

    @pytest.mark.unit
    def test_calculate_basic_stats(self):
        df = pl.LazyFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        stats = calculate_basic_stats(df)
        assert stats["total_records"] == 3
        assert stats["total_columns"] == 2

    @pytest.mark.unit
    def test_extract_year_months_exists(self):
        assert callable(extract_year_months)

    @pytest.mark.unit
    def test_extract_year_months_no_ym_cols(self):
        df = pl.LazyFrame({"a": [1, 2], "b": [3, 4]})
        most_recent, year_months = extract_year_months(df)
        assert year_months == []
        assert most_recent is None


class TestNotebookUtilitiesDeep:
    """Deeper tests for notebook utility functions."""

    @pytest.mark.unit
    def test_basic_stats_single_row(self):
        df = pl.LazyFrame({"x": [42]})
        stats = calculate_basic_stats(df)
        assert stats["total_records"] == 1
        assert stats["total_columns"] == 1

    @pytest.mark.unit
    def test_basic_stats_multiple_columns(self):
        df = pl.LazyFrame({"a": [1], "b": [2], "c": [3], "d": [4]})
        stats = calculate_basic_stats(df)
        assert stats["total_records"] == 1
        assert stats["total_columns"] == 4

    @pytest.mark.unit
    def test_extract_year_months_with_ym_cols(self):
        df = pl.LazyFrame({
            "ym_202401_reach": [True],
            "ym_202401_mssp": [False],
            "ym_202402_reach": [True],
            "ym_202402_mssp": [True],
            "other_col": [1],
        })
        most_recent, year_months = extract_year_months(df)
        assert len(year_months) > 0
        assert "202401" in year_months or "202402" in year_months


class TestNotebookUtilitiesV2:
    """Tests for notebook utility functions."""

    @pytest.mark.unit
    def test_calculate_basic_stats(self):

        df = pl.DataFrame({
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
        }).lazy()

        stats = calculate_basic_stats(df)
        assert stats["total_records"] == 3
        assert stats["total_columns"] == 2

    @pytest.mark.unit
    def test_extract_year_months(self):

        df = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "ym_202401_reach": [True],
            "ym_202401_mssp": [False],
            "ym_202402_reach": [True],
            "ym_202402_mssp": [True],
            "ym_202403_reach": [False],
            "ym_202403_mssp": [True],
        }).lazy()

        most_recent, year_months = extract_year_months(df)
        assert most_recent == "202403"
        assert year_months == ["202401", "202402", "202403"]

    @pytest.mark.unit
    def test_extract_year_months_no_ym_columns(self):

        df = pl.DataFrame({"col1": [1, 2]}).lazy()
        most_recent, year_months = extract_year_months(df)
        assert most_recent is None
        assert year_months == []

    @pytest.mark.unit
    def test_calculate_historical_program_distribution(self):
        df = pl.DataFrame({
            "ever_reach": [True, True, False, False],
            "ever_mssp": [True, False, True, False],
        }).lazy()

        result = calculate_historical_program_distribution(df)
        assert result["ever_reach_count"][0] == 2
        assert result["ever_mssp_count"][0] == 2
        assert result["ever_both_count"][0] == 1
        assert result["never_aligned_count"][0] == 1

    @pytest.mark.unit
    def test_calculate_historical_program_distribution_no_columns(self):
        df = pl.DataFrame({"other_col": [1, 2, 3]}).lazy()
        result = calculate_historical_program_distribution(df)
        assert result["ever_reach_count"][0] == 0

    @pytest.mark.unit
    def test_calculate_current_program_distribution(self):
        df = pl.DataFrame({
            "ym_202403_reach": [True, False, True, False],
            "ym_202403_mssp": [False, True, True, False],
        }).lazy()

        result = calculate_current_program_distribution(df, "202403")
        assert result["currently_reach"][0] == 2
        assert result["currently_mssp"][0] == 2
        assert result["currently_both"][0] == 1
        assert result["currently_neither"][0] == 1

    @pytest.mark.unit
    def test_calculate_current_program_distribution_no_ym(self):
        df = pl.DataFrame({"col": [1]}).lazy()
        result = calculate_current_program_distribution(df, None)
        assert result["currently_reach"][0] == 0

    @pytest.mark.unit
    def test_calculate_current_program_distribution_missing_cols(self):
        df = pl.DataFrame({"col": [1]}).lazy()
        result = calculate_current_program_distribution(df, "202403")
        assert result["currently_reach"][0] == 0

    @pytest.mark.unit
    def test_analyze_sva_action_categories(self):
        df = pl.DataFrame({
            "sva_action_needed": ["renew", "new", "renew", "none"],
        }).lazy()

        result = analyze_sva_action_categories(df)
        assert result.height == 3
        assert "sva_action_needed" in result.columns
        assert "count" in result.columns

    @pytest.mark.unit
    def test_analyze_sva_action_categories_no_column(self):
        df = pl.DataFrame({"other": [1]}).lazy()
        result = analyze_sva_action_categories(df)
        assert result.height == 0

    @pytest.mark.unit
    def test_calculate_current_and_historical_sources_no_ym(self):
        df = pl.DataFrame({"col": [1]}).lazy()
        current, historical = calculate_current_and_historical_sources(df, None)
        assert current["current_alignment_source"][0] == "NO DATA"
        assert historical["primary_alignment_source"][0] == "Unknown"

    @pytest.mark.unit
    def test_calculate_current_and_historical_sources_with_primary(self):
        df = pl.DataFrame({
            "primary_alignment_source": ["sva", "claims", "sva"],
        }).lazy()

        current, historical = calculate_current_and_historical_sources(df, None)
        assert historical.height == 2  # sva and claims
        assert current["current_alignment_source"][0] == "NO DATA"

    @pytest.mark.unit
    def test_enrich_with_outreach_data(self):
        df = pl.DataFrame({
            "current_mbi": ["MBI1", "MBI2"],
        }).lazy()

        email_mbis = pl.DataFrame({
            "mbi": ["MBI1"],
            "voluntary_email_count": [3],
            "voluntary_email_campaigns": [2],
            "email_campaign_periods": ["2024_Q1"],
            "voluntary_emails_opened": [2],
            "voluntary_emails_clicked": [1],
            "last_voluntary_email_date": [None],
        }).lazy()

        mailed_mbis = pl.DataFrame({
            "mbi": ["MBI2"],
            "voluntary_letter_count": [1],
            "voluntary_letter_campaigns": [1],
            "letter_campaign_periods": ["2024_Q2"],
            "last_voluntary_letter_date": [None],
        }).lazy()

        result = enrich_with_outreach_data(df, email_mbis, mailed_mbis).collect()
        assert "voluntary_outreach_attempts" in result.columns
        assert "has_voluntary_outreach" in result.columns
        assert "voluntary_outreach_type" in result.columns
        assert "voluntary_engagement_level" in result.columns

        mbi1_row = result.filter(pl.col("current_mbi") == "MBI1")
        assert mbi1_row["voluntary_outreach_type"][0] == "Email Only"
        assert mbi1_row["voluntary_engagement_level"][0] == "Clicked"


# ===================== Coverage gap: lines 268-270, 310-415 =====================

class TestAlignmentSourceFallbackNoData:
    """Test calculate_alignment_source_stats fallback paths (lines 268-270)."""

    @pytest.mark.unit
    def test_no_data_when_source_expr_missing(self):
        """Returns NO DATA fallback when current_alignment_source cannot be built."""
        df_enriched = pl.DataFrame({
            "current_mbi": ["A", "B"],
        }).lazy()

        current, historical = calculate_current_and_historical_sources(df_enriched, None)
        # Without selected_ym, current source should be NO DATA fallback
        assert current["current_alignment_source"][0] == "NO DATA"


class TestPrepareVoluntaryOutreachData:
    """Test prepare_voluntary_outreach_data function (lines 310-415)."""

    @pytest.mark.unit
    def test_basic_email_and_mail_preparation(self):
        """Test basic email and mail voluntary outreach data preparation."""


        emails_df = pl.DataFrame({
            "campaign": ["2024 Q2 ACO Voluntary Alignment", "Other Campaign"],
            "mbi": ["MBI1", "MBI2"],
            "has_been_opened": ["true", "false"],
            "has_been_clicked": ["false", "false"],
            "send_datetime": ["2024-06-01", "2024-06-01"],
        }).lazy()

        mailed_df = pl.DataFrame({
            "campaign_name": ["2024 Q2 ACO Voluntary Alignment", "Other"],
            "mbi": ["MBI1", "MBI3"],
            "status": ["delivered", "pending"],
            "send_datetime": ["2024-06-15", "2024-06-15"],
        }).lazy()

        email_by_campaign, email_mbis, mailed_by_campaign, mailed_mbis = (
            prepare_voluntary_outreach_data(emails_df, mailed_df)
        )

        # All should be LazyFrames
        assert isinstance(email_by_campaign, pl.LazyFrame)
        assert isinstance(email_mbis, pl.LazyFrame)
        assert isinstance(mailed_by_campaign, pl.LazyFrame)
        assert isinstance(mailed_mbis, pl.LazyFrame)

        # Collect and verify email_mbis filtered to voluntary only
        email_collected = email_mbis.collect()
        assert email_collected.height >= 1  # MBI1 had voluntary email

    @pytest.mark.unit
    def test_no_voluntary_campaigns(self):
        """When no voluntary alignment campaigns exist."""


        emails_df = pl.DataFrame({
            "campaign": ["Other Campaign"],
            "mbi": ["MBI1"],
            "has_been_opened": ["false"],
            "has_been_clicked": ["false"],
            "send_datetime": ["2024-06-01"],
        }).lazy()

        mailed_df = pl.DataFrame({
            "campaign_name": ["Other"],
            "mbi": ["MBI1"],
            "status": ["delivered"],
            "send_datetime": ["2024-06-01"],
        }).lazy()

        email_by_campaign, email_mbis, mailed_by_campaign, mailed_mbis = (
            prepare_voluntary_outreach_data(emails_df, mailed_df)
        )
        # Should return empty results since no voluntary alignment campaigns
        assert email_mbis.collect().height == 0
        assert mailed_mbis.collect().height == 0


# ---------------------------------------------------------------------------
# Coverage gap tests: _notebook_utilities.py lines 268, 270
# ---------------------------------------------------------------------------


class TestAlignmentSourceStatsNoData:
    """Cover NO DATA branches for alignment source stats."""

    @pytest.mark.unit
    def test_current_alignment_source_no_data_branch(self):
        """Lines 268, 270: missing columns produce NO DATA entries."""
        # DataFrame without current_alignment_source column
        df = pl.DataFrame({
            "current_mbi": ["MBI001"],
        }).lazy()

        current, historical = calculate_current_and_historical_sources(df, None)
        assert current["current_alignment_source"][0] == "NO DATA"


class TestCurrentSourceMissingCols:
    """Cover branch 214->270: selected_ym given but reach/mssp cols missing."""

    @pytest.mark.unit
    def test_selected_ym_but_missing_columns(self):
        """Branch 214->270: reach_col not in schema produces NO DATA."""
        df = pl.DataFrame({
            "current_mbi": ["MBI001"],
        }).lazy()
        current, _historical = calculate_current_and_historical_sources(df, "202403")
        assert current["current_alignment_source"][0] == "NO DATA"
        assert current["count"][0] == 0


class TestCurrentSourceEmptyAligned:
    """Cover branch 219->268: columns exist but currently_aligned is empty."""

    @pytest.mark.unit
    def test_no_currently_aligned_beneficiaries(self):
        """Branch 219->268: columns exist but no rows pass the filter."""
        df = pl.DataFrame({
            "ym_202403_reach": [False, False],
            "ym_202403_mssp": [False, False],
            "primary_alignment_source": ["sva", "claims"],
            "has_valid_voluntary_alignment": [True, False],
        }).lazy()
        current, _historical = calculate_current_and_historical_sources(df, "202403")
        assert current["current_alignment_source"][0] == "NO DATA"
        assert current["count"][0] == 0
