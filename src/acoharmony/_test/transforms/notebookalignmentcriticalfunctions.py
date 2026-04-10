# © 2025 HarmonyCares
# All rights reserved.

"""
Comprehensive unit tests for CRITICAL and HIGH priority functions in consolidated_alignments notebook.

This test suite covers all newly extracted calculation functions:
- calculate_voluntary_alignment_stats (CRITICAL)
- analyze_outreach_effectiveness (CRITICAL)
- calculate_quarterly_campaign_effectiveness (CRITICAL)
- calculate_alignment_trends_over_time (CRITICAL)
- calculate_enhanced_campaign_performance (CRITICAL)
- analyze_sva_action_categories (HIGH)
- calculate_enrollment_stats_for_selected_month (HIGH)
- get_sample_data (LOW)
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import sys
from pathlib import Path

import polars as pl
import pytest
import acoharmony

# Add bundled test-fixture notebooks directory to path so we can import
# `consolidated_alignments` (a marimo notebook) for behavioral tests.
sys.path.insert(
    0,
    str(Path(__file__).resolve().parent.parent / "_fixtures" / "notebooks"),
)

# Import the notebook module

try:
    import consolidated_alignments
except ModuleNotFoundError:
    import pytest
    pytest.skip("consolidated_alignments notebook not on path", allow_module_level=True)


@pytest.fixture(scope="module")
def notebook_defs():
    """Run notebook once and cache definitions for all tests."""
    _, defs = consolidated_alignments.app.run()
    return defs


class TestCalculateVoluntaryAlignmentStats:
    """Tests for calculate_voluntary_alignment_stats function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004", "M005"],
            "ym_202401_reach": [True, True, False, False, True],
            "ym_202401_mssp": [False, False, True, True, False],
            "has_valid_voluntary_alignment": [True, False, False, True, False],
            "has_voluntary_alignment": [True, True, False, True, True],
            "has_voluntary_outreach": [True, False, True, False, True],
            "voluntary_outreach_attempts": [2, 0, 1, 0, 3],
            "sva_expiration_date": ["2025-12-31", None, None, "2024-06-30", "2026-01-15"],
            "voluntary_emails_opened": [2, 0, 1, 0, 2],
            "voluntary_emails_clicked": [1, 0, 0, 0, 1],
            "voluntary_email_count": [2, 0, 1, 0, 3],
            "voluntary_letter_count": [0, 0, 0, 0, 1],
        })

    @pytest.mark.unit
    def test_returns_dict_with_required_keys(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_voluntary_alignment_stats"]
        result = func(sample_data, "202401", pl)

        assert isinstance(result, dict)
        # Check some key metrics exist
        assert "currently_voluntary" in result
        assert "currently_claims" in result
        assert "reach_needs_renewal" in result
        assert "total_contacted" in result

    @pytest.mark.unit
    def test_handles_none_yearmo(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_voluntary_alignment_stats"]
        result = func(sample_data, None, pl)

        # Should return dict with zero/None values
        assert isinstance(result, dict)

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_voluntary_alignment_stats"]
        result1 = func(sample_data, "202401", pl)
        result2 = func(sample_data, "202401", pl)

        assert result1 == result2


class TestAnalyzeOutreachEffectiveness:
    """Tests for analyze_outreach_effectiveness function."""

    @pytest.fixture
    def enriched_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004"],
            "ym_202401_reach": [True, True, False, False],
            "ym_202401_mssp": [False, False, True, True],
            "has_valid_voluntary_alignment": [True, False, True, False],
            "has_voluntary_outreach": [True, False, True, True],
            "voluntary_email_count": [2, 0, 1, 0],
            "voluntary_letter_count": [0, 0, 1, 1],
            "voluntary_emails_opened": [1, 0, 1, 0],
            "voluntary_emails_clicked": [1, 0, 0, 0],
        })

    @pytest.mark.unit
    def test_returns_dict_with_metrics(self, notebook_defs, enriched_data):
        func = notebook_defs["analyze_outreach_effectiveness"]
        result = func(enriched_data, "202401", "202401", pl)

        assert isinstance(result, dict)
        assert "total_population" in result
        assert "total_contacted" in result
        assert "contacted_to_sva_rate" in result

    @pytest.mark.unit
    def test_handles_empty_dataset(self, notebook_defs):
        func = notebook_defs["analyze_outreach_effectiveness"]
        # Need to specify dtypes for empty dataframe
        empty_df = pl.LazyFrame({
            "current_mbi": pl.Series([], dtype=pl.String),
            "ym_202401_reach": pl.Series([], dtype=pl.Boolean),
            "ym_202401_mssp": pl.Series([], dtype=pl.Boolean),
            "has_valid_voluntary_alignment": pl.Series([], dtype=pl.Boolean),
            "has_voluntary_outreach": pl.Series([], dtype=pl.Boolean),
            "voluntary_email_count": pl.Series([], dtype=pl.Int64),
            "voluntary_letter_count": pl.Series([], dtype=pl.Int64),
            "voluntary_emails_opened": pl.Series([], dtype=pl.Int64),
            "voluntary_emails_clicked": pl.Series([], dtype=pl.Int64),
        })
        result = func(empty_df, "202401", "202401", pl)

        assert isinstance(result, dict)
        assert result["total_population"] == 0

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, enriched_data):
        func = notebook_defs["analyze_outreach_effectiveness"]
        result1 = func(enriched_data, "202401", "202401", pl)
        result2 = func(enriched_data, "202401", "202401", pl)

        assert result1 == result2


class TestCalculateQuarterlyCampaignEffectiveness:
    """Tests for calculate_quarterly_campaign_effectiveness function."""

    @pytest.fixture
    def enriched_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003"],
            "has_valid_voluntary_alignment": [True, False, True],
        })

    @pytest.fixture
    def email_campaign_data(self):
        return pl.LazyFrame({
            "mbi": ["M001", "M002"],
            "campaign_period": ["2024_Q1", "2024_Q1"],
            "emails_sent": [1, 1],
            "opened": [True, False],
            "clicked": [True, False],
        })

    @pytest.fixture
    def mail_campaign_data(self):
        return pl.LazyFrame({
            "mbi": ["M002", "M003"],
            "campaign_period": ["2024_Q1", "2024_Q1"],
            "letters_sent": [1, 1],
        })

    @pytest.mark.unit
    def test_returns_dataframe(self, notebook_defs, enriched_data, email_campaign_data, mail_campaign_data):
        func = notebook_defs["calculate_quarterly_campaign_effectiveness"]
        result = func(enriched_data, email_campaign_data, mail_campaign_data, pl)

        assert isinstance(result, pl.DataFrame)
        assert "campaign_period" in result.columns
        assert "total_contacted" in result.columns

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, enriched_data, email_campaign_data, mail_campaign_data):
        func = notebook_defs["calculate_quarterly_campaign_effectiveness"]
        result1 = func(enriched_data, email_campaign_data, mail_campaign_data, pl)
        result2 = func(enriched_data, email_campaign_data, mail_campaign_data, pl)

        assert result1.equals(result2)


class TestCalculateAlignmentTrendsOverTime:
    """Tests for calculate_alignment_trends_over_time function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003"],
            "ym_202401_reach": [True, False, True],
            "ym_202401_mssp": [False, True, False],
            "ym_202402_reach": [True, False, False],
            "ym_202402_mssp": [False, True, True],
        })

    @pytest.mark.unit
    def test_returns_dataframe_with_trends(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_alignment_trends_over_time"]
        result = func(sample_data, ["202401", "202402"], pl)

        assert isinstance(result, pl.DataFrame)
        assert "year_month" in result.columns
        assert "REACH" in result.columns
        assert "MSSP" in result.columns
        assert len(result) == 2

    @pytest.mark.unit
    def test_handles_empty_year_months(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_alignment_trends_over_time"]
        result = func(sample_data, [], pl)

        assert result is None

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_alignment_trends_over_time"]
        result1 = func(sample_data, ["202401", "202402"], pl)
        result2 = func(sample_data, ["202401", "202402"], pl)

        assert result1.equals(result2)


class TestCalculateEnhancedCampaignPerformance:
    """Tests for calculate_enhanced_campaign_performance function."""

    @pytest.fixture
    def emails_df(self):
        return pl.LazyFrame({
            "mbi": ["M001", "M002", "M003"],
            "campaign": ["2024 ACO Voluntary Alignment", "2024 ACO Voluntary Alignment", "Other Campaign"],
            "status": ["Delivered", "Bounced", "Delivered"],
            "has_been_opened": ["true", "false", "true"],
            "has_been_clicked": ["true", "false", "false"],
        })

    @pytest.fixture
    def mailed_df(self):
        return pl.LazyFrame({
            "mbi": ["M001", "M002"],
            "campaign_name": ["2024 ACO Voluntary Alignment", "2024 ACO Voluntary Alignment"],
            "status": ["Delivered", "Failed"],
        })

    @pytest.mark.unit
    def test_returns_dict_with_email_and_mail_stats(self, notebook_defs, emails_df, mailed_df):
        func = notebook_defs["calculate_enhanced_campaign_performance"]
        result = func(emails_df, mailed_df, pl)

        assert isinstance(result, dict)
        assert "email" in result
        assert "mail" in result
        assert "total_sent" in result["email"]
        assert "delivery_rate" in result["email"]

    @pytest.mark.unit
    def test_email_metrics_calculated_correctly(self, notebook_defs, emails_df, mailed_df):
        func = notebook_defs["calculate_enhanced_campaign_performance"]
        result = func(emails_df, mailed_df, pl)

        # 2 emails with "ACO Voluntary Alignment" campaign
        assert result["email"]["total_sent"] == 2
        # 1 delivered (M001), 1 bounced (M002)
        assert result["email"]["delivered"] == 1

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, emails_df, mailed_df):
        func = notebook_defs["calculate_enhanced_campaign_performance"]
        result1 = func(emails_df, mailed_df, pl)
        result2 = func(emails_df, mailed_df, pl)

        assert result1 == result2


class TestAnalyzeSvaActionCategories:
    """Tests for analyze_sva_action_categories function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004", "M005"],
            "sva_action_needed": ["Renewal", "Renewal", "New Signature", "None", "Renewal"],
        })

    @pytest.mark.unit
    def test_returns_dataframe_with_counts(self, notebook_defs, sample_data):
        func = notebook_defs["analyze_sva_action_categories"]
        result = func(sample_data, pl)

        assert isinstance(result, pl.DataFrame)
        assert "sva_action_needed" in result.columns
        assert "count" in result.columns

    @pytest.mark.unit
    def test_counts_correct(self, notebook_defs, sample_data):
        func = notebook_defs["analyze_sva_action_categories"]
        result = func(sample_data, pl)

        # Should have 3 "Renewal", 1 "New Signature", 1 "None"
        renewal_count = result.filter(pl.col("sva_action_needed") == "Renewal")["count"][0]
        assert renewal_count == 3

    @pytest.mark.unit
    def test_sorted_descending(self, notebook_defs, sample_data):
        func = notebook_defs["analyze_sva_action_categories"]
        result = func(sample_data, pl)

        # First row should have highest count
        counts = result["count"].to_list()
        assert counts == sorted(counts, reverse=True)

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["analyze_sva_action_categories"]
        result1 = func(sample_data, pl).sort("sva_action_needed")
        result2 = func(sample_data, pl).sort("sva_action_needed")

        # Sort both by category name to ensure consistent order for comparison
        assert result1.equals(result2)


class TestCalculateEnrollmentStatsForSelectedMonth:
    """Tests for calculate_enrollment_stats_for_selected_month function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004", "M005"],
            "ym_202401_reach": [True, False, True, False, False],
            "ym_202401_mssp": [False, True, False, True, False],
            "ym_202401_ffs": [False, False, False, False, True],
        })

    @pytest.mark.unit
    def test_returns_dict_with_stats(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_enrollment_stats_for_selected_month"]
        result = func(sample_data, "202401", pl)

        assert isinstance(result, dict)
        assert "REACH" in result
        assert "MSSP" in result
        assert "FFS" in result
        assert "Not Enrolled" in result

    @pytest.mark.unit
    def test_counts_correct(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_enrollment_stats_for_selected_month"]
        result = func(sample_data, "202401", pl)

        assert result["REACH"] == 2  # M001, M003
        assert result["MSSP"] == 2   # M002, M004
        assert result["FFS"] == 1    # M005
        assert result["Not Enrolled"] == 0  # All 5 accounted for

    @pytest.mark.unit
    def test_handles_none_yearmo(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_enrollment_stats_for_selected_month"]
        result = func(sample_data, None, pl)

        assert result is None

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_enrollment_stats_for_selected_month"]
        result1 = func(sample_data, "202401", pl)
        result2 = func(sample_data, "202401", pl)

        assert result1 == result2


class TestGetSampleData:
    """Tests for get_sample_data function."""

    @pytest.fixture
    def enriched_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004", "M005"],
            "consolidated_program": ["REACH", "MSSP", "BOTH", "NONE", "REACH"],
            "has_voluntary_alignment": [True, False, True, False, True],
            "months_in_reach": [12, 0, 6, 0, 18],
            "office_location": ["Dallas", "Houston", "Dallas", None, "Austin"],
        })

    @pytest.mark.unit
    def test_returns_dataframe(self, notebook_defs, enriched_data):
        func = notebook_defs["get_sample_data"]
        result = func(enriched_data, 3, pl)

        assert isinstance(result, pl.DataFrame)

    @pytest.mark.unit
    def test_respects_sample_size(self, notebook_defs, enriched_data):
        func = notebook_defs["get_sample_data"]
        result = func(enriched_data, 3, pl)

        assert len(result) == 3

    @pytest.mark.unit
    def test_filters_to_existing_columns(self, notebook_defs, enriched_data):
        func = notebook_defs["get_sample_data"]
        result = func(enriched_data, 5, pl)

        # Should only include columns that exist in the data
        assert "current_mbi" in result.columns
        assert "consolidated_program" in result.columns

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, enriched_data):
        func = notebook_defs["get_sample_data"]
        result1 = func(enriched_data, 3, pl)
        result2 = func(enriched_data, 3, pl)

        assert result1.equals(result2)
