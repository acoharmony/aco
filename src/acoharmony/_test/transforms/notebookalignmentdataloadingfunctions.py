# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for data loading and preparation functions in consolidated_alignments notebook.

Tests the newly extracted idempotent functions:
- load_outreach_data
- load_consolidated_alignment_data
- calculate_basic_stats
- extract_year_months
- calculate_historical_program_distribution
- calculate_current_program_distribution
- prepare_voluntary_outreach_data
- enrich_with_outreach_data
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


class TestCalculateBasicStats:
    """Tests for calculate_basic_stats function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003"],
            "bene_zip": ["75001", "77001", "78701"],
            "ever_reach": [True, False, True],
            "ever_mssp": [False, True, False],
        })

    @pytest.mark.unit
    def test_returns_dict_with_correct_keys(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_basic_stats"]
        result = func(sample_data, pl)

        assert isinstance(result, dict)
        assert "total_records" in result
        assert "total_columns" in result

    @pytest.mark.unit
    def test_correct_record_count(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_basic_stats"]
        result = func(sample_data, pl)

        assert result["total_records"] == 3

    @pytest.mark.unit
    def test_correct_column_count(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_basic_stats"]
        result = func(sample_data, pl)

        assert result["total_columns"] == 4

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_basic_stats"]
        result1 = func(sample_data, pl)
        result2 = func(sample_data, pl)

        assert result1 == result2


class TestExtractYearMonths:
    """Tests for extract_year_months function."""

    @pytest.mark.unit
    def test_extracts_year_months_from_columns(self, notebook_defs):
        func = notebook_defs["extract_year_months"]

        df = pl.LazyFrame({
            "current_mbi": ["M001"],
            "ym_202401_reach": [True],
            "ym_202401_mssp": [False],
            "ym_202402_reach": [True],
            "ym_202402_mssp": [False],
        })

        most_recent, year_months = func(df)

        assert most_recent == "202402"
        assert year_months == ["202401", "202402"]

    @pytest.mark.unit
    def test_handles_no_ym_columns(self, notebook_defs):
        func = notebook_defs["extract_year_months"]

        df = pl.LazyFrame({
            "current_mbi": ["M001"],
            "ever_reach": [True],
        })

        most_recent, year_months = func(df)

        assert most_recent is None
        assert year_months == []

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs):
        func = notebook_defs["extract_year_months"]

        df = pl.LazyFrame({
            "current_mbi": ["M001"],
            "ym_202401_reach": [True],
            "ym_202402_reach": [True],
        })

        result1 = func(df)
        result2 = func(df)

        assert result1 == result2


class TestCalculateHistoricalProgramDistribution:
    """Tests for calculate_historical_program_distribution function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004", "M005"],
            "ever_reach": [True, False, True, False, True],
            "ever_mssp": [False, True, True, False, False],
        })

    @pytest.mark.unit
    def test_returns_dataframe(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_historical_program_distribution"]
        result = func(sample_data, pl)

        assert isinstance(result, pl.DataFrame)

    @pytest.mark.unit
    def test_has_correct_columns(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_historical_program_distribution"]
        result = func(sample_data, pl)

        assert "ever_reach_count" in result.columns
        assert "ever_mssp_count" in result.columns
        assert "ever_both_count" in result.columns
        assert "never_aligned_count" in result.columns

    @pytest.mark.unit
    def test_correct_counts(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_historical_program_distribution"]
        result = func(sample_data, pl)

        assert result["ever_reach_count"][0] == 3  # M001, M003, M005
        assert result["ever_mssp_count"][0] == 2   # M002, M003
        assert result["ever_both_count"][0] == 1   # M003
        assert result["never_aligned_count"][0] == 1  # M004

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_historical_program_distribution"]
        result1 = func(sample_data, pl)
        result2 = func(sample_data, pl)

        assert result1.equals(result2)


class TestCalculateCurrentProgramDistribution:
    """Tests for calculate_current_program_distribution function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004", "M005"],
            "ym_202401_reach": [True, False, True, False, True],
            "ym_202401_mssp": [False, True, True, False, False],
        })

    @pytest.mark.unit
    def test_returns_dataframe(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_current_program_distribution"]
        result = func(sample_data, "202401", pl)

        assert isinstance(result, pl.DataFrame)

    @pytest.mark.unit
    def test_has_correct_columns(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_current_program_distribution"]
        result = func(sample_data, "202401", pl)

        assert "currently_reach" in result.columns
        assert "currently_mssp" in result.columns
        assert "currently_both" in result.columns
        assert "currently_neither" in result.columns

    @pytest.mark.unit
    def test_correct_counts(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_current_program_distribution"]
        result = func(sample_data, "202401", pl)

        assert result["currently_reach"][0] == 3   # M001, M003, M005
        assert result["currently_mssp"][0] == 2    # M002, M003
        assert result["currently_both"][0] == 1    # M003
        assert result["currently_neither"][0] == 1 # M004

    @pytest.mark.unit
    def test_handles_no_yearmo(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_current_program_distribution"]
        result = func(sample_data, None, pl)

        # Should return zeros
        assert result["currently_reach"][0] == 0
        assert result["currently_mssp"][0] == 0

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_current_program_distribution"]
        result1 = func(sample_data, "202401", pl)
        result2 = func(sample_data, "202401", pl)

        assert result1.equals(result2)


class TestPrepareVoluntaryOutreachData:
    """Tests for prepare_voluntary_outreach_data function."""

    @pytest.fixture
    def sample_emails(self):
        return pl.LazyFrame({
            "mbi": ["M001", "M001", "M002"],
            "campaign": [
                "2024 Q1 ACO Voluntary Alignment",
                "2024 Q2 ACO Voluntary Alignment",
                "2024 Q1 ACO Voluntary Alignment"
            ],
            "send_datetime": ["2024-01-15", "2024-04-15", "2024-01-20"],
            "status": ["sent", "sent", "sent"],
            "has_been_opened": ["true", "false", "true"],
            "has_been_clicked": ["true", "false", "false"],
        })

    @pytest.fixture
    def sample_mailed(self):
        return pl.LazyFrame({
            "mbi": ["M001", "M003"],
            "campaign_name": [
                "2024 Q1 ACO Voluntary Alignment",
                "2024 Q2 ACO Voluntary Alignment"
            ],
            "send_datetime": ["2024-01-10", "2024-04-10"],
            "status": ["delivered", "delivered"],
        })

    @pytest.mark.unit
    def test_returns_four_lazyframes(self, notebook_defs, sample_emails, sample_mailed):
        func = notebook_defs["prepare_voluntary_outreach_data"]
        result = func(sample_emails, sample_mailed, pl)

        assert isinstance(result, tuple)
        assert len(result) == 4

    @pytest.mark.unit
    def test_email_mbis_aggregation(self, notebook_defs, sample_emails, sample_mailed):
        func = notebook_defs["prepare_voluntary_outreach_data"]
        email_by_campaign, email_mbis, mailed_by_campaign, mailed_mbis = func(
            sample_emails, sample_mailed, pl
        )

        email_mbis_collected = email_mbis.collect()

        # M001 should have 2 emails
        m001 = email_mbis_collected.filter(pl.col("mbi") == "M001")
        assert len(m001) == 1
        assert m001["voluntary_email_count"][0] == 2

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_emails, sample_mailed):
        func = notebook_defs["prepare_voluntary_outreach_data"]

        result1 = func(sample_emails, sample_mailed, pl)
        result2 = func(sample_emails, sample_mailed, pl)

        # Compare collected results - sort by mbi for consistent ordering
        assert result1[1].collect().sort("mbi").equals(result2[1].collect().sort("mbi"))


class TestEnrichWithOutreachData:
    """Tests for enrich_with_outreach_data function."""

    @pytest.fixture
    def sample_df(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003"],
            "ever_reach": [True, False, True],
        })

    @pytest.fixture
    def sample_email_mbis(self):
        return pl.LazyFrame({
            "mbi": ["M001", "M002"],
            "voluntary_email_count": [2, 1],
            "voluntary_email_campaigns": [2, 1],
            "email_campaign_periods": ["2024_Q1, 2024_Q2", "2024_Q1"],
            "voluntary_emails_opened": [1, 0],
            "voluntary_emails_clicked": [1, 0],
            "last_voluntary_email_date": ["2024-04-15", "2024-01-20"],
        })

    @pytest.fixture
    def sample_mailed_mbis(self):
        return pl.LazyFrame({
            "mbi": ["M001"],
            "voluntary_letter_count": [1],
            "voluntary_letter_campaigns": [1],
            "letter_campaign_periods": ["2024_Q1"],
            "last_voluntary_letter_date": ["2024-01-10"],
        })

    @pytest.mark.unit
    def test_returns_lazyframe(self, notebook_defs, sample_df, sample_email_mbis, sample_mailed_mbis):
        func = notebook_defs["enrich_with_outreach_data"]
        result = func(sample_df, sample_email_mbis, sample_mailed_mbis, pl)

        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_adds_outreach_columns(self, notebook_defs, sample_df, sample_email_mbis, sample_mailed_mbis):
        func = notebook_defs["enrich_with_outreach_data"]
        result = func(sample_df, sample_email_mbis, sample_mailed_mbis, pl)

        result_collected = result.collect()

        assert "voluntary_outreach_attempts" in result_collected.columns
        assert "has_voluntary_outreach" in result_collected.columns
        assert "voluntary_outreach_type" in result_collected.columns
        assert "voluntary_engagement_level" in result_collected.columns

    @pytest.mark.unit
    def test_correct_outreach_counts(self, notebook_defs, sample_df, sample_email_mbis, sample_mailed_mbis):
        func = notebook_defs["enrich_with_outreach_data"]
        result = func(sample_df, sample_email_mbis, sample_mailed_mbis, pl).collect()

        # M001 has 2 emails + 1 letter = 3 total
        m001 = result.filter(pl.col("current_mbi") == "M001")
        assert m001["voluntary_outreach_attempts"][0] == 3
        assert m001["has_voluntary_outreach"][0]
        assert m001["voluntary_outreach_type"][0] == "Email & Letter"

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_df, sample_email_mbis, sample_mailed_mbis):
        func = notebook_defs["enrich_with_outreach_data"]

        result1 = func(sample_df, sample_email_mbis, sample_mailed_mbis, pl).collect()
        result2 = func(sample_df, sample_email_mbis, sample_mailed_mbis, pl).collect()

        assert result1.equals(result2)


class TestIdempotencyComprehensive:
    """Comprehensive idempotency tests for all data loading/prep functions."""

    @pytest.fixture
    def comprehensive_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004"],
            "ym_202401_reach": [True, False, True, False],
            "ym_202401_mssp": [False, True, True, False],
            "ever_reach": [True, True, True, False],
            "ever_mssp": [False, True, True, False],
        })

    @pytest.mark.unit
    def test_all_functions_idempotent(self, notebook_defs, comprehensive_data):
        """All data prep functions return identical results when called multiple times."""

        # Test calculate_basic_stats
        func = notebook_defs["calculate_basic_stats"]
        assert func(comprehensive_data, pl) == func(comprehensive_data, pl)

        # Test extract_year_months
        func = notebook_defs["extract_year_months"]
        assert func(comprehensive_data) == func(comprehensive_data)

        # Test calculate_historical_program_distribution
        func = notebook_defs["calculate_historical_program_distribution"]
        r1 = func(comprehensive_data, pl)
        r2 = func(comprehensive_data, pl)
        assert r1.equals(r2)

        # Test calculate_current_program_distribution
        func = notebook_defs["calculate_current_program_distribution"]
        r1 = func(comprehensive_data, "202401", pl)
        r2 = func(comprehensive_data, "202401", pl)
        assert r1.equals(r2)
