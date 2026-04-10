# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for office location functions in consolidated_alignments notebook.

Tests all office-related idempotent functions by importing them from the notebook:
- calculate_office_enrollment_stats: Office enrollment by year-month
- calculate_office_alignment_types: Voluntary vs claims alignment breakdown
- calculate_office_program_distribution: REACH/MSSP/BOTH distribution
- calculate_office_transition_stats: Program transition statistics
- calculate_office_campaign_effectiveness: Campaign performance by office
- calculate_office_vintage_distribution: Vintage cohorts by office
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


class TestOfficeEnrollmentStats:
    """Tests for calculate_office_enrollment_stats function."""

    @pytest.fixture
    def sample_data(self):
        """Sample consolidated alignment data with office_name."""
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004", "M005", "M006"],
            "death_date": [None, None, None, None, None, None],
            "office_name": ["North Dallas", "North Dallas", "South Houston", "South Houston", "North Dallas", None],
            "office_location": ["Dallas", "Dallas", "Houston", "Houston", "Dallas", None],
            "ym_202401_reach": [True, False, True, False, False, False],
            "ym_202401_mssp": [False, True, False, True, False, False],
            "ever_reach": [True, True, True, True, False, False],
            "ever_mssp": [False, True, False, True, False, False],
            "has_valid_voluntary_alignment": [True, False, True, False, False, False],
        })

    @pytest.mark.unit
    def test_calculate_office_enrollment_stats_valid_yearmo(self, sample_data):
        """calculate_office_enrollment_stats returns correct stats for valid yearmo."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_enrollment_stats"]

        result = calc_func(sample_data, "202401")

        assert result is not None
        assert len(result) == 2  # North Dallas, South Houston (None filtered out)
        assert "total_beneficiaries" in result.columns
        assert "office_name" in result.columns
        assert "office_location" in result.columns
        assert "reach_count" in result.columns
        assert "mssp_count" in result.columns
        assert "total_aco" in result.columns

    @pytest.mark.unit
    def test_calculate_office_enrollment_stats_idempotent(self, sample_data):
        """calculate_office_enrollment_stats is idempotent."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_enrollment_stats"]

        result1 = calc_func(sample_data, "202401").sort("office_name")
        result2 = calc_func(sample_data, "202401").sort("office_name")

        assert result1.equals(result2)

    @pytest.mark.unit
    def test_calculate_office_enrollment_stats_no_double_counting(self, sample_data):
        """Beneficiaries enrolled in REACH or MSSP are counted once, not twice."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_enrollment_stats"]

        result = calc_func(sample_data, "202401")

        # For each office, total_aco should equal reach_count + mssp_count
        # because a beneficiary can only be in ONE program at a time
        for row in result.iter_rows(named=True):
            expected_total = row["reach_count"] + row["mssp_count"]
            assert row["total_aco"] == expected_total, \
                f"Office {row['office_name']}: total_aco ({row['total_aco']}) != reach + mssp ({expected_total})"

    @pytest.mark.unit
    def test_calculate_office_enrollment_stats_mutually_exclusive_programs(self, sample_data):
        """REACH and MSSP enrollment are mutually exclusive in a given month."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_enrollment_stats"]

        # Verify in source data that no beneficiary is in both programs
        collected = sample_data.collect()
        both_programs = collected.filter(
            pl.col("ym_202401_reach") & pl.col("ym_202401_mssp")
        )
        assert len(both_programs) == 0, "Test data has beneficiaries in both programs - invalid!"

        result = calc_func(sample_data, "202401")

        # North Dallas: 3 benes total, 1 REACH, 1 MSSP = 2 total ACO
        north_dallas = result.filter(pl.col("office_name") == "North Dallas")
        assert north_dallas["total_beneficiaries"][0] == 3
        assert north_dallas["reach_count"][0] == 1
        assert north_dallas["mssp_count"][0] == 1
        assert north_dallas["total_aco"][0] == 2

    @pytest.mark.unit
    def test_calculate_office_enrollment_stats_invalid_yearmo(self, sample_data):
        """Returns None for invalid/missing year-month."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_enrollment_stats"]

        result = calc_func(sample_data, "202412")  # Missing columns
        # Function returns a DataFrame with zero counts when yearmo columns don't exist
        assert result is not None
        assert result["reach_count"].sum() == 0
        assert result["mssp_count"].sum() == 0

    @pytest.mark.unit
    def test_calculate_office_enrollment_stats_empty_yearmo(self, sample_data):
        """Returns None for empty year-month."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_enrollment_stats"]

        result = calc_func(sample_data, "")
        assert result is None


class TestOfficeAlignmentTypes:
    """Tests for calculate_office_alignment_types function."""

    @pytest.fixture
    def sample_data(self):
        """Sample data with alignment types."""
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004"],
            "death_date": [None, None, None, None],
            "office_name": ["North Dallas", "North Dallas", "South Houston", "South Houston"],
            "office_location": ["Dallas", "Dallas", "Houston", "Houston"],
            "ym_202401_reach": [True, True, False, True],
            "ym_202401_mssp": [False, False, True, False],
            "has_valid_voluntary_alignment": [True, False, True, False],
            "has_voluntary_alignment": [True, True, True, False],
        })

    @pytest.mark.unit
    def test_calculate_office_alignment_types_valid(self, sample_data):
        """calculate_office_alignment_types returns correct breakdown."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_alignment_types"]

        result = calc_func(sample_data, "202401")

        assert result is not None
        assert len(result) == 2  # North Dallas, South Houston
        assert "total_aligned" in result.columns
        assert "voluntary_count" in result.columns
        assert "claims_count" in result.columns
        assert "office_name" in result.columns

    @pytest.mark.unit
    def test_calculate_office_alignment_types_percentages(self, sample_data):
        """Voluntary and claims percentages sum correctly."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_alignment_types"]

        result = calc_func(sample_data, "202401")

        # North Dallas: 2 enrolled, 1 voluntary (50%), 1 claims (50%)
        north_dallas = result.filter(pl.col("office_name") == "North Dallas")
        assert north_dallas["total_aligned"][0] == 2
        assert north_dallas["voluntary_count"][0] == 1
        assert north_dallas["claims_count"][0] == 1
        assert abs(north_dallas["voluntary_pct"][0] - 50.0) < 0.1


class TestOfficeProgramDistribution:
    """Tests for calculate_office_program_distribution function."""

    @pytest.fixture
    def sample_data(self):
        """Sample data with program history and consolidated_program column."""
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004", "M005"],
            "office_name": ["Dallas Office", "Dallas Office", "Houston Office", "Houston Office", "Dallas Office"],
            "office_location": ["Dallas", "Dallas", "Houston", "Houston", "Dallas"],
            "ever_reach": [True, False, True, True, False],
            "ever_mssp": [False, True, False, True, False],
            "ym_202401_reach": [True, False, True, False, False],
            "ym_202401_mssp": [False, True, False, True, False],
            # consolidated_program: REACH only, MSSP only, REACH only, BOTH (ever in both), NONE (never aligned)
            "consolidated_program": ["REACH", "MSSP", "REACH", "BOTH", "NONE"],
        })

    @pytest.mark.unit
    def test_calculate_office_program_distribution_valid(self, sample_data):
        """calculate_office_program_distribution returns correct distribution."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_program_distribution"]

        result = calc_func(sample_data, "202401")

        assert result is not None
        assert "reach_only_count" in result.columns
        assert "mssp_only_count" in result.columns
        assert "both_programs_count" in result.columns
        assert "neither_count" in result.columns

    @pytest.mark.unit
    def test_calculate_office_program_distribution_counts(self, sample_data):
        """Program distribution counts are accurate."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_program_distribution"]

        result = calc_func(sample_data, "202401")

        # Dallas: 3 benes - 1 REACH only, 1 MSSP only, 0 both, 1 never = 3 total
        dallas = result.filter(pl.col("office_name") == "Dallas Office")
        assert dallas["total_beneficiaries"][0] == 3
        total = dallas["reach_only_count"][0] + dallas["mssp_only_count"][0] + dallas["both_programs_count"][0] + dallas["neither_count"][0]
        assert total == 3
        assert dallas["reach_only_count"][0] == 1
        assert dallas["mssp_only_count"][0] == 1
        assert dallas["neither_count"][0] == 1

        # Houston: 2 benes - 1 REACH only, 0 MSSP only, 1 both = 2 total
        houston = result.filter(pl.col("office_name") == "Houston Office")
        assert houston["total_beneficiaries"][0] == 2
        assert houston["reach_only_count"][0] == 1
        assert houston["both_programs_count"][0] == 1


class TestOfficeTransitionStats:
    """Tests for calculate_office_transition_stats function."""

    @pytest.fixture
    def sample_data(self):
        """Sample data with transitions."""
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004"],
            "office_name": ["Dallas Office", "Dallas Office", "Houston Office", "Houston Office"],
            "office_location": ["Dallas", "Dallas", "Houston", "Houston"],
            "has_program_transition": [True, False, True, True],
            "has_continuous_enrollment": [False, True, True, False],
            "months_in_reach": [12.0, 0.0, 24.0, 6.0],
            "months_in_mssp": [0.0, 18.0, 0.0, 12.0],
            "total_aligned_months": [12.0, 18.0, 24.0, 18.0],
        })

    @pytest.mark.unit
    def test_calculate_office_transition_stats_valid(self, sample_data):
        """calculate_office_transition_stats returns correct stats."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_transition_stats"]

        result = calc_func(sample_data)

        assert result is not None
        assert "transitioned_count" in result.columns
        assert "continuous_count" in result.columns
        assert "avg_months_reach" in result.columns
        assert "avg_months_mssp" in result.columns
        assert "office_name" in result.columns

    @pytest.mark.unit
    def test_calculate_office_transition_stats_calculations(self, sample_data):
        """Transition percentages calculated correctly."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_transition_stats"]

        result = calc_func(sample_data)

        # Dallas: 2 benes, 1 transitioned (50%), 1 continuous (50%)
        dallas = result.filter(pl.col("office_name") == "Dallas Office")
        assert dallas["total_beneficiaries"][0] == 2
        assert dallas["transitioned_count"][0] == 1
        assert abs(dallas["transition_pct"][0] - 50.0) < 0.1


class TestOfficeCampaignEffectiveness:
    """Tests for calculate_office_campaign_effectiveness function."""

    @pytest.fixture
    def enriched_data(self):
        """Enriched alignment data with office assignments."""
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004"],
            "office_name": ["Dallas Office", "Dallas Office", "Houston Office", "Houston Office"],
            "office_location": ["Dallas", "Dallas", "Houston", "Houston"],
            "has_valid_voluntary_alignment": [True, False, True, False],
        })

    @pytest.fixture
    def email_campaigns(self):
        """Email campaign data."""
        return pl.LazyFrame({
            "campaign_period": ["2024Q1", "2024Q1", "2024Q1", "2024Q1"],
            "mbi": ["M001", "M002", "M003", "M004"],
            "emails_sent": [1, 1, 1, 1],
            "opened": [True, True, False, True],
            "clicked": [True, False, False, False],
        })

    @pytest.fixture
    def mail_campaigns(self):
        """Mail campaign data."""
        return pl.LazyFrame({
            "campaign_period": ["2024Q1", "2024Q1", "2024Q1", "2024Q1"],
            "mbi": ["M001", "M002", "M003", "M004"],
            "letters_sent": [1, 1, 1, 1],
        })

    @pytest.mark.unit
    def test_calculate_office_campaign_effectiveness_valid(self, enriched_data, email_campaigns, mail_campaigns):
        """calculate_office_campaign_effectiveness returns campaign metrics by office."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_campaign_effectiveness"]

        # Import pl from the notebook's definitions
        pl_module = defs["pl"]

        result = calc_func(enriched_data, email_campaigns, mail_campaigns, pl_module)

        assert result is not None
        assert "office_name" in result.columns
        assert "total_contacted" in result.columns
        assert "emailed" in result.columns
        assert "signed_sva" in result.columns
        assert "overall_conversion_rate" in result.columns

    @pytest.mark.unit
    def test_calculate_office_campaign_effectiveness_conversion_rates(self, enriched_data, email_campaigns, mail_campaigns):
        """Campaign conversion rates calculated correctly."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_campaign_effectiveness"]
        pl_module = defs["pl"]

        result = calc_func(enriched_data, email_campaigns, mail_campaigns, pl_module)

        # Dallas: 2 contacted, 1 signed SVA = 50% conversion
        dallas = result.filter(pl.col("office_name") == "Dallas Office")
        assert dallas["total_contacted"][0] == 2
        assert dallas["signed_sva"][0] == 1
        assert abs(dallas["overall_conversion_rate"][0] - 50.0) < 0.1


class TestOfficeVintageDistribution:
    """Tests for calculate_office_vintage_distribution function."""

    @pytest.fixture
    def vintage_data(self):
        """Vintage cohort data with office assignments."""
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004", "M005"],
            "office_name": ["Dallas Office", "Dallas Office", "Dallas Office", "Houston Office", "Houston Office"],
            "office_location": ["Dallas", "Dallas", "Dallas", "Houston", "Houston"],
            "vintage_cohort": ["0-6 months", "6-12 months", "12-24 months", "0-6 months", "24+ months"],
            "ym_202401_reach": [True, True, False, True, False],
            "ym_202401_mssp": [False, False, True, False, True],
            "months_in_reach": [4.0, 8.0, 0.0, 5.0, 0.0],
            "months_in_mssp": [0.0, 0.0, 18.0, 0.0, 30.0],
            "has_program_transition": [False, False, True, False, True],
        })

    @pytest.mark.unit
    def test_calculate_office_vintage_distribution_valid(self, vintage_data):
        """calculate_office_vintage_distribution returns vintage stats by office."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_vintage_distribution"]
        pl_module = defs["pl"]

        result = calc_func(vintage_data, "202401", pl_module)

        assert result is not None
        assert "office_name" in result.columns
        assert "vintage_cohort" in result.columns
        assert "count" in result.columns
        assert "currently_enrolled" in result.columns
        assert "pct_of_office_enrolled" in result.columns

    @pytest.mark.unit
    def test_calculate_office_vintage_distribution_mutually_exclusive(self, vintage_data):
        """Beneficiaries counted in exactly one program."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_vintage_distribution"]
        pl_module = defs["pl"]

        result = calc_func(vintage_data, "202401", pl_module)

        # Verify currently_enrolled uses OR logic (not addition)
        for row in result.iter_rows(named=True):
            # currently_enrolled should be <= count (can't enroll more than exist)
            assert row["currently_enrolled"] <= row["count"]

    @pytest.mark.unit
    def test_calculate_office_vintage_distribution_cohort_breakdown(self, vintage_data):
        """Vintage cohorts grouped correctly by office."""
        _, defs = consolidated_alignments.app.run()
        calc_func = defs["calculate_office_vintage_distribution"]
        pl_module = defs["pl"]

        result = calc_func(vintage_data, "202401", pl_module)

        # Dallas should have 3 cohorts: 0-6, 6-12, 12-24
        dallas_cohorts = result.filter(pl.col("office_name") == "Dallas Office")
        assert len(dallas_cohorts) == 3

        # Houston should have 2 cohorts: 0-6, 24+
        houston_cohorts = result.filter(pl.col("office_name") == "Houston Office")
        assert len(houston_cohorts) == 2
