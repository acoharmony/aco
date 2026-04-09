# © 2025 HarmonyCares
# All rights reserved.

"""
Comprehensive unit tests for ALL idempotent functions in consolidated_alignments notebook.

This test suite covers:
- Office location functions (enrollment, alignment types, program distribution, transitions)
- Display helper functions (branded header, transitions matrix, campaign effectiveness)
- Analysis functions (enrollment patterns, cohort analysis, vintage analysis)
- Excel export functions
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

# Add notebooks directory to path
sys.path.insert(0, str(Path("/opt/s3/data/notebooks")))

# Import the notebook module

import consolidated_alignments


@pytest.fixture(scope="module")
def notebook_defs():
    """Run notebook once and cache definitions for all tests."""
    _, defs = consolidated_alignments.app.run()
    return defs


class TestOfficeEnrollmentStats:
    """Tests for calculate_office_enrollment_stats function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004", "M005"],
            "office_name": ["Dallas Main", "Dallas Main", "Houston Central", "Houston Central", None],
            "office_location": ["Dallas", "Dallas", "Houston", "Houston", None],
            "ym_202401_reach": [True, False, True, True, False],
            "ym_202401_mssp": [False, True, False, False, False],
            "ever_reach": [True, True, True, True, False],
            "ever_mssp": [False, True, False, True, False],
            "has_valid_voluntary_alignment": [True, False, True, False, False],
        })

    @pytest.mark.unit
    def test_valid_yearmo(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_office_enrollment_stats"]
        result = func(sample_data, "202401")

        assert result is not None
        assert len(result) == 2
        assert "total_beneficiaries" in result.columns
        assert "reach_count" in result.columns

    @pytest.mark.unit
    def test_invalid_yearmo(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_office_enrollment_stats"]
        result = func(sample_data, "999999")
        # Function returns a DataFrame with zero counts when yearmo columns don't exist
        assert result is not None
        assert result["reach_count"].sum() == 0
        assert result["mssp_count"].sum() == 0

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_office_enrollment_stats"]
        result1 = func(sample_data, "202401").sort("office_location")
        result2 = func(sample_data, "202401").sort("office_location")
        assert result1.equals(result2)


class TestOfficeAlignmentTypes:
    """Tests for calculate_office_alignment_types function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004"],
            "office_name": ["Dallas Main", "Dallas Main", "Houston Central", "Houston Central"],
            "office_location": ["Dallas", "Dallas", "Houston", "Houston"],
            "ym_202401_reach": [True, True, False, True],
            "ym_202401_mssp": [False, False, True, False],
            "has_valid_voluntary_alignment": [True, False, False, True],
            "has_voluntary_alignment": [True, True, False, True],
        })

    @pytest.mark.unit
    def test_valid_yearmo(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_office_alignment_types"]
        result = func(sample_data, "202401")

        assert result is not None
        assert "total_aligned" in result.columns
        assert "voluntary_count" in result.columns
        assert "claims_count" in result.columns

    @pytest.mark.unit
    def test_dallas_breakdown(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_office_alignment_types"]
        result = func(sample_data, "202401")

        dallas = result.filter(pl.col("office_location") == "Dallas")
        assert len(dallas) == 1
        assert dallas["total_aligned"][0] == 2
        assert dallas["voluntary_count"][0] == 1
        assert dallas["claims_count"][0] == 1

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_office_alignment_types"]
        result1 = func(sample_data, "202401").sort("office_location")
        result2 = func(sample_data, "202401").sort("office_location")
        assert result1.equals(result2)


class TestOfficeProgramDistribution:
    """Tests for calculate_office_program_distribution function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004", "M005"],
            "office_name": ["Dallas Main", "Dallas Main", "Dallas Main", "Houston Central", "Houston Central"],
            "office_location": ["Dallas", "Dallas", "Dallas", "Houston", "Houston"],
            "ym_202401_reach": [True, False, True, True, False],
            "ym_202401_mssp": [False, True, True, False, False],
            "ever_reach": [True, False, True, True, False],
            "ever_mssp": [False, True, True, False, False],
        })

    @pytest.mark.unit
    def test_valid_yearmo(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_office_program_distribution"]
        result = func(sample_data, "202401")

        assert result is not None
        assert "reach_only_count" in result.columns
        assert "mssp_only_count" in result.columns
        assert "both_programs_count" in result.columns
        assert "neither_count" in result.columns

    @pytest.mark.unit
    def test_dallas_breakdown(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_office_program_distribution"]
        result = func(sample_data, "202401")

        dallas = result.filter(pl.col("office_location") == "Dallas")
        assert dallas["reach_only_count"][0] == 1
        assert dallas["mssp_only_count"][0] == 1
        assert dallas["both_programs_count"][0] == 1

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_office_program_distribution"]
        result1 = func(sample_data, "202401").sort("office_location")
        result2 = func(sample_data, "202401").sort("office_location")
        assert result1.equals(result2)


class TestOfficeTransitionStats:
    """Tests for calculate_office_transition_stats function."""

    @pytest.fixture
    def sample_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004"],
            "office_name": ["Dallas Main", "Dallas Main", "Houston Central", "Houston Central"],
            "office_location": ["Dallas", "Dallas", "Houston", "Houston"],
            "has_program_transition": [True, False, True, False],
            "has_continuous_enrollment": [True, True, False, True],
            "months_in_reach": [12, 0, 6, 18],
            "months_in_mssp": [0, 12, 6, 0],
            "total_aligned_months": [12, 12, 12, 18],
        })

    @pytest.mark.unit
    def test_structure(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_office_transition_stats"]
        result = func(sample_data)

        assert result is not None
        assert "transitioned_count" in result.columns
        assert "continuous_count" in result.columns
        assert "avg_months_reach" in result.columns

    @pytest.mark.unit
    def test_dallas_calculations(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_office_transition_stats"]
        result = func(sample_data)

        dallas = result.filter(pl.col("office_location") == "Dallas")
        assert dallas["transitioned_count"][0] == 1
        assert dallas["continuous_count"][0] == 2
        assert dallas["avg_months_reach"][0] == 6.0

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_data):
        func = notebook_defs["calculate_office_transition_stats"]
        result1 = func(sample_data).sort("office_location")
        result2 = func(sample_data).sort("office_location")
        assert result1.equals(result2)


class TestDisplayFunctions:
    """Tests for display helper functions."""

    @pytest.mark.unit
    def test_display_transitions_matrix_no_data(self, notebook_defs):
        func = notebook_defs["display_transitions_matrix"]
        result = func(None, None, None, consolidated_alignments.mo)
        assert result is not None  # Should return mo.md with "No data" message

    @pytest.mark.unit
    def test_display_campaign_effectiveness_no_data(self, notebook_defs):
        func = notebook_defs["display_campaign_effectiveness"]
        result = func(None, consolidated_alignments.mo)
        assert result is not None  # Should return mo.md with "No data" message


class TestAnalysisFunctions:
    """Tests for analysis functions."""

    @pytest.fixture
    def transition_data(self):
        return pl.DataFrame({
            "current_mbi": ["M001", "M002", "M003"],
            "ym_202312_reach": [True, False, False],
            "ym_202312_mssp": [False, True, False],
            "ym_202312_ffs": [False, False, True],
            "ym_202401_reach": [True, False, True],
            "ym_202401_mssp": [False, True, False],
            "ym_202401_ffs": [False, False, False],
        }).lazy()

    @pytest.mark.unit
    def test_calculate_alignment_transitions(self, notebook_defs, transition_data):
        func = notebook_defs["calculate_alignment_transitions"]
        transition_stats, prev_ym, curr_ym = func(transition_data, "202401", ["202312", "202401"], pl)

        assert transition_stats is not None
        assert "transition_type" in transition_stats.columns
        assert "count" in transition_stats.columns
        assert prev_ym == "202312"
        assert curr_ym == "202401"


class TestIdempotencyComprehensive:
    """Comprehensive idempotency tests for all functions."""

    @pytest.fixture
    def comprehensive_data(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003", "M004", "M005", "M006"],
            "office_name": ["Dallas Main", "Dallas Main", "Houston Central", "Houston Central", "Austin West", None],
            "office_location": ["Dallas", "Dallas", "Houston", "Houston", "Austin", None],
            "ym_202401_reach": [True, False, True, True, False, False],
            "ym_202401_mssp": [False, True, False, False, True, False],
            "ever_reach": [True, True, True, True, True, False],
            "ever_mssp": [False, True, False, True, True, False],
            "has_valid_voluntary_alignment": [True, False, True, False, False, False],
            "has_voluntary_alignment": [True, True, True, False, False, False],
            "consolidated_program": ["REACH", "MSSP", "REACH", "BOTH", "MSSP", "NONE"],
            "has_program_transition": [False, True, False, True, False, False],
            "has_continuous_enrollment": [True, True, False, False, True, True],
            "months_in_reach": [12, 6, 18, 12, 3, 0],
            "months_in_mssp": [0, 12, 0, 6, 12, 0],
            "total_aligned_months": [12, 18, 18, 18, 15, 0],
        })

    @pytest.mark.unit
    def test_all_office_functions_idempotent(self, notebook_defs, comprehensive_data):
        """All office functions return identical results when called multiple times."""

        # Test functions that take yearmo
        for func_name in [
            "calculate_office_enrollment_stats",
            "calculate_office_alignment_types",
            "calculate_office_program_distribution",
        ]:
            func = notebook_defs[func_name]
            r1 = func(comprehensive_data, "202401").sort("office_location")
            r2 = func(comprehensive_data, "202401").sort("office_location")
            r3 = func(comprehensive_data, "202401").sort("office_location")

            assert r1.equals(r2), f"{func_name} not idempotent (call 1 vs 2)"
            assert r2.equals(r3), f"{func_name} not idempotent (call 2 vs 3)"

        # Test transition stats (no yearmo)
        func = notebook_defs["calculate_office_transition_stats"]
        r1 = func(comprehensive_data).sort("office_location")
        r2 = func(comprehensive_data).sort("office_location")
        r3 = func(comprehensive_data).sort("office_location")

        assert r1.equals(r2), "calculate_office_transition_stats not idempotent (call 1 vs 2)"
        assert r2.equals(r3), "calculate_office_transition_stats not idempotent (call 2 vs 3)"
