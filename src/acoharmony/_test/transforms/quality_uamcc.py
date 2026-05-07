"""Tests for _transforms.quality_uamcc module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import patch
import pytest
import acoharmony


class TestAllCauseUnplannedAdmissions:
    """Tests for UAMCC AllCauseUnplannedAdmissions quality measure."""

    @pytest.mark.unit
    def test_import_module(self):
        assert AllCauseUnplannedAdmissions is not None

    @pytest.mark.unit
    def test_metadata(self):
        measure = AllCauseUnplannedAdmissions(config={"measurement_year": 2025})
        meta = measure.get_metadata()
        assert meta.measure_id == "NQF2888"
        assert "Unplanned" in meta.measure_name

    @pytest.mark.unit
    def test_measure_registered(self):
        assert "NQF2888" in MeasureFactory.list_measures()


# ===================== Coverage gap: lines 73-89, 102-129, 155 =====================

class TestUAMCCCalculateDenominator:
    """Test calculate_denominator method (lines 73-89)."""

    @pytest.mark.unit
    def test_calculate_denominator_returns_lazyframe(self):
        """Denominator calculation returns a LazyFrame with denominator_flag."""



        transform = AllCauseUnplannedAdmissions.__new__(AllCauseUnplannedAdmissions)
        transform.config = {"measurement_year": 2025}

        claims = pl.DataFrame({"person_id": ["A"]}).lazy()
        eligibility = pl.DataFrame({"person_id": ["A"]}).lazy()
        value_sets = {"cohort": pl.DataFrame().lazy()}

        with patch("acoharmony._transforms._quality_uamcc.UamccExpression") as mock_expr:
            mock_expr.identify_mcc_cohort.return_value = pl.DataFrame({"person_id": ["A"]}).lazy()
            mock_expr.build_denominator.return_value = pl.DataFrame({"person_id": ["A"]}).lazy()

            result = transform.calculate_denominator(claims, eligibility, value_sets)
            collected = result.collect()
            assert "person_id" in collected.columns
            assert "denominator_flag" in collected.columns


class TestUAMCCCalculateNumerator:
    """Test calculate_numerator method (lines 102-129)."""

    @pytest.mark.unit
    def test_calculate_numerator_returns_lazyframe(self):
        """Numerator calculation returns a LazyFrame with numerator_flag."""



        transform = AllCauseUnplannedAdmissions.__new__(AllCauseUnplannedAdmissions)
        transform.config = {"measurement_year": 2025}

        denominator = pl.DataFrame({"person_id": ["A", "B"]}).lazy()
        claims = pl.DataFrame({"person_id": ["A"]}).lazy()
        value_sets = {}

        with patch("acoharmony._transforms._quality_uamcc.UamccExpression") as mock_expr:
            # Two distinct admissions for person A, one excluded for person B.
            mock_planned = pl.DataFrame(
                {
                    "person_id": ["A", "A", "B"],
                    "claim_id": ["c1", "c2", "c3"],
                    "is_excluded": [False, False, True],
                }
            ).lazy()
            mock_expr.classify_planned_admissions.return_value = mock_planned
            mock_expr.apply_outcome_exclusions.return_value = mock_planned
            # link_admission_spells now collapses contiguous stays into
            # single spells before the per-person count. For this test
            # each claim is its own independent spell — pre-link
            # behaviour — so we mock the spell frame shape directly.
            mock_spells = pl.DataFrame(
                {
                    "person_id": ["A", "A", "B"],
                    "spell_id": ["c1", "c2", "c3"],
                    "is_excluded": [False, False, True],
                }
            ).lazy()
            mock_expr.link_admission_spells.return_value = mock_spells

            result = transform.calculate_numerator(denominator, claims, value_sets)
            collected = result.collect()
            assert "person_id" in collected.columns
            assert "count_unplanned_adm" in collected.columns
            assert "numerator_flag" in collected.columns

            by_person = dict(
                zip(
                    collected["person_id"].to_list(),
                    collected["count_unplanned_adm"].to_list(),
                    strict=False,
                )
            )
            assert by_person["A"] == 2  # two unplanned admissions
            assert by_person["B"] == 0  # only excluded admission


class TestUAMCCCalculateExclusions:
    """Test calculate_exclusions method (line 155)."""

    @pytest.mark.unit
    def test_calculate_exclusions_returns_false_flags(self):
        """UAMCC exclusions returns all False flags."""


        transform = AllCauseUnplannedAdmissions.__new__(AllCauseUnplannedAdmissions)
        denominator = pl.DataFrame({"person_id": ["A", "B"]}).lazy()
        claims = pl.DataFrame().lazy()
        value_sets = {}

        result = transform.calculate_exclusions(denominator, claims, value_sets)
        collected = result.collect()
        assert all(not v for v in collected["exclusion_flag"].to_list())
