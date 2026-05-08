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

    @staticmethod
    def _full_enrollment_eligibility(person_ids: list[str], py: int = 2025) -> pl.LazyFrame:
        """Eligibility frame where every bene satisfies the §3.2.2 p13
        12-month-prior + full-PY continuous A+B exclusion checks."""
        from datetime import date as _date
        n = len(person_ids)
        return pl.LazyFrame(
            {
                "person_id": person_ids,
                "enrollment_start_date": [_date(py - 2, 1, 1)] * n,
                "enrollment_end_date": [_date(py, 12, 31)] * n,
                "death_date": [None] * n,
            },
            schema={
                "person_id": pl.Utf8,
                "enrollment_start_date": pl.Date,
                "enrollment_end_date": pl.Date,
                "death_date": pl.Date,
            },
        )

    @staticmethod
    def _empty_claims() -> pl.LazyFrame:
        return pl.LazyFrame(
            {"person_id": [], "bill_type_code": [], "claim_start_date": []},
            schema={
                "person_id": pl.Utf8,
                "bill_type_code": pl.Utf8,
                "claim_start_date": pl.Date,
            },
        )

    @pytest.mark.unit
    def test_calculate_denominator_returns_lazyframe(self):
        """Denominator calculation returns a LazyFrame with denominator_flag."""
        transform = AllCauseUnplannedAdmissions.__new__(AllCauseUnplannedAdmissions)
        transform.config = {"measurement_year": 2025}

        claims = self._empty_claims()
        eligibility = self._full_enrollment_eligibility(["A"])
        value_sets = {"cohort": pl.DataFrame().lazy()}

        with patch("acoharmony._transforms._quality_uamcc.UamccExpression") as mock_expr:
            mock_expr.identify_mcc_cohort.return_value = pl.DataFrame({"person_id": ["A"]}).lazy()
            mock_expr.build_denominator.return_value = pl.DataFrame({"person_id": ["A"]}).lazy()

            result = transform.calculate_denominator(claims, eligibility, value_sets)
            collected = result.collect()
            assert "person_id" in collected.columns
            assert "denominator_flag" in collected.columns

    @pytest.mark.unit
    def test_reach_aligned_filter_narrows_denominator(self):
        """value_sets['reach_aligned_persons'] inner-joins to narrow denominator."""
        transform = AllCauseUnplannedAdmissions.__new__(AllCauseUnplannedAdmissions)
        transform.config = {"measurement_year": 2025}

        with patch("acoharmony._transforms._quality_uamcc.UamccExpression") as mock_expr:
            mock_expr.identify_mcc_cohort.return_value = pl.DataFrame({"person_id": ["A", "B", "C"]}).lazy()
            mock_expr.build_denominator.return_value = pl.DataFrame({"person_id": ["A", "B", "C"]}).lazy()

            reach = pl.LazyFrame({"person_id": ["A", "B"]})
            result = transform.calculate_denominator(
                self._empty_claims(),
                self._full_enrollment_eligibility(["A", "B", "C"]),
                {"cohort": pl.DataFrame().lazy(), "reach_aligned_persons": reach},
            ).collect()

        assert sorted(result["person_id"].to_list()) == ["A", "B"]


class TestUAMCCApplySpecDenominatorExclusions:
    """CMS PY2025 QMMR §3.2.2 p13 denominator exclusions."""

    @staticmethod
    def _denom(person_ids: list[str]) -> pl.LazyFrame:
        return pl.LazyFrame(
            {
                "person_id": person_ids,
                "denominator_flag": [True] * len(person_ids),
            }
        )

    @pytest.mark.unit
    def test_excludes_short_prior_year_lookback(self):
        """§3.2.2 p13 #1: <12mo continuous A+B in year before measurement."""
        from datetime import date as _date
        denom = self._denom(["FULL", "SHORT"])
        elig = pl.LazyFrame(
            {
                "person_id": ["FULL", "SHORT"],
                "enrollment_start_date": [_date(2023, 1, 1), _date(2024, 6, 1)],
                "enrollment_end_date": [_date(2025, 12, 31), _date(2025, 12, 31)],
                "death_date": [None, None],
            },
            schema={
                "person_id": pl.Utf8,
                "enrollment_start_date": pl.Date,
                "enrollment_end_date": pl.Date,
                "death_date": pl.Date,
            },
        )
        empty_claims = pl.LazyFrame(
            {"person_id": [], "bill_type_code": [], "claim_start_date": []},
            schema={"person_id": pl.Utf8, "bill_type_code": pl.Utf8, "claim_start_date": pl.Date},
        )
        result = AllCauseUnplannedAdmissions._apply_spec_denominator_exclusions(
            denom, empty_claims, elig, py=2025
        ).collect()
        assert result["person_id"].to_list() == ["FULL"]

    @pytest.mark.unit
    def test_excludes_short_measurement_year_enrollment(self):
        """§3.2.2 p13 #2: <12mo A+B during measurement year (no death/hospice escape)."""
        from datetime import date as _date
        denom = self._denom(["FULL", "DROPPED"])
        elig = pl.LazyFrame(
            {
                "person_id": ["FULL", "DROPPED"],
                "enrollment_start_date": [_date(2023, 1, 1), _date(2023, 1, 1)],
                "enrollment_end_date": [_date(2025, 12, 31), _date(2025, 6, 30)],
                "death_date": [None, None],
            },
            schema={
                "person_id": pl.Utf8,
                "enrollment_start_date": pl.Date,
                "enrollment_end_date": pl.Date,
                "death_date": pl.Date,
            },
        )
        empty_claims = pl.LazyFrame(
            {"person_id": [], "bill_type_code": [], "claim_start_date": []},
            schema={"person_id": pl.Utf8, "bill_type_code": pl.Utf8, "claim_start_date": pl.Date},
        )
        result = AllCauseUnplannedAdmissions._apply_spec_denominator_exclusions(
            denom, empty_claims, elig, py=2025
        ).collect()
        assert result["person_id"].to_list() == ["FULL"]

    @pytest.mark.unit
    def test_keeps_died_during_py_with_short_enrollment(self):
        """§3.2.2 p13 #2 escape clause: bene who died during PY is kept if
        continuously enrolled until death."""
        from datetime import date as _date
        denom = self._denom(["DIED"])
        elig = pl.LazyFrame(
            {
                "person_id": ["DIED"],
                "enrollment_start_date": [_date(2023, 1, 1)],
                "enrollment_end_date": [_date(2025, 7, 1)],
                "death_date": [_date(2025, 7, 1)],
            },
            schema={
                "person_id": pl.Utf8,
                "enrollment_start_date": pl.Date,
                "enrollment_end_date": pl.Date,
                "death_date": pl.Date,
            },
        )
        empty_claims = pl.LazyFrame(
            {"person_id": [], "bill_type_code": [], "claim_start_date": []},
            schema={"person_id": pl.Utf8, "bill_type_code": pl.Utf8, "claim_start_date": pl.Date},
        )
        result = AllCauseUnplannedAdmissions._apply_spec_denominator_exclusions(
            denom, empty_claims, elig, py=2025
        ).collect()
        assert result["person_id"].to_list() == ["DIED"]

    @pytest.mark.unit
    def test_excludes_hospice_in_year_before_or_at_py_start(self):
        """§3.2.2 p13 #3: hospice during prior year or at start of PY → exclude."""
        from datetime import date as _date
        denom = self._denom(["NO_HOSPICE", "HOSPICE_PRIOR"])
        elig = pl.LazyFrame(
            {
                "person_id": ["NO_HOSPICE", "HOSPICE_PRIOR"],
                "enrollment_start_date": [_date(2023, 1, 1), _date(2023, 1, 1)],
                "enrollment_end_date": [_date(2025, 12, 31), _date(2025, 12, 31)],
                "death_date": [None, None],
            },
            schema={
                "person_id": pl.Utf8,
                "enrollment_start_date": pl.Date,
                "enrollment_end_date": pl.Date,
                "death_date": pl.Date,
            },
        )
        # HOSPICE_PRIOR has a hospice TOB 813 claim in 2024 (year before PY).
        claims = pl.LazyFrame(
            {
                "person_id": ["HOSPICE_PRIOR"],
                "bill_type_code": ["813"],
                "claim_start_date": [_date(2024, 6, 1)],
            },
            schema={"person_id": pl.Utf8, "bill_type_code": pl.Utf8, "claim_start_date": pl.Date},
        )
        result = AllCauseUnplannedAdmissions._apply_spec_denominator_exclusions(
            denom, claims, elig, py=2025
        ).collect()
        assert result["person_id"].to_list() == ["NO_HOSPICE"]

    @pytest.mark.unit
    def test_keeps_hospice_started_after_py_start(self):
        """§3.2.2 p13 #3 boundary: hospice starting AFTER py_start is OK."""
        from datetime import date as _date
        denom = self._denom(["MID_PY_HOSPICE"])
        elig = pl.LazyFrame(
            {
                "person_id": ["MID_PY_HOSPICE"],
                "enrollment_start_date": [_date(2023, 1, 1)],
                "enrollment_end_date": [_date(2025, 12, 31)],
                "death_date": [None],
            },
            schema={
                "person_id": pl.Utf8,
                "enrollment_start_date": pl.Date,
                "enrollment_end_date": pl.Date,
                "death_date": pl.Date,
            },
        )
        # Hospice claim starts in mid-PY 2025 → not pre-PY → should keep bene.
        claims = pl.LazyFrame(
            {
                "person_id": ["MID_PY_HOSPICE"],
                "bill_type_code": ["813"],
                "claim_start_date": [_date(2025, 6, 1)],
            },
            schema={"person_id": pl.Utf8, "bill_type_code": pl.Utf8, "claim_start_date": pl.Date},
        )
        result = AllCauseUnplannedAdmissions._apply_spec_denominator_exclusions(
            denom, claims, elig, py=2025
        ).collect()
        assert result["person_id"].to_list() == ["MID_PY_HOSPICE"]

    @pytest.mark.unit
    def test_no_claim_columns_skips_hospice_check(self):
        """Defensive: claims source without bill_type_code/claim_start_date
        should pass denom through (not crash)."""
        from datetime import date as _date
        denom = self._denom(["A"])
        elig = pl.LazyFrame(
            {
                "person_id": ["A"],
                "enrollment_start_date": [_date(2023, 1, 1)],
                "enrollment_end_date": [_date(2025, 12, 31)],
                "death_date": [None],
            },
            schema={
                "person_id": pl.Utf8,
                "enrollment_start_date": pl.Date,
                "enrollment_end_date": pl.Date,
                "death_date": pl.Date,
            },
        )
        # Claims missing bill_type_code: hospice check no-ops.
        claims = pl.LazyFrame(
            {"person_id": ["A"], "other_col": ["X"]},
            schema={"person_id": pl.Utf8, "other_col": pl.Utf8},
        )
        result = AllCauseUnplannedAdmissions._apply_spec_denominator_exclusions(
            denom, claims, elig, py=2025
        ).collect()
        assert result["person_id"].to_list() == ["A"]


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
