"""Tests for _transforms.quality_acr module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date  # noqa: E402
from unittest.mock import patch  # noqa: E402

import polars as pl
import pytest
import acoharmony


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()
class TestAllConditionReadmission:
    """Tests for ACR AllConditionReadmission quality measure."""

    @pytest.mark.unit
    def test_import_module(self):
        assert AllConditionReadmission is not None

    @pytest.mark.unit
    def test_metadata(self):
        measure = AllConditionReadmission(config={"measurement_year": 2025})
        meta = measure.get_metadata()
        assert meta.measure_id == "NQF1789"
        assert "Readmission" in meta.measure_name

    @pytest.mark.unit
    def test_measure_registered(self):
        assert "NQF1789" in MeasureFactory.list_measures()


class TestAcrQualityMeasure:
    """Tests for ACR (NQF #1789) quality measure."""

    @pytest.mark.unit
    def test_acr_metadata(self):

        m = AllConditionReadmission(config={"measurement_year": 2025})
        meta = m.get_metadata()
        assert meta.measure_id == "NQF1789"
        assert "readmission" in meta.measure_name.lower()

    @pytest.mark.unit
    def test_acr_exclusions_always_false(self):

        m = AllConditionReadmission(config={"measurement_year": 2025})
        denom = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "denominator_flag": [True, True],
        }).lazy()
        claims = pl.DataFrame({"person_id": ["P1"]}).lazy()
        result = m.calculate_exclusions(denom, claims, {}).collect()
        assert all(not v for v in result["exclusion_flag"].to_list())

    @pytest.mark.unit
    def test_acr_measure_metadata(self):

        measure = AllConditionReadmission(config={"measurement_year": 2025})
        meta = measure.get_metadata()
        assert meta.measure_id == "NQF1789"
        assert meta.measure_steward == "CMS"

    @pytest.mark.unit
    def test_acr_registered_in_factory(self):

        assert "NQF1789" in MeasureFactory.list_measures()




class TestAcrDenominator:
    """Tests for AllConditionReadmission.calculate_denominator."""

    def _make_claims(self):
        return pl.DataFrame({
            "claim_id": ["C1", "C2", "C3"],
            "person_id": ["P1", "P1", "P2"],
            "bill_type_code": ["111", "111", "111"],
            "admission_date": [date(2025, 1, 5), date(2025, 3, 1), date(2025, 2, 1)],
            "discharge_date": [date(2025, 1, 10), date(2025, 3, 5), date(2025, 2, 5)],
            "diagnosis_code_1": ["A01", "A02", "B01"],
            "claim_start_date": [date(2025, 1, 5), date(2025, 3, 1), date(2025, 2, 1)],
            "claim_end_date": [date(2025, 1, 10), date(2025, 3, 5), date(2025, 2, 5)],
        }).lazy()

    def _make_eligibility(self):
        return pl.DataFrame({
            "person_id": ["P1", "P2"],
            "birth_date": [date(1955, 1, 1), date(1950, 6, 15)],
            "gender": ["M", "F"],
        }).lazy()

    @pytest.mark.unit
    def test_calculate_denominator_runs(self):
        """calculate_denominator runs without error given proper inputs."""


        m = AllConditionReadmission(config={"measurement_year": 2025})
        claims = self._make_claims()
        eligibility = self._make_eligibility()
        value_sets = {}

        # Mock identify_index_admissions to return admissions with exclusion flags
        mock_admissions = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "claim_id": ["C1", "C3"],
            "exclusion_flag": [False, False],
        }).lazy()

        with patch.object(
            type(m).__mro__[0].__mro__[0],  # use direct patch
            "calculate_denominator",
            return_value=mock_admissions,
        ):
            pass

        # Direct approach: mock the expression
        with patch.object(AcrReadmissionExpression, "identify_index_admissions", return_value=mock_admissions):
            result = m.calculate_denominator(claims, eligibility, value_sets)
            collected = result.collect()
            assert "person_id" in collected.columns
            assert "denominator_flag" in collected.columns


class TestAcrNumerator:
    """Tests for AllConditionReadmission.calculate_numerator."""

    def _make_denom(self):
        return pl.DataFrame({
            "person_id": ["P1", "P2"],
            "denominator_flag": [True, True],
        }).lazy()

    def _make_claims(self):
        return pl.DataFrame({
            "claim_id": ["C1", "C2", "C3"],
            "person_id": ["P1", "P1", "P2"],
            "bill_type_code": ["111", "111", "111"],
            "admission_date": [date(2025, 1, 5), date(2025, 1, 20), date(2025, 2, 1)],
            "discharge_date": [date(2025, 1, 10), date(2025, 1, 25), date(2025, 2, 5)],
            "diagnosis_code_1": ["A01", "A02", "B01"],
        }).lazy()

    @pytest.mark.unit
    def test_numerator_no_readmissions(self):
        """When no readmissions within 30 days, numerator is all False."""

        m = AllConditionReadmission(config={"measurement_year": 2025})

        # Claims with no readmission pairs (discharge far apart)
        claims = pl.DataFrame({
            "claim_id": ["C1", "C2"],
            "person_id": ["P1", "P2"],
            "bill_type_code": ["111", "111"],
            "admission_date": [date(2025, 1, 5), date(2025, 6, 1)],
            "discharge_date": [date(2025, 1, 10), date(2025, 6, 5)],
            "diagnosis_code_1": ["A01", "B01"],
        }).lazy()

        result = m.calculate_numerator(self._make_denom(), claims, {})
        collected = result.collect()
        assert all(not v for v in collected["numerator_flag"].to_list())

    @pytest.mark.unit
    def test_numerator_with_readmission(self):
        """When readmission within 30 days, numerator flag is True."""

        m = AllConditionReadmission(config={"measurement_year": 2025})
        result = m.calculate_numerator(self._make_denom(), self._make_claims(), {})
        collected = result.collect()
        # P1 has C1 discharge 1/10 and C2 admission 1/20 = 10 days = readmission
        p1_row = collected.filter(pl.col("person_id") == "P1")
        assert p1_row["numerator_flag"][0] is True

    @pytest.mark.unit
    def test_numerator_with_ccs_mapping(self):
        """When ccs_mapping and paa2 value_sets are provided, applies PAA exclusions."""

        m = AllConditionReadmission(config={"measurement_year": 2025})

        ccs_mapping = pl.DataFrame({
            "icd_10_cm": ["A02"],
            "ccs_category": ["CANCER"],
        }).lazy()

        paa2 = pl.DataFrame({
            "ccs_diagnosis_category": ["CANCER"],
        }).lazy()

        value_sets = {"ccs_icd10_cm": ccs_mapping, "paa2": paa2}

        result = m.calculate_numerator(self._make_denom(), self._make_claims(), value_sets)
        collected = result.collect()
        # A02 readmission should be excluded since it maps to CANCER which is in paa2
        p1_row = collected.filter(pl.col("person_id") == "P1")
        assert p1_row["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_numerator_empty_ccs_mapping(self):
        """When ccs_mapping is empty, adds null readmit_dx_ccs."""

        m = AllConditionReadmission(config={"measurement_year": 2025})

        ccs_mapping = pl.DataFrame({
            "icd_10_cm": pl.Series([], dtype=pl.Utf8),
            "ccs_category": pl.Series([], dtype=pl.Utf8),
        }).lazy()

        value_sets = {"ccs_icd10_cm": ccs_mapping}
        result = m.calculate_numerator(self._make_denom(), self._make_claims(), value_sets)
        collected = result.collect()
        assert "numerator_flag" in collected.columns

    @pytest.mark.unit
    def test_numerator_empty_paa2(self):
        """When paa2 is empty, all readmissions count as unplanned."""

        m = AllConditionReadmission(config={"measurement_year": 2025})

        paa2 = pl.DataFrame({
            "ccs_diagnosis_category": pl.Series([], dtype=pl.Utf8),
        }).lazy()

        value_sets = {"paa2": paa2}
        result = m.calculate_numerator(self._make_denom(), self._make_claims(), value_sets)
        collected = result.collect()
        p1_row = collected.filter(pl.col("person_id") == "P1")
        assert p1_row["numerator_flag"][0] is True

    @pytest.mark.unit
    def test_numerator_none_ccs_and_none_paa2(self):
        """When both ccs_mapping and paa2 are None, else branches fire (156->168, 172->183)."""
        m = AllConditionReadmission(config={"measurement_year": 2025})

        # Explicitly pass None for both value sets
        value_sets = {"ccs_icd10_cm": None, "paa2": None}
        result = m.calculate_numerator(self._make_denom(), self._make_claims(), value_sets)
        collected = result.collect()
        # Should still produce valid output
        assert "numerator_flag" in collected.columns
        # P1 has a readmission within 30 days, should be flagged
        p1_row = collected.filter(pl.col("person_id") == "P1")
        assert p1_row["numerator_flag"][0] is True

    @pytest.mark.unit
    def test_numerator_populated_ccs_and_paa2(self):
        """When both ccs_mapping and paa2 have data, true branches fire (156->157, 172->173)."""
        m = AllConditionReadmission(config={"measurement_year": 2025})

        # ccs_mapping maps the readmission diagnosis to a CCS category
        ccs_mapping = pl.DataFrame({
            "icd_10_cm": ["A01", "A02", "B01"],
            "ccs_category": ["INFECT", "INFECT2", "OTHER"],
        }).lazy()

        # paa2 excludes INFECT2 (planned admissions)
        paa2 = pl.DataFrame({
            "ccs_diagnosis_category": ["INFECT2"],
        }).lazy()

        value_sets = {"ccs_icd10_cm": ccs_mapping, "paa2": paa2}

        result = m.calculate_numerator(self._make_denom(), self._make_claims(), value_sets)
        collected = result.collect()
        assert "numerator_flag" in collected.columns

    @pytest.mark.unit
    def test_numerator_nonempty_ccs_but_empty_paa2(self):
        """Non-empty ccs_mapping but empty paa2 -> 156->157 then 172->183."""
        m = AllConditionReadmission(config={"measurement_year": 2025})

        ccs_mapping = pl.DataFrame({
            "icd_10_cm": ["A02"],
            "ccs_category": ["SOME_CCS"],
        }).lazy()

        paa2 = pl.DataFrame({
            "ccs_diagnosis_category": pl.Series([], dtype=pl.Utf8),
        }).lazy()

        value_sets = {"ccs_icd10_cm": ccs_mapping, "paa2": paa2}

        result = m.calculate_numerator(self._make_denom(), self._make_claims(), value_sets)
        collected = result.collect()
        assert "numerator_flag" in collected.columns
        p1_row = collected.filter(pl.col("person_id") == "P1")
        assert p1_row["numerator_flag"][0] is True

    @pytest.mark.unit
    def test_numerator_empty_ccs_but_nonempty_paa2(self):
        """Empty ccs_mapping but non-empty paa2 -> 156->168 then 172->173 (paa2 filters on null CCS)."""
        m = AllConditionReadmission(config={"measurement_year": 2025})

        ccs_mapping = pl.DataFrame({
            "icd_10_cm": pl.Series([], dtype=pl.Utf8),
            "ccs_category": pl.Series([], dtype=pl.Utf8),
        }).lazy()

        paa2 = pl.DataFrame({
            "ccs_diagnosis_category": ["CANCER"],
        }).lazy()

        value_sets = {"ccs_icd10_cm": ccs_mapping, "paa2": paa2}

        result = m.calculate_numerator(self._make_denom(), self._make_claims(), value_sets)
        collected = result.collect()
        assert "numerator_flag" in collected.columns
