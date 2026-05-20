"""Tests for _transforms.admissions_analysis module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from datetime import date, datetime  # noqa: F811

import polars as pl
import pytest
import acoharmony


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestAdmissionsAnalysisPublic:
    """Tests for admissions_analysis public transform module."""

    @pytest.mark.unit
    def test_import_module(self):
        assert admissions_analysis is not None

DEFAULT_CONFIG = {"measurement_year": 2024}


def _make_claims(
    rows: list[dict],
    *,
    extra_schema: dict | None = None,
) -> pl.LazyFrame:
    """Build a LazyFrame with standard claims columns, filling missing cols with defaults."""
    defaults = {
        "person_id": "P001",
        "claim_id": "C001",
        "claim_type": "institutional",
        "bill_type_code": "110",
        "admission_date": date(2024, 3, 1),
        "discharge_date": date(2024, 3, 5),
        "diagnosis_code_1": "J18.9",
        "diagnosis_code_2": None,
        "diagnosis_code_3": None,
        "procedure_code_1": "99213",
        "facility_npi": "1234567890",
        "paid_amount": 1000.0,
        "allowed_amount": 1200.0,
        "claim_start_date": date(2024, 3, 1),
        "claim_end_date": date(2024, 3, 5),
        "revenue_code": "0100",
        "place_of_service_code": "21",
    }
    filled = []
    for row in rows:
        merged = {**defaults, **row}
        filled.append(merged)
    schema = {
        "person_id": pl.Utf8,
        "claim_id": pl.Utf8,
        "claim_type": pl.Utf8,
        "bill_type_code": pl.Utf8,
        "admission_date": pl.Date,
        "discharge_date": pl.Date,
        "diagnosis_code_1": pl.Utf8,
        "diagnosis_code_2": pl.Utf8,
        "diagnosis_code_3": pl.Utf8,
        "procedure_code_1": pl.Utf8,
        "facility_npi": pl.Utf8,
        "paid_amount": pl.Float64,
        "allowed_amount": pl.Float64,
        "claim_start_date": pl.Date,
        "claim_end_date": pl.Date,
        "revenue_code": pl.Utf8,
        "place_of_service_code": pl.Utf8,
    }
    if extra_schema:
        schema.update(extra_schema)
    return pl.DataFrame(filled, schema=schema).lazy()


def _make_eligibility(rows: list[dict] | None = None) -> pl.LazyFrame:
    if rows is None:
        rows = [
            {
                "person_id": "P001",
                "enrollment_start_date": date(2024, 1, 1),
                "enrollment_end_date": date(2024, 12, 31),
            },
            {
                "person_id": "P002",
                "enrollment_start_date": date(2024, 1, 1),
                "enrollment_end_date": date(2024, 12, 31),
            },
        ]
    return pl.DataFrame(rows).lazy()


class TestAdmissionsAnalysisIdentifyInpatient:

    @pytest.mark.unit
    def test_basic_inpatient_identification(self):

        claims = _make_claims([
            {"person_id": "P001", "claim_id": "C001", "claim_type": "institutional",
             "bill_type_code": "111", "admission_date": date(2024, 3, 1),
             "discharge_date": date(2024, 3, 5), "diagnosis_code_1": "J18.9",
             "procedure_code_1": "99213", "facility_npi": "NPI1",
             "paid_amount": 5000.0, "allowed_amount": 6000.0},
            {"person_id": "P002", "claim_id": "C002", "claim_type": "institutional",
             "bill_type_code": "112", "admission_date": date(2024, 4, 1),
             "discharge_date": date(2024, 4, 3)},
        ])
        result = AdmissionsAnalysisTransform.identify_inpatient_admissions(claims, DEFAULT_CONFIG)
        assert isinstance(result, pl.LazyFrame)
        df = result.collect()
        assert df.height == 2
        expected_cols = {
            "person_id", "claim_id", "encounter_type", "admission_date",
            "discharge_date", "principal_diagnosis", "principal_procedure",
            "facility_npi", "paid_amount", "allowed_amount", "length_of_stay",
        }
        assert set(df.columns) == expected_cols

    @pytest.mark.unit
    def test_encounter_type_is_inpatient(self):

        claims = _make_claims([
            {"claim_id": "C001", "bill_type_code": "111"},
        ])
        df = AdmissionsAnalysisTransform.identify_inpatient_admissions(claims, DEFAULT_CONFIG).collect()
        assert df["encounter_type"][0] == "inpatient"

    @pytest.mark.unit
    def test_length_of_stay_calculation(self):

        claims = _make_claims([
            {"claim_id": "C001", "bill_type_code": "111",
             "admission_date": date(2024, 1, 1), "discharge_date": date(2024, 1, 6)},
        ])
        df = AdmissionsAnalysisTransform.identify_inpatient_admissions(claims, DEFAULT_CONFIG).collect()
        assert df["length_of_stay"][0] == 5

    @pytest.mark.unit
    def test_filters_non_institutional(self):

        claims = _make_claims([
            {"claim_id": "C001", "claim_type": "professional", "bill_type_code": "111"},
        ])
        df = AdmissionsAnalysisTransform.identify_inpatient_admissions(claims, DEFAULT_CONFIG).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_filters_non_11x_bill_type(self):

        claims = _make_claims([
            {"claim_id": "C001", "bill_type_code": "131"},
        ])
        df = AdmissionsAnalysisTransform.identify_inpatient_admissions(claims, DEFAULT_CONFIG).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_filters_null_admission_date(self):

        claims = _make_claims([
            {"claim_id": "C001", "bill_type_code": "111", "admission_date": None},
        ])
        df = AdmissionsAnalysisTransform.identify_inpatient_admissions(claims, DEFAULT_CONFIG).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_filters_null_discharge_date(self):

        claims = _make_claims([
            {"claim_id": "C001", "bill_type_code": "111", "discharge_date": None},
        ])
        df = AdmissionsAnalysisTransform.identify_inpatient_admissions(claims, DEFAULT_CONFIG).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_empty_claims(self):

        claims = _make_claims([
            {"claim_id": "C001", "claim_type": "professional", "bill_type_code": "999"},
        ])
        df = AdmissionsAnalysisTransform.identify_inpatient_admissions(claims, DEFAULT_CONFIG).collect()
        assert df.height == 0


class TestAdmissionsAnalysisIdentifyEdVisits:

    @pytest.mark.unit
    def test_ed_by_revenue_code_045x(self):

        claims = _make_claims([
            {"claim_id": "C001", "claim_type": "institutional", "revenue_code": "0450",
             "bill_type_code": "131", "place_of_service_code": "21"},
            {"claim_id": "C002", "claim_type": "institutional", "revenue_code": "0451",
             "bill_type_code": "131", "place_of_service_code": "21"},
        ])
        df = AdmissionsAnalysisTransform.identify_ed_visits(claims, DEFAULT_CONFIG).collect()
        assert df.height == 2
        assert all(v == "emergency_department" for v in df["encounter_type"].to_list())

    @pytest.mark.unit
    def test_ed_by_revenue_code_0981(self):

        claims = _make_claims([
            {"claim_id": "C001", "claim_type": "institutional", "revenue_code": "0981"},
        ])
        df = AdmissionsAnalysisTransform.identify_ed_visits(claims, DEFAULT_CONFIG).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_ed_by_bill_type_13x_and_pos_23(self):

        claims = _make_claims([
            {"claim_id": "C001", "claim_type": "institutional", "bill_type_code": "131",
             "place_of_service_code": "23", "revenue_code": "0100"},
        ])
        df = AdmissionsAnalysisTransform.identify_ed_visits(claims, DEFAULT_CONFIG).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_ed_deduplicates_by_claim_id(self):
        """A claim matching both revenue-code and bill-type criteria appears once."""

        claims = _make_claims([
            {"claim_id": "C001", "claim_type": "institutional", "bill_type_code": "131",
             "place_of_service_code": "23", "revenue_code": "0450"},
        ])
        df = AdmissionsAnalysisTransform.identify_ed_visits(claims, DEFAULT_CONFIG).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_ed_length_of_stay_is_zero(self):

        claims = _make_claims([
            {"claim_id": "C001", "claim_type": "institutional", "revenue_code": "0450"},
        ])
        df = AdmissionsAnalysisTransform.identify_ed_visits(claims, DEFAULT_CONFIG).collect()
        assert df["length_of_stay"][0] == 0

    @pytest.mark.unit
    def test_ed_output_columns(self):

        claims = _make_claims([
            {"claim_id": "C001", "claim_type": "institutional", "revenue_code": "0450"},
        ])
        df = AdmissionsAnalysisTransform.identify_ed_visits(claims, DEFAULT_CONFIG).collect()
        expected_cols = {
            "person_id", "claim_id", "encounter_type", "admission_date",
            "discharge_date", "principal_diagnosis", "principal_procedure",
            "facility_npi", "paid_amount", "allowed_amount", "length_of_stay",
        }
        assert set(df.columns) == expected_cols

    @pytest.mark.unit
    def test_ed_no_matches(self):

        claims = _make_claims([
            {"claim_id": "C001", "claim_type": "institutional", "revenue_code": "0100",
             "bill_type_code": "110", "place_of_service_code": "21"},
        ])
        df = AdmissionsAnalysisTransform.identify_ed_visits(claims, DEFAULT_CONFIG).collect()
        assert df.height == 0


class TestAdmissionsAnalysisObservationStays:

    @pytest.mark.unit
    def test_observation_by_revenue_code_0762(self):

        claims = _make_claims([
            {"claim_id": "C001", "claim_type": "institutional", "revenue_code": "0762",
             "claim_start_date": date(2024, 5, 1), "claim_end_date": date(2024, 5, 2)},
        ])
        df = AdmissionsAnalysisTransform.identify_observation_stays(claims, DEFAULT_CONFIG).collect()
        assert df.height == 1
        assert df["encounter_type"][0] == "observation"

    @pytest.mark.unit
    def test_observation_length_of_stay(self):

        claims = _make_claims([
            {"claim_id": "C001", "claim_type": "institutional", "revenue_code": "0762",
             "claim_start_date": date(2024, 5, 1), "claim_end_date": date(2024, 5, 2)},
        ])
        df = AdmissionsAnalysisTransform.identify_observation_stays(claims, DEFAULT_CONFIG).collect()
        assert df["length_of_stay"][0] == 1

    @pytest.mark.unit
    def test_observation_non_matching_revenue_code(self):

        claims = _make_claims([
            {"claim_id": "C001", "claim_type": "institutional", "revenue_code": "0100"},
        ])
        df = AdmissionsAnalysisTransform.identify_observation_stays(claims, DEFAULT_CONFIG).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_observation_filters_professional(self):

        claims = _make_claims([
            {"claim_id": "C001", "claim_type": "professional", "revenue_code": "0762"},
        ])
        df = AdmissionsAnalysisTransform.identify_observation_stays(claims, DEFAULT_CONFIG).collect()
        assert df.height == 0


class TestAdmissionsAnalysisRates:

    def _make_admissions(self) -> pl.LazyFrame:
        return pl.DataFrame({
            "person_id": ["P001", "P001", "P002"],
            "encounter_type": ["inpatient", "emergency_department", "inpatient"],
            "admission_date": [date(2024, 2, 1), date(2024, 3, 1), date(2024, 4, 1)],
            "discharge_date": [date(2024, 2, 5), date(2024, 3, 1), date(2024, 4, 3)],
            "paid_amount": [5000.0, 1000.0, 3000.0],
            "allowed_amount": [6000.0, 1200.0, 3500.0],
            "length_of_stay": [4, 0, 2],
            "principal_diagnosis": ["J18.9", "R10.0", "I21.0"],
            "principal_procedure": ["99213", "99281", "99213"],
            "facility_npi": ["NPI1", "NPI1", "NPI2"],
            "claim_id": ["C001", "C002", "C003"],
        }).lazy()

    @pytest.mark.unit
    def test_rate_output_columns(self):

        admissions = self._make_admissions()
        eligibility = _make_eligibility()
        result = AdmissionsAnalysisTransform.calculate_admission_rates(
            admissions, eligibility, DEFAULT_CONFIG,
        ).collect()
        assert "admissions_per_1000" in result.columns
        assert "admissions_per_1000_mm" in result.columns
        assert "admissions_per_member_per_year" in result.columns
        assert "encounter_type" in result.columns
        assert "admission_count" in result.columns
        assert "member_count" in result.columns
        assert "total_member_months" in result.columns

    @pytest.mark.unit
    def test_rate_per_1000_calculation(self):

        admissions = self._make_admissions()
        eligibility = _make_eligibility()
        result = AdmissionsAnalysisTransform.calculate_admission_rates(
            admissions, eligibility, DEFAULT_CONFIG,
        ).collect()
        # 2 unique members, 2 inpatient admissions -> 2/2*1000 = 1000
        ip_row = result.filter(pl.col("encounter_type") == "inpatient")
        assert ip_row["admissions_per_1000"][0] == 1000.0


class TestAdmissionsAnalysisTopDiagnoses:

    @pytest.mark.unit
    def test_top_diagnoses_output(self):

        admissions = pl.DataFrame({
            "encounter_type": ["inpatient"] * 5,
            "principal_diagnosis": ["J18.9", "J18.9", "I21.0", "I21.0", "I21.0"],
            "paid_amount": [1000.0, 2000.0, 3000.0, 4000.0, 5000.0],
        }).lazy()
        result = AdmissionsAnalysisTransform.analyze_top_diagnoses(admissions, DEFAULT_CONFIG, top_n=2).collect()
        assert "rank" in result.columns
        assert "admission_count" in result.columns
        assert "total_paid" in result.columns
        assert result.height == 2

    @pytest.mark.unit
    def test_top_diagnoses_ranking(self):

        admissions = pl.DataFrame({
            "encounter_type": ["inpatient"] * 5,
            "principal_diagnosis": ["A", "A", "A", "B", "B"],
            "paid_amount": [100.0] * 5,
        }).lazy()
        result = AdmissionsAnalysisTransform.analyze_top_diagnoses(admissions, DEFAULT_CONFIG, top_n=1).collect()
        assert result.height == 1
        assert result["principal_diagnosis"][0] == "A"

    @pytest.mark.unit
    def test_top_diagnoses_respects_top_n(self):

        admissions = pl.DataFrame({
            "encounter_type": ["inpatient"] * 10,
            "principal_diagnosis": [f"D{i}" for i in range(10)],
            "paid_amount": [100.0] * 10,
        }).lazy()
        result = AdmissionsAnalysisTransform.analyze_top_diagnoses(admissions, DEFAULT_CONFIG, top_n=5).collect()
        assert result.height == 5


class TestAdmissionsAnalysisByFacility:

    @pytest.mark.unit
    def test_facility_analysis_output_columns(self):

        admissions = pl.DataFrame({
            "facility_npi": ["NPI1", "NPI1", "NPI2"],
            "encounter_type": ["inpatient", "inpatient", "emergency_department"],
            "length_of_stay": [4, 2, 0],
            "paid_amount": [5000.0, 3000.0, 1000.0],
            "allowed_amount": [6000.0, 3500.0, 1200.0],
            "person_id": ["P001", "P002", "P001"],
        }).lazy()
        result = AdmissionsAnalysisTransform.analyze_by_facility(admissions, DEFAULT_CONFIG).collect()
        expected_cols = {
            "facility_npi", "encounter_type", "admission_count",
            "avg_length_of_stay", "total_paid", "total_allowed",
            "unique_patients", "avg_cost_per_admission",
        }
        assert set(result.columns) == expected_cols

    @pytest.mark.unit
    def test_facility_avg_cost_per_admission(self):

        admissions = pl.DataFrame({
            "facility_npi": ["NPI1", "NPI1"],
            "encounter_type": ["inpatient", "inpatient"],
            "length_of_stay": [4, 2],
            "paid_amount": [4000.0, 6000.0],
            "allowed_amount": [5000.0, 7000.0],
            "person_id": ["P001", "P002"],
        }).lazy()
        result = AdmissionsAnalysisTransform.analyze_by_facility(admissions, DEFAULT_CONFIG).collect()
        assert result["avg_cost_per_admission"][0] == 5000.0  # (4000+6000)/2


class TestAdmissionsAnalysisComprehensive:

    def _full_claims(self) -> pl.LazyFrame:
        return _make_claims([
            # Inpatient
            {"person_id": "P001", "claim_id": "C001", "claim_type": "institutional",
             "bill_type_code": "111", "admission_date": date(2024, 2, 1),
             "discharge_date": date(2024, 2, 5), "revenue_code": "0100",
             "place_of_service_code": "21"},
            # ED via revenue code
            {"person_id": "P002", "claim_id": "C002", "claim_type": "institutional",
             "bill_type_code": "131", "revenue_code": "0450",
             "place_of_service_code": "21",
             "admission_date": date(2024, 3, 1), "discharge_date": date(2024, 3, 1),
             "claim_start_date": date(2024, 3, 1), "claim_end_date": date(2024, 3, 1)},
            # Observation
            {"person_id": "P001", "claim_id": "C003", "claim_type": "institutional",
             "bill_type_code": "131", "revenue_code": "0762",
             "place_of_service_code": "21",
             "admission_date": date(2024, 4, 1), "discharge_date": date(2024, 4, 2),
             "claim_start_date": date(2024, 4, 1), "claim_end_date": date(2024, 4, 2)},
        ])

    @pytest.mark.unit
    def test_comprehensive_returns_tuple_of_five(self):

        claims = self._full_claims()
        eligibility = _make_eligibility()
        result = AdmissionsAnalysisTransform.calculate_comprehensive_admissions(
            claims, eligibility, DEFAULT_CONFIG,
        )
        assert isinstance(result, tuple)
        assert len(result) == 5

    @pytest.mark.unit
    def test_comprehensive_all_admissions(self):

        claims = self._full_claims()
        eligibility = _make_eligibility()
        all_admissions, *_ = AdmissionsAnalysisTransform.calculate_comprehensive_admissions(
            claims, eligibility, DEFAULT_CONFIG,
        )
        df = all_admissions.collect()
        assert df.height == 3
        types = set(df["encounter_type"].to_list())
        assert types == {"inpatient", "emergency_department", "observation"}

    @pytest.mark.unit
    def test_comprehensive_summary(self):

        claims = self._full_claims()
        eligibility = _make_eligibility()
        _, _, _, _, summary = AdmissionsAnalysisTransform.calculate_comprehensive_admissions(
            claims, eligibility, DEFAULT_CONFIG,
        )
        df = summary.collect()
        assert "total_admissions" in df.columns
        assert "unique_patients" in df.columns
        assert "avg_length_of_stay" in df.columns
        assert "total_paid" in df.columns
