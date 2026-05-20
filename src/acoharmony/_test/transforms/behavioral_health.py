# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.behavioral_health module."""

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

DEFAULT_CONFIG = {"measurement_year": 2024}






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


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestBehavioralHealthPublic:
    """Tests for behavioral_health public transform module."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import behavioral_health
        assert behavioral_health is not None

    @pytest.mark.unit
    def test_behavioral_health_transform_class(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform
        assert BehavioralHealthTransform is not None


def _make_bh_claims(rows: list[dict] | None = None) -> pl.LazyFrame:
    """Claims with diagnosis columns suitable for behavioral health tests."""
    if rows is None:
        rows = [
            {"person_id": "P001", "claim_id": "C001", "diagnosis_code_1": "F32.1",
             "diagnosis_code_2": None, "diagnosis_code_3": None,
             "claim_end_date": date(2024, 6, 1), "claim_type": "professional",
             "bill_type_code": "110", "place_of_service_code": "11",
             "paid_amount": 200.0},
            {"person_id": "P002", "claim_id": "C002", "diagnosis_code_1": "F10.20",
             "diagnosis_code_2": None, "diagnosis_code_3": None,
             "claim_end_date": date(2024, 7, 1), "claim_type": "institutional",
             "bill_type_code": "111", "place_of_service_code": "21",
             "paid_amount": 5000.0},
            {"person_id": "P003", "claim_id": "C003", "diagnosis_code_1": "J18.9",
             "diagnosis_code_2": "F41.1", "diagnosis_code_3": None,
             "claim_end_date": date(2024, 8, 1), "claim_type": "professional",
             "bill_type_code": "110", "place_of_service_code": "11",
             "paid_amount": 150.0},
        ]
    defaults = {
        "person_id": "P001", "claim_id": "C001",
        "diagnosis_code_1": "J18.9", "diagnosis_code_2": None, "diagnosis_code_3": None,
        "claim_end_date": date(2024, 6, 1), "claim_type": "professional",
        "bill_type_code": "110", "place_of_service_code": "11", "paid_amount": 100.0,
    }
    filled = [{**defaults, **r} for r in rows]
    return pl.DataFrame(filled).lazy()


class TestBehavioralHealthIdentifyConditions:

    @pytest.mark.unit
    def test_identifies_f32_depression(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = _make_bh_claims()
        result = BehavioralHealthTransform.identify_behavioral_health_conditions(claims, DEFAULT_CONFIG)
        df = result.collect()
        assert df.height >= 1
        assert "bh_diagnosis_code" in df.columns

    @pytest.mark.unit
    def test_identifies_from_diagnosis_code_2(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = _make_bh_claims([
            {"person_id": "P001", "claim_id": "C001",
             "diagnosis_code_1": "J18.9", "diagnosis_code_2": "F41.0",
             "diagnosis_code_3": None, "claim_end_date": date(2024, 5, 1)},
        ])
        df = BehavioralHealthTransform.identify_behavioral_health_conditions(claims, DEFAULT_CONFIG).collect()
        assert df.height == 1
        assert df["bh_diagnosis_code"][0] == "F41.0"

    @pytest.mark.unit
    def test_identifies_from_diagnosis_code_3(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = _make_bh_claims([
            {"person_id": "P001", "claim_id": "C001",
             "diagnosis_code_1": "J18.9", "diagnosis_code_2": "E11.9",
             "diagnosis_code_3": "F31.0", "claim_end_date": date(2024, 5, 1)},
        ])
        df = BehavioralHealthTransform.identify_behavioral_health_conditions(claims, DEFAULT_CONFIG).collect()
        assert df.height == 1
        assert df["bh_diagnosis_code"][0] == "F31.0"

    @pytest.mark.unit
    def test_filters_measurement_year(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = _make_bh_claims([
            {"person_id": "P001", "claim_id": "C001",
             "diagnosis_code_1": "F32.1", "claim_end_date": date(2023, 6, 1)},
        ])
        df = BehavioralHealthTransform.identify_behavioral_health_conditions(claims, DEFAULT_CONFIG).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_non_bh_diagnosis_excluded(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = _make_bh_claims([
            {"person_id": "P001", "claim_id": "C001",
             "diagnosis_code_1": "E11.9", "diagnosis_code_2": "J18.9",
             "diagnosis_code_3": "M54.5", "claim_end_date": date(2024, 5, 1)},
        ])
        df = BehavioralHealthTransform.identify_behavioral_health_conditions(claims, DEFAULT_CONFIG).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_substance_use_codes_identified(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = _make_bh_claims([
            {"person_id": "P001", "claim_id": "C001",
             "diagnosis_code_1": "F11.20", "claim_end_date": date(2024, 5, 1)},
        ])
        df = BehavioralHealthTransform.identify_behavioral_health_conditions(claims, DEFAULT_CONFIG).collect()
        assert df.height == 1
        assert df["bh_diagnosis_code"][0] == "F11.20"

    @pytest.mark.unit
    def test_crisis_codes_identified(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = _make_bh_claims([
            {"person_id": "P001", "claim_id": "C001",
             "diagnosis_code_1": "R45.851", "claim_end_date": date(2024, 5, 1)},
        ])
        df = BehavioralHealthTransform.identify_behavioral_health_conditions(claims, DEFAULT_CONFIG).collect()
        assert df.height == 1


class TestBehavioralHealthCategorize:

    def _categorized_input(self, bh_code: str) -> pl.LazyFrame:
        return pl.DataFrame({
            "person_id": ["P001"],
            "claim_id": ["C001"],
            "bh_diagnosis_code": [bh_code],
            "claim_end_date": [date(2024, 5, 1)],
        }).lazy()

    @pytest.mark.unit
    def test_depression_category(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        df = BehavioralHealthTransform.categorize_behavioral_health_conditions(
            self._categorized_input("F32.1"), DEFAULT_CONFIG,
        ).collect()
        assert df["bh_condition_category"][0] == "depression"
        assert df["bh_high_level_category"][0] == "mental_health_disorder"

    @pytest.mark.unit
    def test_anxiety_category(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        df = BehavioralHealthTransform.categorize_behavioral_health_conditions(
            self._categorized_input("F41.0"), DEFAULT_CONFIG,
        ).collect()
        assert df["bh_condition_category"][0] == "anxiety"

    @pytest.mark.unit
    def test_bipolar_category(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        df = BehavioralHealthTransform.categorize_behavioral_health_conditions(
            self._categorized_input("F31.0"), DEFAULT_CONFIG,
        ).collect()
        assert df["bh_condition_category"][0] == "bipolar"

    @pytest.mark.unit
    def test_schizophrenia_category(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        df = BehavioralHealthTransform.categorize_behavioral_health_conditions(
            self._categorized_input("F20.0"), DEFAULT_CONFIG,
        ).collect()
        assert df["bh_condition_category"][0] == "schizophrenia"

    @pytest.mark.unit
    def test_opioid_use_disorder_category(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        df = BehavioralHealthTransform.categorize_behavioral_health_conditions(
            self._categorized_input("F11.20"), DEFAULT_CONFIG,
        ).collect()
        assert df["bh_condition_category"][0] == "opioid_use_disorder"
        assert df["bh_high_level_category"][0] == "substance_use_disorder"

    @pytest.mark.unit
    def test_alcohol_use_disorder_category(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        df = BehavioralHealthTransform.categorize_behavioral_health_conditions(
            self._categorized_input("F10.20"), DEFAULT_CONFIG,
        ).collect()
        assert df["bh_condition_category"][0] == "alcohol_use_disorder"

    @pytest.mark.unit
    def test_suicidal_ideation_crisis(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        df = BehavioralHealthTransform.categorize_behavioral_health_conditions(
            self._categorized_input("R45.851"), DEFAULT_CONFIG,
        ).collect()
        assert df["bh_condition_category"][0] == "suicidal_ideation"
        assert df["bh_high_level_category"][0] == "crisis"

    @pytest.mark.unit
    def test_self_harm_crisis(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        df = BehavioralHealthTransform.categorize_behavioral_health_conditions(
            self._categorized_input("X71"), DEFAULT_CONFIG,
        ).collect()
        assert df["bh_condition_category"][0] == "self_harm"
        assert df["bh_high_level_category"][0] == "crisis"

    @pytest.mark.unit
    def test_tobacco_use_disorder(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        df = BehavioralHealthTransform.categorize_behavioral_health_conditions(
            self._categorized_input("F17.210"), DEFAULT_CONFIG,
        ).collect()
        assert df["bh_condition_category"][0] == "tobacco_use_disorder"

    @pytest.mark.unit
    def test_other_substance_use(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        df = BehavioralHealthTransform.categorize_behavioral_health_conditions(
            self._categorized_input("F13.20"), DEFAULT_CONFIG,
        ).collect()
        assert df["bh_condition_category"][0] == "other_substance_use_disorder"


class TestBehavioralHealthMemberProfile:

    def _make_categorized_claims(self) -> pl.LazyFrame:
        return pl.DataFrame({
            "person_id": ["P001", "P001", "P001", "P001", "P002"],
            "claim_id": ["C1", "C2", "C3", "C4", "C5"],
            "bh_condition_category": ["depression", "anxiety", "depression", "depression", "suicidal_ideation"],
            "bh_high_level_category": ["mental_health_disorder", "mental_health_disorder",
                                       "mental_health_disorder", "mental_health_disorder", "crisis"],
            "claim_end_date": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1),
                               date(2024, 4, 1), date(2024, 5, 1)],
        }).lazy()

    @pytest.mark.unit
    def test_member_profile_output_columns(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        result = BehavioralHealthTransform.calculate_member_bh_profile(
            self._make_categorized_claims(), DEFAULT_CONFIG,
        ).collect()
        expected = {
            "person_id", "bh_conditions", "unique_bh_conditions", "bh_claim_count",
            "first_bh_service_date", "last_bh_service_date",
            "has_crisis_condition", "has_substance_use_disorder",
            "has_mental_health_disorder", "has_dual_diagnosis",
            "has_engagement", "bh_complexity",
        }
        assert expected.issubset(set(result.columns))

    @pytest.mark.unit
    def test_engagement_flag(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        result = BehavioralHealthTransform.calculate_member_bh_profile(
            self._make_categorized_claims(), DEFAULT_CONFIG,
        ).collect()
        p001 = result.filter(pl.col("person_id") == "P001")
        assert p001["has_engagement"][0] is True  # 4 claims
        p002 = result.filter(pl.col("person_id") == "P002")
        assert p002["has_engagement"][0] is False  # 1 claim

    @pytest.mark.unit
    def test_crisis_condition_flag(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        result = BehavioralHealthTransform.calculate_member_bh_profile(
            self._make_categorized_claims(), DEFAULT_CONFIG,
        ).collect()
        p002 = result.filter(pl.col("person_id") == "P002")
        assert p002["has_crisis_condition"][0] is True

    @pytest.mark.unit
    def test_complexity_high_for_crisis(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        result = BehavioralHealthTransform.calculate_member_bh_profile(
            self._make_categorized_claims(), DEFAULT_CONFIG,
        ).collect()
        p002 = result.filter(pl.col("person_id") == "P002")
        assert p002["bh_complexity"][0] == "high"

    @pytest.mark.unit
    def test_dual_diagnosis(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = pl.DataFrame({
            "person_id": ["P001", "P001"],
            "claim_id": ["C1", "C2"],
            "bh_condition_category": ["depression", "alcohol_use_disorder"],
            "bh_high_level_category": ["mental_health_disorder", "substance_use_disorder"],
            "claim_end_date": [date(2024, 1, 1), date(2024, 2, 1)],
        }).lazy()
        result = BehavioralHealthTransform.calculate_member_bh_profile(claims, DEFAULT_CONFIG).collect()
        assert result["has_dual_diagnosis"][0] is True


class TestBehavioralHealthServiceUtilization:

    @pytest.mark.unit
    def test_service_utilization_output(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = pl.DataFrame({
            "person_id": ["P001", "P001", "P002"],
            "claim_type": ["professional", "institutional", "professional"],
            "bill_type_code": ["110", "111", "110"],
            "place_of_service_code": ["11", "21", "02"],
            "paid_amount": [200.0, 5000.0, 150.0],
            "bh_diagnosis_code": ["F32.1", "F10.20", "F41.1"],
        }).lazy()
        result = BehavioralHealthTransform.calculate_service_utilization(claims, DEFAULT_CONFIG).collect()
        assert "person_id" in result.columns
        assert "bh_service_type" in result.columns
        assert "visit_count" in result.columns
        assert "total_cost" in result.columns

    @pytest.mark.unit
    def test_service_type_office_based(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = pl.DataFrame({
            "person_id": ["P001"],
            "claim_type": ["professional"],
            "bill_type_code": ["110"],
            "place_of_service_code": ["11"],
            "paid_amount": [200.0],
        }).lazy()
        result = BehavioralHealthTransform.calculate_service_utilization(claims, DEFAULT_CONFIG).collect()
        assert result["bh_service_type"][0] == "office_based"

    @pytest.mark.unit
    def test_service_type_telehealth(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = pl.DataFrame({
            "person_id": ["P001"],
            "claim_type": ["professional"],
            "bill_type_code": ["110"],
            "place_of_service_code": ["02"],
            "paid_amount": [100.0],
        }).lazy()
        result = BehavioralHealthTransform.calculate_service_utilization(claims, DEFAULT_CONFIG).collect()
        assert result["bh_service_type"][0] == "telehealth"

    @pytest.mark.unit
    def test_service_type_inpatient_psychiatric(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = pl.DataFrame({
            "person_id": ["P001"],
            "claim_type": ["institutional"],
            "bill_type_code": ["111"],
            "place_of_service_code": ["21"],
            "paid_amount": [5000.0],
        }).lazy()
        result = BehavioralHealthTransform.calculate_service_utilization(claims, DEFAULT_CONFIG).collect()
        assert result["bh_service_type"][0] == "inpatient_psychiatric"


class TestBehavioralHealthConditionPrevalence:

    @pytest.mark.unit
    def test_prevalence_output(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = pl.DataFrame({
            "person_id": ["P001", "P001", "P002", "P003"],
            "bh_condition_category": ["depression", "depression", "depression", "anxiety"],
            "paid_amount": [100.0, 200.0, 150.0, 300.0],
        }).lazy()
        result = BehavioralHealthTransform.calculate_condition_prevalence(claims, DEFAULT_CONFIG).collect()
        assert "bh_condition_category" in result.columns
        assert "member_count" in result.columns
        assert "total_claims" in result.columns
        assert "total_cost" in result.columns

    @pytest.mark.unit
    def test_prevalence_sorted_descending(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = pl.DataFrame({
            "person_id": ["P001", "P002", "P003", "P004"],
            "bh_condition_category": ["depression", "depression", "depression", "anxiety"],
            "paid_amount": [100.0] * 4,
        }).lazy()
        result = BehavioralHealthTransform.calculate_condition_prevalence(claims, DEFAULT_CONFIG).collect()
        assert result["bh_condition_category"][0] == "depression"
        assert result["member_count"][0] == 3


class TestBehavioralHealthComprehensive:

    @pytest.mark.unit
    def test_analyze_behavioral_health_returns_tuple_of_four(self):
        from acoharmony._transforms.behavioral_health import BehavioralHealthTransform

        claims = _make_bh_claims([
            {"person_id": "P001", "claim_id": "C001", "diagnosis_code_1": "F32.1",
             "claim_end_date": date(2024, 6, 1), "claim_type": "professional",
             "bill_type_code": "110", "place_of_service_code": "11", "paid_amount": 200.0},
            {"person_id": "P002", "claim_id": "C002", "diagnosis_code_1": "F10.20",
             "claim_end_date": date(2024, 7, 1), "claim_type": "institutional",
             "bill_type_code": "111", "place_of_service_code": "21", "paid_amount": 5000.0},
        ])
        eligibility = _make_eligibility()
        result = BehavioralHealthTransform.analyze_behavioral_health(claims, eligibility, DEFAULT_CONFIG)
        assert isinstance(result, tuple)
        assert len(result) == 4
        member_profile, service_util, prevalence, bh_claims = result
        assert isinstance(member_profile, pl.LazyFrame)
        assert isinstance(service_util, pl.LazyFrame)
        assert isinstance(prevalence, pl.LazyFrame)
        assert isinstance(bh_claims, pl.LazyFrame)
