# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.quality_cardiovascular module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
import inspect
import types
from datetime import date, datetime  # noqa: F811

import polars as pl
import pytest
import acoharmony


def _get_classes(mod: types.ModuleType) -> list[type]:
    """Return all classes defined directly in a module."""
    return [
        obj for name, obj in inspect.getmembers(mod, inspect.isclass)
        if obj.__module__ == mod.__name__
    ]


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


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


def _make_claims_df():
    """Create minimal claims DataFrame for quality measure tests."""
    return pl.DataFrame(
        {
            "person_id": ["P1", "P2", "P3"],
            "claim_end_date": [date(2024, 3, 1), date(2024, 6, 1), date(2024, 9, 1)],
            "diagnosis_code_1": ["I25.10", "E11.9", "I10"],
            "procedure_code": ["J1234", "J5678", "J9012"],
        }
    ).lazy()


def _make_eligibility_df():
    """Create minimal eligibility DataFrame."""
    return pl.DataFrame(
        {
            "person_id": ["P1", "P2", "P3"],
            "age": [55, 45, 65],
            "enrollment_start_date": [date(2024, 1, 1)] * 3,
            "enrollment_end_date": [date(2024, 12, 31)] * 3,
        }
    ).lazy()


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestQualityCardiovascular:
    """Tests for cardiovascular quality measures."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import _quality_cardiovascular
        assert acoharmony._transforms._quality_cardiovascular is not None

    @pytest.mark.unit
    def test_controlling_high_blood_pressure_metadata(self):
        from acoharmony._transforms._quality_cardiovascular import ControllingHighBloodPressure
        m = ControllingHighBloodPressure()
        meta = m.get_metadata()
        assert meta.measure_id == "NQF0018"
        assert "Blood Pressure" in meta.measure_name
        assert meta.measure_steward == "NCQA"


class TestQualityCardiovascularDeep:
    """Deeper tests for cardiovascular quality measures."""

    @pytest.mark.unit
    def test_all_classes_discoverable(self):
        from acoharmony._transforms import _quality_cardiovascular
        classes = _get_classes(_quality_cardiovascular)
        class_names = [c.__name__ for c in classes]
        assert "ControllingHighBloodPressure" in class_names

from acoharmony._transforms._quality_cardiovascular import (  # noqa: E402
    ControllingHighBloodPressure,
    IschemicVascularDiseaseAspirin,
    StatinTherapyCardiovascular,
)


class TestControllingHighBloodPressure:
    """Tests for ControllingHighBloodPressure."""

    @pytest.mark.unit
    def test_metadata(self):
        measure = ControllingHighBloodPressure()
        metadata = measure.get_metadata()
        assert metadata.measure_id == "NQF0018"
        assert "NCQA" in metadata.measure_steward

    @pytest.mark.unit
    def test_denominator_no_value_sets(self):
        measure = ControllingHighBloodPressure()
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), {}
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_denominator_with_hypertension(self):
        measure = ControllingHighBloodPressure(config={"measurement_year": 2024})
        htn_codes = pl.DataFrame({"code": ["I10"]}).lazy()
        value_sets = {"Essential Hypertension": htn_codes}
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height == 1
        assert result["person_id"][0] == "P3"

    @pytest.mark.unit
    def test_numerator_placeholder(self):
        measure = ControllingHighBloodPressure()
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_numerator(denominator, _make_claims_df(), {}).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_exclusions_no_value_sets(self):
        measure = ControllingHighBloodPressure()
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_exclusions(denominator, _make_claims_df(), {}).collect()
        assert result["exclusion_flag"][0] is False


class TestIschemicVascularDiseaseAspirin:
    """Tests for IschemicVascularDiseaseAspirin."""

    @pytest.mark.unit
    def test_metadata(self):
        measure = IschemicVascularDiseaseAspirin()
        metadata = measure.get_metadata()
        assert metadata.measure_id == "NQF0068"

    @pytest.mark.unit
    def test_denominator_no_value_sets(self):
        measure = IschemicVascularDiseaseAspirin()
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), {}
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_denominator_with_ivd_codes(self):
        measure = IschemicVascularDiseaseAspirin(config={"measurement_year": 2024})
        ivd_codes = pl.DataFrame({"code": ["I25.10"]}).lazy()
        value_sets = {"Ischemic Vascular Disease": ivd_codes}
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height == 1
        assert result["person_id"][0] == "P1"

    @pytest.mark.unit
    def test_numerator_no_value_sets(self):
        """Numerator with no antiplatelet value sets returns all False."""
        measure = IschemicVascularDiseaseAspirin()
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_numerator(denominator, _make_claims_df(), {}).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_numerator_with_medication_codes(self):
        """Numerator identifies patients with antiplatelet prescriptions."""
        measure = IschemicVascularDiseaseAspirin(config={"measurement_year": 2024})
        aspirin_codes = pl.DataFrame({"code": ["J1234"]}).lazy()
        value_sets = {"Aspirin": aspirin_codes}
        denominator = pl.DataFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        ).lazy()
        result = measure.calculate_numerator(denominator, _make_claims_df(), value_sets).collect()
        assert "numerator_flag" in result.columns


class TestStatinTherapyCardiovascular:
    """Tests for StatinTherapyCardiovascular."""

    @pytest.mark.unit
    def test_metadata(self):
        measure = StatinTherapyCardiovascular()
        metadata = measure.get_metadata()
        assert metadata.measure_id == "NQF0439"

    @pytest.mark.unit
    def test_denominator_no_value_sets(self):
        measure = StatinTherapyCardiovascular()
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), {}
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_numerator_no_statin_codes(self):
        """Numerator with no statin value set returns all False."""
        measure = StatinTherapyCardiovascular()
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_numerator(denominator, _make_claims_df(), {}).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_numerator_with_empty_statin_codes(self):
        """Numerator with empty statin value set returns all False."""
        measure = StatinTherapyCardiovascular(config={"measurement_year": 2024})
        statin_codes = pl.DataFrame({"code": pl.Series([], dtype=pl.String)}).lazy()
        value_sets = {"Statin Medications": statin_codes}
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_numerator(denominator, _make_claims_df(), value_sets).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_exclusions_no_value_sets(self):
        measure = StatinTherapyCardiovascular()
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_exclusions(denominator, _make_claims_df(), {}).collect()
        assert result["exclusion_flag"][0] is False

    @pytest.mark.unit
    def test_full_calculation(self):
        """Full measure calculation with all components."""
        measure = StatinTherapyCardiovascular(config={"measurement_year": 2024})
        cvd_codes = pl.DataFrame({"code": ["I25.10"]}).lazy()
        statin_codes = pl.DataFrame({"code": ["J1234"]}).lazy()
        value_sets = {
            "Myocardial Infarction": cvd_codes,
            "Statin Medications": statin_codes,
        }

        # Build eligibility with age >= 21
        eligibility = pl.DataFrame(
            {
                "person_id": ["P1", "P2"],
                "age": [55, 45],
                "enrollment_start_date": [date(2024, 1, 1)] * 2,
                "enrollment_end_date": [date(2024, 12, 31)] * 2,
            }
        ).lazy()

        result = measure.calculate(
            _make_claims_df(), eligibility, value_sets
        ).collect()
        assert "performance_met" in result.columns


class TestControllingHighBloodPressureExclusions:
    """Test exclusions with various value sets."""

    @pytest.mark.unit
    def test_exclusions_with_esrd_code(self):
        measure = ControllingHighBloodPressure(config={"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        ).lazy()
        claims = pl.DataFrame({
            "person_id": ["P1"],
            "diagnosis_code_1": ["N18.6"],
            "claim_end_date": [date(2024, 5, 1)],
            "procedure_code": ["99213"],
        }).lazy()
        value_sets = {"ESRD": pl.DataFrame({"code": ["N18.6"]}).lazy()}
        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()
        p1 = result.filter(pl.col("person_id") == "P1")
        assert p1["exclusion_flag"][0] is True

    @pytest.mark.unit
    def test_exclusions_with_multiple_concepts(self):
        measure = ControllingHighBloodPressure(config={"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1", "P2", "P3"], "denominator_flag": [True, True, True]}
        ).lazy()
        claims = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "diagnosis_code_1": ["N18.6", "O10"],
            "claim_end_date": [date(2024, 5, 1), date(2024, 6, 1)],
            "procedure_code": ["99213", "99213"],
        }).lazy()
        value_sets = {
            "ESRD": pl.DataFrame({"code": ["N18.6"]}).lazy(),
            "Pregnancy": pl.DataFrame({"code": ["O10"]}).lazy(),
        }
        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()
        assert result.filter(pl.col("person_id") == "P1")["exclusion_flag"][0] is True
        assert result.filter(pl.col("person_id") == "P2")["exclusion_flag"][0] is True

    @pytest.mark.unit
    def test_denominator_with_hypertension_fallback_key(self):
        """Use 'Hypertension' key when 'Essential Hypertension' not present."""
        measure = ControllingHighBloodPressure(config={"measurement_year": 2024})
        htn_codes = pl.DataFrame({"code": ["I10"]}).lazy()
        value_sets = {"Hypertension": htn_codes}
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height == 1


class TestIschemicVascularDiseaseAspirinDeep:
    """Additional coverage for IVD measure."""

    @pytest.mark.unit
    def test_denominator_with_multiple_ivd_concepts(self):
        measure = IschemicVascularDiseaseAspirin(config={"measurement_year": 2024})
        claims = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "diagnosis_code_1": ["I25.10", "I63.9"],
            "procedure_code": ["99213", "99213"],
            "claim_end_date": [date(2024, 3, 1), date(2024, 6, 1)],
        }).lazy()
        eligibility = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "age": [55, 65],
            "enrollment_start_date": [date(2024, 1, 1)] * 2,
            "enrollment_end_date": [date(2024, 12, 31)] * 2,
        }).lazy()
        value_sets = {
            "Ischemic Vascular Disease": pl.DataFrame({"code": ["I25.10"]}).lazy(),
            "Acute Myocardial Infarction": pl.DataFrame({"code": ["I63.9"]}).lazy(),
        }
        result = measure.calculate_denominator(claims, eligibility, value_sets).collect()
        assert result.height == 2

    @pytest.mark.unit
    def test_exclusions_with_bleeding_disorder(self):
        measure = IschemicVascularDiseaseAspirin(config={"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        claims = pl.DataFrame({
            "person_id": ["P1"],
            "diagnosis_code_1": ["D68.9"],
            "claim_end_date": [date(2024, 5, 1)],
            "procedure_code": ["99213"],
        }).lazy()
        value_sets = {
            "Bleeding Disorders": pl.DataFrame({"code": ["D68.9"]}).lazy(),
        }
        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()
        assert result["exclusion_flag"][0] is True

    @pytest.mark.unit
    def test_exclusions_with_multiple_concepts(self):
        measure = IschemicVascularDiseaseAspirin(config={"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        ).lazy()
        claims = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "diagnosis_code_1": ["D68.9", "K27.9"],
            "claim_end_date": [date(2024, 5, 1), date(2024, 6, 1)],
            "procedure_code": ["99213", "99213"],
        }).lazy()
        value_sets = {
            "Bleeding Disorders": pl.DataFrame({"code": ["D68.9"]}).lazy(),
            "Peptic Ulcer": pl.DataFrame({"code": ["K27.9"]}).lazy(),
        }
        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()
        assert result.filter(pl.col("person_id") == "P1")["exclusion_flag"][0] is True
        assert result.filter(pl.col("person_id") == "P2")["exclusion_flag"][0] is True

    @pytest.mark.unit
    def test_exclusions_no_value_sets(self):
        measure = IschemicVascularDiseaseAspirin()
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_exclusions(denominator, _make_claims_df(), {}).collect()
        assert result["exclusion_flag"][0] is False

    @pytest.mark.unit
    def test_numerator_with_multiple_antiplatelet_concepts(self):
        measure = IschemicVascularDiseaseAspirin(config={"measurement_year": 2024})
        claims = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "diagnosis_code_1": ["I25.10", "I25.10"],
            "procedure_code": ["ASP01", "ANTI01"],
            "claim_end_date": [date(2024, 3, 1), date(2024, 6, 1)],
        }).lazy()
        value_sets = {
            "Aspirin": pl.DataFrame({"code": ["ASP01"]}).lazy(),
            "Antiplatelet Medications": pl.DataFrame({"code": ["ANTI01"]}).lazy(),
        }
        denominator = pl.DataFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        ).lazy()
        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        assert result.height == 2

    @pytest.mark.unit
    def test_full_calculation(self):
        measure = IschemicVascularDiseaseAspirin(config={"measurement_year": 2024})
        ivd_codes = pl.DataFrame({"code": ["I25.10"]}).lazy()
        aspirin_codes = pl.DataFrame({"code": ["J1234"]}).lazy()
        value_sets = {
            "Ischemic Vascular Disease": ivd_codes,
            "Aspirin": aspirin_codes,
        }
        eligibility = pl.DataFrame({
            "person_id": ["P1"],
            "age": [55],
            "enrollment_start_date": [date(2024, 1, 1)],
            "enrollment_end_date": [date(2024, 12, 31)],
        }).lazy()
        result = measure.calculate(_make_claims_df(), eligibility, value_sets).collect()
        assert "performance_met" in result.columns


class TestStatinTherapyDeep:
    """Additional coverage for Statin Therapy measure."""

    @pytest.mark.unit
    def test_denominator_with_multiple_cvd_concepts(self):
        measure = StatinTherapyCardiovascular(config={"measurement_year": 2024})
        claims = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "diagnosis_code_1": ["I21.9", "I25.10"],
            "procedure_code": ["99213", "99213"],
            "claim_end_date": [date(2024, 3, 1), date(2024, 6, 1)],
        }).lazy()
        eligibility = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "age": [55, 65],
            "enrollment_start_date": [date(2024, 1, 1)] * 2,
            "enrollment_end_date": [date(2024, 12, 31)] * 2,
        }).lazy()
        value_sets = {
            "Myocardial Infarction": pl.DataFrame({"code": ["I21.9"]}).lazy(),
            "Ischemic Vascular Disease": pl.DataFrame({"code": ["I25.10"]}).lazy(),
        }
        result = measure.calculate_denominator(claims, eligibility, value_sets).collect()
        assert result.height == 2

    @pytest.mark.unit
    def test_numerator_with_statin_codes(self):
        measure = StatinTherapyCardiovascular(config={"measurement_year": 2024})
        statin_codes = pl.DataFrame({"code": ["J1234"]}).lazy()
        value_sets = {"Statin Medications": statin_codes}
        denominator = pl.DataFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        ).lazy()
        result = measure.calculate_numerator(denominator, _make_claims_df(), value_sets).collect()
        assert "numerator_flag" in result.columns
        assert result.height == 2

    @pytest.mark.unit
    def test_exclusions_with_pregnancy(self):
        measure = StatinTherapyCardiovascular(config={"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        claims = pl.DataFrame({
            "person_id": ["P1"],
            "diagnosis_code_1": ["O10"],
            "claim_end_date": [date(2024, 6, 1)],
            "procedure_code": ["99213"],
        }).lazy()
        value_sets = {"Pregnancy": pl.DataFrame({"code": ["O10"]}).lazy()}
        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()
        assert result["exclusion_flag"][0] is True

    @pytest.mark.unit
    def test_exclusions_with_multiple_concepts(self):
        measure = StatinTherapyCardiovascular(config={"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        ).lazy()
        claims = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "diagnosis_code_1": ["O10", "N18.6"],
            "claim_end_date": [date(2024, 6, 1), date(2024, 7, 1)],
            "procedure_code": ["99213", "99213"],
        }).lazy()
        value_sets = {
            "Pregnancy": pl.DataFrame({"code": ["O10"]}).lazy(),
            "ESRD": pl.DataFrame({"code": ["N18.6"]}).lazy(),
        }
        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()
        assert result.filter(pl.col("person_id") == "P1")["exclusion_flag"][0] is True
        assert result.filter(pl.col("person_id") == "P2")["exclusion_flag"][0] is True

    @pytest.mark.unit
    def test_numerator_with_statins_fallback_key(self):
        """Use 'Statins' key when 'Statin Medications' not present."""
        measure = StatinTherapyCardiovascular(config={"measurement_year": 2024})
        statin_codes = pl.DataFrame({"code": ["J1234"]}).lazy()
        value_sets = {"Statins": statin_codes}
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_numerator(denominator, _make_claims_df(), value_sets).collect()
        assert "numerator_flag" in result.columns


class TestCardiovascularMeasureRegistration:
    """Test factory registration."""

    @pytest.mark.unit
    def test_measures_registered(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory
        measures = MeasureFactory.list_measures()
        assert "NQF0018" in measures
        assert "NQF0068" in measures
        assert "NQF0439" in measures

    @pytest.mark.unit
    def test_factory_creates_instances(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory
        m = MeasureFactory.create("NQF0018")
        assert m.metadata.measure_id == "NQF0018"

    @pytest.mark.unit
    def test_ivd_metadata_details(self):
        m = IschemicVascularDiseaseAspirin()
        meta = m.get_metadata()
        assert meta.measure_version == "2024"
        assert "aspirin" in meta.description.lower() or "antiplatelet" in meta.description.lower()

    @pytest.mark.unit
    def test_statin_metadata_details(self):
        m = StatinTherapyCardiovascular()
        meta = m.get_metadata()
        assert meta.measure_version == "2024"
        assert "statin" in meta.description.lower()
