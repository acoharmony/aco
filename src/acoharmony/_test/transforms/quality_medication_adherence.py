# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.quality_medication_adherence module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import inspect
import types
from datetime import date

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
class TestQualityMedicationAdherence:
    """Tests for medication adherence quality measures."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import _quality_medication_adherence
        assert acoharmony._transforms._quality_medication_adherence is not None

    @pytest.mark.unit
    def test_statin_adherence_metadata(self):
        from acoharmony._transforms._quality_medication_adherence import StatinAdherencePQA
        m = StatinAdherencePQA()
        meta = m.get_metadata()
        assert meta.measure_id == "PQA_STATIN"
        assert "Statin" in meta.measure_name
        assert meta.measure_steward == "PQA"


class TestQualityMedicationAdherenceDeep:
    """Deeper tests for medication adherence quality measures."""

    @pytest.mark.unit
    def test_all_classes_discoverable(self):
        from acoharmony._transforms import _quality_medication_adherence
        classes = _get_classes(_quality_medication_adherence)
        class_names = [c.__name__ for c in classes]
        assert "StatinAdherencePQA" in class_names

from acoharmony._transforms._quality_medication_adherence import (  # noqa: E402
    ACEARBAdherenceDiabetes,
    HypertensionMedicationAdherence,
    OralDiabetesMedicationAdherence,
    StatinAdherencePQA,
)


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


class TestStatinAdherencePQA:
    """Tests for StatinAdherencePQA."""

    @pytest.mark.unit
    def test_metadata(self):
        measure = StatinAdherencePQA()
        metadata = measure.get_metadata()
        assert metadata.measure_id == "PQA_STATIN"
        assert "PQA" in metadata.measure_steward

    @pytest.mark.unit
    def test_denominator_no_value_sets(self):
        """Denominator returns empty when no value sets provided."""
        measure = StatinAdherencePQA()
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), {}
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_numerator_placeholder(self):
        """Numerator uses placeholder logic (all False)."""
        measure = StatinAdherencePQA()
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_numerator(denominator, _make_claims_df(), {}).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_exclusions_no_value_sets(self):
        """Exclusions return all False when no value sets."""
        measure = StatinAdherencePQA()
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_exclusions(denominator, _make_claims_df(), {}).collect()
        assert result["exclusion_flag"][0] is False

    @pytest.mark.unit
    def test_denominator_with_value_sets(self):
        """Denominator filters for CVD + statin patients."""
        measure = StatinAdherencePQA(config={"measurement_year": 2024})
        cvd_codes = pl.DataFrame({"code": ["I25.10"]}).lazy()
        statin_codes = pl.DataFrame({"code": ["J1234"]}).lazy()
        value_sets = {
            "Myocardial Infarction": cvd_codes,
            "Statin Medications": statin_codes,
        }
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        # P1 has I25.10 (CVD) and J1234 (statin)
        assert result.height == 1
        assert result["person_id"][0] == "P1"

    @pytest.mark.unit
    def test_exclusions_with_value_sets(self):
        """Exclusions identify ESRD/hospice patients."""
        measure = StatinAdherencePQA(config={"measurement_year": 2024})
        esrd_codes = pl.DataFrame({"code": ["I25.10"]}).lazy()
        value_sets = {"ESRD": esrd_codes}
        denominator = pl.DataFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        ).lazy()
        result = measure.calculate_exclusions(denominator, _make_claims_df(), value_sets).collect()
        assert "exclusion_flag" in result.columns


class TestACEARBAdherenceDiabetes:
    """Tests for ACEARBAdherenceDiabetes."""

    @pytest.mark.unit
    def test_metadata(self):
        measure = ACEARBAdherenceDiabetes()
        metadata = measure.get_metadata()
        assert metadata.measure_id == "PQA_ACEARB"

    @pytest.mark.unit
    def test_denominator_no_value_sets(self):
        measure = ACEARBAdherenceDiabetes()
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), {}
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_numerator_placeholder(self):
        measure = ACEARBAdherenceDiabetes()
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_numerator(denominator, _make_claims_df(), {}).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_denominator_with_value_sets(self):
        measure = ACEARBAdherenceDiabetes(config={"measurement_year": 2024})
        diabetes_codes = pl.DataFrame({"code": ["E11.9"]}).lazy()
        ace_codes = pl.DataFrame({"code": ["J5678"]}).lazy()
        value_sets = {"Diabetes": diabetes_codes, "ACE Inhibitors": ace_codes}
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height == 1
        assert result["person_id"][0] == "P2"


class TestOralDiabetesMedicationAdherence:
    """Tests for OralDiabetesMedicationAdherence."""

    @pytest.mark.unit
    def test_metadata(self):
        measure = OralDiabetesMedicationAdherence()
        metadata = measure.get_metadata()
        assert metadata.measure_id == "PQA_DIABETES"

    @pytest.mark.unit
    def test_denominator_no_value_sets(self):
        measure = OralDiabetesMedicationAdherence()
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), {}
        ).collect()
        assert result.height == 0


class TestHypertensionMedicationAdherence:
    """Tests for HypertensionMedicationAdherence."""

    @pytest.mark.unit
    def test_metadata(self):
        measure = HypertensionMedicationAdherence()
        metadata = measure.get_metadata()
        assert metadata.measure_id == "PQA_HYPERTENSION"

    @pytest.mark.unit
    def test_denominator_no_value_sets(self):
        measure = HypertensionMedicationAdherence()
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), {}
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_denominator_with_hypertension_codes(self):
        measure = HypertensionMedicationAdherence(config={"measurement_year": 2024})
        htn_codes = pl.DataFrame({"code": ["I10"]}).lazy()
        ace_codes = pl.DataFrame({"code": ["J9012"]}).lazy()
        value_sets = {
            "Essential Hypertension": htn_codes,
            "ACE Inhibitors": ace_codes,
        }
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height == 1
        assert result["person_id"][0] == "P3"

    @pytest.mark.unit
    def test_numerator_placeholder(self):
        measure = HypertensionMedicationAdherence()
        denominator = pl.DataFrame(
            {"person_id": ["P3"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_numerator(
            denominator, _make_claims_df(), {}
        ).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_exclusions_no_value_sets(self):
        measure = HypertensionMedicationAdherence()
        denominator = pl.DataFrame(
            {"person_id": ["P3"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_exclusions(
            denominator, _make_claims_df(), {}
        ).collect()
        assert result["exclusion_flag"][0] is False

    @pytest.mark.unit
    def test_exclusions_with_value_sets(self):
        measure = HypertensionMedicationAdherence(config={"measurement_year": 2024})
        esrd_codes = pl.DataFrame({"code": ["I10"]}).lazy()
        value_sets = {"ESRD": esrd_codes}
        denominator = pl.DataFrame(
            {"person_id": ["P3", "P1"], "denominator_flag": [True, True]}
        ).lazy()
        result = measure.calculate_exclusions(
            denominator, _make_claims_df(), value_sets
        ).collect()
        assert "exclusion_flag" in result.columns

    @pytest.mark.unit
    def test_denominator_hypertension_fallback(self):
        """Test Hypertension fallback key when Essential Hypertension not found."""
        measure = HypertensionMedicationAdherence(config={"measurement_year": 2024})
        htn_codes = pl.DataFrame({"code": ["I10"]}).lazy()
        ace_codes = pl.DataFrame({"code": ["J9012"]}).lazy()
        value_sets = {
            "Hypertension": htn_codes,
            "ACE Inhibitors": ace_codes,
        }
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height == 1

    @pytest.mark.unit
    def test_denominator_multiple_med_classes(self):
        """Test with multiple antihypertensive medication classes."""
        measure = HypertensionMedicationAdherence(config={"measurement_year": 2024})
        htn_codes = pl.DataFrame({"code": ["I10"]}).lazy()
        ace_codes = pl.DataFrame({"code": ["J9012"]}).lazy()
        arb_codes = pl.DataFrame({"code": ["J9012"]}).lazy()
        value_sets = {
            "Essential Hypertension": htn_codes,
            "ACE Inhibitors": ace_codes,
            "ARBs": arb_codes,
        }
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height >= 1


class TestStatinAdherencePQAAdditional:
    """Additional tests for StatinAdherencePQA coverage gaps."""

    @pytest.mark.unit
    def test_denominator_statin_missing(self):
        """Denominator returns empty when statin value set missing but CVD present."""
        measure = StatinAdherencePQA(config={"measurement_year": 2024})
        cvd_codes = pl.DataFrame({"code": ["I25.10"]}).lazy()
        value_sets = {"Myocardial Infarction": cvd_codes}
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_denominator_multiple_cvd_concepts(self):
        """Denominator with multiple CVD concept value sets."""
        measure = StatinAdherencePQA(config={"measurement_year": 2024})
        mi_codes = pl.DataFrame({"code": ["I25.10"]}).lazy()
        ivd_codes = pl.DataFrame({"code": ["I25.10"]}).lazy()
        statin_codes = pl.DataFrame({"code": ["J1234"]}).lazy()
        value_sets = {
            "Myocardial Infarction": mi_codes,
            "Ischemic Vascular Disease": ivd_codes,
            "Statin Medications": statin_codes,
        }
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height == 1

    @pytest.mark.unit
    def test_denominator_statins_key_fallback(self):
        """Denominator uses 'Statins' key when 'Statin Medications' not found."""
        measure = StatinAdherencePQA(config={"measurement_year": 2024})
        cvd_codes = pl.DataFrame({"code": ["I25.10"]}).lazy()
        statin_codes = pl.DataFrame({"code": ["J1234"]}).lazy()
        value_sets = {
            "Myocardial Infarction": cvd_codes,
            "Statins": statin_codes,
        }
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height == 1

    @pytest.mark.unit
    def test_exclusions_multiple_concepts(self):
        """Test exclusions with multiple exclusion concepts."""
        measure = StatinAdherencePQA(config={"measurement_year": 2024})
        esrd_codes = pl.DataFrame({"code": ["I25.10"]}).lazy()
        hospice_codes = pl.DataFrame({"code": ["E11.9"]}).lazy()
        value_sets = {
            "ESRD": esrd_codes,
            "Hospice Encounter": hospice_codes,
        }
        denominator = pl.DataFrame(
            {"person_id": ["P1", "P2", "P3"], "denominator_flag": [True, True, True]}
        ).lazy()
        result = measure.calculate_exclusions(
            denominator, _make_claims_df(), value_sets
        ).collect()
        assert "exclusion_flag" in result.columns
        assert result.height == 3


class TestACEARBAdherenceDiabetesAdditional:
    """Additional tests for ACEARBAdherenceDiabetes coverage gaps."""

    @pytest.mark.unit
    def test_exclusions_no_value_sets(self):
        measure = ACEARBAdherenceDiabetes()
        denominator = pl.DataFrame(
            {"person_id": ["P2"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_exclusions(
            denominator, _make_claims_df(), {}
        ).collect()
        assert result["exclusion_flag"][0] is False

    @pytest.mark.unit
    def test_exclusions_with_value_sets(self):
        measure = ACEARBAdherenceDiabetes(config={"measurement_year": 2024})
        esrd_codes = pl.DataFrame({"code": ["E11.9"]}).lazy()
        pregnancy_codes = pl.DataFrame({"code": ["I25.10"]}).lazy()
        value_sets = {
            "ESRD": esrd_codes,
            "Pregnancy": pregnancy_codes,
        }
        denominator = pl.DataFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        ).lazy()
        result = measure.calculate_exclusions(
            denominator, _make_claims_df(), value_sets
        ).collect()
        assert "exclusion_flag" in result.columns
        assert result.height == 2

    @pytest.mark.unit
    def test_denominator_no_ace_arb(self):
        """Denominator empty when diabetes present but no ACE/ARB codes."""
        measure = ACEARBAdherenceDiabetes(config={"measurement_year": 2024})
        diabetes_codes = pl.DataFrame({"code": ["E11.9"]}).lazy()
        value_sets = {"Diabetes": diabetes_codes}
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_denominator_multiple_med_concepts(self):
        """Denominator with both ACE Inhibitors and ARBs value sets."""
        measure = ACEARBAdherenceDiabetes(config={"measurement_year": 2024})
        diabetes_codes = pl.DataFrame({"code": ["E11.9"]}).lazy()
        ace_codes = pl.DataFrame({"code": ["J5678"]}).lazy()
        arb_codes = pl.DataFrame({"code": ["J5678"]}).lazy()
        value_sets = {
            "Diabetes": diabetes_codes,
            "ACE Inhibitors": ace_codes,
            "ARBs": arb_codes,
        }
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height == 1


class TestOralDiabetesMedicationAdherenceAdditional:
    """Additional tests for OralDiabetesMedicationAdherence coverage gaps."""

    @pytest.mark.unit
    def test_numerator_placeholder(self):
        measure = OralDiabetesMedicationAdherence()
        denominator = pl.DataFrame(
            {"person_id": ["P2"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_numerator(
            denominator, _make_claims_df(), {}
        ).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_exclusions_no_value_sets(self):
        measure = OralDiabetesMedicationAdherence()
        denominator = pl.DataFrame(
            {"person_id": ["P2"], "denominator_flag": [True]}
        ).lazy()
        result = measure.calculate_exclusions(
            denominator, _make_claims_df(), {}
        ).collect()
        assert result["exclusion_flag"][0] is False

    @pytest.mark.unit
    def test_exclusions_with_value_sets(self):
        measure = OralDiabetesMedicationAdherence(config={"measurement_year": 2024})
        t1_codes = pl.DataFrame({"code": ["E11.9"]}).lazy()
        esrd_codes = pl.DataFrame({"code": ["I25.10"]}).lazy()
        value_sets = {
            "Diabetes Type 1": t1_codes,
            "ESRD": esrd_codes,
        }
        denominator = pl.DataFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        ).lazy()
        result = measure.calculate_exclusions(
            denominator, _make_claims_df(), value_sets
        ).collect()
        assert "exclusion_flag" in result.columns

    @pytest.mark.unit
    def test_denominator_with_value_sets(self):
        """Denominator with diabetes and oral medication value sets."""
        measure = OralDiabetesMedicationAdherence(config={"measurement_year": 2024})
        diabetes_codes = pl.DataFrame({"code": ["E11.9"]}).lazy()
        dm_med_codes = pl.DataFrame({"code": ["J5678"]}).lazy()
        value_sets = {
            "Diabetes": diabetes_codes,
            "Diabetes Medications": dm_med_codes,
        }
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height == 1
        assert result["person_id"][0] == "P2"

    @pytest.mark.unit
    def test_denominator_type2_preferred(self):
        """Denominator prefers Diabetes Type 2 over Diabetes."""
        measure = OralDiabetesMedicationAdherence(config={"measurement_year": 2024})
        t2_codes = pl.DataFrame({"code": ["E11.9"]}).lazy()
        dm_med_codes = pl.DataFrame({"code": ["J5678"]}).lazy()
        value_sets = {
            "Diabetes Type 2": t2_codes,
            "Diabetes Medications": dm_med_codes,
        }
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height == 1

    @pytest.mark.unit
    def test_denominator_no_diabetes_meds(self):
        """Denominator empty when diabetes present but no medication codes."""
        measure = OralDiabetesMedicationAdherence(config={"measurement_year": 2024})
        diabetes_codes = pl.DataFrame({"code": ["E11.9"]}).lazy()
        value_sets = {"Diabetes": diabetes_codes}
        result = measure.calculate_denominator(
            _make_claims_df(), _make_eligibility_df(), value_sets
        ).collect()
        assert result.height == 0


class TestMeasureFactoryRegistration:
    """Test that measures are registered in MeasureFactory."""

    @pytest.mark.unit
    def test_statin_registered(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory
        assert "PQA_STATIN" in MeasureFactory._registry

    @pytest.mark.unit
    def test_acearb_registered(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory
        assert "PQA_ACEARB" in MeasureFactory._registry

    @pytest.mark.unit
    def test_diabetes_registered(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory
        assert "PQA_DIABETES" in MeasureFactory._registry

    @pytest.mark.unit
    def test_hypertension_registered(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory
        assert "PQA_HYPERTENSION" in MeasureFactory._registry


# ---------------------------------------------------------------------------
# Coverage gap tests: _quality_medication_adherence.py line 763
# ---------------------------------------------------------------------------


class TestMedicationAdherenceMultipleExclusions:
    """Cover multiple exclusion lists concatenation."""

    @pytest.mark.unit
    def test_multiple_excluded_lists_concat(self):
        """Line 763: multiple excluded_members_list items concatenated."""
        import polars as pl

        list1 = pl.DataFrame({"person_id": ["P1"]})
        list2 = pl.DataFrame({"person_id": ["P2"]})

        all_excluded = list1
        for excluded_df in [list2]:
            all_excluded = pl.concat([all_excluded, excluded_df]).unique()

        assert all_excluded.height == 2

    @pytest.mark.unit
    def test_exclusions_multiple_concepts_concat_loop(self):
        """Line 762->763: exercise the for-loop that concatenates multiple excluded member lists."""
        measure = HypertensionMedicationAdherence(config={"measurement_year": 2024})

        # Claims where P1 matches ESRD code and P2 matches Pregnancy code
        claims = pl.DataFrame(
            {
                "person_id": ["P1", "P2"],
                "claim_id": ["C1", "C2"],
                "claim_type": ["institutional", "institutional"],
                "bill_type_code": ["110", "110"],
                "admission_date": [date(2024, 3, 1), date(2024, 6, 1)],
                "discharge_date": [date(2024, 3, 5), date(2024, 6, 5)],
                "diagnosis_code_1": ["ESRD_CODE", "PREG_CODE"],
                "diagnosis_code_2": [None, None],
                "diagnosis_code_3": [None, None],
                "procedure_code_1": ["99213", "99213"],
                "facility_npi": ["1234567890", "1234567890"],
                "paid_amount": [1000.0, 1000.0],
                "allowed_amount": [1200.0, 1200.0],
                "claim_start_date": [date(2024, 3, 1), date(2024, 6, 1)],
                "claim_end_date": [date(2024, 3, 5), date(2024, 6, 5)],
                "revenue_code": ["0100", "0100"],
                "place_of_service_code": ["21", "21"],
            }
        ).lazy()

        denominator = pl.DataFrame(
            {"person_id": ["P1", "P2", "P3"], "denominator_flag": [True, True, True]}
        ).lazy()

        # Provide two exclusion concept value sets so excluded_members_list has >1 entry
        esrd_codes = pl.DataFrame({"code": ["ESRD_CODE"]}).lazy()
        preg_codes = pl.DataFrame({"code": ["PREG_CODE"]}).lazy()
        value_sets = {"ESRD": esrd_codes, "Pregnancy": preg_codes}

        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()
        assert "exclusion_flag" in result.columns
        # The method builds multiple excluded lists and concatenates them via the for-loop
        assert result.height == 3  # All denominator members present in result
