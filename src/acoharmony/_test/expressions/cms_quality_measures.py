# © 2025 HarmonyCares
# All rights reserved.

"""
Tests for CMS quality measures expressions (UAMCC, ACR, HWR).

Each function is tested with hand-crafted inline LazyFrames that exercise
the primary logic path and key branches (planned admission rules, MCC
cohort identification, specialty cohort assignment, exclusions, etc.).
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from acoharmony._expressions._cms_quality_measures import (
    MCC_GROUPS,
    SPECIALTY_COHORTS,
    acr_int_index_admission,
    acr_int_planned_readmission,
    acr_int_specialty_cohort,
    acr_performance_period,
    acr_summary,
    hwr_int_denominator,
    hwr_int_planned_readmission,
    hwr_performance_period,
    hwr_summary,
    stg_medical_claim,
    stg_medical_claim_condition,
    uamcc_int_denominator,
    uamcc_int_denominator_exclusion,
    uamcc_int_mcc_cohort,
    uamcc_int_numerator,
    uamcc_int_outcome_exclusion,
    uamcc_int_person_time,
    uamcc_int_planned_admission,
    uamcc_performance_period,
    uamcc_summary,
)


# ── Shared fixtures ──────────────────────────────────────────────────────────

def _medical_claim(**overrides) -> pl.LazyFrame:
    defaults = {
        "claim_id": "C1",
        "person_id": "P1",
        "claim_start_date": date(2024, 3, 15),
        "claim_end_date": date(2024, 3, 20),
        "hcpcs_code": "99213",
        "place_of_service_code": "21",
    }
    return pl.LazyFrame([{**defaults, **overrides}])


def _condition(**overrides) -> pl.LazyFrame:
    defaults = {
        "claim_id": "C1",
        "normalized_code": "E11.9",
        "condition_rank": 1,
    }
    return pl.LazyFrame([{**defaults, **overrides}])


def _patient(**overrides) -> pl.LazyFrame:
    defaults = {
        "person_id": "P1",
        "birth_date": date(1955, 6, 1),
        "death_date": None,
        "sex": "female",
    }
    return pl.LazyFrame([{**defaults, **overrides}])


def _perf_period(measure_id: str = "UAMCC", py: int = 2024) -> pl.LazyFrame:
    return pl.LazyFrame([{
        "measure_id": measure_id,
        "measure_name": f"{measure_id} Measure",
        "nqf_id": "2888" if measure_id == "UAMCC" else "1789",
        "performance_year": py,
        "performance_period_begin": date(py, 1, 1),
        "performance_period_end": date(py, 12, 31),
        "lookback_period_begin": date(py - 1, 1, 1),
        "lookback_period_end": date(py - 1, 12, 31),
    }])


# ═══════════════════════════════════════════════════════════════════════════
# Staging
# ═══════════════════════════════════════════════════════════════════════════


class TestStgMedicalClaim:
    @pytest.mark.unit
    def test_joins_principal_diagnosis(self):
        _claims = _medical_claim()
        _conds = pl.concat([
            _condition(condition_rank=1, normalized_code="I50.1"),
            _condition(condition_rank=2, normalized_code="E11.9"),
        ])
        _result = stg_medical_claim(_claims, _conds).collect()
        assert _result.height == 1
        assert _result["principal_diagnosis_code"][0] == "I50.1"

    @pytest.mark.unit
    def test_no_condition_yields_null_diagnosis(self):
        _claims = _medical_claim(claim_id="C99")
        _conds = _condition(claim_id="OTHER")
        _result = stg_medical_claim(_claims, _conds).collect()
        assert _result["principal_diagnosis_code"][0] is None


class TestStgMedicalClaimCondition:
    @pytest.mark.unit
    def test_produces_one_row_per_diagnosis(self):
        _claims = _medical_claim()
        _conds = pl.concat([
            _condition(normalized_code="I50.1"),
            _condition(normalized_code="E11.9"),
        ])
        _result = stg_medical_claim_condition(_claims, _conds).collect()
        assert _result.height == 2
        assert set(_result["normalized_code"].to_list()) == {"I50.1", "E11.9"}


# ═══════════════════════════════════════════════════════════════════════════
# UAMCC
# ═══════════════════════════════════════════════════════════════════════════


class TestUamccPerformancePeriod:
    @pytest.mark.unit
    def test_passes_through_period(self):
        _pp = _perf_period("UAMCC", 2024)
        _result = uamcc_performance_period(_pp).collect()
        assert _result.height == 1
        assert _result["performance_year"][0] == 2024


class TestUamccIntMccCohort:
    @pytest.mark.unit
    def test_groups_conditions_by_person(self):
        _stg = pl.LazyFrame([
            {"claim_id": "C1", "person_id": "P1", "claim_start_date": date(2024, 3, 1), "normalized_code": "E11.9"},
            {"claim_id": "C2", "person_id": "P1", "claim_start_date": date(2024, 4, 1), "normalized_code": "I50.1"},
        ])
        _vs = pl.LazyFrame([
            {"icd_10_cm": "E11.9", "chronic_condition_group": "DIABETES", "lookback_years": 1},
            {"icd_10_cm": "I50.1", "chronic_condition_group": "HEART_FAILURE", "lookback_years": 2},
        ])
        _result = uamcc_int_mcc_cohort(_stg, _vs).collect()
        assert _result.height == 2
        assert set(_result["chronic_condition_group"].to_list()) == {"DIABETES", "HEART_FAILURE"}

    @pytest.mark.unit
    def test_unmatched_code_excluded(self):
        _stg = pl.LazyFrame([
            {"claim_id": "C1", "person_id": "P1", "claim_start_date": date(2024, 3, 1), "normalized_code": "Z00.00"},
        ])
        _vs = pl.LazyFrame([
            {"icd_10_cm": "E11.9", "chronic_condition_group": "DIABETES", "lookback_years": 1},
        ])
        _result = uamcc_int_mcc_cohort(_stg, _vs).collect()
        assert _result.height == 0


class TestUamccIntDenominator:
    @pytest.mark.unit
    def test_requires_age_66_and_2_conditions(self):
        _cohort = pl.LazyFrame([
            {"person_id": "P1", "chronic_condition_group": "DIABETES", "qualifying_code": "E11.9", "qualifying_code_date": date(2024, 1, 1), "claim_count": 1, "lookback_years": 1},
            {"person_id": "P1", "chronic_condition_group": "HEART_FAILURE", "qualifying_code": "I50.1", "qualifying_code_date": date(2024, 1, 1), "claim_count": 1, "lookback_years": 2},
            {"person_id": "P2", "chronic_condition_group": "DIABETES", "qualifying_code": "E11.9", "qualifying_code_date": date(2024, 1, 1), "claim_count": 1, "lookback_years": 1},
        ])
        _patients = pl.LazyFrame([
            {"person_id": "P1", "birth_date": date(1955, 1, 1), "death_date": None, "sex": "male"},
            {"person_id": "P2", "birth_date": date(1955, 1, 1), "death_date": None, "sex": "female"},
        ])
        _pp = _perf_period("UAMCC", 2024).collect()
        _result = uamcc_int_denominator(_cohort, _patients, _pp).collect()
        # P1 has 2 conditions → qualifies; P2 has 1 → excluded
        assert _result.height == 1
        assert _result["person_id"][0] == "P1"

    @pytest.mark.unit
    def test_under_66_excluded(self):
        _cohort = pl.LazyFrame([
            {"person_id": "YOUNG", "chronic_condition_group": "DIABETES", "qualifying_code": "E11.9", "qualifying_code_date": date(2024, 1, 1), "claim_count": 1, "lookback_years": 1},
            {"person_id": "YOUNG", "chronic_condition_group": "CKD", "qualifying_code": "N18.6", "qualifying_code_date": date(2024, 1, 1), "claim_count": 1, "lookback_years": 1},
        ])
        _patients = pl.LazyFrame([
            {"person_id": "YOUNG", "birth_date": date(1970, 1, 1), "death_date": None, "sex": "male"},
        ])
        _pp = _perf_period("UAMCC", 2024).collect()
        _result = uamcc_int_denominator(_cohort, _patients, _pp).collect()
        assert _result.height == 0


class TestUamccIntDenominatorExclusion:
    @pytest.mark.unit
    def test_excludes_deceased_before_period(self):
        _denom = pl.LazyFrame([
            {"person_id": "P1", "age_at_period_start": 70, "chronic_condition_count": 3},
        ])
        _patients = pl.LazyFrame([
            {"person_id": "P1", "birth_date": date(1954, 1, 1), "death_date": date(2023, 6, 1), "sex": "male"},
        ])
        _result = uamcc_int_denominator_exclusion(_denom, _patients).collect()
        # Person died before measurement period — should be flagged
        assert _result.height == 1


class TestUamccIntPlannedAdmission:
    @pytest.mark.unit
    def test_rule1_always_planned_procedure(self):
        _stg = pl.LazyFrame([{
            "claim_id": "C1", "person_id": "P1",
            "claim_start_date": date(2024, 5, 1), "claim_end_date": date(2024, 5, 5),
            "hcpcs_code": "TRANSPLANT_PX",
            "place_of_service_code": "21",
            "principal_diagnosis_code": "Z94.0",
        }])
        _paa1 = pl.LazyFrame([{"ccs_procedure_category": "64"}])
        _paa2 = pl.LazyFrame({"ccs_diagnosis_category": pl.Series([], dtype=pl.Utf8)})
        _paa3 = pl.LazyFrame({"code_type": pl.Series([], dtype=pl.Utf8), "category_or_code": pl.Series([], dtype=pl.Utf8)})
        _paa4 = pl.LazyFrame({"code_type": pl.Series([], dtype=pl.Utf8), "category_or_code": pl.Series([], dtype=pl.Utf8)})
        _dx_ccs = pl.LazyFrame([{"icd_10_cm": "Z94.0", "ccs_category": "254"}])
        _px_ccs = pl.LazyFrame([{"icd_10_pcs": "TRANSPLANT_PX", "ccs_category": "64"}])

        _result = uamcc_int_planned_admission(_stg, _paa1, _paa2, _paa3, _paa4, _dx_ccs, _px_ccs).collect()
        assert _result.height == 1
        assert _result["is_planned"][0] == 1
        assert _result["planned_rule"][0] == "RULE1"

    @pytest.mark.unit
    def test_unplanned_when_no_rules_match(self):
        _stg = pl.LazyFrame([{
            "claim_id": "C1", "person_id": "P1",
            "claim_start_date": date(2024, 5, 1), "claim_end_date": date(2024, 5, 5),
            "hcpcs_code": "99213",
            "place_of_service_code": "21",
            "principal_diagnosis_code": "J18.9",
        }])
        _paa1 = pl.LazyFrame({"ccs_procedure_category": pl.Series([], dtype=pl.Utf8)})
        _paa2 = pl.LazyFrame({"ccs_diagnosis_category": pl.Series([], dtype=pl.Utf8)})
        _paa3 = pl.LazyFrame({"code_type": pl.Series([], dtype=pl.Utf8), "category_or_code": pl.Series([], dtype=pl.Utf8)})
        _paa4 = pl.LazyFrame({"code_type": pl.Series([], dtype=pl.Utf8), "category_or_code": pl.Series([], dtype=pl.Utf8)})
        _dx_ccs = pl.LazyFrame([{"icd_10_cm": "J18.9", "ccs_category": "122"}])
        _px_ccs = pl.LazyFrame({"icd_10_pcs": pl.Series([], dtype=pl.Utf8), "ccs_category": pl.Series([], dtype=pl.Utf8)})

        _result = uamcc_int_planned_admission(_stg, _paa1, _paa2, _paa3, _paa4, _dx_ccs, _px_ccs).collect()
        assert _result.height == 1
        assert _result["is_planned"][0] == 0
        assert _result["planned_rule"][0] is None


# ═══════════════════════════════════════════════════════════════════════════
# ACR
# ═══════════════════════════════════════════════════════════════════════════


class TestAcrPerformancePeriod:
    @pytest.mark.unit
    def test_passes_through(self):
        _pp = _perf_period("ACR", 2024)
        _result = acr_performance_period(_pp).collect()
        assert _result.height == 1
        assert _result["measure_id"][0] == "ACR"


class TestAcrIntIndexAdmission:
    @pytest.mark.unit
    def test_identifies_inpatient_index_stay(self):
        _encounters = pl.LazyFrame([{
            "encounter_id": "E1",
            "person_id": "P1",
            "encounter_type": "acute inpatient",
            "encounter_start_date": date(2024, 3, 1),
            "encounter_end_date": date(2024, 3, 5),
            "discharge_disposition_code": "01",
            "drg_code_type": "MS-DRG",
            "drg_code": "470",
            "facility_id": "F1",
            "primary_diagnosis_code": "J18.9",
        }])
        _exclusions = pl.LazyFrame({"ccs_diagnosis_category": pl.Series([], dtype=pl.Utf8)})
        _dx_ccs = pl.LazyFrame([{"icd_10_cm": "J18.9", "ccs_category": "122", "ccs_description": "Pneumonia"}])

        _result = acr_int_index_admission(_encounters, _exclusions, _dx_ccs).collect()
        assert _result.height == 1
        assert _result["person_id"][0] == "P1"
        assert _result["exclusion_flag"][0] == 0


class TestAcrIntSpecialtyCohort:
    @pytest.mark.unit
    def test_assigns_medicine_by_default(self):
        """No matching surgery procedure or CCS cohort → default MEDICINE."""
        _index = pl.LazyFrame([{
            "encounter_id": "E1",
            "person_id": "P1",
            "admission_date": date(2024, 3, 1),
            "discharge_date": date(2024, 3, 5),
            "discharge_disposition_code": "01",
            "facility_id": "F1",
            "principal_diagnosis_code": "J18.9",
            "ccs_diagnosis_category": "122",
            "drg_code_type": "MS-DRG",
            "drg_code": "470",
            "exclusion_flag": 0,
            "exclusion_reason": None,
        }])
        _cohort_ccs = pl.LazyFrame({
            "ccs_category": pl.Series([], dtype=pl.Utf8),
            "specialty_cohort": pl.Series([], dtype=pl.Utf8),
            "procedure_or_diagnosis": pl.Series([], dtype=pl.Utf8),
        })
        _cohort_icd = pl.LazyFrame({"icd_10_pcs": pl.Series([], dtype=pl.Utf8)})
        _procedures = pl.LazyFrame({"encounter_id": pl.Series([], dtype=pl.Utf8), "normalized_code": pl.Series([], dtype=pl.Utf8)})

        _result = acr_int_specialty_cohort(_index, _cohort_ccs, _cohort_icd, _procedures).collect()
        assert _result.height == 1
        assert _result["specialty_cohort"][0] == "MEDICINE"
        assert _result["cohort_assignment_rule"][0] == "DEFAULT_MEDICINE"


# ═══════════════════════════════════════════════════════════════════════════
# HWR
# ═══════════════════════════════════════════════════════════════════════════


class TestHwrPerformancePeriod:
    @pytest.mark.unit
    def test_passes_through(self):
        _pp = pl.LazyFrame([{
            "measure_id": "HWR",
            "measure_name": "Hospital-Wide Readmission",
            "nqf_id": "1789",
            "performance_year": 2024,
            "performance_period_begin": date(2024, 7, 1),
            "performance_period_end": date(2025, 6, 30),
            "lookback_period_begin": date(2024, 1, 1),
            "lookback_period_end": date(2024, 6, 30),
        }])
        _result = hwr_performance_period(_pp).collect()
        assert _result.height == 1


# ═══════════════════════════════════════════════════════════════════════════
# UAMCC pipeline (remaining)
# ═══════════════════════════════════════════════════════════════════════════


class TestUamccIntOutcomeExclusion:
    @pytest.mark.unit
    def test_planned_admission_excluded(self):
        _stg = pl.LazyFrame([{
            "claim_id": "C1", "person_id": "P1",
            "claim_start_date": date(2024, 5, 1), "claim_end_date": date(2024, 5, 5),
            "hcpcs_code": "99213", "place_of_service_code": "21",
            "principal_diagnosis_code": "J18.9",
        }])
        _planned = pl.LazyFrame([{"claim_id": "C1", "person_id": "P1", "admission_date": date(2024, 5, 1), "is_planned": 1, "planned_rule": "RULE1"}])
        _excl_vs = pl.LazyFrame({"exclusion_category": pl.Series([], dtype=pl.Utf8), "category_or_code": pl.Series([], dtype=pl.Utf8)})
        _dx_ccs = pl.LazyFrame([{"icd_10_cm": "J18.9", "ccs_category": "122"}])

        _result = uamcc_int_outcome_exclusion(_stg, _planned, _excl_vs, _dx_ccs).collect()
        assert _result.height == 1
        assert _result["is_planned"][0] == 1

    @pytest.mark.unit
    def test_complication_ccs_excluded(self):
        _stg = pl.LazyFrame([{
            "claim_id": "C2", "person_id": "P1",
            "claim_start_date": date(2024, 6, 1), "claim_end_date": date(2024, 6, 3),
            "hcpcs_code": "99213", "place_of_service_code": "21",
            "principal_diagnosis_code": "T81.4",
        }])
        _planned = pl.LazyFrame({"claim_id": pl.Series([], dtype=pl.Utf8), "person_id": pl.Series([], dtype=pl.Utf8), "admission_date": pl.Series([], dtype=pl.Date), "is_planned": pl.Series([], dtype=pl.Int32), "planned_rule": pl.Series([], dtype=pl.Utf8)})
        _excl_vs = pl.LazyFrame([{"exclusion_category": "Complications of procedures or surgeries", "category_or_code": "238"}])
        _dx_ccs = pl.LazyFrame([{"icd_10_cm": "T81.4", "ccs_category": "238"}])

        _result = uamcc_int_outcome_exclusion(_stg, _planned, _excl_vs, _dx_ccs).collect()
        assert _result.height == 1
        assert _result["is_procedure_complication"][0] == 1


class TestUamccIntPersonTime:
    @pytest.mark.unit
    def test_full_year_no_hospital_days(self):
        _denom = pl.LazyFrame([{"person_id": "P1", "age_at_period_start": 70, "chronic_condition_count": 3}])
        _encounters = pl.LazyFrame({
            "encounter_id": pl.Series([], dtype=pl.Utf8),
            "person_id": pl.Series([], dtype=pl.Utf8),
            "encounter_type": pl.Series([], dtype=pl.Utf8),
            "encounter_start_date": pl.Series([], dtype=pl.Date),
            "encounter_end_date": pl.Series([], dtype=pl.Date),
            "length_of_stay": pl.Series([], dtype=pl.Int64),
        })
        _pp = _perf_period("UAMCC", 2024).collect()

        _result = uamcc_int_person_time(_denom, _encounters, _pp).collect()
        assert _result.height == 1
        assert _result["at_risk_days"][0] == 366  # 2024 is leap year
        assert _result["person_years"][0] == pytest.approx(366 / 365.25, abs=0.01)

    @pytest.mark.unit
    def test_hospital_days_subtracted(self):
        _denom = pl.LazyFrame([{"person_id": "P1", "age_at_period_start": 70, "chronic_condition_count": 3}])
        _encounters = pl.LazyFrame([{
            "encounter_id": "E1", "person_id": "P1",
            "encounter_type": "acute inpatient",
            "encounter_start_date": date(2024, 3, 1),
            "encounter_end_date": date(2024, 3, 11),
            "length_of_stay": 10,
        }])
        _pp = _perf_period("UAMCC", 2024).collect()

        _result = uamcc_int_person_time(_denom, _encounters, _pp).collect()
        assert _result["days_in_hospital"][0] == 10
        assert _result["at_risk_days"][0] == 366 - 10


class TestUamccIntNumerator:
    @pytest.mark.unit
    def test_unplanned_claim_in_denominator_counts(self):
        _stg = pl.LazyFrame([{
            "claim_id": "C1", "person_id": "P1",
            "claim_start_date": date(2024, 5, 1), "claim_end_date": date(2024, 5, 5),
            "hcpcs_code": "99213", "place_of_service_code": "21",
            "principal_diagnosis_code": "J18.9",
        }])
        _denom = pl.LazyFrame([{"person_id": "P1", "age_at_period_start": 70, "chronic_condition_count": 3}])
        _excl = pl.LazyFrame({"claim_id": pl.Series([], dtype=pl.Utf8), "person_id": pl.Series([], dtype=pl.Utf8)})
        _dx_ccs = pl.LazyFrame([{"icd_10_cm": "J18.9", "ccs_category": "122"}])

        _result = uamcc_int_numerator(_stg, _denom, _excl, _dx_ccs).collect()
        assert _result.height == 1
        assert _result["unplanned_admission_flag"][0] == 1

    @pytest.mark.unit
    def test_excluded_claim_not_counted(self):
        _stg = pl.LazyFrame([{
            "claim_id": "C1", "person_id": "P1",
            "claim_start_date": date(2024, 5, 1), "claim_end_date": date(2024, 5, 5),
            "hcpcs_code": "99213", "place_of_service_code": "21",
            "principal_diagnosis_code": "J18.9",
        }])
        _denom = pl.LazyFrame([{"person_id": "P1", "age_at_period_start": 70, "chronic_condition_count": 3}])
        _excl = pl.LazyFrame([{"claim_id": "C1", "person_id": "P1"}])
        _dx_ccs = pl.LazyFrame([{"icd_10_cm": "J18.9", "ccs_category": "122"}])

        _result = uamcc_int_numerator(_stg, _denom, _excl, _dx_ccs).collect()
        assert _result.height == 0


class TestUamccSummary:
    @pytest.mark.unit
    def test_computes_observed_rate(self):
        _numer = pl.LazyFrame([
            {"person_id": "P1", "claim_id": "C1", "admission_date": date(2024, 5, 1), "discharge_date": date(2024, 5, 5), "principal_diagnosis_code": "J18.9", "ccs_diagnosis_category": "122", "unplanned_admission_flag": 1},
            {"person_id": "P1", "claim_id": "C2", "admission_date": date(2024, 8, 1), "discharge_date": date(2024, 8, 3), "principal_diagnosis_code": "I50.1", "ccs_diagnosis_category": "108", "unplanned_admission_flag": 1},
        ])
        _pt = pl.LazyFrame([{"person_id": "P1", "at_risk_days": 340, "person_years": 340 / 365.25, "days_in_hospital": 26, "days_in_snf_rehab": 0, "days_in_buffer": 0, "days_in_hospice": 0}])
        _denom = pl.LazyFrame([{"person_id": "P1", "age_at_period_start": 70, "chronic_condition_count": 3}])
        _pp = _perf_period("UAMCC", 2024).collect()

        _result = uamcc_summary(_numer, _pt, _denom, _pp).collect()
        assert _result.height == 1
        assert _result["observed_admissions"][0] == 2
        assert _result["denominator_count"][0] == 1
        assert _result["observed_rate_per_100"][0] > 0


# ═══════════════════════════════════════════════════════════════════════════
# ACR pipeline (remaining)
# ═══════════════════════════════════════════════════════════════════════════


class TestAcrIntPlannedReadmission:
    @pytest.mark.unit
    def test_readmission_within_30_days_detected(self):
        _encounters = pl.LazyFrame([
            {"encounter_id": "READM", "person_id": "P1", "encounter_type": "acute inpatient", "encounter_start_date": date(2024, 3, 20), "encounter_end_date": date(2024, 3, 25), "discharge_disposition_code": "01", "drg_code_type": "MS-DRG", "drg_code": "470", "facility_id": "F1", "primary_diagnosis_code": "J18.9"},
        ])
        _index = pl.LazyFrame([{
            "encounter_id": "IDX", "person_id": "P1", "admission_date": date(2024, 3, 1), "discharge_date": date(2024, 3, 5),
            "discharge_disposition_code": "01", "facility_id": "F1",
            "principal_diagnosis_code": "I50.1", "ccs_diagnosis_category": "108",
            "drg_code_type": "MS-DRG", "drg_code": "291",
            "exclusion_flag": 0, "exclusion_reason": None,
        }])
        _paa1 = pl.LazyFrame({"ccs_procedure_category": pl.Series([], dtype=pl.Utf8)})
        _paa2 = pl.LazyFrame({"ccs_diagnosis_category": pl.Series([], dtype=pl.Utf8)})
        _paa3 = pl.LazyFrame({"code_type": pl.Series([], dtype=pl.Utf8), "category_or_code": pl.Series([], dtype=pl.Utf8)})
        _paa4 = pl.LazyFrame({"code_type": pl.Series([], dtype=pl.Utf8), "category_or_code": pl.Series([], dtype=pl.Utf8)})
        _dx_ccs = pl.LazyFrame([{"icd_10_cm": "J18.9", "ccs_category": "122", "ccs_description": "Pneumonia"}])
        _px_ccs = pl.LazyFrame({"icd_10_pcs": pl.Series([], dtype=pl.Utf8), "ccs_category": pl.Series([], dtype=pl.Utf8)})

        _result = acr_int_planned_readmission(_encounters, _index, _paa1, _paa2, _paa3, _paa4, _dx_ccs, _px_ccs).collect()
        assert _result.height == 1
        assert _result["unplanned_readmission_flag"][0] == 1


class TestAcrSummary:
    @pytest.mark.unit
    def test_computes_readmission_rate(self):
        _index = pl.LazyFrame([
            {"encounter_id": "E1", "person_id": "P1", "admission_date": date(2024, 3, 1), "discharge_date": date(2024, 3, 5), "discharge_disposition_code": "01", "facility_id": "F1", "principal_diagnosis_code": "I50.1", "ccs_diagnosis_category": "108", "drg_code_type": "MS-DRG", "drg_code": "291", "exclusion_flag": 0, "exclusion_reason": None},
            {"encounter_id": "E2", "person_id": "P2", "admission_date": date(2024, 4, 1), "discharge_date": date(2024, 4, 5), "discharge_disposition_code": "01", "facility_id": "F1", "principal_diagnosis_code": "J18.9", "ccs_diagnosis_category": "122", "drg_code_type": "MS-DRG", "drg_code": "470", "exclusion_flag": 0, "exclusion_reason": None},
        ])
        _readm = pl.LazyFrame([
            {"index_encounter_id": "E1", "readmission_encounter_id": "R1", "person_id": "P1", "index_discharge_date": date(2024, 3, 5), "readmission_date": date(2024, 3, 20), "days_to_readmission": 15, "is_within_30_days": 1, "is_planned": 0, "planned_rule": None, "is_psychiatric_or_rehab": 0, "unplanned_readmission_flag": 1},
            {"index_encounter_id": "E2", "readmission_encounter_id": None, "person_id": "P2", "index_discharge_date": date(2024, 4, 5), "readmission_date": None, "days_to_readmission": None, "is_within_30_days": 0, "is_planned": 0, "planned_rule": None, "is_psychiatric_or_rehab": 0, "unplanned_readmission_flag": 0},
        ])
        _pp = _perf_period("ACR", 2024).collect()

        _result = acr_summary(_index, _readm, _pp).collect()
        assert _result.height == 1
        assert _result["denominator_count"][0] == 2
        assert _result["observed_readmissions"][0] == 1
        assert _result["observed_rate"][0] == pytest.approx(0.5)


# ═══════════════════════════════════════════════════════════════════════════
# HWR pipeline
# ═══════════════════════════════════════════════════════════════════════════


class TestHwrIntDenominator:
    @pytest.mark.unit
    def test_identifies_eligible_encounter(self):
        _encounters = pl.LazyFrame([{
            "encounter_id": "E1", "person_id": "P1",
            "encounter_type": "acute inpatient",
            "encounter_start_date": date(2024, 9, 1),
            "encounter_end_date": date(2024, 9, 5),
            "discharge_disposition_code": "01",
            "drg_code_type": "MS-DRG", "drg_code": "470",
            "facility_id": "F1", "paid_amount": 10000.0,
            "primary_diagnosis_code": "J18.9",
            "attending_provider_id": "NPI1",
            "ccs_diagnosis_category": "122",
        }])
        _excl = pl.LazyFrame({"ccs_diagnosis_category": pl.Series([], dtype=pl.Utf8)})
        _cohort_ccs = pl.LazyFrame({"ccs_category": pl.Series([], dtype=pl.Utf8), "specialty_cohort": pl.Series([], dtype=pl.Utf8), "procedure_or_diagnosis": pl.Series([], dtype=pl.Utf8)})
        _surg_gyn = pl.LazyFrame({"icd_10_pcs": pl.Series([], dtype=pl.Utf8)})
        _procedures = pl.LazyFrame({"encounter_id": pl.Series([], dtype=pl.Utf8), "normalized_code": pl.Series([], dtype=pl.Utf8)})

        _result = hwr_int_denominator(_encounters, _excl, _cohort_ccs, _surg_gyn, _procedures).collect()
        assert _result.height == 1
        assert _result["specialty_cohort"][0] == "MEDICINE"


class TestHwrIntPlannedReadmission:
    @pytest.mark.unit
    def test_no_readmission_yields_zero_flag(self):
        _encounters = pl.LazyFrame({
            "encounter_id": pl.Series([], dtype=pl.Utf8),
            "person_id": pl.Series([], dtype=pl.Utf8),
            "encounter_type": pl.Series([], dtype=pl.Utf8),
            "encounter_start_date": pl.Series([], dtype=pl.Date),
            "encounter_end_date": pl.Series([], dtype=pl.Date),
            "discharge_disposition_code": pl.Series([], dtype=pl.Utf8),
            "drg_code_type": pl.Series([], dtype=pl.Utf8),
            "drg_code": pl.Series([], dtype=pl.Utf8),
            "facility_id": pl.Series([], dtype=pl.Utf8),
            "primary_diagnosis_code": pl.Series([], dtype=pl.Utf8),
        })
        _denom = pl.LazyFrame([{
            "encounter_id": "IDX", "person_id": "P1",
            "admission_date": date(2024, 9, 1), "discharge_date": date(2024, 9, 5),
            "discharge_disposition_code": "01", "facility_id": "F1",
            "principal_diagnosis_code": "J18.9", "ccs_diagnosis_category": "122",
            "drg_code_type": "MS-DRG", "drg_code": "470",
            "specialty_cohort": "MEDICINE", "attending_provider_id": "NPI1",
            "exclusion_flag": 0, "exclusion_reason": None,
        }])
        _paa1 = pl.LazyFrame({"ccs_procedure_category": pl.Series([], dtype=pl.Utf8)})
        _paa2 = pl.LazyFrame({"ccs_diagnosis_category": pl.Series([], dtype=pl.Utf8)})
        _paa3 = pl.LazyFrame({"code_type": pl.Series([], dtype=pl.Utf8), "category_or_code": pl.Series([], dtype=pl.Utf8)})
        _paa4 = pl.LazyFrame({"code_type": pl.Series([], dtype=pl.Utf8), "category_or_code": pl.Series([], dtype=pl.Utf8)})
        _dx_ccs = pl.LazyFrame({"icd_10_cm": pl.Series([], dtype=pl.Utf8), "ccs_category": pl.Series([], dtype=pl.Utf8), "ccs_description": pl.Series([], dtype=pl.Utf8)})

        _result = hwr_int_planned_readmission(_encounters, _denom, _paa1, _paa2, _paa3, _paa4, _dx_ccs).collect()
        assert _result.height == 0


class TestHwrSummary:
    @pytest.mark.unit
    def test_computes_rate(self):
        _denom = pl.LazyFrame([
            {"encounter_id": "E1", "person_id": "P1", "admission_date": date(2024, 9, 1), "discharge_date": date(2024, 9, 5), "discharge_disposition_code": "01", "facility_id": "F1", "principal_diagnosis_code": "J18.9", "ccs_diagnosis_category": "122", "drg_code_type": "MS-DRG", "drg_code": "470", "specialty_cohort": "MEDICINE", "attending_provider_id": "NPI1", "exclusion_flag": 0, "exclusion_reason": None, "attributed_tin": None, "attribution_role": None},
        ])
        _readm = pl.LazyFrame([
            {"index_encounter_id": "E1", "readmission_encounter_id": None, "person_id": "P1", "index_discharge_date": date(2024, 9, 5), "readmission_date": None, "days_to_readmission": None, "is_within_30_days": 0, "is_planned": 0, "is_psychiatric_or_rehab": 0, "unplanned_readmission_flag": 0, "attributed_tin": None},
        ])
        _pp = pl.LazyFrame([{
            "measure_id": "HWR", "measure_name": "HWR", "nqf_id": "1789",
            "performance_year": 2024,
            "performance_period_begin": date(2024, 7, 1),
            "performance_period_end": date(2025, 6, 30),
            "lookback_period_begin": date(2024, 1, 1),
            "lookback_period_end": date(2024, 6, 30),
        }]).collect()

        _result = hwr_summary(_denom, _readm, _pp).collect()
        assert _result.height >= 1
        assert _result["observed_rate"][0] == pytest.approx(0.0)


# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════


class TestConstants:
    @pytest.mark.unit
    def test_mcc_groups_has_9_conditions(self):
        assert len(MCC_GROUPS) == 9
        assert "DIABETES" in MCC_GROUPS
        assert "HEART_FAILURE" in MCC_GROUPS

    @pytest.mark.unit
    def test_specialty_cohorts_has_5(self):
        assert len(SPECIALTY_COHORTS) == 5
        assert SPECIALTY_COHORTS[0] == "SURGERY_GYNECOLOGY"
        assert SPECIALTY_COHORTS[-1] == "MEDICINE"

    @pytest.mark.unit
    def test_all_cohort_names_uppercase(self):
        for c in SPECIALTY_COHORTS:
            assert c == c.upper()
        for g in MCC_GROUPS:
            assert g == g.upper()
