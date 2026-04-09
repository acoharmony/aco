from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from acoharmony._expressions._acr_readmission import AcrReadmissionExpression


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------


def _config(**overrides) -> dict[str, Any]:
    cfg = {
        "performance_year": 2025,
        "lookback_days": 30,
        "min_age": 65,
        "patient_id_column": "person_id",
        "admission_date_column": "admission_date",
        "discharge_date_column": "discharge_date",
        "diagnosis_column": "diagnosis_code_1",
    }
    cfg.update(overrides)
    return cfg


def _empty_vs() -> dict[str, pl.LazyFrame]:
    return {
        "ccs_icd10_cm": pl.DataFrame().lazy(),
        "exclusions": pl.DataFrame().lazy(),
        "cohort_icd10": pl.DataFrame().lazy(),
        "cohort_ccs": pl.DataFrame().lazy(),
        "paa2": pl.DataFrame().lazy(),
    }


def _index_lf(**overrides):
    """Standard single-row index admissions LazyFrame."""
    data: dict[str, Any] = {
        "claim_id": ["C001"],
        "person_id": ["P001"],
        "admission_date": [date(2025, 3, 1)],
        "discharge_date": [date(2025, 3, 5)],
        "exclusion_flag": [False],
        "ccs_diagnosis_category": ["CCS100"],
        "discharge_status_code": ["01"],
        "facility_id": ["NPI001"],
        "principal_diagnosis_code": ["A01"],
        "age_at_admission": [75],
    }
    data.update(overrides)
    return pl.DataFrame(data).lazy()


def _claims_lf(**overrides) -> pl.LazyFrame:
    data: dict[str, Any] = {
        "claim_id": ["C001"],
        "person_id": ["P001"],
        "admission_date": [date(2025, 3, 15)],
        "discharge_date": [date(2025, 3, 20)],
        "diagnosis_code_1": ["A01"],
        "bill_type_code": ["111"],
        "facility_npi": ["NPI001"],
        "discharge_status_code": ["01"],
    }
    data.update(overrides)
    return pl.DataFrame(data).lazy()


def _eligibility_lf(**overrides) -> pl.LazyFrame:
    data: dict[str, Any] = {
        "person_id": ["P001"],
        "birth_date": [date(1950, 1, 1)],
    }
    data.update(overrides)
    return pl.DataFrame(data).lazy()


# ---------------------------------------------------------------------------
# Existing tests (fixed)
# ---------------------------------------------------------------------------


class TestAcrReadmissionExpression:

    @pytest.mark.unit
    def test_build(self):
        result = AcrReadmissionExpression.build({})
        assert "config" in result

    @pytest.mark.unit
    def test_load_acr_value_sets_missing(self):
        result = AcrReadmissionExpression.load_acr_value_sets(Path("/nonexistent"))
        assert "ccs_icd10_cm" in result
        for _key, lf in result.items():
            assert lf.collect().height == 0


class TestCalculateAcrSummaryEdgeCases:
    """Additional edge cases for calculate_acr_summary."""

    @pytest.mark.unit
    def test_no_unplanned_readmissions(self):
        """All readmissions are planned, observed_readmissions should be 0."""
        index = pl.DataFrame(
            {"claim_id": ["C001", "C002"], "exclusion_flag": [False, False]}
        ).lazy()
        readmission_pairs = pl.DataFrame(
            {
                "readmission_claim_id": ["C003"],
                "unplanned_readmission_flag": [False],
            }
        ).lazy()
        config = _config()
        result = AcrReadmissionExpression.calculate_acr_summary(
            index, readmission_pairs, config
        ).collect()
        assert result["observed_readmissions"][0] == 0
        assert result["observed_rate"][0] == 0.0


# ---------------------------------------------------------------------------
# Branch-coverage tests for load_acr_value_sets  (lines 127-137)
# ---------------------------------------------------------------------------


class TestLoadAcrValueSets:
    """Cover branches 127->128, 127->137, 130->131, 130->133."""

    @pytest.mark.unit
    def test_file_exists(self, tmp_path):
        """130->131: file exists -> scan_parquet succeeds."""
        # Create a real parquet file for one of the value sets
        df = pl.DataFrame({"icd_10_cm": ["A01"], "ccs_category": ["CCS100"]})
        df.write_parquet(tmp_path / "value_sets_acr_ccs_icd10_cm.parquet")
        result = AcrReadmissionExpression.load_acr_value_sets(tmp_path)
        assert result["ccs_icd10_cm"].collect().height == 1
        # Other value sets should be empty (files don't exist -> 130->133)
        assert result["exclusions"].collect().height == 0
        assert result["cohort_icd10"].collect().height == 0

    @pytest.mark.unit
    def test_file_does_not_exist(self, tmp_path):
        """130->133: file does not exist -> empty DataFrame."""
        result = AcrReadmissionExpression.load_acr_value_sets(tmp_path)
        for _key, lf in result.items():
            assert lf.collect().height == 0

    @pytest.mark.unit
    def test_exception_triggers_fallback(self, tmp_path, monkeypatch):
        """127->128 exception path (line 134-135): exception falls back to empty DF."""
        import polars as _pl

        # Force scan_parquet to raise so the except block fires
        real_scan = _pl.scan_parquet

        def _boom(path, **kwargs):
            if "ccs_icd10_cm" in str(path):
                raise OSError("simulated read error")
            return real_scan(path, **kwargs)

        monkeypatch.setattr(_pl, "scan_parquet", _boom)

        # Put a real file so exists() returns True but scan_parquet raises
        (tmp_path / "value_sets_acr_ccs_icd10_cm.parquet").write_bytes(b"x")
        result = AcrReadmissionExpression.load_acr_value_sets(tmp_path)
        # Should fall back to empty DataFrame
        assert result["ccs_icd10_cm"].collect().height == 0


# ---------------------------------------------------------------------------
# Branch-coverage tests for identify_index_admissions  (lines 203-231)
# ---------------------------------------------------------------------------


class TestIdentifyIndexAdmissions:
    """Cover branches 203->204/215, 221->222/231."""

    def _base_claims(self):
        return pl.DataFrame(
            {
                "claim_id": ["C001"],
                "person_id": ["P001"],
                "admission_date": [date(2025, 3, 15)],
                "discharge_date": [date(2025, 3, 20)],
                "diagnosis_code_1": ["A01"],
                "bill_type_code": ["111"],
                "facility_npi": ["NPI001"],
                "discharge_status_code": ["01"],
            }
        ).lazy()

    def _base_eligibility(self):
        return pl.DataFrame(
            {
                "person_id": ["P001"],
                "birth_date": [date(1950, 1, 1)],
            }
        ).lazy()

    @pytest.mark.unit
    def test_with_ccs_mapping_populated(self):
        """203->204: ccs_mapping not None and height > 0 -> join."""
        vs = _empty_vs()
        vs["ccs_icd10_cm"] = pl.DataFrame(
            {"icd_10_cm": ["A01"], "ccs_category": ["CCS100"]}
        ).lazy()
        result = AcrReadmissionExpression.identify_index_admissions(
            self._base_claims(), self._base_eligibility(), vs, _config()
        ).collect()
        assert "ccs_diagnosis_category" in result.columns
        assert result["ccs_diagnosis_category"][0] == "CCS100"

    @pytest.mark.unit
    def test_with_ccs_mapping_empty(self):
        """203->215: ccs_mapping is empty -> add null column."""
        vs = _empty_vs()
        result = AcrReadmissionExpression.identify_index_admissions(
            self._base_claims(), self._base_eligibility(), vs, _config()
        ).collect()
        assert "ccs_diagnosis_category" in result.columns
        assert result["ccs_diagnosis_category"][0] is None

    @pytest.mark.unit
    def test_with_exclusions_populated(self):
        """221->222: exclusions not None and height > 0 -> join exclusion flag."""
        vs = _empty_vs()
        vs["ccs_icd10_cm"] = pl.DataFrame(
            {"icd_10_cm": ["A01"], "ccs_category": ["CCS100"]}
        ).lazy()
        vs["exclusions"] = pl.DataFrame(
            {"ccs_diagnosis_category": ["CCS100"]}
        ).lazy()
        result = AcrReadmissionExpression.identify_index_admissions(
            self._base_claims(), self._base_eligibility(), vs, _config()
        ).collect()
        assert result["exclusion_flag"][0] is True

    @pytest.mark.unit
    def test_with_exclusions_empty(self):
        """221->231: exclusions is empty -> add False exclusion_flag."""
        vs = _empty_vs()
        result = AcrReadmissionExpression.identify_index_admissions(
            self._base_claims(), self._base_eligibility(), vs, _config()
        ).collect()
        assert result["exclusion_flag"][0] is False


# ---------------------------------------------------------------------------
# Branch-coverage tests for assign_specialty_cohorts  (lines 291-373)
# ---------------------------------------------------------------------------


class TestAssignSpecialtyCohorts:
    """Cover branches in assign_specialty_cohorts."""

    @pytest.mark.unit
    def test_cohort_icd10_populated_with_proc_cols(self):
        """291->293, 300->301, 302->303, 302->308: cohort_icd10 populated, proc cols exist."""
        index = _index_lf()
        claims = pl.DataFrame(
            {
                "claim_id": ["C001"],
                "person_id": ["P001"],
                "admission_date": [date(2025, 3, 15)],
                "discharge_date": [date(2025, 3, 20)],
                "diagnosis_code_1": ["A01"],
                "bill_type_code": ["111"],
                "facility_npi": ["NPI001"],
                "discharge_status_code": ["01"],
                "procedure_code_1": ["0016070"],
            }
        ).lazy()
        vs = _empty_vs()
        vs["cohort_icd10"] = pl.DataFrame(
            {
                "icd_10_pcs": ["0016070"],
                "specialty_cohort": ["SURGERY_GYNECOLOGY"],
            }
        ).lazy()
        result = AcrReadmissionExpression.assign_specialty_cohorts(
            index, claims, vs, _config()
        ).collect()
        assert result["specialty_cohort"][0] == "SURGERY_GYNECOLOGY"

    @pytest.mark.unit
    def test_cohort_icd10_empty(self):
        """291->329: cohort_icd10 is empty -> skip surgery assignment."""
        index = _index_lf()
        claims = _claims_lf()
        vs = _empty_vs()
        result = AcrReadmissionExpression.assign_specialty_cohorts(
            index, claims, vs, _config()
        ).collect()
        assert result["specialty_cohort"][0] == "MEDICINE"

    @pytest.mark.unit
    def test_cohort_icd10_no_proc_cols(self):
        """300->329: available_proc_cols is empty -> surgery_claim_ids stays None."""
        index = _index_lf()
        # Claims with no procedure_code_N columns at all
        claims = pl.DataFrame(
            {
                "claim_id": ["C001"],
                "person_id": ["P001"],
                "admission_date": [date(2025, 3, 15)],
                "discharge_date": [date(2025, 3, 20)],
                "diagnosis_code_1": ["A01"],
                "bill_type_code": ["111"],
                "facility_npi": ["NPI001"],
                "discharge_status_code": ["01"],
            }
        ).lazy()
        vs = _empty_vs()
        vs["cohort_icd10"] = pl.DataFrame(
            {
                "icd_10_pcs": ["0016070"],
                "specialty_cohort": ["SURGERY_GYNECOLOGY"],
            }
        ).lazy()
        result = AcrReadmissionExpression.assign_specialty_cohorts(
            index, claims, vs, _config()
        ).collect()
        # No proc cols -> can't match surgery -> default MEDICINE
        assert result["specialty_cohort"][0] == "MEDICINE"

    @pytest.mark.unit
    def test_cohort_ccs_populated_no_surgery(self):
        """332->333, 335->346 (surgery None), 367->368, 370->371: CCS cohort without surgery."""
        index = _index_lf()
        claims = _claims_lf()
        vs = _empty_vs()
        vs["cohort_ccs"] = pl.DataFrame(
            {
                "ccs_category": ["CCS100"],
                "specialty_cohort": ["CARDIORESPIRATORY"],
            }
        ).lazy()
        result = AcrReadmissionExpression.assign_specialty_cohorts(
            index, claims, vs, _config()
        ).collect()
        assert result["specialty_cohort"][0] == "CARDIORESPIRATORY"

    @pytest.mark.unit
    def test_cohort_ccs_empty(self):
        """332->364: cohort_ccs is empty -> skip CCS assignment."""
        index = _index_lf()
        claims = _claims_lf()
        vs = _empty_vs()
        result = AcrReadmissionExpression.assign_specialty_cohorts(
            index, claims, vs, _config()
        ).collect()
        assert result["specialty_cohort"][0] == "MEDICINE"
        assert result["cohort_assignment_rule"][0] == "DEFAULT_MEDICINE"

    @pytest.mark.unit
    def test_surgery_and_ccs_combined(self):
        """335->336: surgery_claim_ids is not None when CCS also present."""
        index = pl.DataFrame(
            {
                "claim_id": ["C001", "C002"],
                "person_id": ["P001", "P001"],
                "admission_date": [date(2025, 3, 15), date(2025, 3, 15)],
                "discharge_date": [date(2025, 3, 20), date(2025, 3, 20)],
                "exclusion_flag": [False, False],
                "ccs_diagnosis_category": ["CCS100", "CCS200"],
                "discharge_status_code": ["01", "01"],
                "facility_id": ["NPI001", "NPI001"],
                "principal_diagnosis_code": ["A01", "A02"],
                "age_at_admission": [75, 75],
            }
        ).lazy()
        claims = pl.DataFrame(
            {
                "claim_id": ["C001", "C002"],
                "person_id": ["P001", "P001"],
                "admission_date": [date(2025, 3, 15), date(2025, 3, 15)],
                "discharge_date": [date(2025, 3, 20), date(2025, 3, 20)],
                "diagnosis_code_1": ["A01", "A02"],
                "bill_type_code": ["111", "111"],
                "facility_npi": ["NPI001", "NPI001"],
                "discharge_status_code": ["01", "01"],
                "procedure_code_1": ["0016070", None],
            }
        ).lazy()
        vs = _empty_vs()
        vs["cohort_icd10"] = pl.DataFrame(
            {
                "icd_10_pcs": ["0016070"],
                "specialty_cohort": ["SURGERY_GYNECOLOGY"],
            }
        ).lazy()
        vs["cohort_ccs"] = pl.DataFrame(
            {
                "ccs_category": ["CCS200"],
                "specialty_cohort": ["NEUROLOGY"],
            }
        ).lazy()
        result = AcrReadmissionExpression.assign_specialty_cohorts(
            index, claims, vs, _config()
        ).collect()
        cohorts = dict(
            zip(
                result["claim_id"].to_list(),
                result["specialty_cohort"].to_list(),
                strict=False,
            )
        )
        assert cohorts["C001"] == "SURGERY_GYNECOLOGY"
        assert cohorts["C002"] == "NEUROLOGY"

    @pytest.mark.unit
    def test_only_surgery_no_ccs(self):
        """365->366, 367->370, 370->371: surgery present, CCS empty -> only surgery in parts."""
        index = _index_lf()
        claims = pl.DataFrame(
            {
                "claim_id": ["C001"],
                "person_id": ["P001"],
                "admission_date": [date(2025, 3, 15)],
                "discharge_date": [date(2025, 3, 20)],
                "diagnosis_code_1": ["A01"],
                "bill_type_code": ["111"],
                "facility_npi": ["NPI001"],
                "discharge_status_code": ["01"],
                "procedure_code_1": ["0016070"],
            }
        ).lazy()
        vs = _empty_vs()
        vs["cohort_icd10"] = pl.DataFrame(
            {
                "icd_10_pcs": ["0016070"],
                "specialty_cohort": ["SURGERY_GYNECOLOGY"],
            }
        ).lazy()
        # cohort_ccs stays empty
        result = AcrReadmissionExpression.assign_specialty_cohorts(
            index, claims, vs, _config()
        ).collect()
        assert result["specialty_cohort"][0] == "SURGERY_GYNECOLOGY"

    @pytest.mark.unit
    def test_no_cohort_parts_empty_result(self):
        """370->373: cohort_parts is empty -> empty assigned DataFrame."""
        index = _index_lf()
        claims = _claims_lf()
        vs = _empty_vs()
        # Both cohort_icd10 and cohort_ccs are empty
        result = AcrReadmissionExpression.assign_specialty_cohorts(
            index, claims, vs, _config()
        ).collect()
        assert result["specialty_cohort"][0] == "MEDICINE"
        assert result["cohort_assignment_rule"][0] == "DEFAULT_MEDICINE"


# ---------------------------------------------------------------------------
# Branch-coverage tests for identify_planned_readmissions  (lines 468-503)
# ---------------------------------------------------------------------------


class TestIdentifyPlannedReadmissions:
    """Cover branches 468->469/480 and 486->487/503."""

    def _readmission_claims(self):
        """Two claims: index discharge on 3/5, readmission on 3/10."""
        return pl.DataFrame(
            {
                "claim_id": ["C001", "C002"],
                "person_id": ["P001", "P001"],
                "admission_date": [date(2025, 3, 1), date(2025, 3, 10)],
                "discharge_date": [date(2025, 3, 5), date(2025, 3, 15)],
                "diagnosis_code_1": ["A01", "B02"],
                "bill_type_code": ["111", "111"],
                "facility_npi": ["NPI001", "NPI002"],
                "discharge_status_code": ["01", "01"],
            }
        ).lazy()

    @pytest.mark.unit
    def test_ccs_mapping_populated(self):
        """468->469: ccs_mapping not None and height > 0 -> join readmit CCS."""
        index = _index_lf()
        claims = self._readmission_claims()
        vs = _empty_vs()
        vs["ccs_icd10_cm"] = pl.DataFrame(
            {"icd_10_cm": ["B02"], "ccs_category": ["CCS200"]}
        ).lazy()
        result = AcrReadmissionExpression.identify_planned_readmissions(
            index, claims, vs, _config()
        ).collect()
        assert result.height >= 1
        assert "readmit_dx_ccs" in result.columns
        assert result["readmit_dx_ccs"][0] == "CCS200"

    @pytest.mark.unit
    def test_ccs_mapping_empty(self):
        """468->480: ccs_mapping empty -> add null readmit_dx_ccs."""
        index = _index_lf()
        claims = self._readmission_claims()
        vs = _empty_vs()
        result = AcrReadmissionExpression.identify_planned_readmissions(
            index, claims, vs, _config()
        ).collect()
        assert result.height >= 1
        assert result["readmit_dx_ccs"][0] is None

    @pytest.mark.unit
    def test_paa2_populated(self):
        """486->487: paa2 not None and height > 0 -> classify planned."""
        index = _index_lf()
        claims = self._readmission_claims()
        vs = _empty_vs()
        vs["ccs_icd10_cm"] = pl.DataFrame(
            {"icd_10_cm": ["B02"], "ccs_category": ["CCS200"]}
        ).lazy()
        vs["paa2"] = pl.DataFrame(
            {"ccs_diagnosis_category": ["CCS200"]}
        ).lazy()
        result = AcrReadmissionExpression.identify_planned_readmissions(
            index, claims, vs, _config()
        ).collect()
        assert result.height >= 1
        assert result["is_planned"][0] is True
        assert result["planned_rule"][0] == "RULE2"
        assert result["unplanned_readmission_flag"][0] is False

    @pytest.mark.unit
    def test_paa2_empty(self):
        """486->503: paa2 empty -> is_planned=False, planned_rule=None."""
        index = _index_lf()
        claims = self._readmission_claims()
        vs = _empty_vs()
        result = AcrReadmissionExpression.identify_planned_readmissions(
            index, claims, vs, _config()
        ).collect()
        assert result.height >= 1
        assert result["is_planned"][0] is False
        assert result["planned_rule"][0] is None
        assert result["unplanned_readmission_flag"][0] is True
