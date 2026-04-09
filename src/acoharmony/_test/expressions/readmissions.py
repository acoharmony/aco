from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest

from acoharmony._expressions._acr_readmission import AcrReadmissionExpression
from acoharmony._expressions._readmissions import ReadmissionsExpression


def _claims_lf(**overrides) -> pl.LazyFrame:
    data = {'claim_id': ['C001'], 'person_id': ['P001'], 'admission_date': [date(2025, 3, 15)], 'discharge_date': [date(2025, 3, 20)], 'diagnosis_code_1': ['A01'], 'bill_type_code': ['111'], 'facility_npi': ['NPI001'], 'discharge_status_code': ['01']}
    data.update(overrides)
    return pl.DataFrame(data).lazy()


def _empty_vs() -> dict[str, pl.LazyFrame]:
    return {'ccs_icd10_cm': pl.DataFrame().lazy(), 'exclusions': pl.DataFrame().lazy(), 'cohort_icd10': pl.DataFrame().lazy(), 'cohort_ccs': pl.DataFrame().lazy(), 'paa2': pl.DataFrame().lazy()}


def _config(**overrides) -> dict:
    cfg = {'performance_year': 2025, 'lookback_days': 30, 'min_age': 65, 'patient_id_column': 'person_id', 'admission_date_column': 'admission_date', 'discharge_date_column': 'discharge_date', 'diagnosis_column': 'diagnosis_code_1'}
    cfg.update(overrides)
    return cfg


def _index_lf():
    """Standard index admissions LazyFrame."""
    return pl.DataFrame({'claim_id': ['C001'], 'person_id': ['P001'], 'admission_date': [date(2025, 3, 1)], 'discharge_date': [date(2025, 3, 5)], 'exclusion_flag': [False], 'ccs_diagnosis_category': ['CCS100'], 'discharge_status_code': ['01'], 'facility_id': ['NPI001'], 'principal_diagnosis_code': ['A01'], 'age_at_admission': [75]}).lazy()


class TestReadmissionsExpression:

    @pytest.mark.unit
    def test_build(self):
        result = ReadmissionsExpression.build({})
        assert 'config' in result

    @pytest.mark.unit
    def test_deduplicate_readmissions(self):
        df = pl.DataFrame({'patient_id': ['P1', 'P1', 'P2'], 'val': [1, 1, 2]}).lazy()
        result = ReadmissionsExpression.deduplicate_readmissions(df).collect()
        assert len(result) == 2

    @pytest.mark.unit
    def test_identify_readmission_pairs(self):
        claims = pl.DataFrame({'person_id': ['P1', 'P1'], 'claim_id': ['C1', 'C2'], 'admission_date': [date(2024, 1, 1), date(2024, 1, 20)], 'discharge_date': [date(2024, 1, 5), date(2024, 1, 25)], 'bill_type_code': ['111', '111']}).lazy()
        result = ReadmissionsExpression.identify_readmission_pairs(claims).collect()
        assert 'days_to_readmission' in result.columns

class TestTransformReadmissionPairs:
    """Cover transform_readmission_pairs lines 130-174."""

    @pytest.mark.unit
    def test_readmission_pairs_found(self):
        """Index discharge → readmission within 30 days detected."""
        encounters = pl.DataFrame({
            "person_id": ["P1", "P1", "P1"],
            "encounter_id": ["E1", "E2", "E3"],
            "encounter_type": ["inpatient", "inpatient", "outpatient"],
            "admission_date": [date(2024, 1, 1), date(2024, 1, 20), date(2024, 2, 1)],
            "discharge_date": [date(2024, 1, 5), date(2024, 1, 25), date(2024, 2, 3)],
        }).lazy()
        acute_dx = pl.DataFrame({"id": [1]}).lazy()
        planned = pl.DataFrame({"id": [1]}).lazy()

        result = ReadmissionsExpression.transform_readmission_pairs(
            encounters, acute_dx, planned, _config()
        ).collect()

        assert "days_to_readmission" in result.columns
        assert result.height >= 1
        # E1 discharge 1/5 → E2 admission 1/20 = 15 days
        pair = result.filter(
            (pl.col("index_encounter_id") == "E1")
            & (pl.col("readmission_encounter_id") == "E2")
        )
        assert pair.height == 1
        assert pair["days_to_readmission"][0] == 15

    @pytest.mark.unit
    def test_no_readmission_beyond_window(self):
        """Readmission > 30 days after discharge is excluded."""
        encounters = pl.DataFrame({
            "person_id": ["P1", "P1"],
            "encounter_id": ["E1", "E2"],
            "encounter_type": ["inpatient", "inpatient"],
            "admission_date": [date(2024, 1, 1), date(2024, 3, 1)],
            "discharge_date": [date(2024, 1, 5), date(2024, 3, 5)],
        }).lazy()
        result = ReadmissionsExpression.transform_readmission_pairs(
            encounters, pl.DataFrame().lazy(), pl.DataFrame().lazy(), _config()
        ).collect()
        assert result.height == 0


class TestPlannedReadmissionsWithCCS:
    """Test readmission classification with CCS mappings."""

    @pytest.mark.unit
    def test_with_ccs_mapping(self):
        """Readmissions get CCS mapping applied."""
        index = _index_lf()
        claims = pl.DataFrame({'claim_id': ['C001', 'C002'], 'person_id': ['P001', 'P001'], 'admission_date': [date(2025, 3, 1), date(2025, 3, 10)], 'discharge_date': [date(2025, 3, 5), date(2025, 3, 15)], 'diagnosis_code_1': ['A01', 'B02'], 'bill_type_code': ['111', '111'], 'facility_npi': ['NPI001', 'NPI002']}).lazy()
        vs = _empty_vs()
        vs['ccs_icd10_cm'] = pl.DataFrame({'icd_10_cm': ['B02'], 'ccs_category': ['CCS200']}).lazy()
        config = _config()
        result = AcrReadmissionExpression.identify_planned_readmissions(index, claims, vs, config).collect()
        assert result.height == 1
        assert result['readmit_dx_ccs'][0] == 'CCS200'

    @pytest.mark.unit
    def test_with_paa2_planned_readmission(self):
        """PAA Rule 2 classifies always-planned diagnoses."""
        index = _index_lf()
        claims = pl.DataFrame({'claim_id': ['C001', 'C002'], 'person_id': ['P001', 'P001'], 'admission_date': [date(2025, 3, 1), date(2025, 3, 10)], 'discharge_date': [date(2025, 3, 5), date(2025, 3, 15)], 'diagnosis_code_1': ['A01', 'B02'], 'bill_type_code': ['111', '111'], 'facility_npi': ['NPI001', 'NPI002']}).lazy()
        vs = _empty_vs()
        vs['ccs_icd10_cm'] = pl.DataFrame({'icd_10_cm': ['B02'], 'ccs_category': ['CCS200']}).lazy()
        vs['paa2'] = pl.DataFrame({'ccs_diagnosis_category': ['CCS200']}).lazy()
        config = _config()
        result = AcrReadmissionExpression.identify_planned_readmissions(index, claims, vs, config).collect()
        assert result.height == 1
        assert result['is_planned'][0] is True
        assert result['planned_rule'][0] == 'RULE2'
        assert result['unplanned_readmission_flag'][0] is False

class TestCCSBasedCohort:
    """Test CCS-based cohort assignment."""

    @pytest.mark.unit
    def test_ccs_cohort_assignment(self):
        """Claims matched by CCS category get appropriate cohort."""
        index = _index_lf()
        claims = _claims_lf()
        vs = _empty_vs()
        vs['cohort_ccs'] = pl.DataFrame({'ccs_category': ['CCS100'], 'specialty_cohort': ['CARDIORESPIRATORY']}).lazy()
        config = _config()
        result = AcrReadmissionExpression.assign_specialty_cohorts(index, claims, vs, config).collect()
        assert result['specialty_cohort'][0] == 'CARDIORESPIRATORY'

    @pytest.mark.unit
    def test_surgery_and_ccs_combined(self):
        """Surgery claims excluded from CCS cohort assignment."""
        index = pl.DataFrame({'claim_id': ['C001', 'C002'], 'person_id': ['P001', 'P001'], 'admission_date': [date(2025, 3, 15), date(2025, 3, 15)], 'discharge_date': [date(2025, 3, 20), date(2025, 3, 20)], 'exclusion_flag': [False, False], 'ccs_diagnosis_category': ['CCS100', 'CCS200'], 'discharge_status_code': ['01', '01'], 'facility_id': ['NPI001', 'NPI001'], 'principal_diagnosis_code': ['A01', 'A02'], 'age_at_admission': [75, 75]}).lazy()
        claims = pl.DataFrame({'claim_id': ['C001', 'C002'], 'person_id': ['P001', 'P001'], 'admission_date': [date(2025, 3, 15), date(2025, 3, 15)], 'discharge_date': [date(2025, 3, 20), date(2025, 3, 20)], 'diagnosis_code_1': ['A01', 'A02'], 'bill_type_code': ['111', '111'], 'facility_npi': ['NPI001', 'NPI001'], 'discharge_status_code': ['01', '01'], 'procedure_code_1': ['0016070', None]}).lazy()
        vs = _empty_vs()
        vs['cohort_icd10'] = pl.DataFrame({'icd_10_pcs': ['0016070'], 'specialty_cohort': ['SURGERY_GYNECOLOGY']}).lazy()
        vs['cohort_ccs'] = pl.DataFrame({'ccs_category': ['CCS200'], 'specialty_cohort': ['NEUROLOGY']}).lazy()
        config = _config()
        result = AcrReadmissionExpression.assign_specialty_cohorts(index, claims, vs, config).collect()
        cohorts = dict(zip(result['claim_id'].to_list(), result['specialty_cohort'].to_list(), strict=False))
        assert cohorts['C001'] == 'SURGERY_GYNECOLOGY'
        assert cohorts['C002'] == 'NEUROLOGY'


class TestRunFullAcrPipeline:
    """Cover run_full_acr_pipeline lines 617-636."""

    @pytest.mark.unit
    def test_full_pipeline_returns_four_results(self):
        """Full ACR pipeline: index admissions → cohorts → readmissions → summary."""
        claims = pl.DataFrame({
            'claim_id': ['C001', 'C002'],
            'person_id': ['P001', 'P001'],
            'admission_date': [date(2025, 3, 1), date(2025, 3, 20)],
            'discharge_date': [date(2025, 3, 5), date(2025, 3, 25)],
            'diagnosis_code_1': ['A01', 'B02'],
            'bill_type_code': ['111', '111'],
            'facility_npi': ['NPI001', 'NPI002'],
            'discharge_status_code': ['01', '01'],
        }).lazy()
        eligibility = pl.DataFrame({
            'person_id': ['P001'],
            'birth_date': [date(1950, 1, 1)],
            'age': [75],
        }).lazy()
        vs = _empty_vs()
        vs['ccs_icd10_cm'] = pl.DataFrame({
            'icd_10_cm': ['A01', 'B02'],
            'ccs_category': ['CCS100', 'CCS200'],
        }).lazy()
        config = _config()

        result = AcrReadmissionExpression.calculate_acr_measure(
            claims, eligibility, vs, config
        )

        assert len(result) == 4
        index_admissions, specialty_cohorts, readmission_pairs, summary = result
        assert isinstance(index_admissions, pl.LazyFrame)
        assert isinstance(summary, pl.LazyFrame)
