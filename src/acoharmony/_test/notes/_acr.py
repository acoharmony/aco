# © 2025 HarmonyCares
"""Tests for acoharmony._notes._acr (AcrPlugins)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from acoharmony._notes import AcrPlugins
from acoharmony._notes._acr import VALUE_SET_FILES


def _claims_lf(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(
        rows,
        schema={
            "claim_id": pl.Utf8,
            "person_id": pl.Utf8,
            "admission_date": pl.Utf8,
            "discharge_date": pl.Utf8,
            "bill_type_code": pl.Utf8,
            "diagnosis_code_1": pl.Utf8,
            "facility_npi": pl.Utf8,
            "procedure_code_1": pl.Utf8,
            "procedure_code_2": pl.Utf8,
        },
    )


def _eligibility_lf(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(
        rows,
        schema={"person_id": pl.Utf8, "birth_date": pl.Date},
    )


def _empty_value_sets() -> dict[str, pl.LazyFrame]:
    return {key: pl.DataFrame().lazy() for key in VALUE_SET_FILES}


# ---------------------------------------------------------------------------
# load_value_sets
# ---------------------------------------------------------------------------


class TestLoadValueSets:
    @pytest.mark.unit
    def test_missing_files_return_empty(self, tmp_path: Path) -> None:
        out = AcrPlugins().load_value_sets(tmp_path)
        for key in VALUE_SET_FILES:
            assert out[key].collect().is_empty()

    @pytest.mark.unit
    def test_present_file_loaded(self, tmp_path: Path) -> None:
        df = pl.DataFrame({"icd_10_cm": ["A00.0"], "ccs_category": ["C1"]})
        df.write_parquet(tmp_path / VALUE_SET_FILES["ccs_icd10_cm"])
        out = AcrPlugins().load_value_sets(tmp_path)
        assert out["ccs_icd10_cm"].collect().height == 1


# ---------------------------------------------------------------------------
# index_admissions
# ---------------------------------------------------------------------------


class TestIndexAdmissions:
    @pytest.mark.unit
    def test_no_claims_returns_empty(self) -> None:
        out = AcrPlugins().index_admissions(
            None, _eligibility_lf([]), _empty_value_sets(), "2025-01-01", "2025-12-31"
        )
        assert out.is_empty()

    @pytest.mark.unit
    def test_no_eligibility_returns_empty(self) -> None:
        out = AcrPlugins().index_admissions(
            _claims_lf([]), None, _empty_value_sets(), "2025-01-01", "2025-12-31"
        )
        assert out.is_empty()

    @pytest.mark.unit
    def test_filters_to_inpatient_in_period_and_age(self) -> None:
        claims = _claims_lf(
            [
                # Eligible: bill 11x, in period
                {
                    "claim_id": "c1",
                    "person_id": "p1",
                    "admission_date": "2025-03-01",
                    "discharge_date": "2025-03-05",
                    "bill_type_code": "111",
                    "diagnosis_code_1": "A00.0",
                    "facility_npi": "F1",
                    "procedure_code_1": None,
                    "procedure_code_2": None,
                },
                # Excluded: outpatient
                {
                    "claim_id": "c2",
                    "person_id": "p1",
                    "admission_date": "2025-04-01",
                    "discharge_date": "2025-04-02",
                    "bill_type_code": "131",
                    "diagnosis_code_1": "A00.0",
                    "facility_npi": "F1",
                    "procedure_code_1": None,
                    "procedure_code_2": None,
                },
                # Out-of-period
                {
                    "claim_id": "c3",
                    "person_id": "p1",
                    "admission_date": "2024-06-01",
                    "discharge_date": "2024-06-05",
                    "bill_type_code": "111",
                    "diagnosis_code_1": "A00.0",
                    "facility_npi": "F1",
                    "procedure_code_1": None,
                    "procedure_code_2": None,
                },
                # Under 65
                {
                    "claim_id": "c4",
                    "person_id": "p2",
                    "admission_date": "2025-05-01",
                    "discharge_date": "2025-05-03",
                    "bill_type_code": "111",
                    "diagnosis_code_1": "A00.0",
                    "facility_npi": "F1",
                    "procedure_code_1": None,
                    "procedure_code_2": None,
                },
            ]
        )
        elig = _eligibility_lf(
            [
                {"person_id": "p1", "birth_date": date(1950, 1, 1)},  # 75 in 2025
                {"person_id": "p2", "birth_date": date(2000, 1, 1)},  # 25 in 2025
            ]
        )
        out = AcrPlugins().index_admissions(
            claims, elig, _empty_value_sets(), "2025-01-01", "2025-12-31"
        )
        assert out.height == 1
        assert out["claim_id"][0] == "c1"

    @pytest.mark.unit
    def test_applies_ccs_mapping_and_exclusions(self) -> None:
        claims = _claims_lf(
            [
                {
                    "claim_id": "c1",
                    "person_id": "p1",
                    "admission_date": "2025-03-01",
                    "discharge_date": "2025-03-05",
                    "bill_type_code": "111",
                    "diagnosis_code_1": "EXCLUDE_DX",
                    "facility_npi": "F1",
                    "procedure_code_1": None,
                    "procedure_code_2": None,
                }
            ]
        )
        elig = _eligibility_lf([{"person_id": "p1", "birth_date": date(1950, 1, 1)}])
        vs = _empty_value_sets()
        vs["ccs_icd10_cm"] = pl.LazyFrame(
            {"icd_10_cm": ["EXCLUDE_DX"], "ccs_category": ["EXCLUDE_CCS"]}
        )
        vs["exclusions"] = pl.LazyFrame(
            {"ccs_diagnosis_category": ["EXCLUDE_CCS"]}
        )
        out = AcrPlugins().index_admissions(
            claims, elig, vs, "2025-01-01", "2025-12-31"
        )
        assert out.height == 1
        assert out["exclusion_flag"][0] is True


class TestExclusionBreakdown:
    @pytest.mark.unit
    def test_groups_excluded(self) -> None:
        df = pl.DataFrame(
            {
                "claim_id": ["a", "b", "c"],
                "ccs_diagnosis_category": ["X", "X", "Y"],
                "exclusion_flag": [True, True, False],
            }
        )
        out = AcrPlugins().exclusion_breakdown(df)
        assert out.height == 1
        assert out["ccs_diagnosis_category"][0] == "X"
        assert out["excluded_count"][0] == 2


# ---------------------------------------------------------------------------
# specialty_cohorts
# ---------------------------------------------------------------------------


class TestSpecialtyCohorts:
    @pytest.mark.unit
    def test_empty_inputs(self) -> None:
        out = AcrPlugins().specialty_cohorts(None, pl.DataFrame(), _empty_value_sets())
        assert out.is_empty()

    @pytest.mark.unit
    def test_default_medicine(self) -> None:
        index = pl.DataFrame(
            {
                "claim_id": ["c1"],
                "ccs_diagnosis_category": ["UNMAPPED"],
                "exclusion_flag": [False],
            }
        )
        out = AcrPlugins().specialty_cohorts(_claims_lf([]), index, _empty_value_sets())
        assert out.height == 1
        assert out["specialty_cohort"][0] == "MEDICINE"

    @pytest.mark.unit
    def test_ccs_cohort_match(self) -> None:
        index = pl.DataFrame(
            {
                "claim_id": ["c1"],
                "ccs_diagnosis_category": ["NEURO_CCS"],
                "exclusion_flag": [False],
            }
        )
        vs = _empty_value_sets()
        vs["cohort_ccs"] = pl.LazyFrame(
            {"ccs_category": ["NEURO_CCS"], "specialty_cohort": ["NEUROLOGY"]}
        )
        out = AcrPlugins().specialty_cohorts(_claims_lf([]), index, vs)
        assert out["specialty_cohort"][0] == "NEUROLOGY"

    @pytest.mark.unit
    def test_all_excluded_returns_empty(self) -> None:
        index = pl.DataFrame(
            {
                "claim_id": ["c1"],
                "ccs_diagnosis_category": ["X"],
                "exclusion_flag": [True],
            }
        )
        out = AcrPlugins().specialty_cohorts(_claims_lf([]), index, _empty_value_sets())
        assert out.is_empty()

    @pytest.mark.unit
    def test_cohort_icd10_present_but_no_proc_cols(self) -> None:
        # Claims schema lacks procedure_code_*
        claims = pl.LazyFrame(
            schema={"claim_id": pl.Utf8, "person_id": pl.Utf8}
        )
        index = pl.DataFrame(
            {
                "claim_id": ["c1"],
                "ccs_diagnosis_category": ["X"],
                "exclusion_flag": [False],
            }
        )
        vs = _empty_value_sets()
        vs["cohort_icd10"] = pl.LazyFrame(
            {"icd_10_pcs": ["PCS_X"], "specialty_cohort": ["SURGERY_GYNECOLOGY"]}
        )
        out = AcrPlugins().specialty_cohorts(claims, index, vs)
        # Falls through to default MEDICINE because no PCS columns present
        assert out["specialty_cohort"][0] == "MEDICINE"

    @pytest.mark.unit
    def test_cohort_ccs_with_no_non_surgery_rows(self) -> None:
        # All eligible claims are also surgery claims → non_surgery filter empties out
        claims = _claims_lf(
            [
                {
                    "claim_id": "c1",
                    "person_id": "p1",
                    "admission_date": "2025-01-01",
                    "discharge_date": "2025-01-02",
                    "bill_type_code": "111",
                    "diagnosis_code_1": "X",
                    "facility_npi": None,
                    "procedure_code_1": "PCS_SURG",
                    "procedure_code_2": None,
                }
            ]
        )
        index = pl.DataFrame(
            {
                "claim_id": ["c1"],
                "ccs_diagnosis_category": ["X"],
                "exclusion_flag": [False],
            }
        )
        vs = _empty_value_sets()
        vs["cohort_icd10"] = pl.LazyFrame(
            {"icd_10_pcs": ["PCS_SURG"], "specialty_cohort": ["SURGERY_GYNECOLOGY"]}
        )
        vs["cohort_ccs"] = pl.LazyFrame(
            {"ccs_category": ["X"], "specialty_cohort": ["MEDICINE"]}
        )
        out = AcrPlugins().specialty_cohorts(claims, index, vs)
        # Surgery wins; cohort_ccs branch executes but non_surgery is empty
        assert out["specialty_cohort"][0] == "SURGERY_GYNECOLOGY"

    @pytest.mark.unit
    def test_surgery_cohort_via_pcs(self) -> None:
        claims = _claims_lf(
            [
                {
                    "claim_id": "c1",
                    "person_id": "p1",
                    "admission_date": "2025-01-01",
                    "discharge_date": "2025-01-02",
                    "bill_type_code": "111",
                    "diagnosis_code_1": "X",
                    "facility_npi": None,
                    "procedure_code_1": "PCS_SURG",
                    "procedure_code_2": None,
                }
            ]
        )
        index = pl.DataFrame(
            {
                "claim_id": ["c1"],
                "ccs_diagnosis_category": ["X"],
                "exclusion_flag": [False],
            }
        )
        vs = _empty_value_sets()
        vs["cohort_icd10"] = pl.LazyFrame(
            {"icd_10_pcs": ["PCS_SURG"], "specialty_cohort": ["SURGERY_GYNECOLOGY"]}
        )
        out = AcrPlugins().specialty_cohorts(claims, index, vs)
        assert out["specialty_cohort"][0] == "SURGERY_GYNECOLOGY"


class TestCohortDistribution:
    @pytest.mark.unit
    def test_pcts(self) -> None:
        df = pl.DataFrame(
            {"claim_id": ["a", "b", "c"], "specialty_cohort": ["A", "A", "B"]}
        )
        out = AcrPlugins().cohort_distribution(df)
        as_dict = {row["specialty_cohort"]: row for row in out.iter_rows(named=True)}
        assert as_dict["A"]["pct"] == pytest.approx(66.67, rel=1e-2)


# ---------------------------------------------------------------------------
# classify_readmissions
# ---------------------------------------------------------------------------


class TestClassifyReadmissions:
    @pytest.mark.unit
    def test_empty_inputs(self) -> None:
        out = AcrPlugins().classify_readmissions(None, pl.DataFrame(), _empty_value_sets())
        assert out.is_empty()

    @pytest.mark.unit
    def test_all_excluded_returns_empty(self) -> None:
        index = pl.DataFrame(
            {
                "claim_id": ["c1"],
                "person_id": ["p1"],
                "discharge_date": [date(2025, 3, 5)],
                "ccs_diagnosis_category": ["X"],
                "exclusion_flag": [True],
            }
        )
        out = AcrPlugins().classify_readmissions(_claims_lf([]), index, _empty_value_sets())
        assert out.is_empty()

    @pytest.mark.unit
    def test_pairs_within_30_days(self) -> None:
        # Index discharge 2025-03-05; readmission on 2025-03-15 (10 days)
        claims = _claims_lf(
            [
                {
                    "claim_id": "c1",
                    "person_id": "p1",
                    "admission_date": "2025-03-01",
                    "discharge_date": "2025-03-05",
                    "bill_type_code": "111",
                    "diagnosis_code_1": "X",
                    "facility_npi": "F1",
                    "procedure_code_1": None,
                    "procedure_code_2": None,
                },
                {
                    "claim_id": "c2",
                    "person_id": "p1",
                    "admission_date": "2025-03-15",
                    "discharge_date": "2025-03-18",
                    "bill_type_code": "111",
                    "diagnosis_code_1": "Y",
                    "facility_npi": "F2",
                    "procedure_code_1": None,
                    "procedure_code_2": None,
                },
            ]
        )
        index = pl.DataFrame(
            {
                "claim_id": ["c1"],
                "person_id": ["p1"],
                "discharge_date": [date(2025, 3, 5)],
                "ccs_diagnosis_category": ["X"],
                "exclusion_flag": [False],
            }
        )
        out = AcrPlugins().classify_readmissions(claims, index, _empty_value_sets())
        assert out.height == 1
        assert out["days_to_readmission"][0] == 10
        assert out["unplanned_readmission_flag"][0] is True

    @pytest.mark.unit
    def test_paa2_marks_planned(self) -> None:
        claims = _claims_lf(
            [
                {
                    "claim_id": "c1",
                    "person_id": "p1",
                    "admission_date": "2025-03-01",
                    "discharge_date": "2025-03-05",
                    "bill_type_code": "111",
                    "diagnosis_code_1": "X",
                    "facility_npi": "F1",
                    "procedure_code_1": None,
                    "procedure_code_2": None,
                },
                {
                    "claim_id": "c2",
                    "person_id": "p1",
                    "admission_date": "2025-03-10",
                    "discharge_date": "2025-03-12",
                    "bill_type_code": "111",
                    "diagnosis_code_1": "PLANNED_DX",
                    "facility_npi": "F2",
                    "procedure_code_1": None,
                    "procedure_code_2": None,
                },
            ]
        )
        index = pl.DataFrame(
            {
                "claim_id": ["c1"],
                "person_id": ["p1"],
                "discharge_date": [date(2025, 3, 5)],
                "ccs_diagnosis_category": ["X"],
                "exclusion_flag": [False],
            }
        )
        vs = _empty_value_sets()
        vs["ccs_icd10_cm"] = pl.LazyFrame(
            {"icd_10_cm": ["PLANNED_DX"], "ccs_category": ["PLANNED_CCS"]}
        )
        vs["paa2"] = pl.LazyFrame({"ccs_diagnosis_category": ["PLANNED_CCS"]})
        out = AcrPlugins().classify_readmissions(claims, index, vs)
        assert out.height == 1
        assert out["is_planned"][0] is True
        assert out["unplanned_readmission_flag"][0] is False


# ---------------------------------------------------------------------------
# rollups
# ---------------------------------------------------------------------------


def _readmission_pairs() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "readmission_claim_id": ["r1", "r2", "r3", "r4"],
            "readmit_facility_id": ["F1", "F1", "F2", "F2"],
            "is_planned": [False, True, False, False],
            "unplanned_readmission_flag": [True, False, True, True],
            "days_to_readmission": [5, 10, 15, 25],
        }
    )


class TestRollups:
    @pytest.mark.unit
    def test_planned_unplanned(self) -> None:
        out = AcrPlugins().planned_unplanned_breakdown(_readmission_pairs())
        as_dict = {row["readmission_type"]: row for row in out.iter_rows(named=True)}
        assert as_dict["Unplanned"]["readmission_count"] == 3
        assert as_dict["Planned"]["readmission_count"] == 1

    @pytest.mark.unit
    def test_timing(self) -> None:
        out = AcrPlugins().timing_distribution(_readmission_pairs())
        windows = out["readmission_window"].to_list()
        assert "1-7 days" in windows
        assert "22-30 days" in windows

    @pytest.mark.unit
    def test_top_facilities(self) -> None:
        out = AcrPlugins().top_readmit_facilities(_readmission_pairs(), n=10)
        # F2 has 2 unplanned, F1 has 1 → F2 first
        assert out["readmit_facility_id"][0] == "F2"


# ---------------------------------------------------------------------------
# summary / export
# ---------------------------------------------------------------------------


class TestSummary:
    @pytest.mark.unit
    def test_no_admissions(self) -> None:
        out = AcrPlugins().summary(pl.DataFrame(), pl.DataFrame(), 2025)
        assert out["denominator_count"] == 0
        assert out["observed_rate_pct"] == 0.0
        assert out["summary_df"].height == 1

    @pytest.mark.unit
    def test_with_data(self) -> None:
        index = pl.DataFrame(
            {"claim_id": ["a", "b", "c"], "exclusion_flag": [False, False, False]}
        )
        readmits = pl.DataFrame(
            {
                "readmission_claim_id": ["r1", "r2"],
                "unplanned_readmission_flag": [True, True],
            }
        )
        out = AcrPlugins().summary(index, readmits, 2025)
        assert out["denominator_count"] == 3
        assert out["observed_readmissions"] == 2
        assert out["observed_rate_pct"] == pytest.approx(66.67, rel=1e-2)


class TestExportToGold:
    @pytest.mark.unit
    def test_writes_nonempty_only(self, tmp_path: Path) -> None:
        outputs = {
            "a.parquet": pl.DataFrame({"x": [1]}),
            "b.parquet": pl.DataFrame(),
        }
        written = AcrPlugins().export_to_gold(tmp_path, outputs)
        assert any("a.parquet" in w for w in written)
        assert not any("b.parquet" in w for w in written)
        assert (tmp_path / "a.parquet").exists()
        assert not (tmp_path / "b.parquet").exists()
