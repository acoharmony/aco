# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.readmissions_enhanced module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from datetime import date, datetime  # noqa: F811
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
import acoharmony


def _write(df: pl.DataFrame, path: Path) -> None:
    """Write a DataFrame as parquet, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


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


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


def _lazy(df: pl.DataFrame) -> pl.LazyFrame:
    return df.lazy()


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestReadmissionsEnhancedPublic:
    """Tests for readmissions_enhanced public transform module."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import readmissions_enhanced
        assert readmissions_enhanced is not None


class TestReadmissionsEnhanced:
    """Tests for ReadmissionsEnhancedTransform."""

    @pytest.mark.unit
    def test_load_readmissions_value_sets_success(self, tmp_path):
        """Cover lines 63-94: load_readmissions_value_sets with actual files."""
        from acoharmony._transforms.readmissions_enhanced import ReadmissionsEnhancedTransform

        # Create minimal parquet files for each value set
        file_mappings = {
            "value_sets_readmissions_acute_diagnosis_icd_10_cm.parquet": {"icd_10_cm": ["A01"]},
            "value_sets_readmissions_acute_diagnosis_ccs.parquet": {"ccs_category": ["100"]},
            "value_sets_readmissions_always_planned_ccs_diagnosis_category.parquet": {"ccs_category": ["200"]},
            "value_sets_readmissions_always_planned_ccs_procedure_category.parquet": {"ccs_category": ["300"]},
            "value_sets_readmissions_potentially_planned_ccs_procedure_category.parquet": {"ccs_category": ["400"]},
            "value_sets_readmissions_potentially_planned_icd_10_pcs.parquet": {"icd_10_pcs": ["0ABC"]},
            "value_sets_readmissions_exclusion_ccs_diagnosis_category.parquet": {"ccs_category": ["500"]},
            "value_sets_readmissions_icd_10_cm_to_ccs.parquet": {"icd_10_cm": ["A01"], "ccs_category": ["100"]},
            "value_sets_readmissions_icd_10_pcs_to_ccs.parquet": {"icd_10_pcs": ["0ABC"], "ccs_category": ["300"]},
            "value_sets_readmissions_specialty_cohort.parquet": {"ccs_category": ["100"], "cohort_name": ["cardio"]},
            "value_sets_readmissions_surgery_gynecology_cohort.parquet": {"ccs_category": ["600"]},
        }

        for filename, data in file_mappings.items():
            _write(pl.DataFrame(data), tmp_path / filename)

        vs = ReadmissionsEnhancedTransform.load_readmissions_value_sets(tmp_path)
        assert len(vs) == 11
        assert "acute_diagnosis_icd10" in vs

    @pytest.mark.unit
    def test_load_readmissions_value_sets_scan_failure(self, tmp_path):
        """Cover lines 85-88: scan_parquet failure -> empty placeholders."""
        from acoharmony._transforms.readmissions_enhanced import ReadmissionsEnhancedTransform

        with patch("acoharmony._transforms.readmissions_enhanced.pl.scan_parquet", side_effect=OSError("mocked")):
            vs = ReadmissionsEnhancedTransform.load_readmissions_value_sets(tmp_path)
        assert len(vs) == 11
        for _key, lf in vs.items():
            assert lf.collect().height == 0

    @pytest.mark.unit
    def test_classify_planned_vs_unplanned_empty_value_sets(self):
        """Cover line 303 and 314: icd10pcs_to_ccs empty branch."""
        from acoharmony._transforms.readmissions_enhanced import ReadmissionsEnhancedTransform

        pairs = pl.LazyFrame(
            {
                "person_id": ["P1"],
                "index_claim_id": ["C1"],
                "index_admission_date": [date(2024, 1, 1)],
                "index_discharge_date": [date(2024, 1, 5)],
                "index_principal_diagnosis": ["A01"],
                "index_ccs_category": ["100"],
                "index_facility": ["NPI1"],
                "readmit_claim_id": ["C2"],
                "readmit_admission_date": [date(2024, 1, 20)],
                "readmit_discharge_date": [date(2024, 1, 25)],
                "readmit_principal_diagnosis": ["A02"],
                "readmit_principal_procedure": ["0XYZ"],
                "days_to_readmission": [15],
            }
        )

        # All empty value sets
        empty_vs = {
            "icd10cm_to_ccs": pl.DataFrame().lazy(),
            "icd10pcs_to_ccs": pl.DataFrame().lazy(),
            "always_planned_dx": pl.DataFrame().lazy(),
            "always_planned_px": pl.DataFrame().lazy(),
            "potentially_planned_px_ccs": pl.DataFrame().lazy(),
            "acute_diagnosis_ccs": pl.DataFrame().lazy(),
        }

        result = ReadmissionsEnhancedTransform.classify_planned_vs_unplanned(
            pairs, empty_vs, {}
        )
        df = result.collect()
        assert "planned_readmission" in df.columns
        assert "readmission_type" in df.columns
        assert df["readmission_type"][0] == "unplanned"

    @pytest.mark.unit
    def test_classify_planned_vs_unplanned_with_value_sets(self):
        """Cover lines 332-367: full classification with non-empty value sets."""
        from acoharmony._transforms.readmissions_enhanced import ReadmissionsEnhancedTransform

        pairs = pl.LazyFrame(
            {
                "person_id": ["P1", "P2", "P3"],
                "index_claim_id": ["C1", "C3", "C5"],
                "index_admission_date": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)],
                "index_discharge_date": [date(2024, 1, 5), date(2024, 2, 5), date(2024, 3, 5)],
                "index_principal_diagnosis": ["A01", "B01", "C01"],
                "index_ccs_category": ["100", "200", "300"],
                "index_facility": ["NPI1", "NPI2", "NPI3"],
                "readmit_claim_id": ["C2", "C4", "C6"],
                "readmit_admission_date": [date(2024, 1, 20), date(2024, 2, 20), date(2024, 3, 20)],
                "readmit_discharge_date": [date(2024, 1, 25), date(2024, 2, 25), date(2024, 3, 25)],
                "readmit_principal_diagnosis": ["DX_ALWAYS", "DX_MAYBE", "DX_OTHER"],
                "readmit_principal_procedure": ["PX_ALWAYS", "PX_MAYBE", "PX_OTHER"],
                "days_to_readmission": [15, 15, 15],
            }
        )

        vs = {
            "icd10cm_to_ccs": pl.DataFrame(
                {
                    "icd_10_cm": ["DX_ALWAYS", "DX_MAYBE", "DX_OTHER"],
                    "ccs_category": ["AP_DX", "MAYBE_DX", "ACUTE_DX"],
                }
            ).lazy(),
            "icd10pcs_to_ccs": pl.DataFrame(
                {
                    "icd_10_pcs": ["PX_ALWAYS", "PX_MAYBE", "PX_OTHER"],
                    "ccs_category": ["AP_PX", "PP_PX", "OTHER_PX"],
                }
            ).lazy(),
            "always_planned_dx": pl.DataFrame({"ccs_category": ["AP_DX"]}).lazy(),
            "always_planned_px": pl.DataFrame({"ccs_category": ["AP_PX"]}).lazy(),
            "potentially_planned_px_ccs": pl.DataFrame({"ccs_category": ["PP_PX"]}).lazy(),
            "acute_diagnosis_ccs": pl.DataFrame({"ccs_category": ["ACUTE_DX"]}).lazy(),
        }

        result = ReadmissionsEnhancedTransform.classify_planned_vs_unplanned(pairs, vs, {})
        df = result.collect()

        assert len(df) == 3
        # P1 has always_planned_dx match -> always_planned
        p1 = df.filter(pl.col("person_id") == "P1")
        assert p1["readmission_type"][0] == "always_planned"

        # P2 has potentially_planned px and NOT acute dx -> potentially_planned
        p2 = df.filter(pl.col("person_id") == "P2")
        assert p2["readmission_type"][0] == "potentially_planned"

        # P3 has potentially_planned px BUT also acute dx -> unplanned
        p3 = df.filter(pl.col("person_id") == "P3")
        assert p3["readmission_type"][0] == "unplanned"

    @pytest.mark.unit
    def test_assign_specialty_cohorts_empty(self):
        """Cover the no-specialty-cohort branch."""
        from acoharmony._transforms.readmissions_enhanced import ReadmissionsEnhancedTransform

        pairs = pl.LazyFrame(
            {
                "person_id": ["P1"],
                "index_ccs_category": ["100"],
            }
        )

        result = ReadmissionsEnhancedTransform.assign_specialty_cohorts(
            pairs, {"specialty_cohort": pl.DataFrame().lazy()}, {}
        )
        df = result.collect()
        assert df["specialty_cohort"][0] == "general"


class TestReadmissionsIdentifyIndexAdmissions:
    """Tests for ReadmissionsEnhancedTransform.identify_index_admissions."""

    def _make_claims(self) -> pl.LazyFrame:
        return _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2", "P3", "P4"],
                    "claim_id": ["C1", "C2", "C3", "C4"],
                    "claim_type": [
                        "institutional",
                        "institutional",
                        "professional",
                        "institutional",
                    ],
                    "bill_type_code": ["111", "112", "111", "131"],
                    "admission_date": [
                        date(2024, 1, 5),
                        date(2024, 2, 10),
                        date(2024, 3, 15),
                        None,
                    ],
                    "discharge_date": [
                        date(2024, 1, 10),
                        date(2024, 2, 15),
                        date(2024, 3, 20),
                        None,
                    ],
                    "diagnosis_code_1": ["I21.9", "I63.9", "I21.9", "J44.1"],
                    "facility_npi": ["NPI1", "NPI2", "NPI3", "NPI4"],
                }
            )
        )

    @pytest.mark.unit
    def test_filters_institutional_inpatient(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        value_sets = {
            "icd10cm_to_ccs": pl.DataFrame().lazy(),
            "exclusion_dx": pl.DataFrame().lazy(),
            "always_planned_dx": pl.DataFrame().lazy(),
        }

        result = ReadmissionsEnhancedTransform.identify_index_admissions(
            self._make_claims(), value_sets, {}
        ).collect()

        # P3 is professional -> excluded
        # P4 has null admission_date -> excluded
        # P1, P2 are valid institutional inpatient with 11x bill types
        assert result.height == 2
        assert set(result["person_id"].to_list()) == {"P1", "P2"}

    @pytest.mark.unit
    def test_expected_columns(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        value_sets = {
            "icd10cm_to_ccs": pl.DataFrame().lazy(),
            "exclusion_dx": pl.DataFrame().lazy(),
            "always_planned_dx": pl.DataFrame().lazy(),
        }

        result = ReadmissionsEnhancedTransform.identify_index_admissions(
            self._make_claims(), value_sets, {}
        ).collect()

        for col in [
            "person_id",
            "index_claim_id",
            "index_admission_date",
            "index_discharge_date",
            "index_principal_diagnosis",
            "index_ccs_category",
            "index_facility",
        ]:
            assert col in result.columns

    @pytest.mark.unit
    def test_with_exclusion_dx(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        icd10_to_ccs = _lazy(
            pl.DataFrame(
                {
                    "icd_10_cm": ["I21.9", "I63.9"],
                    "ccs_category": ["CCS100", "CCS109"],
                }
            )
        )
        exclusion_dx = _lazy(
            pl.DataFrame({"ccs_category": ["CCS100"]})
        )

        value_sets = {
            "icd10cm_to_ccs": icd10_to_ccs,
            "exclusion_dx": exclusion_dx,
            "always_planned_dx": pl.DataFrame().lazy(),
        }

        result = ReadmissionsEnhancedTransform.identify_index_admissions(
            self._make_claims(), value_sets, {}
        ).collect()

        # CCS100 is excluded, so P1 should be gone
        person_ids = result["person_id"].to_list()
        assert "P1" not in person_ids

    @pytest.mark.unit
    def test_with_always_planned_dx(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        icd10_to_ccs = _lazy(
            pl.DataFrame(
                {
                    "icd_10_cm": ["I21.9", "I63.9"],
                    "ccs_category": ["CCS100", "CCS109"],
                }
            )
        )
        always_planned_dx = _lazy(
            pl.DataFrame({"ccs_category": ["CCS109"]})
        )

        value_sets = {
            "icd10cm_to_ccs": icd10_to_ccs,
            "exclusion_dx": pl.DataFrame().lazy(),
            "always_planned_dx": always_planned_dx,
        }

        result = ReadmissionsEnhancedTransform.identify_index_admissions(
            self._make_claims(), value_sets, {}
        ).collect()

        # CCS109 (P2) is always planned -> excluded
        person_ids = result["person_id"].to_list()
        assert "P2" not in person_ids


class TestReadmissionsIdentifyReadmissions:
    """Tests for ReadmissionsEnhancedTransform.identify_readmissions."""

    @pytest.mark.unit
    def test_30_day_window(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        index_admissions = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "index_claim_id": ["C1"],
                    "index_admission_date": [date(2024, 1, 5)],
                    "index_discharge_date": [date(2024, 1, 10)],
                    "index_principal_diagnosis": ["I21.9"],
                    "index_ccs_category": ["CCS100"],
                    "index_facility": ["NPI1"],
                }
            )
        )
        claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1", "P1"],
                    "claim_id": ["C1", "C2", "C3"],
                    "claim_type": ["institutional"] * 3,
                    "bill_type_code": ["111", "111", "111"],
                    "admission_date": [
                        date(2024, 1, 5),
                        date(2024, 1, 20),  # within 30 days
                        date(2024, 3, 1),  # outside 30 days
                    ],
                    "discharge_date": [
                        date(2024, 1, 10),
                        date(2024, 1, 25),
                        date(2024, 3, 5),
                    ],
                    "diagnosis_code_1": ["I21.9", "I50.9", "J44.1"],
                    "procedure_code_1": [None, None, None],
                }
            )
        )

        result = ReadmissionsEnhancedTransform.identify_readmissions(
            index_admissions, claims, {}, {}
        ).collect()

        # C2 is within 30 days, C3 is not, C1 is the index (same encounter excluded)
        assert result.height == 1
        assert result["readmit_claim_id"][0] == "C2"
        assert result["days_to_readmission"][0] == 10

    @pytest.mark.unit
    def test_custom_lookback(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        index = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "index_claim_id": ["C1"],
                    "index_admission_date": [date(2024, 1, 5)],
                    "index_discharge_date": [date(2024, 1, 10)],
                    "index_principal_diagnosis": ["I21.9"],
                    "index_ccs_category": ["CCS100"],
                    "index_facility": ["NPI1"],
                }
            )
        )
        claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "claim_id": ["C2"],
                    "claim_type": ["institutional"],
                    "bill_type_code": ["111"],
                    "admission_date": [date(2024, 1, 20)],
                    "discharge_date": [date(2024, 1, 25)],
                    "diagnosis_code_1": ["I50.9"],
                    "procedure_code_1": [None],
                }
            )
        )

        # With lookback=5, the 10-day gap should NOT qualify
        result = ReadmissionsEnhancedTransform.identify_readmissions(
            index, claims, {}, {"lookback_days": 5}
        ).collect()

        assert result.height == 0

    @pytest.mark.unit
    def test_no_readmissions(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        index = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "index_claim_id": ["C1"],
                    "index_admission_date": [date(2024, 1, 5)],
                    "index_discharge_date": [date(2024, 1, 10)],
                    "index_principal_diagnosis": ["I21.9"],
                    "index_ccs_category": ["CCS100"],
                    "index_facility": ["NPI1"],
                }
            )
        )
        # No subsequent claims
        claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": pl.Series([], dtype=pl.Utf8),
                    "claim_id": pl.Series([], dtype=pl.Utf8),
                    "claim_type": pl.Series([], dtype=pl.Utf8),
                    "bill_type_code": pl.Series([], dtype=pl.Utf8),
                    "admission_date": pl.Series([], dtype=pl.Date),
                    "discharge_date": pl.Series([], dtype=pl.Date),
                    "diagnosis_code_1": pl.Series([], dtype=pl.Utf8),
                    "procedure_code_1": pl.Series([], dtype=pl.Utf8),
                }
            )
        )

        result = ReadmissionsEnhancedTransform.identify_readmissions(
            index, claims, {}, {}
        ).collect()

        assert result.height == 0


class TestReadmissionsClassifyPlannedVsUnplanned:
    """Tests for ReadmissionsEnhancedTransform.classify_planned_vs_unplanned."""

    def _make_pairs(self) -> pl.LazyFrame:
        return _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2"],
                    "index_claim_id": ["C1", "C3"],
                    "index_admission_date": [date(2024, 1, 5), date(2024, 2, 5)],
                    "index_discharge_date": [date(2024, 1, 10), date(2024, 2, 10)],
                    "index_principal_diagnosis": ["I21.9", "I63.9"],
                    "index_ccs_category": ["CCS100", "CCS109"],
                    "index_facility": ["NPI1", "NPI2"],
                    "readmit_claim_id": ["C2", "C4"],
                    "readmit_admission_date": [date(2024, 1, 20), date(2024, 2, 20)],
                    "readmit_discharge_date": [date(2024, 1, 25), date(2024, 2, 25)],
                    "readmit_principal_diagnosis": ["V58.11", "I50.9"],
                    "readmit_principal_procedure": ["PX_CHEMO", "PX_NONE"],
                    "days_to_readmission": [10, 10],
                }
            )
        )

    @pytest.mark.unit
    def test_with_empty_value_sets(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        value_sets = {
            "icd10cm_to_ccs": pl.DataFrame().lazy(),
            "icd10pcs_to_ccs": pl.DataFrame().lazy(),
            "always_planned_dx": pl.DataFrame().lazy(),
            "always_planned_px": pl.DataFrame().lazy(),
            "potentially_planned_px_ccs": pl.DataFrame().lazy(),
            "acute_diagnosis_ccs": pl.DataFrame().lazy(),
        }

        result = ReadmissionsEnhancedTransform.classify_planned_vs_unplanned(
            self._make_pairs(), value_sets, {}
        ).collect()

        assert "planned_readmission" in result.columns
        assert "readmission_type" in result.columns
        # With empty value sets, all should be unplanned
        assert all(t == "unplanned" for t in result["readmission_type"].to_list())

    @pytest.mark.unit
    def test_always_planned_classification(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        icd10_to_ccs = _lazy(
            pl.DataFrame(
                {
                    "icd_10_cm": ["V58.11", "I50.9"],
                    "ccs_category": ["CCS_CHEMO", "CCS_CHF"],
                }
            )
        )
        always_planned_dx = _lazy(
            pl.DataFrame({"ccs_category": ["CCS_CHEMO"]})
        )

        value_sets = {
            "icd10cm_to_ccs": icd10_to_ccs,
            "icd10pcs_to_ccs": pl.DataFrame().lazy(),
            "always_planned_dx": always_planned_dx,
            "always_planned_px": pl.DataFrame().lazy(),
            "potentially_planned_px_ccs": pl.DataFrame().lazy(),
            "acute_diagnosis_ccs": pl.DataFrame().lazy(),
        }

        result = ReadmissionsEnhancedTransform.classify_planned_vs_unplanned(
            self._make_pairs(), value_sets, {}
        ).collect()

        # P1 readmission dx V58.11 -> CCS_CHEMO -> always planned
        p1 = result.filter(pl.col("person_id") == "P1")
        assert p1["readmission_type"][0] == "always_planned"
        assert p1["planned_readmission"][0] is True

        p2 = result.filter(pl.col("person_id") == "P2")
        assert p2["readmission_type"][0] == "unplanned"


class TestReadmissionsAssignSpecialtyCohorts:
    """Tests for ReadmissionsEnhancedTransform.assign_specialty_cohorts."""

    def _make_pairs(self) -> pl.LazyFrame:
        return _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2"],
                    "index_ccs_category": ["CCS100", "CCS200"],
                }
            )
        )

    @pytest.mark.unit
    def test_no_cohort_mapping_defaults_to_general(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        value_sets = {"specialty_cohort": pl.DataFrame().lazy()}
        result = ReadmissionsEnhancedTransform.assign_specialty_cohorts(
            self._make_pairs(), value_sets, {}
        ).collect()

        assert all(c == "general" for c in result["specialty_cohort"].to_list())

    @pytest.mark.unit
    def test_with_cohort_mapping(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        specialty = _lazy(
            pl.DataFrame(
                {
                    "ccs_category": ["CCS100"],
                    "cohort_name": ["cardiovascular"],
                }
            )
        )
        value_sets = {"specialty_cohort": specialty}
        result = ReadmissionsEnhancedTransform.assign_specialty_cohorts(
            self._make_pairs(), value_sets, {}
        ).collect()

        p1 = result.filter(pl.col("person_id") == "P1")
        assert p1["specialty_cohort"][0] == "cardiovascular"

        p2 = result.filter(pl.col("person_id") == "P2")
        assert p2["specialty_cohort"][0] == "general"  # unmapped -> general


class TestReadmissionsCalculateEnhanced:
    """Tests for ReadmissionsEnhancedTransform.calculate_enhanced_readmissions."""

    @pytest.mark.unit
    def test_end_to_end(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1"],
                    "claim_id": ["C1", "C2"],
                    "claim_type": ["institutional", "institutional"],
                    "bill_type_code": ["111", "111"],
                    "admission_date": [date(2024, 1, 5), date(2024, 1, 20)],
                    "discharge_date": [date(2024, 1, 10), date(2024, 1, 25)],
                    "diagnosis_code_1": ["I21.9", "I50.9"],
                    "procedure_code_1": [None, None],
                    "facility_npi": ["NPI1", "NPI1"],
                }
            )
        )
        value_sets = {
            "icd10cm_to_ccs": pl.DataFrame().lazy(),
            "icd10pcs_to_ccs": pl.DataFrame().lazy(),
            "exclusion_dx": pl.DataFrame().lazy(),
            "always_planned_dx": pl.DataFrame().lazy(),
            "always_planned_px": pl.DataFrame().lazy(),
            "potentially_planned_px_ccs": pl.DataFrame().lazy(),
            "potentially_planned_px_icd10": pl.DataFrame().lazy(),
            "acute_diagnosis_ccs": pl.DataFrame().lazy(),
            "acute_diagnosis_icd10": pl.DataFrame().lazy(),
            "specialty_cohort": pl.DataFrame().lazy(),
            "surgery_gyn_cohort": pl.DataFrame().lazy(),
        }

        pairs, by_cohort, overall = (
            ReadmissionsEnhancedTransform.calculate_enhanced_readmissions(
                claims, value_sets, {}
            )
        )

        assert isinstance(pairs, pl.LazyFrame)
        assert isinstance(by_cohort, pl.LazyFrame)
        assert isinstance(overall, pl.LazyFrame)

        pairs_df = pairs.collect()
        assert pairs_df.height >= 1

        overall_df = overall.collect()
        assert "readmission_type" in overall_df.columns
        assert "readmission_count" in overall_df.columns


class TestReadmissionsEnhancedV2:
    """Tests for ReadmissionsEnhancedTransform."""

    def _make_claims(self):
        return pl.DataFrame({
            "person_id": ["P1", "P1", "P1"],
            "claim_id": ["C1", "C2", "C3"],
            "claim_type": ["institutional", "institutional", "institutional"],
            "bill_type_code": ["111", "111", "111"],
            "admission_date": [
                date(2024, 1, 1),
                date(2024, 1, 20),
                date(2024, 6, 1),
            ],
            "discharge_date": [
                date(2024, 1, 5),
                date(2024, 1, 25),
                date(2024, 6, 5),
            ],
            "diagnosis_code_1": ["A01", "A02", "A03"],
            "procedure_code_1": [None, None, None],
            "facility_npi": ["NPI1", "NPI1", "NPI1"],
        }).lazy()

    @pytest.mark.unit
    def test_load_readmissions_value_sets_with_files(self, tmp_path):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        # Create minimal parquet files
        file_mappings = {
            "acute_diagnosis_icd10": "value_sets_readmissions_acute_diagnosis_icd_10_cm.parquet",
            "acute_diagnosis_ccs": "value_sets_readmissions_acute_diagnosis_ccs.parquet",
            "always_planned_dx": "value_sets_readmissions_always_planned_ccs_diagnosis_category.parquet",
            "always_planned_px": "value_sets_readmissions_always_planned_ccs_procedure_category.parquet",
            "potentially_planned_px_ccs": "value_sets_readmissions_potentially_planned_ccs_procedure_category.parquet",
            "potentially_planned_px_icd10": "value_sets_readmissions_potentially_planned_icd_10_pcs.parquet",
            "exclusion_dx": "value_sets_readmissions_exclusion_ccs_diagnosis_category.parquet",
            "icd10cm_to_ccs": "value_sets_readmissions_icd_10_cm_to_ccs.parquet",
            "icd10pcs_to_ccs": "value_sets_readmissions_icd_10_pcs_to_ccs.parquet",
            "specialty_cohort": "value_sets_readmissions_specialty_cohort.parquet",
            "surgery_gyn_cohort": "value_sets_readmissions_surgery_gynecology_cohort.parquet",
        }
        for filename in file_mappings.values():
            pl.DataFrame({"dummy": [1]}).write_parquet(tmp_path / filename)

        result = ReadmissionsEnhancedTransform.load_readmissions_value_sets(tmp_path)
        assert len(result) == 11
        for key in file_mappings:
            assert key in result

    @pytest.mark.unit
    def test_identify_index_admissions(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        value_sets = {
            "icd10cm_to_ccs": pl.DataFrame().lazy(),
            "exclusion_dx": pl.DataFrame().lazy(),
            "always_planned_dx": pl.DataFrame().lazy(),
        }
        config = {}
        result = ReadmissionsEnhancedTransform.identify_index_admissions(
            self._make_claims(), value_sets, config
        ).collect()

        assert result.height == 3
        assert "index_claim_id" in result.columns
        assert "index_admission_date" in result.columns

    @pytest.mark.unit
    def test_identify_readmissions(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        index_admissions = pl.DataFrame({
            "person_id": ["P1"],
            "index_claim_id": ["C1"],
            "index_admission_date": [date(2024, 1, 1)],
            "index_discharge_date": [date(2024, 1, 5)],
            "index_principal_diagnosis": ["A01"],
            "index_ccs_category": [None],
            "index_facility": ["NPI1"],
        }).lazy()

        value_sets = {}
        config = {"lookback_days": 30}

        result = ReadmissionsEnhancedTransform.identify_readmissions(
            index_admissions, self._make_claims(), value_sets, config
        ).collect()

        # C2 admitted on Jan 20, within 30 days of C1 discharge (Jan 5)
        assert result.height >= 1
        assert "days_to_readmission" in result.columns

    @pytest.mark.unit
    def test_classify_planned_vs_unplanned_no_value_sets(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        pairs = pl.DataFrame({
            "person_id": ["P1"],
            "index_claim_id": ["C1"],
            "readmit_claim_id": ["C2"],
            "readmit_principal_diagnosis": ["A02"],
            "readmit_principal_procedure": [None],
            "days_to_readmission": [15],
        }).lazy()

        value_sets = {
            "icd10cm_to_ccs": pl.DataFrame().lazy(),
            "icd10pcs_to_ccs": pl.DataFrame().lazy(),
            "always_planned_dx": pl.DataFrame().lazy(),
            "always_planned_px": pl.DataFrame().lazy(),
            "potentially_planned_px_ccs": pl.DataFrame().lazy(),
            "acute_diagnosis_ccs": pl.DataFrame().lazy(),
        }
        config = {}

        result = ReadmissionsEnhancedTransform.classify_planned_vs_unplanned(
            pairs, value_sets, config
        ).collect()

        assert "planned_readmission" in result.columns
        assert "readmission_type" in result.columns
        assert result["readmission_type"][0] == "unplanned"

    @pytest.mark.unit
    def test_assign_specialty_cohorts_no_value_set(self):
        from acoharmony._transforms.readmissions_enhanced import (
            ReadmissionsEnhancedTransform,
        )

        pairs = pl.DataFrame({
            "person_id": ["P1"],
            "index_ccs_category": [None],
        }).lazy()

        value_sets = {"specialty_cohort": pl.DataFrame().lazy()}
        config = {}

        result = ReadmissionsEnhancedTransform.assign_specialty_cohorts(
            pairs, value_sets, config
        ).collect()

        assert result["specialty_cohort"][0] == "general"
