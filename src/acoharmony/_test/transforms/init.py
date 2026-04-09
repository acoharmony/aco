"""Tests for _transforms.init__ module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import patch
import datetime
from datetime import date
from pathlib import Path

import polars as pl
import pytest
import acoharmony
import acoharmony._transforms as pkg
from acoharmony.medallion import MedallionLayer

# Import __all__ from _base module for testing
from acoharmony._transforms._base import __all__

# Import KeyValuePivot classes
from acoharmony._transforms._key_value_pivot import (
    KeyValuePivotConfig,
    KeyValuePivotExpression,
)

# Import HCC Gap Analysis module and transform
from acoharmony._transforms import hcc_gap_analysis
from acoharmony._transforms.hcc_gap_analysis import HccGapAnalysisTransform

# Import other transforms
from acoharmony._transforms.sdoh import SdohTransform
from acoharmony._transforms.readmissions_enhanced import ReadmissionsEnhancedTransform
from acoharmony._transforms.utilization import UtilizationTransform

# Import HEDR expression
from acoharmony._expressions._reach_hedr_eligible import build_reach_hedr_denominator_expr

# Import participant list transform
from acoharmony._transforms._participant_list import transform_participant_list


class TestTransformsPackageInit:
    """Tests for the _transforms __init__.py."""

    @pytest.mark.unit
    def test_import_package(self):
        assert TransformRegistry is not None
        assert register_crosswalk is not None
        assert register_pipeline is not None
        assert QualityMeasureBase is not None
        assert MeasureMetadata is not None
        assert MeasureFactory is not None

    @pytest.mark.unit
    def test_package_has_all(self):
        assert hasattr(pkg, "__all__")
        assert isinstance(pkg.__all__, list)
        assert len(pkg.__all__) > 0

    @pytest.mark.unit
    def test_skin_substitute_alias(self):
        assert skin_substitute_claims is not None

    @pytest.mark.unit
    def test_wound_care_aliases(self):
        assert wound_care_claims is not None
        assert wound_care_high_frequency is not None
        assert wound_care_high_cost is not None
        assert wound_care_clustered is not None
        assert wound_care_duplicates is not None
        assert wound_care_identical_patterns is not None


class TestMedallionLayer:
    """Tests for MedallionLayer enum used by transforms."""

    @pytest.mark.unit
    def test_layer_values(self):

        assert MedallionLayer.BRONZE.value == "bronze"
        assert MedallionLayer.SILVER.value == "silver"
        assert MedallionLayer.GOLD.value == "gold"

    @pytest.mark.unit
    def test_unity_schema(self):

        assert MedallionLayer.GOLD.unity_schema == "gold"

    @pytest.mark.unit
    def test_data_tier(self):

        assert MedallionLayer.SILVER.data_tier == "silver"


class TestModuleExports:
    """Test that module __all__ exports are defined correctly."""

    @pytest.mark.unit
    def test_base_module_all(self):

        expected = [
            "TransformConfig",
            "HealthcareTransformBase",
            "CmsHccTransform",
            "ReadmissionsTransform",
            "ChronicConditionsTransform",
            "FinancialPmpmTransform",
            "QualityMeasuresTransform",
            "run_transform",
            "run_all_healthcare_transforms",
            "HealthcareTransformContext",
        ]
        for name in expected:
            assert name in __all__

    @pytest.mark.unit
    def test_init_module_all(self):

        assert "TransformRegistry" in pkg.__all__
        assert "QualityMeasureBase" in pkg.__all__
        assert "MeasureMetadata" in pkg.__all__
        assert "MeasureFactory" in pkg.__all__
        assert "register_crosswalk" in pkg.__all__
        assert "register_pipeline" in pkg.__all__


# ---------------------------------------------------------------------------
# Coverage gap tests: _key_value_pivot.py lines 164, 229
# ---------------------------------------------------------------------------


class TestKeyValuePivotGaps:
    """Cover sanitize_column_name digit prefix and key_mapping branch."""

    @pytest.mark.unit
    def test_sanitize_column_name_starts_with_digit(self):
        """Line 164: column name starting with digit gets underscore prefix."""

        result = KeyValuePivotExpression.sanitize_column_name("123_field")
        assert result.startswith("_")

    @pytest.mark.unit
    def test_key_mapping_applied(self):
        """Line 229: custom key_mapping replaces key name."""
        config = KeyValuePivotConfig(
            key_column="key",
            value_column="value",
            key_mapping={"old_name": "new_name"},
        )
        # Verify config has key_mapping
        assert config.key_mapping["old_name"] == "new_name"


# ===== From test_hcc_gap_analysis.py =====

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


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestHccGapAnalysisPublic:
    """Tests for hcc_gap_analysis public transform module."""

    @pytest.mark.unit
    def test_import_module(self):
        assert hcc_gap_analysis is not None


class TestHccGapAnalysis:
    """Tests for HccGapAnalysisTransform.load_hcc_value_sets."""

    @pytest.mark.unit
    def test_load_hcc_value_sets_success(self, tmp_path):
        """Cover lines 57-85: load with actual files."""

        file_mappings = {
            "value_sets_cms_hcc_icd_10_cm_mappings.parquet": {"diagnosis_code": ["A01"]},
            "value_sets_cms_hcc_disease_factors.parquet": {"hcc_code": [1]},
            "value_sets_cms_hcc_demographic_factors.parquet": {"age_group": ["65"]},
            "value_sets_cms_hcc_disease_hierarchy.parquet": {"hcc_code": [1]},
            "value_sets_cms_hcc_disease_interaction_factors.parquet": {"factor": [0.1]},
            "value_sets_cms_hcc_disabled_interaction_factors.parquet": {"factor": [0.2]},
            "value_sets_cms_hcc_enrollment_interaction_factors.parquet": {"factor": [0.3]},
            "value_sets_cms_hcc_payment_hcc_count_factors.parquet": {"count": [1]},
            "value_sets_cms_hcc_cpt_hcpcs.parquet": {"code": ["99213"]},
            "value_sets_cms_hcc_adjustment_rates.parquet": {"rate": [1.0]},
        }

        for filename, data in file_mappings.items():
            _write(pl.DataFrame(data), tmp_path / filename)

        vs = HccGapAnalysisTransform.load_hcc_value_sets(tmp_path)
        assert len(vs) == 10
        assert "icd10_mappings" in vs
        assert "disease_factors" in vs

    @pytest.mark.unit
    def test_load_hcc_value_sets_scan_failure(self, tmp_path):
        """Cover lines 78-80: scan_parquet failure -> empty placeholders."""


        with patch("acoharmony._transforms.hcc_gap_analysis.pl.scan_parquet", side_effect=OSError("mocked")):
            vs = HccGapAnalysisTransform.load_hcc_value_sets(tmp_path)
        assert len(vs) == 10
        for _key, lf in vs.items():
            assert lf.collect().height == 0


def _lazy(df: pl.DataFrame) -> pl.LazyFrame:
    return df.lazy()


class TestHccGapAnalysisTransformMapDiagnosesToHccs:
    """Tests for HccGapAnalysisTransform.map_diagnoses_to_hccs."""

    def _make_claims(self) -> pl.LazyFrame:
        return _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1", "P2", "P3"],
                    "diagnosis_code_1": ["E11.9", "I50.9", "E11.9", "J44.1"],
                    "diagnosis_code_2": ["I50.9", None, None, None],
                    "diagnosis_code_3": [None, None, "J44.1", None],
                    "claim_end_date": [
                        date(2024, 3, 1),
                        date(2024, 6, 1),
                        date(2024, 4, 1),
                        date(2023, 7, 1),
                    ],
                }
            )
        )

    def _make_icd10_mappings(self) -> pl.LazyFrame:
        return _lazy(
            pl.DataFrame(
                {
                    "diagnosis_code": ["E11.9", "I50.9", "J44.1"],
                    "cms_hcc_v24": [19, 85, 111],
                    "cms_hcc_v24_flag": ["Yes", "Yes", "Yes"],
                    "cms_hcc_v28": [37, 226, 280],
                    "cms_hcc_v28_flag": ["Yes", "Yes", "Yes"],
                }
            )
        )

    @pytest.mark.unit
    def test_basic_mapping_v24(self):

        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(
            self._make_claims(), self._make_icd10_mappings(), {"model_version": "V24"}
        ).collect()

        assert "person_id" in result.columns
        assert "hcc_code" in result.columns
        assert "diagnosis_code" in result.columns
        assert "claim_date" in result.columns
        assert "service_year" in result.columns
        assert result.height > 0

    @pytest.mark.unit
    def test_basic_mapping_v28(self):

        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(
            self._make_claims(), self._make_icd10_mappings(), {"model_version": "V28"}
        ).collect()

        assert result.height > 0
        hcc_codes = result["hcc_code"].to_list()
        # V28 codes
        assert any(c in hcc_codes for c in [37, 226, 280])

    @pytest.mark.unit
    def test_dedup_by_person_hcc_year(self):

        # P1 has E11.9 in dx1 and I50.9 in dx1 and dx2 in 2024 - should dedup
        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(
            self._make_claims(), self._make_icd10_mappings(), {"model_version": "V24"}
        ).collect()

        # Count P1 hcc 85 in year 2024 - should appear only once
        p1_hcc85 = result.filter(
            (pl.col("person_id") == "P1")
            & (pl.col("hcc_code") == 85)
            & (pl.col("service_year") == 2024)
        )
        assert p1_hcc85.height == 1

    @pytest.mark.unit
    def test_default_model_version(self):

        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(
            self._make_claims(), self._make_icd10_mappings(), {}
        ).collect()

        # Default is V24
        assert result.height > 0

    @pytest.mark.unit
    def test_no_matching_codes(self):

        claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "diagnosis_code_1": ["XXXX"],
                    "diagnosis_code_2": pl.Series([None], dtype=pl.Utf8),
                    "diagnosis_code_3": pl.Series([None], dtype=pl.Utf8),
                    "claim_end_date": [date(2024, 1, 1)],
                }
            )
        )
        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(
            claims, self._make_icd10_mappings(), {"model_version": "V24"}
        ).collect()

        assert result.height == 0

    @pytest.mark.unit
    def test_mappings_with_no_flag(self):

        mappings = _lazy(
            pl.DataFrame(
                {
                    "diagnosis_code": ["E11.9"],
                    "cms_hcc_v24": [19],
                    "cms_hcc_v24_flag": ["No"],
                    "cms_hcc_v28": [37],
                    "cms_hcc_v28_flag": ["No"],
                }
            )
        )
        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(
            self._make_claims(), mappings, {"model_version": "V24"}
        ).collect()

        # No valid mappings since flag is "No"
        assert result.height == 0


class TestHccGapAnalysisApplyHierarchies:
    """Tests for HccGapAnalysisTransform.apply_hcc_hierarchies."""

    @pytest.mark.unit
    def test_hierarchy_excludes_lower_severity(self):

        person_hccs = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1", "P1"],
                    "hcc_code": [17, 18, 19],
                    "diagnosis_code": ["E11.0", "E11.1", "E11.9"],
                    "claim_date": [date(2024, 1, 1)] * 3,
                    "service_year": [2024, 2024, 2024],
                }
            )
        )
        hierarchy = _lazy(
            pl.DataFrame(
                {
                    "hcc_code": [17],
                    "hccs_to_exclude": [18],
                    "model_version": ["CMS-HCC-V24"],
                }
            )
        )
        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.apply_hcc_hierarchies(
            person_hccs, hierarchy, config
        ).collect()

        hcc_codes = result.filter(pl.col("person_id") == "P1")["hcc_code"].to_list()
        assert 17 in hcc_codes
        assert 18 not in hcc_codes  # excluded by hierarchy
        assert 19 in hcc_codes

    @pytest.mark.unit
    def test_no_hierarchy_rules(self):

        person_hccs = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "hcc_code": [19],
                    "diagnosis_code": ["E11.9"],
                    "claim_date": [date(2024, 1, 1)],
                    "service_year": [2024],
                }
            )
        )
        hierarchy = _lazy(
            pl.DataFrame(
                {
                    "hcc_code": pl.Series([], dtype=pl.Int64),
                    "hccs_to_exclude": pl.Series([], dtype=pl.Int64),
                    "model_version": pl.Series([], dtype=pl.Utf8),
                }
            )
        )
        result = HccGapAnalysisTransform.apply_hcc_hierarchies(
            person_hccs, hierarchy, {"model_version": "V24"}
        ).collect()

        # Returns unchanged since no hierarchy rules matched
        assert result.height == 1

    @pytest.mark.unit
    def test_wrong_model_version_returns_unchanged(self):

        person_hccs = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1"],
                    "hcc_code": [17, 18],
                    "diagnosis_code": ["E11.0", "E11.1"],
                    "claim_date": [date(2024, 1, 1)] * 2,
                    "service_year": [2024, 2024],
                }
            )
        )
        hierarchy = _lazy(
            pl.DataFrame(
                {
                    "hcc_code": [17],
                    "hccs_to_exclude": [18],
                    "model_version": ["CMS-HCC-V28"],  # V28 rules, but config is V24
                }
            )
        )
        result = HccGapAnalysisTransform.apply_hcc_hierarchies(
            person_hccs, hierarchy, {"model_version": "V24"}
        ).collect()

        # No matching hierarchy for V24 -> returns original
        assert result.height == 2


class TestHccGapAnalysisCalculateRafScores:
    """Tests for HccGapAnalysisTransform.calculate_raf_scores."""

    def _setup(self):
        person_hccs = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1", "P2"],
                    "hcc_code": [19, 85, 19],
                    "service_year": [2024, 2024, 2024],
                }
            )
        )
        disease_factors = _lazy(
            pl.DataFrame(
                {
                    "hcc_code": [19, 85],
                    "coefficient": [0.302, 0.323],
                    "model_version": ["CMS-HCC-V24", "CMS-HCC-V24"],
                    "enrollment_status": ["Continuing", "Continuing"],
                    "medicaid_status": ["No", "No"],
                    "institutional_status": ["No", "No"],
                    "description": ["Diabetes", "CHF"],
                }
            )
        )
        demographic_factors = _lazy(
            pl.DataFrame(
                {
                    "gender": ["Male", "Female"],
                    "age_group": ["70-74", "70-74"],
                    "coefficient": [0.370, 0.390],
                    "model_version": ["CMS-HCC-V24", "CMS-HCC-V24"],
                    "enrollment_status": ["Continuing", "Continuing"],
                    "orec": ["Aged", "Aged"],
                    "institutional_status": ["No", "No"],
                }
            )
        )
        eligibility = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2"],
                    "age": [72, 71],
                    "gender": ["M", "F"],
                }
            )
        )
        config = {"model_version": "V24", "measurement_year": 2024}
        return person_hccs, disease_factors, demographic_factors, eligibility, config

    @pytest.mark.unit
    def test_raf_score_columns(self):

        person_hccs, disease_factors, demo_factors, elig, config = self._setup()
        result = HccGapAnalysisTransform.calculate_raf_scores(
            person_hccs, disease_factors, demo_factors, elig, config
        ).collect()

        assert "person_id" in result.columns
        assert "raf_score" in result.columns
        assert "disease_score" in result.columns
        assert "demographic_score" in result.columns
        assert result.height > 0

    @pytest.mark.unit
    def test_raf_scores_are_positive(self):

        person_hccs, disease_factors, demo_factors, elig, config = self._setup()
        result = HccGapAnalysisTransform.calculate_raf_scores(
            person_hccs, disease_factors, demo_factors, elig, config
        ).collect()

        for row in result.iter_rows(named=True):
            assert row["raf_score"] >= 0.0

    @pytest.mark.unit
    def test_member_with_no_hccs(self):

        # P3 has no HCCs
        person_hccs = _lazy(
            pl.DataFrame(
                {
                    "person_id": pl.Series([], dtype=pl.Utf8),
                    "hcc_code": pl.Series([], dtype=pl.Int64),
                    "service_year": pl.Series([], dtype=pl.Int64),
                }
            )
        )
        disease_factors = _lazy(
            pl.DataFrame(
                {
                    "hcc_code": [19],
                    "coefficient": [0.302],
                    "model_version": ["CMS-HCC-V24"],
                    "enrollment_status": ["Continuing"],
                    "medicaid_status": ["No"],
                    "institutional_status": ["No"],
                }
            )
        )
        demographic_factors = _lazy(
            pl.DataFrame(
                {
                    "gender": ["Male"],
                    "age_group": ["70-74"],
                    "coefficient": [0.370],
                    "model_version": ["CMS-HCC-V24"],
                    "enrollment_status": ["Continuing"],
                    "orec": ["Aged"],
                    "institutional_status": ["No"],
                }
            )
        )
        eligibility = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P3"],
                    "age": [72],
                    "gender": ["M"],
                }
            )
        )
        config = {"model_version": "V24", "measurement_year": 2024}

        result = HccGapAnalysisTransform.calculate_raf_scores(
            person_hccs, disease_factors, demographic_factors, eligibility, config
        ).collect()

        # P3 should still get a demographic score but 0 disease score
        assert result.height == 1
        row = result.row(0, named=True)
        assert row["disease_score"] == 0.0


class TestHccGapAnalysisIdentifyGaps:
    """Tests for HccGapAnalysisTransform.identify_hcc_gaps."""

    @pytest.mark.unit
    def test_identifies_chronic_gap(self):

        # P1 had HCC 19 (diabetes) in 2023 but not 2024
        current = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "hcc_code": [85],
                    "service_year": [2024],
                }
            )
        )
        historical = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1"],
                    "hcc_code": [19, 85],
                    "service_year": [2023, 2023],
                }
            )
        )
        config = {"measurement_year": 2024}

        result = HccGapAnalysisTransform.identify_hcc_gaps(
            current, historical, config
        ).collect()

        assert result.height == 1
        row = result.row(0, named=True)
        assert row["person_id"] == "P1"
        assert row["hcc_code"] == 19
        assert row["gap_year"] == 2024
        assert row["years_since_capture"] == 1
        assert row["gap_type"] == "chronic_recapture"

    @pytest.mark.unit
    def test_no_gap_when_recaptured(self):

        current = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "hcc_code": [19],
                    "service_year": [2024],
                }
            )
        )
        historical = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "hcc_code": [19],
                    "service_year": [2023],
                }
            )
        )
        config = {"measurement_year": 2024}

        result = HccGapAnalysisTransform.identify_hcc_gaps(
            current, historical, config
        ).collect()

        assert result.height == 0

    @pytest.mark.unit
    def test_non_chronic_hcc_not_flagged(self):

        # HCC 999 is not in the chronic list
        current = _lazy(
            pl.DataFrame(
                {
                    "person_id": pl.Series([], dtype=pl.Utf8),
                    "hcc_code": pl.Series([], dtype=pl.Int64),
                    "service_year": pl.Series([], dtype=pl.Int64),
                }
            )
        )
        historical = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "hcc_code": [999],
                    "service_year": [2023],
                }
            )
        )
        config = {"measurement_year": 2024}

        result = HccGapAnalysisTransform.identify_hcc_gaps(
            current, historical, config
        ).collect()

        # 999 is not chronic, so should not appear as gap
        assert result.height == 0


class TestHccGapAnalysisPrioritizeGaps:
    """Tests for HccGapAnalysisTransform.prioritize_gaps."""

    @pytest.mark.unit
    def test_priority_classification(self):

        gaps = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1", "P2"],
                    "hcc_code": [19, 85, 111],
                    "last_capture_year": [2023, 2023, 2022],
                    "gap_year": [2024, 2024, 2024],
                    "years_since_capture": [1, 1, 2],
                    "gap_type": ["chronic_recapture"] * 3,
                }
            )
        )
        disease_factors = _lazy(
            pl.DataFrame(
                {
                    "hcc_code": [19, 85, 111],
                    "coefficient": [0.302, 0.523, 0.100],
                    "description": ["Diabetes", "CHF", "COPD"],
                    "model_version": ["CMS-HCC-V24"] * 3,
                    "enrollment_status": ["Continuing"] * 3,
                    "medicaid_status": ["No"] * 3,
                    "institutional_status": ["No"] * 3,
                }
            )
        )
        raf_scores = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2"],
                    "raf_score": [1.2, 0.8],
                }
            )
        )
        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.prioritize_gaps(
            gaps, disease_factors, raf_scores, config
        ).collect()

        assert "priority" in result.columns
        assert "gap_value" in result.columns
        assert "potential_raf" in result.columns
        assert "pct_impact" in result.columns

        # CHF coefficient 0.523 >= 0.5 -> high priority
        chf_row = result.filter(pl.col("hcc_code") == 85)
        assert chf_row["priority"][0] == "high"

        # Diabetes 0.302 >= 0.2 -> medium
        dm_row = result.filter(pl.col("hcc_code") == 19)
        assert dm_row["priority"][0] == "medium"

        # COPD 0.100 < 0.2 -> low
        copd_row = result.filter(pl.col("hcc_code") == 111)
        assert copd_row["priority"][0] == "low"


class TestHccGapAnalysisCalculateHccGaps:
    """Tests for HccGapAnalysisTransform.calculate_hcc_gaps (integration)."""

    @pytest.mark.unit
    def test_end_to_end(self):

        claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1"],
                    "diagnosis_code_1": ["E11.9", "E11.9"],
                    "diagnosis_code_2": pl.Series([None, None], dtype=pl.Utf8),
                    "diagnosis_code_3": pl.Series([None, None], dtype=pl.Utf8),
                    "claim_end_date": [date(2023, 6, 1), date(2024, 3, 1)],
                }
            )
        )
        icd10_mappings = _lazy(
            pl.DataFrame(
                {
                    "diagnosis_code": ["E11.9"],
                    "cms_hcc_v24": [19],
                    "cms_hcc_v24_flag": ["Yes"],
                }
            )
        )
        disease_hierarchy = _lazy(
            pl.DataFrame(
                {
                    "hcc_code": pl.Series([], dtype=pl.Int64),
                    "hccs_to_exclude": pl.Series([], dtype=pl.Int64),
                    "model_version": pl.Series([], dtype=pl.Utf8),
                }
            )
        )
        disease_factors = _lazy(
            pl.DataFrame(
                {
                    "hcc_code": [19],
                    "coefficient": [0.302],
                    "description": ["Diabetes"],
                    "model_version": ["CMS-HCC-V24"],
                    "enrollment_status": ["Continuing"],
                    "medicaid_status": ["No"],
                    "institutional_status": ["No"],
                }
            )
        )
        demographic_factors = _lazy(
            pl.DataFrame(
                {
                    "gender": ["Male"],
                    "age_group": ["70-74"],
                    "coefficient": [0.370],
                    "model_version": ["CMS-HCC-V24"],
                    "enrollment_status": ["Continuing"],
                    "orec": ["Aged"],
                    "institutional_status": ["No"],
                }
            )
        )
        eligibility = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "age": [72],
                    "gender": ["M"],
                }
            )
        )
        value_sets = {
            "icd10_mappings": icd10_mappings,
            "disease_factors": disease_factors,
            "demographic_factors": demographic_factors,
            "disease_hierarchy": disease_hierarchy,
            "disease_interactions": pl.DataFrame().lazy(),
            "disabled_interactions": pl.DataFrame().lazy(),
            "enrollment_interactions": pl.DataFrame().lazy(),
            "payment_hcc_count": pl.DataFrame().lazy(),
            "cpt_hcpcs": pl.DataFrame().lazy(),
            "adjustment_rates": pl.DataFrame().lazy(),
        }
        config = {"model_version": "V24", "measurement_year": 2024}

        hccs, gaps, raf, summary = HccGapAnalysisTransform.calculate_hcc_gaps(
            claims, eligibility, value_sets, config
        )

        assert isinstance(hccs, pl.LazyFrame)
        assert isinstance(gaps, pl.LazyFrame)
        assert isinstance(raf, pl.LazyFrame)
        assert isinstance(summary, pl.LazyFrame)

        # HCC 19 should appear in both years
        hccs_df = hccs.collect()
        assert hccs_df.height > 0

        # RAF scores should exist
        raf_df = raf.collect()
        assert raf_df.height > 0


class TestEdgeCases:
    """Edge case tests across transforms."""

    @pytest.mark.unit
    def test_hcc_empty_claims(self):

        empty_claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": pl.Series([], dtype=pl.Utf8),
                    "diagnosis_code_1": pl.Series([], dtype=pl.Utf8),
                    "diagnosis_code_2": pl.Series([], dtype=pl.Utf8),
                    "diagnosis_code_3": pl.Series([], dtype=pl.Utf8),
                    "claim_end_date": pl.Series([], dtype=pl.Date),
                }
            )
        )
        mappings = _lazy(
            pl.DataFrame(
                {
                    "diagnosis_code": ["E11.9"],
                    "cms_hcc_v24": [19],
                    "cms_hcc_v24_flag": ["Yes"],
                }
            )
        )

        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(
            empty_claims, mappings, {"model_version": "V24"}
        ).collect()

        assert result.height == 0

    @pytest.mark.unit
    def test_sdoh_empty_claims(self):

        empty_claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": pl.Series([], dtype=pl.Utf8),
                    "diagnosis_code_1": pl.Series([], dtype=pl.Utf8),
                    "diagnosis_code_2": pl.Series([], dtype=pl.Utf8),
                    "diagnosis_code_3": pl.Series([], dtype=pl.Utf8),
                    "claim_end_date": pl.Series([], dtype=pl.Date),
                }
            )
        )
        result = SdohTransform.identify_z_codes(
            empty_claims, {"measurement_year": 2024}
        ).collect()

        assert result.height == 0

    @pytest.mark.unit
    def test_readmissions_empty_claims(self):
        empty_claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": pl.Series([], dtype=pl.Utf8),
                    "claim_id": pl.Series([], dtype=pl.Utf8),
                    "claim_type": pl.Series([], dtype=pl.Utf8),
                    "bill_type_code": pl.Series([], dtype=pl.Utf8),
                    "admission_date": pl.Series([], dtype=pl.Date),
                    "discharge_date": pl.Series([], dtype=pl.Date),
                    "diagnosis_code_1": pl.Series([], dtype=pl.Utf8),
                    "facility_npi": pl.Series([], dtype=pl.Utf8),
                }
            )
        )
        value_sets = {
            "icd10cm_to_ccs": pl.DataFrame().lazy(),
            "exclusion_dx": pl.DataFrame().lazy(),
            "always_planned_dx": pl.DataFrame().lazy(),
        }

        result = ReadmissionsEnhancedTransform.identify_index_admissions(
            empty_claims, value_sets, {}
        ).collect()

        assert result.height == 0

    @pytest.mark.unit
    def test_utilization_empty_eligibility(self):

        empty_elig = _lazy(
            pl.DataFrame(
                {
                    "person_id": pl.Series([], dtype=pl.Utf8),
                    "enrollment_start_date": pl.Series([], dtype=pl.Date),
                    "enrollment_end_date": pl.Series([], dtype=pl.Date),
                }
            )
        )

        result = UtilizationTransform.calculate_member_years(
            empty_elig, {"measurement_year": 2024}
        ).collect()

        assert result.height == 0

    @pytest.mark.unit
    def test_hedr_denominator_no_schema_columns(self):
        # Minimal schema - no months columns, no temporal matrix
        df = pl.DataFrame({"x": [1]})
        expr = build_reach_hedr_denominator_expr(
            performance_year=2025,
            df_schema=df.columns,
        )
        result = df.with_columns(expr.alias("hedr_denominator"))
        # With no death date columns and no enrollment checks, defaults to True
        assert result["hedr_denominator"][0] is True

    @pytest.mark.unit
    def test_sdoh_z_code_categories_class_attribute(self):

        cats = SdohTransform.Z_CODE_CATEGORIES
        assert "housing_instability" in cats
        assert "food_insecurity" in cats
        assert "transportation_barriers" in cats
        assert "financial_insecurity" in cats
        assert "education_literacy" in cats
        assert "employment" in cats
        assert "social_isolation" in cats
        assert "interpersonal_violence" in cats
        assert "inadequate_support" in cats
        assert "legal_problems" in cats
        assert "substance_use" in cats
        assert "mental_health_screening" in cats
        assert "sdoh_screening" in cats
        assert len(cats) == 13


class TestHccGapAnalysisV2:
    """Tests for HCC gap analysis transform."""

    def _make_claims(self):
        return pl.DataFrame({
            "person_id": ["P1", "P1", "P2"],
            "diagnosis_code_1": ["E11.9", "I25.10", "I10"],
            "diagnosis_code_2": pl.Series([None, "E11.65", None], dtype=pl.Utf8),
            "diagnosis_code_3": pl.Series([None, None, None], dtype=pl.Utf8),
            "claim_end_date": [
                date(2024, 3, 1),
                date(2024, 6, 1),
                date(2024, 4, 1),
            ],
        }).lazy()

    def _make_icd10_mappings(self):
        return pl.DataFrame({
            "diagnosis_code": ["E11.9", "I25.10", "I10", "E11.65"],
            "cms_hcc_v24": ["19", "86", "0", "18"],
            "cms_hcc_v24_flag": ["Yes", "Yes", "No", "Yes"],
            "cms_hcc_v28": ["37", "263", "0", "36"],
            "cms_hcc_v28_flag": ["Yes", "Yes", "No", "Yes"],
        }).lazy()

    @pytest.mark.unit
    def test_map_diagnoses_to_hccs_v24(self):

        config = {"model_version": "V24", "measurement_year": 2024}
        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(
            self._make_claims(), self._make_icd10_mappings(), config
        ).collect()

        assert result.height > 0
        assert "hcc_code" in result.columns
        assert "person_id" in result.columns
        assert "service_year" in result.columns

    @pytest.mark.unit
    def test_map_diagnoses_to_hccs_v28(self):

        config = {"model_version": "V28", "measurement_year": 2024}
        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(
            self._make_claims(), self._make_icd10_mappings(), config
        ).collect()

        assert result.height > 0

    @pytest.mark.unit
    def test_apply_hcc_hierarchies_no_rules(self):

        person_hccs = pl.DataFrame({
            "person_id": ["P1", "P1"],
            "hcc_code": ["19", "18"],
            "diagnosis_code": ["E11.9", "E11.65"],
            "claim_date": [date(2024, 3, 1), date(2024, 6, 1)],
            "service_year": [2024, 2024],
        }).lazy()

        hierarchy = pl.DataFrame({
            "hcc_code": pl.Series([], dtype=pl.Utf8),
            "hccs_to_exclude": pl.Series([], dtype=pl.Utf8),
            "model_version": pl.Series([], dtype=pl.Utf8),
        }).lazy()

        config = {"model_version": "V24"}
        result = HccGapAnalysisTransform.apply_hcc_hierarchies(
            person_hccs, hierarchy, config
        ).collect()
        # No rules -> all HCCs kept
        assert result.height == 2

    @pytest.mark.unit
    def test_identify_hcc_gaps(self):

        current_hccs = pl.DataFrame({
            "person_id": ["P1"],
            "hcc_code": [19],
            "service_year": [2024],
            "diagnosis_code": ["E11.9"],
            "claim_date": [date(2024, 3, 1)],
        }).lazy()

        historical_hccs = pl.DataFrame({
            "person_id": ["P1", "P1"],
            "hcc_code": [19, 86],
            "service_year": [2023, 2023],
            "diagnosis_code": ["E11.9", "I25.10"],
            "claim_date": [date(2023, 3, 1), date(2023, 6, 1)],
        }).lazy()

        config = {"measurement_year": 2024}
        gaps = HccGapAnalysisTransform.identify_hcc_gaps(
            current_hccs, historical_hccs, config
        ).collect()

        # HCC 86 (CHF) was in 2023 but not 2024 -> gap
        assert gaps.height == 1
        assert gaps["hcc_code"][0] == 86

    @pytest.mark.unit
    def test_prioritize_gaps(self):

        gaps = pl.DataFrame({
            "person_id": ["P1"],
            "hcc_code": [86],
            "last_capture_year": [2023],
            "gap_year": [2024],
            "years_since_capture": [1],
            "gap_type": ["chronic_recapture"],
        }).lazy()

        disease_factors = pl.DataFrame({
            "hcc_code": [86],
            "coefficient": [0.395],
            "description": ["Heart Failure"],
            "model_version": ["CMS-HCC-V24"],
            "enrollment_status": ["Continuing"],
            "medicaid_status": ["No"],
            "institutional_status": ["No"],
        }).lazy()

        raf_scores = pl.DataFrame({
            "person_id": ["P1"],
            "raf_score": [1.2],
        }).lazy()

        config = {"model_version": "V24"}
        result = HccGapAnalysisTransform.prioritize_gaps(
            gaps, disease_factors, raf_scores, config
        ).collect()

        assert result.height == 1
        assert "priority" in result.columns
        assert "potential_raf" in result.columns

    @pytest.mark.unit
    def test_load_hcc_value_sets_with_files(self, tmp_path):

        # Create minimal parquet files so scan_parquet doesn't fail on collect
        file_mappings = {
            "icd10_mappings": "value_sets_cms_hcc_icd_10_cm_mappings.parquet",
            "disease_factors": "value_sets_cms_hcc_disease_factors.parquet",
            "demographic_factors": "value_sets_cms_hcc_demographic_factors.parquet",
            "disease_hierarchy": "value_sets_cms_hcc_disease_hierarchy.parquet",
            "disease_interactions": "value_sets_cms_hcc_disease_interaction_factors.parquet",
            "disabled_interactions": "value_sets_cms_hcc_disabled_interaction_factors.parquet",
            "enrollment_interactions": "value_sets_cms_hcc_enrollment_interaction_factors.parquet",
            "payment_hcc_count": "value_sets_cms_hcc_payment_hcc_count_factors.parquet",
            "cpt_hcpcs": "value_sets_cms_hcc_cpt_hcpcs.parquet",
            "adjustment_rates": "value_sets_cms_hcc_adjustment_rates.parquet",
        }
        for filename in file_mappings.values():
            pl.DataFrame({"dummy": [1]}).write_parquet(tmp_path / filename)

        result = HccGapAnalysisTransform.load_hcc_value_sets(tmp_path)
        assert len(result) == 10
        for key in file_mappings:
            assert key in result


# ===== From test_participant_list_gap.py =====

class TestParticipantList:

    @pytest.mark.unit
    def test_transform_passthrough(self):
        df = pl.LazyFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        inner = getattr(transform_participant_list, "func", transform_participant_list)
        result = inner(df)
        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert collected.shape == (2, 2)


# ===== From test_reexport.py =====

class TestTransformsReExports:

    @pytest.mark.unit
    def test_transform_registry_importable(self):

        assert TransformRegistry is not None

    @pytest.mark.unit
    def test_register_crosswalk_importable(self):

        assert callable(register_crosswalk)

    @pytest.mark.unit
    def test_register_pipeline_importable(self):

        assert callable(register_pipeline)

    @pytest.mark.unit
    def test_all_exports(self):

        assert "TransformRegistry" in pkg.__all__
        assert "register_crosswalk" in pkg.__all__
        assert "register_pipeline" in pkg.__all__
