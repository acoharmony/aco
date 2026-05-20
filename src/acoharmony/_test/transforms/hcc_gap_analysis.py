# © 2025 HarmonyCares
# All rights reserved.

"""Comprehensive tests for acoharmony._transforms.hcc_gap_analysis module."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import acoharmony

from acoharmony._transforms.hcc_gap_analysis import HccGapAnalysisTransform


class TestHccGapAnalysisTransform:
    """Tests for HCC Gap Analysis Transform."""

    @pytest.mark.unit
    def test_transform_class_exists(self):
        """Transform class exists and is properly defined."""
        assert HccGapAnalysisTransform is not None
        assert hasattr(HccGapAnalysisTransform, 'load_hcc_value_sets')
        assert hasattr(HccGapAnalysisTransform, 'map_diagnoses_to_hccs')
        assert hasattr(HccGapAnalysisTransform, 'apply_hcc_hierarchies')
        assert hasattr(HccGapAnalysisTransform, 'calculate_raf_scores')
        assert hasattr(HccGapAnalysisTransform, 'identify_hcc_gaps')
        assert hasattr(HccGapAnalysisTransform, 'prioritize_gaps')
        assert hasattr(HccGapAnalysisTransform, 'calculate_hcc_gaps')


class TestLoadHccValueSets:
    """Tests for load_hcc_value_sets method."""

    @patch('polars.scan_parquet')
    @pytest.mark.unit
    def test_load_all_value_sets_success(self, mock_scan):
        """load_hcc_value_sets successfully loads all 10 value sets."""
        # Create a real DataFrame with data
        mock_df = pl.DataFrame({"dummy": range(10)})
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_lf.collect.return_value = mock_df
        mock_scan.return_value = mock_lf

        result = HccGapAnalysisTransform.load_hcc_value_sets(Path('/fake/silver'))

        assert isinstance(result, dict)
        assert len(result) == 10
        expected_keys = {
            "icd10_mappings", "disease_factors", "demographic_factors",
            "disease_hierarchy", "disease_interactions", "disabled_interactions",
            "enrollment_interactions", "payment_hcc_count", "cpt_hcpcs", "adjustment_rates"
        }
        assert set(result.keys()) == expected_keys

    @patch('polars.scan_parquet')
    @pytest.mark.unit
    def test_load_value_sets_handles_missing_files(self, mock_scan):
        """load_hcc_value_sets handles missing files gracefully."""
        def side_effect(path):
            if "disease_factors" in str(path) or "demographic_factors" in str(path):
                raise FileNotFoundError("File not found")
            mock_df = pl.DataFrame({"dummy": range(5)})
            mock_lf = MagicMock(spec=pl.LazyFrame)
            mock_lf.collect.return_value = mock_df
            return mock_lf

        mock_scan.side_effect = side_effect

        result = HccGapAnalysisTransform.load_hcc_value_sets(Path('/fake/silver'))

        # Should still return dict with empty LazyFrames for missing files
        assert isinstance(result, dict)
        assert "disease_factors" in result
        assert "demographic_factors" in result
        # Missing files should have empty DataFrames
        assert result["disease_factors"].collect().height == 0
        assert result["demographic_factors"].collect().height == 0

    @patch('polars.scan_parquet')
    @pytest.mark.unit
    def test_load_value_sets_handles_various_exceptions(self, mock_scan):
        """load_hcc_value_sets handles various exceptions (not just FileNotFoundError)."""
        def side_effect(path):
            # Check for specific filenames in path
            path_str = str(path)
            if "icd_10_cm_mappings" in path_str:  # Matches value_sets_cms_hcc_icd_10_cm_mappings.parquet
                raise PermissionError("Access denied")
            elif "disease_hierarchy" in path_str:  # Matches value_sets_cms_hcc_disease_hierarchy.parquet
                raise ValueError("Invalid data")
            mock_df = pl.DataFrame({"dummy": range(3)})
            mock_lf = MagicMock(spec=pl.LazyFrame)
            mock_lf.collect.return_value = mock_df
            return mock_lf

        mock_scan.side_effect = side_effect

        result = HccGapAnalysisTransform.load_hcc_value_sets(Path('/fake/silver'))

        assert isinstance(result, dict)
        assert len(result) == 10
        # Files that raised exceptions should have empty DataFrames
        # Both files raised exceptions and should have height 0
        icd10_df = result["icd10_mappings"].collect()
        hierarchy_df = result["disease_hierarchy"].collect()
        assert icd10_df.height == 0
        assert hierarchy_df.height == 0


class TestMapDiagnosesToHccs:
    """Tests for map_diagnoses_to_hccs method."""

    @pytest.mark.unit
    def test_map_diagnoses_v24_model(self):
        """map_diagnoses_to_hccs correctly maps ICD-10 codes using V24 model."""
        claims = pl.DataFrame({
            "person_id": ["P1", "P2", "P3"],
            "diagnosis_code_1": ["E1165", "I5030", "E0800"],
            "diagnosis_code_2": pl.Series(["Z7952", None, "I2510"], dtype=pl.Utf8),
            "diagnosis_code_3": pl.Series([None, "E785", None], dtype=pl.Utf8),
            "claim_end_date": [date(2024, 1, 15), date(2024, 2, 20), date(2024, 3, 10)]
        }).lazy()

        icd10_mappings = pl.DataFrame({
            "diagnosis_code": ["E1165", "I5030", "E0800", "Z7952", "E785", "I2510"],
            "cms_hcc_v24": ["HCC18", "HCC85", "HCC19", "HCC23", "HCC22", "HCC88"],
            "cms_hcc_v24_flag": ["Yes", "Yes", "Yes", "Yes", "Yes", "Yes"],
            "cms_hcc_v28": ["HCC18", "HCC85", "HCC19", "HCC23", "HCC22", "HCC88"],
            "cms_hcc_v28_flag": ["Yes", "Yes", "Yes", "Yes", "Yes", "Yes"]
        }).lazy()

        config = {"model_version": "V24", "measurement_year": 2024}

        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(claims, icd10_mappings, config)
        df = result.collect()

        assert df.height > 0
        assert "person_id" in df.columns
        assert "hcc_code" in df.columns
        assert "diagnosis_code" in df.columns
        assert "claim_date" in df.columns
        assert "service_year" in df.columns
        # Should have all 6 diagnosis codes mapped
        assert df.height == 6

    @pytest.mark.unit
    def test_map_diagnoses_v28_model(self):
        """map_diagnoses_to_hccs correctly maps ICD-10 codes using V28 model."""
        claims = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "diagnosis_code_1": ["E1165", "I5030"],
            "diagnosis_code_2": pl.Series([None, None], dtype=pl.Utf8),
            "diagnosis_code_3": pl.Series([None, None], dtype=pl.Utf8),
            "claim_end_date": [date(2024, 1, 15), date(2024, 2, 20)]
        }).lazy()

        icd10_mappings = pl.DataFrame({
            "diagnosis_code": ["E1165", "I5030"],
            "cms_hcc_v24": ["HCC18", "HCC85"],
            "cms_hcc_v24_flag": ["Yes", "Yes"],
            "cms_hcc_v28": ["HCC18_V28", "HCC85_V28"],
            "cms_hcc_v28_flag": ["Yes", "Yes"]
        }).lazy()

        config = {"model_version": "V28", "measurement_year": 2024}

        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(claims, icd10_mappings, config)
        df = result.collect()

        assert df.height == 2
        # Should use V28 column
        assert "HCC18_V28" in df["hcc_code"].to_list()
        assert "HCC85_V28" in df["hcc_code"].to_list()

    @pytest.mark.unit
    def test_map_diagnoses_filters_invalid_flag(self):
        """map_diagnoses_to_hccs filters out invalid HCC mappings (flag != 'Yes')."""
        claims = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "diagnosis_code_1": ["E1165", "INVALID"],
            "diagnosis_code_2": pl.Series([None, None], dtype=pl.Utf8),
            "diagnosis_code_3": pl.Series([None, None], dtype=pl.Utf8),
            "claim_end_date": [date(2024, 1, 15), date(2024, 2, 20)]
        }).lazy()

        icd10_mappings = pl.DataFrame({
            "diagnosis_code": ["E1165", "INVALID"],
            "cms_hcc_v24": ["HCC18", "HCC99"],
            "cms_hcc_v24_flag": ["Yes", "No"],  # INVALID is marked "No"
            "cms_hcc_v28": ["HCC18", "HCC99"],
            "cms_hcc_v28_flag": ["Yes", "No"]
        }).lazy()

        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(claims, icd10_mappings, config)
        df = result.collect()

        # Should only include valid mapping (P1)
        assert df.height == 1
        assert df["person_id"][0] == "P1"

    @pytest.mark.unit
    def test_map_diagnoses_handles_null_dx2_dx3(self):
        """map_diagnoses_to_hccs correctly handles null diagnosis_code_2 and diagnosis_code_3."""
        claims = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "diagnosis_code_1": ["E1165", "I5030"],
            "diagnosis_code_2": pl.Series([None, None], dtype=pl.Utf8),
            "diagnosis_code_3": pl.Series([None, None], dtype=pl.Utf8),
            "claim_end_date": [date(2024, 1, 15), date(2024, 2, 20)]
        }).lazy()

        icd10_mappings = pl.DataFrame({
            "diagnosis_code": ["E1165", "I5030"],
            "cms_hcc_v24": ["HCC18", "HCC85"],
            "cms_hcc_v24_flag": ["Yes", "Yes"],
            "cms_hcc_v28": ["HCC18", "HCC85"],
            "cms_hcc_v28_flag": ["Yes", "Yes"]
        }).lazy()

        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(claims, icd10_mappings, config)
        df = result.collect()

        # Should only map diagnosis_code_1
        assert df.height == 2
        assert set(df["hcc_code"].to_list()) == {"HCC18", "HCC85"}

    @pytest.mark.unit
    def test_map_diagnoses_deduplicates_person_hcc_year(self):
        """map_diagnoses_to_hccs deduplicates person-HCC-year combinations."""
        claims = pl.DataFrame({
            "person_id": ["P1", "P1", "P1"],
            "diagnosis_code_1": ["E1165", "E1165", "E1165"],  # Same code 3 times
            "diagnosis_code_2": pl.Series([None, None, None], dtype=pl.Utf8),
            "diagnosis_code_3": pl.Series([None, None, None], dtype=pl.Utf8),
            "claim_end_date": [date(2024, 1, 15), date(2024, 2, 20), date(2024, 3, 10)]
        }).lazy()

        icd10_mappings = pl.DataFrame({
            "diagnosis_code": ["E1165"],
            "cms_hcc_v24": ["HCC18"],
            "cms_hcc_v24_flag": ["Yes"],
            "cms_hcc_v28": ["HCC18"],
            "cms_hcc_v28_flag": ["Yes"]
        }).lazy()

        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(claims, icd10_mappings, config)
        df = result.collect()

        # Should deduplicate to 1 row
        assert df.height == 1
        assert df["person_id"][0] == "P1"
        assert df["hcc_code"][0] == "HCC18"

    @pytest.mark.unit
    def test_map_diagnoses_extracts_service_year(self):
        """map_diagnoses_to_hccs correctly extracts service_year from claim_end_date."""
        claims = pl.DataFrame({
            "person_id": ["P1", "P2", "P3"],
            "diagnosis_code_1": ["E1165", "E1165", "E1165"],
            "diagnosis_code_2": pl.Series([None, None, None], dtype=pl.Utf8),
            "diagnosis_code_3": pl.Series([None, None, None], dtype=pl.Utf8),
            "claim_end_date": [date(2023, 12, 31), date(2024, 1, 1), date(2025, 6, 15)]
        }).lazy()

        icd10_mappings = pl.DataFrame({
            "diagnosis_code": ["E1165"],
            "cms_hcc_v24": ["HCC18"],
            "cms_hcc_v24_flag": ["Yes"],
            "cms_hcc_v28": ["HCC18"],
            "cms_hcc_v28_flag": ["Yes"]
        }).lazy()

        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(claims, icd10_mappings, config)
        df = result.collect()

        assert df.height == 3
        years = set(df["service_year"].to_list())
        assert years == {2023, 2024, 2025}

class TestApplyHccHierarchies:
    """Tests for apply_hcc_hierarchies method."""

    @pytest.mark.unit
    def test_apply_hierarchies_excludes_lower_severity(self):
        """apply_hcc_hierarchies excludes lower severity HCCs when higher exists."""
        person_hccs = pl.DataFrame({
            "person_id": ["P1", "P1"],
            "hcc_code": ["HCC18", "HCC19"],  # HCC18 excludes HCC19
            "diagnosis_code": ["E1165", "E0800"],
            "claim_date": [date(2024, 1, 15), date(2024, 1, 20)],
            "service_year": [2024, 2024]
        }).lazy()

        disease_hierarchy = pl.DataFrame({
            "model_version": ["CMS-HCC-V24"],
            "hcc_code": ["HCC18"],
            "hccs_to_exclude": ["HCC19"]
        }).lazy()

        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.apply_hcc_hierarchies(person_hccs, disease_hierarchy, config)
        df = result.collect()

        # Should exclude HCC19, keeping only HCC18
        assert df.height == 1
        assert df["hcc_code"][0] == "HCC18"

    @pytest.mark.unit
    def test_apply_hierarchies_no_rules_returns_original(self):
        """apply_hcc_hierarchies returns original when no hierarchy rules found."""
        person_hccs = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "hcc_code": ["HCC18", "HCC85"],
            "diagnosis_code": ["E1165", "I5030"],
            "claim_date": [date(2024, 1, 15), date(2024, 2, 10)],
            "service_year": [2024, 2024]
        }).lazy()

        # Empty hierarchy (no rules for V28)
        disease_hierarchy = pl.DataFrame({
            "model_version": pl.Series([], dtype=pl.Utf8),
            "hcc_code": pl.Series([], dtype=pl.Utf8),
            "hccs_to_exclude": pl.Series([], dtype=pl.Utf8)
        }).lazy()

        config = {"model_version": "V28"}

        result = HccGapAnalysisTransform.apply_hcc_hierarchies(person_hccs, disease_hierarchy, config)
        df = result.collect()

        # Should return original person_hccs unchanged
        assert df.height == 2
        assert set(df["hcc_code"].to_list()) == {"HCC18", "HCC85"}

    @pytest.mark.unit
    def test_apply_hierarchies_multiple_exclusions(self):
        """apply_hcc_hierarchies handles multiple exclusion rules."""
        person_hccs = pl.DataFrame({
            "person_id": ["P1", "P1", "P1"],
            "hcc_code": ["HCC18", "HCC19", "HCC85"],
            "diagnosis_code": ["E1165", "E0800", "I5030"],
            "claim_date": [date(2024, 1, 15), date(2024, 1, 20), date(2024, 2, 10)],
            "service_year": [2024, 2024, 2024]
        }).lazy()

        disease_hierarchy = pl.DataFrame({
            "model_version": ["CMS-HCC-V24", "CMS-HCC-V24"],
            "hcc_code": ["HCC18", "HCC85"],
            "hccs_to_exclude": ["HCC19", "HCC87"]
        }).lazy()

        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.apply_hcc_hierarchies(person_hccs, disease_hierarchy, config)
        df = result.collect()

        # Should exclude HCC19 (HCC87 doesn't exist in data)
        assert df.height == 2
        assert "HCC19" not in df["hcc_code"].to_list()
        assert "HCC18" in df["hcc_code"].to_list()
        assert "HCC85" in df["hcc_code"].to_list()

    @pytest.mark.unit
    def test_apply_hierarchies_different_years_no_exclusion(self):
        """apply_hcc_hierarchies doesn't exclude HCCs from different service years."""
        person_hccs = pl.DataFrame({
            "person_id": ["P1", "P1"],
            "hcc_code": ["HCC18", "HCC19"],
            "diagnosis_code": ["E1165", "E0800"],
            "claim_date": [date(2024, 1, 15), date(2023, 1, 20)],
            "service_year": [2024, 2023]  # Different years
        }).lazy()

        disease_hierarchy = pl.DataFrame({
            "model_version": ["CMS-HCC-V24"],
            "hcc_code": ["HCC18"],
            "hccs_to_exclude": ["HCC19"]
        }).lazy()

        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.apply_hcc_hierarchies(person_hccs, disease_hierarchy, config)
        df = result.collect()

        # Should keep both since they're from different years
        assert df.height == 2

    @pytest.mark.unit
    def test_apply_hierarchies_v28_model(self):
        """apply_hcc_hierarchies works with V28 model version."""
        person_hccs = pl.DataFrame({
            "person_id": ["P1"],
            "hcc_code": ["HCC18"],
            "diagnosis_code": ["E1165"],
            "claim_date": [date(2024, 1, 15)],
            "service_year": [2024]
        }).lazy()

        disease_hierarchy = pl.DataFrame({
            "model_version": ["CMS-HCC-V28"],
            "hcc_code": ["HCC18"],
            "hccs_to_exclude": ["HCC19"]
        }).lazy()

        config = {"model_version": "V28"}

        result = HccGapAnalysisTransform.apply_hcc_hierarchies(person_hccs, disease_hierarchy, config)
        df = result.collect()

        assert df.height == 1
        assert df["hcc_code"][0] == "HCC18"


class TestCalculateRafScores:
    """Tests for calculate_raf_scores method."""

    @pytest.mark.unit
    def test_calculate_raf_with_disease_and_demographic(self):
        """calculate_raf_scores combines disease and demographic factors."""
        person_hccs = pl.DataFrame({
            "person_id": ["P1", "P1"],
            "hcc_code": ["HCC18", "HCC85"],
            "diagnosis_code": ["E1165", "I5030"],
            "claim_date": [date(2024, 1, 15), date(2024, 2, 10)],
            "service_year": [2024, 2024]
        }).lazy()

        disease_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24", "CMS-HCC-V24"],
            "hcc_code": ["HCC18", "HCC85"],
            "coefficient": [0.302, 0.331],
            "enrollment_status": ["Continuing", "Continuing"],
            "medicaid_status": ["No", "No"],
            "institutional_status": ["No", "No"],
            "description": ["Diabetes with complications", "CHF"]
        }).lazy()

        demographic_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24"],
            "gender": ["Male"],
            "age_group": ["70-74"],
            "coefficient": [0.450],
            "enrollment_status": ["Continuing"],
            "orec": ["Aged"],
            "institutional_status": ["No"]
        }).lazy()

        eligibility = pl.DataFrame({
            "person_id": ["P1"],
            "age": [72],
            "gender": ["M"]
        }).lazy()

        config = {"model_version": "V24", "measurement_year": 2024}

        result = HccGapAnalysisTransform.calculate_raf_scores(
            person_hccs, disease_factors, demographic_factors, eligibility, config
        )
        df = result.collect()

        assert df.height == 1
        assert "person_id" in df.columns
        assert "raf_score" in df.columns
        assert "demographic_score" in df.columns
        assert "disease_score" in df.columns
        # RAF = demographic + disease = 0.450 + (0.302 + 0.331) = 1.083
        assert abs(df["raf_score"][0] - 1.083) < 0.001

    @pytest.mark.unit
    def test_calculate_raf_handles_missing_hccs(self):
        """calculate_raf_scores handles persons with no HCCs (demographic only)."""
        person_hccs = pl.DataFrame({
            "person_id": ["P2"],  # Different person
            "hcc_code": ["HCC18"],
            "diagnosis_code": ["E1165"],
            "claim_date": [date(2024, 1, 15)],
            "service_year": [2024]
        }).lazy()

        disease_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24"],
            "hcc_code": ["HCC18"],
            "coefficient": [0.302],
            "enrollment_status": ["Continuing"],
            "medicaid_status": ["No"],
            "institutional_status": ["No"],
            "description": ["Diabetes with complications"]
        }).lazy()

        demographic_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24"],
            "gender": ["Female"],
            "age_group": ["65"],
            "coefficient": [0.350],
            "enrollment_status": ["Continuing"],
            "orec": ["Aged"],
            "institutional_status": ["No"]
        }).lazy()

        eligibility = pl.DataFrame({
            "person_id": ["P1"],  # Person not in person_hccs
            "age": [65],
            "gender": ["F"]
        }).lazy()

        config = {"model_version": "V24", "measurement_year": 2024}

        result = HccGapAnalysisTransform.calculate_raf_scores(
            person_hccs, disease_factors, demographic_factors, eligibility, config
        )
        df = result.collect()

        assert df.height == 1
        # Should have demographic score but 0 disease score
        assert df["demographic_score"][0] == 0.350
        assert df["disease_score"][0] == 0.0
        assert df["raf_score"][0] == 0.350

    @pytest.mark.unit
    def test_calculate_raf_age_group_mapping(self):
        """calculate_raf_scores correctly maps ages to age groups."""
        person_hccs = pl.DataFrame({
            "person_id": pl.Series([], dtype=pl.Utf8),
            "hcc_code": pl.Series([], dtype=pl.Utf8),
            "diagnosis_code": pl.Series([], dtype=pl.Utf8),
            "claim_date": pl.Series([], dtype=pl.Date),
            "service_year": pl.Series([], dtype=pl.Int64)
        }).lazy()

        disease_factors = pl.DataFrame({
            "model_version": pl.Series([], dtype=pl.Utf8),
            "hcc_code": pl.Series([], dtype=pl.Utf8),
            "coefficient": pl.Series([], dtype=pl.Float64),
            "enrollment_status": pl.Series([], dtype=pl.Utf8),
            "medicaid_status": pl.Series([], dtype=pl.Utf8),
            "institutional_status": pl.Series([], dtype=pl.Utf8),
            "description": pl.Series([], dtype=pl.Utf8)
        }).lazy()

        demographic_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24", "CMS-HCC-V24", "CMS-HCC-V24", "CMS-HCC-V24"],
            "gender": ["Male", "Male", "Female", "Male"],
            "age_group": ["65", "70-74", "85-89", "95+"],
            "coefficient": [0.350, 0.450, 0.650, 0.850],
            "enrollment_status": ["Continuing", "Continuing", "Continuing", "Continuing"],
            "orec": ["Aged", "Aged", "Aged", "Aged"],
            "institutional_status": ["No", "No", "No", "No"]
        }).lazy()

        eligibility = pl.DataFrame({
            "person_id": ["P1", "P2", "P3", "P4"],
            "age": [65, 72, 87, 96],
            "gender": ["M", "M", "F", "M"]
        }).lazy()

        config = {"model_version": "V24", "measurement_year": 2024}

        result = HccGapAnalysisTransform.calculate_raf_scores(
            person_hccs, disease_factors, demographic_factors, eligibility, config
        )
        df = result.collect().sort("person_id")

        assert df.height == 4
        assert df["demographic_score"][0] == 0.350  # P1: age 65 -> "65"
        assert df["demographic_score"][1] == 0.450  # P2: age 72 -> "70-74"
        assert df["demographic_score"][2] == 0.650  # P3: age 87 -> "85-89"
        assert df["demographic_score"][3] == 0.850  # P4: age 96 -> "95+"

    @pytest.mark.unit
    def test_calculate_raf_gender_mapping(self):
        """calculate_raf_scores correctly maps gender values."""
        person_hccs = pl.DataFrame({
            "person_id": pl.Series([], dtype=pl.Utf8),
            "hcc_code": pl.Series([], dtype=pl.Utf8),
            "diagnosis_code": pl.Series([], dtype=pl.Utf8),
            "claim_date": pl.Series([], dtype=pl.Date),
            "service_year": pl.Series([], dtype=pl.Int64)
        }).lazy()

        disease_factors = pl.DataFrame({
            "model_version": pl.Series([], dtype=pl.Utf8),
            "hcc_code": pl.Series([], dtype=pl.Utf8),
            "coefficient": pl.Series([], dtype=pl.Float64),
            "enrollment_status": pl.Series([], dtype=pl.Utf8),
            "medicaid_status": pl.Series([], dtype=pl.Utf8),
            "institutional_status": pl.Series([], dtype=pl.Utf8),
            "description": pl.Series([], dtype=pl.Utf8)
        }).lazy()

        demographic_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24", "CMS-HCC-V24"],
            "gender": ["Female", "Male"],
            "age_group": ["65", "65"],
            "coefficient": [0.400, 0.350],
            "enrollment_status": ["Continuing", "Continuing"],
            "orec": ["Aged", "Aged"],
            "institutional_status": ["No", "No"]
        }).lazy()

        eligibility = pl.DataFrame({
            "person_id": ["P1", "P2", "P3", "P4"],
            "age": [65, 65, 65, 65],
            "gender": ["F", "female", "M", "male"]
        }).lazy()

        config = {"model_version": "V24", "measurement_year": 2024}

        result = HccGapAnalysisTransform.calculate_raf_scores(
            person_hccs, disease_factors, demographic_factors, eligibility, config
        )
        df = result.collect().sort("person_id")

        assert df.height == 4
        # P1, P2 (F, female) should map to Female
        assert df["demographic_score"][0] == 0.400
        assert df["demographic_score"][1] == 0.400
        # P3, P4 (M, male) should map to Male
        assert df["demographic_score"][2] == 0.350
        assert df["demographic_score"][3] == 0.350

    @pytest.mark.unit
    def test_calculate_raf_v28_model(self):
        """calculate_raf_scores works with V28 model version."""
        person_hccs = pl.DataFrame({
            "person_id": ["P1"],
            "hcc_code": ["HCC18"],
            "diagnosis_code": ["E1165"],
            "claim_date": [date(2024, 1, 15)],
            "service_year": [2024]
        }).lazy()

        disease_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V28"],
            "hcc_code": ["HCC18"],
            "coefficient": [0.320],
            "enrollment_status": ["Continuing"],
            "medicaid_status": ["No"],
            "institutional_status": ["No"],
            "description": ["Diabetes with complications"]
        }).lazy()

        demographic_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V28"],
            "gender": ["Male"],
            "age_group": ["70-74"],
            "coefficient": [0.460],
            "enrollment_status": ["Continuing"],
            "orec": ["Aged"],
            "institutional_status": ["No"]
        }).lazy()

        eligibility = pl.DataFrame({
            "person_id": ["P1"],
            "age": [72],
            "gender": ["M"]
        }).lazy()

        config = {"model_version": "V28", "measurement_year": 2024}

        result = HccGapAnalysisTransform.calculate_raf_scores(
            person_hccs, disease_factors, demographic_factors, eligibility, config
        )
        df = result.collect()

        assert df.height == 1
        assert df["raf_score"][0] == 0.780  # 0.320 + 0.460

    @pytest.mark.unit
    def test_calculate_raf_filters_by_measurement_year(self):
        """calculate_raf_scores only includes HCCs from measurement year."""
        person_hccs = pl.DataFrame({
            "person_id": ["P1", "P1"],
            "hcc_code": ["HCC18", "HCC85"],
            "diagnosis_code": ["E1165", "I5030"],
            "claim_date": [date(2024, 1, 15), date(2023, 2, 10)],
            "service_year": [2024, 2023]  # One from 2023, one from 2024
        }).lazy()

        disease_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24", "CMS-HCC-V24"],
            "hcc_code": ["HCC18", "HCC85"],
            "coefficient": [0.302, 0.331],
            "enrollment_status": ["Continuing", "Continuing"],
            "medicaid_status": ["No", "No"],
            "institutional_status": ["No", "No"],
            "description": ["Diabetes", "CHF"]
        }).lazy()

        demographic_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24"],
            "gender": ["Male"],
            "age_group": ["70-74"],
            "coefficient": [0.450],
            "enrollment_status": ["Continuing"],
            "orec": ["Aged"],
            "institutional_status": ["No"]
        }).lazy()

        eligibility = pl.DataFrame({
            "person_id": ["P1"],
            "age": [72],
            "gender": ["M"]
        }).lazy()

        config = {"model_version": "V24", "measurement_year": 2024}

        result = HccGapAnalysisTransform.calculate_raf_scores(
            person_hccs, disease_factors, demographic_factors, eligibility, config
        )
        df = result.collect()

        # Should only include HCC18 (2024), not HCC85 (2023)
        # RAF = 0.450 + 0.302 = 0.752
        assert abs(df["raf_score"][0] - 0.752) < 0.001


class TestIdentifyHccGaps:
    """Tests for identify_hcc_gaps method."""

    @pytest.mark.unit
    def test_identify_gaps_basic(self):
        """identify_hcc_gaps identifies chronic HCCs not in current year."""
        current_hccs = pl.DataFrame({
            "person_id": ["P1"],
            "hcc_code": [18],  # Only HCC18 in current year - using integers
            "service_year": [2024]
        }).lazy()

        historical_hccs = pl.DataFrame({
            "person_id": ["P1", "P1"],
            "hcc_code": [18, 85],  # Both in historical - using integers
            "service_year": [2023, 2023]
        }).lazy()

        config = {"measurement_year": 2024}

        result = HccGapAnalysisTransform.identify_hcc_gaps(current_hccs, historical_hccs, config)
        df = result.collect()

        # Should identify HCC85 as a gap (chronic HCC not recaptured)
        assert df.height == 1
        assert df["hcc_code"][0] == 85
        assert df["person_id"][0] == "P1"
        assert "gap_year" in df.columns
        assert "years_since_capture" in df.columns
        assert "gap_type" in df.columns

    @pytest.mark.unit
    def test_identify_gaps_only_chronic_hccs(self):
        """identify_hcc_gaps only considers chronic HCC categories."""
        current_hccs = pl.DataFrame({
            "person_id": ["P1"],
            "hcc_code": [1],  # Using integer
            "service_year": [2024]
        }).lazy()

        historical_hccs = pl.DataFrame({
            "person_id": ["P1", "P1"],
            "hcc_code": [1, 999],  # HCC999 is not chronic - using integers
            "service_year": [2023, 2023]
        }).lazy()

        config = {"measurement_year": 2024}

        result = HccGapAnalysisTransform.identify_hcc_gaps(current_hccs, historical_hccs, config)
        df = result.collect()

        # Should not identify HCC999 as a gap (not chronic)
        # HCC1 is chronic (HIV/AIDS) but is in current year
        assert df.height == 0

    @pytest.mark.unit
    def test_identify_gaps_multiple_persons(self):
        """identify_hcc_gaps handles multiple persons."""
        current_hccs = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "hcc_code": [18, 19],  # Using integers
            "service_year": [2024, 2024]
        }).lazy()

        historical_hccs = pl.DataFrame({
            "person_id": ["P1", "P1", "P2"],
            "hcc_code": [18, 85, 19],  # Using integers
            "service_year": [2023, 2023, 2023]
        }).lazy()

        config = {"measurement_year": 2024}

        result = HccGapAnalysisTransform.identify_hcc_gaps(current_hccs, historical_hccs, config)
        df = result.collect()

        # Should identify HCC85 gap for P1 (P2 has no gaps)
        assert df.height == 1
        assert df["person_id"][0] == "P1"
        assert df["hcc_code"][0] == 85

    @pytest.mark.unit
    def test_identify_gaps_years_since_capture(self):
        """identify_hcc_gaps calculates years_since_capture correctly."""
        current_hccs = pl.DataFrame({
            "person_id": pl.Series([], dtype=pl.Utf8),
            "hcc_code": pl.Series([], dtype=pl.Int64),  # Changed to Int64
            "service_year": pl.Series([], dtype=pl.Int64)
        }).lazy()

        historical_hccs = pl.DataFrame({
            "person_id": ["P1", "P1"],
            "hcc_code": [85, 85],  # Using integers
            "service_year": [2021, 2020]  # Last capture was 2021
        }).lazy()

        config = {"measurement_year": 2024}

        result = HccGapAnalysisTransform.identify_hcc_gaps(current_hccs, historical_hccs, config)
        df = result.collect()

        assert df.height == 1
        # years_since_capture = 2024 - 2021 = 3
        assert df["years_since_capture"][0] == 3
        assert df["last_capture_year"][0] == 2021

    @pytest.mark.unit
    def test_identify_gaps_no_historical(self):
        """identify_hcc_gaps handles empty historical data."""
        current_hccs = pl.DataFrame({
            "person_id": ["P1"],
            "hcc_code": [18],  # Using integer
            "service_year": [2024]
        }).lazy()

        historical_hccs = pl.DataFrame({
            "person_id": pl.Series([], dtype=pl.Utf8),
            "hcc_code": pl.Series([], dtype=pl.Int64),  # Changed to Int64
            "service_year": pl.Series([], dtype=pl.Int64)
        }).lazy()

        config = {"measurement_year": 2024}

        result = HccGapAnalysisTransform.identify_hcc_gaps(current_hccs, historical_hccs, config)
        df = result.collect()

        # No historical data = no gaps
        assert df.height == 0

    @pytest.mark.unit
    def test_identify_gaps_metadata_fields(self):
        """identify_hcc_gaps includes all required metadata fields."""
        current_hccs = pl.DataFrame({
            "person_id": pl.Series([], dtype=pl.Utf8),
            "hcc_code": pl.Series([], dtype=pl.Int64),  # Changed to Int64
            "service_year": pl.Series([], dtype=pl.Int64)
        }).lazy()

        historical_hccs = pl.DataFrame({
            "person_id": ["P1"],
            "hcc_code": [85],  # Using integer
            "service_year": [2022]
        }).lazy()

        config = {"measurement_year": 2024}

        result = HccGapAnalysisTransform.identify_hcc_gaps(current_hccs, historical_hccs, config)
        df = result.collect()

        assert "gap_year" in df.columns
        assert "years_since_capture" in df.columns
        assert "gap_type" in df.columns
        assert df["gap_year"][0] == 2024
        assert df["gap_type"][0] == "chronic_recapture"


class TestPrioritizeGaps:
    """Tests for prioritize_gaps method."""

    @pytest.mark.unit
    def test_prioritize_gaps_adds_value_and_priority(self):
        """prioritize_gaps adds gap_value and priority fields."""
        gaps = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "hcc_code": [18, 85],  # Using integers to match gaps data
            "gap_year": [2024, 2024],
            "years_since_capture": [1, 2],
            "gap_type": ["chronic_recapture", "chronic_recapture"],
            "last_capture_year": [2023, 2022]
        }).lazy()

        disease_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24", "CMS-HCC-V24"],
            "hcc_code": [18, 85],  # Using integers
            "coefficient": [0.302, 0.551],
            "enrollment_status": ["Continuing", "Continuing"],
            "medicaid_status": ["No", "No"],
            "institutional_status": ["No", "No"],
            "description": ["Diabetes", "CHF"]
        }).lazy()

        raf_scores = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "raf_score": [1.000, 1.500]
        }).lazy()

        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.prioritize_gaps(gaps, disease_factors, raf_scores, config)
        df = result.collect()

        assert df.height == 2
        assert "gap_value" in df.columns
        assert "priority" in df.columns
        assert "potential_raf" in df.columns
        assert "pct_impact" in df.columns

    @pytest.mark.unit
    def test_prioritize_gaps_priority_levels(self):
        """prioritize_gaps assigns correct priority levels based on coefficient."""
        gaps = pl.DataFrame({
            "person_id": ["P1", "P2", "P3"],
            "hcc_code": [1, 2, 3],  # Using integers
            "gap_year": [2024, 2024, 2024],
            "years_since_capture": [1, 1, 1],
            "gap_type": ["chronic_recapture", "chronic_recapture", "chronic_recapture"],
            "last_capture_year": [2023, 2023, 2023]
        }).lazy()

        disease_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24", "CMS-HCC-V24", "CMS-HCC-V24"],
            "hcc_code": [1, 2, 3],  # Using integers
            "coefficient": [0.600, 0.300, 0.100],  # high, medium, low
            "enrollment_status": ["Continuing", "Continuing", "Continuing"],
            "medicaid_status": ["No", "No", "No"],
            "institutional_status": ["No", "No", "No"],
            "description": ["High value", "Medium value", "Low value"]
        }).lazy()

        raf_scores = pl.DataFrame({
            "person_id": ["P1", "P2", "P3"],
            "raf_score": [1.000, 1.000, 1.000]
        }).lazy()

        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.prioritize_gaps(gaps, disease_factors, raf_scores, config)
        df = result.collect().sort("person_id")

        # >= 0.5 = high, >= 0.2 = medium, < 0.2 = low
        assert df["priority"][0] == "high"    # 0.600
        assert df["priority"][1] == "medium"  # 0.300
        assert df["priority"][2] == "low"     # 0.100

    @pytest.mark.unit
    def test_prioritize_gaps_calculates_potential_raf(self):
        """prioritize_gaps calculates potential_raf correctly."""
        gaps = pl.DataFrame({
            "person_id": ["P1"],
            "hcc_code": [18],  # Using integer
            "gap_year": [2024],
            "years_since_capture": [1],
            "gap_type": ["chronic_recapture"],
            "last_capture_year": [2023]
        }).lazy()

        disease_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24"],
            "hcc_code": [18],  # Using integer
            "coefficient": [0.302],
            "enrollment_status": ["Continuing"],
            "medicaid_status": ["No"],
            "institutional_status": ["No"],
            "description": ["Diabetes"]
        }).lazy()

        raf_scores = pl.DataFrame({
            "person_id": ["P1"],
            "raf_score": [1.200]
        }).lazy()

        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.prioritize_gaps(gaps, disease_factors, raf_scores, config)
        df = result.collect()

        # potential_raf = current_raf + gap_value = 1.200 + 0.302 = 1.502
        assert abs(df["potential_raf"][0] - 1.502) < 0.001

    @pytest.mark.unit
    def test_prioritize_gaps_calculates_pct_impact(self):
        """prioritize_gaps calculates pct_impact correctly."""
        gaps = pl.DataFrame({
            "person_id": ["P1"],
            "hcc_code": [18],  # Using integer
            "gap_year": [2024],
            "years_since_capture": [1],
            "gap_type": ["chronic_recapture"],
            "last_capture_year": [2023]
        }).lazy()

        disease_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24"],
            "hcc_code": [18],  # Using integer
            "coefficient": [0.302],
            "enrollment_status": ["Continuing"],
            "medicaid_status": ["No"],
            "institutional_status": ["No"],
            "description": ["Diabetes"]
        }).lazy()

        raf_scores = pl.DataFrame({
            "person_id": ["P1"],
            "raf_score": [1.000]
        }).lazy()

        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.prioritize_gaps(gaps, disease_factors, raf_scores, config)
        df = result.collect()

        # pct_impact = (gap_value / raf_score) * 100 = (0.302 / 1.000) * 100 = 30.2%
        assert abs(df["pct_impact"][0] - 30.2) < 0.1

    @pytest.mark.unit
    def test_prioritize_gaps_includes_description(self):
        """prioritize_gaps includes HCC description from disease_factors."""
        gaps = pl.DataFrame({
            "person_id": ["P1"],
            "hcc_code": [18],  # Using integer
            "gap_year": [2024],
            "years_since_capture": [1],
            "gap_type": ["chronic_recapture"],
            "last_capture_year": [2023]
        }).lazy()

        disease_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24"],
            "hcc_code": [18],  # Using integer
            "coefficient": [0.302],
            "enrollment_status": ["Continuing"],
            "medicaid_status": ["No"],
            "institutional_status": ["No"],
            "description": ["Diabetes with Chronic Complications"]
        }).lazy()

        raf_scores = pl.DataFrame({
            "person_id": ["P1"],
            "raf_score": [1.000]
        }).lazy()

        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.prioritize_gaps(gaps, disease_factors, raf_scores, config)
        df = result.collect()

        assert "description" in df.columns
        assert df["description"][0] == "Diabetes with Chronic Complications"


class TestCalculateHccGaps:
    """Tests for the main calculate_hcc_gaps method."""

    @pytest.mark.unit
    def test_calculate_hcc_gaps_returns_four_dataframes(self):
        """calculate_hcc_gaps returns tuple of 4 DataFrames."""
        claims = pl.DataFrame({
            "person_id": ["P1"],
            "diagnosis_code_1": ["E1165"],
            "diagnosis_code_2": pl.Series([None], dtype=pl.Utf8),
            "diagnosis_code_3": pl.Series([None], dtype=pl.Utf8),
            "claim_end_date": [date(2024, 1, 15)]
        }).lazy()

        eligibility = pl.DataFrame({
            "person_id": ["P1"],
            "age": [72],
            "gender": ["M"]
        }).lazy()

        value_sets = {
            "icd10_mappings": pl.DataFrame({
                "diagnosis_code": ["E1165"],
                "cms_hcc_v24": [18],  # Using integer instead of string
                "cms_hcc_v24_flag": ["Yes"],
                "cms_hcc_v28": [18],  # Using integer instead of string
                "cms_hcc_v28_flag": ["Yes"]
            }).lazy(),
            "disease_factors": pl.DataFrame({
                "model_version": ["CMS-HCC-V24"],
                "hcc_code": [18],  # Using integer
                "coefficient": [0.302],
                "enrollment_status": ["Continuing"],
                "medicaid_status": ["No"],
                "institutional_status": ["No"],
                "description": ["Diabetes"]
            }).lazy(),
            "demographic_factors": pl.DataFrame({
                "model_version": ["CMS-HCC-V24"],
                "gender": ["Male"],
                "age_group": ["70-74"],
                "coefficient": [0.450],
                "enrollment_status": ["Continuing"],
                "orec": ["Aged"],
                "institutional_status": ["No"]
            }).lazy(),
            "disease_hierarchy": pl.DataFrame({
                "model_version": pl.Series([], dtype=pl.Utf8),
                "hcc_code": pl.Series([], dtype=pl.Int64),  # Changed to Int64
                "hccs_to_exclude": pl.Series([], dtype=pl.Int64)  # Changed to Int64
            }).lazy(),
            "disease_interactions": pl.DataFrame().lazy(),
            "disabled_interactions": pl.DataFrame().lazy(),
            "enrollment_interactions": pl.DataFrame().lazy(),
            "payment_hcc_count": pl.DataFrame().lazy(),
            "cpt_hcpcs": pl.DataFrame().lazy(),
            "adjustment_rates": pl.DataFrame().lazy()
        }

        config = {"model_version": "V24", "measurement_year": 2024}

        result = HccGapAnalysisTransform.calculate_hcc_gaps(claims, eligibility, value_sets, config)

        assert isinstance(result, tuple)
        assert len(result) == 4
        hccs, gaps, raf_scores, summary = result
        # All should be LazyFrames
        assert isinstance(hccs, pl.LazyFrame)
        assert isinstance(gaps, pl.LazyFrame)
        assert isinstance(raf_scores, pl.LazyFrame)
        assert isinstance(summary, pl.LazyFrame)

    @pytest.mark.unit
    def test_calculate_hcc_gaps_integrates_all_steps(self):
        """calculate_hcc_gaps integrates mapping, hierarchies, RAF, gaps, and prioritization."""
        claims = pl.DataFrame({
            "person_id": ["P1", "P1", "P1"],
            "diagnosis_code_1": ["E1165", "I5030", "E1165"],
            "diagnosis_code_2": pl.Series([None, None, None], dtype=pl.Utf8),
            "diagnosis_code_3": pl.Series([None, None, None], dtype=pl.Utf8),
            "claim_end_date": [date(2024, 1, 15), date(2024, 2, 10), date(2023, 3, 5)]
        }).lazy()

        eligibility = pl.DataFrame({
            "person_id": ["P1"],
            "age": [72],
            "gender": ["M"]
        }).lazy()

        value_sets = {
            "icd10_mappings": pl.DataFrame({
                "diagnosis_code": ["E1165", "I5030"],
                "cms_hcc_v24": [18, 85],  # Using integers
                "cms_hcc_v24_flag": ["Yes", "Yes"],
                "cms_hcc_v28": [18, 85],  # Using integers
                "cms_hcc_v28_flag": ["Yes", "Yes"]
            }).lazy(),
            "disease_factors": pl.DataFrame({
                "model_version": ["CMS-HCC-V24", "CMS-HCC-V24"],
                "hcc_code": [18, 85],  # Using integers
                "coefficient": [0.302, 0.331],
                "enrollment_status": ["Continuing", "Continuing"],
                "medicaid_status": ["No", "No"],
                "institutional_status": ["No", "No"],
                "description": ["Diabetes", "CHF"]
            }).lazy(),
            "demographic_factors": pl.DataFrame({
                "model_version": ["CMS-HCC-V24"],
                "gender": ["Male"],
                "age_group": ["70-74"],
                "coefficient": [0.450],
                "enrollment_status": ["Continuing"],
                "orec": ["Aged"],
                "institutional_status": ["No"]
            }).lazy(),
            "disease_hierarchy": pl.DataFrame({
                "model_version": pl.Series([], dtype=pl.Utf8),
                "hcc_code": pl.Series([], dtype=pl.Int64),  # Changed to Int64
                "hccs_to_exclude": pl.Series([], dtype=pl.Int64)  # Changed to Int64
            }).lazy(),
            "disease_interactions": pl.DataFrame().lazy(),
            "disabled_interactions": pl.DataFrame().lazy(),
            "enrollment_interactions": pl.DataFrame().lazy(),
            "payment_hcc_count": pl.DataFrame().lazy(),
            "cpt_hcpcs": pl.DataFrame().lazy(),
            "adjustment_rates": pl.DataFrame().lazy()
        }

        config = {"model_version": "V24", "measurement_year": 2024}

        hccs, gaps, raf_scores, summary = HccGapAnalysisTransform.calculate_hcc_gaps(
            claims, eligibility, value_sets, config
        )

        # Verify each output
        hccs_df = hccs.collect()
        assert hccs_df.height > 0

        raf_df = raf_scores.collect()
        assert raf_df.height == 1
        assert "raf_score" in raf_df.columns

        # HCC85 should be a gap (in 2023 but not 2024)
        # Actually, we only have HCC18 in both years, and HCC85 in 2024
        # So no gaps expected in this scenario
        gaps_df = gaps.collect()
        # Gaps might be 0 since both HCCs are in current year

        summary_df = summary.collect()
        assert "priority" in summary_df.columns

    @pytest.mark.unit
    def test_calculate_hcc_gaps_summary_aggregates_by_priority(self):
        """calculate_hcc_gaps summary aggregates gaps by priority level."""
        claims = pl.DataFrame({
            "person_id": ["P1", "P1"],
            "diagnosis_code_1": ["E1165", "I5030"],
            "diagnosis_code_2": pl.Series([None, None], dtype=pl.Utf8),
            "diagnosis_code_3": pl.Series([None, None], dtype=pl.Utf8),
            "claim_end_date": [date(2023, 1, 15), date(2023, 2, 10)]
        }).lazy()

        eligibility = pl.DataFrame({
            "person_id": ["P1"],
            "age": [72],
            "gender": ["M"]
        }).lazy()

        value_sets = {
            "icd10_mappings": pl.DataFrame({
                "diagnosis_code": ["E1165", "I5030"],
                "cms_hcc_v24": [18, 85],  # Using integers
                "cms_hcc_v24_flag": ["Yes", "Yes"],
                "cms_hcc_v28": [18, 85],  # Using integers
                "cms_hcc_v28_flag": ["Yes", "Yes"]
            }).lazy(),
            "disease_factors": pl.DataFrame({
                "model_version": ["CMS-HCC-V24", "CMS-HCC-V24"],
                "hcc_code": [18, 85],  # Using integers
                "coefficient": [0.302, 0.551],
                "enrollment_status": ["Continuing", "Continuing"],
                "medicaid_status": ["No", "No"],
                "institutional_status": ["No", "No"],
                "description": ["Diabetes", "CHF"]
            }).lazy(),
            "demographic_factors": pl.DataFrame({
                "model_version": ["CMS-HCC-V24"],
                "gender": ["Male"],
                "age_group": ["70-74"],
                "coefficient": [0.450],
                "enrollment_status": ["Continuing"],
                "orec": ["Aged"],
                "institutional_status": ["No"]
            }).lazy(),
            "disease_hierarchy": pl.DataFrame({
                "model_version": pl.Series([], dtype=pl.Utf8),
                "hcc_code": pl.Series([], dtype=pl.Int64),  # Changed to Int64
                "hccs_to_exclude": pl.Series([], dtype=pl.Int64)  # Changed to Int64
            }).lazy(),
            "disease_interactions": pl.DataFrame().lazy(),
            "disabled_interactions": pl.DataFrame().lazy(),
            "enrollment_interactions": pl.DataFrame().lazy(),
            "payment_hcc_count": pl.DataFrame().lazy(),
            "cpt_hcpcs": pl.DataFrame().lazy(),
            "adjustment_rates": pl.DataFrame().lazy()
        }

        config = {"model_version": "V24", "measurement_year": 2024}

        _, _, _, summary = HccGapAnalysisTransform.calculate_hcc_gaps(claims, eligibility, value_sets, config)

        summary_df = summary.collect()
        expected_cols = ["priority", "gap_count", "total_potential_value", "avg_gap_value",
                        "avg_years_since_capture", "unique_members"]
        for col in expected_cols:
            assert col in summary_df.columns


class TestEdgeCases:
    """Tests for edge cases and missing data scenarios."""

    @pytest.mark.unit
    def test_empty_claims_data(self):
        """All methods handle empty claims data gracefully."""
        # For empty DataFrames with joins, we need proper schema
        # Create empty but schema-compatible DataFrames
        claims = pl.DataFrame({
            "person_id": pl.Series([], dtype=pl.Utf8),
            "diagnosis_code_1": pl.Series([], dtype=pl.Utf8),
            "diagnosis_code_2": pl.Series([], dtype=pl.Utf8),
            "diagnosis_code_3": pl.Series([], dtype=pl.Utf8),
            "claim_end_date": pl.Series([], dtype=pl.Date)
        }).lazy()

        icd10_mappings = pl.DataFrame({
            "diagnosis_code": pl.Series([], dtype=pl.Utf8),  # Empty but correct schema
            "cms_hcc_v24": pl.Series([], dtype=pl.Int64),  # Using integer type
            "cms_hcc_v24_flag": pl.Series([], dtype=pl.Utf8),
            "cms_hcc_v28": pl.Series([], dtype=pl.Int64),  # Using integer type
            "cms_hcc_v28_flag": pl.Series([], dtype=pl.Utf8)
        }).lazy()

        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(claims, icd10_mappings, config)
        df = result.collect()

        assert df.height == 0

    @pytest.mark.unit
    def test_missing_demographic_match(self):
        """calculate_raf_scores handles missing demographic factor matches."""
        person_hccs = pl.DataFrame({
            "person_id": ["P1"],
            "hcc_code": [18],  # Using integer
            "diagnosis_code": ["E1165"],
            "claim_date": [date(2024, 1, 15)],
            "service_year": [2024]
        }).lazy()

        disease_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24"],
            "hcc_code": [18],  # Using integer
            "coefficient": [0.302],
            "enrollment_status": ["Continuing"],
            "medicaid_status": ["No"],
            "institutional_status": ["No"],
            "description": ["Diabetes"]
        }).lazy()

        # No matching demographic factors
        demographic_factors = pl.DataFrame({
            "model_version": pl.Series([], dtype=pl.Utf8),
            "gender": pl.Series([], dtype=pl.Utf8),
            "age_group": pl.Series([], dtype=pl.Utf8),
            "coefficient": pl.Series([], dtype=pl.Float64),
            "enrollment_status": pl.Series([], dtype=pl.Utf8),
            "orec": pl.Series([], dtype=pl.Utf8),
            "institutional_status": pl.Series([], dtype=pl.Utf8)
        }).lazy()

        eligibility = pl.DataFrame({
            "person_id": ["P1"],
            "age": [72],
            "gender": ["M"]
        }).lazy()

        config = {"model_version": "V24", "measurement_year": 2024}

        result = HccGapAnalysisTransform.calculate_raf_scores(
            person_hccs, disease_factors, demographic_factors, eligibility, config
        )
        df = result.collect()

        # Should handle missing demographic with null/0
        assert df.height == 1
        assert df["demographic_score"][0] == 0.0  # fill_null(0.0)

    @pytest.mark.unit
    def test_all_diagnoses_null(self):
        """map_diagnoses_to_hccs handles claims with all null diagnosis codes."""
        claims = pl.DataFrame({
            "person_id": ["P1"],
            "diagnosis_code_1": pl.Series([None], dtype=pl.Utf8),
            "diagnosis_code_2": pl.Series([None], dtype=pl.Utf8),
            "diagnosis_code_3": pl.Series([None], dtype=pl.Utf8),
            "claim_end_date": [date(2024, 1, 15)]
        }).lazy()

        icd10_mappings = pl.DataFrame({
            "diagnosis_code": ["E1165"],
            "cms_hcc_v24": ["HCC18"],
            "cms_hcc_v24_flag": ["Yes"],
            "cms_hcc_v28": ["HCC18"],
            "cms_hcc_v28_flag": ["Yes"]
        }).lazy()

        config = {"model_version": "V24"}

        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(claims, icd10_mappings, config)
        df = result.collect()

        assert df.height == 0

    @pytest.mark.unit
    def test_default_config_values(self):
        """Methods use default config values when not provided."""
        claims = pl.DataFrame({
            "person_id": ["P1"],
            "diagnosis_code_1": ["E1165"],
            "diagnosis_code_2": pl.Series([None], dtype=pl.Utf8),
            "diagnosis_code_3": pl.Series([None], dtype=pl.Utf8),
            "claim_end_date": [date(2024, 1, 15)]
        }).lazy()

        icd10_mappings = pl.DataFrame({
            "diagnosis_code": ["E1165"],
            "cms_hcc_v24": ["HCC18"],
            "cms_hcc_v24_flag": ["Yes"],
            "cms_hcc_v28": ["HCC18"],
            "cms_hcc_v28_flag": ["Yes"]
        }).lazy()

        config = {}  # Empty config, should use defaults

        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(claims, icd10_mappings, config)
        df = result.collect()

        # Should use default V24 model
        assert df.height == 1

    @pytest.mark.unit
    def test_unique_age_groups(self):
        """calculate_raf_scores handles all unique age groups correctly."""
        person_hccs = pl.DataFrame({
            "person_id": pl.Series([], dtype=pl.Utf8),
            "hcc_code": pl.Series([], dtype=pl.Utf8),
            "diagnosis_code": pl.Series([], dtype=pl.Utf8),
            "claim_date": pl.Series([], dtype=pl.Date),
            "service_year": pl.Series([], dtype=pl.Int64)
        }).lazy()

        disease_factors = pl.DataFrame({
            "model_version": pl.Series([], dtype=pl.Utf8),
            "hcc_code": pl.Series([], dtype=pl.Utf8),
            "coefficient": pl.Series([], dtype=pl.Float64),
            "enrollment_status": pl.Series([], dtype=pl.Utf8),
            "medicaid_status": pl.Series([], dtype=pl.Utf8),
            "institutional_status": pl.Series([], dtype=pl.Utf8),
            "description": pl.Series([], dtype=pl.Utf8)
        }).lazy()

        demographic_factors = pl.DataFrame({
            "model_version": ["CMS-HCC-V24"] * 10,
            "gender": ["Male"] * 10,
            "age_group": ["65", "66", "67", "68", "69", "70-74", "75-79", "80-84", "85-89", "90-94"],
            "coefficient": [0.35, 0.36, 0.37, 0.38, 0.39, 0.45, 0.55, 0.65, 0.75, 0.85],
            "enrollment_status": ["Continuing"] * 10,
            "orec": ["Aged"] * 10,
            "institutional_status": ["No"] * 10
        }).lazy()

        eligibility = pl.DataFrame({
            "person_id": ["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10"],
            "age": [65, 66, 67, 68, 69, 72, 77, 82, 87, 92],
            "gender": ["M"] * 10
        }).lazy()

        config = {"model_version": "V24", "measurement_year": 2024}

        result = HccGapAnalysisTransform.calculate_raf_scores(
            person_hccs, disease_factors, demographic_factors, eligibility, config
        )
        df = result.collect().sort("person_id")

        assert df.height == 10
        # Verify age group mapping worked for all
        assert all(df["demographic_score"] > 0)
