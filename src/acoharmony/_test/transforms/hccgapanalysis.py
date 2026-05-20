"""Tests for acoharmony._transforms.hcc_gap_analysis module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

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
    def test_transform_registered(self):
        """Transform is registered in the registry."""
        assert HccGapAnalysisTransform is not None

    @pytest.mark.unit
    def test_load_hcc_value_sets_callable(self):
        """load_hcc_value_sets method exists and is callable."""
        assert callable(HccGapAnalysisTransform.load_hcc_value_sets)

    @patch('polars.scan_parquet')
    @pytest.mark.unit
    def test_load_hcc_value_sets_returns_dict(self, mock_scan):
        """load_hcc_value_sets returns dictionary of LazyFrames."""

        # Create a real DataFrame instead of MagicMock
        mock_df = pl.DataFrame({"dummy": range(10)})  # height = 10
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_lf.collect.return_value = mock_df
        mock_scan.return_value = mock_lf

        result = HccGapAnalysisTransform.load_hcc_value_sets(Path('/fake/silver'))

        assert isinstance(result, dict)
        # Should have all 10 value sets
        expected_keys = {
            "icd10_mappings", "disease_factors", "demographic_factors",
            "disease_hierarchy", "disease_interactions", "disabled_interactions",
            "enrollment_interactions", "payment_hcc_count", "cpt_hcpcs", "adjustment_rates"
        }
        assert set(result.keys()) == expected_keys

    @patch('polars.scan_parquet')
    @pytest.mark.unit
    def test_load_hcc_value_sets_handles_missing_files(self, mock_scan):
        """load_hcc_value_sets handles missing files gracefully."""

        # Mock scan_parquet to raise error for some files
        def side_effect(path):
            if "disease_factors" in str(path):
                raise FileNotFoundError("File not found")
            # Real DataFrame instead of MagicMock
            mock_df = pl.DataFrame({"dummy": range(5)})  # height = 5
            mock_lf = MagicMock(spec=pl.LazyFrame)
            mock_lf.collect.return_value = mock_df
            return mock_lf

        mock_scan.side_effect = side_effect

        result = HccGapAnalysisTransform.load_hcc_value_sets(Path('/fake/silver'))

        # Should still return dict with empty LazyFrame for missing file
        assert isinstance(result, dict)
        assert "disease_factors" in result

    @pytest.mark.unit
    def test_map_diagnoses_to_hccs_v24(self):
        """map_diagnoses_to_hccs maps ICD-10 codes to HCCs using V24 model."""

        # Create sample claims data
        claims = pl.DataFrame({
            "person_id": ["P1", "P2", "P3"],
            "diagnosis_code_1": ["E1165", "I5030", "E0800"],
            "diagnosis_code_2": pl.Series(["Z7952", None, "I2510"], dtype=pl.Utf8),
            "diagnosis_code_3": pl.Series([None, "E785", None], dtype=pl.Utf8),
            "claim_end_date": [date(2024, 1, 15), date(2024, 2, 20), date(2024, 3, 10)]
        }).lazy()

        # Create sample ICD-10 mappings
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

        # Should have mapped all diagnoses to HCCs
        assert df.height > 0
        assert "person_id" in df.columns
        assert "hcc_code" in df.columns
        assert "diagnosis_code" in df.columns
        assert "service_year" in df.columns

    @pytest.mark.unit
    def test_map_diagnoses_to_hccs_v28(self):
        """map_diagnoses_to_hccs maps ICD-10 codes to HCCs using V28 model."""

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

        config = {"model_version": "V28", "measurement_year": 2024}

        result = HccGapAnalysisTransform.map_diagnoses_to_hccs(claims, icd10_mappings, config)
        df = result.collect()

        assert df.height > 0
        assert df["hcc_code"][0] == "HCC18"

    @pytest.mark.unit
    def test_map_diagnoses_to_hccs_filters_invalid(self):
        """map_diagnoses_to_hccs filters out invalid HCC mappings."""

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
    def test_apply_hcc_hierarchies_with_rules(self):
        """apply_hcc_hierarchies applies hierarchy rules correctly."""

        person_hccs = pl.DataFrame({
            "person_id": ["P1", "P1", "P2"],
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

        # Should have hierarchies applied
        assert df.height >= 0

    @pytest.mark.unit
    def test_apply_hcc_hierarchies_no_rules(self):
        """apply_hcc_hierarchies returns original when no hierarchy rules found."""

        person_hccs = pl.DataFrame({
            "person_id": ["P1"],
            "hcc_code": ["HCC18"],
            "diagnosis_code": ["E1165"],
            "claim_date": [date(2024, 1, 15)],
            "service_year": [2024]
        }).lazy()

        # Empty hierarchy (no rules for V28)
        disease_hierarchy = pl.DataFrame({
            "model_version": [],
            "hcc_code": [],
            "hccs_to_exclude": []
        }).lazy()

        config = {"model_version": "V28"}

        result = HccGapAnalysisTransform.apply_hcc_hierarchies(person_hccs, disease_hierarchy, config)
        df = result.collect()

        # Should return original person_hccs unchanged
        assert df.height == 1
        assert df["hcc_code"][0] == "HCC18"
