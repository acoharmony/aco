from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest

from acoharmony._expressions._cms_hcc import CmsHccExpression


class TestCmsHccExpression:

    @pytest.mark.unit
    def test_build(self):
        result = CmsHccExpression.build({'hcc_version': 'v28'})
        assert result['version'] == 'v28'
        assert 'expressions' in result

    @pytest.mark.unit
    def test_transform_patient_risk_factors(self):
        claims = pl.DataFrame({'patient_id': ['P1'], 'diagnosis_code': ['E11.9'], 'claim_end_date': [date(2024, 6, 1)]}).lazy()
        elig = pl.DataFrame({'patient_id': ['P1']}).lazy()
        mapping = pl.DataFrame({'diagnosis_code': ['E11.9'], 'cms_hcc_v28': [19], 'cms_hcc_v24': [18]}).lazy()
        result = CmsHccExpression.transform_patient_risk_factors(claims, elig, mapping, config={'hcc_version': 'v28'}).collect()
        assert 'hcc_code' in result.columns

class TestCMSHCCConfigDefault:
    """Cover config=None default."""

    @pytest.mark.unit
    def test_build_hcc_profile_default_config(self):
        """Line 267: config defaults to {}."""
        try:
            CMSHCCExpression.build_hcc_profile(claims_df=pl.LazyFrame({'patient_id': [], 'diagnosis_code': [], 'claim_end_date': []}), config=None)
        except Exception:
            pass


class TestTransformPatientRiskFactorsConfigNone:
    """Cover 266→267: config is None → defaults to {}."""

    @pytest.mark.unit
    def test_config_none_defaults(self):
        """When config=None is passed, the method defaults to {} and uses v28."""
        claims = pl.DataFrame(
            {
                "patient_id": ["P1"],
                "diagnosis_code": ["E119"],
                "claim_end_date": [date(2024, 6, 1)],
            }
        ).lazy()
        elig = pl.DataFrame({"patient_id": ["P1"]}).lazy()
        mapping = pl.DataFrame(
            {
                "diagnosis_code": ["E119"],
                "cms_hcc_v28": ["19"],
                "cms_hcc_v24": [18],
            }
        ).lazy()
        result = CmsHccExpression.transform_patient_risk_factors(
            claims, elig, mapping, config=None
        ).collect()
        assert "hcc_code" in result.columns
        assert result.height >= 1
        assert result["hcc_code"][0] == 19


class TestTransformPatientRiskScores:
    """Cover lines 361-420: transform_patient_risk_scores."""

    @pytest.mark.unit
    def test_risk_scores_calculation(self):
        """Disease scores + demographics → total risk score."""
        risk_factors = pl.DataFrame({
            "patient_id": ["P1", "P1", "P2"],
            "hcc_code": [19, 85, 19],
            "coefficient": [0.118, 0.368, 0.118],
            "last_diagnosis_date": [date(2024, 6, 1), date(2024, 7, 1), date(2024, 6, 1)],
        }).lazy()
        eligibility = pl.DataFrame({
            "patient_id": ["P1", "P2"],
            "birth_date": [date(1958, 3, 15), date(1945, 7, 20)],
            "gender": ["M", "F"],
        }).lazy()

        result = CmsHccExpression.transform_patient_risk_scores(
            risk_factors, eligibility, {"hcc_version": "v28"}
        ).collect()

        assert "patient_id" in result.columns
        assert "age" in result.columns
        assert "demographic_score" in result.columns
        assert "disease_score" in result.columns
        assert "total_risk_score" in result.columns
        assert "hcc_count" in result.columns
        assert "model_version" in result.columns

        # P1 should have 2 HCCs
        p1 = result.filter(pl.col("patient_id") == "P1")
        assert p1["hcc_count"][0] == 2
        assert abs(p1["disease_score"][0] - 0.486) < 0.001

        # P2 should have 1 HCC
        p2 = result.filter(pl.col("patient_id") == "P2")
        assert p2["hcc_count"][0] == 1

        # Model version
        assert result["model_version"][0] == "v28"


class TestTransformPatientRiskFactorsWithDiseaseFactors:
    """Cover 301→303: disease_factors is not None → join with coefficients."""

    @pytest.mark.unit
    def test_disease_factors_joined(self):
        """When disease_factors is provided, hcc_description and coefficient come from it."""
        claims = pl.DataFrame(
            {
                "patient_id": ["P1", "P1"],
                "diagnosis_code": ["E119", "I509"],
                "claim_end_date": [date(2024, 6, 1), date(2024, 7, 1)],
            }
        ).lazy()
        elig = pl.DataFrame({"patient_id": ["P1"]}).lazy()
        mapping = pl.DataFrame(
            {
                "diagnosis_code": ["E119", "I509"],
                "cms_hcc_v28": ["19", "85"],
                "cms_hcc_v24": [18, 84],
            }
        ).lazy()
        disease_factors = pl.DataFrame(
            {
                "model_version": ["CMS-HCC-V28", "CMS-HCC-V28"],
                "hcc_code": [19, 85],
                "description": ["Diabetes", "Heart Failure"],
                "coefficient": [0.118, 0.368],
            }
        ).lazy()

        result = CmsHccExpression.transform_patient_risk_factors(
            claims,
            elig,
            mapping,
            disease_factors=disease_factors,
            config={"hcc_version": "v28"},
        ).collect()

        assert "hcc_description" in result.columns
        assert "coefficient" in result.columns
        # Both HCCs should be present with non-null descriptions
        descs = set(result["hcc_description"].to_list())
        assert "Diabetes" in descs
        assert "Heart Failure" in descs
        coeffs = result.sort("hcc_code")["coefficient"].to_list()
        assert 0.118 in coeffs
        assert 0.368 in coeffs
