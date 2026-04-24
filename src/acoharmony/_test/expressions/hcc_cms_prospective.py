# © 2025 HarmonyCares
# All rights reserved.

"""Tests for expressions._hcc_cms_prospective module."""

from __future__ import annotations

import pytest


class TestCmsHccModelsForPy:
    """Tests for the cms_hcc_models_for_py dispatch function."""

    @pytest.mark.unit
    def test_ad_cohort_py2026_returns_v24_and_v28(self):
        from acoharmony._expressions._hcc_cms_prospective import cms_hcc_models_for_py

        models = cms_hcc_models_for_py(2026, "AD")
        assert "CMS-HCC Model V24" in models
        assert "CMS-HCC Model V28" in models

    @pytest.mark.unit
    def test_ad_cohort_py2023_returns_v24_only(self):
        from acoharmony._expressions._hcc_cms_prospective import cms_hcc_models_for_py

        models = cms_hcc_models_for_py(2023, "AD")
        assert models == ("CMS-HCC Model V24",)

    @pytest.mark.unit
    def test_esrd_cohort_returns_esrd_model(self):
        from acoharmony._expressions._hcc_cms_prospective import cms_hcc_models_for_py

        models = cms_hcc_models_for_py(2026, "ESRD")
        assert models == ("CMS-HCC ESRD Model V24",)

    @pytest.mark.unit
    def test_unknown_cohort_raises_value_error(self):
        """Line 119: raise ValueError for unknown cohort."""
        from acoharmony._expressions._hcc_cms_prospective import cms_hcc_models_for_py

        with pytest.raises(ValueError, match="Unknown cohort"):
            cms_hcc_models_for_py(2026, "UNKNOWN")

    @pytest.mark.unit
    def test_ad_cohort_future_py_falls_back_to_v28(self):
        from acoharmony._expressions._hcc_cms_prospective import cms_hcc_models_for_py

        # A PY not in the dispatch table gets the conservative default.
        models = cms_hcc_models_for_py(2099, "AD")
        assert models == ("CMS-HCC Model V28",)

    @pytest.mark.unit
    def test_esrd_cohort_future_py_falls_back_to_esrd_v24(self):
        from acoharmony._expressions._hcc_cms_prospective import cms_hcc_models_for_py

        models = cms_hcc_models_for_py(2099, "ESRD")
        assert models == ("CMS-HCC ESRD Model V24",)


class TestBeneficiaryScoreInput:
    """Verify BeneficiaryScoreInput dataclass fields."""

    @pytest.mark.unit
    def test_dataclass_is_constructible(self):
        from acoharmony._expressions._hcc_cms_prospective import BeneficiaryScoreInput

        bene = BeneficiaryScoreInput(
            mbi="TEST001",
            age=72,
            sex="F",
            orec="0",
            crec="0",
            dual_elgbl_cd="NA",
        )
        assert bene.mbi == "TEST001"
        assert bene.age == 72
        assert bene.diagnosis_codes == ()

    @pytest.mark.unit
    def test_dataclass_is_frozen(self):
        from acoharmony._expressions._hcc_cms_prospective import BeneficiaryScoreInput

        bene = BeneficiaryScoreInput(
            mbi="TEST001", age=72, sex="F", orec="0", crec="0", dual_elgbl_cd="NA"
        )
        with pytest.raises((AttributeError, TypeError)):
            bene.age = 73  # type: ignore[misc]


class TestScoreBeneficiaryUnderModel:
    """Tests for score_beneficiary_under_model and score_beneficiaries."""

    @pytest.mark.unit
    def test_unknown_model_name_raises_value_error(self):
        """Line 185: unknown model_name must raise ValueError."""
        from acoharmony._expressions._hcc_cms_prospective import (
            score_beneficiary_under_model,
            BeneficiaryScoreInput,
        )

        bene = BeneficiaryScoreInput(
            mbi="X", age=70, sex="F", orec="0", crec="0", dual_elgbl_cd="NA"
        )
        with pytest.raises(ValueError, match="Unknown CMS-HCC model"):
            score_beneficiary_under_model(bene, "CMS-HCC Model INVALID")

    @pytest.mark.unit
    def test_hccinfhir_unavailable_raises_runtime_error(self, monkeypatch):
        """Line 183: when _HCCINFHIR_AVAILABLE is False, RuntimeError is raised."""
        import acoharmony._expressions._hcc_cms_prospective as mod

        monkeypatch.setattr(mod, "_HCCINFHIR_AVAILABLE", False)
        bene = mod.BeneficiaryScoreInput(
            mbi="X", age=70, sex="F", orec="0", crec="0", dual_elgbl_cd="NA"
        )
        with pytest.raises(RuntimeError, match="hccinfhir is not importable"):
            mod.score_beneficiary_under_model(bene, "CMS-HCC Model V24")

    @pytest.mark.unit
    def test_score_beneficiary_returns_cms_hcc_score(self):
        """Happy path: a valid beneficiary + valid model name returns CmsHccScore."""
        from acoharmony._expressions._hcc_cms_prospective import (
            score_beneficiary_under_model,
            BeneficiaryScoreInput,
            CmsHccScore,
        )

        bene = BeneficiaryScoreInput(
            mbi="TEST001",
            age=72,
            sex="F",
            orec="0",
            crec="0",
            dual_elgbl_cd="NA",
            diagnosis_codes=("E1165", "I509"),
        )
        result = score_beneficiary_under_model(bene, "CMS-HCC Model V24")
        assert isinstance(result, CmsHccScore)
        assert result.mbi == "TEST001"
        assert result.model_version == "cms_hcc_v24"
        assert result.total_risk_score > 0.0

    @pytest.mark.unit
    def test_score_beneficiaries_returns_one_score_per_model(self):
        """Lines 230-235: score_beneficiaries iterates benes and models."""
        from acoharmony._expressions._hcc_cms_prospective import (
            score_beneficiaries,
            BeneficiaryScoreInput,
        )

        benes = [
            BeneficiaryScoreInput(
                mbi="AD_BENE",
                age=72,
                sex="F",
                orec="0",
                crec="0",
                dual_elgbl_cd="NA",
            ),
        ]
        cohort_for_mbi = {"AD_BENE": "AD"}
        # PY2026: AD gets V24 + V28 = 2 models.
        scores = score_beneficiaries(benes, 2026, cohort_for_mbi)
        assert len(scores) == 2
        versions = {s.model_version for s in scores}
        assert "cms_hcc_v24" in versions
        assert "cms_hcc_v28" in versions

    @pytest.mark.unit
    def test_score_beneficiaries_unknown_mbi_defaults_to_ad(self):
        """cohort_for_mbi.get(mbi, 'AD') fallback: MBI not in dict -> AD."""
        from acoharmony._expressions._hcc_cms_prospective import (
            score_beneficiaries,
            BeneficiaryScoreInput,
        )

        benes = [
            BeneficiaryScoreInput(
                mbi="UNKNOWN_BENE",
                age=65,
                sex="M",
                orec="0",
                crec="0",
                dual_elgbl_cd="NA",
            )
        ]
        # PY2023: AD gets V24 only.
        scores = score_beneficiaries(benes, 2023, {})
        assert len(scores) == 1
        assert scores[0].model_version == "cms_hcc_v24"
