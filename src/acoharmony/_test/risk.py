# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._risk — the thin adapter over vendored hccinfhir."""

from __future__ import annotations

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

from acoharmony._risk import HCCEngine, RiskScore


class TestRiskScoreDataclass:
    """The acoharmony RiskScore shape is the adapter's public return type."""

    @pytest.mark.unit
    def test_is_immutable(self):
        """RiskScore is a frozen dataclass — reconciliation results must
        not be mutated in place after calculation.
        """
        score = RiskScore(risk_score=1.5, hcc_list=("38",), cc_to_dx={})
        with pytest.raises((AttributeError, TypeError)):
            score.risk_score = 999.0

    @pytest.mark.unit
    def test_constructed_from_fields(self):
        score = RiskScore(
            risk_score=0.737,
            hcc_list=("38",),
            cc_to_dx={"38": frozenset(["E119"])},
        )
        assert score.risk_score == pytest.approx(0.737)
        assert score.hcc_list == ("38",)
        assert score.cc_to_dx == {"38": frozenset(["E119"])}


class TestHCCEngineConstruction:
    """The engine constructs against each supported model."""

    @pytest.mark.unit
    def test_default_is_v28(self):
        engine = HCCEngine()
        assert engine.model_name == "CMS-HCC Model V28"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "model_name",
        [
            "CMS-HCC Model V22",
            "CMS-HCC Model V24",
            "CMS-HCC Model V28",
            "CMS-HCC ESRD Model V21",
            "CMS-HCC ESRD Model V24",
        ],
    )
    def test_supported_models_construct(self, model_name):
        """Every supported model must construct without error against
        the vendored coefficient files."""
        engine = HCCEngine(model_name=model_name)
        assert engine.model_name == model_name


class TestScorePatientV28:
    """V28 score deterministically for known inputs."""

    @pytest.mark.unit
    def test_81yo_male_nondual_e119(self):
        """Same scenario as the raw engine test but routed through the wrapper.

        Verifies the wrapper preserves the score (doesn't drop digits
        in the dataclass conversion) AND that the RiskScore shape is
        populated correctly.
        """
        engine = HCCEngine(model_name="CMS-HCC Model V28")
        result = engine.score_patient(
            diagnosis_codes=["E119"],
            age=81,
            sex="M",
        )
        assert isinstance(result, RiskScore)
        assert result.hcc_list == ("38",)
        assert result.cc_to_dx == {"38": frozenset(["E119"])}
        assert result.risk_score == pytest.approx(0.737, abs=0.001)

    @pytest.mark.unit
    def test_no_diagnoses_gives_demographic_only_score(self):
        """With zero diagnoses the score should equal the demographic
        baseline for the patient's age/sex bucket — no HCCs active."""
        engine = HCCEngine(model_name="CMS-HCC Model V28")
        result = engine.score_patient(
            diagnosis_codes=[],
            age=81,
            sex="M",
        )
        assert result.hcc_list == ()
        assert result.cc_to_dx == {}
        # Demographic-only score is positive and well below 1.0 for a
        # healthy aged male — exact value depends on the vendored
        # coefficient table and is locked to the 2026 CMS release.
        assert 0.0 < result.risk_score < 1.0

    @pytest.mark.unit
    def test_multiple_diagnoses_accumulate_hccs(self):
        """Multiple unrelated diagnoses should produce multiple HCCs."""
        engine = HCCEngine(model_name="CMS-HCC Model V28")
        result = engine.score_patient(
            diagnosis_codes=[
                "E119",  # Type 2 diabetes → HCC 38
                "I5030",  # Unspecified diastolic heart failure
            ],
            age=75,
            sex="F",
        )
        # Diabetes HCC is always present; CHF HCC depends on V28 mapping.
        assert "38" in result.hcc_list
        assert result.risk_score > 0.0

    @pytest.mark.unit
    def test_sex_affects_demographic_score(self):
        """Demographic-only scores differ between male and female patients
        of the same age, because CMS ships separate age/sex coefficient
        tables. Test the relationship, not the specific values.
        """
        engine = HCCEngine(model_name="CMS-HCC Model V28")
        male = engine.score_patient(diagnosis_codes=[], age=75, sex="M")
        female = engine.score_patient(diagnosis_codes=[], age=75, sex="F")
        assert male.risk_score != female.risk_score


class TestEngineIsStatelessAcrossCalls:
    """Scoring patient A then patient B must not leak state between them."""

    @pytest.mark.unit
    def test_repeated_calls_produce_consistent_results(self):
        """Same input → same output regardless of what came before."""
        engine = HCCEngine(model_name="CMS-HCC Model V28")
        first = engine.score_patient(
            diagnosis_codes=["E119"], age=81, sex="M"
        )
        # Score some different patient in between to try to perturb state
        engine.score_patient(diagnosis_codes=["I10"], age=65, sex="F")
        second = engine.score_patient(
            diagnosis_codes=["E119"], age=81, sex="M"
        )
        assert first.risk_score == second.risk_score
        assert first.hcc_list == second.hcc_list
