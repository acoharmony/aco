# © 2025 HarmonyCares
# All rights reserved.

"""
CMS-HCC Prospective Risk Adjustment Model driver.

Computes CMS-HCC prospective risk scores for ACO REACH beneficiaries by
calling ``hccinfhir.calculate_raf()`` once per beneficiary with the
appropriate model version and demographic prefix.

Source definition (PA Appendix B):

    "CMS-HCC Risk Adjustment Model" means a method for measuring the
    health risk of a population with a risk score to reflect the
    predicted expenditures of that population. The CMS-HCC Risk
    Adjustment Model has a prospective model design, which means that
    risk scores are calculated using diagnoses recorded on claims with
    dates of service during the calendar year prior to the calendar
    year in which the risk scores are used for payment purposes.
    CMS-HCC Risk Adjustment Model can be applied to A&D Beneficiaries
    (the non-ESRD segment) and the CMS-HCC ESRD Risk Adjustment Model
    to ESRD Beneficiaries (the ESRD segment). For Performance Year
    2026, the risk score shall be calculated using a 100% weight of
    the updated 2024 CMS-HCC Risk Adjustment Model (Version 28), as
    described in the Announcement of Calendar Year (CY) 2026 Medicare
    Advantage (MA) Capitation Rates and Part C and Part D Payment
    Policies.
        — $BRONZE/ACO_REACH_PY2026_AR_PA_2023_Starters_508.txt, line 4315

Model-version dispatch for High-Needs eligibility (PA IV.B.2, line
3791):

    For each PY prior to PY2024: V24 only (for A&D); ESRD model for
    ESRD.
    Beginning PY2024: V24 OR V28 (take the higher); ESRD model for
    ESRD.

We compute every applicable version so the max-selection downstream can
draw from all of them and the per-version scores stay auditable.

Outputs one row per (mbi, model_version, score_as_of_date), where
``model_version`` is one of:

    ``cms_hcc_v22``       — legacy; rarely used in REACH
    ``cms_hcc_v24``       — PY ≥ 2023 A&D prospective
    ``cms_hcc_v28``       — PY ≥ 2024 A&D prospective
    ``cms_hcc_esrd_v21``  — legacy ESRD
    ``cms_hcc_esrd_v24``  — ESRD prospective (per PA line 4315)

The driver does NOT perform the ``max()`` across versions or with
CMMI-HCC Concurrent — that's the job of the criterion-b/c evaluators,
which per FOG line 1406 take the higher of the applicable scores.

Scoring internals are deferred to hccinfhir; this module contributes
(1) the ACO-REACH-specific model-selection logic per PY/cohort and
(2) the diagnoses-to-per-beneficiary aggregation step. Every individual
coefficient and hierarchy rule in hccinfhir is itself tested by that
library's own suite, which we do not duplicate here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

try:
    from acoharmony._depends.hccinfhir.model_calculate import calculate_raf
    from acoharmony._depends.hccinfhir.datamodels import ModelName
    _HCCINFHIR_AVAILABLE = True
except ImportError:  # ALLOWED: hccinfhir is a vendored dep; guard against absence
    _HCCINFHIR_AVAILABLE = False


# =============================================================================
# PY → applicable model-versions mapping
# =============================================================================
# Per PA Section IV.B.2:
#   PY 2022 (pre-2024): A&D = V24, ESRD = ESRD V24 (or V21 legacy)
#   PY 2023 (pre-2024): A&D = V24, ESRD = ESRD V24
#   PY 2024+         : A&D = max(V24, V28); ESRD = ESRD V24
# CMMI-HCC Concurrent is ALSO applicable for A&D — that's evaluated in
# the CMMI driver and combined by the criterion evaluator.

_AD_MODELS_BY_PY: dict[int, tuple[str, ...]] = {
    2022: ("CMS-HCC Model V24",),
    2023: ("CMS-HCC Model V24",),
    2024: ("CMS-HCC Model V24", "CMS-HCC Model V28"),
    2025: ("CMS-HCC Model V24", "CMS-HCC Model V28"),
    2026: ("CMS-HCC Model V24", "CMS-HCC Model V28"),
}

_ESRD_MODELS_BY_PY: dict[int, tuple[str, ...]] = {
    2022: ("CMS-HCC ESRD Model V24",),
    2023: ("CMS-HCC ESRD Model V24",),
    2024: ("CMS-HCC ESRD Model V24",),
    2025: ("CMS-HCC ESRD Model V24",),
    2026: ("CMS-HCC ESRD Model V24",),
}


def cms_hcc_models_for_py(performance_year: int, cohort: str) -> tuple[str, ...]:
    """
    Return the hccinfhir ``ModelName`` values applicable for a given
    performance year and cohort.

    ``cohort`` must be ``"AD"`` or ``"ESRD"`` (as produced by
    ``_hcc_cohort.classify_cohort``).

    For PYs outside the known dispatch table (earlier than 2022, or a
    future PY we haven't cited yet), returns the most recent known
    mapping for that cohort — with a conservative default of V28 for
    A&D and ESRD V24 for ESRD.
    """
    if cohort == "AD":
        return _AD_MODELS_BY_PY.get(performance_year, ("CMS-HCC Model V28",))
    if cohort == "ESRD":
        return _ESRD_MODELS_BY_PY.get(performance_year, ("CMS-HCC ESRD Model V24",))
    raise ValueError(f"Unknown cohort {cohort!r}; expected 'AD' or 'ESRD'")


# =============================================================================
# hccinfhir wrapper: per-beneficiary score computation
# =============================================================================


@dataclass(frozen=True)
class BeneficiaryScoreInput:
    """
    Minimal demographic + diagnosis envelope passed to ``calculate_raf``.
    All fields map directly onto the hccinfhir function signature.
    """

    mbi: str
    age: int
    sex: str                        # 'F' or 'M'
    orec: str                       # Original Reason for Entitlement Code
    crec: str                       # Current Reason for Entitlement Code; '0' when unknown
    dual_elgbl_cd: str              # 'NA' when unknown
    low_income: bool = False
    lti: bool = False               # Long-term institutional
    new_enrollee: bool = False
    snp: bool = False
    diagnosis_codes: tuple[str, ...] = ()
    graft_months: int | None = None


@dataclass(frozen=True)
class CmsHccScore:
    """One beneficiary's score under one model version."""

    mbi: str
    model_version: str  # canonical short name, e.g. "cms_hcc_v24"
    total_risk_score: float
    demographic_score: float
    disease_score: float
    hcc_count: int


_MODEL_NAME_TO_SLUG: dict[str, str] = {
    "CMS-HCC Model V22": "cms_hcc_v22",
    "CMS-HCC Model V24": "cms_hcc_v24",
    "CMS-HCC Model V28": "cms_hcc_v28",
    "CMS-HCC ESRD Model V21": "cms_hcc_esrd_v21",
    "CMS-HCC ESRD Model V24": "cms_hcc_esrd_v24",
}


def score_beneficiary_under_model(
    bene: BeneficiaryScoreInput,
    model_name: str,
) -> CmsHccScore:
    """
    Invoke ``hccinfhir.calculate_raf`` for a single beneficiary under a
    specific ``ModelName``. Returns a ``CmsHccScore`` with the canonical
    slug for the model version.

    Raises ``RuntimeError`` if hccinfhir isn't available (the vendored
    copy failed to import). Raises ``ValueError`` for an unknown
    ``model_name``.
    """
    if not _HCCINFHIR_AVAILABLE:
        raise RuntimeError("hccinfhir is not importable; vendored copy is missing")
    if model_name not in _MODEL_NAME_TO_SLUG:
        raise ValueError(f"Unknown CMS-HCC model {model_name!r}")

    result = calculate_raf(
        diagnosis_codes=list(bene.diagnosis_codes),
        model_name=model_name,  # type: ignore[arg-type]
        age=bene.age,
        sex=bene.sex,
        dual_elgbl_cd=bene.dual_elgbl_cd,
        orec=bene.orec,
        crec=bene.crec,
        new_enrollee=bene.new_enrollee,
        snp=bene.snp,
        low_income=bene.low_income,
        lti=bene.lti,
        graft_months=bene.graft_months,
    )

    return CmsHccScore(
        mbi=bene.mbi,
        model_version=_MODEL_NAME_TO_SLUG[model_name],
        total_risk_score=float(result.risk_score),
        demographic_score=float(getattr(result, "demographic_score", 0.0) or 0.0),
        disease_score=float(getattr(result, "disease_score", 0.0) or 0.0),
        hcc_count=int(getattr(result, "hcc_count", 0) or 0),
    )


def score_beneficiaries(
    benes: Iterable[BeneficiaryScoreInput],
    performance_year: int,
    cohort_for_mbi: dict[str, str],
) -> list[CmsHccScore]:
    """
    Score every beneficiary under every model applicable to the PY/cohort.

    ``cohort_for_mbi`` maps each beneficiary's MBI to either ``"AD"`` or
    ``"ESRD"`` (output of ``_hcc_cohort.classify_cohort``). The driver
    picks the applicable model list accordingly and emits one
    ``CmsHccScore`` per (mbi, model_version) pair.

    For a given beneficiary, ESRD cohort membership switches the model
    family from CMS-HCC to CMS-HCC ESRD; PA Appendix B Section IV.B.2
    mandates the ESRD model for ESRD beneficiaries. Criterion-b/c
    evaluators use the per-cohort threshold matching this model family.
    """
    scores: list[CmsHccScore] = []
    for bene in benes:
        cohort = cohort_for_mbi.get(bene.mbi, "AD")
        for model_name in cms_hcc_models_for_py(performance_year, cohort):
            scores.append(score_beneficiary_under_model(bene, model_name))
    return scores
