# © 2025 HarmonyCares
# All rights reserved.

"""
CMMI-HCC Concurrent Risk Adjustment Model driver.

Computes CMMI-HCC concurrent risk scores for ACO REACH beneficiaries.
Unlike the CMS-HCC prospective models, CMMI-HCC Concurrent is not
vendored in hccinfhir — CMS only publishes it as a formatted table in
the annual Risk Adjustment Methodology PDF. This module applies those
coefficients directly.

Source definition (PA Appendix B, $BRONZE/ACO_REACH_PY2026_AR_PA_2023
_Starters_508.txt, line 4314):

    "CMMI-HCC Concurrent Risk Adjustment Model" means a method for
    measuring the health risk of a population with a risk score to
    reflect the predicted expenditures of that population. The CMMI-HCC
    Concurrent Risk Adjustment Model has a concurrent model design,
    which means that risk scores are calculated using diagnoses
    recorded on claims with dates of service during the calendar year
    in which the risk scores are used for payment purposes. The
    CMMI-HCC Concurrent Risk Adjustment Model can be applied to Aged &
    Disabled (A&D) Beneficiaries; there is a non-End-Stage Renal
    Disease (ESRD) segment, but no ESRD segment.

The operational construction (PY2023 Risk Adjustment doc section VI):

    "The new CMMI-HCC concurrent risk adjustment model is similar to
    the CMS-HCC prospective model. The key difference is that it uses
    demographic indicators and diagnoses from a given year to predict
    expenditures in that same year."
        — $BRONZE/PY2023 ACO REACH KCC Risk Adjustment.txt, line 63

Model structure
---------------

Score = age/sex factor
      + sum of HCC coefficients (after applying CMMI-HCC hierarchy)
      + payment-HCC-count interaction (if N ≥ 5)
      + age-<65 × HCC interaction (where applicable)
      + post-kidney-transplant indicator (at most one)

All coefficients come from
``_hcc_cmmi_concurrent_coefficients.CMMI_CONCURRENT_2023``. The
hierarchy rules come from the same module
(``CMMI_CONCURRENT_2023_HIERARCHIES``) — sourced from Table A-1 of the
same PDF.

Hierarchy application
---------------------

The CMMI-HCC model's hierarchy matches the CMS-HCC V24 hierarchy for
most HCCs but MODIFIES the kidney hierarchy (HCCs 135–138) per the
Appendix B Modified Hierarchies note (PDF page 41):

    "1. HCC 134 Dialysis Status, which is normally included in V24
        CMS-HCCs, is excluded from this model.
     2. HCC 135 Acute Renal Failure, which is normally above HCCs
        136-138 in the hierarchy and excludes those diagnoses, is
        separated from the rest of the hierarchy in this model. It
        is possible for an individual to have diagnoses for Acute
        Renal Failure and one of the Chronic Kidney Disease HCCs.
     3. Also, as a policy decision, the model does not enforce the
        hierarchy constraint requiring the HCC 80 coefficient to be
        less than or equal to the HCC 27 coefficient. HCC 27 does
        still exclude an HCC 80 diagnosis, however."

The hierarchy table in our coefficients module is the published
dominant→subordinate pairs from Table A-1, which already reflects
these modifications (HCC 135 is NOT a dominant in that table, and HCC
134 does not appear at all).

Inputs
------

This driver takes **pre-aggregated HCCs** per beneficiary, not raw
diagnosis codes. Mapping ICD-10-CM → HCC for the CMMI model relies on
the same V24 dx-to-CC crosswalk hccinfhir already carries (see
``_depends/hccinfhir/data/ra_dx_to_cc_2025.csv``); the caller runs that
mapping upstream and hands us the HCC list. That keeps this module
focused on the coefficient summation (where CMMI differs from CMS) and
keeps the expensive dx-to-HCC join in the transform layer where it can
be computed once and reused across model drivers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from acoharmony._expressions._hcc_cmmi_concurrent_coefficients import (
    CMMI_CONCURRENT_2023_AGE_SEX,
    CMMI_CONCURRENT_2023_HCC,
    CMMI_CONCURRENT_2023_HCC_AGE_LT_65_INTERACTION,
    CMMI_CONCURRENT_2023_HIERARCHIES,
    CMMI_CONCURRENT_2023_PAYMENT_HCC_COUNT,
    CMMI_CONCURRENT_2023_POST_KIDNEY_TRANSPLANT,
)


# =============================================================================
# Age/sex cell resolution
# =============================================================================

_FEMALE_AGE_CUTS: tuple[tuple[int, str], ...] = (
    (34, "F0_34"),
    (44, "F35_44"),
    (54, "F45_54"),
    (59, "F55_59"),
    (64, "F60_64"),
    (69, "F65_69"),
    (74, "F70_74"),
    (79, "F75_79"),
    (84, "F80_84"),
    (89, "F85_89"),
    (94, "F90_94"),
    (200, "F95_GT"),
)

_MALE_AGE_CUTS: tuple[tuple[int, str], ...] = (
    (34, "M0_34"),
    (44, "M35_44"),
    (54, "M45_54"),
    (59, "M55_59"),
    (64, "M60_64"),
    (69, "M65_69"),
    (74, "M70_74"),
    (79, "M75_79"),
    (84, "M80_84"),
    (89, "M85_89"),
    (94, "M90_94"),
    (200, "M95_GT"),
)


def age_sex_cell(age: int, sex: str) -> str:
    """Return the CMMI-HCC age/sex cell key for a beneficiary."""
    cuts = _FEMALE_AGE_CUTS if sex.upper() == "F" else _MALE_AGE_CUTS
    for upper, cell in cuts:
        if age <= upper:
            return cell
    return cuts[-1][1]


# =============================================================================
# Payment-HCC-count bucket
# =============================================================================


def payment_hcc_count_key(n: int) -> str | None:
    """
    Return the key into ``CMMI_CONCURRENT_2023_PAYMENT_HCC_COUNT`` for
    a beneficiary with ``n`` payment HCCs (after hierarchy reduction).

    Counts < 5 contribute zero (there is no coefficient). Counts 5-14
    map to the specific ``=N`` key. Counts >= 15 map to the ``>=15``
    key.
    """
    if n < 5:
        return None
    if n >= 15:
        return ">=15"
    return f"={n}"


# =============================================================================
# Hierarchy reduction
# =============================================================================


def apply_cmmi_hierarchy(hccs: Iterable[str]) -> set[str]:
    """
    Apply the CMMI-HCC hierarchy rules from Table A-1 of the PY2023 Risk
    Adjustment PDF to a set of HCCs, returning only the payment HCCs
    that survive after dominance.

    For each dominant HCC present, every subordinate HCC it declares is
    removed. Dominance is applied in one pass since the published table
    is already transitive (HCC 8 drops 9-12; HCC 9 drops 10-12; any HCC
    that 8 drops, 9 would also have dropped — so dropping via 8 alone
    is sufficient).
    """
    present = {str(h) for h in hccs if str(h) in CMMI_CONCURRENT_2023_HCC}
    drop: set[str] = set()
    for hcc in present:
        if hcc in CMMI_CONCURRENT_2023_HIERARCHIES:
            for sub in CMMI_CONCURRENT_2023_HIERARCHIES[hcc]:
                drop.add(sub)
    return present - drop


# =============================================================================
# Scoring
# =============================================================================


@dataclass(frozen=True)
class CmmiConcurrentInput:
    """Per-beneficiary inputs to the CMMI-HCC Concurrent model."""

    mbi: str
    age: int
    sex: str                             # 'F' or 'M'
    hccs: tuple[str, ...]                # raw HCCs; hierarchy applied by the driver
    post_kidney_transplant_category: str | None = None
    # One of the four keys in CMMI_CONCURRENT_2023_POST_KIDNEY_TRANSPLANT,
    # or None if the beneficiary is not in a post-transplant window.


@dataclass(frozen=True)
class CmmiConcurrentScore:
    """One beneficiary's CMMI-HCC Concurrent score breakdown."""

    mbi: str
    total_risk_score: float
    age_sex_score: float
    hcc_score: float
    hcc_count_score: float
    hcc_age_lt_65_score: float
    post_transplant_score: float
    payment_hccs_after_hierarchy: tuple[str, ...]


def score_cmmi_concurrent(bene: CmmiConcurrentInput) -> CmmiConcurrentScore:
    """
    Compute the CMMI-HCC Concurrent risk score for one beneficiary.

    Follows the Appendix B structure:
        total = age_sex + Σ hcc_coeff(present, after-hierarchy)
              + hcc_count_coeff(N)
              + Σ age<65 × hcc interaction (where applicable)
              + post_transplant_coeff (if applicable)

    Post-transplant: only one of the four categories applies to any
    given beneficiary at any time; the caller resolves which one from
    transplant-claim date metadata and passes its string key.

    The returned score breakdown makes every component auditable so
    reconciliation against CMS's published score is easier.
    """
    age_sex_cell_key = age_sex_cell(bene.age, bene.sex)
    age_sex_score = CMMI_CONCURRENT_2023_AGE_SEX[age_sex_cell_key]

    payment_hccs = apply_cmmi_hierarchy(bene.hccs)

    hcc_score = sum(CMMI_CONCURRENT_2023_HCC[h] for h in payment_hccs)

    count_key = payment_hcc_count_key(len(payment_hccs))
    hcc_count_score = (
        CMMI_CONCURRENT_2023_PAYMENT_HCC_COUNT[count_key]
        if count_key is not None
        else 0.0
    )

    hcc_age_lt_65_score = 0.0
    if bene.age < 65:
        for h in payment_hccs:
            if h in CMMI_CONCURRENT_2023_HCC_AGE_LT_65_INTERACTION:
                hcc_age_lt_65_score += (
                    CMMI_CONCURRENT_2023_HCC_AGE_LT_65_INTERACTION[h]
                )

    post_transplant_score = 0.0
    if bene.post_kidney_transplant_category is not None:
        post_transplant_score = CMMI_CONCURRENT_2023_POST_KIDNEY_TRANSPLANT[
            bene.post_kidney_transplant_category
        ]

    total = (
        age_sex_score
        + hcc_score
        + hcc_count_score
        + hcc_age_lt_65_score
        + post_transplant_score
    )

    return CmmiConcurrentScore(
        mbi=bene.mbi,
        total_risk_score=total,
        age_sex_score=age_sex_score,
        hcc_score=hcc_score,
        hcc_count_score=hcc_count_score,
        hcc_age_lt_65_score=hcc_age_lt_65_score,
        post_transplant_score=post_transplant_score,
        payment_hccs_after_hierarchy=tuple(sorted(payment_hccs, key=lambda s: int(s))),
    )
