# © 2025 HarmonyCares
# All rights reserved.

"""
A&D vs ESRD cohort classifier for ACO REACH risk-score selection.

ACO REACH treats Aged & Disabled (A&D) and End-Stage Renal Disease
(ESRD) beneficiaries as two distinct sub-populations, with different
benchmarks, different normalization factors, and — most importantly for
High-Needs eligibility — different risk-adjustment models and different
threshold values for criteria IV.B.1(b) and IV.B.1(c).

Appendix A Section IV.B.2 of the Participation Agreement
($BRONZE/ACO_REACH_PY2026_AR_PA_2023_Starters_508.txt, line 3791)
specifies which model applies to which sub-population:

    "For each Performance Year prior to Performance Year 2024, CMS
    determines Beneficiary risk scores for the purposes of Section IV.B
    of this Appendix for A&D Beneficiaries by using the risk score
    calculated under the CMS-HCC Risk Adjustment Model or the CMMI-HCC
    Concurrent Risk Adjustment Model (as defined in Appendix B of the
    Agreement), whichever risk score is higher. Beginning Performance
    Year 2024, CMS determines Beneficiary risk scores for the purposes
    of Section IV.B of this Appendix for A&D Beneficiaries by using the
    risk score calculated under the 2020 CMS-HCC Risk Adjustment Model
    (Version 24) or the 2024 CMS-HCC Risk Adjustment Model (Version 28)
    or the CMMI-HCC Concurrent Risk Adjustment Model (as defined in
    Appendix B of the Agreement), whichever risk score is higher. CMS
    calculates Beneficiary risk scores for the purposes of Section IV.B
    of this Appendix for ESRD Beneficiaries by using the CMS-HCC Risk
    Adjustment Model."

Appendix B reiterates the split in both model definitions:

    "CMS-HCC Risk Adjustment Model can be applied to A&D Beneficiaries
    (the non-ESRD segment) and the CMS-HCC ESRD Risk Adjustment Model
    to ESRD Beneficiaries (the ESRD segment)."
        — PA line 4315

    "The CMMI-HCC Concurrent Risk Adjustment Model can be applied to
    Aged & Disabled (A&D) Beneficiaries; there is a non-End-Stage Renal
    Disease (ESRD) segment, but no ESRD segment."
        — PA line 4314

The PA does not define *how to identify* an ESRD beneficiary. CMS's
operational convention, published in the HCC model documentation and
implemented in the hccinfhir library, is:

    Original Reason for Entitlement Code (OREC) values of 2 or 3
    indicate ESRD status. OREC 0 = Old Age and Survivors Insurance,
    OREC 1 = Disability Insurance Benefits, OREC 2 = ESRD, OREC 3 =
    Disability Insurance Benefits AND ESRD.
        — hccinfhir/constants.py OREC_ESRD_CODES = {'2', '3'}

    Medicare Status Code (MSTAT, ``bene_mdcr_stus_cd`` in CCLF8 /
    ``medicare_status_code`` in our eligibility gold) is a separate
    current-status signal that also flags ESRD and uses a different
    coding scheme:

        10 = Aged
        11 = Aged + ESRD
        20 = Disabled
        21 = Disabled + ESRD
        31 = ESRD only

    MSTAT ∈ {11, 21, 31} means the bene currently has ESRD even if OREC
    reflects their original entitlement reason (e.g. "aged in" and later
    developed ESRD). In practice MSTAT catches ~1,700 additional ESRD
    benes per roster whose OREC is blank or non-ESRD — without this
    signal, they get silently classified as A&D and evaluated against
    the CMS-HCC A&D threshold of 3.0 instead of the ESRD threshold of
    0.35, which is a silent under-match on criterion (b).

This module classifies each beneficiary into exactly one of {"AD",
"ESRD"} by reading BOTH OREC and MSTAT: ESRD if either signal says
ESRD, else A&D. It is the single source of truth consumed by the
CMS-HCC, CMS-HCC ESRD, and CMMI-HCC Concurrent drivers so model
selection stays consistent.
"""

from __future__ import annotations

import polars as pl


# OREC values that indicate ESRD. OREC=2 is ESRD-only, OREC=3 is
# Disability-Insurance + ESRD. Any other OREC value maps to A&D.
ESRD_ENTITLEMENT_CODES: frozenset[str] = frozenset({"2", "3"})

# Medicare Status Codes (MSTAT, ``bene_mdcr_stus_cd``) that indicate
# current ESRD regardless of original entitlement. 11 = Aged + ESRD,
# 21 = Disabled + ESRD, 31 = ESRD only.
ESRD_MEDICARE_STATUS_CODES: frozenset[str] = frozenset({"11", "21", "31"})


def build_ad_vs_esrd_expr(
    orec_col: str = "original_reason_entitlement_code",
    mstat_col: str | None = "medicare_status_code",
) -> pl.Expr:
    """
    Build a ``pl.Expr`` that returns the string ``"ESRD"`` for ESRD
    beneficiaries and ``"AD"`` for everyone else.

    ESRD is detected by an OR across two signals:
      - OREC ∈ {"2", "3"}  — original entitlement reason is ESRD
      - MSTAT ∈ {"11", "21", "31"} — current Medicare status carries ESRD

    Either signal is sufficient. OREC and MSTAT use different code
    systems (see module docstring) so they are checked independently
    against their own allow-lists rather than coalesced.

    Parameters
    ----------
    orec_col
        Name of the OREC column. Defaults to
        ``"original_reason_entitlement_code"`` (gold/eligibility.parquet).
    mstat_col
        Name of the Medicare Status Code column. Pass ``None`` to skip
        MSTAT (OREC-only classification). Defaults to
        ``"medicare_status_code"`` (gold/eligibility.parquet).
    """
    orec_str = pl.col(orec_col).cast(pl.String, strict=False)
    orec_is_esrd = orec_str.is_in(list(ESRD_ENTITLEMENT_CODES))

    if mstat_col is None:
        is_esrd = orec_is_esrd
    else:
        mstat_str = pl.col(mstat_col).cast(pl.String, strict=False)
        mstat_is_esrd = mstat_str.is_in(list(ESRD_MEDICARE_STATUS_CODES))
        is_esrd = orec_is_esrd | mstat_is_esrd

    return (
        pl.when(is_esrd)
        .then(pl.lit("ESRD"))
        .otherwise(pl.lit("AD"))
    )


def classify_cohort(
    orec: str | int | None,
    crec: str | int | None = None,
    *,
    medicare_status_code: str | int | None = None,
) -> str:
    """
    Scalar version of the cohort classifier — for use in tests and the
    non-LazyFrame call site inside ``hcc_risk_scores.execute``.

    ESRD if any of the following are true:
      - OREC ∈ {"2", "3"}
      - CREC ∈ {"2", "3"} (CREC treated as an alternate-source OREC)
      - medicare_status_code ∈ {"11", "21", "31"}

    ``crec`` is retained for compatibility with callers that passed
    OREC through twice, but it uses the OREC coding scheme (2/3), NOT
    the MSTAT coding scheme — pass MSTAT via the keyword-only
    ``medicare_status_code`` argument.

    Returns either ``"ESRD"`` or ``"AD"``.
    """
    orec_token = str(orec) if orec is not None else None
    crec_token = str(crec) if crec is not None else None
    mstat_token = (
        str(medicare_status_code) if medicare_status_code is not None else None
    )

    if orec_token in ESRD_ENTITLEMENT_CODES:
        return "ESRD"
    if crec_token in ESRD_ENTITLEMENT_CODES:
        return "ESRD"
    if mstat_token in ESRD_MEDICARE_STATUS_CODES:
        return "ESRD"
    return "AD"
