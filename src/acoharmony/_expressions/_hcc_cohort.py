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

    "Current Reason for Entitlement Code" (CREC) uses the same coding
    scheme and is a secondary signal. CREC may differ from OREC when a
    beneficiary's current entitlement status has changed — e.g. a
    disability-originated enrollee later developed ESRD. For ACO REACH
    purposes, CREC is the authoritative current-status signal when both
    are available, but most feeds only carry OREC.

This module classifies each beneficiary into exactly one of {"AD",
"ESRD"} based on OREC (or CREC if present). It is the single source
of truth consumed by the CMS-HCC, CMS-HCC ESRD, and CMMI-HCC Concurrent
drivers so model selection stays consistent.
"""

from __future__ import annotations

import polars as pl


# Operational ESRD-indicator values (OREC or CREC). Both codes map to the
# ESRD segment; any other value maps to the A&D segment.
ESRD_ENTITLEMENT_CODES: frozenset[str] = frozenset({"2", "3"})


def build_ad_vs_esrd_expr(
    orec_col: str = "original_reason_entitlement_code",
    crec_col: str | None = "medicare_status_code",
) -> pl.Expr:
    """
    Build a ``pl.Expr`` that returns the string ``"ESRD"`` for ESRD
    beneficiaries and ``"AD"`` for everyone else.

    ESRD is detected by checking whether CREC (preferred, when present
    and non-null) or OREC contains the string ``"2"`` or ``"3"`` — the
    operational convention documented above. Both columns are cast to
    ``pl.String`` first so numeric inputs (e.g. ``2`` as int) classify
    correctly.

    Parameters
    ----------
    orec_col
        Name of the OREC column on the input frame. Defaults to
        ``"original_reason_entitlement_code"``, which is the column
        emitted by ``gold/eligibility.parquet``.
    crec_col
        Name of the CREC / current-status column. Pass ``None`` to
        use OREC alone. Defaults to ``"medicare_status_code"``, also
        on ``gold/eligibility.parquet``.

    The output expression assumes the resolved column yields a string;
    callers that need different input semantics (e.g. int-coded CREC)
    should pre-cast.
    """
    orec_str = pl.col(orec_col).cast(pl.String, strict=False)

    if crec_col is None:
        resolved = orec_str
    else:
        resolved = pl.coalesce(
            pl.col(crec_col).cast(pl.String, strict=False),
            orec_str,
        )

    esrd_codes = list(ESRD_ENTITLEMENT_CODES)
    return (
        pl.when(resolved.is_in(esrd_codes))
        .then(pl.lit("ESRD"))
        .otherwise(pl.lit("AD"))
    )


def classify_cohort(orec: str | int | None, crec: str | int | None = None) -> str:
    """
    Scalar version of the cohort classifier — for use in tests and the
    rare non-LazyFrame call site. Same rule as the Polars expression:
    CREC preferred when present and non-null, else OREC; ESRD if the
    resolved code is ``"2"`` or ``"3"``, else A&D.

    Returns either ``"ESRD"`` or ``"AD"``.
    """
    resolved = crec if crec is not None else orec
    if resolved is None:
        return "AD"
    token = str(resolved)
    if token in ESRD_ENTITLEMENT_CODES:
        return "ESRD"
    return "AD"
