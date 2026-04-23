# © 2025 HarmonyCares
# All rights reserved.

"""
Unplanned hospital admissions counter for High-Needs criterion IV.B.1(c).

Counts inpatient admissions per beneficiary where the admission is
"unplanned" per CMS's own operational definition — used solely for the
High-Needs eligibility criterion IV.B.1(c) ("Have a risk score between
2.0 and 3.0 … and two or more unplanned hospital admissions in the
previous 12 months").

This is a *different* definition from the one used in UAMCC
quality-measure scoring. UAMCC applies the elaborate Planned Admission
Algorithm v4.0 (CCS-based planned-procedure / planned-diagnosis
exclusions); the High-Needs eligibility definition is a much simpler
filter codified in the PY2024 Financial Operating Guide footnote 5:

    "An unplanned hospital admission is defined as the claim for the
    inpatient stay being coded as non-elective, specifically based on
    the 'reason for admission' code (CLM_IP_ADMSN_TYPE_CD is not 3)."
        — $BRONZE/REACHPY2024FinancialOperatGde.txt, line 1193

Operational rules
-----------------

1. Source table: CCLF1 (institutional claim header). The relevant
   column in our silver layer is ``clm_admsn_type_cd``. The PA/FOG
   reference "CLM_IP_ADMSN_TYPE_CD" is CMS's name for the same field;
   our feed stores it as ``clm_admsn_type_cd`` on ``silver/cclf1``.

2. Include only inpatient claims. The FOG parent rule (line 1503)
   clarifies "one inpatient claim (claim type 60)" is the accepted
   evidence type. Claim type 60 is the CCLF code for inpatient
   institutional claims (Part A inpatient). We filter
   ``clm_type_cd = '60'`` (stored as string, preserving any leading
   zeros).

3. Deduplicate by admission date per beneficiary. CCLF1 can contain
   multiple claim lines for the same admission (interim bills, adjusted
   claims). Count distinct ``clm_from_dt`` per MBI so a single stay
   contributes once to the admission count.

4. A beneficiary meets criterion IV.B.1(c)'s admissions prong when
   the count of unplanned admissions in the lookback window is
   ``>= 2``. Criterion-c's risk-score prong is evaluated separately and
   ANDed in by the rollup expression in ``_high_needs_criterion_c``.

This module builds Polars expressions only — it does not load data. The
caller scans ``silver/cclf1.parquet`` and composes this expression into
its query.
"""

from __future__ import annotations

from datetime import date

import polars as pl


# Inpatient claim type in the CCLF feed. Per FOG line 1503, inpatient
# claims are the only claim type that counts for criterion IV.B.1(a)'s
# "one inpatient claim" rule; criterion IV.B.1(c)'s admission-counter
# reuses the same inpatient filter.
INPATIENT_CLAIM_TYPE_CODE = "60"

# Admission-type code 3 = "Elective" per the CMS claim-type-code codebook.
# All other admission types (1=Emergency, 2=Urgent, 4=Newborn, 5=Trauma,
# 9=Unknown) are treated as unplanned for the High-Needs eligibility
# evaluation. FOG footnote 5 (line 1193) specifies ``is not 3``; any
# other non-elective code — including nulls — counts as unplanned.
ELECTIVE_ADMISSION_TYPE_CODE = "3"


def build_unplanned_admission_filter(
    claim_type_col: str = "clm_type_cd",
    admission_type_col: str = "clm_admsn_type_cd",
) -> pl.Expr:
    """
    Build a boolean filter expression that is ``True`` for rows
    representing unplanned inpatient admissions.

    Both columns are cast to ``pl.String`` so numeric inputs
    (e.g. an int-coded admission type) classify correctly. ``True`` iff:

        - claim type is inpatient (``"60"``), AND
        - admission type is NOT ``"3"`` (non-elective).

    Null admission type counts as unplanned — consistent with the FOG's
    "is not 3" phrasing, which does not exclude nulls.
    """
    claim_type_str = pl.col(claim_type_col).cast(pl.String, strict=False)
    admsn_type_str = pl.col(admission_type_col).cast(pl.String, strict=False)
    return (
        (claim_type_str == INPATIENT_CLAIM_TYPE_CODE)
        & (admsn_type_str != ELECTIVE_ADMISSION_TYPE_CODE)
    )


def build_unplanned_admission_count_expr(
    mbi_col: str = "bene_mbi_id",
    admission_date_col: str = "clm_from_dt",
    claim_type_col: str = "clm_type_cd",
    admission_type_col: str = "clm_admsn_type_cd",
) -> pl.Expr:
    """
    Build a group-aggregate expression that counts distinct unplanned
    admission dates per beneficiary. Intended to be applied inside a
    ``group_by(mbi_col).agg([...])`` after filtering to the applicable
    Table C lookback window; the caller composes the window filter
    outside this module.

    Returns a ``pl.Expr`` usable directly in ``.agg(...)`` — it yields
    an integer count.

    Deduplication on admission date handles interim-billed and adjusted
    inpatient claims for the same stay: a beneficiary admitted on
    2024-07-15 with three adjustment claims counts once, not three
    times. This matches CMS's operational intent even though the FOG
    does not spell out the dedupe rule.
    """
    return (
        pl.when(
            build_unplanned_admission_filter(claim_type_col, admission_type_col)
        )
        .then(pl.col(admission_date_col))
        .otherwise(None)
        .drop_nulls()
        .n_unique()
        .alias("unplanned_admission_count")
    )


def count_unplanned_admissions_in_window(
    claims_lf: pl.LazyFrame,
    *,
    window_begin: date,
    window_end: date,
    mbi_col: str = "bene_mbi_id",
    admission_date_col: str = "clm_from_dt",
    claim_type_col: str = "clm_type_cd",
    admission_type_col: str = "clm_admsn_type_cd",
) -> pl.LazyFrame:
    """
    Convenience: filter ``claims_lf`` to the [window_begin, window_end]
    inclusive range on ``admission_date_col``, apply the
    inpatient/non-elective filter, deduplicate by (mbi, admission_date),
    and emit one row per MBI with an ``unplanned_admission_count``.

    Uses ``silver/cclf1`` conventions by default. A beneficiary with no
    matching admissions will *not* appear in the output — callers that
    need "zero rows" for the no-admission case should left-join against
    the beneficiary roster.
    """
    admission_date = pl.col(admission_date_col).cast(pl.Date, strict=False)
    return (
        claims_lf.filter(
            build_unplanned_admission_filter(claim_type_col, admission_type_col)
            & admission_date.is_between(window_begin, window_end, closed="both")
        )
        .unique(subset=[mbi_col, admission_date_col])
        .group_by(mbi_col)
        .agg(pl.len().alias("unplanned_admission_count"))
    )
