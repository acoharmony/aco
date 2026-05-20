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

1. Source table: ``gold/medical_claim`` (Tuva-normalised union of
   institutional / DME / professional claims, MBI-crosswalked via
   ``person_id``). The PA/FOG reference to ``CLM_IP_ADMSN_TYPE_CD`` is
   surfaced in Tuva's normalised schema as ``admit_type_code``.

2. Include only institutional inpatient claims. Tuva encodes this as
   ``claim_type == "institutional"`` AND ``bill_type_code`` starting
   with ``"11"`` (UB bill-type facility code 11 = hospital inpatient).
   The FOG's "claim type 60" rule at line 1503 resolves to the same
   population in this schema.

3. Deduplicate by admission_date per person. medical_claim exposes
   one row per claim line, so a single stay can contribute many lines
   (interim bills, adjusted claims, revenue-center detail). Count
   distinct ``admission_date`` per ``person_id`` so one stay
   contributes once to the admission count.

4. Use ``person_id`` (the crosswalked canonical MBI) rather than a raw
   claim-time MBI. A bene whose MBI rotated mid-window has their pre-
   and post-rotation admissions attributed to one identity on the
   medical_claim side.

5. A beneficiary meets criterion IV.B.1(c)'s admissions prong when
   the count of unplanned admissions in the lookback window is
   ``>= 2``. Criterion-c's risk-score prong is evaluated separately and
   ANDed in by the rollup expression in ``_high_needs_criterion_c``.

This module builds Polars expressions only — it does not load data. The
caller scans ``gold/medical_claim.parquet`` and composes this
expression into its query.
"""

from __future__ import annotations

from datetime import date

import polars as pl


# Tuva UB bill-type-code prefix for hospital inpatient facilities.
# (First two digits of bill_type_code encode facility type; 11 = hospital
# inpatient, the Tuva-schema equivalent of CCLF claim type 60.)
HOSPITAL_INPATIENT_BILL_TYPE_PREFIX = "11"

# Legacy constant retained for any code still reading raw CCLF1. The
# HN-criteria path no longer uses it — it now reads gold/medical_claim.
INPATIENT_CLAIM_TYPE_CODE = "60"

# Admission-type code 3 = "Elective" per the CMS claim-type-code codebook.
# All other admission types (1=Emergency, 2=Urgent, 4=Newborn, 5=Trauma,
# 9=Unknown) are treated as unplanned for the High-Needs eligibility
# evaluation. FOG footnote 5 (line 1193) specifies ``is not 3``; any
# other non-elective code — including nulls — counts as unplanned.
ELECTIVE_ADMISSION_TYPE_CODE = "3"


def build_unplanned_admission_filter(
    claim_type_col: str = "claim_type",
    bill_type_col: str = "bill_type_code",
    admission_type_col: str = "admit_type_code",
) -> pl.Expr:
    """
    Build a boolean filter expression that is ``True`` for rows in
    ``gold/medical_claim`` representing unplanned inpatient admissions.

    All columns are cast to ``pl.String`` so numeric inputs classify
    correctly. ``True`` iff:

        - ``claim_type`` is ``"institutional"``, AND
        - ``bill_type_code`` starts with ``"11"`` (hospital inpatient,
          the Tuva equivalent of CCLF claim type 60), AND
        - ``admit_type_code`` is NOT ``"3"`` (non-elective).

    Null admit type counts as unplanned — consistent with the FOG
    footnote 5 "is not 3" phrasing, which does not exclude nulls. In
    Polars ``null != "3"`` evaluates to null (three-valued logic), so
    the filter forces truthy-on-null via ``fill_null`` with a sentinel.
    """
    claim_type_str = pl.col(claim_type_col).cast(pl.String, strict=False)
    bill_type_str = pl.col(bill_type_col).cast(pl.String, strict=False)
    admsn_type_str = pl.col(admission_type_col).cast(pl.String, strict=False)
    not_elective = admsn_type_str.fill_null("__null__") != ELECTIVE_ADMISSION_TYPE_CODE
    return (
        (claim_type_str == "institutional")
        & bill_type_str.str.starts_with(HOSPITAL_INPATIENT_BILL_TYPE_PREFIX)
        & not_elective
    )


def count_unplanned_admissions_in_window(
    medical_claim_lf: pl.LazyFrame,
    *,
    window_begin: date,
    window_end: date,
    mbi_col: str = "person_id",
    admission_date_col: str = "admission_date",
    claim_type_col: str = "claim_type",
    bill_type_col: str = "bill_type_code",
    admission_type_col: str = "admit_type_code",
) -> pl.LazyFrame:
    """
    Filter ``medical_claim_lf`` to the [window_begin, window_end]
    inclusive range on ``admission_date``, apply the
    institutional-inpatient / non-elective filter, deduplicate by
    (person_id, admission_date), and emit one row per person with an
    ``unplanned_admission_count``.

    Reads from ``gold/medical_claim`` (Tuva-normalised, MBI-crosswalked
    via ``person_id``) by default. Using ``person_id`` ensures that a
    bene whose MBI rotated mid-window has their pre- and post-rotation
    admissions counted as one identity, not two.

    Deduplication on admission date handles interim-billed and adjusted
    inpatient claims for the same stay: a beneficiary admitted on
    2024-07-15 with three adjustment claims counts once, not three
    times. A beneficiary with no matching admissions will *not* appear
    in the output — callers that need "zero rows" for the no-admission
    case should left-join against the beneficiary roster.
    """
    admission_date = pl.col(admission_date_col).cast(pl.Date, strict=False)
    return (
        medical_claim_lf.filter(
            build_unplanned_admission_filter(
                claim_type_col=claim_type_col,
                bill_type_col=bill_type_col,
                admission_type_col=admission_type_col,
            )
            & admission_date.is_between(window_begin, window_end, closed="both")
        )
        .unique(subset=[mbi_col, admission_date_col])
        .group_by(mbi_col)
        .agg(pl.len().alias("unplanned_admission_count"))
    )
