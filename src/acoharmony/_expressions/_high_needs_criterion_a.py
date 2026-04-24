# © 2025 HarmonyCares
# All rights reserved.

"""
High-Needs eligibility criterion (a): mobility impairment.

The binding text, Appendix A Section IV.B.1(a) of the Participation
Agreement ($BRONZE/ACO_REACH_PY2026_AR_PA_2023_Starters_508.txt, line
3763):

    "Have one or more developmental or inherited conditions or
    congenital neurological anomalies that impair the Beneficiary's
    mobility or the Beneficiary's neurological condition. Such
    conditions or anomalies could include cerebral palsy, cystic
    fibrosis, muscular dystrophy, metabolic disorders, or any other
    condition as specified by CMS. The codes that will be considered
    for purposes of this Section IV.B.1(a) will be specified by CMS
    prior to the start of the relevant Performance Year."

The operational rule, PY2024 Financial Operating Guide
($BRONZE/REACHPY2024FinancialOperatGde.txt):

    "Have one or more conditions that impair the beneficiary's
    mobility listed in Table B.6.1 (for PY2024)"
        — line 1173

And the FOG's explicit claim-count rule for both (a) and (d) from
line 1503:

    "Per the Chronic Condition Data Warehouse guidelines, one
    inpatient claim (claim type 60) with a diagnosis from B.6.1 will
    be sufficient for meeting High Needs Population ACO eligibility or
    two claims with a HCPCS code from table B.6.2 with different
    dates of services for any other claim types."

Operational summary for criterion (a):

    - Code list: Table B.6.1 ICD-10 codes (mobility impairment),
      loaded from silver/reach_appendix_tables_mobility_impairment_icd10.
    - Claim source: CCLF1 inpatient claims (clm_type_cd == "60").
    - Diagnosis fields on CCLF1: ``prncpl_dgns_cd`` (principal) and
      ``admtg_dgns_cd`` (admitting). A match on either satisfies the
      criterion.
    - Threshold: **one** qualifying inpatient claim in the Table C
      12-month lookback window is sufficient (per the "one inpatient
      claim … will be sufficient" rule above).
    - Claim-date field: ``clm_from_dt`` (admission date on the
      institutional claim).

This module builds Polars expressions only. Callers:

    1. Load and filter the B.6.1 code list from
       silver/reach_appendix_tables_mobility_impairment_icd10.parquet,
       exploding the comma-separated ``icd10_codes`` string into one
       row per code.
    2. Scan silver/cclf1 and restrict to the Table C lookback window.
    3. Compose ``build_criterion_a_expr`` into the join.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from acoharmony._expressions._high_needs_lookback import LookbackWindow
from acoharmony._expressions._hcc_unplanned_admissions import INPATIENT_CLAIM_TYPE_CODE


def _normalize_icd10_code(expr: pl.Expr) -> pl.Expr:
    """Strip whitespace and remove the decimal separator, so codes
    emitted by the B.6.1 table (dotted, e.g. ``G80.0``) join against
    CCLF1 dx columns (dotless, e.g. ``G800``). ICD-10 claim feeds from
    CMS drop the dot before transmission; the B.6.1 workbook keeps it."""
    return expr.str.strip_chars().str.replace_all(".", "", literal=True)


def parse_icd10_codes_from_table_b61(lf_b61: pl.LazyFrame) -> pl.LazyFrame:
    """
    Normalise the Table B.6.1 silver parquet into one row per ICD-10 code.

    Input schema (from ``silver/reach_appendix_tables_mobility_impairment_icd10``):
        category: str
        icd10_codes: str   — comma-separated "G80.0, G80.1, G80.2, ..."

    Output schema:
        category: str
        icd10_code: str    — single code, whitespace-stripped AND dot-stripped
                             so it matches CCLF1's dotless storage format

    Drops the ``(category='x', icd10_codes='x')`` trash row and any
    empty codes that result from trailing commas.
    """
    return (
        lf_b61.filter(pl.col("category") != "x")
        .with_columns(
            pl.col("icd10_codes").str.split(",").alias("_codes_list"),
        )
        .explode("_codes_list")
        .with_columns(
            _normalize_icd10_code(pl.col("_codes_list")).alias("icd10_code"),
        )
        .filter(pl.col("icd10_code").str.len_chars() > 0)
        .select("category", "icd10_code")
    )


def build_criterion_a_qualifying_claims(
    cclf1_lf: pl.LazyFrame,
    codes_lf: pl.LazyFrame,
    *,
    window: LookbackWindow,
    mbi_col: str = "bene_mbi_id",
    claim_type_col: str = "clm_type_cd",
    admission_date_col: str = "clm_from_dt",
    principal_dx_col: str = "prncpl_dgns_cd",
    admitting_dx_col: str = "admtg_dgns_cd",
) -> pl.LazyFrame:
    """
    Find every inpatient claim in the Table C lookback window whose
    principal or admitting diagnosis is in the Table B.6.1 code list.

    Returns one row per qualifying claim with columns:
        mbi_col, admission_date, diagnosis_code, category

    Callers aggregate this to per-MBI results via
    ``build_criterion_a_met_expr``. Separating the two steps keeps the
    intermediate claim list available for the reconciliation transform.
    """
    codes = codes_lf.select(
        pl.col("icd10_code").alias("_match_code"),
        pl.col("category"),
    )

    admission_date = pl.col(admission_date_col).cast(pl.Date, strict=False)
    claim_type_str = pl.col(claim_type_col).cast(pl.String, strict=False)

    inpatient_in_window = cclf1_lf.filter(
        (claim_type_str == INPATIENT_CLAIM_TYPE_CODE)
        & admission_date.is_between(window.begin, window.end, closed="both")
    ).select(
        pl.col(mbi_col),
        admission_date.alias("admission_date"),
        # Cast dx columns to String explicitly so a frame where every
        # row has a null principal or admitting dx (pl.Null inferred
        # dtype) still joins against the String-typed codes frame.
        # Also normalize dots so a dotted-format feed (defensive) joins
        # cleanly; the B.6.1 codes side is already dot-stripped.
        _normalize_icd10_code(
            pl.col(principal_dx_col).cast(pl.String, strict=False)
        ).alias("_principal_dx"),
        _normalize_icd10_code(
            pl.col(admitting_dx_col).cast(pl.String, strict=False)
        ).alias("_admitting_dx"),
    )

    # A claim qualifies if EITHER the principal OR the admitting
    # diagnosis matches a B.6.1 code. Join on each separately and
    # union — simpler than a CASE join and preserves which field
    # matched.
    via_principal = inpatient_in_window.join(
        codes,
        left_on="_principal_dx",
        right_on="_match_code",
        how="inner",
    ).with_columns(pl.col("_principal_dx").alias("diagnosis_code"))

    via_admitting = inpatient_in_window.join(
        codes,
        left_on="_admitting_dx",
        right_on="_match_code",
        how="inner",
    ).with_columns(pl.col("_admitting_dx").alias("diagnosis_code"))

    return pl.concat([via_principal, via_admitting], how="vertical_relaxed").select(
        mbi_col, "admission_date", "diagnosis_code", "category",
    )


def build_criterion_a_met_expr(
    qualifying_claims_lf: pl.LazyFrame,
    *,
    mbi_col: str = "bene_mbi_id",
) -> pl.LazyFrame:
    """
    Collapse the qualifying-claims list to one row per MBI with a boolean
    ``criterion_a_met``.

    Criterion (a) is met when the beneficiary has **one or more**
    qualifying claims — per the FOG line 1503 "one inpatient claim …
    will be sufficient" rule.
    """
    return (
        qualifying_claims_lf.group_by(mbi_col)
        .agg(
            pl.len().alias("qualifying_claim_count"),
            pl.col("admission_date").min().alias("first_qualifying_date"),
            pl.col("diagnosis_code").first().alias("first_qualifying_diagnosis"),
            pl.col("category").first().alias("first_qualifying_category"),
        )
        .with_columns(
            (pl.col("qualifying_claim_count") >= 1).alias("criterion_a_met"),
        )
    )
