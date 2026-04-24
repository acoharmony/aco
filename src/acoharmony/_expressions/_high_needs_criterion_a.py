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
    - Claim source: ``gold/medical_claim`` filtered to institutional
      inpatient rows — ``claim_type == "institutional"`` AND
      ``bill_type_code`` starts with ``"11"`` (UB bill-type facility
      code 11 = hospital inpatient, the Tuva-schema equivalent of CCLF
      claim type 60).
    - Diagnosis scan: **all 25 diagnosis positions** on the claim
      (``diagnosis_code_1`` through ``diagnosis_code_25``). A match in
      ANY position satisfies the criterion. The FOG's "one inpatient
      claim … with a diagnosis from B.6.1" rule does not restrict to
      principal/admitting dx, so a secondary dx match qualifies the
      claim. (Earlier CCLF1 implementation checked only principal and
      admitting — this gold/medical_claim rewrite broadens to the full
      dx set per the FOG text.)
    - Threshold: **one** qualifying inpatient claim in the Table C
      12-month lookback window is sufficient (per the "one inpatient
      claim … will be sufficient" rule above).
    - Claim-date field: ``admission_date`` (Tuva-normalised admission
      date, fully populated on institutional inpatient rows).

MBI crosswalk note
------------------

``gold/medical_claim`` carries ``person_id`` as the crosswalked
canonical MBI — every historical MBI in an ``identity_timeline`` chain
resolves to the chain's ``hop_index=0`` current MBI before rows land
in medical_claim. Callers should join on ``person_id`` so a bene whose
MBI rotated mid-window doesn't lose their pre-rotation inpatient
admission.

This module builds Polars expressions only. Callers:

    1. Load and filter the B.6.1 code list from
       silver/reach_appendix_tables_mobility_impairment_icd10.parquet,
       exploding the comma-separated ``icd10_codes`` string into one
       row per code.
    2. Scan gold/medical_claim and restrict to the Table C lookback
       window on ``admission_date``.
    3. Compose ``build_criterion_a_qualifying_claims`` into the filter.
"""

from __future__ import annotations

import polars as pl

from acoharmony._expressions._high_needs_lookback import LookbackWindow


# Tuva UB bill-type-code prefix for hospital inpatient facilities.
# (First two digits of bill_type_code encode facility type; 11 = hospital
# inpatient, the equivalent of CCLF claim type 60.)
HOSPITAL_INPATIENT_BILL_TYPE_PREFIX = "11"

# Number of diagnosis positions on medical_claim. Tuva's normalized
# schema exposes diagnosis_code_1 through diagnosis_code_25 (principal
# at position 1, secondary/admitting/other at 2-25). CMS's FOG rule
# counts a claim match on any position.
_DIAGNOSIS_POSITIONS = tuple(range(1, 26))
DIAGNOSIS_CODE_COLUMNS = tuple(f"diagnosis_code_{i}" for i in _DIAGNOSIS_POSITIONS)


def _normalize_icd10_code(expr: pl.Expr) -> pl.Expr:
    """Strip whitespace and remove the decimal separator, so codes
    emitted by the B.6.1 table (dotted, e.g. ``G80.0``) join against
    medical_claim dx columns (dotless, e.g. ``G800``). ICD-10 claim
    feeds from CMS drop the dot before transmission; the B.6.1 workbook
    keeps it."""
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
                             so it matches medical_claim's dotless storage
                             format

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
    medical_claim_lf: pl.LazyFrame,
    codes_lf: pl.LazyFrame,
    *,
    window: LookbackWindow,
    mbi_col: str = "person_id",
    claim_type_col: str = "claim_type",
    bill_type_col: str = "bill_type_code",
    admission_date_col: str = "admission_date",
    diagnosis_columns: tuple[str, ...] = DIAGNOSIS_CODE_COLUMNS,
) -> pl.LazyFrame:
    """
    Find every institutional-inpatient claim in the Table C lookback
    window whose diagnosis in any of the 25 positions matches a
    B.6.1 code.

    Reads from ``gold/medical_claim`` (Tuva-normalised, MBI-crosswalked
    via ``person_id``). Filters to institutional inpatient using
    ``claim_type == "institutional"`` AND ``bill_type_code`` starting
    with ``"11"`` — the Tuva equivalent of CCLF inpatient claim type 60.

    Returns one row per (person_id, admission_date,
    matching_diagnosis_code) with columns:

        ``person_id`` (via ``mbi_col``), ``admission_date``,
        ``diagnosis_code``, ``category``

    Callers aggregate this to per-person results via
    ``build_criterion_a_met_expr``.
    """
    codes = codes_lf.select(
        pl.col("icd10_code").alias("_match_code"),
        pl.col("category"),
    )

    admission_date = pl.col(admission_date_col).cast(pl.Date, strict=False)
    claim_type_str = pl.col(claim_type_col).cast(pl.String, strict=False)
    bill_type_str = pl.col(bill_type_col).cast(pl.String, strict=False)

    inpatient_in_window = medical_claim_lf.filter(
        (claim_type_str == "institutional")
        & bill_type_str.str.starts_with(HOSPITAL_INPATIENT_BILL_TYPE_PREFIX)
        & admission_date.is_between(window.begin, window.end, closed="both")
    ).select(
        pl.col(mbi_col),
        admission_date.alias("admission_date"),
        *[
            _normalize_icd10_code(
                pl.col(col).cast(pl.String, strict=False)
            ).alias(col)
            for col in diagnosis_columns
        ],
    )

    # Melt the 25 dx columns into a long form (person, admission_date,
    # dx_code), inner-join against the B.6.1 code list. A claim can
    # match in multiple positions (e.g. same ICD-10 code appears
    # principal AND secondary on a resubmission); dedupe at the caller
    # level via ``group_by`` on the aggregation.
    long = inpatient_in_window.unpivot(
        index=[mbi_col, "admission_date"],
        on=list(diagnosis_columns),
        variable_name="_dx_position",
        value_name="diagnosis_code",
    ).filter(pl.col("diagnosis_code").is_not_null()).filter(
        pl.col("diagnosis_code").str.len_chars() > 0
    )

    return long.join(
        codes,
        left_on="diagnosis_code",
        right_on="_match_code",
        how="inner",
    ).select(mbi_col, "admission_date", "diagnosis_code", "category")


def build_criterion_a_met_expr(
    qualifying_claims_lf: pl.LazyFrame,
    *,
    mbi_col: str = "person_id",
) -> pl.LazyFrame:
    """
    Collapse the qualifying-claims list to one row per person with a
    boolean ``criterion_a_met``.

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
