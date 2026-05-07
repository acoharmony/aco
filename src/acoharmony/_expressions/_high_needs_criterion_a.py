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

And the FOG's claim-count rule, line 1503:

    "Per the Chronic Condition Data Warehouse guidelines, one
    inpatient claim (claim type 60) with a diagnosis from B.6.1 will
    be sufficient for meeting High Needs Population ACO eligibility or
    two claims with a HCPCS code from table B.6.2 with different
    dates of services for any other claim types."

The second clause garbles the criteria — it appears in the B.6.1
paragraph but references the B.6.2 HCPCS table — which is a CMS copy
edit error: it is the standard CCW Other-Chronic / Potentially-Disabling
condition rule applied to whichever qualifying code list the paragraph
introduces. So for criterion (a) the rule is:

    one inpatient claim with a B.6.1 dx
        OR
    two non-inpatient claims with a B.6.1 dx on different dates of service

within the Table C 12-month lookback. We previously implemented only
the inpatient branch; per BAR reconciliation the non-inpatient branch
accounts for the majority of CMS-flagged HN benes who have B.6.1
diagnoses on professional/outpatient claims rather than inpatient
admissions.

Operational summary
-------------------

    - Code list: Table B.6.1 ICD-10 codes (mobility impairment), loaded
      from silver/reach_appendix_tables_mobility_impairment_icd10.
    - Claim source: ``gold/medical_claim`` (Tuva-normalised, MBI-
      crosswalked via ``person_id``).
    - Diagnosis scan: **all 25 diagnosis positions** on the claim
      (``diagnosis_code_1`` through ``diagnosis_code_25``). A match in
      ANY position qualifies the claim. The FOG's "one inpatient
      claim … with a diagnosis from B.6.1" rule does not restrict to
      principal/admitting dx, so a secondary-dx match counts.
    - Inpatient branch: ``claim_type == "institutional"`` AND
      ``bill_type_code`` starts with ``"11"`` (UB bill-type facility
      code 11 = hospital inpatient, the Tuva equivalent of CCLF
      claim type 60). Date column: ``admission_date``. Threshold: ≥ 1.
    - Non-inpatient branch: any other ``claim_type`` / ``bill_type_code``
      combination (professional, institutional outpatient/SNF/HH, etc.).
      Date column: ``claim_start_date`` (``admission_date`` is null on
      professional rows in Tuva's schema). Threshold: ≥ 2 distinct
      service dates.
    - Window: Table C 12-month lookback for both branches.

MBI crosswalk note
------------------

``gold/medical_claim`` carries ``person_id`` as the crosswalked
canonical MBI — every historical MBI in an ``identity_timeline`` chain
resolves to the chain's ``hop_index=0`` current MBI before rows land
in medical_claim. Callers should join on ``person_id`` so a bene whose
MBI rotated mid-window doesn't lose their pre-rotation matching claim.
"""

from __future__ import annotations

import polars as pl

from acoharmony._expressions._high_needs_lookback import LookbackWindow


# Tuva UB bill-type-code prefix for hospital inpatient facilities.
# (First two digits of bill_type_code encode facility type; 11 = hospital
# inpatient, the equivalent of CCLF claim type 60.)
HOSPITAL_INPATIENT_BILL_TYPE_PREFIX = "11"

# Distinct-date threshold for the non-inpatient branch, per FOG line
# 1503's "two claims … with different dates of services for any other
# claim types".
CRITERION_A_MIN_NON_INPATIENT_DISTINCT_DATES = 2

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
    claim_start_date_col: str = "claim_start_date",
    diagnosis_columns: tuple[str, ...] = DIAGNOSIS_CODE_COLUMNS,
) -> pl.LazyFrame:
    """
    Find every claim in the Table C lookback window with a B.6.1 dx in
    any of the 25 positions, tagged as inpatient or non-inpatient so
    the per-bene aggregator can apply the FOG line 1503 OR-of-branches
    rule.

    Inpatient rows are matched on ``admission_date``; non-inpatient
    rows on ``claim_start_date`` (``admission_date`` is null on Tuva
    professional rows). The ``service_date`` column emitted here is
    ``admission_date`` for inpatient rows and ``claim_start_date``
    otherwise — a unified column the aggregator counts distinct values
    over.

    Returns one row per (person_id, service_date, matching_diagnosis_code)
    with columns:

        ``person_id`` (via ``mbi_col``), ``service_date``,
        ``is_inpatient``, ``diagnosis_code``, ``category``
    """
    codes = codes_lf.select(
        pl.col("icd10_code").alias("_match_code"),
        pl.col("category"),
    )

    claim_type_str = pl.col(claim_type_col).cast(pl.String, strict=False)
    bill_type_str = pl.col(bill_type_col).cast(pl.String, strict=False)
    admission_date = pl.col(admission_date_col).cast(pl.Date, strict=False)
    claim_start_date = pl.col(claim_start_date_col).cast(pl.Date, strict=False)

    is_inpatient_expr = (
        (claim_type_str == "institutional")
        & bill_type_str.str.starts_with(HOSPITAL_INPATIENT_BILL_TYPE_PREFIX)
    )

    # service_date: admission_date when the row is inpatient, else
    # claim_start_date. Coalesce isn't quite right here because
    # admission_date can be populated on non-inpatient institutional
    # rows (e.g. SNF stays); we want to deliberately key on the
    # branch's natural date column so the count-distinct semantics
    # stay aligned with the FOG rule.
    service_date_expr = (
        pl.when(is_inpatient_expr)
        .then(admission_date)
        .otherwise(claim_start_date)
        .alias("service_date")
    )

    in_window = medical_claim_lf.with_columns(
        is_inpatient_expr.alias("is_inpatient"),
        service_date_expr,
    ).filter(
        pl.col("service_date").is_between(window.begin, window.end, closed="both")
    ).select(
        pl.col(mbi_col),
        pl.col("is_inpatient"),
        pl.col("service_date"),
        *[
            _normalize_icd10_code(
                pl.col(col).cast(pl.String, strict=False)
            ).alias(col)
            for col in diagnosis_columns
        ],
    )

    long = in_window.unpivot(
        index=[mbi_col, "is_inpatient", "service_date"],
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
    ).select(
        mbi_col, "is_inpatient", "service_date", "diagnosis_code", "category"
    )


def build_criterion_a_met_expr(
    qualifying_claims_lf: pl.LazyFrame,
    *,
    mbi_col: str = "person_id",
) -> pl.LazyFrame:
    """
    Collapse the qualifying-claims list to one row per person with a
    boolean ``criterion_a_met``.

    Per FOG line 1503, criterion (a) is met when **either**:

        - the bene has ≥ 1 inpatient claim with a B.6.1 dx, **or**
        - the bene has ≥ 2 non-inpatient claims with a B.6.1 dx on
          distinct service dates.

    Output columns (one row per ``mbi_col``):

        ``mbi_col``                            str
        inpatient_qualifying_claim_count       u32
        non_inpatient_distinct_service_dates   u32
        criterion_a_inpatient_branch_met       bool
        criterion_a_non_inpatient_branch_met   bool
        criterion_a_met                        bool
        first_qualifying_date                  date — earliest service_date
                                                       across both branches
        first_qualifying_diagnosis             str
        first_qualifying_category              str
        first_qualifying_branch                str — "inpatient" | "non_inpatient"
    """
    is_inpatient = pl.col("is_inpatient").fill_null(False)
    return (
        qualifying_claims_lf
        .group_by(mbi_col)
        .agg(
            is_inpatient.sum().alias("inpatient_qualifying_claim_count"),
            pl.col("service_date").filter(~is_inpatient).n_unique().alias(
                "non_inpatient_distinct_service_dates"
            ),
            pl.col("service_date").min().alias("first_qualifying_date"),
            pl.col("diagnosis_code").first().alias("first_qualifying_diagnosis"),
            pl.col("category").first().alias("first_qualifying_category"),
            pl.when(is_inpatient).then(pl.lit("inpatient"))
            .otherwise(pl.lit("non_inpatient"))
            .first()
            .alias("first_qualifying_branch"),
        )
        .with_columns(
            (pl.col("inpatient_qualifying_claim_count") >= 1).alias(
                "criterion_a_inpatient_branch_met"
            ),
            (
                pl.col("non_inpatient_distinct_service_dates")
                >= CRITERION_A_MIN_NON_INPATIENT_DISTINCT_DATES
            ).alias("criterion_a_non_inpatient_branch_met"),
        )
        .with_columns(
            (
                pl.col("criterion_a_inpatient_branch_met")
                | pl.col("criterion_a_non_inpatient_branch_met")
            ).alias("criterion_a_met"),
        )
    )
