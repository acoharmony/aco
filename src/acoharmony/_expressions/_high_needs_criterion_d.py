# © 2025 HarmonyCares
# All rights reserved.

"""
High-Needs eligibility criterion (d): frailty via DME claims.

The binding text, Appendix A Section IV.B.1(d) of the Participation
Agreement ($BRONZE/ACO_REACH_PY2026_AR_PA_2023_Starters_508.txt, line
3782):

    "Exhibit signs of frailty, as evidenced by a claim submitted by a
    provider or supplier for a hospital bed (e.g., specialized
    pressure-reducing mattresses and some bed safety equipment), or
    transfer equipment (e.g., patient lift mechanisms, safety
    equipment, and standing systems) for use in the home. The codes
    that will be considered for purposes of this Section IV.B.1(d)
    will be specified by CMS prior to the start of the relevant
    Performance Year."

The code list comes from Table B.6.2 of the Financial Operating Guide's
code-sheet workbook, materialised at
``silver/reach_appendix_tables_frailty_hcpcs.parquet``. FOG line 1503
confirms the claim-count rule:

    "... one inpatient claim (claim type 60) with a diagnosis from
    B.6.1 will be sufficient for meeting High Needs Population ACO
    eligibility or two claims with a HCPCS code from table B.6.2 with
    different dates of services for any other claim types."

Operational summary for criterion (d):

    - Code list: Table B.6.2 HCPCS codes (frailty).
    - Lookback: Table D 60-month window (per Tables C/D split — criterion
      (d) uses the 5-year window in recognition that DME equipment is
      not replaced annually, per FOG line 1406).
    - Claim source: CCLF1 for institutional claims AND CCLF5 for
      physician/supplier claims. DME claims come through the supplier
      feed, which in our silver layer is CCLF6 (Part B DME). Start with
      whichever table carries a ``hcpcs_code`` column.
    - Threshold: **two claims with different dates of service** per FOG
      line 1503.

Source table
------------

``gold/medical_claim`` (Tuva-normalised, MBI-crosswalked via
``person_id``). The relevant columns:

    - ``person_id``             — crosswalked canonical MBI
    - ``claim_type``             — "institutional" | "professional"
    - ``hcpcs_code``            — HCPCS line code
    - ``claim_line_start_date`` — service date per line

Scope: **any non-inpatient claim** with a B.6.2 HCPCS qualifies, per
FOG line 1503 — "*one inpatient claim (claim type 60) with a diagnosis
from B.6.1 will be sufficient for meeting High Needs Population ACO
eligibility or two claims with a HCPCS code from table B.6.2 with
different dates of services **for any other claim types**.*" Tuva's
``medical_claim`` rolls both CCLF6 DME and CCLF4 physician/supplier
lines into ``claim_type == "professional"`` with no sub-type
discriminator, so the (d) filter is permissive on ``claim_type`` —
what gates a claim is the HCPCS match, not the feed of origin. We
explicitly exclude ``claim_type == "institutional"`` because the FOG
carves out inpatient as belonging to the (a) pathway.
"""

from __future__ import annotations

import polars as pl

from acoharmony._expressions._high_needs_lookback import LookbackWindow


CRITERION_D_MIN_DISTINCT_SERVICE_DATES = 2


def parse_hcpcs_codes_from_table_b62(lf_b62: pl.LazyFrame) -> pl.LazyFrame:
    """
    Normalise the Table B.6.2 silver parquet. The parquet is already
    one-row-per-code; we just strip whitespace from the code field and
    drop the ``x`` trash row.

    Input columns: ``category``, ``hcpcs_code``, ``long_descriptor``.
    Output columns: ``category``, ``hcpcs_code`` (trimmed).
    """
    return (
        lf_b62.filter(pl.col("category") != "x")
        .with_columns(pl.col("hcpcs_code").str.strip_chars().alias("hcpcs_code"))
        .filter(pl.col("hcpcs_code").str.len_chars() > 0)
        .select("category", "hcpcs_code")
    )


def build_criterion_d_qualifying_claims(
    medical_claim_lf: pl.LazyFrame,
    codes_lf: pl.LazyFrame,
    *,
    window: LookbackWindow,
    mbi_col: str = "person_id",
    claim_type_col: str = "claim_type",
    hcpcs_col: str = "hcpcs_code",
    service_date_col: str = "claim_line_start_date",
) -> pl.LazyFrame:
    """
    Find every non-inpatient claim line whose HCPCS is in Table B.6.2
    and whose service date falls inside the Table D 60-month lookback
    window.

    Reads from ``gold/medical_claim``, excluding rows with
    ``claim_type == "institutional"`` (those belong to criterion (a)'s
    inpatient pathway per FOG line 1503). Joins on ``person_id`` so a
    bene whose MBI rotated mid-window has their pre- and post-rotation
    claims attributed to a single identity.

    Returns one row per matching claim line with columns:
        ``person_id`` (via ``mbi_col``), ``service_date``,
        ``hcpcs_code``, ``category``
    """
    codes = codes_lf.select(
        pl.col("hcpcs_code").alias("_match_code"),
        pl.col("category"),
    )
    service_date = pl.col(service_date_col).cast(pl.Date, strict=False)
    hcpcs_str = pl.col(hcpcs_col).cast(pl.String, strict=False).str.strip_chars()
    claim_type_str = pl.col(claim_type_col).cast(pl.String, strict=False)

    return (
        medical_claim_lf.filter(
            (claim_type_str != "institutional")
            & service_date.is_between(window.begin, window.end, closed="both")
        )
        .with_columns(
            service_date.alias("service_date"),
            hcpcs_str.alias("hcpcs_code"),
        )
        .join(codes, left_on="hcpcs_code", right_on="_match_code", how="inner")
        .select(mbi_col, "service_date", "hcpcs_code", "category")
    )


def build_criterion_d_met_expr(
    qualifying_claims_lf: pl.LazyFrame,
    *,
    mbi_col: str = "person_id",
) -> pl.LazyFrame:
    """
    Collapse the qualifying-claim list to one row per MBI with a
    boolean ``criterion_d_met``.

    Criterion (d) is met iff the beneficiary has **two or more claims
    with different dates of service** (per FOG line 1503).
    """
    return (
        qualifying_claims_lf.group_by(mbi_col)
        .agg(
            pl.col("service_date").n_unique().alias("distinct_service_dates"),
            pl.col("service_date").min().alias("first_qualifying_date"),
            pl.col("hcpcs_code").first().alias("first_qualifying_hcpcs"),
            pl.col("category").first().alias("first_qualifying_category"),
        )
        .with_columns(
            (pl.col("distinct_service_dates") >= CRITERION_D_MIN_DISTINCT_SERVICE_DATES).alias(
                "criterion_d_met"
            ),
        )
    )
