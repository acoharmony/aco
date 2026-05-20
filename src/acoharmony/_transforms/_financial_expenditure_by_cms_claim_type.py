# © 2025 HarmonyCares
# All rights reserved.

"""
Financial expenditure by CMS claim type — final gold transform (PR B3 of 3).

Joins the three pieces built in earlier reconciliation PRs into a single
tidy gold frame keyed by
``(aco_id, program, performance_year, year_month, clm_type_cd)``:

    total_spend   ← build_cclf_mer_spend (PR B1)         numerator
    member_months ← build_member_months_asof (PR B2)     denominator
    pbpm          = total_spend / member_months          this module

Per-bene spend from CCLF is attributed to an ACO via a point-in-time
lookup against the same BAR/ALR historical deliveries used by PR B2's
aggregation. The attribution logic lives here in
``build_bene_attribution_asof`` — it is the per-bene variant of PR B2's
per-aggregate count. Both functions must produce results consistent with
each other (if PR B2 says ACO D0259 had 100 bene-months in 202403, then
``build_bene_attribution_asof`` must emit exactly 100 bene_mbi rows for
D0259/202403). The shared helpers ``_with_delivery_metadata``,
``_latest_delivery_per_aco_py``, ``_month_ends``, and
``AlignmentDeliveryExpression.bar_active_at_month_end_filter`` are
re-used from PR B2 to guarantee that consistency.

Point-in-time
-------------
The same ``as_of_cutoff`` is threaded through every underlying call:
CCLF spend filtering, BAR/ALR latest-delivery selection, and month-grid
capping. A historical reconciliation against a MER delivery from
2025-11-30 will never see CCLF claims, BAR rows, or ALR rows whose
file_date is after 2025-11-30.

Unattributed claims
-------------------
Benes with CCLF claims but no BAR/ALR alignment at the cutoff are
DROPPED from the output, not silently attributed to some placeholder
ACO. This is correct behavior: an unaligned bene's claims belong to
the broader FFS pool, not to any ACO reconciliation. If every CCLF bene
is unaligned (e.g. the cutoff predates the alignment files), the
transform returns an empty but correctly-schemaed frame.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from .._expressions._alignment_delivery import AlignmentDeliveryExpression
from ._cclf_mer_spend import build_cclf_mer_spend
from ._member_months_asof import (
    _latest_delivery_per_aco_py,
    _month_ends,
    _with_delivery_metadata,
    _year_month_int,
    build_member_months_asof,
)

# Output schema for attribution map (per-bene-month rows).
_ATTRIBUTION_SCHEMA = {
    "bene_mbi": pl.Utf8,
    "aco_id": pl.Utf8,
    "program": pl.Utf8,
    "performance_year": pl.Int32,
    "year_month": pl.Int64,
}


def _reach_attribution_for_group(
    group_rows: pl.DataFrame, cutoff: date
) -> pl.DataFrame:
    """Emit per-bene-month REACH attribution rows for one (aco_id, PY) group.

    Mirrors ``_reach_member_months_for_group`` in PR B2 but keeps the
    individual bene_mbi rows rather than collapsing to a count.
    """
    aco_id = group_rows["aco_id"][0]
    performance_year = int(group_rows["performance_year"][0])

    frames: list[pl.DataFrame] = []
    for month_end in _month_ends(performance_year, cutoff):
        active = group_rows.filter(
            AlignmentDeliveryExpression.bar_active_at_month_end_filter(month_end)
        )
        if active.height == 0:
            continue
        frames.append(
            active.select("bene_mbi")
            .unique()
            .with_columns(
                pl.lit(aco_id).alias("aco_id"),
                pl.lit("REACH").alias("program"),
                pl.lit(performance_year, dtype=pl.Int32).alias("performance_year"),
                pl.lit(_year_month_int(month_end), dtype=pl.Int64).alias("year_month"),
            )
        )
    if not frames:
        return pl.DataFrame(schema=_ATTRIBUTION_SCHEMA)
    return pl.concat(frames, how="vertical")


def _mssp_attribution_for_group(
    group_rows: pl.DataFrame, cutoff: date
) -> pl.DataFrame:
    """Emit per-bene-month MSSP attribution rows for one (aco_id, PY) group.

    ALR is roster-style: every distinct bene in the latest delivery is
    attributed to every month in the PY up through the cutoff.
    """
    aco_id = group_rows["aco_id"][0]
    performance_year = int(group_rows["performance_year"][0])
    benes = group_rows.select("bene_mbi").unique()

    frames: list[pl.DataFrame] = []
    for month_end in _month_ends(performance_year, cutoff):
        frames.append(
            benes.with_columns(
                pl.lit(aco_id).alias("aco_id"),
                pl.lit("MSSP").alias("program"),
                pl.lit(performance_year, dtype=pl.Int32).alias("performance_year"),
                pl.lit(_year_month_int(month_end), dtype=pl.Int64).alias("year_month"),
            )
        )
    if not frames:
        return pl.DataFrame(schema=_ATTRIBUTION_SCHEMA)
    return pl.concat(frames, how="vertical")


def build_bene_attribution_asof(
    bar: pl.LazyFrame,
    alr: pl.LazyFrame,
    as_of_cutoff: str,
) -> pl.LazyFrame:
    """
    Build a per-bene-month attribution map from BAR and ALR historical deliveries.

    Returns a LazyFrame with one row per ``(bene_mbi, aco_id, program,
    performance_year, year_month)`` tuple. The same point-in-time rules as
    ``build_member_months_asof`` apply: for each (aco_id, performance_year)
    we consult the single latest delivery with ``file_date <= cutoff``.

    This is the per-bene counterpart to PR B2's aggregated member-months.
    The two functions are guaranteed consistent: summing distinct bene_mbi
    counts over this frame's ``(aco_id, program, performance_year,
    year_month)`` groups reproduces PR B2's ``member_months`` column.
    """
    cutoff = date.fromisoformat(as_of_cutoff)

    bar_with_meta = _with_delivery_metadata(bar)
    bar_latest = _latest_delivery_per_aco_py(bar_with_meta, cutoff).collect()

    reach_rows: list[pl.DataFrame] = []
    if bar_latest.height > 0:
        for (_aco_id, _py), group in bar_latest.group_by(
            ["aco_id", "performance_year"]
        ):
            reach_rows.append(_reach_attribution_for_group(group, cutoff))

    alr_with_meta = _with_delivery_metadata(alr)
    alr_latest = _latest_delivery_per_aco_py(alr_with_meta, cutoff).collect()

    mssp_rows: list[pl.DataFrame] = []
    if alr_latest.height > 0:
        for (_aco_id, _py), group in alr_latest.group_by(
            ["aco_id", "performance_year"]
        ):
            mssp_rows.append(_mssp_attribution_for_group(group, cutoff))

    all_rows = reach_rows + mssp_rows
    if not all_rows:
        return pl.DataFrame(schema=_ATTRIBUTION_SCHEMA).lazy()
    return pl.concat(all_rows, how="vertical").lazy()


def build_financial_expenditure_by_cms_claim_type(
    cclf1: pl.LazyFrame,
    cclf5: pl.LazyFrame,
    cclf6: pl.LazyFrame,
    bar: pl.LazyFrame,
    alr: pl.LazyFrame,
    as_of_cutoff: str,
) -> pl.LazyFrame:
    """
    Build the ``financial_expenditure_by_cms_claim_type`` gold table.

    This is the CCLF-side counterpart to the parsed MER ``data_claims``
    sheet. The output grain and semantics exactly match the MER so a
    straight inner-join on ``(aco_id, program, performance_year,
    year_month, clm_type_cd)`` against ``build_mer_reconciliation_view``
    (PR A) gives the 1:1 reconciliation tie-out that PR C validates.

    Args:
        cclf1: CCLF1 header-level LazyFrame.
        cclf5: CCLF5 line-level LazyFrame (Part B professional).
        cclf6: CCLF6 line-level LazyFrame (Part B DMERC).
        bar: BAR silver LazyFrame (REACH alignment history).
        alr: ALR silver LazyFrame (MSSP alignment history).
        as_of_cutoff: ISO date string. Applied consistently to all inputs
            so a historical reconciliation never sees deliveries that did
            not exist at the cutoff.

    Returns:
        LazyFrame with columns
        ``(aco_id, program, performance_year, year_month, clm_type_cd,
        total_spend, member_months, pbpm)``.

        Benes with CCLF claims but no alignment at the cutoff are dropped.
        Buckets with zero member-months have null PBPM (never division by
        zero), matching the MER view's behavior.
    """
    # --- 1. Per-bene spend from CCLF (grain: bene_mbi_id, year_month, clm_type_cd)
    spend = build_cclf_mer_spend(cclf1, cclf5, cclf6, as_of_cutoff=as_of_cutoff)

    # --- 2. Per-bene-month attribution from BAR/ALR
    attribution = build_bene_attribution_asof(bar, alr, as_of_cutoff=as_of_cutoff)

    # --- 3. Inner-join spend to attribution on (bene, year_month)
    # The bene column names differ between sources: CCLF uses bene_mbi_id,
    # BAR/ALR use bene_mbi. Rename once so the join is symmetric.
    attributed_spend = spend.join(
        attribution.rename({"bene_mbi": "bene_mbi_id"}),
        on=["bene_mbi_id", "year_month"],
        how="inner",
    )

    # --- 4. Sum spend per (aco_id, program, performance_year, year_month, clm_type_cd)
    agg_spend = (
        attributed_spend.group_by(
            "aco_id",
            "program",
            "performance_year",
            "year_month",
            "clm_type_cd",
        )
        .agg(pl.col("total_spend").sum().alias("total_spend"))
    )

    # --- 5. Left-join to member-months on (aco_id, program, performance_year, year_month)
    member_months = build_member_months_asof(bar, alr, as_of_cutoff=as_of_cutoff)
    joined = agg_spend.join(
        member_months,
        on=["aco_id", "program", "performance_year", "year_month"],
        how="left",
    )

    # --- 6. Compute PBPM (null when denominator is zero, mirroring MER view)
    return joined.with_columns(
        pl.when(pl.col("member_months") > 0)
        .then(pl.col("total_spend").cast(pl.Float64) / pl.col("member_months").cast(pl.Float64))
        .otherwise(None)
        .alias("pbpm")
    ).select(
        "aco_id",
        "program",
        "performance_year",
        "year_month",
        "clm_type_cd",
        "total_spend",
        "member_months",
        "pbpm",
    )
