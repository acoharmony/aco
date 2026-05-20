# © 2025 HarmonyCares
# All rights reserved.

"""
Point-in-time member-months transform for MER reconciliation (PR B2 of 3).

Produces a long-format member-months frame keyed by
``(aco_id, program, performance_year, year_month)`` from the silver BAR
(REACH) and ALR (MSSP) tables. Correct as-of a given MER delivery cutoff:
only alignment deliveries with ``file_date <= cutoff`` are consulted, and
for each ``(aco_id, performance_year)`` we pick the single latest delivery
within that window. This matches CMS's "what did the attribution look like
when MER X was issued" semantics and prevents retroactive churn that
arrived in a later delivery from leaking into a historical tie-out.

REACH (BAR) semantics
---------------------
BAR rows carry ``start_date`` and ``end_date`` per beneficiary per
delivery. We expand each delivery's view into a month-end grid for the
performance year and filter by the bar_active_at_month_end rule
(start ≤ month_end AND (end IS NULL OR end > month_end)).

MSSP (ALR) semantics
--------------------
ALR is a roster list — one row per bene in the current attribution list.
No explicit start/end window. A bene appearing in the latest delivery
contributes 1 member-month for every month in the performance year up to
the cutoff month.

Output
------
Emits one row per ``(aco_id, program, performance_year, year_month,
member_months)`` tuple. Months with zero member-months are not emitted
(you would never join them to a spend frame anyway). ``member_months``
is the distinct bene_mbi count — no double counting even if a bene
appears in multiple deliveries at the same cutoff (the latest-delivery
selection step collapses those).
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from .._expressions._alignment_delivery import AlignmentDeliveryExpression
from .._parsers._aco_id import extract_aco_id


def _month_ends(performance_year: int, cutoff: date) -> list[date]:
    """Generate the month-end dates for a PY up to the cutoff.

    For PY 2024 and cutoff 2024-06-15, returns [Jan 31, Feb 29, Mar 31,
    Apr 30, May 31] — June is not yet complete at the cutoff, so it is
    excluded. For cutoff 2024-06-30, June is included.

    The MER's "Claims Incurred Through" field marks the end of the last
    COMPLETED month; anything partial is not reconciled.
    """
    result: list[date] = []
    for month in range(1, 13):
        # First-of-next-month minus one day = last day of this month
        if month == 12:
            next_first = date(performance_year + 1, 1, 1)
        else:
            next_first = date(performance_year, month + 1, 1)
        month_end = next_first - timedelta(days=1)
        if month_end <= cutoff:
            result.append(month_end)
    return result


def _year_month_int(month_end: date) -> int:
    """Convert month-end date to YYYYMM integer matching the spend frame grain."""
    return month_end.year * 100 + month_end.month


def _with_delivery_metadata(frame: pl.LazyFrame) -> pl.LazyFrame:
    """Add ``aco_id``, ``performance_year``, and ``file_date_parsed`` columns.

    Uses the existing ``extract_aco_id`` helper (via map_elements, since
    it is a regex Python function not a polars expression) for the ACO ID,
    and the new ``AlignmentDeliveryExpression.extract_performance_year_from_filename``
    for the PY. ``file_date`` is parsed from the string column carried on
    every silver table.
    """
    return frame.with_columns(
        pl.col("source_filename")
        .map_elements(extract_aco_id, return_dtype=pl.Utf8)
        .alias("aco_id"),
        AlignmentDeliveryExpression.extract_performance_year_from_filename(),
        pl.col("file_date")
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .alias("file_date_parsed"),
    )


def _latest_delivery_per_aco_py(
    frame: pl.LazyFrame, cutoff: date
) -> pl.LazyFrame:
    """Keep only rows from the single latest delivery ≤ cutoff per (aco_id, PY).

    Drops any row whose delivery lacks a parseable aco_id, performance_year,
    or file_date (these can never participate in a reconciled bucket).
    """
    cutoff_lit = pl.lit(cutoff)
    filtered = frame.filter(
        pl.col("aco_id").is_not_null()
        & pl.col("performance_year").is_not_null()
        & pl.col("file_date_parsed").is_not_null()
        & (pl.col("file_date_parsed") <= cutoff_lit)
    )
    # For each (aco_id, performance_year), keep rows from the delivery whose
    # file_date_parsed equals the per-group max.
    max_per_group = (
        filtered.group_by("aco_id", "performance_year")
        .agg(pl.col("file_date_parsed").max().alias("_max_file_date"))
    )
    return filtered.join(
        max_per_group, on=["aco_id", "performance_year"], how="inner"
    ).filter(pl.col("file_date_parsed") == pl.col("_max_file_date")).drop("_max_file_date")


def _reach_member_months_for_group(
    group_rows: pl.DataFrame, cutoff: date
) -> pl.DataFrame:
    """Compute REACH member-months for ONE (aco_id, performance_year) group.

    Iterates the month-end grid for the PY up through the cutoff, applies
    the bar_active_at_month_end filter, and counts distinct bene_mbi.
    Returns a DataFrame with columns
    (aco_id, program, performance_year, year_month, member_months).
    """
    aco_id = group_rows["aco_id"][0]
    performance_year = int(group_rows["performance_year"][0])

    out_rows: list[dict] = []
    for month_end in _month_ends(performance_year, cutoff):
        active = group_rows.filter(
            AlignmentDeliveryExpression.bar_active_at_month_end_filter(month_end)
        )
        n = active["bene_mbi"].n_unique()
        if n > 0:
            out_rows.append(
                {
                    "aco_id": aco_id,
                    "program": "REACH",
                    "performance_year": performance_year,
                    "year_month": _year_month_int(month_end),
                    "member_months": n,
                }
            )
    return pl.DataFrame(
        out_rows,
        schema={
            "aco_id": pl.Utf8,
            "program": pl.Utf8,
            "performance_year": pl.Int32,
            "year_month": pl.Int64,
            "member_months": pl.Int64,
        },
    )


def _mssp_member_months_for_group(
    group_rows: pl.DataFrame, cutoff: date
) -> pl.DataFrame:
    """Compute MSSP member-months for ONE (aco_id, performance_year) group.

    ALR is roster-style: distinct bene_mbi in the group ≙ the member count,
    applied to every month in the PY up through the cutoff.
    """
    aco_id = group_rows["aco_id"][0]
    performance_year = int(group_rows["performance_year"][0])
    n = group_rows["bene_mbi"].n_unique()

    out_rows: list[dict] = []
    for month_end in _month_ends(performance_year, cutoff):
        out_rows.append(
            {
                "aco_id": aco_id,
                "program": "MSSP",
                "performance_year": performance_year,
                "year_month": _year_month_int(month_end),
                "member_months": n,
            }
        )
    return pl.DataFrame(
        out_rows,
        schema={
            "aco_id": pl.Utf8,
            "program": pl.Utf8,
            "performance_year": pl.Int32,
            "year_month": pl.Int64,
            "member_months": pl.Int64,
        },
    )


def build_member_months_asof(
    bar: pl.LazyFrame,
    alr: pl.LazyFrame,
    as_of_cutoff: str,
) -> pl.LazyFrame:
    """
    Build point-in-time member-months from BAR (REACH) and ALR (MSSP).

    Args:
        bar: LazyFrame sourced from silver ``bar``. Must contain
            ``bene_mbi``, ``start_date``, ``end_date``, ``source_filename``,
            ``file_date``.
        alr: LazyFrame sourced from silver ``alr``. Must contain
            ``bene_mbi``, ``source_filename``, ``file_date``.
        as_of_cutoff: ISO date string (``'YYYY-MM-DD'``). All delivery
            file_dates after this cutoff are ignored. Required — there
            is no "latest" default because point-in-time reconciliation
            must be explicit about its as-of date.

    Returns:
        LazyFrame with columns
        ``(aco_id, program, performance_year, year_month, member_months)``.
        Months with zero member counts are not emitted.
    """
    cutoff = date.fromisoformat(as_of_cutoff)

    # --- REACH side ---
    bar_with_meta = _with_delivery_metadata(bar)
    bar_latest = _latest_delivery_per_aco_py(bar_with_meta, cutoff).collect()

    reach_rows: list[pl.DataFrame] = []
    if bar_latest.height > 0:
        for (aco_id, py), group in bar_latest.group_by(
            ["aco_id", "performance_year"]
        ):
            reach_rows.append(_reach_member_months_for_group(group, cutoff))

    # --- MSSP side ---
    alr_with_meta = _with_delivery_metadata(alr)
    alr_latest = _latest_delivery_per_aco_py(alr_with_meta, cutoff).collect()

    mssp_rows: list[pl.DataFrame] = []
    if alr_latest.height > 0:
        for (aco_id, py), group in alr_latest.group_by(
            ["aco_id", "performance_year"]
        ):
            mssp_rows.append(_mssp_member_months_for_group(group, cutoff))

    all_rows = reach_rows + mssp_rows
    if not all_rows:
        # Return an empty but correctly-schemaed LazyFrame.
        return pl.DataFrame(
            schema={
                "aco_id": pl.Utf8,
                "program": pl.Utf8,
                "performance_year": pl.Int32,
                "year_month": pl.Int64,
                "member_months": pl.Int64,
            }
        ).lazy()

    return pl.concat(all_rows, how="vertical").lazy()
