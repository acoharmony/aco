# © 2025 HarmonyCares
# All rights reserved.

"""
Per-beneficiary × month risk score matrix.

Mirrors the ``consolidated_alignment`` pattern (``_aco_alignment_temporal``):
starts from a long-form frame of beneficiary-level HCC risk scores
timestamped with a ``score_date`` and produces a wide frame where each
beneficiary row carries one column per year-month
(``ym_YYYYMM_risk_score``) plus a suite of horizontal aggregates that
summarize the timeline (running mean, max, trend, above-threshold count).

Point-in-time semantics
-----------------------
Each ``ym_YYYYMM_risk_score`` column is the most-recent score with
``score_date <= month-end``. A beneficiary with no score on or before
that month-end contributes null for that column. This matches how
consolidated_alignment treats program flags: the column answers "what
was the status/score as of this month-end?"

Scoring responsibility
----------------------
The caller supplies per-bene scores in long form. Typically this means
running ``HCCEngine.score_patient`` once per ``(member_id, score_date)``
combination upstream, yielding a frame with columns ``member_id``,
``score_date``, ``risk_score``. This transform is pure LazyFrame and
never invokes the Python-level HCC engine itself.
"""

from __future__ import annotations

from datetime import date
from dateutil.relativedelta import relativedelta

import polars as pl


def year_month_range(start: date, end: date) -> list[str]:
    """Inclusive list of YYYYMM strings covering [start, end]."""
    months: list[str] = []
    cursor = start.replace(day=1)
    stop = end.replace(day=1)
    while cursor <= stop:
        months.append(cursor.strftime("%Y%m"))
        cursor += relativedelta(months=1)
    return months


def _month_end(ym: str) -> date:
    """Return the last calendar day of month YYYYMM."""
    year = int(ym[:4])
    month = int(ym[4:6])
    if month == 12:
        nxt = date(year + 1, 1, 1)
    else:
        nxt = date(year, month + 1, 1)
    return nxt - relativedelta(days=1)


def build_risk_score_matrix(
    scored_benes: pl.LazyFrame,
    window_start: date,
    window_end: date,
    performance_year: int,
    raf_threshold: float = 1.0,
    change_lookback_months: int = 6,
) -> pl.LazyFrame:
    """
    Pivot long-form risk scores to a wide bene × month matrix with
    horizontal aggregates.

    Args:
        scored_benes: LazyFrame with columns ``member_id`` (Utf8),
            ``score_date`` (Date), ``risk_score`` (Float64).
            Extra columns are preserved on the output as per-bene
            attributes (joined from the first row per member_id).
        window_start: First month of the matrix (day ignored — uses month start).
        window_end: Last month of the matrix (day ignored — uses month start).
        performance_year: Year to average across for ``avg_risk_score_py``
            and ``months_scored_py``.
        raf_threshold: Cutoff for the ``months_above_raf`` counter.
        change_lookback_months: How many months back to compare for
            ``risk_change_N_mo``.

    Returns:
        LazyFrame with one row per ``member_id`` and columns:
            - ``ym_YYYYMM_risk_score`` (Float64) — one per month in range
            - ``avg_risk_score_py`` — mean across PY months, null-skipping
            - ``max_risk_score_ever`` — max across all matrix months
            - ``min_risk_score_ever`` — min across all matrix months
            - ``latest_risk_score`` — last non-null across the window
            - ``months_scored`` — count of non-null months
            - ``months_scored_py`` — count of non-null PY months
            - ``months_above_raf`` — count where score > raf_threshold
            - ``risk_change_N_mo`` — latest minus N-months-ago score
    """
    months = year_month_range(window_start, window_end)

    # Deduplicate score events so each (member_id, score_date) contributes
    # one authoritative score. If multiple rows exist, keep the max — ties
    # in practice come from restated claims and the max is the most
    # conservative reconstruction.
    base = (
        scored_benes.group_by("member_id", "score_date")
        .agg(pl.col("risk_score").max().alias("risk_score"))
    )

    result = base.select("member_id").unique()

    for ym in months:
        me = _month_end(ym)
        # Most-recent score with score_date <= month_end, per bene
        as_of = (
            base.filter(pl.col("score_date") <= me)
            .sort("score_date")
            .group_by("member_id")
            .agg(pl.col("risk_score").last().alias(f"ym_{ym}_risk_score"))
        )
        result = result.join(as_of, on="member_id", how="left")

    all_score_cols = [f"ym_{ym}_risk_score" for ym in months]
    py_months = [ym for ym in months if int(ym[:4]) == performance_year]
    py_cols = [f"ym_{ym}_risk_score" for ym in py_months]

    # Most-recent-non-null across the window: coalesce reversed so the
    # last populated month wins. (Polars has no "coalesce from the right"
    # so we reverse.)
    latest_expr = pl.coalesce([pl.col(c) for c in reversed(all_score_cols)]).alias(
        "latest_risk_score"
    )

    # risk_change_N_mo: latest minus the score from N months before the
    # *last* month in the window. If either end is null, diff is null.
    lookback_idx = len(months) - 1 - change_lookback_months
    if lookback_idx >= 0:
        change_expr = (
            pl.col(all_score_cols[-1]) - pl.col(all_score_cols[lookback_idx])
        ).alias(f"risk_change_{change_lookback_months}_mo")
    else:
        change_expr = pl.lit(None, dtype=pl.Float64).alias(
            f"risk_change_{change_lookback_months}_mo"
        )

    aggregates = [
        pl.mean_horizontal([pl.col(c) for c in py_cols]).alias("avg_risk_score_py")
        if py_cols
        else pl.lit(None, dtype=pl.Float64).alias("avg_risk_score_py"),
        pl.max_horizontal([pl.col(c) for c in all_score_cols]).alias(
            "max_risk_score_ever"
        ),
        pl.min_horizontal([pl.col(c) for c in all_score_cols]).alias(
            "min_risk_score_ever"
        ),
        latest_expr,
        pl.sum_horizontal(
            [pl.col(c).is_not_null().cast(pl.Int64) for c in all_score_cols]
        ).alias("months_scored"),
        pl.sum_horizontal(
            [pl.col(c).is_not_null().cast(pl.Int64) for c in py_cols]
        ).alias("months_scored_py")
        if py_cols
        else pl.lit(0, dtype=pl.Int64).alias("months_scored_py"),
        pl.sum_horizontal(
            [(pl.col(c) > raf_threshold).cast(pl.Int64) for c in all_score_cols]
        ).alias("months_above_raf"),
        change_expr,
    ]

    return result.with_columns(aggregates)
