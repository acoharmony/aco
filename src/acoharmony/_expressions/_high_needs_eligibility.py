# © 2025 HarmonyCares
# All rights reserved.

"""
High-Needs eligibility rollup — OR the five per-criterion flags and
apply the sticky-alignment rule.

PA Section IV.B.1 (``$BRONZE/ACO_REACH_PY2026_AR_PA_2023_Starters_508.txt``
line 3762) binds the five criteria as alternatives:

    "If the ACO is a High Needs Population ACO, a Beneficiary must
    also meet one or more of the following conditions when first
    aligned to the ACO for a Performance Year, Base Year, reference
    year, or lookback period, as applicable:
        (a) mobility impairment ...
        (b) high risk score ...
        (c) moderate risk score + unplanned admissions ...
        (d) frailty (DME) ...
        (e) SNF/home-health thresholds (PY2024+) ..."

PA Section IV.B.3 (line 3794) specifies sticky alignment:

    "Once a Beneficiary is aligned to a High-Needs Population ACO,
    the Beneficiary will remain aligned to the ACO even if the
    Beneficiary subsequently ceases to meet the criteria in Section
    IV.B.1 of this Appendix."

PA Table B (lines 3819-3857) specifies the quarterly re-check cadence:
a beneficiary who fails the Jan 1 check gets another chance Apr 1,
then Jul 1, then Oct 1.

Rollup semantics for the transform
-----------------------------------

Evaluated per (mbi, check_date, PY):

    criteria_any_met = a OR b OR c OR d OR e

    eligible_as_of_check_date =
        previously_eligible         # sticky from PA IV.B.3
        OR criteria_any_met

    first_eligible_check_date =
        min(check_date where criteria_any_met, over the PY)

The "previously_eligible" flag is maintained across check dates by the
transform layer using a window function over check_date ordered
ascending — a beneficiary eligible at an earlier check remains eligible
at every subsequent check. This module builds the single-check-date
rollup; the transform layer composes it across the four check dates.
"""

from __future__ import annotations

import polars as pl


def build_criteria_any_met_expr() -> pl.Expr:
    """
    Boolean expression: at least one of criterion_a/b/c/d/e is met at
    this check date. Assumes the caller has already joined the five
    per-criterion frames on ``mbi`` so the columns below exist on the
    row (with nulls coalesced to False via ``fill_null`` before this
    call).
    """
    return (
        pl.col("criterion_a_met").fill_null(False)
        | pl.col("criterion_b_met").fill_null(False)
        | pl.col("criterion_c_met").fill_null(False)
        | pl.col("criterion_d_met").fill_null(False)
        | pl.col("criterion_e_met").fill_null(False)
    )


def apply_sticky_alignment(
    per_check_lf: pl.LazyFrame,
    *,
    mbi_col: str = "mbi",
    check_date_col: str = "check_date",
    met_col: str = "criteria_any_met",
) -> pl.LazyFrame:
    """
    Forward-fill the eligibility flag within each MBI across check
    dates: once True, stays True.

    Per PA Section IV.B.3 (line 3794), a beneficiary who meets the
    criteria at any check date remains aligned for the rest of the
    performance year even if a later check would fail. The output adds:

        previously_eligible          bool
        eligible_as_of_check_date    bool  — ``met_col OR previously_eligible``
        first_eligible_check_date    date  — first check where met_col is True
    """
    # Per-mbi cumulative "ever met up to and including this row" — after
    # sorting by check_date ascending.
    ordered = per_check_lf.sort([mbi_col, check_date_col])

    return ordered.with_columns(
        pl.col(met_col)
        .cum_max()
        .over(mbi_col)
        .alias("ever_met"),
    ).with_columns(
        # "previously eligible" excludes the current row; it's whether we
        # were eligible BEFORE this check date.
        pl.col(met_col)
        .shift(1)
        .cum_max()
        .over(mbi_col)
        .fill_null(False)
        .alias("previously_eligible"),
        pl.col("ever_met").alias("eligible_as_of_check_date"),
    ).with_columns(
        pl.when(pl.col(met_col))
        .then(pl.col(check_date_col))
        .otherwise(None)
        .alias("_first_met_this_row"),
    ).with_columns(
        pl.col("_first_met_this_row")
        .min()
        .over(mbi_col)
        .alias("first_eligible_check_date"),
    ).drop("_first_met_this_row", "ever_met")


def join_criteria_to_eligibility(
    per_criterion_frames: dict[str, pl.LazyFrame],
    *,
    mbi_col: str = "mbi",
) -> pl.LazyFrame:
    """
    Outer-join the five per-criterion frames by MBI and emit one row
    per (mbi, check_date).

    Input: ``per_criterion_frames`` maps criterion letter ("a".."e") to
    a LazyFrame carrying columns ``mbi``, ``check_date``, ``criterion_<x>_met``.

    Missing per-criterion rows (beneficiary didn't appear in that
    criterion's qualifying set) are filled with ``False`` for the
    criterion_<x>_met flag.
    """
    import functools

    required = {"a", "b", "c", "d", "e"}
    missing = required - set(per_criterion_frames.keys())
    if missing:
        raise ValueError(f"Missing per-criterion frames: {sorted(missing)}")

    frames = []
    for letter in ("a", "b", "c", "d", "e"):
        lf = per_criterion_frames[letter].select(
            mbi_col, "check_date", f"criterion_{letter}_met"
        )
        frames.append(lf)

    joined = functools.reduce(
        lambda left, right: left.join(right, on=[mbi_col, "check_date"], how="full", coalesce=True),
        frames,
    )

    # Fill null flags with False; a missing per-criterion row means the
    # beneficiary did not qualify under that criterion.
    return joined.with_columns(
        *[
            pl.col(f"criterion_{letter}_met").fill_null(False)
            for letter in ("a", "b", "c", "d", "e")
        ]
    )
