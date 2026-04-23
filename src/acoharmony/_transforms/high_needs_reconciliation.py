# © 2025 HarmonyCares
# All rights reserved.

"""
High-Needs eligibility reconciliation transform.

Joins our computed per-criterion determinations against BAR's
authoritative flags and emits a match/mismatch report. Used in tests
to flag where our logic diverges from CMS's published alignment
decision.

This is the "tie-in reconciliation of the criteria for our tests" ask:
for each beneficiary in the current BAR, compare

    our criterion_a_met     vs   bar.mobility_impairment_flag
    our criterion_b_met     vs   bar.high_risk_flag
    our criterion_c_met     vs   bar.medium_risk_unplanned_flag
    our criterion_d_met     vs   bar.frailty_flag
    our criterion_e_met     vs   (no BAR flag; carried as-is)

BAR does not publish an explicit criterion-(e) flag; BAR's overall
claims_based_flag ORs every criterion together, so we compare our
composite ``eligible_as_of_check_date`` to it and surface disagreements.

Inputs:

    gold/high_needs_eligibility.parquet  — our determination
    silver/bar.parquet                   — CMS's determination

Output:

    gold/high_needs_reconciliation.parquet — columns:

        mbi
        performance_year
        check_date                         — from our side
        bar_file_date                      — most recent BAR
        our_criterion_a / b / c / d / e
        bar_mobility_impairment_flag
        bar_high_risk_flag
        bar_medium_risk_unplanned_flag
        bar_frailty_flag
        bar_claims_based_flag
        disagreement_a / b / c / d
        disagreement_composite
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl


def execute(executor: Any) -> pl.LazyFrame:
    """
    Reconcile our per-criterion eligibility determination against BAR.
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = Path(storage.get_path(MedallionLayer.SILVER))
    gold_path = Path(storage.get_path(MedallionLayer.GOLD))

    performance_year = getattr(executor, "performance_year", 2026)

    # Our determination: one row per (mbi, check_date). Collapse to the
    # final-of-year snapshot (eligible_as_of_check_date at Oct 1) since
    # BAR is stamped at a point in time.
    ours = pl.scan_parquet(gold_path / "high_needs_eligibility.parquet").filter(
        pl.col("performance_year") == performance_year
    )

    # Take the latest check_date per MBI — eligibility by Oct 1 is the
    # definitive PY signal since sticky alignment means later checks can
    # only add beneficiaries, not remove them.
    per_mbi_ours = (
        ours.sort(["mbi", "check_date"])
        .group_by("mbi")
        .agg(
            pl.col("check_date").max().alias("check_date"),
            pl.col("criterion_a_met").last().alias("our_criterion_a"),
            pl.col("criterion_b_met").last().alias("our_criterion_b"),
            pl.col("criterion_c_met").last().alias("our_criterion_c"),
            pl.col("criterion_d_met").last().alias("our_criterion_d"),
            pl.col("criterion_e_met").last().alias("our_criterion_e"),
            pl.col("eligible_as_of_check_date").last().alias("our_eligible"),
        )
    )

    # BAR: take the latest file_date per MBI.
    bar = pl.scan_parquet(silver_path / "bar.parquet").filter(
        pl.col("bene_eligibility_year_1") == performance_year
    )
    latest_bar = (
        bar.sort(["bene_mbi", "file_date"])
        .group_by("bene_mbi")
        .agg(
            pl.col("file_date").max().alias("bar_file_date"),
            pl.col("mobility_impairment_flag").last().alias("bar_mobility_impairment_flag"),
            pl.col("high_risk_flag").last().alias("bar_high_risk_flag"),
            pl.col("medium_risk_unplanned_flag").last().alias("bar_medium_risk_unplanned_flag"),
            pl.col("frailty_flag").last().alias("bar_frailty_flag"),
            pl.col("claims_based_flag").last().alias("bar_claims_based_flag"),
        )
        .rename({"bene_mbi": "mbi"})
    )

    joined = per_mbi_ours.join(latest_bar, on="mbi", how="full", coalesce=True).with_columns(
        pl.lit(performance_year).alias("performance_year"),
    )

    # Cast BAR flags to boolean so comparisons are consistent. BAR stores
    # flags as Y/N strings on some feeds and booleans on others.
    def _to_bool(col: str) -> pl.Expr:
        v = pl.col(col)
        # Already boolean or null — keep as is
        return (
            pl.when(v.cast(pl.String, strict=False).str.to_lowercase().is_in(["y", "true", "1"]))
            .then(pl.lit(True))
            .when(v.cast(pl.String, strict=False).str.to_lowercase().is_in(["n", "false", "0"]))
            .then(pl.lit(False))
            .otherwise(v.cast(pl.Boolean, strict=False))
            .alias(col)
        )

    return joined.with_columns(
        _to_bool("bar_mobility_impairment_flag"),
        _to_bool("bar_high_risk_flag"),
        _to_bool("bar_medium_risk_unplanned_flag"),
        _to_bool("bar_frailty_flag"),
        _to_bool("bar_claims_based_flag"),
    ).with_columns(
        (pl.col("our_criterion_a").fill_null(False) != pl.col("bar_mobility_impairment_flag").fill_null(False)).alias("disagreement_a"),
        (pl.col("our_criterion_b").fill_null(False) != pl.col("bar_high_risk_flag").fill_null(False)).alias("disagreement_b"),
        (pl.col("our_criterion_c").fill_null(False) != pl.col("bar_medium_risk_unplanned_flag").fill_null(False)).alias("disagreement_c"),
        (pl.col("our_criterion_d").fill_null(False) != pl.col("bar_frailty_flag").fill_null(False)).alias("disagreement_d"),
        (pl.col("our_eligible").fill_null(False) != pl.col("bar_claims_based_flag").fill_null(False)).alias("disagreement_composite"),
    )
