# © 2025 HarmonyCares
# All rights reserved.

"""
High-Needs eligibility reconciliation transform.

The universe is every beneficiary evaluated by the eligibility
transform — the full ``gold/eligibility`` population, which is every
MBI in the silver claims feeds rolled up into a beneficiary roster.
BAR lists only the subset CMS aligned to the ACO for a given PY.
PBVAR A2 lists the subset CMS considered but determined HN-ineligible.
Neither is the universe; both are tie-out signals joined in where
available.

Inputs:

    gold/high_needs_eligibility.parquet  — per-(mbi, PY, check) evaluation
    silver/bar.parquet                   — BAR tie-out signal (optional)
    silver/pbvar.parquet                 — PBVAR A2 tie-out signal (optional)

Output:

    gold/high_needs_reconciliation.parquet — one row per MBI in the
    evaluated population, with columns:

        mbi
        performance_year
        check_date                          — latest within-PY check date

        criterion_a_met_ever / b / c / d / e  — TRUE if met at any check
                                                in any PY 2023+ (per PA
                                                §IV.B.3 sticky alignment)
        criterion_a_met / b / c / d / e       — flag at the latest check
                                                in the most recent PY

        high_needs_eligible_sticky      — cross-PY sticky composite
        high_needs_eligible_this_py     — strict within-PY composite
        first_eligible_py               — earliest PY criteria met
        first_eligible_check_date       — earliest check date met

        (BAR tie-out columns, null when bene absent from the BAR
        roster for this PY.)
        bar_file_date
        bar_mobility_impairment_flag
        bar_high_risk_flag
        bar_medium_risk_unplanned_flag
        bar_frailty_flag
        bar_claims_based_flag

        (PBVAR A2 tie-out columns, null when bene absent from PBVAR.)
        pbvar_a2_file_date
        pbvar_a2_present                — TRUE when A2 seen anywhere
        pbvar_response_codes            — raw response-code list
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

    # Our determination is multi-PY. For the BAR comparison we take the
    # row at the latest check date within the target PY — its
    # ``eligible_sticky_across_pys`` column already folds in any prior-PY
    # eligibility per the cross-PY sticky alignment rule (PA Section
    # IV.B.3, line 3794: "remain aligned to the ACO ... even if the
    # Beneficiary subsequently ceases to meet the criteria").
    ours = pl.scan_parquet(gold_path / "high_needs_eligibility.parquet").filter(
        pl.col("performance_year") == performance_year
    )

    # Per-MBI rollup. The eligibility parquet carries multi-PY rows;
    # two views of each criterion are surfaced:
    #
    #   criterion_{letter}_met_ever — TRUE if the criterion was met at
    #                                 any check date in any PY from 2023
    #                                 onward (cross-PY reading per PA
    #                                 §IV.B.3).
    #   criterion_{letter}_met      — flag at the latest check date in
    #                                 the most recent PY (current view).
    #
    # Composite ``high_needs_eligible_sticky`` has the same semantics
    # as the ``_met_ever`` per-criterion flags.
    all_pys = ours.sort(["mbi", "performance_year", "check_date"])
    per_mbi_rollup = (
        all_pys
        .group_by("mbi")
        .agg(
            pl.col("check_date").max().alias("check_date"),
            pl.col("criterion_a_met").any().alias("criterion_a_met_ever"),
            pl.col("criterion_b_met").any().alias("criterion_b_met_ever"),
            pl.col("criterion_c_met").any().alias("criterion_c_met_ever"),
            pl.col("criterion_d_met").any().alias("criterion_d_met_ever"),
            pl.col("criterion_e_met").any().alias("criterion_e_met_ever"),
            pl.col("criterion_a_met").last().alias("criterion_a_met"),
            pl.col("criterion_b_met").last().alias("criterion_b_met"),
            pl.col("criterion_c_met").last().alias("criterion_c_met"),
            pl.col("criterion_d_met").last().alias("criterion_d_met"),
            pl.col("criterion_e_met").last().alias("criterion_e_met"),
            pl.col("eligible_sticky_across_pys")
            .last()
            .alias("high_needs_eligible_sticky"),
            pl.col("eligible_as_of_check_date")
            .last()
            .alias("high_needs_eligible_this_py"),
            pl.col("first_ever_eligible_py")
            .last()
            .alias("first_eligible_py"),
            pl.col("first_ever_eligible_check_date")
            .last()
            .alias("first_eligible_check_date"),
        )
    )

    # BAR for the target PY: we want the authoritative roster of
    # aligned benes for ``performance_year``, which means the
    # single-most-recent snapshot for that PY, NOT the union of every
    # snapshot CMS has ever sent. A bene who was aligned in February
    # and then dropped by October isn't on the current roster; stacking
    # all monthly snapshots would keep them in.
    #
    # CMS file naming:
    #   ALGC{YY} — Current alignment snapshot for PY 20YY, delivered
    #              monthly or quarterly during PY.
    #   ALGR{YY} — Runout reconciliation for PY 20YY, delivered in
    #              early 20(YY+1). Supersedes ALGC once available.
    #
    # Selection: prefer ALGR{YY} if any exists; otherwise use the
    # latest ALGC{YY}. "Latest" means the most recent source_filename
    # (CMS embeds the delivery date in the filename so lexical sort
    # over source_filename = chronological sort).
    py_suffix = str(performance_year)[-2:]
    bar_all = pl.scan_parquet(silver_path / "bar.parquet")
    bar_algc = bar_all.filter(
        pl.col("source_filename").str.contains(rf"\.ALGC{py_suffix}\.RP\.")
    )
    bar_algr = bar_all.filter(
        pl.col("source_filename").str.contains(rf"\.ALGR{py_suffix}\.RP\.")
    )
    # Resolve the latest filename for whichever of runout/current is
    # available. Eager collect a 1-row frame to pick the filename; the
    # eligibility roster itself stays lazy.
    _runout_files = (
        bar_algr.select(pl.col("source_filename").max().alias("fn"))
        .collect()
    )
    _current_files = (
        bar_algc.select(pl.col("source_filename").max().alias("fn"))
        .collect()
    )
    _runout_fn = _runout_files["fn"][0] if _runout_files.height else None
    _current_fn = _current_files["fn"][0] if _current_files.height else None
    _latest_fn = _runout_fn or _current_fn

    if _latest_fn is None:
        # No BAR for this PY — emit an empty frame so the left-join
        # below still works, just with all bar_* columns null.
        latest_bar = pl.LazyFrame(
            schema={
                "mbi": pl.String,
                "bar_file_date": pl.String,
                "bar_mobility_impairment_flag": pl.Boolean,
                "bar_high_risk_flag": pl.Boolean,
                "bar_medium_risk_unplanned_flag": pl.Boolean,
                "bar_frailty_flag": pl.Boolean,
                "bar_claims_based_flag": pl.Boolean,
            }
        )
    else:
        latest_bar = (
            bar_all.filter(pl.col("source_filename") == _latest_fn)
            .select(
                pl.col("bene_mbi").alias("mbi"),
                pl.col("file_date").alias("bar_file_date"),
                pl.col("mobility_impairment_flag").alias("bar_mobility_impairment_flag"),
                pl.col("high_risk_flag").alias("bar_high_risk_flag"),
                pl.col("medium_risk_unplanned_flag").alias("bar_medium_risk_unplanned_flag"),
                pl.col("frailty_flag").alias("bar_frailty_flag"),
                pl.col("claims_based_flag").alias("bar_claims_based_flag"),
            )
        )

    # PBVAR: response code "A2" = "Accepted but Ineligible for Performance
    # Year" per ``_expressions/_response_code_parser.py``. For a High-Needs
    # Population ACO that operationally means CMS processed the SVA but
    # determined the bene does NOT meet IV.B.1 criteria — a direct
    # "not eligible" tie-out signal. We look across the whole PBVAR feed
    # (not PY-scoped) because PBVAR's PY signal is messier than BAR's
    # and CMS's A2 determination rarely flips back to eligible once set.
    pbvar_path = silver_path / "pbvar.parquet"
    if pbvar_path.exists():
        pbvar_a2 = (
            pl.scan_parquet(pbvar_path)
            .filter(pl.col("sva_response_code_list").str.contains("A2"))
            .sort(["bene_mbi", "file_date"])
            .group_by("bene_mbi")
            .agg(
                pl.col("file_date").max().alias("pbvar_a2_file_date"),
                pl.col("sva_response_code_list").last().alias("pbvar_response_codes"),
            )
            .with_columns(pl.lit(True).alias("pbvar_a2_present"))
            .rename({"bene_mbi": "mbi"})
        )
    else:
        pbvar_a2 = pl.LazyFrame(
            schema={
                "mbi": pl.String,
                "pbvar_a2_file_date": pl.String,
                "pbvar_response_codes": pl.String,
                "pbvar_a2_present": pl.Boolean,
            }
        )

    # LEFT-join from our evaluated population. BAR and PBVAR columns fill
    # with null for benes we evaluated but CMS didn't list. Our-side
    # columns are never null because the universe IS our population.
    joined = (
        per_mbi_rollup.join(latest_bar, on="mbi", how="left")
        .join(pbvar_a2, on="mbi", how="left")
        .with_columns(pl.lit(performance_year).alias("performance_year"))
    )

    # Cast BAR flags to boolean. BAR stores flags as Y/N strings on some
    # feeds and booleans on others. ``cast(pl.Boolean)`` from an arbitrary
    # Utf8 column isn't supported in polars, so stringify first and
    # whitelist the known truthy/falsy encodings. Unknown values (including
    # nulls from benes absent from BAR) stay null.
    def _to_bool(col: str) -> pl.Expr:
        s = pl.col(col).cast(pl.String, strict=False).str.to_lowercase()
        return (
            pl.when(s.is_in(["true", "y", "1"]))
            .then(pl.lit(True))
            .when(s.is_in(["false", "n", "0"]))
            .then(pl.lit(False))
            .otherwise(pl.lit(None))
            .cast(pl.Boolean)
            .alias(col)
        )

    return joined.with_columns(
        _to_bool("bar_mobility_impairment_flag"),
        _to_bool("bar_high_risk_flag"),
        _to_bool("bar_medium_risk_unplanned_flag"),
        _to_bool("bar_frailty_flag"),
        _to_bool("bar_claims_based_flag"),
        pl.col("pbvar_a2_present").fill_null(False),
    )
