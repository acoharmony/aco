# © 2025 HarmonyCares
# All rights reserved.

"""
High-Needs eligibility transform.

Evaluates Appendix A Section IV.B.1 criteria (a)-(e) for every
beneficiary at every quarterly check date in a performance year, then
applies the sticky-alignment rule from Section IV.B.3.

Inputs (all silver/gold layer parquets):

    silver/cclf1.parquet                          — inpatient/HH/SNF claims
    silver/cclf6.parquet                          — DME claims (criterion d)
    silver/reach_appendix_tables_mobility_...     — B.6.1 ICD-10 codes
    silver/reach_appendix_tables_frailty_hcpcs    — B.6.2 HCPCS codes
    gold/hcc_risk_scores.parquet                  — scored per model
    gold/eligibility.parquet                      — OREC for cohort

Output:

    gold/high_needs_eligibility.parquet — columns:

        mbi                              str
        check_date                       date
        performance_year                 i64
        criterion_a_met                  bool
        criterion_b_met                  bool
        criterion_c_met                  bool
        criterion_d_met                  bool
        criterion_e_met                  bool
        criteria_any_met                 bool
        previously_eligible              bool
        eligible_as_of_check_date        bool
        first_eligible_check_date        date | null
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from acoharmony._expressions._high_needs_criterion_a import (
    build_criterion_a_met_expr,
    build_criterion_a_qualifying_claims,
    parse_icd10_codes_from_table_b61,
)
from acoharmony._expressions._high_needs_criterion_b import (
    build_criterion_b_met_expr,
)
from acoharmony._expressions._high_needs_criterion_c import (
    build_criterion_c_met_expr,
)
from acoharmony._expressions._high_needs_criterion_d import (
    build_criterion_d_met_expr,
    build_criterion_d_qualifying_claims,
    parse_hcpcs_codes_from_table_b62,
)
from acoharmony._expressions._high_needs_criterion_e import (
    build_criterion_e_applicable,
    build_criterion_e_met_expr,
)
from acoharmony._expressions._high_needs_eligibility import (
    apply_sticky_alignment,
    build_criteria_any_met_expr,
    join_criteria_to_eligibility,
)
from acoharmony._expressions._high_needs_lookback import (
    check_dates_for_py,
    table_c_window,
    table_d_window,
)
from acoharmony._expressions._hcc_snf_home_health_days import (
    build_snf_hh_days_in_window,
)
from acoharmony._expressions._hcc_unplanned_admissions import (
    count_unplanned_admissions_in_window,
)


def _criterion_a_for_check(
    cclf1_lf: pl.LazyFrame,
    codes_lf: pl.LazyFrame,
    window_c,
) -> pl.LazyFrame:
    """One row per MBI where criterion (a) is met at this check date."""
    qualifying = build_criterion_a_qualifying_claims(
        cclf1_lf, codes_lf, window=window_c,
    )
    return build_criterion_a_met_expr(qualifying).select(
        pl.col("bene_mbi_id").alias("mbi"),
        pl.col("criterion_a_met"),
    )


def _criterion_b_for_check(scores_lf: pl.LazyFrame) -> pl.LazyFrame:
    """One row per MBI with criterion (b) decision."""
    return build_criterion_b_met_expr(scores_lf).select(
        "mbi", "criterion_b_met"
    )


def _criterion_c_for_check(
    scores_lf: pl.LazyFrame,
    cclf1_lf: pl.LazyFrame,
    window_c,
) -> pl.LazyFrame:
    admissions = count_unplanned_admissions_in_window(
        cclf1_lf,
        window_begin=window_c.begin,
        window_end=window_c.end,
    ).rename({"bene_mbi_id": "mbi"})
    return build_criterion_c_met_expr(
        scores_lf, admissions, mbi_col="mbi",
    ).select("mbi", "criterion_c_met")


def _criterion_d_for_check(
    cclf6_lf: pl.LazyFrame,
    codes_lf: pl.LazyFrame,
    window_d,
) -> pl.LazyFrame:
    qualifying = build_criterion_d_qualifying_claims(
        cclf6_lf, codes_lf, window=window_d,
    )
    return build_criterion_d_met_expr(qualifying).select(
        pl.col("bene_mbi_id").alias("mbi"),
        pl.col("criterion_d_met"),
    )


def _criterion_e_for_check(
    cclf1_lf: pl.LazyFrame,
    window_c,
    performance_year: int,
) -> pl.LazyFrame:
    if not build_criterion_e_applicable(performance_year):
        # PY 2022/2023: return empty frame; the rollup's fill_null(False)
        # coalesces missing criterion_e rows to False.
        return pl.LazyFrame(
            schema={"mbi": pl.String, "criterion_e_met": pl.Boolean}
        )
    days = build_snf_hh_days_in_window(
        cclf1_lf,
        window_begin=window_c.begin,
        window_end=window_c.end,
    )
    met = build_criterion_e_met_expr(
        days, performance_year, mbi_col="bene_mbi_id",
    )
    return met.select("mbi", "criterion_e_met")


def execute(executor: Any) -> pl.LazyFrame:
    """
    Run the full eligibility evaluation for the configured performance
    year and emit one row per (mbi, check_date).
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = Path(storage.get_path(MedallionLayer.SILVER))
    gold_path = Path(storage.get_path(MedallionLayer.GOLD))

    performance_year = getattr(executor, "performance_year", 2026)

    # Source data — read once per execute, compose lazily.
    cclf1 = pl.scan_parquet(silver_path / "cclf1.parquet")
    cclf6_path = silver_path / "cclf6.parquet"
    cclf6 = pl.scan_parquet(cclf6_path) if cclf6_path.exists() else pl.LazyFrame()

    b61 = parse_icd10_codes_from_table_b61(
        pl.scan_parquet(
            silver_path / "reach_appendix_tables_mobility_impairment_icd10.parquet"
        )
    )
    b62 = parse_hcpcs_codes_from_table_b62(
        pl.scan_parquet(
            silver_path / "reach_appendix_tables_frailty_hcpcs.parquet"
        )
    )

    scores = pl.scan_parquet(gold_path / "hcc_risk_scores.parquet").filter(
        pl.col("performance_year") == performance_year
    )

    per_check_results: list[pl.LazyFrame] = []
    for cd in check_dates_for_py(performance_year):
        window_c = table_c_window(performance_year, cd)
        window_d = table_d_window(performance_year, cd)

        # Per-criterion evaluations at this check date.
        a = _criterion_a_for_check(cclf1, b61, window_c)
        b = _criterion_b_for_check(scores)
        c = _criterion_c_for_check(scores, cclf1, window_c)
        if cclf6.collect_schema().names():
            d = _criterion_d_for_check(cclf6, b62, window_d)
        else:
            d = pl.LazyFrame(schema={"mbi": pl.String, "criterion_d_met": pl.Boolean})
        e = _criterion_e_for_check(cclf1, window_c, performance_year)

        joined = join_criteria_to_eligibility(
            {"a": a.with_columns(pl.lit(cd).alias("check_date")),
             "b": b.with_columns(pl.lit(cd).alias("check_date")),
             "c": c.with_columns(pl.lit(cd).alias("check_date")),
             "d": d.with_columns(pl.lit(cd).alias("check_date")),
             "e": e.with_columns(pl.lit(cd).alias("check_date"))},
        ).with_columns(
            build_criteria_any_met_expr().alias("criteria_any_met"),
            pl.lit(performance_year).alias("performance_year"),
        )

        per_check_results.append(joined)

    all_checks = pl.concat(per_check_results, how="vertical_relaxed")
    return apply_sticky_alignment(all_checks, mbi_col="mbi")
