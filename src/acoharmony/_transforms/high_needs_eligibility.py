# © 2025 HarmonyCares
# All rights reserved.

"""
High-Needs eligibility transform — multi-PY with cross-PY sticky
alignment.

Evaluates Appendix A Section IV.B.1 criteria (a)-(e) for every
beneficiary at every quarterly check date across every performance
year in the configured range, then applies the sticky-alignment rule
from Section IV.B.3 both **within a PY** (across check dates) and
**across PYs** (once ever eligible, always eligible).

PA Section IV.B.3 (line 3794):

    "Once a Beneficiary is aligned to a High-Needs Population ACO, the
    Beneficiary will remain aligned to the ACO even if the Beneficiary
    subsequently ceases to meet the criteria in Section IV.B.1 of this
    Appendix."

"Remain aligned" is persistent — it spans performance years, not just
check dates within one PY. So a beneficiary qualified at PY2023 Apr 1
stays eligible through PY2026 regardless of their per-PY criterion
flags after that point. The downstream reconciliation transform keys
its BAR comparison on ``eligible_sticky_across_pys`` for that reason.

Inputs (all silver/gold layer parquets):

    gold/medical_claim.parquet                    — Tuva-normalised union of
                                                    institutional / DME /
                                                    professional claims, keyed
                                                    on person_id (the crosswalked
                                                    canonical MBI). Feeds
                                                    criteria (a), (c), (d), (e).
    silver/reach_appendix_tables_mobility_...     — B.6.1 ICD-10 codes
    silver/reach_appendix_tables_frailty_hcpcs    — B.6.2 HCPCS codes
    gold/hcc_risk_scores.parquet                  — per-(PY, check_date) scores
    gold/eligibility.parquet                      — OREC + MSTAT for cohort

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
        previously_eligible              bool     — sticky within this PY
        eligible_as_of_check_date        bool     — sticky within this PY
        first_eligible_check_date        date | null — within this PY
        eligible_sticky_across_pys       bool     — cross-PY OR: True at this
                                                    (check_date, PY) if the
                                                    bene was ever eligible
                                                    at any earlier check
                                                    date in any earlier (or
                                                    same) PY
        first_ever_eligible_py           i64  | null
        first_ever_eligible_check_date   date | null
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
    medical_claim_lf: pl.LazyFrame,
    codes_lf: pl.LazyFrame,
    window_c,
) -> pl.LazyFrame:
    """One row per person where criterion (a) is met at this check date."""
    qualifying = build_criterion_a_qualifying_claims(
        medical_claim_lf, codes_lf, window=window_c,
    )
    return build_criterion_a_met_expr(qualifying).select(
        pl.col("person_id").alias("mbi"),
        pl.col("criterion_a_met"),
    )


def _criterion_b_for_check(scores_lf: pl.LazyFrame) -> pl.LazyFrame:
    """One row per MBI with criterion (b) decision."""
    return build_criterion_b_met_expr(scores_lf).select(
        "mbi", "criterion_b_met"
    )


def _criterion_c_for_check(
    scores_lf: pl.LazyFrame,
    medical_claim_lf: pl.LazyFrame,
    window_c,
) -> pl.LazyFrame:
    admissions = count_unplanned_admissions_in_window(
        medical_claim_lf,
        window_begin=window_c.begin,
        window_end=window_c.end,
    ).rename({"person_id": "mbi"})
    return build_criterion_c_met_expr(
        scores_lf, admissions, mbi_col="mbi",
    ).select("mbi", "criterion_c_met")


def _criterion_d_for_check(
    medical_claim_lf: pl.LazyFrame,
    codes_lf: pl.LazyFrame,
    window_d,
) -> pl.LazyFrame:
    qualifying = build_criterion_d_qualifying_claims(
        medical_claim_lf, codes_lf, window=window_d,
    )
    return build_criterion_d_met_expr(qualifying).select(
        pl.col("person_id").alias("mbi"),
        pl.col("criterion_d_met"),
    )


def _criterion_e_for_check(
    medical_claim_lf: pl.LazyFrame,
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
        medical_claim_lf,
        window_begin=window_c.begin,
        window_end=window_c.end,
    )
    met = build_criterion_e_met_expr(
        days, performance_year, mbi_col="person_id",
    )
    return met.select("mbi", "criterion_e_met")


DEFAULT_FIRST_PY = 2023


def _resolve_performance_years(executor: Any) -> list[int]:
    """Pick the list of PYs to evaluate.

    Precedence:
      1. ``executor.performance_years`` — iterable of explicit PYs.
      2. ``executor.performance_year`` — single PY (legacy one-PY mode,
         wrapped as a one-element list).
      3. Default: PY2023 through the current calendar year.
    """
    pys = getattr(executor, "performance_years", None)
    if pys is not None:
        return list(pys)
    single = getattr(executor, "performance_year", None)
    if single is not None:
        return [single]
    import datetime
    return list(range(DEFAULT_FIRST_PY, datetime.date.today().year + 1))


def _evaluate_one_py(
    performance_year: int,
    *,
    medical_claim: pl.LazyFrame,
    b61: pl.LazyFrame,
    b62: pl.LazyFrame,
    scores_all_pys: pl.LazyFrame,
) -> pl.LazyFrame:
    """Run the per-check-date evaluation for ``performance_year`` and
    return a LazyFrame with within-PY sticky alignment applied."""
    scores_this_py = scores_all_pys.filter(
        pl.col("performance_year") == performance_year
    )

    per_check: list[pl.LazyFrame] = []
    for cd in check_dates_for_py(performance_year):
        window_c = table_c_window(performance_year, cd)
        window_d = table_d_window(performance_year, cd)

        # Risk scores are keyed on (mbi, model_version, PY, check_date)
        # since FOG line 1406 pins the dx window to the check date. Slice
        # this check's scores so (b) and (c) evaluate against the
        # check-aligned window, not a stale calendar-year view.
        scores_this_check = scores_this_py.filter(pl.col("check_date") == cd)

        a = _criterion_a_for_check(medical_claim, b61, window_c)
        b = _criterion_b_for_check(scores_this_check)
        c = _criterion_c_for_check(scores_this_check, medical_claim, window_c)
        d = _criterion_d_for_check(medical_claim, b62, window_d)
        e = _criterion_e_for_check(medical_claim, window_c, performance_year)

        joined = join_criteria_to_eligibility(
            {
                "a": a.with_columns(pl.lit(cd).alias("check_date")),
                "b": b.with_columns(pl.lit(cd).alias("check_date")),
                "c": c.with_columns(pl.lit(cd).alias("check_date")),
                "d": d.with_columns(pl.lit(cd).alias("check_date")),
                "e": e.with_columns(pl.lit(cd).alias("check_date")),
            },
        ).with_columns(
            build_criteria_any_met_expr().alias("criteria_any_met"),
            pl.lit(performance_year).alias("performance_year"),
        )
        per_check.append(joined)

    all_checks = pl.concat(per_check, how="vertical_relaxed")
    return apply_sticky_alignment(all_checks, mbi_col="mbi")


def _apply_cross_py_sticky_alignment(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Once ever eligible at any check date in any PY, always eligible.

    Sorts each MBI's rows chronologically across (performance_year,
    check_date), computes a cumulative-max of ``criteria_any_met``,
    and emits:

        eligible_sticky_across_pys       bool
        first_ever_eligible_py           i64  | null
        first_ever_eligible_check_date   date | null

    Per PA Section IV.B.3 (line 3794), "remain aligned to the ACO" is
    persistent; this field is the multi-PY analogue of the within-PY
    ``eligible_as_of_check_date`` field emitted by ``apply_sticky_alignment``.
    """
    ordered = lf.sort(["mbi", "performance_year", "check_date"])

    return ordered.with_columns(
        pl.col("criteria_any_met")
        .cum_max()
        .over("mbi")
        .alias("eligible_sticky_across_pys"),
    ).with_columns(
        pl.when(pl.col("criteria_any_met"))
        .then(pl.col("performance_year"))
        .otherwise(None)
        .alias("_first_py_hit"),
        pl.when(pl.col("criteria_any_met"))
        .then(pl.col("check_date"))
        .otherwise(None)
        .alias("_first_date_hit"),
    ).with_columns(
        pl.col("_first_py_hit")
        .min()
        .over("mbi")
        .alias("first_ever_eligible_py"),
        pl.col("_first_date_hit")
        .min()
        .over("mbi")
        .alias("first_ever_eligible_check_date"),
    ).drop("_first_py_hit", "_first_date_hit")


def execute(executor: Any) -> pl.LazyFrame:
    """
    Run the full multi-PY eligibility evaluation and emit one row per
    (mbi, performance_year, check_date).

    Two-pass materialization to keep peak memory bounded:

      1. Build the per-PY criterion-join tree, ``pl.concat`` across PYs,
         and stream-sink to an intermediate parquet under
         ``gold/_work/``. This drops the entire claims-side join graph
         (medical_claim unpivots, criterion joins, within-PY sticky
         windows) before the cross-PY window runs.
      2. Re-open the intermediate as a fresh scan and apply the cross-PY
         sticky-alignment ``cum_max().over("mbi")``. Window ops over
         per-MBI partitions can't fully stream, but at this point the
         partition input is a narrow per-(mbi, PY, check) row set
         instead of the full join tree, so the partition state stays
         small.

    The outer pipeline (``_pipes/_high_needs.py``) streams the returned
    LazyFrame into ``gold/high_needs_eligibility.parquet``.
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = Path(storage.get_path(MedallionLayer.SILVER))
    gold_path = Path(storage.get_path(MedallionLayer.GOLD))

    performance_years = _resolve_performance_years(executor)

    # Source data — read once per execute, compose lazily. Criteria
    # (a), (c), (d), (e) all read from gold/medical_claim (Tuva-
    # normalised, MBI-crosswalked via person_id). This replaces the
    # prior per-criterion CCLF1/CCLF6 scans and fixes silent claim-
    # attribution loss on MBI rotation.
    medical_claim = pl.scan_parquet(gold_path / "medical_claim.parquet")

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

    scores_all_pys = pl.scan_parquet(gold_path / "hcc_risk_scores.parquet")

    per_py: list[pl.LazyFrame] = []
    for py in performance_years:
        per_py.append(
            _evaluate_one_py(
                py,
                medical_claim=medical_claim,
                b61=b61,
                b62=b62,
                scores_all_pys=scores_all_pys,
            )
        )

    all_pys = pl.concat(per_py, how="vertical_relaxed")

    # Pass 1: stream-sink the per-PY rows so the criterion-join graph
    # gets fully evaluated and released before the per-MBI window runs.
    work_dir = gold_path / "_work"
    work_dir.mkdir(parents=True, exist_ok=True)
    intermediate_path = work_dir / "high_needs_eligibility_pre_cross_py.parquet"
    all_pys.sink_parquet(intermediate_path, engine="streaming")

    # Pass 2: re-open and apply the cross-PY sticky-alignment window
    # over a narrow per-(mbi, PY, check) frame.
    return _apply_cross_py_sticky_alignment(pl.scan_parquet(intermediate_path))
