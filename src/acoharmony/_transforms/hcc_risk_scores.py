# © 2025 HarmonyCares
# All rights reserved.

"""
HCC Risk Scores transform.

Materialises per-beneficiary risk scores under every applicable CMS-HCC,
CMS-HCC ESRD, and CMMI-HCC Concurrent model version **for every
quarterly High-Needs eligibility check date in every performance year**
in the configured range. Output lands as ``gold/hcc_risk_scores.parquet``
with one row per (mbi, model_version, performance_year, check_date) so
the High-Needs eligibility transform can join scores to criteria on the
same (mbi, check_date) key it uses for the claims-based criteria.

Scope note
----------

This transform is scoped to High-Needs eligibility evaluation. It does
NOT compute the normalisation factor, ACO-level CIF, or the symmetric
3% cap — those downstream adjustments convert raw risk scores to
Benchmark-adjustment inputs, which is a financial-settlement concern.
High-Needs criteria (b) and (c) evaluate RAW risk scores against CMS's
hard thresholds (3.0 / 0.35 / 2.0 / 0.24), not normalised scores
(see PA Appendix A Section IV.B.1(b),(c) and FOG lines 1177, 1179).

Diagnosis windows are per-check-date, not per-PY
------------------------------------------------

PY2024 Financial Operating Guide line 1406, quoted in full because this
was originally mis-implemented:

    "For each quarterly High Needs eligibility check, CMS uses the most
    recent period (updated quarterly) of claims history available at
    that time, limiting run-out to the extent possible. To generate
    risk scores for the eligibility criteria listed above, diagnoses
    from the most recent 12-month period are run through both the
    prospective CMS-HCC risk adjustment model and the concurrent
    CMMI-HCC risk adjustment model, and a beneficiary will be
    considered eligible if they meet the requirements with either risk
    score. This allows us to identify High Needs beneficiaries who are
    both chronically ill and more acutely ill. **This 12-month period is
    also used to check for claims-based eligibility criteria like
    mobility and unplanned hospitalizations (see Table B.5.2).** The
    most recent 60-month period will be used for the frailty
    claims-based eligibility criteria, in recognition that DME
    equipment does not need to be replaced annually (see table B.5.3)."

Three facts the FOG pins down together in that paragraph:

1. Risk-score dx window = **the same Table B.5.2 window** used for
   mobility and unplanned hospitalizations.
2. That window is **rolling 12 months ending 3 months before the check
   date** — not a calendar year. See Table B.5.2 rows for PY2026:

       check Jan 1  2026: 11/1/24 – 10/31/25
       check Apr 1  2026:  2/1/25 –  1/31/26
       check Jul 1  2026:  5/1/25 –  4/30/26
       check Oct 1  2026:  8/1/25 –  7/31/26

3. Both the **prospective** (CMS-HCC V24/V28, CMS-HCC ESRD V24) and the
   **concurrent** (CMMI-HCC Concurrent) models read from the **same**
   rolling window. Prior implementations that fed a prior-CY window to
   the prospective model and a same-CY window to the concurrent model
   produced a 3- to 15-month drift between risk scores and admit counts
   at the four check dates, which silently failed criterion (c) benes
   whose risk spikes and admit spikes were genuinely simultaneous but
   landed in mismatched windows.

Consequence: scores are keyed on **(mbi, model_version,
performance_year, check_date)**, not just (mbi, model_version, PY). One
beneficiary gets four score rows per (model, PY) pair — one per check
date — because CMS evaluates eligibility at each of those four anchors
against that anchor's own 12-month dx window.

Multi-PY iteration
-------------------

The PA's sticky-alignment rule (Section IV.B.3, line 3794) — "Once a
Beneficiary is aligned to a High-Needs Population ACO, the Beneficiary
will remain aligned" — requires us to carry eligibility forward across
PYs. The downstream eligibility transform consumes this parquet's
per-(PY, check_date) rows to build that cumulative view.

We iterate over ``executor.performance_years`` (default PY2023 through
the current PY at transform time); the per-PY rows stack into a single
output table. See PA footnote at line 3897: PY2022 is "not relevant to
this Agreement" so the historical window begins at PY2023.

Inputs
------

- ``gold/eligibility.parquet`` — per-beneficiary demographics
  (``member_id``, ``birth_date``, ``gender``,
  ``original_reason_entitlement_code``, ``medicare_status_code``,
  ``dual_status_code``).
- ``silver/int_diagnosis_deduped.parquet`` — one row per (claim, dx)
  with ``current_bene_mbi_id``, ``clm_dgns_cd``, ``clm_from_dt``.

Outputs
-------

``gold/hcc_risk_scores.parquet`` columns:

    mbi                  str
    cohort               str   — "AD" or "ESRD" (see _hcc_cohort)
    model_version        str   — "cms_hcc_v24" | "cms_hcc_v28" |
                                 "cms_hcc_esrd_v24" | "cmmi_concurrent"
    total_risk_score     f64
    score_as_of_date     date  — the check_date itself
    performance_year     i64
    check_date           date  — one of the four quarterly eligibility
                                 check dates (Jan 1, Apr 1, Jul 1,
                                 Oct 1) for ``performance_year``
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from acoharmony._expressions._hcc_cmmi_concurrent import (
    CmmiConcurrentInput,
    score_cmmi_concurrent,
)
from acoharmony._expressions._hcc_cms_prospective import (
    BeneficiaryScoreInput,
    cms_hcc_models_for_py,
    score_beneficiary_under_model,
)
from acoharmony._expressions._hcc_cohort import classify_cohort
from acoharmony._expressions._hcc_dx_to_hcc import (
    BeneficiaryDxInput,
    map_dx_to_cmmi_hccs,
)
from acoharmony._expressions._high_needs_lookback import (
    check_dates_for_py,
    table_c_window,
)


# The historical window begins at PY2023 per the PA's own non-applicability
# footnote for PY2022 (PA line 3897). ``default_performance_years`` resolves
# to PY2023 through the current calendar year at call time.
DEFAULT_FIRST_PY = 2023


def _compute_age(birth_date: date | None, as_of: date) -> int:
    """Integer age as of the reference date; 0 for null birth_date."""
    if birth_date is None:
        return 0
    years = as_of.year - birth_date.year
    if (as_of.month, as_of.day) < (birth_date.month, birth_date.day):
        years -= 1
    return max(years, 0)


def _resolve_performance_years(executor: Any) -> list[int]:
    """Pick the list of PYs to score.

    Precedence:
      1. ``executor.performance_years`` — iterable of explicit PYs.
      2. ``executor.performance_year`` — single PY (legacy one-PY mode;
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


def _dx_window_for_check(
    performance_year: int, check_date: date,
) -> tuple[date, date]:
    """
    Return the rolling 12-month diagnosis window that feeds BOTH the
    prospective (CMS-HCC V24/V28 and CMS-HCC ESRD) AND concurrent
    (CMMI-HCC Concurrent) risk models for a given (PY, check_date) pair.

    Per FOG line 1406, "*For each quarterly High Needs eligibility
    check, CMS uses the most recent period (updated quarterly) of
    claims history available at that time ... This 12-month period is
    also used to check for claims-based eligibility criteria like
    mobility and unplanned hospitalizations.*" This aligns the risk-
    score dx window with the Table B.5.2 (a.k.a. Table C) admission
    window on the exact same calendar bounds — so criterion (c) sees a
    single coherent window rather than two that drift apart by up to
    15 months as the check date shifts through the PY.

    Delegates to ``table_c_window`` so there is exactly one definition
    of the window in the codebase.
    """
    window = table_c_window(performance_year, check_date)
    return window.begin, window.end


def _load_dx_per_mbi_window(
    dx_path: Path,
    begin: date,
    end: date,
) -> dict[str, list[str]]:
    """Collect {mbi: [dx_codes]} for claims with ``clm_from_dt`` inside
    the [begin, end] window. Empty dict if the source file is absent."""
    if not dx_path.exists():
        return {}
    lf = (
        pl.scan_parquet(dx_path)
        .with_columns(pl.col("clm_from_dt").cast(pl.Date, strict=False))
        .filter(pl.col("clm_from_dt").is_between(begin, end, closed="both"))
        .select(
            pl.col("current_bene_mbi_id").alias("mbi"),
            pl.col("clm_dgns_cd").cast(pl.String).alias("dgns_cd"),
        )
        .drop_nulls()
        .group_by("mbi")
        .agg(pl.col("dgns_cd").unique().alias("diagnosis_codes"))
    )
    return {row["mbi"]: row["diagnosis_codes"] for row in lf.collect().to_dicts()}


def execute(executor: Any) -> pl.LazyFrame:
    """
    Compute per-beneficiary HCC risk scores for every (PY, check_date,
    model) triple in the configured range. Emits one row per
    (mbi, model_version, performance_year, check_date).

    Four rows per (mbi, model_version, PY) — one for each quarterly
    eligibility check — because FOG line 1406 pins the risk-score dx
    window to the same rolling 12-month Table B.5.2 window that drives
    the claims-based criteria, so every check date has its own window
    and therefore its own score.
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = Path(storage.get_path(MedallionLayer.SILVER))
    gold_path = Path(storage.get_path(MedallionLayer.GOLD))

    performance_years = _resolve_performance_years(executor)

    # Eligibility + cohort once (static across PYs and check dates).
    eligibility = pl.scan_parquet(gold_path / "eligibility.parquet").select(
        pl.col("member_id").alias("mbi"),
        pl.col("birth_date").cast(pl.Date, strict=False),
        pl.col("gender").cast(pl.String, strict=False).alias("sex"),
        pl.col("original_reason_entitlement_code")
        .cast(pl.String, strict=False)
        .alias("orec"),
        pl.col("medicare_status_code").cast(pl.String, strict=False).alias("mstat"),
        pl.col("dual_status_code").cast(pl.String, strict=False).alias("dual"),
    ).unique(subset=["mbi"])
    elig_rows = eligibility.collect().to_dicts()

    dx_path = silver_path / "int_diagnosis_deduped.parquet"

    output_schema = {
        "mbi": pl.String,
        "cohort": pl.String,
        "model_version": pl.String,
        "total_risk_score": pl.Float64,
        "score_as_of_date": pl.Date,
        "performance_year": pl.Int64,
        "check_date": pl.Date,
    }

    # Per-(PY, check_date) chunks are built as columnar DataFrames and
    # concat'd lazily. The prior implementation held every
    # (bene × model × PY × check) tuple in one all_rows list-of-dicts
    # — ~200 bytes of Python overhead per dict meant ~2 GB for a
    # 500k-bene ACO before anything got written. Arrow-backed chunks
    # drop that to on the order of megabytes per chunk.
    chunks: list[pl.DataFrame] = []

    # Cache dx-window lookups so the same (begin, end) span isn't
    # rescanned from disk across PYs (e.g. the Oct 1 2025 window and
    # the Jan 1 2026 window are distinct, but each is used for every
    # model that applies at that check).
    dx_cache: dict[tuple[date, date], dict[str, list[str]]] = {}

    def _dx_for(begin: date, end: date) -> dict[str, list[str]]:
        key = (begin, end)
        if key not in dx_cache:
            dx_cache[key] = _load_dx_per_mbi_window(dx_path, begin, end)
        return dx_cache[key]

    for performance_year in performance_years:
        for check_date in check_dates_for_py(performance_year):
            # One dx window per (PY, check_date). Both prospective and
            # concurrent models read from this same window — see the
            # FOG line 1406 note in the module docstring.
            dx_begin, dx_end = _dx_window_for_check(performance_year, check_date)
            dx_this_check = _dx_for(dx_begin, dx_end)

            chunk_rows: list[dict[str, Any]] = []
            for row in elig_rows:
                mbi = row["mbi"]
                birth_date = row["birth_date"]
                raw_orec = (row.get("orec") or "").strip()
                orec = raw_orec if raw_orec in {"0", "1", "2", "3"} else "0"
                mstat = (row.get("mstat") or "").strip()
                cohort = classify_cohort(
                    orec=orec, medicare_status_code=mstat,
                )
                # When ESRD is detected via MSTAT but OREC is non-ESRD,
                # the CMS-HCC ESRD V24 model zeros the score unless OREC
                # ∈ {2,3}. Synthesize OREC='3' (disability + ESRD) on
                # the scoring path so the model's age/sex/disability
                # factor sub-model resolves correctly. This does NOT
                # alter the input OREC in eligibility — scoring-only.
                scoring_orec = (
                    "3" if cohort == "ESRD" and orec not in {"2", "3"} else orec
                )
                crec = scoring_orec
                age = _compute_age(birth_date, check_date)
                sex = (row.get("sex") or "F")[:1].upper()
                dual = row.get("dual") or "NA"

                dx_codes = tuple(dx_this_check.get(mbi, []))

                # CMS-HCC Prospective + CMS-HCC ESRD — same dx window.
                for model_name in cms_hcc_models_for_py(performance_year, cohort):
                    bene = BeneficiaryScoreInput(
                        mbi=mbi,
                        age=age,
                        sex=sex,
                        orec=scoring_orec,
                        crec=crec,
                        dual_elgbl_cd=dual,
                        diagnosis_codes=dx_codes,
                    )
                    score = score_beneficiary_under_model(bene, model_name)
                    chunk_rows.append(
                        {
                            "mbi": mbi,
                            "cohort": cohort,
                            "model_version": score.model_version,
                            "total_risk_score": score.total_risk_score,
                            "score_as_of_date": check_date,
                            "performance_year": performance_year,
                            "check_date": check_date,
                        }
                    )

                # CMMI-HCC Concurrent (A&D only) — same dx window as
                # prospective per FOG line 1406 ("diagnoses from the
                # most recent 12-month period are run through both the
                # prospective CMS-HCC risk adjustment model and the
                # concurrent CMMI-HCC risk adjustment model").
                if cohort == "AD":
                    dx_bene = BeneficiaryDxInput(
                        mbi=mbi,
                        age=age,
                        sex=sex,
                        diagnosis_codes=dx_codes,
                    )
                    cmmi_hccs = tuple(map_dx_to_cmmi_hccs(dx_bene))
                    cmmi_bene = CmmiConcurrentInput(
                        mbi=mbi,
                        age=age,
                        sex=sex,
                        hccs=cmmi_hccs,
                    )
                    cmmi_score = score_cmmi_concurrent(cmmi_bene)
                    chunk_rows.append(
                        {
                            "mbi": mbi,
                            "cohort": cohort,
                            "model_version": "cmmi_concurrent",
                            "total_risk_score": cmmi_score.total_risk_score,
                            "score_as_of_date": check_date,
                            "performance_year": performance_year,
                            "check_date": check_date,
                        }
                    )

            chunks.append(pl.DataFrame(chunk_rows, schema=output_schema))
            chunk_rows.clear()

    # Drop dx cache before concat — per-window rosters can be large
    # and are no longer needed once every chunk has consumed them.
    dx_cache.clear()

    if not chunks:
        return pl.LazyFrame(schema=output_schema)

    return pl.concat(chunks, how="vertical").lazy()
