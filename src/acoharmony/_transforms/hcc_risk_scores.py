# © 2025 HarmonyCares
# All rights reserved.

"""
HCC Risk Scores transform.

Materialises per-beneficiary risk scores under every applicable CMS-HCC,
CMS-HCC ESRD, and CMMI-HCC Concurrent model version for every
performance year in the configured range. Output lands as
``gold/hcc_risk_scores.parquet`` with one row per
(mbi, model_version, performance_year) so the High-Needs eligibility
transform and any downstream auditing consumer can pick the
score-model combinations they need.

Scope note
----------

This transform is scoped to High-Needs eligibility evaluation. It does
NOT compute the normalisation factor, ACO-level CIF, or the symmetric
3% cap — those downstream adjustments convert raw risk scores to
Benchmark-adjustment inputs, which is a financial-settlement concern.
High-Needs criteria (b) and (c) evaluate RAW risk scores against CMS's
hard thresholds (3.0 / 0.35 / 2.0 / 0.24), not normalised scores
(see PA Appendix A Section IV.B.1(b),(c) and FOG lines 1177, 1179).

Per-PY diagnosis windows
------------------------

CMS-HCC Prospective models use diagnoses from the **prior calendar
year** to predict expenditures in the current year. So scoring PY2026
under V24/V28 uses dx claims dated 2025-01-01 through 2025-12-31.

CMMI-HCC Concurrent uses diagnoses from the **same calendar year**. So
scoring PY2026 under CMMI uses dx claims dated 2026-01-01 through
2026-12-31.

This transform filters the diagnosis feed per-PY per-model accordingly,
so the same beneficiary gets a different HCC set for different
(PY, model) combinations — as CMS intends.

Multi-PY iteration
-------------------

The PA's sticky-alignment rule (Section IV.B.3, line 3794) — "Once a
Beneficiary is aligned to a High-Needs Population ACO, the Beneficiary
will remain aligned" — requires us to carry eligibility forward across
PYs. The downstream eligibility transform consumes this parquet's
per-PY rows to build that cumulative view.

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
    score_as_of_date     date  — PY end (Dec 31)
    performance_year     i64
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


def _dx_window_for(model_name: str, performance_year: int) -> tuple[date, date]:
    """
    Return the claim-date range whose diagnoses feed a given model/PY.

    - CMS-HCC Prospective (V22/V24/V28) and CMS-HCC ESRD: prior CY.
    - CMMI-HCC Concurrent: same CY as PY.
    """
    if "Concurrent" in model_name or "cmmi" in model_name.lower():
        return date(performance_year, 1, 1), date(performance_year, 12, 31)
    prior = performance_year - 1
    return date(prior, 1, 1), date(prior, 12, 31)


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
    Compute per-beneficiary HCC risk scores for every (PY, model) pair
    in the configured range. Emits one row per (mbi, model_version,
    performance_year).
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = Path(storage.get_path(MedallionLayer.SILVER))
    gold_path = Path(storage.get_path(MedallionLayer.GOLD))

    performance_years = _resolve_performance_years(executor)

    # Eligibility + cohort once (static across PYs).
    eligibility = pl.scan_parquet(gold_path / "eligibility.parquet").select(
        pl.col("member_id").alias("mbi"),
        pl.col("birth_date").cast(pl.Date, strict=False),
        pl.col("gender").cast(pl.String, strict=False).alias("sex"),
        pl.col("original_reason_entitlement_code")
        .cast(pl.String, strict=False)
        .alias("orec"),
        pl.col("dual_status_code").cast(pl.String, strict=False).alias("dual"),
    ).unique(subset=["mbi"])
    elig_rows = eligibility.collect()

    dx_path = silver_path / "int_diagnosis_deduped.parquet"

    all_rows: list[dict[str, Any]] = []

    # Cache dx-window lookups so multiple CMS-HCC models for the same
    # prior-CY window reuse the same filtered dict.
    dx_cache: dict[tuple[date, date], dict[str, list[str]]] = {}

    def _dx_for(begin: date, end: date) -> dict[str, list[str]]:
        key = (begin, end)
        if key not in dx_cache:
            dx_cache[key] = _load_dx_per_mbi_window(dx_path, begin, end)
        return dx_cache[key]

    for performance_year in performance_years:
        score_as_of = date(performance_year, 12, 31)

        # Pre-compute dx windows for this PY's models.
        cms_hcc_dx = _dx_for(*_dx_window_for("CMS-HCC Model V24", performance_year))
        cmmi_dx = _dx_for(*_dx_window_for("CMMI-HCC Concurrent", performance_year))

        for row in elig_rows.to_dicts():
            mbi = row["mbi"]
            birth_date = row["birth_date"]
            raw_orec = (row.get("orec") or "").strip()
            orec = raw_orec if raw_orec in {"0", "1", "2", "3"} else "0"
            crec = orec
            cohort = classify_cohort(orec=orec, crec=crec)
            age = _compute_age(birth_date, score_as_of)
            sex = (row.get("sex") or "F")[:1].upper()
            dual = row.get("dual") or "NA"

            # CMS-HCC / ESRD use prior-year diagnoses.
            dx_codes_prospective = tuple(cms_hcc_dx.get(mbi, []))
            for model_name in cms_hcc_models_for_py(performance_year, cohort):
                bene = BeneficiaryScoreInput(
                    mbi=mbi,
                    age=age,
                    sex=sex,
                    orec=orec,
                    crec=crec,
                    dual_elgbl_cd=dual,
                    diagnosis_codes=dx_codes_prospective,
                )
                score = score_beneficiary_under_model(bene, model_name)
                all_rows.append(
                    {
                        "mbi": mbi,
                        "cohort": cohort,
                        "model_version": score.model_version,
                        "total_risk_score": score.total_risk_score,
                        "score_as_of_date": score_as_of,
                        "performance_year": performance_year,
                    }
                )

            # CMMI-HCC Concurrent (A&D only) uses same-year diagnoses.
            if cohort == "AD":
                dx_codes_concurrent = tuple(cmmi_dx.get(mbi, []))
                dx_bene = BeneficiaryDxInput(
                    mbi=mbi,
                    age=age,
                    sex=sex,
                    diagnosis_codes=dx_codes_concurrent,
                )
                cmmi_hccs = tuple(map_dx_to_cmmi_hccs(dx_bene))
                cmmi_bene = CmmiConcurrentInput(
                    mbi=mbi,
                    age=age,
                    sex=sex,
                    hccs=cmmi_hccs,
                )
                cmmi_score = score_cmmi_concurrent(cmmi_bene)
                all_rows.append(
                    {
                        "mbi": mbi,
                        "cohort": cohort,
                        "model_version": "cmmi_concurrent",
                        "total_risk_score": cmmi_score.total_risk_score,
                        "score_as_of_date": score_as_of,
                        "performance_year": performance_year,
                    }
                )

    return pl.LazyFrame(
        all_rows,
        schema={
            "mbi": pl.String,
            "cohort": pl.String,
            "model_version": pl.String,
            "total_risk_score": pl.Float64,
            "score_as_of_date": pl.Date,
            "performance_year": pl.Int64,
        },
    )
