# © 2025 HarmonyCares
# All rights reserved.

"""
HCC Risk Scores transform.

Materialises per-beneficiary risk scores under every applicable CMS-HCC,
CMS-HCC ESRD, and CMMI-HCC Concurrent model version for the configured
performance year. Output lands as ``gold/hcc_risk_scores.parquet`` with
one row per (mbi, model_version, score_as_of_date) so the High-Needs
eligibility transform and any downstream auditing consumer can pick the
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

Inputs
------

- ``gold/eligibility.parquet`` — per-beneficiary demographics
  (``member_id``, ``birth_date``, ``gender``,
  ``original_reason_entitlement_code``, ``medicare_status_code``,
  ``dual_status_code``).
- ``silver/diagnosis.parquet`` — claim-level ICD-10 diagnoses joined
  to MBI.

Outputs
-------

``gold/hcc_risk_scores.parquet`` columns:

    mbi                  str
    cohort               str   — "AD" or "ESRD" (see _hcc_cohort)
    model_version        str   — "cms_hcc_v24" | "cms_hcc_v28" |
                                 "cms_hcc_esrd_v24" | "cmmi_concurrent"
    total_risk_score     f64
    score_as_of_date     date  — performance-year-end reference date
    performance_year     i64
"""

from __future__ import annotations

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


def _compute_age(birth_date: date | None, as_of: date) -> int:
    """Integer age as of the reference date; 0 for null birth_date."""
    if birth_date is None:
        return 0
    years = as_of.year - birth_date.year
    if (as_of.month, as_of.day) < (birth_date.month, birth_date.day):
        years -= 1
    return max(years, 0)


def execute(executor: Any) -> pl.LazyFrame:
    """
    Compute per-beneficiary HCC risk scores across all applicable
    models for the configured performance year.

    The configured PY defaults to 2026 unless the executor supplies a
    ``performance_year`` attribute. Score-as-of-date is PY end
    (Dec 31) so every criterion evaluation within the PY uses a
    stable age reference.
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = Path(storage.get_path(MedallionLayer.SILVER))
    gold_path = Path(storage.get_path(MedallionLayer.GOLD))

    performance_year = getattr(executor, "performance_year", 2026)
    score_as_of = date(performance_year, 12, 31)

    # 1. Load eligibility and determine cohort per beneficiary.
    # Note: ``medicare_status_code`` is NOT the CREC — it uses a
    # different coding system (10/11/21/31/...) from the OREC/CREC
    # 0/1/2/3 scheme. hccinfhir treats a missing CREC as "same as OREC"
    # which is the correct default when we don't have an independent
    # current-entitlement signal.
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

    # 2. Load diagnosis list per beneficiary — long-form dedupe carries
    # one row per (claim, diagnosis) with mbi + diagnosis code columns.
    # See silver/int_diagnosis_deduped.
    dx_path = silver_path / "int_diagnosis_deduped.parquet"
    if dx_path.exists():
        dx_per_mbi = (
            pl.scan_parquet(dx_path)
            .select(
                pl.col("current_bene_mbi_id").alias("mbi"),
                pl.col("clm_dgns_cd").cast(pl.String).alias("dgns_cd"),
            )
            .drop_nulls()
            .group_by("mbi")
            .agg(pl.col("dgns_cd").unique().alias("diagnosis_codes"))
            .collect()
        )
    else:
        dx_per_mbi = pl.DataFrame(
            {"mbi": [], "diagnosis_codes": []},
            schema={"mbi": pl.String, "diagnosis_codes": pl.List(pl.String)},
        )

    dx_by_mbi = {row["mbi"]: row["diagnosis_codes"] for row in dx_per_mbi.to_dicts()}

    # 3. Compute scores per beneficiary for each applicable model.
    rows: list[dict[str, Any]] = []
    for row in elig_rows.to_dicts():
        mbi = row["mbi"]
        birth_date = row["birth_date"]
        # Validate OREC against hccinfhir's accepted values; default to
        # "0" (Old Age / OASI) for anything outside {0,1,2,3}.
        raw_orec = (row.get("orec") or "").strip()
        orec = raw_orec if raw_orec in {"0", "1", "2", "3"} else "0"
        # No independent CREC signal in our feed — use OREC as CREC
        # (hccinfhir's expected behaviour when CREC is unknown).
        crec = orec
        cohort = classify_cohort(orec=orec, crec=crec)
        age = _compute_age(birth_date, score_as_of)
        sex = (row.get("sex") or "F")[:1].upper()
        dual = row.get("dual") or "NA"
        dx_codes = tuple(dx_by_mbi.get(mbi, []))

        # CMS-HCC / ESRD — delegate to hccinfhir via the driver.
        for model_name in cms_hcc_models_for_py(performance_year, cohort):
            bene = BeneficiaryScoreInput(
                mbi=mbi,
                age=age,
                sex=sex,
                orec=orec,
                crec=crec,
                dual_elgbl_cd=dual,
                diagnosis_codes=dx_codes,
            )
            score = score_beneficiary_under_model(bene, model_name)
            rows.append(
                {
                    "mbi": mbi,
                    "cohort": cohort,
                    "model_version": score.model_version,
                    "total_risk_score": score.total_risk_score,
                    "score_as_of_date": score_as_of,
                    "performance_year": performance_year,
                }
            )

        # CMMI-HCC Concurrent applies only to A&D beneficiaries per PA
        # Appendix B (line 4314): "no ESRD segment".
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
                # post_kidney_transplant_category resolution needs
                # transplant-claim metadata we don't have here;
                # leave None so the coefficient is zero (correct for
                # non-transplant beneficiaries).
            )
            cmmi_score = score_cmmi_concurrent(cmmi_bene)
            rows.append(
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
        rows,
        schema={
            "mbi": pl.String,
            "cohort": pl.String,
            "model_version": pl.String,
            "total_risk_score": pl.Float64,
            "score_as_of_date": pl.Date,
            "performance_year": pl.Int64,
        },
    )
