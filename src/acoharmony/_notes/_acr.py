# © 2025 HarmonyCares
# All rights reserved.

"""
ACR (All-Condition Readmission, NQF #1789) measure analytics.

Backs ``notebooks/acr_readmission.py``: loads the value sets, builds
the eligible index-admission denominator (with CCS exclusions),
assigns specialty cohorts, classifies 30-day readmissions (PAA Rule 2
planned/unplanned), and rolls up the observed rate.

This notebook computes the **observed** rate; risk-standardization
requires the CMS hierarchical model and is performed downstream.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry

VALUE_SET_FILES = {
    "ccs_icd10_cm": "value_sets_acr_ccs_icd10_cm.parquet",
    "exclusions": "value_sets_acr_exclusions.parquet",
    "cohort_icd10": "value_sets_acr_cohort_icd10.parquet",
    "cohort_ccs": "value_sets_acr_cohort_ccs.parquet",
    "paa2": "value_sets_acr_paa2.parquet",
}


def _date_lit(s: str) -> pl.Expr:
    return pl.date(int(s[:4]), int(s[5:7]), int(s[8:]))


class AcrPlugins(PluginRegistry):
    """ACR measure pipeline + summary rollups."""

    # ---- value-set loading --------------------------------------------

    def load_value_sets(self, silver_path: Path) -> dict[str, pl.LazyFrame]:
        """One LazyFrame per ACR value set; missing files become empty frames."""
        out: dict[str, pl.LazyFrame] = {}
        for key, filename in VALUE_SET_FILES.items():
            path = Path(silver_path) / filename
            if path.exists():
                out[key] = pl.scan_parquet(str(path))
            else:
                out[key] = pl.DataFrame().lazy()
        return out

    # ---- step 2: index admissions (denominator) -----------------------

    def index_admissions(
        self,
        claims_lf: pl.LazyFrame | None,
        eligibility_lf: pl.LazyFrame | None,
        value_sets: dict[str, pl.LazyFrame],
        period_begin: str,
        period_end: str,
    ) -> pl.DataFrame:
        """
        Eligible acute-inpatient admissions in the performance period.

        Joins eligibility for age (≥65 filter), maps the principal
        diagnosis to a CCS category, and flags CCS exclusions.
        """
        if claims_lf is None or eligibility_lf is None:
            return pl.DataFrame()

        # admission_date / discharge_date are already Date in gold/medical_claim
        # (silver dedup typing fix landed in commit d27f678). No string coercion.
        inpatient = claims_lf.filter(
            pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("11")
            & pl.col("admission_date").is_not_null()
            & pl.col("discharge_date").is_not_null()
            & (pl.col("admission_date") >= _date_lit(period_begin))
            & (pl.col("admission_date") <= _date_lit(period_end))
        )
        inpatient = inpatient.unique(subset=["claim_id"], keep="first")

        with_age = inpatient.join(
            eligibility_lf.select(
                pl.col("person_id"),
                pl.col("birth_date").cast(pl.Date),
            ).unique(subset=["person_id"], keep="first"),
            on="person_id",
            how="inner",
        ).with_columns(
            ((pl.col("admission_date") - pl.col("birth_date")).dt.total_days() / 365.25)
            .cast(pl.Int32)
            .alias("age_at_admission")
        ).filter(pl.col("age_at_admission") >= 65)

        ccs_mapping = value_sets["ccs_icd10_cm"]
        if ccs_mapping.collect().height > 0:
            with_age = with_age.join(
                ccs_mapping.select(
                    pl.col("icd_10_cm").alias("diagnosis_code_1"),
                    pl.col("ccs_category").alias("ccs_diagnosis_category"),
                ),
                on="diagnosis_code_1",
                how="left",
            )
        else:
            with_age = with_age.with_columns(
                pl.lit(None).cast(pl.Utf8).alias("ccs_diagnosis_category")
            )

        exclusions_vs = value_sets["exclusions"]
        if exclusions_vs.collect().height > 0:
            excluded_ccs = exclusions_vs.select(
                pl.col("ccs_diagnosis_category").unique()
            )
            with_age = with_age.join(
                excluded_ccs.with_columns(pl.lit(True).alias("exclusion_flag")),
                on="ccs_diagnosis_category",
                how="left",
            )
        else:
            with_age = with_age.with_columns(pl.lit(False).alias("exclusion_flag"))

        return (
            with_age.with_columns(pl.col("exclusion_flag").fill_null(False))
            .select(
                "claim_id",
                "person_id",
                "admission_date",
                "discharge_date",
                "diagnosis_code_1",
                "ccs_diagnosis_category",
                "age_at_admission",
                "exclusion_flag",
            )
            .collect()
        )

    def exclusion_breakdown(self, index_admissions_df: pl.DataFrame) -> pl.DataFrame:
        return (
            index_admissions_df.filter(pl.col("exclusion_flag"))
            .group_by("ccs_diagnosis_category")
            .agg(pl.len().alias("excluded_count"))
            .sort("excluded_count", descending=True)
        )

    # ---- step 3: specialty cohorts ------------------------------------

    def specialty_cohorts(
        self,
        claims_lf: pl.LazyFrame | None,
        index_admissions_df: pl.DataFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.DataFrame:
        """
        Assign each eligible claim to one of 5 specialty cohorts.

        Surgery/Gynecology wins via ICD-10-PCS procedure codes, then
        CCS-based cohorts (CARDIORESPIRATORY / CARDIOVASCULAR /
        NEUROLOGY), with MEDICINE as the default.
        """
        empty_schema = {
            "claim_id": pl.Utf8,
            "specialty_cohort": pl.Utf8,
            "cohort_assignment_rule": pl.Utf8,
        }
        if index_admissions_df.is_empty() or claims_lf is None:
            return pl.DataFrame(schema=empty_schema)
        eligible = index_admissions_df.filter(~pl.col("exclusion_flag"))
        if eligible.height == 0:
            return pl.DataFrame(schema=empty_schema)

        # Surgery cohort by procedure code
        surgery_ids = pl.DataFrame(schema={"claim_id": pl.Utf8})
        cohort_icd10 = value_sets["cohort_icd10"]
        if cohort_icd10.collect().height > 0:
            claims_schema = claims_lf.collect_schema().names()
            proc_cols = [
                f"procedure_code_{i}"
                for i in range(1, 26)
                if f"procedure_code_{i}" in claims_schema
            ]
            if proc_cols:
                proc_frames = [
                    claims_lf.select(
                        pl.col("claim_id"),
                        pl.col(c).alias("procedure_code"),
                    ).filter(pl.col("procedure_code").is_not_null())
                    for c in proc_cols
                ]
                claim_procedures = pl.concat(proc_frames)
                surgery_codes = cohort_icd10.filter(
                    pl.col("specialty_cohort") == "SURGERY_GYNECOLOGY"
                ).select(pl.col("icd_10_pcs").alias("procedure_code"))
                surgery_ids = (
                    eligible.lazy()
                    .select("claim_id")
                    .join(claim_procedures, on="claim_id", how="inner")
                    .join(surgery_codes, on="procedure_code", how="inner")
                    .select("claim_id")
                    .unique()
                    .collect()
                )

        surgery_set = set(surgery_ids["claim_id"].to_list()) if surgery_ids.height > 0 else set()

        # CCS-based cohorts
        cohort_ccs = value_sets["cohort_ccs"]
        ccs_cohort_df = pl.DataFrame(
            schema={"claim_id": pl.Utf8, "specialty_cohort": pl.Utf8}
        )
        if cohort_ccs.collect().height > 0:
            non_surgery = eligible.filter(
                ~pl.col("claim_id").is_in(list(surgery_set))
            )
            if non_surgery.height > 0:
                ccs_cohort_df = (
                    non_surgery.lazy()
                    .select("claim_id", "ccs_diagnosis_category")
                    .join(
                        cohort_ccs.select(
                            pl.col("ccs_category").alias("ccs_diagnosis_category"),
                            pl.col("specialty_cohort"),
                        ),
                        on="ccs_diagnosis_category",
                        how="inner",
                    )
                    .select("claim_id", "specialty_cohort")
                    .unique(subset=["claim_id"], keep="first")
                    .collect()
                )

        parts = []
        if surgery_ids.height > 0:
            parts.append(
                surgery_ids.with_columns(
                    pl.lit("SURGERY_GYNECOLOGY").alias("specialty_cohort"),
                    pl.lit("ICD10_PCS").alias("cohort_assignment_rule"),
                )
            )
        if ccs_cohort_df.height > 0:
            parts.append(
                ccs_cohort_df.with_columns(
                    pl.lit("CCS_DIAGNOSIS").alias("cohort_assignment_rule")
                )
            )

        if parts:
            assigned = pl.concat(parts).unique(subset=["claim_id"], keep="first")
        else:
            assigned = pl.DataFrame(schema=empty_schema)

        return (
            eligible.select("claim_id")
            .join(assigned, on="claim_id", how="left")
            .with_columns(
                pl.col("specialty_cohort").fill_null("MEDICINE"),
                pl.col("cohort_assignment_rule").fill_null("DEFAULT_MEDICINE"),
            )
        )

    def cohort_distribution(self, specialty_cohorts_df: pl.DataFrame) -> pl.DataFrame:
        return (
            specialty_cohorts_df.group_by("specialty_cohort")
            .agg(pl.len().alias("admission_count"))
            .with_columns(
                (pl.col("admission_count") * 100.0 / pl.col("admission_count").sum())
                .round(2)
                .alias("pct")
            )
            .sort("admission_count", descending=True)
        )

    # ---- step 4: 30-day readmission classification --------------------

    def classify_readmissions(
        self,
        claims_lf: pl.LazyFrame | None,
        index_admissions_df: pl.DataFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.DataFrame:
        """
        Pair each index discharge with subsequent admissions (1-30 days).

        Maps readmission Dx to CCS, applies PAA Rule 2 to flag planned
        readmissions, and produces an unplanned-readmission flag.
        """
        if index_admissions_df.is_empty() or claims_lf is None:
            return pl.DataFrame()
        eligible = index_admissions_df.filter(~pl.col("exclusion_flag"))
        if eligible.height == 0:
            return pl.DataFrame()

        index_discharges = eligible.select(
            pl.col("claim_id").alias("index_claim_id"),
            pl.col("person_id"),
            pl.col("discharge_date").alias("index_discharge_date"),
        ).lazy()

        # admission_date / discharge_date are already Date (see comment above).
        candidate_admits = (
            claims_lf.filter(
                pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("11")
                & pl.col("admission_date").is_not_null()
            )
            .unique(subset=["claim_id"], keep="first")
            .select(
                pl.col("claim_id").alias("readmission_claim_id"),
                pl.col("person_id"),
                pl.col("admission_date").alias("readmission_date"),
                pl.col("diagnosis_code_1").alias("readmit_diagnosis_code"),
                pl.col("facility_npi").alias("readmit_facility_id"),
            )
        )

        pairs = (
            index_discharges.join(candidate_admits, on="person_id", how="inner")
            .filter(
                (pl.col("readmission_claim_id") != pl.col("index_claim_id"))
                & (pl.col("readmission_date") > pl.col("index_discharge_date"))
                & (
                    (pl.col("readmission_date") - pl.col("index_discharge_date"))
                    .dt.total_days()
                    <= 30
                )
            )
            .with_columns(
                (pl.col("readmission_date") - pl.col("index_discharge_date"))
                .dt.total_days()
                .alias("days_to_readmission")
            )
        )

        ccs_mapping = value_sets["ccs_icd10_cm"]
        if ccs_mapping.collect().height > 0:
            pairs = pairs.join(
                ccs_mapping.select(
                    pl.col("icd_10_cm").alias("readmit_diagnosis_code"),
                    pl.col("ccs_category").alias("readmit_dx_ccs"),
                ),
                on="readmit_diagnosis_code",
                how="left",
            )
        else:
            pairs = pairs.with_columns(
                pl.lit(None).cast(pl.Utf8).alias("readmit_dx_ccs")
            )

        paa2 = value_sets["paa2"]
        if paa2.collect().height > 0:
            paa2_ccs = (
                paa2.select("ccs_diagnosis_category")
                .unique()
                .collect()["ccs_diagnosis_category"]
                .to_list()
            )
            pairs = pairs.with_columns(
                pl.col("readmit_dx_ccs").is_in(paa2_ccs).alias("is_planned"),
                pl.when(pl.col("readmit_dx_ccs").is_in(paa2_ccs))
                .then(pl.lit("RULE2"))
                .otherwise(pl.lit(None))
                .alias("planned_rule"),
            )
        else:
            pairs = pairs.with_columns(
                pl.lit(False).alias("is_planned"),
                pl.lit(None).cast(pl.Utf8).alias("planned_rule"),
            )

        return pairs.with_columns(
            (~pl.col("is_planned")).alias("unplanned_readmission_flag")
        ).collect()

    def planned_unplanned_breakdown(
        self, readmission_pairs_df: pl.DataFrame
    ) -> pl.DataFrame:
        return (
            readmission_pairs_df.with_columns(
                pl.when(pl.col("is_planned"))
                .then(pl.lit("Planned"))
                .otherwise(pl.lit("Unplanned"))
                .alias("readmission_type")
            )
            .group_by("readmission_type")
            .agg(pl.col("readmission_claim_id").n_unique().alias("readmission_count"))
            .with_columns(
                (pl.col("readmission_count") * 100.0 / pl.col("readmission_count").sum())
                .round(2)
                .alias("pct")
            )
        )

    def timing_distribution(self, readmission_pairs_df: pl.DataFrame) -> pl.DataFrame:
        unplanned = readmission_pairs_df.filter(pl.col("unplanned_readmission_flag"))
        return (
            unplanned.with_columns(
                pl.when(pl.col("days_to_readmission").is_between(1, 7))
                .then(pl.lit("1-7 days"))
                .when(pl.col("days_to_readmission").is_between(8, 14))
                .then(pl.lit("8-14 days"))
                .when(pl.col("days_to_readmission").is_between(15, 21))
                .then(pl.lit("15-21 days"))
                .when(pl.col("days_to_readmission").is_between(22, 30))
                .then(pl.lit("22-30 days"))
                .alias("readmission_window")
            )
            .group_by("readmission_window")
            .agg(pl.col("readmission_claim_id").n_unique().alias("readmission_count"))
            .with_columns(
                (pl.col("readmission_count") * 100.0 / pl.col("readmission_count").sum())
                .round(2)
                .alias("pct")
            )
            .sort("readmission_window")
        )

    def top_readmit_facilities(
        self,
        readmission_pairs_df: pl.DataFrame,
        n: int = 10,
    ) -> pl.DataFrame:
        return (
            readmission_pairs_df.filter(pl.col("unplanned_readmission_flag"))
            .group_by("readmit_facility_id")
            .agg(pl.col("readmission_claim_id").n_unique().alias("readmission_count"))
            .sort("readmission_count", descending=True)
            .head(n)
        )

    # ---- step 5: ACR summary -----------------------------------------

    def summary(
        self,
        index_admissions_df: pl.DataFrame,
        readmission_pairs_df: pl.DataFrame,
        performance_year: int,
    ) -> dict[str, Any]:
        """
        Observed ACR rate.

        Returns ``{denominator_count, observed_readmissions, observed_rate_pct, summary_df}``.
        ``summary_df`` is a one-row frame ready for export.
        """
        eligible = (
            index_admissions_df.filter(~pl.col("exclusion_flag"))
            if index_admissions_df.height > 0
            else pl.DataFrame()
        )
        denominator = eligible["claim_id"].n_unique() if eligible.height > 0 else 0
        observed = (
            readmission_pairs_df.filter(pl.col("unplanned_readmission_flag"))[
                "readmission_claim_id"
            ].n_unique()
            if readmission_pairs_df.height > 0
            else 0
        )
        rate = observed / denominator if denominator > 0 else 0.0
        rate_pct = round(rate * 100, 2)

        summary_df = pl.DataFrame(
            {
                "program": ["REACH"],
                "measure_id": ["ACR"],
                "measure_name": ["Risk-Standardized All-Condition Readmission"],
                "nqf_id": ["1789"],
                "performance_year": [performance_year],
                "denominator_count": [denominator],
                "observed_readmissions": [observed],
                "observed_rate_pct": [rate_pct],
            }
        )
        return {
            "denominator_count": denominator,
            "observed_readmissions": observed,
            "observed_rate_pct": rate_pct,
            "summary_df": summary_df,
        }

    # ---- export -------------------------------------------------------

    def export_to_gold(
        self,
        gold_path: Path,
        outputs: dict[str, pl.DataFrame],
    ) -> list[str]:
        """Write per-step parquets, return list of human-readable lines."""
        written = []
        for name, df in outputs.items():
            if df.height > 0:
                df.write_parquet(str(Path(gold_path) / name))
                written.append(f"{name} ({df.height:,} rows)")
        return written
