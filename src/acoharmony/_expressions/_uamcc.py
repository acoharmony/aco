# © 2025 HarmonyCares
# All rights reserved.

"""
UAMCC (NQF #2888) All-Cause Unplanned Admissions for Patients with
Multiple Chronic Conditions expression builder.

Expressions for calculating the ACO REACH UAMCC measure:
1. Identify MCC cohort (9 chronic condition groups from lookback year claims)
2. Build denominator (age >= 66, 2+ MCC groups)
3. Apply Planned Admission Algorithm v4.0 (PAA Rules 1-3)
4. Apply outcome exclusions (planned, complications, injuries)
5. Calculate person-time at risk
6. Count unplanned admissions (numerator)
7. Compute observed rate per 100 person-years

References:
- ACOREACH_PY2025_UAMCC_MIF_posted07072025.pdf
- NQF #2888
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import explain, profile_memory, timeit, traced
from ._registry import register_expression

# Nine MCC chronic condition groups
MCC_GROUPS: list[str] = [
    "AMI",
    "ALZHEIMER",
    "AFIB",
    "CKD",
    "COPD_ASTHMA",
    "DEPRESSION",
    "DIABETES",
    "HEART_FAILURE",
    "STROKE_TIA",
]


@register_expression(
    "uamcc",
    schemas=["silver", "gold"],
    dataset_types=["claims", "eligibility"],
    description="UAMCC NQF #2888 All-Cause Unplanned Admissions for Patients with MCCs",
)
class UamccExpression:
    """
    Generate expressions for UAMCC measure calculation.

        Configuration Structure:
            ```yaml
            uamcc:
              performance_year: 2025
              min_age: 66
              min_mcc_groups: 2
            ```
    """

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def load_uamcc_value_sets(silver_path: Path) -> dict[str, pl.LazyFrame]:
        """Load all UAMCC value sets from silver layer."""
        value_sets: dict[str, pl.LazyFrame] = {}
        file_mappings = {
            "cohort": "value_sets_uamcc_value_set_cohort.parquet",
            "ccs_icd10_cm": "value_sets_uamcc_value_set_ccs_icd10_cm.parquet",
            "ccs_icd10_pcs": "value_sets_uamcc_value_set_ccs_icd10_pcs.parquet",
            "exclusions": "value_sets_uamcc_value_set_exclusions.parquet",
            "paa1": "value_sets_uamcc_value_set_paa1.parquet",
            "paa2": "value_sets_uamcc_value_set_paa2.parquet",
            "paa3": "value_sets_uamcc_value_set_paa3.parquet",
            "paa4": "value_sets_uamcc_value_set_paa4.parquet",
        }

        for key, filename in file_mappings.items():
            file_path = silver_path / filename
            if file_path.exists():
                value_sets[key] = pl.scan_parquet(file_path)
            else:
                value_sets[key] = pl.DataFrame().lazy()

        return value_sets

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    @profile_memory(log_result=True)
    def identify_mcc_cohort(
        claims: pl.LazyFrame,
        cohort_vs: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """Identify beneficiaries with qualifying chronic condition groups.

        Unpivots all diagnosis columns from claims and matches against the
        cohort value set to find which of the 9 MCC groups each beneficiary
        qualifies for. Uses the lookback year (PY-1).
        """
        performance_year = config.get("performance_year", 2025)
        lookback_begin = f"{performance_year - 1}-01-01"
        lookback_end = f"{performance_year - 1}-12-31"

        # Build long-form diagnosis table from all dx columns
        schema_names = claims.collect_schema().names()
        dx_cols = [c for c in schema_names if c.startswith("diagnosis_code_")]

        frames = []
        for col in dx_cols:
            frames.append(
                claims.select(
                    [
                        pl.col("claim_id"),
                        pl.col("person_id"),
                        pl.col("admission_date"),
                        pl.col(col).alias("normalized_code"),
                    ]
                ).filter(pl.col("normalized_code").is_not_null())
            )

        if not frames:
            return pl.DataFrame(
                schema={
                    "person_id": pl.Utf8,
                    "chronic_condition_group": pl.Utf8,
                    "qualifying_code": pl.Utf8,
                    "qualifying_code_date": pl.Date,
                    "claim_count": pl.UInt32,
                }
            ).lazy()

        all_dx = pl.concat(frames)

        # Filter to lookback period — cast admission_date to Date for comparison
        lookback_dx = all_dx.with_columns(
            pl.col("admission_date").str.to_date("%Y-%m-%d", strict=False)
        ).filter(
            (pl.col("admission_date") >= pl.lit(lookback_begin).str.to_date("%Y-%m-%d"))
            & (pl.col("admission_date") <= pl.lit(lookback_end).str.to_date("%Y-%m-%d"))
        )

        # Join with cohort value set
        matched = lookback_dx.join(
            cohort_vs.select(
                [
                    pl.col("icd_10_cm").alias("normalized_code"),
                    pl.col("chronic_condition_group"),
                ]
            ),
            on="normalized_code",
            how="inner",
        )

        return matched.group_by(["person_id", "chronic_condition_group"]).agg(
            [
                pl.col("admission_date").min().alias("qualifying_code_date"),
                pl.col("claim_id").n_unique().alias("claim_count"),
                pl.col("normalized_code").first().alias("qualifying_code"),
            ]
        )

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def build_denominator(
        mcc_cohort: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """Build UAMCC denominator: age >= 66, 2+ MCC groups."""
        performance_year = config.get("performance_year", 2025)
        min_age = config.get("min_age", 66)
        min_mcc_groups = config.get("min_mcc_groups", 2)
        period_begin = pl.date(performance_year, 1, 1)

        condition_counts = (
            mcc_cohort.group_by("person_id")
            .agg(
                [
                    pl.col("chronic_condition_group")
                    .n_unique()
                    .alias("chronic_condition_count"),
                ]
            )
            .filter(pl.col("chronic_condition_count") >= min_mcc_groups)
        )

        patients_with_age = (
            eligibility.select(
                [pl.col("person_id"), pl.col("birth_date").cast(pl.Date)]
            )
            .unique(subset=["person_id"], keep="first")
            .with_columns(
                [
                    (
                        (period_begin - pl.col("birth_date")).dt.total_days()
                        / 365.25
                    )
                    .cast(pl.Int32)
                    .alias("age_at_period_start")
                ]
            )
            .filter(pl.col("age_at_period_start") >= min_age)
        )

        return condition_counts.join(
            patients_with_age.select(["person_id", "age_at_period_start"]),
            on="person_id",
            how="inner",
        )

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def classify_planned_admissions(
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """Apply CMS Planned Admission Algorithm v4.0.

        PAA Rules (first match wins):
          Rule 1 — always-planned procedure CCS (PAA1)
          Rule 2 — always-planned diagnosis CCS (PAA2)
          Rule 3 — potentially-planned procedure (PAA3) AND NOT acute dx (PAA4)
        """
        performance_year = config.get("performance_year", 2025)

        # Deduplicate claims to claim-level, cast dates
        base_claims = (
            claims.with_columns(
                [
                    pl.col("admission_date")
                    .str.to_date("%Y-%m-%d", strict=False)
                    .alias("admission_date"),
                ]
            )
            .filter(
                pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("11")
                & pl.col("admission_date").is_not_null()
                & (pl.col("admission_date") >= pl.date(performance_year, 1, 1))
                & (pl.col("admission_date") <= pl.date(performance_year, 12, 31))
            )
            .unique(subset=["claim_id"], keep="first")
        )

        # Map principal diagnosis to CCS
        dx_ccs = value_sets["ccs_icd10_cm"].select(
            [
                pl.col("icd_10_cm").alias("diagnosis_code_1"),
                pl.col("ccs_category").alias("dx_ccs_category"),
            ]
        )

        # Map procedure (hcpcs_code) to CCS
        px_ccs = value_sets["ccs_icd10_pcs"].select(
            [
                pl.col("icd_10_pcs").alias("hcpcs_code"),
                pl.col("ccs_category").alias("px_ccs_category"),
            ]
        )

        claims_with_ccs = base_claims.join(
            dx_ccs, on="diagnosis_code_1", how="left"
        ).join(px_ccs, on="hcpcs_code", how="left")

        # PAA1: always-planned procedure CCS categories
        paa1_set = (
            value_sets["paa1"]
            .select(pl.col("ccs_procedure_category"))
            .unique()
            .collect()["ccs_procedure_category"]
            .to_list()
        )

        # PAA2: always-planned diagnosis CCS categories
        paa2_set = (
            value_sets["paa2"]
            .select(pl.col("ccs_diagnosis_category"))
            .unique()
            .collect()["ccs_diagnosis_category"]
            .to_list()
        )

        # PAA3: potentially-planned procedure CCS categories
        paa3_set = (
            value_sets["paa3"]
            .filter(pl.col("code_type") == "CCS")
            .select(pl.col("category_or_code"))
            .unique()
            .collect()["category_or_code"]
            .to_list()
        )

        # PAA4: acute diagnosis CCS categories (negate rule 3)
        paa4_set = (
            value_sets["paa4"]
            .filter(pl.col("code_type") == "CCS")
            .select(pl.col("category_or_code"))
            .unique()
            .collect()["category_or_code"]
            .to_list()
        )

        return claims_with_ccs.with_columns(
            [
                pl.col("px_ccs_category").is_in(paa1_set).alias("rule1_flag"),
                pl.col("dx_ccs_category").is_in(paa2_set).alias("rule2_flag"),
                pl.col("px_ccs_category").is_in(paa3_set).alias("paa3_flag"),
                pl.col("dx_ccs_category").is_in(paa4_set).alias("paa4_flag"),
            ]
        ).with_columns(
            [
                (
                    pl.col("rule1_flag")
                    | pl.col("rule2_flag")
                    | (pl.col("paa3_flag") & ~pl.col("paa4_flag"))
                ).alias("is_planned"),
                pl.when(pl.col("rule1_flag"))
                .then(pl.lit("RULE1"))
                .when(pl.col("rule2_flag"))
                .then(pl.lit("RULE2"))
                .when(pl.col("paa3_flag") & ~pl.col("paa4_flag"))
                .then(pl.lit("RULE3"))
                .otherwise(pl.lit(None))
                .alias("planned_rule"),
            ]
        ).select(
            [
                "claim_id",
                "person_id",
                "admission_date",
                "diagnosis_code_1",
                "dx_ccs_category",
                "is_planned",
                "planned_rule",
            ]
        )

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def apply_outcome_exclusions(
        planned_admissions: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Flag admissions excluded from numerator.

        Exclusions: planned, procedure complications (CCS 145/237/238/257),
        injuries (CCS 2601-2621).
        """
        exclusion_vs = value_sets["exclusions"]

        complication_ccs = (
            exclusion_vs.filter(
                pl.col("exclusion_category")
                == "Complications of procedures or surgeries"
            )
            .select(pl.col("category_or_code"))
            .unique()
            .collect()["category_or_code"]
            .to_list()
        )

        injury_ccs = (
            exclusion_vs.filter(
                pl.col("exclusion_category")
                != "Complications of procedures or surgeries"
            )
            .select(pl.col("category_or_code"))
            .unique()
            .collect()["category_or_code"]
            .to_list()
        )

        return planned_admissions.with_columns(
            [
                pl.col("dx_ccs_category")
                .is_in(complication_ccs)
                .alias("is_procedure_complication"),
                pl.col("dx_ccs_category")
                .is_in(injury_ccs)
                .alias("is_injury_or_accident"),
            ]
        ).with_columns(
            [
                (
                    pl.col("is_planned")
                    | pl.col("is_procedure_complication")
                    | pl.col("is_injury_or_accident")
                ).alias("is_excluded")
            ]
        )

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_person_time(
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """Calculate at-risk person-time per beneficiary.

        at_risk_days = total_period_days - institutional_days
        person_years = at_risk_days / 365.25
        """
        performance_year = config.get("performance_year", 2025)
        # 365 days for non-leap, 366 for leap
        total_days = 366 if performance_year % 4 == 0 else 365

        # Institutional days from inpatient claims
        institutional = (
            claims.with_columns(
                [
                    pl.col("admission_date")
                    .str.to_date("%Y-%m-%d", strict=False)
                    .alias("admission_date"),
                    pl.col("discharge_date")
                    .str.to_date("%Y-%m-%d", strict=False)
                    .alias("discharge_date"),
                ]
            )
            .filter(
                pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("11")
                & pl.col("admission_date").is_not_null()
            )
            .unique(subset=["claim_id"], keep="first")
            .with_columns(
                [
                    pl.when(pl.col("discharge_date").is_not_null())
                    .then(
                        (pl.col("discharge_date") - pl.col("admission_date"))
                        .dt.total_days()
                        .clip(lower_bound=1)
                    )
                    .otherwise(pl.lit(1))
                    .alias("los")
                ]
            )
            .group_by("person_id")
            .agg(pl.col("los").sum().alias("days_in_hospital"))
        )

        return (
            denominator.select("person_id")
            .join(institutional, on="person_id", how="left")
            .with_columns(
                [pl.col("days_in_hospital").fill_null(0).alias("days_in_hospital")]
            )
            .with_columns(
                [
                    (pl.lit(total_days) - pl.col("days_in_hospital"))
                    .clip(lower_bound=0)
                    .alias("at_risk_days")
                ]
            )
            .with_columns(
                [(pl.col("at_risk_days") / 365.25).alias("person_years")]
            )
            .filter(pl.col("at_risk_days") > 0)
        )

    @staticmethod
    @traced()
    @timeit(log_level="info")
    @profile_memory(log_result=True)
    @explain(
        why="UAMCC measure calculation failed",
        how="Check medical_claim and eligibility data, and UAMCC value sets in silver",
        causes=[
            "Missing input data",
            "Missing UAMCC value sets",
            "MCC cohort identification error",
        ],
    )
    def calculate_uamcc_measure(
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
        config: dict[str, Any],
    ) -> tuple[
        pl.LazyFrame,
        pl.LazyFrame,
        pl.LazyFrame,
        pl.LazyFrame,
        pl.LazyFrame,
        pl.LazyFrame,
    ]:
        """Run complete UAMCC measure pipeline.

        Returns:
            (mcc_cohort, denominator, planned_admissions,
             outcome_exclusions, person_time, summary)
        """
        # Step 1: MCC cohort from lookback year
        mcc_cohort = UamccExpression.identify_mcc_cohort(
            claims, value_sets["cohort"], config
        )

        # Step 2: Denominator
        denominator = UamccExpression.build_denominator(
            mcc_cohort, eligibility, config
        )

        # Step 3: PAA classification
        planned_admissions = UamccExpression.classify_planned_admissions(
            claims, value_sets, config
        )

        # Step 4: Outcome exclusions
        outcome_exclusions = UamccExpression.apply_outcome_exclusions(
            planned_admissions, value_sets
        )

        # Step 5: Person-time
        person_time = UamccExpression.calculate_person_time(
            denominator, claims, config
        )

        # Step 6: Numerator (unplanned admissions for denominator members)
        excluded_claims = outcome_exclusions.filter(
            pl.col("is_excluded")
        ).select("claim_id")

        numerator = (
            planned_admissions.join(
                denominator.select("person_id"),
                on="person_id",
                how="inner",
            )
            .join(
                excluded_claims.with_columns(
                    [pl.lit(True).alias("_excl")]
                ),
                on="claim_id",
                how="left",
            )
            .filter(pl.col("_excl").is_null())
            .drop("_excl")
            .with_columns([pl.lit(True).alias("unplanned_admission_flag")])
        )

        # Step 7: Summary
        performance_year = config.get("performance_year", 2025)

        denom_count = denominator.select(
            pl.col("person_id").n_unique().alias("denominator_count")
        )
        total_py = person_time.select(
            pl.col("person_years").sum().alias("total_person_years")
        )
        obs_admits = numerator.select(
            pl.col("claim_id").n_unique().alias("observed_admissions")
        )

        summary = (
            denom_count.join(total_py, how="cross")
            .join(obs_admits, how="cross")
            .with_columns(
                [
                    pl.lit("REACH").alias("program"),
                    pl.lit("UAMCC").alias("measure_id"),
                    pl.lit("2888").alias("nqf_id"),
                    pl.lit(performance_year).alias("performance_year"),
                    (
                        pl.col("observed_admissions").cast(pl.Float64)
                        / pl.col("total_person_years")
                        * 100.0
                    ).alias("observed_rate_per_100"),
                ]
            )
        )

        return (
            mcc_cohort,
            denominator,
            planned_admissions,
            outcome_exclusions,
            person_time,
            summary,
        )
