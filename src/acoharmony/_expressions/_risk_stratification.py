# © 2025 HarmonyCares
# All rights reserved.

"""
Member Risk Stratification Expression.

Provides comprehensive member risk stratification:
- Clinical risk scoring (HCC RAF, chronic conditions)
- Utilization risk (admissions, ED visits, readmissions)
- Cost risk (PMPM, total cost trends)
- Social risk (SDOH indicators if available)
- Composite risk tiers for care management prioritization
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .._decor8 import profile_memory, timeit, traced
from .._log import LogWriter
from ._registry import register_expression

logger = LogWriter("expressions.risk_stratification")


@register_expression("risk_stratification", schemas=["gold"], dataset_types=["analytics"])
class RiskStratificationExpression:
    """
    Comprehensive member risk stratification and prioritization.
    """

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_clinical_risk_score(
        eligibility: pl.LazyFrame,
        hcc_raf: pl.LazyFrame | None,
        chronic_conditions: pl.LazyFrame | None,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Calculate clinical risk score.

                Args:
                    eligibility: Member eligibility
                    hcc_raf: Optional HCC RAF scores
                    chronic_conditions: Optional chronic conditions data
                    config: Configuration dict

                Returns:
                    LazyFrame with clinical risk scores
        """
        logger.info("Calculating clinical risk scores...")

        # Start with eligibility
        clinical_risk = eligibility.select(["person_id", "age", "gender"])

        # Age risk factor
        clinical_risk = clinical_risk.with_columns(
            [
                pl.when(pl.col("age") >= 85)
                .then(pl.lit(5))
                .when(pl.col("age") >= 75)
                .then(pl.lit(4))
                .when(pl.col("age") >= 65)
                .then(pl.lit(3))
                .when(pl.col("age") >= 50)
                .then(pl.lit(2))
                .otherwise(pl.lit(1))
                .alias("age_risk_score")
            ]
        )

        # Join with RAF scores if available
        if hcc_raf is not None:
            clinical_risk = clinical_risk.join(
                hcc_raf.select(["person_id", pl.col("raf_score")]), on="person_id", how="left"
            )
            clinical_risk = clinical_risk.with_columns([pl.col("raf_score").fill_null(1.0)])

            # RAF risk score (normalize around 1.0)
            clinical_risk = clinical_risk.with_columns(
                [
                    pl.when(pl.col("raf_score") >= 3.0)
                    .then(pl.lit(5))
                    .when(pl.col("raf_score") >= 2.0)
                    .then(pl.lit(4))
                    .when(pl.col("raf_score") >= 1.5)
                    .then(pl.lit(3))
                    .when(pl.col("raf_score") >= 1.0)
                    .then(pl.lit(2))
                    .otherwise(pl.lit(1))
                    .alias("raf_risk_score")
                ]
            )
        else:
            clinical_risk = clinical_risk.with_columns(
                [
                    pl.lit(None).cast(pl.Float64).alias("raf_score"),
                    pl.lit(2).alias("raf_risk_score"),
                ]
            )

        # Join with chronic conditions if available
        if chronic_conditions is not None:
            chronic_counts = chronic_conditions.group_by("person_id").agg(
                [pl.col("condition").n_unique().alias("chronic_condition_count")]
            )
            clinical_risk = clinical_risk.join(chronic_counts, on="person_id", how="left")
            clinical_risk = clinical_risk.with_columns(
                [pl.col("chronic_condition_count").fill_null(0)]
            )

            # Chronic condition risk score
            clinical_risk = clinical_risk.with_columns(
                [
                    pl.when(pl.col("chronic_condition_count") >= 5)
                    .then(pl.lit(5))
                    .when(pl.col("chronic_condition_count") >= 3)
                    .then(pl.lit(4))
                    .when(pl.col("chronic_condition_count") >= 2)
                    .then(pl.lit(3))
                    .when(pl.col("chronic_condition_count") >= 1)
                    .then(pl.lit(2))
                    .otherwise(pl.lit(1))
                    .alias("chronic_condition_risk_score")
                ]
            )
        else:
            clinical_risk = clinical_risk.with_columns(
                [
                    pl.lit(0).alias("chronic_condition_count"),
                    pl.lit(2).alias("chronic_condition_risk_score"),
                ]
            )

        # Composite clinical risk score (weighted average)
        clinical_risk = clinical_risk.with_columns(
            [
                (
                    pl.col("age_risk_score") * 0.2
                    + pl.col("raf_risk_score") * 0.5
                    + pl.col("chronic_condition_risk_score") * 0.3
                ).alias("clinical_risk_score")
            ]
        )

        logger.info("Clinical risk scores calculated")

        return clinical_risk

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_utilization_risk_score(
        admissions: pl.LazyFrame | None, readmissions: pl.LazyFrame | None, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate utilization risk score.

                Args:
                    admissions: Admissions data
                    readmissions: Readmission pairs
                    config: Configuration dict

                Returns:
                    LazyFrame with utilization risk scores
        """
        logger.info("Calculating utilization risk scores...")

        utilization_risk = None

        # Admission counts
        if admissions is not None:
            # Count IP, ED, obs by member
            admission_counts = admissions.group_by("person_id").agg(
                [
                    pl.when(pl.col("encounter_type") == "inpatient")
                    .then(1)
                    .otherwise(0)
                    .sum()
                    .alias("ip_admission_count"),
                    pl.when(pl.col("encounter_type") == "emergency_department")
                    .then(1)
                    .otherwise(0)
                    .sum()
                    .alias("ed_visit_count"),
                ]
            )

            utilization_risk = admission_counts

            # IP admission risk score
            utilization_risk = utilization_risk.with_columns(
                [
                    pl.when(pl.col("ip_admission_count") >= 3)
                    .then(pl.lit(5))
                    .when(pl.col("ip_admission_count") >= 2)
                    .then(pl.lit(4))
                    .when(pl.col("ip_admission_count") >= 1)
                    .then(pl.lit(3))
                    .otherwise(pl.lit(1))
                    .alias("ip_risk_score")
                ]
            )

            # ED visit risk score
            utilization_risk = utilization_risk.with_columns(
                [
                    pl.when(pl.col("ed_visit_count") >= 5)
                    .then(pl.lit(5))
                    .when(pl.col("ed_visit_count") >= 3)
                    .then(pl.lit(4))
                    .when(pl.col("ed_visit_count") >= 2)
                    .then(pl.lit(3))
                    .when(pl.col("ed_visit_count") >= 1)
                    .then(pl.lit(2))
                    .otherwise(pl.lit(1))
                    .alias("ed_risk_score")
                ]
            )

        # Readmission flag
        if readmissions is not None:
            readmission_flag = readmissions.select(
                [pl.col("person_id").unique(), pl.lit(True).alias("has_readmission")]
            )
            if utilization_risk is not None:
                utilization_risk = utilization_risk.join(
                    readmission_flag, on="person_id", how="left"
                )
            else:
                utilization_risk = readmission_flag

            utilization_risk = utilization_risk.with_columns(
                [
                    pl.col("has_readmission").fill_null(False),
                    pl.when(pl.col("has_readmission"))
                    .then(pl.lit(5))
                    .otherwise(pl.lit(1))
                    .alias("readmission_risk_score"),
                ]
            )

        if utilization_risk is not None:
            # Fill null scores
            if "ip_risk_score" not in utilization_risk.columns:
                utilization_risk = utilization_risk.with_columns([pl.lit(1).alias("ip_risk_score")])
            if "ed_risk_score" not in utilization_risk.columns:
                utilization_risk = utilization_risk.with_columns([pl.lit(1).alias("ed_risk_score")])
            if "readmission_risk_score" not in utilization_risk.columns:
                utilization_risk = utilization_risk.with_columns(
                    [pl.lit(1).alias("readmission_risk_score")]
                )

            # Composite utilization risk score
            utilization_risk = utilization_risk.with_columns(
                [
                    (
                        pl.col("ip_risk_score") * 0.4
                        + pl.col("ed_risk_score") * 0.3
                        + pl.col("readmission_risk_score") * 0.3
                    ).alias("utilization_risk_score")
                ]
            )
        else:
            # No utilization data
            utilization_risk = pl.DataFrame(
                {
                    "person_id": [],
                    "ip_admission_count": [],
                    "ed_visit_count": [],
                    "has_readmission": [],
                    "utilization_risk_score": [],
                }
            ).lazy()

        logger.info("Utilization risk scores calculated")

        return utilization_risk

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_cost_risk_score(
        tcoc: pl.LazyFrame | None, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate cost risk score.

                Args:
                    tcoc: Total cost of care data
                    config: Configuration dict

                Returns:
                    LazyFrame with cost risk scores
        """
        logger.info("Calculating cost risk scores...")

        if tcoc is not None:
            # Use PMPM and total cost to score
            cost_risk = tcoc.select(
                [
                    "person_id",
                    "total_medical_cost",
                    "medical_pmpm",
                    pl.col("cost_tier").alias("tcoc_tier"),
                ]
            )

            # Cost tier risk score
            cost_risk = cost_risk.with_columns(
                [
                    pl.when(pl.col("tcoc_tier") == "top_1_pct")
                    .then(pl.lit(5))
                    .when(pl.col("tcoc_tier") == "top_5_pct")
                    .then(pl.lit(4))
                    .when(pl.col("tcoc_tier") == "top_10_pct")
                    .then(pl.lit(3))
                    .when(pl.col("medical_pmpm") > 500)
                    .then(pl.lit(2))
                    .otherwise(pl.lit(1))
                    .alias("cost_risk_score")
                ]
            )
        else:
            # No cost data
            cost_risk = pl.DataFrame(
                {
                    "person_id": [],
                    "total_medical_cost": [],
                    "medical_pmpm": [],
                    "cost_risk_score": [],
                }
            ).lazy()

        logger.info("Cost risk scores calculated")

        return cost_risk

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_composite_risk_tier(
        clinical_risk: pl.LazyFrame,
        utilization_risk: pl.LazyFrame,
        cost_risk: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Calculate composite risk tier.

                Args:
                    clinical_risk: Clinical risk scores
                    utilization_risk: Utilization risk scores
                    cost_risk: Cost risk scores
                    config: Configuration dict

                Returns:
                    LazyFrame with composite risk tiers
        """
        logger.info("Calculating composite risk tiers...")

        # Join all risk dimensions
        composite = clinical_risk.select(["person_id", "clinical_risk_score"])

        composite = composite.join(
            utilization_risk.select(["person_id", "utilization_risk_score"]),
            on="person_id",
            how="left",
        )
        composite = composite.with_columns([pl.col("utilization_risk_score").fill_null(1.0)])

        composite = composite.join(
            cost_risk.select(["person_id", "cost_risk_score"]), on="person_id", how="left"
        )
        composite = composite.with_columns([pl.col("cost_risk_score").fill_null(1.0)])

        # Weighted composite risk score (clinical 40%, utilization 30%, cost 30%)
        composite = composite.with_columns(
            [
                (
                    pl.col("clinical_risk_score") * 0.4
                    + pl.col("utilization_risk_score") * 0.3
                    + pl.col("cost_risk_score") * 0.3
                ).alias("composite_risk_score")
            ]
        )

        # Assign risk tiers
        composite = composite.with_columns(
            [
                pl.when(pl.col("composite_risk_score") >= 4.5)
                .then(pl.lit("critical"))
                .when(pl.col("composite_risk_score") >= 3.5)
                .then(pl.lit("high"))
                .when(pl.col("composite_risk_score") >= 2.5)
                .then(pl.lit("medium"))
                .when(pl.col("composite_risk_score") >= 1.5)
                .then(pl.lit("low"))
                .otherwise(pl.lit("minimal"))
                .alias("risk_tier")
            ]
        )

        # Priority flag for care management
        composite = composite.with_columns(
            [pl.col("risk_tier").is_in(["critical", "high"]).alias("priority_for_care_management")]
        )

        logger.info("Composite risk tiers calculated")

        return composite

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=60.0)
    @profile_memory(log_result=True)
    def stratify_member_risk(
        eligibility: pl.LazyFrame,
        hcc_raf: pl.LazyFrame | None,
        chronic_conditions: pl.LazyFrame | None,
        admissions: pl.LazyFrame | None,
        readmissions: pl.LazyFrame | None,
        tcoc: pl.LazyFrame | None,
        config: dict[str, Any],
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Perform comprehensive member risk stratification.

                Args:
                    eligibility: Member eligibility
                    hcc_raf: Optional HCC RAF scores
                    chronic_conditions: Optional chronic conditions
                    admissions: Optional admissions data
                    readmissions: Optional readmission pairs
                    tcoc: Optional total cost of care
                    config: Configuration dict

                Returns:
                    Tuple of (member_risk_scores, risk_tier_summary, high_risk_members, composite_risk)
        """
        logger.info("Starting member risk stratification...")

        # Calculate clinical risk
        clinical_risk = RiskStratificationExpression.calculate_clinical_risk_score(
            eligibility, hcc_raf, chronic_conditions, config
        )

        # Calculate utilization risk
        utilization_risk = RiskStratificationExpression.calculate_utilization_risk_score(
            admissions, readmissions, config
        )

        # Calculate cost risk
        cost_risk = RiskStratificationExpression.calculate_cost_risk_score(tcoc, config)

        # Calculate composite risk tier
        composite_risk = RiskStratificationExpression.calculate_composite_risk_tier(
            clinical_risk, utilization_risk, cost_risk, config
        )

        # Join all dimensions for member risk scores
        member_risk_scores = clinical_risk.join(
            utilization_risk.select(
                [
                    "person_id",
                    "utilization_risk_score",
                    pl.col("ip_admission_count").fill_null(0).alias("ip_admission_count"),
                    pl.col("ed_visit_count").fill_null(0).alias("ed_visit_count"),
                ]
            ),
            on="person_id",
            how="left",
        )
        member_risk_scores = member_risk_scores.join(
            cost_risk.select(
                ["person_id", "cost_risk_score", "total_medical_cost", "medical_pmpm"]
            ),
            on="person_id",
            how="left",
        )
        member_risk_scores = member_risk_scores.join(
            composite_risk.select(
                ["person_id", "composite_risk_score", "risk_tier", "priority_for_care_management"]
            ),
            on="person_id",
            how="left",
        )

        # Risk tier summary
        risk_tier_summary = composite_risk.group_by("risk_tier").agg(
            [
                pl.count().alias("member_count"),
                pl.mean("composite_risk_score").alias("avg_composite_score"),
                pl.mean("clinical_risk_score").alias("avg_clinical_score"),
                pl.mean("utilization_risk_score").alias("avg_utilization_score"),
                pl.mean("cost_risk_score").alias("avg_cost_score"),
            ]
        )

        # High-risk members (critical + high)
        high_risk_members = member_risk_scores.filter(
            pl.col("risk_tier").is_in(["critical", "high"])
        ).sort("composite_risk_score", descending=True)

        logger.info("Member risk stratification complete")

        return member_risk_scores, risk_tier_summary, high_risk_members, composite_risk


logger.debug("Registered risk stratification expression")
