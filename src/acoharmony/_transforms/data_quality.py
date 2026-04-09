# © 2025 HarmonyCares
# All rights reserved.

"""
Data Quality Assessment Transform.

Provides comprehensive data quality analysis:
- Completeness checks (null rates, missing critical fields)
- Validity checks (date ranges, code validity, value ranges)
- Consistency checks (duplicate records, conflicting data)
- Timeliness checks (claim lag, data freshness)
- Referential integrity (orphaned records)
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .._decor8 import profile_memory, timeit, traced, transform
from .._log import LogWriter

logger = LogWriter("transforms.data_quality")


@transform(name="data_quality", tier=["gold"])
class DataQualityTransform:
    """
    Comprehensive data quality assessment and reporting.
    """

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def assess_completeness(claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Assess data completeness.

                Args:
                    claims: Medical claims
                    config: Configuration dict

                Returns:
                    LazyFrame with completeness metrics by field
        """
        logger.info("Assessing data completeness...")

        # Calculate null rates for critical fields
        critical_fields = [
            "person_id",
            "claim_id",
            "claim_start_date",
            "claim_end_date",
            "diagnosis_code_1",
            "bill_type_code",
            "revenue_code",
            "paid_amount",
            "allowed_amount",
        ]

        # Get total record count
        total_count = claims.select(pl.count().alias("total_count"))

        # Calculate null counts for each field
        completeness_metrics = []
        for field in critical_fields:
            if field in claims.columns:
                null_count = claims.select(pl.col(field).is_null().sum().alias("null_count"))
                completeness_metrics.append(
                    pl.DataFrame(
                        {
                            "field_name": [field],
                            "null_count": null_count.collect()["null_count"][0],
                            "total_count": total_count.collect()["total_count"][0],
                        }
                    )
                )

        if completeness_metrics:
            completeness = pl.concat(completeness_metrics).lazy()
            completeness = completeness.with_columns(
                [
                    (pl.col("null_count") / pl.col("total_count") * 100).alias("null_rate_pct"),
                    (
                        (pl.col("total_count") - pl.col("null_count")) / pl.col("total_count") * 100
                    ).alias("completeness_pct"),
                ]
            )
        else:
            completeness = pl.DataFrame(
                {
                    "field_name": [],
                    "null_count": [],
                    "total_count": [],
                    "null_rate_pct": [],
                    "completeness_pct": [],
                }
            ).lazy()

        logger.info(f"Completeness assessed for {len(critical_fields)} critical fields")

        return completeness

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def assess_validity(claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Assess data validity.

                Args:
                    claims: Medical claims
                    config: Configuration dict

                Returns:
                    LazyFrame with validity issues
        """
        logger.info("Assessing data validity...")

        validity_issues = []

        # Date validity: claim_end_date >= claim_start_date
        date_issues = claims.filter(
            pl.col("claim_end_date").is_not_null()
            & pl.col("claim_start_date").is_not_null()
            & (pl.col("claim_end_date") < pl.col("claim_start_date"))
        ).select(
            [
                pl.lit("date_sequence").alias("issue_type"),
                pl.lit("claim_end_date before claim_start_date").alias("issue_description"),
                pl.count().alias("issue_count"),
            ]
        )

        validity_issues.append(date_issues)

        # Amount validity: negative amounts
        negative_paid = claims.filter(pl.col("paid_amount") < 0).select(
            [
                pl.lit("negative_amount").alias("issue_type"),
                pl.lit("negative paid_amount").alias("issue_description"),
                pl.count().alias("issue_count"),
            ]
        )

        validity_issues.append(negative_paid)

        # Amount validity: allowed < paid (unusual but not always wrong)
        amount_mismatch = claims.filter(
            pl.col("allowed_amount").is_not_null()
            & pl.col("paid_amount").is_not_null()
            & (pl.col("allowed_amount") < pl.col("paid_amount"))
        ).select(
            [
                pl.lit("amount_mismatch").alias("issue_type"),
                pl.lit("paid_amount exceeds allowed_amount").alias("issue_description"),
                pl.count().alias("issue_count"),
            ]
        )

        validity_issues.append(amount_mismatch)

        validity = DataQualityTransform._combine_validity_issues(validity_issues)

        logger.info("Validity assessment complete")

        return validity

    @staticmethod
    def _combine_validity_issues(
        validity_issues: list[pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Combine validity issue LazyFrames, returning an empty frame when none exist."""
        if validity_issues:
            return pl.concat([issue.collect() for issue in validity_issues]).lazy()
        else:
            return pl.DataFrame(
                {"issue_type": [], "issue_description": [], "issue_count": []}
            ).lazy()

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def assess_duplicates(claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Assess duplicate records.

                Args:
                    claims: Medical claims
                    config: Configuration dict

                Returns:
                    LazyFrame with duplicate statistics
        """
        logger.info("Assessing duplicate records...")

        # Check for duplicate claim_ids
        claim_id_duplicates = (
            claims.group_by("claim_id")
            .agg([pl.count().alias("occurrence_count")])
            .filter(pl.col("occurrence_count") > 1)
        )

        duplicate_summary = claim_id_duplicates.select(
            [
                pl.lit("claim_id").alias("duplicate_type"),
                pl.count().alias("duplicate_claim_ids"),
                pl.sum("occurrence_count").alias("total_duplicate_records"),
            ]
        )

        logger.info("Duplicate assessment complete")

        return duplicate_summary

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def assess_timeliness(claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Assess data timeliness (claim lag).

                Args:
                    claims: Medical claims
                    config: Configuration dict

                Returns:
                    LazyFrame with timeliness metrics
        """
        logger.info("Assessing data timeliness...")

        # Calculate claim lag (days from service end to claim processed/received)
        # Assuming we have a 'claim_received_date' field
        if "claim_received_date" in claims.columns:
            timeliness = claims.filter(
                pl.col("claim_end_date").is_not_null() & pl.col("claim_received_date").is_not_null()
            ).with_columns(
                [
                    (pl.col("claim_received_date") - pl.col("claim_end_date"))
                    .dt.total_days()
                    .alias("claim_lag_days")
                ]
            )

            timeliness_summary = timeliness.select(
                [
                    pl.mean("claim_lag_days").alias("avg_claim_lag_days"),
                    pl.median("claim_lag_days").alias("median_claim_lag_days"),
                    pl.col("claim_lag_days").quantile(0.90).alias("p90_claim_lag_days"),
                    pl.col("claim_lag_days").quantile(0.95).alias("p95_claim_lag_days"),
                ]
            )
        else:
            # No received date available
            timeliness_summary = pl.DataFrame(
                {
                    "avg_claim_lag_days": [None],
                    "median_claim_lag_days": [None],
                    "p90_claim_lag_days": [None],
                    "p95_claim_lag_days": [None],
                }
            ).lazy()

        logger.info("Timeliness assessment complete")

        return timeliness_summary

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def assess_referential_integrity(
        claims: pl.LazyFrame, eligibility: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Assess referential integrity between datasets.

                Args:
                    claims: Medical claims
                    eligibility: Member eligibility
                    config: Configuration dict

                Returns:
                    LazyFrame with referential integrity issues
        """
        logger.info("Assessing referential integrity...")

        # Check for orphaned claims (person_id in claims but not in eligibility)
        claims_persons = claims.select(pl.col("person_id").unique())
        eligibility_persons = eligibility.select(pl.col("person_id").unique())

        # Left anti join to find orphaned records
        orphaned_claims = claims_persons.join(eligibility_persons, on="person_id", how="anti")

        orphaned_summary = orphaned_claims.select(
            [
                pl.lit("orphaned_claims").alias("integrity_issue"),
                pl.count().alias("orphaned_person_count"),
            ]
        )

        logger.info("Referential integrity assessment complete")

        return orphaned_summary

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_quality_score(
        completeness: pl.LazyFrame,
        validity: pl.LazyFrame,
        duplicates: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Calculate overall data quality score.

                Args:
                    completeness: Completeness metrics
                    validity: Validity issues
                    duplicates: Duplicate statistics
                    config: Configuration dict

                Returns:
                    LazyFrame with quality score
        """
        logger.info("Calculating overall quality score...")

        # Get average completeness
        completeness_df = completeness.collect()
        avg_completeness = (
            completeness_df["completeness_pct"].mean() if completeness_df.height > 0 else 100.0
        )

        # Count validity issues
        validity_df = validity.collect()
        total_validity_issues = validity_df["issue_count"].sum() if validity_df.height > 0 else 0

        # Count duplicate records
        duplicates_df = duplicates.collect()
        total_duplicates = (
            duplicates_df["total_duplicate_records"].sum() if duplicates_df.height > 0 else 0
        )

        # Calculate weighted quality score (0-100)
        # 60% completeness, 30% validity, 10% duplicates
        completeness_score = avg_completeness * 0.6

        # Validity score - penalize based on issue count (assume 1M records, scale accordingly)
        validity_penalty = min((total_validity_issues / 10000) * 30, 30)
        validity_score = max(0, 30 - validity_penalty)

        # Duplicate score - penalize based on duplicate count
        duplicate_penalty = min((total_duplicates / 10000) * 10, 10)
        duplicate_score = max(0, 10 - duplicate_penalty)

        overall_score = completeness_score + validity_score + duplicate_score

        quality_score = pl.DataFrame(
            {
                "overall_quality_score": [overall_score],
                "completeness_score": [completeness_score],
                "validity_score": [validity_score],
                "duplicate_score": [duplicate_score],
                "avg_completeness_pct": [avg_completeness],
                "total_validity_issues": [total_validity_issues],
                "total_duplicate_records": [total_duplicates],
            }
        ).lazy()

        logger.info(f"Overall quality score: {overall_score:.2f}/100")

        return quality_score

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=60.0)
    @profile_memory(log_result=True)
    def assess_data_quality(
        claims: pl.LazyFrame, eligibility: pl.LazyFrame, config: dict[str, Any]
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Perform comprehensive data quality assessment.

                Args:
                    claims: Medical claims
                    eligibility: Member eligibility
                    config: Configuration dict

                Returns:
                    Tuple of (completeness, validity, duplicates, timeliness, referential_integrity, quality_score)
        """
        logger.info("Starting comprehensive data quality assessment...")

        # Run all assessments
        completeness = DataQualityTransform.assess_completeness(claims, config)
        validity = DataQualityTransform.assess_validity(claims, config)
        duplicates = DataQualityTransform.assess_duplicates(claims, config)
        timeliness = DataQualityTransform.assess_timeliness(claims, config)
        referential_integrity = DataQualityTransform.assess_referential_integrity(
            claims, eligibility, config
        )

        # Calculate overall quality score
        quality_score = DataQualityTransform.calculate_quality_score(
            completeness, validity, duplicates, config
        )

        logger.info("Data quality assessment complete")

        return completeness, validity, duplicates, timeliness, referential_integrity, quality_score


logger.debug("Registered data quality expression")
