# © 2025 HarmonyCares
# All rights reserved.

"""
Enhanced Provider Attribution Expression.

Provides comprehensive provider attribution analysis:
- Primary care provider (PCP) attribution using plurality method
- Specialist attribution by specialty
- Provider relationship strength scoring
- Member-provider continuity metrics
- Provider quality and cost rankings
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .._decor8 import profile_memory, timeit, traced, transform
from .._log import LogWriter

logger = LogWriter("transforms.provider_attribution_enhanced")


@transform(name="provider_attribution_enhanced", tier=["gold"])
class ProviderAttributionEnhancedTransform:
    """
    Comprehensive provider attribution and relationship analysis.
    """

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_primary_care_visits(claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Identify primary care visits.

                Args:
                    claims: Medical claims
                    config: Configuration dict

                Returns:
                    LazyFrame with primary care visits
        """
        logger.info("Identifying primary care visits...")

        measurement_year = config.get("measurement_year", 2024)

        pcp_specialties = [
            "01",
            "08",
            "11",
            "38",
            "50",
            "97",
        ]  # GP, Family, Internal, Geriatric, NP, PA

        # Primary care E&M CPT codes
        pcp_cpt_codes = [
            "99201",
            "99202",
            "99203",
            "99204",
            "99205",  # New patient E&M
            "99211",
            "99212",
            "99213",
            "99214",
            "99215",  # Established patient E&M
            "99381",
            "99382",
            "99383",
            "99384",
            "99385",
            "99386",
            "99387",  # Preventive new
            "99391",
            "99392",
            "99393",
            "99394",
            "99395",
            "99396",
            "99397",  # Preventive established
        ]

        pcp_visits = claims.filter(
            (pl.col("claim_type") == "professional")
            & (pl.col("claim_end_date").dt.year() == measurement_year)
            & (
                pl.col("rendering_provider_specialty").is_in(pcp_specialties)
                | pl.col("procedure_code").cast(pl.Utf8).is_in(pcp_cpt_codes)
            )
            & pl.col("rendering_provider_npi").is_not_null()
        ).select(
            [
                "person_id",
                "claim_id",
                "claim_end_date",
                "rendering_provider_npi",
                "rendering_provider_specialty",
                "procedure_code",
                "paid_amount",
            ]
        )

        logger.info(f"Identified {pcp_visits.collect().height:,} primary care visits")

        return pcp_visits

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def attribute_pcp_plurality(pcp_visits: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Attribute members to PCP using plurality method.

                Args:
                    pcp_visits: Primary care visits
                    config: Configuration dict

                Returns:
                    LazyFrame with PCP attribution
        """
        logger.info("Attributing members to PCPs using plurality method...")

        provider_visit_counts = pcp_visits.group_by(["person_id", "rendering_provider_npi"]).agg(
            [
                pl.count().alias("visit_count"),
                pl.sum("paid_amount").alias("total_paid"),
                pl.col("claim_end_date").min().alias("first_visit_date"),
                pl.col("claim_end_date").max().alias("last_visit_date"),
            ]
        )

        provider_ranked = provider_visit_counts.with_columns(
            [
                pl.col("visit_count")
                .rank(method="dense", descending=True)
                .over("person_id")
                .alias("provider_rank")
            ]
        )

        pcp_attribution = provider_ranked.filter(pl.col("provider_rank") == 1).select(
            [
                "person_id",
                pl.col("rendering_provider_npi").alias("attributed_pcp_npi"),
                pl.col("visit_count").alias("pcp_visit_count"),
                pl.col("total_paid").alias("pcp_total_paid"),
                "first_visit_date",
                "last_visit_date",
            ]
        )

        logger.info(f"Attributed {pcp_attribution.collect().height:,} members to PCPs")

        return pcp_attribution

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_continuity_of_care(
        pcp_visits: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate continuity of care metrics.

                Uses Usual Provider Continuity (UPC) index:
                UPC = (Visits to most common provider) / (Total visits)

                Args:
                    pcp_visits: Primary care visits
                    config: Configuration dict

                Returns:
                    LazyFrame with continuity metrics
        """
        logger.info("Calculating continuity of care metrics...")

        total_visits_by_member = pcp_visits.group_by("person_id").agg(
            [pl.count().alias("total_pcp_visits")]
        )

        visits_to_most_common = (
            pcp_visits.group_by(["person_id", "rendering_provider_npi"])
            .agg([pl.count().alias("visits_to_provider")])
            .group_by("person_id")
            .agg([pl.col("visits_to_provider").max().alias("visits_to_most_common_provider")])
        )

        continuity = total_visits_by_member.join(visits_to_most_common, on="person_id", how="left")

        continuity = continuity.with_columns(
            [
                (pl.col("visits_to_most_common_provider") / pl.col("total_pcp_visits")).alias(
                    "upc_index"
                ),
                pl.when(pl.col("total_pcp_visits") >= 3)
                .then(pl.lit("adequate"))
                .when(pl.col("total_pcp_visits") >= 1)
                .then(pl.lit("minimal"))
                .otherwise(pl.lit("none"))
                .alias("continuity_category"),
            ]
        )

        logger.info("Continuity of care metrics calculated")

        return continuity

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def analyze_specialist_utilization(
        claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Analyze specialist utilization patterns.

                Args:
                    claims: Medical claims
                    config: Configuration dict

                Returns:
                    LazyFrame with specialist utilization by member
        """
        logger.info("Analyzing specialist utilization...")

        measurement_year = config.get("measurement_year", 2024)

        pcp_specialties = ["01", "08", "11", "38", "50", "97"]

        specialist_visits = claims.filter(
            (pl.col("claim_type") == "professional")
            & (pl.col("claim_end_date").dt.year() == measurement_year)
            & pl.col("rendering_provider_specialty").is_not_null()
            & ~pl.col("rendering_provider_specialty").is_in(pcp_specialties)
            & pl.col("rendering_provider_npi").is_not_null()
        )

        specialist_utilization = specialist_visits.group_by("person_id").agg(
            [
                pl.count().alias("total_specialist_visits"),
                pl.col("rendering_provider_npi").n_unique().alias("unique_specialists"),
                pl.sum("paid_amount").alias("total_specialist_cost"),
                pl.col("rendering_provider_specialty").unique().alias("specialist_types"),
            ]
        )

        logger.info(
            f"Analyzed specialist utilization for {specialist_utilization.collect().height:,} members"
        )

        return specialist_utilization

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def rank_providers_by_quality(
        claims: pl.LazyFrame, readmission_pairs: pl.LazyFrame | None, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Rank providers by quality metrics.

                Args:
                    claims: Medical claims
                    readmission_pairs: Optional readmission pairs data
                    config: Configuration dict

                Returns:
                    LazyFrame with provider quality rankings
        """
        logger.info("Ranking providers by quality metrics...")

        measurement_year = config.get("measurement_year", 2024)

        admissions = claims.filter(
            (pl.col("claim_type") == "institutional")
            & (pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("11"))
            & (pl.col("claim_end_date").dt.year() == measurement_year)
            & pl.col("admitting_provider_npi").is_not_null()
        )

        provider_stats = admissions.group_by("admitting_provider_npi").agg(
            [
                pl.count().alias("total_admissions"),
                pl.sum("paid_amount").alias("total_admission_cost"),
                pl.mean("paid_amount").alias("avg_admission_cost"),
            ]
        )

        if readmission_pairs is not None:
            readmission_counts = (
                readmission_pairs.join(
                    admissions.select(["claim_id", "admitting_provider_npi"]),
                    left_on="index_admission_id",
                    right_on="claim_id",
                    how="left",
                )
                .group_by("admitting_provider_npi")
                .agg([pl.count().alias("readmissions")])
            )

            provider_stats = provider_stats.join(
                readmission_counts, on="admitting_provider_npi", how="left"
            ).with_columns(
                [
                    pl.col("readmissions").fill_null(0),
                    (pl.col("readmissions") / pl.col("total_admissions") * 100).alias(
                        "readmission_rate_pct"
                    ),
                ]
            )

        provider_stats = provider_stats.with_columns(
            [
                pl.col("avg_admission_cost").rank(method="dense").alias("cost_rank"),
            ]
        )

        logger.info(f"Ranked {provider_stats.collect().height:,} providers by quality")

        return provider_stats

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=60.0)
    @profile_memory(log_result=True)
    def calculate_provider_attribution(
        claims: pl.LazyFrame, eligibility: pl.LazyFrame, config: dict[str, Any]
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Calculate comprehensive provider attribution.

                Args:
                    claims: Medical claims
                    eligibility: Member eligibility
                    config: Configuration dict

                Returns:
                    Tuple of (pcp_attribution, continuity, specialist_utilization, provider_summary)
        """
        logger.info("Starting provider attribution analysis...")

        pcp_visits = ProviderAttributionEnhancedTransform.identify_primary_care_visits(
            claims, config
        )

        pcp_attribution = ProviderAttributionEnhancedTransform.attribute_pcp_plurality(
            pcp_visits, config
        )

        continuity = ProviderAttributionEnhancedTransform.calculate_continuity_of_care(
            pcp_visits, config
        )

        specialist_utilization = (
            ProviderAttributionEnhancedTransform.analyze_specialist_utilization(claims, config)
        )

        member_attribution = pcp_attribution.join(continuity, on="person_id", how="left")

        member_attribution = member_attribution.join(
            specialist_utilization, on="person_id", how="left"
        )

        provider_summary = (
            pcp_visits.group_by("rendering_provider_npi")
            .agg(
                [
                    pl.col("person_id").n_unique().alias("attributed_members"),
                    pl.count().alias("total_visits"),
                    pl.sum("paid_amount").alias("total_revenue"),
                    pl.mean("paid_amount").alias("avg_visit_cost"),
                ]
            )
            .sort("attributed_members", descending=True)
        )

        logger.info("Provider attribution analysis complete")

        return member_attribution, continuity, specialist_utilization, provider_summary


logger.debug("Registered enhanced provider attribution expression")
