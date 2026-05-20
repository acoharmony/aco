# © 2025 HarmonyCares
# All rights reserved.

"""
Utilization Analysis Transform.

Provides comprehensive healthcare utilization analysis following Tuva methodology:
- Visits per member per year (PMPY) by service category
- Bed days per 1000 members
- High utilizer identification
- Service category utilization patterns
- Preventable utilization opportunities
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .._decor8 import profile_memory, timeit, traced, transform
from .._log import LogWriter

logger = LogWriter("transforms.utilization")


@transform(name="utilization", tier=["gold"])
class UtilizationTransform:
    """
    Comprehensive healthcare utilization analytics following Tuva methodology.
    """

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_member_years(eligibility: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Calculate member years from eligibility.

                Args:
                    eligibility: Member eligibility data
                    config: Configuration dict

                Returns:
                    LazyFrame with member years
        """
        logger.info("Calculating member years...")

        measurement_year = config.get("measurement_year", 2024)

        # Filter to measurement year
        member_months = eligibility.filter(
            pl.col("enrollment_start_date").dt.year() <= measurement_year
        ).filter(pl.col("enrollment_end_date").dt.year() >= measurement_year)

        # Calculate months enrolled in measurement year
        member_months = member_months.with_columns(
            [
                pl.when(pl.col("enrollment_start_date").dt.year() < measurement_year)
                .then(pl.date(measurement_year, 1, 1))
                .otherwise(pl.col("enrollment_start_date"))
                .alias("effective_start"),
                pl.when(pl.col("enrollment_end_date").dt.year() > measurement_year)
                .then(pl.date(measurement_year, 12, 31))
                .otherwise(pl.col("enrollment_end_date"))
                .alias("effective_end"),
            ]
        )

        # Calculate days and months
        member_months = member_months.with_columns(
            [
                (pl.col("effective_end") - pl.col("effective_start"))
                .dt.total_days()
                .alias("days_enrolled")
            ]
        )

        # Aggregate to member level
        member_years = member_months.group_by("person_id").agg(
            [
                pl.sum("days_enrolled").alias("total_days_enrolled"),
                (pl.sum("days_enrolled") / 365.25).alias("member_years"),
            ]
        )

        logger.info("Member years calculated")

        return member_years

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_visit_utilization(
        claims: pl.LazyFrame, member_years: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate visits per member per year by service category.

                Args:
                    claims: Medical claims with service categories
                    member_years: Member years data
                    config: Configuration dict

                Returns:
                    LazyFrame with visit utilization
        """
        logger.info("Calculating visit utilization...")

        measurement_year = config.get("measurement_year", 2024)

        # Filter to measurement year
        visits = claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)

        # Count visits by member and service category
        visit_counts = visits.group_by(["person_id", "service_category_2"]).agg(
            [pl.count().alias("visit_count")]
        )

        # Join with member years
        utilization = visit_counts.join(
            member_years.select(["person_id", "member_years"]), on="person_id", how="left"
        )

        # Calculate visits per member year
        utilization = utilization.with_columns(
            [(pl.col("visit_count") / pl.col("member_years")).alias("visits_per_member_year")]
        )

        logger.info("Visit utilization calculated")

        return utilization

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_bed_days(
        admissions: pl.LazyFrame, member_years: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate bed days per 1000 members.

                Args:
                    admissions: Inpatient admissions with length of stay
                    member_years: Member years data
                    config: Configuration dict

                Returns:
                    LazyFrame with bed day utilization
        """
        logger.info("Calculating bed days per 1000...")

        # Sum bed days by member
        bed_days = admissions.group_by("person_id").agg(
            [pl.sum("length_of_stay").alias("total_bed_days")]
        )

        # Join with member years
        bed_days = bed_days.join(
            member_years.select(["person_id", "member_years"]), on="person_id", how="left"
        )

        # Calculate bed days per member year
        bed_days = bed_days.with_columns(
            [
                (pl.col("total_bed_days") / pl.col("member_years")).alias(
                    "bed_days_per_member_year"
                ),
                (pl.col("total_bed_days") / pl.col("member_years") * 1000).alias(
                    "bed_days_per_1000"
                ),
            ]
        )

        logger.info("Bed days calculated")

        return bed_days

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_admission_rates(
        admissions: pl.LazyFrame, member_years: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate admission rates per 1000 members.

                Args:
                    admissions: All admissions (IP, ED, obs)
                    member_years: Member years data
                    config: Configuration dict

                Returns:
                    LazyFrame with admission rates
        """
        logger.info("Calculating admission rates...")

        # Count admissions by member and type
        admission_counts = admissions.group_by(["person_id", "encounter_type"]).agg(
            [pl.count().alias("admission_count")]
        )

        # Join with member years
        admission_rates = admission_counts.join(
            member_years.select(["person_id", "member_years"]), on="person_id", how="left"
        )

        # Calculate rates
        admission_rates = admission_rates.with_columns(
            [
                (pl.col("admission_count") / pl.col("member_years")).alias(
                    "admissions_per_member_year"
                ),
                (pl.col("admission_count") / pl.col("member_years") * 1000).alias(
                    "admissions_per_1000"
                ),
            ]
        )

        logger.info("Admission rates calculated")

        return admission_rates

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_high_utilizers(
        visit_utilization: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Identify high utilizers.

                Args:
                    visit_utilization: Visit utilization data
                    config: Configuration dict

                Returns:
                    LazyFrame with high utilizer flags
        """
        logger.info("Identifying high utilizers...")

        # Aggregate total visits per member
        total_visits = visit_utilization.group_by("person_id").agg(
            [
                pl.sum("visit_count").alias("total_visits"),
                pl.sum("visits_per_member_year").alias("total_visits_pmpy"),
            ]
        )

        # Flag high utilizers (top 10% or >12 visits per year)
        p90_threshold = total_visits.select(pl.col("total_visits_pmpy").quantile(0.90)).collect()[
            0, 0
        ]

        high_utilizers = total_visits.with_columns(
            [
                (pl.col("total_visits_pmpy") >= p90_threshold).alias("is_high_utilizer"),
                pl.when(pl.col("total_visits_pmpy") >= p90_threshold)
                .then(pl.lit("high"))
                .when(pl.col("total_visits_pmpy") >= 12)
                .then(pl.lit("moderate"))
                .otherwise(pl.lit("normal"))
                .alias("utilization_tier"),
            ]
        )

        logger.info("High utilizers identified")

        return high_utilizers

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_service_mix(
        visit_utilization: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate service mix percentages.

                Args:
                    visit_utilization: Visit utilization data
                    config: Configuration dict

                Returns:
                    LazyFrame with service mix
        """
        logger.info("Calculating service mix...")

        # Total visits by service category
        service_totals = visit_utilization.group_by("service_category_2").agg(
            [pl.sum("visit_count").alias("total_visits")]
        )

        # Calculate percentages
        overall_total = service_totals.select(pl.sum("total_visits").alias("grand_total"))

        service_mix = service_totals.join(overall_total, how="cross")

        service_mix = service_mix.with_columns(
            [(pl.col("total_visits") / pl.col("grand_total") * 100).alias("percentage_of_visits")]
        )

        service_mix = service_mix.sort("total_visits", descending=True)

        logger.info("Service mix calculated")

        return service_mix

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=60.0)
    @profile_memory(log_result=True)
    def calculate_utilization_metrics(
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        admissions: pl.LazyFrame | None,
        config: dict[str, Any],
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Calculate comprehensive utilization metrics.

                Args:
                    claims: Medical claims with service categories
                    eligibility: Member eligibility
                    admissions: Optional admissions data
                    config: Configuration dict

                Returns:
                    Tuple of (visit_utilization, admission_rates, bed_days, high_utilizers, service_mix)
        """
        logger.info("Starting utilization analysis...")

        # Calculate member years
        member_years = UtilizationTransform.calculate_member_years(eligibility, config)

        # Calculate visit utilization
        visit_utilization = UtilizationTransform.calculate_visit_utilization(
            claims, member_years, config
        )

        # Calculate admission rates
        if admissions is not None:
            admission_rates = UtilizationTransform.calculate_admission_rates(
                admissions, member_years, config
            )

            # Calculate bed days for inpatient admissions
            inpatient_admissions = admissions.filter(pl.col("encounter_type") == "inpatient")
            bed_days = UtilizationTransform.calculate_bed_days(
                inpatient_admissions, member_years, config
            )
        else:
            admission_rates = pl.DataFrame(
                {
                    "person_id": [],
                    "encounter_type": [],
                    "admission_count": [],
                    "admissions_per_1000": [],
                }
            ).lazy()
            bed_days = pl.DataFrame(
                {"person_id": [], "total_bed_days": [], "bed_days_per_1000": []}
            ).lazy()

        # Identify high utilizers
        high_utilizers = UtilizationTransform.identify_high_utilizers(visit_utilization, config)

        # Calculate service mix
        service_mix = UtilizationTransform.calculate_service_mix(visit_utilization, config)

        logger.info("Utilization analysis complete")

        return visit_utilization, admission_rates, bed_days, high_utilizers, service_mix


logger.debug("Registered utilization expression")
