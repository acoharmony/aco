# © 2025 HarmonyCares
# All rights reserved.

"""
Social Determinants of Health (SDOH) Analysis Transform.

Provides comprehensive SDOH tracking and analysis:
- Z-code analysis for social risk factors
- SDOH screening rates
- Social needs identification
- Risk factor prevalence
- Screening compliance tracking
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .._decor8 import profile_memory, timeit, traced, transform
from .._log import LogWriter

logger = LogWriter("transforms.sdoh")


@transform(name="sdoh", tier=["gold"])
class SdohTransform:
    """
    Comprehensive SDOH tracking and analysis.
    """

    # Z-code categories for SDOH
    Z_CODE_CATEGORIES = {
        # Housing/homelessness
        "housing_instability": [
            "Z59.0",
            "Z59.1",
            "Z59.00",
            "Z59.01",
            "Z59.02",
            "Z59.10",
            "Z59.11",
            "Z59.12",
        ],
        # Food insecurity
        "food_insecurity": ["Z59.4", "Z59.41", "Z59.48"],
        # Transportation
        "transportation_barriers": ["Z59.82", "Z59.89"],
        # Financial insecurity
        "financial_insecurity": ["Z59.5", "Z59.6", "Z59.7", "Z59.86", "Z59.87"],
        # Education/literacy
        "education_literacy": [
            "Z55.0",
            "Z55.1",
            "Z55.2",
            "Z55.3",
            "Z55.4",
            "Z55.5",
            "Z55.8",
            "Z55.9",
        ],
        # Employment
        "employment": [
            "Z56.0",
            "Z56.1",
            "Z56.2",
            "Z56.3",
            "Z56.4",
            "Z56.5",
            "Z56.6",
            "Z56.81",
            "Z56.82",
            "Z56.89",
            "Z56.9",
        ],
        # Social isolation
        "social_isolation": ["Z60.2", "Z60.3", "Z60.4", "Z60.5", "Z60.8", "Z60.9"],
        # Interpersonal violence
        "interpersonal_violence": ["Z69.0", "Z69.1", "Z69.11", "Z69.12", "Z69.81", "Z69.82"],
        # Inadequate social support
        "inadequate_support": [
            "Z63.0",
            "Z63.1",
            "Z63.31",
            "Z63.32",
            "Z63.4",
            "Z63.5",
            "Z63.6",
            "Z63.71",
            "Z63.72",
            "Z63.79",
            "Z63.8",
            "Z63.9",
        ],
        # Legal problems
        "legal_problems": ["Z65.0", "Z65.1", "Z65.2", "Z65.3"],
        # Substance use
        "substance_use": ["Z71.41", "Z71.42", "Z72.0", "Z86.4"],
        # Mental health screening
        "mental_health_screening": [
            "Z13.31",
            "Z13.32",
            "Z13.39",
            "Z13.40",
            "Z13.41",
            "Z13.42",
            "Z13.49",
        ],
        # SDOH screening
        "sdoh_screening": ["Z55-Z65"],  # Simplified - represents full Z55-Z65 range
    }

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_z_codes(claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Identify Z-codes in diagnosis fields.

                Args:
                    claims: Medical claims
                    config: Configuration dict

                Returns:
                    LazyFrame with Z-code identifications
        """
        logger.info("Identifying Z-codes...")

        measurement_year = config.get("measurement_year", 2024)

        # Filter claims to measurement year
        z_code_claims = claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)

        # Check all diagnosis positions for Z-codes
        z_code_claims = z_code_claims.filter(
            pl.col("diagnosis_code_1").cast(pl.Utf8).str.starts_with("Z")
            | pl.col("diagnosis_code_2").cast(pl.Utf8).str.starts_with("Z")
            | pl.col("diagnosis_code_3").cast(pl.Utf8).str.starts_with("Z")
        )

        # Gather all Z-codes into a single column for analysis
        z_code_claims = z_code_claims.with_columns(
            [
                pl.when(pl.col("diagnosis_code_1").cast(pl.Utf8).str.starts_with("Z"))
                .then(pl.col("diagnosis_code_1"))
                .when(pl.col("diagnosis_code_2").cast(pl.Utf8).str.starts_with("Z"))
                .then(pl.col("diagnosis_code_2"))
                .when(pl.col("diagnosis_code_3").cast(pl.Utf8).str.starts_with("Z"))
                .then(pl.col("diagnosis_code_3"))
                .alias("z_code")
            ]
        )

        logger.info(f"Identified {z_code_claims.collect().height:,} claims with Z-codes")

        return z_code_claims

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def categorize_sdoh_risk_factors(
        z_code_claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Categorize SDOH risk factors from Z-codes.

                Args:
                    z_code_claims: Claims with Z-codes
                    config: Configuration dict

                Returns:
                    LazyFrame with SDOH risk factor categories
        """
        logger.info("Categorizing SDOH risk factors...")

        # Categorize Z-codes
        categorized = z_code_claims.with_columns(
            [
                pl.when(
                    pl.col("z_code").is_in(SdohTransform.Z_CODE_CATEGORIES["housing_instability"])
                )
                .then(pl.lit("housing_instability"))
                .when(pl.col("z_code").is_in(SdohTransform.Z_CODE_CATEGORIES["food_insecurity"]))
                .then(pl.lit("food_insecurity"))
                .when(
                    pl.col("z_code").is_in(
                        SdohTransform.Z_CODE_CATEGORIES["transportation_barriers"]
                    )
                )
                .then(pl.lit("transportation_barriers"))
                .when(
                    pl.col("z_code").is_in(SdohTransform.Z_CODE_CATEGORIES["financial_insecurity"])
                )
                .then(pl.lit("financial_insecurity"))
                .when(
                    pl.col("z_code").is_in(SdohTransform.Z_CODE_CATEGORIES["education_literacy"])
                )
                .then(pl.lit("education_literacy"))
                .when(pl.col("z_code").is_in(SdohTransform.Z_CODE_CATEGORIES["employment"]))
                .then(pl.lit("employment"))
                .when(pl.col("z_code").is_in(SdohTransform.Z_CODE_CATEGORIES["social_isolation"]))
                .then(pl.lit("social_isolation"))
                .when(
                    pl.col("z_code").is_in(
                        SdohTransform.Z_CODE_CATEGORIES["interpersonal_violence"]
                    )
                )
                .then(pl.lit("interpersonal_violence"))
                .when(
                    pl.col("z_code").is_in(SdohTransform.Z_CODE_CATEGORIES["inadequate_support"])
                )
                .then(pl.lit("inadequate_support"))
                .when(pl.col("z_code").is_in(SdohTransform.Z_CODE_CATEGORIES["legal_problems"]))
                .then(pl.lit("legal_problems"))
                .when(pl.col("z_code").is_in(SdohTransform.Z_CODE_CATEGORIES["substance_use"]))
                .then(pl.lit("substance_use"))
                .when(
                    pl.col("z_code").is_in(
                        SdohTransform.Z_CODE_CATEGORIES["mental_health_screening"]
                    )
                )
                .then(pl.lit("mental_health_screening"))
                .when(pl.col("z_code").cast(pl.Utf8).str.starts_with("Z5"))
                .then(pl.lit("sdoh_screening"))
                .when(pl.col("z_code").cast(pl.Utf8).str.starts_with("Z6"))
                .then(pl.lit("sdoh_screening"))
                .otherwise(pl.lit("other_z_code"))
                .alias("sdoh_category")
            ]
        )

        logger.info("SDOH risk factors categorized")

        return categorized

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_member_sdoh_profile(
        sdoh_claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate member-level SDOH risk profiles.

                Args:
                    sdoh_claims: Claims with SDOH categories
                    config: Configuration dict

                Returns:
                    LazyFrame with member SDOH profiles
        """
        logger.info("Calculating member SDOH profiles...")

        # Aggregate SDOH categories by member
        member_profile = sdoh_claims.group_by(["person_id"]).agg(
            [
                pl.col("sdoh_category").unique().alias("sdoh_risk_factors"),
                pl.col("sdoh_category").n_unique().alias("unique_sdoh_factors"),
                pl.count().alias("sdoh_claim_count"),
                pl.col("claim_end_date").min().alias("first_sdoh_date"),
                pl.col("claim_end_date").max().alias("last_sdoh_date"),
            ]
        )

        # Flag high SDOH risk (3+ unique factors)
        member_profile = member_profile.with_columns(
            [
                (pl.col("unique_sdoh_factors") >= 3).alias("is_high_sdoh_risk"),
                pl.when(pl.col("unique_sdoh_factors") >= 3)
                .then(pl.lit("high"))
                .when(pl.col("unique_sdoh_factors") >= 2)
                .then(pl.lit("moderate"))
                .when(pl.col("unique_sdoh_factors") >= 1)
                .then(pl.lit("identified"))
                .otherwise(pl.lit("none"))
                .alias("sdoh_risk_level"),
            ]
        )

        logger.info("Member SDOH profiles calculated")

        return member_profile

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_screening_rates(
        eligibility: pl.LazyFrame, sdoh_claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate SDOH screening rates.

                Args:
                    eligibility: Member eligibility
                    sdoh_claims: Claims with SDOH categories
                    config: Configuration dict

                Returns:
                    LazyFrame with screening rates
        """
        logger.info("Calculating SDOH screening rates...")

        measurement_year = config.get("measurement_year", 2024)

        # Total eligible members
        total_members = eligibility.filter(
            pl.col("enrollment_start_date").dt.year() <= measurement_year
        ).select(pl.col("person_id").n_unique().alias("total_members"))

        # Members with SDOH screening
        screened_members = sdoh_claims.filter(
            pl.col("sdoh_category").is_in(["sdoh_screening", "mental_health_screening"])
        ).select(pl.col("person_id").n_unique().alias("screened_members"))

        # Members with identified SDOH risk factors
        identified_members = sdoh_claims.filter(
            ~pl.col("sdoh_category").is_in(
                ["sdoh_screening", "mental_health_screening", "other_z_code"]
            )
        ).select(pl.col("person_id").n_unique().alias("identified_members"))

        # Combine metrics
        screening_rates = total_members.join(screened_members, how="cross").join(
            identified_members, how="cross"
        )

        screening_rates = screening_rates.with_columns(
            [
                (pl.col("screened_members") / pl.col("total_members") * 100).alias(
                    "screening_rate_pct"
                ),
                (pl.col("identified_members") / pl.col("total_members") * 100).alias(
                    "identification_rate_pct"
                ),
                (pl.col("identified_members") / pl.col("screened_members") * 100).alias(
                    "positive_screen_rate_pct"
                ),
            ]
        )

        logger.info("SDOH screening rates calculated")

        return screening_rates

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_risk_factor_prevalence(
        sdoh_claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate prevalence of each SDOH risk factor.

                Args:
                    sdoh_claims: Claims with SDOH categories
                    config: Configuration dict

                Returns:
                    LazyFrame with risk factor prevalence
        """
        logger.info("Calculating risk factor prevalence...")

        # Count unique members per SDOH category
        prevalence = sdoh_claims.group_by("sdoh_category").agg(
            [
                pl.col("person_id").n_unique().alias("member_count"),
                pl.count().alias("total_claims"),
            ]
        )

        # Sort by prevalence
        prevalence = prevalence.sort("member_count", descending=True)

        logger.info("Risk factor prevalence calculated")

        return prevalence

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=60.0)
    @profile_memory(log_result=True)
    def analyze_sdoh(
        claims: pl.LazyFrame, eligibility: pl.LazyFrame, config: dict[str, Any]
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Perform comprehensive SDOH analysis.

                Args:
                    claims: Medical claims
                    eligibility: Member eligibility
                    config: Configuration dict

                Returns:
                    Tuple of (sdoh_member_profile, risk_factor_prevalence, screening_rates, z_code_claims)
        """
        logger.info("Starting SDOH analysis...")

        # Identify Z-codes
        z_code_claims = SdohTransform.identify_z_codes(claims, config)

        # Categorize SDOH risk factors
        sdoh_claims = SdohTransform.categorize_sdoh_risk_factors(z_code_claims, config)

        # Calculate member SDOH profiles
        member_profile = SdohTransform.calculate_member_sdoh_profile(sdoh_claims, config)

        # Calculate risk factor prevalence
        prevalence = SdohTransform.calculate_risk_factor_prevalence(sdoh_claims, config)

        # Calculate screening rates
        screening_rates = SdohTransform.calculate_screening_rates(eligibility, sdoh_claims, config)

        logger.info("SDOH analysis complete")

        return member_profile, prevalence, screening_rates, sdoh_claims


logger.debug("Registered SDOH expression")
