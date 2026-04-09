# © 2025 HarmonyCares
# All rights reserved.

"""
Preventive Services Analysis Expression.

Provides comprehensive preventive care tracking:
- Cancer screening rates (breast, colorectal, cervical, lung)
- Immunization compliance (flu, pneumonia, shingles, COVID)
- Wellness visit tracking (AWV, annual physical)
- Preventive screening gaps
- Age-appropriate screening identification
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .._decor8 import profile_memory, timeit, traced, transform
from .._log import LogWriter

logger = LogWriter("transforms.preventive_services")


@transform(name="preventive_services", tier=["gold"])
class PreventiveServicesTransform:
    """
    Comprehensive preventive services tracking and analysis.
    """

    PREVENTIVE_SERVICE_CODES = {
        "annual_wellness_visit": ["G0438", "G0439"],  # Initial AWV, Subsequent AWV
        "annual_physical": [
            "99381",
            "99382",
            "99383",
            "99384",
            "99385",
            "99386",
            "99387",
            "99391",
            "99392",
            "99393",
            "99394",
            "99395",
            "99396",
            "99397",
        ],
        "mammogram": ["77065", "77066", "77067"],
        "colorectal_screening_colonoscopy": ["G0105", "G0121", "45378"],
        "colorectal_screening_fecal": ["G0328", "82270", "82274"],
        "cervical_screening_pap": [
            "88141",
            "88142",
            "88143",
            "88147",
            "88148",
            "88150",
            "88152",
            "88153",
            "88154",
            "88164",
            "88165",
            "88166",
            "88167",
            "88174",
            "88175",
        ],
        "cervical_screening_hpv": ["87624", "87625"],
        "lung_screening": ["G0297", "71271"],
        "prostate_screening_psa": ["G0103", "84153", "84154"],
        "flu_vaccine": [
            "90630",
            "90653",
            "90654",
            "90655",
            "90656",
            "90657",
            "90658",
            "90660",
            "90661",
            "90662",
            "90673",
            "90674",
            "90685",
            "90686",
            "90687",
            "90688",
        ],
        "pneumonia_vaccine": ["90670", "90732"],
        "shingles_vaccine": ["90750", "90736"],
        "covid_vaccine": [
            "91300",
            "91301",
            "91302",
            "91303",
            "91304",
            "91305",
            "91306",
            "91307",
            "91308",
            "91309",
        ],
        "tetanus_vaccine": ["90714", "90715"],
        "bone_density": ["77080", "77081"],
        "abdominal_aortic_aneurysm": ["G0389"],
        "hepatitis_c_screening": ["G0472"],
        "hiv_screening": ["G0432", "G0433", "G0435"],
        "depression_screening": ["G0444"],
        "obesity_screening": ["G0447"],
        "tobacco_cessation": ["99406", "99407", "G0436", "G0437"],
    }

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_preventive_services(claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Identify preventive services from procedure codes.

                Args:
                    claims: Medical claims
                    config: Configuration dict

                Returns:
                    LazyFrame with preventive service identifications
        """
        logger.info("Identifying preventive services...")

        measurement_year = config.get("measurement_year", 2024)

        prev_claims = claims.filter(
            (pl.col("claim_end_date").dt.year() == measurement_year)
            & (pl.col("claim_type") == "professional")
        )

        all_prev_codes = []
        for codes in PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES.values():
            all_prev_codes.extend(codes)

        prev_claims = prev_claims.filter(
            pl.col("procedure_code").cast(pl.Utf8).is_in(all_prev_codes)
        )

        logger.info(f"Identified {prev_claims.collect().height:,} preventive service claims")

        return prev_claims

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def categorize_preventive_services(
        prev_claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Categorize preventive services by type.

                Args:
                    prev_claims: Claims with preventive services
                    config: Configuration dict

                Returns:
                    LazyFrame with service categories
        """
        logger.info("Categorizing preventive services...")

        categorized = prev_claims.with_columns(
            [
                pl.when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "annual_wellness_visit"
                        ]
                    )
                )
                .then(pl.lit("annual_wellness_visit"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["annual_physical"]
                    )
                )
                .then(pl.lit("annual_physical"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["mammogram"]
                    )
                )
                .then(pl.lit("mammogram"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "colorectal_screening_colonoscopy"
                        ]
                    )
                )
                .then(pl.lit("colorectal_screening_colonoscopy"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "colorectal_screening_fecal"
                        ]
                    )
                )
                .then(pl.lit("colorectal_screening_fecal"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "cervical_screening_pap"
                        ]
                    )
                )
                .then(pl.lit("cervical_screening_pap"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "cervical_screening_hpv"
                        ]
                    )
                )
                .then(pl.lit("cervical_screening_hpv"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["lung_screening"]
                    )
                )
                .then(pl.lit("lung_screening"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "prostate_screening_psa"
                        ]
                    )
                )
                .then(pl.lit("prostate_screening_psa"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["flu_vaccine"]
                    )
                )
                .then(pl.lit("flu_vaccine"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["pneumonia_vaccine"]
                    )
                )
                .then(pl.lit("pneumonia_vaccine"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["shingles_vaccine"]
                    )
                )
                .then(pl.lit("shingles_vaccine"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["covid_vaccine"]
                    )
                )
                .then(pl.lit("covid_vaccine"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["tetanus_vaccine"]
                    )
                )
                .then(pl.lit("tetanus_vaccine"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["bone_density"]
                    )
                )
                .then(pl.lit("bone_density"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "abdominal_aortic_aneurysm"
                        ]
                    )
                )
                .then(pl.lit("abdominal_aortic_aneurysm"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "hepatitis_c_screening"
                        ]
                    )
                )
                .then(pl.lit("hepatitis_c_screening"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["hiv_screening"]
                    )
                )
                .then(pl.lit("hiv_screening"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "depression_screening"
                        ]
                    )
                )
                .then(pl.lit("depression_screening"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["obesity_screening"]
                    )
                )
                .then(pl.lit("obesity_screening"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["tobacco_cessation"]
                    )
                )
                .then(pl.lit("tobacco_cessation"))
                .otherwise(pl.lit("other_preventive"))
                .alias("preventive_service_type"),
                pl.when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "annual_wellness_visit"
                        ]
                    )
                    | pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["annual_physical"]
                    )
                )
                .then(pl.lit("wellness_visit"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["mammogram"]
                    )
                    | pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "colorectal_screening_colonoscopy"
                        ]
                    )
                    | pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "colorectal_screening_fecal"
                        ]
                    )
                    | pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "cervical_screening_pap"
                        ]
                    )
                    | pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "cervical_screening_hpv"
                        ]
                    )
                    | pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["lung_screening"]
                    )
                    | pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES[
                            "prostate_screening_psa"
                        ]
                    )
                )
                .then(pl.lit("cancer_screening"))
                .when(
                    pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["flu_vaccine"]
                    )
                    | pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["pneumonia_vaccine"]
                    )
                    | pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["shingles_vaccine"]
                    )
                    | pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["covid_vaccine"]
                    )
                    | pl.col("procedure_code").is_in(
                        PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES["tetanus_vaccine"]
                    )
                )
                .then(pl.lit("immunization"))
                .otherwise(pl.lit("other_screening"))
                .alias("preventive_service_category"),
            ]
        )

        logger.info("Preventive services categorized")

        return categorized

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_member_preventive_profile(
        prev_claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate member-level preventive service profiles.

                Args:
                    prev_claims: Claims with preventive services
                    config: Configuration dict

                Returns:
                    LazyFrame with member preventive profiles
        """
        logger.info("Calculating member preventive profiles...")
        member_profile = prev_claims.group_by("person_id").agg(
            [
                pl.col("preventive_service_type").unique().alias("preventive_services_received"),
                pl.col("preventive_service_type").n_unique().alias("unique_preventive_services"),
                pl.count().alias("total_preventive_services"),
                pl.col("preventive_service_category")
                .is_in(["wellness_visit"])
                .any()
                .alias("has_wellness_visit"),
                pl.col("preventive_service_category")
                .is_in(["cancer_screening"])
                .any()
                .alias("has_cancer_screening"),
                pl.col("preventive_service_category")
                .is_in(["immunization"])
                .any()
                .alias("has_immunization"),
                pl.col("preventive_service_type")
                .is_in(["flu_vaccine"])
                .any()
                .alias("has_flu_vaccine"),
            ]
        )

        member_profile = member_profile.with_columns(
            [
                (
                    pl.col("has_wellness_visit")
                    & pl.col("has_cancer_screening")
                    & pl.col("has_immunization")
                ).alias("fully_engaged"),
                pl.when(pl.col("unique_preventive_services") >= 5)
                .then(pl.lit("high"))
                .when(pl.col("unique_preventive_services") >= 3)
                .then(pl.lit("moderate"))
                .when(pl.col("unique_preventive_services") >= 1)
                .then(pl.lit("low"))
                .otherwise(pl.lit("none"))
                .alias("preventive_care_engagement"),
            ]
        )

        logger.info("Member preventive profiles calculated")

        return member_profile

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_screening_compliance(
        eligibility: pl.LazyFrame, prev_claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate screening compliance rates by age and gender.

                Args:
                    eligibility: Member eligibility
                    prev_claims: Preventive service claims
                    config: Configuration dict

                Returns:
                    LazyFrame with screening compliance rates
        """
        logger.info("Calculating screening compliance rates...")

        measurement_year = config.get("measurement_year", 2024)

        eligible_members = eligibility.filter(
            pl.col("enrollment_start_date").dt.year() <= measurement_year
        ).select(["person_id", "age", "gender"])

        eligible_mammogram = eligible_members.filter(
            (pl.col("gender") == "F") & (pl.col("age").is_between(50, 74))
        ).select(pl.count().alias("eligible_mammogram"))

        received_mammogram = prev_claims.filter(
            pl.col("preventive_service_type") == "mammogram"
        ).select(pl.col("person_id").n_unique().alias("received_mammogram"))

        eligible_colorectal = eligible_members.filter(pl.col("age").is_between(50, 75)).select(
            pl.count().alias("eligible_colorectal")
        )

        received_colorectal = prev_claims.filter(
            pl.col("preventive_service_type").is_in(
                ["colorectal_screening_colonoscopy", "colorectal_screening_fecal"]
            )
        ).select(pl.col("person_id").n_unique().alias("received_colorectal"))

        total_members = eligible_members.select(pl.count().alias("total_members"))

        received_flu = prev_claims.filter(
            pl.col("preventive_service_type") == "flu_vaccine"
        ).select(pl.col("person_id").n_unique().alias("received_flu"))

        compliance = (
            total_members.join(eligible_mammogram, how="cross")
            .join(received_mammogram, how="cross")
            .join(eligible_colorectal, how="cross")
            .join(received_colorectal, how="cross")
            .join(received_flu, how="cross")
        )

        compliance = compliance.with_columns(
            [
                (pl.col("received_mammogram") / pl.col("eligible_mammogram") * 100).alias(
                    "mammogram_compliance_pct"
                ),
                (pl.col("received_colorectal") / pl.col("eligible_colorectal") * 100).alias(
                    "colorectal_compliance_pct"
                ),
                (pl.col("received_flu") / pl.col("total_members") * 100).alias(
                    "flu_vaccine_rate_pct"
                ),
            ]
        )

        logger.info("Screening compliance calculated")

        return compliance

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=60.0)
    @profile_memory(log_result=True)
    def analyze_preventive_services(
        claims: pl.LazyFrame, eligibility: pl.LazyFrame, config: dict[str, Any]
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Perform comprehensive preventive services analysis.

                Args:
                    claims: Medical claims
                    eligibility: Member eligibility
                    config: Configuration dict

                Returns:
                    Tuple of (member_profile, service_utilization, compliance_rates, prev_claims)
        """
        logger.info("Starting preventive services analysis...")

        prev_claims = PreventiveServicesTransform.identify_preventive_services(claims, config)

        prev_categorized = PreventiveServicesTransform.categorize_preventive_services(
            prev_claims, config
        )

        member_profile = PreventiveServicesTransform.calculate_member_preventive_profile(
            prev_categorized, config
        )

        service_utilization = (
            prev_categorized.group_by("preventive_service_type")
            .agg(
                [
                    pl.col("person_id").n_unique().alias("member_count"),
                    pl.count().alias("service_count"),
                ]
            )
            .sort("member_count", descending=True)
        )

        compliance_rates = PreventiveServicesTransform.calculate_screening_compliance(
            eligibility, prev_categorized, config
        )

        logger.info("Preventive services analysis complete")

        return member_profile, service_utilization, compliance_rates, prev_categorized


logger.debug("Registered preventive services expression")
