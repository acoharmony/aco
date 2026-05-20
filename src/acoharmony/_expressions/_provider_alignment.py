# © 2025 HarmonyCares
# All rights reserved.

"""
Provider alignment expressions for extracting TIN-NPI combinations from participant roster.

Implements reusable expressions for:
- Extracting individual participant providers (for voluntary alignment/SVA)
- Extracting preferred providers (organizations for claims-based attribution)
- Filtering and cleaning provider data
- Building provider identifiers and names

These expressions support the participant_list transform pipeline.
"""

import polars as pl

from .._decor8 import expression
from ._registry import register_expression


@register_expression(
    "provider_alignment",
    schemas=["bronze", "silver"],
    dataset_types=["provider", "roster"],
    callable=False,
    description="Extract and process provider TIN-NPI combinations for alignment",
)
class ProviderAlignmentExpression:
    """
    Expressions for provider alignment data extraction and processing.

    Handles:
    - Individual participant extraction (individual_npis)
    - Preferred provider extraction (organization_npi)
    - Provider name construction
    - Facility provider filtering
    - TIN-NPI standardization
    """

    @staticmethod
    @expression(
        name="has_individual_npi",
        tier=["bronze", "silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def filter_has_individual_npi() -> pl.Expr:
        """
        Filter for rows with individual NPI populated.

        Individual NPIs are used for voluntary alignment (SVA) submissions.

        Returns:
            Expression that filters for non-null, non-empty individual_npi
        """
        return (pl.col("individual_npi").is_not_null()) & (pl.col("individual_npi") != "")

    @staticmethod
    @expression(
        name="has_organization_npi",
        tier=["bronze", "silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def filter_has_organization_npi() -> pl.Expr:
        """
        Filter for rows with organization NPI populated.

        Organization NPIs are used for claims-based attribution.

        Returns:
            Expression that filters for non-null, non-empty organization_npi
        """
        return (pl.col("organization_npi").is_not_null()) & (pl.col("organization_npi") != "")

    @staticmethod
    @expression(
        name="clean_individual_npi",
        tier=["bronze", "silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def clean_individual_npi_expr() -> pl.Expr:
        """
        Clean individual NPI value by stripping whitespace.

        Returns:
            Expression that strips whitespace from individual_npi and aliases to npi
        """
        return pl.col("individual_npi").str.strip_chars().alias("npi")

    @staticmethod
    @expression(
        name="provider_name_from_parts",
        tier=["bronze", "silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def build_provider_name_from_parts() -> pl.Expr:
        """
        Build provider name from first_name and last_name.

        Concatenates first_name and last_name with space separator.

        Returns:
            Expression that builds full name
        """
        return pl.concat_str([pl.col("first_name"), pl.col("last_name")], separator=" ").alias(
            "provider_name"
        )

    @staticmethod
    @expression(
        name="individual_participant_columns",
        tier=["bronze", "silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def select_individual_participant_columns() -> list[pl.Expr]:
        """
        Build select expressions for individual participant providers.

        Returns:
            List of expressions selecting standardized columns
        """
        return [
            pl.col("base_provider_tin").alias("tin"),
            pl.col("individual_npi").alias("npi"),
            pl.lit("Individual Participant").alias("provider_category"),
            pl.col("provider_type"),
            pl.col("provider_class"),
            pl.col("individual_first_name").alias("first_name"),
            pl.col("individual_last_name").alias("last_name"),
            pl.concat_str(
                [pl.col("individual_first_name"), pl.col("individual_last_name")], separator=" "
            ).alias("provider_name"),
            pl.col("entity_legal_business_name").alias("organization"),
            pl.col("email"),
            pl.col("entity_id"),
            pl.col("entity_tin"),
            pl.col("performance_year"),
            pl.col("specialty"),
        ]

    @staticmethod
    @expression(
        name="preferred_provider_columns",
        tier=["bronze", "silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def select_preferred_provider_columns() -> list[pl.Expr]:
        """
        Build select expressions for preferred (organization) providers.

        Returns:
            List of expressions selecting standardized columns
        """
        return [
            pl.col("base_provider_tin").alias("tin"),
            pl.col("organization_npi").alias("npi"),
            pl.lit("Preferred Provider").alias("provider_category"),
            pl.col("provider_type"),
            pl.col("provider_class"),
            pl.lit(None).cast(pl.Utf8).alias("first_name"),
            pl.lit(None).cast(pl.Utf8).alias("last_name"),
            pl.col("provider_legal_business_name").alias("provider_name"),
            pl.col("entity_legal_business_name").alias("organization"),
            pl.col("email"),
            pl.col("entity_id"),
            pl.col("entity_tin"),
            pl.col("performance_year"),
            pl.col("specialty"),
        ]

    @staticmethod
    @expression(
        name="filter_non_facility_providers",
        tier=["bronze", "silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def filter_non_facility_providers() -> pl.Expr:
        """
        Filter out facility and institutional providers.

        Facility providers should not be included in TIN-NPI mapping
        for individual attribution.

        Returns:
            Expression that excludes facility providers
        """
        return ~pl.col("provider_type").cast(pl.Utf8).str.starts_with("Facility and Institutional")

    @staticmethod
    @expression(
        name="tin_standardized",
        tier=["bronze", "silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def standardize_tin() -> pl.Expr:
        """
        Standardize TIN format.

        Ensures TIN is string, strips whitespace and padding.

        Returns:
            Expression that standardizes TIN
        """
        return pl.col("base_provider_tin").cast(pl.Utf8).str.strip_chars().alias("tin")

    @staticmethod
    @expression(
        name="npi_standardized",
        tier=["bronze", "silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def standardize_npi() -> pl.Expr:
        """
        Standardize NPI format.

        Ensures NPI is string, strips whitespace and padding.

        Returns:
            Expression that standardizes NPI
        """
        return pl.col("npi").cast(pl.Utf8).str.strip_chars().str.zfill(10).alias("npi")

    @staticmethod
    @expression(
        name="provider_category_label",
        tier=["bronze", "silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def build_provider_category_label() -> pl.Expr:
        """
        Build provider category label based on source.

        Returns:
            Expression that determines provider category
        """
        return (
            pl.when(pl.col("individual_npi").is_not_null() & (pl.col("individual_npi") != ""))
            .then(pl.lit("Individual Participant"))
            .when(pl.col("organization_npi").is_not_null() & (pl.col("organization_npi") != ""))
            .then(pl.lit("Preferred Provider"))
            .otherwise(pl.lit("Unknown"))
            .alias("provider_category")
        )
