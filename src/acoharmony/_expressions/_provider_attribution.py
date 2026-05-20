# © 2025 HarmonyCares
# All rights reserved.

"""
Provider Attribution expression for MSSP and REACH provider assignment.

Implements business logic for determining provider attribution across ACO programs:

MSSP Attribution:
- Uses MOST RECENT FFS claim from valid provider (from provider_list)
- Prioritizes "Preferred Provider" from provider_list
- Falls back to "Individual Participant" if no Preferred Provider
- Pulls TIN/NPI from last_ffs_service + provider_list

REACH Attribution:
- BAR file: Contains both claims-based AND voluntary alignment provider info
  - claims_based_flag = "Y" → Claims-based attribution
  - voluntary_alignment_type != null → Voluntary attribution
- ALR file: ALWAYS claims-based (MSSP program)
- SVA/PBVAR: Most recent voluntary alignment provider
  - Prioritize by file date (most recent wins)

This expression is REUSABLE across multiple alignment workflows.
"""

import polars as pl

from .._decor8 import expression
from ._registry import register_expression


@register_expression(
    "provider_attribution",
    schemas=["silver", "gold"],
    dataset_types=["alignment", "provider"],
    callable=False,
    description="Determine MSSP and REACH provider attribution from multiple sources",
)
class ProviderAttributionExpression:
    """
    Expression for determining provider attribution for MSSP and REACH programs.

        Combines data from:
        - last_ffs_service: Most recent FFS provider visit
        - provider_list: Valid ACO provider roster
        - BAR: REACH alignment with provider info
        - voluntary_alignment: SVA/PBVAR provider data

        Returns per-beneficiary provider attribution with program-specific logic.
    """

    @staticmethod
    @expression(name="mssp_provider_name", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_mssp_provider_name_expr() -> pl.Expr:
        """
        Build expression for MSSP provider name.

        Constructs provider name from first_name/last_name or falls back to organization name.

        Returns:
            Expression that builds provider name
        """
        return (
            pl.when(pl.col("first_name").is_not_null() & pl.col("last_name").is_not_null())
            .then(
                pl.concat_str(
                    [pl.col("last_name"), pl.lit(", "), pl.col("first_name")],
                    ignore_nulls=True,
                )
            )
            .otherwise(pl.col("tin_legal_bus_name"))
            .alias("mssp_provider_name")
        )

    @staticmethod
    @expression(name="mssp_provider_columns", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_mssp_provider_select_expr() -> list[pl.Expr]:
        """
        Build select expressions for MSSP provider attribution.

        Returns:
            List of expressions for MSSP provider columns
        """
        return [
            pl.col("last_ffs_tin").alias("mssp_tin"),
            pl.col("last_ffs_npi").alias("mssp_npi"),
        ]

    @staticmethod
    @expression(name="reach_attribution_type_bar", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_reach_attribution_type_bar_expr() -> pl.Expr:
        """
        Build expression for REACH attribution type from BAR file.

        Determines if alignment is voluntary or claims-based from BAR flags.

        Returns:
            Expression that determines attribution type
        """
        return (
            pl.when(pl.col("voluntary_alignment_type").is_not_null())
            .then(pl.lit("Voluntary"))
            .when(pl.col("claims_based_flag") == "Y")
            .then(pl.lit("Claims-based"))
            .otherwise(pl.lit("Unknown"))
            .alias("reach_attribution_type")
        )

    @staticmethod
    @expression(name="reach_attribution_type_vol", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_reach_attribution_type_vol_expr() -> pl.Expr:
        """
        Build expression for REACH attribution type from voluntary alignment.

        Determines if alignment is voluntary (SVA) or claims-based (PBVAR).

        Returns:
            Expression that determines attribution type
        """
        return (
            pl.when(pl.col("sva_signature_count") > 0)
            .then(pl.lit("Voluntary"))
            .when(pl.col("pbvar_aligned"))
            .then(pl.lit("Claims-based"))
            .otherwise(None)
            .alias("reach_attribution_type")
        )

    @staticmethod
    @expression(name="aligned_provider_tin", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_aligned_provider_tin_expr() -> pl.Expr:
        """
        Build expression for aligned provider TIN based on current program.

        Returns:
            Expression that selects provider TIN based on program
        """
        return (
            pl.when(pl.col("current_program") == "REACH")
            .then(pl.col("reach_tin"))
            .when(pl.col("current_program") == "MSSP")
            .then(pl.col("mssp_tin"))
            .otherwise(None)
            .alias("aligned_provider_tin")
        )

    @staticmethod
    @expression(name="aligned_provider_npi", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_aligned_provider_npi_expr() -> pl.Expr:
        """
        Build expression for aligned provider NPI based on current program.

        Returns:
            Expression that selects provider NPI based on program
        """
        return (
            pl.when(pl.col("current_program") == "REACH")
            .then(pl.col("reach_npi"))
            .when(pl.col("current_program") == "MSSP")
            .then(pl.col("mssp_npi"))
            .otherwise(None)
            .alias("aligned_provider_npi")
        )

    @staticmethod
    @expression(name="aligned_provider_org", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_aligned_provider_org_expr() -> pl.Expr:
        """
        Build expression for aligned provider organization based on current program.

        Returns:
            Expression that selects provider organization based on program
        """
        return (
            pl.when(pl.col("current_program") == "REACH")
            .then(pl.col("reach_provider_name"))
            .when(pl.col("current_program") == "MSSP")
            .then(pl.col("mssp_provider_name"))
            .otherwise(None)
            .alias("aligned_provider_org")
        )

    @staticmethod
    @expression(name="aligned_practitioner_name", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_aligned_practitioner_name_expr() -> pl.Expr:
        """
        Build expression for aligned practitioner name based on current program.

        Returns:
            Expression that selects practitioner name based on program
        """
        return (
            pl.when(pl.col("current_program") == "REACH")
            .then(pl.col("reach_provider_name"))
            .when(pl.col("current_program") == "MSSP")
            .then(pl.col("mssp_provider_name"))
            .otherwise(None)
            .alias("aligned_practitioner_name")
        )

    @staticmethod
    @expression(name="latest_aco_id", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_latest_aco_id_expr() -> pl.Expr:
        """
        Build expression for latest ACO ID.

        Returns:
            Expression that aliases current_aco_id to latest_aco_id
        """
        return pl.col("current_aco_id").alias("latest_aco_id")

    @staticmethod
    @expression(name="provider_attribution_final", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_provider_attribution_final_select() -> list[pl.Expr]:
        """
        Build column list for final provider attribution output.

        Returns:
            List of Polars column expressions for final select
        """
        return [
            pl.col("current_mbi"),
            pl.col("mssp_tin"),
            pl.col("mssp_npi"),
            pl.col("mssp_provider_name"),
            pl.col("reach_tin"),
            pl.col("reach_npi"),
            pl.col("reach_provider_name"),
            pl.col("reach_attribution_type"),
            pl.col("aligned_provider_tin"),
            pl.col("aligned_provider_npi"),
            pl.col("aligned_provider_org"),
            pl.col("aligned_practitioner_name"),
            pl.col("latest_aco_id"),
        ]

