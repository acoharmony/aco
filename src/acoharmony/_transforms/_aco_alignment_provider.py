# © 2025 HarmonyCares
# All rights reserved.

"""
Provider attribution transform for ACO alignment pipeline.

Applies provider attribution logic by building from last_ffs_service and provider_list.
Idempotent and schema-driven.
"""

from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from .._expressions._provider_attribution import ProviderAttributionExpression


@transform(name="aco_alignment_provider", tier=["silver", "gold"], sql_enabled=True)
@transform_method(enable_composition=True, threshold=5.0)
def apply_transform(
    df: pl.LazyFrame, schema: dict, catalog: Any, logger: Any, force: bool = False
) -> pl.LazyFrame:
    """
    Apply provider attribution logic.

    This transform:
    1. Checks idempotency (_provider_attributed flag)
    2. Builds MSSP provider attribution from last_ffs_service
    3. Builds REACH provider attribution from voluntary_alignment/BAR
    4. Determines aligned provider based on current_program
    5. Marks as processed

    Args:
        df: Alignment LazyFrame
        schema: Schema config
        catalog: Catalog for accessing provider attribution sources
        logger: Logger instance
        force: Force reprocessing

    Returns:
        pl.LazyFrame: Transformed data with provider attribution
    """
    # Idempotency check
    if not force and "_provider_attributed" in df.collect_schema().names():
        logger.info("Provider attribution already applied, skipping")
        return df

    logger.info("Applying provider attribution transform")

    # Log input row count
    input_count = df.select(pl.len()).collect().item()
    logger.info(f"Provider input rows: {input_count:,}")

    # Check if required sources are available
    last_ffs = catalog.scan_table("last_ffs_service")
    provider_list = catalog.scan_table("participant_list")  # Use participant_list schema

    if last_ffs is not None and provider_list is not None:
        logger.info("Building provider attribution from last_ffs_service and participant_list")

        # ============================================================
        # MSSP PROVIDER ATTRIBUTION
        # ============================================================
        logger.info("Building MSSP provider attribution")

        # Join last FFS service (bene_mbi → current_mbi)
        mssp_providers = last_ffs.select(
            [
                pl.col("bene_mbi").alias("current_mbi"),
                "last_ffs_tin",
                "last_ffs_npi",
                "last_ffs_date",
            ]
        )

        # Join provider list to get full provider details
        # Map participant_list columns to expected provider columns
        provider_details = provider_list.select(
            [
                pl.col("base_provider_tin").alias("billing_tin"),
                pl.col("individual_npi").alias("individual_npis"),
                pl.col("provider_type"),
                pl.col("provider_class"),
                pl.col("provider_legal_business_name").alias("tin_legal_bus_name"),
                pl.col("individual_first_name").alias("first_name"),
                pl.col("individual_last_name").alias("last_name"),
            ]
        )

        # Create MSSP provider attribution
        mssp_attribution = (
            mssp_providers.join(
                provider_details,
                left_on=["last_ffs_tin", "last_ffs_npi"],
                right_on=["billing_tin", "individual_npis"],
                how="left",
            )
            .with_columns(
                ProviderAttributionExpression.build_mssp_provider_select_expr()
                + [ProviderAttributionExpression.build_mssp_provider_name_expr()]
            )
            .select(
                [
                    "current_mbi",
                    "mssp_tin",
                    "mssp_npi",
                    "mssp_provider_name",
                ]
            )
            # Deduplicate to one row per MBI (prevent cartesian product)
            .unique(subset=["current_mbi"], keep="first", maintain_order=True)
        )

        # Join MSSP attribution to full dataframe (preserves all columns)
        result = df.join(mssp_attribution, on="current_mbi", how="left")

        # ============================================================
        # REACH PROVIDER ATTRIBUTION
        # ============================================================
        logger.info("Building REACH provider attribution")

        reach_provider = None

        # Option 1: BAR file (if available)
        bar = catalog.scan_table("bar")
        if bar is not None:
            logger.info("Using BAR file for REACH attribution")
            bar_providers = bar.select(
                [
                    pl.col("bene_mbi").alias("current_mbi"),
                    ProviderAttributionExpression.build_reach_attribution_type_bar_expr(),
                ]
            )
            reach_provider = bar_providers

        # Option 2: voluntary_alignment (SVA/PBVAR)
        voluntary = catalog.scan_table("voluntary_alignment")
        if voluntary is not None:
            logger.info("Using voluntary_alignment for REACH attribution")
            vol_providers = voluntary.select(
                [
                    pl.col("current_mbi"),
                    pl.col("sva_provider_tin").alias("reach_tin"),
                    pl.col("sva_provider_npi").alias("reach_npi"),
                    pl.col("sva_provider_name").alias("reach_provider_name"),
                    ProviderAttributionExpression.build_reach_attribution_type_vol_expr(),
                ]
            )

            # Merge with BAR providers if available
            if reach_provider is not None:
                # Join voluntary alignment provider info to BAR attribution type
                reach_provider = reach_provider.join(
                    vol_providers.select(["current_mbi", "reach_tin", "reach_npi", "reach_provider_name"]),
                    on="current_mbi",
                    how="left",
                )
            else:
                reach_provider = vol_providers

        # Join REACH attribution to base
        if reach_provider is not None:
            # Deduplicate to one row per MBI before joining (prevent cartesian product)
            reach_provider = reach_provider.unique(subset=["current_mbi"], keep="first", maintain_order=True)
            result = result.join(reach_provider, on="current_mbi", how="left")
        else:
            logger.info("No REACH provider sources available, adding null columns")
            # Add null columns
            result = result.with_columns(
                [
                    pl.lit(None).alias("reach_tin"),
                    pl.lit(None).alias("reach_npi"),
                    pl.lit(None).alias("reach_provider_name"),
                    pl.lit(None).alias("reach_attribution_type"),
                ]
            )

        # ============================================================
        # ALIGNED PROVIDER (Current Program)
        # ============================================================
        logger.info("Determining aligned provider based on current_program")

        # Apply aligned provider expressions using expression builders
        result = result.with_columns(
            [
                ProviderAttributionExpression.build_aligned_provider_tin_expr(),
                ProviderAttributionExpression.build_aligned_provider_npi_expr(),
                ProviderAttributionExpression.build_aligned_provider_org_expr(),
                ProviderAttributionExpression.build_aligned_practitioner_name_expr(),
                ProviderAttributionExpression.build_latest_aco_id_expr(),
            ]
        )

        # Log after provider join
        after_provider_join = result.select(pl.len()).collect().item()
        logger.info(f"After provider join: {after_provider_join:,} rows")
    else:
        logger.warning("last_ffs_service or participant_list not available, adding null provider columns")
        # Add null columns for all provider fields
        result = df.with_columns(
            [
                pl.lit(None).alias("mssp_tin"),
                pl.lit(None).alias("mssp_npi"),
                pl.lit(None).alias("mssp_provider_name"),
                pl.lit(None).alias("reach_tin"),
                pl.lit(None).alias("reach_npi"),
                pl.lit(None).alias("reach_provider_name"),
                pl.lit(None).alias("reach_attribution_type"),
                pl.lit(None).alias("aligned_provider_tin"),
                pl.lit(None).alias("aligned_provider_npi"),
                pl.lit(None).alias("aligned_provider_org"),
                pl.lit(None).alias("aligned_practitioner_name"),
                pl.col("current_aco_id").alias("latest_aco_id"),
            ]
        )

    # Mark as processed
    result = result.with_columns([pl.lit(True).alias("_provider_attributed")])

    logger.info("Provider attribution transform complete")
    return result
