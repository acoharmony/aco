# © 2025 HarmonyCares
# All rights reserved.

"""
Demographics join transform for ACO alignment pipeline.

Applies deduplicated beneficiary demographics join using expression builders.
Idempotent and schema-driven.
"""

from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from .._expressions._aco_demographics_join import (
    build_demographics_select_expr,
    build_zip5_expr,
)


@transform(name="aco_alignment_demographics", tier=["silver", "gold"], sql_enabled=True)
@transform_method(enable_composition=True, threshold=5.0)
def apply_transform(
    df: pl.LazyFrame, schema: dict, catalog: Any, logger: Any, force: bool = False
) -> pl.LazyFrame:
    """
    Apply deduplicated beneficiary demographics join.

    This transform:
    1. Checks idempotency (_demographics_joined flag)
    2. Joins int_beneficiary_demographics_deduped data
    3. Adds bene_county if available
    4. Adds bene_zip_5 for office matching
    5. Marks as processed

    Args:
        df: Base alignment LazyFrame
        schema: Schema config with column names
        catalog: Catalog for accessing int_beneficiary_demographics_deduped source
        logger: Logger instance
        force: Force reprocessing

    Returns:
        pl.LazyFrame: Transformed data with demographics joined
    """
    # Idempotency check - use schema() instead of collect_schema() to avoid file reads
    schema_names = df.schema.names()
    if not force and "_demographics_joined" in schema_names:
        logger.info("Demographics already joined, skipping")
        return df

    logger.info("Applying demographics join transform")

    # Get silver path for scanning deduplicated demographics
    from ..medallion import MedallionLayer

    silver_path = catalog.storage_config.get_path(MedallionLayer.SILVER)

    # Scan deduplicated demographics directly from silver parquet file
    try:
        demo_df = pl.scan_parquet(silver_path / "int_beneficiary_demographics_deduped.parquet")
    except Exception as e:
        raise ValueError(
            f"int_beneficiary_demographics_deduped source not found - required for aco_alignment pipeline. Error: {e}"
        ) from e

    # Build select expressions (includes rename of current_bene_mbi_id to current_mbi)
    select_exprs = build_demographics_select_expr()

    # Add county if available in source
    demo_schema = demo_df.collect_schema().names()
    if "bene_fips_cnty_cd" in demo_schema:
        select_exprs.append(pl.col("bene_fips_cnty_cd").alias("bene_county"))

    # Select demographics columns
    demo_select = demo_df.select(select_exprs)

    # Join to base
    result = df.join(demo_select, on="current_mbi", how="left", suffix="_demo")

    # Add ZIP-5 for office matching
    result = result.with_columns([build_zip5_expr()])

    # Fill missing demographics from BAR files (for beneficiaries not in int_beneficiary_demographics_deduped)
    # This handles REACH beneficiaries who appear in BAR but not in demographics table
    bar_df = catalog.scan_table("bar")
    if bar_df is not None:
        # Get most recent BAR address data per MBI
        bar_address = (
            bar_df
            .group_by("bene_mbi")
            .agg([
                pl.col("file_date").max().alias("max_file_date"),
            ])
            .join(
                bar_df,
                left_on=["bene_mbi", "max_file_date"],
                right_on=["bene_mbi", "file_date"],
                how="left"
            )
            .select([
                pl.col("bene_mbi").alias("current_mbi"),
                pl.col("bene_zip_5").alias("bar_zip_5"),
                pl.col("bene_state").alias("bar_state"),
                pl.col("bene_county_fips").alias("bar_county"),
            ])
            .unique(subset=["current_mbi"], keep="first")
        )

        # Join BAR address data
        result = result.join(bar_address, on="current_mbi", how="left")

        # Coalesce to fill nulls
        result = result.with_columns([
            # Fill bene_zip with BAR data if null, then recalculate bene_zip_5
            pl.coalesce([pl.col("bene_zip"), pl.col("bar_zip_5")]).alias("bene_zip"),
            # Fill state if null
            pl.coalesce([pl.col("bene_state"), pl.col("bar_state")]).alias("bene_state"),
        ])

        # Recalculate bene_zip_5 after filling bene_zip
        result = result.with_columns([
            pl.col("bene_zip").str.slice(0, 5).alias("bene_zip_5"),
        ])

        # Fill county if the column exists
        schema_cols = result.collect_schema().names()
        if "bene_county" in schema_cols:
            result = result.with_columns([
                pl.coalesce([pl.col("bene_county"), pl.col("bar_county")]).alias("bene_county")
            ])

        # Drop temporary BAR columns
        result = result.drop(["bar_zip_5", "bar_state", "bar_county"])
        logger.info("Filled missing demographics from BAR data")

    # Mark as processed
    result = result.with_columns([pl.lit(True).alias("_demographics_joined")])

    logger.info("Demographics join transform complete")
    return result
