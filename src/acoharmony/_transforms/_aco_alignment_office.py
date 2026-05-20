# © 2025 HarmonyCares
# All rights reserved.

"""
Office location matching transform for ACO alignment pipeline.

Applies office location matching using direct and fuzzy strategies.
Uses conditional logic based on data state. Idempotent and schema-driven.
"""

from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from .._expressions._aco_office_match import (
    build_direct_office_select_expr,
    build_fuzzy_office_select_expr,
    build_office_location_alias_expr,
)


@transform(name="aco_alignment_office", tier=["silver", "gold"], sql_enabled=True)
@transform_method(enable_composition=True, threshold=5.0)
def apply_transform(
    df: pl.LazyFrame, schema: dict, catalog: Any, logger: Any, force: bool = False
) -> pl.LazyFrame:
    """
    Apply office location matching with direct and fuzzy strategies.

    This transform:
    1. Checks idempotency (_office_matched flag)
    2. ALWAYS applies direct match (ZIPs with office_name populated)
    3. Checks data state: are there unmatched records?
    4. CONDITIONALLY applies fuzzy match (shortest distance) if needed
    5. Aliases market to office_location
    6. Marks as processed

    Args:
        df: Alignment LazyFrame with bene_zip_5 column
        schema: Schema config
        catalog: Catalog for accessing office_zip source
        logger: Logger instance
        force: Force reprocessing

    Returns:
        pl.LazyFrame: Transformed data with office locations matched
    """
    # Idempotency check
    if not force and "_office_matched" in df.collect_schema().names():
        logger.info("Office matching already applied, skipping")
        return df

    logger.info("Applying office matching transform")

    # Log input row count
    input_count = df.select(pl.len()).collect().item()
    logger.info(f"Input rows: {input_count:,}")

    # Get office_zip source from catalog
    office_zip_df = catalog.scan_table("office_zip")
    if office_zip_df is None:
        logger.warning("office_zip source not found, skipping office matching")
        return df.with_columns([pl.lit(True).alias("_office_matched")])

    # Separate into direct and fuzzy sources
    direct_office = office_zip_df.filter(pl.col("office_name").is_not_null()).select(
        build_direct_office_select_expr()
    )

    fuzzy_office = office_zip_df.filter(pl.col("office_distance").is_not_null()).select(
        build_fuzzy_office_select_expr()
    )

    # ALWAYS apply direct match first
    result = df.join(
        direct_office,
        left_on="bene_zip_5",
        right_on="zip_code",
        how="left",
    )

    # Log after direct join
    after_direct_join = result.select(pl.len()).collect().item()
    logger.info(f"After direct join: {after_direct_join:,} rows")

    # Check data state: do we have unmatched records?
    base_with_office = result.filter(pl.col("office_name").is_not_null())
    base_without_office = result.filter(pl.col("office_name").is_null())

    try:
        # Count unmatched records (requires collect for conditional logic)
        matched_count = base_with_office.select(pl.len()).collect().item()
        unmatched_count = base_without_office.select(pl.len()).collect().item()
        logger.info(f"Split: {matched_count:,} with office, {unmatched_count:,} without office")

        if unmatched_count > 0:
            logger.info(f"Found {unmatched_count} unmatched records, applying fuzzy matching")

            # Get unique beneficiary ZIPs that need fuzzy matching
            unmatched_zips = base_without_office.select(["current_mbi", "bene_zip_5"]).unique()
            unmatched_unique_count = unmatched_zips.select(pl.len()).collect().item()
            logger.info(f"Unique unmatched ZIPs: {unmatched_unique_count:,}")

            # Join with fuzzy office to find shortest distance
            fuzzy_matched = (
                unmatched_zips.join(
                    fuzzy_office, left_on="bene_zip_5", right_on="zip_code", how="left"
                )
                .filter(pl.col("office_distance").is_not_null())
                .sort(["current_mbi", "office_distance"])
                .unique(subset=["current_mbi"], keep="first", maintain_order=True)
                .select(["current_mbi", "office_name", "market"])
            )

            fuzzy_matched_count = fuzzy_matched.select(pl.len()).collect().item()
            logger.info(f"Fuzzy matched results: {fuzzy_matched_count:,} rows")

            # Join fuzzy matches back to unmatched records
            base_without_office = base_without_office.drop(["office_name", "market"]).join(
                fuzzy_matched, on="current_mbi", how="left"
            )

            after_fuzzy_join = base_without_office.select(pl.len()).collect().item()
            logger.info(f"After fuzzy join: {after_fuzzy_join:,} rows")

            # Union matched and fuzzy-matched back together
            result = pl.concat([base_with_office, base_without_office], how="diagonal_relaxed")

            after_concat = result.select(pl.len()).collect().item()
            logger.info(f"After concat: {after_concat:,} rows")
        else:
            logger.info("All records have direct office assignment")
            result = base_with_office

    except Exception as e:
        logger.warning(f"Fuzzy office matching failed, using direct matches only: {e}")
        # Fallback: just use what we have
        result = pl.concat([base_with_office, base_without_office], how="diagonal_relaxed")

    # Alias market to office_location
    result = result.with_columns([build_office_location_alias_expr()])

    # Mark as processed
    result = result.with_columns([pl.lit(True).alias("_office_matched")])

    logger.info("Office matching transform complete")
    return result
