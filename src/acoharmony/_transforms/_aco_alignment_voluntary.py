# © 2025 HarmonyCares
# All rights reserved.

"""
Voluntary alignment transform for ACO alignment pipeline.

Applies voluntary alignment join and validation logic using expression builders.
Idempotent and schema-driven.
"""

from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from .._expressions._aco_voluntary_join import (
    build_valid_voluntary_alignment_expr,
    build_voluntary_alignment_select_expr,
)


@transform(name="aco_alignment_voluntary", tier=["silver", "gold"], sql_enabled=True)
@transform_method(enable_composition=True, threshold=5.0)
def apply_transform(
    df: pl.LazyFrame, schema: dict, catalog: Any, logger: Any, force: bool = False
) -> pl.LazyFrame:
    """
    Apply voluntary alignment join and validation.

    This transform:
    1. Checks idempotency (_voluntary_aligned flag)
    2. Joins voluntary_alignment data to base alignment
    3. Calculates has_valid_voluntary_alignment (provider valid + REACH enrollment)
    4. Marks as processed

    Args:
        df: Base alignment LazyFrame
        schema: Schema config with column names
        catalog: Catalog for accessing voluntary_alignment source
        logger: Logger instance
        force: Force reprocessing

    Returns:
        pl.LazyFrame: Transformed data with voluntary alignment joined
    """
    # Idempotency check - use schema.names() instead of collect_schema() to avoid file reads
    schema_names = df.schema.names()
    if not force and "_voluntary_aligned" in schema_names:
        logger.info("Voluntary alignment already applied, skipping")
        return df

    logger.info("Applying voluntary alignment transform")

    # Get voluntary alignment source from catalog
    voluntary_df = catalog.scan_table("voluntary_alignment")
    if voluntary_df is None:
        raise ValueError("voluntary_alignment source not found - required for aco_alignment pipeline")

    # Select and derive voluntary alignment columns using expression builder
    voluntary_select = voluntary_df.select(build_voluntary_alignment_select_expr())

    # Rename mbi to current_mbi for join (voluntary_alignment uses 'mbi', pipeline uses 'current_mbi')
    if "mbi" in voluntary_select.collect_schema().names():
        voluntary_select = voluntary_select.rename({"mbi": "current_mbi"})
    elif "normalized_mbi" in voluntary_select.collect_schema().names():
        voluntary_select = voluntary_select.rename({"normalized_mbi": "current_mbi"})

    # Join to base
    result = df.join(voluntary_select, on="current_mbi", how="left")

    # Calculate has_valid_voluntary_alignment (must happen after join)
    result = result.with_columns([build_valid_voluntary_alignment_expr()])

    # Mark as processed
    result = result.with_columns([pl.lit(True).alias("_voluntary_aligned")])

    logger.info("Voluntary alignment transform complete")
    return result
