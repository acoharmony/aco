# © 2025 HarmonyCares
# All rights reserved.

"""
FFS first dates transform for historical provider tracking.

Computes the first/earliest FFS service date for each beneficiary
by joining CCLF5 claims with valid provider list.

Idempotent and schema-driven.
"""

from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from .._expressions._ffs_first_dates import FfsFirstDatesExpression


@transform(name="ffs_first_dates", tier=["silver", "gold"], sql_enabled=False)
@transform_method(enable_composition=False, threshold=5.0)
def apply_transform(
    df: pl.LazyFrame | None, schema: dict, catalog: Any, logger: Any, force: bool = False
) -> pl.LazyFrame:
    """
    Build FFS first dates data from CCLF5 claims and provider list.

    This transform:
    1. Filters CCLF5 claims to valid providers from provider_list
    2. Finds the earliest (MIN) service date per beneficiary
    3. Counts total claims from valid providers

    Args:
        df: Not used (foundation transform builds from catalog sources)
        schema: Schema configuration
        catalog: Catalog for accessing source data
        logger: Logger instance
        force: Force reprocessing

    Returns:
        pl.LazyFrame: FFS first dates data with columns:
            - bene_mbi: Beneficiary MBI
            - ffs_first_date: Earliest FFS service date
            - claim_count: Total claim count from valid providers
            - extracted_at: Processing timestamp
    """
    # Check if already exists
    if not force and catalog.get_table_metadata("ffs_first_dates") is not None:
        try:
            logger.info("FFS first dates already exists, loading from catalog")
            return catalog.scan_table("ffs_first_dates")
        except Exception:
            logger.info("FFS first dates metadata found but data missing, rebuilding")

    logger.info("Building FFS first dates from CCLF5 and provider list")

    # Get source data from catalog
    cclf5_df = catalog.scan_table("cclf5")
    provider_list_df = catalog.scan_table("provider_list")

    if cclf5_df is None:
        raise ValueError("CCLF5 data required for FFS first dates")
    if provider_list_df is None:
        raise ValueError("Provider list required for FFS first dates")

    # Step 1: Get valid provider TINs
    valid_tins = provider_list_df.select("billing_tin").unique()
    logger.info("Filtering claims to valid providers")

    # Step 2: Inner join to filter CCLF5 to valid providers, then aggregate
    logger.info("Finding earliest service date per beneficiary")
    result = (
        cclf5_df.join(
            valid_tins, left_on="clm_rndrg_prvdr_tax_num", right_on="billing_tin", how="inner"
        )
        .group_by("bene_mbi_id")
        .agg(FfsFirstDatesExpression.build_ffs_first_aggregations())
        .with_columns(FfsFirstDatesExpression.build_ffs_first_metadata_expr())
        .select(FfsFirstDatesExpression.build_ffs_first_select_columns())
    )

    logger.info("FFS first dates build complete")
    return result
