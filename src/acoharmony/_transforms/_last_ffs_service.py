# © 2025 HarmonyCares
# All rights reserved.

"""
Last FFS service transform for provider attribution.

Computes the most recent FFS service date and provider for each beneficiary
by joining CCLF5 claims with valid provider list.

Idempotent and schema-driven.
"""

from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from .._expressions._last_ffs_service import LastFfsServiceExpression


@transform(name="last_ffs_service", tier=["silver", "gold"], sql_enabled=False)
@transform_method(enable_composition=False, threshold=5.0)
def apply_transform(
    df: pl.LazyFrame | None, schema: dict, catalog: Any, logger: Any, force: bool = False
) -> pl.LazyFrame:
    """
    Build last FFS service data from CCLF5 claims and provider list.

    This transform:
    1. Filters CCLF5 claims to valid providers from provider_list
    2. Finds the most recent (MAX) service date per beneficiary
    3. Extracts provider TIN/NPI from that most recent service
    4. Handles ties deterministically (takes first)

    Args:
        df: Not used (foundation transform builds from catalog sources)
        schema: Schema configuration
        catalog: Catalog for accessing source data
        logger: Logger instance
        force: Force reprocessing

    Returns:
        pl.LazyFrame: Last FFS service data with columns:
            - bene_mbi: Beneficiary MBI
            - last_ffs_date: Most recent FFS service date
            - last_ffs_tin: Provider TIN from most recent service
            - last_ffs_npi: Provider NPI from most recent service
            - claim_count: Total claim count from valid providers
            - extracted_at: Processing timestamp
    """
    # Check if already exists
    if not force and catalog.get_table_metadata("last_ffs_service") is not None:
        try:
            logger.info("Last FFS service already exists, loading from catalog")
            return catalog.scan_table("last_ffs_service")
        except Exception:
            logger.info("Last FFS service metadata found but data missing, rebuilding")

    logger.info("Building last FFS service from CCLF5 and provider list")

    # Get source data from catalog
    cclf5_df = catalog.scan_table("cclf5")
    provider_list_df = catalog.scan_table("provider_list")

    if cclf5_df is None:
        raise ValueError("CCLF5 data required for last FFS service")
    if provider_list_df is None:
        raise ValueError("Provider list required for last FFS service")

    # Step 1: Get valid provider TINs
    valid_tins = provider_list_df.select("billing_tin").unique()
    logger.info("Filtering claims to valid providers")

    # Step 2: Inner join to filter CCLF5 to valid providers only
    valid_claims = cclf5_df.join(
        valid_tins, left_on="clm_rndrg_prvdr_tax_num", right_on="billing_tin", how="inner"
    )

    # Step 3: Find MAX service date per beneficiary using expression builder
    logger.info("Finding most recent service date per beneficiary")
    max_dates = valid_claims.group_by("bene_mbi_id").agg(
        LastFfsServiceExpression.build_last_ffs_aggregations()
    )

    # Step 4: Join back to get provider info from most recent claim
    logger.info("Extracting provider info from most recent claims")
    last_service_claims = (
        valid_claims.join(max_dates, on="bene_mbi_id", how="inner")
        .filter(pl.col("clm_line_from_dt") == pl.col("last_ffs_date"))
        .group_by("bene_mbi_id")
        .agg(LastFfsServiceExpression.build_last_ffs_provider_aggregations())
    )

    # Step 5: Add metadata and select final columns using expression builders
    result = (
        last_service_claims.with_columns(LastFfsServiceExpression.build_last_ffs_metadata_expr())
        .select(LastFfsServiceExpression.build_last_ffs_select_columns())
    )

    logger.info("Last FFS service build complete")
    return result
