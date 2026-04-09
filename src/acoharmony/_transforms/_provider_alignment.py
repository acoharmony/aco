# © 2025 HarmonyCares
# All rights reserved.

"""
Provider alignment transform for extracting TIN-NPI combinations.

Processes participant_list data to extract:
1. Individual participant providers (individual_npi) for voluntary alignment
2. Preferred providers (organization_npi) for claims-based attribution
3. Combined provider roster with standardized TIN-NPI mappings

Outputs silver-tier provider_tin_npi dataset for downstream alignment workflows.

Note: participant_list has one row per provider, so no comma-separated NPI explosion needed.
"""

from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from .._expressions import FileVersionExpression, ProviderAlignmentExpression


@transform(
    name="provider_tin_npi_extraction",
    tier=["silver"],
    sql_enabled=True,
)
@transform_method(enable_composition=True, threshold=5.0)
def apply_transform(
    df: pl.LazyFrame,
    schema: dict,
    catalog: Any,
    logger: Any,
    force: bool = False,
) -> pl.LazyFrame:
    """
    Extract and combine TIN-NPI combinations from provider_list.

    This transform:
    1. Extracts individual participants from individual_npis
    2. Extracts preferred providers from organization_npi
    3. Combines both into standardized format
    4. Adds data quality flags

    Args:
        df: Provider list LazyFrame
        schema: Schema config
        catalog: Catalog instance
        logger: Logger instance
        force: Force reprocessing

    Returns:
        pl.LazyFrame: Combined TIN-NPI provider roster
    """
    # Idempotency check
    if not force and "_tin_npi_extracted" in df.collect_schema().names():
        logger.info("TIN-NPI extraction already applied, skipping")
        return df

    logger.info("Starting provider TIN-NPI extraction")

    # Log input row count (all files)
    input_count = df.select(pl.len()).collect().item()
    logger.info(f"Provider list input rows (all files): {input_count:,}")

    # ============================================================
    # FILTER TO MOST RECENT FILE ONLY
    # ============================================================
    logger.info("Filtering to most recent file version")

    # Get the most recent filename for logging
    most_recent_file = df.select(FileVersionExpression.get_most_recent_filename()).collect().item()
    logger.info(f"Most recent file: {most_recent_file}")

    # Filter to only rows from the most recent file
    df = df.filter(FileVersionExpression.keep_only_most_recent_file())

    # Log count after filtering
    filtered_count = df.select(pl.len()).collect().item()
    logger.info(f"Provider list rows from most recent file: {filtered_count:,}")

    # ============================================================
    # EXTRACT INDIVIDUAL PARTICIPANTS
    # ============================================================
    logger.info("Extracting individual participant providers")

    # Participant list already has one row per provider - no need to explode
    individual_providers = df.filter(
        ProviderAlignmentExpression.filter_has_individual_npi()
    ).select(ProviderAlignmentExpression.select_individual_participant_columns())

    individual_count = individual_providers.select(pl.len()).collect().item()
    logger.info(f"Individual participants extracted: {individual_count:,}")

    # ============================================================
    # EXTRACT PREFERRED PROVIDERS
    # ============================================================
    logger.info("Extracting preferred (organization) providers")

    preferred_providers = df.filter(
        ProviderAlignmentExpression.filter_has_organization_npi()
    ).select(ProviderAlignmentExpression.select_preferred_provider_columns())

    preferred_count = preferred_providers.select(pl.len()).collect().item()
    logger.info(f"Preferred providers extracted: {preferred_count:,}")

    # ============================================================
    # COMBINE ALL PROVIDERS
    # ============================================================
    logger.info("Combining individual and preferred providers")

    # Check if we have any data to combine
    if individual_count == 0 and preferred_count == 0:
        logger.warning("No providers extracted - both individual and preferred are empty")
        # Return empty dataframe with expected schema
        return pl.LazyFrame(
            schema={
                "tin": pl.Utf8,
                "npi": pl.Utf8,
                "provider_category": pl.Utf8,
                "provider_type": pl.Utf8,
                "provider_class": pl.Utf8,
                "first_name": pl.Utf8,
                "last_name": pl.Utf8,
                "provider_name": pl.Utf8,
                "organization": pl.Utf8,
                "email": pl.Utf8,
                "entity_id": pl.Utf8,
                "entity_tin": pl.Utf8,
                "performance_year": pl.Utf8,
                "specialty": pl.Utf8,
                "_tin_npi_extracted": pl.Boolean,
            }
        )

    # Combine based on what we have
    if individual_count == 0:
        combined = preferred_providers
        logger.info("Using only preferred providers (no individual participants)")
    elif preferred_count == 0:
        combined = individual_providers
        logger.info("Using only individual participants (no preferred providers)")
    else:
        combined = pl.concat([individual_providers, preferred_providers], how="vertical")
        logger.info("Combined individual and preferred providers")

    # ============================================================
    # DATA QUALITY AND DEDUPLICATION
    # ============================================================
    logger.info("Applying data quality checks and deduplication")

    result = combined.with_columns(
        [
            # Mark as processed
            pl.lit(True).alias("_tin_npi_extracted"),
            # Add validation flags
            pl.col("tin").is_not_null().alias("_has_valid_tin"),
            pl.col("npi").is_not_null().alias("_has_valid_npi"),
            # Add combination key for deduplication tracking
            pl.concat_str([pl.col("tin"), pl.lit("-"), pl.col("npi")], ignore_nulls=False).alias(
                "tin_npi_key"
            ),
        ]
    )

    # Deduplicate TIN-NPI combinations (keep first occurrence)
    # This prevents duplicate entries from the source data
    result = result.unique(subset=["tin", "npi"], keep="first", maintain_order=True)

    # Log output row count
    output_count = result.select(pl.len()).collect().item()
    logger.info(f"Final TIN-NPI combinations: {output_count:,}")

    # Log data quality metrics
    null_tin_count = result.filter(pl.col("tin").is_null()).select(pl.len()).collect().item()
    null_npi_count = result.filter(pl.col("npi").is_null()).select(pl.len()).collect().item()

    if null_tin_count > 0:
        logger.warning(f"Found {null_tin_count:,} rows with null TIN")
    if null_npi_count > 0:
        logger.warning(f"Found {null_npi_count:,} rows with null NPI")

    logger.info("Provider TIN-NPI extraction complete")
    return result


@transform(
    name="provider_tin_npi_mapping",
    tier=["silver"],
    sql_enabled=True,
)
@transform_method(enable_composition=True, threshold=5.0)
def create_tin_npi_mapping(
    df: pl.LazyFrame,
    schema: dict,
    catalog: Any,
    logger: Any,
    force: bool = False,
) -> pl.LazyFrame:
    """
    Create TIN to NPI list mapping, excluding facility providers.

    This transform:
    1. Filters out facility and institutional providers
    2. Groups NPIs by TIN
    3. Creates sorted, unique NPI lists per TIN

    Args:
        df: Combined TIN-NPI LazyFrame
        schema: Schema config
        catalog: Catalog instance
        logger: Logger instance
        force: Force reprocessing

    Returns:
        pl.LazyFrame: TIN to NPI list mapping
    """
    logger.info("Creating TIN to NPI mapping")

    # Filter out facility providers
    filtered = df.filter(ProviderAlignmentExpression.filter_non_facility_providers())

    filtered_count = filtered.select(pl.len()).collect().item()
    logger.info(f"Non-facility providers: {filtered_count:,}")

    # Group by TIN and aggregate NPIs
    mapping = (
        filtered.group_by("tin")
        .agg(
            [
                pl.col("npi").unique().sort().alias("npi_list"),
                pl.col("npi").n_unique().alias("npi_count"),
                pl.col("provider_category").first().alias("primary_category"),
            ]
        )
        .sort("tin")
    )

    output_count = mapping.select(pl.len()).collect().item()
    logger.info(f"Unique TINs in mapping: {output_count:,}")

    return mapping
