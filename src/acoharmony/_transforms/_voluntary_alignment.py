# © 2025 HarmonyCares
# All rights reserved.

"""
Voluntary alignment consolidation transform.

Consolidates all beneficiary voluntary alignment touchpoints from multiple sources:
- SVA (Shared Voluntary Alignment) signatures
- PBVAR (Provider Beneficiary Voluntary Alignment Report) confirmations
- Email campaign tracking
- Mailed campaign tracking

Uses expression builders for idempotent, composable logic.
"""

from typing import Any

import polars as pl

from .._decor8 import transform_method
from .._expressions._voluntary_alignment_consolidated import (
    build_alignment_journey_status_expr,
    build_chase_list_eligibility_exprs,
    build_data_quality_exprs,
    build_days_in_funnel_expr,
    build_ffs_status_exprs,
    build_outreach_consolidated_exprs,
    build_outreach_response_status_expr,
    build_signature_status_expr,
)
from .._expressions._voluntary_alignment_emails import (
    build_email_aggregation_exprs,
    build_email_derived_exprs,
)
from .._expressions._voluntary_alignment_mailed import (
    build_mailed_aggregation_exprs,
    build_mailed_derived_exprs,
)
from .._expressions._voluntary_alignment_pbvar import build_pbvar_aggregation_exprs
from .._expressions._voluntary_alignment_sva import (
    build_sva_aggregation_exprs,
    build_sva_derived_exprs,
)


@transform_method(enable_composition=False, threshold=10.0)
def apply_transform(
    df: pl.LazyFrame | None, schema: dict, catalog: Any, logger: Any, force: bool = False
) -> pl.LazyFrame:
    """
    Apply voluntary alignment consolidation transform.

    This transform:
    1. Aggregates SVA signatures per beneficiary
    2. Aggregates PBVAR alignment status per beneficiary
    3. Aggregates email campaign tracking per beneficiary
    4. Aggregates mailed campaign tracking per beneficiary
    5. Joins all sources on MBI (with crosswalk normalization)
    6. Calculates derived metrics using expression builders
    7. Validates provider TIN/NPI combinations
    8. Applies data quality checks

    All logic is implemented via pure expression builders for maintainability.

    Args:
        df: Input LazyFrame (not used - builds from catalog sources)
        schema: Schema config with column names
        catalog: Catalog for accessing source data
        logger: Logger instance
        force: Force reprocessing

    Returns:
        pl.LazyFrame: Consolidated voluntary alignment tracking data
    """
    logger.info("Starting voluntary alignment consolidation transform")

    # STEP 1: Load MBI crosswalk for normalization
    logger.info("Loading MBI crosswalk")
    crosswalk_df = _load_crosswalk(catalog, logger)
    mbi_map = _build_mbi_map(crosswalk_df, logger)

    # STEP 2: Aggregate SVA signatures
    logger.info("Aggregating SVA signature data")
    sva_df = catalog.scan_table("sva")
    if sva_df is not None:
        sva_agg = (
            sva_df.with_columns([_build_mbi_crosswalk_expr(mbi_map)])
            .group_by("normalized_mbi")
            .agg(build_sva_aggregation_exprs())
            .with_columns(build_sva_derived_exprs())
            .rename({"normalized_mbi": "bene_mbi"})
        )
    else:
        logger.warning("SVA table not found, skipping SVA aggregation")
        sva_agg = None

    # STEP 3: Aggregate PBVAR alignment
    logger.info("Aggregating PBVAR alignment data")
    pbvar_df = catalog.scan_table("pbvar")
    if pbvar_df is not None:
        pbvar_agg = (
            pbvar_df.with_columns([_build_mbi_crosswalk_expr(mbi_map)])
            .group_by("normalized_mbi")
            .agg(build_pbvar_aggregation_exprs())
            .rename({"normalized_mbi": "bene_mbi"})
        )
    else:
        logger.warning("PBVAR table not found, skipping PBVAR aggregation")
        pbvar_agg = None

    # STEP 4: Aggregate email campaigns
    logger.info("Aggregating email campaign data")
    emails_df = catalog.scan_table("emails")
    if emails_df is not None:
        # Map mbi column to bene_mbi for consistency
        emails_agg = (
            emails_df.with_columns([_build_mbi_crosswalk_expr(mbi_map, source_col="mbi")])
            .group_by("normalized_mbi")
            .agg(build_email_aggregation_exprs())
            .with_columns(build_email_derived_exprs())
            .rename({"normalized_mbi": "bene_mbi"})
        )
    else:
        logger.warning("Emails table not found, skipping email aggregation")
        emails_agg = None

    # STEP 5: Aggregate mailed campaigns
    logger.info("Aggregating mailed campaign data")
    mailed_df = catalog.scan_table("mailed")
    if mailed_df is not None:
        mailed_agg = (
            mailed_df.with_columns([_build_mbi_crosswalk_expr(mbi_map, source_col="mbi")])
            .group_by("normalized_mbi")
            .agg(build_mailed_aggregation_exprs())
            .with_columns(build_mailed_derived_exprs())
            .rename({"normalized_mbi": "bene_mbi"})
        )
    else:
        logger.warning("Mailed table not found, skipping mailed aggregation")
        mailed_agg = None

    # STEP 6: Load email unsubscribes/complaints
    logger.info("Loading email unsubscribe/complaint data")
    unsub_df = catalog.scan_table("email_unsubscribes")
    if unsub_df is not None and emails_df is not None:
        # Check if the table has the expected schema
        unsub_schema = unsub_df.collect_schema().names()
        if "patient_id" in unsub_schema and "event_name" in unsub_schema:
            # email_unsubscribes uses patient_id (UUID) - join with emails table to get MBI
            logger.info("Mapping email_unsubscribes patient_id to MBI via emails table")
            unsub_agg = (
                unsub_df
                # Join with emails table to map patient_id (UUID) to MBI
                .join(
                    emails_df.select(["patient_id", "mbi"]),
                    on="patient_id",
                    how="inner",
                )
                # Filter out rows with empty/null MBI
                .filter(pl.col("mbi").is_not_null() & (pl.col("mbi") != ""))
                # Apply crosswalk normalization to MBI
                .with_columns([_build_mbi_crosswalk_expr(mbi_map, source_col="mbi")])
                # Aggregate unsubscribe/complaint events by normalized MBI
                .group_by("normalized_mbi")
                .agg([
                    pl.col("event_name").is_in(["unsubscribed"]).any().alias("email_unsubscribed"),
                    pl.col("event_name").is_in(["complained"]).any().alias("email_complained"),
                ])
                .rename({"normalized_mbi": "bene_mbi"})
            )
        else:
            logger.warning("email_unsubscribes has unexpected schema, will default to False")
            unsub_agg = None
    else:
        if unsub_df is None:
            logger.warning("Email unsubscribes table not found, will default to False")
        if emails_df is None:
            logger.warning("Emails table not found (needed for unsubscribe mapping), will default to False")
        unsub_agg = None

    # STEP 7: Consolidate all sources
    logger.info("Consolidating all voluntary alignment sources")
    result = _consolidate_sources(
        sva_agg, pbvar_agg, emails_agg, mailed_agg, unsub_agg, crosswalk_df, logger
    )

    # STEP 8: Calculate derived metrics using expression builders
    logger.info("Calculating consolidated metrics")
    result = result.with_columns(build_outreach_consolidated_exprs())
    result = result.with_columns([build_days_in_funnel_expr()])

    # STEP 9: Add FFS status defaults (will be updated when FFS data is available)
    result = result.with_columns(build_ffs_status_exprs())

    # STEP 10: Validate provider TIN/NPI combinations
    logger.info("Validating provider TIN/NPI combinations")
    result = _validate_providers(result, catalog, logger)

    # STEP 11: Calculate journey status and classifications
    logger.info("Calculating alignment journey classifications")
    result = result.with_columns([
        build_alignment_journey_status_expr(),
        build_signature_status_expr(),
        build_outreach_response_status_expr(),
    ])
    result = result.with_columns(build_chase_list_eligibility_exprs())

    # STEP 12: Add data quality flags
    result = result.with_columns(build_data_quality_exprs())

    # STEP 13: Add metadata
    from datetime import datetime

    result = result.with_columns([
        pl.lit(datetime.now()).alias("processed_at"),
    ])

    logger.info("Voluntary alignment consolidation complete")
    return result


def _load_crosswalk(catalog: Any, logger: Any) -> pl.LazyFrame:
    """Load enterprise crosswalk from storage."""
    from ..config import get_config

    config = get_config()
    silver_path = config.storage.base_path / config.storage.silver_dir
    crosswalk_path = silver_path / "enterprise_crosswalk.parquet"

    if not crosswalk_path.exists():
        raise ValueError(
            "enterprise_crosswalk not found - required for voluntary_alignment. "
            "Run 'aco pipeline enterprise_crosswalk' first."
        )

    return pl.scan_parquet(crosswalk_path)


def _build_mbi_map(crosswalk_df: pl.LazyFrame, logger: Any) -> dict[str, str]:
    """Build MBI crosswalk lookup dictionary."""
    mbi_map = {}
    xwalk = crosswalk_df.select(["prvs_num", "crnt_num"]).collect()
    for row in xwalk.iter_rows():
        if row[0] and row[1] and row[0] != row[1]:
            mbi_map[row[0]] = row[1]
    logger.info(f"Built MBI crosswalk with {len(mbi_map)} mappings")
    return mbi_map


def _build_mbi_crosswalk_expr(mbi_map: dict, source_col: str = "bene_mbi") -> pl.Expr:
    """Build expression to normalize MBI using crosswalk."""
    return (
        pl.col(source_col)
        .map_elements(lambda mbi: mbi_map.get(mbi, mbi) if mbi else None, return_dtype=pl.String)
        .alias("normalized_mbi")
    )


def _consolidate_sources(
    sva_agg: pl.LazyFrame | None,
    pbvar_agg: pl.LazyFrame | None,
    emails_agg: pl.LazyFrame | None,
    mailed_agg: pl.LazyFrame | None,
    unsub_agg: pl.LazyFrame | None,
    crosswalk_df: pl.LazyFrame,
    logger: Any,
) -> pl.LazyFrame:
    """Consolidate all sources with full outer joins on normalized MBI."""
    # Start with all unique current MBIs from crosswalk
    # crnt_num is the normalized/current MBI after crosswalk resolution
    all_mbis = crosswalk_df.select([
        pl.col("crnt_num").alias("current_mbi"),  # Normalized current MBI
        pl.col("hcmpi"),
    ]).unique(subset=["current_mbi"])

    # Add previous MBI count (how many historical MBIs map to this current MBI)
    mbi_counts = (
        crosswalk_df.group_by("crnt_num")
        .agg([
            pl.col("prvs_num").n_unique().alias("previous_mbi_count"),
        ])
        .rename({"crnt_num": "current_mbi"})
    )
    all_mbis = all_mbis.join(mbi_counts, on="current_mbi", how="left")

    # Rename current_mbi to normalized_mbi for joining with aggregated sources
    all_mbis = all_mbis.rename({"current_mbi": "normalized_mbi"})

    # Join each source
    if sva_agg is not None:
        all_mbis = all_mbis.join(
            sva_agg.rename({"bene_mbi": "normalized_mbi"}), on="normalized_mbi", how="left"
        )

    if pbvar_agg is not None:
        all_mbis = all_mbis.join(
            pbvar_agg.rename({"bene_mbi": "normalized_mbi"}), on="normalized_mbi", how="left"
        )

    if emails_agg is not None:
        all_mbis = all_mbis.join(
            emails_agg.rename({"bene_mbi": "normalized_mbi"}), on="normalized_mbi", how="left"
        )

    if mailed_agg is not None:
        all_mbis = all_mbis.join(
            mailed_agg.rename({"bene_mbi": "normalized_mbi"}), on="normalized_mbi", how="left"
        )

    if unsub_agg is not None:
        all_mbis = all_mbis.join(
            unsub_agg.rename({"bene_mbi": "normalized_mbi"}), on="normalized_mbi", how="left"
        )
    else:
        # Add default unsubscribe/complaint flags
        all_mbis = all_mbis.with_columns([
            pl.lit(False).alias("email_unsubscribed"),
            pl.lit(False).alias("email_complained"),
        ])

    # Fill nulls for count columns with 0
    count_cols = [
        "sva_signature_count", "email_campaigns_sent", "emails_opened", "emails_clicked",
        "mailed_campaigns_sent", "mailed_delivered", "previous_mbi_count"
    ]
    for col in count_cols:
        if col in all_mbis.collect_schema().names():
            all_mbis = all_mbis.with_columns([pl.col(col).fill_null(0)])

    # Fill nulls for boolean columns with False
    bool_cols = ["pbvar_aligned", "sva_pending_cms", "email_unsubscribed", "email_complained"]
    for col in bool_cols:
        if col in all_mbis.collect_schema().names():
            all_mbis = all_mbis.with_columns([pl.col(col).fill_null(False)])

    # Rename normalized_mbi to current_mbi (the final output column name)
    all_mbis = all_mbis.rename({"normalized_mbi": "current_mbi"})

    return all_mbis


def _validate_providers(result: pl.LazyFrame, catalog: Any, logger: Any) -> pl.LazyFrame:
    """
    Validate provider TIN/NPI combinations against provider registry.

    Marks sva_provider_valid based on whether the TIN/NPI combo exists in provider data.
    """
    # Try to load provider data directly from silver (use participant_list for ACO providers)
    from ..config import get_config

    config = get_config()
    silver_path = config.storage.base_path / config.storage.silver_dir
    provider_path = silver_path / "participant_list.parquet"

    if not provider_path.exists():
        logger.warning("participant_list.parquet not found, defaulting sva_provider_valid to True")
        # If no provider data, assume valid if both TIN and NPI exist
        return result.with_columns([
            (pl.col("sva_provider_npi").is_not_null() & pl.col("sva_provider_tin").is_not_null())
            .fill_null(False)
            .alias("sva_provider_valid")
        ])

    provider_df = pl.scan_parquet(provider_path)

    # Build valid TIN/NPI set from participant_list
    # Use individual_npi and base_provider_tin for provider validation
    valid_providers = provider_df.select([
        pl.col("individual_npi").alias("provider_npi"),
        pl.col("base_provider_tin").alias("provider_tin"),
    ]).filter(
        pl.col("provider_npi").is_not_null() & pl.col("provider_tin").is_not_null()
    ).unique()

    # Join to check validity - use indicator to check if match was found
    valid_providers_marked = valid_providers.with_columns([pl.lit(True).alias("_provider_valid_match")])

    result = result.join(
        valid_providers_marked,
        left_on=["sva_provider_npi", "sva_provider_tin"],
        right_on=["provider_npi", "provider_tin"],
        how="left",
    )

    # Mark as valid if join succeeded (indicator column is True)
    result = result.with_columns([
        pl.col("_provider_valid_match").fill_null(False).alias("sva_provider_valid")
    ])

    # Drop the temporary columns - check if they exist first
    cols_to_drop = []
    result_cols = result.collect_schema().names()
    for col in ["provider_npi", "provider_tin", "_provider_valid_match"]:
        if col in result_cols:
            cols_to_drop.append(col)

    if cols_to_drop:
        result = result.drop(cols_to_drop)

    logger.info("Provider validation complete")
    return result
