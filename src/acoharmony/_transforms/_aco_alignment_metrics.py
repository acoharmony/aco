# © 2025 HarmonyCares
# All rights reserved.

"""
Consolidated metrics transform for ACO alignment pipeline.

Applies business metric calculations using expression builders.
Idempotent and schema-driven.
"""

from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from .._expressions._aco_metrics import (
    build_bene_death_date_expr,
    build_consolidated_program_expr,
    build_crosswalk_mapping_exprs,
    build_has_continuous_enrollment_expr,
    build_has_program_transition_expr,
    build_has_valid_historical_sva_expr,
    build_has_voluntary_alignment_filled_expr,
    build_is_currently_aligned_expr,
    build_mssp_recruitment_exprs,
    build_pbvar_integration_exprs,
    build_previous_program_expr,
    build_primary_alignment_source_expr,
    build_program_transitions_expr,
    build_provider_validation_exprs,
    build_total_aligned_months_expr,
)


@transform(name="aco_alignment_metrics", tier=["silver", "gold"], sql_enabled=True)
@transform_method(enable_composition=True, threshold=5.0)
def apply_transform(
    df: pl.LazyFrame, schema: dict, catalog: Any, logger: Any, force: bool = False
) -> pl.LazyFrame:
    """
    Apply consolidated business metrics calculations.

    This transform:
    1. Checks idempotency (_metrics_calculated flag)
    2. Applies all metric calculations using expression builders:
       - Consolidated program status
       - Alignment sources and validity
       - Program transitions
       - MSSP recruitment targeting
       - PBVAR integration
       - Provider validation
    3. Marks as processed

    Args:
        df: Alignment LazyFrame with all source data joined
        schema: Schema config
        catalog: Catalog instance (unused but required for signature)
        logger: Logger instance
        force: Force reprocessing

    Returns:
        pl.LazyFrame: Transformed data with metrics calculated
    """
    # Idempotency check
    if not force and "_metrics_calculated" in df.collect_schema().names():
        logger.info("Metrics already calculated, skipping")
        return df

    logger.info("Applying metrics calculations transform")

    # Apply all metric expressions in one pass
    result = df.with_columns(
        [
            build_consolidated_program_expr(),
            build_total_aligned_months_expr(),
            build_primary_alignment_source_expr(),
            build_is_currently_aligned_expr(),
            build_has_voluntary_alignment_filled_expr(),
            build_has_valid_historical_sva_expr(),
            build_has_program_transition_expr(),
            build_has_continuous_enrollment_expr(),
            build_bene_death_date_expr(),
        ]
        + build_crosswalk_mapping_exprs()
        + build_mssp_recruitment_exprs()
        + build_pbvar_integration_exprs()
        + build_provider_validation_exprs()
    )

    # Second pass for expressions that depend on first pass
    result = result.with_columns(
        [
            build_previous_program_expr(),
            build_program_transitions_expr(),
        ]
    )

    # Mark as processed
    result = result.with_columns([pl.lit(True).alias("_metrics_calculated")])

    logger.info("Metrics calculations transform complete")
    return result
