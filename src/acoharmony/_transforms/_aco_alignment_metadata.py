# © 2025 HarmonyCares
# All rights reserved.

"""
Metadata and action flags transform for ACO alignment pipeline.

Applies operational metadata, signature lifecycle, response code parsing,
and action flags using expression builders. Idempotent and schema-driven.
"""

from datetime import datetime
from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from .._expressions._aco_metadata import (
    build_data_completeness_expr,
    build_data_date_exprs,
    build_has_opt_out_expr,
    build_last_updated_expr,
    build_lineage_processed_at_expr,
    build_lineage_transform_expr,
    build_outreach_priority_expr,
    build_source_tables_expr,
    build_sva_action_needed_expr,
)
from .._expressions._response_code_parser import ResponseCodeParserExpression
from .._expressions._signature_lifecycle import SignatureLifecycleExpression


@transform(name="aco_alignment_metadata", tier=["silver", "gold"], sql_enabled=True)
@transform_method(enable_composition=True, threshold=5.0)
def apply_transform(
    df: pl.LazyFrame, schema: dict, catalog: Any, logger: Any, force: bool = False
) -> pl.LazyFrame:
    """
    Apply metadata, signature lifecycle, and action flags.

    This transform:
    1. Checks idempotency (_metadata_added flag)
    2. Applies signature lifecycle calculations
    3. Parses PBVAR response codes
    4. Adds operational metadata
    5. Calculates action flags (sva_action_needed, outreach_priority)
    6. Marks as processed

    Args:
        df: Alignment LazyFrame with all metrics calculated
        schema: Schema config
        catalog: Catalog instance (unused but required for signature)
        logger: Logger instance
        force: Force reprocessing

    Returns:
        pl.LazyFrame: Transformed data with metadata and action flags
    """
    # Idempotency check
    if not force and "_metadata_added" in df.collect_schema().names():
        logger.info("Metadata already added, skipping")
        return df

    logger.info("Applying metadata and action flags transform")

    # Apply signature lifecycle calculations
    result = df.with_columns(
        SignatureLifecycleExpression.calculate_signature_lifecycle(
            "last_valid_signature_date",
            current_py=datetime.now().year,
            reference_date=datetime.now().date(),
        )
    )

    # Parse PBVAR response codes
    result = result.with_columns(
        ResponseCodeParserExpression.parse_response_codes("latest_response_codes")
    )

    # Apply metadata expressions (first pass)
    result = result.with_columns(
        [
            build_data_completeness_expr(),
            build_lineage_transform_expr(),
            build_lineage_processed_at_expr(),
        ]
        + build_data_date_exprs()
        + [
            build_source_tables_expr(),
            build_last_updated_expr(),
            build_has_opt_out_expr(),
            build_sva_action_needed_expr(),
        ]
    )

    # Apply outreach priority (second pass - depends on sva_action_needed)
    result = result.with_columns([build_outreach_priority_expr()])

    # Mark as processed
    result = result.with_columns([pl.lit(True).alias("_metadata_added")])

    logger.info("Metadata and action flags transform complete")
    return result
