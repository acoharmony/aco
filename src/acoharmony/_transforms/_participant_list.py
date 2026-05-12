# © 2025 HarmonyCares
# All rights reserved.

"""
Participant list transform.

Fills entity columns (and ``performance_year``) for source files that omit
them — typically HarmonyCares-internal provider list exports whose rows
describe providers under the operating ACO but ship without entity rollup
columns. REACH-issued participant list files already carry these columns
and pass through untouched (the underlying fills are coalesce-style).

ACO entity constants are *not* defined here; they live in ``aco.toml``
under ``[aco_identity]`` and are loaded via the
``_expressions._participant_list_entity`` builders.
"""

from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from .._expressions._participant_list_entity import (
    build_fill_entity_columns_exprs,
    build_performance_year_from_file_date_expr,
)


@transform(name="participant_list", tier=["bronze"], sql_enabled=False)
@transform_method(enable_composition=False, threshold=5.0)
def apply_transform(
    df: pl.LazyFrame, schema: dict, catalog: Any, logger: Any, force: bool = False
) -> pl.LazyFrame:
    """
    Normalize entity columns across participant list source layouts.

    Source files that already supply entity columns pass through untouched;
    files that omit them have their entity columns coalesced against the
    operating ACO identity from ``aco.toml``.

    Args:
        df: Header-mapped LazyFrame from the standard excel parser. Already
            contains ``source_filename`` and ``file_date`` (added by the
            common parse pipeline before the transform stage).
        schema: Table metadata (unused — kept for runner-call-signature parity).
        catalog: Schema catalog (unused).
        logger: Logger instance.
        force: Force-reprocess flag (unused).

    Returns:
        LazyFrame with entity columns and ``performance_year`` guaranteed
        non-null wherever the operating ACO identity can supply a fallback.
    """
    logger.info("Starting transform: participant_list")

    df = df.with_columns(
        [
            *build_fill_entity_columns_exprs(),
            build_performance_year_from_file_date_expr(),
        ]
    )

    logger.info("Completed transform: participant_list")
    return df
