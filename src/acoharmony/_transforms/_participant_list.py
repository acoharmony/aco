# © 2025 HarmonyCares
# All rights reserved.

"""
Transform for participant_list that handles multiple file formats.

Normalizes both ACO REACH Participant List (51 columns) and D0259 Provider List (27 columns)
into a unified schema.
"""

import polars as pl

from .._decor8 import transform


@transform(
    name="participant_list",
    tier=["bronze"],
    description="Normalize participant list data from different file formats",
)
def transform_participant_list(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Normalize participant list data from different formats.

    Note: Parser already handles positional mapping to snake_case.
    This transform is kept for any additional business logic.

    Args:
        df: Input lazy frame with normalized column names

    Returns:
        Lazy frame with any additional transformations
    """
    # Parser already normalized columns by position, just pass through
    return df
