# © 2025 HarmonyCares
# All rights reserved.

"""
BNEX (Beneficiary Data Sharing Opt-Out) transform.

Parses date fields and enriches BNEX data with formatted dates.
"""

from typing import Any

import polars as pl

from .._decor8 import transform, transform_method


@transform(name="bnex", tier=["silver"], sql_enabled=True)
@transform_method(enable_composition=True, threshold=5.0)
def apply_transform(
    df: pl.LazyFrame, schema: dict, catalog: Any, logger: Any, force: bool = False
) -> pl.LazyFrame:
    """
    Transform BNEX data: parse DOB to date format.

    Args:
        df: Input LazyFrame with BNEX data
        schema: Schema configuration
        catalog: Data catalog
        logger: Logger instance
        force: Force reprocessing

    Returns:
        Transformed LazyFrame with date columns parsed
    """
    logger.info("Starting transform: bnex")

    # Parse DOB from YYYYMMDD string to date (replace DOB column)
    df = df.with_columns([
        pl.concat_str([
            pl.col("DOB").str.slice(0, 4),
            pl.lit("-"),
            pl.col("DOB").str.slice(4, 2),
            pl.lit("-"),
            pl.col("DOB").str.slice(6, 2),
        ]).str.to_date("%Y-%m-%d").alias("DOB")
    ])

    # Parse FileCreationDate from source_filename to extract file date
    # Pattern: P.A2671.BNEX.Y25.D251203.T1136030.xml
    # Extract D251203 = YYMMDD format -> 2025-12-03
    df = df.with_columns([
        pl.when(pl.col("source_filename").str.contains(r"\.D\d{6}\."))
        .then(
            # Extract the 6 digits after "D"
            pl.col("source_filename").str.extract(r"\.D(\d{6})\.", 1).alias("date_part")
        )
        .otherwise(None)
        .alias("date_part")
    ]).with_columns([
        pl.when(pl.col("date_part").is_not_null())
        .then(
            pl.concat_str([
                pl.lit("20"),
                pl.col("date_part").str.slice(0, 2),
                pl.lit("-"),
                pl.col("date_part").str.slice(2, 2),
                pl.lit("-"),
                pl.col("date_part").str.slice(4, 2),
            ]).str.to_date("%Y-%m-%d")
        )
        .otherwise(None)
        .alias("file_date")
    ]).drop("date_part")

    logger.info("Completed transform: bnex")

    return df
