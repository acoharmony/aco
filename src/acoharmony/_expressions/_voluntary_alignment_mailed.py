# © 2025 HarmonyCares
# All rights reserved.

"""
Mailed campaign expression builders for voluntary_alignment consolidation.

Pure expression builders for aggregating physical mail campaign data per beneficiary.
"""

import polars as pl


def build_mailed_aggregation_exprs() -> list[pl.Expr]:
    """
    Build expressions to aggregate mailed campaign data per beneficiary.

    Aggregates:
    - Total mailings sent
    - Mailings delivered (non-failed status)
    - First and last mailed dates
    - Campaign names (concatenated)

    Returns:
        list[pl.Expr]: List of aggregation expressions for mailed data
    """
    return [
        pl.col("letter_id").count().alias("mailed_campaigns_sent"),
        (pl.col("status") != "failed").sum().alias("mailed_delivered"),
        pl.col("send_datetime").cast(pl.Datetime, strict=False).min().cast(pl.Date).alias("first_mailed_date"),
        pl.col("send_datetime").cast(pl.Datetime, strict=False).max().cast(pl.Date).alias("last_mailed_date"),
        pl.col("campaign_name").unique().str.concat(", ").alias("mailing_campaigns"),
    ]


def build_mailed_derived_exprs() -> list[pl.Expr]:
    """
    Build expressions for mailed-derived metrics calculated after aggregation.

    Calculates:
    - Mailed delivery rate (percentage)

    Returns:
        list[pl.Expr]: List of derived expressions
    """
    return [
        (pl.col("mailed_delivered") * 100.0 / pl.col("mailed_campaigns_sent")).alias("mailed_delivery_rate"),
    ]
