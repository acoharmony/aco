# © 2025 HarmonyCares
# All rights reserved.

"""
Email campaign expression builders for voluntary_alignment consolidation.

Pure expression builders for aggregating email send and engagement data per beneficiary.
"""

import polars as pl


def build_email_aggregation_exprs() -> list[pl.Expr]:
    """
    Build expressions to aggregate email campaign data per beneficiary.

    Aggregates:
    - Total campaigns sent
    - Emails opened and clicked
    - First and last email dates
    - Open and click rates

    Returns:
        list[pl.Expr]: List of aggregation expressions for email data
    """
    return [
        pl.col("email_id").count().alias("email_campaigns_sent"),
        (pl.col("has_been_opened") == "true").sum().alias("emails_opened"),
        (pl.col("has_been_clicked") == "true").sum().alias("emails_clicked"),
        pl.col("send_datetime").cast(pl.Datetime, strict=False).min().cast(pl.Date).alias("first_email_date"),
        pl.col("send_datetime").cast(pl.Datetime, strict=False).max().cast(pl.Date).alias("last_email_date"),
    ]


def build_email_derived_exprs() -> list[pl.Expr]:
    """
    Build expressions for email-derived metrics calculated after aggregation.

    Calculates:
    - Email open rate (percentage)
    - Email click rate (percentage)

    Returns:
        list[pl.Expr]: List of derived expressions
    """
    return [
        (pl.col("emails_opened") * 100.0 / pl.col("email_campaigns_sent")).alias("email_open_rate"),
        (pl.col("emails_clicked") * 100.0 / pl.col("email_campaigns_sent")).alias("email_click_rate"),
    ]
