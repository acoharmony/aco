# © 2025 HarmonyCares
# All rights reserved.

"""
Voluntary alignment expressions for notebook calculations.

Provides reusable Polars expressions for SVA (Standard Voluntary Alignment)
status checks and metrics.
"""

import polars as pl


def build_has_valid_sva_expr(df_schema: list[str]) -> pl.Expr:
    """
    Build expression to check for valid SVA (Standard Voluntary Alignment).

    Valid SVA means: has_valid_voluntary_alignment column exists and is True

    Args:
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: Boolean expression - True if has valid SVA
    """
    if "has_valid_voluntary_alignment" in df_schema:
        return pl.col("has_valid_voluntary_alignment")
    else:
        return pl.lit(False)


def build_has_any_sva_expr(df_schema: list[str]) -> pl.Expr:
    """
    Build expression to check for any SVA (valid or expired).

    Args:
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: Boolean expression - True if has any SVA record
    """
    if "has_voluntary_alignment" in df_schema:
        return pl.col("has_voluntary_alignment")
    else:
        return pl.lit(False)


def build_needs_sva_renewal_expr(df_schema: list[str]) -> pl.Expr:
    """
    Build expression to check if beneficiary needs SVA renewal.

    Needs renewal if: has voluntary alignment but it's not currently valid
    (had SVA in the past but it expired)

    Args:
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: Boolean expression - True if needs renewal
    """
    has_any = build_has_any_sva_expr(df_schema)
    has_valid = build_has_valid_sva_expr(df_schema)

    return has_any & ~has_valid


def build_has_outreach_expr(df_schema: list[str]) -> pl.Expr:
    """
    Build expression to check if beneficiary has received outreach.

    Args:
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: Boolean expression - True if has received outreach
    """
    if "has_voluntary_outreach" in df_schema:
        return pl.col("has_voluntary_outreach")
    else:
        return pl.lit(False)


def build_email_engaged_expr(df_schema: list[str]) -> pl.Expr:
    """
    Build expression to check if beneficiary engaged with emails.

    Engaged means: opened at least one email

    Args:
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: Boolean expression - True if engaged with emails
    """
    if "voluntary_emails_opened" in df_schema:
        return pl.col("voluntary_emails_opened") > 0
    else:
        return pl.lit(False)


def build_needs_outreach_expr(df_schema: list[str]) -> pl.Expr:
    """
    Build expression to check if beneficiary needs outreach.

    Needs outreach if: doesn't have valid SVA AND hasn't received outreach yet

    Args:
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: Boolean expression - True if needs outreach
    """
    has_valid = build_has_valid_sva_expr(df_schema)
    has_outreach = build_has_outreach_expr(df_schema)

    return ~has_valid & ~has_outreach


def build_voluntary_alignment_aggregations(
    df_schema: list[str],
) -> dict[str, pl.Expr]:
    """
    Build dictionary of aggregation expressions for voluntary alignment stats.

    Returns expressions that can be used in a .select() call to calculate
    all voluntary alignment metrics at once.

    Args:
        df_schema: List of column names in the dataframe

    Returns:
        dict[str, pl.Expr]: Dictionary of named aggregation expressions
    """
    has_valid = build_has_valid_sva_expr(df_schema)
    has_any = build_has_any_sva_expr(df_schema)
    needs_renewal = build_needs_sva_renewal_expr(df_schema)
    has_outreach = build_has_outreach_expr(df_schema)
    email_engaged = build_email_engaged_expr(df_schema)
    needs_outreach = build_needs_outreach_expr(df_schema)

    return {
        "has_valid_sva_count": has_valid.sum(),
        "has_any_sva_count": has_any.sum(),
        "needs_renewal_count": needs_renewal.sum(),
        "has_outreach_count": has_outreach.sum(),
        "email_engaged_count": email_engaged.sum(),
        "needs_outreach_count": needs_outreach.sum(),
        "total_count": pl.len(),
    }
