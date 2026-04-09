# © 2025 HarmonyCares
# All rights reserved.

"""
Office location matching expression builders.

Pure expression builders for matching beneficiary ZIP codes to office locations
using direct matching and fuzzy (distance-based) matching strategies.
"""

import polars as pl


def build_direct_office_select_expr() -> list[pl.Expr]:
    """
    Build expressions to select columns from office_zip for direct matching.

    Direct match: ZIPs where office_name is populated.

    Returns:
        list[pl.Expr]: Columns for direct office matching (zip_code, office_name, market)
    """
    return [
        pl.col("zip_code"),
        pl.col("office_name"),
        pl.col("market"),
    ]


def build_fuzzy_office_select_expr() -> list[pl.Expr]:
    """
    Build expressions to select columns from office_zip for fuzzy matching.

    Fuzzy match: ZIPs with office_distance populated for nearest-neighbor lookup.

    Returns:
        list[pl.Expr]: Columns for fuzzy matching (zip_code, office_distance, office_name, market)
    """
    return [
        pl.col("zip_code"),
        pl.col("office_distance"),
        pl.col("office_name"),
        pl.col("market"),
    ]


def build_office_location_alias_expr(market_col: str = "market") -> pl.Expr:
    """
    Build expression to rename market to office_location.

    Args:
        market_col: Column name for market

    Returns:
        pl.Expr: Expression to alias market as office_location
    """
    return pl.col(market_col).alias("office_location")
