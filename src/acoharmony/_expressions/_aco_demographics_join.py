# © 2025 HarmonyCares
# All rights reserved.

"""
Demographics join expression builders.

Pure expression builders for selecting and renaming demographic columns
from beneficiary_demographics table.
"""

import polars as pl


def build_demographics_select_expr() -> list[pl.Expr]:
    """
    Build expressions to select and rename demographic columns.

    Returns column expressions for:
    - current_mbi (join key)
    - Name fields (first, last, middle)
    - Address fields (line 1, city, state, zip)

    Returns:
        list[pl.Expr]: List of column expressions for demographics
    """
    return [
        pl.col("current_bene_mbi_id").alias("current_mbi"),
        pl.col("bene_fst_name").alias("bene_first_name"),
        pl.col("bene_lst_name").alias("bene_last_name"),
        pl.col("bene_mdl_name").alias("bene_middle_initial"),
        pl.col("bene_line_1_adr").alias("bene_address_line_1"),
        pl.col("geo_zip_plc_name").alias("bene_city"),
        pl.col("geo_usps_state_cd").alias("bene_state"),
        pl.col("geo_zip5_cd").alias("bene_zip"),
    ]


def build_county_expr(county_col: str = "bene_fips_cnty_cd") -> pl.Expr:
    """
    Build expression to extract county from demographics.

    Args:
        county_col: Column name for county code

    Returns:
        pl.Expr: Expression to alias county column
    """
    return pl.col(county_col).alias("bene_county")


def build_zip5_expr(zip_col: str = "bene_zip") -> pl.Expr:
    """
    Build expression to extract first 5 digits of ZIP code.

    Args:
        zip_col: Column name for ZIP code

    Returns:
        pl.Expr: Expression to extract ZIP-5
    """
    return pl.col(zip_col).str.slice(0, 5).alias("bene_zip_5")
