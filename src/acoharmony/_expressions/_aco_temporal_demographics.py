# © 2025 HarmonyCares
# All rights reserved.

"""
Demographics data preparation expressions for temporal alignment.

Provides Polars expressions for extracting and normalizing beneficiary demographic
information including birth/death dates, sex, race, and geographic location.
"""

import polars as pl


def build_demographics_mbi_expr() -> pl.Expr:
    """
    Build expression to map demographics MBI to current_mbi.

    Returns:
        pl.Expr: Expression that aliases current_bene_mbi_id to current_mbi
    """
    return pl.col("current_bene_mbi_id").alias("current_mbi")


def build_demographics_select_expr() -> list[pl.Expr]:
    """
    Build select expressions for demographics columns.

    Returns:
        list[pl.Expr]: Select expressions for demographics output
    """
    return [
        pl.col("current_mbi"),
        pl.col("bene_dob").alias("birth_date"),
        pl.col("bene_death_dt").alias("death_date"),
        pl.col("bene_sex_cd").alias("sex"),
        pl.col("bene_race_cd").alias("race"),
        pl.lit(None).alias("ethnicity"),  # Not in our demographics
        pl.col("bene_fips_state_cd").alias("state"),
        pl.col("bene_fips_cnty_cd").alias("county"),
        pl.col("bene_zip_cd").alias("zip_code"),
    ]
