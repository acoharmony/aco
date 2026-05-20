# © 2025 HarmonyCares
# All rights reserved.

"""
ALR (Alignment Report) data preparation expressions.

Provides Polars expressions for preparing MSSP alignment data from ALR files.
These expressions handle MBI crosswalk normalization, ACO ID extraction, and date parsing.
"""

import polars as pl

from .._parsers._aco_id import extract_aco_id


def build_alr_mbi_crosswalk_expr(mbi_map: dict) -> pl.Expr:
    """
    Build expression to normalize MBIs using crosswalk map.

    Args:
        mbi_map: Dictionary mapping previous_mbi -> current_mbi

    Returns:
        pl.Expr: Expression that maps bene_mbi to current_mbi
    """
    return (
        pl.col("bene_mbi")
        .map_elements(lambda x: mbi_map.get(x, x), return_dtype=pl.String)
        .alias("current_mbi")
    )


def build_alr_aco_id_expr() -> pl.Expr:
    """
    Build expression to extract ACO ID from source filename.

    Returns:
        pl.Expr: Expression that parses ACO ID from source_filename
    """
    return (
        pl.col("source_filename")
        .map_elements(extract_aco_id, return_dtype=pl.String)
        .alias("aco_id")
    )


def build_alr_program_expr() -> pl.Expr:
    """
    Build expression for MSSP program literal.

    Returns:
        pl.Expr: Expression that sets program to 'MSSP'
    """
    return pl.lit("MSSP").alias("program")


def build_alr_file_date_expr() -> pl.Expr:
    """
    Build expression to parse file date string to Date type.

    Returns:
        pl.Expr: Expression that parses file_date string to Date
    """
    return pl.col("file_date").str.strptime(pl.Date, "%Y-%m-%d").alias("file_date_parsed")


def build_alr_preparation_exprs(mbi_map: dict) -> list[pl.Expr]:
    """
    Build all ALR data preparation expressions.

    Args:
        mbi_map: Dictionary mapping previous_mbi -> current_mbi

    Returns:
        list[pl.Expr]: All expressions for ALR data preparation
    """
    return [
        build_alr_mbi_crosswalk_expr(mbi_map),
        build_alr_aco_id_expr(),
        build_alr_program_expr(),
        build_alr_file_date_expr(),
    ]


def build_alr_select_expr() -> list[pl.Expr]:
    """
    Build select expressions for final ALR columns.

    Returns:
        list[pl.Expr]: Select expressions for ALR output columns
    """
    return [
        pl.col("current_mbi"),
        pl.col("aco_id"),
        pl.col("program"),
        pl.col("file_date_parsed"),
        pl.col("bene_mbi"),
        pl.lit(None).cast(pl.Date).alias("bene_date_of_death"),  # Will be filled from demographics
    ]
