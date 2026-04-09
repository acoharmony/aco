# © 2025 HarmonyCares
# All rights reserved.

"""
Citation storage expression builders.

Pure expression builders for generating storage paths and file hashes for citation files.

All functions are idempotent: same input produces same output.
"""

from __future__ import annotations

import polars as pl


def build_file_hash_expr() -> pl.Expr:
    """
    Build expression to generate file hash from dedup_key.

    Uses Polars native hash function for deterministic hashing.

    Returns:
        pl.Expr: File hash (16-char hex string)

    """
    return (
        pl.col("dedup_key")
        .hash(seed=0)
        .cast(pl.Utf8)
        .str.slice(0, 16)
        .alias("file_hash")
    )


def build_raw_storage_path_expr() -> pl.Expr:
    """
    Build expression to generate raw file storage path.

    Path: cites/raw/{content_type}/{url_hash}.{content_extension}

    Returns:
        pl.Expr: Raw storage path

    """
    return pl.concat_str(
        [
            pl.lit("cites/raw/"),
            pl.col("content_type"),
            pl.lit("/"),
            pl.col("url_hash"),
            pl.lit("."),
            pl.col("content_extension"),
        ]
    ).alias("raw_storage_path")


def build_corpus_storage_path_expr() -> pl.Expr:
    """
    Build expression to generate corpus storage path.

    Path: cites/corpus/{file_hash}.parquet

    Returns:
        pl.Expr: Corpus storage path

    """
    # Compute file_hash inline to avoid dependency
    file_hash_expr = pl.col("dedup_key").hash(seed=0).cast(pl.Utf8).str.slice(0, 16)

    return pl.concat_str(
        [
            pl.lit("cites/corpus/"),
            file_hash_expr,
            pl.lit(".parquet"),
        ]
    ).alias("corpus_storage_path")


def build_corpus_json_path_expr() -> pl.Expr:
    """
    Build expression to generate JSON corpus storage path.

    Path: cites/corpus/{file_hash}.json

    Returns:
        pl.Expr: JSON corpus storage path

    """
    # Compute file_hash inline to avoid dependency
    file_hash_expr = pl.col("dedup_key").hash(seed=0).cast(pl.Utf8).str.slice(0, 16)

    return pl.concat_str(
        [
            pl.lit("cites/corpus/"),
            file_hash_expr,
            pl.lit(".json"),
        ]
    ).alias("corpus_json_path")


def build_storage_tier_expr() -> pl.Expr:
    """
    Build expression to determine storage tier.

    Tiers:
        - raw: Original downloaded content
        - corpus: Processed and standardized

    Returns:
        pl.Expr: Storage tier

    """
    return pl.lit("corpus").alias("storage_tier")


def build_storage_timestamp_expr() -> pl.Expr:
    """
    Build expression for storage timestamp.

    Returns:
        pl.Expr: Current timestamp

    """
    from datetime import datetime

    return pl.lit(datetime.now()).alias("storage_timestamp")


def build_storage_metadata_exprs() -> list[pl.Expr]:
    """
    Build all storage-related expressions.

    Returns:
        list[pl.Expr]: Storage metadata expressions

    """
    return [
        build_file_hash_expr(),
        build_raw_storage_path_expr(),
        build_corpus_storage_path_expr(),
        build_corpus_json_path_expr(),
        build_storage_tier_expr(),
        build_storage_timestamp_expr(),
    ]
