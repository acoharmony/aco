# © 2025 HarmonyCares
# All rights reserved.

"""
Citation download expression builders.

Pure expression builders for URL normalization, content type detection,
and filename generation for citation downloads.

All functions are idempotent: same input produces same output.
"""

from __future__ import annotations

from datetime import datetime
from urllib.parse import urlparse

import polars as pl


def build_download_url_expr() -> pl.Expr:
    """
    Build expression to normalize URLs.

    Normalization:
        - Strip whitespace
        - Add https:// if no scheme (file:// URLs are kept)
        - Remove fragments (#section)

    Returns:
        pl.Expr: Normalized URL

    """
    return (
        pl.when(~pl.col("url").str.contains(r"://"))
        .then(pl.concat_str([pl.lit("https://"), pl.col("url").str.strip_chars()]))
        .otherwise(pl.col("url").str.strip_chars())
        .str.replace(r"#.*$", "")
        .alias("normalized_url")
    )


def build_url_hash_expr() -> pl.Expr:
    """
    Build expression to generate URL hash for deduplication.

    Uses Polars native hash function for deterministic hashing.

    Returns:
        pl.Expr: URL hash (hex string)

    """
    return (
        pl.col("normalized_url")
        .hash(seed=0)
        .cast(pl.Utf8)
        .str.slice(0, 16)
        .alias("url_hash")
    )


def build_download_filename_expr() -> pl.Expr:
    """
    Build expression to generate filename from URL.

    Uses URL hash + extension from content type.

    Returns:
        pl.Expr: Generated filename

    """
    return pl.concat_str(
        [
            pl.col("url_hash"),
            pl.lit("."),
            pl.col("content_extension"),
        ]
    ).alias("download_filename")


def build_content_type_detection_expr() -> pl.Expr:
    """
    Build expression to detect content type from URL.

    Detects:
        - PDF: .pdf extension
        - HTML: .html, .htm extension or no extension
        - Markdown: .md, .markdown extension
        - LaTeX: .tex extension

    Returns:
        pl.Expr: Content type expression

    """
    # Extract path and detect content type in one expression
    url_path_expr = pl.col("normalized_url").map_elements(
        lambda url: urlparse(url).path.lower(), return_dtype=pl.Utf8
    )

    return (
        pl.when(url_path_expr.str.ends_with(".pdf"))
        .then(pl.lit("pdf"))
        .when(url_path_expr.str.ends_with(".html") | url_path_expr.str.ends_with(".htm"))
        .then(pl.lit("html"))
        .when(url_path_expr.str.ends_with(".md") | url_path_expr.str.ends_with(".markdown"))
        .then(pl.lit("markdown"))
        .when(url_path_expr.str.ends_with(".tex"))
        .then(pl.lit("latex"))
        .otherwise(pl.lit("html"))  # Default to HTML for URLs without extension
        .alias("content_type")
    )


def build_content_extension_expr() -> pl.Expr:
    """
    Build expression to map content type to file extension.

    Returns:
        pl.Expr: File extension expression

    """
    return (
        pl.when(pl.col("content_type") == "pdf")
        .then(pl.lit("pdf"))
        .when(pl.col("content_type") == "html")
        .then(pl.lit("html"))
        .when(pl.col("content_type") == "markdown")
        .then(pl.lit("md"))
        .when(pl.col("content_type") == "latex")
        .then(pl.lit("tex"))
        .otherwise(pl.lit("txt"))
        .alias("content_extension")
    )


def build_download_timestamp_expr() -> pl.Expr:
    """
    Build expression for download timestamp.

    Uses current timestamp for tracking when download occurred.

    Returns:
        pl.Expr: Current timestamp

    """
    return pl.lit(datetime.now()).alias("download_timestamp")


def build_url_domain_expr() -> pl.Expr:
    """
    Build expression to extract domain from URL.

    Useful for categorizing sources (e.g., arxiv.org, pubmed.gov).

    Returns:
        pl.Expr: Domain from URL

    """
    return (
        pl.col("normalized_url")
        .map_elements(
            lambda url: urlparse(url).netloc,
            return_dtype=pl.Utf8,
        )
        .alias("url_domain")
    )


def build_is_valid_url_expr() -> pl.Expr:
    """
    Build expression to validate URL format.

    Checks for valid scheme (http/https) and netloc.

    Returns:
        pl.Expr: Boolean indicating valid URL

    """
    return (
        pl.col("normalized_url")
        .map_elements(
            lambda url: bool(urlparse(url).scheme in ["http", "https"] and urlparse(url).netloc),
            return_dtype=pl.Boolean,
        )
        .alias("is_valid_url")
    )
