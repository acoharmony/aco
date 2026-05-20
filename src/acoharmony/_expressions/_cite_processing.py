# © 2025 HarmonyCares
# All rights reserved.

"""
Citation processing expression builders.

Pure expression builders for normalizing, cleaning, and processing citation metadata.

All functions are idempotent: same input produces same output.
"""

from __future__ import annotations

import polars as pl


def build_title_normalization_expr() -> pl.Expr:
    """
    Build expression to normalize title.

    Normalization:
        - Trim whitespace
        - Remove extra spaces
        - Title case (optional)

    Returns:
        pl.Expr: Normalized title

    """
    return (
        pl.col("extracted_title")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .alias("normalized_title")
    )


def build_author_parsing_exprs() -> list[pl.Expr]:
    """
    Build expressions to parse author string.

    Extracts:
        - authors_list: Split on common delimiters
        - first_author: First author from list
        - author_count: Number of authors

    Returns:
        list[pl.Expr]: Author parsing expressions

    """
    # Compute author split inline for each expression to avoid dependencies
    author_split = pl.col("extracted_author").str.split(r"[,;]|\band\b").list.eval(
        pl.element().str.strip_chars()
    )

    return [
        # Split authors on common delimiters
        author_split.alias("authors_list"),
        # Extract first author (compute split inline)
        author_split.list.get(0).fill_null("").alias("first_author"),
        # Count authors (compute split inline)
        author_split.list.len().alias("author_count"),
    ]


def build_date_normalization_expr() -> pl.Expr:
    """
    Build expression to normalize publication date.

    Attempts to extract year from various date formats.
    Falls back to accessed_date year if no publication date found.

    Returns:
        pl.Expr: Normalized year

    """
    # Extract year from extracted_date
    extracted_year = pl.col("extracted_date").str.extract(r"(20\d{2}|19\d{2})", 1)

    # Extract year from accessed_date as fallback
    accessed_year = pl.col("accessed_date").str.extract(r"(20\d{2}|19\d{2})", 1)

    return pl.coalesce([extracted_year, accessed_year]).fill_null("").alias("publication_year")


def build_deduplication_key_expr() -> pl.Expr:
    """
    Build expression to generate deduplication key.

    Uses DOI if available, otherwise hash of (title + first_author + year).

    Returns:
        pl.Expr: Deduplication key

    """
    return (
        pl.when(pl.col("extracted_doi") != "")
        .then(pl.col("extracted_doi"))
        .otherwise(
            pl.concat_str(
                [
                    pl.col("normalized_title"),
                    pl.col("first_author"),
                    pl.col("publication_year"),
                ],
                separator="|",
            )
            .hash(seed=0)
            .cast(pl.Utf8)
            .str.slice(0, 16)
        )
        .alias("dedup_key")
    )


def build_citation_type_expr() -> pl.Expr:
    """
    Build expression to classify citation type.

    Types:
        - preprint: Has arXiv ID
        - journal_article: Has DOI or PubMed ID
        - book: Has ISBN
        - web_page: No identifiers, from HTML
        - document: Other

    Returns:
        pl.Expr: Citation type classification

    """
    return (
        pl.when(pl.col("extracted_arxiv_id") != "")
        .then(pl.lit("preprint"))
        .when((pl.col("extracted_doi") != "") | (pl.col("extracted_pubmed_id") != ""))
        .then(pl.lit("journal_article"))
        .when(pl.col("extracted_isbn") != "")
        .then(pl.lit("book"))
        .when(pl.col("content_type") == "html")
        .then(pl.lit("web_page"))
        .otherwise(pl.lit("document"))
        .alias("citation_type")
    )


def build_completeness_score_expr() -> pl.Expr:
    """
    Build expression to calculate citation completeness score.

    Score based on presence of key fields:
        - Title: 0.3
        - Author: 0.2
        - Year: 0.2
        - Identifier (DOI/PMID/arXiv): 0.2
        - Accessed date: 0.1 (always present as fallback)

    Returns:
        pl.Expr: Completeness score (0.0 to 1.0)

    """
    return (
        pl.when(pl.col("normalized_title") != "").then(0.3).otherwise(0.0)
        + pl.when(pl.col("first_author") != "").then(0.2).otherwise(0.0)
        + pl.when(pl.col("publication_year") != "").then(0.2).otherwise(0.0)
        + pl.when(
            (pl.col("extracted_doi") != "")
            | (pl.col("extracted_pubmed_id") != "")
            | (pl.col("extracted_arxiv_id") != "")
        )
        .then(0.2)
        .otherwise(0.0)
        + pl.when(pl.col("accessed_date") != "").then(0.1).otherwise(0.0)
    ).alias("completeness_score")


def build_processing_metadata_exprs() -> list[pl.Expr]:
    """
    Build expressions for processing metadata.

    Adds:
        - processing_timestamp
        - processing_version

    Returns:
        list[pl.Expr]: Processing metadata expressions

    """
    from datetime import datetime

    return [
        pl.lit(datetime.now()).alias("processing_timestamp"),
        pl.lit("cite_v1.0").alias("processing_version"),
    ]
