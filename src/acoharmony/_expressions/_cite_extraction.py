# © 2025 HarmonyCares
# All rights reserved.

"""
Citation extraction expression builders.

Pure expression builders for extracting citation identifiers (DOI, PubMed, arXiv)
and metadata from parsed document text.

All functions are idempotent: same input produces same output.
"""

from __future__ import annotations

import polars as pl


def build_doi_extraction_expr() -> pl.Expr:
    """
    Build expression to extract DOI from text.

    Pattern: 10.xxxx/xxxxx (DOI prefix always starts with 10.)

    Returns:
        pl.Expr: DOI string (empty if not found)

    """
    return (
        pl.col("text_content")
        .str.extract(r"(10\.\d{4,}/[^\s]+)", 1)
        .str.replace(r"[.,;]$", "")  # Remove trailing punctuation
        .fill_null("")
        .alias("extracted_doi")
    )


def build_pubmed_extraction_expr() -> pl.Expr:
    """
    Build expression to extract PubMed ID from text.

    Pattern: PMID: 12345678 or PMID 12345678

    Returns:
        pl.Expr: PubMed ID string (empty if not found)

    """
    return (
        pl.col("text_content")
        .str.extract(r"PMID:?\s*(\d{7,})", 1)
        .fill_null("")
        .alias("extracted_pubmed_id")
    )


def build_arxiv_extraction_expr() -> pl.Expr:
    """
    Build expression to extract arXiv ID from text.

    Pattern: arXiv:1234.5678 or arXiv:1234.56789

    Returns:
        pl.Expr: arXiv ID string (empty if not found)

    """
    return (
        pl.col("text_content")
        .str.extract(r"arXiv:(\d{4}\.\d{4,5})", 1)
        .fill_null("")
        .alias("extracted_arxiv_id")
    )


def build_isbn_extraction_expr() -> pl.Expr:
    """
    Build expression to extract ISBN from text.

    Pattern: ISBN-13 (978-x-xxx-xxxxx-x) or ISBN-10

    Returns:
        pl.Expr: ISBN string (empty if not found)

    """
    return (
        pl.col("text_content")
        .str.extract(r"ISBN[:\s]*([0-9-]{10,17})", 1)
        .str.replace("-", "")  # Remove hyphens
        .fill_null("")
        .alias("extracted_isbn")
    )


def build_citation_identifier_exprs() -> list[pl.Expr]:
    """
    Build all citation identifier extraction expressions.

    Extracts: DOI, PubMed ID, arXiv ID, ISBN

    Returns:
        list[pl.Expr]: List of extraction expressions

    """
    return [
        build_doi_extraction_expr(),
        build_pubmed_extraction_expr(),
        build_arxiv_extraction_expr(),
        build_isbn_extraction_expr(),
    ]


def build_has_citation_expr() -> pl.Expr:
    """
    Build expression to detect if document has citations.

    Checks for common citation markers:
        - "references" section
        - "bibliography" section
        - Citation numbers [1], [2], etc.
        - Year citations (2020), (2021), etc.

    Returns:
        pl.Expr: Boolean indicating presence of citations

    """
    return (
        pl.when(
            pl.col("text_content")
            .str.to_lowercase()
            .str.contains(r"references|bibliography|works cited")
            | pl.col("text_content").str.contains(r"\[\d+\]")
            | pl.col("text_content").str.contains(r"\(20\d{2}\)")
        )
        .then(True)
        .otherwise(False)
        .alias("has_citations")
    )


def build_reference_count_expr() -> pl.Expr:
    """
    Build expression to count references in text.

    Counts citation numbers [1], [2], [3], etc.

    Returns:
        pl.Expr: Number of references found

    """
    return pl.col("text_content").str.extract_all(r"\[(\d+)\]").list.len().alias("reference_count")


def build_title_extraction_expr() -> pl.Expr:
    """
    Build expression to extract title from metadata fields.

    After normalization, uses the normalized "title" column.

    Returns:
        pl.Expr: Extracted title

    """
    return pl.col("title").fill_null("").alias("extracted_title")


def build_author_extraction_expr() -> pl.Expr:
    """
    Build expression to extract author from metadata fields.

    After normalization, uses the normalized "author" column.

    Returns:
        pl.Expr: Extracted author

    """
    return pl.col("author").fill_null("").alias("extracted_author")


def build_date_extraction_expr() -> pl.Expr:
    """
    Build expression to extract publication date from metadata.

    After normalization, uses the normalized "date" column.

    Returns:
        pl.Expr: Extracted date

    """
    return pl.col("date").fill_null("").alias("extracted_date")


def build_accessed_date_expr() -> pl.Expr:
    """
    Build expression to capture access date.

    Returns current timestamp as the date content was accessed/downloaded.

    Returns:
        pl.Expr: Current date in ISO format

    """
    from datetime import datetime

    return pl.lit(datetime.now().date().isoformat()).alias("accessed_date")


def build_citation_metadata_exprs() -> list[pl.Expr]:
    """
    Build all citation metadata extraction expressions.

    Extracts: title, author, date, accessed_date

    Returns:
        list[pl.Expr]: List of metadata extraction expressions

    """
    return [
        build_title_extraction_expr(),
        build_author_extraction_expr(),
        build_date_extraction_expr(),
        build_accessed_date_expr(),
    ]
