# © 2025 HarmonyCares
# All rights reserved.

"""
Citation enrichment expression builders.

Pure expression builders for API enrichment placeholders.
These are stubs for future integration with external citation APIs.

All functions are idempotent: same input produces same output.
"""

from __future__ import annotations

import polars as pl


def build_crossref_lookup_expr() -> pl.Expr:
    """
    Build expression for Crossref API enrichment (stub).

    Future: Query Crossref API with DOI to enrich metadata.

    Returns:
        pl.Expr: Placeholder for Crossref data

    """
    return pl.lit("").alias("crossref_enrichment")


def build_semantic_scholar_lookup_expr() -> pl.Expr:
    """
    Build expression for Semantic Scholar API enrichment (stub).

    Future: Query Semantic Scholar API for citation graph data.

    Returns:
        pl.Expr: Placeholder for Semantic Scholar data

    """
    return pl.lit("").alias("semantic_scholar_enrichment")


def build_pubmed_lookup_expr() -> pl.Expr:
    """
    Build expression for PubMed API enrichment (stub).

    Future: Query PubMed API with PMID to enrich metadata.

    Returns:
        pl.Expr: Placeholder for PubMed data

    """
    return pl.lit("").alias("pubmed_enrichment")


def build_arxiv_lookup_expr() -> pl.Expr:
    """
    Build expression for arXiv API enrichment (stub).

    Future: Query arXiv API to enrich preprint metadata.

    Returns:
        pl.Expr: Placeholder for arXiv data

    """
    return pl.lit("").alias("arxiv_enrichment")


def build_enrichment_needed_expr() -> pl.Expr:
    """
    Build expression to flag citations needing enrichment.

    Flags citations with identifiers but low completeness score.

    Returns:
        pl.Expr: Boolean indicating enrichment needed

    """
    return (
        pl.when(
            (pl.col("completeness_score") < 0.7)
            & (
                (pl.col("extracted_doi") != "")
                | (pl.col("extracted_pubmed_id") != "")
                | (pl.col("extracted_arxiv_id") != "")
            )
        )
        .then(True)
        .otherwise(False)
        .alias("enrichment_needed")
    )


def build_enrichment_placeholder_exprs() -> list[pl.Expr]:
    """
    Build all enrichment placeholder expressions.

    Returns:
        list[pl.Expr]: Enrichment expressions

    """
    return [
        build_crossref_lookup_expr(),
        build_semantic_scholar_lookup_expr(),
        build_pubmed_lookup_expr(),
        build_arxiv_lookup_expr(),
        build_enrichment_needed_expr(),
    ]
