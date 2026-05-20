"""Tests for acoharmony._expressions._cite_enrichment module."""

import polars as pl
import pytest

from acoharmony._expressions._cite_enrichment import (
    build_arxiv_lookup_expr,
    build_crossref_lookup_expr,
    build_enrichment_needed_expr,
    build_enrichment_placeholder_exprs,
    build_pubmed_lookup_expr,
    build_semantic_scholar_lookup_expr,
)


class TestCiteEnrichmentStubs:
    """Cover all stub expression builders."""

    @pytest.mark.unit
    def test_crossref_lookup(self):
        df = pl.DataFrame({"id": [1]})
        result = df.select(build_crossref_lookup_expr())
        assert "crossref_enrichment" in result.columns
        assert result["crossref_enrichment"][0] == ""

    @pytest.mark.unit
    def test_semantic_scholar_lookup(self):
        df = pl.DataFrame({"id": [1]})
        result = df.select(build_semantic_scholar_lookup_expr())
        assert "semantic_scholar_enrichment" in result.columns

    @pytest.mark.unit
    def test_pubmed_lookup(self):
        df = pl.DataFrame({"id": [1]})
        result = df.select(build_pubmed_lookup_expr())
        assert "pubmed_enrichment" in result.columns

    @pytest.mark.unit
    def test_arxiv_lookup(self):
        df = pl.DataFrame({"id": [1]})
        result = df.select(build_arxiv_lookup_expr())
        assert "arxiv_enrichment" in result.columns

    @pytest.mark.unit
    def test_enrichment_needed(self):
        df = pl.DataFrame({
            "completeness_score": [0.5, 0.9, 0.3],
            "extracted_doi": ["10.1234/test", "", ""],
            "extracted_pubmed_id": ["", "", "12345"],
            "extracted_arxiv_id": ["", "", ""],
        })
        result = df.select(build_enrichment_needed_expr())
        assert "enrichment_needed" in result.columns
        # Low completeness + has DOI → True
        assert result["enrichment_needed"][0] is True
        # High completeness → False
        assert result["enrichment_needed"][1] is False
        # Low completeness + has PMID → True
        assert result["enrichment_needed"][2] is True

    @pytest.mark.unit
    def test_enrichment_placeholder_exprs(self):
        exprs = build_enrichment_placeholder_exprs()
        assert len(exprs) == 5
