"""Tests for acoharmony._expressions._cite_download module."""

import polars as pl
import pytest

from acoharmony._expressions._cite_download import (
    build_download_filename_expr,
    build_download_timestamp_expr,
    build_is_valid_url_expr,
)


class TestCiteDownloadExpressions:
    """Cover uncovered function bodies."""

    @pytest.mark.unit
    def test_download_filename(self):
        """Cover line 72."""
        df = pl.DataFrame({
            "url_hash": ["abc123"],
            "content_extension": ["pdf"],
        })
        result = df.select(build_download_filename_expr())
        assert result["download_filename"][0] == "abc123.pdf"

    @pytest.mark.unit
    def test_download_timestamp(self):
        """Cover line 146."""
        df = pl.DataFrame({"id": [1]})
        result = df.select(build_download_timestamp_expr())
        assert "download_timestamp" in result.columns
        assert result["download_timestamp"][0] is not None

    @pytest.mark.unit
    def test_is_valid_url(self):
        """Cover line 179."""
        df = pl.DataFrame({
            "normalized_url": ["https://example.com/paper.pdf", "not-a-url", "http://valid.org"],
        })
        result = df.select(build_is_valid_url_expr())
        assert result["is_valid_url"][0] is True
        assert result["is_valid_url"][1] is False
        assert result["is_valid_url"][2] is True
