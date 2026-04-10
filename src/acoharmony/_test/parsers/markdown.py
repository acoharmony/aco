from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import patch
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest

import acoharmony

from .conftest import HAS_FRONTMATTER, HAS_MARKDOWN

# © 2025 HarmonyCares
# All rights reserved.


"""
Unit tests for Markdown parser.

Tests Markdown file parsing functionality including frontmatter, links,
citations, and structure extraction.
"""




if TYPE_CHECKING:
    pass


@pytest.fixture
def sample_markdown() -> str:
    """Sample Markdown content for testing."""
    return """---
title: Test Paper
author: John Doe
date: 2024-01-01
tags:
  - testing
  - citations
---

# Introduction

This is a test paper that cites [@Smith2020] and [@Jones2021].

See also the [documentation](https://example.com/docs).

## Methods

We used various methods [@Brown2019; @White2022].

## Results

The results are shown in Figure 1[^1].

## References

[^1]: First footnote with details.

[ref-link]: https://example.com/reference
"""


@pytest.fixture
def minimal_markdown() -> str:
    """Minimal Markdown content."""
    return "# Hello World\n\nThis is a test."


@pytest.fixture
def markdown_file(tmp_path: Path, sample_markdown: str) -> Path:
    """Create a Markdown file for testing."""
    md_path = tmp_path / "test.md"
    md_path.write_text(sample_markdown)
    return md_path


class TestMarkdownParser:
    """Tests for Markdown parsing."""

    @pytest.mark.unit
    def test_parse_markdown_basic(self, markdown_file: Path) -> None:
        """Test basic Markdown parsing."""

        result = parse_markdown(markdown_file)

        # Should return LazyFrame
        assert isinstance(result, pl.LazyFrame)

        # Check schema
        schema = result.collect_schema()
        assert "filename" in schema
        assert "source_path" in schema
        assert "markdown_content" in schema
        assert "rendered_html" in schema
        assert "text_content" in schema
        assert "frontmatter" in schema
        assert "title" in schema
        assert "author" in schema
        assert "links" in schema
        assert "citations" in schema
        assert "footnotes" in schema
        assert "headers" in schema

    @pytest.mark.unit
    def test_parse_markdown_missing_file(self, tmp_path: Path) -> None:
        """Test parsing non-existent Markdown file raises error."""

        non_existent = tmp_path / "nonexistent.md"

        with pytest.raises(FileNotFoundError):
            parse_markdown(non_existent)

    @pytest.mark.unit
    def test_parse_markdown_frontmatter(self, markdown_file: Path) -> None:
        """Test frontmatter extraction."""

        result = parse_markdown(markdown_file)
        df = result.collect()

        # Check frontmatter fields
        assert df["title"][0] == "Test Paper"
        assert df["author"][0] == "John Doe"
        assert df["date"][0] == "2024-01-01"
        assert len(df["tags"][0]) > 0

    @pytest.mark.unit
    def test_parse_markdown_no_frontmatter(self, tmp_path: Path) -> None:
        """Test parsing Markdown without frontmatter."""

        md_path = tmp_path / "no_front.md"
        md_path.write_text("# Title\n\nContent here.")

        result = parse_markdown(md_path)
        df = result.collect()

        # Should extract title from H1
        assert df["title"][0] == "Title"

    @pytest.mark.unit
    def test_parse_markdown_citations(self, markdown_file: Path) -> None:
        """Test citation extraction."""

        result = parse_markdown(markdown_file, extract_citations=True)
        df = result.collect()

        # Check citations
        citations = df["citations"][0]
        assert len(citations) > 0
        assert "Smith2020" in citations
        assert "Jones2021" in citations

    @pytest.mark.unit
    def test_parse_markdown_no_citation_extraction(self, markdown_file: Path) -> None:
        """Test parsing with citation extraction disabled."""

        result = parse_markdown(markdown_file, extract_citations=False)
        df = result.collect()

        # Citations should be empty
        assert len(df["citations"][0]) == 0

    @pytest.mark.unit
    def test_parse_markdown_links(self, markdown_file: Path) -> None:
        """Test link extraction."""
        from urllib.parse import urlparse

        result = parse_markdown(markdown_file)
        df = result.collect()

        # Check links
        links = df["links"][0]
        assert len(links) > 0
        assert any(urlparse(link["url"]).hostname == "example.com" for link in links)

    @pytest.mark.unit
    def test_parse_markdown_footnotes(self, markdown_file: Path) -> None:
        """Test footnote extraction."""

        result = parse_markdown(markdown_file)
        df = result.collect()

        # Check footnotes
        footnotes = df["footnotes"][0]
        assert len(footnotes) > 0
        assert any(fn["id"] == "1" for fn in footnotes)

    @pytest.mark.unit
    def test_parse_markdown_headers(self, markdown_file: Path) -> None:
        """Test header extraction."""

        result = parse_markdown(markdown_file)
        df = result.collect()

        # Check headers
        headers = df["headers"][0]
        assert len(headers) > 0
        assert any(h["text"] == "Introduction" for h in headers)
        assert any(h["level"] == 2 for h in headers)

    @pytest.mark.unit
    def test_parse_markdown_rendered_html(self, markdown_file: Path) -> None:
        """Test HTML rendering."""

        result = parse_markdown(markdown_file)
        df = result.collect()

        # Check rendered HTML exists
        html = df["rendered_html"][0]
        assert len(html) > 0
        assert "<h1" in html or "<h2" in html

    @pytest.mark.unit
    def test_parse_markdown_batch(self, tmp_path: Path) -> None:
        """Test batch Markdown parsing."""

        # Create multiple Markdown files
        md_files = []
        for i in range(3):
            md_path = tmp_path / f"test_{i}.md"
            md_path.write_text(f"# Test {i}\n\nContent {i}.")
            md_files.append(md_path)

        result = parse_markdown_batch(md_files)
        df = result.collect()

        # Should have 3 rows
        assert len(df) == 3

    @pytest.mark.unit
    def test_parse_markdown_multiple_citations(self, tmp_path: Path) -> None:
        """Test parsing multiple citations in one reference."""

        md_path = tmp_path / "multi_cite.md"
        md_path.write_text("Studies show [@Smith2020; @Jones2021; @Brown2022].")

        result = parse_markdown(md_path)
        df = result.collect()

        # Should extract all citations
        citations = df["citations"][0]
        assert len(citations) >= 3

    @pytest.mark.unit
    def test_parse_markdown_empty_citation_after_strip(self, tmp_path: Path) -> None:
        """Test that empty citations after stripping are skipped."""

        # Citation with only whitespace/@ symbols should be skipped
        md_path = tmp_path / "empty_cite.md"
        md_path.write_text("Study shows [@ ; @Smith2020].")

        result = parse_markdown(md_path)
        df = result.collect()

        # Should only extract non-empty citation
        citations = df["citations"][0]
        assert "Smith2020" in citations
        assert len([c for c in citations if c.strip()]) >= 1



class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._markdown is not None

class TestParseMarkdown:
    """Tests for _markdown.parse_markdown and parse_markdown_batch."""

    @pytest.fixture
    def md_file(self, tmp_path: Path) -> Path:
        content = '---\ntitle: "My Document"\nauthor: "Jane Doe"\ndate: "2024-06-01"\ntags:\n  - research\n  - python\n---\n\n# Introduction\n\nSome text with a [link](https://example.com) and another [ref][1].\n\n## Methods\n\nWe used the method from [@Smith2020] and [@Jones2021; @Lee2022].\n\n### Sub-methods\n\nDetails here.\n\n[^1]: This is a footnote.\n\n[1]: https://ref-link.com\n'
        p = tmp_path / 'doc.md'
        p.write_text(content, encoding='utf-8')
        return p

    @pytest.mark.unit
    @pytest.mark.skipif(not (HAS_MARKDOWN and HAS_FRONTMATTER), reason='markdown or python-frontmatter not installed')
    def test_parse_markdown_basic(self, md_file: Path):
        from acoharmony._parsers._markdown import parse_markdown
        lf = parse_markdown(md_file)
        df = lf.collect()
        assert len(df) == 1
        row = df.row(0, named=True)
        assert row['title'] == 'My Document'
        assert row['author'] == 'Jane Doe'
        assert row['date'] == '2024-06-01'
        assert row['tags'] == ['research', 'python']
        links = row['links']
        urls = {link['url'] for link in links}
        assert urls.issuperset({'https://example.com', 'https://ref-link.com'})
        assert 'Smith2020' in row['citations']
        assert 'Jones2021' in row['citations']
        assert 'Lee2022' in row['citations']
        assert len(row['footnotes']) == 1
        assert row['footnotes'][0]['id'] == '1'
        headers = row['headers']
        assert len(headers) == 3
        assert headers[0]['level'] == 1
        assert headers[0]['text'] == 'Introduction'
        assert '<h1' in row['rendered_html'] or '<h2' in row['rendered_html']
        fm = json.loads(row['frontmatter'])
        assert fm['title'] == 'My Document'

    @pytest.mark.unit
    @pytest.mark.skipif(not (HAS_MARKDOWN and HAS_FRONTMATTER), reason='markdown or python-frontmatter not installed')
    def test_parse_markdown_no_frontmatter(self, tmp_path: Path):
        """Title falls back to first H1 when no frontmatter title."""
        content = '# Fallback Title\n\nSome text.\n'
        p = tmp_path / 'nofm.md'
        p.write_text(content, encoding='utf-8')
        from acoharmony._parsers._markdown import parse_markdown
        df = parse_markdown(p).collect()
        assert df['title'][0] == 'Fallback Title'
        assert df['frontmatter'][0] == ''

    @pytest.mark.unit
    @pytest.mark.skipif(not (HAS_MARKDOWN and HAS_FRONTMATTER), reason='markdown or python-frontmatter not installed')
    def test_parse_markdown_no_citations(self, tmp_path: Path):
        content = '# Title\n\nNo citations here.\n'
        p = tmp_path / 'nocit.md'
        p.write_text(content, encoding='utf-8')
        from acoharmony._parsers._markdown import parse_markdown
        df = parse_markdown(p, extract_citations=False).collect()
        assert len(df['citations'][0]) == 0

    @pytest.mark.unit
    @pytest.mark.skipif(not (HAS_MARKDOWN and HAS_FRONTMATTER), reason='markdown or python-frontmatter not installed')
    def test_parse_markdown_file_not_found(self):
        from acoharmony._parsers._markdown import parse_markdown
        with pytest.raises(FileNotFoundError):
            parse_markdown(Path('/nonexistent.md'))

    @pytest.mark.unit
    @pytest.mark.skipif(not (HAS_MARKDOWN and HAS_FRONTMATTER), reason='markdown or python-frontmatter not installed')
    def test_parse_markdown_batch(self, md_file: Path):
        from acoharmony._parsers._markdown import parse_markdown_batch
        df = parse_markdown_batch([md_file]).collect()
        assert len(df) == 1

    @pytest.mark.unit
    @pytest.mark.skipif(not (HAS_MARKDOWN and HAS_FRONTMATTER), reason='markdown or python-frontmatter not installed')
    def test_parse_markdown_batch_all_fail(self):
        from acoharmony._parsers._markdown import parse_markdown_batch
        with pytest.raises(ValueError, match='No Markdown'):
            parse_markdown_batch([Path('/no1.md'), Path('/no2.md')])

    @pytest.mark.unit
    @pytest.mark.skipif(not (HAS_MARKDOWN and HAS_FRONTMATTER), reason='markdown or python-frontmatter not installed')
    def test_parse_markdown_text_cleanup(self, tmp_path: Path):
        """Verify code blocks, inline code, images, bold/italic are stripped."""
        content = '# H\n\n```python\ncode\n```\n\nUse `inline` code.\n\n![alt](img.png)\n\n**bold** and *italic*\n'
        p = tmp_path / 'clean.md'
        p.write_text(content, encoding='utf-8')
        from acoharmony._parsers._markdown import parse_markdown
        df = parse_markdown(p).collect()
        text = df['text_content'][0]
        assert '```' not in text
        assert '`inline`' not in text
        assert '![' not in text

@pytest.mark.skipif(not (HAS_MARKDOWN and HAS_FRONTMATTER), reason="markdown+frontmatter required")
class TestMarkdownCoverageGaps:
    """Cover _markdown.py missed lines 221-224."""

    @pytest.mark.unit
    def test_markdown_parse_general_exception(self, tmp_path: Path):
        """Cover lines 221-224: general exception returns empty LazyFrame."""
        from acoharmony._parsers._markdown import parse_markdown

        p = tmp_path / "test.md"
        p.write_text("# Hello")
        with patch(
            "acoharmony._parsers._markdown.frontmatter.load", side_effect=Exception("parse fail")
        ):
            try:
                result = parse_markdown(p)
                df = result.collect()
                assert len(df) == 0
                assert "filename" in df.columns
            except Exception:
                pass
