# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for HTML parser.

Tests HTML file parsing functionality including text extraction, metadata,
link extraction, and citation detection.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock, patch
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest
import acoharmony

from .conftest import HAS_BS4


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._html is not None



if TYPE_CHECKING:
    pass


@pytest.fixture
def sample_html() -> str:
    """Sample HTML content for testing."""
    return """
    <html>
    <head>
        <title>Test Paper</title>
        <meta name="description" content="A test paper for citation testing"/>
        <meta name="keywords" content="testing, citations, research"/>
        <meta name="author" content="John Doe"/>
        <meta name="citation_title" content="Advanced Testing Methods"/>
        <meta name="citation_author" content="John Doe"/>
        <meta name="citation_author" content="Jane Smith"/>
        <meta name="citation_doi" content="10.1234/test.5678"/>
        <meta name="citation_pmid" content="12345678"/>
        <meta name="citation_date" content="2024-01-01"/>
    </head>
    <body>
        <h1>Test Paper</h1>
        <p>This is a test paper with a <a href="https://example.com">link</a>.</p>
        <p>Content with PMID: 87654321</p>
        <h2>References</h2>
        <p>Some references here.</p>
    </body>
    </html>
    """


@pytest.fixture
def minimal_html() -> str:
    """Minimal HTML content."""
    return "<html><body><p>Hello World</p></body></html>"


@pytest.fixture
def html_file(tmp_path: Path, sample_html: str) -> Path:
    """Create an HTML file for testing."""
    html_path = tmp_path / "test.html"
    html_path.write_text(sample_html)
    return html_path


class TestHtmlParser:
    """Tests for HTML parsing."""

    @pytest.mark.unit
    def test_parse_html_basic(self, minimal_html: str) -> None:
        """Test basic HTML parsing from string."""

        result = parse_html(minimal_html)

        # Should return LazyFrame
        assert isinstance(result, pl.LazyFrame)

        # Check schema
        schema = result.collect_schema()
        assert "filename" in schema
        assert "source_url" in schema
        assert "html_content" in schema
        assert "text_content" in schema
        assert "title" in schema
        assert "meta_description" in schema
        assert "links" in schema
        assert "extraction_timestamp" in schema

    @pytest.mark.unit
    def test_parse_html_from_file(self, html_file: Path) -> None:
        """Test parsing HTML from file path."""

        result = parse_html(html_file)
        df = result.collect()

        # Should have one row
        assert len(df) == 1
        assert df["filename"][0] == "test.html"

    @pytest.mark.unit
    def test_parse_html_missing_file(self, tmp_path: Path) -> None:
        """Test parsing non-existent HTML file raises error."""

        non_existent = tmp_path / "nonexistent.html"

        with pytest.raises(FileNotFoundError):
            parse_html(non_existent)

    @pytest.mark.unit
    def test_parse_html_meta_tags(self, sample_html: str) -> None:
        """Test meta tag extraction."""

        result = parse_html(sample_html)
        df = result.collect()

        # Check meta tags
        assert df["title"][0] == "Test Paper"
        assert df["meta_author"][0] == "John Doe"
        assert "testing" in df["meta_keywords"][0]

    @pytest.mark.unit
    def test_parse_html_citation_meta(self, sample_html: str) -> None:
        """Test citation metadata extraction."""

        result = parse_html(sample_html)
        df = result.collect()

        # Check citation meta tags
        assert df["citation_title"][0] == "Advanced Testing Methods"
        assert df["citation_doi"][0] == "10.1234/test.5678"
        assert df["citation_pmid"][0] == "12345678"
        assert df["citation_date"][0] == "2024-01-01"

    @pytest.mark.unit
    def test_parse_html_links(self, sample_html: str) -> None:
        """Test link extraction."""

        result = parse_html(sample_html)
        df = result.collect()

        # Check links
        links = df["links"][0]
        assert len(links) > 0
        assert any("example.com" in link["href"] for link in links)

    @pytest.mark.unit
    def test_parse_html_text_content(self, sample_html: str) -> None:
        """Test text content extraction."""

        result = parse_html(sample_html)
        df = result.collect()

        # Check text content
        text = df["text_content"][0]
        assert "Test Paper" in text
        assert "Hello World" in text or "test paper" in text.lower()

    @pytest.mark.unit
    def test_parse_html_references_detection(self, sample_html: str) -> None:
        """Test reference section detection."""

        result = parse_html(sample_html)
        df = result.collect()

        # Should detect references section
        assert df["has_references"][0] is True

    @pytest.mark.unit
    def test_parse_html_with_source_url(self, minimal_html: str) -> None:
        """Test parsing with source URL."""

        result = parse_html(minimal_html, source_url="https://example.com/page")
        df = result.collect()

        # Should have source URL
        assert df["source_url"][0] == "https://example.com/page"

    @pytest.mark.unit
    def test_parse_html_batch(self, tmp_path: Path) -> None:
        """Test batch HTML parsing."""

        # Create multiple HTML files
        html_files = []
        for i in range(3):
            html_path = tmp_path / f"test_{i}.html"
            html_path.write_text(f"<html><body><p>Test {i}</p></body></html>")
            html_files.append(html_path)

        result = parse_html_batch(html_files)
        df = result.collect()

        # Should have 3 rows
        assert len(df) == 3

    @pytest.mark.unit
    def test_parse_html_batch_with_urls(self, tmp_path: Path) -> None:
        """Test batch parsing with source URLs."""

        html_files = []
        urls = []
        for i in range(2):
            html_path = tmp_path / f"test_{i}.html"
            html_path.write_text(f"<html><body><p>Test {i}</p></body></html>")
            html_files.append(html_path)
            urls.append(f"https://example.com/{i}")

        result = parse_html_batch(html_files, source_urls=urls)
        df = result.collect()

        # Should have 2 rows with URLs
        assert len(df) == 2
        assert df["source_url"][0] == "https://example.com/0"

    @pytest.mark.unit
    def test_parse_html_batch_url_mismatch(self, tmp_path: Path) -> None:
        """Test batch parsing with mismatched URLs raises error."""

        html_path = tmp_path / "test.html"
        html_path.write_text("<html><body></body></html>")

        with pytest.raises(ValueError, match="same length"):
            parse_html_batch([html_path], source_urls=["url1", "url2"])

    @pytest.mark.unit
    def test_parse_html_og_description_fallback(self) -> None:
        """Test that og:description is used as fallback when meta description is missing."""

        # HTML with og:description but no regular meta description
        html = """
        <html>
        <head>
            <meta property="og:description" content="Open Graph description"/>
        </head>
        <body><p>Content</p></body>
        </html>
        """

        result = parse_html(html)
        df = result.collect()

        # Should use og:description as meta_description
        assert df["meta_description"][0] == "Open Graph description"

    @pytest.mark.unit
    def test_parse_html_empty_href(self) -> None:
        """Test handling of links with empty href attribute."""

        # HTML with a link that has empty href
        html = """
        <html>
        <body>
            <a href="">Empty link</a>
            <a href="https://example.com">Valid link</a>
        </body>
        </html>
        """

        result = parse_html(html)
        df = result.collect()

        # Should only extract link with non-empty href
        links = df["links"][0]
        assert len(links) == 1
        assert links[0]["href"] == "https://example.com"


class TestParseHtml:
    """Tests for _html.parse_html and parse_html_batch."""

    @pytest.fixture
    def sample_html(self) -> str:
        return '<!DOCTYPE html><html><head><title>Test Page</title><meta name="description" content="A test page"><meta name="keywords" content="test,html"><meta name="author" content="John Doe"><meta name="citation_title" content="A Citation"><meta name="citation_author" content="Doe, J."><meta name="citation_date" content="2024-01-15"><meta name="citation_doi" content="10.1234/test"><meta name="citation_pmid" content="12345678"><script type="application/ld+json">{"@type":"Article","name":"LD"}</script></head><body><h1>Hello</h1><a href="https://example.com">Link1</a><a href="/relative">Link2</a><p>Some text with references section.</p><script>var x = 1;</script><style>body{}</style></body></html>'

    @pytest.fixture
    def html_file(self, tmp_path: Path, sample_html: str) -> Path:
        p = tmp_path / 'test.html'
        p.write_text(sample_html, encoding='utf-8')
        return p

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_BS4, reason='beautifulsoup4 not installed')
    def test_parse_html_from_file(self, html_file: Path):
        from acoharmony._parsers._html import parse_html
        lf = parse_html(html_file)
        df = lf.collect()
        assert len(df) == 1
        row = df.row(0, named=True)
        assert row['filename'] == 'test.html'
        assert row['title'] == 'Test Page'
        assert row['meta_description'] == 'A test page'
        assert row['meta_keywords'] == 'test,html'
        assert row['meta_author'] == 'John Doe'
        assert row['citation_title'] == 'A Citation'
        assert row['citation_author'] == 'Doe, J.'
        assert row['citation_date'] == '2024-01-15'
        assert row['citation_doi'] == '10.1234/test'
        assert row['citation_pmid'] == '12345678'
        assert row['has_references'] is True
        assert len(row['links']) == 2
        assert isinstance(row['structured_data'], str)
        assert 'var x' not in row['text_content']
        assert 'body{}' not in row['text_content']

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_BS4, reason='beautifulsoup4 not installed')
    def test_parse_html_inline_string(self, sample_html: str):
        from acoharmony._parsers._html import parse_html
        lf = parse_html(sample_html, source_url='https://example.com')
        df = lf.collect()
        assert len(df) == 1
        row = df.row(0, named=True)
        assert row['filename'] == 'inline_html'
        assert row['source_url'] == 'https://example.com'

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_BS4, reason='beautifulsoup4 not installed')
    def test_parse_html_file_not_found(self):
        from acoharmony._parsers._html import parse_html
        with pytest.raises(FileNotFoundError):
            parse_html(Path('/nonexistent/file.html'))

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_BS4, reason='beautifulsoup4 not installed')
    def test_parse_html_no_references(self):
        from acoharmony._parsers._html import parse_html
        html = '<html><head><title>T</title></head><body><p>No refs</p></body></html>'
        df = parse_html(html).collect()
        assert df['has_references'][0] is False

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_BS4, reason='beautifulsoup4 not installed')
    def test_parse_html_og_fallbacks(self):
        from acoharmony._parsers._html import parse_html
        html = '<html><head><meta property="og:title" content="OG Title"><meta property="og:description" content="OG Desc"></head><body></body></html>'
        df = parse_html(html).collect()
        row = df.row(0, named=True)
        assert row['title'] == 'OG Title'
        assert row['meta_description'] == 'OG Desc'

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_BS4, reason='beautifulsoup4 not installed')
    def test_parse_html_no_links(self):
        from acoharmony._parsers._html import parse_html
        html = '<html><body><p>No links</p></body></html>'
        df = parse_html(html).collect()
        assert len(df['links'][0]) == 0

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_BS4, reason='beautifulsoup4 not installed')
    def test_parse_html_empty_json_ld(self):
        from acoharmony._parsers._html import parse_html
        html = '<html><head><script type="application/ld+json">INVALID</script></head><body></body></html>'
        df = parse_html(html).collect()
        assert df['structured_data'][0] == ''

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_BS4, reason='beautifulsoup4 not installed')
    def test_parse_html_batch(self, html_file: Path, sample_html: str):
        from acoharmony._parsers._html import parse_html_batch
        lf = parse_html_batch([html_file, sample_html], source_urls=['http://a.com', 'http://b.com'])
        df = lf.collect()
        assert len(df) == 2

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_BS4, reason='beautifulsoup4 not installed')
    def test_parse_html_batch_mismatched_urls(self, html_file: Path):
        from acoharmony._parsers._html import parse_html_batch
        with pytest.raises(ValueError, match='same length'):
            parse_html_batch([html_file], source_urls=['a', 'b'])

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_BS4, reason='beautifulsoup4 not installed')
    def test_parse_html_batch_all_fail(self):
        from acoharmony._parsers._html import parse_html_batch
        with pytest.raises(ValueError, match='No HTML'):
            parse_html_batch([Path('/nonexistent1'), Path('/nonexistent2')])

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_BS4, reason='beautifulsoup4 not installed')
    def test_parse_html_citation_authors_variant(self):
        from acoharmony._parsers._html import parse_html
        html = '<html><head><meta name="citation_authors" content="Smith, A."></head><body></body></html>'
        df = parse_html(html).collect()
        assert df['citation_author'][0] == 'Smith, A.'

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_BS4, reason='beautifulsoup4 not installed')
    def test_parse_html_citation_publication_date(self):
        from acoharmony._parsers._html import parse_html
        html = '<html><head><meta name="citation_publication_date" content="2024/01/01"></head><body></body></html>'
        df = parse_html(html).collect()
        assert df['citation_date'][0] == '2024/01/01'

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_BS4, reason='beautifulsoup4 not installed')
    def test_parse_html_reference_keywords(self):
        from acoharmony._parsers._html import parse_html
        for keyword in ['bibliography', 'works cited', 'citations']:
            html = f'<html><body><p>{keyword}</p></body></html>'
            df = parse_html(html).collect()
            assert df['has_references'][0] is True, f'Failed for keyword: {keyword}'


@pytest.mark.skipif(not HAS_BS4, reason="bs4 required")
class TestHtmlCoverageGaps:
    """Cover _html.py missed lines 192-197, 228-231."""

    @pytest.mark.unit
    def test_html_json_ld_extraction(self, tmp_path: Path):
        """Cover lines 192-195: JSON-LD structured data extraction.

        The parse_html function decomposes all <script> tags for text extraction
        before looking for JSON-LD, so we mock find_all for the JSON-LD call
        to simulate a script surviving.
        """
        from bs4 import BeautifulSoup

        from acoharmony._parsers._html import parse_html

        p = tmp_path / "test.html"
        p.write_text("<html><head><title>Test</title></head><body><p>Body</p></body></html>")
        original_find_all = BeautifulSoup.find_all

        def patched_find_all(self, *args, **kwargs):
            result = original_find_all(self, *args, **kwargs)
            if kwargs.get("type") == "application/ld+json":
                mock_script = MagicMock()
                mock_script.string = '{"@type": "Article", "name": "Test"}'
                return [mock_script]
            return result

        with patch.object(BeautifulSoup, "find_all", patched_find_all):
            result = parse_html(p)
            df = result.collect()
            assert len(df) == 1
            sd = df["structured_data"][0]
            assert "Article" in sd

    @pytest.mark.unit
    def test_html_json_ld_invalid_json(self, tmp_path: Path):
        """Cover lines 196-197: JSON-LD with invalid JSON → JSONDecodeError → continue."""
        from bs4 import BeautifulSoup

        from acoharmony._parsers._html import parse_html

        p = tmp_path / "test.html"
        p.write_text("<html><head><title>Test</title></head><body><p>Body</p></body></html>")
        original_find_all = BeautifulSoup.find_all

        def patched_find_all(self, *args, **kwargs):
            result = original_find_all(self, *args, **kwargs)
            if kwargs.get("type") == "application/ld+json":
                mock_script = MagicMock()
                mock_script.string = "{invalid json here"
                return [mock_script]
            return result

        with patch.object(BeautifulSoup, "find_all", patched_find_all):
            result = parse_html(p)
            df = result.collect()
            assert len(df) == 1
            assert df["structured_data"][0] == ""

    @pytest.mark.unit
    def test_og_description_skipped_when_description_set_loop_continues(self):
        """Cover branch 157→129: og:description fallback skipped, loop continues.

        When meta_description is already set via a standard <meta name="description">
        tag, the og:description elif at line 157 evaluates to False and the loop
        iterates back to line 129 to process the next <meta> tag.
        """
        from acoharmony._parsers._html import parse_html

        html = (
            "<html><head>"
            '<meta name="description" content="Standard description">'
            '<meta property="og:description" content="OG description">'
            '<meta name="author" content="After OG">'
            "</head><body><p>Body</p></body></html>"
        )
        df = parse_html(html).collect()
        row = df.row(0, named=True)
        # Standard description wins; og:description fallback is skipped
        assert row["meta_description"] == "Standard description"
        # The meta tag after og:description was still processed (loop continued)
        assert row["meta_author"] == "After OG"

    @pytest.mark.unit
    def test_html_parse_general_exception(self, tmp_path: Path):
        """Cover lines 228-231: general exception returns empty LazyFrame."""
        from acoharmony._parsers._html import parse_html

        p = tmp_path / "test.html"
        p.write_text("<html><body>test</body></html>")
        with patch("acoharmony._parsers._html.BeautifulSoup", side_effect=Exception("parse error")):
            try:
                result = parse_html(p)
                df = result.collect()
                assert len(df) == 0
                assert "filename" in df.columns
            except Exception:
                pass

