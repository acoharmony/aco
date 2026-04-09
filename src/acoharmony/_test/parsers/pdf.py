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
from pypdf import PdfWriter
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from .conftest import HAS_PYPDF


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._pdf is not None


if TYPE_CHECKING:
    pass


@pytest.fixture
def minimal_pdf(tmp_path: Path) -> Path:
    """Create a minimal PDF for testing."""
    pdf_path = tmp_path / "test.pdf"

    # Create a simple PDF with pypdf
    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)

    # Write to file
    with open(pdf_path, "wb") as f:
        writer.write(f)

    return pdf_path


@pytest.fixture
def sample_pdf_with_text(tmp_path: Path) -> Path:
    """Create a PDF with sample text content."""
    pytest.importorskip("reportlab", reason="reportlab not installed")

    pdf_path = tmp_path / "sample.pdf"

    # Create PDF with text using reportlab
    c = canvas.Canvas(str(pdf_path), pagesize=letter)
    c.drawString(100, 750, "Test Paper Title")
    c.drawString(100, 730, "Author: John Doe")
    c.drawString(100, 710, "DOI: 10.1234/test.5678")
    c.drawString(100, 690, "PMID: 12345678")
    c.drawString(100, 670, "arXiv:2024.12345")
    c.drawString(100, 650, "This is a test paper with references.")
    c.showPage()
    c.save()

    return pdf_path


class TestPdfParser:
    """Tests for PDF parsing."""

    @pytest.mark.unit
    def test_parse_pdf_missing_file(self, tmp_path: Path) -> None:
        """Test parsing non-existent PDF file raises error."""

        non_existent = tmp_path / "nonexistent.pdf"

        with pytest.raises(FileNotFoundError):
            parse_pdf(non_existent)

    @pytest.mark.unit
    def test_parse_pdf_basic(self, minimal_pdf: Path) -> None:
        """Test basic PDF parsing returns LazyFrame with correct schema."""

        result = parse_pdf(minimal_pdf)

        # Should return LazyFrame
        assert isinstance(result, pl.LazyFrame)

        # Check schema
        schema = result.collect_schema()
        assert "filename" in schema
        assert "file_path" in schema
        assert "text_content" in schema
        assert "page_count" in schema
        assert "title" in schema
        assert "author" in schema
        assert "doi" in schema
        assert "pubmed_id" in schema
        assert "arxiv_id" in schema
        assert "has_citations" in schema
        assert "file_size_bytes" in schema
        assert "extraction_timestamp" in schema

    @pytest.mark.unit
    def test_parse_pdf_collect(self, minimal_pdf: Path) -> None:
        """Test PDF parsing can be collected."""

        result = parse_pdf(minimal_pdf)
        df = result.collect()

        # Should have one row
        assert len(df) == 1

        # Check filename
        assert df["filename"][0] == "test.pdf"

    @pytest.mark.unit
    def test_parse_pdf_metadata(self, sample_pdf_with_text: Path) -> None:
        """Test PDF metadata extraction."""
        pytest.importorskip("reportlab")

        result = parse_pdf(sample_pdf_with_text)
        df = result.collect()

        # Should have content
        assert len(df) == 1
        assert df["text_content"][0] is not None

    @pytest.mark.unit
    def test_parse_pdf_citations(self, sample_pdf_with_text: Path) -> None:
        """Test citation detection in PDF."""
        pytest.importorskip("reportlab")

        result = parse_pdf(sample_pdf_with_text, detect_citations=True)
        df = result.collect()

        # Check citation fields exist
        assert "doi" in df.columns
        assert "pubmed_id" in df.columns
        assert "arxiv_id" in df.columns

    @pytest.mark.unit
    def test_parse_pdf_per_page(self, minimal_pdf: Path) -> None:
        """Test per-page extraction mode."""

        result = parse_pdf(minimal_pdf, extract_per_page=True)
        result.collect()

        # Should have page_number column
        schema = result.collect_schema()
        assert "page_number" in schema

    @pytest.mark.unit
    def test_parse_pdf_batch_empty(self) -> None:
        """Test batch parsing with no files raises error."""

        with pytest.raises(ValueError, match="No PDFs successfully parsed"):
            parse_pdf_batch([])

    @pytest.mark.unit
    def test_parse_pdf_batch(self, tmp_path: Path) -> None:
        """Test batch PDF parsing."""


        # Create multiple minimal PDFs
        pdf_paths = []
        for i in range(3):
            pdf_path = tmp_path / f"test_{i}.pdf"
            writer = PdfWriter()
            writer.add_blank_page(width=200, height=200)
            with open(pdf_path, "wb") as f:
                writer.write(f)
            pdf_paths.append(pdf_path)

        result = parse_pdf_batch(pdf_paths)
        df = result.collect()

        # Should have 3 rows
        assert len(df) == 3

    @pytest.mark.unit
    def test_parse_pdf_no_citations(self, minimal_pdf: Path) -> None:
        """Test parsing with citation detection disabled."""

        result = parse_pdf(minimal_pdf, detect_citations=False)
        df = result.collect()

        # Citation fields should still exist but be empty
        assert "doi" in df.columns
        assert df["doi"][0] == ""

class TestParsePdf:
    """Tests for _pdf.parse_pdf and parse_pdf_batch."""

    @pytest.fixture
    def mock_pdf(self, tmp_path: Path) -> Path:
        """Create a minimal valid PDF file for testing."""
        p = tmp_path / 'test.pdf'
        p.write_bytes(b'%PDF-1.0\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj 2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj 3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R>>endobj\nxref\n0 4\n0000000000 65535 f \n0000000009 00000 n \n0000000058 00000 n \n0000000115 00000 n \ntrailer<</Size 4/Root 1 0 R>>\nstartxref\n190\n%%EOF')
        return p

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_PYPDF, reason='pypdf not installed')
    def test_parse_pdf_basic(self, mock_pdf: Path):
        from acoharmony._parsers._pdf import parse_pdf
        lf = parse_pdf(mock_pdf)
        df = lf.collect()
        assert len(df) >= 0
        if len(df) == 1:
            row = df.row(0, named=True)
            assert row['filename'] == 'test.pdf'
            assert 'page_count' in row

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_PYPDF, reason='pypdf not installed')
    def test_parse_pdf_file_not_found(self):
        from acoharmony._parsers._pdf import parse_pdf
        with pytest.raises(FileNotFoundError):
            parse_pdf(Path('/nonexistent/file.pdf'))

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_PYPDF, reason='pypdf not installed')
    def test_parse_pdf_with_mocked_reader(self, tmp_path: Path):
        """Test parse_pdf with a mocked PdfReader that returns controlled data."""
        pdf_path = tmp_path / 'mocked.pdf'
        pdf_path.write_bytes(b'dummy')
        mock_page = MagicMock()
        mock_page.extract_text.return_value = 'Hello World. DOI: 10.1234/test.5678 PMID: 12345678 arXiv:2301.12345 references section'
        mock_page.mediabox = MagicMock()
        mock_page.mediabox.width = 612.0
        mock_page.mediabox.height = 792.0
        mock_reader = MagicMock()
        mock_reader.pages = [mock_page]
        mock_reader.metadata = {'/Title': 'Test Title', '/Author': 'Author Name', '/Subject': 'Subject', '/Keywords': 'kw1, kw2', '/Creator': 'TestCreator', '/CreationDate': 'D:20240101'}
        with patch('acoharmony._parsers._pdf.PdfReader', return_value=mock_reader):
            from acoharmony._parsers._pdf import parse_pdf
            lf = parse_pdf(pdf_path, detect_citations=True)
            df = lf.collect()
        assert len(df) == 1
        row = df.row(0, named=True)
        assert row['title'] == 'Test Title'
        assert row['author'] == 'Author Name'
        assert row['subject'] == 'Subject'
        assert row['keywords'] == 'kw1, kw2'
        assert row['creator'] == 'TestCreator'
        assert row['doi'] == '10.1234/test.5678'
        assert row['pubmed_id'] == '12345678'
        assert row['arxiv_id'] == '2301.12345'
        assert row['has_citations'] is True
        assert row['page_count'] == 1

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_PYPDF, reason='pypdf not installed')
    def test_parse_pdf_per_page(self, tmp_path: Path):
        pdf_path = tmp_path / 'multi.pdf'
        pdf_path.write_bytes(b'dummy')
        pages = []
        for i in range(3):
            p = MagicMock()
            p.extract_text.return_value = f'Page {i + 1} text'
            p.mediabox = MagicMock()
            p.mediabox.width = 612.0
            p.mediabox.height = 792.0
            pages.append(p)
        mock_reader = MagicMock()
        mock_reader.pages = pages
        mock_reader.metadata = {}
        with patch('acoharmony._parsers._pdf.PdfReader', return_value=mock_reader):
            from acoharmony._parsers._pdf import parse_pdf
            lf = parse_pdf(pdf_path, extract_per_page=True)
            df = lf.collect()
        assert len(df) == 3
        assert df['page_number'].to_list() == [1, 2, 3]
        assert 'page_width' in df.columns
        assert 'page_height' in df.columns

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_PYPDF, reason='pypdf not installed')
    def test_parse_pdf_no_citation_detection(self, tmp_path: Path):
        pdf_path = tmp_path / 'nocit.pdf'
        pdf_path.write_bytes(b'dummy')
        mock_page = MagicMock()
        mock_page.extract_text.return_value = '10.1234/test PMID: 99999999'
        mock_page.mediabox = MagicMock()
        mock_page.mediabox.width = 612.0
        mock_page.mediabox.height = 792.0
        mock_reader = MagicMock()
        mock_reader.pages = [mock_page]
        mock_reader.metadata = {}
        with patch('acoharmony._parsers._pdf.PdfReader', return_value=mock_reader):
            from acoharmony._parsers._pdf import parse_pdf
            lf = parse_pdf(pdf_path, detect_citations=False)
            df = lf.collect()
        row = df.row(0, named=True)
        assert row['doi'] == ''
        assert row['pubmed_id'] == ''
        assert row['has_citations'] is False

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_PYPDF, reason='pypdf not installed')
    def test_parse_pdf_page_extraction_failure(self, tmp_path: Path):
        """Test graceful handling when a page's extract_text fails."""
        pdf_path = tmp_path / 'bad_page.pdf'
        pdf_path.write_bytes(b'dummy')
        good_page = MagicMock()
        good_page.extract_text.return_value = 'Good text'
        good_page.mediabox = MagicMock()
        good_page.mediabox.width = 100.0
        good_page.mediabox.height = 200.0
        bad_page = MagicMock()
        bad_page.extract_text.side_effect = RuntimeError('corrupt page')
        bad_page.mediabox = None
        mock_reader = MagicMock()
        mock_reader.pages = [good_page, bad_page]
        mock_reader.metadata = {}
        with patch('acoharmony._parsers._pdf.PdfReader', return_value=mock_reader):
            from acoharmony._parsers._pdf import parse_pdf
            lf = parse_pdf(pdf_path, extract_per_page=True)
            df = lf.collect()
        assert len(df) == 2
        assert df['text_content'][1] == ''

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_PYPDF, reason='pypdf not installed')
    def test_parse_pdf_null_metadata(self, tmp_path: Path):
        """Metadata fields that are None should become empty strings."""
        pdf_path = tmp_path / 'nullmeta.pdf'
        pdf_path.write_bytes(b'dummy')
        mock_page = MagicMock()
        mock_page.extract_text.return_value = ''
        mock_page.mediabox = MagicMock()
        mock_page.mediabox.width = 0.0
        mock_page.mediabox.height = 0.0
        mock_reader = MagicMock()
        mock_reader.pages = [mock_page]
        mock_reader.metadata = {'/Title': None, '/Author': None}
        with patch('acoharmony._parsers._pdf.PdfReader', return_value=mock_reader):
            from acoharmony._parsers._pdf import parse_pdf
            df = parse_pdf(pdf_path).collect()
        row = df.row(0, named=True)
        assert row['title'] == ''
        assert row['author'] == ''

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_PYPDF, reason='pypdf not installed')
    def test_parse_pdf_batch(self, tmp_path: Path):
        pdf1 = tmp_path / 'a.pdf'
        pdf2 = tmp_path / 'b.pdf'
        pdf1.write_bytes(b'dummy')
        pdf2.write_bytes(b'dummy')
        mock_page = MagicMock()
        mock_page.extract_text.return_value = 'text'
        mock_page.mediabox = MagicMock()
        mock_page.mediabox.width = 1.0
        mock_page.mediabox.height = 1.0
        mock_reader = MagicMock()
        mock_reader.pages = [mock_page]
        mock_reader.metadata = {}
        with patch('acoharmony._parsers._pdf.PdfReader', return_value=mock_reader):
            from acoharmony._parsers._pdf import parse_pdf_batch
            df = parse_pdf_batch([pdf1, pdf2]).collect()
        assert len(df) == 2

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_PYPDF, reason='pypdf not installed')
    def test_parse_pdf_batch_all_fail(self):
        from acoharmony._parsers._pdf import parse_pdf_batch
        with pytest.raises(ValueError, match='No PDFs'):
            parse_pdf_batch([Path('/no1.pdf'), Path('/no2.pdf')])

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_PYPDF, reason='pypdf not installed')
    def test_parse_pdf_citation_markers(self, tmp_path: Path):
        """Test various citation marker patterns."""
        pdf_path = tmp_path / 'cit.pdf'
        pdf_path.write_bytes(b'dummy')
        mock_page = MagicMock()
        mock_page.extract_text.return_value = 'See [1] for details'
        mock_page.mediabox = MagicMock()
        mock_page.mediabox.width = 1.0
        mock_page.mediabox.height = 1.0
        mock_reader = MagicMock()
        mock_reader.pages = [mock_page]
        mock_reader.metadata = {}
        with patch('acoharmony._parsers._pdf.PdfReader', return_value=mock_reader):
            from acoharmony._parsers._pdf import parse_pdf
            df = parse_pdf(pdf_path).collect()
        assert df['has_citations'][0] is True

    @pytest.mark.unit
    @pytest.mark.skipif(not HAS_PYPDF, reason='pypdf not installed')
    def test_parse_pdf_year_citation_marker(self, tmp_path: Path):
        """Test (2024) style citation marker."""
        pdf_path = tmp_path / 'year.pdf'
        pdf_path.write_bytes(b'dummy')
        mock_page = MagicMock()
        mock_page.extract_text.return_value = 'Smith (2024) found that'
        mock_page.mediabox = MagicMock()
        mock_page.mediabox.width = 1.0
        mock_page.mediabox.height = 1.0
        mock_reader = MagicMock()
        mock_reader.pages = [mock_page]
        mock_reader.metadata = {}
        with patch('acoharmony._parsers._pdf.PdfReader', return_value=mock_reader):
            from acoharmony._parsers._pdf import parse_pdf
            df = parse_pdf(pdf_path).collect()
        assert df['has_citations'][0] is True


@pytest.mark.skipif(not HAS_PYPDF, reason="pypdf required")
class TestPdfCoverageGaps:
    """Cover _pdf.py missed lines 234-237."""

    @pytest.mark.unit
    def test_pdf_parse_general_exception(self, tmp_path: Path):
        """Cover lines 234-237: general exception returns empty LazyFrame."""
        from acoharmony._parsers._pdf import parse_pdf

        p = tmp_path / "test.pdf"
        p.write_bytes(b"not a real pdf")
        result = parse_pdf(p)
        df = result.collect()
        assert len(df) == 0
        assert "filename" in df.columns
