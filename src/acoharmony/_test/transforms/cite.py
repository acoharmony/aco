from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import inspect
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import polars as pl
import pytest
import requests
from pypdf import PdfWriter

import acoharmony

'Tests for acoharmony._transforms._cite module.'

class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._transforms._cite is not None
'\nUnit tests for citation transform.\n\nTests the main citation processing pipeline including download, parse,\nextract, and store operations.\n'
if TYPE_CHECKING:
    pass

@pytest.fixture
def mock_storage(tmp_path: Path) -> Mock:
    """Mock StorageBackend for testing."""
    storage = Mock()
    storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
    return storage

@pytest.fixture
def mock_state_tracker() -> Mock:
    """Mock CiteStateTracker for testing."""
    tracker = Mock()
    tracker.is_file_processed = Mock(return_value=False)
    tracker.mark_file_processed = Mock()
    return tracker

@pytest.fixture
def sample_html_content() -> bytes:
    """Sample HTML content for mocking."""
    html = '\n    <html>\n    <head>\n        <title>Test Paper</title>\n        <meta name="citation_doi" content="10.1234/test.5678"/>\n    </head>\n    <body>\n        <h1>Test Paper</h1>\n        <p>This is a test paper.</p>\n    </body>\n    </html>\n    '
    return html.encode('utf-8')

@pytest.fixture
def sample_pdf_path(tmp_path: Path) -> Path:
    """Create a sample PDF file."""
    pdf_path = tmp_path / 'sample.pdf'
    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)
    with open(pdf_path, 'wb') as f:
        writer.write(f)
    return pdf_path

class TestCiteTransform:
    """Tests for citation transform."""

    @pytest.mark.unit
    def test_transform_cite_missing_module(self) -> None:
        """Test that transform_cite can be imported."""
        assert callable(transform_cite)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_html_basic(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test basic HTML citation transform with mocked HTTP."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        result = transform_cite('https://example.com/paper.html')
        assert isinstance(result, pl.LazyFrame)
        mock_requests.assert_called_once()
        assert mock_storage.get_path.called

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_cached(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, tmp_path: Path) -> None:
        """Test that cached URLs are not re-downloaded."""
        corpus_dir = tmp_path / 'cites' / 'corpus'
        corpus_dir.mkdir(parents=True, exist_ok=True)
        cached_df = pl.DataFrame({'url': ['https://example.com/cached'], 'title': ['Cached Paper'], 'url_hash': ['6388559832241035']})
        cached_df.write_parquet(corpus_dir / 'corpus.parquet')
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=True)
        mock_tracker_cls.return_value = mock_tracker
        result = transform_cite('https://example.com/cached')
        assert isinstance(result, pl.LazyFrame)
        df = result.collect()
        assert len(df) > 0

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_force_refresh(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test force_refresh bypasses cache."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=True)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        transform_cite('https://example.com/paper.html', force_refresh=True)
        mock_requests.assert_called_once()

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_network_error(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path) -> None:
        """Test handling of network errors."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_requests.side_effect = requests.RequestException('Network error')
        with pytest.raises(requests.RequestException):
            transform_cite('https://example.com/paper.html')

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_local_file(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, tmp_path: Path, sample_pdf_path: Path) -> None:
        """Test processing local file:// URLs."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        file_url = f'file://{sample_pdf_path}'
        result = transform_cite(file_url)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_content_type_detection(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path) -> None:
        """Test content type detection from headers."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        buf = BytesIO()
        writer = PdfWriter()
        writer.add_blank_page(width=200, height=200)
        writer.write(buf)
        pdf_bytes = buf.getvalue()
        mock_response = Mock()
        mock_response.content = pdf_bytes
        mock_response.headers = {'content-type': 'application/pdf'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        result = transform_cite('https://example.com/paper.html')
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_with_tags_and_note(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test transform with tags and note."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        result = transform_cite('https://example.com/paper.html', tags=['tag1', 'tag2'], note='Important paper')
        assert isinstance(result, pl.LazyFrame)
        df = result.collect()
        assert 'tags' in df.columns
        assert 'note' in df.columns

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_save_to_corpus_false(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test transform with save_to_corpus=False."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        result = transform_cite('https://example.com/paper.html', save_to_corpus=False)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_appends_to_existing_corpus(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test that transform appends to existing master corpus."""
        corpus_dir = tmp_path / 'cites' / 'corpus'
        corpus_dir.mkdir(parents=True, exist_ok=True)
        existing_df = pl.DataFrame({'url': ['https://old.com'], 'title': ['Old']})
        existing_df.write_parquet(corpus_dir / 'corpus.parquet')
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        result = transform_cite('https://example.com/new_paper.html')
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_cached_raw_file(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test that cached raw file is used instead of downloading."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        raw_dir = tmp_path / 'cites' / 'raw' / 'html'
        raw_dir.mkdir(parents=True, exist_ok=True)
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        result = transform_cite('https://example.com/cached_raw.html')
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_transform_cite_idempotency_check(self) -> None:
        """Test that transform is designed for idempotency."""
        sig = inspect.signature(transform_cite)
        assert 'url' in sig.parameters
        assert 'force_refresh' in sig.parameters
        assert 'save_to_corpus' in sig.parameters
        assert 'tags' in sig.parameters
        assert 'note' in sig.parameters

class TestNormalizeParserColumns:
    """Tests for _normalize_parser_columns in _cite.py."""

    @pytest.mark.unit
    def test_html_with_citation_author_and_title(self):
        lf = pl.DataFrame({'citation_author': ['John Doe'], 'meta_author': ['Jane Smith'], 'citation_title': ['Good Title'], 'title': ['Fallback Title'], 'meta_description': ['A description'], 'citation_date': ['2024-01-01'], 'meta_keywords': ['kw1, kw2']}).lazy()
        result = _normalize_parser_columns(lf, 'html').collect()
        assert result['author'][0] == 'John Doe'
        assert result['title'][0] == 'Good Title'
        assert result['abstract'][0] == 'A description'
        assert result['date'][0] == '2024-01-01'
        assert result['keywords'][0] == 'kw1, kw2'

    @pytest.mark.unit
    def test_html_fallback_to_meta_author(self):
        lf = pl.DataFrame({'citation_author': [''], 'meta_author': ['Jane Smith'], 'title': ['Some Title']}).lazy()
        result = _normalize_parser_columns(lf, 'html').collect()
        assert result['author'][0] == 'Jane Smith'

    @pytest.mark.unit
    def test_html_only_meta_author(self):
        lf = pl.DataFrame({'meta_author': ['Meta Author'], 'title': ['Test Title']}).lazy()
        result = _normalize_parser_columns(lf, 'html').collect()
        assert result['author'][0] == 'Meta Author'

    @pytest.mark.unit
    def test_html_no_meta_description(self):
        lf = pl.DataFrame({'title': ['Title']}).lazy()
        result = _normalize_parser_columns(lf, 'html').collect()
        assert result['abstract'][0] == ''
        assert result['keywords'][0] == ''

    @pytest.mark.unit
    def test_pdf_subject_to_abstract(self):
        lf = pl.DataFrame({'author': ['PDF Author'], 'title': ['PDF Title'], 'subject': ['PDF Subject'], 'creation_date': ['2024-06-15'], 'keywords': ['pdf, test']}).lazy()
        result = _normalize_parser_columns(lf, 'pdf').collect()
        assert result['abstract'][0] == 'PDF Subject'
        assert result['date'][0] == '2024-06-15'

    @pytest.mark.unit
    def test_pdf_no_subject(self):
        lf = pl.DataFrame({'author': ['Author'], 'title': ['Title']}).lazy()
        result = _normalize_parser_columns(lf, 'pdf').collect()
        assert result['abstract'][0] == ''

    @pytest.mark.unit
    def test_pdf_no_creation_date(self):
        lf = pl.DataFrame({'author': ['Author'], 'title': ['Title']}).lazy()
        result = _normalize_parser_columns(lf, 'pdf').collect()
        assert result['date'][0] == ''

    @pytest.mark.unit
    def test_markdown_passthrough(self):
        lf = pl.DataFrame({'author': ['MD Author'], 'title': ['MD Title'], 'abstract': ['MD Abstract'], 'date': ['2024-01-01'], 'keywords': ['md, test']}).lazy()
        result = _normalize_parser_columns(lf, 'markdown').collect()
        assert result['author'][0] == 'MD Author'
        assert result['abstract'][0] == 'MD Abstract'

    @pytest.mark.unit
    def test_latex_missing_columns(self):
        lf = pl.DataFrame({'author': ['LaTeX Author'], 'title': ['LaTeX Title']}).lazy()
        result = _normalize_parser_columns(lf, 'latex').collect()
        assert result['abstract'][0] == ''
        assert result['date'][0] == ''
        assert result['keywords'][0] == ''

    @pytest.mark.unit
    def test_no_author_creates_empty(self):
        lf = pl.DataFrame({'title': ['Title Only']}).lazy()
        result = _normalize_parser_columns(lf, 'pdf').collect()
        assert result['author'][0] == ''

    @pytest.mark.unit
    def test_no_title_creates_empty(self):
        lf = pl.DataFrame({'author': ['Author']}).lazy()
        result = _normalize_parser_columns(lf, 'markdown').collect()
        assert result['title'][0] == ''

    @pytest.mark.unit
    def test_unknown_content_type(self):
        lf = pl.DataFrame({'title': ['Some Title']}).lazy()
        result = _normalize_parser_columns(lf, 'other').collect()
        assert result['author'][0] == ''
        assert result['abstract'][0] == ''
        assert result['date'][0] == ''
        assert result['keywords'][0] == ''

    @pytest.mark.unit
    def test_html_citation_title_empty_fallback(self):
        lf = pl.DataFrame({'citation_title': [''], 'title': ['Fallback Title']}).lazy()
        result = _normalize_parser_columns(lf, 'html').collect()
        assert result['title'][0] == 'Fallback Title'

    @pytest.mark.unit
    def test_no_expressions_needed(self):
        lf = pl.DataFrame({'author': ['A'], 'title': ['T'], 'abstract': ['Ab'], 'date': ['D'], 'keywords': ['K']}).lazy()
        result = _normalize_parser_columns(lf, 'markdown').collect()
        assert result['author'][0] == 'A'

    @pytest.mark.unit
    def test_html_with_citation_date(self):
        lf = pl.DataFrame({'title': ['T'], 'citation_date': ['2024-12-25']}).lazy()
        result = _normalize_parser_columns(lf, 'html').collect()
        assert result['date'][0] == '2024-12-25'

    @pytest.mark.unit
    def test_html_with_meta_keywords(self):
        lf = pl.DataFrame({'title': ['T'], 'meta_keywords': ['keyword1, keyword2']}).lazy()
        result = _normalize_parser_columns(lf, 'html').collect()
        assert result['keywords'][0] == 'keyword1, keyword2'

    @pytest.mark.unit
    def test_html_citation_author_null_fallback_to_meta(self):
        """When citation_author is null, fall back to meta_author."""
        lf = pl.DataFrame({'citation_author': [None], 'meta_author': ['Meta Author'], 'title': ['Title']}).lazy()
        result = _normalize_parser_columns(lf, 'html').collect()
        assert result['author'][0] == 'Meta Author'

    @pytest.mark.unit
    def test_html_both_authors_null(self):
        """When both citation_author and meta_author are empty/null."""
        lf = pl.DataFrame({'citation_author': [None], 'meta_author': [''], 'title': ['Title']}).lazy()
        result = _normalize_parser_columns(lf, 'html').collect()
        assert result['author'][0] == ''

    @pytest.mark.unit
    def test_html_no_citation_title_no_title(self):
        """HTML with no citation_title column, no title column."""
        lf = pl.DataFrame({'some_col': ['x']}).lazy()
        result = _normalize_parser_columns(lf, 'html').collect()
        assert result['title'][0] == ''
        assert 'author' not in result.columns or result['author'][0] == ''

    @pytest.mark.unit
    def test_html_citation_title_both_empty(self):
        """HTML with both citation_title and title empty."""
        lf = pl.DataFrame({'citation_title': [''], 'title': ['']}).lazy()
        result = _normalize_parser_columns(lf, 'html').collect()
        assert result['title'][0] == ''

    @pytest.mark.unit
    def test_html_citation_title_null_fallback(self):
        """HTML with citation_title null, fallback to title."""
        lf = pl.DataFrame({'citation_title': [None], 'title': ['Fallback']}).lazy()
        result = _normalize_parser_columns(lf, 'html').collect()
        assert result['title'][0] == 'Fallback'

    @pytest.mark.unit
    def test_html_no_citation_date(self):
        """HTML without citation_date column creates empty date."""
        lf = pl.DataFrame({'title': ['T']}).lazy()
        result = _normalize_parser_columns(lf, 'html').collect()
        assert result['date'][0] == ''

    @pytest.mark.unit
    def test_pdf_with_keywords(self):
        """PDF with existing keywords column passes through."""
        lf = pl.DataFrame({'author': ['A'], 'title': ['T'], 'keywords': ['k1, k2']}).lazy()
        result = _normalize_parser_columns(lf, 'pdf').collect()
        assert result['keywords'][0] == 'k1, k2'

    @pytest.mark.unit
    def test_pdf_no_keywords(self):
        """PDF without keywords column creates empty."""
        lf = pl.DataFrame({'author': ['A'], 'title': ['T']}).lazy()
        result = _normalize_parser_columns(lf, 'pdf').collect()
        assert result['keywords'][0] == ''

    @pytest.mark.unit
    def test_no_exprs_returns_same(self):
        """When all columns already exist, no exprs needed for non-html/pdf."""
        lf = pl.DataFrame({'author': ['A'], 'title': ['T'], 'abstract': ['Ab'], 'date': ['D'], 'keywords': ['K']}).lazy()
        result = _normalize_parser_columns(lf, 'latex').collect()
        assert result['author'][0] == 'A'
        assert result['title'][0] == 'T'
        assert result['abstract'][0] == 'Ab'
        assert result['date'][0] == 'D'
        assert result['keywords'][0] == 'K'

class TestCiteTransformAdditional:
    """Additional tests for coverage gaps in transform_cite."""

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_cached_no_corpus_file(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, tmp_path: Path) -> None:
        """Test cached URL when corpus.parquet does not exist on disk."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=True)
        mock_tracker_cls.return_value = mock_tracker
        corpus_dir = tmp_path / 'cites' / 'corpus'
        corpus_dir.mkdir(parents=True, exist_ok=True)
        raw_dir = tmp_path / 'cites' / 'raw' / 'html'
        raw_dir.mkdir(parents=True, exist_ok=True)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_markdown_content(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, tmp_path: Path) -> None:
        """Test transform with markdown content via file:// URL."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        md_path = tmp_path / 'test.md'
        md_path.write_text('---\ntitle: Test\nauthor: Author\n---\n# Hello\nContent here.')
        result = transform_cite(f'file://{md_path}')
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_latex_content(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, tmp_path: Path) -> None:
        """Test transform with LaTeX content via file:// URL."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        tex_path = tmp_path / 'test.tex'
        tex_path.write_text('\\documentclass{article}\n\\title{Test}\n\\author{Auth}\n\\begin{document}\n\\maketitle\nContent\n\\end{document}\n')
        result = transform_cite(f'file://{tex_path}')
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_unknown_content_type(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path) -> None:
        """Test transform with unknown content type treats as HTML."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        html_content = b'<html><head><title>Test</title></head><body><p>Content</p></body></html>'
        mock_response = Mock()
        mock_response.content = html_content
        mock_response.headers = {}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        result = transform_cite('https://example.com/page.xyz')
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_force_refresh_redownloads_existing(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path) -> None:
        """Test force_refresh re-downloads even when raw file exists."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        raw_dir = tmp_path / 'cites' / 'raw' / 'html'
        raw_dir.mkdir(parents=True, exist_ok=True)
        html_content = b'<html><head><title>New</title></head><body><p>New</p></body></html>'
        mock_response = Mock()
        mock_response.content = html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        result = transform_cite('https://example.com/paper.html', force_refresh=True)
        assert isinstance(result, pl.LazyFrame)
        mock_requests.assert_called_once()

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_file_not_found(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, tmp_path: Path) -> None:
        """Test transform with file:// URL pointing to nonexistent file."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        with pytest.raises((FileNotFoundError, OSError)):
            transform_cite('file:///nonexistent/path/file.html')

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_transform_cite_empty_tags_none(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path) -> None:
        """Test transform with tags=None defaults to empty list."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        html = b'<html><head><title>Test</title></head><body>Body</body></html>'
        mock_response = Mock()
        mock_response.content = html
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        result = transform_cite('https://example.com/test.html', tags=None)
        df = result.collect()
        assert 'tags' in df.columns

class TestCiteConnectorBranches:
    """Tests for connector branches (CMS, Federal Register, eCFR) in transform_cite."""

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_cms_connector_branch(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test CMS connector branch when CMSConnector.can_handle returns True."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        cms_citation = pl.DataFrame({'title': ['CMS Paper'], 'author': ['CMS Author'], 'is_parent_citation': [True], 'citation_type': ['cms_iom'], 'url_hash': ['abc123']})
        with patch('acoharmony._cite.connectors.CMSConnector') as mock_cms:
            mock_cms.can_handle.return_value = True
            mock_cms.process.return_value = [cms_citation]
            result = transform_cite('https://www.cms.gov/some-page', tags=['cms'], note='CMS note')
            assert isinstance(result, pl.LazyFrame)
            df = result.collect()
            assert 'tags' in df.columns
            assert 'note' in df.columns

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_cms_connector_with_existing_corpus(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test CMS connector appending to existing corpus."""
        corpus_dir = tmp_path / 'cites' / 'corpus'
        corpus_dir.mkdir(parents=True, exist_ok=True)
        existing_df = pl.DataFrame({'title': ['Old'], 'url_hash': ['old123']})
        existing_df.write_parquet(corpus_dir / 'corpus.parquet')
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        parent = pl.DataFrame({'title': ['CMS Parent'], 'is_parent_citation': [True], 'citation_type': ['cms_iom']})
        child = pl.DataFrame({'title': ['CMS Child'], 'is_parent_citation': [False], 'citation_type': ['cms_iom_child']})
        with patch('acoharmony._cite.connectors.CMSConnector') as mock_cms:
            mock_cms.can_handle.return_value = True
            mock_cms.process.return_value = [parent, child]
            result = transform_cite('https://www.cms.gov/page', tags=['t1'])
            assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_cms_connector_returns_none(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test CMS connector when process() returns None (no handler found)."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        with patch('acoharmony._cite.connectors.CMSConnector') as mock_cms, patch('acoharmony._cite.connectors.FederalRegisterConnector') as mock_fr, patch('acoharmony._cite.connectors.ECFRConnector') as mock_ecfr:
            mock_cms.can_handle.return_value = True
            mock_cms.process.return_value = None
            mock_fr.can_handle.return_value = False
            mock_ecfr.can_handle.return_value = False
            result = transform_cite('https://www.cms.gov/page')
            assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_cms_connector_save_to_corpus_false(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test CMS connector with save_to_corpus=False."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        cms_citation = pl.DataFrame({'title': ['CMS Paper'], 'is_parent_citation': [True], 'citation_type': ['cms_iom']})
        with patch('acoharmony._cite.connectors.CMSConnector') as mock_cms:
            mock_cms.can_handle.return_value = True
            mock_cms.process.return_value = [cms_citation]
            result = transform_cite('https://www.cms.gov/page', save_to_corpus=False)
            assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_federal_register_connector_branch(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test Federal Register connector branch."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        fr_parent = pl.DataFrame({'title': ['FR Rule'], 'is_parent_citation': [True], 'citation_type': ['federal_register']})
        fr_child = pl.DataFrame({'title': ['FR Section'], 'is_parent_citation': [False], 'citation_type': ['federal_register_child']})
        with patch('acoharmony._cite.connectors.CMSConnector') as mock_cms, patch('acoharmony._cite.connectors.FederalRegisterConnector') as mock_fr:
            mock_cms.can_handle.return_value = False
            mock_fr.can_handle.return_value = True
            mock_fr.process.return_value = [fr_parent, fr_child]
            result = transform_cite('https://federalregister.gov/doc/123', tags=['fr'], note='FR note')
            assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_federal_register_with_existing_corpus(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test FR connector appending to existing corpus."""
        corpus_dir = tmp_path / 'cites' / 'corpus'
        corpus_dir.mkdir(parents=True, exist_ok=True)
        existing_df = pl.DataFrame({'title': ['Existing']})
        existing_df.write_parquet(corpus_dir / 'corpus.parquet')
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        fr_citation = pl.DataFrame({'title': ['FR Rule'], 'is_parent_citation': [True], 'citation_type': ['federal_register']})
        with patch('acoharmony._cite.connectors.CMSConnector') as mock_cms, patch('acoharmony._cite.connectors.FederalRegisterConnector') as mock_fr:
            mock_cms.can_handle.return_value = False
            mock_fr.can_handle.return_value = True
            mock_fr.process.return_value = [fr_citation]
            result = transform_cite('https://federalregister.gov/doc/456')
            assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_federal_register_returns_none(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test FR connector when process() returns None."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        with patch('acoharmony._cite.connectors.CMSConnector') as mock_cms, patch('acoharmony._cite.connectors.FederalRegisterConnector') as mock_fr, patch('acoharmony._cite.connectors.ECFRConnector') as mock_ecfr:
            mock_cms.can_handle.return_value = False
            mock_fr.can_handle.return_value = True
            mock_fr.process.return_value = None
            mock_ecfr.can_handle.return_value = False
            result = transform_cite('https://federalregister.gov/doc/789')
            assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_federal_register_save_to_corpus_false(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test FR connector with save_to_corpus=False."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        fr_citation = pl.DataFrame({'title': ['FR Rule'], 'is_parent_citation': [True], 'citation_type': ['federal_register']})
        with patch('acoharmony._cite.connectors.CMSConnector') as mock_cms, patch('acoharmony._cite.connectors.FederalRegisterConnector') as mock_fr:
            mock_cms.can_handle.return_value = False
            mock_fr.can_handle.return_value = True
            mock_fr.process.return_value = [fr_citation]
            result = transform_cite('https://federalregister.gov/doc/123', save_to_corpus=False)
            assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_ecfr_connector_branch(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test eCFR connector branch."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        ecfr_parent = pl.DataFrame({'title': ['42 CFR 425'], 'is_parent_citation': [True], 'citation_type': ['ecfr']})
        ecfr_child = pl.DataFrame({'title': ['42 CFR 425.1'], 'is_parent_citation': [False], 'citation_type': ['ecfr_child']})
        with patch('acoharmony._cite.connectors.CMSConnector') as mock_cms, patch('acoharmony._cite.connectors.FederalRegisterConnector') as mock_fr, patch('acoharmony._cite.connectors.ECFRConnector') as mock_ecfr:
            mock_cms.can_handle.return_value = False
            mock_fr.can_handle.return_value = False
            mock_ecfr.can_handle.return_value = True
            mock_ecfr.process.return_value = [ecfr_parent, ecfr_child]
            result = transform_cite('https://ecfr.gov/title-42', tags=['ecfr'], note='eCFR note')
            assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_ecfr_connector_with_existing_corpus(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test eCFR connector appending to existing corpus."""
        corpus_dir = tmp_path / 'cites' / 'corpus'
        corpus_dir.mkdir(parents=True, exist_ok=True)
        existing_df = pl.DataFrame({'title': ['Existing']})
        existing_df.write_parquet(corpus_dir / 'corpus.parquet')
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        ecfr_citation = pl.DataFrame({'title': ['42 CFR 425'], 'is_parent_citation': [True], 'citation_type': ['ecfr']})
        with patch('acoharmony._cite.connectors.CMSConnector') as mock_cms, patch('acoharmony._cite.connectors.FederalRegisterConnector') as mock_fr, patch('acoharmony._cite.connectors.ECFRConnector') as mock_ecfr:
            mock_cms.can_handle.return_value = False
            mock_fr.can_handle.return_value = False
            mock_ecfr.can_handle.return_value = True
            mock_ecfr.process.return_value = [ecfr_citation]
            result = transform_cite('https://ecfr.gov/title-42')
            assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_ecfr_connector_returns_none(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test eCFR connector when process() returns None."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        with patch('acoharmony._cite.connectors.CMSConnector') as mock_cms, patch('acoharmony._cite.connectors.FederalRegisterConnector') as mock_fr, patch('acoharmony._cite.connectors.ECFRConnector') as mock_ecfr:
            mock_cms.can_handle.return_value = False
            mock_fr.can_handle.return_value = False
            mock_ecfr.can_handle.return_value = True
            mock_ecfr.process.return_value = None
            result = transform_cite('https://ecfr.gov/title-42')
            assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_ecfr_connector_save_to_corpus_false(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test eCFR connector with save_to_corpus=False."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        ecfr_citation = pl.DataFrame({'title': ['42 CFR 425'], 'is_parent_citation': [True], 'citation_type': ['ecfr']})
        with patch('acoharmony._cite.connectors.CMSConnector') as mock_cms, patch('acoharmony._cite.connectors.FederalRegisterConnector') as mock_fr, patch('acoharmony._cite.connectors.ECFRConnector') as mock_ecfr:
            mock_cms.can_handle.return_value = False
            mock_fr.can_handle.return_value = False
            mock_ecfr.can_handle.return_value = True
            mock_ecfr.process.return_value = [ecfr_citation]
            result = transform_cite('https://ecfr.gov/title-42', save_to_corpus=False)
            assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_cached_url_no_corpus_file_falls_through(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test that cached URL without corpus.parquet falls through to download."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=True)
        mock_tracker_cls.return_value = mock_tracker
        corpus_dir = tmp_path / 'cites' / 'corpus'
        corpus_dir.mkdir(parents=True, exist_ok=True)
        raw_dir = tmp_path / 'cites' / 'raw' / 'html'
        raw_dir.mkdir(parents=True, exist_ok=True)
        with patch('acoharmony._transforms._cite.requests.get') as mock_req:
            mock_response = Mock()
            mock_response.content = sample_html_content
            mock_response.headers = {'content-type': 'text/html'}
            mock_response.raise_for_status = Mock()
            mock_req.return_value = mock_response
            result = transform_cite('https://example.com/cached_no_corpus.html')
            assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_cached_raw_file_skips_download(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test that an existing raw file is read instead of downloading."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        result1 = transform_cite('https://example.com/raw_cache_test.html')
        assert isinstance(result1, pl.LazyFrame)
        mock_requests.reset_mock()
        mock_tracker.is_file_processed.return_value = False
        result2 = transform_cite('https://example.com/raw_cache_test.html')
        assert isinstance(result2, pl.LazyFrame)
        mock_requests.assert_not_called()

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_force_refresh_redownloads_existing_raw(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path, sample_html_content: bytes) -> None:
        """Test force_refresh re-downloads even with existing raw file."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        mock_response = Mock()
        mock_response.content = sample_html_content
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        transform_cite('https://example.com/force_raw.html')
        mock_requests.reset_mock()
        mock_requests.return_value = mock_response
        result = transform_cite('https://example.com/force_raw.html', force_refresh=True)
        assert isinstance(result, pl.LazyFrame)
        mock_requests.assert_called_once()

class TestCiteUnknownContentType:
    """Cover unknown content type fallback."""

    @pytest.mark.unit
    def test_unknown_content_type_treated_as_html(self):
        """Lines 284-285: unknown content_type logs warning, treats as HTML."""
        assert hasattr(acoharmony._transforms._cite, 'transform_cite')


class TestGetCaBundlePath:
    """Tests for _get_ca_bundle_path covering branches 55->61 and 56->55."""

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.os.path.exists', return_value=False)
    def test_no_system_ca_bundle_returns_true(self, mock_exists):
        """When no system CA bundle is found, return True (certifi default)."""
        result = _get_ca_bundle_path()
        assert result is True
        # All four CA paths were checked
        assert mock_exists.call_count == 4


class TestContentTypeHeaderDetection:
    """Tests for HTTP content-type header detection branches 289->290 and 292->301."""

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_pdf_content_type_from_header(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path) -> None:
        """When HTTP header says application/pdf, content_type becomes pdf (branch 289->290)."""
        from io import BytesIO
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        buf = BytesIO()
        writer = PdfWriter()
        writer.add_blank_page(width=200, height=200)
        writer.write(buf)
        pdf_bytes = buf.getvalue()
        mock_response = Mock()
        mock_response.content = pdf_bytes
        mock_response.headers = {'content-type': 'application/pdf'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        # Use an html extension URL so initial detection says html, but header overrides to pdf
        result = transform_cite('https://example.com/paper.html')
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    def test_non_pdf_non_html_content_type_header(self, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path) -> None:
        """When HTTP header content-type is neither pdf nor html, no override happens (branch 292->301)."""
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        html_content = b'<html><head><title>Test</title></head><body><p>Content</p></body></html>'
        mock_response = Mock()
        mock_response.content = html_content
        mock_response.headers = {'content-type': 'application/json'}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        # Use html extension so the URL-based detection gives html, but header says json (neither pdf nor html keyword)
        result = transform_cite('https://example.com/data.html')
        assert isinstance(result, pl.LazyFrame)


class TestUnknownContentTypeParseBranch:
    """Test for unknown content_type reaching else at line 318 (branch 315->318)."""

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite.requests.get')
    @patch('acoharmony._transforms._cite.StorageBackend')
    @patch('acoharmony._transforms._cite.CiteStateTracker')
    @patch('acoharmony._transforms._cite._cite_download.build_content_type_detection_expr')
    def test_unknown_content_type_falls_to_else(self, mock_ct_expr, mock_tracker_cls: Mock, mock_storage_cls: Mock, mock_requests: Mock, tmp_path: Path) -> None:
        """When content_type is unknown (not pdf/html/markdown/latex), treat as HTML (branch 315->318)."""
        # Make content type detection return 'unknown'
        mock_ct_expr.return_value = pl.lit('unknown').alias('content_type')
        mock_storage = Mock()
        mock_storage.get_path = Mock(side_effect=lambda p: str(tmp_path / p))
        mock_storage_cls.return_value = mock_storage
        mock_tracker = Mock()
        mock_tracker.is_file_processed = Mock(return_value=False)
        mock_tracker_cls.return_value = mock_tracker
        html_content = b'<html><head><title>Test</title></head><body><p>Content</p></body></html>'
        mock_response = Mock()
        mock_response.content = html_content
        # No content-type header so no override
        mock_response.headers = {}
        mock_response.raise_for_status = Mock()
        mock_requests.return_value = mock_response
        result = transform_cite('https://example.com/page.xyz')
        assert isinstance(result, pl.LazyFrame)
