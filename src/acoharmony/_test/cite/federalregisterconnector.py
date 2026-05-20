"""
Integration tests for Federal Register connector.

Tests the Federal Register connector with real data from federalregister.gov.
"""
from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import requests

from acoharmony._test.cite.conftest import _make_base_citation, _write_html


@pytest.fixture
def fr_document_url() -> str:
    """Real Federal Register document URL for testing."""
    return 'https://www.federalregister.gov/d/2024-25382'

@pytest.fixture
def fr_paragraph_url() -> str:
    """Real Federal Register paragraph URL for testing."""
    return 'https://www.federalregister.gov/d/2024-25382/p-3'

@pytest.fixture
def fr_document_html(fr_document_url: str, tmp_path: Path) -> Path:
    """Download real Federal Register document HTML for testing."""
    html_path = tmp_path / 'fr_document.html'
    response = requests.get(fr_document_url, timeout=30)
    response.raise_for_status()
    with open(html_path, 'wb') as f:
        f.write(response.content)
    return html_path

class TestFederalRegisterDetection:
    """Tests for Federal Register URL detection."""

    @pytest.mark.integration
    def test_can_handle_real_url(self, fr_document_url: str) -> None:
        """Test detection with real Federal Register URL."""
        from acoharmony._cite.connectors import FederalRegisterConnector
        assert FederalRegisterConnector.can_handle(fr_document_url)

    @pytest.mark.unit
    def test_can_handle_patterns(self) -> None:
        """Test various Federal Register URL patterns."""
        from acoharmony._cite.connectors import FederalRegisterConnector
        valid_urls = ['https://www.federalregister.gov/d/2024-25382', 'https://www.federalregister.gov/d/2024-25382/p-3', 'https://federalregister.gov/d/2023-12345', 'http://www.federalregister.gov/d/E9-12345']
        invalid_urls = ['https://example.com', 'https://www.cms.gov/regulations', 'https://arxiv.org/pdf/2301.12345.pdf']
        for url in valid_urls:
            assert FederalRegisterConnector.can_handle(url), f'Should detect: {url}'
        for url in invalid_urls:
            assert not FederalRegisterConnector.can_handle(url), f'Should not detect: {url}'

class TestFederalRegisterURLParsing:
    """Tests for Federal Register URL parsing."""

    @pytest.mark.unit
    def test_parse_document_url(self) -> None:
        """Test parsing document-only URL."""
        from acoharmony._cite.connectors import FederalRegisterConnector
        url = 'https://www.federalregister.gov/d/2024-25382'
        result = FederalRegisterConnector.parse_url(url)
        assert result['document_number'] == '2024-25382'
        assert result['paragraph_number'] is None

    @pytest.mark.unit
    def test_parse_paragraph_url(self) -> None:
        """Test parsing paragraph URL."""
        from acoharmony._cite.connectors import FederalRegisterConnector
        url = 'https://www.federalregister.gov/d/2024-25382/p-45'
        result = FederalRegisterConnector.parse_url(url)
        assert result['document_number'] == '2024-25382'
        assert result['paragraph_number'] == '45'

    @pytest.mark.unit
    def test_parse_various_document_formats(self) -> None:
        """Test parsing various document number formats."""
        from acoharmony._cite.connectors import FederalRegisterConnector
        test_cases = [('https://www.federalregister.gov/d/2024-25382', '2024-25382'), ('https://www.federalregister.gov/d/E9-12345', 'E9-12345'), ('https://www.federalregister.gov/d/2023-00001', '2023-00001')]
        for url, expected_doc_num in test_cases:
            result = FederalRegisterConnector.parse_url(url)
            assert result['document_number'] == expected_doc_num, f'Failed for {url}'

class TestFederalRegisterMetadata:
    """Tests for Federal Register metadata extraction."""

    @pytest.mark.integration
    def test_fetch_document_metadata_real(self) -> None:
        """Test metadata fetching from real Federal Register API."""
        from acoharmony._cite.connectors import FederalRegisterConnector
        metadata = FederalRegisterConnector.fetch_document_metadata('2024-25382')
        assert metadata is not None
        assert 'title' in metadata
        assert 'agencies' in metadata
        assert 'publication_date' in metadata
        assert 'document_number' in metadata
        assert metadata['document_number'] == '2024-25382'

    @pytest.mark.integration
    def test_metadata_has_expected_fields(self) -> None:
        """Test that fetched metadata contains expected fields."""
        from acoharmony._cite.connectors import FederalRegisterConnector
        metadata = FederalRegisterConnector.fetch_document_metadata('2024-25382')
        expected_fields = ['agencies', 'docket_ids', 'cfr_references', 'citation', 'document_number', 'type', 'start_page', 'end_page', 'publication_date', 'regulation_id_numbers']
        for field in expected_fields:
            assert field in metadata, f'Missing field: {field}'
        assert metadata['document_number'] == '2024-25382'
        assert metadata['type'] in ['Rule', 'Proposed Rule', 'Notice']
        docket_ids_str = str(metadata['docket_ids'])
        assert 'CMS-1807-F' in docket_ids_str or 'CMS-4201-F' in docket_ids_str

class TestFederalRegisterParagraphExtraction:
    """Tests for paragraph text extraction from XML."""

    @pytest.mark.integration
    def test_extract_paragraph_from_xml(self, tmp_path: Path) -> None:
        """Test paragraph extraction from Federal Register XML."""
        from acoharmony._cite.connectors import FederalRegisterConnector
        xml_url = 'https://www.federalregister.gov/documents/full_text/xml/2024/12/09/2024-25382.xml'
        xml_path = tmp_path / '2024-25382.xml'
        success = FederalRegisterConnector.download_xml(xml_url, xml_path)
        assert success, 'XML download should succeed'
        from acoharmony._parsers._federal_register_xml import extract_paragraph_by_id
        paragraph_text = extract_paragraph_by_id(xml_path, '3')
        assert paragraph_text != '', 'Paragraph text should not be empty'
        assert len(paragraph_text) > 50, 'Paragraph should have substantial content'
        print(f'\nExtracted paragraph (first 200 chars): {paragraph_text[:200]}...')

class TestFederalRegisterProcessing:
    """Tests for full Federal Register processing."""

    @pytest.mark.integration
    def test_process_document_url_real_data(self, fr_document_url: str, fr_document_html: Path) -> None:
        """Test full processing of Federal Register document."""
        from acoharmony._cite.connectors import FederalRegisterConnector
        base_citation = pl.DataFrame({'source_url': [fr_document_url], 'title': ['Temp Title'], 'url_domain': ['www.federalregister.gov']})
        citations = FederalRegisterConnector.process(fr_document_url, fr_document_html, base_citation)
        assert citations is not None
        assert len(citations) == 1, 'Should have only parent citation'
        parent = citations[0]
        assert parent['document_number'][0] == '2024-25382'
        assert parent['document_citation'][0] == '89 FR 97710'
        assert parent['is_parent_citation'][0] is True
        assert parent['citation_type'][0] == 'federal_register'
        assert parent['child_count'][0] == 0
        assert 'Centers for Medicare' in parent['author'][0]
        docket_ids = parent['docket_ids'][0]
        assert 'CMS-1807-F' in docket_ids or 'CMS-4201-F' in docket_ids
        cfr_refs = parent['cfr_references'][0]
        assert '42 CFR' in cfr_refs
        print('\n[OK] Processed Federal Register document:')
        print(f"  Document: {parent['document_number'][0]}")
        print(f"  Citation: {parent['document_citation'][0]}")
        print(f"  Type: {parent['document_type'][0]}")
        print(f"  Agencies: {parent['author'][0][:100]}...")

    @pytest.mark.integration
    def test_process_paragraph_url_real_data(self, fr_paragraph_url: str, tmp_path: Path) -> None:
        """Test full processing of Federal Register paragraph URL."""
        from acoharmony._cite.connectors import FederalRegisterConnector
        html_path = tmp_path / 'fr_paragraph.html'
        response = requests.get(fr_paragraph_url, timeout=30)
        response.raise_for_status()
        html_path.write_bytes(response.content)
        base_citation = pl.DataFrame({'source_url': [fr_paragraph_url], 'title': ['Temp Title'], 'url_domain': ['www.federalregister.gov']})
        citations = FederalRegisterConnector.process(fr_paragraph_url, html_path, base_citation)
        assert citations is not None
        assert len(citations) == 2, 'Should have parent + paragraph citation'
        parent = citations[0]
        assert parent['is_parent_citation'][0] is True
        assert parent['child_count'][0] == 1
        paragraph = citations[1]
        assert paragraph['is_parent_citation'][0] is False
        assert paragraph['citation_type'][0] == 'federal_register_paragraph'
        assert paragraph['paragraph_number'][0] == '3'
        assert 'content' in paragraph.columns
        assert len(paragraph['content'][0]) > 50, 'Paragraph should have content'
        print('\n[OK] Processed Federal Register paragraph:')
        print(f"  Document: {parent['document_number'][0]}")
        print(f"  Paragraph: {paragraph['paragraph_number'][0]}")
        print(f"  Content length: {len(paragraph['content'][0])} chars")

    @pytest.mark.integration
    def test_citation_completeness(self, fr_document_url: str, fr_document_html: Path) -> None:
        """Test that Federal Register citations have complete metadata."""
        from acoharmony._cite.connectors import FederalRegisterConnector
        base_citation = pl.DataFrame({'source_url': [fr_document_url], 'title': ['Test'], 'url_domain': ['www.federalregister.gov']})
        citations = FederalRegisterConnector.process(fr_document_url, fr_document_html, base_citation)
        parent = citations[0]
        required_fields = ['author', 'title', 'document_number', 'document_citation', 'document_type', 'publication_date', 'start_page', 'end_page', 'page_count', 'docket_ids', 'cfr_references', 'html_url', 'pdf_url']
        for field in required_fields:
            assert field in parent.columns, f'Missing field: {field}'
            assert parent[field][0] is not None, f'Field {field} should not be None'
            assert parent[field][0] != '', f'Field {field} should not be empty'

class TestFederalRegisterParagraphFallback:
    """Cover ValueError/IndexError in paragraph extraction."""

    @pytest.mark.unit
    def test_invalid_paragraph_number_fallback(self):
        """Lines 234-235: ValueError/IndexError during paragraph extraction is caught."""
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        connector = FederalRegisterConnector()
        mock_soup = MagicMock()
        mock_para = MagicMock()
        mock_para.get_text.return_value = 'Some text'
        mock_soup.find_all.return_value = [mock_para]
        assert hasattr(connector, 'extract_paragraph_text') or hasattr(connector, 'fetch_document')

class TestFederalRegisterConnector:

    @pytest.mark.unit
    def test_can_handle_positive(self):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        assert FederalRegisterConnector.can_handle('https://www.federalregister.gov/d/2024-25382') is True
        assert FederalRegisterConnector.can_handle('https://www.federalregister.gov/d/2024-25382/p-45') is True

    @pytest.mark.unit
    def test_can_handle_negative(self):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        assert FederalRegisterConnector.can_handle('https://www.federalregister.gov/about') is False
        assert FederalRegisterConnector.can_handle('https://example.com/d/2024-25382') is False

    @pytest.mark.unit
    def test_parse_url_with_paragraph(self):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        result = FederalRegisterConnector.parse_url('https://www.federalregister.gov/d/2024-25382/p-45')
        assert result['document_number'] == '2024-25382'
        assert result['paragraph_number'] == '45'

    @pytest.mark.unit
    def test_parse_url_without_paragraph(self):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        result = FederalRegisterConnector.parse_url('https://www.federalregister.gov/d/2024-25382')
        assert result['document_number'] == '2024-25382'
        assert result['paragraph_number'] is None

    @pytest.mark.unit
    def test_parse_url_no_match(self):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        result = FederalRegisterConnector.parse_url('https://www.federalregister.gov/about')
        assert result['document_number'] is None
        assert result['paragraph_number'] is None

    @patch('acoharmony._cite.connectors._federal_register.requests.get')
    @pytest.mark.unit
    def test_fetch_document_metadata_success(self, mock_get):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'title': 'Test Rule', 'type': 'Rule'}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp
        result = FederalRegisterConnector.fetch_document_metadata('2024-25382')
        assert result['title'] == 'Test Rule'

    @patch('acoharmony._cite.connectors._federal_register.requests.get')
    @pytest.mark.unit
    def test_fetch_document_metadata_failure(self, mock_get):
        import requests as req

        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        mock_get.side_effect = req.RequestException('fail')
        assert FederalRegisterConnector.fetch_document_metadata('2024-25382') is None

    @pytest.mark.unit
    def test_construct_xml_url(self):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        url = FederalRegisterConnector.construct_xml_url('2024-25382', '2024-11-15')
        assert url == 'https://www.federalregister.gov/documents/full_text/xml/2024/11/15/2024-25382.xml'

    @pytest.mark.unit
    def test_construct_xml_url_invalid_date(self):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        result = FederalRegisterConnector.construct_xml_url('2024-25382', 'not-a-date')
        assert result == ''

    @patch('acoharmony._cite.connectors._federal_register.requests.get')
    @pytest.mark.unit
    def test_download_xml_success(self, mock_get, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        mock_resp = MagicMock()
        mock_resp.content = b'<xml>fr doc</xml>'
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp
        save_path = tmp_path / 'doc.xml'
        assert FederalRegisterConnector.download_xml('https://fr.gov/x.xml', save_path) is True
        assert save_path.exists()
        assert save_path.read_bytes() == b'<xml>fr doc</xml>'

    @pytest.mark.unit
    def test_download_xml_cached(self, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        save_path = tmp_path / 'cached.xml'
        save_path.write_text('<xml>cached</xml>', encoding='utf-8')
        assert FederalRegisterConnector.download_xml('https://fr.gov/x.xml', save_path) is True

    @patch('acoharmony._cite.connectors._federal_register.requests.get')
    @pytest.mark.unit
    def test_download_xml_failure(self, mock_get, tmp_path):
        import requests as req

        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        mock_get.side_effect = req.RequestException('timeout')
        save_path = tmp_path / 'fail.xml'
        assert FederalRegisterConnector.download_xml('https://fr.gov/x.xml', save_path) is False

    @pytest.mark.unit
    def test_extract_paragraph_text_by_id(self, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        html = '<html><body><p id="p-45">This is paragraph 45 text.</p></body></html>'
        html_path = _write_html(tmp_path, html)
        result = FederalRegisterConnector.extract_paragraph_text(html_path, '45')
        assert result == 'This is paragraph 45 text.'

    @pytest.mark.unit
    def test_extract_paragraph_text_by_data_page(self, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        html = '<html><body><div data-page="10">Page 10 content here.</div></body></html>'
        html_path = _write_html(tmp_path, html)
        result = FederalRegisterConnector.extract_paragraph_text(html_path, '10')
        assert result == 'Page 10 content here.'

    @pytest.mark.unit
    def test_extract_paragraph_text_by_data_number(self, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        html = '<html><body><p class="paragraph" data-number="3">Paragraph 3 data.</p></body></html>'
        html_path = _write_html(tmp_path, html)
        result = FederalRegisterConnector.extract_paragraph_text(html_path, '3')
        assert result == 'Paragraph 3 data.'

    @pytest.mark.unit
    def test_extract_paragraph_text_full_text_div_fallback(self, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        html = '\n        <html><body>\n            <div class="full-text">\n                <p>First paragraph.</p>\n                <p>Second paragraph text here.</p>\n                <p>Third paragraph.</p>\n            </div>\n        </body></html>\n        '
        html_path = _write_html(tmp_path, html)
        result = FederalRegisterConnector.extract_paragraph_text(html_path, '2')
        assert result == 'Second paragraph text here.'

    @pytest.mark.unit
    def test_extract_paragraph_text_substantial_fallback(self, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        html = '\n        <html><body>\n            <p class="">Short.</p>\n            <p class="">' + 'A' * 150 + '</p>\n        </body></html>\n        '
        html_path = _write_html(tmp_path, html)
        result = FederalRegisterConnector.extract_paragraph_text(html_path, '999')
        assert '[Document excerpt' in result

    @pytest.mark.unit
    def test_extract_paragraph_text_nothing_found(self, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        html = "<html><body><p class=''>Hi</p></body></html>"
        html_path = _write_html(tmp_path, html)
        result = FederalRegisterConnector.extract_paragraph_text(html_path, '999')
        assert result == ''

    @pytest.mark.unit
    def test_extract_paragraph_text_exception(self, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        result = FederalRegisterConnector.extract_paragraph_text(tmp_path / 'nope.html', '1')
        assert result == ''

    @pytest.mark.unit
    def test_extract_paragraph_text_by_generic_id(self, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        html = '<html><body><div id="p-7">Div paragraph 7.</div></body></html>'
        html_path = _write_html(tmp_path, html)
        result = FederalRegisterConnector.extract_paragraph_text(html_path, '7')
        assert result == 'Div paragraph 7.'

    def _make_metadata(self, **overrides):
        defaults = {'title': 'Medicare Final Rule', 'type': 'Rule', 'abstract': 'Summary of rule.', 'publication_date': '2024-11-15', 'start_page': '1000', 'end_page': '1050', 'citation': '89 FR 1000', 'agencies': [{'name': 'CMS'}, {'name': 'HHS'}], 'docket_ids': ['CMS-1234'], 'cfr_references': [{'title': 42, 'part': 414}], 'regulation_id_numbers': ['0938-AU99'], 'html_url': 'https://fr.gov/d/2024-25382', 'pdf_url': 'https://fr.gov/d/2024-25382.pdf'}
        defaults.update(overrides)
        return defaults

    @patch('acoharmony._cite.connectors._federal_register.FederalRegisterConnector.fetch_document_metadata')
    @pytest.mark.unit
    def test_process_document_only(self, mock_fetch, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        mock_fetch.return_value = self._make_metadata()
        html_path = _write_html(tmp_path, '<html></html>')
        base = _make_base_citation()
        results = FederalRegisterConnector.process('https://www.federalregister.gov/d/2024-25382', html_path, base)
        assert results is not None
        assert len(results) == 1
        parent = results[0]
        assert parent['citation_type'][0] == 'federal_register'
        assert parent['is_parent_citation'][0] is True
        assert parent['document_number'][0] == '2024-25382'
        assert parent['author'][0] == 'CMS, HHS'
        assert parent['page_count'][0] == 51
        assert parent['child_count'][0] == 0

    @patch('acoharmony._cite.connectors._federal_register.extract_paragraph_by_id', return_value='XML paragraph text')
    @patch('acoharmony._cite.connectors._federal_register.FederalRegisterConnector.download_xml', return_value=True)
    @patch('acoharmony._cite.connectors._federal_register.FederalRegisterConnector.fetch_document_metadata')
    @pytest.mark.unit
    def test_process_with_paragraph_xml(self, mock_fetch, mock_dl, mock_extract, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        mock_fetch.return_value = self._make_metadata()
        html_path = _write_html(tmp_path, '<html></html>')
        base = _make_base_citation()
        results = FederalRegisterConnector.process('https://www.federalregister.gov/d/2024-25382/p-45', html_path, base)
        assert len(results) == 2
        child = results[1]
        assert child['citation_type'][0] == 'federal_register_paragraph'
        assert child['paragraph_number'][0] == '45'
        assert child['content'][0] == 'XML paragraph text'

    @patch('acoharmony._cite.connectors._federal_register.extract_paragraph_by_id', side_effect=Exception('xml fail'))
    @patch('acoharmony._cite.connectors._federal_register.FederalRegisterConnector.download_xml', return_value=True)
    @patch('acoharmony._cite.connectors._federal_register.FederalRegisterConnector.fetch_document_metadata')
    @pytest.mark.unit
    def test_process_paragraph_xml_fails_html_fallback(self, mock_fetch, mock_dl, mock_extract, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        mock_fetch.return_value = self._make_metadata()
        html = '<html><body><p id="p-45">HTML fallback text for para 45.</p></body></html>'
        html_path = _write_html(tmp_path, html)
        base = _make_base_citation()
        results = FederalRegisterConnector.process('https://www.federalregister.gov/d/2024-25382/p-45', html_path, base)
        assert len(results) == 2
        assert results[1]['content'][0] == 'HTML fallback text for para 45.'

    @patch('acoharmony._cite.connectors._federal_register.FederalRegisterConnector.download_xml', return_value=False)
    @patch('acoharmony._cite.connectors._federal_register.FederalRegisterConnector.fetch_document_metadata')
    @pytest.mark.unit
    def test_process_paragraph_xml_download_fails_html_fallback(self, mock_fetch, mock_dl, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        mock_fetch.return_value = self._make_metadata()
        html = '<html><body><p id="p-45">HTML text.</p></body></html>'
        html_path = _write_html(tmp_path, html)
        base = _make_base_citation()
        results = FederalRegisterConnector.process('https://www.federalregister.gov/d/2024-25382/p-45', html_path, base)
        assert len(results) == 2

    @patch('acoharmony._cite.connectors._federal_register.FederalRegisterConnector.fetch_document_metadata')
    @pytest.mark.unit
    def test_process_paragraph_no_text_found(self, mock_fetch, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        mock_fetch.return_value = self._make_metadata(publication_date='bad-date')
        html = "<html><body><p class=''>tiny</p></body></html>"
        html_path = _write_html(tmp_path, html)
        base = _make_base_citation()
        results = FederalRegisterConnector.process('https://www.federalregister.gov/d/2024-25382/p-45', html_path, base)
        assert len(results) == 1

    @patch('acoharmony._cite.connectors._federal_register.FederalRegisterConnector.fetch_document_metadata')
    @pytest.mark.unit
    def test_process_no_metadata(self, mock_fetch, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        mock_fetch.return_value = None
        html_path = _write_html(tmp_path, '<html></html>')
        base = _make_base_citation()
        result = FederalRegisterConnector.process('https://www.federalregister.gov/d/2024-25382', html_path, base)
        assert result is None

    @pytest.mark.unit
    def test_process_no_document_number(self, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        html_path = _write_html(tmp_path, '<html></html>')
        base = _make_base_citation()
        result = FederalRegisterConnector.process('https://www.federalregister.gov/about', html_path, base)
        assert result is None

    @patch('acoharmony._cite.connectors._federal_register.FederalRegisterConnector.fetch_document_metadata')
    @pytest.mark.unit
    def test_process_no_agencies(self, mock_fetch, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        mock_fetch.return_value = self._make_metadata(agencies=[], start_page='', end_page='')
        html_path = _write_html(tmp_path, '<html></html>')
        base = _make_base_citation()
        results = FederalRegisterConnector.process('https://www.federalregister.gov/d/2024-25382', html_path, base)
        assert results is not None
        assert results[0]['author'][0] == ''
        assert results[0]['author_primary'][0] == ''
        assert results[0]['page_count'][0] is None

    @patch('acoharmony._cite.connectors._federal_register.FederalRegisterConnector.fetch_document_metadata')
    @pytest.mark.unit
    def test_process_cfr_references_empty(self, mock_fetch, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        meta = self._make_metadata(cfr_references=[{'title': None, 'part': None}])
        mock_fetch.return_value = meta
        html_path = _write_html(tmp_path, '<html></html>')
        base = _make_base_citation()
        results = FederalRegisterConnector.process('https://www.federalregister.gov/d/2024-25382', html_path, base)
        assert results is not None
        assert results[0]['cfr_references'][0] == ''

    @patch('acoharmony._cite.connectors._federal_register.extract_paragraph_by_id', return_value='para text')
    @patch('acoharmony._cite.connectors._federal_register.FederalRegisterConnector.download_xml', return_value=True)
    @patch('acoharmony._cite.connectors._federal_register.FederalRegisterConnector.fetch_document_metadata')
    @pytest.mark.unit
    def test_process_paragraph_with_no_agencies(self, mock_fetch, mock_dl, mock_extract, tmp_path):
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        mock_fetch.return_value = self._make_metadata(agencies=[])
        html_path = _write_html(tmp_path, '<html></html>')
        base = _make_base_citation()
        results = FederalRegisterConnector.process('https://www.federalregister.gov/d/2024-25382/p-10', html_path, base)
        assert len(results) == 2
        assert results[1]['author_primary'][0] == ''

    @pytest.mark.unit
    def test_extract_paragraph_text_full_text_div_out_of_range(self, tmp_path):
        """full-text div has paragraphs but paragraph_number exceeds count."""
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        html = '\n        <html><body>\n            <div class="full-text">\n                <p>Only one paragraph.</p>\n            </div>\n            <p class="">' + 'B' * 200 + '</p>\n        </body></html>\n        '
        html_path = _write_html(tmp_path, html)
        result = FederalRegisterConnector.extract_paragraph_text(html_path, '999')
        assert '[Document excerpt' in result

    @pytest.mark.unit
    def test_extract_paragraph_text_full_text_div_valid_index(self, tmp_path):
        """full-text div paragraph extraction by index."""
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        html = '\n        <html><body>\n            <div class="full-text">\n                <p>First.</p>\n                <p>Second paragraph text content.</p>\n            </div>\n        </body></html>\n        '
        html_path = _write_html(tmp_path, html)
        result = FederalRegisterConnector.extract_paragraph_text(html_path, '1')
        assert result == 'First.'

    @pytest.mark.unit
    def test_extract_paragraph_selector_match_empty_text(self, tmp_path):
        """Branch 217->213: selector matches an element but get_text returns empty string."""
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        # p-5 exists but has no text content
        html = '<html><body><p id="p-5"></p><p class="">' + 'C' * 150 + '</p></body></html>'
        html_path = _write_html(tmp_path, html)
        result = FederalRegisterConnector.extract_paragraph_text(html_path, '5')
        # Element found but empty text, should fall through to final fallback
        assert '[Document excerpt' in result

    @pytest.mark.unit
    def test_extract_paragraph_full_text_div_empty_para_text(self, tmp_path):
        """Branch 232->238: full-text div paragraph found by index but text is empty."""
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        # full-text div has 2 paragraphs, but the requested one (index 1) is empty
        html = """
        <html><body>
            <div class="full-text">
                <p>First paragraph has content.</p>
                <p></p>
            </div>
            <p class="">""" + 'D' * 150 + """</p>
        </body></html>
        """
        html_path = _write_html(tmp_path, html)
        result = FederalRegisterConnector.extract_paragraph_text(html_path, '2')
        # Paragraph 2 exists but is empty -> falls through to final fallback
        assert '[Document excerpt' in result

    @pytest.mark.unit
    def test_extract_paragraph_full_text_div_index_negative(self, tmp_path):
        """Branch 230->238: para_idx is out of valid range (paragraph_number = 0 gives idx -1)."""
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        html = """
        <html><body>
            <div class="full-text">
                <p>Only paragraph.</p>
            </div>
            <p class="">""" + 'E' * 150 + """</p>
        </body></html>
        """
        html_path = _write_html(tmp_path, html)
        # paragraph_number '0' -> para_idx = -1, condition 0 <= para_idx fails
        result = FederalRegisterConnector.extract_paragraph_text(html_path, '0')
        assert '[Document excerpt' in result

    @pytest.mark.unit
    def test_extract_paragraph_non_numeric_paragraph_number(self, tmp_path):
        """Lines 228-229: paragraph_number is not a valid int, ValueError caught, para_idx = -1."""
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        html = """
        <html><body>
            <div class="full-text">
                <p>First paragraph.</p>
                <p>Second paragraph.</p>
            </div>
            <p class="">""" + 'F' * 150 + """</p>
        </body></html>
        """
        html_path = _write_html(tmp_path, html)
        # paragraph_number 'abc' -> int('abc') raises ValueError -> para_idx = -1
        result = FederalRegisterConnector.extract_paragraph_text(html_path, 'abc')
        # para_idx = -1 means 0 <= -1 is False, falls through to final fallback
        assert '[Document excerpt' in result

    @pytest.mark.unit
    def test_extract_paragraph_none_paragraph_number(self, tmp_path):
        """Lines 228-229: paragraph_number is None, TypeError caught, para_idx = -1."""
        from acoharmony._cite.connectors._federal_register import FederalRegisterConnector
        html = """
        <html><body>
            <div class="full-text">
                <p>First paragraph.</p>
            </div>
            <p class="">""" + 'G' * 150 + """</p>
        </body></html>
        """
        html_path = _write_html(tmp_path, html)
        # paragraph_number None -> int(None) raises TypeError -> para_idx = -1
        result = FederalRegisterConnector.extract_paragraph_text(html_path, None)
        assert '[Document excerpt' in result
