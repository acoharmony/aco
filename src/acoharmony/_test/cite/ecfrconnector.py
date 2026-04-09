"""
Integration tests for eCFR connector.

Tests the eCFR connector with real data from ecfr.gov.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import requests

from acoharmony._cite.connectors._ecfr import ECFRConnector
from acoharmony._test.cite.conftest import _make_base_citation


@pytest.fixture
def ecfr_section_url() -> str:
    """Real eCFR section URL for testing."""
    return "https://www.ecfr.gov/current/title-42/section-414.2"


class TestECFRDetection:
    """Tests for eCFR URL detection."""

    @pytest.mark.integration
    def test_can_handle_real_url(self, ecfr_section_url: str) -> None:
        """Test detection with real eCFR URL."""

        assert ECFRConnector.can_handle(ecfr_section_url)

    @pytest.mark.unit
    def test_can_handle_patterns(self) -> None:
        """Test various eCFR URL patterns."""

        valid_urls = [
            "https://www.ecfr.gov/current/title-42/section-414.2",
            "https://www.ecfr.gov/current/title-42/chapter-IV/part-414/section-414.2",
            "https://www.ecfr.gov/on/2024-11-01/title-42/section-414.2",
            "https://ecfr.gov/current/title-42/part-425",
        ]

        invalid_urls = [
            "https://example.com",
            "https://www.cms.gov/regulations",
            "https://www.federalregister.gov/d/2024-25382",
        ]

        for url in valid_urls:
            assert ECFRConnector.can_handle(url), f"Should detect: {url}"

        for url in invalid_urls:
            assert not ECFRConnector.can_handle(url), f"Should not detect: {url}"


class TestECFRURLParsing:
    """Tests for eCFR URL parsing."""

    @pytest.mark.unit
    def test_parse_section_url(self) -> None:
        """Test parsing section URL."""

        url = "https://www.ecfr.gov/current/title-42/section-414.2"
        result = ECFRConnector.parse_url(url)

        assert result["title"] == "42"
        assert result["section"] == "414.2"
        assert result["part"] == "414"
        assert result["date"] is None

    @pytest.mark.unit
    def test_parse_versioned_url(self) -> None:
        """Test parsing dated/versioned URL."""

        url = "https://www.ecfr.gov/on/2024-11-01/title-42/section-414.2"
        result = ECFRConnector.parse_url(url)

        assert result["title"] == "42"
        assert result["section"] == "414.2"
        assert result["date"] == "2024-11-01"

    @pytest.mark.unit
    def test_parse_part_url(self) -> None:
        """Test parsing part URL without specific section."""

        url = "https://www.ecfr.gov/current/title-42/chapter-IV/part-425"
        result = ECFRConnector.parse_url(url)

        assert result["title"] == "42"
        assert result["part"] == "425"
        assert result["section"] is None


class TestECFRStructure:
    """Tests for eCFR structure metadata."""

    @pytest.mark.integration
    def test_get_latest_date(self) -> None:
        """Test fetching latest available date from eCFR API."""

        latest_date = ECFRConnector.get_latest_date("42")

        assert latest_date is not None, "Should return a date"
        # Verify date format YYYY-MM-DD
        assert re.match(r"\d{4}-\d{2}-\d{2}", latest_date), f"Invalid date format: {latest_date}"

    @pytest.mark.integration
    def test_get_latest_structure(self) -> None:
        """Test fetching structure from eCFR API."""

        structure = ECFRConnector.get_latest_structure("42")

        # Structure API may not be available, that's okay
        # We primarily rely on XML for content extraction
        if structure:
            assert isinstance(structure, dict)

    @pytest.mark.integration
    def test_construct_xml_url_current(self) -> None:
        """Test XML URL construction for current version (requires network to get latest date)."""

        xml_url = ECFRConnector.construct_xml_url("42")

        assert xml_url is not None, "Should return a valid URL"
        assert "title-42.xml" in xml_url
        assert ECFRConnector.API_BASE in xml_url
        # Should contain a date in YYYY-MM-DD format
        assert re.search(r"\d{4}-\d{2}-\d{2}", xml_url), "Should contain a date"

    @pytest.mark.unit
    def test_construct_xml_url_dated(self) -> None:
        """Test XML URL construction for specific date."""

        xml_url = ECFRConnector.construct_xml_url("42", "2024-11-01")

        assert "title-42.xml" in xml_url
        assert "2024-11-01" in xml_url


class TestECFRProcessing:
    """Tests for full eCFR processing."""

    @pytest.mark.integration
    def test_process_section_url_real_data(self, ecfr_section_url: str, tmp_path: Path) -> None:
        """Test full processing of eCFR section URL."""

        # Download HTML (though we'll use XML for content)
        html_path = tmp_path / "ecfr.html"
        response = requests.get(ecfr_section_url, timeout=30)
        response.raise_for_status()
        html_path.write_bytes(response.content)

        # Create base citation
        base_citation = pl.DataFrame(
            {
                "source_url": [ecfr_section_url],
                "title": ["Temp Title"],
                "url_domain": ["www.ecfr.gov"],
            }
        )

        # Process
        citations = ECFRConnector.process(ecfr_section_url, html_path, base_citation)

        # Verify we got at least parent citation
        assert citations is not None
        assert len(citations) >= 1, "Should have at least parent citation"

        # Verify parent
        parent = citations[0]
        assert parent["cfr_title"][0] == "42"
        assert parent["cfr_section"][0] == "414.2"
        assert parent["is_parent_citation"][0] is True
        assert parent["citation_type"][0] == "ecfr"

        print("\n[OK] Processed eCFR section:")
        print(f"  Title: {parent['cfr_title'][0]} CFR")
        print(f"  Section: § {parent['cfr_section'][0]}")
        print(f"  Citations: {len(citations)} (parent + {len(citations) - 1} children)")

        # If section child was extracted
        if len(citations) == 2:
            section = citations[1]
            assert section["is_parent_citation"][0] is False
            assert section["citation_type"][0] == "ecfr_section"
            assert section["cfr_section"][0] == "414.2"
            assert "content" in section.columns

            if section["content"][0]:
                print(f"  Section content: {len(section['content'][0])} chars")
        else:
            print("  Note: Section content requires eCFR XML API access")

    @pytest.mark.integration
    def test_citation_completeness(self, ecfr_section_url: str, tmp_path: Path) -> None:
        """Test that eCFR citations have complete metadata."""

        html_path = tmp_path / "ecfr.html"
        response = requests.get(ecfr_section_url, timeout=30)
        response.raise_for_status()
        html_path.write_bytes(response.content)

        base_citation = pl.DataFrame(
            {
                "source_url": [ecfr_section_url],
                "title": ["Test"],
                "url_domain": ["www.ecfr.gov"],
            }
        )

        citations = ECFRConnector.process(ecfr_section_url, html_path, base_citation)

        parent = citations[0]

        # Check required parent fields
        required_fields = [
            "author",
            "title",
            "cfr_title",
            "cfr_section",
            "citation_type",
        ]

        for field in required_fields:
            assert field in parent.columns, f"Missing field: {field}"
            assert parent[field][0] is not None, f"Field {field} should not be None"
            assert parent[field][0] != "", f"Field {field} should not be empty"

        # If section citation exists, check its fields
        if len(citations) > 1:
            section = citations[1]
            required_section_fields = [
                "cfr_title",
                "cfr_section",
                "section_title",
            ]

            for field in required_section_fields:
                assert field in section.columns, f"Missing section field: {field}"


# ===========================================================================
# 8. ECFRConnector tests
# ===========================================================================


class TestECFRConnector:
    @pytest.mark.unit
    def test_can_handle_positive(self):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        assert (
            ECFRConnector.can_handle("https://www.ecfr.gov/current/title-42/section-414.2") is True
        )
        assert ECFRConnector.can_handle("https://www.ecfr.gov/current/title-42/part-414") is True

    @pytest.mark.unit
    def test_can_handle_negative(self):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        assert ECFRConnector.can_handle("https://www.ecfr.gov/about") is False
        assert ECFRConnector.can_handle("https://example.com/title-42") is False

    @pytest.mark.unit
    def test_parse_url_full(self):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        result = ECFRConnector.parse_url(
            "https://www.ecfr.gov/on/2024-11-01/title-42/chapter-IV/part-414/section-414.2"
        )
        assert result["title"] == "42"
        assert result["section"] == "414.2"
        assert result["part"] == "414"
        assert result["date"] == "2024-11-01"

    @pytest.mark.unit
    def test_parse_url_section_only(self):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        result = ECFRConnector.parse_url("https://www.ecfr.gov/current/title-42/section-414.2")
        assert result["title"] == "42"
        assert result["section"] == "414.2"
        assert result["part"] == "414"  # inferred from section
        assert result["date"] is None

    @pytest.mark.unit
    def test_parse_url_part_only(self):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        result = ECFRConnector.parse_url("https://www.ecfr.gov/current/title-42/part-414")
        assert result["title"] == "42"
        assert result["section"] is None
        assert result["part"] == "414"

    @pytest.mark.unit
    def test_parse_url_minimal(self):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        result = ECFRConnector.parse_url("https://www.ecfr.gov/current/title-42")
        assert result["title"] == "42"
        assert result["section"] is None
        assert result["part"] is None
        assert result["date"] is None

    @pytest.mark.unit
    def test_parse_url_section_no_decimal(self):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        result = ECFRConnector.parse_url("https://www.ecfr.gov/current/title-42/section-414")
        assert result["section"] == "414"
        assert result["part"] is None  # no decimal, no inferred part

    @patch("acoharmony._cite.connectors._ecfr.requests.get")
    @pytest.mark.unit
    def test_get_latest_date_success(self, mock_get):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "content_versions": [
                {"date": "2024-01-01"},
                {"date": "2024-06-15"},
                {"date": "2024-03-01"},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp
        result = ECFRConnector.get_latest_date("42")
        assert result == "2024-06-15"
        mock_get.assert_called_once()

    @patch("acoharmony._cite.connectors._ecfr.requests.get")
    @pytest.mark.unit
    def test_get_latest_date_empty_versions(self, mock_get):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"content_versions": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp
        assert ECFRConnector.get_latest_date("42") is None

    @patch("acoharmony._cite.connectors._ecfr.requests.get")
    @pytest.mark.unit
    def test_get_latest_date_request_error(self, mock_get):
        import requests as req

        from acoharmony._cite.connectors._ecfr import ECFRConnector

        mock_get.side_effect = req.RequestException("network error")
        assert ECFRConnector.get_latest_date("42") is None

    @patch("acoharmony._cite.connectors._ecfr.requests.get")
    @pytest.mark.unit
    def test_get_latest_structure_success(self, mock_get):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"title": {"name": "Public Health"}}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp
        result = ECFRConnector.get_latest_structure("42", date="2024-06-15")
        assert result == {"title": {"name": "Public Health"}}

    @patch("acoharmony._cite.connectors._ecfr.requests.get")
    @pytest.mark.unit
    def test_get_latest_structure_no_date(self, mock_get):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        # First call for get_latest_date, second for structure
        date_resp = MagicMock()
        date_resp.json.return_value = {"content_versions": [{"date": "2024-06-15"}]}
        date_resp.raise_for_status = MagicMock()
        struct_resp = MagicMock()
        struct_resp.json.return_value = {"title": "42"}
        struct_resp.raise_for_status = MagicMock()
        mock_get.side_effect = [date_resp, struct_resp]
        result = ECFRConnector.get_latest_structure("42")
        assert result == {"title": "42"}

    @patch("acoharmony._cite.connectors._ecfr.requests.get")
    @pytest.mark.unit
    def test_get_latest_structure_no_date_available(self, mock_get):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        date_resp = MagicMock()
        date_resp.json.return_value = {"content_versions": []}
        date_resp.raise_for_status = MagicMock()
        mock_get.return_value = date_resp
        assert ECFRConnector.get_latest_structure("42") is None

    @patch("acoharmony._cite.connectors._ecfr.requests.get")
    @pytest.mark.unit
    def test_get_latest_structure_request_error(self, mock_get):
        import requests as req

        from acoharmony._cite.connectors._ecfr import ECFRConnector

        mock_get.side_effect = req.RequestException("fail")
        assert ECFRConnector.get_latest_structure("42", date="2024-01-01") is None

    @pytest.mark.unit
    def test_construct_xml_url_with_date(self):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        url = ECFRConnector.construct_xml_url("42", "2024-06-15")
        assert url == "https://www.ecfr.gov/api/versioner/v1/full/2024-06-15/title-42.xml"

    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_date", return_value="2024-06-15"
    )
    @pytest.mark.unit
    def test_construct_xml_url_no_date(self, mock_date):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        url = ECFRConnector.construct_xml_url("42")
        assert "2024-06-15" in url

    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_date", return_value=None)
    @pytest.mark.unit
    def test_construct_xml_url_no_date_available(self, mock_date):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        assert ECFRConnector.construct_xml_url("42") is None

    @patch("acoharmony._cite.connectors._ecfr.requests.get")
    @pytest.mark.unit
    def test_download_xml_success(self, mock_get, tmp_path):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        mock_resp = MagicMock()
        mock_resp.content = b"<xml>data</xml>"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp
        save_path = tmp_path / "ecfr.xml"
        assert ECFRConnector.download_xml("https://ecfr.gov/api/x.xml", save_path) is True
        assert save_path.read_bytes() == b"<xml>data</xml>"

    @pytest.mark.unit
    def test_download_xml_cached(self, tmp_path):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        save_path = tmp_path / "cached.xml"
        save_path.write_text("<xml>cached</xml>", encoding="utf-8")
        assert ECFRConnector.download_xml("https://ecfr.gov/api/x.xml", save_path) is True

    @patch("acoharmony._cite.connectors._ecfr.requests.get")
    @pytest.mark.unit
    def test_download_xml_cached_empty_redownloads(self, mock_get, tmp_path):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        save_path = tmp_path / "empty.xml"
        save_path.write_text("", encoding="utf-8")
        mock_resp = MagicMock()
        mock_resp.content = b"<xml>fresh</xml>"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp
        assert ECFRConnector.download_xml("https://ecfr.gov/api/x.xml", save_path) is True
        assert save_path.read_bytes() == b"<xml>fresh</xml>"

    @patch("acoharmony._cite.connectors._ecfr.requests.get")
    @pytest.mark.unit
    def test_download_xml_force(self, mock_get, tmp_path):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        save_path = tmp_path / "forced.xml"
        save_path.write_text("<xml>old</xml>", encoding="utf-8")
        mock_resp = MagicMock()
        mock_resp.content = b"<xml>new</xml>"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp
        assert (
            ECFRConnector.download_xml("https://ecfr.gov/api/x.xml", save_path, force=True) is True
        )
        assert save_path.read_bytes() == b"<xml>new</xml>"

    @patch("acoharmony._cite.connectors._ecfr.requests.get")
    @pytest.mark.unit
    def test_download_xml_failure(self, mock_get, tmp_path):
        import requests as req

        from acoharmony._cite.connectors._ecfr import ECFRConnector

        mock_get.side_effect = req.RequestException("timeout")
        save_path = tmp_path / "fail.xml"
        assert ECFRConnector.download_xml("https://ecfr.gov/api/x.xml", save_path) is False

    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.download_xml", return_value=True)
    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.construct_xml_url",
        return_value="https://ecfr.gov/api/full/2024-06-15/title-42.xml",
    )
    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_date", return_value="2024-06-15"
    )
    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_structure",
        return_value={"title": {"name": "Public Health"}},
    )
    @patch("acoharmony._cite.connectors._ecfr.extract_section_by_number")
    @pytest.mark.unit
    def test_process_with_section(
        self, mock_extract, mock_struct, mock_date, mock_xml_url, mock_dl, tmp_path
    ):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        mock_extract.return_value = {
            "section_text": "The regulation text here...",
            "section_title": "Scope of Benefits",
            "part_number": "414",
            "subpart": "A",
            "authority": "42 USC 1395",
            "source": "FR 12345",
        }
        html_path = tmp_path / "html" / "page.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text("<html></html>")
        # Create xml directory at sibling level
        (tmp_path / "xml").mkdir(exist_ok=True)
        base = _make_base_citation()
        results = ECFRConnector.process(
            "https://www.ecfr.gov/current/title-42/section-414.2", html_path, base
        )
        assert results is not None
        assert len(results) == 2  # parent + section child
        parent = results[0]
        assert parent["citation_type"][0] == "ecfr"
        assert "42 CFR" in parent["title"][0]
        assert parent["is_parent_citation"][0] is True

        child = results[1]
        assert child["citation_type"][0] == "ecfr_section"
        assert "414.2" in child["title"][0]
        assert child["content"][0] == "The regulation text here..."

    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_structure",
        return_value={"title": "Title 42"},
    )
    @pytest.mark.unit
    def test_process_no_section(self, mock_struct, tmp_path):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        html_path = tmp_path / "html" / "page.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text("<html></html>")
        base = _make_base_citation()
        results = ECFRConnector.process(
            "https://www.ecfr.gov/current/title-42/part-414", html_path, base
        )
        assert results is not None
        assert len(results) == 1
        assert "42 CFR Part 414" in results[0]["title"][0]

    @pytest.mark.unit
    def test_process_no_title(self, tmp_path):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        html_path = tmp_path / "page.html"
        html_path.write_text("<html></html>")
        base = _make_base_citation()
        result = ECFRConnector.process(
            "https://www.ecfr.gov/current/something-weird", html_path, base
        )
        assert result is None

    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.download_xml", return_value=True)
    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.construct_xml_url",
        return_value="https://ecfr.gov/x.xml",
    )
    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_date", return_value="2024-06-15"
    )
    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_structure", return_value={})
    @patch("acoharmony._cite.connectors._ecfr.extract_section_by_number", return_value=None)
    @pytest.mark.unit
    def test_process_section_not_found_in_xml(
        self, mock_extract, mock_struct, mock_date, mock_url, mock_dl, tmp_path
    ):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        html_path = tmp_path / "html" / "page.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text("<html></html>")
        (tmp_path / "xml").mkdir(exist_ok=True)
        base = _make_base_citation()
        results = ECFRConnector.process(
            "https://www.ecfr.gov/current/title-42/section-414.2", html_path, base
        )
        assert len(results) == 1  # parent only

    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.download_xml", return_value=True)
    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.construct_xml_url",
        return_value="https://ecfr.gov/x.xml",
    )
    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_date", return_value="2024-06-15"
    )
    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_structure", return_value={})
    @patch(
        "acoharmony._cite.connectors._ecfr.extract_section_by_number",
        side_effect=Exception("parse error"),
    )
    @pytest.mark.unit
    def test_process_section_extraction_error(
        self, mock_extract, mock_struct, mock_date, mock_url, mock_dl, tmp_path
    ):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        html_path = tmp_path / "html" / "page.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text("<html></html>")
        (tmp_path / "xml").mkdir(exist_ok=True)
        base = _make_base_citation()
        results = ECFRConnector.process(
            "https://www.ecfr.gov/current/title-42/section-414.2", html_path, base
        )
        assert len(results) == 1  # parent only due to error

    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.download_xml", return_value=False)
    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.construct_xml_url",
        return_value="https://ecfr.gov/x.xml",
    )
    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_date", return_value="2024-06-15"
    )
    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_structure", return_value={})
    @pytest.mark.unit
    def test_process_xml_download_fails(self, mock_struct, mock_date, mock_url, mock_dl, tmp_path):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        html_path = tmp_path / "html" / "page.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text("<html></html>")
        (tmp_path / "xml").mkdir(exist_ok=True)
        base = _make_base_citation()
        results = ECFRConnector.process(
            "https://www.ecfr.gov/current/title-42/section-414.2", html_path, base
        )
        assert len(results) == 1  # parent only

    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.construct_xml_url", return_value=None)
    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_structure", return_value={})
    @pytest.mark.unit
    def test_process_no_xml_url(self, mock_struct, mock_url, tmp_path):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        html_path = tmp_path / "html" / "page.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text("<html></html>")
        base = _make_base_citation()
        results = ECFRConnector.process(
            "https://www.ecfr.gov/current/title-42/section-414.2", html_path, base
        )
        assert len(results) == 1  # parent only

    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_structure", return_value=None
    )
    @pytest.mark.unit
    def test_process_no_structure(self, mock_struct, tmp_path):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        html_path = tmp_path / "html" / "page.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text("<html></html>")
        base = _make_base_citation()
        results = ECFRConnector.process(
            "https://www.ecfr.gov/current/title-42/part-414", html_path, base
        )
        assert results is not None
        assert len(results) == 1

    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.download_xml", return_value=True)
    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.construct_xml_url",
        return_value="https://ecfr.gov/x.xml",
    )
    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_date", return_value="2024-06-15"
    )
    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_structure", return_value={})
    @patch("acoharmony._cite.connectors._ecfr.extract_section_by_number")
    @pytest.mark.unit
    def test_process_section_empty_text(
        self, mock_extract, mock_struct, mock_date, mock_url, mock_dl, tmp_path
    ):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        mock_extract.return_value = {"section_text": "", "section_title": "Empty"}
        html_path = tmp_path / "html" / "page.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text("<html></html>")
        (tmp_path / "xml").mkdir(exist_ok=True)
        base = _make_base_citation()
        results = ECFRConnector.process(
            "https://www.ecfr.gov/current/title-42/section-414.2", html_path, base
        )
        assert len(results) == 1  # parent only (section_text is falsy)

    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_structure")
    @pytest.mark.unit
    def test_process_title_name_is_string(self, mock_struct, tmp_path):
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        mock_struct.return_value = {"title": "Public Health"}
        html_path = tmp_path / "html" / "page.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text("<html></html>")
        base = _make_base_citation()
        results = ECFRConnector.process(
            "https://www.ecfr.gov/current/title-42/part-414", html_path, base
        )
        assert results is not None

    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.download_xml", return_value=True)
    @patch(
        "acoharmony._cite.connectors._ecfr.ECFRConnector.construct_xml_url",
        return_value="https://ecfr.gov/x.xml",
    )
    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_date", return_value=None)
    @patch("acoharmony._cite.connectors._ecfr.ECFRConnector.get_latest_structure", return_value={})
    @patch(
        "acoharmony._cite.connectors._ecfr.extract_section_by_number",
        return_value={"section_text": "text", "section_title": "t"},
    )
    @pytest.mark.unit
    def test_process_section_no_date(
        self, mock_extract, mock_struct, mock_date, mock_url, mock_dl, tmp_path
    ):
        """When date is None, xml filename omits date."""
        from acoharmony._cite.connectors._ecfr import ECFRConnector

        html_path = tmp_path / "html" / "page.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text("<html></html>")
        (tmp_path / "xml").mkdir(exist_ok=True)
        base = _make_base_citation()
        results = ECFRConnector.process(
            "https://www.ecfr.gov/current/title-42/section-414.2", html_path, base
        )
        # Should still work, date=None triggers the else branch for xml_filename
        assert len(results) == 2
