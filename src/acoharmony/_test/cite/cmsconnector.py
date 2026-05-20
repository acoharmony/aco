# © 2025 HarmonyCares
# All rights reserved.

"""
Integration tests for CMS Manual connector.

Tests the CMS connector with real data from cms.gov.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path

import polars as pl
import pytest

from acoharmony._test.cite.conftest import _make_base_citation, _write_html


@pytest.fixture
def cms_manual_url() -> str:
    """Real CMS manual URL for testing."""
    return "https://www.cms.gov/regulations-and-guidance/guidance/manuals/internet-only-manuals-ioms-items/cms018912"


class TestCMSConnectorDetection:
    """Tests for CMS URL detection."""

    @pytest.mark.integration
    def test_is_cms_manual_real_url(self, cms_manual_url: str) -> None:
        """Test detection with real CMS URL."""
        from acoharmony._cite.connectors import CMSConnector

        assert CMSConnector.can_handle(cms_manual_url)

    @pytest.mark.unit
    def test_is_cms_manual_patterns(self) -> None:
        """Test various CMS URL patterns."""
        from acoharmony._cite.connectors import CMSConnector, IOMHandler

        cms_urls = [
            "https://www.cms.gov/regulations-and-guidance/guidance/manuals/internet-only-manuals-ioms-items/cms018912",
            "https://www.cms.gov/medicare/regulations-guidance/manuals/internet-only-manuals",
            "https://cms.gov/manuals/iom/pub100-04",
        ]

        non_cms_urls = [
            "https://example.com",
            "https://arxiv.org/pdf/2301.12345.pdf",
            "https://pubmed.ncbi.nlm.nih.gov/12345678/",
        ]

        for url in cms_urls:
            assert IOMHandler.can_handle(url), f"Should detect: {url}"
            assert CMSConnector.can_handle(url), f"Should detect: {url}"

        for url in non_cms_urls:
            assert not IOMHandler.can_handle(url), f"Should not detect: {url}"
            assert not CMSConnector.can_handle(url), f"Should not detect: {url}"


class TestCMSMetadataExtraction:
    """Tests for CMS metadata extraction."""

    @pytest.mark.unit
    def test_extract_publication_number_real_page(
        self, cms_manual_url: str
    ) -> None:
        """Test publication number extraction from realistic CMS page content."""
        from acoharmony._cite.connectors import IOMHandler

        # Realistic HTML resembling the cms018912 manual page
        html_content = """
        <html>
        <head><title>Medicare Claims Processing Manual | CMS</title></head>
        <body>
            <h1>Medicare Claims Processing Manual</h1>
            <p>Publication Number: 100-04</p>
            <p>This Internet-Only Manual (IOM) provides instructions for
            processing Medicare claims.</p>
        </body>
        </html>
        """

        pub_number = IOMHandler.extract_publication_number(cms_manual_url, html_content)

        # cms018912 is Medicare Claims Processing Manual (Pub 100-04)
        assert pub_number == "Pub 100-04"

    @pytest.mark.unit
    def test_extract_publication_number_from_url(self) -> None:
        """Test publication number extraction from URL patterns."""
        from acoharmony._cite.connectors import IOMHandler

        test_cases = [
            (
                "https://www.cms.gov/manuals/cms018912",
                "<html></html>",
                "Pub 100-04",
            ),
            (
                "https://www.cms.gov/manuals/cms018913",
                "<html></html>",
                "Pub 100-02",
            ),
            (
                "https://www.cms.gov/manuals/cms018915",
                "<html></html>",
                "Pub 100-01",
            ),
        ]

        for url, html, expected in test_cases:
            result = IOMHandler.extract_publication_number(url, html)
            assert result == expected, f"Failed for {url}"

    @pytest.mark.unit
    def test_extract_publication_number_from_content(self) -> None:
        """Test publication number extraction from page content."""
        from acoharmony._cite.connectors import IOMHandler

        html_with_pub = """
        <html>
        <body>
            <h1>Medicare Claims Processing Manual</h1>
            <p>Publication Number: 100-04</p>
        </body>
        </html>
        """

        result = IOMHandler.extract_publication_number("", html_with_pub)
        assert result == "Pub 100-04"


class TestCMSChapterExtraction:
    """Tests for CMS chapter download extraction."""

    @pytest.mark.unit
    def test_extract_chapter_downloads_real_page(
        self, cms_manual_url: str
    ) -> None:
        """Test chapter extraction from realistic CMS manual page content."""
        from acoharmony._cite.connectors import IOMHandler

        # Realistic HTML resembling the cms018912 manual page with chapter links
        html_content = """
        <html>
        <head><title>Medicare Claims Processing Manual | CMS</title></head>
        <body>
            <h1>Medicare Claims Processing Manual</h1>
            <p>Publication Number: 100-04</p>
            <h2>Downloads</h2>
            <ul>
                <li><a href="/files/document/chapter-1-general-billing-requirements.pdf">Chapter 1 - General Billing Requirements (PDF)</a></li>
                <li><a href="/files/document/chapter-2-admission-billing.pdf">Chapter 2 - Admission and Billing (PDF)</a></li>
                <li><a href="/files/document/chapter-3-inpatient-hospital.pdf">Chapter 3 - Inpatient Hospital Billing (PDF)</a></li>
                <li><a href="/files/document/chapter-4-part-b.zip">Chapter 4 - Part B Hospital (ZIP)</a></li>
                <li><a href="/files/document/chapter-5-part-b-outpatient.docx">Chapter 5 - Part B Outpatient Rehabilitation (DOCX)</a></li>
            </ul>
        </body>
        </html>
        """

        chapters = IOMHandler.extract_chapter_downloads(html_content, cms_manual_url)

        # Verify we found chapters
        assert len(chapters) > 0, "Should find chapter downloads"

        # Verify chapter structure
        for chapter in chapters:
            assert "title" in chapter
            assert "url" in chapter
            assert "chapter_num" in chapter

            # Verify URLs are absolute
            assert chapter["url"].startswith("http")

            # Check for valid extensions
            assert any(ext in chapter["url"].lower() for ext in [".pdf", ".zip", ".docx"]), (
                f"Invalid URL: {chapter['url']}"
            )

        # Verify specific chapter data
        assert len(chapters) == 5
        assert chapters[0]["chapter_num"] == "1"
        assert chapters[1]["chapter_num"] == "2"
        assert "General Billing" in chapters[0]["title"]

    @pytest.mark.unit
    def test_extract_chapter_downloads_sample_html(self) -> None:
        """Test chapter extraction from sample HTML."""
        from acoharmony._cite.connectors import IOMHandler

        sample_html = """
        <html>
        <body>
            <h2>Downloads</h2>
            <ul>
                <li><a href="/files/chapter1.pdf">Chapter 1 - Introduction</a></li>
                <li><a href="/files/chapter2.pdf">Chapter 2 - Eligibility</a></li>
                <li><a href="https://example.com/chapter3.pdf">Chapter 3 - Claims</a></li>
            </ul>
        </body>
        </html>
        """

        base_url = "https://www.cms.gov/manuals/test"
        chapters = IOMHandler.extract_chapter_downloads(sample_html, base_url)

        assert len(chapters) == 3

        # Verify chapter 1
        assert chapters[0]["title"] == "Chapter 1 - Introduction"
        assert chapters[0]["url"].startswith("https://")
        assert chapters[0]["chapter_num"] == "1"

        # Verify chapter 2
        assert chapters[1]["chapter_num"] == "2"

        # Verify chapter 3 (absolute URL)
        assert chapters[2]["url"] == "https://example.com/chapter3.pdf"


class TestCMSManualProcessing:
    """Tests for full CMS manual processing."""

    @pytest.mark.unit
    def test_process_cms_manual_real_data(self, cms_manual_url: str, tmp_path: Path) -> None:
        """Test full processing of CMS manual page with realistic mock HTML."""
        from acoharmony._cite.connectors import CMSConnector

        # Realistic HTML resembling the cms018912 manual page
        html_content = """
        <html>
        <head><title>Medicare Claims Processing Manual | CMS</title></head>
        <body>
            <h1>Medicare Claims Processing Manual</h1>
            <p>Publication Number: 100-04</p>
            <h2>Downloads</h2>
            <ul>
                <li><a href="/files/document/chapter-1-general-billing.pdf">Chapter 1 - General Billing Requirements</a></li>
                <li><a href="/files/document/chapter-2-admission.pdf">Chapter 2 - Admission and Billing</a></li>
                <li><a href="/files/document/chapter-3-inpatient.pdf">Chapter 3 - Inpatient Hospital Billing</a></li>
            </ul>
        </body>
        </html>
        """
        html_path = _write_html(tmp_path, html_content, "cms_manual.html")

        # Create minimal parent citation
        parent_citation = pl.DataFrame(
            {
                "source_url": [cms_manual_url],
                "title": ["Medicare Claims Processing Manual"],
                "url_domain": ["www.cms.gov"],
            }
        )

        # Process manual
        citations = CMSConnector.process(cms_manual_url, html_path, parent_citation)

        # Verify we got parent + chapters
        assert len(citations) >= 2, "Should have at least parent + 1 chapter"

        # Verify parent citation
        parent = citations[0]
        assert parent["author"][0] == "CMS"
        assert parent["author_full"][0] == "Centers for Medicare & Medicaid Services"
        assert parent["publication_number"][0] == "Pub 100-04"
        assert parent["is_parent_citation"][0] is True
        assert parent["citation_type"][0] == "cms_iom"
        assert parent["child_count"][0] > 0

        # Verify chapter citations
        for chapter_citation in citations[1:]:
            assert chapter_citation["author"][0] == "CMS"
            assert chapter_citation["publication_number"][0] == "Pub 100-04"
            assert chapter_citation["is_parent_citation"][0] is False
            assert chapter_citation["citation_type"][0] == "cms_iom_chapter"
            assert "download_url" in chapter_citation.columns
            assert "chapter_number" in chapter_citation.columns
            assert chapter_citation["parent_url"][0] == cms_manual_url

    @pytest.mark.unit
    def test_cms_manual_citation_completeness(
        self, cms_manual_url: str, tmp_path: Path
    ) -> None:
        """Test that CMS citations have complete metadata."""
        from acoharmony._cite.connectors import CMSConnector

        # Realistic HTML with chapters for completeness check
        html_content = """
        <html>
        <head><title>Medicare Claims Processing Manual | CMS</title></head>
        <body>
            <h1>Medicare Claims Processing Manual</h1>
            <p>Publication Number: 100-04</p>
            <h2>Downloads</h2>
            <ul>
                <li><a href="/files/document/chapter-1-general-billing.pdf">Chapter 1 - General Billing Requirements</a></li>
                <li><a href="/files/document/chapter-2-admission.pdf">Chapter 2 - Admission and Billing</a></li>
            </ul>
        </body>
        </html>
        """
        html_path = _write_html(tmp_path, html_content, "cms_manual.html")

        parent_citation = pl.DataFrame(
            {
                "source_url": [cms_manual_url],
                "title": ["Medicare Claims Processing Manual"],
                "url_domain": ["www.cms.gov"],
            }
        )

        citations = CMSConnector.process(cms_manual_url, html_path, parent_citation)

        # Check parent completeness
        parent = citations[0]
        required_parent_fields = [
            "author",
            "author_full",
            "publication_number",
            "child_count",
            "is_parent_citation",
        ]
        for field in required_parent_fields:
            assert field in parent.columns, f"Missing field: {field}"
            assert parent[field][0] is not None

        # Check chapter completeness
        if len(citations) > 1:
            chapter = citations[1]
            required_chapter_fields = [
                "author",
                "title",
                "normalized_title",
                "download_url",
                "publication_number",
                "parent_url",
                "is_parent_citation",
                "child_sequence",
            ]
            for field in required_chapter_fields:
                assert field in chapter.columns, f"Missing field: {field}"
                assert chapter[field][0] is not None


class TestPFSHandler:
    """Tests for PFS regulation handler."""

    @pytest.fixture
    def pfs_url(self) -> str:
        """Real PFS regulation URL."""
        return "https://www.cms.gov/medicare/payment/fee-schedules/physician/federal-regulation-notices/cms-1832-p"

    @pytest.mark.unit
    def test_pfs_can_handle(self, pfs_url: str) -> None:
        """Test PFS URL detection."""
        from acoharmony._cite.connectors import PFSHandler

        assert PFSHandler.can_handle(pfs_url)

    @pytest.mark.unit
    def test_pfs_extract_regulation_number(self, pfs_url: str) -> None:
        """Test regulation number extraction from URL."""
        from acoharmony._cite.connectors import PFSHandler

        reg_num = PFSHandler.extract_regulation_number(pfs_url, "<html></html>")
        assert reg_num == "CMS-1832-P"

    @pytest.mark.unit
    def test_pfs_process_real_page(self, pfs_url: str, tmp_path: Path) -> None:
        """Test PFS processing with realistic mock page data."""
        from acoharmony._cite.connectors import CMSConnector, PFSHandler

        # Realistic HTML resembling a PFS regulation notice page
        html_content = """
        <html>
        <head><title>CMS-1832-P | CMS</title></head>
        <body>
            <h1>CMS-1832-P: Medicare and Medicaid Programs; CY 2025 Payment Policies
            Under the Physician Fee Schedule</h1>
            <p>2025 Physician Fee Schedule Proposed Rule</p>
            <h2>Related Documents</h2>
            <ul>
                <li><a href="/files/document/cms-1832-p-proposed-rule.pdf">CMS-1832-P Proposed Rule (PDF)</a></li>
                <li><a href="/files/document/cms-1832-p-fact-sheet.pdf">Fact Sheet Summary (PDF)</a></li>
                <li><a href="/files/document/cms-1832-p-display-copy.pdf">Display Copy of Proposed Rule (PDF)</a></li>
            </ul>
        </body>
        </html>
        """
        html_path = _write_html(tmp_path, html_content, "pfs.html")

        # Create base citation
        base_citation = pl.DataFrame(
            {
                "source_url": [pfs_url],
                "title": ["CMS-1832-P Physician Fee Schedule"],
                "url_domain": ["www.cms.gov"],
            }
        )

        # Process
        assert PFSHandler.can_handle(pfs_url)
        citations = CMSConnector.process(pfs_url, html_path, base_citation)

        # Verify
        assert citations is not None
        assert len(citations) >= 1

        # Check parent
        parent = citations[0]
        assert parent["author"][0] == "CMS"
        assert parent["citation_type"][0] == "cms_pfs_regulation"
        assert "regulation_number" in parent.columns

    @pytest.mark.unit
    def test_can_handle_positive(self):
        from acoharmony._cite.connectors._cms import PFSHandler

        assert (
            PFSHandler.can_handle("https://cms.gov/physician/federal-regulation-notices/2024")
            is True
        )
        assert PFSHandler.can_handle("https://cms.gov/fee-schedules/physician/final-rule") is True
        assert PFSHandler.can_handle("https://cms.gov/docs/cms-1832-p") is True

    @pytest.mark.unit
    def test_can_handle_negative(self):
        from acoharmony._cite.connectors._cms import PFSHandler

        assert PFSHandler.can_handle("https://cms.gov/manual/chapter1") is False
        assert (
            PFSHandler.can_handle("https://example.com/physician/federal-regulation-notices")
            is False
        )

    @pytest.mark.unit
    def test_extract_regulation_number_from_url(self):
        from acoharmony._cite.connectors._cms import PFSHandler

        result = PFSHandler.extract_regulation_number(
            "https://cms.gov/docs/cms-1832-p/final", "<html></html>"
        )
        assert result == "CMS-1832-P"

    @pytest.mark.unit
    def test_extract_regulation_number_from_content(self):
        from acoharmony._cite.connectors._cms import PFSHandler

        html = "<html><body>The regulation CMS-1784-F was issued.</body></html>"
        result = PFSHandler.extract_regulation_number("https://cms.gov/docs", html)
        assert result == "CMS-1784-F"

    @pytest.mark.unit
    def test_extract_regulation_number_from_content_with_label(self):
        from acoharmony._cite.connectors._cms import PFSHandler

        html = "<html><body>Regulation Number: CMS-1234-A</body></html>"
        result = PFSHandler.extract_regulation_number("https://cms.gov/docs", html)
        assert result == "CMS-1234-A"

    @pytest.mark.unit
    def test_extract_regulation_number_not_found(self):
        from acoharmony._cite.connectors._cms import PFSHandler

        result = PFSHandler.extract_regulation_number("https://cms.gov/docs", "<html></html>")
        assert result == ""

    @pytest.mark.unit
    def test_extract_regulation_year_from_content(self):
        from acoharmony._cite.connectors._cms import PFSHandler

        html = "<html><body>2024 Physician Fee Schedule Final Rule</body></html>"
        result = PFSHandler.extract_regulation_year("https://cms.gov/pfs", html)
        assert result == "2024"

    @pytest.mark.unit
    def test_extract_regulation_year_from_url(self):
        from acoharmony._cite.connectors._cms import PFSHandler

        html = "<html><body>No year info</body></html>"
        result = PFSHandler.extract_regulation_year("https://cms.gov/pfs/2023/rule", html)
        assert result == "2023"

    @pytest.mark.unit
    def test_extract_regulation_year_not_found(self):
        from acoharmony._cite.connectors._cms import PFSHandler

        result = PFSHandler.extract_regulation_year("https://cms.gov/pfs", "<html></html>")
        assert result == ""

    @pytest.mark.unit
    def test_extract_document_downloads(self):
        from acoharmony._cite.connectors._cms import PFSHandler

        html = """
        <html><body>
            <a href="/final-rule.pdf">Final Rule Document</a>
            <a href="/proposed.pdf">Proposed NPRM</a>
            <a href="/facts.pdf">Fact Sheet Summary</a>
            <a href="/display.pdf">Display Copy</a>
            <a href="/comments.xlsx">Response to Comments</a>
            <a href="/data.zip">Other Data</a>
            <a href="/page">Not a download</a>
        </body></html>
        """
        downloads = PFSHandler.extract_document_downloads(html, "https://cms.gov/pfs/")
        assert len(downloads) == 6
        types = [d["doc_type"] for d in downloads]
        assert "final_rule" in types
        assert "proposed_rule" in types
        assert "fact_sheet" in types
        assert "display_copy" in types
        assert "comments" in types
        assert "other" in types

    @pytest.mark.unit
    def test_extract_document_downloads_regulation_in_title(self):
        from acoharmony._cite.connectors._cms import PFSHandler

        html = '<html><body><a href="/reg.pdf">Regulation Notice</a></body></html>'
        downloads = PFSHandler.extract_document_downloads(html, "https://cms.gov/")
        assert downloads[0]["doc_type"] == "final_rule"

    @pytest.mark.unit
    def test_extract_document_downloads_empty_title(self):
        from acoharmony._cite.connectors._cms import PFSHandler

        html = '<html><body><li>Parent context <a href="/doc.pdf"></a></li></body></html>'
        downloads = PFSHandler.extract_document_downloads(html, "https://cms.gov/")
        assert len(downloads) == 1
        assert "Parent context" in downloads[0]["title"]

    @pytest.mark.unit
    def test_process(self, tmp_path):
        from acoharmony._cite.connectors._cms import PFSHandler

        html = """
        <html><body>
            <p>2024 Physician Fee Schedule Final Rule CMS-1807-F</p>
            <a href="/rule.pdf">Final Rule</a>
            <a href="/comment.pdf">Response to Comments</a>
        </body></html>
        """
        html_path = _write_html(tmp_path, html)
        base = _make_base_citation()
        results = PFSHandler.process(
            "https://cms.gov/physician/federal-regulation-notices/2024", html_path, base
        )
        assert len(results) == 3
        parent = results[0]
        assert parent["author"][0] == "CMS"
        assert parent["citation_type"][0] == "cms_pfs_regulation"
        assert parent["is_parent_citation"][0] is True
        assert parent["regulation_year"][0] == "2024"

        child = results[1]
        assert child["citation_type"][0] == "cms_pfs_document"
        assert child["is_parent_citation"][0] is False
        assert "CMS-1807-F" in child["title"][0]

    @pytest.mark.unit
    def test_process_no_reg_number(self, tmp_path):
        from acoharmony._cite.connectors._cms import PFSHandler

        html = """
        <html><body>
            <a href="/doc.pdf">Some Document</a>
        </body></html>
        """
        html_path = _write_html(tmp_path, html)
        base = _make_base_citation()
        results = PFSHandler.process("https://cms.gov/fee-schedules/physician/x", html_path, base)
        child = results[1]
        # No reg number -> title is just doc title
        assert child["title"][0] == "Some Document"


# ===========================================================================
# 7. CMSConnector tests
# ===========================================================================


class TestCMSConnector:
    @pytest.mark.unit
    def test_can_handle_iom(self):
        from acoharmony._cite.connectors._cms import CMSConnector

        assert CMSConnector.can_handle("https://cms.gov/manual/chapter1") is True

    @pytest.mark.unit
    def test_can_handle_pfs(self):
        from acoharmony._cite.connectors._cms import CMSConnector

        assert (
            CMSConnector.can_handle("https://cms.gov/physician/federal-regulation-notices/2024")
            is True
        )

    @pytest.mark.unit
    def test_can_handle_false(self):
        from acoharmony._cite.connectors._cms import CMSConnector

        assert CMSConnector.can_handle("https://example.com/unrelated") is False

    @pytest.mark.unit
    def test_process_routes_to_iom(self, tmp_path):
        from acoharmony._cite.connectors._cms import CMSConnector

        html = "<html><body><p>Pub 100-04</p></body></html>"
        html_path = _write_html(tmp_path, html)
        base = _make_base_citation()
        results = CMSConnector.process("https://cms.gov/manual/chapter", html_path, base)
        assert results is not None
        assert len(results) >= 1
        assert results[0]["citation_type"][0] == "cms_iom"

    @pytest.mark.unit
    def test_process_routes_to_pfs(self, tmp_path):
        from acoharmony._cite.connectors._cms import CMSConnector

        html = "<html><body><p>2024 Physician Fee</p></body></html>"
        html_path = _write_html(tmp_path, html)
        base = _make_base_citation()
        results = CMSConnector.process(
            "https://cms.gov/physician/federal-regulation-notices/2024", html_path, base
        )
        assert results is not None
        assert results[0]["citation_type"][0] == "cms_pfs_regulation"

    @pytest.mark.unit
    def test_process_no_handler(self, tmp_path):
        from acoharmony._cite.connectors._cms import CMSConnector

        html_path = _write_html(tmp_path, "<html></html>")
        base = _make_base_citation()
        result = CMSConnector.process("https://example.com/unknown", html_path, base)
        assert result is None


# ===========================================================================
# 5. IOMHandler tests
# ===========================================================================


class TestIOMHandler:
    @pytest.mark.unit
    def test_can_handle_positive(self):
        from acoharmony._cite.connectors._cms import IOMHandler

        assert IOMHandler.can_handle("https://www.cms.gov/manual/chapter1") is True
        assert IOMHandler.can_handle("https://www.cms.gov/iom/stuff") is True
        assert IOMHandler.can_handle("https://www.cms.gov/internet-only-manual/x") is True
        assert IOMHandler.can_handle("https://www.cms.gov/cms018912/docs") is True

    @pytest.mark.unit
    def test_can_handle_negative(self):
        from acoharmony._cite.connectors._cms import IOMHandler

        assert IOMHandler.can_handle("https://www.cms.gov/physician/fees") is False
        assert IOMHandler.can_handle("https://example.com/manual") is False

    @pytest.mark.unit
    def test_extract_publication_number_from_url(self):
        from acoharmony._cite.connectors._cms import IOMHandler

        result = IOMHandler.extract_publication_number(
            "https://www.cms.gov/cms018912/chapter1", "<html></html>"
        )
        assert result == "Pub 100-04"

    @pytest.mark.unit
    def test_extract_publication_number_from_url_other_codes(self):
        from acoharmony._cite.connectors._cms import IOMHandler

        assert (
            IOMHandler.extract_publication_number("https://cms.gov/cms018913/x", "<html></html>")
            == "Pub 100-02"
        )
        assert (
            IOMHandler.extract_publication_number("https://cms.gov/cms018915/x", "<html></html>")
            == "Pub 100-01"
        )
        assert (
            IOMHandler.extract_publication_number("https://cms.gov/cms019033/x", "<html></html>")
            == "Pub 100-08"
        )

    @pytest.mark.unit
    def test_extract_publication_number_unknown_cms_code(self):
        from acoharmony._cite.connectors._cms import IOMHandler

        # Unknown CMS code falls through to page content
        result = IOMHandler.extract_publication_number(
            "https://cms.gov/cms099999/x", "<html><body>No pub info</body></html>"
        )
        assert result == ""

    @pytest.mark.unit
    def test_extract_publication_number_from_content(self):
        from acoharmony._cite.connectors._cms import IOMHandler

        html = "<html><body>This is Publication 100-04 manual.</body></html>"
        result = IOMHandler.extract_publication_number("https://cms.gov/docs", html)
        assert result == "Pub 100-04"

    @pytest.mark.unit
    def test_extract_publication_number_from_content_cms_pub(self):
        from acoharmony._cite.connectors._cms import IOMHandler

        html = "<html><body>CMS Pub. 100-02 Reference</body></html>"
        result = IOMHandler.extract_publication_number("https://cms.gov/docs", html)
        assert result == "Pub 100-02"

    @pytest.mark.unit
    def test_extract_publication_number_from_content_pub_number(self):
        from acoharmony._cite.connectors._cms import IOMHandler

        html = "<html><body>Publication Number: 100-08</body></html>"
        result = IOMHandler.extract_publication_number("https://cms.gov/docs", html)
        assert result == "Pub 100-08"

    @pytest.mark.unit
    def test_extract_publication_number_not_found(self):
        from acoharmony._cite.connectors._cms import IOMHandler

        result = IOMHandler.extract_publication_number("https://cms.gov/docs", "<html></html>")
        assert result == ""

    @pytest.mark.unit
    def test_extract_chapter_downloads(self):
        from acoharmony._cite.connectors._cms import IOMHandler

        html = """
        <html><body>
            <a href="/docs/chapter1.pdf">Chapter 1 Introduction</a>
            <a href="chapter2.zip">Chapter 2 Billing</a>
            <a href="/page">Not a download</a>
            <a href="/docs/appendix.docx">Appendix A</a>
        </body></html>
        """
        downloads = IOMHandler.extract_chapter_downloads(html, "https://cms.gov/manual/")
        assert len(downloads) == 3
        assert downloads[0]["title"] == "Chapter 1 Introduction"
        assert downloads[0]["chapter_num"] == "1"
        assert downloads[0]["url"] == "https://cms.gov/docs/chapter1.pdf"
        assert downloads[1]["chapter_num"] == "2"
        assert downloads[2]["chapter_num"] == ""  # Appendix has no chapter number

    @pytest.mark.unit
    def test_extract_chapter_downloads_empty_title_uses_parent(self):
        from acoharmony._cite.connectors._cms import IOMHandler

        html = """
        <html><body>
            <li>Context text for link <a href="/file.pdf"></a></li>
        </body></html>
        """
        downloads = IOMHandler.extract_chapter_downloads(html, "https://cms.gov/")
        assert len(downloads) == 1
        assert "Context text" in downloads[0]["title"]

    @pytest.mark.unit
    def test_extract_chapter_downloads_empty_title_no_parent(self):
        from acoharmony._cite.connectors._cms import IOMHandler

        html = """<html><body><a href="/file.pdf"></a></body></html>"""
        downloads = IOMHandler.extract_chapter_downloads(html, "https://cms.gov/")
        assert len(downloads) == 1
        # title may be empty string
        assert downloads[0]["url"] == "https://cms.gov/file.pdf"

    @pytest.mark.unit
    def test_process(self, tmp_path):
        from acoharmony._cite.connectors._cms import IOMHandler

        html = """
        <html><body>
            <p>Publication 100-04 Manual</p>
            <a href="/ch1.pdf">Chapter 1 Overview</a>
            <a href="/ch2.pdf">Chapter 2 Details</a>
        </body></html>
        """
        html_path = _write_html(tmp_path, html)
        base = _make_base_citation()
        results = IOMHandler.process("https://cms.gov/cms018912/manual", html_path, base)
        assert len(results) == 3  # 1 parent + 2 chapters
        parent = results[0]
        assert parent["author"][0] == "CMS"
        assert parent["citation_type"][0] == "cms_iom"
        assert parent["is_parent_citation"][0] is True
        assert parent["child_count"][0] == 2

        child1 = results[1]
        assert child1["citation_type"][0] == "cms_iom_chapter"
        assert child1["is_parent_citation"][0] is False
        assert child1["child_sequence"][0] == 1
        assert "Chapter 1" in child1["title"][0]

    @pytest.mark.unit
    def test_process_no_chapters(self, tmp_path):
        from acoharmony._cite.connectors._cms import IOMHandler

        html = "<html><body><p>No links here</p></body></html>"
        html_path = _write_html(tmp_path, html)
        base = _make_base_citation()
        results = IOMHandler.process("https://cms.gov/iom/empty", html_path, base)
        assert len(results) == 1  # parent only

    @pytest.mark.unit
    def test_process_chapter_title_with_pub_no_chapter_num(self, tmp_path):
        from acoharmony._cite.connectors._cms import IOMHandler

        html = """
        <html><body>
            <p>Publication 100-04 Manual</p>
            <a href="/appendix.pdf">Appendix A</a>
        </body></html>
        """
        html_path = _write_html(tmp_path, html)
        base = _make_base_citation()
        results = IOMHandler.process("https://cms.gov/manual/docs", html_path, base)
        # chapter has pub_number but no chapter_num -> "Pub 100-04 - Appendix A"
        child = results[1]
        assert "Pub 100-04 - Appendix A" in child["title"][0]

    @pytest.mark.unit
    def test_process_chapter_title_no_pub(self, tmp_path):
        from acoharmony._cite.connectors._cms import IOMHandler

        html = """
        <html><body>
            <a href="/ch1.pdf">Chapter 1 Overview</a>
        </body></html>
        """
        html_path = _write_html(tmp_path, html)
        base = _make_base_citation()
        results = IOMHandler.process("https://cms.gov/iom/docs", html_path, base)
        child = results[1]
        assert child["title"][0] == "Chapter 1 Overview"
