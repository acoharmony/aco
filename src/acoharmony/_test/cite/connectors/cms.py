"""Tests for acoharmony._cite.connectors._cms module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony
from acoharmony._cite.connectors._cms import PFSHandler


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._cite.connectors._cms is not None


class TestExtractDocumentDownloadsParentFallback:
    """Cover branch 360->364: link with no text and no parent element."""

    @pytest.mark.unit
    def test_link_no_text_no_parent_falls_through(self):
        """When a link has no text and no parent li/td/div, title stays empty
        and doc_type defaults to 'other' (branch 360->364 parent is None)."""
        html = '<html><body><a href="/doc.pdf"></a></body></html>'
        result = PFSHandler.extract_document_downloads(html, "https://cms.gov")
        assert len(result) == 1
        assert result[0]["title"] == ""
        assert result[0]["doc_type"] == "other"
        assert result[0]["url"] == "https://cms.gov/doc.pdf"

    @pytest.mark.unit
    def test_link_no_text_with_parent_gets_parent_text(self):
        """When a link has no text but has a parent li, title comes from parent."""
        html = '<html><body><li>Final Rule Document<a href="/rule.pdf"></a></li></body></html>'
        result = PFSHandler.extract_document_downloads(html, "https://cms.gov")
        assert len(result) == 1
        assert "Final Rule Document" in result[0]["title"]
        assert result[0]["doc_type"] == "final_rule"
