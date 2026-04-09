"""Tests for acoharmony._cite.connectors._ecfr module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

import acoharmony
from acoharmony._cite.connectors._ecfr import ECFRConnector


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._cite.connectors._ecfr is not None


# ---------------------------------------------------------------------------
# Coverage gap tests: _ecfr.py lines 277→280, 307→312
# ---------------------------------------------------------------------------


class TestProcessBranchNoSectionNoPart:
    """Cover line 277→280: elif part is falsy, so parent_title stays '{title} CFR'."""

    @pytest.mark.unit
    @patch.object(ECFRConnector, "get_latest_structure", return_value={})
    @patch.object(ECFRConnector, "parse_url")
    def test_process_title_only_no_section_no_part(self, mock_parse, mock_struct, tmp_path):
        """When URL has title but no section and no part, parent_title = '{title} CFR'."""
        mock_parse.return_value = {
            "title": "42",
            "section": None,
            "part": None,
            "date": None,
        }

        html_path = tmp_path / "cites" / "raw" / "html" / "page.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text("<html></html>")

        base_citation = pl.DataFrame({"url": ["https://ecfr.gov/current/title-42"], "source": ["test"]})

        result = ECFRConnector.process(
            url="https://www.ecfr.gov/current/title-42",
            html_path=html_path,
            base_citation=base_citation,
        )

        assert result is not None
        assert len(result) == 1
        parent_df = result[0]
        title_col = parent_df["title"].to_list()
        assert title_col[0] == "42 CFR"


class TestProcessBranchDateAlreadySet:
    """Cover line 307→312: when section is set AND date is already truthy, skip get_latest_date."""

    @pytest.mark.unit
    @patch("acoharmony._cite.connectors._ecfr.extract_section_by_number", return_value=None)
    @patch.object(ECFRConnector, "download_xml", return_value=True)
    @patch.object(ECFRConnector, "construct_xml_url", return_value="https://ecfr.gov/full/2024-01-01/title-42.xml")
    @patch.object(ECFRConnector, "get_latest_structure", return_value={})
    @patch.object(ECFRConnector, "parse_url")
    def test_process_section_with_date_skips_get_latest_date(
        self, mock_parse, mock_struct, mock_xml_url, mock_download, mock_extract, tmp_path
    ):
        """When URL has section AND date, the 'if not date' branch is skipped (307→312)."""
        mock_parse.return_value = {
            "title": "42",
            "section": "414.2",
            "part": "414",
            "date": "2024-01-01",
        }

        html_path = tmp_path / "cites" / "raw" / "html" / "page.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text("<html></html>")

        base_citation = pl.DataFrame({"url": ["https://ecfr.gov/on/2024-01-01/title-42/section-414.2"], "source": ["test"]})

        with patch.object(ECFRConnector, "get_latest_date") as mock_latest:
            result = ECFRConnector.process(
                url="https://www.ecfr.gov/on/2024-01-01/title-42/section-414.2",
                html_path=html_path,
                base_citation=base_citation,
            )
            # get_latest_date should NOT have been called because date was already set
            mock_latest.assert_not_called()

        assert result is not None
        assert len(result) >= 1
