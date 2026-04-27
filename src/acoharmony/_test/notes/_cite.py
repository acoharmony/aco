# © 2025 HarmonyCares
"""Tests for acoharmony._notes._cite (CitePlugins)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from acoharmony._notes import CitePlugins, PanelPlugins, UIPlugins


def _row(**kw):
    """Build a fake CiteStateTracker row."""
    defaults = dict(
        source_path=Path("/x.json"),
        source_type="json",
        record_count=1,
        process_timestamp=datetime(2024, 1, 1, 12, 0),
        metadata={},
    )
    defaults.update(kw)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# state_summary / state_dataframe
# ---------------------------------------------------------------------------


class TestStateSummary:
    @pytest.mark.unit
    def test_empty(self) -> None:
        out = CitePlugins().state_summary([])
        assert out["total"] == 0
        assert out["unique_types"] == 0

    @pytest.mark.unit
    def test_collects_metadata(self) -> None:
        files = [
            _row(metadata={"citation_type": "cms", "url_domain": "cms.gov", "tags": ["a"], "note": "n1"}),
            _row(metadata={"citation_type": "cms", "url_domain": "cms.gov", "tags": ["b", "c"]}),
            _row(metadata={"citation_type": "fr", "url_domain": "federalregister.gov"}),
        ]
        out = CitePlugins().state_summary(files)
        assert out["total"] == 3
        assert out["unique_types"] == 2
        assert out["unique_domains"] == 2
        assert out["tagged_count"] == 3  # ["a","b","c"]
        assert out["note_count"] == 1

    @pytest.mark.unit
    def test_handles_none_metadata(self) -> None:
        files = [_row(metadata=None)]
        out = CitePlugins().state_summary(files)
        assert out["citation_types"] == ["unknown"]
        assert out["domains"] == ["unknown"]


class TestStateDataframe:
    @pytest.mark.unit
    def test_records_built_with_defaults(self) -> None:
        files = [
            _row(
                metadata={
                    "url": "u",
                    "url_domain": "d",
                    "title": "t",
                    "citation_type": "ct",
                    "doi": "doi",
                    "tags": ["x", "y"],
                    "note": "nn",
                },
                record_count=42,
                process_timestamp=datetime(2024, 6, 1),
            ),
            _row(metadata=None, process_timestamp=None),
        ]
        out = CitePlugins().state_dataframe(files)
        assert out.height == 2
        first = out.row(0, named=True)
        assert first["url"] == "u"
        assert first["tags"] == "x, y"
        assert first["record_count"] == 42
        assert first["processed_at"] == "2024-06-01T00:00:00"
        second = out.row(1, named=True)
        assert second["processed_at"] == ""
        assert second["tags"] == ""


# ---------------------------------------------------------------------------
# rollup helpers
# ---------------------------------------------------------------------------


def _citations_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "url_domain": ["a", "a", "b"],
            "citation_type": ["t1", "t2", "t1"],
            "title": ["A", "B", "C"],
            "tags": ["x", "", "y"],
            "note": ["", "n", ""],
            "processed_at": ["2024-03-01", "2024-01-01", "2024-02-01"],
        }
    )


class TestRollups:
    @pytest.mark.unit
    def test_by_type(self) -> None:
        out = CitePlugins().by_type(_citations_df())
        assert out.height == 2
        # t1 has count 2, t2 has count 1 → sorted desc
        assert out["citation_type"][0] == "t1"

    @pytest.mark.unit
    def test_by_domain(self) -> None:
        out = CitePlugins().by_domain(_citations_df())
        assert out.row(0, named=True)["url_domain"] == "a"
        assert out.row(0, named=True)["count"] == 2

    @pytest.mark.unit
    def test_recent(self) -> None:
        out = CitePlugins().recent(_citations_df(), limit=2)
        assert out.height == 2
        assert out["processed_at"][0] == "2024-03-01"

    @pytest.mark.unit
    def test_tagged_only(self) -> None:
        out = CitePlugins().tagged_only(_citations_df())
        assert out.height == 2
        assert "" not in out["tags"].to_list()


# ---------------------------------------------------------------------------
# master corpus + Federal Register grouping
# ---------------------------------------------------------------------------


def _full_corpus() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "url_hash": ["h1", "h2", "h3"],
            "citation_type": ["federal_register", "federal_register", "cms"],
            "document_number": ["d1", "d1", None],
            "is_parent_citation": [True, False, False],
            "title": ["Parent", "Child", "Other"],
            "html_content": ["<p/>", "<p/>", None],
            "text_content": ["t", None, None],
        }
    )


class TestLoadMasterCorpus:
    @pytest.mark.unit
    def test_missing_file(self, tmp_path: Path) -> None:
        editor, full = CitePlugins().load_master_corpus(tmp_path / "nope.parquet")
        assert editor.is_empty()
        assert full.is_empty()

    @pytest.mark.unit
    def test_loads_and_strips_content_cols(self, tmp_path: Path) -> None:
        path = tmp_path / "corpus.parquet"
        _full_corpus().write_parquet(path)
        editor, full = CitePlugins().load_master_corpus(path)
        assert editor.height == 3
        assert "html_content" not in editor.columns
        assert "text_content" not in editor.columns
        assert "html_content" in full.columns


class TestFederalRegister:
    @pytest.mark.unit
    def test_filters_to_fr(self) -> None:
        out = CitePlugins().federal_register_only(_full_corpus())
        assert out.height == 2

    @pytest.mark.unit
    def test_empty_corpus(self) -> None:
        assert CitePlugins().federal_register_only(pl.DataFrame()).is_empty()

    @pytest.mark.unit
    def test_no_citation_type_column(self) -> None:
        df = pl.DataFrame({"x": [1]})
        assert CitePlugins().federal_register_only(df).is_empty()

    @pytest.mark.unit
    def test_grouping_splits_parent_children(self) -> None:
        fr = CitePlugins().federal_register_only(_full_corpus())
        grouped = CitePlugins().group_by_final_rule(fr)
        assert "d1" in grouped
        assert grouped["d1"]["parent"].height == 1
        assert grouped["d1"]["children"].height == 1
        assert grouped["d1"]["total_count"] == 2

    @pytest.mark.unit
    def test_grouping_empty(self) -> None:
        assert CitePlugins().group_by_final_rule(pl.DataFrame()) == {}

    @pytest.mark.unit
    def test_grouping_no_doc_number_column(self) -> None:
        assert CitePlugins().group_by_final_rule(pl.DataFrame({"x": [1]})) == {}

    @pytest.mark.unit
    def test_grouping_sorts_by_paragraph_when_present(self) -> None:
        df = pl.DataFrame(
            {
                "document_number": ["d1", "d1", "d1"],
                "is_parent_citation": [True, False, False],
                "paragraph_number": [None, "2", "1"],
            }
        )
        grouped = CitePlugins().group_by_final_rule(df)
        assert grouped["d1"]["children"]["paragraph_number"].to_list() == ["1", "2"]

    @pytest.mark.unit
    def test_grouping_sorts_by_source_url_when_no_paragraph(self) -> None:
        df = pl.DataFrame(
            {
                "document_number": ["d1", "d1", "d1"],
                "is_parent_citation": [True, False, False],
                "source_url": [None, "u_b", "u_a"],
            }
        )
        grouped = CitePlugins().group_by_final_rule(df)
        assert grouped["d1"]["children"]["source_url"].to_list() == ["u_a", "u_b"]


class TestLookupFullRecord:
    @pytest.mark.unit
    def test_returns_match(self) -> None:
        out = CitePlugins().lookup_full_record(_full_corpus(), "h2")
        assert out.height == 1

    @pytest.mark.unit
    def test_empty_when_no_match(self) -> None:
        out = CitePlugins().lookup_full_record(_full_corpus(), "nope")
        assert out.is_empty()

    @pytest.mark.unit
    def test_none_url_hash(self) -> None:
        assert CitePlugins().lookup_full_record(_full_corpus(), None).is_empty()

    @pytest.mark.unit
    def test_empty_corpus(self) -> None:
        assert CitePlugins().lookup_full_record(pl.DataFrame(), "h1").is_empty()


class TestReplaceRecord:
    @pytest.mark.unit
    def test_splices_metadata_keeps_content(self) -> None:
        full = _full_corpus()
        edited = pl.DataFrame(
            {
                "url_hash": ["h1"],
                "citation_type": ["federal_register"],
                "document_number": ["d1"],
                "is_parent_citation": [True],
                "title": ["Parent EDITED"],
            }
        )
        out = CitePlugins().replace_record(full, "h1", edited)
        assert out.height == 3
        h1_row = out.filter(pl.col("url_hash") == "h1").row(0, named=True)
        assert h1_row["title"] == "Parent EDITED"
        # Content column preserved
        assert h1_row["html_content"] == "<p/>"

    @pytest.mark.unit
    def test_no_url_hash_column_returns_full_unchanged(self) -> None:
        df = pl.DataFrame({"x": [1, 2]})
        out = CitePlugins().replace_record(df, "any", pl.DataFrame({"x": [3]}))
        assert out.equals(df)

    @pytest.mark.unit
    def test_missing_original_content(self) -> None:
        full = pl.DataFrame(
            {
                "url_hash": ["h1"],
                "title": ["Parent"],
            }
        )
        edited = pl.DataFrame({"url_hash": ["h2"], "title": ["NEW"]})
        out = CitePlugins().replace_record(full, "h2", edited)
        # h2 didn't exist; out concatenates: original h1 row + edited h2 row
        assert out.height == 2
        assert "NEW" in out["title"].to_list()


# ---------------------------------------------------------------------------
# Panels
# ---------------------------------------------------------------------------


@pytest.fixture
def panel_with_mock_mo():
    from unittest.mock import MagicMock

    ui = UIPlugins(); ui._mo = MagicMock()
    p = PanelPlugins(ui); p._mo = ui._mo
    return p


class TestCitePanels:
    @pytest.mark.unit
    def test_summary_cards_calls_summary_cards(self, panel_with_mock_mo):
        summary = {
            "total": 10,
            "unique_types": 3,
            "unique_domains": 2,
            "tagged_count": 5,
            "note_count": 1,
        }
        panel_with_mock_mo.cite_summary_cards(summary)
        # ui.summary_cards renders via mo.md
        panel_with_mock_mo._mo.md.assert_called_once()

    @pytest.mark.unit
    def test_table_section_returns_vstack(self, panel_with_mock_mo):
        panel_with_mock_mo.cite_table_section(
            pl.DataFrame({"a": [1]}), "## T", "lbl"
        )
        panel_with_mock_mo._mo.vstack.assert_called_once()
        panel_with_mock_mo._mo.ui.table.assert_called_once()

    @pytest.mark.unit
    def test_federal_register_panel_empty(self, panel_with_mock_mo):
        panel_with_mock_mo.cite_federal_register_panel({})
        # Renders the warning markdown
        panel_with_mock_mo._mo.md.assert_called_once()
        assert "No Federal Register" in panel_with_mock_mo._mo.md.call_args.args[0]

    @pytest.mark.unit
    def test_federal_register_panel_with_data(self, panel_with_mock_mo):
        parent = pl.DataFrame(
            {
                "title": ["P"],
                "author": ["A"],
                "publication_date": ["2024-01-01"],
                "document_citation": ["c"],
                "document_type": ["Final Rule"],
                "cfr_references": ["42 CFR"],
                "html_url": ["http://h"],
                "pdf_url": ["http://p"],
            }
        )
        panel_with_mock_mo.cite_federal_register_panel(
            {"d1": {"parent": parent, "children": pl.DataFrame(), "total_count": 1}}
        )
        # Outer vstack + at least one md call inside
        panel_with_mock_mo._mo.vstack.assert_called_once()
        assert panel_with_mock_mo._mo.md.call_count >= 2

    @pytest.mark.unit
    def test_federal_register_panel_skips_empty_parents(self, panel_with_mock_mo):
        panel_with_mock_mo.cite_federal_register_panel(
            {
                "d1": {
                    "parent": pl.DataFrame(),
                    "children": pl.DataFrame(),
                    "total_count": 0,
                }
            }
        )
        # Only the header gets rendered
        panel_with_mock_mo._mo.vstack.assert_called_once()
        # Exactly one md call (the header)
        assert panel_with_mock_mo._mo.md.call_count == 1

    @pytest.mark.unit
    def test_usage_panel(self, panel_with_mock_mo):
        panel_with_mock_mo.cite_usage_panel()
        panel_with_mock_mo._mo.md.assert_called_once()
        assert "How to Add Citations" in panel_with_mock_mo._mo.md.call_args.args[0]
