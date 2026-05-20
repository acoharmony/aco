# © 2025 HarmonyCares
"""Tests for the UI helpers added to acoharmony._notes._ui."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from acoharmony._notes import UIPlugins


@pytest.fixture
def ui_with_mock_mo():
    plugin = UIPlugins()
    plugin._mo = MagicMock()
    return plugin


class TestSummaryCardHtml:
    @pytest.mark.unit
    def test_returns_html_with_inputs(self):
        plugin = UIPlugins()
        html = plugin.summary_card_html("LBL", "42", "sub", "#FF00FF")
        assert "LBL" in html
        assert "42" in html
        assert "sub" in html
        assert "#FF00FF" in html


class TestSummaryCardsRow:
    @pytest.mark.unit
    def test_renders_grid_with_n_columns(self, ui_with_mock_mo):
        ui_with_mock_mo.summary_cards_row(["<a/>", "<b/>", "<c/>"])
        ui_with_mock_mo._mo.md.assert_called_once()
        rendered = ui_with_mock_mo._mo.md.call_args.args[0]
        assert "repeat(3, 1fr)" in rendered
        assert "<a/>" in rendered and "<b/>" in rendered and "<c/>" in rendered


class TestCsvDownload:
    @pytest.mark.unit
    def test_returns_none_for_empty_or_missing(self, ui_with_mock_mo):
        assert ui_with_mock_mo.csv_download(None, "lbl", "f.csv") is None
        empty = pl.DataFrame({"a": []})
        assert ui_with_mock_mo.csv_download(empty, "lbl", "f.csv") is None

    @pytest.mark.unit
    def test_calls_mo_download_with_csv_bytes(self, ui_with_mock_mo):
        df = pl.DataFrame({"a": [1, 2]})
        ui_with_mock_mo.csv_download(df, "lbl", "f.csv")
        ui_with_mock_mo._mo.download.assert_called_once()
        kwargs = ui_with_mock_mo._mo.download.call_args.kwargs
        assert kwargs["filename"] == "f.csv"
        assert kwargs["mimetype"] == "text/csv"
        assert kwargs["label"] == "lbl"
        assert kwargs["data"].decode().startswith("a\n1")
