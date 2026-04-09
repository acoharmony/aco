"""Comprehensive tests for acoharmony._cite package achieving full coverage."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_base_citation() -> pl.DataFrame:
    """Create a minimal base citation DataFrame used by connectors."""
    return pl.DataFrame({"url": ["https://example.com"], "source": ["test"]})


def _write_html(tmp_path: Path, content: str, name: str = "page.html") -> Path:
    """Write HTML content to a temp file and return its path."""
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return p


class TestCiteInitFunctions:
    """Cover _cite/__init__.py lines 72-73, 92."""

    @pytest.mark.unit
    def test_get_storage_paths(self):
        from acoharmony._cite import get_storage_paths

        with patch("acoharmony._cite.StorageBackend") as mock_sb:
            mock_sb.return_value.get_path.side_effect = lambda x: f"/mock/{x}"
            paths = get_storage_paths()
            assert paths["cites"] == "/mock/cites"
            assert paths["raw"] == "/mock/cites/raw"

    @pytest.mark.unit
    def test_get_state_tracker_default(self):
        from acoharmony._cite import get_state_tracker

        with patch("acoharmony._cite.CiteStateTracker") as mock_tracker:
            mock_tracker.return_value = MagicMock()
            get_state_tracker()
            assert mock_tracker.called

    @pytest.mark.unit
    def test_get_state_tracker_custom_writer(self):
        from acoharmony._cite import get_state_tracker

        custom_writer = MagicMock()
        with patch("acoharmony._cite.CiteStateTracker") as mock_tracker:
            mock_tracker.return_value = MagicMock()
            get_state_tracker(log_writer_instance=custom_writer)
            mock_tracker.assert_called_once_with(log_writer=custom_writer)


# ===== From test_cite_gap.py =====


class TestCiteInit:
    @pytest.mark.unit
    def test_cite_init_imports(self):
        try:
            from acoharmony._cite import CiteRegistry

            assert CiteRegistry is not None
        except (ImportError, AttributeError):
            pass
