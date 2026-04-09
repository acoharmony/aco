# © 2025 HarmonyCares – Shared fixtures for _cite tests
"""Pytest fixtures for acoharmony._cite tests."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def _make_base_citation() -> pl.DataFrame:
    """Create a minimal base citation DataFrame used by connectors."""
    return pl.DataFrame({"url": ["https://example.com"], "source": ["test"]})


def _write_html(tmp_path: Path, content: str, name: str = "page.html") -> Path:
    """Write HTML content to a temp file and return its path."""
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return p
