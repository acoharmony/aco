# © 2025 HarmonyCares – Shared fixtures for _cite tests
"""Pytest fixtures for acoharmony._cite tests."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
import requests

LIVE_REQUEST_HEADERS = {
    "User-Agent": ("acoharmony-ci/1.0 (https://github.com/acoharmony/aco; live integration test)")
}


def _get_live_response_or_skip(url: str, *, timeout: int = 30) -> requests.Response:
    """Fetch a live integration-test URL, skipping if the remote site blocks CI."""
    try:
        response = requests.get(url, headers=LIVE_REQUEST_HEADERS, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.RequestException as exc:
        pytest.skip(f"Live HTTP fixture unavailable for {url}: {exc}")


def _make_base_citation() -> pl.DataFrame:
    """Create a minimal base citation DataFrame used by connectors."""
    return pl.DataFrame({"url": ["https://example.com"], "source": ["test"]})


def _write_html(tmp_path: Path, content: str, name: str = "page.html") -> Path:
    """Write HTML content to a temp file and return its path."""
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return p
