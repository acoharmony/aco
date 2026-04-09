# © 2025 HarmonyCares
# All rights reserved.

"""Shared test fixtures for _transforms test modules."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def tmp_base(tmp_path: Path) -> Path:
    """Provide a base temp directory for test data."""
    return tmp_path / "data"
