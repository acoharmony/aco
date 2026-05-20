# © 2025 HarmonyCares – Shared fixtures for _runner tests
"""Pytest fixtures for acoharmony._runner tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from acoharmony._runner._schema_transformer import SchemaTransformer

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def storage_config(tmp_path):
    """Mock storage config."""
    mock = MagicMock()
    silver_path = tmp_path / "silver"
    silver_path.mkdir()
    bronze_path = tmp_path / "bronze"
    bronze_path.mkdir()
    mock.get_path.side_effect = lambda tier: {
        "silver": silver_path,
        "bronze": bronze_path,
    }.get(tier, tmp_path)
    return mock


@pytest.fixture
def catalog():
    """Mock catalog."""
    return MagicMock()


@pytest.fixture
def logger():
    """Mock logger."""
    return MagicMock()


@pytest.fixture
def transformer(storage_config, catalog, logger):
    """Create a SchemaTransformer instance."""
    return SchemaTransformer(storage_config, catalog, logger)
