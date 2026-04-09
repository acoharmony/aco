"""Pytest configuration for tracing tests."""


import pytest

from acoharmony._trace import shutdown_tracing


@pytest.fixture(autouse=True)
def cleanup_tracing():
    """Automatically cleanup tracing after each test."""
    yield
    # Cleanup after test
    shutdown_tracing()
