# © 2025 HarmonyCares
# All rights reserved.

"""
Pytest configuration for Tuva tests.

Provides shared fixtures and utilities.
"""

import os

import pytest

# Ignore third-party dbt test suites in _depends/
collect_ignore_glob = ["_depends/*"]


def pytest_configure(config):
    """Configure pytest with custom settings."""
    # Set ACO_PROFILE to dev for all tests
    os.environ["ACO_PROFILE"] = "dev"

    # Register markers
    config.addinivalue_line("markers", "unit: Unit tests (fast, no data loading)")
    config.addinivalue_line("markers", "small: Small integration tests (sample data)")
    config.addinivalue_line("markers", "integration: Integration tests (full data)")
    config.addinivalue_line("markers", "heavy: Heavy tests (run individually)")


@pytest.fixture(scope="session")
def storage_backend():
    """
    Get storage backend for tests.

    NOTE: This now points to test fixtures, not production data.
    """
    from acoharmony._store import StorageBackend

    return StorageBackend()


@pytest.fixture(scope="session")
def processed_data_path(fixtures_dir):
    """
    Get path to processed data - now using test fixtures.

    This fixture redirects to the fixtures/silver directory.
    """
    path = fixtures_dir / "silver"
    if not path.exists():
        pytest.skip("Test fixtures not available. Run 'aco dev generate-mocks'")
    return path


@pytest.fixture(scope="session")
def available_cclf_files(processed_data_path):
    """Get list of available CCLF fixture files."""
    files = {}
    for i in range(10):
        file_path = processed_data_path / f"cclf{i}.parquet"
        if file_path.exists():
            files[f"cclf{i}"] = file_path

    # Also check for cclfa and cclfb
    for suffix in ['a', 'b']:
        file_path = processed_data_path / f"cclf{suffix}.parquet"
        if file_path.exists():
            files[f"cclf{suffix}"] = file_path

    return files


@pytest.fixture
def sample_data_factory(load_fixture):
    """
    Factory for creating sample data with limited rows from fixtures.

    Now uses the standardized load_fixture from root conftest.py.
    """

    def _load_sample(file_name: str, n_rows: int = 100, layer: str = "silver"):
        # Remove .parquet extension if provided
        table_name = file_name.replace(".parquet", "")

        # Load fixture (already limited to 1000 rows)
        df = load_fixture(table_name, layer=layer, lazy=False)

        # Further limit if requested
        if n_rows < len(df):
            return df.head(n_rows)
        return df

    return _load_sample


@pytest.fixture
def tuva_executor(storage_backend):
    """Create a Tuva SQL executor for testing."""
    from acoharmony._tuva.executor import TuvaSQLExecutor

    return TuvaSQLExecutor(storage=storage_backend)
