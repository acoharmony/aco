# © 2025 HarmonyCares
# All rights reserved.

"""
Pytest configuration and shared fixtures for acoharmony tests.

Following Polars testing conventions:
- Type annotations
- Proper fixture scoping
- Reusable test data
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import polars as pl
import pytest

if TYPE_CHECKING:
    from polars import DataFrame


# Load fixtures configuration from the packaged aco.toml
def _get_fixtures_config() -> dict:
    """Load fixtures configuration from the packaged aco.toml."""
    from acoharmony._config_loader import load_aco_config

    return load_aco_config().get("fixtures", {})


# Test data paths
@pytest.fixture(scope="session")
def test_data_root() -> Path:
    """Root directory for test data."""
    return Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    """Directory containing test fixtures (parquet files)."""
    config = _get_fixtures_config()
    return Path(config.get("fixtures_dir", "/opt/s3/data/workspace/logs/dev/fixtures"))


@pytest.fixture(scope="session")
def fixtures_db_path() -> Path:
    """Path to DuckDB test database."""
    config = _get_fixtures_config()
    return Path(config.get("duckdb_path", "/opt/s3/data/workspace/logs/dev/test.duckdb"))


@pytest.fixture(scope="session")
def fixtures_db(fixtures_db_path: Path):
    """DuckDB connection to test fixtures database."""
    if not fixtures_db_path.exists():
        pytest.skip(f"Fixtures database not found: {fixtures_db_path}. Run 'aco dev populate-test-db'")

    con = duckdb.connect(str(fixtures_db_path), read_only=True)
    yield con
    con.close()


@pytest.fixture
def load_fixture(fixtures_dir: Path):
    """
    Factory fixture to load fixture parquet files directly.

    Usage:
        def test_something(load_fixture):
            df = load_fixture("cclf1")  # Default: silver layer, eager load
            lf = load_fixture("bar", lazy=True)  # Lazy load
            bronze_df = load_fixture("plaru_meta", layer="bronze")
            gold_df = load_fixture("medical_claim", layer="gold")
    """
    def _load(table_name: str, layer: str = "silver", lazy: bool = False) -> DataFrame | pl.LazyFrame:
        fixture_path = fixtures_dir / layer / f"{table_name}.parquet"
        if not fixture_path.exists():
            pytest.skip(f"Fixture not found: {fixture_path}")

        if lazy:
            return pl.scan_parquet(fixture_path)
        return pl.scan_parquet(fixture_path).collect()

    return _load


@pytest.fixture
def query_fixture(fixtures_db):
    """
    Factory fixture to query fixture tables via DuckDB SQL.

    Usage:
        def test_something(query_fixture):
            # Query with full schema qualification
            df = query_fixture("SELECT * FROM silver.cclf1 LIMIT 100")

            # Query from fixtures schema (all tables)
            df = query_fixture("SELECT * FROM fixtures.bar WHERE year = 2024")

            # Complex queries with joins
            df = query_fixture('''
                SELECT b.*, d.diagnosis_code
                FROM silver.beneficiary_demographics b
                JOIN silver.diagnosis d ON b.bene_mbi_id = d.bene_mbi_id
                LIMIT 100
            ''')
    """
    def _query(sql: str) -> DataFrame:
        result = fixtures_db.execute(sql).pl()
        return result

    return _query


# Convenient pre-defined fixtures for commonly used tables
@pytest.fixture
def fixture_cclf1(load_fixture) -> DataFrame:
    """CCLF1 Part A claims header fixture."""
    return load_fixture("cclf1", layer="silver")


@pytest.fixture
def fixture_cclf8(load_fixture) -> DataFrame:
    """CCLF8 beneficiary demographics fixture."""
    return load_fixture("cclf8", layer="silver")


@pytest.fixture
def fixture_bar(load_fixture) -> DataFrame:
    """BAR (Beneficiary Alignment Roster) fixture."""
    return load_fixture("bar", layer="silver")


@pytest.fixture
def fixture_alr(load_fixture) -> DataFrame:
    """ALR (Alignment List Report) fixture."""
    return load_fixture("alr", layer="silver")


@pytest.fixture
def fixture_beneficiary_demographics(load_fixture) -> DataFrame:
    """Beneficiary demographics fixture."""
    return load_fixture("beneficiary_demographics", layer="silver")


@pytest.fixture
def fixture_medical_claim(load_fixture) -> DataFrame:
    """Medical claim fixture (gold layer)."""
    return load_fixture("medical_claim", layer="gold")


@pytest.fixture
def fixture_pharmacy_claim(load_fixture) -> DataFrame:
    """Pharmacy claim fixture (gold layer)."""
    return load_fixture("pharmacy_claim", layer="gold")


@pytest.fixture
def fixture_eligibility(load_fixture) -> DataFrame:
    """Eligibility fixture (gold layer)."""
    return load_fixture("eligibility", layer="gold")


@pytest.fixture
def small_sample_dataframe() -> DataFrame:
    """Small sample DataFrame for unit tests."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "city": ["NYC", "LA", "Chicago", "Houston", "Phoenix"],
        }
    )


@pytest.fixture
def sample_beneficiary_df() -> DataFrame:
    """Sample beneficiary data for testing."""
    return pl.DataFrame(
        {
            "bene_mbi_id": ["A1234", "B5678", "C9012"],
            "bene_fst_name": ["John", "Jane", "Bob"],
            "bene_lst_name": ["Doe", "Smith", "Johnson"],
            "bene_dob": ["1950-01-01", "1960-05-15", "1955-12-31"],
            "bene_zip_cd": ["12345", "54321", "67890"],
        }
    )


@pytest.fixture
def sample_claims_df() -> DataFrame:
    """Sample claims data for testing."""
    return pl.DataFrame(
        {
            "clm_id": ["CLM001", "CLM002", "CLM003"],
            "bene_mbi_id": ["A1234", "B5678", "A1234"],
            "clm_from_dt": ["2024-01-01", "2024-01-15", "2024-02-01"],
            "clm_thru_dt": ["2024-01-05", "2024-01-20", "2024-02-10"],
            "clm_tot_chrg_amt": [1000.00, 2500.00, 750.00],
        }
    )


# Configuration fixtures
@pytest.fixture
def test_config() -> dict:
    """Test configuration dictionary."""
    return {
        "profile": "local",
        "log_level": "DEBUG",
        "threads": 2,
    }


# Auto-import magic: inject module exports into test namespace
import importlib
import sys


def _get_module_under_test(test_file_path: str) -> str | None:
    """Derive the module path from the test file path.

    Examples:
        src/acoharmony/_test/expressions/registry.py -> acoharmony._expressions._registry
        src/acoharmony/_test/transforms/base.py -> acoharmony._transforms._base
    """
    path = Path(test_file_path)
    if "_test" not in path.parts:
        return None

    try:
        test_idx = path.parts.index("_test")
        submodule_parts = path.parts[test_idx + 1 : -1]
        module_name = path.stem

        # Convert: expressions/registry -> _expressions._registry
        # Special case: foureye -> _4icli
        prefixed_parts = []
        for part in submodule_parts:
            if part == "foureye":
                prefixed_parts.append("_4icli")
            else:
                prefixed_parts.append("_" + part)

        prefixed_module = "_" + module_name if module_name != "init" else ""

        module_path = "acoharmony." + ".".join(prefixed_parts)
        if prefixed_module:
            module_path += "." + prefixed_module

        return module_path
    except (ValueError, IndexError):
        return None


def pytest_collection_modifyitems(session, config, items):
    """Auto-inject exports from tested modules into test namespaces."""
    for item in items:
        test_module = sys.modules.get(item.module.__name__)
        if not test_module or not hasattr(item, "fspath"):
            continue

        module_path = _get_module_under_test(str(item.fspath))
        if not module_path:
            continue

        try:
            target_module = importlib.import_module(module_path)
            exports = [n for n in dir(target_module) if not n.startswith("_")]

            for name in exports:
                if not hasattr(test_module, name):
                    setattr(test_module, name, getattr(target_module, name))
        except Exception:
            pass  # Let test fail if module doesn't exist


# Markers for slow tests
def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "requires_data: marks tests that require real data files")
    config.addinivalue_line("markers", "auto_import: uses automatic import injection from tested module")
