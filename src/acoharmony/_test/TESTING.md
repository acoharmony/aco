# Testing Guide

## Overview

`acoharmony` uses a **tiered testing strategy** for speed and maintainability:

1. **Unit Tests** (seconds) - Fast tests with inline data, marked `@pytest.mark.unit`
2. **Integration Tests** (seconds) - Tests using fixtures, marked `@pytest.mark.integration`
3. **Coverage Gap Tests** (minutes) - Auto-generated tests in `tests/_coverage/` (run on CI)

This ensures:
- ✅ Fast development cycle (<10 seconds for unit tests)
- ✅ Realistic integration testing (fixtures with 1000 rows)
- ✅ High code coverage (gap tests run on CI)
- ✅ No production data dependencies
- ✅ Memory-safe (no large datasets)

## Quick Start

### Run Tests (Fast)

```bash
# Unit tests only (default, seconds)
pytest

# Unit + Integration tests
pytest -m "unit or integration"

# Parallel (even faster)
pytest -n auto
```

### Run Coverage Tests (Slow)

```bash
# Auto-generated gap tests (run on CI)
pytest tests/_coverage/
```

See `tests/EXAMPLES.md` for code examples.

## Fixture Architecture

### Fixture Location

Fixtures are stored at: `/opt/s3/data/workspace/logs/dev/fixtures/`

```
fixtures/
├── bronze/          # 9 tables (PLARU data)
├── silver/          # 199 tables (CCLF, intermediate, reference)
├── gold/            # 11 tables (final analytical tables)
└── schemas.json     # Metadata about fixtures
```

### DuckDB Test Database

A DuckDB database at `/opt/s3/data/workspace/logs/dev/test.duckdb` provides SQL access to all fixtures:

**Schemas:**
- `fixtures` - All fixtures combined (219 tables)
- `bronze` - Bronze layer (9 tables)
- `silver` - Silver layer (199 tables)
- `gold` - Gold layer (11 tables)

## How to Use Fixtures in Tests

### Method 1: Using `load_fixture` (Recommended for Single Tables)

```python
def test_cclf1_processing(load_fixture):
    # Load silver layer fixture (default)
    df = load_fixture("cclf1")

    # Load from specific layer
    bronze_df = load_fixture("plaru_meta", layer="bronze")
    gold_df = load_fixture("medical_claim", layer="gold")

    # Lazy load for performance
    lf = load_fixture("beneficiary_demographics", lazy=True)
    result = lf.filter(pl.col("state") == "CA").collect()

    assert len(df) > 0
```

### Method 2: Using Pre-defined Fixtures (Fastest)

```python
def test_bar_processing(fixture_bar):
    # fixture_bar is pre-loaded from conftest.py
    assert "bene_mbi_id" in fixture_bar.columns
    assert len(fixture_bar) == 1000
```

**Available pre-defined fixtures:**
- `fixture_cclf1` - CCLF1 Part A claims header
- `fixture_cclf8` - CCLF8 beneficiary demographics
- `fixture_bar` - Beneficiary Alignment Roster
- `fixture_alr` - Alignment List Report
- `fixture_beneficiary_demographics` - Beneficiary demographics
- `fixture_medical_claim` - Medical claims (gold)
- `fixture_pharmacy_claim` - Pharmacy claims (gold)
- `fixture_eligibility` - Eligibility (gold)

### Method 3: Using `query_fixture` (SQL Access)

```python
def test_join_logic(query_fixture):
    # Query with SQL
    result = query_fixture("""
        SELECT b.bene_mbi_id, COUNT(*) as claim_count
        FROM silver.beneficiary_demographics b
        JOIN silver.diagnosis d ON b.bene_mbi_id = d.bene_mbi_id
        GROUP BY b.bene_mbi_id
        LIMIT 100
    """)

    assert len(result) > 0
```


## Common Patterns

### Loading Multiple Tables

```python
def test_pipeline(load_fixture):
    cclf1 = load_fixture("cclf1")
    cclf8 = load_fixture("cclf8")
    diagnosis = load_fixture("diagnosis")

    # Your test logic here
```

### Testing Transforms

```python
def test_transform_logic(load_fixture):
    # Load input
    input_df = load_fixture("int_beneficiary_demographics_deduped")

    # Apply transform
    result = my_transform(input_df)

    # Assert
    assert len(result) > 0
    assert "expected_column" in result.columns
```

### Testing with Lazy Evaluation

```python
def test_lazy_transform(load_fixture):
    # Load lazy
    lf = load_fixture("cclf1", lazy=True)

    # Chain operations
    result = (
        lf
        .filter(pl.col("clm_from_dt") > "2024-01-01")
        .select(["bene_mbi_id", "clm_from_dt"])
        .collect()
    )

    assert len(result) > 0
```

## Fixture Management

### Generating Fixtures

```bash
# Generate all fixtures (bronze, silver, gold)
uv run aco dev generate-mocks --layers bronze silver gold --n-rows 1000

# Regenerate if they exist
uv run aco dev generate-mocks --force

# Generate specific tables only
uv run aco dev generate-mocks --tables cclf1 cclf8 bar
```

### Populating DuckDB

```bash
# Create/update DuckDB test database
uv run python -m acoharmony._dev.populate_test_duckdb --force
```

### Checking Fixture Status

```python
# In a test file
def test_fixture_availability(fixtures_dir, fixtures_db_path):
    assert fixtures_dir.exists(), "Fixtures directory not found"
    assert (fixtures_dir / "silver" / "cclf1.parquet").exists()
    assert fixtures_db_path.exists(), "DuckDB not found"
```

## Configuration

Fixture settings are defined in `pyproject.toml`:

```toml
[tool.acoharmony.fixtures]
fixtures_dir = "/opt/s3/data/workspace/logs/dev/fixtures"
duckdb_path = "/opt/s3/data/workspace/logs/dev/test.duckdb"
default_n_rows = 1000
schemas = ["fixtures", "bronze", "silver", "gold"]
```

## Test Markers

### Primary Markers (Use These)

```python
@pytest.mark.unit           # Fast, inline data (< 0.01s per test)
@pytest.mark.integration    # Uses fixtures, realistic data (< 0.1s per test)
```

### Secondary Markers

```python
@pytest.mark.slow           # Slow tests (run separately)
@pytest.mark.requires_data  # Needs external data files
```

### When to Use Each

**Unit Tests** (`@pytest.mark.unit`):
- Pure logic, expressions, calculations
- Inline `pl.DataFrame()` with 3-10 rows
- No file I/O, no external dependencies
- Example: Testing a filter expression

**Integration Tests** (`@pytest.mark.integration`):
- Multi-table operations, pipelines
- Uses `load_fixture()` or pre-defined fixtures
- Tests end-to-end workflows
- Example: Testing transform pipeline with realistic data

## Best Practices

### DO

1. ✅ **Mark all tests** with `@pytest.mark.unit` or `@pytest.mark.integration`
2. ✅ **Use inline DataFrames for unit tests** (3-10 rows, fast)
3. ✅ **Use fixtures for integration tests** (`load_fixture()`)
4. ✅ **Use lazy loading** when possible (`lazy=True`)
5. ✅ **Use real Polars DataFrames**, not MagicMock for data
6. ✅ **Keep unit tests < 0.01s**, integration tests < 0.1s

### DON'T

1. ❌ **Don't use MagicMock for DataFrames** (use real `pl.DataFrame()`)
2. ❌ **Don't create large inline DataFrames** (use fixtures for > 100 rows)
3. ❌ **Don't read production files directly** (use fixtures)
4. ❌ **Don't add tests to `tests/_coverage/`** (auto-generated only)
5. ❌ **Don't commit tests without markers**

## Troubleshooting

### Fixtures Not Found

```bash
# Generate fixtures
uv run aco dev generate-mocks --layers bronze silver gold --n-rows 1000
```

### DuckDB Not Found

```bash
# Populate DuckDB
uv run python -m acoharmony._dev.populate_test_duckdb --force
```

### Test Skipped

If you see: `"Test fixtures not available. Run 'aco dev generate-mocks'"`

This is intentional - tests gracefully skip when fixtures are missing.

### Memory Issues

Fixtures are limited to 1000 rows and use `scan_parquet` (lazy) by default. If you still have memory issues:

1. Use `lazy=True` in `load_fixture`
2. Use `.head(N)` to further limit rows
3. Use SQL with `LIMIT` via `query_fixture`

## Test Organization

```
tests/
├── _coverage/              # Auto-generated gap tests (excluded by default)
│   ├── test_parsers_gaps.py      # 12,247 import/structure tests
│   └── test_expressions_gaps.py  # 5,839 import/structure tests
├── _expressions/           # Expression unit tests (@pytest.mark.unit)
├── _transforms/            # Transform integration tests (@pytest.mark.integration)
├── _helpers/               # Test utilities (not tests)
├── conftest.py             # Shared fixtures
├── TESTING.md              # This file
└── EXAMPLES.md             # Code examples
```

## Running Different Test Suites

```bash
# Development (fast, seconds)
pytest                              # Unit tests only
pytest -m unit -n auto              # Parallel unit tests

# Pre-commit (medium, 30-60s)
pytest -m "unit or integration"     # Unit + integration

# CI/Release (full, minutes)
pytest tests/ tests/_coverage/      # Everything

# Specific modules
pytest tests/_expressions/ -m unit  # Just expression unit tests
pytest tests/_transforms/           # Just transform tests
```

## Summary

**Three-tier approach:**
1. **Unit tests** - Inline data, fast, marked `@pytest.mark.unit`
2. **Integration tests** - Fixtures, realistic, marked `@pytest.mark.integration`
3. **Coverage tests** - Auto-generated, in `tests/_coverage/`, run on CI

See `tests/EXAMPLES.md` for detailed code examples and `tests/FAST_TESTING.md` for the strategy.
