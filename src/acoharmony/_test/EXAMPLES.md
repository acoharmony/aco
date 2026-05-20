# Test Examples

This guide shows how to write tests following the fast testing strategy.

## Unit Tests (Fast, Inline Data)

**When to use:** Testing pure logic, transformations, expressions, calculations

**Characteristics:**
- ✅ Inline `pl.DataFrame()` (3-10 rows)
- ✅ No file I/O
- ✅ No external dependencies
- ✅ Mark with `@pytest.mark.unit`
- ✅ Target: < 0.01s per test

### Example 1: Expression Logic

```python
import polars as pl
import pytest

@pytest.mark.unit
def test_filter_adults():
    """Filter rows where age >= 18."""
    df = pl.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [17, 25, 30]
    })

    result = df.filter(pl.col("age") >= 18)

    assert result.height == 2
    assert result["name"].to_list() == ["Bob", "Charlie"]

@pytest.mark.unit
def test_calculate_risk_score():
    """Risk score calculation with inline data."""
    from acoharmony._expressions import calculate_risk_score

    df = pl.DataFrame({
        "ip_admissions": [0, 2, 5],
        "ed_visits": [1, 3, 8],
        "chronic_conditions": [2, 4, 6]
    })

    result = calculate_risk_score(df)

    assert "risk_score" in result.columns
    assert result["risk_score"][2] > result["risk_score"][0]  # Higher utilization = higher risk
```

### Example 2: Transform Logic

```python
import polars as pl
import pytest

@pytest.mark.unit
class TestDeduplication:
    """Unit tests for deduplication logic."""

    def test_dedupe_by_key(self):
        """Deduplication removes duplicates by key."""
        df = pl.DataFrame({
            "id": [1, 2, 2, 3],
            "value": ["a", "b", "c", "d"]
        })

        result = df.unique(subset=["id"], maintain_order=True)

        assert result.height == 3
        assert result["id"].to_list() == [1, 2, 3]

    def test_dedupe_keeps_first(self):
        """Deduplication keeps first occurrence."""
        df = pl.DataFrame({
            "id": [1, 1, 1],
            "timestamp": ["2024-01-01", "2024-01-02", "2024-01-03"]
        })

        result = df.unique(subset=["id"], maintain_order=True)

        assert result.height == 1
        assert result["timestamp"][0] == "2024-01-01"
```

### Example 3: Edge Cases

```python
import polars as pl
import pytest

@pytest.mark.unit
def test_handle_empty_dataframe():
    """Function handles empty DataFrame gracefully."""
    from acoharmony._expressions import apply_filter

    empty_df = pl.DataFrame({"col1": []}, schema={"col1": pl.Int64})

    result = apply_filter(empty_df)

    assert result.height == 0
    assert "col1" in result.columns

@pytest.mark.unit
def test_handle_nulls():
    """Function handles null values correctly."""
    df = pl.DataFrame({
        "value": [1, None, 3, None, 5]
    })

    result = df.filter(pl.col("value").is_not_null())

    assert result.height == 3
```

## Integration Tests (Fixtures, Realistic Data)

**When to use:** Testing pipelines, multi-table operations, end-to-end workflows

**Characteristics:**
- ✅ Use `load_fixture()` or pre-defined fixtures
- ✅ Realistic data (100-1000 rows)
- ✅ Tests full workflows
- ✅ Mark with `@pytest.mark.integration`
- ✅ Target: < 0.1s per test

### Example 1: Using load_fixture

```python
import polars as pl
import pytest

@pytest.mark.integration
def test_transform_pipeline(load_fixture):
    """Test complete transform pipeline with realistic data."""
    from acoharmony._transforms import transform_beneficiary_demographics

    # Load fixture (1000 rows from /opt/s3/data/workspace/logs/dev/fixtures/)
    input_df = load_fixture("stg_beneficiary_demographics", layer="silver")

    result = transform_beneficiary_demographics(input_df)

    assert result.height > 0
    assert "bene_mbi_id" in result.columns
    assert "age" in result.columns
    assert result["age"].min() >= 0

@pytest.mark.integration
def test_join_multiple_tables(load_fixture):
    """Test joining multiple fixture tables."""
    cclf1 = load_fixture("cclf1", layer="silver")
    cclf8 = load_fixture("cclf8", layer="silver")

    # Join claims with demographics
    result = cclf1.join(cclf8, on="bene_mbi_id", how="left")

    assert result.height == cclf1.height
    assert "bene_dob" in result.columns  # From cclf8
```

### Example 2: Using Pre-defined Fixtures

```python
import polars as pl
import pytest

@pytest.mark.integration
def test_bar_processing(fixture_bar):
    """Test BAR processing with pre-loaded fixture."""
    # fixture_bar is pre-loaded from conftest.py

    assert fixture_bar.height == 1000  # Standard fixture size
    assert "bene_mbi_id" in fixture_bar.columns

    # Apply transformation
    result = fixture_bar.filter(pl.col("start_date") >= "2024-01-01")

    assert result.height > 0

@pytest.mark.integration
class TestAlignmentPipeline:
    """Integration tests for alignment pipeline."""

    def test_with_bar_and_alr(self, fixture_bar, fixture_alr):
        """Test alignment combining BAR and ALR."""
        from acoharmony._transforms import create_consolidated_alignment

        result = create_consolidated_alignment(fixture_bar, fixture_alr)

        assert result.height > 0
        assert "alignment_source" in result.columns
```

### Example 3: Using SQL Queries

```python
import polars as pl
import pytest

@pytest.mark.integration
def test_complex_join_via_sql(query_fixture):
    """Test complex joins using SQL on fixture database."""
    result = query_fixture("""
        SELECT
            b.bene_mbi_id,
            b.bene_dob,
            COUNT(c.clm_id) as claim_count
        FROM silver.cclf8 b
        LEFT JOIN silver.cclf1 c ON b.bene_mbi_id = c.bene_mbi_id
        GROUP BY b.bene_mbi_id, b.bene_dob
        HAVING COUNT(c.clm_id) > 0
        LIMIT 100
    """)

    assert result.height > 0
    assert "claim_count" in result.columns
```

## DO NOT Do This

### ❌ BAD: MagicMock for Data

```python
# DON'T DO THIS
from unittest.mock import MagicMock

def test_transform_bad():
    mock_df = MagicMock()
    mock_df.columns = ["col1", "col2"]
    # This doesn't test anything real!
```

### ❌ BAD: Large Inline DataFrames

```python
# DON'T DO THIS - use fixtures for large data
def test_with_huge_inline_df():
    df = pl.DataFrame({
        "col1": list(range(10000)),  # Too big for unit test!
        "col2": ["val"] * 10000
    })
```

### ❌ BAD: Reading Production Files

```python
# DON'T DO THIS - no hard-coded paths
def test_reading_prod_data():
    df = pl.read_parquet("/opt/s3/data/workspace/silver/cclf1.parquet")  # BAD!
```

## DO This Instead

### ✅ GOOD: Real Polars DataFrames

```python
@pytest.mark.unit
def test_transform_good():
    """Use real DataFrames, not mocks."""
    df = pl.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    result = my_transform(df)
    assert result.height == 2
```

### ✅ GOOD: Fixtures for Large Data

```python
@pytest.mark.integration
def test_with_fixture(load_fixture):
    """Use fixtures for realistic data."""
    df = load_fixture("cclf1")  # Loads from fixtures directory
    result = my_transform(df)
    assert result.height > 0
```

### ✅ GOOD: Lazy Loading

```python
@pytest.mark.integration
def test_lazy_processing(load_fixture):
    """Use lazy loading for better performance."""
    lf = load_fixture("cclf1", lazy=True)  # Returns LazyFrame

    result = (
        lf
        .filter(pl.col("clm_from_dt") > "2024-01-01")
        .select(["bene_mbi_id", "clm_tot_chrg_amt"])
        .collect()  # Materialize only when needed
    )

    assert result.height > 0
```

## Quick Reference

| Test Type | Data Source | Marker | Speed Goal | Use Case |
|-----------|-------------|--------|------------|----------|
| Unit | Inline `pl.DataFrame()` | `@pytest.mark.unit` | < 0.01s | Logic, expressions, pure functions |
| Integration | `load_fixture()` | `@pytest.mark.integration` | < 0.1s | Pipelines, joins, workflows |
| Coverage | Auto-generated | None | N/A | Import checks, structure tests |

## Running Tests

```bash
# Fast: Unit tests only (seconds)
pytest -m unit

# Medium: Unit + Integration (30-60s)
pytest -m "unit or integration"

# Full: Everything including coverage (minutes)
pytest tests/ tests/_coverage/

# Parallel: Even faster
pytest -m unit -n auto
```
