# Tuva Integration Tests

## Test Structure

Tests are organized by resource intensity and scope:

### Unit Tests (Fast, No Data)
- `test_executor_unit.py` - Executor functionality without data loading
- `test_cli.py` - CLI interface tests
- `test_config.py` - Configuration tests
- `test_bridge.py` - Bridge functionality tests

**Run with:** `pytest tests/_tuva -m unit`

### Small Tests (Sample Data)
- `test_staging_small.py` - Staging models with 100-row samples

**Run with:** `pytest tests/_tuva -m small`

### Integration Tests (Full Data, Run Individually)
- `test_integration_single.py` - One model at a time with full datasets

**Run with:** `pytest tests/_tuva/test_integration_single.py::TestStagingBeneficiaryDemographicsFull -v`

### Legacy Tests (Heavy, May Crash Docker)
- `test_staging_models.py` - Original staging tests (loads all data)
- `test_intermediate_models.py` - Original intermediate tests (loads all data)
- `test_executor.py` - Original executor tests (loads full data)

**Not recommended to run these in Docker** - use the new test files instead.

## Recommended Test Workflow

### 1. Quick validation (30s)
```bash
pytest tests/_tuva -m unit
```

### 2. Small integration tests (2-5min)
```bash
pytest tests/_tuva -m small
```

### 3. Single model test (run one at a time)
```bash
# Test beneficiary demographics
pytest tests/_tuva/test_integration_single.py::TestStagingBeneficiaryDemographicsFull -v

# Test beneficiary xref
pytest tests/_tuva/test_integration_single.py::TestStagingBeneficiaryXrefFull -v
```

### 4. Heavy tests (one at a time, may need more memory)
```bash
pytest tests/_tuva -m heavy -k "BeneficiaryDemographics" -v
```

## Pytest Markers

- `@pytest.mark.unit` - Fast unit tests, no data loading
- `@pytest.mark.small` - Small integration tests with sample data
- `@pytest.mark.integration` - Integration tests with full data
- `@pytest.mark.heavy` - Heavy tests that load large datasets

## Memory Management

The original tests crashed Docker because they:
1. Loaded all CCLF files (1-9) in module-scoped fixtures
2. Held everything in memory simultaneously
3. Ran multiple heavy tests in parallel

The new test structure:
1. Uses function-scoped fixtures (data released after each test)
2. Loads only required data per test
3. Uses sample data (100 rows) for most tests
4. Marks heavy tests so they can be run individually

## Adding New Tests

### For fast tests (no data):
```python
@pytest.mark.unit
def test_something(executor):
    # Test logic only
    assert executor.something() == expected
```

### For small tests (sample data):
```python
@pytest.mark.small
def test_with_sample(executor, sample_data_factory):
    data = sample_data_factory('cclf8.parquet', n_rows=100)
    executor.register_source('medicare_cclf', 'table', data)
    # Test with small data
```

### For heavy tests (full data):
```python
@pytest.mark.integration
@pytest.mark.heavy
def test_full_pipeline(processed_data_path):
    executor = TuvaSQLExecutor()
    # Load only what's needed
    df = pl.read_parquet(processed_data_path / 'cclf8.parquet')
    # Test
```

## CI/CD Recommendations

In CI, run:
```bash
# Fast tests always
pytest tests/_tuva -m unit

# Small tests for PRs
pytest tests/_tuva -m small

# Heavy tests only on main/release (with more resources)
pytest tests/_tuva -m heavy --maxfail=1
```
