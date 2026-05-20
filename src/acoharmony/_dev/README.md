# _dev

Development utilities for ACO Harmony.

## Structure

### docs/
Documentation generation from code.

**Tools:**
- `orchestrator.py` - Generate all docs
- `pipelines.py` - Generate pipeline docs
- `connectors.py` - Generate connector docs
- `notebooks.py` - Generate notebook docs
- `lineage.py` - Generate lineage diagrams

**Usage:**
```bash
uv run aco dev generate-docs
```

### test/
Test utilities and mock generation.

**Tools:**
- `mocks.py` - Generate test mocks
- `coverage.py` - Check test coverage
- `fixtures.py` - Organize test fixtures

### analysis/
Code analysis tools.

**Tools:**
- `docstrings.py` - Audit docstrings
- `imports.py` - Analyze import chains
- `exceptions.py` - Lint exception handling
- `schemas.py` - Introspect schemas

### excel/
Excel file analysis and comparison.

**Tools:**
- `analyzer.py` - Analyze Excel files
- `diffs.py` - Compare Excel files

### setup/
Setup and initialization utilities.

**Tools:**
- `storage.py` - Setup storage tiers
- `database.py` - Populate test database
- `copyright.py` - Add copyright headers

### generators/
Code and content generators.

**Tools:**
- `metadata.py` - Generate ACO metadata
- `cclf_guide.py` - Generate CCLF guides
