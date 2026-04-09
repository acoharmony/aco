# Coverage Tracking Infrastructure

Best-practice coverage state management and test generation loop for acoharmony.

## Architecture

This system implements the recommended pattern for LLM-driven test generation:

1. **External State Management**: Coverage state is stored in YAML files, not LLM memory
2. **Machine-Readable Output**: Uses coverage.py JSON for authoritative data
3. **Granular Tracking**: Tracks missing lines and branches at file+line level
4. **Priority-Based Planning**: Uses heuristics to choose high-value targets first
5. **Iterative Loop**: Generates one test at a time, validates progress

## Components

### State Management (`state.py`)

Data structures for tracking coverage:
- `FileState`: Per-file coverage with missing lines/branches
- `BranchMiss`: Missing branch (source → target)
- `CoverageState`: Overall project state with YAML serialization

### Extract Missing (`extract_missing.py`)

Parses `coverage.json` and extracts:
- Missing lines by file
- Missing branches (source → target)
- Excluded lines
- Coverage percentages

**Usage:**
```bash
uv run python -m acoharmony._test.coverage.extract_missing .test-state/coverage.json
```

### Plan Next Target (`plan_next_target.py`)

Uses heuristics to prioritize uncovered code:

**Priority Rules:**
1. Branches > straight-line statements (2x multiplier)
2. Partially-covered functions > uncovered (1.5x)
3. Higher file coverage > lower (prefer completing files)
4. Utility modules > integration code
5. Exception paths near existing tests

**Usage:**
```bash
uv run python -m acoharmony._test.coverage.plan_next_target coverage_state.yaml
```

### Diff Coverage (`diff_coverage.py`)

Compares two coverage states and reports:
- Overall coverage delta
- Files with improvements
- Files with regressions
- Lines/branches added/removed

**Usage:**
```bash
uv run python -m acoharmony._test.coverage.diff_coverage old_state.yaml new_state.yaml
```

### Orchestrator (`orchestrator.py`)

Manages the full iteration loop:

1. Run tests with coverage
2. Extract missing coverage
3. Plan next target
4. (External: Generate test via LLM)
5. Run relevant tests
6. Diff coverage
7. Repeat

**Usage:**
```bash
uv run python -m acoharmony._test.coverage.orchestrator --test-path tests/_parsers/test_excel.py
```

## Workflow

### One-Shot Coverage Analysis

```bash
# Run tests with full coverage
pytest --cov=src/acoharmony --cov-branch --cov-report=json:.test-state/coverage.json --cov-report=term-missing

# Extract state
uv run python -m acoharmony._test.coverage.extract_missing .test-state/coverage.json

# Plan next targets
uv run python -m acoharmony._test.coverage.plan_next_target coverage_state.yaml
```

### Iterative Loop

```bash
# Run one full iteration (test → extract → diff → plan)
uv run python -m acoharmony._test.coverage.orchestrator

# Target specific test file
uv run python -m acoharmony._test.coverage.orchestrator --test-path tests/_parsers/test_excel.py

# After making changes, run again to see diff
uv run python -m acoharmony._test.coverage.orchestrator
```

### Manual Diff

```bash
# Save current state as baseline
cp .coverage/coverage_state.yaml .coverage/baseline.yaml

# Make changes and re-run coverage
uv run python -m acoharmony._test.coverage.orchestrator

# Diff against baseline
uv run python -m acoharmony._test.coverage.diff_coverage .coverage/baseline.yaml .coverage/coverage_state.yaml
```

## Configuration

Coverage settings are in `pyproject.toml`:

```toml
[tool.coverage.run]
branch = true  # Track branch coverage
dynamic_context = "test_function"  # Per-test tracking
data_file = ".test-state/.coverage"

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "if TYPE_CHECKING:",
    "raise AssertionError",
    "# ALLOWED:.*",  # Intentional exceptions
]
sort = "Cover"  # Show lowest coverage first

[tool.coverage.json]
output = ".test-state/coverage.json"
show_contexts = true  # Include per-test data
pretty_print = true
```

## Output Files

All outputs are in `.coverage/` directory:

- `coverage.json` - Raw coverage.py JSON (authoritative)
- `coverage_state.yaml` - Extracted state (for LLM consumption)
- `coverage_state_previous.yaml` - Previous run (for diffing)
- `next_targets.yaml` - Prioritized test targets

## Stopping Rules

Don't aim for 100% blindly. Stop when:

1. **File threshold met**: e.g., 95% on utility modules
2. **High-value coverage complete**: Core business logic covered
3. **Only exclusions remain**: Unreachable/platform-specific code
4. **Diminishing returns**: Low-priority edge cases

### Exclusions

Mark unreachable code with pragmas:

```python
if sys.platform == "win32":  # pragma: no cover
    # Windows-specific code
    ...

if TYPE_CHECKING:  # Automatically excluded
    from typing import ...

raise RuntimeError("unreachable")  # Automatically excluded
```

## LLM Integration

The LLM should:

1. **Read** `next_targets.yaml` for prioritized targets
2. **Read** source file around target line
3. **Read** existing test file
4. **Propose** one small test to cover target
5. **Not track state** - state is in YAML files

The orchestrator provides:
- Machine-readable target list
- File + line + branch specifics
- Priority scores
- Previous attempts (to avoid repeating)

## Best Practices

### Per-Module Strategy

1. **Utility modules** (`_parsers`, `_utils`): Aim for 95-100%
2. **Business logic** (`_transforms`, `_expressions`): Aim for 90%+
3. **CLI code** (`cli.py`, `_4icli`): Aim for 70-80%
4. **Entry points** (`__main__.py`): Document exclusions

### Granularity

- Work on **one function** at a time
- Cover **one branch** per test
- **Validate** coverage improved before moving on
- **Reject regressions** immediately

### Iteration Size

- Generate **one test** per iteration
- Run **relevant test file only** for speed
- Check **diff** before accepting
- Commit when **meaningful delta** achieved

## Example Session

```bash
# Initial run
$ uv run python -m acoharmony._test.coverage.orchestrator --test-path tests/_parsers/test_excel.py

[1/4] Running tests with coverage...
========================================== 58 passed in 5.10s ==========================================

[2/4] Extracting coverage state...
Coverage: 100.00%
Uncovered items: 0

[3/4] Comparing with previous run...
Overall Coverage:
  Previous: 98.49%
  Current:  100.00%
  Change:   +1.51% ✓

Improvements (1 files):
src/acoharmony/_parsers/_excel.py
  Coverage: 98.49% → 100.00%
  Lines covered: 3
  Branches covered: 1

[4/4] Planning next targets...
No uncovered targets found. Coverage is complete!

🎉 Coverage is complete!
```

## Advanced: Per-Test Context Tracking

Coverage.py can track which tests hit which lines:

```bash
pytest \
  --cov=src/acoharmony \
  --cov-branch \
  --cov-context=test \
  --cov-report=json:.test-state/coverage.json

# Then inspect contexts in coverage.json
```

This helps identify:
- Which tests overlap (redundancy)
- Which code has no tests (gaps)
- Which tests are most valuable (coverage span)

**Note**: Per-test contexts not supported with Python 3.14+ sysmon. Use `dynamic_context` instead.

## Troubleshooting

### Coverage.py not finding modules

Ensure `source_pkgs = ["acoharmony"]` in `pyproject.toml` and tests run from project root.

### Branch coverage seems incomplete

Some branches are optimized away by Python bytecode. Use `show_contexts=true` to verify.

### Targets not prioritized correctly

Edit `plan_next_target.py` heuristics. Multipliers and path patterns are configurable.

### State file corruption

Delete `.coverage/` directory and re-run from scratch:

```bash
rm -rf .coverage
uv run python -m acoharmony._test.coverage.orchestrator
```

## References

- [coverage.py docs](https://coverage.readthedocs.io/)
- [pytest-cov docs](https://pytest-cov.readthedocs.io/)
- [Best practices for LLM-driven coverage](https://stackoverflow.com/questions/79336481)


# Coverage Gap Tests

This directory contains **auto-generated tests** that check for basic code coverage gaps (imports, basic structure, etc.).

## Why separate?

These tests are **SLOW** and **numerous** (~18,000+ tests). They're excluded from default test runs to keep development fast.

## When to run

- **Weekly on CI** (scheduled job)
- **Before releases** (manual)
- **When adding new modules** (verify they can be imported)

## How to run

```bash
# Run all coverage gap tests
pytest tests/_coverage/

# Run specific gap tests
pytest tests/_coverage/test_parsers_gaps.py
pytest tests/_coverage/test_expressions_gaps.py

# Run with coverage report
pytest tests/_coverage/ --cov=acoharmony
```

## What's included

- **test_parsers_gaps.py** - 12,247 tests for _parsers module
- **test_expressions_gaps.py** - 5,839 tests for _expressions module

## Do NOT

- ❌ Add these tests to default test runs
- ❌ Write functional tests here (use tests/_expressions/, tests/_transforms/, etc.)
- ❌ Mock heavy functionality here (these are import/structure tests)

## DO

- ✅ Run periodically on CI
- ✅ Check before releases
- ✅ Use as a safety net for refactoring
