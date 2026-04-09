# _4icli

4icli DataHub integration for APM data submission.

## Purpose

Interface with 4icli DataHub. Submit APM data. Track submissions.

## 4icli DataHub

CMS contractor system for:
- ACO data submission
- Quality measure reporting
- Financial reconciliation
- Alignment data

## Configuration

From `pyproject.toml`:
- **APM ID:** D0259
- **Year:** 2025
- **Binary:** `/usr/local/bin/4icli`
- **Config:** Profile-based paths

## Usage

```python
from acoharmony._4icli import submit_data

# Submit ACO data
result = submit_data(
    data_type="alignment",
    file_path="/path/to/data.txt"
)
```

## Key Features

- **Automated submission** - Submit via CLI
- **Status tracking** - Track submission status
- **Error handling** - Parse error responses
- **File validation** - Validate before submission
- **Audit logging** - Log all submissions

## Integration

- `_config/` - Profile-based configuration
- `_runner/` - Execution orchestration
- CLI - Submit via `aco submit`
