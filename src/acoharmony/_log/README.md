# _log

Logging system for all modules.

## Purpose

Centralized logging. Structured logs. Profile-based log levels.

## LogWriter

Main logging class.

```python
from acoharmony._log import LogWriter

logger = LogWriter("module_name")

logger.debug("Debug message")
logger.info("Info message")
logger.warning("Warning message")
logger.error("Error message")
```

## Log Locations

Profile-based paths from `pyproject.toml`:

- **local/dev:** `/opt/s3/data/workspace/logs/`
- **staging:** S3-compatible storage
- **prod:** Databricks volumes

## Log Types

### Application Logs
- Transform execution
- Pipeline runs
- Expression evaluation

### Tracking Logs
- Data lineage
- Row counts
- Processing metrics

### Error Logs
- Exceptions
- Validation failures
- Parse errors

## Key Features

- **Structured logging** - JSON format
- **Profile-based levels** - Debug in dev, info in prod
- **Context injection** - Automatic metadata
- **Performance tracking** - Timing data
- **Error context** - Stack traces, input data

## Integration

All modules use `LogWriter` for logging.
