# _trace

Distributed tracing for transforms and pipelines.

## Purpose

Trace execution. Performance monitoring. Debug pipeline issues.

## Usage

```python
from acoharmony._trace import traced, trace_pipeline

@traced("transform_name")
def my_transform(df):
    return df

@trace_pipeline("pipeline_name")
def my_pipeline(df):
    return df
```

## Trace Data

Captured for each execution:
- Start/end timestamps
- Duration
- Input/output row counts
- Memory usage
- Errors

## Key Features

- **Automatic tracing** - Decorator-based
- **Performance metrics** - Timing, memory
- **Error tracking** - Exception details
- **Pipeline visibility** - Stage-by-stage view
- **Distributed traces** - Track across stages

## Integration

- `_log/` - Trace data logged
- `_decor8/` - Tracing decorators
- All transforms - Auto-traced
