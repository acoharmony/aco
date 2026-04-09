# _decor8

Decorator suite for functions, expressions, transforms, and pipelines.

## Purpose

Provide decorators for metadata, validation, performance, and registration.

## Modules

### `expressions.py`

Decorator for Polars expression functions.

```python
@expression(
    name="filter_active",
    tier=["bronze", "silver"],
    idempotent=True,
    sql_enabled=True
)
def filter_active() -> pl.Expr:
    return pl.col("status") == "A"
```

**Features:**
- Metadata attachment (name, tier, description)
- Return type validation (`pl.Expr` or `list[pl.Expr]`)
- Registry integration
- Composability via `>>` operator

### `transforms.py`

Decorator for LazyFrame transformations.

```python
@transform(name="dedupe_claims", tier="silver")
def dedupe(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.unique(subset=["claim_id"])
```

### `pipelines.py`

Decorator for multi-transform pipelines.

```python
@pipeline(name="claims_pipeline", tier="silver")
def process_claims(df: pl.LazyFrame) -> pl.LazyFrame:
    df = dedupe(df)
    df = standardize(df)
    return df
```

### `validation.py`

Validation decorators.

```python
@validate_args
@require_columns(["claim_id", "member_id"])
@validate_schema(schema_name="medical_claim")
@check_not_empty
def process(df: pl.LazyFrame) -> pl.LazyFrame:
    return df
```

**Available:**
- `@validate_args` - Validate function arguments
- `@require_columns` - Check required columns exist
- `@validate_schema` - Validate against schema
- `@check_not_empty` - Ensure DataFrame not empty
- `@validate_path_exists` - Check file paths exist
- `@validate_file_format` - Validate file format

### `performance.py`

Performance monitoring decorators.

```python
@timeit
@profile_memory
@warn_slow(threshold_seconds=10)
@measure_dataframe_size
def expensive_operation(df: pl.LazyFrame) -> pl.LazyFrame:
    return df
```

**Available:**
- `@timeit` - Measure execution time
- `@profile_memory` - Track memory usage
- `@warn_slow` - Warn on slow execution
- `@measure_dataframe_size` - Log DataFrame size

### `composition.py`

Function composition decorators.

```python
@composable
def step1(df): return df
@composable
def step2(df): return df

# Compose with >> operator
pipeline = step1 >> step2
```

**Available:**
- `@composable` - Enable function composition
- `@compose` - Compose multiple functions
- `@expression_method` - Mark as expression method
- `@transform_method` - Mark as transform method
- `@pipeline_method` - Mark as pipeline method
- `@parser_method` - Mark as parser method
- `@runner_method` - Mark as runner method

### `sql_generation.py`

SQL generation decorators.

```python
@sql_safe
@generate_pipeline_sql
def transform(df: pl.LazyFrame) -> pl.LazyFrame:
    return df
```

**Available:**
- `@sql_safe` - Mark as SQL-compatible
- `@generate_pipeline_sql` - Generate SQL for pipeline

## Usage

```python
from acoharmony._decor8 import (
    expression,
    transform,
    pipeline,
    validate_args,
    require_columns,
    timeit,
    composable
)

@expression("my_expr", tier=["silver"])
@timeit
def my_expression() -> pl.Expr:
    return pl.col("value") * 2

@transform("my_transform", tier="silver")
@require_columns(["claim_id"])
@check_not_empty
def my_transform(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(my_expression())
```

## Integration

Decorators integrate with:
- `_expressions/_registry.py` - Expression registration
- `_transforms/_registry.py` - Transform registration
- `_trace/` - Tracing system
- `_exceptions/` - Exception handling
- `_log/` - Logging system

## Key Features

- **Metadata attachment** - Name, tier, description
- **Automatic registration** - Import-time registration
- **Validation** - Runtime checks
- **Performance monitoring** - Timing and memory
- **Composability** - Chain with operators
- **SQL generation** - Convert to SQL
