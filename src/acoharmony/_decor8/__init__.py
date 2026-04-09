# © 2025 HarmonyCares
# All rights reserved.

"""
Centralized decorator imports for easy access.

This module consolidates all decorators from various parts of the codebase,
making them easily accessible with a single import.

Usage
"""

# Tracing decorators (existing)
# Exception decorators (existing)
from .._exceptions._decorators import (
    catch_and_explain,
    explain,
    explain_on_error,
    log_errors,
    retry_with_explanation,
    suppress_and_log,
    trace_errors,
)

try:
    from .._trace.decorators import trace_method, trace_pipeline, traced
except ImportError:
    # Skinny install: tracing decorators become no-ops
    def _noop_decorator(func=None, **kwargs):
        if func is not None:
            return func
        return _noop_decorator

    traced = _noop_decorator
    trace_method = _noop_decorator
    trace_pipeline = _noop_decorator
from .composition import (
    composable,
    compose,
    expression_method,
    parser_method,
    pipeline_method,
    runner_method,
    transform_method,
)

# Decorator suite — some require full-package deps
from .expressions import expression

# Performance decorators
from .performance import (
    measure_dataframe_size,
    profile_memory,
    timeit,
    warn_slow,
)
from .transforms import transform

# These modules may import full-package deps (narwhals, result, etc.)
try:
    from .pipelines import pipeline
except ImportError:
    pipeline = None

# Validation decorators (new)
from .validation import (
    check_not_empty,
    require_columns,
    validate_args,
    validate_file_format,
    validate_path_exists,
    validate_schema,
)

__all__ = [
    # Tracing
    "traced",
    "trace_method",
    "trace_pipeline",
    # Exceptions
    "explain",
    "catch_and_explain",
    "retry_with_explanation",
    "log_errors",
    "trace_errors",
    "explain_on_error",
    "suppress_and_log",
    # Validation
    "validate_args",
    "require_columns",
    "validate_schema",
    "check_not_empty",
    "validate_path_exists",
    "validate_file_format",
    # Performance
    "timeit",
    "warn_slow",
    "profile_memory",
    "measure_dataframe_size",
    # Decorator Suite
    "expression",
    "transform",
    "pipeline",
    "composable",
    "compose",
    # Meta-decorators
    "runner_method",
    "pipeline_method",
    "transform_method",
    "parser_method",
    "expression_method",
]
