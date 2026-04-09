# © 2025 HarmonyCares
# All rights reserved.

"""
Composability decorator for functional composition with >> operator.

The @composable decorator adds __rshift__ method to functions, enabling
elegant functional composition: df >> func1 >> func2 >> func3
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import TypeVar

import polars as pl

from .._log import LogWriter

logger = LogWriter("decor8.composition")

T = TypeVar("T")


class ComposableFunction:
    """Wrapper class to enable >> operator for function composition."""

    def __init__(self, func: Callable[..., pl.LazyFrame]):
        self.func = func
        self._is_composable = True
        functools.update_wrapper(self, func)

    def __call__(self, df: pl.LazyFrame, *args, **kwargs) -> pl.LazyFrame:
        """Execute the wrapped function."""
        result = self.func(df, *args, **kwargs)

        # Validate return type
        if not isinstance(result, pl.LazyFrame):
            logger.warning(
                f"Composable function '{self.func.__name__}' returned {type(result)}, "
                f"expected pl.LazyFrame"
            )

        return result

    def __rshift__(self, other: Callable) -> ComposableFunction:
        """Enable >> operator for composition."""

        def composed(df: pl.LazyFrame, *args, **kwargs):
            """Composed function."""
            # Apply self first
            intermediate = self(df, *args, **kwargs)
            # Then apply other
            return other(intermediate)

        # Return as ComposableFunction so it's also composable
        return ComposableFunction(composed)


def composable(func: Callable[..., pl.LazyFrame]) -> ComposableFunction:
    """
    Decorator to make functions composable with >> operator.

    Wraps function in a ComposableFunction class that supports composition.
    Works with any function that takes and returns pl.LazyFrame.

    Parameters
    ----------
    func : Callable
        Function to make composable (must take/return LazyFrame)

    Returns
    -------
    Callable
        Composable function with >> operator support

    Examples
    --------
    Basic composition:

    >>> @composable
    >>> def filter_active(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.filter(pl.col("status") == "A")
    >>>
    >>> @composable
    >>> def add_total(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.with_columns((pl.col("amount") * pl.col("qty")).alias("total"))
    >>>
    >>> # Compose with >>
    >>> result = df >> filter_active >> add_total

    Chain multiple operations:

    >>> @composable
    >>> def dedupe(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.unique(subset=["id"])
    >>>
    >>> @composable
    >>> def standardize(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.with_columns(pl.col("name").str.upper())
    >>>
    >>> @composable
    >>> def aggregate(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.group_by("category").agg(pl.col("amount").sum())
    >>>
    >>> # Full pipeline
    >>> result = (
    ...     df
    ...     >> dedupe
    ...     >> standardize
    ...     >> aggregate
    ... )

    With parameters:

    >>> @composable
    >>> def filter_by_status(df: pl.LazyFrame, status: str) -> pl.LazyFrame:
    ...     return df.filter(pl.col("status") == status)
    >>>
    >>> @composable
    >>> def top_n(df: pl.LazyFrame, n: int) -> pl.LazyFrame:
    ...     return df.head(n)
    >>>
    >>> # Partial application for composition
    >>> from functools import partial
    >>> filter_active = partial(filter_by_status, status="A")
    >>> top_10 = partial(top_n, n=10)
    >>>
    >>> result = df >> filter_active >> top_10

    Works with transform decorator:

    >>> @composable
    >>> @transform("process_claims")
    >>> def process(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.filter(pl.col("amount") > 0)
    >>>
    >>> result = df >> process

    Functional style:

    >>> pipeline = (
    ...     composable(lambda df: df.filter(pl.col("active")))
    ...     >> composable(lambda df: df.select(["id", "name"]))
    ...     >> composable(lambda df: df.unique())
    ... )
    >>>
    >>> result = df >> pipeline

    Notes
    -----
    - Function signature is preserved via functools.wraps
    - Type hints are maintained for IDE support
    - Works seamlessly with @transform and @pipeline decorators
    - No performance overhead (just adds method to function object)
    - LazyFrame stays lazy until .collect() is called

    See Also
    --------
    transform : Decorator for LazyFrame transformations
    pipeline : Decorator for multi-transform pipelines
    """
    return ComposableFunction(func)


def compose(*funcs: Callable) -> Callable:
    """
    Compose multiple functions into a single function.

    Alternative to >> operator for explicit composition.

    Parameters
    ----------
    *funcs : Callable
        Functions to compose (applied left-to-right)

    Returns
    -------
    Callable
        Composed function

    Examples
    --------
    >>> dedupe = lambda df: df.unique(subset=["id"])
    >>> standardize = lambda df: df.with_columns(pl.col("name").str.upper())
    >>> aggregate = lambda df: df.group_by("category").agg(pl.col("amount").sum())
    >>>
    >>> # Explicit composition
    >>> pipeline = compose(dedupe, standardize, aggregate)
    >>> result = pipeline(df)
    >>>
    >>> # Equivalent to:
    >>> result = df >> dedupe >> standardize >> aggregate

    With decorated functions:

    >>> @composable
    >>> def step1(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.filter(pl.col("active"))
    >>>
    >>> @composable
    >>> def step2(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.select(["id", "name"])
    >>>
    >>> pipeline = compose(step1, step2)
    >>> result = pipeline(df)
    """
    if not funcs:
        return lambda x: x

    def composed(df: pl.LazyFrame) -> pl.LazyFrame:
        """Apply all functions in sequence."""
        result = df
        for func in funcs:
            result = func(result)
        return result

    # Make the composed function also composable
    return composable(composed)


def runner_method(
    schema_arg: str | None = None,
    threshold: float = 10.0,
    track_memory: bool = False,
    validate_args_types: dict[str, type] | None = None,
) -> Callable:
    """
    Meta-decorator applying standard runner method decorator stack.

    Applies decorators in the correct order:
    1. @timeit(threshold=threshold)
    2. @profile_memory() (if track_memory=True)
    3. @trace_pipeline(schema_name_arg=schema_arg) (if schema_arg provided)
    4. @validate_schema(schema_name_arg=schema_arg) (if schema_arg provided)
    5. @validate_args(**validate_args_types) (if validate_args_types provided)

    Parameters
    ----------
    schema_arg : str, optional
        Name of schema_name argument for validation/tracing
    threshold : float, default=10.0
        Time threshold in seconds for @timeit
    track_memory : bool, default=False
        Whether to track memory usage
    validate_args_types : dict[str, type], optional
        Type validation dict for @validate_args

    Returns
    -------
    Callable
        Decorator function that applies the full stack

    Examples
    --------
    Basic usage with schema validation:

    >>> @runner_method(
    ...     schema_arg="schema_name",
    ...     threshold=10.0,
    ...     track_memory=True,
    ... )
    ... def transform_schema(self, schema_name: str, force: bool = False):
    ...     return self.processor.transform(schema_name, force=force)

    With type validation:

    >>> @runner_method(
    ...     schema_arg="schema_name",
    ...     validate_args_types={"schema_name": str, "force": bool},
    ... )
    ... def transform_schema(self, schema_name: str, force: bool = False):
    ...     return self.processor.transform(schema_name, force=force)

    Full stack example:

    >>> @runner_method(
    ...     schema_arg="schema_name",
    ...     threshold=10.0,
    ...     track_memory=True,
    ...     validate_args_types={"schema_name": str, "force": bool},
    ... )
    ... def transform_schema(self, schema_name: str, force: bool = False):
    ...     return self.processor.transform(schema_name, force=force)

    Notes
    -----
    - Decorators are applied in reverse order (innermost first)
    - Type validation happens first, followed by schema validation
    - Tracing wraps validation for complete observability
    - Memory profiling and timing are outermost
    - Each decorator is optional based on provided arguments

    See Also
    --------
    pipeline_method : Specialized meta-decorator for pipeline operations
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        # Import decorators
        try:
            from .._trace.decorators import trace_pipeline
        except ImportError:
            trace_pipeline = None
        from .performance import profile_memory, timeit
        from .validation import validate_args, validate_schema

        # Apply decorators in REVERSE order (innermost first)
        result = func

        # 5. Type validation (innermost)
        if validate_args_types:
            result = validate_args(**validate_args_types)(result)

        # 4. Schema validation
        if schema_arg:
            result = validate_schema(schema_name_arg=schema_arg)(result)

        # 3. Tracing (skip if opentelemetry not available)
        if trace_pipeline is not None and schema_arg:
            result = trace_pipeline(schema_name_arg=schema_arg)(result)

        # 2. Memory tracking
        if track_memory:
            result = profile_memory(log_result=True)(result)

        # 1. Timing (outermost)
        result = timeit(log_level="info", threshold=threshold)(result)

        return result

    return decorator


def pipeline_method(
    pipeline_arg: str = "pipeline_name",
    threshold: float = 60.0,
    track_memory: bool = True,
) -> Callable:
    """
    Meta-decorator for pipeline execution methods.

    Similar to @runner_method but optimized for pipeline operations:
    - Higher time threshold (60s default vs 10s)
    - Always tracks memory by default
    - Uses pipeline-specific tracing

    Parameters
    ----------
    pipeline_arg : str, default="pipeline_name"
        Name of pipeline argument for validation/tracing
    threshold : float, default=60.0
        Time threshold in seconds (pipelines typically take longer)
    track_memory : bool, default=True
        Whether to track memory usage (usually True for pipelines)

    Returns
    -------
    Callable
        Decorator function that applies the pipeline stack

    Examples
    --------
    Basic pipeline method:

    >>> @pipeline_method()
    ... def run_pipeline(self, pipeline_name: str):
    ...     return self.executor.execute(pipeline_name)

    Custom threshold and argument name:

    >>> @pipeline_method(
    ...     pipeline_arg="name",
    ...     threshold=120.0,  # Allow 2 minutes
    ... )
    ... def execute_long_pipeline(self, name: str):
    ...     return self.executor.execute(name)

    Notes
    -----
    - Internally uses @runner_method with pipeline-optimized defaults
    - Higher time threshold accounts for longer pipeline execution
    - Memory tracking is on by default for pipelines
    - Use this for any pipeline execution or orchestration methods

    See Also
    --------
    runner_method : Generic meta-decorator for runner methods
    """
    # Don't pass schema_arg - pipelines are not schemas and shouldn't be validated as such
    return runner_method(
        schema_arg=None,  # Pipelines don't need schema validation
        threshold=threshold,
        track_memory=track_memory,
    )


def transform_method(
    threshold: float = 5.0,
    track_memory: bool = False,
    enable_composition: bool = True,
    check_not_empty: str | None = None,
    validate_args_types: dict[str, type] | None = None,
) -> Callable:
    """
    Meta-decorator for transform functions.

    Applies decorators in the correct order:
    1. @composable (if enable_composition=True)
    2. @timeit(threshold=threshold)
    3. @profile_memory() (if track_memory=True)
    4. @traced()
    5. @check_not_empty(param_name=check_not_empty) (if check_not_empty provided)
    6. @validate_args(**validate_args_types) (if validate_args_types provided)

    Parameters
    ----------
    threshold : float, default=5.0
        Time threshold in seconds for @timeit (transforms are typically fast)
    track_memory : bool, default=False
        Whether to track memory usage
    enable_composition : bool, default=True
        Whether to make function composable with >> operator
    check_not_empty : str, optional
        Parameter name to check for empty DataFrame
    validate_args_types : dict[str, type], optional
        Type validation dict for @validate_args

    Returns
    -------
    Callable
        Decorator function that applies the transform stack

    Examples
    --------
    Basic transform:

    >>> @transform_method()
    ... def dedupe(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.unique(subset=["id"])

    With validation:

    >>> @transform_method(
    ...     check_not_empty="df",
    ... )
    ... def dedupe(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.unique(subset=["id"])

    Full stack:

    >>> @transform_method(
    ...     threshold=10.0,
    ...     track_memory=True,
    ...     check_not_empty="df",
    ...     validate_args_types={"df": pl.LazyFrame},
    ... )
    ... def complex_transform(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.with_columns(pl.col("value") * 2)

    Notes
    -----
    - Optimized for LazyFrame transformations
    - Lower time threshold (5s) for fast operations
    - Composable by default for pipeline usage
    - Always traced for observability

    See Also
    --------
    runner_method : Meta-decorator for runner methods
    parser_method : Meta-decorator for parser functions
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        # Import decorators
        try:
            from .._trace.decorators import traced
        except ImportError:
            traced = None
        from .performance import profile_memory, timeit
        from .validation import check_not_empty as check_not_empty_decorator
        from .validation import validate_args

        # Apply decorators in REVERSE order (innermost first)
        result = func

        # 5. Type validation (innermost)
        if validate_args_types:
            result = validate_args(**validate_args_types)(result)

        # 4. Empty DataFrame check
        if check_not_empty:
            result = check_not_empty_decorator(param_name=check_not_empty)(result)

        # 3. Tracing (skip if opentelemetry not available)
        if traced is not None:
            result = traced()(result)

        # 2. Memory tracking
        if track_memory:
            result = profile_memory(log_result=True)(result)

        # 1. Timing
        result = timeit(log_level="info", threshold=threshold)(result)

        # 0. Composability (outermost)
        if enable_composition:
            result = composable(result)

        return result

    return decorator


def parser_method(
    threshold: float = 15.0,
    track_memory: bool = False,
    validate_path: str | None = None,
    validate_args_types: dict[str, type] | None = None,
) -> Callable:
    """
    Meta-decorator for parser functions.

    Applies decorators in the correct order:
    1. @timeit(threshold=threshold)
    2. @profile_memory() (if track_memory=True)
    3. @traced()
    4. @validate_path_exists(param_name=validate_path) (if validate_path provided)
    5. @validate_args(**validate_args_types) (if validate_args_types provided)

    Parameters
    ----------
    threshold : float, default=15.0
        Time threshold in seconds for @timeit (parsing is typically I/O-bound)
    track_memory : bool, default=False
        Whether to track memory usage
    validate_path : str, optional
        Parameter name for file path validation
    validate_args_types : dict[str, type], optional
        Type validation dict for @validate_args

    Returns
    -------
    Callable
        Decorator function that applies the parser stack

    Examples
    --------
    Basic parser:

    >>> @parser_method()
    ... def parse_csv(file_path: str) -> pl.LazyFrame:
    ...     return pl.scan_csv(file_path)

    With validation:

    >>> @parser_method(
    ...     validate_path="file_path",
    ... )
    ... def parse_csv(file_path: str) -> pl.LazyFrame:
    ...     return pl.scan_csv(file_path)

    Full stack:

    >>> @parser_method(
    ...     threshold=30.0,
    ...     track_memory=True,
    ...     validate_path="file_path",
    ...     validate_args_types={"file_path": str, "sheet_name": str},
    ... )
    ... def parse_excel(file_path: str, sheet_name: str) -> pl.LazyFrame:
    ...     return pl.read_excel(file_path, sheet_name=sheet_name).lazy()

    Notes
    -----
    - Optimized for file I/O operations
    - Higher time threshold (15s) accounts for disk/network I/O
    - Path validation ensures files exist before parsing
    - Always traced for observability

    See Also
    --------
    runner_method : Meta-decorator for runner methods
    transform_method : Meta-decorator for transform functions
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        # Import decorators
        try:
            from .._trace.decorators import traced
        except ImportError:
            traced = None
        from .performance import profile_memory, timeit
        from .validation import validate_args, validate_path_exists

        # Apply decorators in REVERSE order (innermost first)
        result = func

        # 5. Type validation (innermost)
        if validate_args_types:
            result = validate_args(**validate_args_types)(result)

        # 4. Path validation
        if validate_path:
            result = validate_path_exists(param_name=validate_path)(result)

        # 3. Tracing (skip if opentelemetry not available)
        if traced is not None:
            result = traced()(result)

        # 2. Memory tracking
        if track_memory:
            result = profile_memory(log_result=True)(result)

        # 1. Timing (outermost)
        result = timeit(log_level="info", threshold=threshold)(result)

        return result

    return decorator


def expression_method(
    expression_name: str,
    tier: list[str] | None = None,
    idempotent: bool = True,
    sql_enabled: bool = True,
    threshold: float = 1.0,
    track_memory: bool = False,
) -> Callable:
    """
    Meta-decorator for expression builder functions.

    Applies decorators in the correct order:
    1. @expression(name=expression_name, tier=tier, idempotent=idempotent, sql_enabled=sql_enabled)
    2. @timeit(threshold=threshold)
    3. @profile_memory() (if track_memory=True)
    4. @traced()

    Parameters
    ----------
    expression_name : str
        Name to register expression as
    tier : list[str], optional
        Data tiers this expression applies to (e.g., ["bronze", "silver"])
    idempotent : bool, default=True
        Whether expression is idempotent
    sql_enabled : bool, default=True
        Whether expression can be converted to SQL
    threshold : float, default=1.0
        Time threshold in seconds for @timeit (expressions are typically very fast)
    track_memory : bool, default=False
        Whether to track memory usage

    Returns
    -------
    Callable
        Decorator function that applies the expression stack

    Examples
    --------
    Basic expression builder:

    >>> @expression_method(expression_name="pivot_build")
    ... def build(config: dict[str, Any]) -> dict[str, pl.Expr]:
    ...     return {"total": pl.col("amount").sum()}

    With tier and SQL settings:

    >>> @expression_method(
    ...     expression_name="dedupe_build",
    ...     tier=["bronze", "silver"],
    ...     idempotent=True,
    ...     sql_enabled=False,
    ... )
    ... def build(config: dict[str, Any]) -> list[pl.Expr]:
    ...     return [pl.col("id").first()]

    Full stack:

    >>> @expression_method(
    ...     expression_name="standardize_build",
    ...     tier=["bronze", "silver"],
    ...     threshold=2.0,
    ... )
    ... def build(config: dict[str, Any]) -> list[pl.Expr]:
    ...     return [pl.col(c).cast(pl.Utf8) for c in config["columns"]]

    Notes
    -----
    - Optimized for fast expression building
    - Very low time threshold (1s) for quick operations
    - Automatically registers expression in registry
    - Always traced for observability

    See Also
    --------
    runner_method : Meta-decorator for runner methods
    transform_method : Meta-decorator for transform functions
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        # Import decorators
        try:
            from .._trace.decorators import traced
        except ImportError:
            traced = None
        from .expressions import expression
        from .performance import profile_memory, timeit

        # Apply decorators in REVERSE order (innermost first)
        result = func

        # 4. Tracing (skip if opentelemetry not available)
        if traced is not None:
            result = traced()(result)

        # 3. Memory tracking
        if track_memory:
            result = profile_memory(log_result=True)(result)

        # 2. Timing
        result = timeit(log_level="info", threshold=threshold)(result)

        # 1. Expression registration (outermost)
        result = expression(
            name=expression_name,
            tier=tier or ["bronze", "silver", "gold"],
            idempotent=idempotent,
            sql_enabled=sql_enabled,
        )(result)

        return result

    return decorator


__all__ = [
    "composable",
    "compose",
    "runner_method",
    "pipeline_method",
    "transform_method",
    "parser_method",
    "expression_method",
]
