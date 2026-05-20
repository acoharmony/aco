# © 2025 HarmonyCares
# All rights reserved.

"""
Transform decorator for wrapping functions that operate on LazyFrames.

The @transform decorator wraps functions that take and return pl.LazyFrame,
providing metadata, validation, and optional auto-composition from expressions.
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from typing import Literal, TypeVar, get_args, get_origin

import polars as pl

from .._log import LogWriter

logger = LogWriter("decor8.transforms")

# Type aliases
Tier = Literal["bronze", "silver", "gold"]
T = TypeVar("T")


class TransformFunction:
    """Wrapper class to enable >> operator and metadata for transforms."""

    def __init__(
        self,
        func: Callable[..., pl.LazyFrame],
        name: str,
        tiers: list[Tier],
        expressions: list[Callable] | None,
        description: str | None,
        sql_enabled: bool,
    ):
        self.func = func
        self._transform_name = name
        self._transform_tiers = tiers
        self._transform_expressions = expressions or []
        self._transform_description = description or func.__doc__
        self._transform_sql_enabled = sql_enabled

        # Check if function accepts None as first argument
        sig = inspect.signature(func)
        params = list(sig.parameters.values())
        if params:
            first_param_annotation = params[0].annotation
            # Check if annotation is Union type with None (e.g., pl.LazyFrame | None)
            self._accepts_none = (
                first_param_annotation != inspect.Parameter.empty
                and (
                    get_origin(first_param_annotation) is type(None | int)  # UnionType
                    or (hasattr(first_param_annotation, '__args__') and type(None) in get_args(first_param_annotation))
                )
            )
        else:
            self._accepts_none = False

        functools.update_wrapper(self, func)

    def __call__(self, df: pl.LazyFrame, *args, **kwargs) -> pl.LazyFrame:
        """Execute the wrapped transform with validation."""
        # Validate input type (allow None if function signature accepts it)
        if df is None and not self._accepts_none:
            raise TypeError(
                f"Transform '{self._transform_name}' expects pl.LazyFrame as first argument, "
                f"got {type(df)}"
            )
        elif df is not None and not isinstance(df, pl.LazyFrame):
            raise TypeError(
                f"Transform '{self._transform_name}' expects pl.LazyFrame as first argument, "
                f"got {type(df)}"
            )

        # Apply expressions if provided (auto-composition)
        if self._transform_expressions:
            # Collect all expressions
            exprs: list[pl.Expr] = []
            for expr_func in self._transform_expressions:
                result = expr_func()
                if isinstance(result, pl.Expr):
                    exprs.append(result)
                elif isinstance(result, list):
                    exprs.extend(result)
                else:
                    logger.warning(
                        f"Expression function '{expr_func.__name__}' returned "
                        f"unexpected type {type(result)}, skipping"
                    )

            # Apply all expressions at once
            if exprs:
                df = df.with_columns(exprs)

        # Execute the transform
        result = self.func(df, *args, **kwargs)

        # Validate return type
        if not isinstance(result, pl.LazyFrame):
            raise TypeError(
                f"Transform '{self._transform_name}' must return pl.LazyFrame, got {type(result)}"
            )

        return result

    def __rshift__(self, other: Callable) -> TransformFunction:
        """Enable >> operator for composition."""

        def composed(df: pl.LazyFrame, *args, **kwargs):
            intermediate = self(df, *args, **kwargs)
            return other(intermediate)

        # Return as TransformFunction so it's also composable
        return TransformFunction(
            composed,
            f"{self._transform_name}_composed",
            self._transform_tiers,
            None,
            f"Composition of {self._transform_name}",
            self._transform_sql_enabled,
        )


def transform(
    name: str,
    tier: Tier | list[Tier] = "bronze",
    expressions: list[Callable] | None = None,
    description: str | None = None,
    sql_enabled: bool = True,
) -> Callable[[Callable[..., pl.LazyFrame]], Callable[..., pl.LazyFrame]]:
    """
    Decorator for functions that transform LazyFrames.

    Wraps functions that take and return pl.LazyFrame, providing:
    - Input/output validation
    - Optional auto-composition from expressions
    - Metadata for lineage tracking
    - SQL generation hooks
    - Composability via >> operator

    Parameters
    ----------
    name : str
        Transform name for identification and documentation
    tier : Tier or list[Tier], default="bronze"
        Data tier(s) where this transform applies
    expressions : list[Callable], optional
        List of expression functions to auto-compose with .with_columns()
        If provided, these expressions are applied to the input LazyFrame
    description : str, optional
        Human-readable description (uses docstring if not provided)
    sql_enabled : bool, default=True
        Whether this transform can be converted to SQL

    Returns
    -------
    Callable
        Decorated function with validation and metadata

    Raises
    ------
    TypeError
        If decorated function doesn't take/return pl.LazyFrame

    Examples
    --------
    Basic transform:

    >>> @transform("standardize_claims")
    >>> def standardize(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.with_columns([
    ...         pl.col("status").str.upper().alias("status_std"),
    ...         pl.col("amount").cast(pl.Float64),
    ...     ])

    Auto-compose from expressions:

    >>> @expression("status_upper")
    >>> def status_expr() -> pl.Expr:
    ...     return pl.col("status").str.upper().alias("status_std")
    >>>
    >>> @expression("amount_float")
    >>> def amount_expr() -> pl.Expr:
    ...     return pl.col("amount").cast(pl.Float64)
    >>>
    >>> @transform(
    ...     "standardize_auto",
    ...     expressions=[status_expr, amount_expr]
    ... )
    >>> def standardize_auto(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     # Expressions are auto-applied before this function runs
    ...     return df

    Multi-tier transform:

    >>> @transform(
    ...     "deduplicate_claims",
    ...     tier=["bronze", "silver"],
    ...     description="Remove duplicate claims by claim_id"
    ... )
    >>> def dedupe(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.unique(subset=["claim_id"], maintain_order=True)

    Composable transforms:

    >>> @transform("filter_active")
    >>> def filter_active(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.filter(pl.col("status") == "A")
    >>>
    >>> @transform("aggregate_by_patient")
    >>> def aggregate(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.group_by("patient_id").agg(pl.col("amount").sum())
    >>>
    >>> # Compose with >> operator
    >>> result = df >> filter_active >> aggregate

    SQL generation hooks:

    >>> @transform("complex_filter", sql_enabled=True)
    >>> def complex_filter(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     # This transform can be converted to SQL
    ...     return df.filter(
    ...         (pl.col("status") == "A") & (pl.col("amount") > 1000)
    ...     )

    See Also
    --------
    expression : Decorator for Polars expressions
    pipeline : Decorator for multi-transform pipelines
    sql_safe : Decorator for SQL generation
    """

    def decorator(func: Callable[..., pl.LazyFrame]) -> TransformFunction:
        # Normalize tier to list
        tiers = [tier] if isinstance(tier, str) else tier

        # Log registration
        logger.debug(
            f"Registered transform '{name}': "
            f"tiers={tiers}, sql_enabled={sql_enabled}, "
            f"expressions={len(expressions or [])}"
        )

        return TransformFunction(func, name, tiers, expressions, description, sql_enabled)

    return decorator


__all__ = ["transform", "Tier"]
