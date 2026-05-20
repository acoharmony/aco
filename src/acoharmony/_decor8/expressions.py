# © 2025 HarmonyCares
# All rights reserved.

"""
Expression decorator for wrapping functions that return Polars expressions.

The @expression decorator wraps functions that return pl.Expr or list[pl.Expr],
providing metadata attachment, composability, and registry integration.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Literal, TypeVar

import polars as pl

from .._log import LogWriter

logger = LogWriter("decor8.expressions")

# Type aliases
Tier = Literal["bronze", "silver", "gold"]
T = TypeVar("T")


def expression(
    name: str,
    tier: Tier | list[Tier] = "bronze",
    description: str | None = None,
    idempotent: bool = True,
    sql_enabled: bool = True,
) -> Callable[[Callable[..., pl.Expr | list[pl.Expr]]], Callable[..., pl.Expr | list[pl.Expr]]]:
    """
    Decorator for functions that return Polars expressions.

    Wraps functions that return pl.Expr or list[pl.Expr], providing:
    - Metadata attachment for discovery and documentation
    - Return type validation
    - ExpressionRegistry integration
    - Composability support via >> operator

    Parameters
    ----------
    name : str
        Expression name for identification and documentation
    tier : Tier or list[Tier], default="bronze"
        Data tier(s) where this expression applies:
        - "bronze": Raw data ingestion
        - "silver": Standardized/cleaned data
        - "gold": Analytics/aggregations
    description : str, optional
        Human-readable description (uses docstring if not provided)
    idempotent : bool, default=True
        Whether this expression is safe to apply multiple times
    sql_enabled : bool, default=True
        Whether this expression can be converted to SQL

    Returns
    -------
    Callable
        Decorated function with metadata and validation

    Raises
    ------
    TypeError
        If decorated function doesn't return pl.Expr or list[pl.Expr]

    Examples
    --------
    Basic expression:

    >>> @expression("amount_doubled")
    >>> def double_amount() -> pl.Expr:
    ...     return pl.col("amount") * 2

    Multi-tier expression with description:

    >>> @expression(
    ...     "claim_standardization",
    ...     tier=["bronze", "silver"],
    ...     description="Standardize claim status codes"
    ... )
    >>> def standardize_status() -> list[pl.Expr]:
    ...     return [
    ...         pl.col("status").str.upper().alias("status_upper"),
    ...         pl.col("status").is_in(["A", "D", "R"]).alias("status_valid"),
    ...     ]

    Composable expressions:

    >>> @expression("filter_active")
    >>> def filter_active() -> pl.Expr:
    ...     return pl.col("status") == "A"
    >>>
    >>> @expression("filter_high_value")
    >>> def filter_high_value() -> pl.Expr:
    ...     return pl.col("amount") > 1000
    >>>
    >>> # Compose with >> operator
    >>> df = df.filter(filter_active() & filter_high_value())

    Registry integration:

    >>> from acoharmony._expressions._registry import ExpressionRegistry
    >>>
    >>> @expression("deduplication", tier="bronze")
    >>> def dedupe_expr() -> pl.Expr:
    ...     return pl.col("id")
    >>>
    >>> # Expression is automatically registered
    >>> assert "deduplication" in ExpressionRegistry.list_builders()

    See Also
    --------
    transform : Decorator for LazyFrame transformations
    pipeline : Decorator for multi-transform pipelines
    """

    def decorator(
        func: Callable[..., pl.Expr | list[pl.Expr]],
    ) -> Callable[..., pl.Expr | list[pl.Expr]]:
        # Normalize tier to list
        tiers = [tier] if isinstance(tier, str) else tier

        # Store metadata on function
        func._expression_name = name  # type: ignore
        func._expression_tiers = tiers  # type: ignore
        func._expression_description = description or func.__doc__  # type: ignore
        func._expression_idempotent = idempotent  # type: ignore
        func._expression_sql_enabled = sql_enabled  # type: ignore

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> pl.Expr | list[pl.Expr]:
            """Wrapped expression with validation."""
            # Execute the expression
            result = func(*args, **kwargs)

            # Validate return type
            if isinstance(result, list):
                if not all(isinstance(expr, pl.Expr) for expr in result):
                    raise TypeError(
                        f"Expression '{name}' must return pl.Expr or list[pl.Expr], "
                        f"got list with non-Expr elements"
                    )
            elif not isinstance(result, pl.Expr):
                raise TypeError(
                    f"Expression '{name}' must return pl.Expr or list[pl.Expr], got {type(result)}"
                )

            return result

        # Add metadata as attributes
        wrapper._expression_name = name  # type: ignore
        wrapper._expression_tiers = tiers  # type: ignore
        wrapper._expression_description = description or func.__doc__  # type: ignore
        wrapper._expression_idempotent = idempotent  # type: ignore
        wrapper._expression_sql_enabled = sql_enabled  # type: ignore

        # Add composability support
        def __rshift__(self, other: Callable) -> Callable:
            """Enable >> operator for composition."""

            @functools.wraps(other)
            def composed(*args, **kwargs):
                return other(self(*args, **kwargs))

            return composed

        wrapper.__rshift__ = __rshift__  # type: ignore

        # Register with ExpressionRegistry if available
        try:
            from .._expressions._registry import ExpressionRegistry

            # Register the expression
            ExpressionRegistry.register(
                expression_type=name,
                metadata={
                    "function": func.__name__,
                    "idempotent": idempotent,
                    "sql_enabled": sql_enabled,
                },
                schemas=tiers,
                callable=True,
                description=description or func.__doc__,
            )(lambda: wrapper)

            logger.debug(
                f"Registered expression '{name}' with ExpressionRegistry: "
                f"tiers={tiers}, sql_enabled={sql_enabled}"
            )
        except (ImportError, AttributeError) as e:
            logger.debug(f"ExpressionRegistry not available for registration: {e}")

        return wrapper

    return decorator


__all__ = ["expression", "Tier"]
