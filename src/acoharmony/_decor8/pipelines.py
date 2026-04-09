# © 2025 HarmonyCares
# All rights reserved.

"""
Pipeline decorator for orchestrating multiple transforms.

The @pipeline decorator chains multiple transforms in sequence, providing
checkpointing support, error handling, and SQL generation for full pipelines.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Literal, TypeVar

import polars as pl

from .._log import LogWriter
from ..result import Result

logger = LogWriter("decor8.pipelines")

# Type aliases
Tier = Literal["bronze", "silver", "gold"]
T = TypeVar("T")


def pipeline(
    name: str,
    transforms: list[Callable] | None = None,
    description: str | None = None,
    sql_enabled: bool = True,
    checkpoint_after: list[str] | None = None,
) -> Callable[[Callable[..., pl.LazyFrame]], Callable[..., Result[pl.LazyFrame]]]:
    """
    Decorator for functions that orchestrate multiple transforms.

    Wraps functions that chain transforms, providing:
    - Sequential transform execution
    - Optional checkpointing between transforms
    - Error handling with Result monad
    - SQL generation for full pipeline using CTEs
    - Metadata for lineage tracking

    Parameters
    ----------
    name : str
        Pipeline name for identification and documentation
    transforms : list[Callable], optional
        List of transform functions to chain sequentially
        If provided, these are executed before the decorated function
    description : str, optional
        Human-readable description (uses docstring if not provided)
    sql_enabled : bool, default=True
        Whether this pipeline can be converted to SQL
    checkpoint_after : list[str], optional
        List of transform names after which to checkpoint
        (documentation only - implementation TBD)

    Returns
    -------
    Callable
        Decorated function that returns Result[LazyFrame]

    Raises
    ------
    TypeError
        If decorated function doesn't return pl.LazyFrame or Result[LazyFrame]

    Examples
    --------
    Basic pipeline:

    >>> @pipeline("claims_processing")
    >>> def process_claims(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return (
    ...         df
    ...         .filter(pl.col("status") == "A")
    ...         .with_columns(pl.col("amount").cast(pl.Float64))
    ...         .unique(subset=["claim_id"])
    ...     )

    Auto-chain transforms:

    >>> @transform("dedupe")
    >>> def dedupe(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.unique(subset=["claim_id"])
    >>>
    >>> @transform("standardize")
    >>> def standardize(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.with_columns(pl.col("status").str.upper())
    >>>
    >>> @pipeline(
    ...     "claims_pipeline",
    ...     transforms=[dedupe, standardize]
    ... )
    >>> def claims_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     # Transforms are auto-applied before this function runs
    ...     return df

    With checkpointing:

    >>> @pipeline(
    ...     "complex_pipeline",
    ...     checkpoint_after=["dedupe", "enrich"],
    ...     description="Multi-stage claims processing with checkpoints"
    ... )
    >>> def complex_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     # Checkpoints are documented for future implementation
    ...     return df

    Error handling with Result:

    >>> @pipeline("safe_pipeline")
    >>> def safe_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return df.filter(pl.col("amount") > 0)
    >>>
    >>> result = safe_pipeline(df)
    >>> if result.success:
    ...     processed_df = result.data
    >>> else:
    ...     logger.error(f"Pipeline failed: {result.error}")

    SQL generation:

    >>> @pipeline("sql_pipeline", sql_enabled=True)
    >>> def sql_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
    ...     return (
    ...         df
    ...         .filter(pl.col("status") == "A")
    ...         .group_by("patient_id")
    ...         .agg(pl.col("amount").sum())
    ...     )
    >>>
    >>> # Can be converted to SQL using @sql_safe decorator

    See Also
    --------
    transform : Decorator for single transforms
    expression : Decorator for Polars expressions
    sql_safe : Decorator for SQL generation
    """

    def decorator(func: Callable[..., pl.LazyFrame]) -> Callable[..., Result[pl.LazyFrame]]:
        # Store metadata on function
        func._pipeline_name = name  # type: ignore
        func._pipeline_transforms = transforms or []  # type: ignore
        func._pipeline_description = description or func.__doc__  # type: ignore
        func._pipeline_sql_enabled = sql_enabled  # type: ignore
        func._pipeline_checkpoint_after = checkpoint_after or []  # type: ignore

        @functools.wraps(func)
        def wrapper(df: pl.LazyFrame, *args, **kwargs) -> Result[pl.LazyFrame]:
            """Wrapped pipeline with error handling."""
            try:
                # Validate input type
                if not isinstance(df, pl.LazyFrame):
                    return Result.error(
                        message=f"Pipeline '{name}' expects pl.LazyFrame as first argument, got {type(df)}",
                        errors=[f"Invalid input type for pipeline '{name}'"],
                    )

                # Apply transforms if provided (auto-chaining)
                current_df = df
                if transforms:
                    for i, transform_func in enumerate(transforms):
                        try:
                            # Apply transform
                            current_df = transform_func(current_df)

                            # Validate intermediate result
                            if not isinstance(current_df, pl.LazyFrame):
                                return Result.error(
                                    message=f"Pipeline '{name}' failed at transform {i}",
                                    errors=[
                                        f"Transform {i} ('{getattr(transform_func, '__name__', 'unknown')}') "
                                        f"returned {type(current_df)} instead of LazyFrame"
                                    ],
                                )

                            # Check if we should checkpoint (future implementation)
                            transform_name = getattr(
                                transform_func, "_transform_name", transform_func.__name__
                            )
                            if checkpoint_after and transform_name in checkpoint_after:
                                logger.debug(
                                    f"Checkpoint marker after transform '{transform_name}' "
                                    f"(not yet implemented)"
                                )

                        except Exception as e:
                            return Result.error(
                                message=f"Pipeline '{name}' failed at transform {i} "
                                f"('{getattr(transform_func, '__name__', 'unknown')}')",
                                errors=[str(e)],
                            )

                # Execute the pipeline function
                result = func(current_df, *args, **kwargs)

                # Validate return type
                if isinstance(result, Result):
                    # Function already returns Result
                    return result
                elif isinstance(result, pl.LazyFrame):
                    # Wrap in Result
                    return Result.ok(
                        data=result, message=f"Pipeline '{name}' completed successfully"
                    )
                else:
                    return Result.error(
                        message=f"Invalid return type from pipeline '{name}'",
                        errors=[
                            f"Pipeline '{name}' must return pl.LazyFrame or Result[LazyFrame], "
                            f"got {type(result)}"
                        ],
                    )

            except Exception as e:
                logger.error(f"Pipeline '{name}' failed with exception: {e}")
                return Result.error(
                    message=f"Pipeline '{name}' failed with unexpected error",
                    errors=[str(e)],
                )

        # Add metadata as attributes
        wrapper._pipeline_name = name  # type: ignore
        wrapper._pipeline_transforms = transforms or []  # type: ignore
        wrapper._pipeline_description = description or func.__doc__  # type: ignore
        wrapper._pipeline_sql_enabled = sql_enabled  # type: ignore
        wrapper._pipeline_checkpoint_after = checkpoint_after or []  # type: ignore

        # Log registration
        logger.debug(
            f"Registered pipeline '{name}': "
            f"sql_enabled={sql_enabled}, "
            f"transforms={len(transforms or [])}, "
            f"checkpoints={len(checkpoint_after or [])}"
        )

        return wrapper

    return decorator


__all__ = ["pipeline", "Tier"]
