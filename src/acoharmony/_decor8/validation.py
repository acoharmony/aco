# © 2025 HarmonyCares
# All rights reserved.

"""
Input validation decorators.

These validate inputs BEFORE any Polars operations, ensuring we fail fast
with clear error messages rather than cryptic Polars errors deep in execution.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from functools import wraps
from pathlib import Path
from typing import TypeVar

import polars as pl

T = TypeVar("T")


def validate_args(**type_checks) -> Callable:
    """
    Validate function arguments match expected types.

        Parameters

        **type_checks
            Mapping of parameter names to expected types

        Returns

        Callable
            Decorated function that validates arguments
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Import here to avoid circular imports
            from .._exceptions import ValidationError

            # Bind arguments and validate types
            sig = inspect.signature(func)
            try:
                bound = sig.bind(*args, **kwargs)
            except TypeError as e:
                raise ValidationError(
                    f"Invalid arguments for {func.__name__}: {e}",
                    how="Check function signature and parameter names",
                ) from e

            for param_name, expected_type in type_checks.items():
                if param_name in bound.arguments:
                    value = bound.arguments[param_name]
                    if value is not None and not isinstance(value, expected_type):
                        raise ValidationError(
                            f"Parameter '{param_name}' must be {expected_type.__name__}, "
                            f"got {type(value).__name__}",
                            how=f"Pass a {expected_type.__name__} for parameter '{param_name}'",
                        )

            return func(*args, **kwargs)

        return wrapper

    return decorator


def require_columns(*required_cols: str) -> Callable:
    """
    Validate that a LazyFrame/DataFrame has required columns.

        Parameters

        *required_cols
            Column names that must exist in the DataFrame

        Returns

        Callable
            Decorated function that validates DataFrame columns
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Import here to avoid circular imports
            from .._exceptions import ValidationError

            # Find the LazyFrame/DataFrame argument
            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)

            for _param_name, value in bound.arguments.items():
                if isinstance(value, pl.LazyFrame | pl.DataFrame):
                    df_cols = value.columns
                    missing = [col for col in required_cols if col not in df_cols]
                    if missing:
                        raise ValidationError(
                            f"DataFrame missing required columns: {missing}",
                            how=f"Ensure input DataFrame has columns: {list(required_cols)}",
                            causes=[
                                "Wrong DataFrame passed",
                                "Columns not yet added",
                                "Column name mismatch",
                            ],
                            metadata={
                                "required_columns": list(required_cols),
                                "missing_columns": missing,
                                "available_columns": df_cols,
                            },
                        )

            return func(*args, **kwargs)

        return wrapper

    return decorator


def validate_schema(schema_name_arg: str = "schema_name") -> Callable:
    """
    Validate that a schema exists in the catalog.

        Parameters

        schema_name_arg : str, default="schema_name"
            Name of the argument containing the schema name

        Returns

        Callable
            Decorated function that validates schema existence
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Import here to avoid circular imports
            from .._catalog import Catalog
            from .._exceptions import ValidationError

            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)

            if schema_name_arg in bound.arguments:
                schema_name = bound.arguments[schema_name_arg]
                catalog = Catalog()

                schema = catalog.get_schema(schema_name)
                if schema is None:
                    available = catalog.list_tables()
                    raise ValidationError(
                        f"Schema '{schema_name}' not found in catalog",
                        how="Check schema name spelling or create the schema definition",
                        causes=[
                            "Schema name misspelled",
                            "Schema not yet defined",
                            "Wrong catalog loaded",
                        ],
                        metadata={
                            "requested_schema": schema_name,
                            "available_schemas": available[:10],  # First 10
                            "total_schemas": len(available),
                        },
                    )

            return func(*args, **kwargs)

        return wrapper

    return decorator


def check_not_empty(param_name: str = "df") -> Callable:
    """
    Validate that a DataFrame/LazyFrame is not empty.

        Parameters

        param_name : str, default="df"
            Name of the DataFrame parameter to check

        Returns

        Callable
            Decorated function that validates DataFrame is not empty
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Import here to avoid circular imports
            from .._exceptions import ValidationError

            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)

            if param_name in bound.arguments:
                df = bound.arguments[param_name]
                if isinstance(df, pl.LazyFrame):
                    # Only way to check is to collect first row
                    if df.head(1).collect().height == 0:
                        raise ValidationError(
                            f"LazyFrame '{param_name}' is empty",
                            how="Ensure input data has at least one row",
                            causes=["No data in source", "All rows filtered out"],
                        )
                elif isinstance(df, pl.DataFrame):
                    if df.height == 0:
                        raise ValidationError(
                            f"DataFrame '{param_name}' is empty",
                            how="Ensure input data has at least one row",
                            causes=["No data in source", "All rows filtered out"],
                        )

            return func(*args, **kwargs)

        return wrapper

    return decorator


def validate_path_exists(param_name: str = "path") -> Callable:
    """
    Validate that a file/directory path exists.

        Parameters

        param_name : str, default="path"
            Name of the path parameter to check

        Returns

        Callable
            Decorated function that validates path existence
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Import here to avoid circular imports
            from .._exceptions import ValidationError

            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)

            if param_name in bound.arguments:
                path = bound.arguments[param_name]
                if isinstance(path, str | Path):
                    path = Path(path)
                    if not path.exists():
                        raise ValidationError(
                            f"Path does not exist: {path}",
                            how="Check the path is correct and file/directory exists",
                            causes=[
                                "Path does not exist",
                                "Permission denied",
                                "Network drive not mounted",
                            ],
                            metadata={"path": str(path), "absolute_path": str(path.absolute())},
                        )

            return func(*args, **kwargs)

        return wrapper

    return decorator


def validate_file_format(
    param_name: str = "file_path", formats: list[str] | None = None
) -> Callable:
    """
    Validate that a file has the expected format/extension.

        Parameters

        param_name : str, default="file_path"
            Name of the path parameter to check
        formats : list[str], optional
            List of valid file extensions (e.g., ['.csv', '.parquet'])

        Returns

        Callable
            Decorated function that validates file format
    """
    if formats is None:
        formats = []

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Import here to avoid circular imports
            from .._exceptions import ValidationError

            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)

            if param_name in bound.arguments:
                path = bound.arguments[param_name]
                if isinstance(path, str | Path):
                    path = Path(path)
                    if formats and path.suffix.lower() not in [f.lower() for f in formats]:
                        raise ValidationError(
                            f"Invalid file format: {path.suffix}",
                            how=f"Use one of the supported formats: {formats}",
                            causes=["Wrong file type", "File extension missing"],
                            metadata={
                                "file": str(path),
                                "file_format": path.suffix,
                                "supported_formats": formats,
                            },
                        )

            return func(*args, **kwargs)

        return wrapper

    return decorator
