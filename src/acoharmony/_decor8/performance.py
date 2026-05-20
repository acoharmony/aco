# © 2025 HarmonyCares
# All rights reserved.

"""
Performance monitoring decorators.

These OBSERVE execution without changing behavior or interfering with
Polars' native optimization.
"""

from __future__ import annotations

import functools
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def timeit(log_level: str = "info", threshold: float | None = None) -> Callable:
    """
    Time function execution and log the duration.

        Does NOT cache or modify behavior - purely observational.

        Parameters

        log_level : str, default="info"
            Log level for timing info (debug, info, warning)
        threshold : float, optional
            Only log if execution exceeds this threshold (seconds)

        Returns

        Callable
            Decorated function that times execution
        'done'
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Import here to avoid circular imports
            import inspect

            from .._log import get_logger

            # Strip 'acoharmony.' prefix if present to avoid duplication
            module_name = func.__module__.removeprefix("acoharmony.")
            logger = get_logger(module_name)

            # Determine if this is a method and get class name
            func_name = func.__name__
            sig = inspect.signature(func)
            if args and sig.parameters and list(sig.parameters.keys())[0] in ("self", "cls"):
                # This is a method - include class name
                instance_or_class = args[0]
                if hasattr(instance_or_class, "__class__"):
                    class_name = instance_or_class.__class__.__name__
                    func_name = f"{class_name}.{func.__name__}"

            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.perf_counter() - start

                # Only log if no threshold or exceeds threshold
                if threshold is None or duration >= threshold:
                    log_func = getattr(logger, log_level, logger.info)
                    log_func(
                        f"{func_name} completed in {duration:.3f}s",
                        extra={
                            "function": func_name,
                            "duration_seconds": duration,
                            "exceeded_threshold": threshold and duration >= threshold,
                        },
                    )

        return wrapper

    return decorator


def warn_slow(threshold_seconds: float) -> Callable:
    """
    Warn if a function takes longer than expected.

        Parameters

        threshold_seconds : float
            Threshold in seconds above which to warn

        Returns

        Callable
            Decorated function that warns on slow execution
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Import here to avoid circular imports
            import inspect

            from .._log import get_logger

            # Strip 'acoharmony.' prefix if present to avoid duplication
            module_name = func.__module__.removeprefix("acoharmony.")
            logger = get_logger(module_name)

            # Determine if this is a method and get class name
            func_name = func.__name__
            sig = inspect.signature(func)
            if args and sig.parameters and list(sig.parameters.keys())[0] in ("self", "cls"):
                # This is a method - include class name
                instance_or_class = args[0]
                if hasattr(instance_or_class, "__class__"):
                    class_name = instance_or_class.__class__.__name__
                    func_name = f"{class_name}.{func.__name__}"

            start = time.perf_counter()
            result = func(*args, **kwargs)
            duration = time.perf_counter() - start

            if duration >= threshold_seconds:
                logger.warning(
                    f"{func_name} took {duration:.2f}s (threshold: {threshold_seconds}s)",
                    extra={
                        "function": func_name,
                        "duration": duration,
                        "threshold": threshold_seconds,
                    },
                )

            return result

        return wrapper

    return decorator


def profile_memory(log_result: bool = True) -> Callable:
    """
    Profile memory usage during function execution.

        Parameters

        log_result : bool, default=True
            Whether to log the memory profile results

        Returns

        Callable
            Decorated function that profiles memory usage
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            try:
                import psutil

                process = psutil.Process()
            except ImportError:
                # If psutil not available, just run the function
                return func(*args, **kwargs)

            # Import here to avoid circular imports
            import inspect

            from .._log import get_logger

            # Strip 'acoharmony.' prefix if present to avoid duplication
            module_name = func.__module__.removeprefix("acoharmony.")
            logger = get_logger(module_name)

            # Determine if this is a method and get class name
            func_name = func.__name__
            sig = inspect.signature(func)
            if args and sig.parameters and list(sig.parameters.keys())[0] in ("self", "cls"):
                # This is a method - include class name
                instance_or_class = args[0]
                if hasattr(instance_or_class, "__class__"):
                    class_name = instance_or_class.__class__.__name__
                    func_name = f"{class_name}.{func.__name__}"

            mem_before = process.memory_info().rss / 1024 / 1024  # MB
            result = func(*args, **kwargs)
            mem_after = process.memory_info().rss / 1024 / 1024  # MB

            mem_delta = mem_after - mem_before

            if log_result:
                logger.info(
                    f"{func_name} memory: {mem_before:.1f}MB → {mem_after:.1f}MB (Δ{mem_delta:+.1f}MB)",
                    extra={
                        "function": func_name,
                        "memory_before_mb": mem_before,
                        "memory_after_mb": mem_after,
                        "memory_delta_mb": mem_delta,
                    },
                )

            return result

        return wrapper

    return decorator


def measure_dataframe_size(param_name: str = "df", collect_if_lazy: bool = False) -> Callable:
    """
    Measure and log the size of a Polars DataFrame/LazyFrame.

        Parameters

        param_name : str, default="df"
            Name of the DataFrame parameter to measure
        collect_if_lazy : bool, default=False
            Whether to collect LazyFrame to measure size (use with caution)

        Returns

        Callable
            Decorated function that measures DataFrame size
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            import inspect

            import polars as pl

            # Import here to avoid circular imports
            from .._log import get_logger

            # Strip 'acoharmony.' prefix if present to avoid duplication
            module_name = func.__module__.removeprefix("acoharmony.")
            logger = get_logger(module_name)

            # Determine if this is a method and get class name
            func_name = func.__name__
            sig = inspect.signature(func)
            if args and sig.parameters and list(sig.parameters.keys())[0] in ("self", "cls"):
                # This is a method - include class name
                instance_or_class = args[0]
                if hasattr(instance_or_class, "__class__"):
                    class_name = instance_or_class.__class__.__name__
                    func_name = f"{class_name}.{func.__name__}"

            bound = sig.bind(*args, **kwargs)

            if param_name in bound.arguments:
                df = bound.arguments[param_name]
                if isinstance(df, pl.DataFrame):
                    logger.debug(
                        f"{func_name}: DataFrame size: {df.height} rows × {df.width} cols",
                        extra={
                            "function": func_name,
                            "df_rows": df.height,
                            "df_cols": df.width,
                            "df_type": "DataFrame",
                        },
                    )
                elif isinstance(df, pl.LazyFrame) and collect_if_lazy:
                    # Use with caution - forces collection
                    collected = df.collect()
                    logger.debug(
                        f"{func_name}: LazyFrame size: {collected.height} rows × {collected.width} cols",
                        extra={
                            "function": func_name,
                            "df_rows": collected.height,
                            "df_cols": collected.width,
                            "df_type": "LazyFrame",
                        },
                    )

            return func(*args, **kwargs)

        return wrapper

    return decorator
