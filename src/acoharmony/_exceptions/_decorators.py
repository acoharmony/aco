# © 2025 HarmonyCares
# All rights reserved.

"""
Exception handling decorators.

Provides syntactic sugar for automatic exception handling, logging, and tracing.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from contextlib import contextmanager
from typing import TypeVar

from ._base import ACOHarmonyException

T = TypeVar("T")


def explain(
    why: str = "",
    how: str = "",
    causes: list[str] | None = None,
    remediation_steps: list[str] | None = None,
    error_code: str = "",
    category: str = "",
):
    """
    Decorator to add explanation to any exception raised by a function.

        Parameters

        why : str
            Explanation of why errors might occur
        how : str
            Instructions on how to fix
        causes : list[str], optional
            Possible causes
        remediation_steps : list[str], optional
            Step-by-step remediation
        error_code : str, optional
            Error code to use
        category : str, optional
            Error category
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            try:
                return func(*args, **kwargs)
            except ACOHarmonyException:
                # Already an ACOHarmonyException, re-raise as is
                raise
            except Exception as e:
                # Wrap in ACOHarmonyException with explanation
                raise ACOHarmonyException(
                    f"Error in {func.__name__}: {e}",
                    original_error=e,
                    why=why,
                    how=how,
                    causes=causes or [],
                    remediation_steps=remediation_steps or [],
                    metadata={
                        "function": func.__name__,
                        "module": func.__module__,
                    },
                ) from e

        return wrapper

    return decorator


def trace_errors(span_name: str | None = None, **span_attributes):
    """
    Decorator to automatically trace exceptions.

        Parameters

        span_name : str, optional
            Custom span name (defaults to function name)
        **span_attributes
            Additional span attributes
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            try:
                from opentelemetry.trace import Status, StatusCode

                from .._trace import get_tracer

                tracer = get_tracer(func.__module__)
                name = span_name or f"{func.__name__}"

                with tracer.start_as_current_span(
                    name,
                    attributes={
                        "function": func.__name__,
                        **span_attributes,
                    },
                ) as span:
                    try:
                        result = func(*args, **kwargs)
                        span.set_status(Status(StatusCode.OK))
                        return result
                    except Exception as e:
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        span.record_exception(e)
                        raise

            except Exception as e:
                # If tracing setup fails, still execute function
                if "tracer" not in str(e):
                    raise
                return func(*args, **kwargs)

        return wrapper

    return decorator


def log_errors(logger_name: str | None = None, level: str = "error"):
    """
    Decorator to automatically log exceptions.

        Parameters

        logger_name : str, optional
            Logger name (defaults to module name)
        level : str, default="error"
            Log level (debug, info, warning, error, critical)
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                try:
                    from .._log import get_logger

                    logger = get_logger(logger_name or func.__module__)
                    log_method = getattr(logger, level, logger.error)

                    log_method(
                        f"Error in {func.__name__}: {e}",
                        extra={
                            "function": func.__name__,
                            "module": func.__module__,
                            "error_type": type(e).__name__,
                        },
                        exc_info=True,
                    )
                except Exception:  # ALLOWED: Suppress logging errors to prevent cascading failures
                    pass

                raise

        return wrapper

    return decorator


def catch_and_explain(
    exception_type: type[Exception] = Exception,
    why: str = "",
    how: str = "",
    causes: list[str] | None = None,
    remediation_steps: list[str] | None = None,
    reraise: bool = True,
):
    """
    Decorator to catch specific exceptions and add explanation.

        Parameters

        exception_type : type[Exception], default=Exception
            Type of exception to catch
        why : str
            Explanation of why
        how : str
            How to fix
        causes : list[str], optional
            Possible causes
        remediation_steps : list[str], optional
            Remediation steps
        reraise : bool, default=True
            Whether to re-raise the exception
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            try:
                return func(*args, **kwargs)
            except exception_type as e:
                explained_error = ACOHarmonyException(
                    f"Error in {func.__name__}: {e}",
                    original_error=e,
                    why=why,
                    how=how,
                    causes=causes or [],
                    remediation_steps=remediation_steps or [],
                    metadata={
                        "function": func.__name__,
                        "module": func.__module__,
                        "caught_type": type(e).__name__,
                    },
                )

                if reraise:
                    raise explained_error from e
                else:
                    # Just log/trace, don't raise
                    return None

        return wrapper

    return decorator


@contextmanager
def explain_on_error(
    why: str = "",
    how: str = "",
    causes: list[str] | None = None,
    remediation_steps: list[str] | None = None,
    context_name: str = "operation",
):
    """
    Context manager to add explanation to any exception.

        Parameters

        why : str
            Why explanation
        how : str
            How to fix
        causes : list[str], optional
            Possible causes
        remediation_steps : list[str], optional
            Remediation steps
        context_name : str, default="operation"
            Name of the operation for error messages

        Yields

        None
    """
    try:
        yield
    except ACOHarmonyException:
        # Already explained, just re-raise
        raise
    except Exception as e:
        # Wrap with explanation
        raise ACOHarmonyException(
            f"Error during {context_name}: {e}",
            original_error=e,
            why=why,
            how=how,
            causes=causes or [],
            remediation_steps=remediation_steps or [],
            metadata={"context": context_name},
        ) from e


@contextmanager
def suppress_and_log(
    *exception_types: type[Exception],
    logger_name: str = "exceptions",
    log_level: str = "warning",
):
    """
    Context manager to suppress exceptions but log them.

        Parameters

        *exception_types : type[Exception]
            Exception types to suppress (default: all)
        logger_name : str, default="exceptions"
            Logger to use
        log_level : str, default="warning"
            Log level
    """
    types = exception_types or (Exception,)

    try:
        yield
    except types as e:  # ALLOWED: Purpose of this decorator is to suppress exceptions
        try:
            from .._log import get_logger

            logger = get_logger(logger_name)
            log_method = getattr(logger, log_level, logger.warning)
            log_method(f"Suppressed exception: {e}", exc_info=True)
        except Exception:  # ALLOWED: Suppress logging errors to prevent cascading failures
            pass


def retry_with_explanation(
    max_attempts: int = 3,
    why: str = "",
    how: str = "",
    backoff_seconds: float = 1.0,
):
    """
    Decorator to retry function with exponential backoff.

        Parameters

        max_attempts : int, default=3
            Maximum retry attempts
        why : str
            Why retries might be needed
        how : str
            How to fix underlying issue
        backoff_seconds : float, default=1.0
            Initial backoff time
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            import time

            last_error = None
            backoff = backoff_seconds

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e

                    if attempt < max_attempts - 1:
                        # Log retry attempt
                        try:
                            from .._log import get_logger

                            logger = get_logger(func.__module__)
                            logger.warning(
                                f"Retry {attempt + 1}/{max_attempts} for {func.__name__}: {e}",
                                extra={
                                    "attempt": attempt + 1,
                                    "max_attempts": max_attempts,
                                    "backoff": backoff,
                                },
                            )
                        except (
                            Exception
                        ):  # ALLOWED: Suppress logging errors to prevent cascading failures
                            pass

                        time.sleep(backoff)
                        backoff *= 2  # Exponential backoff
                    else:
                        # Final attempt failed
                        raise ACOHarmonyException(
                            f"Failed after {max_attempts} attempts: {last_error}",
                            original_error=last_error,
                            why=why,
                            how=how,
                            metadata={
                                "function": func.__name__,
                                "attempts": max_attempts,
                            },
                        ) from last_error

            return None  # Should never reach here

        return wrapper

    return decorator
