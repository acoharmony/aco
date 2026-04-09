"""
Decorators for simplified tracing.

Provides convenient decorators for adding tracing to functions and methods
without explicit span management.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from functools import wraps
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from .config import get_trace_config, get_tracer


def traced(
    span_name: str | None = None,
    kind: trace.SpanKind = trace.SpanKind.INTERNAL,
    **default_attributes,
) -> Callable:
    """
    Decorator to trace any function.

        Automatically creates a span for the function execution with error handling.

        Parameters

        span_name : str, optional
            Name for the span. If None, uses the function name.
        kind : SpanKind, optional
            Type of span (INTERNAL, SERVER, CLIENT, etc.).
        **default_attributes
            Default attributes to set on every span.

        Returns

        Callable
            Decorator function.
    """

    def decorator(func: Callable) -> Callable:
        config = get_trace_config()

        @wraps(func)
        def wrapper(*args, **kwargs):
            if not config.enabled:
                return func(*args, **kwargs)

            # Get module name for tracer
            module_name = func.__module__.replace("acoharmony.", "")
            tracer = get_tracer(module_name)

            # Determine span name
            name = span_name or func.__name__

            with tracer.start_as_current_span(name, kind=kind) as span:
                # Set default attributes
                for key, value in default_attributes.items():
                    span.set_attribute(key, value)

                # Try to add function parameters as attributes
                try:
                    sig = inspect.signature(func)
                    bound_args = sig.bind(*args, **kwargs)
                    bound_args.apply_defaults()

                    # Add simple parameters as attributes
                    for param_name, param_value in bound_args.arguments.items():
                        if param_name != "self" and _is_traceable_param(param_name, param_value):
                            attr_name = f"param.{param_name}"
                            span.set_attribute(attr_name, _serialize_value(param_value))
                except Exception:  # ALLOWED: Tracing should not break application, ignore parameter extraction errors
                    # Ignore parameter extraction errors
                    pass

                try:
                    result = func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise

        return wrapper

    return decorator


def trace_pipeline(
    schema_name_arg: str = "schema_name",
    include_args: list[str] | None = None,
) -> Callable:
    """
    Decorator specifically for pipeline methods.

        Creates a span with pipeline-specific attributes like schema name.

        Parameters

        schema_name_arg : str, optional
            Name of the argument containing the schema name (default: "schema_name").
        include_args : list[str], optional
            Additional arguments to include as span attributes.

        Returns

        Callable
            Decorator function.
    """

    def decorator(func: Callable) -> Callable:
        config = get_trace_config()

        @wraps(func)
        def wrapper(*args, **kwargs):
            if not config.enabled:
                return func(*args, **kwargs)

            # Get module name for tracer
            module_name = func.__module__.replace("acoharmony.", "")
            tracer = get_tracer(module_name)

            # Extract schema name and other arguments
            try:
                sig = inspect.signature(func)
                bound_args = sig.bind(*args, **kwargs)
                bound_args.apply_defaults()

                schema_name = bound_args.arguments.get(schema_name_arg, "unknown")
                span_name = f"{func.__name__}.{schema_name}"

                attributes = {
                    "pipeline.schema": schema_name,
                    "pipeline.operation": func.__name__,
                }

                # Add additional requested arguments
                if include_args:
                    for arg_name in include_args:
                        if arg_name in bound_args.arguments:
                            value = bound_args.arguments[arg_name]
                            if _is_simple_type(value):
                                attributes[f"pipeline.{arg_name}"] = value

            except Exception:  # ALLOWED: Optional feature, continues with degraded functionality
                # Fallback to simple naming
                span_name = func.__name__
                attributes = {"pipeline.operation": func.__name__}

            with tracer.start_as_current_span(span_name, kind=trace.SpanKind.INTERNAL) as span:
                # Set attributes
                for key, value in attributes.items():
                    span.set_attribute(key, value)

                try:
                    result = func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))

                    # Try to extract result metrics if available
                    if hasattr(result, "total_records"):
                        span.set_attribute("pipeline.records_processed", result.total_records)
                    if hasattr(result, "total_files"):
                        span.set_attribute("pipeline.files_processed", result.total_files)

                    return result
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise

        return wrapper

    return decorator


def trace_method(
    span_name: str | None = None,
    include_class_name: bool = True,
    **default_attributes,
) -> Callable:
    """
    Decorator specifically for class methods.

        Parameters

        span_name : str, optional
            Name for the span. If None, uses class.method format.
        include_class_name : bool, optional
            Whether to include the class name in the span name.
        **default_attributes
            Default attributes to set on the span.

        Returns

        Callable
            Decorator function.
    """

    def decorator(func: Callable) -> Callable:
        config = get_trace_config()

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not config.enabled:
                return func(self, *args, **kwargs)

            # Get module name for tracer
            module_name = func.__module__.replace("acoharmony.", "")
            tracer = get_tracer(module_name)

            # Determine span name
            if span_name:
                name = span_name
            elif include_class_name:
                class_name = self.__class__.__name__
                name = f"{class_name}.{func.__name__}"
            else:
                name = func.__name__

            with tracer.start_as_current_span(name) as span:
                # Set default attributes
                for key, value in default_attributes.items():
                    span.set_attribute(key, value)

                # Add class name as attribute
                if include_class_name:
                    span.set_attribute("class", self.__class__.__name__)

                try:
                    result = func(self, *args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise

        return wrapper

    return decorator


def _is_simple_type(value: Any) -> bool:
    """
    Check if a value is a simple type that can be used as a span attribute.

        OpenTelemetry only accepts bool, str, bytes, int, float (not None).

        Parameters

        value : Any
            Value to check.

        Returns

        bool
            True if the value is a simple type that can be used as a span attribute.
    """
    return isinstance(value, str | int | float | bool)


def _is_traceable_param(param_name: str, param_value: Any) -> bool:
    """
    Check if a parameter should be traced.

        Parameters

        param_name : str
            Name of the parameter.
        param_value : Any
            Value of the parameter.

        Returns

        bool
            True if the parameter should be traced.
    """
    # Skip sensitive parameters
    sensitive_names = {"password", "secret", "token", "key", "credential", "api_key"}
    if any(sensitive in param_name.lower() for sensitive in sensitive_names):
        return False

    # Only trace simple types and paths
    if _is_simple_type(param_value):
        return True

    # Also allow Path objects
    try:
        from pathlib import Path

        if isinstance(param_value, Path):
            return True
    except Exception:  # ALLOWED: Returns False to indicate error as part of API contract
        pass

    return False


def _serialize_value(value: Any) -> str | int | float | bool:
    """
    Serialize a value for use as a span attribute.

        OpenTelemetry only accepts bool, str, bytes, int, float as attribute values.
        None values are converted to the string "None".

        Parameters

        value : Any
            Value to serialize.

        Returns

        str or int or float or bool
            Serialized value suitable for OpenTelemetry span attributes.
    """
    # Handle None explicitly
    if value is None:
        return "None"

    if _is_simple_type(value):
        return value

    try:
        from pathlib import Path

        if isinstance(value, Path):
            return str(value)
    except Exception:  # ALLOWED: Fallback to str() if Path check fails, no error needed
        pass

    return str(value)
