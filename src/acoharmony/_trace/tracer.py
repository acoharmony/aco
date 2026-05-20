"""
Tracer wrapper for ACO Harmony.

Provides a unified tracing interface that simplifies OpenTelemetry usage.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from .config import TraceConfig, get_trace_config, get_tracer


class TracerWrapper:
    """
    Wrapper for OpenTelemetry tracer with simplified API.

        Provides context managers and decorators for easy span creation
        with automatic error handling and attribute management.
    """

    def __init__(self, name: str, config: TraceConfig | None = None):
        """
        Initialize tracer wrapper.

                Parameters

                name : str
                    Name of this component (e.g., "TransformRunner", "transform.cclf1").
                config : TraceConfig, optional
                    Tracing configuration. Uses default if not provided.
        """
        self.name = name
        self.config = config or get_trace_config()
        self.tracer = get_tracer(name)

        # Also get a logger for fallback logging
        from .._log import get_logger

        self.logger = get_logger(f"trace.{name}")

    @contextmanager
    def span(
        self,
        span_name: str,
        kind: trace.SpanKind = trace.SpanKind.INTERNAL,
        **attributes,
    ):
        """
        Create a span with automatic error handling.

                Parameters

                span_name : str
                    Name of the span.
                kind : SpanKind, optional
                    Type of span (INTERNAL, SERVER, CLIENT, PRODUCER, CONSUMER).
                **attributes
                    Attributes to set on the span.

                Yields

                Span
                    The created span.
        """
        if not self.config.enabled:
            # If tracing is disabled, yield a no-op context
            from contextlib import nullcontext

            with nullcontext() as span:
                yield span
            return

        with self.tracer.start_as_current_span(span_name, kind=kind) as span:
            # Set initial attributes
            for key, value in attributes.items():
                span.set_attribute(key, value)

            try:
                yield span
            except Exception as e:
                # Record the exception in the span
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                self.logger.error(f"Error in span '{span_name}': {e}")
                raise

    @contextmanager
    def span_with_metrics(
        self,
        span_name: str,
        kind: trace.SpanKind = trace.SpanKind.INTERNAL,
        **attributes,
    ):
        """
        Create a span that automatically tracks execution time metrics.

                Parameters

                span_name : str
                    Name of the span.
                kind : SpanKind, optional
                    Type of span.
                **attributes
                    Initial attributes to set on the span.

                Yields

                Span
                    The created span.
        """
        import time

        start_time = time.perf_counter()

        with self.span(span_name, kind=kind, **attributes) as span:
            yield span
            # Add execution time as an event
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            if span is not None:
                span.add_event(
                    "execution_completed",
                    attributes={"duration_ms": elapsed_ms},
                )

    def add_event(self, event_name: str, **attributes):
        """
        Add an event to the current span.

                Parameters

                event_name : str
                    Name of the event.
                **attributes
                    Attributes for the event.
        """
        if not self.config.enabled:
            return

        current_span = trace.get_current_span()
        if current_span:
            current_span.add_event(event_name, attributes=attributes)

    def set_attribute(self, key: str, value: Any):
        """
        Set an attribute on the current span.

                Parameters

                key : str
                    Attribute key.
                value : Any
                    Attribute value.
        """
        if not self.config.enabled:
            return

        current_span = trace.get_current_span()
        if current_span:
            current_span.set_attribute(key, value)

    def set_attributes(self, attributes: dict[str, Any]):
        """
        Set multiple attributes on the current span.

                Parameters

                attributes : dict
                    Dictionary of attributes to set.
        """
        if not self.config.enabled:
            return

        current_span = trace.get_current_span()
        if current_span:
            for key, value in attributes.items():
                current_span.set_attribute(key, value)

    def record_exception(self, exception: Exception, **attributes):
        """
        Record an exception in the current span.

                Parameters

                exception : Exception
                    Exception to record.
                **attributes
                    Additional attributes for the exception event.
        """
        if not self.config.enabled:
            return

        current_span = trace.get_current_span()
        if current_span:
            current_span.record_exception(exception, attributes=attributes)
            current_span.set_status(Status(StatusCode.ERROR, str(exception)))

    def trace_function(
        self,
        func: Callable,
        span_name: str | None = None,
        **default_attributes,
    ) -> Callable:
        """
        Decorator to trace a function.

                Parameters

                func : Callable
                    Function to trace.
                span_name : str, optional
                    Name for the span. Defaults to function name.
                **default_attributes
                    Default attributes to set on the span.

                Returns

                Callable
                    Wrapped function.
        """
        from functools import wraps

        @wraps(func)
        def wrapper(*args, **kwargs):
            name = span_name or func.__name__
            with self.span(name, **default_attributes) as span:
                # Try to add function parameters as attributes
                try:
                    # Get function signature
                    import inspect

                    sig = inspect.signature(func)
                    bound_args = sig.bind(*args, **kwargs)
                    bound_args.apply_defaults()

                    # Add non-self arguments as attributes
                    for param_name, param_value in bound_args.arguments.items():
                        if param_name != "self" and _is_simple_type(param_value):
                            span.set_attribute(f"param.{param_name}", str(param_value))
                except Exception:  # ALLOWED: Tracing should not break application
                    # Ignore errors in parameter extraction
                    pass

                result = func(*args, **kwargs)
                return result

        return wrapper

    def get_current_trace_id(self) -> str | None:
        """
        Get the current trace ID.

                Returns

                str or None
                    Trace ID as hex string, or None if no active span.
        """
        current_span = trace.get_current_span()
        if current_span and current_span.get_span_context().is_valid:
            trace_id = current_span.get_span_context().trace_id
            return f"{trace_id:032x}"
        return None

    def get_current_span_id(self) -> str | None:
        """
        Get the current span ID.

                Returns

                str or None
                    Span ID as hex string, or None if no active span.
        """
        current_span = trace.get_current_span()
        if current_span and current_span.get_span_context().is_valid:
            span_id = current_span.get_span_context().span_id
            return f"{span_id:016x}"
        return None

    def link_to_log(self, log_message: str, **log_attributes):
        """
        Add trace context to a log message.

                Useful for correlating logs with traces.

                Parameters

                log_message : str
                    Log message to emit.
                **log_attributes
                    Additional attributes for the log.
        """
        trace_id = self.get_current_trace_id()
        span_id = self.get_current_span_id()

        if trace_id and span_id:
            self.logger.info(
                log_message,
                extra={
                    "trace_id": trace_id,
                    "span_id": span_id,
                    **log_attributes,
                },
            )
        else:
            self.logger.info(log_message, extra=log_attributes)


def _is_simple_type(value: Any) -> bool:
    """
    Check if a value is a simple type that can be used as a span attribute.

        Parameters

        value : Any
            Value to check.

        Returns

        bool
            True if the value is a simple type.
    """
    return isinstance(value, str | int | float | bool) or value is None
