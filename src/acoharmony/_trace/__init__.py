"""
OpenTelemetry tracing integration for ACO Harmony.

 distributed tracing capabilities following the same pattern
as the _log module, enabling comprehensive observability for all pipeline operations.
"""

from .config import TraceConfig, get_tracer, setup_tracing, shutdown_tracing
from .decorators import trace_pipeline, traced
from .tracer import TracerWrapper

__all__ = [
    "TraceConfig",
    "get_tracer",
    "setup_tracing",
    "shutdown_tracing",
    "TracerWrapper",
    "traced",
    "trace_pipeline",
]
