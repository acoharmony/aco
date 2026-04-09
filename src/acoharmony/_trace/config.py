"""
Tracing configuration for ACO Harmony.

Provides centralized OpenTelemetry tracing configuration that integrates with storage backends.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SpanExporter,
)


@dataclass
class TraceConfig:
    """
    Configuration for ACO Harmony OpenTelemetry tracing.

        Integrates with the storage configuration and medallion architecture
        to ensure traces go to the appropriate backend (console, file, OTLP).

        Attributes

        storage_config : StorageBackend
            Storage configuration instance for path resolution.
        namespace : str
            Namespace for traces (stored separately from medallion layers).
        service_name : str
            Service name for tracing (appears in trace UI).
        exporter_type : str
            Type of exporter: "console", "file", "otlp", "jaeger", "none".
        otlp_endpoint : str, optional
            OTLP collector endpoint (e.g., "http://localhost:4317").
        sample_rate : float
            Sampling rate (0.0 to 1.0). 1.0 traces everything.
        enabled : bool
            Whether tracing is enabled.
    """

    storage_config: Any | None = None  # Will be StorageBackend instance
    namespace: str = "traces"
    service_name: str = "acoharmony"
    exporter_type: str = "console"  # "console", "file", "otlp", "jaeger", "none"
    otlp_endpoint: str | None = None
    sample_rate: float = 1.0
    enabled: bool = True
    _base_path: Path | str | None = None
    _exporters: list[SpanExporter] = field(default_factory=list)

    def __post_init__(self):
        """Initialize storage-aware tracing."""
        if not self.enabled:
            return

        if self.storage_config is None:
            # Try to create storage config with default profile
            try:
                import os

                from .._store import StorageBackend

                profile = os.getenv("ACO_PROFILE", "local")
                self.storage_config = StorageBackend(profile=profile)
            except Exception as e:
                import os

                from .._exceptions import StorageBackendError

                profile = os.getenv("ACO_PROFILE")
                raise StorageBackendError.from_initialization_error(e, profile) from e

        # Ensure trace directory exists for local storage (file exporter)
        if self.exporter_type == "file":
            if self.storage_config and self.storage_config.get_storage_type() == "local":
                trace_path = self.get_base_path()
                if isinstance(trace_path, Path):
                    trace_path.mkdir(parents=True, exist_ok=True)

    def get_base_path(self) -> Path | str:
        """
        Get the base path for traces using storage configuration.

                Returns

                Path or str
                    Base path for traces (local Path or cloud URL string)
        """
        if self._base_path is not None:
            return self._base_path

        # storage_config must exist (or initialization would have failed)
        # Use storage config to get traces path under logs directory
        # Traces should be stored in logs/traces subdirectory
        logs_base = self.storage_config.get_path("logs")
        if isinstance(logs_base, Path):
            return logs_base / "traces"
        else:
            # For cloud storage, append /traces to the logs path
            return f"{logs_base}/traces"

    @classmethod
    def from_env(cls) -> TraceConfig:
        """
        Create config from environment variables.

                Checks for:
                - ACO_PROFILE (for storage profile)
                - ACO_TRACE_ENABLED (default: true)
                - ACO_TRACE_EXPORTER (default: console)
                - ACO_TRACE_ENDPOINT (for OTLP)
                - ACO_TRACE_SAMPLE_RATE (default: 1.0)
                - ACO_TRACE_SERVICE_NAME (default: acoharmony)

                Returns

                TraceConfig
                    Configuration from environment.
        """
        from .._store import StorageBackend

        # Create storage config from environment profile
        profile = os.getenv("ACO_PROFILE", "local")
        try:
            storage_config = StorageBackend(profile=profile)
        except Exception as e:
            from .._exceptions import StorageBackendError

            raise StorageBackendError.from_initialization_error(e, profile) from e

        enabled = os.getenv("ACO_TRACE_ENABLED", "true").lower() == "true"
        exporter_type = os.getenv("ACO_TRACE_EXPORTER", "file")  # Default to file exporter
        otlp_endpoint = os.getenv("ACO_TRACE_ENDPOINT")
        sample_rate = float(os.getenv("ACO_TRACE_SAMPLE_RATE", "1.0"))
        service_name = os.getenv("ACO_TRACE_SERVICE_NAME", "acoharmony")

        return cls(
            storage_config=storage_config,
            enabled=enabled,
            exporter_type=exporter_type,
            otlp_endpoint=otlp_endpoint,
            sample_rate=sample_rate,
            service_name=service_name,
        )

    @classmethod
    def from_storage(cls, storage_config) -> TraceConfig:
        """
        Create config from storage configuration.

                Parameters

                storage_config : StorageBackend
                    Storage configuration instance.

                Returns

                TraceConfig
                    Configuration using the storage config.
        """
        return cls(storage_config=storage_config)

    def create_exporter(self) -> SpanExporter | None:
        """
        Create the appropriate span exporter based on configuration.

                Returns

                SpanExporter or None
                    Configured exporter, or None if exporter_type is "none".
        """
        if not self.enabled or self.exporter_type == "none":
            return None

        if self.exporter_type == "console":
            return ConsoleSpanExporter()

        elif self.exporter_type == "file":
            # Import the file exporter (we'll create this)
            from .exporters import FileSpanExporter

            trace_path = self.get_base_path()
            return FileSpanExporter(base_path=trace_path)

        elif self.exporter_type == "otlp":
            if not self.otlp_endpoint:
                raise ValueError("otlp_endpoint must be set when using OTLP exporter")
            return OTLPSpanExporter(endpoint=self.otlp_endpoint)

        elif self.exporter_type == "jaeger":
            # Jaeger uses OTLP now
            from opentelemetry.exporter.jaeger.thrift import JaegerExporter

            return JaegerExporter()

        else:
            raise ValueError(f"Unknown exporter type: {self.exporter_type}")


# Global configuration instance
_config: TraceConfig | None = None
_tracer_provider: TracerProvider | None = None


def shutdown_tracing():
    """
    Shutdown tracing and clean up resources.

        Should be called at application exit to ensure all spans are flushed
        and resources are properly released.
    """
    global _tracer_provider, _config

    if _tracer_provider is not None:
        try:
            _tracer_provider.force_flush(timeout_millis=5000)
            _tracer_provider.shutdown()
        except Exception:  # ALLOWED: Cleanup operation, errors during cleanup are non-critical
            # Ignore shutdown errors
            pass
        _tracer_provider = None

    _config = None


def setup_tracing(config: TraceConfig | None = None, profile: str | None = None) -> TraceConfig:
    """
    Setup global OpenTelemetry tracing configuration.

        Parameters

        config : TraceConfig, optional
            Configuration to use. If None, creates from environment.
        profile : str, optional
            Storage profile to use (overrides environment).

        Returns

        TraceConfig
            The active configuration.
    """
    global _config, _tracer_provider

    # Check if already configured
    if _config is not None and config is None and _tracer_provider is not None:
        return _config

    if config is None:
        if profile:
            # Create config with specific profile
            from .._store import StorageBackend

            storage_config = StorageBackend(profile=profile)
            config = TraceConfig.from_storage(storage_config)
        else:
            # Create from environment
            config = TraceConfig.from_env()

    _config = config

    if not config.enabled:
        # Use a no-op tracer provider
        from opentelemetry.trace import NoOpTracerProvider

        _tracer_provider = NoOpTracerProvider()
        trace.set_tracer_provider(_tracer_provider)
        return config

    # Only set up if not already configured
    if _tracer_provider is None:
        # Create resource with service name
        resource = Resource.create(
            {
                "service.name": config.service_name,
                "service.version": _get_version(),
            }
        )

        # Create tracer provider
        _tracer_provider = TracerProvider(resource=resource)

        # Add exporter
        exporter = config.create_exporter()
        if exporter:
            span_processor = BatchSpanProcessor(exporter)
            _tracer_provider.add_span_processor(span_processor)
            config._exporters.append(exporter)

        # Set as global tracer provider
        trace.set_tracer_provider(_tracer_provider)

        # Log the configuration (using standard logging)
        import logging

        logger = logging.getLogger("acoharmony.trace")
        base_path = config.get_base_path() if config.exporter_type == "file" else "N/A"
        logger.info(
            "Tracing configured",
            extra={
                "exporter": config.exporter_type,
                "service": config.service_name,
                "sample_rate": config.sample_rate,
                "path": str(base_path),
            },
        )

    return config


def get_tracer(name: str) -> trace.Tracer:
    """
    Get a tracer instance.

        Parameters

        name : str
            Name of the tracer (typically module name).

        Returns

        Tracer
            Configured tracer instance.
    """
    # Ensure tracing is setup
    global _config
    if _config is None:
        setup_tracing()

    return trace.get_tracer(f"acoharmony.{name}")


def get_trace_config() -> TraceConfig:
    """
    Get the current trace configuration.

        Returns

        TraceConfig
            Current configuration, or creates default if not initialized.
    """
    global _config
    if _config is None:
        setup_tracing()
    return _config


def _get_version() -> str:
    """Get the package version."""
    try:
        from .. import __version__

        return __version__
    except (
        Exception
    ):  # ALLOWED: Version fallback - infrastructure, returns default if version unavailable
        return "unknown"
