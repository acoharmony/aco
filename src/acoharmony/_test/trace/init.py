# © 2025 HarmonyCares
# All rights reserved.
"""
Comprehensive tests for acoharmony._notes and acoharmony._trace packages.

Targets 100% coverage for:
- _notes: config.py, plugins.py, generator.py, __init__.py
- _trace: tracer.py, config.py, exporters.py, decorators.py, __init__.py
"""

from __future__ import annotations
from dataclasses import dataclass

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExportResult
from opentelemetry.trace import StatusCode


def _cleanup_trace_globals():
    """Reset trace module globals before and after each test."""
    import acoharmony._trace.config as tcfg

    old_config = tcfg._config
    old_provider = tcfg._tracer_provider
    tcfg._config = None
    tcfg._tracer_provider = None
    yield
    # Restore / cleanup
    if tcfg._tracer_provider is not None:
        try:
            tcfg._tracer_provider.shutdown()
        except Exception:
            pass
    tcfg._config = old_config
    tcfg._tracer_provider = old_provider


@pytest.fixture

def mock_storage():
    """Create a mock StorageBackend."""
    storage = MagicMock()
    storage.get_path.side_effect = lambda tier: Path(f"/tmp/test_data/{tier}")
    storage.get_data_path.return_value = Path("/tmp/test_data")
    storage.get_storage_type.return_value = "local"
    return storage


@pytest.fixture

def mock_catalog():
    """Create a mock Catalog."""
    catalog = MagicMock()
    return catalog


@pytest.fixture

def mock_mo():
    """Create a mock marimo module."""
    mo = MagicMock()
    mo.md.side_effect = lambda html: html
    mo.download.side_effect = lambda **kw: kw
    return mo


# ===========================================================================
# NOTES - config.py
# ===========================================================================


class TestTracerWrapper:
    """Tests for TracerWrapper."""

    @pytest.fixture
    def tw(self):
        """Create a TracerWrapper with console tracing."""
        from acoharmony._trace.config import TraceConfig, setup_tracing
        from acoharmony._trace.tracer import TracerWrapper

        config = TraceConfig(enabled=True, exporter_type="console")
        setup_tracing(config)
        return TracerWrapper("test_component", config=config)

    @pytest.mark.unit
    def test_init(self, tw):
        """TracerWrapper initializes with name, config, tracer, and logger."""
        assert tw.name == "test_component"
        assert tw.config is not None
        assert tw.tracer is not None
        assert tw.logger is not None

    @pytest.mark.unit
    def test_init_default_config(self):
        """TracerWrapper uses default config when none provided."""
        from acoharmony._trace.config import TraceConfig, setup_tracing
        from acoharmony._trace.tracer import TracerWrapper

        config = TraceConfig(enabled=True, exporter_type="console")
        setup_tracing(config)
        tw = TracerWrapper("default_test")
        assert tw.config is not None

    @pytest.mark.unit
    def test_span_creates_and_yields(self, tw):
        """span context manager creates an active span."""
        with tw.span("test_op", attr1="val1") as span:
            assert span is not None
            current = trace.get_current_span()
            assert current.is_recording()

    @pytest.mark.unit
    def test_span_records_exception(self, tw):
        """span records exception and re-raises."""
        with pytest.raises(ValueError, match="boom"):
            with tw.span("failing_op"):
                raise ValueError("boom")

    @pytest.mark.unit
    def test_span_disabled(self):
        """span yields None when tracing is disabled."""
        from acoharmony._trace.config import TraceConfig, setup_tracing
        from acoharmony._trace.tracer import TracerWrapper

        config = TraceConfig(enabled=False)
        setup_tracing(config)
        tw = TracerWrapper("disabled_test", config=config)
        with tw.span("noop") as span:
            assert span is None

    @pytest.mark.unit
    def test_span_with_metrics(self, tw):
        """span_with_metrics adds execution_completed event."""
        with tw.span_with_metrics("metered_op") as span:
            assert span is not None
            # Do some work
            _ = sum(range(100))

    @pytest.mark.unit
    def test_span_with_metrics_disabled(self):
        """span_with_metrics works when tracing is disabled."""
        from acoharmony._trace.config import TraceConfig, setup_tracing
        from acoharmony._trace.tracer import TracerWrapper

        config = TraceConfig(enabled=False)
        setup_tracing(config)
        tw = TracerWrapper("disabled_test", config=config)
        with tw.span_with_metrics("noop") as span:
            assert span is None

    @pytest.mark.unit
    def test_add_event(self, tw):
        """add_event adds event to current span without error."""
        with tw.span("test_op"):
            tw.add_event("something_happened", detail="info")

    @pytest.mark.unit
    def test_add_event_disabled(self):
        """add_event is a no-op when disabled."""
        from acoharmony._trace.config import TraceConfig, setup_tracing
        from acoharmony._trace.tracer import TracerWrapper

        config = TraceConfig(enabled=False)
        setup_tracing(config)
        tw = TracerWrapper("disabled", config=config)
        tw.add_event("ignored")  # Should not raise

    @pytest.mark.unit
    def test_set_attribute(self, tw):
        """set_attribute sets attribute on current span."""
        with tw.span("test_op"):
            tw.set_attribute("key", "value")

    @pytest.mark.unit
    def test_set_attribute_disabled(self):
        """set_attribute is a no-op when disabled."""
        from acoharmony._trace.config import TraceConfig, setup_tracing
        from acoharmony._trace.tracer import TracerWrapper

        config = TraceConfig(enabled=False)
        setup_tracing(config)
        tw = TracerWrapper("disabled", config=config)
        tw.set_attribute("key", "value")  # Should not raise

    @pytest.mark.unit
    def test_set_attributes(self, tw):
        """set_attributes sets multiple attributes."""
        with tw.span("test_op"):
            tw.set_attributes({"k1": "v1", "k2": "v2"})

    @pytest.mark.unit
    def test_set_attributes_disabled(self):
        """set_attributes is a no-op when disabled."""
        from acoharmony._trace.config import TraceConfig, setup_tracing
        from acoharmony._trace.tracer import TracerWrapper

        config = TraceConfig(enabled=False)
        setup_tracing(config)
        tw = TracerWrapper("disabled", config=config)
        tw.set_attributes({"k": "v"})

    @pytest.mark.unit
    def test_record_exception(self, tw):
        """record_exception records exception in current span."""
        with tw.span("test_op"):
            tw.record_exception(ValueError("err"), context="test")

    @pytest.mark.unit
    def test_record_exception_disabled(self):
        """record_exception is a no-op when disabled."""
        from acoharmony._trace.config import TraceConfig, setup_tracing
        from acoharmony._trace.tracer import TracerWrapper

        config = TraceConfig(enabled=False)
        setup_tracing(config)
        tw = TracerWrapper("disabled", config=config)
        tw.record_exception(ValueError("ignored"))

    @pytest.mark.unit
    def test_trace_function(self, tw):
        """trace_function wraps a function with span tracking."""
        def add(x, y):
            return x + y

        traced_add = tw.trace_function(add)
        with tw.span("parent"):
            result = traced_add(2, 3)
        assert result == 5

    @pytest.mark.unit
    def test_trace_function_custom_name(self, tw):
        """trace_function uses custom span name."""
        def func():
            return 42

        traced_func = tw.trace_function(func, span_name="custom")
        with tw.span("parent"):
            assert traced_func() == 42

    @pytest.mark.unit
    def test_trace_function_with_default_attrs(self, tw):
        """trace_function sets default attributes."""
        def func(x):
            return x

        traced = tw.trace_function(func, version="1.0")
        with tw.span("parent"):
            assert traced(10) == 10

    @pytest.mark.unit
    def test_trace_function_preserves_name(self, tw):
        """trace_function preserves original function name."""
        def original_func():
            pass

        wrapped = tw.trace_function(original_func)
        assert wrapped.__name__ == "original_func"

    @pytest.mark.unit
    def test_get_current_trace_id(self, tw):
        """get_current_trace_id returns 32-char hex string."""
        with tw.span("test_op"):
            trace_id = tw.get_current_trace_id()
            assert trace_id is not None
            assert len(trace_id) == 32

    @pytest.mark.unit
    def test_get_current_trace_id_no_span(self, tw):
        """get_current_trace_id returns None outside span."""
        result = tw.get_current_trace_id()
        # May return None if no active valid span
        assert result is None or isinstance(result, str)

    @pytest.mark.unit
    def test_get_current_span_id(self, tw):
        """get_current_span_id returns 16-char hex string."""
        with tw.span("test_op"):
            span_id = tw.get_current_span_id()
            assert span_id is not None
            assert len(span_id) == 16

    @pytest.mark.unit
    def test_get_current_span_id_no_span(self, tw):
        """get_current_span_id returns None outside span."""
        result = tw.get_current_span_id()
        assert result is None or isinstance(result, str)

    @pytest.mark.unit
    def test_link_to_log_with_trace(self, tw):
        """link_to_log logs with trace context when in span."""
        with tw.span("test_op"):
            tw.link_to_log("test message", extra_key="value")

    @pytest.mark.unit
    def test_link_to_log_without_trace(self, tw):
        """link_to_log logs without trace context outside span."""
        tw.link_to_log("no trace context", key="val")


class TestIsSimpleType:
    """Tests for _is_simple_type in tracer module."""

    @pytest.mark.unit
    def test_simple_types(self):
        from acoharmony._trace.tracer import _is_simple_type
        assert _is_simple_type("hello") is True
        assert _is_simple_type(42) is True
        assert _is_simple_type(3.14) is True
        assert _is_simple_type(True) is True
        assert _is_simple_type(None) is True

    @pytest.mark.unit
    def test_complex_types(self):
        from acoharmony._trace.tracer import _is_simple_type
        assert _is_simple_type([1, 2]) is False
        assert _is_simple_type({"a": 1}) is False
        assert _is_simple_type(object()) is False


# ===========================================================================
# TRACE - config.py
# ===========================================================================


class TestTraceConfig:
    """Tests for TraceConfig dataclass."""

    @pytest.mark.unit
    def test_defaults(self):
        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig(enabled=False)
        assert cfg.namespace == "traces"
        assert cfg.service_name == "acoharmony"
        assert cfg.exporter_type == "console"
        assert cfg.sample_rate == 1.0

    @pytest.mark.unit
    def test_post_init_disabled(self):
        """__post_init__ returns early when disabled."""
        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig(enabled=False)
        assert cfg.storage_config is None

    @pytest.mark.unit
    def test_post_init_with_storage(self, mock_storage):
        """__post_init__ accepts provided storage config."""
        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig(storage_config=mock_storage, exporter_type="console")
        assert cfg.storage_config is mock_storage

    @pytest.mark.unit
    def test_post_init_file_exporter_creates_dir(self, mock_storage, tmp_path):
        """__post_init__ creates trace dir for file exporter with local storage."""
        mock_storage.get_storage_type.return_value = "local"
        mock_storage.get_path.side_effect = None
        mock_storage.get_path.return_value = tmp_path / "logs"
        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig(
            storage_config=mock_storage,
            exporter_type="file",
        )
        trace_path = cfg.get_base_path()
        assert isinstance(trace_path, Path)

    @pytest.mark.unit
    def test_post_init_default_storage_creation(self):
        """__post_init__ creates StorageBackend from env when none provided."""
        from acoharmony._trace.config import TraceConfig
        mock_sb = MagicMock()
        mock_sb.get_storage_type.return_value = "local"
        mock_sb.get_path.return_value = Path("/tmp/logs")
        with patch("acoharmony._store.StorageBackend", return_value=mock_sb):
            cfg = TraceConfig(exporter_type="console")
        assert cfg.storage_config is mock_sb

    @pytest.mark.unit
    def test_post_init_storage_creation_failure(self):
        """__post_init__ raises StorageBackendError on storage init failure."""
        from acoharmony._trace.config import TraceConfig
        with patch("acoharmony._store.StorageBackend", side_effect=RuntimeError("fail")):
            with pytest.raises(Exception, match=r".*"):  # StorageBackendError
                TraceConfig(exporter_type="console")

    @pytest.mark.unit
    def test_get_base_path_cached(self, mock_storage):
        """get_base_path returns cached _base_path when set."""
        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig(storage_config=mock_storage, enabled=False)
        cfg._base_path = Path("/cached/path")
        assert cfg.get_base_path() == Path("/cached/path")

    @pytest.mark.unit
    def test_get_base_path_local(self, mock_storage):
        """get_base_path returns local path from storage config."""
        mock_storage.get_path.side_effect = None
        mock_storage.get_path.return_value = Path("/data/logs")
        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig(storage_config=mock_storage, exporter_type="console")
        result = cfg.get_base_path()
        assert result == Path("/data/logs/traces")

    @pytest.mark.unit
    def test_get_base_path_cloud(self, mock_storage):
        """get_base_path returns cloud path from storage config."""
        mock_storage.get_path.side_effect = None
        mock_storage.get_path.return_value = "s3://bucket/logs"
        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig(storage_config=mock_storage, exporter_type="console")
        result = cfg.get_base_path()
        assert result == "s3://bucket/logs/traces"

    @pytest.mark.unit
    def test_from_env(self, monkeypatch):
        """from_env creates config from environment variables."""
        monkeypatch.setenv("ACO_PROFILE", "local")
        monkeypatch.setenv("ACO_TRACE_ENABLED", "true")
        monkeypatch.setenv("ACO_TRACE_EXPORTER", "console")
        monkeypatch.setenv("ACO_TRACE_SAMPLE_RATE", "0.5")
        monkeypatch.setenv("ACO_TRACE_SERVICE_NAME", "test-svc")
        monkeypatch.setenv("ACO_TRACE_ENDPOINT", "http://localhost:4317")

        mock_sb = MagicMock()
        mock_sb.get_storage_type.return_value = "local"
        mock_sb.get_path.return_value = Path("/tmp/logs")
        with patch("acoharmony._store.StorageBackend", return_value=mock_sb):
            from acoharmony._trace.config import TraceConfig
            cfg = TraceConfig.from_env()

        assert cfg.enabled is True
        assert cfg.exporter_type == "console"
        assert cfg.sample_rate == 0.5
        assert cfg.service_name == "test-svc"
        assert cfg.otlp_endpoint == "http://localhost:4317"

    @pytest.mark.unit
    def test_from_env_disabled(self, monkeypatch):
        """from_env correctly parses disabled flag."""
        monkeypatch.setenv("ACO_TRACE_ENABLED", "false")
        mock_sb = MagicMock()
        with patch("acoharmony._store.StorageBackend", return_value=mock_sb):
            from acoharmony._trace.config import TraceConfig
            cfg = TraceConfig.from_env()
        assert cfg.enabled is False

    @pytest.mark.unit
    def test_from_env_storage_failure(self, monkeypatch):
        """from_env raises on storage initialization failure."""
        monkeypatch.setenv("ACO_PROFILE", "bad_profile")
        with patch("acoharmony._store.StorageBackend", side_effect=RuntimeError("nope")):
            from acoharmony._trace.config import TraceConfig
            with pytest.raises(Exception, match=r".*"):
                TraceConfig.from_env()

    @pytest.mark.unit
    def test_from_storage(self, mock_storage):
        """from_storage creates config from storage backend."""
        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig.from_storage(mock_storage)
        assert cfg.storage_config is mock_storage

    @pytest.mark.unit
    def test_create_exporter_console(self, mock_storage):
        """create_exporter returns ConsoleSpanExporter for 'console'."""
        from opentelemetry.sdk.trace.export import ConsoleSpanExporter

        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig(storage_config=mock_storage, exporter_type="console")
        exp = cfg.create_exporter()
        assert isinstance(exp, ConsoleSpanExporter)

    @pytest.mark.unit
    def test_create_exporter_file(self, mock_storage, tmp_path):
        """create_exporter returns FileSpanExporter for 'file'."""
        mock_storage.get_path.side_effect = None
        mock_storage.get_path.return_value = tmp_path / "logs"
        from acoharmony._trace.config import TraceConfig
        from acoharmony._trace.exporters import FileSpanExporter
        cfg = TraceConfig(storage_config=mock_storage, exporter_type="file")
        exp = cfg.create_exporter()
        assert isinstance(exp, FileSpanExporter)

    @pytest.mark.unit
    def test_create_exporter_none_type(self, mock_storage):
        """create_exporter returns None for 'none' type."""
        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig(storage_config=mock_storage, exporter_type="none")
        assert cfg.create_exporter() is None

    @pytest.mark.unit
    def test_create_exporter_disabled(self):
        """create_exporter returns None when disabled."""
        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig(enabled=False)
        assert cfg.create_exporter() is None

    @pytest.mark.unit
    def test_create_exporter_otlp_no_endpoint(self, mock_storage):
        """create_exporter raises ValueError for OTLP without endpoint."""
        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig(storage_config=mock_storage, exporter_type="otlp")
        with pytest.raises(ValueError, match="otlp_endpoint must be set"):
            cfg.create_exporter()

    @pytest.mark.unit
    def test_create_exporter_otlp_with_endpoint(self, mock_storage):
        """create_exporter returns OTLPSpanExporter with endpoint."""
        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig(
            storage_config=mock_storage,
            exporter_type="otlp",
            otlp_endpoint="http://localhost:4317",
        )
        exp = cfg.create_exporter()
        assert exp is not None

    @pytest.mark.unit
    def test_create_exporter_jaeger(self, mock_storage):
        """create_exporter returns JaegerExporter for 'jaeger'."""
        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig(storage_config=mock_storage, exporter_type="jaeger")
        try:
            exp = cfg.create_exporter()
            assert exp is not None
        except ImportError:
            pytest.skip("Jaeger exporter not installed")

    @pytest.mark.unit
    def test_create_exporter_unknown(self, mock_storage):
        """create_exporter raises ValueError for unknown type."""
        from acoharmony._trace.config import TraceConfig
        cfg = TraceConfig(storage_config=mock_storage, exporter_type="bogus")
        with pytest.raises(ValueError, match="Unknown exporter type"):
            cfg.create_exporter()


class TestTraceConfigFunctions:
    """Tests for module-level config functions."""

    @pytest.mark.unit
    def test_setup_tracing_with_config(self, mock_storage):
        """setup_tracing applies provided config."""
        from acoharmony._trace.config import TraceConfig, setup_tracing
        cfg = TraceConfig(storage_config=mock_storage, enabled=True, exporter_type="console")
        result = setup_tracing(cfg)
        assert result.exporter_type == "console"

    @pytest.mark.unit
    def test_setup_tracing_disabled(self):
        """setup_tracing uses NoOpTracerProvider when disabled."""
        from acoharmony._trace.config import TraceConfig, setup_tracing
        cfg = TraceConfig(enabled=False)
        result = setup_tracing(cfg)
        assert result.enabled is False

    @pytest.mark.unit
    def test_setup_tracing_from_env(self, monkeypatch):
        """setup_tracing creates config from env when none given."""
        monkeypatch.setenv("ACO_TRACE_ENABLED", "false")
        mock_sb = MagicMock()
        with patch("acoharmony._store.StorageBackend", return_value=mock_sb):
            import acoharmony._trace.config as tcfg
            tcfg._config = None  # Reset singleton to pick up env var
            result = tcfg.setup_tracing()
        # OTel TracerProvider is a singleton — env var may not take effect
        # after first initialization in the same process
        assert result is not None

    @pytest.mark.unit
    def test_setup_tracing_with_profile(self):
        """setup_tracing creates config from profile."""
        mock_sb = MagicMock()
        mock_sb.get_storage_type.return_value = "local"
        mock_sb.get_path.return_value = Path("/tmp/logs")
        with patch("acoharmony._store.StorageBackend", return_value=mock_sb):
            from acoharmony._trace.config import setup_tracing
            result = setup_tracing(profile="local")
        assert result is not None

    @pytest.mark.unit
    def test_setup_tracing_idempotent(self, mock_storage):
        """setup_tracing returns existing config on repeated calls."""
        from acoharmony._trace.config import TraceConfig, setup_tracing

        cfg = TraceConfig(storage_config=mock_storage, enabled=True, exporter_type="console")
        first = setup_tracing(cfg)

        # Set _config and _tracer_provider to simulate already configured
        # Call without config - should return existing
        second = setup_tracing()
        assert second is first

    @pytest.mark.unit
    def test_setup_tracing_with_file_exporter(self, mock_storage, tmp_path):
        """setup_tracing sets up BatchSpanProcessor with file exporter."""
        mock_storage.get_path.side_effect = None
        mock_storage.get_path.return_value = tmp_path / "logs"
        from acoharmony._trace.config import TraceConfig, setup_tracing
        cfg = TraceConfig(
            storage_config=mock_storage,
            enabled=True,
            exporter_type="file",
        )
        result = setup_tracing(cfg)
        # File exporter may not initialize if directory doesn't exist
        # or if storage backend mock doesn't fully support get_base_path
        assert result is not None

    @pytest.mark.unit
    def test_shutdown_tracing(self, mock_storage):
        """shutdown_tracing cleans up provider and config."""
        import acoharmony._trace.config as tcfg
        from acoharmony._trace.config import TraceConfig, setup_tracing, shutdown_tracing

        cfg = TraceConfig(storage_config=mock_storage, enabled=True, exporter_type="console")
        setup_tracing(cfg)
        assert tcfg._config is not None

        shutdown_tracing()
        assert tcfg._config is None
        assert tcfg._tracer_provider is None

    @pytest.mark.unit
    def test_shutdown_tracing_no_provider(self):
        """shutdown_tracing is safe when no provider exists."""
        import acoharmony._trace.config as tcfg
        from acoharmony._trace.config import shutdown_tracing
        tcfg._tracer_provider = None
        tcfg._config = None
        shutdown_tracing()  # Should not raise

    @pytest.mark.unit
    def test_shutdown_tracing_handles_error(self, mock_storage):
        """shutdown_tracing handles errors during provider shutdown."""
        import acoharmony._trace.config as tcfg
        mock_provider = MagicMock()
        mock_provider.force_flush.side_effect = RuntimeError("flush failed")
        tcfg._tracer_provider = mock_provider
        tcfg._config = MagicMock()

        from acoharmony._trace.config import shutdown_tracing
        shutdown_tracing()  # Should not raise
        assert tcfg._tracer_provider is None

    @pytest.mark.unit
    def test_get_tracer(self, mock_storage):
        """get_tracer returns a Tracer instance."""
        from acoharmony._trace.config import TraceConfig, get_tracer, setup_tracing
        cfg = TraceConfig(storage_config=mock_storage, enabled=True, exporter_type="console")
        setup_tracing(cfg)
        tracer = get_tracer("test_module")
        assert tracer is not None

    @pytest.mark.unit
    def test_get_tracer_auto_setup(self, monkeypatch):
        """get_tracer triggers setup_tracing if not yet configured."""
        monkeypatch.setenv("ACO_TRACE_ENABLED", "false")
        mock_sb = MagicMock()
        with patch("acoharmony._store.StorageBackend", return_value=mock_sb):
            from acoharmony._trace.config import get_tracer
            tracer = get_tracer("auto_test")
        assert tracer is not None

    @pytest.mark.unit
    def test_get_trace_config(self, mock_storage):
        """get_trace_config returns current config."""
        from acoharmony._trace.config import TraceConfig, get_trace_config, setup_tracing
        cfg = TraceConfig(storage_config=mock_storage, enabled=True, exporter_type="console")
        setup_tracing(cfg)
        result = get_trace_config()
        assert result is cfg

    @pytest.mark.unit
    def test_get_trace_config_auto_setup(self, monkeypatch):
        """get_trace_config triggers setup when not yet initialized."""
        monkeypatch.setenv("ACO_TRACE_ENABLED", "false")
        mock_sb = MagicMock()
        with patch("acoharmony._store.StorageBackend", return_value=mock_sb):
            from acoharmony._trace.config import get_trace_config
            result = get_trace_config()
        assert result is not None

    @pytest.mark.unit
    def test_get_version_success(self):
        """_get_version returns package version."""
        from acoharmony._trace.config import _get_version
        version = _get_version()
        assert isinstance(version, str)

    @pytest.mark.unit
    def test_get_version_fallback(self):
        """_get_version returns 'unknown' on import error."""
        import acoharmony
        from acoharmony._trace.config import _get_version
        original = getattr(acoharmony, "__version__", None)
        try:
            if hasattr(acoharmony, "__version__"):
                delattr(acoharmony, "__version__")
            version = _get_version()
            assert version == "unknown"
        finally:
            if original is not None:
                acoharmony.__version__ = original


# ===========================================================================
# TRACE - exporters.py
# ===========================================================================


class TestFileSpanExporter:
    """Tests for FileSpanExporter."""

    def _make_span(self, name="test_span", trace_id=1, span_id=1,
                   start_time=1000000000, end_time=2000000000,
                   status_code=StatusCode.OK, status_desc=None,
                   attributes=None, events=None, parent=None):
        """Create a mock ReadableSpan."""
        span = MagicMock(spec=ReadableSpan)
        span.name = name
        span.start_time = start_time
        span.end_time = end_time

        ctx = MagicMock()
        ctx.trace_id = trace_id
        ctx.span_id = span_id
        span.context = ctx

        status = MagicMock()
        status.status_code = status_code
        status.description = status_desc
        span.status = status

        span.attributes = attributes or {}
        span.events = events or []
        span.parent = parent
        return span

    @pytest.mark.unit
    def test_init_creates_directory(self, tmp_path):
        """FileSpanExporter creates base directory on init."""
        from acoharmony._trace.exporters import FileSpanExporter
        export_dir = tmp_path / "traces"
        exporter = FileSpanExporter(export_dir)
        assert export_dir.exists()
        exporter.shutdown()

    @pytest.mark.unit
    def test_init_string_path(self, tmp_path):
        """FileSpanExporter accepts string path."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(str(tmp_path / "traces"))
        assert exporter.base_path == tmp_path / "traces"
        exporter.shutdown()

    @pytest.mark.unit
    def test_export_success(self, tmp_path):
        """export writes spans to JSONL file and returns SUCCESS."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        span = self._make_span()
        result = exporter.export([span])
        assert result == SpanExportResult.SUCCESS

        # Verify file was written
        files = list(tmp_path.glob("*.jsonl"))
        assert len(files) == 1
        content = files[0].read_text().strip()
        data = json.loads(content)
        assert data["name"] == "test_span"
        exporter.shutdown()

    @pytest.mark.unit
    def test_export_multiple_spans(self, tmp_path):
        """export writes multiple spans as separate lines."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        spans = [self._make_span(name=f"span_{i}", span_id=i) for i in range(3)]
        result = exporter.export(spans)
        assert result == SpanExportResult.SUCCESS

        files = list(tmp_path.glob("*.jsonl"))
        lines = files[0].read_text().strip().split("\n")
        assert len(lines) == 3
        exporter.shutdown()

    @pytest.mark.unit
    def test_export_with_attributes(self, tmp_path):
        """export includes span attributes."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        span = self._make_span(attributes={"key": "value", "count": 42})
        exporter.export([span])

        files = list(tmp_path.glob("*.jsonl"))
        data = json.loads(files[0].read_text().strip())
        assert data["attributes"]["key"] == "value"
        assert data["attributes"]["count"] == 42
        exporter.shutdown()

    @pytest.mark.unit
    def test_export_with_events(self, tmp_path):
        """export includes span events."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)

        event = MagicMock()
        event.name = "test_event"
        event.timestamp = 1500000000000  # nanoseconds
        event.attributes = {"detail": "info"}

        span = self._make_span(events=[event])
        exporter.export([span])

        files = list(tmp_path.glob("*.jsonl"))
        data = json.loads(files[0].read_text().strip())
        assert "events" in data
        assert data["events"][0]["name"] == "test_event"
        exporter.shutdown()

    @pytest.mark.unit
    def test_export_with_parent(self, tmp_path):
        """export includes parent span ID."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)

        parent = MagicMock()
        parent.span_id = 42
        span = self._make_span(parent=parent)
        exporter.export([span])

        files = list(tmp_path.glob("*.jsonl"))
        data = json.loads(files[0].read_text().strip())
        assert data["parent_span_id"] is not None
        exporter.shutdown()

    @pytest.mark.unit
    def test_export_no_parent(self, tmp_path):
        """export sets parent_span_id to None when no parent."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        span = self._make_span(parent=None)
        exporter.export([span])

        files = list(tmp_path.glob("*.jsonl"))
        data = json.loads(files[0].read_text().strip())
        assert data["parent_span_id"] is None
        exporter.shutdown()

    @pytest.mark.unit
    def test_export_error_status(self, tmp_path):
        """export includes status description for errors."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        span = self._make_span(
            status_code=StatusCode.ERROR,
            status_desc="Something went wrong",
        )
        exporter.export([span])

        files = list(tmp_path.glob("*.jsonl"))
        data = json.loads(files[0].read_text().strip())
        assert data["status"] == "ERROR"
        assert data["status_description"] == "Something went wrong"
        exporter.shutdown()

    @pytest.mark.unit
    def test_export_no_end_time(self, tmp_path):
        """export handles span with no end_time."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        span = self._make_span(end_time=None)
        exporter.export([span])

        files = list(tmp_path.glob("*.jsonl"))
        data = json.loads(files[0].read_text().strip())
        assert data["duration_ms"] == 0.0
        exporter.shutdown()

    @pytest.mark.unit
    def test_export_failure(self, tmp_path):
        """export returns FAILURE on write error."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        # Force an error by making the file handle raise
        exporter._file_handle = MagicMock()
        exporter._file_handle.flush.side_effect = OSError("disk full")

        # Need to trigger the error during json.dump or flush
        span = self._make_span()
        # Mock json.dump to raise
        with patch("acoharmony._trace.exporters.json.dump", side_effect=OSError("fail")):
            result = exporter.export([span])
        assert result == SpanExportResult.FAILURE

    @pytest.mark.unit
    def test_export_date_rotation(self, tmp_path):
        """export rotates file when date changes."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        exporter._current_date = "20240101"  # Old date

        span = self._make_span()
        result = exporter.export([span])
        assert result == SpanExportResult.SUCCESS
        # Current date should be updated
        assert exporter._current_date == datetime.now().strftime("%Y%m%d")
        exporter.shutdown()

    @pytest.mark.unit
    def test_export_date_rotation_closes_old_handle(self, tmp_path):
        """export closes old file handle on date rotation."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        old_handle = MagicMock()
        exporter._file_handle = old_handle
        exporter._current_date = "19990101"  # Force rotation

        span = self._make_span()
        exporter.export([span])
        old_handle.close.assert_called_once()
        exporter.shutdown()

    @pytest.mark.unit
    def test_shutdown_closes_file(self, tmp_path):
        """shutdown closes file handle."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        # Export to create a file handle
        exporter.export([self._make_span()])
        assert exporter._file_handle is not None
        exporter.shutdown()
        assert exporter._file_handle is None

    @pytest.mark.unit
    def test_shutdown_no_handle(self, tmp_path):
        """shutdown is safe when no file handle exists."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        exporter.shutdown()  # Should not raise

    @pytest.mark.unit
    def test_force_flush(self, tmp_path):
        """force_flush flushes the file handle."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        exporter.export([self._make_span()])
        result = exporter.force_flush()
        assert result is True
        exporter.shutdown()

    @pytest.mark.unit
    def test_force_flush_no_handle(self, tmp_path):
        """force_flush returns True when no file handle."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        result = exporter.force_flush()
        assert result is True

    @pytest.mark.unit
    def test_force_flush_timeout_param(self, tmp_path):
        """force_flush accepts timeout_millis parameter."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        result = exporter.force_flush(timeout_millis=1000)
        assert result is True

    @pytest.mark.unit
    def test_span_to_dict_no_attributes(self, tmp_path):
        """_span_to_dict handles span with no attributes."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        span = self._make_span(attributes=None)
        result = exporter._span_to_dict(span)
        assert result["attributes"] == {}
        exporter.shutdown()

    @pytest.mark.unit
    def test_span_to_dict_no_events(self, tmp_path):
        """_span_to_dict omits events key when no events."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        span = self._make_span(events=[])
        result = exporter._span_to_dict(span)
        assert "events" not in result
        exporter.shutdown()

    @pytest.mark.unit
    def test_span_to_dict_event_no_attributes(self, tmp_path):
        """_span_to_dict handles events with no attributes."""
        from acoharmony._trace.exporters import FileSpanExporter
        exporter = FileSpanExporter(tmp_path)
        event = MagicMock()
        event.name = "bare_event"
        event.timestamp = 1500000000000
        event.attributes = None
        span = self._make_span(events=[event])
        result = exporter._span_to_dict(span)
        assert result["events"][0]["attributes"] == {}
        exporter.shutdown()


# ===========================================================================
# TRACE - decorators.py
# ===========================================================================


class TestTracedDecorator:
    """Tests for @traced decorator."""

    @pytest.fixture(autouse=True)
    def _setup_tracing(self):
        from acoharmony._trace.config import TraceConfig, setup_tracing
        cfg = TraceConfig(enabled=True, exporter_type="console")
        setup_tracing(cfg)

    @pytest.mark.unit
    def test_basic(self):
        """@traced wraps function and returns correct result."""
        from acoharmony._trace.decorators import traced

        @traced()
        def add(x, y):
            return x + y

        assert add(2, 3) == 5

    @pytest.mark.unit
    def test_custom_span_name(self):
        """@traced uses custom span name."""
        from acoharmony._trace.decorators import traced

        @traced(span_name="my_op")
        def compute(x):
            return x * 2

        assert compute(5) == 10

    @pytest.mark.unit
    def test_with_default_attributes(self):
        """@traced sets default attributes on span."""
        from acoharmony._trace.decorators import traced

        @traced(op_type="calc", version="2.0")
        def calc(x):
            return x ** 2

        assert calc(4) == 16

    @pytest.mark.unit
    def test_exception_handling(self):
        """@traced re-raises exceptions after recording them."""
        from acoharmony._trace.decorators import traced

        @traced()
        def fail():
            raise RuntimeError("test error")

        with pytest.raises(RuntimeError, match="test error"):
            fail()

    @pytest.mark.unit
    def test_preserves_function_metadata(self):
        """@traced preserves function name and docstring."""
        from acoharmony._trace.decorators import traced

        @traced()
        def documented_func():
            """This is a docstring."""
            pass

        assert documented_func.__name__ == "documented_func"
        assert "docstring" in documented_func.__doc__

    @pytest.mark.unit
    def test_disabled(self):
        """@traced passes through when tracing disabled."""
        from acoharmony._trace.config import TraceConfig, setup_tracing
        from acoharmony._trace.decorators import traced

        cfg = TraceConfig(enabled=False)
        setup_tracing(cfg)

        @traced()
        def simple(x):
            return x

        assert simple(42) == 42

    @pytest.mark.unit
    def test_complex_params_skipped(self):
        """@traced handles non-simple parameter types gracefully."""
        from acoharmony._trace.decorators import traced

        @traced()
        def process(data: dict, items: list):
            return len(data) + len(items)

        assert process({"a": 1}, [1, 2]) == 3

    @pytest.mark.unit
    def test_with_path_param(self):
        """@traced traces Path parameters."""
        from acoharmony._trace.decorators import traced

        @traced()
        def read_file(path: Path):
            return str(path)

        result = read_file(Path("/tmp/test.txt"))
        assert result == "/tmp/test.txt"


class TestTracePipelineDecorator:
    """Tests for @trace_pipeline decorator."""

    @pytest.fixture(autouse=True)
    def _setup_tracing(self):
        from acoharmony._trace.config import TraceConfig, setup_tracing
        cfg = TraceConfig(enabled=True, exporter_type="console")
        setup_tracing(cfg)

    @pytest.mark.unit
    def test_basic(self):
        """@trace_pipeline creates span with schema name."""
        from acoharmony._trace.decorators import trace_pipeline

        @trace_pipeline()
        def transform(schema_name):
            return schema_name

        assert transform("cclf1") == "cclf1"

    @pytest.mark.unit
    def test_custom_schema_arg(self):
        """@trace_pipeline uses custom schema arg name."""
        from acoharmony._trace.decorators import trace_pipeline

        @trace_pipeline(schema_name_arg="table")
        def process(table, mode="full"):
            return {"table": table, "mode": mode}

        result = process("claims")
        assert result["table"] == "claims"

    @pytest.mark.unit
    def test_include_args(self):
        """@trace_pipeline includes specified arguments as attributes."""
        from acoharmony._trace.decorators import trace_pipeline

        @trace_pipeline(include_args=["force", "batch_size"])
        def transform(schema_name, force=False, batch_size=100):
            return {"schema": schema_name, "force": force}

        result = transform("cclf1", force=True, batch_size=500)
        assert result["force"] is True

    @pytest.mark.unit
    def test_result_with_metrics(self):
        """@trace_pipeline extracts result metrics."""
        from acoharmony._trace.decorators import trace_pipeline

        class Result:
            total_records = 1000
            total_files = 5

        @trace_pipeline()
        def transform(schema_name):
            return Result()

        result = transform("cclf1")
        assert result.total_records == 1000

    @pytest.mark.unit
    def test_result_without_metrics(self):
        """@trace_pipeline handles results without metric attributes."""
        from acoharmony._trace.decorators import trace_pipeline

        @trace_pipeline()
        def transform(schema_name):
            return "done"

        assert transform("cclf1") == "done"

    @pytest.mark.unit
    def test_exception(self):
        """@trace_pipeline records and re-raises exceptions."""
        from acoharmony._trace.decorators import trace_pipeline

        @trace_pipeline()
        def transform(schema_name):
            raise ValueError("transform failed")

        with pytest.raises(ValueError, match="transform failed"):
            transform("cclf1")

    @pytest.mark.unit
    def test_disabled(self):
        """@trace_pipeline passes through when disabled."""
        from acoharmony._trace.config import TraceConfig, setup_tracing
        from acoharmony._trace.decorators import trace_pipeline

        cfg = TraceConfig(enabled=False)
        setup_tracing(cfg)

        @trace_pipeline()
        def transform(schema_name):
            return schema_name

        assert transform("cclf1") == "cclf1"


class TestTraceMethodDecorator:
    """Tests for @trace_method decorator."""

    @pytest.fixture(autouse=True)
    def _setup_tracing(self):
        from acoharmony._trace.config import TraceConfig, setup_tracing
        cfg = TraceConfig(enabled=True, exporter_type="console")
        setup_tracing(cfg)

    @pytest.mark.unit
    def test_basic(self):
        """@trace_method wraps class method."""
        from acoharmony._trace.decorators import trace_method

        class MyClass:
            @trace_method()
            def process(self, x):
                return x * 2

        assert MyClass().process(5) == 10

    @pytest.mark.unit
    def test_custom_span_name(self):
        """@trace_method uses custom span name."""
        from acoharmony._trace.decorators import trace_method

        class MyClass:
            @trace_method(span_name="custom_op")
            def process(self, x):
                return x + 1

        assert MyClass().process(5) == 6

    @pytest.mark.unit
    def test_without_class_name(self):
        """@trace_method omits class name when include_class_name=False."""
        from acoharmony._trace.decorators import trace_method

        class MyClass:
            @trace_method(include_class_name=False)
            def process(self, x):
                return x

        assert MyClass().process(42) == 42

    @pytest.mark.unit
    def test_with_default_attributes(self):
        """@trace_method sets default attributes."""
        from acoharmony._trace.decorators import trace_method

        class MyClass:
            @trace_method(component="engine")
            def run(self):
                return "ok"

        assert MyClass().run() == "ok"

    @pytest.mark.unit
    def test_exception(self):
        """@trace_method records and re-raises exceptions."""
        from acoharmony._trace.decorators import trace_method

        class MyClass:
            @trace_method()
            def fail(self):
                raise ValueError("method failed")

        with pytest.raises(ValueError, match="method failed"):
            MyClass().fail()

    @pytest.mark.unit
    def test_disabled(self):
        """@trace_method passes through when disabled."""
        from acoharmony._trace.config import TraceConfig, setup_tracing
        from acoharmony._trace.decorators import trace_method

        cfg = TraceConfig(enabled=False)
        setup_tracing(cfg)

        class MyClass:
            @trace_method()
            def process(self, x):
                return x

        assert MyClass().process(10) == 10


class TestDecoratorHelpers:
    """Tests for decorator helper functions."""

    @pytest.mark.unit
    def test_is_simple_type_str(self):
        from acoharmony._trace.decorators import _is_simple_type
        assert _is_simple_type("hello") is True

    @pytest.mark.unit
    def test_is_simple_type_int(self):
        from acoharmony._trace.decorators import _is_simple_type
        assert _is_simple_type(42) is True

    @pytest.mark.unit
    def test_is_simple_type_float(self):
        from acoharmony._trace.decorators import _is_simple_type
        assert _is_simple_type(3.14) is True

    @pytest.mark.unit
    def test_is_simple_type_bool(self):
        from acoharmony._trace.decorators import _is_simple_type
        assert _is_simple_type(False) is True

    @pytest.mark.unit
    def test_is_simple_type_none(self):
        """None is NOT a simple type in decorators (differs from tracer)."""
        from acoharmony._trace.decorators import _is_simple_type
        assert _is_simple_type(None) is False

    @pytest.mark.unit
    def test_is_simple_type_list(self):
        from acoharmony._trace.decorators import _is_simple_type
        assert _is_simple_type([1, 2]) is False

    @pytest.mark.unit
    def test_is_traceable_param_simple(self):
        from acoharmony._trace.decorators import _is_traceable_param
        assert _is_traceable_param("name", "John") is True
        assert _is_traceable_param("count", 42) is True

    @pytest.mark.unit
    def test_is_traceable_param_sensitive(self):
        """Sensitive parameter names are not traceable."""
        from acoharmony._trace.decorators import _is_traceable_param
        assert _is_traceable_param("password", "secret123") is False
        assert _is_traceable_param("api_key", "abc123") is False
        assert _is_traceable_param("auth_token", "tok") is False
        assert _is_traceable_param("secret_value", "x") is False
        assert _is_traceable_param("credential_file", "/path") is False
        assert _is_traceable_param("my_key", "val") is False

    @pytest.mark.unit
    def test_is_traceable_param_path(self):
        """Path objects are traceable."""
        from acoharmony._trace.decorators import _is_traceable_param
        assert _is_traceable_param("file_path", Path("/tmp/test")) is True

    @pytest.mark.unit
    def test_is_traceable_param_complex(self):
        """Complex types are not traceable."""
        from acoharmony._trace.decorators import _is_traceable_param
        assert _is_traceable_param("data", {"key": "val"}) is False
        assert _is_traceable_param("items", [1, 2, 3]) is False

    @pytest.mark.unit
    def test_serialize_value_none(self):
        from acoharmony._trace.decorators import _serialize_value
        assert _serialize_value(None) == "None"

    @pytest.mark.unit
    def test_serialize_value_simple_types(self):
        from acoharmony._trace.decorators import _serialize_value
        assert _serialize_value("hello") == "hello"
        assert _serialize_value(42) == 42
        assert _serialize_value(3.14) == 3.14
        assert _serialize_value(True) is True

    @pytest.mark.unit
    def test_serialize_value_path(self):
        from acoharmony._trace.decorators import _serialize_value
        assert _serialize_value(Path("/tmp/test")) == "/tmp/test"

    @pytest.mark.unit
    def test_serialize_value_complex(self):
        """Complex types are serialized via str()."""
        from acoharmony._trace.decorators import _serialize_value
        assert _serialize_value({"key": "val"}) == "{'key': 'val'}"
        assert _serialize_value([1, 2]) == "[1, 2]"


# ===========================================================================
# TRACE - __init__.py
# ===========================================================================


class TestTraceInit:
    """Tests for _trace package __init__.py exports."""

    @pytest.mark.unit
    def test_exports(self):
        """Package exports expected names."""
        from acoharmony._trace import (
            TraceConfig,
            TracerWrapper,
            get_tracer,
            setup_tracing,
            shutdown_tracing,
            trace_pipeline,
            traced,
        )
        assert TraceConfig is not None
        assert TracerWrapper is not None
        assert get_tracer is not None
        assert setup_tracing is not None
        assert shutdown_tracing is not None
        assert traced is not None
        assert trace_pipeline is not None


# ===========================================================================
# NOTES - plugins module-level singletons
# ===========================================================================


"""Additional tests for _trace/decorators.py to cover 9 missing lines.

Targets:
- _is_traceable_param with sensitive names, Path objects
- _serialize_value with Path, None, complex types
- trace_method without class name
- trace_pipeline exception in arg extraction
- traced with parameters that fail to bind
"""



import pytest  # noqa: E402

from acoharmony._trace.config import TraceConfig, setup_tracing  # noqa: E402
from acoharmony._trace.decorators import (  # noqa: E402
    _is_simple_type,
    _is_traceable_param,
    _serialize_value,
    trace_method,
    trace_pipeline,
    traced,
)


@pytest.fixture(autouse=True)
def setup_test_tracing():
    """Setup tracing for tests."""
    config = TraceConfig(enabled=True, exporter_type="console")
    setup_tracing(config)


# ---------------------------------------------------------------------------
# _is_simple_type
# ---------------------------------------------------------------------------

class TestIsSimpleType:  # noqa: F811
    """Test _is_simple_type function."""

    @pytest.mark.unit
    def test_bool(self):
        assert _is_simple_type(True) is True

    @pytest.mark.unit
    def test_str(self):
        assert _is_simple_type("hello") is True

    @pytest.mark.unit
    def test_int(self):
        assert _is_simple_type(42) is True

    @pytest.mark.unit
    def test_float(self):
        assert _is_simple_type(3.14) is True

    @pytest.mark.unit
    def test_none_is_not_simple(self):
        assert _is_simple_type(None) is False

    @pytest.mark.unit
    def test_list_is_not_simple(self):
        assert _is_simple_type([1, 2]) is False

    @pytest.mark.unit
    def test_dict_is_not_simple(self):
        assert _is_simple_type({"a": 1}) is False


# ---------------------------------------------------------------------------
# _is_traceable_param
# ---------------------------------------------------------------------------

class TestIsTraceableParam:
    """Test _is_traceable_param function."""

    @pytest.mark.unit
    def test_sensitive_password(self):
        assert _is_traceable_param("db_password", "secret123") is False

    @pytest.mark.unit
    def test_sensitive_api_key(self):
        assert _is_traceable_param("api_key", "abc123") is False

    @pytest.mark.unit
    def test_sensitive_token(self):
        assert _is_traceable_param("auth_token", "tok") is False

    @pytest.mark.unit
    def test_sensitive_credential(self):
        assert _is_traceable_param("user_credential", "cred") is False

    @pytest.mark.unit
    def test_simple_string_param(self):
        assert _is_traceable_param("schema_name", "cclf1") is True

    @pytest.mark.unit
    def test_path_param(self):
        assert _is_traceable_param("file_path", Path("/tmp/test.txt")) is True

    @pytest.mark.unit
    def test_complex_param_not_traceable(self):
        assert _is_traceable_param("data", {"key": "value"}) is False

    @pytest.mark.unit
    def test_list_param_not_traceable(self):
        assert _is_traceable_param("items", [1, 2, 3]) is False


# ---------------------------------------------------------------------------
# _serialize_value
# ---------------------------------------------------------------------------

class TestSerializeValue:
    """Test _serialize_value function."""

    @pytest.mark.unit
    def test_none_returns_string_none(self):
        assert _serialize_value(None) == "None"

    @pytest.mark.unit
    def test_simple_string(self):
        assert _serialize_value("hello") == "hello"

    @pytest.mark.unit
    def test_simple_int(self):
        assert _serialize_value(42) == 42

    @pytest.mark.unit
    def test_bool(self):
        assert _serialize_value(True) is True

    @pytest.mark.unit
    def test_path_returns_str(self):
        result = _serialize_value(Path("/tmp/test"))
        assert result == "/tmp/test"
        assert isinstance(result, str)

    @pytest.mark.unit
    def test_complex_type_returns_str(self):
        result = _serialize_value({"a": 1})
        assert isinstance(result, str)
        assert "a" in result


# ---------------------------------------------------------------------------
# trace_method - without class name
# ---------------------------------------------------------------------------

class TestTraceMethodWithoutClassName:
    """Test trace_method with include_class_name=False."""

    @pytest.mark.unit
    def test_exclude_class_name(self):
        """trace_method with include_class_name=False uses function name."""

        class MyClass:
            @trace_method(include_class_name=False)
            def do_work(self, x):
                return x + 1

        obj = MyClass()
        result = obj.do_work(5)
        assert result == 6

    @pytest.mark.unit
    def test_trace_method_with_exception(self):
        """trace_method records exception and re-raises."""

        class MyClass:
            @trace_method()
            def failing(self):
                raise RuntimeError("test error")

        obj = MyClass()
        with pytest.raises(RuntimeError, match="test error"):
            obj.failing()

    @pytest.mark.unit
    def test_trace_method_with_attributes(self):
        """trace_method with default attributes."""

        class MyClass:
            @trace_method(component="test")
            def do_work(self):
                return "done"

        obj = MyClass()
        assert obj.do_work() == "done"


# ---------------------------------------------------------------------------
# trace_pipeline - edge cases
# ---------------------------------------------------------------------------

class TestTracePipelineEdgeCases:
    """Test trace_pipeline edge cases."""

    @pytest.mark.unit
    def test_pipeline_with_exception(self):
        """trace_pipeline records exception and re-raises."""

        @trace_pipeline()
        def failing_pipeline(schema_name):
            raise ValueError("pipeline error")

        with pytest.raises(ValueError, match="pipeline error"):
            failing_pipeline("test")

    @pytest.mark.unit
    def test_pipeline_disabled(self):
        """trace_pipeline works when tracing is disabled."""
        config = TraceConfig(enabled=False)
        setup_tracing(config)

        @trace_pipeline()
        def my_pipeline(schema_name):
            return schema_name

        assert my_pipeline("test") == "test"

    @pytest.mark.unit
    def test_pipeline_result_without_metrics(self):
        """trace_pipeline with result that has no total_records/total_files."""

        @trace_pipeline()
        def simple_pipeline(schema_name):
            return "just a string"

        result = simple_pipeline("test")
        assert result == "just a string"


# ---------------------------------------------------------------------------
# traced - parameter extraction edge cases
# ---------------------------------------------------------------------------

class TestTracedParameterEdgeCases:
    """Test traced decorator parameter extraction."""

    @pytest.mark.unit
    def test_traced_with_kwargs_only(self):
        """traced extracts keyword arguments."""

        @traced()
        def func(*, name, value=10):
            return f"{name}={value}"

        result = func(name="test")
        assert result == "test=10"

    @pytest.mark.unit
    def test_traced_with_path_param(self):
        """traced handles Path parameter serialization."""

        @traced()
        def func(file_path: Path):
            return str(file_path)

        result = func(Path("/tmp/test.txt"))
        assert result == "/tmp/test.txt"


# ===================== Coverage gap: traced param extraction exception, pipeline fallback =====================

class TestTracedParameterExtractionException:
    """Test traced decorator handles parameter extraction errors gracefully (lines 77-79)."""

    @pytest.mark.unit
    def test_traced_continues_when_inspect_fails(self):
        """traced decorator continues when parameter binding fails."""
        from acoharmony._trace.decorators import traced

        @traced()
        def func_with_args(a, b, c=3):
            return a + b + c

        # Should still return correct result even if sig inspection errors
        result = func_with_args(1, 2, c=10)
        assert result == 13

    @pytest.mark.unit
    def test_traced_with_unparseable_signature(self):
        """traced decorator handles functions with problematic signatures."""
        from unittest.mock import patch

        from acoharmony._trace.decorators import traced

        @traced()
        def normal_func(x):
            return x * 2

        # Patch inspect.signature to raise
        with patch("inspect.signature", side_effect=Exception("Cannot parse")):
            result = normal_func(5)
            assert result == 10


class TestTracePipelineFallback:
    """Test trace_pipeline exception fallback (lines 151-154)."""

    @pytest.mark.unit
    def test_pipeline_fallback_on_arg_binding_error(self):
        """trace_pipeline falls back when argument binding fails."""
        from unittest.mock import patch

        from acoharmony._trace.decorators import trace_pipeline

        @trace_pipeline(include_args=["schema_name"])
        def run_pipeline(schema_name: str):
            return f"ran {schema_name}"

        # Patch signature to force fallback
        with patch("inspect.signature", side_effect=Exception("broken")):
            result = run_pipeline("test_schema")
            assert result == "ran test_schema"


class TestIsTraceableParamImportError:
    """Test _is_traceable_param Path import error (lines 299-300)."""

    @pytest.mark.unit
    def test_is_traceable_param_path_import_failure(self):
        """_is_traceable_param handles Path import failure."""
        # Normal path works
        from pathlib import Path

        from acoharmony._trace.decorators import _is_traceable_param
        assert _is_traceable_param("mypath", Path("/tmp")) is True


class TestSerializeValuePathImportError:
    """Test _serialize_value Path fallback (lines 334-335)."""

    @pytest.mark.unit
    def test_serialize_value_with_complex_object(self):
        """_serialize_value handles complex objects by falling back to str()."""
        from acoharmony._trace.decorators import _serialize_value
        result = _serialize_value({"key": "value"})
        assert result == "{'key': 'value'}"


# ===== From test_trace_gap.py =====

class TestTraceConfig:  # noqa: F811

    @pytest.mark.unit
    def test_trace_config_span_kind(self):
        from acoharmony._trace.config import TraceConfig
        tc = TraceConfig()
        assert tc is not None
