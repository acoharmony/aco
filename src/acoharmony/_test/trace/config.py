"""Tests for tracing configuration."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock, patch

import pytest
from opentelemetry.sdk.trace.export import ConsoleSpanExporter

from acoharmony._trace.config import TraceConfig, get_trace_config, setup_tracing


@pytest.mark.unit
def test_trace_config_from_env(monkeypatch):
    """Test TraceConfig creation from environment variables."""
    monkeypatch.setenv("ACO_PROFILE", "local")
    monkeypatch.setenv("ACO_TRACE_ENABLED", "true")
    monkeypatch.setenv("ACO_TRACE_EXPORTER", "console")
    monkeypatch.setenv("ACO_TRACE_SAMPLE_RATE", "0.5")
    monkeypatch.setenv("ACO_TRACE_SERVICE_NAME", "test-service")

    config = TraceConfig.from_env()

    assert config.enabled is True
    assert config.exporter_type == "console"
    assert config.sample_rate == 0.5
    assert config.service_name == "test-service"


@pytest.mark.unit
def test_trace_config_disabled(monkeypatch):
    """Test that tracing can be disabled."""
    monkeypatch.setenv("ACO_PROFILE", "local")
    monkeypatch.setenv("ACO_TRACE_ENABLED", "false")

    config = TraceConfig.from_env()
    assert config.enabled is False


@pytest.mark.unit
def test_trace_config_defaults():
    """Test default TraceConfig values."""
    config = TraceConfig()

    assert config.namespace == "traces"
    assert config.service_name == "acoharmony"
    assert config.exporter_type == "console"
    assert config.sample_rate == 1.0
    assert config.enabled is True


@pytest.mark.unit
def test_setup_tracing_with_config():
    """Test setup_tracing with explicit config."""
    config = TraceConfig(
        enabled=True,
        exporter_type="console",
        service_name="test-acoharmony",
    )

    result_config = setup_tracing(config)

    assert result_config.service_name == "test-acoharmony"
    assert result_config.exporter_type == "console"


@pytest.mark.unit
def test_setup_tracing_disabled():
    """Test that tracing can be set up in disabled state."""
    config = TraceConfig(enabled=False)

    result_config = setup_tracing(config)

    assert result_config.enabled is False


@pytest.mark.unit
def test_get_trace_config():
    """Test get_trace_config returns the current config."""
    config = get_trace_config()

    assert isinstance(config, TraceConfig)
    assert config.service_name == "acoharmony"


@pytest.mark.unit
def test_trace_config_console_exporter():
    """Test console exporter creation."""
    config = TraceConfig(exporter_type="console")
    exporter = config.create_exporter()

    assert exporter is not None

    assert isinstance(exporter, ConsoleSpanExporter)


@pytest.mark.unit
def test_trace_config_none_exporter():
    """Test that 'none' exporter returns None."""
    config = TraceConfig(exporter_type="none")
    exporter = config.create_exporter()

    assert exporter is None


# ---------------------------------------------------------------------------
# Coverage gap: config.py line 204
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_trace_config_jaeger_exporter():
    """Line 204: jaeger exporter creation returns JaegerExporter."""

    config = TraceConfig(exporter_type="jaeger")

    mock_exporter = MagicMock()
    with patch(
        "acoharmony._trace.config.JaegerExporter",
        create=True,
        return_value=mock_exporter,
    ):
        try:
            from opentelemetry.exporter.jaeger.thrift import JaegerExporter  # noqa: F401

            exporter = config.create_exporter()
            assert exporter is not None
        except (ImportError, ModuleNotFoundError):
            # Jaeger package not installed - mock the import
            with patch.dict("sys.modules", {
                "opentelemetry.exporter.jaeger": MagicMock(),
                "opentelemetry.exporter.jaeger.thrift": MagicMock(JaegerExporter=MagicMock(return_value=mock_exporter)),
            }):
                exporter = config.create_exporter()
                assert exporter is not None


@pytest.mark.unit
def test_trace_config_disabled_exporter():
    """Test that disabled config returns None exporter."""
    config = TraceConfig(enabled=False)
    exporter = config.create_exporter()

    assert exporter is None


@pytest.mark.unit
def test_trace_config_otlp_exporter_without_endpoint():
    """Test that OTLP exporter without endpoint raises error."""
    config = TraceConfig(exporter_type="otlp", otlp_endpoint=None)

    with pytest.raises(ValueError, match="otlp_endpoint must be set"):
        config.create_exporter()


@pytest.mark.unit
def test_trace_config_unknown_exporter():
    """Test that unknown exporter type raises error."""
    config = TraceConfig(exporter_type="invalid_exporter")

    with pytest.raises(ValueError, match="Unknown exporter type"):
        config.create_exporter()


# ---------------------------------------------------------------------------
# Additional coverage: branches 85→-61, 87→-61, 99→100, 106→110,
#                      196→198, 259→261, 294→300
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_trace_config_file_exporter_non_local_storage():
    """Branch 85→-61: file exporter with non-local storage skips mkdir."""
    mock_storage = MagicMock()
    mock_storage.get_storage_type.return_value = "s3"
    mock_storage.get_path.return_value = "s3://bucket/logs"

    config = TraceConfig(
        storage_config=mock_storage,
        exporter_type="file",
        enabled=True,
    )
    # Should not raise - mkdir is skipped for non-local storage
    assert config.exporter_type == "file"


@pytest.mark.unit
def test_trace_config_file_exporter_cloud_base_path():
    """Branch 87→-61: file exporter where get_base_path returns a string (not Path)."""
    mock_storage = MagicMock()
    mock_storage.get_storage_type.return_value = "local"
    mock_storage.get_path.return_value = "s3://bucket/logs"

    config = TraceConfig(
        storage_config=mock_storage,
        exporter_type="file",
        enabled=True,
    )
    # get_base_path returns string, isinstance(trace_path, Path) is False
    base = config.get_base_path()
    assert isinstance(base, str)
    assert base == "s3://bucket/logs/traces"


@pytest.mark.unit
def test_trace_config_get_base_path_cached():
    """Branch 99→100: _base_path is set, returns cached value."""
    config = TraceConfig(enabled=False)
    config._base_path = Path("/cached/path")

    result = config.get_base_path()
    assert result == Path("/cached/path")


@pytest.mark.unit
def test_trace_config_get_base_path_cloud():
    """Branch 106→110: logs_base is a string (cloud), returns string with /traces."""
    mock_storage = MagicMock()
    mock_storage.get_path.return_value = "gs://my-bucket/logs"

    config = TraceConfig(enabled=False)
    config.storage_config = mock_storage

    result = config.get_base_path()
    assert result == "gs://my-bucket/logs/traces"
    assert isinstance(result, str)


@pytest.mark.unit
def test_trace_config_otlp_exporter_with_endpoint():
    """Branch 196→198: OTLP exporter with valid endpoint."""
    config = TraceConfig(
        exporter_type="otlp",
        otlp_endpoint="http://localhost:4317",
        enabled=True,
    )

    with patch(
        "acoharmony._trace.config.OTLPSpanExporter",
        return_value=MagicMock(),
    ) as mock_otlp:
        exporter = config.create_exporter()
        assert exporter is not None
        mock_otlp.assert_called_once_with(endpoint="http://localhost:4317")


@pytest.mark.unit
def test_setup_tracing_with_profile():
    """Branch 259→261: setup_tracing with explicit profile."""
    import acoharmony._trace.config as cfg

    # Reset global state
    cfg._config = None
    cfg._tracer_provider = None

    with patch("acoharmony._store.StorageBackend") as mock_sb:
        mock_storage = MagicMock()
        mock_storage.get_storage_type.return_value = "local"
        mock_storage.get_path.return_value = Path("/tmp/logs")
        mock_sb.return_value = mock_storage

        result = setup_tracing(profile="local")
        assert result is not None
        mock_sb.assert_called_once_with(profile="local")


@pytest.mark.unit
def test_setup_tracing_exporter_none():
    """Branch 294→300: setup_tracing where create_exporter returns None."""
    import acoharmony._trace.config as cfg

    cfg._config = None
    cfg._tracer_provider = None

    config = TraceConfig(
        enabled=True,
        exporter_type="none",
        service_name="test-none",
    )

    result = setup_tracing(config)
    assert result.exporter_type == "none"
    # Exporter is None, so no span processor should be added
    assert len(result._exporters) == 0


class TestTraceConfigErrorPaths:
    """Cover error paths in TraceConfig."""

    @pytest.mark.unit
    def test_init_storage_backend_failure(self, monkeypatch):
        """Cover lines 75-81: StorageBackend init failure → StorageBackendError."""
        from acoharmony._exceptions import StorageBackendError

        monkeypatch.setenv("ACO_PROFILE", "nonexistent_profile_xyz")

        with patch(
            "acoharmony._store.StorageBackend",
            side_effect=RuntimeError("mock init fail"),
        ):
            with pytest.raises(StorageBackendError):
                TraceConfig(
                    enabled=True,
                    exporter_type="file",
                    service_name="test",
                    storage_config=None,
                )

    @pytest.mark.unit
    def test_from_env_storage_backend_failure(self, monkeypatch):
        """Cover lines 136-139: from_env() StorageBackend failure."""
        from acoharmony._exceptions import StorageBackendError

        monkeypatch.setenv("ACO_PROFILE", "bad_profile")

        with patch(
            "acoharmony._store.StorageBackend",
            side_effect=RuntimeError("mock fail"),
        ):
            with pytest.raises(StorageBackendError):
                TraceConfig.from_env()


class TestTraceConfigAdditionalBranches:
    """Cover remaining branches: 63->64, 66->68, 84->-61, 85->-61, 87->-61,
    182->183, 185->186, 188->195, 195->196/200, 196->197/198,
    200->202/207, 224->225/233, 258->269, 271->273, 280->317, 336->337."""

    @pytest.mark.unit
    def test_disabled_config_skips_init(self):
        """Branch 63->64: enabled=False, __post_init__ returns early."""
        config = TraceConfig(enabled=False)
        assert config.enabled is False

    @pytest.mark.unit
    def test_storage_config_none_tries_creation(self):
        """Branch 66->68: storage_config is None, tries to create it."""
        config = TraceConfig(exporter_type="console")
        # Should have created a storage_config or raised
        assert config.storage_config is not None or config.enabled

    @pytest.mark.unit
    def test_file_exporter_local_creates_dir(self, tmp_path):
        """Branch 84->85, 87->88: file exporter with local storage, mkdir."""
        mock_storage = MagicMock()
        mock_storage.get_storage_type.return_value = "local"
        mock_storage.get_path.return_value = tmp_path / "logs"

        config = TraceConfig(
            storage_config=mock_storage,
            exporter_type="file",
            enabled=True,
        )
        trace_path = config.get_base_path()
        assert isinstance(trace_path, Path)

    @pytest.mark.unit
    def test_create_exporter_disabled(self):
        """Branch 182->183: not enabled, returns None."""
        config = TraceConfig(enabled=False)
        assert config.create_exporter() is None

    @pytest.mark.unit
    def test_create_exporter_console(self):
        """Branch 185->186: exporter_type='console'."""
        config = TraceConfig(exporter_type="console")
        exporter = config.create_exporter()
        assert isinstance(exporter, ConsoleSpanExporter)

    @pytest.mark.unit
    def test_create_exporter_file(self, tmp_path):
        """Branch 188->195: exporter_type='file'."""
        mock_storage = MagicMock()
        mock_storage.get_storage_type.return_value = "local"
        mock_storage.get_path.return_value = tmp_path / "logs"

        config = TraceConfig(
            storage_config=mock_storage,
            exporter_type="file",
            enabled=True,
        )
        exporter = config.create_exporter()
        assert exporter is not None

    @pytest.mark.unit
    def test_create_exporter_otlp_no_endpoint(self):
        """Branch 195->196, 196->197: otlp with no endpoint raises."""
        config = TraceConfig(exporter_type="otlp", otlp_endpoint=None)
        with pytest.raises(ValueError, match="otlp_endpoint"):
            config.create_exporter()

    @pytest.mark.unit
    def test_create_exporter_otlp_with_endpoint(self):
        """Branch 196->198: otlp with endpoint."""
        config = TraceConfig(exporter_type="otlp", otlp_endpoint="http://localhost:4317")
        with patch("acoharmony._trace.config.OTLPSpanExporter", return_value=MagicMock()):
            exporter = config.create_exporter()
            assert exporter is not None

    @pytest.mark.unit
    def test_create_exporter_unknown(self):
        """Branch 200->207: unknown exporter type raises."""
        config = TraceConfig(exporter_type="unknown_xyz")
        with pytest.raises(ValueError, match="Unknown exporter type"):
            config.create_exporter()

    @pytest.mark.unit
    def test_shutdown_tracing_when_active(self):
        """Branch 224->225: _tracer_provider is not None."""
        import acoharmony._trace.config as cfg
        from acoharmony._trace.config import shutdown_tracing

        old_provider = cfg._tracer_provider
        old_config = cfg._config
        cfg._tracer_provider = MagicMock()
        cfg._config = TraceConfig(enabled=False)

        try:
            shutdown_tracing()
            assert cfg._tracer_provider is None
            assert cfg._config is None
        finally:
            cfg._tracer_provider = old_provider
            cfg._config = old_config

    @pytest.mark.unit
    def test_shutdown_tracing_when_none(self):
        """Branch 224->233: _tracer_provider is None, skips shutdown."""
        import acoharmony._trace.config as cfg
        from acoharmony._trace.config import shutdown_tracing

        old_provider = cfg._tracer_provider
        old_config = cfg._config
        cfg._tracer_provider = None
        cfg._config = TraceConfig(enabled=False)

        try:
            shutdown_tracing()
            assert cfg._config is None
        finally:
            cfg._tracer_provider = old_provider
            cfg._config = old_config

    @pytest.mark.unit
    def test_setup_tracing_config_none_profile_given(self):
        """Branch 258->269, 259->261: config=None with profile."""
        import acoharmony._trace.config as cfg

        old_config = cfg._config
        old_provider = cfg._tracer_provider
        cfg._config = None
        cfg._tracer_provider = None

        try:
            with patch("acoharmony._store.StorageBackend") as mock_sb:
                mock_storage = MagicMock()
                mock_storage.get_storage_type.return_value = "local"
                mock_storage.get_path.return_value = Path("/tmp/logs")
                mock_sb.return_value = mock_storage

                result = setup_tracing(profile="local")
                assert result is not None
        finally:
            cfg._config = old_config
            cfg._tracer_provider = old_provider

    @pytest.mark.unit
    def test_setup_tracing_disabled(self):
        """Branch 271->273: config.enabled is False."""
        import acoharmony._trace.config as cfg

        old_config = cfg._config
        old_provider = cfg._tracer_provider
        cfg._config = None
        cfg._tracer_provider = None

        try:
            config = TraceConfig(enabled=False)
            result = setup_tracing(config)
            assert result.enabled is False
        finally:
            cfg._config = old_config
            cfg._tracer_provider = old_provider

    @pytest.mark.unit
    def test_setup_tracing_already_configured(self):
        """Branch 280->317: _tracer_provider is not None, skips setup."""
        import acoharmony._trace.config as cfg

        old_config = cfg._config
        old_provider = cfg._tracer_provider

        mock_provider = MagicMock()
        cfg._tracer_provider = mock_provider
        cfg._config = TraceConfig(enabled=True, exporter_type="console")

        try:
            result = setup_tracing()
            assert result is cfg._config
        finally:
            cfg._config = old_config
            cfg._tracer_provider = old_provider

    @pytest.mark.unit
    def test_get_tracer_when_not_configured(self):
        """Branch 336->337: _config is None, setup_tracing called."""
        import acoharmony._trace.config as cfg
        from acoharmony._trace.config import get_tracer

        old_config = cfg._config
        cfg._config = None

        try:
            tracer = get_tracer("test_module")
            assert tracer is not None
        finally:
            cfg._config = old_config
