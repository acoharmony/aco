"""Tests for TracerWrapper."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import patch

import pytest
from opentelemetry import trace

from acoharmony._trace import TracerWrapper
from acoharmony._trace.config import TraceConfig, setup_tracing
from acoharmony._trace.decorators import traced


@pytest.fixture
def tracer():
    """Create a test tracer with console exporter."""
    config = TraceConfig(enabled=True, exporter_type="console")
    setup_tracing(config)
    return TracerWrapper("test_component")


@pytest.mark.unit
def test_tracer_wrapper_initialization(tracer):
    """Test TracerWrapper initialization."""
    assert tracer.name == "test_component"
    assert tracer.config is not None
    assert tracer.tracer is not None


@pytest.mark.unit
def test_tracer_span_context_manager(tracer):
    """Test span creation with context manager."""
    with tracer.span("test_operation", test_attr="value") as span:
        assert span is not None
        # Verify we're in a span context
        current_span = trace.get_current_span()
        assert current_span.is_recording()


@pytest.mark.unit
def test_tracer_span_with_exception(tracer):
    """Test span records exceptions properly."""
    with pytest.raises(ValueError, match=r".*"):
        with tracer.span("test_operation"):
            raise ValueError("Test error")


@pytest.mark.unit
def test_tracer_set_attribute(tracer):
    """Test setting attributes on current span."""
    with tracer.span("test_operation"):
        tracer.set_attribute("key", "value")
        # Should not raise an error


@pytest.mark.unit
def test_tracer_set_attributes(tracer):
    """Test setting multiple attributes."""
    with tracer.span("test_operation"):
        tracer.set_attributes({"key1": "value1", "key2": "value2"})


@pytest.mark.unit
def test_tracer_add_event(tracer):
    """Test adding events to span."""
    with tracer.span("test_operation"):
        tracer.add_event("test_event", detail="test detail")


@pytest.mark.unit
def test_tracer_record_exception(tracer):
    """Test recording exceptions."""
    with tracer.span("test_operation"):
        try:
            raise ValueError("Test error")
        except ValueError as e:
            tracer.record_exception(e)


@pytest.mark.unit
def test_tracer_get_trace_id(tracer):
    """Test getting current trace ID."""
    with tracer.span("test_operation"):
        trace_id = tracer.get_current_trace_id()
        assert trace_id is not None
        assert isinstance(trace_id, str)
        assert len(trace_id) == 32  # Hex string of 128-bit ID


@pytest.mark.unit
def test_tracer_get_span_id(tracer):
    """Test getting current span ID."""
    with tracer.span("test_operation"):
        span_id = tracer.get_current_span_id()
        assert span_id is not None
        assert isinstance(span_id, str)
        assert len(span_id) == 16  # Hex string of 64-bit ID


@pytest.mark.unit
def test_tracer_disabled():
    """Test tracer when tracing is disabled."""
    config = TraceConfig(enabled=False)
    setup_tracing(config)
    tracer = TracerWrapper("test_component")

    # Should work without errors even when disabled
    with tracer.span("test_operation"):
        tracer.set_attribute("key", "value")
        tracer.add_event("event")


@pytest.mark.unit
def test_tracer_nested_spans(tracer):
    """Test nested span creation."""
    with tracer.span("outer_operation"):
        outer_trace_id = tracer.get_current_trace_id()

        with tracer.span("inner_operation"):
            inner_trace_id = tracer.get_current_trace_id()

            # Nested spans should have same trace ID
            assert outer_trace_id == inner_trace_id


@pytest.mark.unit
def test_tracer_span_with_metrics(tracer):
    """Test span with automatic metrics tracking."""
    with tracer.span_with_metrics("test_operation"):
        # Do some work
        result = sum(range(100))
        assert result > 0


# ---------------------------------------------------------------------------
# Coverage gap tests: tracer.py lines 245, 247
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_traced_function_parameter_extraction_exception(tracer):
    """Lines 245, 247: exception during parameter extraction is silently ignored."""


    @traced()
    def my_func(x, y):
        return x + y

    # Even if inspect.signature raises, the function should still work
    with patch("acoharmony._trace.decorators.inspect.signature", side_effect=ValueError("bad")):
        result = my_func(1, 2)
    assert result == 3


# ---------------------------------------------------------------------------
# Additional coverage: tracer.py branches
# 146→-131, 164→-149, 180→-167, 199→-184, 243→242
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_add_event_no_current_span(tracer):
    """Branch 146→-131: add_event when there is no active span (no-op)."""
    # Outside any span context, current_span is a no-op span
    # The code checks `if current_span:` - NonRecordingSpan is truthy but
    # we need to test when it's falsy. We mock get_current_span to return None.
    with patch.object(trace, "get_current_span", return_value=None):
        tracer.add_event("test_event", key="value")
        # Should not raise


@pytest.mark.unit
def test_set_attribute_no_current_span(tracer):
    """Branch 164→-149: set_attribute when there is no active span."""
    with patch.object(trace, "get_current_span", return_value=None):
        tracer.set_attribute("key", "value")
        # Should not raise


@pytest.mark.unit
def test_set_attributes_no_current_span(tracer):
    """Branch 180→-167: set_attributes when there is no active span."""
    with patch.object(trace, "get_current_span", return_value=None):
        tracer.set_attributes({"key1": "v1", "key2": "v2"})
        # Should not raise


@pytest.mark.unit
def test_record_exception_no_current_span(tracer):
    """Branch 199→-184: record_exception when there is no active span."""
    with patch.object(trace, "get_current_span", return_value=None):
        tracer.record_exception(ValueError("test"), detail="info")
        # Should not raise


@pytest.mark.unit
def test_trace_function_skips_self_and_complex_types(tracer):
    """Branch 243→242: trace_function skips 'self' param and non-simple types."""

    class MyClass:
        def my_method(self, data, count):
            return count

    obj = MyClass()
    wrapped = tracer.trace_function(obj.my_method)
    # data is a dict (non-simple type), should be skipped
    # 'self' should be skipped
    result = wrapped({"complex": True}, 42)
    assert result == 42
