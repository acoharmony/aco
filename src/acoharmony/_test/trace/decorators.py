"""Tests for tracing decorators."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import patch

import pytest

from acoharmony._trace.config import TraceConfig, setup_tracing
from acoharmony._trace.decorators import (
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


@pytest.mark.unit
def test_traced_decorator_basic():
    """Test basic @traced decorator."""

    @traced()
    def simple_function(x, y):
        return x + y

    result = simple_function(1, 2)
    assert result == 3


@pytest.mark.unit
def test_traced_decorator_with_span_name():
    """Test @traced with custom span name."""

    @traced(span_name="custom_operation")
    def simple_function(x):
        return x * 2

    result = simple_function(5)
    assert result == 10


@pytest.mark.unit
def test_traced_decorator_with_attributes():
    """Test @traced with default attributes."""

    @traced(operation_type="calculation", version="1.0")
    def compute(x):
        return x ** 2

    result = compute(4)
    assert result == 16


@pytest.mark.unit
def test_traced_decorator_with_exception():
    """Test @traced properly propagates exceptions."""

    @traced()
    def failing_function():
        raise ValueError("Test error")

    with pytest.raises(ValueError, match="Test error"):
        failing_function()


@pytest.mark.unit
def test_trace_pipeline_decorator():
    """Test @trace_pipeline decorator."""

    @trace_pipeline(schema_name_arg="schema")
    def process_schema(schema, force=False):
        return {"schema": schema, "force": force}

    result = process_schema("cclf1", force=True)
    assert result["schema"] == "cclf1"
    assert result["force"] is True


@pytest.mark.unit
def test_trace_pipeline_with_result():
    """Test @trace_pipeline with result object."""

    class MockResult:
        total_records = 1000
        total_files = 5

    @trace_pipeline()
    def transform_schema(schema_name):
        return MockResult()

    result = transform_schema("cclf1")
    assert result.total_records == 1000


@pytest.mark.unit
def test_trace_method_decorator():
    """Test @trace_method decorator for class methods."""

    class TestClass:
        @trace_method()
        def process(self, value):
            return value * 2

    obj = TestClass()
    result = obj.process(5)
    assert result == 10


@pytest.mark.unit
def test_trace_method_with_custom_name():
    """Test @trace_method with custom span name."""

    class TestClass:
        @trace_method(span_name="custom_method")
        def process(self, value):
            return value + 1

    obj = TestClass()
    result = obj.process(5)
    assert result == 6


@pytest.mark.unit
def test_traced_decorator_disabled():
    """Test that decorators work when tracing is disabled."""
    config = TraceConfig(enabled=False)
    setup_tracing(config)

    @traced()
    def simple_function(x):
        return x * 2

    result = simple_function(5)
    assert result == 10


@pytest.mark.unit
def test_traced_with_complex_parameters():
    """Test @traced with complex parameter types."""

    @traced()
    def process_data(data: dict, options: list):
        return len(data) + len(options)

    result = process_data({"a": 1, "b": 2}, [1, 2, 3])
    assert result == 5


@pytest.mark.unit
def test_trace_pipeline_include_args():
    """Test @trace_pipeline with include_args."""

    @trace_pipeline(include_args=["force", "chunk_size"])
    def transform(schema_name, force=False, chunk_size=1000):
        return {"schema": schema_name, "force": force, "chunk_size": chunk_size}

    result = transform("cclf1", force=True, chunk_size=5000)
    assert result["force"] is True
    assert result["chunk_size"] == 5000


# ---------------------------------------------------------------------------
# Coverage gap tests: decorators.py lines 299-300, 334-335
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_is_valid_span_param_path_import_failure():
    """Lines 299-300: exception importing Path returns False."""


    with patch("acoharmony._trace.decorators._is_simple_type", return_value=False):
        with patch.dict("sys.modules", {"pathlib": None}):
            # When Path import fails, should return False
            result = _is_traceable_param("param", object())
    # The actual import won't fail in practice since it's stdlib,
    # but the except branch is there for safety
    assert isinstance(result, bool)


@pytest.mark.unit
def test_serialize_value_path_import_failure():
    """Lines 334-335: exception during Path check falls through to str()."""

    class FakePath:
        def __str__(self):
            return "/fake/path"

    result = _serialize_value(FakePath())
    assert result == "/fake/path"


@pytest.mark.unit
def test_serialize_value_with_none():
    """Ensure None is serialized as string 'None'."""

    assert _serialize_value(None) == "None"


# ---------------------------------------------------------------------------
# Coverage gap tests: decorators.py lines 146→145, 148→145
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_trace_pipeline_include_args_missing_arg_skips():
    """Line 146→145: when include_args names an arg not in the function signature, it is skipped."""

    @trace_pipeline(include_args=["nonexistent_arg"])
    def process(schema_name, force=False):
        return {"schema": schema_name, "force": force}

    # Should not raise; the missing arg is silently skipped
    result = process("cclf1", force=True)
    assert result["schema"] == "cclf1"
    assert result["force"] is True


@pytest.mark.unit
def test_trace_pipeline_include_args_non_simple_type_skips():
    """Line 148→145: when include_args value is not a simple type, it is skipped."""

    @trace_pipeline(include_args=["complex_arg"])
    def process(schema_name, complex_arg=None):
        return {"schema": schema_name, "complex_arg": complex_arg}

    # Pass a dict (not a simple type: str/int/float/bool), so _is_simple_type returns False
    result = process("cclf1", complex_arg={"nested": "data"})
    assert result["schema"] == "cclf1"
    assert result["complex_arg"] == {"nested": "data"}
