from dataclasses import dataclass
import sys
import warnings
from unittest.mock import patch

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

from acoharmony._exceptions import ACOHarmonyException, ACOHarmonyWarning, ExceptionContext

# © 2025 HarmonyCares
# All rights reserved.


"""
Tests for base exception classes.
"""




class TestExceptionContext:
    """Tests for ExceptionContext dataclass."""

    @pytest.mark.unit
    def test_context_creation(self):
        """Create exception context with all fields."""
        context = ExceptionContext(
            original_error=ValueError("test"),
            why="Testing",
            how="Fix it",
            causes=["cause1", "cause2"],
            remediation_steps=["step1", "step2"],
            metadata={"key": "value"},
        )

        assert context.original_error is not None
        assert context.why == "Testing"
        assert context.how == "Fix it"
        assert len(context.causes) == 2
        assert len(context.remediation_steps) == 2
        assert context.metadata["key"] == "value"

    @pytest.mark.unit
    def test_context_captures_stack_trace(self):
        """Context captures stack trace from original error."""
        try:
            raise ValueError("original")
        except ValueError as e:
            context = ExceptionContext(original_error=e)
            assert context.stack_trace is not None
            assert "ValueError" in context.stack_trace


class TestACOHarmonyException:
    """Tests for ACOHarmonyException base class."""

    @pytest.mark.unit
    def test_basic_exception(self):
        """Create basic exception."""
        exc = ACOHarmonyException("Test error", auto_log=False, auto_trace=False)

        assert exc.message == "Test error"
        assert exc.error_code == "ACO_UNKNOWN"
        assert exc.category == "general"

    @pytest.mark.unit
    def test_exception_with_context(self):
        """Create exception with full context."""
        exc = ACOHarmonyException(
            "Test error",
            why="Testing the framework",
            how="Fix by testing properly",
            causes=["cause1", "cause2"],
            remediation_steps=["step1", "step2"],
            metadata={"test": "value"},
            auto_log=False,
            auto_trace=False,
        )

        assert exc.context.why == "Testing the framework"
        assert exc.context.how == "Fix by testing properly"
        assert len(exc.context.causes) == 2
        assert len(exc.context.remediation_steps) == 2
        assert exc.context.metadata["test"] == "value"

    @pytest.mark.unit
    def test_exception_with_original_error(self):
        """Create exception wrapping original error."""
        original = ValueError("original error")

        exc = ACOHarmonyException(
            "Wrapped error",
            original_error=original,
            auto_log=False,
            auto_trace=False,
        )

        assert exc.context.original_error is original
        assert exc.context.stack_trace is not None

    @pytest.mark.unit
    def test_format_message_simple(self):
        """Format simple message."""
        exc = ACOHarmonyException("Test error", auto_log=False, auto_trace=False)

        msg = exc.format_message(detailed=False)
        assert msg == "Test error"

    @pytest.mark.unit
    def test_format_message_detailed(self):
        """Format detailed message with all context."""
        exc = ACOHarmonyException(
            "Test error",
            why="Testing",
            how="Fix it",
            causes=["cause1"],
            remediation_steps=["step1"],
            auto_log=False,
            auto_trace=False,
        )

        msg = exc.format_message(detailed=True)
        assert "Test error" in msg
        assert "WHY THIS HAPPENED" in msg
        assert "Testing" in msg
        assert "HOW TO FIX" in msg
        assert "Fix it" in msg
        assert "POSSIBLE CAUSES" in msg
        assert "cause1" in msg
        assert "REMEDIATION STEPS" in msg
        assert "step1" in msg

    @pytest.mark.unit
    def test_str_returns_detailed_message(self):
        """__str__ returns detailed message."""
        exc = ACOHarmonyException(
            "Test error",
            why="Testing",
            auto_log=False,
            auto_trace=False,
        )

        msg = str(exc)
        assert "Test error" in msg
        assert "WHY THIS HAPPENED" in msg

    @pytest.mark.unit
    def test_repr_returns_concise(self):
        """__repr__ returns concise representation."""
        exc = ACOHarmonyException("Test error", auto_log=False, auto_trace=False)

        repr_str = repr(exc)
        assert "ACOHarmonyException" in repr_str
        assert "Test error" in repr_str
        assert "ACO_UNKNOWN" in repr_str

    @pytest.mark.unit
    def test_exception_can_be_raised(self):
        """Exception can be raised and caught."""
        with pytest.raises(ACOHarmonyException) as exc_info:
            raise ACOHarmonyException("Test error", auto_log=False, auto_trace=False)

        assert exc_info.value.message == "Test error"

    @pytest.mark.unit
    def test_exception_logging(self):
        """Exception automatically logs when created."""
        # This test verifies logging doesn't break exception creation
        # Actual log content is tested separately
        exc = ACOHarmonyException(
            "Test error",
            why="Testing logging",
            auto_log=True,  # Enable logging
            auto_trace=False,
        )

        assert exc.message == "Test error"

    @pytest.mark.unit
    def test_exception_tracing(self):
        """Exception automatically traces when created."""
        # This test verifies tracing doesn't break exception creation
        # Actual trace content is tested separately
        exc = ACOHarmonyException(
            "Test error",
            why="Testing tracing",
            auto_log=False,
            auto_trace=True,  # Enable tracing
        )

        assert exc.message == "Test error"


class TestACOHarmonyWarning:
    """Tests for ACOHarmonyWarning."""

    @pytest.mark.unit
    def test_warning_creation(self):
        """Create warning."""
        warning = ACOHarmonyWarning("Test warning", category="test", metadata={"key": "value"})

        assert warning.message == "Test warning"
        assert warning.category == "test"
        assert warning.metadata["key"] == "value"

    @pytest.mark.unit
    def test_warning_can_be_raised(self):
        """Warning can be raised."""

        with pytest.warns(ACOHarmonyWarning):
            warnings.warn("Test warning", ACOHarmonyWarning, stacklevel=2)



# © 2025 HarmonyCares
"""Tests for acoharmony/_exceptions/_base.py."""



class TestBase:
    """Test suite for _base."""

    @pytest.mark.unit
    def test_format_message(self) -> None:
        """Test format_message function."""
        exc = ACOHarmonyException(
            "Test error",
            why="Reason",
            how="Solution",
            causes=["cause1"],
            remediation_steps=["step1"],
            auto_log=False,
            auto_trace=False,
        )
        simple = exc.format_message(detailed=False)
        assert simple == "Test error"

        detailed = exc.format_message(detailed=True)
        assert "Test error" in detailed
        assert "WHY THIS HAPPENED" in detailed
        assert "Reason" in detailed
        assert "HOW TO FIX" in detailed
        assert "Solution" in detailed
        assert "POSSIBLE CAUSES" in detailed
        assert "cause1" in detailed
        assert "REMEDIATION STEPS" in detailed
        assert "step1" in detailed

    @pytest.mark.unit
    def test_exceptioncontext_init(self) -> None:
        """Test ExceptionContext initialization."""
        ctx = ExceptionContext()
        assert ctx.original_error is None
        assert ctx.why == ""
        assert ctx.how == ""
        assert ctx.causes == []
        assert ctx.remediation_steps == []
        assert ctx.metadata == {}
        assert ctx.stack_trace is None

        ctx2 = ExceptionContext(
            original_error=ValueError("err"),
            why="why",
            how="how",
            causes=["c"],
            remediation_steps=["s"],
            metadata={"k": "v"},
        )
        assert isinstance(ctx2.original_error, ValueError)
        assert ctx2.why == "why"
        assert ctx2.how == "how"
        assert ctx2.causes == ["c"]
        assert ctx2.remediation_steps == ["s"]
        assert ctx2.metadata == {"k": "v"}

    @pytest.mark.unit
    def test_acoharmonyexception_init(self) -> None:
        """Test ACOHarmonyException initialization."""
        exc = ACOHarmonyException("msg", auto_log=False, auto_trace=False)
        assert exc.message == "msg"
        assert exc.error_code == "ACO_UNKNOWN"
        assert exc.category == "general"
        assert isinstance(exc.context, ExceptionContext)
        assert isinstance(exc, Exception)
        assert str(exc) == exc.format_message(detailed=True)
        assert "ACOHarmonyException" in repr(exc)

    @pytest.mark.unit
    def test_acoharmonywarning_init(self) -> None:
        """Test ACOHarmonyWarning initialization."""
        w = ACOHarmonyWarning("warn msg", category="test_cat", metadata={"k": "v"})
        assert w.message == "warn msg"
        assert w.category == "test_cat"
        assert w.metadata == {"k": "v"}
        assert isinstance(w, UserWarning)

    @pytest.mark.unit
    def test_warning_logger_fails(self) -> None:


        # Force the `from .._log import get_logger` inside __init__ to fail
        with patch.dict(sys.modules, {"acoharmony._log": None}):
            w = ACOHarmonyWarning("test warning", category="test_cat")
            assert w.message == "test warning"
            assert w.category == "test_cat"

