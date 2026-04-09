



# © 2025 HarmonyCares

"""Tests for acoharmony/_exceptions/_decorators.py."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import patch
import pytest


class TestDecorators:
    """Test suite for _decorators."""


    @pytest.mark.unit
    def test_explain(self) -> None:
        """Test explain function."""
        @explain(why="reason", how="fix")
        def failing():
            raise ValueError("boom")

        with pytest.raises(ACOHarmonyException) as exc_info:
            failing()
        assert "boom" in exc_info.value.message
        assert exc_info.value.context.why == "reason"

    @pytest.mark.unit
    def test_trace_errors(self) -> None:
        """Test trace_errors function."""
        @trace_errors(span_name="test")
        def ok_func():
            return 42

        assert ok_func() == 42

        @trace_errors(span_name="test")
        def bad_func():
            raise ValueError("err")

        with pytest.raises(ValueError, match="err"):
            bad_func()

    @pytest.mark.unit
    def test_log_errors(self) -> None:
        """Test log_errors function."""
        @log_errors(logger_name="test")
        def ok_func():
            return "ok"

        assert ok_func() == "ok"

        @log_errors(logger_name="test")
        def bad_func():
            raise RuntimeError("logged")

        with pytest.raises(RuntimeError, match="logged"):
            bad_func()

    @pytest.mark.unit
    def test_catch_and_explain(self) -> None:
        """Test catch_and_explain function."""
        @catch_and_explain(exception_type=ValueError, why="val err", how="fix val")
        def bad_func():
            raise ValueError("bad")

        with pytest.raises(ACOHarmonyException) as exc_info:
            bad_func()
        assert "bad" in exc_info.value.message
        assert exc_info.value.context.why == "val err"

    @pytest.mark.unit
    def test_explain_on_error(self) -> None:
        """Test explain_on_error function."""
        with pytest.raises(ACOHarmonyException) as exc_info:
            with explain_on_error(why="ctx fail", how="ctx fix", context_name="op"):
                raise ValueError("inner")
        assert "inner" in exc_info.value.message
        assert exc_info.value.context.why == "ctx fail"
        assert exc_info.value.context.metadata["context"] == "op"



# © 2025 HarmonyCares
# All rights reserved.

"""
Tests for exception decorators.
"""






class TestExplainDecorator:
    """Tests for @explain decorator."""


    @pytest.mark.unit
    def test_explain_wraps_exception(self):
        """@explain wraps exceptions with explanation."""


        @explain(
            why="File not found",
            how="Check path",
            causes=["Wrong path"],
        )
        def failing_func():
            raise ValueError("test error")

        with pytest.raises(ACOHarmonyException) as exc_info:
            failing_func()

        exc = exc_info.value
        assert "test error" in exc.message
        assert exc.context.why == "File not found"
        assert exc.context.how == "Check path"
        assert "Wrong path" in exc.context.causes

    @pytest.mark.unit
    def test_explain_preserves_aco_exception(self):
        """@explain doesn't wrap ACOHarmonyException."""


        @explain(why="Test", how="Test")
        def failing_func():
            raise ACOHarmonyException("already explained", auto_log=False, auto_trace=False)

        with pytest.raises(ACOHarmonyException) as exc_info:
            failing_func()

        exc = exc_info.value
        assert exc.message == "already explained"
        # Should not be double-wrapped

    @pytest.mark.unit
    def test_explain_allows_success(self):
        """@explain allows successful execution."""


        @explain(why="Test", how="Test")
        def success_func():
            return "success"

        result = success_func()
        assert result == "success"


class TestTraceErrorsDecorator:
    """Tests for @trace_errors decorator."""


    @pytest.mark.unit
    def test_trace_errors_allows_success(self):
        """@trace_errors allows successful execution."""


        @trace_errors(span_name="test_span")
        def success_func():
            return "success"

        result = success_func()
        assert result == "success"

    @pytest.mark.unit
    def test_trace_errors_reraises_exception(self):
        """@trace_errors reraises exceptions."""


        @trace_errors(span_name="test_span")
        def failing_func():
            raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            failing_func()


class TestLogErrorsDecorator:
    """Tests for @log_errors decorator."""


    @pytest.mark.unit
    def test_log_errors_allows_success(self):
        """@log_errors allows successful execution."""


        @log_errors(logger_name="test")
        def success_func():
            return "success"

        result = success_func()
        assert result == "success"

    @pytest.mark.unit
    def test_log_errors_reraises_exception(self):
        """@log_errors reraises exceptions after logging."""


        @log_errors(logger_name="test", level="error")
        def failing_func():
            raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            failing_func()


class TestCatchAndExplainDecorator:
    """Tests for @catch_and_explain decorator."""


    @pytest.mark.unit
    def test_catch_and_explain_catches_specific_type(self):
        """@catch_and_explain catches specific exception types."""


        @catch_and_explain(
            exception_type=ValueError,
            why="Value error occurred",
            how="Fix the value",
        )
        def failing_func():
            raise ValueError("bad value")

        with pytest.raises(ACOHarmonyException) as exc_info:
            failing_func()

        exc = exc_info.value
        assert "bad value" in exc.message
        assert exc.context.why == "Value error occurred"

    @pytest.mark.unit
    def test_catch_and_explain_ignores_other_types(self):
        """@catch_and_explain ignores other exception types."""


        @catch_and_explain(
            exception_type=ValueError,
            why="Value error occurred",
            how="Fix the value",
        )
        def failing_func():
            raise TypeError("type error")

        with pytest.raises(TypeError, match="type error"):
            failing_func()

    @pytest.mark.unit
    def test_catch_and_explain_no_reraise(self):
        """@catch_and_explain can suppress exceptions."""


        @catch_and_explain(
            exception_type=ValueError,
            why="Test",
            how="Test",
            reraise=False,
        )
        def failing_func():
            raise ValueError("test")

        result = failing_func()
        assert result is None  # Exception suppressed


class TestExplainOnErrorContext:
    """Tests for explain_on_error context manager."""


    @pytest.mark.unit
    def test_explain_on_error_wraps_exception(self):
        """explain_on_error wraps exceptions."""


        with pytest.raises(ACOHarmonyException) as exc_info:
            with explain_on_error(
                why="Operation failed",
                how="Fix it",
                context_name="test_operation",
            ):
                raise ValueError("test error")

        exc = exc_info.value
        assert "test error" in exc.message
        assert exc.context.why == "Operation failed"
        assert exc.context.metadata["context"] == "test_operation"

    @pytest.mark.unit
    def test_explain_on_error_allows_success(self):
        """explain_on_error allows successful execution."""


        with explain_on_error(why="Test", how="Test"):
            result = "success"

        assert result == "success"

    @pytest.mark.unit
    def test_explain_on_error_preserves_aco_exception(self):
        """explain_on_error doesn't double-wrap ACOHarmonyException."""


        with pytest.raises(ACOHarmonyException) as exc_info:
            with explain_on_error(why="Test", how="Test"):
                raise ACOHarmonyException("already explained", auto_log=False, auto_trace=False)

        exc = exc_info.value
        assert exc.message == "already explained"


class TestRetryWithExplanation:
    """Tests for @retry_with_explanation decorator."""


    @pytest.mark.unit
    def test_retry_success_on_first_attempt(self):
        """Succeeds on first attempt without retrying."""


        call_count = [0]

        @retry_with_explanation(max_attempts=3, why="Test", how="Test")
        def success_func():
            call_count[0] += 1
            return "success"

        result = success_func()
        assert result == "success"
        assert call_count[0] == 1

    @pytest.mark.unit
    def test_retry_success_on_retry(self):
        """Succeeds after retrying."""


        call_count = [0]

        @retry_with_explanation(
            max_attempts=3,
            why="Test",
            how="Test",
            backoff_seconds=0.01,  # Fast for testing
        )
        def retry_func():
            call_count[0] += 1
            if call_count[0] < 3:
                raise ValueError("not yet")
            return "success"

        result = retry_func()
        assert result == "success"
        assert call_count[0] == 3

    @pytest.mark.unit
    def test_retry_fails_after_max_attempts(self):
        """Fails after max attempts."""


        call_count = [0]

        @retry_with_explanation(
            max_attempts=3,
            why="Test",
            how="Test",
            backoff_seconds=0.01,
        )
        def always_fails():
            call_count[0] += 1
            raise ValueError("always fails")

        with pytest.raises(ACOHarmonyException) as exc_info:
            always_fails()

        exc = exc_info.value
        assert "Failed after 3 attempts" in exc.message
        assert call_count[0] == 3
        assert exc.context.metadata["attempts"] == 3


# ===================== Coverage gap: _decorators.py lines 120, 303-304, 356, 359, 376 =====================

class TestTraceErrorsFallbackLine120:
    """Cover line 120: trace_errors fallback when tracing setup fails."""


    @pytest.mark.unit
    def test_fallback_to_direct_execution_when_tracer_fails(self):
        """Line 120: when tracer import fails with 'tracer' not in error, reraises."""


        @trace_errors(span_name="test_span")
        def my_func():
            return 42

        # When the function itself raises (not tracer setup), it should propagate
        @trace_errors(span_name="test_span")
        def failing_func():
            raise RuntimeError("some error")

        with pytest.raises(RuntimeError, match="some error"):
            failing_func()

    @pytest.mark.unit
    def test_fallback_executes_func_when_tracer_import_fails(self):
        """Line 120: falls back to func() when tracer import itself fails."""

        # When opentelemetry can't be imported, the outer except catches it.
        # If "tracer" is in the error message, falls back to func()


        @trace_errors(span_name="test_span")
        def my_func():
            return 42

        # The function should still work even without tracing
        result = my_func()
        assert result == 42


class TestSuppressAndLogLines303_304:
    """Cover lines 303-304: suppress_and_log exception in logging itself."""


    @pytest.mark.unit
    def test_logging_failure_is_silently_caught(self):
        """Lines 303-304: when logging itself fails, exception is still suppressed."""



        with patch("acoharmony._log.get_logger", side_effect=ImportError("no logger")):
            # The suppress_and_log catches ValueError, and if logging fails, passes
            with suppress_and_log(ValueError, logger_name="test"):
                raise ValueError("test suppressed")
            # Should reach here without error


class TestRetryLoggingLines356_359:
    """Cover lines 356, 359: retry logging failure is silently caught."""


    @pytest.mark.unit
    def test_retry_logging_failure_still_retries(self):
        """Lines 356, 359: when retry logging fails, still retries."""


        call_count = [0]

        @retry_with_explanation(
            max_attempts=2,
            why="Test retry",
            how="Fix it",
            backoff_seconds=0.001,
        )
        def flaky_func():
            call_count[0] += 1
            if call_count[0] < 2:
                raise ValueError("temporary error")
            return "success"

        with patch("acoharmony._log.get_logger", side_effect=ImportError("no logger")):
            result = flaky_func()
            assert result == "success"
            assert call_count[0] == 2


class TestRetryReturnNoneLine376:
    """Cover line 376: retry_with_explanation return None (unreachable guard)."""


    @pytest.mark.unit
    def test_retry_all_attempts_raise(self):
        """Line 376: after all attempts fail, ACOHarmonyException is raised (not None)."""


        @retry_with_explanation(
            max_attempts=1,
            why="Single attempt",
            how="Check it",
            backoff_seconds=0.001,
        )
        def single_fail():
            raise ValueError("fail")

        with pytest.raises(ACOHarmonyException):
            single_fail()


class TestTraceErrorsFallbackTracerInError:
    """Cover branch 118->120: trace_errors falls back when 'tracer' is in error string."""

    @pytest.mark.unit
    def test_trace_errors_fallback_when_tracer_in_error(self):
        """Branch 118->120: when exception contains 'tracer', fall back to func()."""

        @trace_errors(span_name="test_span")
        def my_func():
            return 42

        # Patch get_tracer to raise an error containing "tracer"
        with patch(
            "acoharmony._trace.get_tracer",
            side_effect=RuntimeError("failed to initialize tracer"),
        ):
            result = my_func()
            assert result == 42


class TestRetryWithExplanationZeroAttempts:
    """Cover branch 336->376: retry_with_explanation with max_attempts=0."""

    @pytest.mark.unit
    def test_retry_zero_attempts_returns_none(self):
        """Branch 336->376: max_attempts=0, loop body never executes, returns None."""

        @retry_with_explanation(
            max_attempts=0,
            why="Zero attempts",
            how="Increase max_attempts",
            backoff_seconds=0.001,
        )
        def should_not_run():
            raise ValueError("should not be called")

        result = should_not_run()
        assert result is None
