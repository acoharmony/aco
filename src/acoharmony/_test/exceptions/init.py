"""
Boost coverage for root modules, _runner/, _log/, and _exceptions/.

Targets uncovered code paths not exercised by test_runner_root_coverage.py.
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import patch

import pytest


class TestExceptionContextDeeper:
    """Cover ExceptionContext stack trace capture."""

    @pytest.mark.unit
    def test_stack_trace_captured(self):

        try:
            raise ValueError("test error")
        except ValueError as e:
            ctx = ExceptionContext(original_error=e)
            assert ctx.stack_trace is not None
            assert "ValueError" in ctx.stack_trace

    @pytest.mark.unit
    def test_no_stack_trace_no_error(self):

        ctx = ExceptionContext()
        assert ctx.stack_trace is None

    @pytest.mark.unit
    def test_explicit_stack_trace(self):

        ctx = ExceptionContext(stack_trace="custom trace")
        assert ctx.stack_trace == "custom trace"


class TestACOHarmonyExceptionDeeper:
    """Cover ACOHarmonyException format_message branches."""

    @pytest.mark.unit
    def test_format_message_detailed(self):

        exc = ACOHarmonyException(
            "Test error",
            original_error=ValueError("orig"),
            why="Because reasons",
            how="Fix it like this",
            causes=["Cause A", "Cause B"],
            remediation_steps=["Step 1", "Step 2"],
            metadata={"key": "val"},
            auto_log=False,
            auto_trace=False,
        )
        msg = exc.format_message(detailed=True)
        assert "Test error" in msg
        assert "ORIGINAL ERROR" in msg
        assert "WHY THIS HAPPENED" in msg
        assert "POSSIBLE CAUSES" in msg
        assert "HOW TO FIX" in msg
        assert "REMEDIATION STEPS" in msg
        assert "ADDITIONAL CONTEXT" in msg

    @pytest.mark.unit
    def test_format_message_not_detailed(self):

        exc = ACOHarmonyException("Simple", auto_log=False, auto_trace=False)
        msg = exc.format_message(detailed=False)
        assert msg == "Simple"

    @pytest.mark.unit
    def test_str_returns_detailed(self):

        exc = ACOHarmonyException("Msg", auto_log=False, auto_trace=False)
        s = str(exc)
        assert "Msg" in s

    @pytest.mark.unit
    def test_repr(self):

        exc = ACOHarmonyException("Msg", auto_log=False, auto_trace=False)
        r = repr(exc)
        assert "ACOHarmonyException" in r
        assert "Msg" in r
        assert "ACO_UNKNOWN" in r

    @pytest.mark.unit
    def test_log_error_called(self):

        with patch.object(ACOHarmonyException, "_log_error") as mock_log:
            with patch.object(ACOHarmonyException, "_trace_error"):
                ACOHarmonyException("test", auto_log=True, auto_trace=False)
                mock_log.assert_called_once()

    @pytest.mark.unit
    def test_trace_error_called(self):

        with patch.object(ACOHarmonyException, "_trace_error") as mock_trace:
            with patch.object(ACOHarmonyException, "_log_error"):
                ACOHarmonyException("test", auto_log=False, auto_trace=True)
                mock_trace.assert_called_once()

    @pytest.mark.unit
    def test_format_no_original_error(self):

        exc = ACOHarmonyException(
            "Test",
            why="why text",
            how="how text",
            auto_log=False,
            auto_trace=False,
        )
        msg = exc.format_message(detailed=True)
        assert "ORIGINAL ERROR" not in msg
        assert "WHY THIS HAPPENED" in msg
        assert "HOW TO FIX" in msg


class TestACOHarmonyWarning:
    """Cover ACOHarmonyWarning."""

    @pytest.mark.unit
    def test_warning_creation(self):

        with patch("acoharmony._exceptions._base.ACOHarmonyWarning.__init__", return_value=None):
            pass  # Just test import

    @pytest.mark.unit
    def test_warning_init(self):

        w = ACOHarmonyWarning("test warning", category="test", metadata={"k": "v"})
        assert w.message == "test warning"
        assert w.category == "test"
        assert w.metadata == {"k": "v"}


class TestExceptionDecorators:
    """Cover _exceptions/_decorators.py."""

    @pytest.mark.unit
    def test_explain_wraps_non_aco_exception(self):

        @explain(why="because", how="do this")
        def failing_func():
            raise ValueError("boom")

        with pytest.raises(Exception, match=r".*") as exc_info:
            failing_func()
        assert "boom" in str(exc_info.value)

    @pytest.mark.unit
    def test_explain_passes_aco_exception(self):

        @explain(why="because")
        def failing_func():
            raise ACOHarmonyException("already aco", auto_log=False, auto_trace=False)

        with pytest.raises(ACOHarmonyException, match="already aco"):
            failing_func()

    @pytest.mark.unit
    def test_explain_success(self):

        @explain(why="because")
        def ok_func():
            return 42

        assert ok_func() == 42

    @pytest.mark.unit
    def test_catch_and_explain_reraise(self):

        @catch_and_explain(ValueError, why="oops", reraise=True)
        def f():
            raise ValueError("bad")

        with pytest.raises(Exception, match=r".*"):
            f()

    @pytest.mark.unit
    def test_catch_and_explain_no_reraise(self):

        @catch_and_explain(ValueError, why="oops", reraise=False)
        def f():
            raise ValueError("bad")

        result = f()
        assert result is None

    @pytest.mark.unit
    def test_catch_and_explain_success(self):

        @catch_and_explain(ValueError)
        def f():
            return 99

        assert f() == 99

    @pytest.mark.unit
    def test_explain_on_error_context_manager(self):

        with pytest.raises(ACOHarmonyException):
            with explain_on_error(why="ctx fail"):
                raise ValueError("inner")

    @pytest.mark.unit
    def test_explain_on_error_aco_passthrough(self):

        with pytest.raises(ACOHarmonyException, match="already"):
            with explain_on_error(why="ctx"):
                raise ACOHarmonyException("already", auto_log=False, auto_trace=False)

    @pytest.mark.unit
    def test_explain_on_error_success(self):

        with explain_on_error(why="ctx"):
            pass  # no error

    @pytest.mark.unit
    def test_suppress_and_log(self):

        with suppress_and_log(ValueError):
            raise ValueError("suppressed")

    @pytest.mark.unit
    def test_suppress_and_log_default_types(self):

        with suppress_and_log():
            raise RuntimeError("suppressed")

    @pytest.mark.unit
    def test_log_errors_decorator(self):

        @log_errors(logger_name="test")
        def f():
            raise ValueError("logged")

        with pytest.raises(ValueError, match=r".*"):
            f()

    @pytest.mark.unit
    def test_log_errors_success(self):

        @log_errors()
        def f():
            return 42

        assert f() == 42

    @pytest.mark.unit
    def test_trace_errors_no_otel(self):

        @trace_errors(span_name="test")
        def f():
            return 42

        # Should work even without otel
        result = f()
        assert result == 42

    @pytest.mark.unit
    def test_retry_with_explanation_success(self):

        @retry_with_explanation(max_attempts=3, backoff_seconds=0.01)
        def f():
            return 42

        assert f() == 42

    @pytest.mark.unit
    def test_retry_with_explanation_fails(self):

        call_count = 0

        @retry_with_explanation(max_attempts=2, why="flaky", backoff_seconds=0.01)
        def f():
            nonlocal call_count
            call_count += 1
            raise ValueError("always fails")

        with pytest.raises(Exception, match="Failed after 2 attempts"):
            f()
        assert call_count == 2


# ===========================================================================
# _exceptions/_registry.py - deeper coverage
# ===========================================================================


class TestExceptionRegistryDeeper:
    """Cover ExceptionRegistry methods."""

    @pytest.mark.unit
    def test_get_existing(self):

        # StorageBackendError should be registered
        result = ExceptionRegistry.get("STORAGE_001")
        assert result is not None

    @pytest.mark.unit
    def test_get_nonexistent(self):

        assert ExceptionRegistry.get("NONEXISTENT") is None

    @pytest.mark.unit
    def test_get_by_category(self):

        storage_exceptions = ExceptionRegistry.get_by_category("storage")
        assert len(storage_exceptions) > 0

    @pytest.mark.unit
    def test_get_by_category_empty(self):

        result = ExceptionRegistry.get_by_category("nonexistent_category")
        assert result == []

    @pytest.mark.unit
    def test_all_codes(self):

        codes = ExceptionRegistry.all_codes()
        assert isinstance(codes, list)
        assert len(codes) > 0
        assert codes == sorted(codes)

    @pytest.mark.unit
    def test_all_categories(self):

        cats = ExceptionRegistry.all_categories()
        assert isinstance(cats, list)
        assert "storage" in cats

    @pytest.mark.unit
    def test_generate_docs_all(self):

        docs = ExceptionRegistry.generate_docs()
        assert "# ACO Harmony Exception Reference" in docs
        assert "## All Exceptions" in docs

    @pytest.mark.unit
    def test_generate_docs_by_category(self):

        docs = ExceptionRegistry.generate_docs(category="storage")
        assert "## Category: storage" in docs

    @pytest.mark.unit
    def test_print_summary(self, capsys):

        ExceptionRegistry.print_summary()
        captured = capsys.readouterr()
        assert "Registered Exceptions:" in captured.out
        assert "Categories:" in captured.out


# ===========================================================================
# _exceptions/ - domain-specific exceptions
# ===========================================================================


class TestStorageExceptions:
    """Cover storage exception factory methods."""

    @pytest.mark.unit
    def test_storage_backend_error_from_init(self):

        err = StorageBackendError.from_initialization_error(ValueError("bad"), profile="staging")
        assert "staging" in str(err)
        assert err.context.metadata["profile"] == "staging"

    @pytest.mark.unit
    def test_storage_configuration_error_missing_profile(self):

        err = StorageConfigurationError.missing_profile("test", ["local", "dev"])
        assert "test" in str(err)
        assert "local" in str(err)

    @pytest.mark.unit
    def test_storage_configuration_error_no_profiles(self):

        err = StorageConfigurationError.missing_profile("test", [])
        assert "test" in str(err)

    @pytest.mark.unit
    def test_storage_path_error(self):

        err = StoragePathError.path_not_found("/missing/path", "s3")
        assert "/missing/path" in str(err)

    @pytest.mark.unit
    def test_invalid_tier_error(self):

        err = InvalidTierError.invalid_tier("platinum")
        assert "platinum" in str(err)


class TestParsingExceptions:
    """Cover parsing exception factory methods."""

    @pytest.mark.unit
    def test_schema_not_found_for_schema(self):

        err = SchemaNotFoundError.for_schema("cclf99")
        assert "cclf99" in str(err)

    @pytest.mark.unit
    def test_invalid_file_format_for_file(self):

        err = InvalidFileFormatError.for_file("/data/bad.csv", "parquet")
        assert "parquet" in str(err)

    @pytest.mark.unit
    def test_missing_column_for_columns(self):

        err = MissingColumnError.for_columns(["col_a", "col_b"], "/data/file.csv", "cclf1")
        assert "col_a" in str(err)
        assert "col_b" in str(err)


class TestPipelineExceptions:
    """Cover pipeline exception classes."""

    @pytest.mark.unit
    def test_pipeline_error(self):

        err = PipelineError("pipeline failed", auto_log=False, auto_trace=False)
        assert "pipeline failed" in str(err)

    @pytest.mark.unit
    def test_stage_error(self):

        err = StageError("stage failed", auto_log=False, auto_trace=False)
        assert "stage failed" in str(err)

    @pytest.mark.unit
    def test_dependency_error(self):

        err = DependencyError("dep missing", auto_log=False, auto_trace=False)
        assert "dep missing" in str(err)


class TestTransformExceptions:
    """Cover transform exception classes."""

    @pytest.mark.unit
    def test_transform_error(self):

        err = TransformError("transform failed", auto_log=False, auto_trace=False)
        assert "transform failed" in str(err)

    @pytest.mark.unit
    def test_transform_schema_error(self):

        err = TransformSchemaError("bad schema", auto_log=False, auto_trace=False)
        assert "bad schema" in str(err)

    @pytest.mark.unit
    def test_transform_source_error(self):

        err = TransformSourceError("no source", auto_log=False, auto_trace=False)
        assert "no source" in str(err)

    @pytest.mark.unit
    def test_transform_output_error(self):

        err = TransformOutputError("cannot write", auto_log=False, auto_trace=False)
        assert "cannot write" in str(err)


class TestCatalogExceptions:
    """Cover catalog exception classes."""

    @pytest.mark.unit
    def test_catalog_error(self):

        err = CatalogError("catalog broken", auto_log=False, auto_trace=False)
        assert "catalog broken" in str(err)

    @pytest.mark.unit
    def test_table_not_found(self):

        err = TableNotFoundError("missing table", auto_log=False, auto_trace=False)
        assert "missing table" in str(err)

    @pytest.mark.unit
    def test_schema_registration_error(self):

        err = SchemaRegistrationError("reg failed", auto_log=False, auto_trace=False)
        assert "reg failed" in str(err)


class TestValidationExceptions:
    """Cover validation exception classes."""

    @pytest.mark.unit
    def test_validation_error(self):

        err = ValidationError("invalid", auto_log=False, auto_trace=False)
        assert "invalid" in str(err)

    @pytest.mark.unit
    def test_missing_columns_error(self):

        err = MissingColumnsError("cols missing", auto_log=False, auto_trace=False)
        assert "cols missing" in str(err)

    @pytest.mark.unit
    def test_type_validation_error(self):

        err = TypeValidationError("wrong type", auto_log=False, auto_trace=False)
        assert "wrong type" in str(err)

    @pytest.mark.unit
    def test_empty_data_error(self):

        err = EmptyDataError("no data", auto_log=False, auto_trace=False)
        assert "no data" in str(err)

    @pytest.mark.unit
    def test_path_validation_error(self):

        err = PathValidationError("bad path", auto_log=False, auto_trace=False)
        assert "bad path" in str(err)

    @pytest.mark.unit
    def test_file_format_validation_error(self):

        err = FileFormatValidationError("bad format", auto_log=False, auto_trace=False)
        assert "bad format" in str(err)


# ===========================================================================
# result.py - deeper coverage
# ===========================================================================
