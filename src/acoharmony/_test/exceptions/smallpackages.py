"""
Comprehensive tests for small packages.
#TODO place tests in a more logical organization, 
small packages naming is anti-pattern

Covers:
1. _cli_commands/sva_validate.py - SVA validation pipeline
2. _crosswalks/__init__.py - Crosswalk loading from YAML
3. _validators/field_validators.py - All validator patterns
4. Gap coverage for _exceptions, _log, _decor8
"""

import pytest

from acoharmony._exceptions import (
    ACOHarmonyException,
    ACOHarmonyWarning,
    CatalogError,
    DependencyError,
    EmptyDataError,
    ExceptionContext,
    ExceptionRegistry,
    FileFormatValidationError,
    InvalidFileFormatError,
    InvalidTierError,
    MissingColumnError,
    MissingColumnsError,
    ParseError,
    PathValidationError,
    PipelineError,
    SchemaNotFoundError,
    SchemaRegistrationError,
    StageError,
    StorageAccessError,
    StorageBackendError,
    StorageConfigurationError,
    StoragePathError,
    TableNotFoundError,
    TransformError,
    TransformOutputError,
    TransformSchemaError,
    TransformSourceError,
    TypeValidationError,
    ValidationError,
    catch_and_explain,
    explain,
    explain_on_error,
    log_errors,
    retry_with_explanation,
    suppress_and_log,
    trace_errors,
)


class TestExceptionContextNoBugStack:
    """Additional ExceptionContext coverage."""

    @pytest.mark.unit
    def test_no_stack_trace_when_no_original_error(self):
        """Stack trace is None when no original_error given."""

        ctx = ExceptionContext()
        assert ctx.stack_trace is None

    @pytest.mark.unit
    def test_explicit_stack_trace_not_overridden(self):
        """Explicit stack_trace is not overridden."""

        ctx = ExceptionContext(
            original_error=ValueError("x"),
            stack_trace="explicit trace",
        )
        assert ctx.stack_trace == "explicit trace"


class TestACOHarmonyExceptionFormatGaps:
    """Cover format_message branches not previously tested."""

    @pytest.mark.unit
    def test_format_with_original_error(self):
        """Format includes ORIGINAL ERROR section."""

        exc = ACOHarmonyException(
            "test",
            original_error=ValueError("orig"),
            auto_log=False,
            auto_trace=False,
        )
        msg = exc.format_message(detailed=True)
        assert "ORIGINAL ERROR" in msg
        assert "ValueError" in msg

    @pytest.mark.unit
    def test_format_with_metadata(self):
        """Format includes ADDITIONAL CONTEXT section."""

        exc = ACOHarmonyException(
            "test",
            metadata={"key": "val"},
            auto_log=False,
            auto_trace=False,
        )
        msg = exc.format_message(detailed=True)
        assert "ADDITIONAL CONTEXT" in msg
        assert "key: val" in msg


class TestExceptionRegistryGaps:
    """Additional coverage for ExceptionRegistry."""

    @pytest.mark.unit
    def test_get_nonexistent_returns_none(self):
        """get() returns None for unknown code."""

        assert ExceptionRegistry.get("NONEXISTENT_CODE_99999") is None

    @pytest.mark.unit
    def test_get_by_category_empty(self):
        """get_by_category() returns empty list for unknown category."""

        assert ExceptionRegistry.get_by_category("nonexistent_category_xyz") == []

    @pytest.mark.unit
    def test_all_codes_returns_sorted_list(self):
        """all_codes() returns sorted list of registered codes."""

        codes = ExceptionRegistry.all_codes()
        assert isinstance(codes, list)
        assert codes == sorted(codes)
        # We know STORAGE_001, PARSE_001, etc. are registered
        assert len(codes) > 0

    @pytest.mark.unit
    def test_all_categories_returns_sorted_list(self):
        """all_categories() returns sorted list of categories."""

        categories = ExceptionRegistry.all_categories()
        assert isinstance(categories, list)
        assert categories == sorted(categories)
        assert "storage" in categories
        assert "parsing" in categories

    @pytest.mark.unit
    def test_generate_docs_all(self):
        """generate_docs() without category returns markdown."""

        docs = ExceptionRegistry.generate_docs()
        assert "# ACO Harmony Exception Reference" in docs
        assert "## All Exceptions" in docs
        assert "**Error Code:**" in docs

    @pytest.mark.unit
    def test_generate_docs_by_category(self):
        """generate_docs(category) returns filtered markdown."""

        docs = ExceptionRegistry.generate_docs(category="storage")
        assert "## Category: storage" in docs
        assert "StorageBackendError" in docs

    @pytest.mark.unit
    def test_print_summary(self, capsys):
        """print_summary() prints to stdout."""

        ExceptionRegistry.print_summary()
        captured = capsys.readouterr()
        assert "Registered Exceptions:" in captured.out
        assert "Categories:" in captured.out


class TestStorageExceptions:
    """Tests for _exceptions._storage coverage."""

    @pytest.mark.unit
    def test_storage_backend_error_from_init(self):
        """StorageBackendError.from_initialization_error creates detailed error."""

        orig = RuntimeError("connection failed")
        exc = StorageBackendError.from_initialization_error(orig, profile="test_profile")

        assert "STORAGE BACKEND INITIALIZATION FAILED" in exc.message
        assert exc.context.metadata["profile"] == "test_profile"
        assert exc.context.metadata["original_error_type"] == "RuntimeError"

    @pytest.mark.unit
    def test_storage_backend_error_from_init_no_profile(self):
        """StorageBackendError.from_initialization_error without profile."""

        orig = RuntimeError("err")
        exc = StorageBackendError.from_initialization_error(orig, profile=None)

        assert exc.context.metadata["profile"] == "not_set"

    @pytest.mark.unit
    def test_storage_configuration_error_missing_profile(self):
        """StorageConfigurationError.missing_profile creates detailed error."""

        exc = StorageConfigurationError.missing_profile(
            "my_profile", ["local", "prod"]
        )
        assert "my_profile" in exc.message
        assert "local" in exc.message

    @pytest.mark.unit
    def test_storage_configuration_error_missing_profile_empty(self):
        """StorageConfigurationError.missing_profile with empty available list."""

        exc = StorageConfigurationError.missing_profile("p", [])
        assert "(none found)" in exc.message

    @pytest.mark.unit
    def test_storage_path_error(self):
        """StoragePathError.path_not_found."""

        exc = StoragePathError.path_not_found("/tmp/missing", "local")
        assert "/tmp/missing" in exc.message

    @pytest.mark.unit
    def test_invalid_tier_error(self):
        """InvalidTierError.invalid_tier."""

        exc = InvalidTierError.invalid_tier("mythical")
        assert "mythical" in exc.message
        assert "bronze" in str(exc)


class TestParsingExceptions:
    """Tests for _exceptions._parsing coverage."""

    @pytest.mark.unit
    def test_schema_not_found_error(self):
        """SchemaNotFoundError.for_schema."""

        exc = SchemaNotFoundError.for_schema("my_schema")
        assert "my_schema" in exc.message
        assert exc.context.metadata["schema_name"] == "my_schema"

    @pytest.mark.unit
    def test_invalid_file_format_error(self):
        """InvalidFileFormatError.for_file."""

        exc = InvalidFileFormatError.for_file("/tmp/bad.csv", "parquet")
        assert "parquet" in exc.message
        assert "/tmp/bad.csv" in exc.message

    @pytest.mark.unit
    def test_missing_column_error(self):
        """MissingColumnError.for_columns."""

        exc = MissingColumnError.for_columns(
            ["col_a", "col_b"], "/tmp/data.csv", "my_schema"
        )
        assert "col_a" in exc.message
        assert "col_b" in exc.message
        assert exc.context.metadata["schema_name"] == "my_schema"


class TestPipelineExceptions:
    """Tests for _exceptions._pipeline coverage."""

    @pytest.mark.unit
    def test_pipeline_error(self):

        exc = PipelineError("pipeline failed", auto_log=False, auto_trace=False)
        assert exc.error_code == "PIPELINE_001"

    @pytest.mark.unit
    def test_stage_error(self):

        exc = StageError("stage failed", auto_log=False, auto_trace=False)
        assert exc.error_code == "PIPELINE_002"

    @pytest.mark.unit
    def test_dependency_error(self):

        exc = DependencyError("dep not met", auto_log=False, auto_trace=False)
        assert exc.error_code == "PIPELINE_003"


class TestCatalogExceptions:
    """Tests for _exceptions._catalog coverage."""

    @pytest.mark.unit
    def test_catalog_error(self):

        exc = CatalogError("catalog failed", auto_log=False, auto_trace=False)
        assert exc.error_code == "CATALOG_001"

    @pytest.mark.unit
    def test_table_not_found_error(self):

        exc = TableNotFoundError("table missing", auto_log=False, auto_trace=False)
        assert exc.error_code == "CATALOG_002"

    @pytest.mark.unit
    def test_schema_registration_error(self):

        exc = SchemaRegistrationError("reg failed", auto_log=False, auto_trace=False)
        assert exc.error_code == "CATALOG_003"


class TestTransformExceptions:
    """Tests for _exceptions._transform coverage."""

    @pytest.mark.unit
    def test_transform_error(self):

        exc = TransformError("transform failed", auto_log=False, auto_trace=False)
        assert exc.error_code == "TRANSFORM_001"

    @pytest.mark.unit
    def test_transform_schema_error(self):

        exc = TransformSchemaError("schema bad", auto_log=False, auto_trace=False)
        assert exc.error_code == "TRANSFORM_002"

    @pytest.mark.unit
    def test_transform_source_error(self):

        exc = TransformSourceError("source bad", auto_log=False, auto_trace=False)
        assert exc.error_code == "TRANSFORM_003"

    @pytest.mark.unit
    def test_transform_output_error(self):

        exc = TransformOutputError("output bad", auto_log=False, auto_trace=False)
        assert exc.error_code == "TRANSFORM_004"


class TestValidationExceptions:
    """Tests for _exceptions._validation coverage."""

    @pytest.mark.unit
    def test_validation_error(self):

        exc = ValidationError("val failed", auto_log=False, auto_trace=False)
        assert exc.error_code == "VALIDATION_001"

    @pytest.mark.unit
    def test_missing_columns_error(self):

        exc = MissingColumnsError("cols missing", auto_log=False, auto_trace=False)
        assert exc.error_code == "VALIDATION_002"

    @pytest.mark.unit
    def test_type_validation_error(self):

        exc = TypeValidationError("type wrong", auto_log=False, auto_trace=False)
        assert exc.error_code == "VALIDATION_003"

    @pytest.mark.unit
    def test_empty_data_error(self):

        exc = EmptyDataError("empty", auto_log=False, auto_trace=False)
        assert exc.error_code == "VALIDATION_004"

    @pytest.mark.unit
    def test_path_validation_error(self):

        exc = PathValidationError("path bad", auto_log=False, auto_trace=False)
        assert exc.error_code == "VALIDATION_005"

    @pytest.mark.unit
    def test_file_format_validation_error(self):

        exc = FileFormatValidationError("format bad", auto_log=False, auto_trace=False)
        assert exc.error_code == "VALIDATION_006"


class TestSuppressAndLog:
    """Tests for suppress_and_log context manager."""

    @pytest.mark.unit
    def test_suppress_specified_exception(self):
        """Suppresses the specified exception type."""

        with suppress_and_log(ValueError):
            raise ValueError("suppressed")
        # No exception raised

    @pytest.mark.unit
    def test_suppress_default_all(self):
        """Suppresses all exceptions when no types specified."""

        with suppress_and_log():
            raise RuntimeError("suppressed")

    @pytest.mark.unit
    def test_does_not_suppress_unmatched(self):
        """Does not suppress exception types not in the list."""

        with pytest.raises(TypeError):
            with suppress_and_log(ValueError):
                raise TypeError("not suppressed")
