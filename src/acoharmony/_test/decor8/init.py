"""
Comprehensive tests for _unity and _decor8 packages to achieve near-100% coverage.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest
import acoharmony._decor8 as decor8
from acoharmony._decor8 import (
    __all__,
    runner_method,
    pipeline_method,
    parser_method,
    composable,
    compose,
    timeit,
)


class TestRunnerMethod:
    """Tests for the runner_method meta-decorator."""

    @pytest.mark.unit
    def test_runner_method_basic(self):
        """Test runner_method with minimal args."""

        @runner_method()
        def run(data):
            return data

        result = run("hello")
        assert result == "hello"

    @pytest.mark.unit
    def test_runner_method_with_threshold(self):
        """Test runner_method with threshold."""

        @runner_method(threshold=5.0)
        def run(data):
            return data

        assert run(42) == 42

    @pytest.mark.unit
    def test_runner_method_with_memory_tracking(self):
        """Test runner_method with memory tracking enabled."""

        @runner_method(track_memory=True)
        def run(data):
            return data

        assert run("test") == "test"

    @pytest.mark.unit
    def test_runner_method_with_validate_args(self):
        """Test runner_method with type validation."""

        @runner_method(validate_args_types={"name": str})
        def run(name):
            return name

        assert run("hello") == "hello"


class TestPipelineMethod:
    """Tests for the pipeline_method meta-decorator."""

    @pytest.mark.unit
    def test_pipeline_method_basic(self):
        """Test pipeline_method with defaults."""

        @pipeline_method()
        def run_pipeline(data):
            return data

        assert run_pipeline("data") == "data"

    @pytest.mark.unit
    def test_pipeline_method_custom(self):
        """Test pipeline_method with custom args."""

        @pipeline_method(threshold=120.0, track_memory=False)
        def run_pipeline(data):
            return data

        assert run_pipeline("data") == "data"


class TestParserMethod:
    """Tests for the parser_method meta-decorator."""

    @pytest.mark.unit
    def test_parser_method_basic(self):
        """Test parser_method with defaults."""

        @parser_method()
        def parse(data):
            return data

        assert parse("test") == "test"

    @pytest.mark.unit
    def test_parser_method_with_path_validation(self, tmp_path):
        """Test parser_method with path validation."""

        test_file = tmp_path / "data.csv"
        test_file.write_text("a,b\n1,2\n")

        @parser_method(validate_path="file_path")
        def parse(file_path):
            return file_path

        assert parse(str(test_file)) == str(test_file)

    @pytest.mark.unit
    def test_parser_method_with_memory(self):
        """Test parser_method with memory tracking."""

        @parser_method(track_memory=True)
        def parse(data):
            return data

        assert parse("test") == "test"

    @pytest.mark.unit
    def test_parser_method_with_validate_args(self):
        """Test parser_method with type validation."""

        @parser_method(validate_args_types={"data": str})
        def parse(data):
            return data

        assert parse("test") == "test"


class TestDecor8Init:
    """Test _decor8 package imports."""

    @pytest.mark.unit
    def test_all_exports_exist(self):
        """Test that __all__ exports are importable."""

        for name in __all__:
            assert hasattr(decor8, name), f"Missing export: {name}"

    @pytest.mark.unit
    def test_import_key_items(self):
        """Test key imports work."""
        assert callable(composable)
        assert callable(compose)
        assert callable(timeit)
