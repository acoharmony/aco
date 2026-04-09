# © 2025 HarmonyCares
"""Tests for acoharmony/_runner/_registry.py."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestRegistry:
    """Test suite for _registry."""

    @pytest.mark.unit
    def test_register_operation(self) -> None:
        """Test register_operation function."""
        from acoharmony._runner._registry import RunnerRegistry

        @RunnerRegistry.register_operation("test_op_1")
        def my_op():
            return "hello"

        assert RunnerRegistry.get_operation("test_op_1") is not None
        assert RunnerRegistry.get_operation("test_op_1")() == "hello"

    @pytest.mark.unit
    def test_register_processor(self) -> None:
        """Test register_processor function."""
        from acoharmony._runner._registry import RunnerRegistry

        @RunnerRegistry.register_processor("test_proc_1")
        class MyProcessor:
            pass

        assert RunnerRegistry.get_processor("test_proc_1") is MyProcessor
        assert MyProcessor._processor_type == "test_proc_1"

    @pytest.mark.unit
    def test_register_operation(self) -> None:  # noqa: F811
        """Test register_operation with metadata."""
        from acoharmony._runner._registry import RunnerRegistry

        @RunnerRegistry.register_operation("test_op_2", metadata={"version": 1})
        def my_op2():
            return 42

        assert RunnerRegistry.get_metadata("test_op_2") == {"version": 1}

    @pytest.mark.unit
    def test_register_processor(self) -> None:  # noqa: F811
        """Test register_processor with metadata."""
        from acoharmony._runner._registry import RunnerRegistry

        @RunnerRegistry.register_processor("test_proc_2", metadata={"v": 2})
        class MyProc2:
            pass

        assert RunnerRegistry.get_metadata("test_proc_2") == {"v": 2}

    @pytest.mark.unit
    def test_get_operation(self) -> None:
        """Test get_operation function."""
        from acoharmony._runner._registry import RunnerRegistry

        # Non-existent operation should return None
        result = RunnerRegistry.get_operation("totally_fake_operation_xyz")
        assert result is None

    @pytest.mark.unit
    def test_runnerregistry_init(self) -> None:
        """Test RunnerRegistry class attributes exist."""
        from acoharmony._runner._registry import RunnerRegistry

        assert hasattr(RunnerRegistry, "_operations")
        assert hasattr(RunnerRegistry, "_processors")
        assert hasattr(RunnerRegistry, "_metadata")
        assert isinstance(RunnerRegistry.list_operations(), list)
        assert isinstance(RunnerRegistry.list_processors(), list)


class TestRunnerRegistryBranches:
    """Cover branches 47->48/51 (metadata truthy/falsy) and 77->78 (proc metadata)."""

    @pytest.mark.unit
    def test_register_operation_with_metadata(self):
        """Branch 47->48: metadata is provided and truthy."""
        from acoharmony._runner._registry import RunnerRegistry

        @RunnerRegistry.register_operation("op_meta_test", metadata={"v": 42})
        def my_op():
            return "result"

        assert RunnerRegistry.get_metadata("op_meta_test") == {"v": 42}
        result = my_op()
        assert result == "result"

    @pytest.mark.unit
    def test_register_operation_no_metadata(self):
        """Branch 47->51: metadata is None."""
        from acoharmony._runner._registry import RunnerRegistry

        @RunnerRegistry.register_operation("op_no_meta_test")
        def my_op():
            return "result"

        result = my_op()
        assert result == "result"

    @pytest.mark.unit
    def test_register_processor_with_metadata(self):
        """Branch 77->78: processor metadata is truthy."""
        from acoharmony._runner._registry import RunnerRegistry

        @RunnerRegistry.register_processor("proc_meta_test", metadata={"version": 2})
        class MyProc:
            pass

        assert RunnerRegistry.get_metadata("proc_meta_test") == {"version": 2}

    @pytest.mark.unit
    def test_register_processor_no_metadata(self):
        """Branch 77->78 NOT taken: metadata is None."""
        from acoharmony._runner._registry import RunnerRegistry

        @RunnerRegistry.register_processor("proc_no_meta_test")
        class MyProc:
            pass

        assert MyProc._processor_type == "proc_no_meta_test"

