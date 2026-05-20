# © 2025 HarmonyCares
# All rights reserved.

"""Tests for runner registry and runner components."""

import pytest

from acoharmony._runner._registry import RunnerRegistry, register_operation, register_processor


class TestRunnerRegistryOperations:
    """Test RunnerRegistry operation registration and lookup."""

    def setup_method(self):
        """Save and clear registry state before each test."""
        self._saved_ops = dict(RunnerRegistry._operations)
        self._saved_procs = dict(RunnerRegistry._processors)
        self._saved_meta = dict(RunnerRegistry._metadata)
        RunnerRegistry.clear()

    def teardown_method(self):
        """Restore registry state after each test."""
        RunnerRegistry._operations = self._saved_ops
        RunnerRegistry._processors = self._saved_procs
        RunnerRegistry._metadata = self._saved_meta

    @pytest.mark.unit
    def test_register_operation_adds_to_registry(self):
        """register_operation decorator should add the function to the registry."""
        @register_operation("test_op")
        def my_op():
            return "result"

        assert RunnerRegistry.get_operation("test_op") is not None

    @pytest.mark.unit
    def test_registered_operation_is_callable(self):
        """A registered operation should remain callable."""
        @register_operation("callable_op")
        def my_op(x):
            return x * 2

        op = RunnerRegistry.get_operation("callable_op")
        assert op is not None
        assert op(5) == 10

    @pytest.mark.unit
    def test_list_operations(self):
        """list_operations should return all registered operation names."""
        @register_operation("op_a")
        def op_a():
            pass

        @register_operation("op_b")
        def op_b():
            pass

        ops = RunnerRegistry.list_operations()
        assert "op_a" in ops
        assert "op_b" in ops

    @pytest.mark.unit
    def test_get_nonexistent_operation_returns_none(self):
        """get_operation for an unregistered name should return None."""
        assert RunnerRegistry.get_operation("nonexistent") is None


class TestRunnerRegistryProcessors:
    """Test RunnerRegistry processor registration and lookup."""

    def setup_method(self):
        """Save and clear registry state before each test."""
        self._saved_ops = dict(RunnerRegistry._operations)
        self._saved_procs = dict(RunnerRegistry._processors)
        self._saved_meta = dict(RunnerRegistry._metadata)
        RunnerRegistry.clear()

    def teardown_method(self):
        """Restore registry state after each test."""
        RunnerRegistry._operations = self._saved_ops
        RunnerRegistry._processors = self._saved_procs
        RunnerRegistry._metadata = self._saved_meta

    @pytest.mark.unit
    def test_register_processor_adds_to_registry(self):
        """register_processor decorator should add the class to the registry."""
        @register_processor("test_proc")
        class MyProcessor:
            pass

        assert RunnerRegistry.get_processor("test_proc") is MyProcessor

    @pytest.mark.unit
    def test_list_processors(self):
        """list_processors should return all registered processor names."""
        @register_processor("proc_a")
        class ProcA:
            pass

        @register_processor("proc_b")
        class ProcB:
            pass

        procs = RunnerRegistry.list_processors()
        assert "proc_a" in procs
        assert "proc_b" in procs

    @pytest.mark.unit
    def test_get_nonexistent_processor_returns_none(self):
        """get_processor for an unregistered name should return None."""
        assert RunnerRegistry.get_processor("nonexistent") is None

    @pytest.mark.unit
    def test_clear_removes_all_entries(self):
        """clear should remove all operations, processors, and metadata."""
        @register_operation("temp_op")
        def temp():
            pass

        @register_processor("temp_proc")
        class TempProc:
            pass

        RunnerRegistry.clear()
        assert RunnerRegistry.list_operations() == []
        assert RunnerRegistry.list_processors() == []
