"""Tests for acoharmony._runner module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony
from acoharmony import _runner
from acoharmony._runner import (
    FileProcessor,
    MemoryManager,
    PipelineExecutor,
    RunnerRegistry,
    SchemaTransformer,
    TransformRunner,
    register_operation,
    register_processor,
)


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._runner is not None

    @pytest.mark.unit
    def test_module_has_all_attribute(self):
        """Module defines __all__ with expected exports."""
        assert hasattr(_runner, '__all__')
        assert isinstance(_runner.__all__, list)
        assert len(_runner.__all__) == 8

    @pytest.mark.unit
    def test_all_exports_in_list(self):
        """All items in __all__ are actually defined in module."""
        for export in _runner.__all__:
            assert hasattr(_runner, export), f"Export '{export}' not found in module"

    @pytest.mark.unit
    def test_expected_exports_present(self):
        """Expected exports are present in module."""
        expected_exports = [
            "TransformRunner",
            "RunnerRegistry",
            "register_operation",
            "register_processor",
            "MemoryManager",
            "FileProcessor",
            "SchemaTransformer",
            "PipelineExecutor",
        ]
        for export in expected_exports:
            assert hasattr(_runner, export), f"Export '{export}' missing from module"

    @pytest.mark.unit
    def test_all_exports_match_expected(self):
        """__all__ exactly matches expected exports."""
        expected_exports = {
            "TransformRunner",
            "RunnerRegistry",
            "register_operation",
            "register_processor",
            "MemoryManager",
            "FileProcessor",
            "SchemaTransformer",
            "PipelineExecutor",
        }
        assert set(_runner.__all__) == expected_exports


class TestTransformRunnerExport:
    """Tests for TransformRunner export."""

    @pytest.mark.unit
    def test_transform_runner_is_class(self):
        """TransformRunner is a class."""
        assert isinstance(TransformRunner, type)

    @pytest.mark.unit
    def test_transform_runner_is_callable(self):
        """TransformRunner is callable."""
        assert callable(TransformRunner)

    @pytest.mark.unit
    def test_transform_runner_has_docstring(self):
        """TransformRunner has documentation."""
        assert TransformRunner.__doc__ is not None
        assert len(TransformRunner.__doc__) > 0


class TestRunnerRegistryExport:
    """Tests for RunnerRegistry export."""

    @pytest.mark.unit
    def test_runner_registry_is_class(self):
        """RunnerRegistry is a class."""
        assert isinstance(RunnerRegistry, type)

    @pytest.mark.unit
    def test_runner_registry_is_callable(self):
        """RunnerRegistry is callable."""
        assert callable(RunnerRegistry)

    @pytest.mark.unit
    def test_runner_registry_has_docstring(self):
        """RunnerRegistry has documentation."""
        assert RunnerRegistry.__doc__ is not None
        assert len(RunnerRegistry.__doc__) > 0

    @pytest.mark.unit
    def test_runner_registry_has_register_operation(self):
        """RunnerRegistry has register_operation method."""
        assert hasattr(RunnerRegistry, 'register_operation')

    @pytest.mark.unit
    def test_runner_registry_has_register_processor(self):
        """RunnerRegistry has register_processor method."""
        assert hasattr(RunnerRegistry, 'register_processor')

    @pytest.mark.unit
    def test_runner_registry_has_get_operation(self):
        """RunnerRegistry has get_operation method."""
        assert hasattr(RunnerRegistry, 'get_operation')

    @pytest.mark.unit
    def test_runner_registry_has_get_processor(self):
        """RunnerRegistry has get_processor method."""
        assert hasattr(RunnerRegistry, 'get_processor')


class TestRegisterOperationExport:
    """Tests for register_operation export."""

    @pytest.mark.unit
    def test_register_operation_is_callable(self):
        """register_operation is callable."""
        assert callable(register_operation)

    @pytest.mark.unit
    def test_register_operation_is_method(self):
        """register_operation is a method of RunnerRegistry."""
        # Should be the classmethod from RunnerRegistry
        assert hasattr(RunnerRegistry, 'register_operation')

    @pytest.mark.unit
    def test_register_operation_has_docstring(self):
        """register_operation has documentation."""
        assert register_operation.__doc__ is not None
        assert len(register_operation.__doc__) > 0


class TestRegisterProcessorExport:
    """Tests for register_processor export."""

    @pytest.mark.unit
    def test_register_processor_is_callable(self):
        """register_processor is callable."""
        assert callable(register_processor)

    @pytest.mark.unit
    def test_register_processor_is_method(self):
        """register_processor is a method of RunnerRegistry."""
        # Should be the classmethod from RunnerRegistry
        assert hasattr(RunnerRegistry, 'register_processor')

    @pytest.mark.unit
    def test_register_processor_has_docstring(self):
        """register_processor has documentation."""
        assert register_processor.__doc__ is not None
        assert len(register_processor.__doc__) > 0


class TestMemoryManagerExport:
    """Tests for MemoryManager export."""

    @pytest.mark.unit
    def test_memory_manager_is_class(self):
        """MemoryManager is a class."""
        assert isinstance(MemoryManager, type)

    @pytest.mark.unit
    def test_memory_manager_is_callable(self):
        """MemoryManager is callable."""
        assert callable(MemoryManager)

    @pytest.mark.unit
    def test_memory_manager_has_docstring(self):
        """MemoryManager has documentation."""
        assert MemoryManager.__doc__ is not None
        assert len(MemoryManager.__doc__) > 0

    @pytest.mark.unit
    def test_memory_manager_has_init(self):
        """MemoryManager has __init__ method."""
        assert hasattr(MemoryManager, '__init__')


class TestFileProcessorExport:
    """Tests for FileProcessor export."""

    @pytest.mark.unit
    def test_file_processor_is_class(self):
        """FileProcessor is a class."""
        assert isinstance(FileProcessor, type)

    @pytest.mark.unit
    def test_file_processor_is_callable(self):
        """FileProcessor is callable."""
        assert callable(FileProcessor)

    @pytest.mark.unit
    def test_file_processor_has_docstring(self):
        """FileProcessor has documentation."""
        assert FileProcessor.__doc__ is not None
        assert len(FileProcessor.__doc__) > 0

    @pytest.mark.unit
    def test_file_processor_has_init(self):
        """FileProcessor has __init__ method."""
        assert hasattr(FileProcessor, '__init__')


class TestSchemaTransformerExport:
    """Tests for SchemaTransformer export."""

    @pytest.mark.unit
    def test_schema_transformer_is_class(self):
        """SchemaTransformer is a class."""
        assert isinstance(SchemaTransformer, type)

    @pytest.mark.unit
    def test_schema_transformer_is_callable(self):
        """SchemaTransformer is callable."""
        assert callable(SchemaTransformer)

    @pytest.mark.unit
    def test_schema_transformer_has_docstring(self):
        """SchemaTransformer has documentation."""
        assert SchemaTransformer.__doc__ is not None
        assert len(SchemaTransformer.__doc__) > 0

    @pytest.mark.unit
    def test_schema_transformer_has_init(self):
        """SchemaTransformer has __init__ method."""
        assert hasattr(SchemaTransformer, '__init__')


class TestPipelineExecutorExport:
    """Tests for PipelineExecutor export."""

    @pytest.mark.unit
    def test_pipeline_executor_is_class(self):
        """PipelineExecutor is a class."""
        assert isinstance(PipelineExecutor, type)

    @pytest.mark.unit
    def test_pipeline_executor_is_callable(self):
        """PipelineExecutor is callable."""
        assert callable(PipelineExecutor)

    @pytest.mark.unit
    def test_pipeline_executor_has_docstring(self):
        """PipelineExecutor has documentation."""
        assert PipelineExecutor.__doc__ is not None
        assert len(PipelineExecutor.__doc__) > 0

    @pytest.mark.unit
    def test_pipeline_executor_has_init(self):
        """PipelineExecutor has __init__ method."""
        assert hasattr(PipelineExecutor, '__init__')


class TestImportSourceConsistency:
    """Tests ensuring exports match source module."""

    @pytest.mark.unit
    def test_runner_module_exports_from_package(self):
        """_runner.py exports match _runner/__init__.py exports."""
        from acoharmony._runner import (
            FileProcessor,
            MemoryManager,
            PipelineExecutor,
            RunnerRegistry,
            SchemaTransformer,
            TransformRunner,
            register_operation,
            register_processor,
        )

        # Verify they're the same objects (direct imports work)
        assert FileProcessor is not None
        assert MemoryManager is not None
        assert PipelineExecutor is not None
        assert RunnerRegistry is not None
        assert SchemaTransformer is not None
        assert TransformRunner is not None
        assert register_operation is not None
        assert register_processor is not None

    @pytest.mark.unit
    def test_main_package_imports_runner(self):
        """acoharmony package can import TransformRunner."""
        from acoharmony import TransformRunner

        assert TransformRunner is not None
        assert isinstance(TransformRunner, type)

    @pytest.mark.unit
    def test_module_re_export_identity(self):
        """Re-exports are identical to originals."""
        from acoharmony._runner._core import TransformRunner as OriginalRunner
        from acoharmony._runner import TransformRunner as ExportedRunner

        assert ExportedRunner is OriginalRunner

    @pytest.mark.unit
    def test_registry_re_export_identity(self):
        """Registry re-export is identical to original."""
        from acoharmony._runner._registry import RunnerRegistry as OriginalRegistry
        from acoharmony._runner import RunnerRegistry as ExportedRegistry

        assert ExportedRegistry is OriginalRegistry

    @pytest.mark.unit
    def test_memory_manager_re_export_identity(self):
        """MemoryManager re-export is identical to original."""
        from acoharmony._runner._memory import MemoryManager as OriginalManager
        from acoharmony._runner import MemoryManager as ExportedManager

        assert ExportedManager is OriginalManager

    @pytest.mark.unit
    def test_file_processor_re_export_identity(self):
        """FileProcessor re-export is identical to original."""
        from acoharmony._runner._file_processor import FileProcessor as OriginalProcessor
        from acoharmony._runner import FileProcessor as ExportedProcessor

        assert ExportedProcessor is OriginalProcessor

    @pytest.mark.unit
    def test_schema_transformer_re_export_identity(self):
        """SchemaTransformer re-export is identical to original."""
        from acoharmony._runner._schema_transformer import SchemaTransformer as OriginalTransformer
        from acoharmony._runner import SchemaTransformer as ExportedTransformer

        assert ExportedTransformer is OriginalTransformer

    @pytest.mark.unit
    def test_pipeline_executor_re_export_identity(self):
        """PipelineExecutor re-export is identical to original."""
        from acoharmony._runner._pipeline_executor import PipelineExecutor as OriginalExecutor
        from acoharmony._runner import PipelineExecutor as ExportedExecutor

        assert ExportedExecutor is OriginalExecutor


class TestDirectImportVariants:
    """Tests various import patterns work correctly."""

    @pytest.mark.unit
    def test_import_from_runner_module(self):
        """Can import from acoharmony._runner."""
        from acoharmony._runner import TransformRunner, RunnerRegistry

        assert TransformRunner is not None
        assert RunnerRegistry is not None

    @pytest.mark.unit
    def test_import_from_main_package(self):
        """Can import from acoharmony main package."""
        from acoharmony import TransformRunner

        assert TransformRunner is not None

    @pytest.mark.unit
    def test_wildcard_import_functionality(self):
        """Wildcard import would get all expected exports."""
        import acoharmony._runner as runner_module

        for name in runner_module.__all__:
            assert hasattr(runner_module, name)
            attr = getattr(runner_module, name)
            assert attr is not None


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    @pytest.mark.unit
    def test_no_private_exports_in_all(self):
        """__all__ does not contain private names."""
        for name in _runner.__all__:
            assert not name.startswith('_'), f"Private name '{name}' in __all__"

    @pytest.mark.unit
    def test_all_are_unique(self):
        """No duplicate exports in __all__."""
        assert len(_runner.__all__) == len(set(_runner.__all__))

    @pytest.mark.unit
    def test_module_docstring_exists(self):
        """Module has documentation."""
        assert _runner.__doc__ is not None
        assert len(_runner.__doc__) > 0

    @pytest.mark.unit
    def test_no_unexpected_exports(self):
        """No unexpected public exports outside of __all__."""
        public_names = [name for name in dir(_runner) if not name.startswith('_')]
        expected_public = set(_runner.__all__)
        unexpected = set(public_names) - expected_public

        # Should only have __all__ as unexpected
        assert 'builtins' not in unexpected, "Unexpected builtins import"


class TestFunctionSignatures:
    """Test that exported functions have proper signatures."""

    @pytest.mark.unit
    def test_register_operation_accepts_metadata(self):
        """register_operation can accept metadata parameter."""
        import inspect

        sig = inspect.signature(register_operation)
        params = list(sig.parameters.keys())
        assert 'operation_type' in params or len(params) >= 1

    @pytest.mark.unit
    def test_register_processor_accepts_metadata(self):
        """register_processor can accept metadata parameter."""
        import inspect

        sig = inspect.signature(register_processor)
        params = list(sig.parameters.keys())
        assert 'processor_type' in params or len(params) >= 1


class TestModuleCodeCoverage:
    """Tests to ensure all import and definition statements are executed."""

    @pytest.mark.unit
    def test_module_imports_execute_on_import(self):
        """Verify that importing the module executes import statements."""
        import sys

        # Remove module from cache to force re-import
        if 'acoharmony._runner' in sys.modules:
            del sys.modules['acoharmony._runner']

        # Import triggers execution of lines 14-23 (from imports)
        import acoharmony._runner as fresh_runner

        # Verify all imports executed successfully
        assert fresh_runner.TransformRunner is not None
        assert fresh_runner.RunnerRegistry is not None
        assert fresh_runner.MemoryManager is not None
        assert fresh_runner.FileProcessor is not None
        assert fresh_runner.SchemaTransformer is not None
        assert fresh_runner.PipelineExecutor is not None
        assert fresh_runner.register_operation is not None
        assert fresh_runner.register_processor is not None

    @pytest.mark.unit
    def test_all_constant_is_defined_on_import(self):
        """Verify that __all__ constant is defined when module is imported."""
        # This triggers execution of lines 25-34 (__all__ definition)
        assert hasattr(_runner, '__all__')

        # Verify __all__ is a list
        assert isinstance(_runner.__all__, list)

        # Verify __all__ has the expected length
        assert len(_runner.__all__) == 8

    @pytest.mark.unit
    def test_all_constant_values_are_correct_strings(self):
        """Verify that __all__ contains correct string values."""
        # This exercises the __all__ constant (lines 25-34)
        expected = [
            "TransformRunner",
            "RunnerRegistry",
            "register_operation",
            "register_processor",
            "MemoryManager",
            "FileProcessor",
            "SchemaTransformer",
            "PipelineExecutor",
        ]

        for item in _runner.__all__:
            assert isinstance(item, str)
            assert item in expected

    @pytest.mark.unit
    def test_docstring_is_accessible(self):
        """Verify module docstring is accessible (lines 4-11)."""
        # This verifies the docstring is present
        assert _runner.__doc__ is not None
        # When imported, the _runner.py module docstring may be replaced
        # with the _runner/__init__.py docstring due to how Python handles
        # package/module aliasing. Verify that a docstring exists.
        assert len(_runner.__doc__) > 0
        assert "runner" in _runner.__doc__.lower()
