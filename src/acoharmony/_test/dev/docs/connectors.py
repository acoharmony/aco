"""Tests for acoharmony._dev.docs.connectors module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._dev.docs.connectors is not None


class TestExtractClassDocImportedMethod:
    """Test extract_class_doc skips methods from other modules (branch 100->99)."""

    @pytest.mark.unit
    def test_inherited_method_from_other_module_is_skipped(self):
        """Methods where __module__ != module_name are skipped."""
        from acoharmony._dev.docs.connectors import document_class

        # Create a class with a method that claims to be from a different module
        class MyClass:
            def local_method(self):
                """A local method."""
                pass

            def foreign_method(self):
                """An imported method."""
                pass

        MyClass.foreign_method.__module__ = "some.other.module"
        MyClass.local_method.__module__ = "test_module"

        result = document_class(MyClass, module_path=__file__, module_name="test_module")
        method_names = [m.name for m in result.methods]
        assert "local_method" in method_names
        assert "foreign_method" not in method_names


class TestDocumentModuleSkipsImported:
    """Cover branches 148->147 and 155->154: skip classes/functions from other modules."""

    @pytest.mark.unit
    def test_module_with_imported_class_and_function(self):
        """Classes and functions from other modules are skipped (branches 148->147, 155->154)."""
        from acoharmony._dev.docs.connectors import document_module
        from pathlib import Path
        from unittest.mock import patch, MagicMock
        import types

        # Create a fake module with a class and function from different modules
        fake_module = types.ModuleType("fake_mod")
        fake_module.__doc__ = "A fake module."

        class LocalClass:
            """A local class."""
            pass

        class ForeignClass:
            """A foreign class."""
            pass

        def local_func():
            """A local function."""
            pass

        def foreign_func():
            """A foreign function."""
            pass

        LocalClass.__module__ = "fake_mod"
        ForeignClass.__module__ = "some.other.module"
        local_func.__module__ = "fake_mod"
        foreign_func.__module__ = "some.other.module"

        fake_module.LocalClass = LocalClass
        fake_module.ForeignClass = ForeignClass
        fake_module.local_func = local_func
        fake_module.foreign_func = foreign_func

        # Create a minimal Python file for AST parsing
        import tempfile, os
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, dir='/tmp') as f:
            f.write('def local_func() -> str:\n    pass\n')
            temp_path = f.name

        try:
            fake_path = Path(temp_path)
            # Patch relative_to so it works with our temp path
            with patch.object(Path, 'relative_to', return_value=Path("fake_mod.py")):
                with patch('importlib.import_module', return_value=fake_module):
                    result = document_module(fake_path)

            assert result is not None
            class_names = [c.name for c in result.classes]
            assert "LocalClass" in class_names
            assert "ForeignClass" not in class_names

            func_names = [f.name for f in result.functions]
            assert "local_func" in func_names
            assert "foreign_func" not in func_names
        finally:
            os.unlink(temp_path)
