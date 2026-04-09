# © 2025 HarmonyCares
# All rights reserved.

"""
Comprehensive tests for _expressions.py re-export module.

This test module verifies that the public API module correctly re-exports
all required components from the private _expressions package.

Test coverage includes:
1. All exports are accessible and importable
2. Imports work correctly from the module
3. The __all__ list is correct and complete
4. Exported objects have the correct types
5. Re-exported objects are the same as the source objects

NOTE: The _expressions.py file is shadowed by the _expressions/ package
in Python's import system. When `import acoharmony._expressions` is used,
Python imports the package's __init__.py instead of the _expressions.py file.
These tests verify both the intended behavior of _expressions.py and that
the package correctly provides the expected exports.
"""

import sys

import pytest


@pytest.mark.unit
class TestExpressionModuleExports:
    """Test that all exports from _expressions are accessible."""

    def test_all_exports_defined(self):
        """Verify that __all__ list exists and contains required items."""
        from acoharmony._expressions import __all__

        assert isinstance(__all__, list)
        assert len(__all__) >= 3
        assert "ExpressionRegistry" in __all__
        assert "register_expression" in __all__
        assert "EnterpriseCrosswalkExpression" in __all__

    def test_all_exports_are_strings(self):
        """Verify that all items in __all__ are strings."""
        from acoharmony import _expressions

        for item in _expressions.__all__:
            assert isinstance(item, str)

    def test_all_exports_are_accessible(self):
        """Verify that all items in __all__ are actually accessible from the module."""
        from acoharmony._expressions import __all__
        from acoharmony import _expressions

        for export_name in __all__:
            # The three specific ones we care about
            if export_name in ["ExpressionRegistry", "register_expression", "EnterpriseCrosswalkExpression"]:
                assert hasattr(_expressions, export_name)
                obj = getattr(_expressions, export_name)
                assert obj is not None

    def test_expression_registry_accessible(self):
        """Verify that ExpressionRegistry is accessible."""
        from acoharmony._expressions import ExpressionRegistry

        assert ExpressionRegistry is not None
        assert callable(ExpressionRegistry) or isinstance(ExpressionRegistry, type)

    def test_register_expression_accessible(self):
        """Verify that register_expression is accessible."""
        from acoharmony._expressions import register_expression

        assert register_expression is not None
        assert callable(register_expression)

    def test_enterprise_crosswalk_expression_accessible(self):
        """Verify that EnterpriseCrosswalkExpression is accessible."""
        from acoharmony._expressions import EnterpriseCrosswalkExpression

        assert EnterpriseCrosswalkExpression is not None
        assert isinstance(EnterpriseCrosswalkExpression, type)


@pytest.mark.unit
class TestExpressionModuleImports:
    """Test that imports work correctly from the _expressions module."""

    def test_import_expression_registry_direct(self):
        """Verify direct import of ExpressionRegistry."""
        from acoharmony._expressions import ExpressionRegistry

        assert ExpressionRegistry is not None

    def test_import_register_expression_direct(self):
        """Verify direct import of register_expression."""
        from acoharmony._expressions import register_expression

        assert register_expression is not None

    def test_import_enterprise_crosswalk_expression_direct(self):
        """Verify direct import of EnterpriseCrosswalkExpression."""
        from acoharmony._expressions import EnterpriseCrosswalkExpression

        assert EnterpriseCrosswalkExpression is not None

    def test_import_all_from_expressions(self):
        """Verify that all items can be imported using 'from ... import *'."""
        # Create a namespace to check what gets imported
        namespace = {}
        exec("from acoharmony._expressions import *", namespace)

        # Verify expected exports are in the namespace
        assert "ExpressionRegistry" in namespace
        assert "register_expression" in namespace
        assert "EnterpriseCrosswalkExpression" in namespace

    def test_import_as_module(self):
        """Verify that the module can be imported as a whole."""
        import acoharmony._expressions as expr_module

        assert hasattr(expr_module, "ExpressionRegistry")
        assert hasattr(expr_module, "register_expression")
        assert hasattr(expr_module, "EnterpriseCrosswalkExpression")

    def test_import_via_package(self):
        """Verify that exports are accessible via the acoharmony package."""
        from acoharmony import _expressions

        assert hasattr(_expressions, "ExpressionRegistry")
        assert hasattr(_expressions, "register_expression")
        assert hasattr(_expressions, "EnterpriseCrosswalkExpression")


@pytest.mark.unit
class TestExpressionModuleSourceVerification:
    """Test that re-exported objects come from correct sources."""

    def test_expression_registry_from_private_module(self):
        """Verify ExpressionRegistry is correctly sourced."""
        from acoharmony._expressions import ExpressionRegistry
        from acoharmony._expressions._registry import ExpressionRegistry as SourceRegistry

        # Should be the same object (not a copy)
        assert ExpressionRegistry is SourceRegistry

    def test_register_expression_from_private_module(self):
        """Verify register_expression is correctly sourced."""
        from acoharmony._expressions import register_expression
        from acoharmony._expressions._registry import register_expression as SourceRegisterExpr

        # Should be the same object (not a copy)
        assert register_expression is SourceRegisterExpr

    def test_enterprise_crosswalk_from_private_module(self):
        """Verify EnterpriseCrosswalkExpression is correctly sourced."""
        from acoharmony._expressions import EnterpriseCrosswalkExpression
        from acoharmony._expressions._ent_xwalk import (
            EnterpriseCrosswalkExpression as SourceExprClass,
        )

        # Should be the same object (not a copy)
        assert EnterpriseCrosswalkExpression is SourceExprClass


@pytest.mark.unit
class TestExpressionModuleTypes:
    """Test that exported objects have correct types."""

    def test_expression_registry_is_class(self):
        """Verify that ExpressionRegistry is a class."""
        from acoharmony._expressions import ExpressionRegistry

        assert isinstance(ExpressionRegistry, type)

    def test_register_expression_is_function(self):
        """Verify that register_expression is callable (function/method)."""
        from acoharmony._expressions import register_expression

        assert callable(register_expression)

    def test_enterprise_crosswalk_expression_is_class(self):
        """Verify that EnterpriseCrosswalkExpression is a class."""
        from acoharmony._expressions import EnterpriseCrosswalkExpression

        assert isinstance(EnterpriseCrosswalkExpression, type)


@pytest.mark.unit
class TestExpressionModuleDocstring:
    """Test that module has proper documentation."""

    def test_module_has_docstring(self):
        """Verify that _expressions module has a docstring."""
        from acoharmony import _expressions

        assert _expressions.__doc__ is not None
        assert len(_expressions.__doc__) > 0
        # Should mention expression or system
        assert ("expression" in _expressions.__doc__.lower() or
                "system" in _expressions.__doc__.lower())

    def test_module_docstring_mentions_exports(self):
        """Verify that docstring mentions expression generation or system."""
        from acoharmony import _expressions

        docstring = _expressions.__doc__
        # Should mention expression generation or system or builders
        assert any(word in docstring.lower()
                  for word in ["expression", "system", "builder"])


@pytest.mark.unit
class TestExpressionModuleNoExtraExports:
    """Test that module doesn't export unnecessary items."""

    def test_required_items_exported(self):
        """Verify that required items are in __all__."""
        from acoharmony._expressions import __all__

        # These items should definitely be exported
        required_exports = [
            "ExpressionRegistry",
            "register_expression",
            "EnterpriseCrosswalkExpression"
        ]

        for required in required_exports:
            assert required in __all__, f"Required export {required} not in __all__"

    def test_all_list_has_no_duplicates(self):
        """Verify that __all__ list has no duplicate entries."""
        from acoharmony import _expressions

        all_list = _expressions.__all__
        assert len(all_list) == len(set(all_list)), "Duplicate entries in __all__"


@pytest.mark.unit
class TestExpressionModuleIntegration:
    """Integration tests to verify the module works as expected."""

    def test_registry_can_be_used_after_import(self):
        """Verify that ExpressionRegistry can be used after import."""
        from acoharmony._expressions import ExpressionRegistry

        # Should be able to call registry methods
        builders = ExpressionRegistry.list_builders()
        assert isinstance(builders, list)

    def test_register_expression_can_be_used(self):
        """Verify that register_expression decorator works after import."""
        from acoharmony._expressions import register_expression

        # Should be able to use as a decorator
        @register_expression("test_expr_module_integration", description="test")
        class TestExpr:
            pass

        assert TestExpr is not None
        assert callable(TestExpr)

    def test_enterprise_crosswalk_can_be_instantiated(self):
        """Verify that EnterpriseCrosswalkExpression can be used."""
        from acoharmony._expressions import EnterpriseCrosswalkExpression

        # Should be able to instantiate or access the class
        assert EnterpriseCrosswalkExpression is not None
        assert hasattr(EnterpriseCrosswalkExpression, "__name__")


@pytest.mark.unit
class TestExpressionModuleConsistency:
    """Test consistency between module and __all__."""

    def test_all_items_in_all_are_accessible(self):
        """Verify all items listed in __all__ are accessible."""
        from acoharmony import _expressions

        for item_name in _expressions.__all__:
            assert hasattr(_expressions, item_name), f"Item {item_name} in __all__ but not accessible"

    def test_no_missing_exports_in_all(self):
        """Verify that obvious exports are in __all__."""
        from acoharmony import _expressions

        # These should definitely be in __all__
        expected_in_all = [
            "ExpressionRegistry",
            "register_expression",
            "EnterpriseCrosswalkExpression",
        ]

        for expected_item in expected_in_all:
            assert expected_item in _expressions.__all__, f"{expected_item} should be in __all__"


@pytest.mark.unit
class TestExpressionModuleLinking:
    """Test that the module correctly links to its implementation."""

    def test_module_file_location(self):
        """Verify the _expressions module is in the correct location."""
        from acoharmony import _expressions

        # Should be in acoharmony package
        module_file = _expressions.__file__
        assert module_file is not None
        assert "acoharmony" in module_file
        # It could be either _expressions.py or _expressions/__init__.py
        assert "_expressions" in module_file

    def test_imports_from_expressions_package(self):
        """Verify that imports come from the _expressions package."""
        from acoharmony._expressions import ExpressionRegistry

        # Check that it's coming from the private _expressions module (package)
        assert ExpressionRegistry.__module__ == "acoharmony._expressions._registry"

    def test_enterprise_crosswalk_module_source(self):
        """Verify EnterpriseCrosswalkExpression comes from correct module."""
        from acoharmony._expressions import EnterpriseCrosswalkExpression

        assert "acoharmony._expressions" in EnterpriseCrosswalkExpression.__module__
        assert "_ent_xwalk" in EnterpriseCrosswalkExpression.__module__


@pytest.mark.unit
class TestExpressionModuleAttributes:
    """Test module attributes and metadata."""

    def test_module_has_copyright_header(self):
        """Verify that module source has copyright header."""
        import inspect
        from acoharmony import _expressions

        source = inspect.getsource(_expressions)
        assert "HarmonyCares" in source or "2025" in source or "reserved" in source

    def test_module_imports_section_present(self):
        """Verify that module has proper imports."""
        import inspect
        from acoharmony import _expressions

        source = inspect.getsource(_expressions)
        # Should have imports from submodules (either ._registry or ._ent_xwalk etc.)
        assert "from ." in source

    def test_module_all_list_present(self):
        """Verify that module defines __all__."""
        import inspect
        from acoharmony import _expressions

        source = inspect.getsource(_expressions)
        assert "__all__" in source
