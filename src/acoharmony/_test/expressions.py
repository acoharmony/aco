# © 2025 HarmonyCares
# All rights reserved.

"""
Tests for the _expressions.py re-export module.

This test module verifies that the public API re-export module correctly
exposes the expression registry and the registration decorator. (The
previously-exposed EnterpriseCrosswalkExpression class was removed when the
enterprise_crosswalk transform was replaced by identity_timeline.)

NOTE: The _expressions.py file is shadowed by the _expressions/ package in
Python's import resolution. The package's __init__.py is what actually gets
loaded by `import acoharmony._expressions`. These tests confirm both the
module's intended public surface and that the package provides it.
"""

import pytest


@pytest.mark.unit
class TestExpressionModuleExports:
    """Test that the core re-exports are accessible."""

    def test_all_exports_defined(self):
        """__all__ contains the registry and decorator."""
        from acoharmony._expressions import __all__

        assert isinstance(__all__, list)
        assert "ExpressionRegistry" in __all__
        assert "register_expression" in __all__

    def test_all_exports_are_strings(self):
        from acoharmony import _expressions

        for item in _expressions.__all__:
            assert isinstance(item, str)

    def test_all_exports_are_accessible(self):
        """Every name in __all__ can be imported from the module."""
        from acoharmony import _expressions

        for name in _expressions.__all__:
            assert hasattr(_expressions, name), f"{name} missing from module"


@pytest.mark.unit
class TestExpressionRegistryReExport:
    """ExpressionRegistry is re-exported and usable."""

    def test_registry_is_accessible(self):
        from acoharmony._expressions import ExpressionRegistry

        assert ExpressionRegistry is not None

    def test_registry_direct_import(self):
        from acoharmony._expressions import ExpressionRegistry

        assert ExpressionRegistry is not None


@pytest.mark.unit
class TestRegisterExpressionReExport:
    """register_expression is re-exported and callable."""

    def test_register_expression_is_accessible(self):
        from acoharmony._expressions import register_expression

        assert register_expression is not None
        assert callable(register_expression)

    def test_register_expression_direct_import(self):
        from acoharmony._expressions import register_expression

        assert callable(register_expression)


@pytest.mark.unit
class TestModuleContainsExpected:
    """Module-level attribute sanity checks."""

    def test_module_has_expression_registry(self):
        from acoharmony import _expressions

        assert hasattr(_expressions, "ExpressionRegistry")

    def test_module_has_register_expression(self):
        from acoharmony import _expressions

        assert hasattr(_expressions, "register_expression")


@pytest.mark.unit
class TestImportResolution:
    """Confirm the package shadowing behavior documented in the module docstring."""

    def test_module_is_importable(self):
        import acoharmony._expressions  # noqa: F401

    def test_module_has_docstring_or_is_package(self):
        from acoharmony import _expressions

        # Either the shadowed file's docstring or the package's is fine.
        assert _expressions.__doc__ is not None or hasattr(_expressions, "__path__")
