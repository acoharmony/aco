# © 2025 HarmonyCares
"""Tests for acoharmony/_exceptions/_registry.py."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

from acoharmony._exceptions._base import ACOHarmonyException
from acoharmony._exceptions._registry import ExceptionRegistry, register_exception


class TestRegistry:
    """Test suite for _registry."""

    @pytest.mark.unit
    def test_register_exception(self) -> None:
        """Test register_exception function."""
        decorator = register_exception("TEST_REG_001", category="test")
        assert callable(decorator)

    @pytest.mark.unit
    def test_register(self) -> None:
        """Test register function."""
        # ExceptionRegistry.register is used as a decorator and tested via
        # the existing registered exceptions; verify a known one is present
        exc_cls = ExceptionRegistry.get("PARSE_001")
        assert exc_cls is not None
        assert exc_cls.error_code == "PARSE_001"

    @pytest.mark.unit
    def test_get(self) -> None:
        """Test get function."""
        result = ExceptionRegistry.get("STORAGE_001")
        assert result is not None
        assert result.error_code == "STORAGE_001"
        assert ExceptionRegistry.get("NONEXISTENT_999") is None

    @pytest.mark.unit
    def test_get_by_category(self) -> None:
        """Test get_by_category function."""
        storage_exceptions = ExceptionRegistry.get_by_category("storage")
        assert len(storage_exceptions) > 0
        assert ExceptionRegistry.get_by_category("nonexistent") == []

    @pytest.mark.unit
    def test_all_codes(self) -> None:
        """Test all_codes function."""
        codes = ExceptionRegistry.all_codes()
        assert isinstance(codes, list)
        assert len(codes) > 0
        assert codes == sorted(codes)  # verify sorted
        assert "STORAGE_001" in codes

    @pytest.mark.unit
    def test_exceptionregistry_init(self) -> None:
        """Test ExceptionRegistry initialization."""
        # ExceptionRegistry uses class-level dicts, verify they exist
        assert isinstance(ExceptionRegistry._registry, dict)
        assert isinstance(ExceptionRegistry._by_category, dict)
        # Verify categories exist
        categories = ExceptionRegistry.all_categories()
        assert isinstance(categories, list)
        assert len(categories) > 0


class TestExceptionRegistryBranches:
    """Cover uncovered branches in _exceptions/_registry.py."""

    def setup_method(self):
        """Save original registry state."""
        self._orig_registry = ExceptionRegistry._registry.copy()
        self._orig_by_category = {k: v.copy() for k, v in ExceptionRegistry._by_category.items()}

    def teardown_method(self):
        """Restore original registry state."""
        ExceptionRegistry._registry = self._orig_registry
        ExceptionRegistry._by_category = self._orig_by_category

    @pytest.mark.unit
    def test_register_with_why_and_how_templates(self):
        """Branches 62->64, 64->66: why_template and how_template set on class."""

        @ExceptionRegistry.register(
            error_code="TEST_WHY_HOW_001",
            category="test_category",
            why_template="Something went wrong because {reason}",
            how_template="Fix it by doing {action}",
        )
        class TestExc(ACOHarmonyException):
            """Test exception with why and how."""
            pass

        assert TestExc._why_template == "Something went wrong because {reason}"
        assert TestExc._how_template == "Fix it by doing {action}"
        assert TestExc.error_code == "TEST_WHY_HOW_001"
        assert TestExc.category == "test_category"

    @pytest.mark.unit
    def test_register_with_default_causes_and_remediation(self):
        """Branches 66->67, 68->69: default_causes and default_remediation set."""

        @ExceptionRegistry.register(
            error_code="TEST_CAUSES_001",
            category="test_category",
            default_causes=["Cause A", "Cause B"],
            default_remediation=["Step 1", "Step 2"],
        )
        class TestExc2(ACOHarmonyException):
            """Test exception with causes."""
            pass

        assert TestExc2._default_causes == ["Cause A", "Cause B"]
        assert TestExc2._default_remediation == ["Step 1", "Step 2"]

    @pytest.mark.unit
    def test_generate_docs_with_all_templates(self):
        """Branches 174->178, 178->183, 183->188: generate_docs includes templates."""

        @ExceptionRegistry.register(
            error_code="TEST_DOCS_001",
            category="docs_test",
            why_template="This happens when X",
            how_template="Fix by doing Y",
            default_causes=["Config missing", "Invalid input"],
            default_remediation=["Check config", "Validate input"],
        )
        class TestDocsExc(ACOHarmonyException):
            """A documented test exception."""
            pass

        docs = ExceptionRegistry.generate_docs(category="docs_test")
        assert "Why This Happens" in docs
        assert "This happens when X" in docs
        assert "How To Fix" in docs
        assert "Fix by doing Y" in docs
        assert "Common Causes" in docs
        assert "Config missing" in docs
        assert "Remediation Steps" in docs
        assert "Check config" in docs

    @pytest.mark.unit
    def test_generate_docs_without_category(self):
        """Generate docs for all exceptions (no category filter)."""
        docs = ExceptionRegistry.generate_docs()
        assert "All Exceptions" in docs

    @pytest.mark.unit
    def test_generate_docs_with_category_filter(self):
        """Generate docs for specific category."""
        docs = ExceptionRegistry.generate_docs(category="nonexistent_category")
        assert "Category: nonexistent_category" in docs

    @pytest.mark.unit
    def test_generate_docs_no_docstring_no_templates(self):
        """Branches 174->178, 178->183, 183->188: exc class without docstring or templates."""

        @ExceptionRegistry.register(
            error_code="TEST_BARE_001",
            category="bare_test",
        )
        class BareExc(ACOHarmonyException):
            pass

        # Remove docstring if any
        BareExc.__doc__ = None

        docs = ExceptionRegistry.generate_docs(category="bare_test")
        assert "BareExc" in docs
        # Should NOT contain these sections since no templates set
        assert "Why This Happens" not in docs
        assert "How To Fix" not in docs
        assert "Common Causes" not in docs

    @pytest.mark.unit
    def test_register_exception_convenience(self):
        """Test register_exception convenience function."""
        decorator = register_exception("TEST_CONV_001", category="conv")
        assert callable(decorator)

