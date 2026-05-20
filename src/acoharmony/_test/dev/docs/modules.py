"""Tests for acoharmony._dev.docs.modules module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony
from acoharmony._dev.docs.modules import (
    FuncInfo,
    ParamInfo,
    _extract_all_exports,
    _sig_str,
    parse_module,
)


import ast
import tempfile
from pathlib import Path


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._dev.docs.modules is not None


class TestExtractAllExportsNoAll:
    """Cover branch 250->248: __all__ not found (loop exhausts without returning)."""

    @pytest.mark.unit
    def test_no_all_export_returns_empty(self):
        """Module with no __all__ returns empty list."""
        source = "x = 1\ny = 2\n"
        tree = ast.parse(source)
        result = _extract_all_exports(tree)
        assert result == []

    @pytest.mark.unit
    def test_all_not_list_or_tuple(self):
        """__all__ assigned to a non-list/tuple (e.g., a set) returns empty."""
        source = "__all__ = {'foo', 'bar'}\n"
        tree = ast.parse(source)
        result = _extract_all_exports(tree)
        assert result == []


class TestSigStrWithDefault:
    """Cover branch 298->300: param has a default value."""

    @pytest.mark.unit
    def test_param_with_annotation_and_default(self):
        """A param with annotation and default produces 'name: type = default'."""
        func = FuncInfo(
            name="example",
            params=[
                ParamInfo(name="x", annotation="int", default="10"),
                ParamInfo(name="y", annotation="str", default="'hello'"),
            ],
            returns="None",
        )
        result = _sig_str(func)
        assert result == "(x: int = 10, y: str = 'hello') -> None"

    @pytest.mark.unit
    def test_param_with_default_no_annotation(self):
        """A param with default but no annotation produces 'name = default'."""
        func = FuncInfo(
            name="example",
            params=[
                ParamInfo(name="flag", annotation="", default="True"),
            ],
            returns="",
        )
        result = _sig_str(func)
        assert result == "(flag = True)"


class TestParseModuleAllExports:
    """Cover branch 250->248 via parse_module."""

    @pytest.mark.unit
    def test_parse_module_with_all_list(self):
        """parse_module extracts __all__ when it's a list."""
        source = '__all__ = ["foo", "bar"]\n\ndef foo(): pass\ndef bar(): pass\n'
        with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
            f.write(source)
            f.flush()
            info = parse_module(Path(f.name))
        assert info.all_exports == ["foo", "bar"]

    @pytest.mark.unit
    def test_parse_module_without_all(self):
        """parse_module returns empty all_exports when no __all__."""
        source = 'def foo(): pass\n'
        with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
            f.write(source)
            f.flush()
            info = parse_module(Path(f.name))
        assert info.all_exports == []
