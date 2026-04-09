"""Tests for acoharmony._dev.docs.pipelines module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import acoharmony
from acoharmony._dev.docs.pipelines import (
    document_module,
    generate_expression_docs,
)


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._dev.docs.pipelines is not None


class TestDocumentModuleSkipsImportedFunctions:
    """Cover branch 98->97: function whose __module__ != module_name is skipped."""

    @pytest.mark.unit
    def test_imported_function_excluded(self, tmp_path, monkeypatch):
        """Functions imported from other modules are excluded from the documented functions list."""
        # Create a module file under src/ so relative_to(Path("src")) works
        src_dir = tmp_path / "src"
        pkg_dir = src_dir / "fakepkg"
        pkg_dir.mkdir(parents=True)
        mod_file = pkg_dir / "mymod.py"
        mod_file.write_text(
            '"""Module doc."""\n\n'
            "def local_func():\n"
            '    """Local."""\n'
            "    pass\n"
        )

        # Build a fake module with both a local and an imported function
        fake_mod = types.ModuleType("fakepkg.mymod")
        fake_mod.__doc__ = "Module doc."

        def local_func():
            """Local."""

        local_func.__module__ = "fakepkg.mymod"

        # An imported function whose __module__ is different
        def imported_func():
            """Imported."""

        imported_func.__module__ = "os.path"

        fake_mod.local_func = local_func
        fake_mod.imported_func = imported_func

        # chdir so that relative Path("src") resolves under tmp_path
        monkeypatch.chdir(tmp_path)
        # Use a relative path so relative_to(Path("src")) works
        rel_mod_file = Path("src") / "fakepkg" / "mymod.py"

        with patch("acoharmony._dev.docs.pipelines.importlib.import_module", return_value=fake_mod):
            doc = document_module(rel_mod_file, "expression")

        assert doc is not None
        func_names = [f.name for f in doc.functions]
        assert "local_func" in func_names
        assert "imported_func" not in func_names


class TestGenerateExpressionDocsNoneModule:
    """Cover branch 146->141: document_module returns None, skipped."""

    @pytest.mark.unit
    def test_none_module_skipped(self, tmp_path):
        """When document_module returns None, it is not added to the docs list."""
        # Create a directory with a Python file matching _*.py pattern
        expr_dir = tmp_path / "expressions"
        expr_dir.mkdir()
        (expr_dir / "_bad_module.py").write_text("raise SyntaxError\n")

        with patch(
            "acoharmony._dev.docs.pipelines.Path",
            return_value=expr_dir,
        ) as mock_path_cls:
            # Make Path("src/acoharmony/_expressions") return our temp dir
            mock_path_cls.side_effect = lambda x: expr_dir if "_expressions" in str(x) else Path(x)

            with patch(
                "acoharmony._dev.docs.pipelines.document_module",
                return_value=None,
            ) as mock_doc:
                # Patch the expressions dir glob to return our file
                with patch.object(
                    Path, "glob", return_value=[expr_dir / "_bad_module.py"]
                ):
                    docs = generate_expression_docs()

        assert docs == []
