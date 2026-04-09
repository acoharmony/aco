"""Tests for acoharmony._dev.analysis.docstrings module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from acoharmony._dev.analysis import docstrings
from acoharmony._dev.analysis.docstrings import (
    AuditReport,
    DocstringIssue,
    audit_all_modules,
    audit_module_file,
    generate_audit_report,
)


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert docstrings is not None


class TestAuditModuleFileClassWithExamples:
    """Cover branch 297->218: class has docstring with examples in non-dev file."""

    @pytest.mark.unit
    def test_class_with_examples_section_no_issue(self, tmp_path):
        """A class with an Examples section in a non-dev file produces no 'no_examples' issue."""
        src = textwrap.dedent('''\
            """Module docstring.

            Examples
            --------
            >>> import foo
            """

            class MyClass:
                """Class docstring.

                Examples
                --------
                >>> MyClass()
                """
                pass
        ''')
        py_file = tmp_path / "sample.py"
        py_file.write_text(src)
        issues = audit_module_file(py_file)
        class_example_issues = [
            i for i in issues
            if i.item_type == "class" and i.issue_type == "no_examples"
        ]
        assert class_example_issues == []

    @pytest.mark.unit
    def test_class_in_dev_tool_skips_example_check(self, tmp_path):
        """A class in a _dev path skips the examples check (elif is False)."""
        dev_dir = tmp_path / "_dev"
        dev_dir.mkdir()
        src = textwrap.dedent('''\
            """Module docstring."""

            class MyClass:
                """Class docstring without examples."""
                pass
        ''')
        py_file = dev_dir / "sample.py"
        py_file.write_text(src)
        issues = audit_module_file(py_file)
        class_example_issues = [
            i for i in issues
            if i.item_type == "class" and i.issue_type == "no_examples"
        ]
        assert class_example_issues == []


class TestAuditAllModulesNoReturns:
    """Cover branch 364->351: issue with type 'no_returns' in audit_all_modules."""

    @pytest.mark.unit
    def test_no_returns_issue_categorized(self, tmp_path):
        """An issue with issue_type='no_returns' is appended to report.missing_returns."""
        src = textwrap.dedent('''\
            """Module docstring."""

            def compute(x: int) -> int:
                """Compute something.

                Parameters
                ----------
                x : int
                    Input value.
                """
                return x + 1
        ''')
        py_file = tmp_path / "mod.py"
        py_file.write_text(src)
        report = audit_all_modules(src_dir=tmp_path)
        assert len(report.missing_returns) >= 1
        assert any(i.item_name == "compute" for i in report.missing_returns)


class TestGenerateAuditReportMultipleIssuesSameFile:
    """Cover branch 426->428: second issue from same file already in by_file dict."""

    @pytest.mark.unit
    def test_multiple_missing_examples_same_file(self, tmp_path):
        """Two missing-examples items from the same file exercises the else branch of 'if file_str not in by_file'."""
        src = textwrap.dedent('''\
            """Module docstring.

            Examples
            --------
            >>> pass
            """

            def foo():
                """Foo docstring."""
                pass

            def bar():
                """Bar docstring."""
                pass
        ''')
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        py_file = src_dir / "two_funcs.py"
        py_file.write_text(src)

        with patch(
            "acoharmony._dev.analysis.docstrings.audit_all_modules"
        ) as mock_audit:
            report = AuditReport()
            report.total_modules = 1
            issue1 = DocstringIssue(
                file_path=py_file,
                line_number=8,
                item_type="function",
                item_name="foo",
                issue_type="no_examples",
                current_docstring="Foo docstring.",
            )
            issue2 = DocstringIssue(
                file_path=py_file,
                line_number=12,
                item_type="function",
                item_name="bar",
                issue_type="no_examples",
                current_docstring="Bar docstring.",
            )
            report.issues = [issue1, issue2]
            report.missing_examples = [issue1, issue2]
            mock_audit.return_value = report

            output_file = tmp_path / "report.md"
            result = generate_audit_report(output_path=output_file)
            assert result is True
            content = output_file.read_text()
            assert "foo" in content
            assert "bar" in content


class TestAuditAllModulesNoReturnsBranch:
    """Cover branch 364->351: issue with type 'no_returns' is categorized."""

    @pytest.mark.unit
    def test_no_returns_issue_categorized(self, tmp_path):
        """Branch 364->351: issue_type 'no_returns' appends to missing_returns."""
        py_file = tmp_path / "test_module.py"
        py_file.write_text(
            '"""Module doc."""\n\n'
            'def my_func():\n'
            '    """Does something.\n\n'
            '    Parameters\n'
            '    ----------\n'
            '    x : int\n'
            '        A param.\n'
            '    """\n'
            '    pass\n'
        )

        no_returns_issue = DocstringIssue(
            file_path=py_file,
            item_name="my_func",
            item_type="function",
            issue_type="no_returns",
            line_number=3,
        )

        with patch("acoharmony._dev.analysis.docstrings.audit_module_file") as mock_audit:
            mock_audit.return_value = [no_returns_issue]

            # Create a minimal src dir with one .py file
            src_dir = tmp_path / "src" / "acoharmony"
            src_dir.mkdir(parents=True)
            (src_dir / "mod.py").write_text("# code\n")

            report = audit_all_modules(src_dir=src_dir)
            assert len(report.missing_returns) == 1
            assert report.missing_returns[0].issue_type == "no_returns"


class TestAuditAllModulesUnknownIssueType:
    """Cover branch 364->351: issue_type not matching any known category falls through."""

    @pytest.mark.unit
    def test_unknown_issue_type_still_added_to_issues(self, tmp_path):
        """An issue with an unrecognized issue_type is added to report.issues but not categorized."""
        unknown_issue = DocstringIssue(
            file_path=tmp_path / "mod.py",
            item_name="func",
            item_type="function",
            issue_type="unknown_type",
            line_number=1,
        )

        with patch("acoharmony._dev.analysis.docstrings.audit_module_file") as mock_audit:
            mock_audit.return_value = [unknown_issue]

            src_dir = tmp_path / "src" / "acoharmony"
            src_dir.mkdir(parents=True)
            (src_dir / "mod.py").write_text("# code\n")

            report = audit_all_modules(src_dir=src_dir)
            # Issue is in the master list but not in any category
            assert len(report.issues) == 1
            assert len(report.missing_module_docs) == 0
            assert len(report.missing_function_docs) == 0
            assert len(report.missing_examples) == 0
            assert len(report.missing_params) == 0
            assert len(report.missing_returns) == 0
