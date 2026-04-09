#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""
Audit Python modules for missing or incomplete docstrings.

This module scans all Python files in the project to identify:
- Modules without docstrings
- Functions/methods without docstrings
- Docstrings missing Examples sections
- Docstrings missing Parameters/Returns sections

Generate a markdown report:
True
"""

import ast
from dataclasses import dataclass, field
from pathlib import Path

from acoharmony._log import get_logger

logger = get_logger("dev.audit_docstrings")


@dataclass
class DocstringIssue:
    """
    Represents a docstring issue in the code.

        Attributes

        file_path : Path
            Path to the file with the issue
        line_number : int
            Line number where the issue occurs
        item_type : str
            Type of item (module, function, class, method)
        item_name : str
            Name of the item
        issue_type : str
            Type of issue (missing, no_examples, no_params, no_returns)
        current_docstring : str
            Current docstring content (may be empty)
    """

    file_path: Path
    line_number: int
    item_type: str
    item_name: str
    issue_type: str
    current_docstring: str = ""


@dataclass
class AuditReport:
    """
    Complete audit report of docstring issues.

        Attributes

        total_modules : int
            Total number of modules scanned
        total_functions : int
            Total number of functions found
        total_classes : int
            Total number of classes found
        issues : list[DocstringIssue]
            List of all issues found
        missing_module_docs : list[DocstringIssue]
            Modules without docstrings
        missing_function_docs : list[DocstringIssue]
            Functions without docstrings
        missing_examples : list[DocstringIssue]
            Items without example sections
        missing_params : list[DocstringIssue]
            Functions/methods missing parameter documentation
        missing_returns : list[DocstringIssue]
            Functions/methods missing return documentation
    """

    total_modules: int = 0
    total_functions: int = 0
    total_classes: int = 0
    issues: list[DocstringIssue] = field(default_factory=list)
    missing_module_docs: list[DocstringIssue] = field(default_factory=list)
    missing_function_docs: list[DocstringIssue] = field(default_factory=list)
    missing_examples: list[DocstringIssue] = field(default_factory=list)
    missing_params: list[DocstringIssue] = field(default_factory=list)
    missing_returns: list[DocstringIssue] = field(default_factory=list)


def has_examples_section(docstring: str) -> bool:
    """
    Check if a docstring has an Examples section.

        Parameters

        docstring : str
            The docstring to check

        Returns

        bool
            True if Examples section found
        False
    """
    if not docstring:
        return False

    lines = docstring.lower().split("\n")
    for line in lines:
        if line.strip() in ["examples", "example"]:
            return True
    return False


def has_parameters_section(docstring: str) -> bool:
    """
    Check if a docstring has a Parameters section.

        Parameters

        docstring : str
            The docstring to check

        Returns

        bool
            True if Parameters section found
    """
    if not docstring:
        return False

    lines = docstring.lower().split("\n")
    for line in lines:
        if line.strip() in ["parameters", "params", "arguments", "args"]:
            return True
    return False


def has_returns_section(docstring: str) -> bool:
    """
    Check if a docstring has a Returns section.

        Parameters

        docstring : str
            The docstring to check

        Returns

        bool
            True if Returns section found
    """
    if not docstring:
        return False

    lines = docstring.lower().split("\n")
    for line in lines:
        if line.strip() in ["returns", "return"]:
            return True
    return False


def audit_module_file(file_path: Path) -> list[DocstringIssue]:
    """
    Audit a single Python file for docstring issues.

        Parameters

        file_path : Path
            Path to the Python file

        Returns

        list[DocstringIssue]
            List of issues found in the file
    """
    issues = []

    # Skip example checks for _dev tools
    is_dev_tool = "_dev" in file_path.parts

    try:
        with open(file_path) as f:
            source = f.read()

        tree = ast.parse(source)

        # Check module docstring
        module_doc = ast.get_docstring(tree)
        if not module_doc:
            issues.append(
                DocstringIssue(
                    file_path=file_path,
                    line_number=1,
                    item_type="module",
                    item_name=str(file_path),
                    issue_type="missing",
                    current_docstring="",
                )
            )
        elif not is_dev_tool and not has_examples_section(module_doc):
            issues.append(
                DocstringIssue(
                    file_path=file_path,
                    line_number=1,
                    item_type="module",
                    item_name=str(file_path),
                    issue_type="no_examples",
                    current_docstring=module_doc,
                )
            )

        # Check all functions and classes
        for node in ast.walk(tree):
            # Check functions
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                # Skip private functions
                if node.name.startswith("_") and not node.name.startswith("__"):
                    continue

                func_doc = ast.get_docstring(node)

                if not func_doc:
                    issues.append(
                        DocstringIssue(
                            file_path=file_path,
                            line_number=node.lineno,
                            item_type="function",
                            item_name=node.name,
                            issue_type="missing",
                            current_docstring="",
                        )
                    )
                else:
                    # Check for Examples (skip for _dev tools)
                    if not is_dev_tool and not has_examples_section(func_doc):
                        issues.append(
                            DocstringIssue(
                                file_path=file_path,
                                line_number=node.lineno,
                                item_type="function",
                                item_name=node.name,
                                issue_type="no_examples",
                                current_docstring=func_doc,
                            )
                        )

                    # Check for Parameters (if function has args)
                    if node.args.args and not has_parameters_section(func_doc):
                        issues.append(
                            DocstringIssue(
                                file_path=file_path,
                                line_number=node.lineno,
                                item_type="function",
                                item_name=node.name,
                                issue_type="no_params",
                                current_docstring=func_doc,
                            )
                        )

                    # Check for Returns (if function has return annotation)
                    if node.returns and not has_returns_section(func_doc):
                        issues.append(
                            DocstringIssue(
                                file_path=file_path,
                                line_number=node.lineno,
                                item_type="function",
                                item_name=node.name,
                                issue_type="no_returns",
                                current_docstring=func_doc,
                            )
                        )

            # Check classes
            elif isinstance(node, ast.ClassDef):
                # Skip private classes
                if node.name.startswith("_") and not node.name.startswith("__"):
                    continue

                class_doc = ast.get_docstring(node)

                if not class_doc:
                    issues.append(
                        DocstringIssue(
                            file_path=file_path,
                            line_number=node.lineno,
                            item_type="class",
                            item_name=node.name,
                            issue_type="missing",
                            current_docstring="",
                        )
                    )
                elif not is_dev_tool and not has_examples_section(class_doc):
                    issues.append(
                        DocstringIssue(
                            file_path=file_path,
                            line_number=node.lineno,
                            item_type="class",
                            item_name=node.name,
                            issue_type="no_examples",
                            current_docstring=class_doc,
                        )
                    )

        return issues

    except Exception as e:  # ALLOWED: Logs error and returns, caller handles the error condition
        logger.error(f"Failed to audit {file_path}: {e}")
        return []


def audit_all_modules(src_dir: Path = Path("src/acoharmony")) -> AuditReport:
    """
    Audit all Python modules in the project.

        Parameters

        src_dir : Path
            Source directory to scan

        Returns

        AuditReport
            Complete audit report
    """
    logger.info(f"Starting docstring audit of {src_dir}")

    report = AuditReport()

    if not src_dir.exists():
        logger.error(f"Source directory not found: {src_dir}")
        return report

    # Find all Python files
    py_files = []
    for py_file in src_dir.rglob("*.py"):
        if "__pycache__" not in str(py_file):
            py_files.append(py_file)

    report.total_modules = len(py_files)
    logger.info(f"Found {report.total_modules} Python modules to audit")

    # Audit each file
    for py_file in py_files:
        issues = audit_module_file(py_file)

        for issue in issues:
            report.issues.append(issue)

            # Categorize issues
            if issue.issue_type == "missing":
                if issue.item_type == "module":
                    report.missing_module_docs.append(issue)
                elif issue.item_type in ["function", "method"]:
                    report.missing_function_docs.append(issue)
            elif issue.issue_type == "no_examples":
                report.missing_examples.append(issue)
            elif issue.issue_type == "no_params":
                report.missing_params.append(issue)
            elif issue.issue_type == "no_returns":
                report.missing_returns.append(issue)

    logger.info(f"Audit complete. Found {len(report.issues)} total issues")
    logger.info(f"  - Missing module docstrings: {len(report.missing_module_docs)}")
    logger.info(f"  - Missing function docstrings: {len(report.missing_function_docs)}")
    logger.info(f"  - Missing examples: {len(report.missing_examples)}")
    logger.info(f"  - Missing parameters docs: {len(report.missing_params)}")
    logger.info(f"  - Missing returns docs: {len(report.missing_returns)}")

    return report


def generate_audit_report(output_path: Path = Path("docs/DOCSTRING_AUDIT.md")) -> bool:
    """
    Generate a markdown report of docstring audit findings.

        Parameters

        output_path : Path
            Where to write the report

        Returns

        bool
            True if successful
    """
    logger.info("Generating docstring audit report")

    report = audit_all_modules()

    lines = []
    lines.append("# Docstring Audit Report\n")
    lines.append("*Generated automatically by `audit_docstrings.py`*\n")
    lines.append("\n## Summary\n")
    lines.append(f"- **Total Modules Scanned**: {report.total_modules}")
    lines.append(f"- **Total Issues Found**: {len(report.issues)}")
    lines.append(f"- **Modules Missing Docstrings**: {len(report.missing_module_docs)}")
    lines.append(f"- **Functions Missing Docstrings**: {len(report.missing_function_docs)}")
    lines.append(f"- **Items Missing Examples**: {len(report.missing_examples)}")
    lines.append(f"- **Functions Missing Parameters Docs**: {len(report.missing_params)}")
    lines.append(f"- **Functions Missing Returns Docs**: {len(report.missing_returns)}")

    # Missing module docstrings
    if report.missing_module_docs:
        lines.append("\n## Modules Without Docstrings\n")
        for issue in sorted(report.missing_module_docs, key=lambda x: str(x.file_path)):
            lines.append(f"- `{issue.file_path}` (line {issue.line_number})")

    # Missing function docstrings
    if report.missing_function_docs:
        lines.append("\n## Functions Without Docstrings\n")
        for issue in sorted(report.missing_function_docs, key=lambda x: (str(x.file_path), x.line_number)):
            lines.append(f"- `{issue.file_path}:{issue.line_number}` - `{issue.item_name}()`")

    # Missing examples (high priority)
    if report.missing_examples:
        lines.append("\n## Items Missing Examples (Priority)\n")
        lines.append("\nThese items have docstrings but no Examples section:\n")
        by_file = {}
        for issue in report.missing_examples:
            file_str = str(issue.file_path)
            if file_str not in by_file:
                by_file[file_str] = []
            by_file[file_str].append(issue)

        for file_path in sorted(by_file.keys()):
            lines.append(f"\n### `{file_path}`\n")
            for issue in sorted(by_file[file_path], key=lambda x: x.line_number):
                lines.append(f"- Line {issue.line_number}: `{issue.item_name}` ({issue.item_type})")

    # Missing parameter docs
    if report.missing_params:
        lines.append("\n## Functions Missing Parameter Documentation\n")
        for issue in sorted(report.missing_params, key=lambda x: (str(x.file_path), x.line_number))[:50]:
            lines.append(f"- `{issue.file_path}:{issue.line_number}` - `{issue.item_name}()`")
        if len(report.missing_params) > 50:
            lines.append(f"\n*... and {len(report.missing_params) - 50} more*")

    # Action items
    lines.append("\n## Action Items\n")
    lines.append("1. Add module docstrings to files listed above")
    lines.append("2. Add function docstrings to all public functions")
    lines.append("3. **Priority**: Add Examples sections to all docstrings")
    lines.append("4. Add Parameters documentation for all function arguments")
    lines.append("5. Add Returns documentation for all functions with return values")
    lines.append("\n## Example Format\n")
    lines.append("```python")
    """
    Short description.')
        lines.append("")
        lines.append("Longer description if needed.")
        lines.append("")
        lines.append("Parameters")
        lines.append("----------")
        lines.append("arg1 : type")
        lines.append("    Description")
        lines.append("")
        lines.append("Returns")
        lines.append("-------")
        lines.append("return_type")
        lines.append("    Description")
        lines.append("")
        lines.append("Examples")
        lines.append("--------")
        lines.append(">>> from module import function")
        lines.append(">>> result = function(arg1)")
        lines.append(">>> isinstance(result, expected_type)")
        lines.append("True")
        lines.append('
    """
    lines.append("```")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write("\n".join(lines))

    logger.info(f"Report written to {output_path}")
    return True
