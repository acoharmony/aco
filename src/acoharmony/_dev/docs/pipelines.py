#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""
Generate comprehensive pipeline documentation from code.

This tool extracts docstrings and structure from transforms and expressions
to generate markdown documentation showing the refactored architecture.

"""

import ast
import importlib
import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from acoharmony._log import get_logger

logger = get_logger("dev.generate_pipeline_docs")


@dataclass
class FunctionDoc:
    """Documentation for a single function."""

    name: str
    signature: str
    docstring: str
    module: str
    is_private: bool
    returns: str


@dataclass
class ModuleDoc:
    """Documentation for a module."""

    name: str
    path: Path
    docstring: str
    functions: list[FunctionDoc]
    module_type: str  # "expression" or "transform"


def extract_return_type_from_source(filepath: Path, function_name: str) -> str:
    """
    Extract return type from function source using AST.

    Args:
        filepath: Path to the Python file
        function_name: Name of the function

    Returns:
        str: Return type annotation or "Unknown"
    """
    try:
        with open(filepath) as f:
            tree = ast.parse(f.read())

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == function_name:
                if node.returns:
                    return ast.unparse(node.returns)
        return "Unknown"
    except Exception as e:
        logger.debug(f"Could not extract return type for {function_name}: {e}")
        return "Unknown"


def document_module(module_path: Path, module_type: str) -> ModuleDoc | None:
    """
    Extract documentation from a Python module.

    Args:
        module_path: Path to the Python file
        module_type: Type of module ("expression" or "transform")

    Returns:
        ModuleDoc | None: Module documentation or None if failed
    """
    try:
        # Convert path to module name
        rel_path = module_path.relative_to(Path("src"))
        module_name = str(rel_path.with_suffix("")).replace("/", ".")

        # Import the module
        module = importlib.import_module(module_name)

        # Get module docstring
        module_doc = inspect.getdoc(module) or ""

        # Extract functions
        functions = []
        for name, obj in inspect.getmembers(module, inspect.isfunction):
            if obj.__module__ == module_name:  # Only functions defined in this module
                sig = str(inspect.signature(obj))
                doc = inspect.getdoc(obj) or ""
                is_private = name.startswith("_")

                # Try to get return type from source
                return_type = extract_return_type_from_source(module_path, name)

                functions.append(
                    FunctionDoc(
                        name=name,
                        signature=f"{name}{sig}",
                        docstring=doc,
                        module=module_name,
                        is_private=is_private,
                        returns=return_type,
                    )
                )

        return ModuleDoc(
            name=module_path.stem,
            path=module_path,
            docstring=module_doc,
            functions=functions,
            module_type=module_type,
        )

    except Exception as e:
        logger.warning(f"Failed to document {module_path}: {e}")
        return None


def generate_expression_docs() -> list[ModuleDoc]:
    """
    Generate documentation for all expression modules.

    Returns:
        list[ModuleDoc]: List of documented expression modules
    """
    logger.info("Generating expression documentation...")
    expressions_dir = Path("src/acoharmony/_expressions")

    docs = []
    for py_file in sorted(expressions_dir.glob("_*.py")):
        if py_file.stem == "__init__":
            continue

        doc = document_module(py_file, "expression")
        if doc:
            docs.append(doc)
            logger.debug(f"Documented expression: {doc.name}")

    logger.info(f"Documented {len(docs)} expression modules")
    return docs


def generate_transform_docs() -> list[ModuleDoc]:
    """
    Generate documentation for all transform modules.

    Returns:
        list[ModuleDoc]: List of documented transform modules
    """
    logger.info("Generating transform documentation...")
    transforms_dir = Path("src/acoharmony/_transforms")

    docs = []
    for py_file in sorted(transforms_dir.glob("*.py")):
        if py_file.stem == "__init__":
            continue

        doc = document_module(py_file, "transform")
        if doc:
            docs.append(doc)
            logger.debug(f"Documented transform: {doc.name}")

    logger.info(f"Documented {len(docs)} transform modules")
    return docs


def format_function_doc(func: FunctionDoc, include_private: bool = False) -> str:
    """
    Format a function's documentation as markdown.

    Args:
        func: Function documentation
        include_private: Whether to include private functions

    Returns:
        str: Markdown formatted documentation
    """
    if func.is_private and not include_private:
        return ""

    visibility = "🔒 Private" if func.is_private else "🌐 Public"
    md = [
        f"### `{func.name}()`",
        "",
        f"**{visibility}** | Returns: `{func.returns}`",
        "",
    ]

    if func.docstring:
        md.extend(["```", func.docstring, "```", ""])
    else:
        md.extend(["*No documentation available*", ""])

    md.append("---")
    md.append("")

    return "\n".join(md)


def format_module_doc(module: ModuleDoc, include_private: bool = True) -> str:
    """
    Format a module's documentation as markdown.

    Args:
        module: Module documentation
        include_private: Whether to include private functions

    Returns:
        str: Markdown formatted documentation
    """
    icon = "🔧" if module.module_type == "expression" else "⚙"

    md = [
        f"## {icon} `{module.name}`",
        "",
        f"**Type**: {module.module_type.title()}",
        "",
    ]

    if module.docstring:
        md.extend([module.docstring, "", "---", ""])

    # Separate public and private functions
    public_funcs = [f for f in module.functions if not f.is_private]
    private_funcs = [f for f in module.functions if f.is_private]

    if public_funcs:
        md.extend(["### Public Functions", ""])
        for func in public_funcs:
            md.append(format_function_doc(func, include_private=False))

    if private_funcs and include_private:
        md.extend(["### Helper Functions", ""])
        for func in private_funcs:
            md.append(format_function_doc(func, include_private=True))

    return "\n".join(md)


def generate_architecture_overview() -> str:
    """
    Generate overview of the refactored architecture.

    Returns:
        str: Markdown formatted architecture overview
    """
    return """# ACOHarmony Pipeline Architecture

*Auto-generated documentation from source code*

## Overview

The ACOHarmony pipeline follows a clean separation between **expressions** and **transforms**:

- **Expressions** (`_expressions/`): Return `pl.Expr` or `list[pl.Expr]` - pure column logic
- **Transforms** (`_transforms/`): Return `pl.LazyFrame` - data orchestration

This architecture enables:
- **Composability**: Expressions can be reused across transforms
- **Testability**: Pure functions are easy to test
- **Maintainability**: Clear separation of concerns
- **Performance**: Lazy evaluation throughout the pipeline

## Medallion Architecture

```
Bronze (Raw Data)
    ↓
    Expression Builders → Column Logic (pl.Expr)
    ↓
Silver (Cleaned & Normalized)
    ↓
    Transform Functions → Data Orchestration (pl.LazyFrame)
    ↓
Gold (Analytics-Ready)
```

## Key Principles

1. **Idempotency**: Same inputs always produce same outputs
2. **No Side Effects**: Functions don't modify external state
3. **Lazy Evaluation**: Use `pl.LazyFrame` for memory efficiency
4. **Type Safety**: All functions have type annotations
5. **Documentation**: All functions have docstrings

---

"""


def generate_pipeline_groups() -> str:
    """
    Generate documentation for logical pipeline groups.

    Returns:
        str: Markdown formatted pipeline groups
    """
    return """# Pipeline Groups

Transforms are organized into logical groups by domain:

## ACO Alignment Pipeline

Transforms enrollment data from CMS alignment files (BAR/ALR) into temporal tracking matrices.

**Transforms**:
- `_aco_alignment_temporal` - Build temporal matrix with year-month columns
- `_aco_alignment_voluntary` - Join voluntary alignment (SVA/PBVAR)
- `_aco_alignment_provider` - Validate provider TINs/NPIs
- `_aco_alignment_office` - Match office locations
- `_aco_alignment_demographics` - Join demographics data
- `_aco_alignment_metrics` - Calculate enrollment metrics
- `_aco_alignment_metadata` - Add action flags and enrichment

**Key Expressions**:
- `_aco_temporal_bar` - REACH DCE alignment preparation
- `_aco_temporal_alr` - MSSP alignment preparation
- `_aco_temporal_ffs` - FFS claims logic
- `_aco_temporal_demographics` - Demographics joins
- `_aco_temporal_summary` - Summary statistics
- `_aco_voluntary_join` - SVA/PBVAR joins
- `_aco_provider_join` - Provider validation
- `_aco_office_match` - Office matching
- `_aco_metrics` - Metric calculations
- `_aco_metadata` - Metadata enrichment

## Claims Processing Pipeline

Transforms raw claims data into analytics-ready formats following Tuva data models.

## Analytics Pipeline

Downstream analytics transforms for risk scoring, quality measures, and financial analysis.

---

"""


def generate_full_documentation(output_dir: Path = Path("docs/pipelines")):
    """
    Generate complete pipeline documentation.

    Args:
        output_dir: Directory to write documentation files
    """
    logger.info("Starting comprehensive pipeline documentation generation")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate architecture overview
    logger.info("Generating architecture overview...")
    overview = generate_architecture_overview()
    with open(output_dir / "00_ARCHITECTURE.md", "w") as f:
        f.write(overview)

    # Generate pipeline groups
    logger.info("Generating pipeline groups...")
    groups = generate_pipeline_groups()
    with open(output_dir / "01_PIPELINE_GROUPS.md", "w") as f:
        f.write(groups)

    # Generate expression documentation
    logger.info("Generating expression documentation...")
    expr_docs = generate_expression_docs()
    expr_md = ["# Expression Modules\n\n"]
    expr_md.append("Expression modules return `pl.Expr` or `list[pl.Expr]` for column-level logic.\n\n")
    expr_md.append("---\n\n")

    for doc in expr_docs:
        expr_md.append(format_module_doc(doc, include_private=True))
        expr_md.append("\n\n")

    with open(output_dir / "02_EXPRESSIONS.md", "w") as f:
        f.write("\n".join(expr_md))

    # Generate transform documentation
    logger.info("Generating transform documentation...")
    transform_docs = generate_transform_docs()
    transform_md = ["# Transform Modules\n\n"]
    transform_md.append("Transform modules return `pl.LazyFrame` for data orchestration.\n\n")
    transform_md.append("---\n\n")

    for doc in transform_docs:
        transform_md.append(format_module_doc(doc, include_private=True))
        transform_md.append("\n\n")

    with open(output_dir / "03_TRANSFORMS.md", "w") as f:
        f.write("\n".join(transform_md))

    # Generate index
    logger.info("Generating index...")
    index = [
        "# Pipeline Documentation Index\n\n",
        "Complete documentation for ACOHarmony pipelines, expressions, and transforms.\n\n",
        "## Documentation Files\n\n",
        "1. [Architecture Overview](00_ARCHITECTURE.md) - System design and principles\n",
        "2. [Pipeline Groups](01_PIPELINE_GROUPS.md) - Logical groupings of transforms\n",
        f"3. [Expression Modules](02_EXPRESSIONS.md) - {len(expr_docs)} expression builders\n",
        f"4. [Transform Modules](03_TRANSFORMS.md) - {len(transform_docs)} data transforms\n",
        "\n## Statistics\n\n",
        f"- **Expression Modules**: {len(expr_docs)}\n",
        f"- **Transform Modules**: {len(transform_docs)}\n",
        f"- **Total Functions**: {sum(len(d.functions) for d in expr_docs + transform_docs)}\n",
        "\n---\n\n",
        "*Auto-generated from source code. Run `uv run python -m acoharmony._dev.generate_pipeline_docs` to update.*\n",
    ]

    with open(output_dir / "README.md", "w") as f:
        f.write("\n".join(index))

    logger.info(f"[SUCCESS] Documentation generated in {output_dir}")
    logger.info(f"   - {len(expr_docs)} expression modules")
    logger.info(f"   - {len(transform_docs)} transform modules")


if __name__ == "__main__":
    generate_full_documentation()
