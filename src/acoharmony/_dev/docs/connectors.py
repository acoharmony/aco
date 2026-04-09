#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""
Generate comprehensive connector documentation from code.

This tool extracts docstrings and structure from citation connectors
to generate markdown documentation for domain-specific citation sources.

"""

import ast
import importlib
import inspect
from dataclasses import dataclass
from pathlib import Path

from acoharmony._log import get_logger

logger = get_logger("dev.generate_connector_docs")


@dataclass
class FunctionDoc:
    """Documentation for a single function."""

    name: str
    signature: str
    docstring: str
    module: str
    is_private: bool
    returns: str
    is_static: bool


@dataclass
class ClassDoc:
    """Documentation for a class."""

    name: str
    docstring: str
    methods: list[FunctionDoc]
    bases: list[str]


@dataclass
class ModuleDoc:
    """Documentation for a connector module."""

    name: str
    path: Path
    docstring: str
    classes: list[ClassDoc]
    functions: list[FunctionDoc]


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


def document_class(cls: type, module_path: Path, module_name: str) -> ClassDoc:
    """
    Extract documentation from a class.

    Args:
        cls: Class to document
        module_path: Path to the module file
        module_name: Name of the module

    Returns:
        ClassDoc: Class documentation
    """
    class_doc = inspect.getdoc(cls) or ""
    bases = [base.__name__ for base in cls.__bases__ if base.__name__ != "object"]

    methods = []
    for name, method in inspect.getmembers(cls, inspect.isfunction):
        if method.__module__ == module_name:  # Only methods defined in this module
            sig = str(inspect.signature(method))
            doc = inspect.getdoc(method) or ""
            is_private = name.startswith("_") and not name.startswith("__")
            is_static = isinstance(inspect.getattr_static(cls, name), staticmethod)

            # Try to get return type from source
            return_type = extract_return_type_from_source(module_path, name)

            methods.append(
                FunctionDoc(
                    name=name,
                    signature=f"{name}{sig}",
                    docstring=doc,
                    module=module_name,
                    is_private=is_private,
                    returns=return_type,
                    is_static=is_static,
                )
            )

    return ClassDoc(name=cls.__name__, docstring=class_doc, methods=methods, bases=bases)


def document_module(module_path: Path) -> ModuleDoc | None:
    """
    Extract documentation from a connector module.

    Args:
        module_path: Path to the Python file

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

        # Extract classes
        classes = []
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module_name:  # Only classes defined in this module
                class_doc = document_class(obj, module_path, module_name)
                classes.append(class_doc)

        # Extract module-level functions (if any)
        functions = []
        for name, obj in inspect.getmembers(module, inspect.isfunction):
            if obj.__module__ == module_name:
                sig = str(inspect.signature(obj))
                doc = inspect.getdoc(obj) or ""
                is_private = name.startswith("_")
                return_type = extract_return_type_from_source(module_path, name)

                functions.append(
                    FunctionDoc(
                        name=name,
                        signature=f"{name}{sig}",
                        docstring=doc,
                        module=module_name,
                        is_private=is_private,
                        returns=return_type,
                        is_static=False,
                    )
                )

        return ModuleDoc(
            name=module_path.stem,
            path=module_path,
            docstring=module_doc,
            classes=classes,
            functions=functions,
        )

    except Exception as e:
        logger.warning(f"Failed to document {module_path}: {e}")
        return None


def generate_connector_docs() -> list[ModuleDoc]:
    """
    Generate documentation for all connector modules.

    Returns:
        list[ModuleDoc]: List of documented connector modules
    """
    logger.info("Generating connector documentation...")
    connectors_dir = Path("src/acoharmony/_cite/connectors")

    docs = []
    for py_file in sorted(connectors_dir.glob("_*.py")):
        if py_file.stem == "__init__":
            continue

        doc = document_module(py_file)
        if doc:
            docs.append(doc)
            logger.debug(f"Documented connector: {doc.name}")

    logger.info(f"Documented {len(docs)} connector modules")
    return docs


def format_method_doc(method: FunctionDoc) -> str:
    """
    Format a method's documentation as markdown.

    Args:
        method: Method documentation

    Returns:
        str: Markdown formatted documentation
    """
    visibility = "🔒 Private" if method.is_private else "🌐 Public"
    static_badge = " | 🔧 Static" if method.is_static else ""
    md = [
        f"#### `{method.name}()`",
        "",
        f"**{visibility}**{static_badge} | Returns: `{method.returns}`",
        "",
    ]

    if method.docstring:
        md.extend(["```", method.docstring, "```", ""])
    else:
        md.extend(["*No documentation available*", ""])

    md.append("")
    return "\n".join(md)


def format_class_doc(cls: ClassDoc) -> str:
    """
    Format a class's documentation as markdown.

    Args:
        cls: Class documentation

    Returns:
        str: Markdown formatted documentation
    """
    md = [f"### 🏛 `{cls.name}`", ""]

    if cls.bases:
        md.extend([f"**Inherits from**: `{', '.join(cls.bases)}`", ""])

    if cls.docstring:
        md.extend([cls.docstring, "", "---", ""])

    # Separate public and private methods
    public_methods = [m for m in cls.methods if not m.is_private]
    private_methods = [m for m in cls.methods if m.is_private]

    if public_methods:
        md.extend(["#### Public Methods", ""])
        for method in public_methods:
            md.append(format_method_doc(method))

    if private_methods:
        md.extend(["#### Helper Methods", ""])
        for method in private_methods:
            md.append(format_method_doc(method))

    md.append("---")
    md.append("")
    return "\n".join(md)


def format_module_doc(module: ModuleDoc) -> str:
    """
    Format a module's documentation as markdown.

    Args:
        module: Module documentation

    Returns:
        str: Markdown formatted documentation
    """
    md = [
        f"## 🔌 `{module.name}`",
        "",
    ]

    if module.docstring:
        md.extend([module.docstring, "", "---", ""])

    # Document classes
    for cls in module.classes:
        md.append(format_class_doc(cls))

    # Document module-level functions (if any)
    if module.functions:
        md.extend(["### Module Functions", ""])
        for func in module.functions:
            md.append(format_method_doc(func))

    return "\n".join(md)


def generate_connector_overview() -> str:
    """
    Generate overview of citation connectors.

    Returns:
        str: Markdown formatted connector overview
    """
    return """# Citation Connectors

*Auto-generated documentation from source code*

## Overview

Citation connectors are domain-specific handlers that understand the structure and metadata
conventions of specialized citation sources (CMS, arXiv, PubMed, etc.).

## Architecture

Connectors follow an extensible handler pattern:

```
CMSConnector (Main Router)
    ↓
    ├─ IOMHandler (Internet-Only Manuals)
    ├─ PFSHandler (Physician Fee Schedule Regulations)
    └─ [Future handlers...]
```

## Key Features

1. **Domain-Specific Detection**: Each handler can identify applicable URLs
2. **Metadata Extraction**: Extract publication numbers, regulation IDs, chapter info
3. **Parent-Child Citations**: Generate citations for parent document + all downloadable children
4. **Extensibility**: Add new handlers by subclassing `CMSHandler`

## Adding New Connectors

To add a new citation source connector:

1. Subclass `CMSHandler` (or create new base for non-CMS sources)
2. Implement `can_handle(url: str) -> bool` for URL detection
3. Implement `process(url, html_path, base_citation) -> list[pl.DataFrame]` for citation generation
4. Add handler to connector's `HANDLERS` list
5. Write integration tests with real data

---

"""


def generate_full_documentation(output_dir: Path = Path("docs/citations")):
    """
    Generate complete connector documentation.

    Args:
        output_dir: Directory to write documentation files
    """
    logger.info("Starting comprehensive connector documentation generation")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate connector overview
    logger.info("Generating connector overview...")
    overview = generate_connector_overview()
    with open(output_dir / "00_OVERVIEW.md", "w") as f:
        f.write(overview)

    # Generate connector documentation
    logger.info("Generating connector module documentation...")
    connector_docs = generate_connector_docs()
    connector_md = ["# Connector Modules\n\n"]
    connector_md.append(
        "Connector modules handle domain-specific citation sources with specialized metadata extraction.\n\n"
    )
    connector_md.append("---\n\n")

    for doc in connector_docs:
        connector_md.append(format_module_doc(doc))
        connector_md.append("\n\n")

    with open(output_dir / "01_CONNECTORS.md", "w") as f:
        f.write("\n".join(connector_md))

    # Generate index
    logger.info("Generating index...")
    index = [
        "# Citation Connector Documentation\n\n",
        "Domain-specific handlers for specialized citation sources.\n\n",
        "## Documentation Files\n\n",
        "1. [Overview](00_OVERVIEW.md) - Connector architecture and design\n",
        f"2. [Connector Modules](01_CONNECTORS.md) - {len(connector_docs)} connector implementations\n",
        "\n## Statistics\n\n",
        f"- **Connector Modules**: {len(connector_docs)}\n",
        f"- **Total Classes**: {sum(len(d.classes) for d in connector_docs)}\n",
        f"- **Total Methods**: {sum(len(m.methods) for c in connector_docs for m in c.classes)}\n",
        "\n## Supported Sources\n\n",
        "- **CMS.gov**: Internet-Only Manuals (IOM) and Physician Fee Schedule (PFS) regulations\n",
        "\n---\n\n",
        "*Auto-generated from source code. Run `uv run python -m acoharmony._dev.generate_connector_docs` to update.*\n",
    ]

    with open(output_dir / "README.md", "w") as f:
        f.write("\n".join(index))

    logger.info(f"[SUCCESS] Connector documentation generated in {output_dir}")
    logger.info(f"   - {len(connector_docs)} connector modules")
    logger.info(
        f"   - {sum(len(d.classes) for d in connector_docs)} connector classes"
    )


if __name__ == "__main__":
    generate_full_documentation()
