#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""
Generate Docusaurus documentation for all namespaced modules using AST.

Walks every _* package under src/acoharmony/, parses each .py file with
the ast module (no imports required), and emits Docusaurus-compatible
markdown into docs/docs/modules/.

Each namespace becomes a sidebar category with:
  - index.md  (package overview from __init__.py docstring + exports)
  - <file>.md (per-file docs with classes, functions, signatures, docstrings)
"""

import ast
import json
from dataclasses import dataclass, field
from pathlib import Path

from acoharmony._log import get_logger

logger = get_logger("dev.generate_module_docs")

SRC_ROOT = Path(__file__).resolve().parents[2]  # src/acoharmony
DOCS_OUTPUT = SRC_ROOT.parent.parent / "docs" / "docs" / "modules"


def _safe_name(name: str) -> str:
    """Strip leading underscores for Docusaurus-safe file/dir names.

    Docusaurus ignores files/dirs starting with _ (treats them as partials).
    """
    return name.lstrip("_") or name


def _escape_mdx(text: str) -> str:
    """Escape characters in docstrings that break MDX compilation.

    MDX treats <tags> as JSX and {expressions} as JS. We need to
    escape these when they appear in docstrings outside of code fences.
    """
    import re

    # Split on code fences to only escape non-code sections
    parts = re.split(r"(```[\s\S]*?```)", text)
    result = []
    for i, part in enumerate(parts):
        if i % 2 == 1:
            # Inside code fence — leave as-is
            result.append(part)
        else:
            # Outside code fence — escape MDX-breaking chars
            # Escape { and } (JSX expressions)
            escaped = part.replace("{", "\\{").replace("}", "\\}")
            # Escape < that look like HTML tags
            escaped = re.sub(
                r"<([A-Za-z/][A-Za-z0-9_-]*)(?:\s[^>]*)?>",
                lambda m: m.group(0).replace("<", "&lt;").replace(">", "&gt;"),
                escaped,
            )
            result.append(escaped)
    return "".join(result)

# Friendly display names for namespace packages
DISPLAY_NAMES: dict[str, str] = {
    "_4icli": "4iCLI (DataHub)",
    "_cite": "Citations",
    "_config": "Configuration",
    "_crosswalks": "Crosswalks",
    "_databricks": "Databricks",
    "_decor8": "Decorators",
    "_deploy": "Deployment",
    "_dev": "Development Tools",
    "_exceptions": "Exceptions",
    "_expressions": "Expressions",
    "_log": "Logging",
    "_notes": "Notebooks",
    "_parsers": "Parsers",
    "_pipes": "Pipelines",
    "_puf": "Public Use Files",
    "_registry": "Registry",
    "_runner": "Runner",
    "_tables": "Table Models",
    "_trace": "Tracing",
    "_transforms": "Transforms",
    "_tuva": "Tuva Seeds",
    "_unity": "Unity Catalog",
    "_utils": "Utilities",
    "_validators": "Validators",
}


# ---------------------------------------------------------------------------
# AST extraction dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ParamInfo:
    """A single function parameter."""
    name: str
    annotation: str = ""
    default: str = ""


@dataclass
class FuncInfo:
    """Extracted function/method metadata."""
    name: str
    docstring: str = ""
    params: list[ParamInfo] = field(default_factory=list)
    returns: str = ""
    decorators: list[str] = field(default_factory=list)
    is_async: bool = False
    is_static: bool = False
    is_classmethod: bool = False
    is_property: bool = False


@dataclass
class ClassInfo:
    """Extracted class metadata."""
    name: str
    docstring: str = ""
    bases: list[str] = field(default_factory=list)
    methods: list[FuncInfo] = field(default_factory=list)
    decorators: list[str] = field(default_factory=list)


@dataclass
class ModuleInfo:
    """Extracted module metadata."""
    name: str
    path: Path
    docstring: str = ""
    classes: list[ClassInfo] = field(default_factory=list)
    functions: list[FuncInfo] = field(default_factory=list)
    all_exports: list[str] = field(default_factory=list)
    constants: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------

def _unparse_safe(node: ast.AST | None) -> str:
    """Unparse an AST node to source, returning empty string on failure."""
    if node is None:
        return ""
    try:
        return ast.unparse(node)
    except Exception:
        return ""


def _extract_docstring(body: list[ast.stmt]) -> str:
    """Extract docstring from the first statement of a body."""
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        return body[0].value.value.strip()
    return ""


def _extract_decorators(node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef) -> list[str]:
    """Extract decorator names from a node."""
    names = []
    for dec in node.decorator_list:
        names.append(_unparse_safe(dec))
    return names


def _extract_params(args: ast.arguments) -> list[ParamInfo]:
    """Extract parameter info from function arguments."""
    params = []
    defaults_offset = len(args.args) - len(args.defaults)

    for i, arg in enumerate(args.args):
        if arg.arg == "self" or arg.arg == "cls":
            continue
        annotation = _unparse_safe(arg.annotation)
        default = ""
        default_idx = i - defaults_offset
        if default_idx >= 0:
            default = _unparse_safe(args.defaults[default_idx])
        params.append(ParamInfo(name=arg.arg, annotation=annotation, default=default))

    # *args
    if args.vararg:
        ann = _unparse_safe(args.vararg.annotation)
        params.append(ParamInfo(name=f"*{args.vararg.arg}", annotation=ann))

    # keyword-only
    kw_defaults_map = {i: d for i, d in enumerate(args.kw_defaults) if d is not None}
    for i, arg in enumerate(args.kwonlyargs):
        annotation = _unparse_safe(arg.annotation)
        default = _unparse_safe(kw_defaults_map.get(i))
        params.append(ParamInfo(name=arg.arg, annotation=annotation, default=default))

    # **kwargs
    if args.kwarg:
        ann = _unparse_safe(args.kwarg.annotation)
        params.append(ParamInfo(name=f"**{args.kwarg.arg}", annotation=ann))

    return params


def _extract_function(node: ast.FunctionDef | ast.AsyncFunctionDef) -> FuncInfo:
    """Extract function metadata from an AST node."""
    decorators = _extract_decorators(node)
    dec_names_lower = [d.lower() for d in decorators]
    return FuncInfo(
        name=node.name,
        docstring=_extract_docstring(node.body),
        params=_extract_params(node.args),
        returns=_unparse_safe(node.returns),
        decorators=decorators,
        is_async=isinstance(node, ast.AsyncFunctionDef),
        is_static="staticmethod" in dec_names_lower,
        is_classmethod="classmethod" in dec_names_lower,
        is_property="property" in dec_names_lower,
    )


def _extract_class(node: ast.ClassDef) -> ClassInfo:
    """Extract class metadata from an AST node."""
    methods = []
    for item in node.body:
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
            methods.append(_extract_function(item))
    return ClassInfo(
        name=node.name,
        docstring=_extract_docstring(node.body),
        bases=[_unparse_safe(b) for b in node.bases],
        methods=methods,
        decorators=_extract_decorators(node),
    )


def _extract_all_exports(tree: ast.Module) -> list[str]:
    """Extract __all__ list from module AST."""
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "__all__":
                    if isinstance(node.value, (ast.List, ast.Tuple)):
                        return [
                            elt.value
                            for elt in node.value.elts
                            if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                        ]
    return []


def parse_module(filepath: Path) -> ModuleInfo:
    """Parse a single Python file and extract all documentation."""
    source = filepath.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(filepath))

    classes = []
    functions = []
    constants = []

    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            classes.append(_extract_class(node))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append(_extract_function(node))
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.isupper():
                    constants.append(target.id)

    return ModuleInfo(
        name=filepath.stem,
        path=filepath,
        docstring=_extract_docstring(tree.body),
        classes=classes,
        functions=functions,
        all_exports=_extract_all_exports(tree),
        constants=constants,
    )


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------

def _sig_str(func: FuncInfo) -> str:
    """Build a compact signature string."""
    parts = []
    for p in func.params:
        s = p.name
        if p.annotation:
            s += f": {p.annotation}"
        if p.default:
            s += f" = {p.default}"
        parts.append(s)
    ret = f" -> {func.returns}" if func.returns else ""
    return f"({', '.join(parts)}){ret}"


def _render_func_md(func: FuncInfo, heading: str = "###") -> str:
    """Render a function/method as markdown."""
    lines: list[str] = []

    badges: list[str] = []
    if func.is_async:
        badges.append("`async`")
    if func.is_static:
        badges.append("`@staticmethod`")
    if func.is_classmethod:
        badges.append("`@classmethod`")
    if func.is_property:
        badges.append("`@property`")

    badge_str = " ".join(badges)
    if badge_str:
        badge_str = " " + badge_str

    if func.is_property:
        lines.append(f"{heading} `{func.name}`{badge_str}")
    else:
        lines.append(f"{heading} `{func.name}`{badge_str}")
        lines.append("")
        lines.append(f"```python\n{func.name}{_sig_str(func)}\n```")

    lines.append("")

    if func.docstring:
        lines.append(_escape_mdx(func.docstring))
        lines.append("")

    return "\n".join(lines)


def _render_class_md(cls: ClassInfo) -> str:
    """Render a class as markdown."""
    lines: list[str] = []

    lines.append(f"## `{cls.name}`")
    if cls.bases:
        bases_str = ", ".join(f"`{b}`" for b in cls.bases if b != "object")
        if bases_str:
            lines.append(f"\n*Inherits from:* {bases_str}")
    lines.append("")

    if cls.docstring:
        lines.append(_escape_mdx(cls.docstring))
        lines.append("")

    # Separate public / private
    public = [m for m in cls.methods if not m.name.startswith("_")]
    dunder = [m for m in cls.methods if m.name.startswith("__") and m.name.endswith("__") and m.name != "__init__"]
    init = [m for m in cls.methods if m.name == "__init__"]
    private = [m for m in cls.methods if m.name.startswith("_") and not m.name.startswith("__")]

    if init:
        lines.append("### Constructor")
        lines.append("")
        lines.append(f"```python\n{cls.name}{_sig_str(init[0])}\n```")
        lines.append("")
        if init[0].docstring:
            lines.append(init[0].docstring)
            lines.append("")

    if public:
        lines.append("### Methods")
        lines.append("")
        for m in public:
            lines.append(_render_func_md(m, heading="####"))
            lines.append("---")
            lines.append("")

    if private:
        lines.append("")
        lines.append("### Internal Methods")
        lines.append("")
        for m in private:
            lines.append(_render_func_md(m, heading="####"))
        lines.append("")

    return "\n".join(lines)


def render_module_page(mod: ModuleInfo, package_name: str) -> str:
    """Render a full module documentation page."""
    lines: list[str] = []

    # Frontmatter
    title = mod.name.lstrip("_") or mod.name
    lines.append("---")
    lines.append(f"title: {title}")
    lines.append(f"description: \"Module {package_name}.{mod.name}\"")
    lines.append("---")
    lines.append("")
    lines.append(f"# `{package_name}.{mod.name}`")
    lines.append("")

    if mod.docstring:
        lines.append(_escape_mdx(mod.docstring))
        lines.append("")

    if mod.all_exports:
        lines.append("## Exports")
        lines.append("")
        lines.append("```python")
        lines.append(f"__all__ = {mod.all_exports!r}")
        lines.append("```")
        lines.append("")

    # Classes
    for cls in mod.classes:
        lines.append(_render_class_md(cls))
        lines.append("")

    # Module-level functions
    public_funcs = [f for f in mod.functions if not f.name.startswith("_")]
    private_funcs = [f for f in mod.functions if f.name.startswith("_")]

    if public_funcs:
        lines.append("## Functions")
        lines.append("")
        for func in public_funcs:
            lines.append(_render_func_md(func, heading="###"))
            lines.append("---")
            lines.append("")

    if private_funcs:
        lines.append("## Internal Functions")
        lines.append("")
        for func in private_funcs:
            lines.append(_render_func_md(func, heading="###"))
        lines.append("")

    return "\n".join(lines)


def render_package_index(
    package_name: str,
    init_info: ModuleInfo | None,
    submodules: list[ModuleInfo],
) -> str:
    """Render the index.md for a namespace package."""
    display = DISPLAY_NAMES.get(package_name, package_name.lstrip("_").title())
    lines: list[str] = []

    safe_pkg = _safe_name(package_name)
    lines.append("---")
    lines.append(f"title: {display}")
    lines.append(f"slug: /modules/{safe_pkg}")
    lines.append(f"description: \"Package acoharmony.{package_name}\"")
    lines.append("---")
    lines.append("")
    lines.append(f"# {display}")
    lines.append("")
    lines.append(f"`acoharmony.{package_name}`")
    lines.append("")

    if init_info and init_info.docstring:
        lines.append(_escape_mdx(init_info.docstring))
        lines.append("")

    if init_info and init_info.all_exports:
        lines.append("## Public API")
        lines.append("")
        lines.append("```python")
        for exp in init_info.all_exports:
            lines.append(f"from acoharmony.{package_name} import {exp}")
        lines.append("```")
        lines.append("")

    if submodules:
        lines.append("## Submodules")
        lines.append("")
        lines.append("| Module | Description |")
        lines.append("|--------|-------------|")
        for sm in sorted(submodules, key=lambda m: m.name):
            desc = _escape_mdx((sm.docstring or "").split("\n")[0][:100])
            safe_sm = _safe_name(sm.name)
            lines.append(f"| [{sm.name}](./{safe_sm}) | {desc} |")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Discovery + generation
# ---------------------------------------------------------------------------

def discover_packages() -> list[Path]:
    """Find all namespaced _* packages under src/acoharmony/."""
    packages = []
    for child in sorted(SRC_ROOT.iterdir()):
        if (
            child.is_dir()
            and child.name.startswith("_")
            and not child.name.startswith("__")
            and (child / "__init__.py").exists()
        ):
            packages.append(child)
    return packages


def generate_module_docs(output_dir: Path | None = None) -> bool:
    """
    Generate Docusaurus documentation for all namespaced modules.

    Walks every _* package, parses .py files with AST, and writes
    markdown + Docusaurus category metadata to the output directory.

    Args:
        output_dir: Where to write docs. Defaults to docs/docs/modules/.

    Returns:
        True if successful.
    """
    output_dir = output_dir or DOCS_OUTPUT
    output_dir.mkdir(parents=True, exist_ok=True)

    packages = discover_packages()
    logger.info(f"Found {len(packages)} namespace packages to document")

    # Top-level category
    _write_category(output_dir, "API Reference", position=2)

    total_modules = 0
    total_classes = 0
    total_functions = 0

    for position, pkg_dir in enumerate(packages, start=1):
        pkg_name = pkg_dir.name
        display = DISPLAY_NAMES.get(pkg_name, pkg_name.lstrip("_").title())

        logger.info(f"Documenting {pkg_name} ({display})")

        # Parse __init__.py
        init_path = pkg_dir / "__init__.py"
        try:
            init_info = parse_module(init_path)
        except Exception as e:
            logger.warning(f"Failed to parse {init_path}: {e}")
            init_info = None

        # Parse submodules (skip __init__, __pycache__)
        submodules: list[ModuleInfo] = []
        for py_file in sorted(pkg_dir.glob("*.py")):
            if py_file.name == "__init__.py":
                continue
            try:
                info = parse_module(py_file)
                submodules.append(info)
                total_modules += 1
                total_classes += len(info.classes)
                total_functions += len(info.functions)
            except Exception as e:
                logger.warning(f"Failed to parse {py_file}: {e}")

        # Write package directory (strip _ prefix for Docusaurus)
        safe_pkg = _safe_name(pkg_name)
        pkg_out = output_dir / safe_pkg
        pkg_out.mkdir(parents=True, exist_ok=True)

        _write_category(pkg_out, display, position=position)

        # Write index
        index_md = render_package_index(pkg_name, init_info, submodules)
        (pkg_out / "index.md").write_text(index_md, encoding="utf-8")

        # Write submodule pages (strip _ prefix for filenames)
        for sm in submodules:
            page_md = render_module_page(sm, f"acoharmony.{pkg_name}")
            safe_sm = _safe_name(sm.name)
            (pkg_out / f"{safe_sm}.md").write_text(page_md, encoding="utf-8")

    logger.info(
        f"Generated docs for {len(packages)} packages, "
        f"{total_modules} modules, {total_classes} classes, "
        f"{total_functions} functions"
    )
    logger.info(f"Output: {output_dir}")
    return True


def _write_category(directory: Path, label: str, position: int = 1):
    """Write a Docusaurus _category_.json file."""
    cat = {
        "label": label,
        "position": position,
        "link": {"type": "generated-index"},
    }
    (directory / "_category_.json").write_text(
        json.dumps(cat, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    generate_module_docs()
