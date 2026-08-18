#!/usr/bin/env python3
"""One-shot audit for silver parquet files not covered by existing _pipes.

This is intentionally uncommitted diagnostic glue. It statically walks the
pipeline package plus transform modules imported by those pipelines, then
compares the discovered silver inputs/outputs against files present in a local
silver directory.
"""

from __future__ import annotations

import argparse
import ast
import os
import re
from collections import defaultdict, deque
from collections.abc import Iterable
from pathlib import Path

DEFAULT_SILVER_DIR = Path("/opt/s3/data/workspace/silver")
PARQUET_NAME_RE = re.compile(
    r"(?:(?P<layer>silver|gold|bronze)/)?"
    r"(?P<name>[A-Za-z0-9][A-Za-z0-9_.-]*\.parquet)"
)
MX_VALIDATE_OUTPUT_RE = re.compile(r"^mx_validate_[A-Za-z0-9]+_PY\d{4}_Q\d\.parquet$")


def repo_default() -> Path:
    return Path(__file__).resolve().parents[1]


def rel(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path)


def parse_python(path: Path) -> ast.AST | None:
    try:
        return ast.parse(path.read_text(), filename=str(path))
    except (OSError, SyntaxError) as exc:
        print(f"WARN: could not parse {path}: {exc}")
        return None


def iter_string_constants(node: ast.AST) -> Iterable[tuple[str, int]]:
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            yield child.value, getattr(child, "lineno", 0)


def parquet_names_from_text(text: str, *, explicit_silver_only: bool = False) -> list[str]:
    names: list[str] = []
    for match in PARQUET_NAME_RE.finditer(text):
        layer = match.group("layer")
        if explicit_silver_only and layer != "silver":
            continue
        if layer in {"gold", "bronze"}:
            continue
        names.append(match.group("name"))
    return names


def dotted_name(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = dotted_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    if isinstance(node, ast.Call):
        return dotted_name(node.func)
    return None


def literal_string(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def literal_string_list(node: ast.AST | None) -> list[str]:
    if isinstance(node, ast.List | ast.Tuple | ast.Set):
        out: list[str] = []
        for elt in node.elts:
            value = literal_string(elt)
            if value:
                out.append(value)
        return out
    return []


def is_silver_layer_arg(node: ast.AST | None) -> bool:
    return dotted_name(node) in {"MedallionLayer.SILVER", "silver", "MedallionLayer.SILVER.value"}


def is_silver_base(node: ast.AST | None) -> bool:
    name = dotted_name(node)
    if name in {"silver_path", "silver_dir"}:
        return True
    if isinstance(node, ast.Call) and dotted_name(node.func) == "Path" and node.args:
        return is_silver_base(node.args[0])
    return False


def transform_alias_map(transform_dir: Path) -> dict[str, str]:
    """Map names re-exported by _transforms.__init__ to module filenames."""
    init_file = transform_dir / "__init__.py"
    tree = parse_python(init_file)
    aliases: dict[str, str] = {}
    if tree is None:
        return aliases

    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.level != 1:
            continue
        if node.module is not None:
            continue
        for alias in node.names:
            module_name = alias.name
            if (transform_dir / f"{module_name}.py").exists():
                aliases[alias.asname or alias.name] = module_name
    return aliases


def resolve_transform_symbol(
    symbol: str, transform_dir: Path, aliases: dict[str, str]
) -> Path | None:
    module_name = aliases.get(symbol, symbol)
    candidates = [module_name]
    if not module_name.startswith("_"):
        candidates.append(f"_{module_name}")

    for candidate in candidates:
        path = transform_dir / f"{candidate}.py"
        if path.exists():
            return path
    return None


def module_file(module_name: str, transform_dir: Path) -> Path | None:
    short = module_name.rsplit(".", 1)[-1]
    path = transform_dir / f"{short}.py"
    return path if path.exists() else None


def imported_transform_files(
    tree: ast.AST,
    source_path: Path,
    transform_dir: Path,
    aliases: dict[str, str],
) -> set[Path]:
    imports: set[Path] = set()
    in_transforms = transform_dir in source_path.resolve().parents

    def add_symbol(symbol: str) -> None:
        if symbol == "*":
            return
        path = resolve_transform_symbol(symbol, transform_dir, aliases)
        if path:
            imports.add(path)

    def add_module(module: str) -> None:
        path = module_file(module, transform_dir)
        if path:
            imports.add(path)

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "_transforms" or module == "acoharmony._transforms":
                for alias in node.names:
                    add_symbol(alias.name)
            elif module.startswith("_transforms."):
                add_module(module)
            elif module.startswith("acoharmony._transforms."):
                add_module(module)
            elif in_transforms and node.level == 1 and module:
                add_module(module)
            elif in_transforms and node.level == 1 and not module:
                for alias in node.names:
                    add_symbol(alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.name
                if name.startswith("acoharmony._transforms."):
                    add_module(name)
    return imports


class PipeCoverageVisitor(ast.NodeVisitor):
    def __init__(self, source_path: Path, repo_root: Path) -> None:
        self.source_path = source_path
        self.repo_root = repo_root
        self.evidence: dict[str, set[str]] = defaultdict(set)
        self.stage_names: list[tuple[str, str | None, int]] = []
        self.bronze_stage_names: list[tuple[str, int]] = []
        self.execute_stage_to_silver = False
        self.measure_registry_keys: set[str] = set()

    def add(self, parquet_name: str, line: int, reason: str) -> None:
        if not parquet_name.endswith(".parquet"):
            parquet_name = f"{parquet_name}.parquet"
        where = rel(self.source_path, self.repo_root)
        suffix = f":{line}" if line else ""
        self.evidence[parquet_name].add(f"{where}{suffix}: {reason}")

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str):
            for name in parquet_names_from_text(node.value, explicit_silver_only=True):
                self.add(name, getattr(node, "lineno", 0), "explicit silver parquet mention")
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        target_names = {target.id for target in node.targets if isinstance(target, ast.Name)}
        for target in node.targets:
            if isinstance(target, ast.Tuple):
                target_names.update(elt.id for elt in target.elts if isinstance(elt, ast.Name))

        if any("VALUE_SET" in name or name.endswith("_FILES") for name in target_names):
            for value, line in iter_string_constants(node.value):
                for parquet_name in parquet_names_from_text(value):
                    self.add(
                        parquet_name,
                        line,
                        f"{', '.join(sorted(target_names))} silver file registry",
                    )

        if "_MEASURE_REGISTRY" in target_names and isinstance(node.value, ast.Dict):
            for key in node.value.keys:
                value = literal_string(key)
                if value:
                    self.measure_registry_keys.add(value)

        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        target_names = {node.target.id} if isinstance(node.target, ast.Name) else set()
        self._visit_named_value(target_names, node.value)
        self.generic_visit(node)

    def _visit_named_value(self, target_names: set[str], value_node: ast.AST | None) -> None:
        if value_node is None:
            return

        if any("VALUE_SET" in name or name.endswith("_FILES") for name in target_names):
            for value, line in iter_string_constants(value_node):
                for parquet_name in parquet_names_from_text(value):
                    self.add(
                        parquet_name,
                        line,
                        f"{', '.join(sorted(target_names))} silver file registry",
                    )

        if "_MEASURE_REGISTRY" in target_names and isinstance(value_node, ast.Dict):
            for key in value_node.keys:
                value = literal_string(key)
                if value:
                    self.measure_registry_keys.add(value)

    def visit_Dict(self, node: ast.Dict) -> None:
        # identity_timeline routes just one stage to silver via stage_base.
        for key, value in zip(node.keys, node.values, strict=False):
            stage_name = literal_string(key)
            if stage_name and is_silver_base(value):
                self.add(
                    f"{stage_name}.parquet",
                    getattr(key, "lineno", 0),
                    "stage_base routes stage to silver",
                )
        self.generic_visit(node)

    def visit_BinOp(self, node: ast.BinOp) -> None:
        if isinstance(node.op, ast.Div) and is_silver_base(node.left):
            value = literal_string(node.right)
            if value:
                for parquet_name in parquet_names_from_text(value):
                    self.add(parquet_name, getattr(node, "lineno", 0), "silver_path parquet access")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func_name = dotted_name(node.func) or ""
        short_func = func_name.rsplit(".", 1)[-1]

        if short_func in {"PipelineStage", "AnalysisStage", "BronzeStage"}:
            name = self._stage_name(node)
            group = self._keyword_string(node, "group")
            if name:
                self.stage_names.append((name, group, getattr(node, "lineno", 0)))
                if group == "silver":
                    self.add(
                        f"{name}.parquet",
                        getattr(node, "lineno", 0),
                        "PipelineStage(group='silver')",
                    )
                if short_func == "BronzeStage":
                    self.bronze_stage_names.append((name, getattr(node, "lineno", 0)))
                    self.add(
                        f"{name}.parquet",
                        getattr(node, "lineno", 0),
                        "BronzeStage writes to silver",
                    )

            depends_on = self._keyword_value(node, "depends_on")
            for dependency in literal_string_list(depends_on):
                self.add(
                    f"{dependency}.parquet",
                    getattr(depends_on, "lineno", getattr(node, "lineno", 0)),
                    "PipelineStage depends_on",
                )

        if short_func == "execute_stage":
            for arg in node.args:
                if is_silver_base(arg):
                    self.execute_stage_to_silver = True
            for keyword in node.keywords:
                if is_silver_base(keyword.value):
                    self.execute_stage_to_silver = True

        if short_func == "scan_table" and node.args:
            table_name = literal_string(node.args[0])
            if table_name:
                self.add(f"{table_name}.parquet", getattr(node, "lineno", 0), "catalog.scan_table")

        if short_func in {"load_parquet", "load_optional_parquet"} and node.args:
            filename = literal_string(node.args[0])
            if filename:
                layer_arg = node.args[1] if len(node.args) > 1 else None
                for keyword in node.keywords:
                    if keyword.arg == "layer":
                        layer_arg = keyword.value
                if is_silver_layer_arg(layer_arg):
                    self.add(filename, getattr(node, "lineno", 0), f"{short_func}(..., SILVER)")

        self.generic_visit(node)

    def finalize(self) -> None:
        if self.execute_stage_to_silver:
            for name, _group, line in self.stage_names:
                self.add(f"{name}.parquet", line, "execute_stage(..., silver_path)")

        for measure in self.measure_registry_keys:
            self.add(f"blqqr_{measure}.parquet", 0, "mx_validate measure registry")

    def _stage_name(self, node: ast.Call) -> str | None:
        for keyword in node.keywords:
            if keyword.arg == "name":
                return literal_string(keyword.value)
        if node.args:
            return literal_string(node.args[0])
        return None

    def _keyword_string(self, node: ast.Call, key: str) -> str | None:
        value = self._keyword_value(node, key)
        return literal_string(value)

    def _keyword_value(self, node: ast.Call, key: str) -> ast.AST | None:
        for keyword in node.keywords:
            if keyword.arg == key:
                return keyword.value
        return None


def collect_sheet_types(table_file: Path) -> set[str]:
    tree = parse_python(table_file)
    if tree is None:
        return set()

    sheet_types: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values, strict=False):
                if literal_string(key) == "sheet_type":
                    sheet_type = literal_string(value)
                    if sheet_type:
                        sheet_types.add(sheet_type)
        elif isinstance(node, ast.keyword) and node.arg == "sheet_type":
            sheet_type = literal_string(node.value)
            if sheet_type:
                sheet_types.add(sheet_type)
    return sheet_types


def discover_tuva_project_dirs(repo_root: Path, explicit_dirs: list[str] | None) -> list[Path]:
    if explicit_dirs:
        return [Path(value) for value in explicit_dirs]

    candidates = [
        repo_root / "src/acoharmony/_tuva/_depends/repos/tuva",
        repo_root
        / "src/acoharmony/_tuva/_depends/repos/cclf_connector/dbt_packages/the_tuva_project",
    ]
    return [path for path in candidates if (path / "dbt_project.yml").exists()]


def tuva_seed_outputs(tuva_project_dir: Path) -> set[str]:
    """Return silver parquet filenames created by the reference-data pipe."""
    try:
        import yaml  # type: ignore[import-not-found]
    except Exception as exc:
        print(f"WARN: PyYAML unavailable, skipping Tuva seed metadata parse: {exc}")
        return set()

    dbt_project = tuva_project_dir / "dbt_project.yml"
    try:
        config = yaml.safe_load(dbt_project.read_text()) or {}
    except Exception as exc:
        print(f"WARN: could not read Tuva dbt_project.yml at {dbt_project}: {exc}")
        return set()

    seed_config = config.get("seeds", {}).get("the_tuva_project", {}) or {}
    outputs: set[str] = set()

    def has_seed_hook(value: object) -> bool:
        if not isinstance(value, dict):
            return False
        hook = value.get("+post-hook")
        return isinstance(hook, str) and ("load_seed" in hook or "load_versioned_seed" in hook)

    def walk(parent_schema: str, data: object) -> None:
        if not isinstance(data, dict):
            return
        for key, value in data.items():
            if not isinstance(key, str) or not isinstance(value, dict):
                continue

            if has_seed_hook(value):
                schema = parent_schema or key.split("__", 1)[0]
                table = key.split("__", 1)[1] if "__" in key else key
                outputs.add(f"{schema}_{table}.parquet")
                continue

            next_parent = f"{parent_schema}_{key}" if parent_schema else key
            walk(next_parent, value)

    for schema_name, schema_config in seed_config.items():
        walk(str(schema_name), schema_config)

    return outputs


def add_evidence(
    evidence: dict[str, set[str]],
    parquet_name: str,
    reason: str,
) -> None:
    if not parquet_name.endswith(".parquet"):
        parquet_name = f"{parquet_name}.parquet"
    evidence[parquet_name].add(reason)


def analyze(
    repo_root: Path, silver_dir: Path, tuva_project_dirs: list[str] | None
) -> tuple[
    dict[str, set[str]],
    set[str],
    list[Path],
    list[Path],
    int,
]:
    pipes_dir = repo_root / "src/acoharmony/_pipes"
    transform_dir = repo_root / "src/acoharmony/_transforms"
    tables_dir = repo_root / "src/acoharmony/_tables"

    pipe_files = sorted(pipes_dir.glob("*.py"))
    aliases = transform_alias_map(transform_dir)

    evidence: dict[str, set[str]] = defaultdict(set)
    bronze_stages: list[tuple[str, int, Path]] = []
    scanned_files: list[Path] = []
    transform_files: list[Path] = []

    queue: deque[Path] = deque(pipe_files)
    seen: set[Path] = set()

    while queue:
        path = queue.popleft().resolve()
        if path in seen or not path.exists():
            continue
        seen.add(path)

        tree = parse_python(path)
        if tree is None:
            continue

        visitor = PipeCoverageVisitor(path, repo_root)
        visitor.visit(tree)
        visitor.finalize()

        for parquet_name, items in visitor.evidence.items():
            evidence[parquet_name].update(items)
        for stage_name, line in visitor.bronze_stage_names:
            bronze_stages.append((stage_name, line, path))

        scanned_files.append(path)
        if transform_dir in path.parents:
            transform_files.append(path)

        for imported in sorted(imported_transform_files(tree, path, transform_dir, aliases)):
            if imported.resolve() not in seen:
                queue.append(imported)

    silver_files = {path.name for path in silver_dir.glob("*.parquet") if path.is_file()}

    # BronzeStage transforms write to silver. Multi-output Excel stages split
    # by _output_table as `{schema}_{sheet_type}.parquet`, and several real
    # parsers also emit schema-prefixed families.
    for stage_name, line, path in bronze_stages:
        source = f"{rel(path, repo_root)}:{line}: BronzeStage output family"
        add_evidence(evidence, f"{stage_name}.parquet", source)
        prefix = f"{stage_name}_"
        for silver_file in silver_files:
            if silver_file.startswith(prefix):
                add_evidence(evidence, silver_file, source)

        table_file = tables_dir / f"{stage_name}.py"
        for sheet_type in collect_sheet_types(table_file):
            add_evidence(evidence, f"{stage_name}_{sheet_type}.parquet", source)
        if (silver_dir / f"{stage_name}_meta.parquet").exists():
            add_evidence(evidence, f"{stage_name}_meta.parquet", source)

        if stage_name in {
            "annual_beneficiary_level_quality_report",
            "quarterly_beneficiary_level_quality_report",
        }:
            for silver_file in silver_files:
                if silver_file.startswith("blqqr_"):
                    add_evidence(evidence, silver_file, source)

    # mx_validate creates dynamic per-scope silver files.
    if any(path.name == "_mx_validate.py" for path in scanned_files):
        for silver_file in silver_files:
            if MX_VALIDATE_OUTPUT_RE.match(silver_file):
                add_evidence(
                    evidence,
                    silver_file,
                    "src/acoharmony/_pipes/_mx_validate.py: dynamic per-scope compute output",
                )

    tuva_count = 0
    for tuva_dir in discover_tuva_project_dirs(repo_root, tuva_project_dirs):
        outputs = tuva_seed_outputs(tuva_dir)
        tuva_count += len(outputs)
        for parquet_name in outputs:
            add_evidence(
                evidence,
                parquet_name,
                f"{rel(tuva_dir / 'dbt_project.yml', repo_root)}: reference-data seed output",
            )

    return evidence, silver_files, pipe_files, transform_files, tuva_count


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Identify local silver parquet files not used or created by existing _pipes."
    )
    parser.add_argument(
        "--silver-dir",
        type=Path,
        default=Path(os.environ.get("ACO_SILVER_DIR", DEFAULT_SILVER_DIR)),
        help=f"Silver directory to inspect (default: {DEFAULT_SILVER_DIR})",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=repo_default(),
        help="Repository root (default: parent of this script)",
    )
    parser.add_argument(
        "--tuva-project-dir",
        action="append",
        help="Tuva project dir with dbt_project.yml; may be provided multiple times.",
    )
    parser.add_argument(
        "--show-used",
        action="store_true",
        help="Also print silver files with coverage evidence.",
    )
    parser.add_argument(
        "--explain",
        action="store_true",
        help="Print evidence for covered files.",
    )
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    silver_dir = args.silver_dir.resolve()

    if not silver_dir.exists():
        print(f"ERROR: silver dir does not exist: {silver_dir}")
        return 2

    evidence, silver_files, pipe_files, transform_files, tuva_count = analyze(
        repo_root, silver_dir, args.tuva_project_dir
    )

    covered = sorted(name for name in silver_files if name in evidence)
    unused = sorted(silver_files.difference(evidence))

    print(f"Repository: {repo_root}")
    print(f"Silver dir: {silver_dir}")
    print(f"Pipe files scanned: {len(pipe_files)}")
    print(f"Pipe-imported transform files scanned: {len(set(transform_files))}")
    print(f"Tuva reference seed outputs discovered: {tuva_count}")
    print(f"Silver parquet files found: {len(silver_files)}")
    print(f"Covered by existing _pipes: {len(covered)}")
    print(f"Not covered by existing _pipes: {len(unused)}")
    print()

    print("Unused silver files:")
    if unused:
        for name in unused:
            print(f"  {name}")
    else:
        print("  (none)")

    if args.show_used:
        print()
        print("Covered silver files:")
        for name in covered:
            print(f"  {name}")
            if args.explain:
                for item in sorted(evidence[name]):
                    print(f"    - {item}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
