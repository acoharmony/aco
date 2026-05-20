#!/usr/bin/env python3
"""Analyze complete import dependency chains from CLI entry points."""

import ast
import json
from pathlib import Path
from typing import Any


class ImportCollector(ast.NodeVisitor):
    """Collect all imports from a Python file."""

    def __init__(self) -> None:
        self.imports: set[str] = set()

    def visit_Import(self, node: ast.Import) -> None:
        """Visit Import node."""
        for alias in node.names:
            if alias.name.startswith("acoharmony"):
                self.imports.add(alias.name)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Visit ImportFrom node."""
        if node.module and node.module.startswith("acoharmony"):
            # Add the module itself
            self.imports.add(node.module)
            # Also add module.name for each imported name
            for alias in node.names:
                if alias.name != "*":
                    self.imports.add(f"{node.module}.{alias.name}")


def get_module_file(module_path: str, base_path: Path) -> Path | None:
    """Convert module path to file path."""
    # Handle both absolute and relative module paths
    parts = module_path.replace("acoharmony.", "").split(".")

    # Try as a module file
    file_path = base_path / "/".join(parts[:-1]) / f"{parts[-1]}.py"
    if file_path.exists():
        return file_path

    # Try as a package __init__.py
    package_path = base_path / "/".join(parts) / "__init__.py"
    if package_path.exists():
        return package_path

    # Try as direct file in base
    direct_path = base_path / f"{parts[0]}.py"
    if direct_path.exists():
        return direct_path

    return None


def extract_imports_from_file(file_path: Path) -> set[str]:
    """Extract all acoharmony imports from a Python file."""
    try:
        with open(file_path) as f:
            tree = ast.parse(f.read(), filename=str(file_path))
        collector = ImportCollector()
        collector.visit(tree)
        return collector.imports
    except (SyntaxError, UnicodeDecodeError):
        return set()


def build_dependency_graph(seed_imports: list[str], base_path: Path) -> dict[str, Any]:
    """Build complete dependency graph from seed imports."""
    graph: dict[str, set[str]] = {}
    visited: set[str] = set()
    to_visit: list[str] = seed_imports.copy()

    while to_visit:
        current = to_visit.pop(0)

        if current in visited:
            continue

        visited.add(current)

        # Find the file for this module
        file_path = get_module_file(current, base_path)
        if not file_path:
            continue

        # Extract imports from this file
        imports = extract_imports_from_file(file_path)
        graph[current] = imports

        # Add new imports to visit queue
        for imp in imports:
            if imp not in visited:
                to_visit.append(imp)

    return {"graph": {k: list(v) for k, v in graph.items()}, "all_modules": sorted(visited)}


def main() -> None:
    """Main entry point."""
    # Load CLI import map
    cli_map_path = Path(__file__).parent / "cli_import_map.json"
    if not cli_map_path.exists():
        print("[FAILED] cli_import_map.json not found. Run map_cli_imports.py first.")
        return

    with open(cli_map_path) as f:
        cli_data = json.load(f)

    # Collect all seed imports from CLI
    seed_imports: set[str] = set()
    for cmd_data in cli_data["command_imports"].values():
        seed_imports.update(cmd_data["direct_imports"])

    print(f"Starting with {len(seed_imports)} seed imports from CLI...")

    # Build dependency graph
    base_path = Path(__file__).parent.parent
    print(f"Scanning from: {base_path}")

    graph_data = build_dependency_graph(sorted(seed_imports), base_path)

    # Save results
    output_path = Path(__file__).parent / "dependency_graph.json"
    with open(output_path, "w") as f:
        json.dump(graph_data, f, indent=2)

    print(f"\n[OK] Analyzed {len(graph_data['graph'])} modules")
    print(f"[OK] Found {len(graph_data['all_modules'])} total reachable modules")
    print(f"[OK] Saved to {output_path}")

    # Save just the reachable modules list
    reachable_path = Path(__file__).parent / "reachable_modules.txt"
    with open(reachable_path, "w") as f:
        for module in graph_data["all_modules"]:
            f.write(f"{module}\n")

    print(f"[OK] Saved reachable modules to {reachable_path}")

    # Print statistics
    print("\nTop 10 modules with most dependencies:")
    print("=" * 80)
    deps_count = [(mod, len(deps)) for mod, deps in graph_data["graph"].items()]
    for mod, count in sorted(deps_count, key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {mod}: {count} imports")


if __name__ == "__main__":
    main()
