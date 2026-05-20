# © 2025 HarmonyCares
# All rights reserved.

"""
Extract missing coverage from coverage.py JSON report.

Parses coverage.json and extracts uncovered lines and branches at granular level.
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import json
import sys
from datetime import datetime
from pathlib import Path

from .state import BranchMiss, CoverageState, FileState


def extract_missing_coverage(
    coverage_json_path: Path, src_root: str = "src/acoharmony"
) -> CoverageState:
    """
    Extract missing coverage from coverage.py JSON report.

    Args:
        coverage_json_path: Path to coverage.json file
        src_root: Root directory to filter for (only track files under this path)

    Returns:
        CoverageState with extracted missing lines and branches
    """
    with open(coverage_json_path) as f:
        data = json.load(f)

    state = CoverageState(timestamp=datetime.now().isoformat())

    # Extract totals
    totals = data.get("totals", {})
    state.total_statements = totals.get("num_statements", 0)
    state.total_covered = totals.get("covered_lines", 0)
    state.percent_covered = totals.get("percent_covered", 0.0)

    # Process each file
    files_data = data.get("files", {})
    for file_path, file_info in files_data.items():
        # Filter to only track files under src_root
        if src_root not in file_path:
            continue

        # Extract summary
        summary = file_info.get("summary", {})
        num_statements = summary.get("num_statements", 0)
        covered_lines = summary.get("covered_lines", 0)
        percent_covered = summary.get("percent_covered", 0.0)

        # Extract missing lines
        missing_lines = file_info.get("missing_lines", [])

        # Extract missing branches
        # Format: [[source_line, target_line], ...]
        missing_branches_raw = file_info.get("missing_branches", [])
        missing_branches = [
            BranchMiss(source_line=src, target_line=tgt) for src, tgt in missing_branches_raw
        ]

        # Extract excluded lines
        excluded_lines = file_info.get("excluded_lines", [])

        file_state = FileState(
            path=file_path,
            num_statements=num_statements,
            covered_lines=covered_lines,
            percent_covered=percent_covered,
            missing_lines=missing_lines,
            missing_branches=missing_branches,
            excluded_lines=excluded_lines,
        )

        state.files[file_path] = file_state

    return state


def extract_and_save(
    coverage_json_path: Path,
    output_path: Path,
    src_root: str = "src/acoharmony",
) -> CoverageState:
    """
    Extract missing coverage and save to YAML file.

    Args:
        coverage_json_path: Path to coverage.json
        output_path: Path to save coverage_state.yaml
        src_root: Root directory to filter for

    Returns:
        Extracted CoverageState
    """
    state = extract_missing_coverage(coverage_json_path, src_root)
    state.save(output_path)
    return state


def main() -> None:
    """CLI entry point."""

    if len(sys.argv) < 2:
        print("Usage: python -m acoharmony._test.coverage.extract_missing <coverage.json>")
        sys.exit(1)

    coverage_json = Path(sys.argv[1])
    output_yaml = Path("coverage_state.yaml")

    if len(sys.argv) >= 3:
        output_yaml = Path(sys.argv[2])

    state = extract_and_save(coverage_json, output_yaml)

    print("Extracted coverage state:")
    print(f"  Total statements: {state.total_statements}")
    print(f"  Covered: {state.total_covered}")
    print(f"  Coverage: {state.percent_covered:.2f}%")
    print(f"  Files tracked: {len(state.files)}")
    print(f"  Uncovered items: {state.get_uncovered_count()}")
    print(f"\nSaved to: {output_yaml}")


if __name__ == "__main__":
    main()
