# © 2025 HarmonyCares
# All rights reserved.

"""
Diff coverage between two runs.

Compares coverage state to identify improvements and regressions.
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import sys
from dataclasses import dataclass
from pathlib import Path

from .state import CoverageState


@dataclass
class CoverageDiff:
    """Represents a coverage difference between two states."""

    file_path: str
    old_percent: float
    new_percent: float
    lines_added: list[int]  # Newly covered lines
    lines_removed: list[int]  # Lines that became uncovered (regression)
    branches_added: int
    branches_removed: int

    @property
    def is_improvement(self) -> bool:
        """Check if this is an improvement."""
        return (
            len(self.lines_added) > len(self.lines_removed)
            or self.branches_added > self.branches_removed
        )

    @property
    def is_regression(self) -> bool:
        """Check if this is a regression."""
        return (
            len(self.lines_removed) > len(self.lines_added)
            or self.branches_removed > self.branches_added
        )


def diff_coverage(old_state: CoverageState, new_state: CoverageState) -> list[CoverageDiff]:
    """
    Compare two coverage states and identify changes.

    Args:
        old_state: Previous coverage state
        new_state: Current coverage state

    Returns:
        List of CoverageDiff objects for files that changed
    """
    diffs: list[CoverageDiff] = []

    # Get union of all files
    all_files = set(old_state.files.keys()) | set(new_state.files.keys())

    for file_path in all_files:
        old_file = old_state.files.get(file_path)
        new_file = new_state.files.get(file_path)

        # File added or removed
        if old_file is None or new_file is None:
            continue

        # Calculate changes
        old_missing = set(old_file.missing_lines)
        new_missing = set(new_file.missing_lines)

        lines_added = list(old_missing - new_missing)  # Were missing, now covered
        lines_removed = list(new_missing - old_missing)  # Were covered, now missing

        old_branches = len(old_file.missing_branches)
        new_branches = len(new_file.missing_branches)
        branches_added = max(0, old_branches - new_branches)
        branches_removed = max(0, new_branches - old_branches)

        # Only include if there's a change
        if lines_added or lines_removed or branches_added or branches_removed:
            diff = CoverageDiff(
                file_path=file_path,
                old_percent=old_file.percent_covered,
                new_percent=new_file.percent_covered,
                lines_added=sorted(lines_added),
                lines_removed=sorted(lines_removed),
                branches_added=branches_added,
                branches_removed=branches_removed,
            )
            diffs.append(diff)

    return diffs


def format_diff_report(
    diffs: list[CoverageDiff],
    old_state: CoverageState,
    new_state: CoverageState,
) -> str:
    """Format coverage diff as readable report."""
    lines = ["Coverage Diff Report\n"]
    lines.append("=" * 60)

    # Overall summary
    old_pct = old_state.percent_covered
    new_pct = new_state.percent_covered
    delta = new_pct - old_pct

    lines.append("\nOverall Coverage:")
    lines.append(f"  Previous: {old_pct:.2f}%")
    lines.append(f"  Current:  {new_pct:.2f}%")
    if delta > 0:
        lines.append(f"  Change:   +{delta:.2f}% ✓")
    elif delta < 0:
        lines.append(f"  Change:   {delta:.2f}% ✗")
    else:
        lines.append(f"  Change:   {delta:.2f}%")

    # Improvements
    improvements = [d for d in diffs if d.is_improvement]
    if improvements:
        lines.append(f"\n\nImprovements ({len(improvements)} files):")
        lines.append("-" * 60)
        for diff in improvements:
            lines.append(f"\n{diff.file_path}")
            lines.append(f"  Coverage: {diff.old_percent:.2f}% → {diff.new_percent:.2f}%")
            if diff.lines_added:
                lines.append(f"  Lines covered: {len(diff.lines_added)} ({diff.lines_added[:5]}...)")
            if diff.branches_added:
                lines.append(f"  Branches covered: {diff.branches_added}")

    # Regressions
    regressions = [d for d in diffs if d.is_regression]
    if regressions:
        lines.append(f"\n\nRegressions ({len(regressions)} files):")
        lines.append("-" * 60)
        for diff in regressions:
            lines.append(f"\n{diff.file_path}")
            lines.append(f"  Coverage: {diff.old_percent:.2f}% → {diff.new_percent:.2f}%")
            if diff.lines_removed:
                lines.append(
                    f"  Lines lost: {len(diff.lines_removed)} ({diff.lines_removed[:5]}...)"
                )
            if diff.branches_removed:
                lines.append(f"  Branches lost: {diff.branches_removed}")

    # No changes
    unchanged = [d for d in diffs if not d.is_improvement and not d.is_regression]
    if unchanged:
        lines.append(f"\n\nUnchanged ({len(unchanged)} files with minor changes)")

    if not diffs:
        lines.append("\nNo changes detected.")

    return "\n".join(lines)


def main() -> None:
    """CLI entry point."""

    if len(sys.argv) < 3:
        print(
            "Usage: python -m acoharmony._test.coverage.diff_coverage <old_state.yaml> <new_state.yaml>"
        )
        sys.exit(1)

    old_path = Path(sys.argv[1])
    new_path = Path(sys.argv[2])

    old_state = CoverageState.load(old_path)
    new_state = CoverageState.load(new_path)

    diffs = diff_coverage(old_state, new_state)
    report = format_diff_report(diffs, old_state, new_state)

    print(report)


if __name__ == "__main__":
    main()
