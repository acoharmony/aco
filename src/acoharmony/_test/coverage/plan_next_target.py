# © 2025 HarmonyCares
# All rights reserved.

"""
Plan next test target based on coverage state.

Uses heuristics to choose the most valuable uncovered code to target next.
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .state import CoverageState, FileState


@dataclass
class CoverageTarget:
    """Represents a specific test target."""

    file_path: str
    target_type: str  # "branch" or "line"
    line_number: int
    branch_target: int | None = None  # For branch targets
    priority: float = 0.0
    reason: str = ""
    context: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        result = {
            "file": self.file_path,
            "type": self.target_type,
            "line": self.line_number,
            "priority": round(self.priority, 2),
            "reason": self.reason,
        }
        if self.branch_target is not None:
            result["branch_to"] = self.branch_target
        if self.context:
            result["context"] = self.context
        return result


def calculate_priority(
    file_state: FileState,
    is_branch: bool,
    has_partial_coverage: bool,
) -> float:
    """
    Calculate priority score for a target.

    Higher priority = should test sooner.

    Heuristics:
    - Branches > straight-line statements (2x multiplier)
    - Partially-covered functions > uncovered functions (1.5x)
    - Higher file coverage % > lower (prefer completing files)
    - Pure utility modules > integration code (detect via path)
    """
    priority = 1.0

    # Branch vs line
    if is_branch:
        priority *= 2.0

    # Partial coverage bonus
    if has_partial_coverage:
        priority *= 1.5

    # File coverage bonus (prefer completing files close to 100%)
    coverage_bonus = file_state.percent_covered / 100.0
    priority *= 1.0 + coverage_bonus

    # Path-based heuristics
    file_path = file_state.path
    if "/_parsers/" in file_path:
        priority *= 1.3  # Pure utility modules
    elif "/_transforms/" in file_path:
        priority *= 1.2  # Business logic
    elif "/_expressions/" in file_path:
        priority *= 1.2  # Business logic
    elif "/_utils/" in file_path:
        priority *= 1.3  # Utility modules
    elif "/cli.py" in file_path or "/_cli/" in file_path:
        priority *= 0.8  # CLI code harder to test
    elif "/__main__.py" in file_path:
        priority *= 0.5  # Entry points

    return priority


def plan_next_target(state: CoverageState, limit: int = 10) -> list[CoverageTarget]:
    """
    Plan next test targets based on coverage state.

    Args:
        state: Current coverage state
        limit: Maximum number of targets to return

    Returns:
        List of CoverageTarget objects ordered by priority (highest first)
    """
    targets: list[CoverageTarget] = []

    for file_path, file_state in state.files.items():
        # Skip fully covered files
        if file_state.percent_covered >= 100.0:
            continue

        has_partial_coverage = file_state.percent_covered > 0

        # Add branch targets
        for branch in file_state.missing_branches:
            priority = calculate_priority(file_state, is_branch=True, has_partial_coverage=True)

            target = CoverageTarget(
                file_path=file_path,
                target_type="branch",
                line_number=branch.source_line,
                branch_target=branch.target_line,
                priority=priority,
                reason=f"Branch {branch.source_line}->{branch.target_line} uncovered",
                context=branch.context,
            )
            targets.append(target)

        # Add line targets
        for line_num in file_state.missing_lines:
            priority = calculate_priority(
                file_state, is_branch=False, has_partial_coverage=has_partial_coverage
            )

            target = CoverageTarget(
                file_path=file_path,
                target_type="line",
                line_number=line_num,
                priority=priority,
                reason=f"Line {line_num} uncovered",
            )
            targets.append(target)

    # Sort by priority (highest first)
    targets.sort(key=lambda t: t.priority, reverse=True)

    return targets[:limit]


def format_target_report(targets: list[CoverageTarget]) -> str:
    """Format targets as readable report."""
    if not targets:
        return "No uncovered targets found. Coverage is complete!"

    lines = ["Next Test Targets (by priority):\n"]

    for i, target in enumerate(targets, 1):
        lines.append(f"{i}. {target.file_path}")
        lines.append(f"   Type: {target.target_type}")
        if target.target_type == "branch":
            lines.append(f"   Branch: line {target.line_number} -> {target.branch_target}")
        else:
            lines.append(f"   Line: {target.line_number}")
        lines.append(f"   Priority: {target.priority:.2f}")
        lines.append(f"   Reason: {target.reason}")
        if target.context:
            lines.append(f"   Context: {target.context}")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    """CLI entry point."""


    if len(sys.argv) < 2:
        print("Usage: python -m acoharmony._test.coverage.plan_next_target <coverage_state.yaml>")
        sys.exit(1)

    state_path = Path(sys.argv[1])
    state = CoverageState.load(state_path)

    targets = plan_next_target(state, limit=10)

    # Print report
    print(format_target_report(targets))

    # Save as YAML
    output_path = Path("next_targets.yaml")
    with open(output_path, "w") as f:
        yaml.dump(
            {"targets": [t.to_dict() for t in targets]},
            f,
            default_flow_style=False,
            sort_keys=False,
        )
    print(f"Saved targets to: {output_path}")


if __name__ == "__main__":
    main()
