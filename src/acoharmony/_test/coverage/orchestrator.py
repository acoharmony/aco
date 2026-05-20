# © 2025 HarmonyCares
# All rights reserved.

"""
Coverage orchestrator.

Manages the iterative coverage improvement loop:
1. Run tests with coverage
2. Extract missing coverage
3. Plan next target
4. Generate test (external, via LLM)
5. Run relevant tests
6. Diff coverage
7. Repeat
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any

from .diff_coverage import diff_coverage, format_diff_report
from .extract_missing import extract_missing_coverage
from .plan_next_target import format_target_report, plan_next_target
from .state import CoverageState


class CoverageOrchestrator:
    """Orchestrates the coverage improvement loop."""

    def __init__(
        self,
        src_root: str = "src/acoharmony",
        work_dir: Path | None = None,
    ):
        """
        Initialize orchestrator.

        Args:
            src_root: Root directory for coverage tracking
            work_dir: Working directory for coverage files (defaults to .coverage_state/)
        """
        self.src_root = src_root
        self.work_dir = work_dir or Path(".coverage_state")
        self.work_dir.mkdir(parents=True, exist_ok=True)

        self.coverage_json = self.work_dir / "coverage.json"
        self.current_state = self.work_dir / "coverage_state.yaml"
        self.previous_state = self.work_dir / "coverage_state_previous.yaml"
        self.targets_file = self.work_dir / "next_targets.yaml"

    def run_coverage(
        self,
        test_path: str | None = None,
        extra_args: list[str] | None = None,
    ) -> int:
        """
        Run pytest with coverage.

        Uses `coverage run -m pytest` so each invocation writes a unique
        fragment file (parallel=true in pyproject.toml).  No internal
        combine() happens, making this safe for concurrent agents.

        Args:
            test_path: Specific test file/directory to run (None = all tests)
            extra_args: Additional pytest arguments

        Returns:
            Exit code from pytest
        """
        cmd = [
            "coverage", "run", "--append", "-m", "pytest",
            "--no-cov",  # Disable pytest-cov; coverage.py handles collection
            "-q",
        ]

        if test_path:
            cmd.append(test_path)

        if extra_args:
            cmd.extend(extra_args)

        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd)

        return result.returncode

    @staticmethod
    def combine_fragments() -> None:
        """Merge pending .coverage.<suffix> fragments into .coverage.

        With parallel=false (the default), coverage run --append writes
        directly to .coverage and no fragments are created.  This method
        is a safe no-op in that case.  If parallel=true fragments exist
        (e.g., from a CI matrix), it merges them.
        """
        import glob

        data_dir = Path(".test-state")
        fragments = glob.glob(str(data_dir / ".coverage.*"))
        if not fragments:
            return

        subprocess.run(
            ["coverage", "combine", "--append"],
            capture_output=True,
        )

    def extract_state(self) -> CoverageState:
        """Extract coverage state from JSON report.

        Regenerates the JSON report from the current .coverage data
        file (does NOT call combine — call combine_fragments() first
        if there are pending fragments to merge).
        """
        import shutil

        # Regenerate JSON from the .coverage data file.
        test_state_json = Path(".test-state/coverage.json")
        subprocess.run(
            ["coverage", "json", "-o", str(test_state_json)],
            capture_output=True,
        )

        # Sync to work_dir.
        if test_state_json.exists():
            if (
                not self.coverage_json.exists()
                or test_state_json.stat().st_mtime > self.coverage_json.stat().st_mtime
            ):
                shutil.copy(test_state_json, self.coverage_json)

        if not self.coverage_json.exists():
            raise FileNotFoundError(f"Coverage JSON not found: {self.coverage_json}")

        # Save previous state if exists
        if self.current_state.exists():
            shutil.copy(self.current_state, self.previous_state)

        # Extract new state
        state = extract_missing_coverage(self.coverage_json, self.src_root)
        state.save(self.current_state)

        return state

    def plan_targets(self, limit: int = 10) -> list[Any]:
        """Plan next test targets."""
        state = CoverageState.load(self.current_state)
        targets = plan_next_target(state, limit=limit)

        # Save targets to file
        import yaml

        with open(self.targets_file, "w") as f:
            yaml.dump(
                {"targets": [t.to_dict() for t in targets]},
                f,
                default_flow_style=False,
                sort_keys=False,
            )

        return targets

    def diff_states(self) -> str:
        """Diff current and previous coverage states."""
        if not self.previous_state.exists():
            return "No previous state to compare against."

        old_state = CoverageState.load(self.previous_state)
        new_state = CoverageState.load(self.current_state)

        diffs = diff_coverage(old_state, new_state)
        return format_diff_report(diffs, old_state, new_state)

    def iterate_once(
        self,
        test_path: str | None = None,
        show_targets: bool = True,
    ) -> dict[str, Any]:
        """
        Run one iteration of the coverage loop.

        Args:
            test_path: Specific test path to run
            show_targets: Whether to display next targets

        Returns:
            Dict with iteration results
        """
        print("\n" + "=" * 60)
        print("Coverage Iteration")
        print("=" * 60)

        # Step 1: Run coverage
        print("\n[1/4] Running tests with coverage...")
        exit_code = self.run_coverage(test_path)

        if exit_code != 0:
            print(f"Tests failed with exit code {exit_code}")
            return {"success": False, "exit_code": exit_code}

        # Step 2: Extract state
        print("\n[2/4] Extracting coverage state...")
        state = self.extract_state()
        print(f"Coverage: {state.percent_covered:.2f}%")
        print(f"Uncovered items: {state.get_uncovered_count()}")

        # Step 3: Diff with previous
        print("\n[3/4] Comparing with previous run...")
        diff_report = self.diff_states()
        print(diff_report)

        # Step 4: Plan next targets
        targets = []
        if show_targets and state.get_uncovered_count() > 0:
            print("\n[4/4] Planning next targets...")
            targets = self.plan_targets()
            print(format_target_report(targets))

        return {
            "success": True,
            "coverage": state.percent_covered,
            "uncovered_count": state.get_uncovered_count(),
            "targets": [t.to_dict() for t in targets],
        }


def main() -> None:
    """CLI entry point."""

    parser = argparse.ArgumentParser(description="Coverage orchestration loop")
    parser.add_argument("--test-path", help="Specific test file/directory to run")
    parser.add_argument("--no-targets", action="store_true", help="Don't show next targets")
    parser.add_argument("--work-dir", default=".coverage_state", help="Working directory")

    args = parser.parse_args()

    orchestrator = CoverageOrchestrator(work_dir=Path(args.work_dir))

    result = orchestrator.iterate_once(
        test_path=args.test_path,
        show_targets=not args.no_targets,
    )

    if not result["success"]:
        sys.exit(1)

    if result["uncovered_count"] == 0:
        print("\n🎉 Coverage is complete!")
    else:
        print(f"\n📋 Next: Review targets in {orchestrator.targets_file}")


if __name__ == "__main__":
    main()
