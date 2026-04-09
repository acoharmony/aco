#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""
Automated coverage improvement loop.

Iteratively runs targeted tests until 100% coverage is achieved.

Usage:
    python -m acoharmony._test.coverage.auto_improve [--max-iterations N] [--target-pct 100.0]
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

import yaml


def load_state():
    """Load current coverage state."""
    state_file = Path(".coverage_state/coverage_state.yaml")
    if not state_file.exists():
        return None

    with open(state_file) as f:
        return yaml.safe_load(f)


def load_targets():
    """Load next targets."""
    targets_file = Path(".coverage_state/next_targets.yaml")
    if not targets_file.exists():
        return None

    with open(targets_file) as f:
        return yaml.safe_load(f)


def get_coverage_percent(state):
    """Get coverage percentage from state."""
    if not state:
        return 0.0
    return state["summary"]["percent_covered"]


def suggest_test_path(target_file):
    """Suggest which test path to run for a target file.

    Maps source paths to test paths:
        src/acoharmony/_parsers/excel.py -> src/acoharmony/_test/parsers/test_excel.py
        src/acoharmony/_utils/unpack.py  -> src/acoharmony/_test/utils/test_unpack.py
    """
    if not target_file.startswith("src/acoharmony/"):
        return None

    rel_path = target_file.replace("src/acoharmony/", "")
    parts = Path(rel_path).parts

    if len(parts) < 2:
        return None

    module = parts[0]  # e.g., "_parsers"
    file_name = Path(parts[-1]).stem  # e.g., "excel"

    # Strip leading underscore from module name for test path
    # _parsers -> parsers, _utils -> utils, _transforms -> transforms
    test_module = module.lstrip("_")

    test_base = Path("src/acoharmony/_test")

    # Try specific test file first
    test_patterns = [
        test_base / test_module / f"test_{file_name}.py",
        test_base / test_module / f"test_coverage_{file_name}.py",
        test_base / test_module / f"test_coverage.py",
        # Also try without stripping underscore (some modules keep it)
        test_base / module / f"test_{file_name}.py",
    ]

    for pattern in test_patterns:
        if pattern.exists():
            return str(pattern)

    # Fall back to module directory
    for mod_name in [test_module, module]:
        module_dir = test_base / mod_name
        if module_dir.exists():
            return str(module_dir)

    return None


def run_targeted_test(test_path, quiet=False):
    """Run targeted test using aco test."""
    cmd = ["python", "src/acoharmony/cli.py", "test", "--test-path", test_path]

    if quiet:
        cmd.append("--no-targets")

    print(f"\n{'='*80}")
    print(f"Running: {' '.join(cmd)}")
    print(f"{'='*80}\n")

    result = subprocess.run(cmd)
    return result.returncode == 0


def group_targets_by_file(targets):
    """Group targets by file to batch test runs."""
    if not targets or "targets" not in targets:
        return {}

    file_groups = {}
    for target in targets["targets"]:
        file_path = target["file"]
        if file_path not in file_groups:
            file_groups[file_path] = []
        file_groups[file_path].append(target)

    return file_groups


def main():
    """Main iteration loop."""
    parser = argparse.ArgumentParser(
        description="Automatically improve coverage until target is reached"
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=50,
        help="Maximum number of iterations (default: 50)",
    )
    parser.add_argument(
        "--target-pct",
        type=float,
        default=100.0,
        help="Target coverage percentage (default: 100.0)",
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Run tests for all files in a module at once",
    )
    args = parser.parse_args()

    print("\n" + "="*80)
    print("AUTOMATED COVERAGE IMPROVEMENT")
    print("="*80)
    print(f"Target coverage: {args.target_pct}%")
    print(f"Max iterations: {args.max_iterations}")
    print("="*80)

    # Check initial state
    state = load_state()
    if not state:
        print("❌ No coverage state found. Run 'aco test' first to establish baseline.")
        sys.exit(1)

    initial_pct = get_coverage_percent(state)
    print(f"\n📊 Starting coverage: {initial_pct:.2f}%")

    if initial_pct >= args.target_pct:
        print(f"✅ Already at target coverage! ({initial_pct:.2f}% >= {args.target_pct}%)")
        return

    # Main iteration loop
    iteration = 0
    previous_pct = initial_pct
    tested_files = set()

    while iteration < args.max_iterations:
        iteration += 1

        print(f"\n{'='*80}")
        print(f"ITERATION {iteration}/{args.max_iterations}")
        print(f"{'='*80}")

        # Load current targets
        targets = load_targets()
        if not targets or not targets.get("targets"):
            print("✅ No more targets found - coverage is complete!")
            break

        # Group targets by file
        file_groups = group_targets_by_file(targets)

        # Find next file to test (skip already tested in this run)
        target_file = None
        for file_path in file_groups.keys():
            if file_path not in tested_files:
                target_file = file_path
                break

        if not target_file:
            print("⚠️  All files with targets have been tested this run.")
            print("    Coverage may not be improving. Breaking loop.")
            break

        # Mark as tested
        tested_files.add(target_file)

        # Get test path
        test_path = suggest_test_path(target_file)

        if not test_path:
            print(f"⚠️  No test file found for {target_file}")
            print("    Skipping this file.")
            continue

        # Show what we're testing
        num_targets = len(file_groups[target_file])
        short_path = target_file.replace("src/acoharmony/", "")
        print(f"\n🎯 Target: {short_path}")
        print(f"   Uncovered items: {num_targets}")
        print(f"   Test path: {test_path}")

        # Run test
        success = run_targeted_test(test_path, quiet=True)

        if not success:
            print(f"⚠️  Tests failed for {test_path}")
            print("    Continuing to next target...")
            continue

        # Check new coverage
        state = load_state()
        if not state:
            print("❌ Coverage state not found after test run")
            break

        current_pct = get_coverage_percent(state)
        improvement = current_pct - previous_pct

        print(f"\n📊 Coverage: {previous_pct:.2f}% → {current_pct:.2f}% ({improvement:+.2f}%)")

        # Check if we reached target
        if current_pct >= args.target_pct:
            print(f"\n🎉 Target coverage reached! ({current_pct:.2f}% >= {args.target_pct}%)")
            break

        # Check if no improvement
        if improvement <= 0:
            print("⚠️  No improvement in coverage")
        else:
            # Reset tested files if we made progress
            tested_files.clear()

        previous_pct = current_pct

    # Final summary
    print(f"\n{'='*80}")
    print("FINAL SUMMARY")
    print(f"{'='*80}")
    print(f"Iterations completed: {iteration}")
    print(f"Starting coverage: {initial_pct:.2f}%")
    print(f"Final coverage: {previous_pct:.2f}%")
    print(f"Total improvement: {previous_pct - initial_pct:+.2f}%")

    if previous_pct >= args.target_pct:
        print(f"\n✅ SUCCESS! Target coverage {args.target_pct}% achieved!")
    else:
        remaining = args.target_pct - previous_pct
        print(f"\n⚠️  Target not reached. Remaining: {remaining:.2f}%")
        print("    Run again or manually review: python -m acoharmony._test.coverage.iterate")

    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
