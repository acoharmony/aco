#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""
Interactive coverage iteration helper.

Usage:
    python -m acoharmony._test.coverage.iterate

This script:
1. Shows current coverage state
2. Displays top priority targets
3. Helps identify which test file to run next
4. Guides you through the iteration process
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import sys
from pathlib import Path

import yaml


def load_state():
    """Load current coverage state."""
    state_file = Path(".coverage_state/coverage_state.yaml")
    if not state_file.exists():
        print("❌ No coverage state found. Run 'aco test' first.")
        sys.exit(1)

    with open(state_file) as f:
        return yaml.safe_load(f)


def load_targets():
    """Load next targets."""
    targets_file = Path(".coverage_state/next_targets.yaml")
    if not targets_file.exists():
        print("❌ No targets file found. Run 'aco test' first.")
        sys.exit(1)

    with open(targets_file) as f:
        return yaml.safe_load(f)


def show_summary(state):
    """Show coverage summary."""
    summary = state["summary"]

    print("=" * 80)
    print("📊 COVERAGE SUMMARY")
    print("=" * 80)
    print(f"Coverage: {summary['percent_covered']:.2f}%")
    print(f"Covered:  {summary['total_covered']:,} / {summary['total_statements']:,} statements")
    print(f"Remaining: {summary['total_statements'] - summary['total_covered']:,} statements")
    print()


def show_targets(targets, limit=10):
    """Show top priority targets."""
    print("=" * 80)
    print(f"🎯 TOP {limit} PRIORITY TARGETS")
    print("=" * 80)

    target_list = targets.get("targets", [])[:limit]

    if not target_list:
        print("✅ No targets found - coverage is complete!")
        return

    for i, target in enumerate(target_list, 1):
        file_path = target["file"]
        line = target["line"]
        reason = target["reason"]
        priority = target.get("priority", 0)

        # Shorten file path
        short_path = file_path.replace("src/acoharmony/", "")

        print(f"\n{i}. {short_path}:{line}")
        print(f"   Priority: {priority:.2f}")
        print(f"   {reason}")


def suggest_test_file(target_file):
    """Suggest which test file to run."""
    # Convert source file to test file path
    # src/acoharmony/_utils/unpack.py -> tests/_utils/test_unpack.py

    if not target_file.startswith("src/acoharmony/"):
        return None

    rel_path = target_file.replace("src/acoharmony/", "")
    parts = Path(rel_path).parts

    if len(parts) < 2:
        return None

    module = parts[0]  # e.g., "_utils"
    file_name = Path(parts[-1]).stem  # e.g., "unpack"

    # Common test patterns
    test_patterns = [
        f"tests/{module}/test_{file_name}.py",
        f"tests/{module}/test_coverage_{file_name}.py",
        f"tests/{module}/test_coverage.py",
        f"tests/{module}/",
    ]

    for pattern in test_patterns:
        test_path = Path(pattern)
        if test_path.exists():
            return str(test_path)

    # Return module directory as fallback
    module_dir = Path(f"tests/{module}/")
    if module_dir.exists():
        return str(module_dir)

    return None


def show_next_steps(targets):
    """Show suggested next steps."""
    target_list = targets.get("targets", [])

    if not target_list:
        print("\n✅ Coverage is complete!")
        return

    top_target = target_list[0]
    target_file = top_target["file"]
    suggested_test = suggest_test_file(target_file)

    print("\n" + "=" * 80)
    print("🚀 NEXT STEPS")
    print("=" * 80)

    print("\n1. Examine the uncovered code:")
    print(f"   cat {target_file}")
    print(f"   # Or open in editor at line {top_target['line']}")

    if suggested_test:
        print("\n2. Run targeted tests:")
        print(f"   aco test --test-path {suggested_test}")
        print("\n   # Or just the specific test file:")
        if suggested_test.endswith(".py"):
            print(f"   aco test --test-path {suggested_test} --no-targets")
    else:
        print(f"\n2. Create test file for {target_file}")
        rel_path = target_file.replace("src/acoharmony/", "")
        module = Path(rel_path).parts[0]
        print(f"   # Consider creating tests/{module}/test_coverage.py")

    print("\n3. Write test to cover:")
    print(f"   {top_target['reason']}")

    print("\n4. Re-run this script to see progress:")
    print("   python -m acoharmony._test.coverage.iterate")


def show_file_groups(targets):
    """Group targets by file to show which files need most attention."""
    target_list = targets.get("targets", [])

    file_counts = {}
    for target in target_list:
        file_path = target["file"]
        file_counts[file_path] = file_counts.get(file_path, 0) + 1

    # Sort by count
    sorted_files = sorted(file_counts.items(), key=lambda x: -x[1])[:10]

    print("\n" + "=" * 80)
    print("📁 FILES NEEDING MOST ATTENTION")
    print("=" * 80)

    for file_path, count in sorted_files:
        short_path = file_path.replace("src/acoharmony/", "")
        suggested_test = suggest_test_file(file_path)

        print(f"\n{short_path}: {count} uncovered items")
        if suggested_test:
            print(f"  → Run: aco test --test-path {suggested_test}")


def main():
    """Main entry point."""
    print("\n")
    state = load_state()
    targets = load_targets()

    show_summary(state)
    show_targets(targets, limit=5)
    show_file_groups(targets)
    show_next_steps(targets)

    print("\n" + "=" * 80)
    print()


if __name__ == "__main__":
    main()
