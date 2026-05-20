# © 2025 HarmonyCares
# All rights reserved.

"""
Coverage tracking infrastructure.

Provides best-practice coverage state management following the pattern:
1. Extract missing coverage from coverage.py JSON
2. Plan next test target based on priority
3. Track attempts and results
4. Diff coverage between runs
"""

from .diff_coverage import diff_coverage
from .extract_missing import extract_missing_coverage
from .plan_next_target import plan_next_target
from .state import BranchMiss, CoverageState, FileState

__all__ = [
    "extract_missing_coverage",
    "plan_next_target",
    "diff_coverage",
    "CoverageState",
    "FileState",
    "BranchMiss",
]
