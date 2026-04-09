# © 2025 HarmonyCares
# All rights reserved.

"""
Coverage state management.

Defines data structures for tracking coverage state across runs.
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class BranchMiss:
    """Represents a missing branch in coverage."""

    source_line: int
    target_line: int
    context: str = ""  # Description of what the branch does


@dataclass
class FileState:
    """Coverage state for a single file."""

    path: str
    num_statements: int
    covered_lines: int
    percent_covered: float
    missing_lines: list[int] = field(default_factory=list)
    missing_branches: list[BranchMiss] = field(default_factory=list)
    excluded_lines: list[int] = field(default_factory=list)
    last_attempted_tests: list[str] = field(default_factory=list)
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for YAML serialization."""
        return {
            "path": self.path,
            "summary": {
                "num_statements": self.num_statements,
                "covered_lines": self.covered_lines,
                "percent_covered": round(self.percent_covered, 2),
            },
            "missing_lines": sorted(self.missing_lines),
            "missing_branches": [
                {"source": b.source_line, "target": b.target_line, "context": b.context}
                for b in self.missing_branches
            ],
            "excluded_lines": sorted(self.excluded_lines),
            "last_attempted_tests": self.last_attempted_tests,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FileState":
        """Create from dictionary."""
        summary = data.get("summary", {})
        return cls(
            path=data["path"],
            num_statements=summary.get("num_statements", 0),
            covered_lines=summary.get("covered_lines", 0),
            percent_covered=summary.get("percent_covered", 0.0),
            missing_lines=data.get("missing_lines", []),
            missing_branches=[
                BranchMiss(
                    source_line=b["source"],
                    target_line=b["target"],
                    context=b.get("context", ""),
                )
                for b in data.get("missing_branches", [])
            ],
            excluded_lines=data.get("excluded_lines", []),
            last_attempted_tests=data.get("last_attempted_tests", []),
            notes=data.get("notes", ""),
        )


@dataclass
class CoverageState:
    """Overall coverage state for the project."""

    files: dict[str, FileState] = field(default_factory=dict)
    timestamp: str = ""
    total_statements: int = 0
    total_covered: int = 0
    percent_covered: float = 0.0

    def save(self, path: Path) -> None:
        """Save state to YAML file."""
        data = {
            "timestamp": self.timestamp,
            "summary": {
                "total_statements": self.total_statements,
                "total_covered": self.total_covered,
                "percent_covered": round(self.percent_covered, 2),
            },
            "files": {file_path: state.to_dict() for file_path, state in self.files.items()},
        }

        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    @classmethod
    def load(cls, path: Path) -> "CoverageState":
        """Load state from YAML file."""
        if not path.exists():
            return cls()

        with open(path) as f:
            data = yaml.safe_load(f)

        if not data:
            return cls()

        summary = data.get("summary", {})
        files = {
            file_path: FileState.from_dict(file_data)
            for file_path, file_data in data.get("files", {}).items()
        }

        return cls(
            files=files,
            timestamp=data.get("timestamp", ""),
            total_statements=summary.get("total_statements", 0),
            total_covered=summary.get("total_covered", 0),
            percent_covered=summary.get("percent_covered", 0.0),
        )

    def get_uncovered_count(self) -> int:
        """Get total number of uncovered lines across all files."""
        return sum(len(f.missing_lines) + len(f.missing_branches) for f in self.files.values())
