#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.
# ruff: noqa
# ruff: noqa

"""Add copyright headers to Python source files."""

from datetime import datetime
from pathlib import Path

from acoharmony._log import get_logger

logger = get_logger("dev.add_copyright")


def get_copyright_header(year=None):
    """Generate copyright header."""
    if year is None:
        year = datetime.now().year
    """
    # © {year} HarmonyCares
    # All rights reserved.
    """


def has_copyright(filepath):
    """Check if file already has copyright header."""
    try:
        with open(filepath, encoding="utf-8") as f:
            first_lines = "".join(f.readlines()[:3])
            return "© " in first_lines and "HarmonyCares" in first_lines
    except Exception as e:  # ALLOWED: Returns False to indicate error as part of API contract
        logger.warning(f"Could not check {filepath}: {e}")
        return False


def add_copyright_to_file(filepath, year=None, dry_run=False):
    """
    Add copyright header to a Python file.

        Parameters

        filepath : Path
            Path to the Python file
        year : int, optional
            Copyright year (defaults to current year)
        dry_run : bool
            If True, don't modify files, just report what would be done

        Returns

        bool
            True if header was added, False if skipped
    """
    if has_copyright(filepath):
        logger.debug(f"Skipping {filepath} - already has copyright")
        return False

    if dry_run:
        logger.info(f"Would add copyright to {filepath}")
        return True

    try:
        # Read existing content
        with open(filepath, encoding="utf-8") as f:
            content = f.read()

        # Check if file starts with shebang
        if content.startswith("#!"):
            # Insert copyright after shebang
            lines = content.split("\n", 1)
            if len(lines) > 1:
                new_content = lines[0] + "\n" + get_copyright_header(year) + lines[1]
            else:
                new_content = lines[0] + "\n" + get_copyright_header(year)
        else:
            # Insert copyright at the beginning
            new_content = get_copyright_header(year) + content

        # Write back
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(new_content)

        logger.info(f"Added copyright to {filepath}")
        return True

    except Exception as e:  # ALLOWED: Returns False to indicate error as part of API contract
        logger.error(f"Failed to add copyright to {filepath}: {e}")
        return False


def add_copyright(force=False, dry_run=False, year=None):
    """
    Add copyright headers to all Python files in src.

        Parameters

        force : bool
            Force adding copyright even if it exists (replaces existing)
        dry_run : bool
            If True, don't modify files, just report what would be done
        year : int, optional
            Copyright year (defaults to current year)

        Returns

        bool
            True if successful, False otherwise
    """
    src_dir = Path("src")

    if not src_dir.exists():
        logger.error(f"Source directory not found at {src_dir.absolute()}")
        return False

    logger.info(
        f"{'[DRY RUN] ' if dry_run else ''}Adding copyright headers to Python files in src/"
    )

    # Find all Python files
    py_files = list(src_dir.rglob("*.py"))
    logger.info(f"Found {len(py_files)} Python files")

    added_count = 0
    skipped_count = 0
    failed_count = 0

    for py_file in py_files:
        # Skip __pycache__ directories
        if "__pycache__" in str(py_file):
            continue

        # Skip test files if desired
        if "_test" in str(py_file) or "test_" in py_file.name:
            logger.debug(f"Skipping test file {py_file}")
            skipped_count += 1
            continue

        if force and not dry_run:
            # Remove existing copyright first
            try:
                with open(py_file, encoding="utf-8") as f:
                    lines = f.readlines()

                # Find where copyright ends (if it exists)
                content_start = 0
                for i, line in enumerate(lines[:10]):  # Check first 10 lines
                    if "© " in line and "HarmonyCares" in line:
                        # Found copyright, find where it ends
                        for j in range(i, min(i + 5, len(lines))):
                            if lines[j].strip() == "":
                                content_start = j + 1
                                break
                        break

                # Write without old copyright
                if content_start > 0:
                    with open(py_file, "w", encoding="utf-8") as f:
                        f.writelines(lines[content_start:])

            except Exception:  # ALLOWED: Error handling for file operations
                pass

        # Now add new copyright
        if not dry_run:
            header = get_copyright_header(year)
            add_header_to_file(py_file, header)

        processed_count += 1
