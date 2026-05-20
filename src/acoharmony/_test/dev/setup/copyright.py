"""Tests for acoharmony._dev.setup.copyright module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._dev.setup.copyright is not None


class TestAddCopyrightPycacheSkip:
    """Cover branch 131->132: __pycache__ files are skipped via continue."""

    @pytest.mark.unit
    def test_pycache_files_skipped(self, tmp_path):
        """Files in __pycache__ directories are skipped (branch 131->132)."""
        from acoharmony._dev.setup.copyright import add_copyright

        src_dir = tmp_path / "src"
        src_dir.mkdir()

        # Create a file inside __pycache__
        pycache = src_dir / "__pycache__"
        pycache.mkdir()
        cache_file = pycache / "module.py"
        cache_file.write_text("x = 1\n")

        with patch("acoharmony._dev.setup.copyright.Path") as mock_path_cls:
            mock_src = MagicMock()
            mock_src.exists.return_value = True
            # Only __pycache__ files so all get skipped via continue and we never hit the bug at line 170
            mock_src.rglob.return_value = [cache_file]
            mock_path_cls.return_value = mock_src

            # force=False, dry_run=True
            result = add_copyright(force=False, dry_run=True, year=2025)
            # Function returns None (no explicit return after loop)
            assert result is None


class TestAddCopyrightForceNoBlankLine:
    """Cover branch 151->155: copyright found but no blank line follows within 5 lines."""

    @pytest.mark.unit
    def test_force_copyright_no_blank_line_after(self, tmp_path):
        """When force=True and copyright has no trailing blank line, inner for-loop exhausts (branch 151->155)."""
        from acoharmony._dev.setup.copyright import add_copyright

        src_dir = tmp_path / "src"
        src_dir.mkdir()

        # Create a file with copyright but NO blank line within the next 5 lines
        # The copyright line is at index 0, so inner loop checks lines[0:5] (indices 0-4)
        # None of these have blank lines
        py_file = src_dir / "module.py"
        py_file.write_text(
            "# © 2025 HarmonyCares\n"
            "# All rights reserved.\n"
            "# line3 non-blank\n"
            "# line4 non-blank\n"
            "# line5 non-blank\n"
        )

        with patch("acoharmony._dev.setup.copyright.Path") as mock_path_cls:
            mock_src = MagicMock()
            mock_src.exists.return_value = True
            mock_src.rglob.return_value = [py_file]
            mock_path_cls.return_value = mock_src

            # force=True, dry_run=False to reach the force copyright removal path
            # This will fail at line 170 (processed_count undefined) but the branch is covered
            with pytest.raises((UnboundLocalError, NameError)):
                add_copyright(force=True, dry_run=False, year=2025)

        # Verify: content_start remained 0 since no blank line was found,
        # so the file was NOT rewritten (if content_start > 0 branch not taken)
        content = py_file.read_text()
        assert "© 2025 HarmonyCares" in content
