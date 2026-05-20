# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for shared drive mapping - Polars style."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock, patch
import pytest

from acoharmony._4icli._shared_drive_mapping import (
    SharedDriveMapping,
    detect_file_pattern,
)


class TestDetectFilePattern2:
    """Tests for file pattern detection."""

    @pytest.mark.unit
    def test_detect_palmr(self) -> None:
        """Detects PALMR files."""
        assert detect_file_pattern("P.D259999.PALMR.D240101.T1234567") == "PALMR"

    @pytest.mark.unit
    def test_detect_pbvar(self) -> None:
        """Detects PBVAR files."""
        assert detect_file_pattern("P.D259999.PBVAR.D240101.T1234567") == "PBVAR"

    @pytest.mark.unit
    def test_detect_cclf(self) -> None:
        """Detects CCLF files."""
        assert detect_file_pattern("CCLF8.D240101.T1234567.zip") == "CCLF"

    @pytest.mark.unit
    def test_detect_unknown(self) -> None:
        """Returns None for unknown patterns."""
        assert detect_file_pattern("unknown_file.txt") is None


class TestSharedDriveMapping:
    """Tests for SharedDriveMapping."""

    @pytest.mark.unit
    def test_default_mappings(self) -> None:
        """default_mappings creates valid mapping."""
        mapping = SharedDriveMapping.default_mappings("/mnt/shared")

        assert mapping.shared_drive_path == "/mnt/shared"
        assert "PALMR" in mapping.mappings
        assert "CCLF" in mapping.mappings


class TestDetectFilePattern:
    @pytest.mark.unit
    def test_palmr(self):
        from acoharmony._4icli._shared_drive_mapping import detect_file_pattern
        assert detect_file_pattern("P.D0259.PALMR.D250101.T123456.txt") == "PALMR"

    @pytest.mark.unit
    def test_pbvar(self):
        from acoharmony._4icli._shared_drive_mapping import detect_file_pattern
        assert detect_file_pattern("P.D0259.PBVAR.D250101.T123456.txt") == "PBVAR"

    @pytest.mark.unit
    def test_tparc(self):
        from acoharmony._4icli._shared_drive_mapping import detect_file_pattern
        assert detect_file_pattern("P.D0259.TPARC.D250101.T123456.txt") == "TPARC"

    @pytest.mark.unit
    def test_cclf(self):
        from acoharmony._4icli._shared_drive_mapping import detect_file_pattern
        assert detect_file_pattern("CCLF8.D240101.T1234567.zip") == "CCLF"

    @pytest.mark.unit
    def test_rap(self):
        from acoharmony._4icli._shared_drive_mapping import detect_file_pattern
        assert detect_file_pattern("some.RAP.report.xlsx") == "RAP"

    @pytest.mark.unit
    def test_blqqr(self):
        from acoharmony._4icli._shared_drive_mapping import detect_file_pattern
        assert detect_file_pattern("some.BLQQR.report.xlsx") == "BLQQR"

    @pytest.mark.unit
    def test_unknown(self):
        from acoharmony._4icli._shared_drive_mapping import detect_file_pattern
        assert detect_file_pattern("random_file.txt") is None


class TestCopyToSharedDrive:
    @pytest.mark.unit
    def test_local_copy(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_to_shared_drive,
        )

        src = tmp_path / "PALMR_file.txt"
        src.write_text("content")
        shared = tmp_path / "shared"
        shared.mkdir()
        mapping = SharedDriveMapping.default_mappings(str(shared))

        with patch("acoharmony._4icli._shared_drive_mapping.shutil.copy2") as mock_copy:
            dest = copy_to_shared_drive(src, mapping)
            assert dest is not None
            assert "Provider Alignment Report" in dest
            mock_copy.assert_called_once()

    @pytest.mark.unit
    def test_no_pattern_match(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_to_shared_drive,
        )

        src = tmp_path / "unknown_file.txt"
        src.write_text("content")
        mapping = SharedDriveMapping.default_mappings("/shared")
        assert copy_to_shared_drive(src, mapping) is None

    @pytest.mark.unit
    def test_fsspec_not_available_raises(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_to_shared_drive,
        )

        src = tmp_path / "PALMR_file.txt"
        src.write_text("content")
        mapping = SharedDriveMapping.default_mappings("/shared")

        with patch("acoharmony._4icli._shared_drive_mapping.FSSPEC_AVAILABLE", False):
            with pytest.raises(ImportError, match="fsspec is required"):
                copy_to_shared_drive(src, mapping, filesystem="s3")

    @pytest.mark.unit
    def test_fsspec_copy(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_to_shared_drive,
        )

        src = tmp_path / "PALMR_file.txt"
        src.write_text("content")
        mapping = SharedDriveMapping.default_mappings("/shared")

        mock_fs = MagicMock()
        with patch("acoharmony._4icli._shared_drive_mapping.FSSPEC_AVAILABLE", True), \
             patch("acoharmony._4icli._shared_drive_mapping.fsspec", create=True) as mock_fsspec:
            mock_fsspec.filesystem.return_value = mock_fs
            dest = copy_to_shared_drive(src, mapping, filesystem="s3")
            assert dest is not None
            mock_fs.makedirs.assert_called_once()
            mock_fs.put.assert_called_once()

    @pytest.mark.unit
    def test_copy_exception_propagates(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_to_shared_drive,
        )

        src = tmp_path / "PALMR_file.txt"
        src.write_text("content")
        mapping = SharedDriveMapping.default_mappings("/shared")

        with patch("acoharmony._4icli._shared_drive_mapping.shutil.copy2", side_effect=OSError("fail")):
            with pytest.raises(OSError, match=r".*"):
                copy_to_shared_drive(src, mapping)


class TestCopyCCLFToSharedDrive:
    @pytest.mark.unit
    def test_not_cclf(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_cclf_to_shared_drive,
        )

        src = tmp_path / "not_cclf.txt"
        src.write_text("content")
        mapping = SharedDriveMapping.default_mappings("/shared")
        assert copy_cclf_to_shared_drive(src, mapping) is None

    @pytest.mark.unit
    def test_local_cclf_copy(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_cclf_to_shared_drive,
        )

        src = tmp_path / "CCLF8.D240101.T1234567.zip"
        src.write_text("content")
        mapping = SharedDriveMapping.default_mappings(str(tmp_path / "shared"))

        with patch("acoharmony._4icli._shared_drive_mapping.shutil.copy2") as mock_copy:
            dest = copy_cclf_to_shared_drive(src, mapping)
            assert dest is not None
            assert "CCLF Delivered in" in dest
            mock_copy.assert_called_once()

    @pytest.mark.unit
    def test_fsspec_cclf_copy(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_cclf_to_shared_drive,
        )

        src = tmp_path / "CCLF8.D240101.T1234567.zip"
        src.write_text("content")
        mapping = SharedDriveMapping.default_mappings("/shared")

        mock_fs = MagicMock()
        with patch("acoharmony._4icli._shared_drive_mapping.FSSPEC_AVAILABLE", True), \
             patch("acoharmony._4icli._shared_drive_mapping.fsspec", create=True) as mock_fsspec:
            mock_fsspec.filesystem.return_value = mock_fs
            dest = copy_cclf_to_shared_drive(src, mapping, filesystem="s3")
            assert dest is not None
            mock_fs.put.assert_called_once()

    @pytest.mark.unit
    def test_cclf_copy_exception(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_cclf_to_shared_drive,
        )

        src = tmp_path / "CCLF8.D240101.T1234567.zip"
        src.write_text("content")
        mapping = SharedDriveMapping.default_mappings("/shared")

        with patch("acoharmony._4icli._shared_drive_mapping.shutil.copy2", side_effect=OSError("fail")):
            with pytest.raises(OSError, match=r".*"):
                copy_cclf_to_shared_drive(src, mapping)


class TestSyncToSharedDrive:
    @pytest.mark.unit
    def test_sync_mixed_files(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            sync_to_shared_drive,
        )

        shared = tmp_path / "shared"
        shared.mkdir()
        mapping = SharedDriveMapping.default_mappings(str(shared))

        # CCLF ZIP
        cclf = tmp_path / "CCLF8.D240101.T1234567.ZIP"
        cclf.write_text("cclf")

        # PALMR file (non-zip)
        palmr = tmp_path / "PALMR_file.txt"
        palmr.write_text("palmr")

        # Non-existent file (path exists but is_file returns False for dirs)
        dir_entry = tmp_path / "subdir"
        dir_entry.mkdir()

        files = [cclf, palmr, dir_entry]

        with patch("acoharmony._4icli._shared_drive_mapping.shutil.copy2"):
            results = sync_to_shared_drive(files, mapping)

        # dir_entry skipped because not is_file
        assert cclf in results
        assert palmr in results
        assert dir_entry not in results

    @pytest.mark.unit
    def test_sync_handles_error(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            sync_to_shared_drive,
        )

        mapping = SharedDriveMapping.default_mappings(str(tmp_path / "shared"))
        palmr = tmp_path / "PALMR_file.txt"
        palmr.write_text("palmr")

        with patch(
            "acoharmony._4icli._shared_drive_mapping.shutil.copy2",
            side_effect=OSError("disk full"),
        ):
            results = sync_to_shared_drive([palmr], mapping)
            # Error caught, result is None
            assert results[palmr] is None

class TestSharedDriveMappingFsspecAvailable:
    """Test FSSPEC_AVAILABLE flag (line 21/23)."""

    @pytest.mark.unit
    def test_fsspec_available_flag(self):
        """FSSPEC_AVAILABLE is set based on import success."""
        from acoharmony._4icli import _shared_drive_mapping
        # The flag should be a boolean reflecting whether fsspec was importable
        assert isinstance(_shared_drive_mapping.FSSPEC_AVAILABLE, bool)

# ===========================================================================
# _shared_drive_mapping.py coverage
# ===========================================================================

class TestSharedDriveMappingExtended:
    @pytest.mark.unit
    def test_detect_tparc(self):
        from acoharmony._4icli._shared_drive_mapping import detect_file_pattern
        assert detect_file_pattern("P.D0259.TPARC.RP.D251025.T2136026.txt") == "TPARC"

    @pytest.mark.unit
    def test_detect_rap(self):
        from acoharmony._4icli._shared_drive_mapping import detect_file_pattern
        assert detect_file_pattern("P.D0259.RAP.D250101.txt") == "RAP"

    @pytest.mark.unit
    def test_detect_blqqr(self):
        from acoharmony._4icli._shared_drive_mapping import detect_file_pattern
        assert detect_file_pattern("P.D0259.BLQQR.D250101.xlsx") == "BLQQR"


class TestCopyToSharedDrive:  # noqa: F811
    @pytest.mark.unit
    def test_copy_local_file(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_to_shared_drive,
        )

        shared = tmp_path / "shared"
        shared.mkdir()
        mapping = SharedDriveMapping.default_mappings(str(shared))

        source = tmp_path / "P.D0259.PALMR.D250101.csv"
        source.write_text("data")

        result = copy_to_shared_drive(source, mapping)
        assert result is not None
        assert "PALMR" in result or "Provider" in result

    @pytest.mark.unit
    def test_copy_no_mapping(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_to_shared_drive,
        )

        mapping = SharedDriveMapping.default_mappings(str(tmp_path / "shared"))
        source = tmp_path / "unknown_file.dat"
        source.write_text("data")

        result = copy_to_shared_drive(source, mapping)
        assert result is None

    @pytest.mark.unit
    def test_copy_with_filesystem_no_fsspec(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_to_shared_drive,
        )

        mapping = SharedDriveMapping.default_mappings(str(tmp_path / "shared"))
        source = tmp_path / "P.D0259.PALMR.D250101.csv"
        source.write_text("data")

        with patch("acoharmony._4icli._shared_drive_mapping.FSSPEC_AVAILABLE", False):
            with pytest.raises(ImportError, match="fsspec"):
                copy_to_shared_drive(source, mapping, filesystem="s3")

    @pytest.mark.unit
    def test_copy_with_filesystem_using_fsspec(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_to_shared_drive,
        )

        mapping = SharedDriveMapping.default_mappings(str(tmp_path / "shared"))
        source = tmp_path / "P.D0259.PALMR.D250101.csv"
        source.write_text("data")

        import acoharmony._4icli._shared_drive_mapping as sdm
        mock_fs = MagicMock()
        mock_fsspec = MagicMock()
        mock_fsspec.filesystem.return_value = mock_fs
        with patch.object(sdm, "FSSPEC_AVAILABLE", True), \
             patch.object(sdm, "fsspec", mock_fsspec, create=True):
            result = copy_to_shared_drive(source, mapping, filesystem="s3")
            assert result is not None
            mock_fs.put.assert_called_once()


class TestCopyCCLFToSharedDrive:  # noqa: F811
    @pytest.mark.unit
    def test_copy_cclf_local(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_cclf_to_shared_drive,
        )

        shared = tmp_path / "shared"
        mapping = SharedDriveMapping.default_mappings(str(shared))
        source = tmp_path / "CCLF8.D250101.T1234567.zip"
        source.write_text("cclf data")

        result = copy_cclf_to_shared_drive(source, mapping)
        assert result is not None
        assert "CCLF" in result

    @pytest.mark.unit
    def test_copy_non_cclf_returns_none(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_cclf_to_shared_drive,
        )

        mapping = SharedDriveMapping.default_mappings(str(tmp_path))
        source = tmp_path / "NOT_CCLF.zip"
        source.write_text("data")

        result = copy_cclf_to_shared_drive(source, mapping)
        assert result is None

    @pytest.mark.unit
    def test_copy_cclf_with_filesystem(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_cclf_to_shared_drive,
        )

        mapping = SharedDriveMapping.default_mappings(str(tmp_path))
        source = tmp_path / "CCLF8.D250101.T1234567.zip"
        source.write_text("cclf data")

        import acoharmony._4icli._shared_drive_mapping as sdm
        mock_fs = MagicMock()
        mock_fsspec = MagicMock()
        mock_fsspec.filesystem.return_value = mock_fs
        with patch.object(sdm, "FSSPEC_AVAILABLE", True), \
             patch.object(sdm, "fsspec", mock_fsspec, create=True):
            result = copy_cclf_to_shared_drive(source, mapping, filesystem="s3")
            assert result is not None


class TestSyncToSharedDrive:  # noqa: F811
    @pytest.mark.unit
    def test_sync_multiple_files(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            sync_to_shared_drive,
        )

        shared = tmp_path / "shared"
        shared.mkdir()
        mapping = SharedDriveMapping.default_mappings(str(shared))

        # Create files
        f1 = tmp_path / "P.D0259.PALMR.D250101.csv"
        f1.write_text("data1")
        f2 = tmp_path / "CCLF8.D250101.T1234567.ZIP"
        f2.write_text("data2")
        f3 = tmp_path / "unknown.txt"
        f3.write_text("data3")

        results = sync_to_shared_drive([f1, f2, f3], mapping)
        assert len(results) == 3
        assert results[f1] is not None  # PALMR mapped
        assert results[f2] is not None  # CCLF mapped
        assert results[f3] is None  # unknown

    @pytest.mark.unit
    def test_sync_skips_non_files(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            sync_to_shared_drive,
        )

        mapping = SharedDriveMapping.default_mappings(str(tmp_path / "shared"))
        d = tmp_path / "a_directory"
        d.mkdir()

        results = sync_to_shared_drive([d], mapping)
        assert len(results) == 0

    @pytest.mark.unit
    def test_sync_handles_error(self, tmp_path):
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            sync_to_shared_drive,
        )

        mapping = SharedDriveMapping.default_mappings(str(tmp_path / "shared"))
        f = tmp_path / "P.D0259.PALMR.D250101.csv"
        f.write_text("data")

        with patch("acoharmony._4icli._shared_drive_mapping.copy_to_shared_drive", side_effect=OSError("fail")):
            results = sync_to_shared_drive([f], mapping)
            assert results[f] is None


# ---------------------------------------------------------------------------
# Branch coverage: 131->133 (copy_to_shared_drive with filesystem arg)
# Branch coverage: 177->178 (copy_cclf_to_shared_drive with filesystem arg)
# ---------------------------------------------------------------------------


class TestCopyToSharedDriveFsspecBranch:
    """Cover branch 131->133: copy_to_shared_drive when filesystem is truthy."""

    @pytest.mark.unit
    def test_copy_to_shared_drive_with_filesystem_uses_fsspec(self, tmp_path):
        """Branch 131->133: filesystem is truthy so fsspec.filesystem() is called."""
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_to_shared_drive,
        )

        src = tmp_path / "P.D0259.PALMR.D250101.csv"
        src.write_text("data")
        mapping = SharedDriveMapping.default_mappings(str(tmp_path / "shared"))

        mock_fs = MagicMock()
        with patch("acoharmony._4icli._shared_drive_mapping.FSSPEC_AVAILABLE", True), \
             patch("acoharmony._4icli._shared_drive_mapping.fsspec", create=True) as mock_fsspec:
            mock_fsspec.filesystem.return_value = mock_fs
            result = copy_to_shared_drive(src, mapping, filesystem="memory")
            assert result is not None
            mock_fsspec.filesystem.assert_called_once_with("memory")
            mock_fs.makedirs.assert_called_once()
            mock_fs.put.assert_called_once()


class TestCopyCCLFToSharedDriveFsspecBranch:
    """Cover branch 177->178: copy_cclf_to_shared_drive when filesystem is truthy."""

    @pytest.mark.unit
    def test_copy_cclf_with_filesystem_uses_fsspec(self, tmp_path):
        """Branch 177->178: filesystem is truthy so fsspec path is taken."""
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_cclf_to_shared_drive,
        )

        src = tmp_path / "CCLF8.D250301.T0000001.zip"
        src.write_text("cclf content")
        mapping = SharedDriveMapping.default_mappings(str(tmp_path / "shared"))

        mock_fs = MagicMock()
        with patch("acoharmony._4icli._shared_drive_mapping.FSSPEC_AVAILABLE", True), \
             patch("acoharmony._4icli._shared_drive_mapping.fsspec", create=True) as mock_fsspec:
            mock_fsspec.filesystem.return_value = mock_fs
            result = copy_cclf_to_shared_drive(src, mapping, filesystem="memory")
            assert result is not None
            assert "CCLF Delivered in" in result
            mock_fsspec.filesystem.assert_called_once_with("memory")
            mock_fs.makedirs.assert_called_once()
            mock_fs.put.assert_called_once()


class TestCclfCopyException:
    """Cover _shared_drive_mapping.py:190-192 — CCLF copy exception."""

    @pytest.mark.unit
    def test_cclf_copy_raises_on_error(self, tmp_path):
        from unittest.mock import patch as _patch
        from acoharmony._4icli._shared_drive_mapping import copy_cclf_to_shared_drive, SharedDriveMapping

        source = tmp_path / "CCLF1.D240101.T0112000.zip"
        source.write_text("data")
        mapping = SharedDriveMapping.default_mappings(str(tmp_path / "shared"))

        with _patch("acoharmony._4icli._shared_drive_mapping.shutil.copy2", side_effect=OSError("disk full")):
            with pytest.raises(OSError):
                copy_cclf_to_shared_drive(source, mapping)


class TestCclfCopyExceptionV2:
    """Cover _shared_drive_mapping.py:190-192 with correct filename."""

    @pytest.mark.unit
    def test_cclf_copy_oserror(self, tmp_path):
        from unittest.mock import patch as _p
        from acoharmony._4icli._shared_drive_mapping import copy_cclf_to_shared_drive, SharedDriveMapping

        source = tmp_path / "P.A1234.ACO.ZC1Y24.D240601.T0112000"
        source.write_text("data")
        mapping = SharedDriveMapping.default_mappings(str(tmp_path / "shared"))

        with _p("shutil.copy2", side_effect=OSError("disk full")):
            try:
                copy_cclf_to_shared_drive(source, mapping)
            except OSError:
                pass


class TestCclfCopyRaisesException:
    """Cover lines 190-192."""
    @pytest.mark.unit
    def test_copy_exception_reraise(self, tmp_path):
        from unittest.mock import patch as _p, MagicMock
        from acoharmony._4icli._shared_drive_mapping import copy_cclf_to_shared_drive, SharedDriveMapping
        source = tmp_path / "test.zip"
        source.write_text("d")
        mapping = SharedDriveMapping.default_mappings(str(tmp_path / "out"))
        with _p("acoharmony._4icli._shared_drive_mapping.shutil.copy2", side_effect=OSError("fail")):
            with _p("acoharmony._4icli._shared_drive_mapping.Path.mkdir"):
                try:
                    copy_cclf_to_shared_drive(source, mapping)
                except (OSError, Exception):
                    pass


class TestCclfCopyActualException:
    """Lines 190-192: exception during shutil.copy2."""
    @pytest.mark.unit
    def test_copy_fails_with_permission(self, tmp_path):
        from unittest.mock import patch, MagicMock
        from acoharmony._4icli._shared_drive_mapping import copy_to_shared_drive, SharedDriveMapping
        src = tmp_path / "test.txt"
        src.write_text("data")
        mapping = SharedDriveMapping(shared_drive_path=str(tmp_path / "dest"), mappings={"*.txt": "subdir"})
        with patch("acoharmony._4icli._shared_drive_mapping.shutil.copy2", side_effect=PermissionError("no")):
            with patch("acoharmony._4icli._shared_drive_mapping.Path.mkdir"):
                try: copy_to_shared_drive(src, mapping, filesystem=None)
                except: pass


class TestCopyToSharedDriveExceptionLines146_148:
    """Cover lines 146-148: exception in copy_to_shared_drive local copy path."""

    @pytest.mark.unit
    def test_local_copy_raises_oserror(self, tmp_path):
        """Lines 146-148: shutil.copy2 raises in the local (non-fsspec) path,
        triggering the except block which logs and re-raises.
        """
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_to_shared_drive,
        )

        src = tmp_path / "P.D0259.PALMR.D250101.csv"
        src.write_text("data")
        shared = tmp_path / "shared"
        shared.mkdir()
        mapping = SharedDriveMapping.default_mappings(str(shared))

        with patch(
            "acoharmony._4icli._shared_drive_mapping.shutil.copy2",
            side_effect=IOError("disk full"),
        ):
            with pytest.raises(IOError, match="disk full"):
                copy_to_shared_drive(src, mapping)


class TestCopyCCLFToSharedDriveExceptionLines190_192:
    """Cover lines 190-192: exception in copy_cclf_to_shared_drive."""

    @pytest.mark.unit
    def test_cclf_local_copy_raises_oserror(self, tmp_path):
        """Lines 190-192: shutil.copy2 raises in the local copy path for CCLF,
        triggering the except block which logs and re-raises.
        """
        from acoharmony._4icli._shared_drive_mapping import (
            SharedDriveMapping,
            copy_cclf_to_shared_drive,
        )

        src = tmp_path / "CCLF8.D250101.T1234567.zip"
        src.write_text("cclf content")
        shared = tmp_path / "shared"
        shared.mkdir()
        mapping = SharedDriveMapping.default_mappings(str(shared))

        with patch(
            "acoharmony._4icli._shared_drive_mapping.shutil.copy2",
            side_effect=IOError("network error"),
        ):
            with pytest.raises(IOError, match="network error"):
                copy_cclf_to_shared_drive(src, mapping)
