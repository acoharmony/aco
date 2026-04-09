"""Tests for acoharmony._puf.puf_unpack module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import acoharmony
from acoharmony._puf.puf_unpack import (
    extract_puf_zip,
    make_puf_filename,
    unpack_puf_zips,
)


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._puf.puf_unpack is not None


class TestMakePufFilename:
    """Tests for make_puf_filename."""

    @pytest.mark.unit
    def test_rvu_with_quarter(self):
        result = make_puf_filename(
            "rvu", "2024", "Final", "addenda", "test.csv",
            metadata={"quarter": "A"},
        )
        assert result == "rvu_2024_q1_addenda_test.csv"

    @pytest.mark.unit
    def test_rvu_without_quarter(self):
        result = make_puf_filename("rvu", "2024", "Final", "addenda", "test.csv")
        assert result == "rvu_2024_addenda_test.csv"

    @pytest.mark.unit
    def test_zipcarrier(self):
        result = make_puf_filename("zipcarrier", "2024", "Final", "zips", "data.csv")
        assert result == "zipcarrier_2024_zips_data.csv"

    @pytest.mark.unit
    def test_pfs_default(self):
        result = make_puf_filename("pfs", "2024", "Final Rule", "addenda", "test.csv")
        assert result == "pfs_2024_final_rule_addenda_test.csv"


class TestExtractPufZip:
    """Tests for extract_puf_zip."""

    @pytest.mark.unit
    def test_extract_basic(self, tmp_path):
        # Create a test zip
        zip_path = tmp_path / "test.zip"
        dest_dir = tmp_path / "output"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.csv", "a,b\n1,2")

        result = extract_puf_zip(
            zip_path, dest_dir, "pfs", "2024", "Final", "addenda", "test_key"
        )
        assert len(result) == 1
        assert result[0][0] == "data.csv"

    @pytest.mark.unit
    def test_extract_dry_run(self, tmp_path):
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.csv", "a,b\n1,2")

        result = extract_puf_zip(
            zip_path, tmp_path / "out", "pfs", "2024", "Final",
            "addenda", "key", dry_run=True,
        )
        assert len(result) == 1
        # Destination dir should NOT be created
        assert not (tmp_path / "out").exists()

    @pytest.mark.unit
    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            extract_puf_zip(
                tmp_path / "nonexistent.zip", tmp_path / "out",
                "pfs", "2024", "Final", "cat", "key",
            )

    @pytest.mark.unit
    def test_skip_existing_file(self, tmp_path):
        zip_path = tmp_path / "test.zip"
        dest_dir = tmp_path / "output"
        dest_dir.mkdir(parents=True)

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.csv", "a,b\n1,2")

        # Pre-create the target file
        expected_name = make_puf_filename("pfs", "2024", "Final", "cat", "data.csv")
        (dest_dir / expected_name).write_text("existing")

        result = extract_puf_zip(
            zip_path, dest_dir, "pfs", "2024", "Final", "cat", "key"
        )
        assert len(result) == 1
        # File content should be unchanged (not overwritten)
        assert (dest_dir / expected_name).read_text() == "existing"


class TestUnpackPufZips:
    """Tests covering uncovered branches in unpack_puf_zips."""

    def _make_entry(self, **overrides):
        """Create a mock PUFFileEntry."""
        defaults = {
            "year": "2024",
            "rule_type": "Final",
            "file_key": "test_key",
            "category": "addenda",
            "dataset_key": "pfs",
            "downloaded": True,
            "extracted": False,
            "zip_filename": "test.zip",
            "found_in_archive": False,
            "found_in_bronze": False,
            "corpus_path": None,
            "metadata": {},
            "error_message": None,
        }
        defaults.update(overrides)
        entry = MagicMock(**defaults)
        # Make sure attribute access returns the mock values
        for k, v in defaults.items():
            setattr(entry, k, v)
        return entry

    @pytest.mark.unit
    def test_filter_by_rule_type(self, tmp_path):
        """Branch 243→244: entry filtered out by rule_type mismatch."""
        entry = self._make_entry(rule_type="Proposed")

        mock_tracker = MagicMock()
        mock_tracker.state.files = {"k1": entry}

        with (
            patch("acoharmony._puf.puf_unpack.get_puf_directories",
                  return_value=(tmp_path / "bronze", tmp_path / "archive",
                                tmp_path / "pufs", tmp_path / "corpus")),
            patch("acoharmony._puf.puf_unpack.PUFStateTracker") as mock_pst,
        ):
            mock_pst.load.return_value = mock_tracker
            (tmp_path / "pufs").mkdir(parents=True, exist_ok=True)
            stats = unpack_puf_zips(rule_type="Final", verbose=False)

        assert stats["found"] == 0

    @pytest.mark.unit
    def test_filter_by_category(self, tmp_path):
        """Branch 245→246: entry filtered out by category mismatch."""
        entry = self._make_entry(category="gpci")

        mock_tracker = MagicMock()
        mock_tracker.state.files = {"k1": entry}

        with (
            patch("acoharmony._puf.puf_unpack.get_puf_directories",
                  return_value=(tmp_path / "bronze", tmp_path / "archive",
                                tmp_path / "pufs", tmp_path / "corpus")),
            patch("acoharmony._puf.puf_unpack.PUFStateTracker") as mock_pst,
        ):
            mock_pst.load.return_value = mock_tracker
            (tmp_path / "pufs").mkdir(parents=True, exist_ok=True)
            stats = unpack_puf_zips(category="addenda", verbose=False)

        assert stats["found"] == 0

    @pytest.mark.unit
    def test_skip_not_downloaded(self, tmp_path):
        """Branch 249→250: entry not downloaded is skipped."""
        entry = self._make_entry(downloaded=False)

        mock_tracker = MagicMock()
        mock_tracker.state.files = {"k1": entry}

        with (
            patch("acoharmony._puf.puf_unpack.get_puf_directories",
                  return_value=(tmp_path / "bronze", tmp_path / "archive",
                                tmp_path / "pufs", tmp_path / "corpus")),
            patch("acoharmony._puf.puf_unpack.PUFStateTracker") as mock_pst,
        ):
            mock_pst.load.return_value = mock_tracker
            (tmp_path / "pufs").mkdir(parents=True, exist_ok=True)
            stats = unpack_puf_zips(verbose=False)

        assert stats["found"] == 0

    @pytest.mark.unit
    def test_found_in_archive_flat(self, tmp_path):
        """Branch 263→264: ZIP found via flat archive structure."""
        archive_dir = tmp_path / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        pufs_dir = tmp_path / "pufs"
        pufs_dir.mkdir(parents=True, exist_ok=True)

        # Create the ZIP in flat archive dir
        zip_path = archive_dir / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.csv", "content")

        entry = self._make_entry(found_in_archive=True)

        mock_tracker = MagicMock()
        mock_tracker.state.files = {"k1": entry}

        with (
            patch("acoharmony._puf.puf_unpack.get_puf_directories",
                  return_value=(tmp_path / "bronze", archive_dir,
                                pufs_dir, tmp_path / "corpus")),
            patch("acoharmony._puf.puf_unpack.PUFStateTracker") as mock_pst,
        ):
            mock_pst.load.return_value = mock_tracker
            stats = unpack_puf_zips(verbose=False)

        assert stats["processed"] == 1

    @pytest.mark.unit
    def test_zip_not_found(self, tmp_path):
        """Branch 298→300: ZIP file not found, stats['failed'] incremented."""
        entry = self._make_entry(
            found_in_archive=False,
            found_in_bronze=False,
            corpus_path=None,
        )

        mock_tracker = MagicMock()
        mock_tracker.state.files = {"k1": entry}

        with (
            patch("acoharmony._puf.puf_unpack.get_puf_directories",
                  return_value=(tmp_path / "bronze", tmp_path / "archive",
                                tmp_path / "pufs", tmp_path / "corpus")),
            patch("acoharmony._puf.puf_unpack.PUFStateTracker") as mock_pst,
        ):
            mock_pst.load.return_value = mock_tracker
            (tmp_path / "pufs").mkdir(parents=True, exist_ok=True)
            stats = unpack_puf_zips(verbose=False)

        assert stats["failed"] == 1

    @pytest.mark.unit
    def test_found_in_bronze(self, tmp_path):
        """Branch 310→312: ZIP found in bronze directory."""
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        pufs_dir = tmp_path / "pufs"
        pufs_dir.mkdir(parents=True, exist_ok=True)

        # Create ZIP in bronze
        zip_path = bronze_dir / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.csv", "content")

        entry = self._make_entry(
            found_in_archive=False,
            found_in_bronze=True,
        )

        mock_tracker = MagicMock()
        mock_tracker.state.files = {"k1": entry}

        with (
            patch("acoharmony._puf.puf_unpack.get_puf_directories",
                  return_value=(bronze_dir, tmp_path / "archive",
                                pufs_dir, tmp_path / "corpus")),
            patch("acoharmony._puf.puf_unpack.PUFStateTracker") as mock_pst,
        ):
            mock_pst.load.return_value = mock_tracker
            stats = unpack_puf_zips(verbose=False)

        assert stats["processed"] == 1

    @pytest.mark.unit
    def test_corpus_path_found(self, tmp_path):
        """Branch 294→297: ZIP found via corpus_path."""
        pufs_dir = tmp_path / "pufs"
        pufs_dir.mkdir(parents=True, exist_ok=True)

        corpus_zip = tmp_path / "corpus" / "test.zip"
        corpus_zip.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(corpus_zip, "w") as zf:
            zf.writestr("data.csv", "content")

        entry = self._make_entry(
            found_in_archive=False,
            found_in_bronze=False,
            corpus_path=str(corpus_zip),
        )

        mock_tracker = MagicMock()
        mock_tracker.state.files = {"k1": entry}

        with (
            patch("acoharmony._puf.puf_unpack.get_puf_directories",
                  return_value=(tmp_path / "bronze", tmp_path / "archive",
                                pufs_dir, tmp_path / "corpus")),
            patch("acoharmony._puf.puf_unpack.PUFStateTracker") as mock_pst,
        ):
            mock_pst.load.return_value = mock_tracker
            stats = unpack_puf_zips(verbose=False)

        assert stats["processed"] == 1

    @pytest.mark.unit
    def test_dry_run_verbose_many_files(self, tmp_path):
        """Branch 323→329, 327→328: dry_run verbose with >5 files."""
        archive_dir = tmp_path / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        pufs_dir = tmp_path / "pufs"

        # Create a ZIP with more than 5 files
        zip_path = archive_dir / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            for i in range(7):
                zf.writestr(f"file{i}.csv", f"data{i}")

        entry = self._make_entry(found_in_archive=True)

        mock_tracker = MagicMock()
        mock_tracker.state.files = {"k1": entry}

        with (
            patch("acoharmony._puf.puf_unpack.get_puf_directories",
                  return_value=(tmp_path / "bronze", archive_dir,
                                pufs_dir, tmp_path / "corpus")),
            patch("acoharmony._puf.puf_unpack.PUFStateTracker") as mock_pst,
        ):
            mock_pst.load.return_value = mock_tracker
            stats = unpack_puf_zips(dry_run=True, verbose=True)

        assert stats["processed"] == 1
        assert stats["extracted"] == 7


class TestUnpackPufZipsVerboseOutput:
    """Cover verbose branches: 263->264 (skipped_already_extracted print),
    310->312 (dry_run verbose print), 323->329 (dry_run verbose >5 files)."""

    def _make_entry(self, **overrides):
        """Create a mock PUFFileEntry."""
        defaults = {
            "year": "2024",
            "rule_type": "Final",
            "file_key": "test_key",
            "category": "addenda",
            "dataset_key": "pfs",
            "downloaded": True,
            "extracted": False,
            "zip_filename": "test.zip",
            "found_in_archive": False,
            "found_in_bronze": False,
            "corpus_path": None,
            "metadata": {},
            "error_message": None,
        }
        defaults.update(overrides)
        entry = MagicMock(**defaults)
        for k, v in defaults.items():
            setattr(entry, k, v)
        return entry

    @pytest.mark.unit
    def test_verbose_skipped_already_extracted(self, tmp_path, capsys):
        """Branch 263->264: verbose=True, skipped_already_extracted > 0 prints message."""
        entry = self._make_entry(extracted=True)  # already extracted

        mock_tracker = MagicMock()
        mock_tracker.state.files = {"k1": entry}

        with (
            patch("acoharmony._puf.puf_unpack.get_puf_directories",
                  return_value=(tmp_path / "bronze", tmp_path / "archive",
                                tmp_path / "pufs", tmp_path / "corpus")),
            patch("acoharmony._puf.puf_unpack.PUFStateTracker") as mock_pst,
        ):
            mock_pst.load.return_value = mock_tracker
            (tmp_path / "pufs").mkdir(parents=True, exist_ok=True)
            stats = unpack_puf_zips(verbose=True)

        captured = capsys.readouterr()
        assert stats["skipped_already_extracted"] == 1
        assert "Skipping" in captured.out
        assert "already extracted" in captured.out

    @pytest.mark.unit
    def test_verbose_zip_not_found(self, tmp_path, capsys):
        """Branch 294->297 with verbose: ZIP not found, error message printed."""
        entry = self._make_entry(
            found_in_archive=False,
            found_in_bronze=False,
            corpus_path=None,
        )

        mock_tracker = MagicMock()
        mock_tracker.state.files = {"k1": entry}

        with (
            patch("acoharmony._puf.puf_unpack.get_puf_directories",
                  return_value=(tmp_path / "bronze", tmp_path / "archive",
                                tmp_path / "pufs", tmp_path / "corpus")),
            patch("acoharmony._puf.puf_unpack.PUFStateTracker") as mock_pst,
        ):
            mock_pst.load.return_value = mock_tracker
            (tmp_path / "pufs").mkdir(parents=True, exist_ok=True)
            stats = unpack_puf_zips(verbose=True)

        captured = capsys.readouterr()
        assert stats["failed"] == 1
        assert "ERROR" in captured.out

    @pytest.mark.unit
    def test_dry_run_verbose_with_few_files(self, tmp_path, capsys):
        """Branch 310->312, 323->329: dry_run verbose with <=5 files (no truncation)."""
        archive_dir = tmp_path / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        pufs_dir = tmp_path / "pufs"

        zip_path = archive_dir / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            for i in range(3):
                zf.writestr(f"file{i}.csv", f"data{i}")

        entry = self._make_entry(found_in_archive=True)

        mock_tracker = MagicMock()
        mock_tracker.state.files = {"k1": entry}

        with (
            patch("acoharmony._puf.puf_unpack.get_puf_directories",
                  return_value=(tmp_path / "bronze", archive_dir,
                                pufs_dir, tmp_path / "corpus")),
            patch("acoharmony._puf.puf_unpack.PUFStateTracker") as mock_pst,
        ):
            mock_pst.load.return_value = mock_tracker
            stats = unpack_puf_zips(dry_run=True, verbose=True)

        captured = capsys.readouterr()
        assert stats["processed"] == 1
        assert "DRY RUN" in captured.out
        assert "... and" not in captured.out  # <= 5, no truncation

    @pytest.mark.unit
    def test_corpus_path_file_not_exists(self, tmp_path):
        """Branch 294->297: corpus_path set but file doesn't exist, zip_path stays None."""
        entry = self._make_entry(
            found_in_archive=False,
            found_in_bronze=False,
            corpus_path=str(tmp_path / "nonexistent" / "fake.zip"),
        )

        mock_tracker = MagicMock()
        mock_tracker.state.files = {"k1": entry}

        with (
            patch("acoharmony._puf.puf_unpack.get_puf_directories",
                  return_value=(tmp_path / "bronze", tmp_path / "archive",
                                tmp_path / "pufs", tmp_path / "corpus")),
            patch("acoharmony._puf.puf_unpack.PUFStateTracker") as mock_pst,
        ):
            mock_pst.load.return_value = mock_tracker
            (tmp_path / "pufs").mkdir(parents=True, exist_ok=True)
            stats = unpack_puf_zips(verbose=False)

        assert stats["failed"] == 1

    @pytest.mark.unit
    def test_dry_run_not_verbose(self, tmp_path):
        """Branch 310->312 (False): dry_run=True, verbose=False."""
        archive_dir = tmp_path / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        pufs_dir = tmp_path / "pufs"

        zip_path = archive_dir / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.csv", "content")

        entry = self._make_entry(found_in_archive=True)

        mock_tracker = MagicMock()
        mock_tracker.state.files = {"k1": entry}

        with (
            patch("acoharmony._puf.puf_unpack.get_puf_directories",
                  return_value=(tmp_path / "bronze", archive_dir,
                                pufs_dir, tmp_path / "corpus")),
            patch("acoharmony._puf.puf_unpack.PUFStateTracker") as mock_pst,
        ):
            mock_pst.load.return_value = mock_tracker
            stats = unpack_puf_zips(dry_run=True, verbose=False)

        assert stats["processed"] == 1
