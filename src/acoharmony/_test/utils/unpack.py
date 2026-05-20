# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for unpack module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import zipfile
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
import yaml

import acoharmony._utils.unpack as unpack_module
from acoharmony._utils.unpack import (
    _load_schemas,
    _match_file_to_schemas,
    _extract_zip_flat,
    unpack_bronze_zips,
)

if TYPE_CHECKING:
    pass



# Test _load_schemas

@pytest.mark.unit
def test__load_schemas_loads_schemas(monkeypatch) -> None:
    """_load_schemas loads schemas from SchemaRegistry."""
    from unittest.mock import patch

    with patch("acoharmony._utils.unpack.SchemaRegistry") as MockSR:
        MockSR.list_schemas.return_value = ["test_schema"]
        MockSR.get_full_table_config.return_value = {"name": "test_schema", "columns": ["col1", "col2"]}
        schemas = _load_schemas()

    assert "test_schema" in schemas
    assert schemas["test_schema"]["name"] == "test_schema"


@pytest.mark.unit
def test__load_schemas_missing_directory() -> None:
    """_load_schemas returns empty dict when no schemas registered."""
    from unittest.mock import patch

    with patch("acoharmony._utils.unpack.SchemaRegistry") as MockSR:
        MockSR.list_schemas.return_value = []
        schemas = _load_schemas()
    assert schemas == {}


@pytest.mark.unit
def test__load_schemas_skips_invalid_yaml() -> None:
    """_load_schemas skips schemas with no config."""
    from unittest.mock import patch

    with patch("acoharmony._utils.unpack.SchemaRegistry") as MockSR:
        MockSR.list_schemas.return_value = ["valid", "invalid"]
        MockSR.get_full_table_config.side_effect = lambda name: {
            "valid": {"name": "valid"},
            "invalid": None,
        }.get(name)
        schemas = _load_schemas()

    assert "valid" in schemas
    assert "invalid" not in schemas


# Test _match_file_to_schemas

@pytest.mark.unit
def test__match_file_to_schemas_matches_pattern() -> None:
    """_match_file_to_schemas matches filename against patterns."""
    schemas = {
        "cclf0": {
            "storage": {
                "file_patterns": {
                    "mssp": "P.A*.ZC0Y*.dat"
                }
            }
        }
    }

    matches = _match_file_to_schemas("P.ACO.ZC0Y.D240101.T123456.dat", schemas)
    assert "cclf0:mssp" in matches


@pytest.mark.unit
def test__match_file_to_schemas_no_match() -> None:
    """_match_file_to_schemas returns empty list when no patterns match."""
    schemas = {
        "cclf0": {
            "storage": {
                "file_patterns": {
                    "mssp": "P.A*.ZC0Y*.dat"
                }
            }
        }
    }

    matches = _match_file_to_schemas("unrelated_file.csv", schemas)
    assert matches == []


@pytest.mark.unit
def test__match_file_to_schemas_none_schema() -> None:
    """_match_file_to_schemas skips None schemas."""
    schemas = {
        "valid": {
            "storage": {
                "file_patterns": {
                    "mssp": "*.dat"
                }
            }
        },
        "none_schema": None
    }

    matches = _match_file_to_schemas("test.dat", schemas)
    assert "valid:mssp" in matches
    assert len([m for m in matches if "none_schema" in m]) == 0


@pytest.mark.unit
def test__match_file_to_schemas_none_storage() -> None:
    """_match_file_to_schemas skips schemas with None storage."""
    schemas = {
        "valid": {
            "storage": {
                "file_patterns": {
                    "mssp": "*.dat"
                }
            }
        },
        "none_storage": {
            "storage": None
        }
    }

    matches = _match_file_to_schemas("test.dat", schemas)
    assert "valid:mssp" in matches
    assert len([m for m in matches if "none_storage" in m]) == 0


@pytest.mark.unit
def test__match_file_to_schemas_none_file_patterns() -> None:
    """_match_file_to_schemas skips schemas with None file_patterns."""
    schemas = {
        "valid": {
            "storage": {
                "file_patterns": {
                    "mssp": "*.dat"
                }
            }
        },
        "none_patterns": {
            "storage": {
                "file_patterns": None
            }
        }
    }

    matches = _match_file_to_schemas("test.dat", schemas)
    assert "valid:mssp" in matches
    assert len([m for m in matches if "none_patterns" in m]) == 0


# Test _extract_zip_flat

@pytest.mark.unit
def test__extract_zip_flat_extracts_files(tmp_path: Path) -> None:
    """_extract_zip_flat extracts files from ZIP."""

    # Create a ZIP file
    zip_path = tmp_path / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file1.txt", "content1")
        zf.writestr("nested/file2.txt", "content2")

    # Extract
    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()
    logger = MagicMock()

    extracted = _extract_zip_flat(zip_path, dest_dir, logger)

    assert len(extracted) == 2
    assert (dest_dir / "file1.txt").exists()
    assert (dest_dir / "file2.txt").exists()


@pytest.mark.unit
def test__extract_zip_flat_skips_existing(tmp_path: Path) -> None:
    """_extract_zip_flat skips files that already exist."""

    # Create a ZIP file
    zip_path = tmp_path / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file1.txt", "content1")

    # Create existing file
    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()
    (dest_dir / "file1.txt").write_text("existing")

    logger = MagicMock()
    extracted = _extract_zip_flat(zip_path, dest_dir, logger)

    assert len(extracted) == 0
    assert (dest_dir / "file1.txt").read_text() == "existing"


@pytest.mark.unit
def test__extract_zip_flat_deletes_empty_files(tmp_path: Path) -> None:
    """_extract_zip_flat deletes empty (0 byte) files."""

    # Create a ZIP file with an empty file
    zip_path = tmp_path / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("empty.txt", "")
        zf.writestr("nonempty.txt", "content")

    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()
    logger = MagicMock()

    extracted = _extract_zip_flat(zip_path, dest_dir, logger)

    assert len(extracted) == 1
    assert not (dest_dir / "empty.txt").exists()
    assert (dest_dir / "nonempty.txt").exists()


# Test unpack_bronze_zips

@pytest.mark.unit
def test_unpack_bronze_zips_no_zips(tmp_path: Path, monkeypatch) -> None:
    """unpack_bronze_zips returns early when no ZIP files found."""

    # Mock config
    config = MagicMock()
    config.storage.base_path = tmp_path
    config.storage.bronze_dir = "bronze"
    config.storage.archive_dir = "archive"

    monkeypatch.setattr("acoharmony._utils.unpack.get_config", lambda: config)

    result = unpack_bronze_zips()

    assert result["found"] == 0
    assert result["processed"] == 0


@pytest.mark.unit
def test_unpack_bronze_zips_dry_run(tmp_path: Path, monkeypatch) -> None:
    """unpack_bronze_zips in dry run mode doesn't modify files."""

    # Setup directories
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()

    # Create a ZIP file
    zip_path = bronze_dir / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file1.txt", "content")

    # Mock config
    config = MagicMock()
    config.storage.base_path = tmp_path
    config.storage.bronze_dir = "bronze"
    config.storage.archive_dir = "archive"

    monkeypatch.setattr("acoharmony._utils.unpack.get_config", lambda: config)

    result = unpack_bronze_zips(dry_run=True)

    assert result["found"] == 1
    assert result["processed"] == 1
    assert zip_path.exists()  # ZIP should still exist
    assert not (bronze_dir / "file1.txt").exists()  # File not extracted


@pytest.mark.unit
def test_unpack_bronze_zips_extracts_and_archives(tmp_path: Path, monkeypatch) -> None:
    """unpack_bronze_zips extracts files and archives ZIP."""

    # Setup directories
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()

    # Create a ZIP file
    zip_path = bronze_dir / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file1.txt", "content")

    # Mock config
    config = MagicMock()
    config.storage.base_path = tmp_path
    config.storage.bronze_dir = "bronze"
    config.storage.archive_dir = "archive"

    monkeypatch.setattr("acoharmony._utils.unpack.get_config", lambda: config)

    result = unpack_bronze_zips()

    assert result["found"] == 1
    assert result["processed"] == 1
    assert result["extracted"] == 1
    assert not zip_path.exists()  # ZIP moved
    assert (archive_dir / "test.zip").exists()  # ZIP in archive
    assert (bronze_dir / "file1.txt").exists()  # File extracted


@pytest.mark.unit
def test_unpack_bronze_zips_finds_zipfile_without_extension(tmp_path: Path, monkeypatch) -> None:
    """unpack_bronze_zips finds ZIP files without .zip extension."""

    # Setup directories
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()

    # Create a ZIP file without .zip extension
    zip_path = bronze_dir / "NOEXTENSION"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file1.txt", "content")

    # Mock config
    config = MagicMock()
    config.storage.base_path = tmp_path
    config.storage.bronze_dir = "bronze"
    config.storage.archive_dir = "archive"

    monkeypatch.setattr("acoharmony._utils.unpack.get_config", lambda: config)

    result = unpack_bronze_zips()

    assert result["found"] == 1
    assert result["processed"] == 1
    assert result["extracted"] == 1


@pytest.mark.unit
def test_unpack_bronze_zips_handles_archive_exists(tmp_path: Path, monkeypatch) -> None:
    """unpack_bronze_zips deletes original when archive already exists."""

    # Setup directories
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()

    # Create a ZIP file
    zip_path = bronze_dir / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file1.txt", "content")

    # Create archive file that already exists
    (archive_dir / "test.zip").touch()

    # Mock config
    config = MagicMock()
    config.storage.base_path = tmp_path
    config.storage.bronze_dir = "bronze"
    config.storage.archive_dir = "archive"

    monkeypatch.setattr("acoharmony._utils.unpack.get_config", lambda: config)

    result = unpack_bronze_zips()

    assert result["processed"] == 1
    assert not zip_path.exists()  # Original deleted


@pytest.mark.unit
def test_unpack_bronze_zips_updates_state_tracker(tmp_path: Path, monkeypatch) -> None:
    """unpack_bronze_zips updates state tracker when provided."""

    # Setup directories
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()

    # Create a ZIP file
    zip_path = bronze_dir / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file1.txt", "content")

    # Mock config
    config = MagicMock()
    config.storage.base_path = tmp_path
    config.storage.bronze_dir = "bronze"
    config.storage.archive_dir = "archive"

    # Mock state tracker
    state_tracker = MagicMock()

    monkeypatch.setattr("acoharmony._utils.unpack.get_config", lambda: config)

    result = unpack_bronze_zips(state_tracker=state_tracker)

    assert result["processed"] == 1
    state_tracker.update_file_location.assert_called_once()


@pytest.mark.unit
def test_unpack_bronze_zips_handles_bad_zip(tmp_path: Path, monkeypatch) -> None:
    """unpack_bronze_zips handles corrupted ZIP files."""

    # Setup directories
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()

    # Create an invalid ZIP file
    zip_path = bronze_dir / "bad.zip"
    zip_path.write_text("not a zip file")

    # Mock config
    config = MagicMock()
    config.storage.base_path = tmp_path
    config.storage.bronze_dir = "bronze"
    config.storage.archive_dir = "archive"

    monkeypatch.setattr("acoharmony._utils.unpack.get_config", lambda: config)

    result = unpack_bronze_zips()

    assert result["found"] == 1
    assert result["failed"] == 1
    assert zip_path.exists()  # Bad ZIP kept for manual review


@pytest.mark.unit
def test_unpack_bronze_zips_state_tracker_error_continues(tmp_path: Path, monkeypatch) -> None:
    """unpack_bronze_zips continues processing even if state tracker fails."""

    # Setup directories
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()

    # Create a ZIP file
    zip_path = bronze_dir / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file1.txt", "content")

    # Mock config
    config = MagicMock()
    config.storage.base_path = tmp_path
    config.storage.bronze_dir = "bronze"
    config.storage.archive_dir = "archive"

    # Mock state tracker that raises error
    state_tracker = MagicMock()
    state_tracker.update_file_location.side_effect = Exception("State error")

    monkeypatch.setattr("acoharmony._utils.unpack.get_config", lambda: config)

    result = unpack_bronze_zips(state_tracker=state_tracker)

    # Should still process successfully despite state tracker error
    assert result["processed"] == 1
    assert result["failed"] == 0


@pytest.mark.unit
def test__load_schemas_skips_empty_config(monkeypatch) -> None:
    """_load_schemas skips schemas whose full config is empty (covers branch 46->44)."""
    from acoharmony._registry import SchemaRegistry

    def fake_get_full_table_config(cls, name: str):
        if name == "empty_schema":
            return {}  # falsy – triggers the uncovered branch
        return {"name": name, "storage": {}}

    monkeypatch.setattr(
        SchemaRegistry,
        "list_schemas",
        classmethod(lambda cls: ["good_schema", "empty_schema"]),
    )
    monkeypatch.setattr(
        SchemaRegistry,
        "get_full_table_config",
        classmethod(fake_get_full_table_config),
    )

    schemas = _load_schemas()

    assert "good_schema" in schemas
    assert "empty_schema" not in schemas
    assert len(schemas) == 1


@pytest.mark.unit
def test_unpack_bronze_zips_skips_known_ext_in_iterdir(tmp_path: Path, monkeypatch) -> None:
    """unpack_bronze_zips skips known-extension files when scanning for extensionless zips (covers branch 206->205)."""

    # Setup directories
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()

    # Create a file with known extension (.csv) so line 206 condition is false -> loops back (206->205)
    (bronze_dir / "data.csv").write_text("a,b,c")
    # Create a non-zip extensionless file so is_zipfile is false -> loops back (207->205)
    (bronze_dir / "PLAINFILE").write_text("not a zip")
    # Create an extensionless zip so the loop iterates again and enters line 207 as true
    zip_no_ext = bronze_dir / "DATAFILE"
    with zipfile.ZipFile(zip_no_ext, "w") as zf:
        zf.writestr("inner.txt", "content")

    # Mock config
    config = MagicMock()
    config.storage.base_path = tmp_path
    config.storage.bronze_dir = "bronze"
    config.storage.archive_dir = "archive"

    monkeypatch.setattr("acoharmony._utils.unpack.get_config", lambda: config)

    result = unpack_bronze_zips()

    # The extensionless zip should be found; the .csv should be skipped
    assert result["found"] == 1
    assert result["processed"] == 1
    assert result["extracted"] == 1
