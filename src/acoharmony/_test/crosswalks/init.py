"""Tests for acoharmony._crosswalks module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from acoharmony._crosswalks import (
    _get_crosswalks_dir,
    get_mapping,
    get_mappings,
    get_source_info,
    get_target_info,
    list_crosswalks,
    load_crosswalk,
)


class TestGetCrosswalksDir:
    """Tests for _get_crosswalks_dir."""


    @pytest.mark.unit
    def test_returns_path_object(self):
        """Returns a Path object pointing to crosswalks directory."""


        result = _get_crosswalks_dir()
        assert isinstance(result, Path)
        assert result.is_dir()


class TestListCrosswalks:
    """Tests for list_crosswalks."""


    @pytest.mark.unit
    def test_lists_yaml_files(self):
        """Lists available crosswalk names."""


        result = list_crosswalks()
        assert isinstance(result, list)
        # bar_to_crr.yaml exists
        assert "bar_to_crr" in result

    @pytest.mark.unit
    def test_excludes_underscored_files(self, tmp_path):
        """Files starting with underscore are excluded."""
        # Create yaml files in a mock dir


        (tmp_path / "good.yaml").write_text("crosswalk: {}")
        (tmp_path / "_private.yaml").write_text("crosswalk: {}")

        with patch(
            "acoharmony._crosswalks._get_crosswalks_dir",
            return_value=tmp_path,
        ):
            result = list_crosswalks()

        assert "good" in result
        assert "_private" not in result


class TestLoadCrosswalk:
    """Tests for load_crosswalk."""


    @pytest.mark.unit
    def test_load_existing_crosswalk(self):
        """Loads an existing crosswalk YAML file."""


        result = load_crosswalk("bar_to_crr")
        assert "crosswalk" in result
        assert result["crosswalk"]["name"] == "bar_to_crr"

    @pytest.mark.unit
    def test_load_nonexistent_raises_file_not_found(self):
        """Raises FileNotFoundError for missing crosswalk."""


        with pytest.raises(FileNotFoundError, match="Crosswalk 'nonexistent' not found"):
            load_crosswalk("nonexistent")

    @pytest.mark.unit
    def test_load_from_mock_yaml(self, tmp_path):
        """Loads crosswalk from mock YAML file."""


        yaml_content = {
            "crosswalk": {
                "name": "test_xwalk",
                "source": {"schema": "src"},
                "target": {"schema": "tgt", "table": "t1"},
                "mappings": [
                    {
                        "source_column": "col_a",
                        "target_column": "col_b",
                        "data_type": "string",
                    }
                ],
            }
        }
        yaml_file = tmp_path / "test_xwalk.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        with patch(
            "acoharmony._crosswalks._get_crosswalks_dir",
            return_value=tmp_path,
        ):
            result = load_crosswalk("test_xwalk")

        assert result["crosswalk"]["name"] == "test_xwalk"
        assert result["crosswalk"]["target"]["table"] == "t1"


class TestGetMapping:
    """Tests for get_mapping."""


    @pytest.mark.unit
    def test_get_existing_mapping(self, tmp_path):
        """Returns mapping dict for existing source column."""


        yaml_content = {
            "crosswalk": {
                "mappings": [
                    {"source_column": "col_a", "target_column": "col_b"},
                    {"source_column": "col_c", "target_column": "col_d"},
                ]
            }
        }
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        with patch(
            "acoharmony._crosswalks._get_crosswalks_dir",
            return_value=tmp_path,
        ):
            result = get_mapping("test", "col_a")

        assert result is not None
        assert result["target_column"] == "col_b"

    @pytest.mark.unit
    def test_get_nonexistent_mapping_returns_none(self, tmp_path):
        """Returns None when source column not found."""


        yaml_content = {
            "crosswalk": {
                "mappings": [
                    {"source_column": "col_a", "target_column": "col_b"},
                ]
            }
        }
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        with patch(
            "acoharmony._crosswalks._get_crosswalks_dir",
            return_value=tmp_path,
        ):
            result = get_mapping("test", "nonexistent")

        assert result is None


class TestGetMappings:
    """Tests for get_mappings."""


    @pytest.mark.unit
    def test_returns_all_mappings(self, tmp_path):
        """Returns list of all mapping dicts."""


        yaml_content = {
            "crosswalk": {
                "mappings": [
                    {"source_column": "a", "target_column": "b"},
                    {"source_column": "c", "target_column": "d"},
                ]
            }
        }
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        with patch(
            "acoharmony._crosswalks._get_crosswalks_dir",
            return_value=tmp_path,
        ):
            result = get_mappings("test")

        assert len(result) == 2

    @pytest.mark.unit
    def test_returns_empty_when_no_mappings(self, tmp_path):
        """Returns empty list when no mappings key."""


        yaml_content = {"crosswalk": {}}
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        with patch(
            "acoharmony._crosswalks._get_crosswalks_dir",
            return_value=tmp_path,
        ):
            result = get_mappings("test")

        assert result == []


class TestGetTargetInfo:
    """Tests for get_target_info."""


    @pytest.mark.unit
    def test_returns_target_dict(self, tmp_path):
        """Returns target schema information."""


        yaml_content = {
            "crosswalk": {
                "target": {"catalog": "cat", "schema": "sch", "table": "tbl"},
            }
        }
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        with patch(
            "acoharmony._crosswalks._get_crosswalks_dir",
            return_value=tmp_path,
        ):
            result = get_target_info("test")

        assert result["catalog"] == "cat"
        assert result["table"] == "tbl"


class TestGetSourceInfo:
    """Tests for get_source_info."""


    @pytest.mark.unit
    def test_returns_source_dict(self, tmp_path):
        """Returns source schema information."""


        yaml_content = {
            "crosswalk": {
                "source": {"schema": "src", "file_format": "csv"},
            }
        }
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        with patch(
            "acoharmony._crosswalks._get_crosswalks_dir",
            return_value=tmp_path,
        ):
            result = get_source_info("test")

        assert result["schema"] == "src"
        assert result["file_format"] == "csv"
