# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for seed_manager module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import TYPE_CHECKING
from unittest.mock import patch, MagicMock

import pytest
import yaml

if TYPE_CHECKING:
    pass


class TestTuvaSeedManager:
    """Tests for TuvaSeedManager."""

    @pytest.mark.unit
    def test_tuvaseedmanager_initialization(self, tmp_path) -> None:
        """TuvaSeedManager can be initialized with explicit paths."""
        from acoharmony._tuva.seed_manager import TuvaSeedManager

        tuva_dir = tmp_path / 'tuva'
        tuva_dir.mkdir()
        duckdb_path = tmp_path / 'test.duckdb'
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path

        mgr = TuvaSeedManager(
            tuva_project_dir=tuva_dir,
            duckdb_path=duckdb_path,
            storage=mock_storage,
        )
        assert mgr.tuva_project_dir == tuva_dir
        assert mgr.duckdb_path == duckdb_path

    @pytest.mark.unit
    def test_tuvaseedmanager_basic_functionality(self, tmp_path) -> None:
        """TuvaSeedManager basic functionality -- parse_seed_definitions with empty project."""
        from acoharmony._tuva.seed_manager import TuvaSeedManager

        tuva_dir = tmp_path / 'tuva'
        tuva_dir.mkdir()
        # Create a minimal dbt_project.yml with no seeds
        dbt_project = {
            'vars': {'custom_bucket_name': 'test-bucket'},
            'seeds': {'the_tuva_project': {}},
        }
        (tuva_dir / 'dbt_project.yml').write_text(yaml.dump(dbt_project))

        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path

        mgr = TuvaSeedManager(
            tuva_project_dir=tuva_dir,
            duckdb_path=tmp_path / 'test.duckdb',
            storage=mock_storage,
        )
        result = mgr.parse_seed_definitions()
        assert isinstance(result, list)
        assert len(result) == 0


class TestParseSeedDefinitionsBranches:
    """Cover seed_manager.py branches 168->151 and 201->200."""

    @pytest.mark.unit
    def test_missing_s3_path_skips_seed(self, tmp_path):
        """Branch 168->151: when s3_path or csv_filename is missing, skip seed entry."""
        from acoharmony._tuva.seed_manager import TuvaSeedManager

        # Create a dbt_project.yml where the post-hook has no path starting with /
        dbt_project = {
            "vars": {"custom_bucket_name": "test-bucket"},
            "seeds": {
                "the_tuva_project": {
                    "test_schema": {
                        "test__seed_table": {
                            "+post-hook": "{{ load_seed('no_slash_path', 'data.csv') }}"
                        }
                    }
                }
            },
        }
        dbt_file = tmp_path / "dbt_project.yml"
        dbt_file.write_text(yaml.dump(dbt_project))

        mgr = MagicMock(spec=TuvaSeedManager)
        mgr.tuva_project_dir = tmp_path
        mgr.log = MagicMock()

        # Call actual method
        result = TuvaSeedManager.parse_seed_definitions(mgr)
        # s3_path is None because no part starts with '/', so seed is skipped
        assert len(result) == 0

    @pytest.mark.unit
    def test_schema_config_not_dict_skips(self, tmp_path):
        """Branch 201->200: when schema_config is not a dict, skip it."""
        from acoharmony._tuva.seed_manager import TuvaSeedManager

        dbt_project = {
            "vars": {"custom_bucket_name": "test-bucket"},
            "seeds": {
                "the_tuva_project": {
                    "some_string_value": "not_a_dict",
                    "another_non_dict": 42,
                }
            },
        }
        dbt_file = tmp_path / "dbt_project.yml"
        dbt_file.write_text(yaml.dump(dbt_project))

        mgr = MagicMock(spec=TuvaSeedManager)
        mgr.tuva_project_dir = tmp_path
        mgr.log = MagicMock()

        result = TuvaSeedManager.parse_seed_definitions(mgr)
        assert len(result) == 0
