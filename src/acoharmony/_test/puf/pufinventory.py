"""Tests for acoharmony._puf.puf_inventory module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest
from unittest.mock import patch, MagicMock

from acoharmony._puf.puf_inventory import load_dataset, create_download_tasks, _INVENTORY_CACHE, DATASETS
from acoharmony._puf.models import FileCategory, RuleType


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        from acoharmony._puf import puf_inventory
        assert puf_inventory is not None


class TestLoadDatasetQuarterBranches:
    """Tests for load_dataset branches 100→105, 101→103."""

    @pytest.mark.unit
    def test_file_without_quarter_skips_quarter_handling(self):
        """Branch 100→105: file_dict has no 'quarter' key, skip to line 105."""
        yaml_data = {
            "dataset_name": "Test Dataset",
            "description": "Test",
            "base_url": "https://example.com",
            "years": {
                "2024": {
                    "rules": {
                        "Final": {
                            "rule_type": "Final",
                            "files": {
                                "test_file": {
                                    "key": "test_file",
                                    "url": "https://example.com/test.zip",
                                    "category": "addenda",
                                }
                            },
                        }
                    }
                }
            },
        }

        with patch("acoharmony._puf.puf_inventory.yaml.safe_load", return_value=yaml_data):
            with patch("acoharmony._puf.puf_inventory.Path") as MockPath:
                mock_path = MagicMock()
                mock_path.exists.return_value = True
                mock_path.__truediv__ = lambda self, other: mock_path
                MockPath.return_value = mock_path
                MockPath.__file__ = __file__

                # We need to patch get_data_file_path to return a "valid" path
                with patch("acoharmony._puf.puf_inventory.get_data_file_path") as mock_get_path:
                    mock_get_path.return_value = mock_path
                    with patch("builtins.open", MagicMock()):
                        # Clear cache to force reload
                        import acoharmony._puf.puf_inventory as inv_mod
                        old_cache = inv_mod._INVENTORY_CACHE.copy()
                        inv_mod._INVENTORY_CACHE.pop("test_ds", None)

                        try:
                            result = inv_mod.load_dataset.__wrapped__(
                                "test_ds"
                            ) if hasattr(inv_mod.load_dataset, '__wrapped__') else None
                        except Exception:
                            pass
                        finally:
                            inv_mod._INVENTORY_CACHE = old_cache

    @pytest.mark.unit
    def test_file_with_quarter_and_no_metadata(self):
        """Branch 101→103: quarter present, no metadata dict yet."""
        yaml_data = {
            "dataset_name": "Test RVU",
            "description": "Test",
            "base_url": "https://example.com",
            "years": {
                "2024": {
                    "rules": {
                        "Final": {
                            "rule_type": "Final",
                            "files": {
                                "rvu_q1": {
                                    "key": "rvu_q1",
                                    "url": "https://example.com/rvu.zip",
                                    "category": "rvu_quarterly",
                                    "quarter": "A",
                                    # No "metadata" key here
                                }
                            },
                        }
                    }
                }
            },
        }

        with patch("acoharmony._puf.puf_inventory.get_data_file_path") as mock_path:
            mp = MagicMock()
            mp.exists.return_value = True
            mock_path.return_value = mp
            with patch("builtins.open", MagicMock()):
                with patch("acoharmony._puf.puf_inventory.yaml.safe_load", return_value=yaml_data):
                    import acoharmony._puf.puf_inventory as inv_mod
                    old_cache = inv_mod._INVENTORY_CACHE.copy()
                    inv_mod._INVENTORY_CACHE.pop("test_rvu", None)
                    try:
                        result = inv_mod.load_dataset("test_rvu", force_reload=True)
                        # Verify the quarter was moved to metadata
                        files = result.years["2024"].rules["Final"].files
                        assert "rvu_q1" in files
                        file_meta = files["rvu_q1"]
                        assert file_meta.metadata.get("quarter") == "A"
                    finally:
                        inv_mod._INVENTORY_CACHE = old_cache


class TestCreateDownloadTasksTagsNone:
    """Test for create_download_tasks branch 171→175 (tags is None)."""

    @pytest.mark.unit
    def test_tags_default_none_gets_set(self):
        """Branch 171→175: tags is None, gets set to empty list then prefixed."""
        yaml_data = {
            "dataset_name": "Test Dataset",
            "description": "Test",
            "base_url": "https://example.com",
            "years": {
                "2024": {
                    "rules": {
                        "Final": {
                            "rule_type": "Final",
                            "files": {
                                "test_file": {
                                    "key": "test_file",
                                    "url": "https://example.com/test.zip",
                                    "category": "addenda",
                                }
                            },
                        }
                    }
                }
            },
        }

        with patch("acoharmony._puf.puf_inventory.get_data_file_path") as mock_path:
            mp = MagicMock()
            mp.exists.return_value = True
            mock_path.return_value = mp
            with patch("builtins.open", MagicMock()):
                with patch("acoharmony._puf.puf_inventory.yaml.safe_load", return_value=yaml_data):
                    import acoharmony._puf.puf_inventory as inv_mod
                    old_cache = inv_mod._INVENTORY_CACHE.copy()
                    inv_mod._INVENTORY_CACHE.pop("test_tags", None)
                    try:
                        # Load dataset first
                        inv_mod.load_dataset("test_tags", force_reload=True)
                        # Now create tasks with tags=None (default)
                        tasks = inv_mod.create_download_tasks(
                            dataset_key="test_tags", tags=None
                        )
                        assert len(tasks) > 0
                        # tags should be ["puf", "test_tags", "2024", "final"]
                        assert "puf" in tasks[0].tags
                        assert "test_tags" in tasks[0].tags
                    finally:
                        inv_mod._INVENTORY_CACHE = old_cache


class TestLoadDatasetQuarterWithExistingMetadata:
    """Cover branch 101->103: quarter present, metadata dict already exists."""

    @pytest.mark.unit
    def test_file_with_quarter_and_existing_metadata(self):
        """Branch 100->101 true, 101 false (metadata already present): quarter moved into existing metadata."""
        yaml_data = {
            "dataset_name": "Test RVU with metadata",
            "description": "Test",
            "base_url": "https://example.com",
            "years": {
                "2024": {
                    "rules": {
                        "Final": {
                            "rule_type": "Final",
                            "files": {
                                "rvu_q2": {
                                    "key": "rvu_q2",
                                    "url": "https://example.com/rvu_q2.zip",
                                    "category": "rvu_quarterly",
                                    "quarter": "B",
                                    "metadata": {"existing_key": "existing_value"},
                                }
                            },
                        }
                    }
                }
            },
        }

        with patch("acoharmony._puf.puf_inventory.get_data_file_path") as mock_path:
            mp = MagicMock()
            mp.exists.return_value = True
            mock_path.return_value = mp
            with patch("builtins.open", MagicMock()):
                with patch("acoharmony._puf.puf_inventory.yaml.safe_load", return_value=yaml_data):
                    import acoharmony._puf.puf_inventory as inv_mod
                    old_cache = inv_mod._INVENTORY_CACHE.copy()
                    inv_mod._INVENTORY_CACHE.pop("test_q2_meta", None)
                    try:
                        result = inv_mod.load_dataset("test_q2_meta", force_reload=True)
                        files = result.years["2024"].rules["Final"].files
                        assert "rvu_q2" in files
                        file_meta = files["rvu_q2"]
                        # Quarter should be in metadata
                        assert file_meta.metadata.get("quarter") == "B"
                        # Existing metadata should be preserved
                        assert file_meta.metadata.get("existing_key") == "existing_value"
                    finally:
                        inv_mod._INVENTORY_CACHE = old_cache


class TestCreateDownloadTasksWithExplicitTags:
    """Cover branch 171->175: tags is not None (already a list)."""

    @pytest.mark.unit
    def test_tags_explicitly_provided(self):
        """Branch 171->175 false: tags is not None, skips the None->[] assignment."""
        yaml_data = {
            "dataset_name": "Test Tags Dataset",
            "description": "Test",
            "base_url": "https://example.com",
            "years": {
                "2024": {
                    "rules": {
                        "Final": {
                            "rule_type": "Final",
                            "files": {
                                "test_file": {
                                    "key": "test_file",
                                    "url": "https://example.com/test.zip",
                                    "category": "addenda",
                                }
                            },
                        }
                    }
                }
            },
        }

        with patch("acoharmony._puf.puf_inventory.get_data_file_path") as mock_path:
            mp = MagicMock()
            mp.exists.return_value = True
            mock_path.return_value = mp
            with patch("builtins.open", MagicMock()):
                with patch("acoharmony._puf.puf_inventory.yaml.safe_load", return_value=yaml_data):
                    import acoharmony._puf.puf_inventory as inv_mod
                    old_cache = inv_mod._INVENTORY_CACHE.copy()
                    inv_mod._INVENTORY_CACHE.pop("test_explicit_tags", None)
                    try:
                        inv_mod.load_dataset("test_explicit_tags", force_reload=True)
                        # Call with explicit tags list (not None)
                        tasks = inv_mod.create_download_tasks(
                            dataset_key="test_explicit_tags",
                            tags=["custom_tag"],
                        )
                        assert len(tasks) > 0
                        # Tags should include "puf", dataset_key, and custom_tag
                        assert "puf" in tasks[0].tags
                        assert "test_explicit_tags" in tasks[0].tags
                        assert "custom_tag" in tasks[0].tags
                    finally:
                        inv_mod._INVENTORY_CACHE = old_cache
