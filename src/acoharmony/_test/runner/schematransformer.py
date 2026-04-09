# © 2025 HarmonyCares
"""Tests for acoharmony/_runner/_schema_transformer.py."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestSchemaTransformer:
    """Test suite for _schema_transformer."""

    @pytest.mark.unit
    def test_transform_schema(self) -> None:
        """Test transform_schema returns error for unknown schema."""
        from unittest.mock import MagicMock

        from acoharmony._runner._schema_transformer import SchemaTransformer

        st = SchemaTransformer.__new__(SchemaTransformer)
        st.storage_config = MagicMock()
        st.catalog = MagicMock()
        st.catalog.get_table_metadata.return_value = None
        st.logger = MagicMock()

        import polars as pl

        from acoharmony._exceptions import ValidationError

        tracker = MagicMock()
        tracker.state.metadata = {}
        df = pl.DataFrame({"a": [1]}).lazy()
        # The @runner_method decorator includes @validate_schema which raises ValidationError
        with pytest.raises(ValidationError, match="not found"):
            st.transform_schema("nonexistent_xyz", df, tracker, no_tracking=True)

    @pytest.mark.unit
    def test_schematransformer_init(self) -> None:
        """Test SchemaTransformer initialization."""
        from unittest.mock import MagicMock

        from acoharmony._runner._schema_transformer import SchemaTransformer

        storage = MagicMock()
        catalog = MagicMock()
        logger = MagicMock()

        st = SchemaTransformer(storage, catalog, logger)
        assert st.storage_config is storage
        assert st.catalog is catalog
        assert st.logger is logger

