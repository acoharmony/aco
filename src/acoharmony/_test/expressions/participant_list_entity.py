"""Tests for acoharmony._expressions._participant_list_entity."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import polars as pl
import pytest

from acoharmony._expressions._participant_list_entity import (
    build_fill_entity_columns_exprs,
    build_performance_year_from_file_date_expr,
)


class TestBuildFillEntityColumnsExprs:
    """Tests for build_fill_entity_columns_exprs()."""

    @pytest.mark.unit
    def test_fills_only_null_entity_values(self):
        """Existing values pass through; only nulls are coalesced to the identity row."""
        identity = {
            "apm_id": "TEST1",
            "tin": "999999999",
            "legal_business_name": "Test ACO LLC",
        }
        df = pl.DataFrame(
            {
                "entity_id": [None, "EXISTING"],
                "entity_tin": [None, "12"],
                "entity_legal_business_name": [None, "Existing ACO"],
            }
        ).with_columns(build_fill_entity_columns_exprs(identity=identity))

        assert df["entity_id"][0] == "TEST1"
        assert df["entity_tin"][0] == "999999999"
        assert df["entity_legal_business_name"][0] == "Test ACO LLC"
        # Pre-populated row unchanged.
        assert df["entity_id"][1] == "EXISTING"
        assert df["entity_tin"][1] == "12"
        assert df["entity_legal_business_name"][1] == "Existing ACO"

    @pytest.mark.unit
    def test_loads_identity_lazily_when_none(self, monkeypatch):
        """Calling without identity triggers get_aco_identity()."""
        import acoharmony._expressions._participant_list_entity as mod

        called = {"hit": False}

        def fake_loader():
            called["hit"] = True
            return {
                "apm_id": "FROMLOADER",
                "tin": "1",
                "legal_business_name": "Loaded",
            }

        monkeypatch.setattr(mod, "get_aco_identity", fake_loader)
        df = pl.DataFrame(
            {"entity_id": [None], "entity_tin": [None], "entity_legal_business_name": [None]}
        ).with_columns(build_fill_entity_columns_exprs())
        assert called["hit"] is True
        assert df["entity_id"][0] == "FROMLOADER"


class TestBuildPerformanceYearFromFileDateExpr:
    """Tests for build_performance_year_from_file_date_expr()."""

    @pytest.mark.unit
    def test_derives_py_from_iso_file_date_when_null(self):
        df = pl.DataFrame(
            {"performance_year": [None], "file_date": ["2026-04-27"]}
        ).with_columns(build_performance_year_from_file_date_expr())
        assert df["performance_year"][0] == "PY2026"

    @pytest.mark.unit
    def test_preserves_existing_performance_year(self):
        df = pl.DataFrame(
            {"performance_year": ["PY2024"], "file_date": ["2030-01-01"]}
        ).with_columns(build_performance_year_from_file_date_expr())
        assert df["performance_year"][0] == "PY2024"
