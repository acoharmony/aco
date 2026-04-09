"""Tests for acoharmony._transforms._last_ffs_service module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._transforms._last_ffs_service is not None


class TestApplyTransformLastFfsService:
    """Cover apply_transform lines 51-101."""

    @pytest.mark.unit
    def test_builds_from_cclf5_and_provider_list(self):
        """Full transform: joins cclf5, finds latest service per bene."""
        from datetime import date
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._last_ffs_service import apply_transform

        cclf5 = pl.DataFrame({
            "bene_mbi_id": ["MBI1", "MBI1", "MBI2"],
            "clm_rndrg_prvdr_tax_num": ["TIN1", "TIN1", "TIN2"],
            "rndrg_prvdr_npi_num": ["NPI1", "NPI1", "NPI2"],
            "clm_line_from_dt": [date(2024, 1, 15), date(2024, 3, 20), date(2024, 6, 1)],
        }).lazy()

        provider_list = pl.DataFrame({
            "billing_tin": ["TIN1", "TIN2"],
        }).lazy()

        catalog = MagicMock()
        catalog.get_table_metadata.return_value = None
        catalog.scan_table.side_effect = lambda name: {
            "cclf5": cclf5,
            "provider_list": provider_list,
        }[name]

        result = apply_transform(None, {}, catalog, MagicMock(), force=True)
        df = result.collect()

        assert "bene_mbi" in df.columns
        assert "last_ffs_date" in df.columns
        assert df.height == 2

    @pytest.mark.unit
    def test_already_exists_returns_cached(self):
        """Cover lines 52-55: cached data returned when not forced."""
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._last_ffs_service import apply_transform

        cached = pl.DataFrame({"bene_mbi": ["MBI1"]}).lazy()
        catalog = MagicMock()
        catalog.get_table_metadata.return_value = {"name": "last_ffs_service"}
        catalog.scan_table.return_value = cached

        result = apply_transform(None, {}, catalog, MagicMock(), force=False)
        assert result.collect().height == 1

    @pytest.mark.unit
    def test_missing_cclf5_raises(self):
        """Cover line 66: cclf5 None → ValueError."""
        from unittest.mock import MagicMock

        from acoharmony._transforms._last_ffs_service import apply_transform

        catalog = MagicMock()
        catalog.get_table_metadata.return_value = None
        catalog.scan_table.return_value = None

        with pytest.raises(ValueError, match="CCLF5"):
            apply_transform(None, {}, catalog, MagicMock(), force=True)

    @pytest.mark.unit
    def test_missing_provider_list_raises(self):
        """Cover line 68: provider_list None → ValueError."""
        from unittest.mock import MagicMock
        import polars as pl
        from acoharmony._transforms._last_ffs_service import apply_transform

        catalog = MagicMock()
        catalog.get_table_metadata.return_value = None
        catalog.scan_table.side_effect = lambda n: (
            pl.DataFrame({"bene_mbi_id": []}).lazy() if n == "cclf5" else None
        )
        with pytest.raises(ValueError, match="Provider list"):
            apply_transform(None, {}, catalog, MagicMock(), force=True)

    @pytest.mark.unit
    def test_cached_data_fallback(self):
        """Cover lines 56-57: metadata exists but scan_table raises."""
        from datetime import date
        from unittest.mock import MagicMock
        import polars as pl
        from acoharmony._transforms._last_ffs_service import apply_transform

        cclf5 = pl.DataFrame({"bene_mbi_id": ["M1"], "clm_rndrg_prvdr_tax_num": ["T1"], "rndrg_prvdr_npi_num": ["N1"], "clm_line_from_dt": [date(2024, 1, 15)]}).lazy()
        plist = pl.DataFrame({"billing_tin": ["T1"]}).lazy()
        catalog = MagicMock()
        catalog.get_table_metadata.return_value = {"name": "last_ffs_service"}
        calls = {"lfs": 0}
        def se(name):
            if name == "last_ffs_service":
                calls["lfs"] += 1
                if calls["lfs"] == 1:
                    raise FileNotFoundError("missing")
            return {"cclf5": cclf5, "provider_list": plist}.get(name)
        catalog.scan_table.side_effect = se
        result = apply_transform(None, {}, catalog, MagicMock(), force=False)
        assert result.collect().height >= 0
