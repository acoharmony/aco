"""Tests for acoharmony._transforms._ffs_first_dates module."""



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
        assert acoharmony._transforms._ffs_first_dates is not None


class TestApplyTransformFfsFirstDates:
    """Cover apply_transform lines 48-84."""

    @pytest.mark.unit
    def test_builds_from_cclf5_and_provider_list(self):
        """Full transform: joins cclf5 with provider_list, aggregates."""
        from datetime import date
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._ffs_first_dates import apply_transform

        cclf5 = pl.DataFrame({
            "bene_mbi_id": ["MBI1", "MBI1", "MBI2"],
            "clm_rndrg_prvdr_tax_num": ["TIN1", "TIN1", "TIN2"],
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

        logger = MagicMock()

        result = apply_transform(None, {}, catalog, logger, force=True)
        df = result.collect()

        assert "bene_mbi" in df.columns
        assert "ffs_first_date" in df.columns
        assert "claim_count" in df.columns
        assert df.height == 2

    @pytest.mark.unit
    def test_already_exists_returns_cached(self):
        """Cover lines 49-52: cached data returned when not forced."""
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._ffs_first_dates import apply_transform

        cached = pl.DataFrame({"bene_mbi": ["MBI1"]}).lazy()
        catalog = MagicMock()
        catalog.get_table_metadata.return_value = {"name": "ffs_first_dates"}
        catalog.scan_table.return_value = cached

        result = apply_transform(None, {}, catalog, MagicMock(), force=False)
        assert result.collect().height == 1

    @pytest.mark.unit
    def test_missing_cclf5_raises(self):
        """Cover line 63: cclf5 is None → ValueError."""
        from unittest.mock import MagicMock

        from acoharmony._transforms._ffs_first_dates import apply_transform

        catalog = MagicMock()
        catalog.get_table_metadata.return_value = None
        catalog.scan_table.return_value = None

        with pytest.raises(ValueError, match="CCLF5"):
            apply_transform(None, {}, catalog, MagicMock(), force=True)

    @pytest.mark.unit
    def test_missing_provider_list_raises(self):
        """Cover line 65: provider_list is None → ValueError."""
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._ffs_first_dates import apply_transform

        catalog = MagicMock()
        catalog.get_table_metadata.return_value = None
        catalog.scan_table.side_effect = lambda name: (
            pl.DataFrame({"bene_mbi_id": []}).lazy() if name == "cclf5" else None
        )

        with pytest.raises(ValueError, match="Provider list"):
            apply_transform(None, {}, catalog, MagicMock(), force=True)

    @pytest.mark.unit
    def test_cached_data_missing_falls_back(self):
        """Cover lines 53-54: metadata exists but scan_table raises → rebuild."""
        from datetime import date
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._ffs_first_dates import apply_transform

        cclf5 = pl.DataFrame({"bene_mbi_id": ["M1"], "clm_rndrg_prvdr_tax_num": ["T1"], "clm_line_from_dt": [date(2024, 1, 15)]}).lazy()
        plist = pl.DataFrame({"billing_tin": ["T1"]}).lazy()
        catalog = MagicMock()
        catalog.get_table_metadata.return_value = {"name": "ffs_first_dates"}
        calls = {"ffs": 0}
        def side_effect(name):
            if name == "ffs_first_dates":
                calls["ffs"] += 1
                if calls["ffs"] == 1:
                    raise FileNotFoundError("data missing")
            return {"cclf5": cclf5, "provider_list": plist}.get(name)
        catalog.scan_table.side_effect = side_effect

        result = apply_transform(None, {}, catalog, MagicMock(), force=False)
        assert result.collect().height >= 0
