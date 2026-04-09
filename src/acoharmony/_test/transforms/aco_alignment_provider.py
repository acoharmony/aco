"""Tests for acoharmony._transforms._aco_alignment_provider module."""



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
        assert acoharmony._transforms._aco_alignment_provider is not None


class TestApplyTransformProvider:
    """Cover apply_transform lines 44-222."""

    @pytest.mark.unit
    def test_null_sources_adds_null_columns(self):
        """Cover lines 198-216: no last_ffs or participant_list → null columns."""
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._aco_alignment_provider import apply_transform

        df = pl.DataFrame({
            "current_mbi": ["M1", "M2"],
            "current_program": ["REACH", "MSSP"],
            "current_aco_id": ["ACO1", "ACO2"],
        }).lazy()

        catalog = MagicMock()
        catalog.scan_table.return_value = None  # All sources missing

        result = apply_transform(df, {}, catalog, MagicMock(), force=True).collect()

        assert "mssp_tin" in result.columns
        assert "reach_tin" in result.columns
        assert "aligned_provider_tin" in result.columns
        assert "_provider_attributed" in result.columns
        assert result.height == 2

    @pytest.mark.unit
    def test_idempotency_skip(self):
        """Cover lines 45-47: already processed → skip."""
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._aco_alignment_provider import apply_transform

        df = pl.DataFrame({
            "current_mbi": ["M1"],
            "_provider_attributed": [True],
        }).lazy()

        result = apply_transform(df, {}, MagicMock(), MagicMock(), force=False)
        assert result.collect().height == 1

    @pytest.mark.unit
    def test_full_attribution_with_sources(self):
        """Cover full path lines 56-197: all sources available."""
        from datetime import date
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._aco_alignment_provider import apply_transform

        df = pl.DataFrame({
            "current_mbi": ["M1", "M2"],
            "current_program": ["REACH", "MSSP"],
            "current_aco_id": ["ACO1", "ACO2"],
        }).lazy()

        last_ffs = pl.DataFrame({
            "bene_mbi": ["M1", "M2"],
            "last_ffs_tin": ["TIN1", "TIN2"],
            "last_ffs_npi": ["NPI1", "NPI2"],
            "last_ffs_date": [date(2024, 6, 1), date(2024, 7, 1)],
        }).lazy()

        participant_list = pl.DataFrame({
            "base_provider_tin": ["TIN1", "TIN2"],
            "individual_npi": ["NPI1", "NPI2"],
            "provider_type": ["MD", "DO"],
            "provider_class": ["Individual", "Individual"],
            "provider_legal_business_name": ["Group A", "Group B"],
            "individual_first_name": ["John", "Jane"],
            "individual_last_name": ["Doe", "Smith"],
        }).lazy()

        voluntary = pl.DataFrame({
            "current_mbi": ["M1"],
            "sva_provider_tin": ["RTIN1"],
            "sva_provider_npi": ["RNPI1"],
            "sva_provider_name": ["REACH Provider"],
            "sva_signature_count": [1],
            "pbvar_aligned": [False],
        }).lazy()

        catalog = MagicMock()
        catalog.scan_table.side_effect = lambda name: {
            "last_ffs_service": last_ffs,
            "participant_list": participant_list,
            "bar": None,
            "voluntary_alignment": voluntary,
        }.get(name)

        result = apply_transform(df, {}, catalog, MagicMock(), force=True).collect()

        assert "mssp_tin" in result.columns
        assert "mssp_provider_name" in result.columns
        assert "reach_tin" in result.columns
        assert "aligned_provider_tin" in result.columns
        assert "_provider_attributed" in result.columns
        assert result.height == 2

    @pytest.mark.unit
    def test_bar_plus_voluntary_merge(self):
        """Cover lines 128-135, 154: BAR + voluntary paths."""
        from datetime import date
        from unittest.mock import MagicMock
        import polars as pl
        from acoharmony._transforms._aco_alignment_provider import apply_transform

        df = pl.DataFrame({"current_mbi": ["M1"], "current_program": ["REACH"], "current_aco_id": ["ACO1"]}).lazy()
        last_ffs = pl.DataFrame({"bene_mbi": ["M1"], "last_ffs_tin": ["T1"], "last_ffs_npi": ["N1"], "last_ffs_date": [date(2024, 6, 1)]}).lazy()
        plist = pl.DataFrame({"base_provider_tin": ["T1"], "individual_npi": ["N1"], "provider_type": ["MD"], "provider_class": ["I"], "provider_legal_business_name": ["G"], "individual_first_name": ["J"], "individual_last_name": ["D"]}).lazy()
        bar = pl.DataFrame({"bene_mbi": ["M1"], "voluntary_alignment_type": ["SVA"], "claims_based_flag": [None]}).lazy()
        vol = pl.DataFrame({"current_mbi": ["M1"], "sva_provider_tin": ["RT"], "sva_provider_npi": ["RN"], "sva_provider_name": ["RP"], "sva_signature_count": [1], "pbvar_aligned": [False]}).lazy()
        catalog = MagicMock()
        catalog.scan_table.side_effect = lambda n: {"last_ffs_service": last_ffs, "participant_list": plist, "bar": bar, "voluntary_alignment": vol}.get(n)
        result = apply_transform(df, {}, catalog, MagicMock(), force=True).collect()
        assert "reach_attribution_type" in result.columns

    @pytest.mark.unit
    def test_no_reach_sources(self):
        """Cover lines 168-170: no BAR/voluntary → null REACH columns."""
        from datetime import date
        from unittest.mock import MagicMock
        import polars as pl
        from acoharmony._transforms._aco_alignment_provider import apply_transform

        df = pl.DataFrame({"current_mbi": ["M1"], "current_program": ["MSSP"], "current_aco_id": ["ACO1"]}).lazy()
        last_ffs = pl.DataFrame({"bene_mbi": ["M1"], "last_ffs_tin": ["T1"], "last_ffs_npi": ["N1"], "last_ffs_date": [date(2024, 6, 1)]}).lazy()
        plist = pl.DataFrame({"base_provider_tin": ["T1"], "individual_npi": ["N1"], "provider_type": ["MD"], "provider_class": ["I"], "provider_legal_business_name": ["G"], "individual_first_name": ["J"], "individual_last_name": ["D"]}).lazy()
        catalog = MagicMock()
        catalog.scan_table.side_effect = lambda n: {"last_ffs_service": last_ffs, "participant_list": plist, "bar": None, "voluntary_alignment": None}.get(n)
        result = apply_transform(df, {}, catalog, MagicMock(), force=True).collect()
        assert result["reach_tin"][0] is None
