"""Tests for acoharmony._transforms._pfs_rates module."""



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
        assert acoharmony._transforms._pfs_rates is not None


class TestCalculatePfsRates:
    """Cover calculate_pfs_rates lines 223-487."""

    @pytest.mark.unit
    def test_full_pfs_calculation(self):
        """Full PFS rate calculation with mocked catalog."""
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates

        office_zip = pl.DataFrame({
            "zip_code": ["60601", "10001"],
            "office_name": ["Chicago Office", "NYC Office"],
            "state": ["IL", "NY"],
            "market": ["Chicago", "NYC"],
            "region_name": ["Midwest", "Northeast"],
            "office_distance": [None, None],
        }).lazy()

        cms_geo = pl.DataFrame({
            "year_quarter": ["2024Q1", "2024Q1"],
            "geo_zip_5": ["60601", "10001"],
            "geo_state_cd": ["IL", "NY"],
            "carrier": ["00952", "15999"],
            "locality": ["16", "00"],
        }).lazy()

        gpci = pl.DataFrame({
            "geo_locality_state_cd": ["IL", "NY"],
            "geo_locality_num": ["16", "00"],
            "geo_locality_name": ["Chicago Metro", "Manhattan"],
            "pw_gpci": [1.0, 1.05],
            "pe_gpci": [1.02, 1.25],
            "pe_mp_gpci": [0.95, 1.10],
        }).lazy()

        pprvu = pl.DataFrame({
            "hcpcs": ["99213", "99214"],
            "description": ["Office Visit E&M Lvl 3", "Office Visit E&M Lvl 4"],
            "work_rvu": [1.30, 1.92],
            "nf_pe_rvu": [1.59, 2.11],
            "mp_rvu": [0.10, 0.14],
            "conversion_factor": [33.2875, 33.2875],
        }).lazy()

        catalog = MagicMock()
        catalog.scan_table.side_effect = lambda name: {
            "office_zip": office_zip,
            "cms_geo_zips": cms_geo,
            "gpci_inputs": gpci,
            "pprvu_inputs": pprvu,
        }.get(name)

        config = PFSRateCalcConfig(
            hcpcs_codes=["99213", "99214"],
            year=2024,
            conversion_factor=33.2875,
        )

        result = calculate_pfs_rates(None, {}, catalog, MagicMock(), config=config, force=True)
        df = result.collect()

        assert "payment_rate" in df.columns
        assert "hcpcs_code" in df.columns
        assert "office_name" in df.columns
        assert df.height == 4  # 2 offices × 2 HCPCS codes
        assert all(df["payment_rate"] > 0)

    @pytest.mark.unit
    def test_no_hcpcs_codes_raises(self):
        """Cover line 238: no HCPCS codes → ValueError."""
        from unittest.mock import MagicMock

        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates

        config = PFSRateCalcConfig(hcpcs_codes=[], use_home_visit_codes=False)

        with pytest.raises(ValueError, match="Must specify HCPCS"):
            calculate_pfs_rates(None, {}, MagicMock(), MagicMock(), config=config)

    @pytest.mark.unit
    def test_missing_office_zip_raises(self):
        """Cover line 244: missing office_zip → ValueError."""
        from unittest.mock import MagicMock

        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates

        catalog = MagicMock()
        catalog.scan_table.return_value = None
        config = PFSRateCalcConfig(hcpcs_codes=["99213"], year=2024)

        with pytest.raises(ValueError, match="office_zip"):
            calculate_pfs_rates(None, {}, catalog, MagicMock(), config=config)

    @pytest.mark.unit
    def test_pfs_with_comparison_branch(self):
        """Cover PFS include_comparison=True branch."""
        from unittest.mock import MagicMock
        import polars as pl

        office_zip = pl.DataFrame({"zip_code": ["60601"], "office_name": ["Chicago"], "state": ["IL"], "market": ["Chicago"], "region_name": ["Midwest"], "office_distance": [None]}).lazy()
        cms_geo = pl.DataFrame({"year_quarter": ["2024Q1"], "geo_zip_5": ["60601"], "geo_state_cd": ["IL"], "carrier": ["00952"], "locality": ["16"]}).lazy()
        gpci = pl.DataFrame({"geo_locality_state_cd": ["IL"], "geo_locality_num": ["16"], "geo_locality_name": ["Chicago"], "pw_gpci": [1.0], "pe_gpci": [1.0], "pe_mp_gpci": [1.0]}).lazy()
        pprvu = pl.DataFrame({"hcpcs": ["99213"], "description": ["E&M"], "work_rvu": [1.3], "nf_pe_rvu": [1.59], "mp_rvu": [0.1], "conversion_factor": [33.29]}).lazy()

        catalog = MagicMock()
        catalog.scan_table.side_effect = lambda n: {"office_zip": office_zip, "cms_geo_zips": cms_geo, "gpci_inputs": gpci, "pprvu_inputs": pprvu}.get(n)

        config = PFSRateCalcConfig(hcpcs_codes=["99213"], year=2024, conversion_factor=33.29, include_comparison=True)
        result = calculate_pfs_rates(None, {}, catalog, MagicMock(), config=config, force=True)
        df = result.collect()
        assert "prior_year" in df.columns


class TestPfsRatesIncludeComparison:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_pfs_rates_include_comparison(self, tmp_path):
        """450->464: config.include_comparison is True."""
        from unittest.mock import MagicMock
        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates
        office_zip = pl.DataFrame({"zip_code": ["60601"], "office_name": ["Chi"], "state": ["IL"], "market": ["Chi"], "region_name": ["MW"], "office_distance": [None]}).lazy()
        cms_geo = pl.DataFrame({"year_quarter": ["2024Q1"], "geo_zip_5": ["60601"], "geo_state_cd": ["IL"], "carrier": ["009"], "locality": ["16"]}).lazy()
        gpci = pl.DataFrame({"geo_locality_state_cd": ["IL"], "geo_locality_num": ["16"], "geo_locality_name": ["Chi"], "pw_gpci": [1.0], "pe_gpci": [1.0], "pe_mp_gpci": [1.0]}).lazy()
        pprvu = pl.DataFrame({"hcpcs": ["99213"], "description": ["EM"], "work_rvu": [1.3], "nf_pe_rvu": [1.59], "mp_rvu": [0.1], "conversion_factor": [33.29]}).lazy()
        catalog = MagicMock()
        catalog.scan_table.side_effect = lambda n: {"office_zip": office_zip, "cms_geo_zips": cms_geo, "gpci_inputs": gpci, "pprvu_inputs": pprvu}.get(n)
        config = PFSRateCalcConfig(hcpcs_codes=["99213"], year=2024, conversion_factor=33.29, include_comparison=True)
        result = calculate_pfs_rates(None, {}, catalog, MagicMock(), config=config, force=True)
        df = result.collect()
        assert "prior_year" in df.columns


class TestPfsRatesUncoveredBranches:
    """Cover remaining uncovered branches in _pfs_rates.py."""

    # -- helpers ----------------------------------------------------------

    @staticmethod
    def _office_zip_lf():
        return pl.DataFrame({
            "zip_code": ["60601"],
            "office_name": ["Chicago Office"],
            "state": ["IL"],
            "market": ["Chicago"],
            "region_name": ["Midwest"],
        }).lazy()

    @staticmethod
    def _cms_geo_lf(year="2024"):
        return pl.DataFrame({
            "year_quarter": [f"{year}Q1"],
            "geo_zip_5": ["60601"],
            "geo_state_cd": ["IL"],
            "carrier": ["00952"],
            "locality": ["16"],
        }).lazy()

    @staticmethod
    def _gpci_lf():
        return pl.DataFrame({
            "geo_locality_state_cd": ["IL"],
            "geo_locality_num": ["16"],
            "geo_locality_name": ["Chicago Metro"],
            "pw_gpci": [1.0],
            "pe_gpci": [1.02],
            "pe_mp_gpci": [0.95],
        }).lazy()

    @staticmethod
    def _pprvu_lf(codes=None):
        if codes is None:
            codes = ["99213"]
        rows = {
            "99213": ("Office Visit E&M Lvl 3", 1.30, 1.59, 0.10, 33.2875),
            "99341": ("Home Visit New 20min", 1.00, 1.20, 0.08, 33.2875),
            "99342": ("Home Visit New 30min", 1.50, 1.40, 0.12, 33.2875),
            "99344": ("Home Visit New 60min", 2.50, 2.10, 0.20, 33.2875),
            "99345": ("Home Visit New 75min", 3.00, 2.60, 0.25, 33.2875),
            "99347": ("Home Visit Est 15min", 0.76, 0.90, 0.06, 33.2875),
            "99348": ("Home Visit Est 25min", 1.92, 1.31, 0.15, 33.2875),
            "99349": ("Home Visit Est 40min", 2.60, 1.80, 0.18, 33.2875),
            "99350": ("Home Visit Est 60min", 3.20, 2.50, 0.22, 33.2875),
            "G2211": ("Visit Complexity", 0.33, 0.18, 0.02, 33.2875),
            "G0556": ("APC Mgmt 60min", 2.10, 1.50, 0.10, 33.2875),
            "G0557": ("APC Mgmt Add 30min", 1.05, 0.75, 0.05, 33.2875),
            "G0558": ("APC Mgmt Single", 1.40, 1.00, 0.07, 33.2875),
        }
        selected = {c: rows[c] for c in codes if c in rows}
        return pl.DataFrame({
            "hcpcs": list(selected.keys()),
            "description": [v[0] for v in selected.values()],
            "work_rvu": [v[1] for v in selected.values()],
            "nf_pe_rvu": [v[2] for v in selected.values()],
            "mp_rvu": [v[3] for v in selected.values()],
            "conversion_factor": [v[4] for v in selected.values()],
        }).lazy()

    def _make_catalog(self, office_zip=None, cms_geo=None, gpci=None, pprvu=None):
        from unittest.mock import MagicMock
        catalog = MagicMock()
        tables = {
            "office_zip": office_zip,
            "cms_geo_zips": cms_geo,
            "gpci_inputs": gpci,
            "pprvu_inputs": pprvu,
        }
        catalog.scan_table.side_effect = lambda name: tables.get(name)
        return catalog

    # -- Line 227: config is None → default PFSRateCalcConfig() ----------

    @pytest.mark.unit
    def test_default_config_none_raises(self):
        """Line 227: config=None → PFSRateCalcConfig() with defaults.

        Default config has use_home_visit_codes=False and hcpcs_codes=[],
        so it hits the 'else' branch (line 237) raising ValueError.
        """
        from unittest.mock import MagicMock
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates

        catalog = MagicMock()
        with pytest.raises(ValueError, match="Must specify HCPCS"):
            calculate_pfs_rates(None, {}, catalog, MagicMock(), config=None)

    # -- Lines 231-232: use_home_visit_codes=True -------------------------

    @pytest.mark.unit
    def test_use_home_visit_codes_true(self):
        """Lines 231-232: use_home_visit_codes=True uses predefined code list."""
        from unittest.mock import MagicMock
        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig
        from acoharmony._expressions._hcpcs_filter import HCPCSFilterExpression
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates

        home_codes = HCPCSFilterExpression.home_visit_codes
        catalog = self._make_catalog(
            office_zip=self._office_zip_lf(),
            cms_geo=self._cms_geo_lf(),
            gpci=self._gpci_lf(),
            pprvu=self._pprvu_lf(codes=home_codes),
        )
        config = PFSRateCalcConfig(
            use_home_visit_codes=True,
            year=2024,
            conversion_factor=33.2875,
            include_comparison=False,
        )
        result = calculate_pfs_rates(None, {}, catalog, MagicMock(), config=config, force=True)
        df = result.collect()
        assert df.height > 0
        # All returned HCPCS codes should be from home visit list
        assert set(df["hcpcs_code"].to_list()).issubset(set(home_codes))

    # -- Line 268: cms_geo_zips returns None → ValueError -----------------

    @pytest.mark.unit
    def test_missing_cms_geo_zips_raises(self):
        """Line 268: cms_geo_zips table not found → ValueError."""
        from unittest.mock import MagicMock
        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates

        catalog = self._make_catalog(
            office_zip=self._office_zip_lf(),
            cms_geo=None,  # not found
        )
        config = PFSRateCalcConfig(hcpcs_codes=["99213"], year=2024)
        with pytest.raises(ValueError, match="cms_geo_zips"):
            calculate_pfs_rates(None, {}, catalog, MagicMock(), config=config)

    # -- Lines 273-279: config.year is None → auto-detect year -----------

    @pytest.mark.unit
    def test_year_auto_detection(self):
        """Lines 273-279: year=None auto-detects from cms_geo_zips max year_quarter."""
        from unittest.mock import MagicMock
        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates

        catalog = self._make_catalog(
            office_zip=self._office_zip_lf(),
            cms_geo=self._cms_geo_lf(year="2025"),
            gpci=self._gpci_lf(),
            pprvu=self._pprvu_lf(),
        )
        config = PFSRateCalcConfig(
            hcpcs_codes=["99213"],
            year=None,  # trigger auto-detection
            conversion_factor=33.2875,
            include_comparison=False,
        )
        result = calculate_pfs_rates(None, {}, catalog, MagicMock(), config=config, force=True)
        df = result.collect()
        # Auto-detected year should come from the data
        assert df["year"][0] == 2025

    # -- Lines 304-306: unmapped offices warning --------------------------

    @pytest.mark.unit
    def test_unmapped_offices_warning(self):
        """Lines 304-306: offices with no locality mapping trigger warning."""
        from unittest.mock import MagicMock
        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates

        # Office ZIP 99999 does not exist in cms_geo_zips
        office_zip = pl.DataFrame({
            "zip_code": ["60601", "99999"],
            "office_name": ["Chicago Office", "Unmapped Office"],
            "state": ["IL", "ZZ"],
            "market": ["Chicago", "Unknown"],
            "region_name": ["Midwest", "Unknown"],
        }).lazy()

        catalog = self._make_catalog(
            office_zip=office_zip,
            cms_geo=self._cms_geo_lf(),
            gpci=self._gpci_lf(),
            pprvu=self._pprvu_lf(),
        )
        logger = MagicMock()
        config = PFSRateCalcConfig(
            hcpcs_codes=["99213"],
            year=2024,
            conversion_factor=33.2875,
            include_comparison=False,
        )
        result = calculate_pfs_rates(None, {}, catalog, logger, config=config, force=True)
        df = result.collect()
        assert df.height == 2  # 2 offices x 1 HCPCS
        # Verify warning was logged about unmapped offices
        warning_calls = [
            c for c in logger.warning.call_args_list
            if "could not be mapped" in str(c)
        ]
        assert len(warning_calls) > 0

    # -- Line 312: gpci_inputs returns None → ValueError ------------------

    @pytest.mark.unit
    def test_missing_gpci_inputs_raises(self):
        """Line 312: gpci_inputs table not found → ValueError."""
        from unittest.mock import MagicMock
        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates

        catalog = self._make_catalog(
            office_zip=self._office_zip_lf(),
            cms_geo=self._cms_geo_lf(),
            gpci=None,  # not found
        )
        config = PFSRateCalcConfig(hcpcs_codes=["99213"], year=2024)
        with pytest.raises(ValueError, match="gpci_inputs"):
            calculate_pfs_rates(None, {}, catalog, MagicMock(), config=config)

    # -- Line 344: pprvu_inputs returns None → ValueError -----------------

    @pytest.mark.unit
    def test_missing_pprvu_inputs_raises(self):
        """Line 344: pprvu_inputs table not found → ValueError."""
        from unittest.mock import MagicMock
        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates

        catalog = self._make_catalog(
            office_zip=self._office_zip_lf(),
            cms_geo=self._cms_geo_lf(),
            gpci=self._gpci_lf(),
            pprvu=None,  # not found
        )
        config = PFSRateCalcConfig(hcpcs_codes=["99213"], year=2024)
        with pytest.raises(ValueError, match="pprvu_inputs"):
            calculate_pfs_rates(None, {}, catalog, MagicMock(), config=config)

    # -- Line 365->372: conversion_factor is None (no override) -----------

    @pytest.mark.unit
    def test_no_conversion_factor_override(self):
        """Line 365->372: conversion_factor=None uses CF from RVU data."""
        from unittest.mock import MagicMock
        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates

        catalog = self._make_catalog(
            office_zip=self._office_zip_lf(),
            cms_geo=self._cms_geo_lf(),
            gpci=self._gpci_lf(),
            pprvu=self._pprvu_lf(),
        )
        config = PFSRateCalcConfig(
            hcpcs_codes=["99213"],
            year=2024,
            conversion_factor=None,  # no override, use data CF
            include_comparison=False,
        )
        result = calculate_pfs_rates(None, {}, catalog, MagicMock(), config=config, force=True)
        df = result.collect()
        assert df.height == 1
        # CF from data is 33.2875; verify it was used
        assert df["conversion_factor"][0] == pytest.approx(33.2875)
        assert df["payment_rate"][0] > 0

    # -- Lines 401->422, 450->464: include_comparison=False ---------------

    @pytest.mark.unit
    def test_include_comparison_false(self):
        """Lines 401->422, 450->464: include_comparison=False skips
        prior year columns.
        """
        from unittest.mock import MagicMock
        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates

        catalog = self._make_catalog(
            office_zip=self._office_zip_lf(),
            cms_geo=self._cms_geo_lf(),
            gpci=self._gpci_lf(),
            pprvu=self._pprvu_lf(),
        )
        config = PFSRateCalcConfig(
            hcpcs_codes=["99213"],
            year=2024,
            conversion_factor=33.2875,
            include_comparison=False,
        )
        result = calculate_pfs_rates(None, {}, catalog, MagicMock(), config=config, force=True)
        df = result.collect()
        assert df.height == 1
        # Prior year columns should NOT be present
        assert "prior_year" not in df.columns
        assert "prior_payment_rate" not in df.columns
        assert "rate_change_dollars" not in df.columns
        assert "rate_change_percent" not in df.columns
        # Core columns should still be present
        assert "payment_rate" in df.columns
        assert "hcpcs_code" in df.columns

    # -- Line 402: include_comparison with prior_year=None ----------------

    @pytest.mark.unit
    def test_include_comparison_prior_year_default(self):
        """Line 402: include_comparison=True with prior_year=None
        defaults to year - 1.
        """
        from unittest.mock import MagicMock
        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates

        catalog = self._make_catalog(
            office_zip=self._office_zip_lf(),
            cms_geo=self._cms_geo_lf(),
            gpci=self._gpci_lf(),
            pprvu=self._pprvu_lf(),
        )
        config = PFSRateCalcConfig(
            hcpcs_codes=["99213"],
            year=2024,
            conversion_factor=33.2875,
            include_comparison=True,
            prior_year=None,  # defaults to 2023
        )
        result = calculate_pfs_rates(None, {}, catalog, MagicMock(), config=config, force=True)
        df = result.collect()
        assert "prior_year" in df.columns
        assert df["prior_year"][0] == 2023

    # -- Line 402: include_comparison with explicit prior_year ------------

    @pytest.mark.unit
    def test_include_comparison_explicit_prior_year(self):
        """Line 402: include_comparison=True with explicit prior_year."""
        from unittest.mock import MagicMock
        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig
        from acoharmony._transforms._pfs_rates import calculate_pfs_rates

        catalog = self._make_catalog(
            office_zip=self._office_zip_lf(),
            cms_geo=self._cms_geo_lf(),
            gpci=self._gpci_lf(),
            pprvu=self._pprvu_lf(),
        )
        config = PFSRateCalcConfig(
            hcpcs_codes=["99213"],
            year=2024,
            conversion_factor=33.2875,
            include_comparison=True,
            prior_year=2022,
        )
        result = calculate_pfs_rates(None, {}, catalog, MagicMock(), config=config, force=True)
        df = result.collect()
        assert "prior_year" in df.columns
        assert df["prior_year"][0] == 2022
