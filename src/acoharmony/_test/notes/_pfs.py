# © 2025 HarmonyCares
"""Tests for acoharmony._notes._pfs (PfsPlugins)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from acoharmony._notes import PfsPlugins
from acoharmony._notes._pfs import (
    CONVERSION_FACTOR_2025,
    CONVERSION_FACTOR_2026_APM,
    CONVERSION_FACTOR_2026_NON_APM,
    HARMONYCARES_OFFICES,
)


# ---------------------------------------------------------------------------
# offices
# ---------------------------------------------------------------------------


class TestOffices:
    @pytest.mark.unit
    def test_returns_dataframe(self) -> None:
        out = PfsPlugins().offices()
        assert out.height == len(HARMONYCARES_OFFICES)
        assert "office_zip" in out.columns
        assert "office_name" in out.columns


# ---------------------------------------------------------------------------
# load_zip_to_locality_2026 / load_gpci_2026 / load_pprvu_2026 (mocked excel)
# ---------------------------------------------------------------------------


class TestLoadZipToLocality:
    @pytest.mark.unit
    def test_filters_to_office_zips(self, tmp_path: Path) -> None:
        plugin = PfsPlugins()
        offices = pl.DataFrame({"office_zip": ["12345"]})
        raw = pl.DataFrame(
            {
                "ZIP CODE": ["12345", "99999"],
                "STATE": ["MA", "NY"],
                "CARRIER": ["C1", "C2"],
                "LOCALITY": ["L1", "L2"],
            }
        )
        # Make the file glob produce a real path
        path = tmp_path / "zipcarrier_2026_final_locality_ZIP5_JAN2026.xlsx"
        path.touch()
        with patch(
            "acoharmony._notes._pfs.pl.read_excel",
            return_value=raw,
        ):
            out = plugin.load_zip_to_locality_2026(tmp_path, offices)
        assert out.height == 1
        assert out["geo_zip_5"][0] == "12345"


class TestLoadGpci2026:
    @pytest.mark.unit
    def test_parses_and_floors(self, tmp_path: Path) -> None:
        # Mock pl.read_excel to return a 5-row frame with header in row 0
        raw_with_header = pl.DataFrame(
            {
                "0": [
                    "Medicare Administrative Contractor (MAC)",
                    None,
                    None,
                    None,
                    "C1",
                    "C2",
                ],
                "1": ["State", None, None, None, "MA", "NY"],
                "2": ["Locality Number", None, None, None, "L1", "L2"],
                "3": ["Locality Name", None, None, None, "Boston", "NY"],
                "4": [
                    "2026 PW GPCI (without 1.0 Floor)",
                    None,
                    None,
                    None,
                    "1.5",
                    "0.8",
                ],
                "5": ["2026 PE GPCI", None, None, None, "1.2", "0.7"],
                "6": ["2026 MP GPCI", None, None, None, "0.9", "0.5"],
            }
        )
        path = tmp_path / "pfs_2026_final_addenda_Addendum E_x.xlsx"
        path.touch()
        with patch(
            "acoharmony._notes._pfs.pl.read_excel", return_value=raw_with_header
        ):
            out = PfsPlugins().load_gpci_2026(tmp_path)
        # First row should have pe_gpci_with_floor = max(1.2, 1.0) = 1.2
        # Second row: max(0.7, 1.0) = 1.0
        as_dict = {row["carrier"]: row for row in out.iter_rows(named=True)}
        assert as_dict["C1"]["pe_gpci_with_floor"] == 1.2
        assert as_dict["C2"]["pe_gpci_with_floor"] == 1.0


class TestLoadPprvu2026:
    @pytest.mark.unit
    def test_filters_to_visit_codes(self, tmp_path: Path) -> None:
        raw = pl.DataFrame(
            {
                "0": [
                    "CPT1/ HCPCS",
                    None,
                    "99213",
                    "00000",
                ],
                "1": ["DESCRIPTION", None, "Office Visit", "Other"],
                "2": ["Work RVUs2", None, "1.0", "0.5"],
                "3": ["Non- Facility PE RVUs2", None, "0.5", "0.2"],
                "4": ["Facility PE RVUs2", None, "0.4", "0.2"],
                "5": ["Mal- Practice RVUs2", None, "0.05", "0.02"],
            }
        )
        path = tmp_path / "pfs_2026_final_addenda_Addendum B_x.xlsx"
        path.touch()
        with patch("acoharmony._notes._pfs.pl.read_excel", return_value=raw):
            out = PfsPlugins().load_pprvu_2026(tmp_path, ["99213"])
        assert out.height == 1
        assert out["hcpcs"][0] == "99213"


class TestLoadGpci2025:
    @pytest.mark.unit
    def test_renames_and_casts(self, tmp_path: Path) -> None:
        raw = pl.DataFrame(
            {
                "Medicare Administrative Contractor (MAC)": ["C1"],
                "State": ["MA"],
                "Locality Number": ["L1"],
                "Locality Name": ["X"],
                "2025 PW GPCI (with 1.0 Floor)": ["1.0"],
                "2025 PE GPCI": ["1.1"],
                "2025 MP GPCI": ["0.9"],
            }
        )
        path = tmp_path / "rvu_2025_q4_rvu_quarterly_GPCI2025.xlsx"
        path.touch()
        with patch("acoharmony._notes._pfs.pl.read_excel", return_value=raw):
            out = PfsPlugins().load_gpci_2025(tmp_path)
        assert "pw_gpci" in out.columns
        assert out["pw_gpci"][0] == 1.0


class TestLoadPprvu2025:
    @pytest.mark.unit
    def test_filters_codes(self, tmp_path: Path) -> None:
        # The function reads positional columns 0/5/6/8/10
        raw = pl.DataFrame(
            {f"col_{i}": ["99213" if i == 0 else "1.0" if i in (5, 6, 8, 10) else "x"] for i in range(11)}
        )
        path = tmp_path / "rvu_2025_q4_rvu_quarterly_PPRRVU2025_Oct.xlsx"
        path.touch()
        with patch("acoharmony._notes._pfs.pl.read_excel", return_value=raw):
            out = PfsPlugins().load_pprvu_2025(tmp_path, ["99213"])
        assert out.height == 1


# ---------------------------------------------------------------------------
# select_gpci_2026
# ---------------------------------------------------------------------------


def _gpci_raw() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "carrier": ["C1"],
            "state_name": ["MA"],
            "locality": ["L1"],
            "locality_name": ["Boston"],
            "pw_gpci": [1.5],
            "pe_gpci_without_floor": [0.7],
            "pe_gpci_with_floor": [1.0],
            "mp_gpci": [0.9],
        }
    )


class TestSelectGpci2026:
    @pytest.mark.unit
    def test_with_floor(self) -> None:
        out = PfsPlugins().select_gpci_2026(_gpci_raw(), with_floor=True)
        assert out["pe_gpci"][0] == 1.0

    @pytest.mark.unit
    def test_without_floor(self) -> None:
        out = PfsPlugins().select_gpci_2026(_gpci_raw(), with_floor=False)
        assert out["pe_gpci"][0] == 0.7


# ---------------------------------------------------------------------------
# calculate_rates / comparison
# ---------------------------------------------------------------------------


def _offices_df() -> pl.DataFrame:
    return pl.DataFrame(
        {"office_zip": ["12345"], "office_name": ["Test"]}
    )


def _zip_to_loc() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "geo_zip_5": ["12345"],
            "geo_state_cd": ["MA"],
            "carrier": ["C1"],
            "locality": ["L1"],
        }
    )


def _gpci_simple() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "carrier": ["C1"],
            "locality": ["L1"],
            "pw_gpci": [1.0],
            "pe_gpci": [1.0],
            "mp_gpci": [1.0],
        }
    )


def _pprvu_simple(with_desc: bool = True) -> pl.DataFrame:
    cols = {
        "hcpcs": ["99213"],
        "work_rvu": [1.0],
        "nf_pe_rvu": [0.5],
        "f_pe_rvu": [0.4],
        "mp_rvu": [0.1],
    }
    if with_desc:
        cols["description"] = ["Office Visit"]
    return pl.DataFrame(cols)


class TestCalculateRates:
    @pytest.mark.unit
    def test_basic(self) -> None:
        out = PfsPlugins().calculate_rates(
            _offices_df(),
            _zip_to_loc(),
            _gpci_simple(),
            _pprvu_simple(),
            conversion_factor=33.0,
        )
        assert out.height == 1
        assert "payment_rate" in out.columns
        assert out["hcpcs_code"][0] == "99213"
        assert out["hcpcs_description"][0] == "Office Visit"

    @pytest.mark.unit
    def test_no_description_column(self) -> None:
        out = PfsPlugins().calculate_rates(
            _offices_df(),
            _zip_to_loc(),
            _gpci_simple(),
            _pprvu_simple(with_desc=False),
            conversion_factor=33.0,
        )
        # description is auto-added by hcpcs_description rename only when present
        assert out.height == 1


class TestComparison:
    @pytest.mark.unit
    def test_diff_columns(self) -> None:
        rates_2026 = pl.DataFrame(
            {
                "office_zip": ["12345"],
                "office_name": ["X"],
                "hcpcs_code": ["99213"],
                "hcpcs_description": ["Visit"],
                "payment_rate": [110.0],
            }
        )
        rates_2025 = pl.DataFrame(
            {
                "office_zip": ["12345"],
                "hcpcs_code": ["99213"],
                "payment_rate": [100.0],
            }
        )
        out = PfsPlugins().comparison(rates_2026, rates_2025)
        assert out["dollar_change"][0] == 10.0
        assert out["percent_change"][0] == pytest.approx(10.0)


class TestComparisonSummary:
    @pytest.mark.unit
    def test_groups_by_hcpcs(self) -> None:
        comparison = pl.DataFrame(
            {
                "hcpcs_code": ["99213", "99213"],
                "hcpcs_description": ["Visit", "Visit"],
                "payment_2025": [100.0, 110.0],
                "payment_2026": [110.0, 121.0],
                "dollar_change": [10.0, 11.0],
                "percent_change": [10.0, 10.0],
            }
        )
        out = PfsPlugins().comparison_summary(comparison)
        assert out.height == 1
        assert out["avg_percent_change"][0] == pytest.approx(10.0)


class TestAllScenarios:
    @pytest.mark.unit
    def test_eight_columns(self) -> None:
        gpci_raw = _gpci_raw()
        out = PfsPlugins().all_scenarios(
            _offices_df(),
            _zip_to_loc(),
            gpci_raw,
            _pprvu_simple(),
        )
        # Eight scenario columns
        scenario_cols = [c for c in out.columns if c.startswith("2026_")]
        assert len(scenario_cols) == 8


class TestModuleConstants:
    @pytest.mark.unit
    def test_factors_match(self) -> None:
        assert CONVERSION_FACTOR_2025 == 32.3465
        assert CONVERSION_FACTOR_2026_APM > CONVERSION_FACTOR_2026_NON_APM
