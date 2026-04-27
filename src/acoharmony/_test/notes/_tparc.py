# © 2025 HarmonyCares
"""Tests for acoharmony._notes._tparc (TparcPlugins)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from acoharmony._notes import TparcPlugins


def _tparc_lf() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "record_type": ["CLMH", "CLMH", "CLML", "CLML", "CLML", "CLML"],
            "from_date": [20250115, 20250620, 20250115, 20250115, 20250620, 20240601],
            "thru_date": [20250115, 20250620, 20250115, 20250115, 20250620, 20240601],
            "source_filename": ["w1", "w2", "w1", "w1", "w2", "w0"],
            "hcpcs_code": [None, None, "99213", "99213", "99214", "99214"],
            "patient_control_num": [None, None, "P1", "P2", "P3", "P4"],
            "rendering_provider_npi": [None, None, "NPI1", "NPI1", "NPI2", "NPI3"],
            "total_charge_amt": [0.0, 0.0, 100.0, 200.0, 50.0, 75.0],
            "allowed_charge_amt": [0.0, 0.0, 80.0, 160.0, 40.0, 60.0],
            "covered_paid_amt": [0.0, 0.0, 70.0, 140.0, 35.0, 55.0],
            "pcc_reduction_amt": [0.0, 0.0, 10.0, 20.0, 5.0, 5.0],
            "sequestration_amt": [0.0, 0.0, 1.0, 2.0, 0.5, 0.5],
        }
    )


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------


class TestLoad:
    @pytest.mark.unit
    def test_returns_lazyframe(self, tmp_path: Path) -> None:
        df = pl.DataFrame({"x": [1]})
        df.write_parquet(tmp_path / "tparc.parquet")
        out = TparcPlugins().load(tmp_path)
        assert out.collect().equals(df)


# ---------------------------------------------------------------------------
# overall_stats / record_type_counts
# ---------------------------------------------------------------------------


class TestOverallStats:
    @pytest.mark.unit
    def test_counts(self) -> None:
        stats, types = TparcPlugins().overall_stats(_tparc_lf())
        assert stats["total_records"][0] == 6
        assert stats["record_types"][0] == 2
        assert stats["files_processed"][0] == 3
        as_dict = dict(zip(types["record_type"].to_list(), types["count"].to_list()))
        assert as_dict == {"CLMH": 2, "CLML": 4}


# ---------------------------------------------------------------------------
# CLMH date range
# ---------------------------------------------------------------------------


class TestClmhDateRange:
    @pytest.mark.unit
    def test_min_max(self) -> None:
        out = TparcPlugins().clmh_date_range(_tparc_lf())
        assert out["earliest_date"][0] == 20250115
        assert out["latest_date"][0] == 20250620


# ---------------------------------------------------------------------------
# CLML helpers
# ---------------------------------------------------------------------------


class TestClmlSummary:
    @pytest.mark.unit
    def test_financial(self) -> None:
        plugin = TparcPlugins()
        out = plugin.clml_financial_summary(plugin.clml_records(_tparc_lf()))
        assert out["total_charges"][0] == 425.0
        assert out["total_reductions"][0] == 40.0


class TestTopHcpcs:
    @pytest.mark.unit
    def test_groups_and_sorts(self) -> None:
        plugin = TparcPlugins()
        out = plugin.top_hcpcs(plugin.clml_records(_tparc_lf()), n=10)
        as_dict = {row["hcpcs_code"]: row for row in out.iter_rows(named=True)}
        assert as_dict["99213"]["claim_lines"] == 2
        assert as_dict["99214"]["claim_lines"] == 2


class TestUniquePatients:
    @pytest.mark.unit
    def test_counts_distinct(self) -> None:
        plugin = TparcPlugins()
        assert plugin.unique_patients(plugin.clml_records(_tparc_lf())) == 4


# ---------------------------------------------------------------------------
# weekly file rollup
# ---------------------------------------------------------------------------


class TestFileStats:
    @pytest.mark.unit
    def test_per_file_counts(self) -> None:
        out = TparcPlugins().file_stats(_tparc_lf())
        as_dict = {row["source_filename"]: row for row in out.iter_rows(named=True)}
        assert as_dict["w1"]["records"] == 3  # 1 CLMH + 2 CLML
        assert as_dict["w1"]["claims"] == 1
        assert as_dict["w1"]["line_items"] == 2


# ---------------------------------------------------------------------------
# monthly_office_breakdown / top_offices
# ---------------------------------------------------------------------------


class TestMonthlyOfficeBreakdown:
    @pytest.mark.unit
    def test_year_filter_and_format(self) -> None:
        out = TparcPlugins().monthly_office_breakdown(_tparc_lf(), 2025)
        # 2024 row excluded; 2025 → three rows: NPI1×2025-01, NPI1+NPI2×2025-06? Actually NPI1 has two Jan rows + NPI2 one Jun row
        as_dict = {
            (row["office_npi"], row["month"]): row for row in out.iter_rows(named=True)
        }
        assert as_dict[("NPI1", "2025-01")]["claim_lines"] == 2
        assert as_dict[("NPI2", "2025-06")]["claim_lines"] == 1
        # reduction_rate = 30 / 240 * 100 = 12.5 for NPI1's January
        assert as_dict[("NPI1", "2025-01")]["reduction_rate_pct"] == pytest.approx(12.5)

    @pytest.mark.unit
    def test_excludes_non_clml_and_other_years(self) -> None:
        out = TparcPlugins().monthly_office_breakdown(_tparc_lf(), 2024)
        # The only 2024 row was NPI3 in the fixture
        assert out.height == 1
        assert out["office_npi"][0] == "NPI3"


class TestTopOfficesByReductions:
    @pytest.mark.unit
    def test_groups_and_sorts(self) -> None:
        plugin = TparcPlugins()
        breakdown = plugin.monthly_office_breakdown(_tparc_lf(), 2025)
        out = plugin.top_offices_by_reductions(breakdown, n=10)
        # NPI1 had 30 reductions, NPI2 had 5 → NPI1 first
        assert out["office_npi"][0] == "NPI1"
        assert out["total_reductions"][0] == 30.0

    @pytest.mark.unit
    def test_zero_allowed_safe(self) -> None:
        df = pl.DataFrame(
            {
                "office_npi": ["NPI1"],
                "month": ["2025-01"],
                "claim_lines": [1],
                "total_charges": [0.0],
                "total_allowed": [0.0],
                "total_paid": [0.0],
                "total_reductions": [0.0],
                "total_sequestration": [0.0],
                "reduction_rate_pct": [0.0],
            }
        )
        out = TparcPlugins().top_offices_by_reductions(df)
        assert out["avg_reduction_rate"][0] == 0
