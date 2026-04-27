# © 2025 HarmonyCares
"""Tests for acoharmony._notes._home_visit (HomeVisitPlugins)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from acoharmony._notes import HomeVisitPlugins
from acoharmony._notes._home_visit import (
    HOME_VISIT_HCPCS_DESCRIPTIONS,
    RATE_INCREASE_2026,
)


def _claims_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "tin": ["T1", "T1", "T2", "T3"],
            "npi": ["N1", "N2", "N3", "N4"],
            "person_id": ["A", "B", "B", "C"],
            "hcpcs_code": ["99347", "99348", "99347", "99350"],
            "claim_start_date": [
                date(2024, 1, 1),
                date(2025, 6, 1),
                date(2024, 6, 1),
                date(2025, 1, 1),
            ],
            "paid_amount": [100.0, 200.0, 50.0, 300.0],
            "allowed_amount": [120.0, 240.0, 60.0, 360.0],
            "place_of_service_code": ["12", "12", "13", "12"],
        }
    )


# ---------------------------------------------------------------------------
# provider_tin_npi
# ---------------------------------------------------------------------------


class TestProviderTinNpi:
    @pytest.mark.unit
    def test_missing_returns_empty(self, tmp_path: Path) -> None:
        out = HomeVisitPlugins().provider_tin_npi(tmp_path)
        assert out.is_empty()

    @pytest.mark.unit
    def test_extracts_individual_and_org(self, tmp_path: Path) -> None:
        from unittest.mock import patch

        df = pl.DataFrame(
            {
                "base_provider_tin": ["T1", "T1", "T2"],
                "individual_npi": ["N1", None, "N3"],
                "organization_npi": [None, "ORG1", None],
            }
        )
        df.write_parquet(tmp_path / "participant_list.parquet")
        with patch(
            "acoharmony._expressions._file_version.FileVersionExpression.keep_only_most_recent_file",
            return_value=pl.lit(True),
        ):
            out = HomeVisitPlugins().provider_tin_npi(tmp_path)
        # T1+N1 (ind), T1+ORG1 (org), T2+N3 (ind)
        as_set = {(row["tin"], row["npi"]) for row in out.iter_rows(named=True)}
        assert ("T1", "N1") in as_set
        assert ("T1", "ORG1") in as_set
        assert ("T2", "N3") in as_set


# ---------------------------------------------------------------------------
# partition_by_provider_list
# ---------------------------------------------------------------------------


class TestPartition:
    @pytest.mark.unit
    def test_no_provider_list(self) -> None:
        provider, outside = HomeVisitPlugins().partition_by_provider_list(
            _claims_df(), pl.DataFrame()
        )
        assert provider.height == 4
        assert outside.is_empty()

    @pytest.mark.unit
    def test_partitions(self) -> None:
        providers = pl.DataFrame({"tin": ["T1"], "npi": ["N1"]})
        provider, outside = HomeVisitPlugins().partition_by_provider_list(
            _claims_df(), providers
        )
        assert provider.height == 1
        assert outside.height == 3


# ---------------------------------------------------------------------------
# rollups
# ---------------------------------------------------------------------------


class TestRollups:
    @pytest.mark.unit
    def test_summary_stats(self) -> None:
        out = HomeVisitPlugins().summary_stats(_claims_df())
        assert out["total_claims"] == 4
        assert out["total_patients"] == 3
        assert out["total_paid"] == 650.0

    @pytest.mark.unit
    def test_summary_stats_empty(self) -> None:
        out = HomeVisitPlugins().summary_stats(pl.DataFrame())
        assert out["total_claims"] == 0
        assert out["total_paid"] == 0.0
        assert out["min_date"] is None

    @pytest.mark.unit
    def test_hcpcs_distribution(self) -> None:
        out = HomeVisitPlugins().hcpcs_distribution(_claims_df())
        as_dict = {row["hcpcs_code"]: row for row in out.iter_rows(named=True)}
        assert as_dict["99347"]["claim_count"] == 2

    @pytest.mark.unit
    def test_top_providers(self) -> None:
        out = HomeVisitPlugins().top_providers(_claims_df(), n=10)
        assert out.height == 4

    @pytest.mark.unit
    def test_monthly_trends(self) -> None:
        out = HomeVisitPlugins().monthly_trends(_claims_df())
        # 4 distinct months in fixture
        assert out.height == 4

    @pytest.mark.unit
    def test_place_of_service(self) -> None:
        out = HomeVisitPlugins().place_of_service(_claims_df())
        as_dict = {row["place_of_service_code"]: row for row in out.iter_rows(named=True)}
        assert as_dict["12"]["claim_count"] == 3


class TestFilterByProvider:
    @pytest.mark.unit
    def test_empty_search_returns_all(self) -> None:
        out = HomeVisitPlugins().filter_by_provider(_claims_df(), "")
        assert out.height == 4

    @pytest.mark.unit
    def test_npi_filter(self) -> None:
        out = HomeVisitPlugins().filter_by_provider(_claims_df(), "N1")
        assert out.height == 1

    @pytest.mark.unit
    def test_tin_filter(self) -> None:
        out = HomeVisitPlugins().filter_by_provider(_claims_df(), "T1")
        assert out.height == 2


# ---------------------------------------------------------------------------
# year_comparison + project_2026 + projection_totals
# ---------------------------------------------------------------------------


class TestYearComparison:
    @pytest.mark.unit
    def test_groups_2024_2025(self) -> None:
        out = HomeVisitPlugins().year_comparison(_claims_df())
        # 99347 has both 2024 (T1+N1) and 2024 (T2+N3) — same year
        # 99348 has 2025; 99350 has 2025
        years = sorted(set(out["year"].to_list()))
        assert years == [2024, 2025]


class TestProject2026:
    @pytest.mark.unit
    def test_baseline_and_projection(self) -> None:
        plugin = HomeVisitPlugins()
        year_cmp = plugin.year_comparison(_claims_df())
        out = plugin.project_2026(year_cmp)
        as_dict = {row["hcpcs_code"]: row for row in out.iter_rows(named=True)}
        # 99347 had 2024 paid totals only; 2025 = 0 → baseline = avg(150, 0) = 75
        # rate increase w_addon for 99347 is 0.42 → projected_increase = 31.5
        assert as_dict["99347"]["avg_paid_2024_2025"] == pytest.approx(75.0)
        assert as_dict["99347"]["projected_increase_2026_w_addon"] == pytest.approx(31.5)


class TestProjectionTotals:
    @pytest.mark.unit
    def test_empty(self) -> None:
        out = HomeVisitPlugins().projection_totals(pl.DataFrame())
        assert out["total_2024"] == 0
        assert out["pct_increase_w"] == 0

    @pytest.mark.unit
    def test_rolls_up(self) -> None:
        plugin = HomeVisitPlugins()
        year_cmp = plugin.year_comparison(_claims_df())
        proj = plugin.project_2026(year_cmp)
        totals = plugin.projection_totals(proj)
        assert totals["total_avg_baseline"] > 0
        assert totals["pct_increase_w"] >= 0


class TestModuleConstants:
    @pytest.mark.unit
    def test_descriptions_present(self) -> None:
        for code in HOME_VISIT_HCPCS_DESCRIPTIONS:
            assert code in RATE_INCREASE_2026
