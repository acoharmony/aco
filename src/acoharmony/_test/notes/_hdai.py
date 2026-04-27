# © 2025 HarmonyCares
"""Tests for acoharmony._notes._hdai (HdaiPlugins)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from acoharmony._notes import HdaiPlugins


def _hdai_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mbi": ["M1", "M2", "M3"],
            "patient_first_name": ["A", "B", "C"],
            "patient_last_name": ["X", "Y", "Z"],
            "plurality_assigned_provider_name": ["P1", "P1", "P2"],
            "total_spend_ytd": [100000.0, 50000.0, 30000.0],
            "er_admits_ytd": [5, 2, 0],
            "er_admits_90_day_prior": [3, 0, 0],
            "any_inpatient_hospital_admits_ytd": [3, 1, 0],
            "any_inpatient_hospital_admits_90_day_prior": [2, 0, 0],
            "hospice_admission": [False, False, True],
            "inpatient_spend_ytd": [50000.0, 25000.0, 15000.0],
            "outpatient_spend_ytd": [20000.0, 10000.0, 5000.0],
            "snf_cost_ytd": [10000.0, 5000.0, 0.0],
            "home_health_spend_ytd": [5000.0, 2500.0, 1000.0],
            "hospice_spend_ytd": [0.0, 0.0, 4000.0],
            "dme_spend_ytd": [1000.0, 500.0, 0.0],
            "b_carrier_cost": [10000.0, 5000.0, 3000.0],
            "em_cost_ytd": [4000.0, 2000.0, 2000.0],
            "em_visits_ytd": [12, 8, 4],
            "last_em_visit": [date(2024, 6, 1), date(2024, 5, 1), date(2024, 3, 1)],
            "aco_em_name": ["Dr. A", "Dr. B", "Dr. C"],
            "aco_em_npi": ["N1", "N2", "N3"],
            "most_recent_awv_date": [date(2024, 1, 1), None, None],
            "awv_claim_id": ["AW1", None, None],
            "flag_em_hcmg": ["1", "0", "1"],
        }
    )


# ---------------------------------------------------------------------------
# load_with_crosswalk
# ---------------------------------------------------------------------------


class TestLoadWithCrosswalk:
    @pytest.mark.unit
    def test_missing_silver(self, tmp_path: Path) -> None:
        out = HdaiPlugins().load_with_crosswalk(tmp_path)
        assert out.collect().is_empty()

    @pytest.mark.unit
    def test_no_timeline_returns_base(self, tmp_path: Path) -> None:
        df = pl.DataFrame({"mbi": ["M1"], "x": [1]})
        df.write_parquet(tmp_path / "hdai_reach.parquet")
        out = HdaiPlugins().load_with_crosswalk(tmp_path)
        assert out.collect().height == 1

    @pytest.mark.unit
    def test_with_timeline_joins(self, tmp_path: Path) -> None:
        pl.DataFrame({"mbi": ["M1"], "x": [1]}).write_parquet(
            tmp_path / "hdai_reach.parquet"
        )
        # Create the timeline file marker
        pl.DataFrame({"x": [1]}).write_parquet(tmp_path / "identity_timeline.parquet")
        crosswalk = pl.LazyFrame(
            {
                "prvs_num": ["M1"],
                "crnt_num": ["M1_NEW"],
                "hcmpi": ["HC1"],
            }
        )
        with patch(
            "acoharmony._transforms._identity_timeline.current_mbi_with_hcmpi_lookup_lazy",
            return_value=crosswalk,
        ):
            out = HdaiPlugins().load_with_crosswalk(tmp_path).collect()
        assert "current_mbi" in out.columns
        assert out["current_mbi"][0] == "M1_NEW"
        assert out["hcmpi"][0] == "HC1"


# ---------------------------------------------------------------------------
# filter_to_most_recent
# ---------------------------------------------------------------------------


class TestFilterToMostRecent:
    @pytest.mark.unit
    def test_empty_frame(self) -> None:
        out = HdaiPlugins().filter_to_most_recent(pl.LazyFrame())
        assert out.collect().is_empty()

    @pytest.mark.unit
    def test_no_file_date_column(self) -> None:
        lf = pl.LazyFrame({"mbi": ["M1"]})
        out = HdaiPlugins().filter_to_most_recent(lf)
        assert out.collect().height == 1

    @pytest.mark.unit
    def test_filters(self) -> None:
        lf = pl.LazyFrame(
            {
                "mbi": ["M1", "M2"],
                "file_date": ["2024-01-01", "2024-06-01"],
            }
        )
        out = HdaiPlugins().filter_to_most_recent(lf).collect()
        assert out.height == 1
        assert out["file_date"][0] == "2024-06-01"


# ---------------------------------------------------------------------------
# flag_already_discussed
# ---------------------------------------------------------------------------


class TestFlagAlreadyDiscussed:
    @pytest.mark.unit
    def test_empty_frame(self) -> None:
        out = HdaiPlugins().flag_already_discussed(pl.LazyFrame(), ["M1"]).collect()
        # Empty frame, with just the new column
        assert "already_discussed" in out.columns

    @pytest.mark.unit
    def test_flags(self) -> None:
        lf = pl.LazyFrame({"mbi": ["M1", "M2", "M3"]})
        out = HdaiPlugins().flag_already_discussed(lf, ["M2"]).collect()
        as_dict = {row["mbi"]: row["already_discussed"] for row in out.iter_rows(named=True)}
        assert as_dict == {"M1": False, "M2": True, "M3": False}


# ---------------------------------------------------------------------------
# discussed-state file
# ---------------------------------------------------------------------------


class TestDiscussedState:
    @pytest.mark.unit
    def test_load_missing(self, tmp_path: Path) -> None:
        assert HdaiPlugins().load_discussed_state(tmp_path / "x.json") == {}

    @pytest.mark.unit
    def test_load_corrupt(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        path.write_text("{not json")
        assert HdaiPlugins().load_discussed_state(path) == {}

    @pytest.mark.unit
    def test_save_and_reload(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        plugin = HdaiPlugins()
        plugin.save_discussed_state(
            path, {"M1": {"discussed_date": "2024-01-01", "notes": "n"}}
        )
        out = plugin.load_discussed_state(path)
        assert "M1" in out

    @pytest.mark.unit
    def test_mark_discussed_explicit_date(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        out = HdaiPlugins().mark_discussed(
            path, "M1", notes="hi", discussed_date="2024-06-01"
        )
        assert out["M1"]["discussed_date"] == "2024-06-01"
        assert json.loads(path.read_text())["M1"]["notes"] == "hi"

    @pytest.mark.unit
    def test_mark_discussed_default_today(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        out = HdaiPlugins().mark_discussed(path, "M1")
        assert "discussed_date" in out["M1"]


# ---------------------------------------------------------------------------
# rollups
# ---------------------------------------------------------------------------


class TestProviderSummary:
    @pytest.mark.unit
    def test_all_providers(self) -> None:
        out = HdaiPlugins().provider_summary(_hdai_df())
        as_dict = {
            row["plurality_assigned_provider_name"]: row for row in out.iter_rows(named=True)
        }
        assert as_dict["P1"]["patient_count"] == 2
        assert as_dict["P1"]["total_spend"] == 150000.0

    @pytest.mark.unit
    def test_provider_filter(self) -> None:
        out = HdaiPlugins().provider_summary(_hdai_df(), provider_name="P2")
        assert out.height == 1
        assert out["plurality_assigned_provider_name"][0] == "P2"


class TestHighCostPatients:
    @pytest.mark.unit
    def test_default_top_n(self) -> None:
        out = HdaiPlugins().high_cost_patients(_hdai_df(), top_n=2)
        assert out.height == 2
        assert out["mbi"].to_list() == ["M1", "M2"]

    @pytest.mark.unit
    def test_min_cost_filter(self) -> None:
        out = HdaiPlugins().high_cost_patients(
            _hdai_df(), min_cost=60000, top_n=10
        )
        assert out.height == 1

    @pytest.mark.unit
    def test_max_cost_filter(self) -> None:
        out = HdaiPlugins().high_cost_patients(
            _hdai_df(), max_cost=40000, top_n=10
        )
        assert out["mbi"].to_list() == ["M3"]

    @pytest.mark.unit
    def test_min_er_filter(self) -> None:
        out = HdaiPlugins().high_cost_patients(
            _hdai_df(), min_er_admits=3, top_n=10
        )
        assert out["mbi"].to_list() == ["M1"]

    @pytest.mark.unit
    def test_min_ip_filter(self) -> None:
        out = HdaiPlugins().high_cost_patients(
            _hdai_df(), min_inpatient_admits=2, top_n=10
        )
        assert out["mbi"].to_list() == ["M1"]

    @pytest.mark.unit
    def test_provider_filter(self) -> None:
        out = HdaiPlugins().high_cost_patients(
            _hdai_df(), provider_name="P2", top_n=10
        )
        assert out["mbi"].to_list() == ["M3"]

    @pytest.mark.unit
    def test_optional_columns_included(self) -> None:
        df = _hdai_df().with_columns(
            pl.lit(False).alias("already_discussed"),
            pl.lit("HC").alias("hcmpi"),
        )
        out = HdaiPlugins().high_cost_patients(df, top_n=10)
        assert "already_discussed" in out.columns
        assert "hcmpi" in out.columns


class TestExceptionPaths:
    @pytest.mark.unit
    def test_filter_to_most_recent_collect_exception(self) -> None:
        # Pass a fake lf object whose collect_schema raises
        class _Bad:
            def collect_schema(self):
                raise RuntimeError("boom")

        out = HdaiPlugins().filter_to_most_recent(_Bad())
        # Should swallow and return the same input
        assert isinstance(out, _Bad)

    @pytest.mark.unit
    def test_flag_already_discussed_collect_exception(self) -> None:
        from unittest.mock import MagicMock

        bad = MagicMock()
        bad.collect_schema.side_effect = RuntimeError("boom")
        bad.with_columns.return_value = "stub"
        out = HdaiPlugins().flag_already_discussed(bad, ["M1"])
        assert out == "stub"


class TestSpendBreakdownNoSpend:
    @pytest.mark.unit
    def test_skips_zero_categories(self) -> None:
        df = pl.DataFrame(
            {
                "inpatient_spend_ytd": pl.Series([100.0], dtype=pl.Float64),
                "outpatient_spend_ytd": pl.Series([0.0], dtype=pl.Float64),
                "snf_cost_ytd": pl.Series([None], dtype=pl.Float64),
                "home_health_spend_ytd": pl.Series([50.0], dtype=pl.Float64),
                "hospice_spend_ytd": pl.Series([0.0], dtype=pl.Float64),
                "dme_spend_ytd": pl.Series([0.0], dtype=pl.Float64),
                "b_carrier_cost": pl.Series([0.0], dtype=pl.Float64),
                "em_cost_ytd": pl.Series([0.0], dtype=pl.Float64),
            }
        )
        _, items = HdaiPlugins().spend_breakdown(df)
        # Only Inpatient + Home Health survive
        assert len(items) == 2


class TestUtilizationTrends:
    @pytest.mark.unit
    def test_filters_to_recent_activity(self) -> None:
        out = HdaiPlugins().utilization_trends(_hdai_df())
        # Only M1 has 90-day activity
        assert out["mbi"].to_list() == ["M1"]
        assert out["er_recent_pct"][0] == pytest.approx(60.0)


class TestSpendBreakdown:
    @pytest.mark.unit
    def test_summary(self) -> None:
        summary, items = HdaiPlugins().spend_breakdown(_hdai_df())
        # IP is biggest
        assert items[0]["Category"] == "Inpatient"
        assert summary["Inpatient"] == 90000.0
        # Sorted desc
        amounts = [item["Total Spend"] for item in items]
        assert amounts == sorted(amounts, reverse=True)


class TestSaveDiscussedStateAtomic:
    @pytest.mark.unit
    def test_atomic_replace(self, tmp_path: Path) -> None:
        path = tmp_path / "deep" / "state.json"
        plugin = HdaiPlugins()
        plugin.save_discussed_state(path, {"M2": {}, "M1": {"notes": "x"}})
        # Sorted keys → M1 before M2
        body = path.read_text()
        assert body.index('"M1"') < body.index('"M2"')

    @pytest.mark.unit
    def test_replace_failure_cleans_tmp(self, tmp_path: Path) -> None:
        from unittest.mock import patch

        path = tmp_path / "state.json"
        with patch(
            "acoharmony._notes._hdai.os.replace",
            side_effect=RuntimeError("fail"),
        ):
            with pytest.raises(RuntimeError):
                HdaiPlugins().save_discussed_state(path, {"M1": {}})
        # No leftover .tmp files
        leftovers = list(tmp_path.glob(".hdai_reach_discussed.*.tmp"))
        assert leftovers == []


class TestDiscussedStateFile:
    @pytest.mark.unit
    def test_uses_storage_logs(self) -> None:
        from unittest.mock import MagicMock

        plugin = HdaiPlugins()
        fake = MagicMock()
        fake.get_path.return_value = "/x/logs"
        plugin._storage = fake
        out = plugin.discussed_state_file()
        assert out == Path("/x/logs/tracking/hdai_reach_discussed_state.json")

    @pytest.mark.unit
    def test_storage_failure_falls_back(self) -> None:
        from unittest.mock import MagicMock

        plugin = HdaiPlugins()
        fake = MagicMock()
        fake.get_path.side_effect = RuntimeError("boom")
        plugin._storage = fake
        out = plugin.discussed_state_file()
        assert out == Path("/tmp/tracking/hdai_reach_discussed_state.json")


class TestStateRowConverters:
    @pytest.mark.unit
    def test_state_to_rows_sorted(self) -> None:
        plugin = HdaiPlugins()
        out = plugin.discussed_state_to_rows(
            {"B": {"discussed_date": "2024-01-01", "notes": "n"}, "A": None}
        )
        assert [r["mbi"] for r in out] == ["A", "B"]
        # Empty entry yields blank fields, not crash
        assert out[0]["discussed_date"] == ""
        assert out[1]["notes"] == "n"

    @pytest.mark.unit
    def test_rows_to_state_drops_blank(self) -> None:
        out = HdaiPlugins().rows_to_discussed_state(
            [
                {"mbi": " m1 ", "discussed_date": " 2024-01-01 ", "notes": "x"},
                {"mbi": "", "notes": "ignored"},
                {"mbi": None, "notes": "also ignored"},
            ]
        )
        assert "M1" in out
        assert out["M1"]["discussed_date"] == "2024-01-01"
        assert "" not in out

    @pytest.mark.unit
    def test_rows_to_state_handles_none_fields(self) -> None:
        out = HdaiPlugins().rows_to_discussed_state(
            [{"mbi": "M1", "discussed_date": None, "notes": None}]
        )
        assert out["M1"] == {"discussed_date": "", "notes": ""}


class TestLoadDashboardData:
    @pytest.mark.unit
    def test_orchestrates(self, tmp_path: Path) -> None:
        pl.DataFrame(
            {
                "mbi": ["M1", "M2"],
                "file_date": ["2024-01-01", "2024-06-01"],
            }
        ).write_parquet(tmp_path / "hdai_reach.parquet")
        out = HdaiPlugins().load_dashboard_data(tmp_path, ["M2"])
        assert out.height == 1
        assert out["mbi"][0] == "M2"
        assert out["already_discussed"][0] is True


class TestFormatHighCostRows:
    @pytest.mark.unit
    def test_full_row(self) -> None:
        df = _hdai_df().with_columns(
            pl.lit("HC1").alias("hcmpi"),
            pl.lit("M1_NEW").alias("current_mbi"),
            pl.lit("Detroit").alias("cms_city"),
            pl.lit("MI").alias("cms_state"),
            pl.lit(date(2024, 1, 1)).alias("cms_death_dt"),
            pl.lit("Office A").alias("office_name"),
            pl.lit("Mid").alias("office_market"),
            pl.lit(True).alias("already_discussed"),
        )
        out = HdaiPlugins().format_high_cost_rows(df)
        assert out[0]["MBI"] == "M1 → M1_NEW"
        assert out[0]["Location"] == "Detroit, MI"
        assert "†" in out[0]["Name"]
        assert out[0]["Discussed"] == "✓"
        assert out[0]["E&M w/ HC"] == "✓"
        assert out[0]["HCMPI"] == "HC1"
        assert out[0]["Last AWV"] == "2024-01-01"

    @pytest.mark.unit
    def test_missing_optional_cols(self) -> None:
        out = HdaiPlugins().format_high_cost_rows(_hdai_df())
        # No hcmpi column → unmapped placeholder; no current_mbi → plain MBI
        assert out[0]["HCMPI"] == "⚠️ Not Mapped"
        assert out[0]["MBI"] == "M1"
        # Last EM exists for M1 only (date 2024-06-01)
        assert out[0]["Last E&M"] == "2024-06-01"
        # Empty M3 hospice column
        m3 = next(r for r in out if r["MBI"] == "M3")
        assert m3["Hospice"] == "✓"

    @pytest.mark.unit
    def test_state_only_no_city(self) -> None:
        df = _hdai_df().with_columns(
            pl.lit(None).cast(pl.Utf8).alias("cms_city"),
            pl.lit("MI").alias("cms_state"),
        )
        out = HdaiPlugins().format_high_cost_rows(df)
        assert out[0]["Location"] == "MI"

    @pytest.mark.unit
    def test_zero_spend(self) -> None:
        df = _hdai_df().with_columns(pl.lit(0.0).alias("total_spend_ytd"))
        out = HdaiPlugins().format_high_cost_rows(df)
        assert out[0]["Total Spend"] == "$0"


class TestFormatProviderRows:
    @pytest.mark.unit
    def test_formats(self) -> None:
        plugin = HdaiPlugins()
        rows = plugin.format_provider_rows(plugin.provider_summary(_hdai_df()))
        as_dict = {r["Provider"]: r for r in rows}
        assert as_dict["P1"]["Patients"] == "2"
        assert as_dict["P1"]["Total Spend"] == "$150,000"
        assert as_dict["P1"]["ER Admits"] == "7"
        assert as_dict["P1"]["IP Admits"] == "4"
        assert as_dict["P1"]["Hospice"] == "0"

    @pytest.mark.unit
    def test_zeroed_provider(self) -> None:
        zero_df = pl.DataFrame(
            {
                "plurality_assigned_provider_name": ["X"],
                "patient_count": [0],
                "total_spend": [0.0],
                "avg_spend_per_patient": [0.0],
                "total_er_admits": [0],
                "total_inpatient_admits": [0],
                "hospice_admissions": [0],
            }
        )
        out = HdaiPlugins().format_provider_rows(zero_df)
        assert out[0]["Total Spend"] == "$0"
        assert out[0]["Avg Spend"] == "$0"
        assert out[0]["ER Admits"] == "0"
        assert out[0]["IP Admits"] == "0"
        assert out[0]["Hospice"] == "0"


class TestFormatUtilizationRows:
    @pytest.mark.unit
    def test_intensities(self) -> None:
        plugin = HdaiPlugins()
        df = plugin.utilization_trends(_hdai_df())
        # Inject all-three buckets via lit overrides
        df_test = df.with_columns(
            pl.Series([60.0]).alias("er_recent_pct"),
            pl.Series([30.0]).alias("ip_recent_pct"),
        )
        out = plugin.format_utilization_rows(df_test)
        assert "🔴" in out[0]["ER Recent %"]
        assert "🟡" in out[0]["IP Recent %"]

    @pytest.mark.unit
    def test_low_intensity_and_zero_spend(self) -> None:
        plugin = HdaiPlugins()
        df = plugin.utilization_trends(_hdai_df()).with_columns(
            pl.Series([10.0]).alias("er_recent_pct"),
            pl.Series([10.0]).alias("ip_recent_pct"),
            pl.lit(0.0).alias("total_spend_ytd"),
        )
        out = plugin.format_utilization_rows(df)
        assert "🟢" in out[0]["ER Recent %"]
        assert out[0]["Total Spend"] == "$0"


class TestFormatSpendRows:
    @pytest.mark.unit
    def test_formats(self) -> None:
        plugin = HdaiPlugins()
        _, items = plugin.spend_breakdown(_hdai_df())
        rows = plugin.format_spend_rows(items)
        assert rows[0]["Category"] == "Inpatient"
        assert rows[0]["Total Spend"].startswith("$")
        assert rows[0]["Percentage"].endswith("%")
        # Visual bar uses block characters
        assert "█" in rows[0]["Visual"]


class TestOverviewMetrics:
    @pytest.mark.unit
    def test_empty(self) -> None:
        out = HdaiPlugins().overview_metrics(pl.DataFrame())
        assert out["total_patients"] == 0
        assert out["report_date"] == "Unknown"

    @pytest.mark.unit
    def test_with_hcmpi(self) -> None:
        df = _hdai_df().with_columns(
            pl.lit("2024-06-01").alias("file_date"),
            pl.Series(["HC1", None, "HC3"]).alias("hcmpi"),
        )
        out = HdaiPlugins().overview_metrics(df)
        assert out["total_patients"] == 3
        assert out["total_with_hcmpi"] == 2
        assert out["hcmpi_pct"] == pytest.approx(66.66, rel=0.01)
        assert out["report_date"] == "2024-06-01"

    @pytest.mark.unit
    def test_without_hcmpi_column(self) -> None:
        df = _hdai_df()
        out = HdaiPlugins().overview_metrics(df)
        assert out["total_with_hcmpi"] == 0
        assert out["hcmpi_pct"] == 0.0


class TestProviderDropdownOptions:
    @pytest.mark.unit
    def test_empty(self) -> None:
        assert HdaiPlugins().provider_dropdown_options(pl.DataFrame()) == [
            "All Providers"
        ]

    @pytest.mark.unit
    def test_with_data_filters_nones(self) -> None:
        df = pl.DataFrame(
            {
                "plurality_assigned_provider_name": ["B", None, "A", "B"],
            }
        )
        out = HdaiPlugins().provider_dropdown_options(df)
        assert out == ["All Providers", "A", "B"]
