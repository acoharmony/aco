# © 2025 HarmonyCares
"""Tests for acoharmony._notes._reach (ReachPlugins)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from acoharmony._notes import ReachPlugins


def _bar_lf(rows: list[dict]) -> pl.LazyFrame:
    schema = {
        "bene_mbi": pl.Utf8,
        "start_date": pl.Date,
        "end_date": pl.Date,
        "bene_date_of_death": pl.Date,
        "voluntary_alignment_type": pl.Utf8,
    }
    return pl.LazyFrame(rows, schema=schema)


def _bar_with_year_month(rows: list[dict]) -> pl.LazyFrame:
    """Rows already have year_month — used after load_bar."""
    return pl.LazyFrame(
        rows,
        schema={
            "bene_mbi": pl.Utf8,
            "start_date": pl.Date,
            "end_date": pl.Date,
            "bene_date_of_death": pl.Date,
            "voluntary_alignment_type": pl.Utf8,
            "year_month": pl.Int64,
        },
    )


# ---------------------------------------------------------------------------
# load_bar
# ---------------------------------------------------------------------------


class TestLoadBar:
    @pytest.mark.unit
    def test_missing_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="BAR file not found"):
            ReachPlugins().load_bar(tmp_path)

    @pytest.mark.unit
    def test_adds_year_month(self, tmp_path: Path) -> None:
        df = pl.DataFrame(
            {
                "bene_mbi": ["M"],
                "start_date": [date(2024, 6, 15)],
                "end_date": [None],
                "bene_date_of_death": [None],
                "voluntary_alignment_type": [None],
            }
        )
        df.write_parquet(tmp_path / "bar.parquet")
        lf = ReachPlugins().load_bar(tmp_path)
        out = lf.collect()
        assert "year_month" in out.columns
        assert out["year_month"][0] == 202406


# ---------------------------------------------------------------------------
# benes_for_window / benes_for_month
# ---------------------------------------------------------------------------


def _ym_rows() -> list[dict]:
    return [
        {"bene_mbi": "A", "start_date": date(2025, 1, 15), "end_date": None,
         "bene_date_of_death": None, "voluntary_alignment_type": None, "year_month": 202501},
        {"bene_mbi": "A", "start_date": date(2025, 6, 1), "end_date": None,
         "bene_date_of_death": None, "voluntary_alignment_type": None, "year_month": 202506},
        {"bene_mbi": "B", "start_date": date(2025, 7, 1), "end_date": None,
         "bene_date_of_death": None, "voluntary_alignment_type": None, "year_month": 202507},
        {"bene_mbi": "C", "start_date": date(2026, 1, 5), "end_date": None,
         "bene_date_of_death": None, "voluntary_alignment_type": None, "year_month": 202601},
    ]


class TestBenesForWindow:
    @pytest.mark.unit
    def test_distinct_mbis_in_range(self) -> None:
        lf = _bar_with_year_month(_ym_rows())
        out = ReachPlugins().benes_for_window(lf, 202501, 202512)
        # A appears twice → distinct → 2 rows total (A, B)
        assert sorted(out["mbi"].to_list()) == ["A", "B"]

    @pytest.mark.unit
    def test_single_month_window(self) -> None:
        lf = _bar_with_year_month(_ym_rows())
        out = ReachPlugins().benes_for_window(lf, 202507)
        assert out.height == 1
        assert out["mbi"][0] == "B"


class TestBenesForMonth:
    @pytest.mark.unit
    def test_filters_to_month(self) -> None:
        lf = _bar_with_year_month(_ym_rows())
        out = ReachPlugins().benes_for_month(lf, 202601)
        assert out.height == 1
        assert out["mbi"][0] == "C"


# ---------------------------------------------------------------------------
# attribution_loss
# ---------------------------------------------------------------------------


class TestAttributionLoss:
    @pytest.mark.unit
    def test_diff(self) -> None:
        prev = pl.DataFrame({"mbi": ["A", "B", "C"], "year_month": [202501, 202506, 202507]})
        nxt = pl.DataFrame({"mbi": ["B", "D"], "start_date": [None, None]})
        out = ReachPlugins().attribution_loss(prev, nxt)
        assert out["lost_mbis"] == {"A", "C"}
        assert out["total_lost"] == 2
        assert out["total_prev"] == 3
        assert out["total_next"] == 2
        # lost_benes preserves prev rows for A and C
        assert sorted(out["lost_benes"]["mbi"].to_list()) == ["A", "C"]


# ---------------------------------------------------------------------------
# load_crr_for_lost
# ---------------------------------------------------------------------------


class TestLoadCrrForLost:
    @pytest.mark.unit
    def test_missing_returns_none(self, tmp_path: Path) -> None:
        assert ReachPlugins().load_crr_for_lost(tmp_path, ["A"]) is None

    @pytest.mark.unit
    def test_filters_to_lost(self, tmp_path: Path) -> None:
        crr = pl.DataFrame(
            {
                "bene_mbi": ["A", "B", "C"],
                "bene_death_dt": [date(2024, 12, 1), None, None],
            }
        )
        crr.write_parquet(tmp_path / "crr.parquet")
        out = ReachPlugins().load_crr_for_lost(tmp_path, {"A", "B"})
        assert sorted(out["mbi"].to_list()) == ["A", "B"]
        assert "bene_death_dt" in out.columns


# ---------------------------------------------------------------------------
# lost_bar_records / categorize_term_reasons / breakdown_stats
# ---------------------------------------------------------------------------


def _bar_with_lost() -> pl.LazyFrame:
    return _bar_with_year_month(
        [
            {"bene_mbi": "A", "start_date": date(2025, 1, 1), "end_date": date(2025, 6, 1),
             "bene_date_of_death": date(2025, 6, 1), "voluntary_alignment_type": None, "year_month": 202501},
            {"bene_mbi": "A", "start_date": date(2025, 6, 1), "end_date": date(2025, 6, 1),
             "bene_date_of_death": date(2025, 6, 1), "voluntary_alignment_type": None, "year_month": 202506},
            {"bene_mbi": "B", "start_date": date(2025, 1, 1), "end_date": None,
             "bene_date_of_death": None, "voluntary_alignment_type": "Voluntary", "year_month": 202501},
            {"bene_mbi": "C", "start_date": date(2025, 1, 1), "end_date": None,
             "bene_date_of_death": None, "voluntary_alignment_type": None, "year_month": 202501},
        ]
    )


class TestLostBarRecords:
    @pytest.mark.unit
    def test_takes_latest_per_mbi(self) -> None:
        out = ReachPlugins().lost_bar_records(_bar_with_lost(), {"A", "B", "C"})
        as_dict = {row["mbi"]: row for row in out.iter_rows(named=True)}
        assert as_dict["A"]["last_alignment_month"] == 202506
        assert as_dict["B"]["voluntary_type"] == "Voluntary"


class TestCategorizeTermReasons:
    @pytest.mark.unit
    def test_with_crr(self) -> None:
        lost_bar = ReachPlugins().lost_bar_records(_bar_with_lost(), {"A", "B", "C"})
        crr = pl.DataFrame({"mbi": ["A"], "bene_death_dt": [date(2025, 6, 1)]})
        cat, summary = ReachPlugins().categorize_term_reasons(lost_bar, crr)
        # A → Expired (death_date in BAR), B → Lost Provider, C → Other/Unknown
        as_dict = {row["mbi"]: row for row in cat.iter_rows(named=True)}
        assert as_dict["A"]["term_category"] == "Expired"
        assert as_dict["B"]["term_category"] == "Lost Provider"
        assert as_dict["C"]["term_category"] == "Other/Unknown"
        # Summary sums match
        sums = dict(zip(summary["term_category"].to_list(), summary["count"].to_list()))
        assert sums == {"Expired": 1, "Lost Provider": 1, "Other/Unknown": 1}

    @pytest.mark.unit
    def test_without_crr(self) -> None:
        # Synthesize a case where death_date in BAR is None but voluntary set
        lost_bar = pl.DataFrame(
            {
                "mbi": ["B"],
                "last_alignment_month": [202501],
                "end_date": [None],
                "death_date": [None],
                "voluntary_type": ["Voluntary"],
            }
        )
        cat, summary = ReachPlugins().categorize_term_reasons(lost_bar, None)
        assert cat["term_category"][0] == "Lost Provider"


class TestBreakdownStats:
    @pytest.mark.unit
    def test_flattens(self) -> None:
        summary = pl.DataFrame(
            {"term_category": ["Expired", "Other/Unknown"], "count": [3, 2]}
        )
        out = ReachPlugins().breakdown_stats(summary, total_lost=10, has_end_date=8)
        assert out["Total Lost"] == 10
        assert out["Expired (SVA)"] == 3
        assert out["Other/Unknown Reason"] == 2
        assert out["Lost Provider"] == 0
        assert out["No End Date"] == 2
        assert out["Moved to MA"] == 0


class TestTemporalDistribution:
    @pytest.mark.unit
    def test_sorts_by_month(self) -> None:
        df = pl.DataFrame(
            {
                "mbi": ["A", "B", "C"],
                "last_alignment_month": [202506, 202501, 202506],
            }
        )
        out = ReachPlugins().temporal_distribution(df)
        assert out["last_month_str"].to_list() == ["202501", "202506"]
        assert out["count"].to_list() == [1, 2]


# ---------------------------------------------------------------------------
# delivery provenance
# ---------------------------------------------------------------------------


def _delivery_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "schema_name": ["bar", "bar", "bar", "crr", "crr", None],
            "period": ["M01", "Q1", "S1", "A", "M02", "M03"],
            "py": ["PY2025"] * 6,
            "expected_date": [
                date(2025, 1, 15),
                date(2025, 4, 1),
                date(2025, 6, 30),
                date(2025, 12, 31),
                date(2025, 2, 15),
                None,
            ],
            "actual_delivery_date": [
                date(2025, 1, 15),
                date(2025, 4, 8),
                None,
                date(2025, 12, 30),
                date(2025, 2, 13),
                date(2025, 3, 5),
            ],
            "actual_delivery_source": [
                "remote_metadata",
                "filename",
                None,
                "download",
                "remote_metadata",
                "remote_metadata",
            ],
            "delivery_diff_days": [0, 7, None, -1, -2, None],
            "delivery_status": [
                "on_time",
                "late",
                "unscheduled",
                "early",
                "early",
                "unscheduled",
            ],
            "description": ["d1", "d2", "d3", "d4", "d5", "d6"],
            "category": ["c1", "c1", "c1", "c2", "c2", None],
            "delivered_file_count": [1, 1, 1, 1, 1, 1],
            "delivered_filenames": [["f"]] * 6,
        }
    )


class TestCadenceBucket:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "period,expected",
        [
            (None, "unknown"),
            ("M01", "monthly"),
            ("Q1", "quarterly"),
            ("S2", "semi_annual"),
            ("A", "annual"),
            ("X", "other"),
        ],
    )
    def test_classifies(self, period, expected):
        assert ReachPlugins.cadence_bucket(period) == expected


class TestDeliveryStatusPivot:
    @pytest.mark.unit
    def test_pivots_by_schema(self) -> None:
        out = ReachPlugins().delivery_status_pivot(_delivery_df())
        as_dict = {row["schema_name"]: row for row in out.iter_rows(named=True)}
        assert as_dict["bar"]["total"] == 3
        assert as_dict["bar"]["on_time"] == 1
        assert as_dict["bar"]["late"] == 1
        assert as_dict["bar"]["unscheduled"] == 1
        # Null schema rows excluded
        assert None not in as_dict


class TestDeliveryDiffStats:
    @pytest.mark.unit
    def test_returns_stats(self) -> None:
        out = ReachPlugins().delivery_diff_stats(_delivery_df())
        assert out["n"][0] == 4  # rows with non-null delivery_diff_days
        assert "mean_days" in out.columns

    @pytest.mark.unit
    def test_empty(self) -> None:
        df = pl.DataFrame(
            {"delivery_diff_days": pl.Series([None, None], dtype=pl.Int64)}
        )
        out = ReachPlugins().delivery_diff_stats(df)
        assert out.is_empty()


class TestDeliveryOutliers:
    @pytest.mark.unit
    def test_late_sorted_desc(self) -> None:
        out = ReachPlugins().delivery_outliers(_delivery_df(), "late", n=10)
        assert out.height == 1
        assert out["delivery_diff_days"][0] == 7

    @pytest.mark.unit
    def test_early_sorted_asc(self) -> None:
        out = ReachPlugins().delivery_outliers(_delivery_df(), "early", n=10)
        # Two early rows; sorted asc → -2 first
        assert out["delivery_diff_days"][0] == -2


class TestDeliveryCadence:
    @pytest.mark.unit
    def test_dominant_cadence_per_schema(self) -> None:
        out = ReachPlugins().delivery_cadence(_delivery_df())
        as_dict = {row["schema_name"]: row for row in out.iter_rows(named=True)}
        # bar has M01, Q1, S1 → all distinct buckets, mode picks one
        assert as_dict["bar"]["dominant_cadence"] in {"monthly", "quarterly", "semi_annual"}
        assert as_dict["crr"]["scheduled_count"] == 2


class TestDeliveryTrend:
    @pytest.mark.unit
    def test_groups_by_quarter(self) -> None:
        out = ReachPlugins().delivery_trend(_delivery_df())
        assert "year" in out.columns and "quarter" in out.columns
        # Q1 quarter for bar's January expected_date
        bar_q1 = out.filter((pl.col("schema_name") == "bar") & (pl.col("quarter") == 1))
        assert bar_q1.height >= 1

    @pytest.mark.unit
    def test_empty(self) -> None:
        df = pl.DataFrame({"delivery_diff_days": pl.Series([None], dtype=pl.Int64)})
        out = ReachPlugins().delivery_trend(df)
        assert out.is_empty()


class TestUnexpectedDeliveries:
    @pytest.mark.unit
    def test_filters_to_unscheduled(self) -> None:
        out = ReachPlugins().unexpected_deliveries(_delivery_df())
        assert out.height == 2
        # Sorted by actual_delivery_date desc — None last in our fixture
        assert set(out["actual_delivery_date"].to_list()) == {date(2025, 3, 5), None}


class TestSchemaDrilldown:
    @pytest.mark.unit
    def test_filters_and_sorts(self) -> None:
        out = ReachPlugins().schema_drilldown(_delivery_df(), "bar")
        assert out.height == 3
        # Sorted by coalesce(expected, actual) desc
        assert "delivery_status" in out.columns


# ---------------------------------------------------------------------------
# BNMR (Benchmark Report Analysis)
# ---------------------------------------------------------------------------


class TestBnmrDeliveries:
    @pytest.mark.unit
    def test_extracts_delivery_date(self) -> None:
        rp = pl.DataFrame(
            {
                "source_filename": [
                    "ACO.BNMR.D250115.T123000",
                    "ACO.BNMR.D250115.T123000",  # dup
                    "ACO.BNMR.D250601.T010000",
                ],
                "performance_year": ["PY2025", "PY2025", "PY2025"],
                "aco_id": ["A", "A", "A"],
            }
        )
        out = ReachPlugins().bnmr_deliveries(rp)
        assert out.height == 2  # deduped
        assert out["delivery_date"].to_list() == [date(2025, 1, 15), date(2025, 6, 1)]


class TestBnmrClaimsSpend:
    @pytest.mark.unit
    def test_groups_with_claim_type_label(self) -> None:
        claims = pl.DataFrame(
            {
                "source_filename": ["d1", "d1", "d1"],
                "performance_year": ["PY2025"] * 3,
                "clm_type_cd": ["10", "60", "99"],  # 99 → Other
                "clm_pmt_amt_agg": ["100", "200", "50"],
            }
        )
        out = ReachPlugins().bnmr_claims_spend(claims)
        labels = sorted(out["claim_type"].to_list())
        assert "10 – HHA" in labels
        assert "Other" in labels


class TestBnmrCountyRates:
    @pytest.mark.unit
    def test_filters_null_rate(self) -> None:
        county = pl.DataFrame(
            {
                "cty_accrl_cd": ["1", "2"],
                "bnmrk": ["AD", "ESRD"],
                "performance_year": ["PY2025", "PY2025"],
                "cty_rate": [None, "1500.50"],
                "source_filename": ["d1", "d1"],
            }
        )
        out = ReachPlugins().bnmr_county_rates(county)
        assert out.height == 1
        assert out["rate"][0] == 1500.50


class TestBnmrUspccTrend:
    @pytest.mark.unit
    def test_dedupes_and_sorts(self) -> None:
        uspcc = pl.DataFrame(
            {
                "clndr_yr": ["2024", "2024", "2023"],
                "bnmrk": ["AD", "AD", "AD"],
                "uspcc": ["1000", "1000", "950"],
                "performance_year": ["PY2025", "PY2025", "PY2025"],
            }
        )
        out = ReachPlugins().bnmr_uspcc_trend(uspcc)
        assert out["calendar_year"].to_list() == [2023, 2024]


class TestBnmrNormalizationFactor:
    @pytest.mark.unit
    def test_filters_to_normalization_lines(self) -> None:
        rs_ad = pl.DataFrame(
            {
                "line_description": ["Normalization Factor", "Other Line", None],
                "py_value": ["0.95", "1.0", None],
                "source_filename": ["d1", "d1", "d1"],
                "performance_year": ["PY2025"] * 3,
            }
        )
        out = ReachPlugins().bnmr_normalization_factor(rs_ad)
        assert out.height == 1
        assert out["normalization_factor"][0] == 0.95


class TestBnmrCapitationAggregate:
    @pytest.mark.unit
    def test_legacy_column(self) -> None:
        cap = pl.DataFrame(
            {
                "aco_tcc_amt_total": ["1000", "2000", "0"],
                "pmt_mnth": ["202501", "202502", "202503"],
                "performance_year": ["PY2025"] * 3,
                "bnmrk": ["AD"] * 3,
            }
        )
        out = ReachPlugins().bnmr_capitation_aggregate(cap)
        # 0-value row excluded
        assert out.height == 2

    @pytest.mark.unit
    def test_new_column_only(self) -> None:
        cap = pl.DataFrame(
            {
                "aco_tcc_amt_post_seq_paid": ["1500"],
                "pmt_mnth": ["202501"],
                "performance_year": ["PY2025"],
                "bnmrk": ["AD"],
            }
        )
        out = ReachPlugins().bnmr_capitation_aggregate(cap)
        assert out["total_tcc"][0] == 1500.0


class TestBnmrStopLossCounty:
    @pytest.mark.unit
    def test_filters_null_payout(self) -> None:
        slc = pl.DataFrame(
            {
                "cty_accrl_cd": ["1", "2"],
                "bnmrk": ["AD", "AD"],
                "performance_year": ["PY2025", "PY2025"],
                "avg_payout_pct": [None, "0.05"],
            }
        )
        out = ReachPlugins().bnmr_stop_loss_county_payouts(slc)
        assert out.height == 1


class TestBnmrRiskCounts:
    @pytest.mark.unit
    def test_aggregates(self) -> None:
        risk = pl.DataFrame(
            {
                "bene_dcnt": ["100", "200", None],
                "elig_mnths": ["1200", "2400", None],
                "source_filename": ["d1", "d1", "d1"],
                "performance_year": ["PY2025", "PY2025", "PY2025"],
                "bnmrk": ["AD", "AD", "AD"],
            }
        )
        out = ReachPlugins().bnmr_risk_counts(risk)
        assert out["total_bene_dcnt"][0] == 300


class TestBnmrSilverInventory:
    @pytest.mark.unit
    def test_skips_missing(self, tmp_path: Path) -> None:
        # No files at all → empty
        out = ReachPlugins().bnmr_silver_inventory(tmp_path)
        assert out.is_empty()

    @pytest.mark.unit
    def test_one_file_present(self, tmp_path: Path) -> None:
        df = pl.DataFrame(
            {
                "source_filename": ["d1"],
                "performance_year": ["PY2025"],
                "x": [1],
            }
        )
        df.write_parquet(tmp_path / "reach_bnmr_claims.parquet")
        out = ReachPlugins().bnmr_silver_inventory(tmp_path)
        assert out.height == 1
        assert out["table"][0] == "claims"
        assert out["deliveries"][0] == 1


class TestBnmrReportParametersView:
    @pytest.mark.unit
    def test_keeps_only_present_columns(self) -> None:
        rp = pl.DataFrame(
            {
                "performance_year": ["PY2025", "PY2025"],
                "source_filename": ["d1", "d1"],  # dup
                "shared_savings_rate": ["0.5", "0.5"],
                "extra_column": ["x", "y"],  # not in COLS
            }
        )
        out = ReachPlugins().bnmr_report_parameters_view(rp)
        assert "extra_column" not in out.columns
        assert out.height == 1  # deduped
