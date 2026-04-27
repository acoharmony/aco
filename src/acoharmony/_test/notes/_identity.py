# © 2025 HarmonyCares
"""Tests for acoharmony._notes._identity (IdentityPlugins)."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import polars as pl
import pytest

from acoharmony._notes import IdentityPlugins, PanelPlugins, UIPlugins


def _gold_lf(rows: list[dict]) -> pl.LazyFrame:
    schema = {
        "mbi": pl.Utf8,
        "chain_id": pl.Int64,
        "file_date": pl.Date,
        "hop_index": pl.Int64,
        "hcmpi": pl.Utf8,
        "observation_type": pl.Utf8,
        "notes": pl.Utf8,
    }
    return pl.LazyFrame(rows, schema=schema)


class TestResolveAsOf:
    @pytest.mark.unit
    def test_empty_gold_returns_error(self) -> None:
        out = IdentityPlugins().resolve_as_of(pl.LazyFrame(), "M", date(2024, 1, 1))
        assert out == {"error": "gold identity_timeline not loaded"}

    @pytest.mark.unit
    def test_unknown_mbi(self) -> None:
        gold = _gold_lf([])
        out = IdentityPlugins().resolve_as_of(gold, "M", date(2024, 1, 1))
        assert out["chain_id"] is None
        assert "not found" in out["note"]

    @pytest.mark.unit
    def test_no_observations_before_date(self) -> None:
        gold = _gold_lf(
            [
                {
                    "mbi": "M",
                    "chain_id": 1,
                    "file_date": date(2024, 6, 1),
                    "hop_index": 0,
                    "hcmpi": None,
                    "observation_type": "cclf",
                    "notes": None,
                }
            ]
        )
        out = IdentityPlugins().resolve_as_of(gold, "M", date(2024, 1, 1))
        assert out["chain_id"] == 1
        assert out["canonical_mbi"] is None
        assert "No observations" in out["note"]

    @pytest.mark.unit
    def test_canonical_picks_latest_leaf(self) -> None:
        gold = _gold_lf(
            [
                {
                    "mbi": "OLD",
                    "chain_id": 1,
                    "file_date": date(2024, 1, 1),
                    "hop_index": 1,
                    "hcmpi": None,
                    "observation_type": "cclf",
                    "notes": None,
                },
                {
                    "mbi": "NEW",
                    "chain_id": 1,
                    "file_date": date(2024, 6, 1),
                    "hop_index": 0,
                    "hcmpi": "HC",
                    "observation_type": "cclf",
                    "notes": None,
                },
                {
                    "mbi": "OLDER",
                    "chain_id": 1,
                    "file_date": date(2024, 3, 1),
                    "hop_index": 0,
                    "hcmpi": None,
                    "observation_type": "cclf",
                    "notes": None,
                },
            ]
        )
        out = IdentityPlugins().resolve_as_of(gold, "OLD", date(2024, 12, 31))
        assert out["canonical_mbi"] == "NEW"  # latest leaf
        assert out["hcmpi"] == "HC"
        assert out["opted_out"] is False
        assert sorted(out["chain_members"]) == ["NEW", "OLD", "OLDER"]
        assert out["last_observed"] == date(2024, 6, 1)

    @pytest.mark.unit
    def test_no_leaves_falls_back(self) -> None:
        gold = _gold_lf(
            [
                {
                    "mbi": "M1",
                    "chain_id": 1,
                    "file_date": date(2024, 1, 1),
                    "hop_index": 2,
                    "hcmpi": None,
                    "observation_type": "cclf",
                    "notes": None,
                },
                {
                    "mbi": "M2",
                    "chain_id": 1,
                    "file_date": date(2024, 1, 1),
                    "hop_index": 1,
                    "hcmpi": None,
                    "observation_type": "cclf",
                    "notes": None,
                },
            ]
        )
        out = IdentityPlugins().resolve_as_of(gold, "M1", date(2024, 12, 31))
        # No hop_index=0 row → smallest hop_index wins (M2)
        assert out["canonical_mbi"] == "M2"

    @pytest.mark.unit
    def test_optout_collected(self) -> None:
        gold = _gold_lf(
            [
                {
                    "mbi": "M",
                    "chain_id": 1,
                    "file_date": date(2024, 1, 1),
                    "hop_index": 0,
                    "hcmpi": None,
                    "observation_type": "cclf",
                    "notes": None,
                },
                {
                    "mbi": "M",
                    "chain_id": 1,
                    "file_date": date(2024, 6, 1),
                    "hop_index": 0,
                    "hcmpi": None,
                    "observation_type": "bnex_optout",
                    "notes": "ResearchOptOut",
                },
                {
                    "mbi": "M",
                    "chain_id": 1,
                    "file_date": date(2024, 7, 1),
                    "hop_index": 0,
                    "hcmpi": None,
                    "observation_type": "bnex_optout",
                    "notes": None,
                },
            ]
        )
        out = IdentityPlugins().resolve_as_of(gold, "M", date(2024, 12, 31))
        assert out["opted_out"] is True
        assert out["opt_out_reasons"] == ["ResearchOptOut"]


def _metrics_lf(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(
        rows,
        schema={
            "metric_name": pl.Utf8,
            "file_date": pl.Date,
            "value": pl.Float64,
        },
    )


class TestChurnByFileDate:
    @pytest.mark.unit
    def test_pivots_relevant_metrics(self) -> None:
        metrics = _metrics_lf(
            [
                {"metric_name": "remaps_total", "file_date": date(2024, 1, 1), "value": 100.0},
                {"metric_name": "remaps_new", "file_date": date(2024, 1, 1), "value": 25.0},
                {"metric_name": "chains_touched", "file_date": date(2024, 1, 1), "value": 80.0},
                {"metric_name": "irrelevant", "file_date": date(2024, 1, 1), "value": 1.0},
            ]
        )
        out = IdentityPlugins().churn_by_file_date(metrics)
        assert out.height == 1
        assert out["remaps_total"][0] == 100.0
        assert "irrelevant" not in out.columns


class TestChainLengthDistribution:
    @pytest.mark.unit
    def test_counts_chains_by_size(self) -> None:
        timeline = pl.LazyFrame(
            {
                "chain_id": [1, 1, 1, 2, 3],
                "mbi": ["A", "B", "C", "D", "E"],
                "is_current_as_of_file_date": [True, True, True, True, False],
            }
        )
        out = IdentityPlugins().chain_length_distribution(timeline)
        # chain 1: size 3, chain 2: size 1; chain 3 excluded (not current)
        sizes = dict(zip(out["chain_size"].to_list(), out["n_chains"].to_list()))
        assert sizes == {1: 1, 3: 1}


class TestQualityMetrics:
    @pytest.mark.unit
    def test_pivots_and_caps_to_n(self) -> None:
        metrics = _metrics_lf(
            [
                {"metric_name": "hcmpi_coverage_pct", "file_date": date(2024, 1, 1), "value": 80.0},
                {"metric_name": "circular_refs", "file_date": date(2024, 1, 1), "value": 0.0},
                {"metric_name": "hcmpi_coverage_pct", "file_date": date(2024, 2, 1), "value": 82.0},
                {"metric_name": "irrelevant", "file_date": date(2024, 1, 1), "value": 1.0},
            ]
        )
        out = IdentityPlugins().quality_metrics(metrics, last_n_dates=1)
        assert out.height == 1
        assert out["file_date"][0] == date(2024, 2, 1)  # most recent
        assert "irrelevant" not in out.columns


class TestIdentityLookupPanel:
    @pytest.fixture
    def panel(self):
        ui = UIPlugins(); ui._mo = MagicMock()
        p = PanelPlugins(ui); p._mo = ui._mo
        return p

    @pytest.mark.unit
    def test_error_branch(self, panel):
        panel.identity_lookup_panel({"error": "boom"})
        panel._mo.md.assert_called_once()
        assert "boom" in panel._mo.md.call_args.args[0]

    @pytest.mark.unit
    def test_not_found(self, panel):
        panel.identity_lookup_panel({"input_mbi": "M", "chain_id": None})
        panel._mo.md.assert_called_once()
        assert "not found" in panel._mo.md.call_args.args[0]

    @pytest.mark.unit
    def test_opted_out(self, panel):
        panel.identity_lookup_panel(
            {
                "input_mbi": "M",
                "chain_id": 1,
                "canonical_mbi": "M",
                "hcmpi": "HC",
                "opted_out": True,
                "opt_out_reasons": ["X"],
                "chain_members": ["M"],
                "last_observed": "2024-01-01",
            }
        )
        html = panel._mo.md.call_args.args[0]
        assert "OPTED OUT" in html
        assert "(X)" in html

    @pytest.mark.unit
    def test_not_opted_out_with_no_reasons(self, panel):
        panel.identity_lookup_panel(
            {
                "input_mbi": "M",
                "chain_id": 1,
                "canonical_mbi": "M",
                "hcmpi": None,
                "opted_out": False,
                "opt_out_reasons": [],
                "chain_members": ["M"],
                "last_observed": "2024-01-01",
            }
        )
        html = panel._mo.md.call_args.args[0]
        assert "Not opted out" in html
        assert "(not mapped)" in html

    @pytest.mark.unit
    def test_opted_out_no_reason(self, panel):
        panel.identity_lookup_panel(
            {
                "input_mbi": "M",
                "chain_id": 1,
                "canonical_mbi": "M",
                "hcmpi": "HC",
                "opted_out": True,
                "opt_out_reasons": [],
                "chain_members": ["M"],
                "last_observed": "2024-01-01",
            }
        )
        html = panel._mo.md.call_args.args[0]
        assert "no reason given" in html
