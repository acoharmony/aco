# © 2025 HarmonyCares
"""Tests for acoharmony._notes._quality (QualityPlugins)."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from acoharmony._notes import QualityPlugins
from acoharmony._notes._quality import (
    KNOWN_MEASURES,
    MEASURE_LABELS,
    MEASURE_UNITS,
)


class TestQtlqrMeasures:
    @pytest.mark.unit
    def test_filters_and_parses(self) -> None:
        qtlqr = pl.LazyFrame(
            [
                {
                    "source_filename": "QTLQR.Q1.PY2024.D240501.T120000.csv",
                    "measure": "ACR",
                    "measure_score": "10.5",
                    "measure_volume": "100",
                    "reporting_period": "2024Q1",
                },
                {
                    "source_filename": "QTLQR.Q2.PY2024.D240801.T120000.csv",
                    "measure": "DAH",
                    "measure_score": "320",
                    "measure_volume": "200",
                    "reporting_period": "2024Q2",
                },
                {
                    "source_filename": "QTLQR.Q1.PY2024.D240501.T120000.csv",
                    "measure": "OTHER",  # should be filtered
                    "measure_score": "1",
                    "measure_volume": "1",
                    "reporting_period": "2024Q1",
                },
            ]
        )
        out = QualityPlugins().qtlqr_measures(qtlqr)
        assert out.height == 2
        assert "OTHER" not in out["measure"].to_list()
        # Sorted by delivery_date ascending → ACR first
        assert out["measure"][0] == "ACR"
        assert out["delivery_date"][0] == date(2024, 5, 1)
        assert out["quarter"][0] == "Q1"
        assert out["score"][0] == 10.5
        assert out["volume"][0] == 100.0
        assert MEASURE_LABELS["ACR"] in out["measure_label"].to_list()


class TestBlqqrAggregate:
    @pytest.mark.unit
    def test_acr_rate(self) -> None:
        df = pl.LazyFrame(
            [
                {
                    "source_filename": "BLQQR_ACR.Q1.PY2024.x",
                    "bene_id": "B1",
                    "radm30_flag": 1,
                },
                {
                    "source_filename": "BLQQR_ACR.Q1.PY2024.x",
                    "bene_id": "B1",
                    "radm30_flag": 0,
                },
                {
                    "source_filename": "BLQQR_ACR.Q1.PY2024.x",
                    "bene_id": "B2",
                    "radm30_flag": 0,
                },
            ]
        )
        out = QualityPlugins().blqqr_aggregate(df, "acr")
        row = out.to_dicts()[0]
        assert row["index_stays"] == 3
        assert row["benes"] == 2
        assert row["readmissions"] == 1
        assert row["raw_rate"] == pytest.approx(33.333, rel=1e-3)
        assert row["period"] == "PY2024 Q1"

    @pytest.mark.unit
    def test_dah_mean(self) -> None:
        df = pl.LazyFrame(
            [
                {
                    "source_filename": "BLQQR_DAH.Q1.PY2024.x",
                    "bene_id": "B1",
                    "survival_days": "365",
                    "observed_dah": "60",
                },
                {
                    "source_filename": "BLQQR_DAH.Q1.PY2024.x",
                    "bene_id": "B2",
                    "survival_days": "365",
                    "observed_dah": "30",
                },
            ]
        )
        out = QualityPlugins().blqqr_aggregate(df, "dah")
        row = out.to_dicts()[0]
        assert row["benes"] == 2
        assert row["raw_dah"] == pytest.approx(320.0)


class TestBlqqrExclusions:
    @pytest.mark.unit
    def test_period_columns_added(self) -> None:
        df = pl.LazyFrame(
            [
                {"source_filename": "BLQQR_EXCL.Q1.PY2024.x", "ct_opting_out_acr": 5},
            ]
        )
        out = QualityPlugins().blqqr_exclusions(df)
        assert out["quarter"][0] == "Q1"
        assert out["perf_year"][0] == "PY2024"


class TestExclusionLong:
    @pytest.mark.unit
    def test_pivots_acr_and_dah(self) -> None:
        df = pl.DataFrame(
            {
                "perf_year": ["PY2024"],
                "quarter": ["Q1"],
                "ct_opting_out_acr": ["10"],
                "ct_elig_prior_acr": ["3"],
                "ct_opting_out_dah": ["8"],
                "ct_elig_prior_dah": ["2"],
            }
        )
        out = QualityPlugins().exclusion_long(df)
        assert out.height == 2
        assert set(out["measure"].to_list()) == {"ACR", "DAH"}
        assert all(out["period"].to_list()[i] == "PY2024 Q1" for i in (0, 1))

    @pytest.mark.unit
    def test_drops_null_optout_rows(self) -> None:
        df = pl.DataFrame(
            {
                "perf_year": ["PY2024"],
                "quarter": ["Q1"],
                "ct_opting_out_acr": [None],
                "ct_elig_prior_acr": [None],
                "ct_opting_out_dah": ["8"],
                "ct_elig_prior_dah": ["2"],
            }
        )
        out = QualityPlugins().exclusion_long(df)
        # Only DAH row survives
        assert out.height == 1
        assert out["measure"][0] == "DAH"


class TestModuleConstants:
    @pytest.mark.unit
    def test_known_measures_complete(self) -> None:
        assert set(KNOWN_MEASURES) == {"ACR", "DAH", "UAMCC"}
        assert all(m in MEASURE_LABELS for m in KNOWN_MEASURES)
        assert all(m in MEASURE_UNITS for m in KNOWN_MEASURES)
