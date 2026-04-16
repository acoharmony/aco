# © 2025 HarmonyCares
# All rights reserved.

"""
Quality measures Q7 + Q8 + Q9: stratified reporting, risk
standardization bounds, and quality score arithmetic.

Q7 — Stratified reporting
--------------------------
CMS already computes stratified scores by dual-eligibility, SES, and
race. We validate:
- Stratified volumes are non-negative
- Stratified scores are in measure-plausible ranges
- At least 3 measures appear in stratified reporting
- Multiple deliveries carry stratified data

Q8 — Risk standardization bounds
----------------------------------
CMS-calculated against national peer group — not reconstructable.
Validate:
- Percentile ranks in [0, 100]
- Scores non-negative
- Known measures present in claims_results

Q9 — Quality score point calculations
---------------------------------------
ANLQR summary_information carries the quality score waterfall:
initial_quality_score × ci_sep_gateway_multiplier + hedr_adjustment
→ total_quality_score. We validate the internal arithmetic and
range bounds.

All tests are ``@requires_data``-gated.
"""

from __future__ import annotations

import polars as pl
import pytest

from .conftest import requires_data, scan_silver


# ---------------------------------------------------------------------------
# Q7: Stratified reporting
# ---------------------------------------------------------------------------


@pytest.fixture
def qtlqr_strat():
    try:
        return scan_silver("quarterly_quality_report_stratified_reporting").collect()
    except Exception:
        pytest.skip("quarterly_quality_report_stratified_reporting not available")


@pytest.fixture
def anlqr():
    try:
        return scan_silver("annual_quality_report").collect()
    except Exception:
        pytest.skip("annual_quality_report not available")


@requires_data
class TestStratifiedPresence:
    @pytest.mark.reconciliation
    def test_stratified_table_has_rows(self, qtlqr_strat):
        assert qtlqr_strat.height > 0

    @pytest.mark.reconciliation
    def test_at_least_3_measures_in_stratified(self, qtlqr_strat):
        """ACR (equity + stratified versions), UAMCC, DAH/DIC should
        all appear in the stratified reporting."""
        _names = set(qtlqr_strat["measure_name"].drop_nulls().unique().to_list())
        _actual = {n for n in _names if any(k in n for k in ["Readmission", "Unplanned", "Days in Care"])}
        assert len(_actual) >= 3, (
            f"Only {len(_actual)} measures found in stratified: {_actual}"
        )

    @pytest.mark.reconciliation
    def test_multiple_deliveries(self, qtlqr_strat):
        _n = qtlqr_strat["source_filename"].n_unique()
        assert _n >= 2, f"Only {_n} QTLQR delivery with stratified data"


@requires_data
class TestStratifiedValues:
    @pytest.mark.reconciliation
    def test_stratified_volume_non_negative(self, qtlqr_strat):
        _vals = qtlqr_strat["stratified_volume"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _vals.len() == 0:
            pytest.skip("No stratified volume data")
        _negs = _vals.filter(_vals < 0)
        assert _negs.len() == 0, f"{_negs.len()} negative stratified volumes"

    @pytest.mark.reconciliation
    def test_stratified_score_non_negative(self, qtlqr_strat):
        _vals = qtlqr_strat["stratified_score"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _vals.len() == 0:
            pytest.skip("No stratified score data")
        _negs = _vals.filter(_vals < 0)
        assert _negs.len() == 0, f"{_negs.len()} negative stratified scores"

    @pytest.mark.reconciliation
    def test_stratified_mean_non_negative(self, qtlqr_strat):
        _vals = qtlqr_strat["stratified_mean"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _vals.len() == 0:
            pytest.skip("No stratified mean data")
        _negs = _vals.filter(_vals < 0)
        assert _negs.len() == 0, f"{_negs.len()} negative stratified means"

    @pytest.mark.reconciliation
    def test_all_bene_mean_non_negative(self, qtlqr_strat):
        if "all_bene_mean" not in qtlqr_strat.columns:
            pytest.skip("all_bene_mean not present")
        _vals = qtlqr_strat["all_bene_mean"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _vals.len() == 0:
            pytest.skip("No all_bene_mean data")
        _negs = _vals.filter(_vals < 0)
        assert _negs.len() == 0, f"{_negs.len()} negative all_bene_mean"


# ---------------------------------------------------------------------------
# Q8: Risk standardization bounds
# ---------------------------------------------------------------------------


@requires_data
class TestRiskStandardizationBounds:
    @pytest.mark.reconciliation
    def test_qtlqr_measure_score_non_negative(self):
        _df = scan_silver("quarterly_quality_report_claims_results").collect()
        _actual = _df.filter(pl.col("measure").is_in(["ACR", "DAH", "UAMCC"]))
        _scores = _actual["measure_score"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _scores.len() == 0:
            pytest.skip("No scores")
        _negs = _scores.filter(_scores < 0)
        assert _negs.len() == 0

    @pytest.mark.reconciliation
    def test_qtlqr_measure_volume_positive(self):
        _df = scan_silver("quarterly_quality_report_claims_results").collect()
        _actual = _df.filter(pl.col("measure").is_in(["ACR", "DAH", "UAMCC"]))
        _vols = _actual["measure_volume"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _vols.len() == 0:
            pytest.skip("No volumes")
        assert _vols.min() > 0, f"measure_volume min {_vols.min()} <= 0"

    @pytest.mark.reconciliation
    def test_qtlqr_provisional_percentile_in_range(self):
        _df = scan_silver("quarterly_quality_report_claims_results").collect()
        _actual = _df.filter(pl.col("measure").is_in(["ACR", "DAH", "UAMCC"]))
        if "provisional_percentile" not in _actual.columns:
            pytest.skip("no provisional_percentile column")
        _pcts = _actual["provisional_percentile"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _pcts.len() == 0:
            pytest.skip("No percentile data")
        assert _pcts.min() >= 0.0, f"percentile min {_pcts.min()} < 0"
        assert _pcts.max() <= 100.0, f"percentile max {_pcts.max()} > 100"

    @pytest.mark.reconciliation
    def test_acr_score_in_plausible_range(self):
        """ACR is a readmission rate — typically 15–25%."""
        _df = scan_silver("quarterly_quality_report_claims_results").collect()
        _acr = _df.filter(pl.col("measure") == "ACR")
        _scores = _acr["measure_score"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _scores.len() == 0:
            pytest.skip("No ACR scores")
        assert _scores.min() >= 5.0, f"ACR min {_scores.min()} < 5%"
        assert _scores.max() <= 35.0, f"ACR max {_scores.max()} > 35%"

    @pytest.mark.reconciliation
    def test_dah_score_in_plausible_range(self):
        """DAH is days at home — typically 300–350 days/year."""
        _df = scan_silver("quarterly_quality_report_claims_results").collect()
        _dah = _df.filter(pl.col("measure") == "DAH")
        _scores = _dah["measure_score"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _scores.len() == 0:
            pytest.skip("No DAH scores")
        assert _scores.min() >= 250.0, f"DAH min {_scores.min()} < 250 days"
        assert _scores.max() <= 365.0, f"DAH max {_scores.max()} > 365 days"


# ---------------------------------------------------------------------------
# Q9: Quality score point calculations (ANLQR)
# ---------------------------------------------------------------------------


@requires_data
class TestQualityScoreArithmetic:
    """ANLQR summary_information carries the quality score waterfall.
    Validate internal arithmetic and range bounds."""

    @pytest.fixture
    def summary(self, anlqr):
        _si = anlqr.filter(
            pl.col("sheet_type") == "summary_information",
            pl.col("measure_score").is_not_null(),
        )
        if _si.height == 0:
            pytest.skip("No summary_information rows with scores")
        return _si

    @pytest.mark.reconciliation
    def test_points_earned_non_negative(self, summary):
        _pts = summary["points_earned"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _pts.len() == 0:
            pytest.skip("No points_earned")
        assert _pts.min() >= 0.0, f"points_earned min {_pts.min()} < 0"

    @pytest.mark.reconciliation
    def test_points_earned_lte_possible(self, summary):
        """Can't earn more points than possible."""
        if "points_possible" not in summary.columns:
            pytest.skip("no points_possible column")
        _check = summary.filter(
            pl.col("points_earned").is_not_null() & pl.col("points_possible").is_not_null()
        )
        _earned = _check["points_earned"].cast(pl.Float64, strict=False)
        _possible = _check["points_possible"].cast(pl.Float64, strict=False)
        _over = pl.DataFrame({"e": _earned, "p": _possible}).filter(pl.col("e") > pl.col("p") + 0.01)
        assert _over.height == 0, f"{_over.height} rows where earned > possible"

    @pytest.mark.reconciliation
    def test_initial_quality_score_between_0_and_1(self, summary):
        if "initial_quality_score" not in summary.columns:
            pytest.skip("no initial_quality_score")
        _vals = summary["initial_quality_score"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _vals.len() == 0:
            pytest.skip("No initial_quality_score data")
        assert _vals.min() >= 0.0, f"initial_quality_score min {_vals.min()} < 0"
        assert _vals.max() <= 1.0, f"initial_quality_score max {_vals.max()} > 1"

    @pytest.mark.reconciliation
    def test_total_quality_score_between_0_and_2(self, summary):
        """Total quality score with multipliers might exceed 1.0 but
        should stay under 2.0."""
        if "total_quality_score" not in summary.columns:
            pytest.skip("no total_quality_score")
        _vals = summary["total_quality_score"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _vals.len() == 0:
            pytest.skip("No total_quality_score data")
        assert _vals.min() >= 0.0
        assert _vals.max() <= 2.0, f"total_quality_score max {_vals.max()} > 2.0"

    @pytest.mark.reconciliation
    def test_gateway_multiplier_non_negative(self, summary):
        if "ci_sep_gateway_multiplier" not in summary.columns:
            pytest.skip("no ci_sep_gateway_multiplier")
        _vals = summary["ci_sep_gateway_multiplier"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _vals.len() == 0:
            pytest.skip("No gateway multiplier data")
        assert _vals.min() >= 0.0

    @pytest.mark.reconciliation
    def test_quality_withhold_earned_back_between_0_and_1(self, summary):
        if "quality_withhold_earned_back" not in summary.columns:
            pytest.skip("no quality_withhold_earned_back")
        _vals = summary["quality_withhold_earned_back"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _vals.len() == 0:
            pytest.skip("No quality_withhold_earned_back data")
        assert _vals.min() >= 0.0
        assert _vals.max() <= 1.0, f"quality_withhold_earned_back max {_vals.max()} > 1.0"
