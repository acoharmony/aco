# © 2025 HarmonyCares
"""Tests for acoharmony._notes._wound_care (WoundCarePlugins)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from acoharmony._notes import WoundCarePlugins


def _cclf_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "member_id": ["M1", "M1", "M2", "M3"],
            "rendering_npi": ["N1", "N1", "N1", "N2"],
            "hcpcs_code": ["A1", "A1", "A2", "A3"],
            "claim_end_date": [
                date(2025, 1, 1),
                date(2025, 1, 4),
                date(2025, 5, 1),
                date(2025, 6, 1),
            ],
            "paid_amount": [100.0, 100.0, 250.0, 1500000.0],
        }
    )


def _hdai_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mbi": ["M1"],
            "claim_id": ["C1"],
            "claim_through_date": [date(2025, 1, 1)],
            "hcpcs_code": ["A1"],
            "line_payment_amount": [100.0],
            "rendering_npi": ["N1"],
            "claim_status": ["Adjudicated"],
        }
    )


# ---------------------------------------------------------------------------
# date / cohort
# ---------------------------------------------------------------------------


class TestResolveDateRange:
    @pytest.mark.unit
    def test_preset(self) -> None:
        out = WoundCarePlugins().resolve_date_range("2025")
        assert out == (date(2025, 1, 1), date(2025, 12, 31))

    @pytest.mark.unit
    def test_tuple_passthrough(self) -> None:
        rng = (date(2024, 6, 1), date(2024, 6, 30))
        assert WoundCarePlugins().resolve_date_range(rng) == rng

    @pytest.mark.unit
    def test_invalid_raises(self) -> None:
        with pytest.raises(ValueError):
            WoundCarePlugins().resolve_date_range("nope")


class TestCohortMbis:
    @pytest.mark.unit
    def test_all_returns_none(self, tmp_path: Path) -> None:
        assert WoundCarePlugins().cohort_mbis("all", tmp_path) is None

    @pytest.mark.unit
    def test_reach_current(self, tmp_path: Path) -> None:
        pl.DataFrame(
            {
                "current_mbi": ["M1", "M2"],
                "observable_end": [date(2025, 6, 30), date(2025, 6, 30)],
                "ym_202506_reach": [True, False],
                "ever_reach": [True, False],
            }
        ).write_parquet(tmp_path / "consolidated_alignment.parquet")
        out = WoundCarePlugins().cohort_mbis("reach_current", tmp_path)
        assert out == ["M1"]

    @pytest.mark.unit
    def test_reach_ever(self, tmp_path: Path) -> None:
        pl.DataFrame(
            {
                "current_mbi": ["M1", "M2"],
                "observable_end": [date(2025, 6, 30), date(2025, 6, 30)],
                "ym_202506_reach": [True, False],
                "ever_reach": [True, True],
            }
        ).write_parquet(tmp_path / "consolidated_alignment.parquet")
        out = WoundCarePlugins().cohort_mbis("reach_ever", tmp_path)
        assert sorted(out) == ["M1", "M2"]

    @pytest.mark.unit
    def test_invalid_cohort(self, tmp_path: Path) -> None:
        pl.DataFrame(
            {
                "current_mbi": ["M1"],
                "observable_end": [date(2025, 6, 30)],
                "ym_202506_reach": [True],
                "ever_reach": [True],
            }
        ).write_parquet(tmp_path / "consolidated_alignment.parquet")
        with pytest.raises(ValueError):
            WoundCarePlugins().cohort_mbis("nope", tmp_path)


# ---------------------------------------------------------------------------
# filtered_claims
# ---------------------------------------------------------------------------


class TestFilteredClaims:
    @pytest.mark.unit
    def test_cclf_filter(self, tmp_path: Path) -> None:
        _cclf_df().write_parquet(tmp_path / "skin_substitute_claims.parquet")
        out = WoundCarePlugins().filtered_claims(
            "skin_substitute", "2025", "all", "cclf", tmp_path, tmp_path
        )
        assert out is not None
        assert out.height == 4

    @pytest.mark.unit
    def test_cclf_wound_care_file(self, tmp_path: Path) -> None:
        _cclf_df().write_parquet(tmp_path / "wound_care_claims.parquet")
        out = WoundCarePlugins().filtered_claims(
            "wound_care", "2025", "all", "cclf", tmp_path, tmp_path
        )
        assert out is not None
        assert out.height == 4

    @pytest.mark.unit
    def test_hdai_only(self, tmp_path: Path) -> None:
        with patch(
            "acoharmony._notes._wound_care.pl.read_excel",
            return_value=pl.DataFrame(
                {
                    "MBI NUM": ["M1"],
                    "Claim ID": ["C1"],
                    "Claim Through Date": [date(2025, 1, 1)],
                    "HCPCS Code": ["A1"],
                    "Line Payment Amount": [100.0],
                    "Rendering Provider NPI": ["N1"],
                    "Claim Status": ["Adjudicated"],
                }
            ),
        ):
            out = WoundCarePlugins().filtered_claims(
                "skin_substitute",
                "2025",
                "all",
                "hdai",
                tmp_path,
                tmp_path,
                skin_substitute_codes=["A1"],
            )
        assert out is not None
        assert out.height == 1

    @pytest.mark.unit
    def test_hdai_cohort_filter_applied(self, tmp_path: Path) -> None:
        # HDAI source + reach_ever cohort → covers line 130 (hdai cohort filter)
        pl.DataFrame(
            {
                "current_mbi": ["M1"],
                "observable_end": [date(2025, 6, 30)],
                "ym_202506_reach": [True],
                "ever_reach": [True],
            }
        ).write_parquet(tmp_path / "consolidated_alignment.parquet")
        with patch(
            "acoharmony._notes._wound_care.pl.read_excel",
            return_value=pl.DataFrame(
                {
                    "MBI NUM": ["M1", "OTHER"],
                    "Claim ID": ["C1", "C2"],
                    "Claim Through Date": [date(2025, 1, 1), date(2025, 1, 1)],
                    "HCPCS Code": ["A1", "A1"],
                    "Line Payment Amount": [100.0, 200.0],
                    "Rendering Provider NPI": ["N1", "N2"],
                    "Claim Status": ["Adjudicated"] * 2,
                }
            ),
        ):
            out = WoundCarePlugins().filtered_claims(
                "skin_substitute",
                "2025",
                "reach_ever",
                "hdai",
                tmp_path,
                tmp_path,
                skin_substitute_codes=["A1"],
            )
        assert out is not None
        # Cohort filter drops "OTHER", keeps M1
        assert out.height == 1

    @pytest.mark.unit
    def test_matched(self, tmp_path: Path) -> None:
        _cclf_df().head(1).write_parquet(tmp_path / "skin_substitute_claims.parquet")
        with patch(
            "acoharmony._notes._wound_care.pl.read_excel",
            return_value=pl.DataFrame(
                {
                    "MBI NUM": ["M1"],
                    "Claim ID": ["C1"],
                    "Claim Through Date": [date(2025, 1, 1)],
                    "HCPCS Code": ["A1"],
                    "Line Payment Amount": [100.0],
                    "Rendering Provider NPI": ["N1"],
                    "Claim Status": ["Adjudicated"],
                }
            ),
        ):
            out = WoundCarePlugins().filtered_claims(
                "skin_substitute",
                "2025",
                "all",
                "matched",
                tmp_path,
                tmp_path,
            )
        assert out is not None
        assert out.height == 1

    @pytest.mark.unit
    def test_cohort_filter_applied(self, tmp_path: Path) -> None:
        _cclf_df().write_parquet(tmp_path / "skin_substitute_claims.parquet")
        pl.DataFrame(
            {
                "current_mbi": ["M1"],
                "observable_end": [date(2025, 6, 30)],
                "ym_202506_reach": [True],
                "ever_reach": [True],
            }
        ).write_parquet(tmp_path / "consolidated_alignment.parquet")
        out = WoundCarePlugins().filtered_claims(
            "skin_substitute",
            "2025",
            "reach_ever",
            "cclf",
            tmp_path,
            tmp_path,
        )
        assert out["member_id"].unique().to_list() == ["M1"]


# ---------------------------------------------------------------------------
# claim_summary
# ---------------------------------------------------------------------------


class TestClaimSummary:
    @pytest.mark.unit
    def test_summary_with_data(self, tmp_path: Path) -> None:
        _cclf_df().write_parquet(tmp_path / "skin_substitute_claims.parquet")
        out = WoundCarePlugins().claim_summary(
            "skin_substitute", "2025", "all", "cclf", tmp_path, tmp_path
        )
        assert out["claims_df"] is not None
        assert out["summary_stats"].height == 7
        assert out["top_npis"].height == 2
        assert out["top_hcpcs"].height == 3

    @pytest.mark.unit
    def test_summary_empty(self, tmp_path: Path) -> None:
        # No data → bare files
        empty = pl.DataFrame(
            schema={
                "member_id": pl.Utf8,
                "rendering_npi": pl.Utf8,
                "hcpcs_code": pl.Utf8,
                "claim_end_date": pl.Date,
                "paid_amount": pl.Float64,
            }
        )
        empty.write_parquet(tmp_path / "skin_substitute_claims.parquet")
        out = WoundCarePlugins().claim_summary(
            "skin_substitute", "2025", "all", "cclf", tmp_path, tmp_path
        )
        assert out["claims_df"] is None


# ---------------------------------------------------------------------------
# pattern detection
# ---------------------------------------------------------------------------


class TestPatternDetection:
    @pytest.mark.unit
    def test_high_freq_none_input(self) -> None:
        assert WoundCarePlugins().high_frequency_providers(None, "cclf") is None

    @pytest.mark.unit
    def test_high_freq_finds(self) -> None:
        df = pl.DataFrame(
            {
                "rendering_npi": ["N1"] * 16,
                "member_id": ["M1"] * 16,
                "hcpcs_code": ["A1"] * 16,
                "claim_end_date": [date(2025, 1, i + 1) for i in range(16)],
                "paid_amount": [100.0] * 16,
            }
        )
        out = WoundCarePlugins().high_frequency_providers(df, "cclf")
        assert out is not None
        assert out["npi_summary"].height == 1
        assert out["patient_level"]["application_count"][0] == 16

    @pytest.mark.unit
    def test_high_cost_none_input(self) -> None:
        assert WoundCarePlugins().high_cost_patients(None, "cclf") is None

    @pytest.mark.unit
    def test_high_cost_finds(self) -> None:
        out = WoundCarePlugins().high_cost_patients(
            _cclf_df(), "cclf", min_cost=1000.0
        )
        assert out is not None
        assert out.height == 1  # only M3 with $1.5M

    @pytest.mark.unit
    def test_clustered_none_input(self) -> None:
        assert WoundCarePlugins().clustered_claims(None, "cclf") is None

    @pytest.mark.unit
    def test_clustered_finds(self) -> None:
        df = pl.DataFrame(
            {
                "rendering_npi": ["N1"] * 4,
                "member_id": ["M1"] * 4,
                "hcpcs_code": ["A1"] * 4,
                "claim_end_date": [
                    date(2025, 1, 1),
                    date(2025, 1, 2),
                    date(2025, 1, 3),
                    date(2025, 1, 4),
                ],
                "paid_amount": [100.0] * 4,
            }
        )
        out = WoundCarePlugins().clustered_claims(df, "cclf", min_claims_in_week=3)
        assert out is not None
        assert out["cluster_details"].height >= 1

    @pytest.mark.unit
    def test_same_day_dups_none_input(self) -> None:
        assert WoundCarePlugins().same_day_duplicates(None, "cclf") is None

    @pytest.mark.unit
    def test_same_day_dups_finds(self) -> None:
        df = pl.DataFrame(
            {
                "rendering_npi": ["N1", "N1"],
                "member_id": ["M1", "M1"],
                "hcpcs_code": ["A1", "A1"],
                "claim_end_date": [date(2025, 1, 1), date(2025, 1, 1)],
                "paid_amount": [100.0, 100.0],
            }
        )
        out = WoundCarePlugins().same_day_duplicates(df, "cclf")
        assert out is not None
        assert out["duplicate_details"].height == 1

    @pytest.mark.unit
    def test_identical_none_input(self) -> None:
        assert WoundCarePlugins().identical_billing_patterns(None, "cclf") is None

    @pytest.mark.unit
    def test_identical_finds(self) -> None:
        df = pl.DataFrame(
            {
                "rendering_npi": ["N1"] * 12,
                "member_id": [f"M{i}" for i in range(12)],
                "hcpcs_code": ["A1"] * 12,
                "claim_end_date": [date(2025, 1, i + 1) for i in range(12)],
                "paid_amount": [100.0] * 12,
            }
        )
        out = WoundCarePlugins().identical_billing_patterns(df, "cclf")
        assert out is not None
        assert out["npi_summary"].height == 1


# ---------------------------------------------------------------------------
# NPI comparison
# ---------------------------------------------------------------------------


def _summary_with_npis(label_npis: list[tuple[str, float, int, int]]) -> dict:
    npis = pl.DataFrame(
        {
            "rendering_npi": [r[0] for r in label_npis],
            "claim_lines": [r[3] for r in label_npis],
            "total_paid": [r[1] for r in label_npis],
            "unique_patients": [r[2] for r in label_npis],
            "unique_hcpcs": [1] * len(label_npis),
        }
    )
    return {"top_npis": npis}


class TestNpiComparison:
    @pytest.mark.unit
    def test_no_data(self) -> None:
        out = WoundCarePlugins().npi_comparison(
            {"a": {"top_npis": None}, "b": {}}, [("a", "A"), ("b", "B")]
        )
        assert out is None

    @pytest.mark.unit
    def test_two_way(self) -> None:
        summaries = {
            "a": _summary_with_npis([("N1", 1000.0, 10, 100), ("N2", 500.0, 5, 50)]),
            "b": _summary_with_npis([("N1", 800.0, 8, 80)]),
        }
        out = WoundCarePlugins().npi_comparison(summaries, [("a", "A"), ("b", "B")])
        assert out is not None
        assert "Diff (A - B)" in out.columns

    @pytest.mark.unit
    def test_three_way(self) -> None:
        summaries = {
            "a": _summary_with_npis([("N1", 1000.0, 10, 100)]),
            "b": _summary_with_npis([("N1", 500.0, 5, 50)]),
            "c": _summary_with_npis([("N1", 250.0, 2, 25)]),
        }
        out = WoundCarePlugins().npi_comparison(
            summaries, [("a", "A"), ("b", "B"), ("c", "C")]
        )
        assert out is not None
        assert "Largest Value" in out.columns


class TestCodeSetRelationship:
    @pytest.mark.unit
    def test_overlap_counts(self) -> None:
        out = WoundCarePlugins().code_set_relationship(
            ["A", "B", "C"], ["B", "C", "D"]
        )
        assert out == {
            "wound_total": 3,
            "skin_total": 3,
            "overlap": 2,
            "wound_only": 1,
            "skin_only": 1,
        }


class TestColumnNames:
    @pytest.mark.unit
    def test_cclf(self) -> None:
        cols = WoundCarePlugins().column_names("cclf")
        assert cols["patient"] == "member_id"

    @pytest.mark.unit
    def test_hdai(self) -> None:
        cols = WoundCarePlugins().column_names("hdai")
        assert cols["patient"] == "mbi"

    @pytest.mark.unit
    def test_matched_alias(self) -> None:
        cols = WoundCarePlugins().column_names("matched")
        assert cols["patient"] == "mbi"
