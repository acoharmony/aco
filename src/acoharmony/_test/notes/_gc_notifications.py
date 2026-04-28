# © 2025 HarmonyCares
"""Tests for acoharmony._notes._gc_notifications (GcNotificationsPlugins)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from acoharmony._notes import GcNotificationsPlugins


def _bar_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "bene_mbi": ["M1", "M2", "M3"],
            "bene_first_name": ["A", "B", "C"],
            "bene_last_name": ["X", "Y", "Z"],
            "bene_state": ["MI"] * 3,
            "bene_county_fips": ["001"] * 3,
            "bene_zip_5": ["48226"] * 3,
            "start_date": [date(2024, 1, 1)] * 3,
            "end_date": [None, None, None],
            "voluntary_alignment_type": ["A"] * 3,
            "claims_based_flag": ["Y"] * 3,
            "source_filename": ["P.D0259.ALGC25.RP.D250601"] * 3,
            "bene_date_of_death": [None, None, None],
            "file_date": [date(2025, 6, 1)] * 3,
        }
    )


# ---------------------------------------------------------------------------
# State file
# ---------------------------------------------------------------------------


class TestStateFile:
    @pytest.mark.unit
    def test_uses_storage_logs(self) -> None:
        plugin = GcNotificationsPlugins()
        fake = MagicMock()
        fake.get_path.return_value = "/x/logs"
        plugin._storage = fake
        out = plugin.state_file()
        assert out == Path("/x/logs/tracking/gc_notifications_awv_lists.json")

    @pytest.mark.unit
    def test_storage_failure_falls_back(self) -> None:
        plugin = GcNotificationsPlugins()
        fake = MagicMock()
        fake.get_path.side_effect = RuntimeError("boom")
        plugin._storage = fake
        out = plugin.state_file()
        assert out == Path("/tmp/tracking/gc_notifications_awv_lists.json")


class TestLoadState:
    @pytest.mark.unit
    def test_missing_file(self, tmp_path: Path) -> None:
        out = GcNotificationsPlugins().load_state(tmp_path / "x.json")
        assert out == {"hdai_awv": [], "census_awv": [], "quads_list": []}

    @pytest.mark.unit
    def test_corrupt_file(self, tmp_path: Path) -> None:
        f = tmp_path / "x.json"
        f.write_text("{not json")
        assert GcNotificationsPlugins().load_state(f)["hdai_awv"] == []

    @pytest.mark.unit
    def test_loads(self, tmp_path: Path) -> None:
        f = tmp_path / "x.json"
        f.write_text(
            '{"hdai_awv": ["M1"], "census_awv": ["M2"], "quads_list": ["M3"]}'
        )
        out = GcNotificationsPlugins().load_state(f)
        assert out["hdai_awv"] == ["M1"]
        assert out["quads_list"] == ["M3"]


# ---------------------------------------------------------------------------
# Orphan computation
# ---------------------------------------------------------------------------


class TestOrphanRecords:
    @pytest.mark.unit
    def test_orphan_set(self) -> None:
        out = GcNotificationsPlugins().orphan_records(
            census_awv=["M1", "M2", "M3"], hdai_awv=["M1"]
        )
        assert sorted(out) == ["M2", "M3"]


# ---------------------------------------------------------------------------
# Claim rollups
# ---------------------------------------------------------------------------


class TestAwvClaims:
    @pytest.mark.unit
    def test_no_file(self, tmp_path: Path) -> None:
        out = GcNotificationsPlugins().awv_claims_for(tmp_path, ["M1"])
        assert out.is_empty()

    @pytest.mark.unit
    def test_filter_to_codes(self, tmp_path: Path) -> None:
        pl.DataFrame(
            {
                "person_id": ["M1", "M1", "M2"],
                "hcpcs_code": ["G0438", "OTHER", "G0439"],
                "claim_start_date": [
                    date(2025, 6, 1),
                    date(2025, 7, 1),
                    date(2024, 1, 1),
                ],
                "claim_end_date": [date(2025, 6, 1)] * 3,
                "claim_id": ["C1", "C2", "C3"],
            }
        ).write_parquet(tmp_path / "medical_claim.parquet")
        out = GcNotificationsPlugins().awv_claims_for(tmp_path, ["M1", "M2"])
        # M1 G0438 in 2025 only (M2 G0439 in 2024 < cutoff; OTHER not AWV)
        assert out.height == 1


class TestAwvPerMember:
    @pytest.mark.unit
    def test_missing(self, tmp_path: Path) -> None:
        out = GcNotificationsPlugins().awv_per_member(tmp_path)
        assert out.is_empty()

    @pytest.mark.unit
    def test_aggregates(self, tmp_path: Path) -> None:
        pl.DataFrame(
            {
                "person_id": ["M1", "M1", "M2"],
                "hcpcs_code": ["G0438", "G0439", "G0438"],
                "claim_start_date": [
                    date(2025, 6, 1),
                    date(2025, 8, 1),
                    date(2025, 7, 1),
                ],
                "claim_end_date": [date(2025, 6, 1)] * 3,
            }
        ).write_parquet(tmp_path / "medical_claim.parquet")
        out = GcNotificationsPlugins().awv_per_member(tmp_path)
        m1 = next(r for r in out.iter_rows(named=True) if r["person_id"] == "M1")
        assert m1["first_awv_date_2025"] == date(2025, 6, 1)
        assert m1["awv_claim_count"] == 2


# ---------------------------------------------------------------------------
# Current REACH
# ---------------------------------------------------------------------------


class TestCurrentReach:
    @pytest.mark.unit
    def test_no_file(self, tmp_path: Path) -> None:
        out = GcNotificationsPlugins().current_reach(tmp_path)
        assert out.is_empty()

    @pytest.mark.unit
    def test_with_file(self, tmp_path: Path) -> None:
        _bar_df().write_parquet(tmp_path / "bar.parquet")
        with patch(
            "acoharmony._expressions._current_reach.build_current_reach_with_bar_expr",
            return_value=pl.lit(True),
        ):
            out = GcNotificationsPlugins().current_reach(tmp_path)
        assert out.height == 3


class TestReachWithAwv:
    @pytest.mark.unit
    def test_empty_inputs(self) -> None:
        out = GcNotificationsPlugins().reach_with_awv(pl.DataFrame(), pl.DataFrame())
        assert out.is_empty()

    @pytest.mark.unit
    def test_inner_join(self) -> None:
        reach = _bar_df()
        awv = pl.DataFrame(
            {
                "person_id": ["M1"],
                "first_awv_date_2025": [date(2025, 6, 1)],
                "last_awv_date_2025": [date(2025, 6, 1)],
                "awv_claim_count": [1],
            }
        )
        out = GcNotificationsPlugins().reach_with_awv(reach, awv)
        assert out.height == 1


# ---------------------------------------------------------------------------
# Orphan classification
# ---------------------------------------------------------------------------


class TestOrphanReachBreakdown:
    @pytest.mark.unit
    def test_breakdown(self) -> None:
        reach = _bar_df()
        awv_reach = reach.head(1)
        out = GcNotificationsPlugins().orphan_reach_breakdown(
            ["M1", "M2", "M99"], reach, awv_reach
        )
        assert out["orphan_count"] == 3
        assert out["all_reach_count"] == 3
        assert out["orphan_in_reach_count"] == 2
        assert out["orphan_with_awv_count"] == 1


class TestOrphansNotInReach:
    @pytest.mark.unit
    def test_diff(self) -> None:
        out = GcNotificationsPlugins().orphans_not_in_reach(
            ["M1", "M2", "M3"], {"M1"}
        )
        assert out == {"M2", "M3"}


class TestProgramStatus:
    @pytest.mark.unit
    def test_no_file(self, tmp_path: Path) -> None:
        out = GcNotificationsPlugins().program_status(tmp_path, {"M1"})
        assert out["not_found"] == 1
        assert out["df"].is_empty()

    @pytest.mark.unit
    def test_empty_set(self, tmp_path: Path) -> None:
        out = GcNotificationsPlugins().program_status(tmp_path, set())
        assert out["total"] == 0

    @pytest.mark.unit
    def test_classifies(self, tmp_path: Path) -> None:
        pl.DataFrame(
            {
                "bene_mbi": ["M1", "M2"],
                "bene_first_name": ["A", "B"],
                "bene_last_name": ["X", "Y"],
                "bene_state": ["MI", "MI"],
                "bene_zip_5": ["48226", "48226"],
                "bene_county": ["Wayne", "Wayne"],
                "current_program": ["MSSP", None],
                "is_currently_aligned": [True, True],
                "first_reach_date": [None, None],
                "last_reach_date": [None, None],
                "first_mssp_date": [date(2024, 1, 1), None],
                "last_mssp_date": [date(2024, 6, 1), None],
                "ever_reach": [False, False],
                "ever_mssp": [True, False],
                "ever_ffs": [False, True],
                "months_in_reach": [0, 0],
                "months_in_mssp": [6, 0],
                "months_in_ffs": [0, 6],
                "death_date": [None, None],
            }
        ).write_parquet(tmp_path / "consolidated_alignment.parquet")
        out = GcNotificationsPlugins().program_status(tmp_path, {"M1", "M2", "M99"})
        assert out["in_mssp"] == 1
        assert out["in_ffs"] == 1
        assert out["not_found"] == 1


# ---------------------------------------------------------------------------
# BAR comparison
# ---------------------------------------------------------------------------


class TestBarComparison:
    @pytest.mark.unit
    def test_no_file(self, tmp_path: Path) -> None:
        out = GcNotificationsPlugins().bar_comparison(tmp_path, {"M1"})
        assert out["neither"] == 1

    @pytest.mark.unit
    def test_empty_orphans(self, tmp_path: Path) -> None:
        out = GcNotificationsPlugins().bar_comparison(tmp_path, set())
        assert out["neither"] == 0

    @pytest.mark.unit
    def test_with_data(self, tmp_path: Path) -> None:
        pl.DataFrame(
            {
                "bene_mbi": ["M1", "M2", "M3"],
                "source_filename": [
                    "P.D0259.ALGC24.RP.D241018",
                    "P.D0259.ALGC25.RP.D250601",
                    "P.D0259.ALGC24.RP.D241018",
                ],
            }
        ).write_parquet(tmp_path / "bar.parquet")
        out = GcNotificationsPlugins().bar_comparison(tmp_path, {"M1", "M2", "M3"})
        assert "M2" in out["in_latest_not_october"]
        assert "M1" in out["in_october_not_latest"]


# ---------------------------------------------------------------------------
# Quads
# ---------------------------------------------------------------------------


class TestQuadsAnalysis:
    @pytest.mark.unit
    def test_basic(self) -> None:
        reach = _bar_df()
        awv = pl.DataFrame(
            {
                "person_id": ["M1"],
                "first_awv_date_2025": [date(2025, 6, 1)],
                "last_awv_date_2025": [date(2025, 6, 1)],
                "awv_claim_count": [1],
            }
        )
        reach_with_awv = GcNotificationsPlugins().reach_with_awv(reach, awv)
        out = GcNotificationsPlugins().quads_analysis(
            ["M1", "M2", "M99"], reach, awv, reach_with_awv
        )
        assert out["total"] == 3
        assert out["in_reach"] == 2
        assert out["with_awv"] == 1
        assert out["in_reach_with_awv"] == 1

    @pytest.mark.unit
    def test_empty_reach(self) -> None:
        out = GcNotificationsPlugins().quads_analysis(
            ["M1"], pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
        )
        assert out["in_reach"] == 0
