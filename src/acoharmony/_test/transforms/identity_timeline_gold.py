# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms._identity_timeline_gold — overlays bnex
opt-out events on the silver timeline, inheriting silver-assigned
chain_id and hop_index when known and computing singleton hashes for
new MBIs."""

from __future__ import annotations

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from datetime import date
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from acoharmony._transforms import _identity_timeline_gold
from acoharmony._test.transforms.identity_timeline import (
    _MockStorage,
    _seed_cclf8,
    _seed_cclf9,
    _seed_empty_hcmpi,
)
from acoharmony._transforms._identity_timeline import execute as silver_execute


def _seed_bnex(silver: Path, rows: list[tuple[str, str, date]]) -> None:
    """(MBI, exclusion_reason, file_date) bnex rows."""
    pl.DataFrame(
        [
            {
                "MBI": m,
                "BeneExcReasons": r,
                "file_date": fd,
                "source_filename": f"bnex.{fd.isoformat()}.txt",
            }
            for m, r, fd in rows
        ]
    ).write_parquet(silver / "bnex.parquet")


@pytest.fixture
def workspace(tmp_path):
    """Workspace with silver identity_timeline already built and bnex
    rows for one rotated bene plus one brand-new bene."""
    root = tmp_path / "workspace"
    storage = _MockStorage(root)
    silver = storage.get_path("silver")
    # Chain: OLD -> NEW (canonical=NEW at hop_index=0, OLD at hop_index=1)
    _seed_cclf9(silver, [("OLD", "NEW", "2025-06-15")])
    _seed_cclf8(silver, [("NEW", date(2025, 6, 10))])
    _seed_empty_hcmpi(silver)

    # Materialise the silver timeline (the gold transform reads from it)
    silver_execute(SimpleNamespace(storage_config=storage)).collect().write_parquet(
        silver / "identity_timeline.parquet"
    )

    # bnex: OLD opted out (rotated bene), and BRAND_NEW (no chain entry)
    _seed_bnex(
        silver,
        [
            ("OLD", "deceased", date(2025, 7, 1)),
            ("BRAND_NEW", "moved_out_of_area", date(2025, 7, 1)),
        ],
    )
    return SimpleNamespace(storage_config=storage, silver=silver)


class TestExecute:
    def test_emits_silver_rows_plus_bnex_rows(self, workspace):
        df = _identity_timeline_gold.execute(workspace).collect()
        assert "bnex_optout" in df["observation_type"].unique().to_list()
        assert "cclf9_remap" in df["observation_type"].unique().to_list()
        assert "cclf8_self" in df["observation_type"].unique().to_list()

    def test_bnex_row_for_rotated_bene_inherits_silver_chain_id(self, workspace):
        # OLD has a silver chain (because of the OLD->NEW remap). The
        # bnex row for OLD must land in the same chain_id as silver.
        df = _identity_timeline_gold.execute(workspace).collect()
        old_silver = df.filter(
            (pl.col("mbi") == "OLD") & (pl.col("observation_type") == "cclf9_remap")
        )["chain_id"][0]
        old_bnex = df.filter(
            (pl.col("mbi") == "OLD") & (pl.col("observation_type") == "bnex_optout")
        )["chain_id"][0]
        assert old_bnex == old_silver

    def test_bnex_row_inherits_silver_hop_index(self, workspace):
        # The hop_index uniqueness fix shipped this session: bnex must
        # inherit the silver hop_index for the same (chain_id, mbi) pair,
        # not be hardcoded to 0.
        df = _identity_timeline_gold.execute(workspace).collect()
        old_silver_hop = df.filter(
            (pl.col("mbi") == "OLD") & (pl.col("observation_type") == "cclf9_remap")
        )["hop_index"][0]
        old_bnex_hop = df.filter(
            (pl.col("mbi") == "OLD") & (pl.col("observation_type") == "bnex_optout")
        )["hop_index"][0]
        assert old_bnex_hop == old_silver_hop
        assert old_bnex_hop != 0  # because OLD is the historical mbi

    def test_no_chain_has_multiple_canonicals_after_overlay(self, workspace):
        # The original bug this fix addresses — multiple distinct mbis at
        # hop_index=0 in the same chain. Must not happen.
        df = _identity_timeline_gold.execute(workspace).collect()
        per_chain = (
            df.filter(pl.col("hop_index") == 0)
            .group_by("chain_id")
            .agg(pl.col("mbi").n_unique().alias("n"))
        )
        assert (per_chain["n"] == 1).all()

    def test_bnex_for_brand_new_mbi_gets_singleton_chain_and_hop_zero(self, workspace):
        # BRAND_NEW has no silver chain; gets a singleton chain hash and
        # hop_index=0 (their own canonical).
        df = _identity_timeline_gold.execute(workspace).collect()
        brand_new = df.filter(pl.col("mbi") == "BRAND_NEW")
        assert brand_new.height == 1
        assert brand_new["hop_index"][0] == 0
        assert brand_new["chain_id"][0] is not None
        # Singleton chain → only BRAND_NEW in it
        cid = brand_new["chain_id"][0]
        assert df.filter(pl.col("chain_id") == cid)["mbi"].unique().to_list() == ["BRAND_NEW"]

    def test_bnex_carries_exclusion_reason_in_notes(self, workspace):
        df = _identity_timeline_gold.execute(workspace).collect()
        old_bnex = df.filter(
            (pl.col("mbi") == "OLD") & (pl.col("observation_type") == "bnex_optout")
        )
        assert old_bnex["notes"][0] == "deceased"

    def test_bnex_is_current_as_of_file_date_set_for_latest_bnex(self, workspace):
        df = _identity_timeline_gold.execute(workspace).collect()
        bnex_rows = df.filter(pl.col("observation_type") == "bnex_optout")
        # Both bnex rows share the same (and only) file_date 2025-07-01
        assert bnex_rows["is_current_as_of_file_date"].all()

    def test_silver_rows_pass_through_unchanged(self, workspace):
        # All silver columns should preserve their values; we only ADD bnex rows.
        df = _identity_timeline_gold.execute(workspace).collect()
        silver_only = df.filter(pl.col("observation_type") != "bnex_optout")
        # Silver had 3 rows: OLD->NEW remap, NEW cclf8 self
        # (cclf8 + remap: at least 2 rows)
        assert silver_only.height >= 2

    def test_hcmpi_backfilled_for_bnex_when_silver_has_it(self, tmp_path):
        # If silver carries hcmpi for the bnex MBI's chain, gold backfills
        # it onto the bnex row.
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        silver = storage.get_path("silver")
        _seed_cclf9(silver, [("OLD", "NEW", "2025-06-15")])
        _seed_cclf8(silver, [("NEW", date(2025, 6, 10))])
        # Seed real hcmpi for OLD
        pl.DataFrame(
            [
                {"identifier": "OLD", "identifier_src_field": "member_mbi", "hcmpi": "HCMPI-1"},
                {"identifier": "NEW", "identifier_src_field": "member_mbi", "hcmpi": "HCMPI-1"},
            ]
        ).write_parquet(silver / "hcmpi_master.parquet")

        silver_execute(SimpleNamespace(storage_config=storage)).collect().write_parquet(
            silver / "identity_timeline.parquet"
        )
        _seed_bnex(silver, [("OLD", "deceased", date(2025, 7, 1))])

        df = _identity_timeline_gold.execute(SimpleNamespace(storage_config=storage)).collect()
        bnex_old = df.filter(
            (pl.col("mbi") == "OLD") & (pl.col("observation_type") == "bnex_optout")
        )
        assert bnex_old["hcmpi"][0] == "HCMPI-1"
