# © 2025 HarmonyCares
# All rights reserved.

"""
GC notifications / AWV orphan analytics.

Backs ``notebooks/gc_notifications.py``: loads the runtime MBI lists
(HDAI October report, Census 11/1 pull, quads list) from the state
file under ``{logs}/tracking/``, then computes orphan / current REACH /
program-status / BAR-comparison / quads-analysis rollups.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry

AWV_HCPCS_CODES = ("G0438", "G0439")
DEFAULT_AWV_CUTOFF = date(2025, 1, 1)
STATE_FILENAME = "gc_notifications_awv_lists.json"


class GcNotificationsPlugins(PluginRegistry):
    """GC notifications / AWV orphan analytics."""

    # ---- runtime MBI lists -------------------------------------------

    def state_file(self) -> Path:
        try:
            logs_root = Path(self.storage.get_path("logs"))
        except Exception:  # ALLOWED: storage backend may be offline in tests
            logs_root = Path("/tmp")
        return logs_root / "tracking" / STATE_FILENAME

    def load_state(self, state_file: Path | None = None) -> dict[str, list[str]]:
        path = Path(state_file) if state_file else self.state_file()
        if not path.exists():
            return {"hdai_awv": [], "census_awv": [], "quads_list": []}
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return {"hdai_awv": [], "census_awv": [], "quads_list": []}
        return {
            "hdai_awv": data.get("hdai_awv", []),
            "census_awv": data.get("census_awv", []),
            "quads_list": data.get("quads_list", []),
        }

    # ---- orphan computation ------------------------------------------

    def orphan_records(
        self, census_awv: list[str], hdai_awv: list[str]
    ) -> list[str]:
        """In-census but NOT-in-HDAI MBIs."""
        return list(set(census_awv) - set(hdai_awv))

    # ---- claim rollups -----------------------------------------------

    def awv_claims_for(
        self,
        gold_path: Path,
        mbis: list[str],
        cutoff: date = DEFAULT_AWV_CUTOFF,
    ) -> pl.DataFrame:
        path = Path(gold_path) / "medical_claim.parquet"
        if not path.exists():
            return pl.DataFrame()
        return (
            pl.scan_parquet(path)
            .filter(
                pl.col("hcpcs_code").is_in(list(AWV_HCPCS_CODES))
                & (pl.col("claim_start_date") >= cutoff)
            )
            .filter(pl.col("person_id").is_in(mbis))
            .select(
                ["person_id", "hcpcs_code", "claim_start_date", "claim_end_date", "claim_id"]
            )
            .collect()
        )

    def awv_per_member(
        self, gold_path: Path, cutoff: date = DEFAULT_AWV_CUTOFF
    ) -> pl.DataFrame:
        path = Path(gold_path) / "medical_claim.parquet"
        if not path.exists():
            return pl.DataFrame()
        return (
            pl.scan_parquet(path)
            .filter(
                pl.col("hcpcs_code").is_in(list(AWV_HCPCS_CODES))
                & (pl.col("claim_start_date") >= cutoff)
            )
            .group_by("person_id")
            .agg(
                pl.col("claim_start_date").min().alias("first_awv_date_2025"),
                pl.col("claim_start_date").max().alias("last_awv_date_2025"),
                pl.col("hcpcs_code").n_unique().alias("awv_claim_count"),
            )
            .collect()
        )

    # ---- BAR (current REACH) -----------------------------------------

    def current_reach(self, silver_path: Path) -> pl.DataFrame:
        from acoharmony._expressions._current_reach import (
            build_current_reach_with_bar_expr,
        )

        path = Path(silver_path) / "bar.parquet"
        if not path.exists():
            return pl.DataFrame()
        bar = pl.scan_parquet(path)
        schema = bar.collect_schema().names()
        expr = build_current_reach_with_bar_expr(
            reference_date=date.today(), df_schema=schema
        )
        return (
            bar.filter(expr)
            .select(
                [
                    "bene_mbi",
                    "bene_first_name",
                    "bene_last_name",
                    "bene_state",
                    "bene_county_fips",
                    "bene_zip_5",
                    "start_date",
                    "end_date",
                    "voluntary_alignment_type",
                    "claims_based_flag",
                    "source_filename",
                ]
            )
            .collect()
        )

    def reach_with_awv(
        self, current_reach: pl.DataFrame, awv_per_member: pl.DataFrame
    ) -> pl.DataFrame:
        if current_reach.is_empty() or awv_per_member.is_empty():
            return pl.DataFrame()
        return current_reach.join(
            awv_per_member, left_on="bene_mbi", right_on="person_id", how="inner"
        )

    # ---- orphan classification ---------------------------------------

    def orphan_reach_breakdown(
        self,
        orphan_records: list[str],
        current_reach: pl.DataFrame,
        reach_with_awv_df: pl.DataFrame,
    ) -> dict[str, Any]:
        orphan_set = set(orphan_records)
        all_reach = (
            set(current_reach["bene_mbi"].to_list())
            if not current_reach.is_empty()
            else set()
        )
        awv_reach = (
            set(reach_with_awv_df["bene_mbi"].to_list())
            if not reach_with_awv_df.is_empty()
            else set()
        )
        in_reach = orphan_set & all_reach
        with_awv = orphan_set & awv_reach
        return {
            "orphan_count": len(orphan_set),
            "all_reach_count": len(all_reach),
            "reach_with_awv_count": len(awv_reach),
            "orphan_in_reach_count": len(in_reach),
            "orphan_in_reach_mbis": in_reach,
            "orphan_with_awv_count": len(with_awv),
        }

    def orphans_not_in_reach(
        self, orphan_records: list[str], orphan_in_reach: set[str]
    ) -> set[str]:
        return set(orphan_records) - set(orphan_in_reach)

    def program_status(
        self,
        gold_path: Path,
        orphan_not_in_reach: set[str],
    ) -> dict[str, Any]:
        path = Path(gold_path) / "consolidated_alignment.parquet"
        if not path.exists() or not orphan_not_in_reach:
            return {
                "df": pl.DataFrame(),
                "summary": pl.DataFrame(),
                "in_mssp": 0,
                "in_ffs": 0,
                "not_found": len(orphan_not_in_reach),
                "total": len(orphan_not_in_reach),
            }
        consolidated = pl.scan_parquet(path)
        df = (
            consolidated.filter(
                pl.col("bene_mbi").is_in(list(orphan_not_in_reach))
            )
            .select(
                [
                    "bene_mbi",
                    "bene_first_name",
                    "bene_last_name",
                    "bene_state",
                    "bene_zip_5",
                    "bene_county",
                    "current_program",
                    "is_currently_aligned",
                    "first_reach_date",
                    "last_reach_date",
                    "first_mssp_date",
                    "last_mssp_date",
                    "ever_reach",
                    "ever_mssp",
                    "ever_ffs",
                    "months_in_reach",
                    "months_in_mssp",
                    "months_in_ffs",
                    "death_date",
                ]
            )
            .unique(subset=["bene_mbi"])
            .collect()
        )
        found = set(df["bene_mbi"].to_list())
        not_found = orphan_not_in_reach - found
        summary = (
            df.group_by("current_program")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
        )
        in_mssp = df.filter(pl.col("current_program") == "MSSP").height
        in_ffs = df.filter(
            pl.col("current_program").is_null() | (pl.col("current_program") == "")
        ).height
        return {
            "df": df,
            "summary": summary,
            "in_mssp": in_mssp,
            "in_ffs": in_ffs,
            "not_found": len(not_found),
            "total": len(orphan_not_in_reach),
        }

    # ---- BAR file comparison -----------------------------------------

    def bar_comparison(
        self,
        silver_path: Path,
        orphan_not_in_reach: set[str],
    ) -> dict[str, Any]:
        path = Path(silver_path) / "bar.parquet"
        if not path.exists() or not orphan_not_in_reach:
            return {
                "in_october_not_latest": set(),
                "in_latest_not_october": set(),
                "in_both": set(),
                "neither": len(orphan_not_in_reach),
            }
        bar = pl.scan_parquet(path)
        october = (
            bar.filter(pl.col("source_filename").str.contains(r"\.ALGC24\.RP\.D2410"))
            .filter(pl.col("bene_mbi").is_in(list(orphan_not_in_reach)))
            .select("bene_mbi")
            .unique()
            .collect()
        )
        max_algc = (
            bar.filter(pl.col("source_filename").str.contains(r"\.ALGC"))
            .select(pl.col("source_filename").max())
            .collect()
            .item()
        )
        latest = (
            bar.filter(pl.col("source_filename") == max_algc)
            .filter(pl.col("bene_mbi").is_in(list(orphan_not_in_reach)))
            .select("bene_mbi")
            .unique()
            .collect()
        )
        oct_set = set(october["bene_mbi"].to_list())
        latest_set = set(latest["bene_mbi"].to_list())
        in_both = oct_set & latest_set
        in_oct_not_latest = oct_set - latest_set
        in_latest_not_oct = latest_set - oct_set
        return {
            "in_october_not_latest": in_oct_not_latest,
            "in_latest_not_october": in_latest_not_oct,
            "in_both": in_both,
            "neither": len(orphan_not_in_reach)
            - len(in_oct_not_latest)
            - len(in_latest_not_oct)
            - len(in_both),
        }

    # ---- quads analysis ----------------------------------------------

    def quads_analysis(
        self,
        quads_list: list[str],
        current_reach: pl.DataFrame,
        awv_per_member_df: pl.DataFrame,
        reach_with_awv_df: pl.DataFrame,
    ) -> dict[str, Any]:
        quads = set(quads_list)
        reach_set = (
            set(current_reach["bene_mbi"].to_list())
            if not current_reach.is_empty()
            else set()
        )
        awv_set = (
            set(awv_per_member_df["person_id"].to_list())
            if not awv_per_member_df.is_empty()
            else set()
        )
        reach_awv_set = (
            set(reach_with_awv_df["bene_mbi"].to_list())
            if not reach_with_awv_df.is_empty()
            else set()
        )
        in_reach = quads & reach_set
        with_awv = quads & awv_set
        in_reach_with_awv = quads & reach_awv_set
        details = (
            reach_with_awv_df.filter(
                pl.col("bene_mbi").is_in(list(in_reach_with_awv))
            )
            if not reach_with_awv_df.is_empty()
            else pl.DataFrame()
        )
        return {
            "total": len(quads),
            "in_reach": len(in_reach),
            "with_awv": len(with_awv),
            "in_reach_with_awv": len(in_reach_with_awv),
            "details": details,
        }
