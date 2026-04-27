# © 2025 HarmonyCares
# All rights reserved.

"""
HDAI REACH (Clinical Leadership Workgroup) analytics.

Backs ``notebooks/hdai_reach.py``: filters HDAI silver to the most
recent file_date, joins identity_timeline for HCMPI/current_mbi
enrichment, manages the runtime "already discussed" state file,
provides per-provider summaries, high-cost patient lists with
optional filters, utilization-trend rollups, and spend-breakdown
totals.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry

DEFAULT_HIGH_COST_COLS = (
    "mbi",
    "patient_first_name",
    "patient_last_name",
    "plurality_assigned_provider_name",
    "total_spend_ytd",
    "er_admits_ytd",
    "er_admits_90_day_prior",
    "any_inpatient_hospital_admits_ytd",
    "any_inpatient_hospital_admits_90_day_prior",
    "hospice_admission",
    "inpatient_spend_ytd",
    "outpatient_spend_ytd",
    "snf_cost_ytd",
    "home_health_spend_ytd",
    "em_visits_ytd",
    "last_em_visit",
    "aco_em_name",
    "aco_em_npi",
    "most_recent_awv_date",
    "awv_claim_id",
    "flag_em_hcmg",
)

OPTIONAL_HIGH_COST_COLS = (
    "already_discussed",
    "hcmpi",
    "current_mbi",
    "mapping_type",
    "cms_city",
    "cms_state",
    "cms_death_dt",
    "office_name",
    "office_market",
    "office_region",
)

SPEND_CATEGORIES = (
    ("inpatient_spend_ytd", "Inpatient"),
    ("outpatient_spend_ytd", "Outpatient"),
    ("snf_cost_ytd", "SNF"),
    ("home_health_spend_ytd", "Home Health"),
    ("hospice_spend_ytd", "Hospice"),
    ("dme_spend_ytd", "DME"),
    ("b_carrier_cost", "Part B"),
    ("em_cost_ytd", "E&M"),
)


class HdaiPlugins(PluginRegistry):
    """HDAI REACH workgroup analytics."""

    # ---- silver loading ------------------------------------------------

    def load_with_crosswalk(self, silver_path: Path) -> pl.LazyFrame:
        """
        Load ``hdai_reach.parquet`` left-joined to identity_timeline crosswalk.

        Returns the raw LazyFrame (un-filtered to most-recent-date) so callers
        can apply additional filters in lazy form. Empty LazyFrame when the
        silver file is missing.
        """
        path = Path(silver_path) / "hdai_reach.parquet"
        if not path.exists():
            return pl.LazyFrame()
        base = pl.scan_parquet(str(path))

        timeline_path = Path(silver_path) / "identity_timeline.parquet"
        if not timeline_path.exists():
            return base

        from acoharmony._transforms._identity_timeline import (
            current_mbi_with_hcmpi_lookup_lazy,
        )

        crosswalk = current_mbi_with_hcmpi_lookup_lazy(Path(silver_path))
        return base.join(
            crosswalk.select(
                pl.col("prvs_num"),
                pl.col("crnt_num").alias("current_mbi"),
                pl.col("hcmpi"),
            ),
            left_on="mbi",
            right_on="prvs_num",
            how="left",
        )

    def filter_to_most_recent(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Filter to ``file_date == max(file_date)``; idempotent."""
        try:
            schema = lf.collect_schema()
            if len(schema) == 0:
                return lf
        except Exception:
            return lf
        if "file_date" not in lf.collect_schema().names():
            return lf
        most_recent = lf.select(pl.col("file_date").max()).collect().item()
        return lf.filter(pl.col("file_date") == most_recent)

    def flag_already_discussed(
        self,
        lf: pl.LazyFrame,
        already_discussed: list[str],
    ) -> pl.LazyFrame:
        """Add ``already_discussed`` boolean column from a list of MBIs."""
        try:
            schema = lf.collect_schema()
            if len(schema) == 0:
                return lf.with_columns(pl.lit(False).alias("already_discussed"))
        except Exception:
            return lf.with_columns(pl.lit(False).alias("already_discussed"))
        return lf.with_columns(
            pl.col("mbi").is_in(already_discussed).alias("already_discussed")
        )

    # ---- discussed-state file management -------------------------------

    def load_discussed_state(self, state_file: Path) -> dict[str, dict[str, Any]]:
        """``{mbi: {"discussed_date": ..., "notes": ...}}``; empty when missing."""
        if not Path(state_file).exists():
            return {}
        try:
            return json.loads(Path(state_file).read_text())
        except (OSError, json.JSONDecodeError):
            return {}

    def save_discussed_state(
        self,
        state_file: Path,
        state: dict[str, dict[str, Any]],
    ) -> None:
        """Atomically write the discussed-patients state to disk (sorted keys)."""
        path = Path(state_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = (
            json.dumps({k: state[k] for k in sorted(state.keys())}, indent=2)
            + "\n"
        )
        fd, tmp_path = tempfile.mkstemp(
            prefix=".hdai_reach_discussed.",
            suffix=".json.tmp",
            dir=str(path.parent),
        )
        try:
            with os.fdopen(fd, "w") as f:
                f.write(payload)
            os.replace(tmp_path, path)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def discussed_state_file(self) -> Path:
        """Runtime path to the discussed-patients JSON (under logs/tracking)."""
        try:
            logs_root = Path(self.storage.get_path("logs"))
        except Exception:  # ALLOWED: storage backend may be offline in tests
            logs_root = Path("/tmp")
        return logs_root / "tracking" / "hdai_reach_discussed_state.json"

    def discussed_state_to_rows(
        self, state: dict[str, dict[str, Any]]
    ) -> list[dict[str, str]]:
        """Convert MBI-keyed state to row dicts (sorted by MBI) for data_editor."""
        rows = []
        for mbi in sorted(state.keys()):
            entry = state[mbi] or {}
            rows.append(
                {
                    "mbi": mbi,
                    "discussed_date": entry.get("discussed_date", ""),
                    "notes": entry.get("notes", ""),
                }
            )
        return rows

    def rows_to_discussed_state(
        self, rows: list[dict[str, Any]]
    ) -> dict[str, dict[str, str]]:
        """Inverse of ``discussed_state_to_rows``; drops blank MBIs, normalises case."""
        out: dict[str, dict[str, str]] = {}
        for row in rows:
            mbi_raw = row.get("mbi") or ""
            mbi = str(mbi_raw).strip().upper()
            if not mbi:
                continue
            out[mbi] = {
                "discussed_date": str(row.get("discussed_date") or "").strip(),
                "notes": str(row.get("notes") or "").strip(),
            }
        return out

    def mark_discussed(
        self,
        state_file: Path,
        mbi: str,
        notes: str = "",
        discussed_date: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Append/overwrite an MBI in the state file; returns the new state."""
        state = self.load_discussed_state(state_file)
        state[mbi] = {
            "discussed_date": discussed_date or datetime.now().strftime("%Y-%m-%d"),
            "notes": notes,
        }
        self.save_discussed_state(state_file, state)
        return state

    # ---- summaries ----------------------------------------------------

    def provider_summary(
        self,
        df: pl.DataFrame,
        provider_name: str | None = None,
    ) -> pl.DataFrame:
        """Per-provider patient counts, spend totals, ER/IP/hospice counts."""
        query = df
        if provider_name:
            query = query.filter(
                pl.col("plurality_assigned_provider_name") == provider_name
            )
        return (
            query.group_by("plurality_assigned_provider_name")
            .agg(
                pl.len().alias("patient_count"),
                pl.col("total_spend_ytd").sum().alias("total_spend"),
                pl.col("total_spend_ytd").mean().alias("avg_spend_per_patient"),
                pl.col("er_admits_ytd").sum().alias("total_er_admits"),
                pl.col("any_inpatient_hospital_admits_ytd")
                .sum()
                .alias("total_inpatient_admits"),
                pl.col("hospice_admission").sum().alias("hospice_admissions"),
            )
            .sort("total_spend", descending=True)
        )

    def high_cost_patients(
        self,
        df: pl.DataFrame,
        provider_name: str | None = None,
        top_n: int = 20,
        min_cost: float | None = None,
        max_cost: float | None = None,
        min_er_admits: int | None = None,
        min_inpatient_admits: int | None = None,
    ) -> pl.DataFrame:
        """Top-N highest-spend patients with optional provider/cost/utilization filters."""
        query = df
        if provider_name:
            query = query.filter(
                pl.col("plurality_assigned_provider_name") == provider_name
            )
        if min_cost is not None:
            query = query.filter(pl.col("total_spend_ytd") >= min_cost)
        if max_cost is not None:
            query = query.filter(pl.col("total_spend_ytd") <= max_cost)
        if min_er_admits is not None:
            query = query.filter(pl.col("er_admits_ytd") >= min_er_admits)
        if min_inpatient_admits is not None:
            query = query.filter(
                pl.col("any_inpatient_hospital_admits_ytd") >= min_inpatient_admits
            )
        cols = list(DEFAULT_HIGH_COST_COLS) + [
            c for c in OPTIONAL_HIGH_COST_COLS if c in query.columns
        ]
        return (
            query.select(cols).sort("total_spend_ytd", descending=True).head(top_n)
        )

    def utilization_trends(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        90-day-as-percentage-of-YTD utilization rates.

        High recent percentages flag patients with recent escalation.
        """
        return (
            df.with_columns(
                pl.when(pl.col("er_admits_ytd") > 0)
                .then(pl.col("er_admits_90_day_prior") / pl.col("er_admits_ytd") * 100)
                .otherwise(0)
                .alias("er_recent_pct"),
                pl.when(pl.col("any_inpatient_hospital_admits_ytd") > 0)
                .then(
                    pl.col("any_inpatient_hospital_admits_90_day_prior")
                    / pl.col("any_inpatient_hospital_admits_ytd")
                    * 100
                )
                .otherwise(0)
                .alias("ip_recent_pct"),
            )
            .select(
                "mbi",
                "patient_first_name",
                "patient_last_name",
                "plurality_assigned_provider_name",
                "er_admits_ytd",
                "er_admits_90_day_prior",
                "er_recent_pct",
                "any_inpatient_hospital_admits_ytd",
                "any_inpatient_hospital_admits_90_day_prior",
                "ip_recent_pct",
                "total_spend_ytd",
            )
            .filter(
                (pl.col("er_admits_90_day_prior") > 0)
                | (pl.col("any_inpatient_hospital_admits_90_day_prior") > 0)
            )
            .sort("ip_recent_pct", descending=True)
        )

    def spend_breakdown(self, df: pl.DataFrame) -> tuple[dict, list[dict]]:
        """
        Spend totals by category.

        Returns ``(raw_totals_dict, sorted_list_of_dicts)`` where the list is
        sorted by amount descending and includes percentage of total.
        """
        cols = [
            pl.col(col_name).sum().alias(label) for col_name, label in SPEND_CATEGORIES
        ]
        spend_summary = df.select(cols).to_dicts()[0]
        total = sum(v for v in spend_summary.values() if v)
        items = []
        for label, amount in sorted(
            spend_summary.items(), key=lambda kv: kv[1] or 0, reverse=True
        ):
            if not amount:
                continue
            pct = (amount / total * 100) if total > 0 else 0
            items.append(
                {
                    "Category": label,
                    "Total Spend": amount,
                    "Percentage": pct,
                }
            )
        return spend_summary, items

    # ---- one-call orchestration ---------------------------------------

    def load_dashboard_data(
        self,
        silver_path: Path,
        already_discussed: list[str],
    ) -> pl.DataFrame:
        """Silver → most-recent → flag-discussed → collect, in one call."""
        lf = self.load_with_crosswalk(silver_path)
        lf = self.filter_to_most_recent(lf)
        lf = self.flag_already_discussed(lf, already_discussed)
        return lf.collect()

    # ---- presentation formatters --------------------------------------

    def format_high_cost_rows(self, patients_df: pl.DataFrame) -> list[dict]:
        """Row-dicts for the high-cost-patients ``mo.ui.table``."""
        truthy_em_flags = {"1", "true", "True", "TRUE", "Y", "y", "Yes", "YES"}
        rows = []
        for row in patients_df.to_dicts():
            last_em = row.get("last_em_visit")
            last_em_str = str(last_em) if last_em else ""
            city_state = ""
            if row.get("cms_city") and row.get("cms_state"):
                city_state = f"{row['cms_city']}, {row['cms_state']}"
            elif row.get("cms_state"):
                city_state = row["cms_state"]
            current_mbi = row.get("current_mbi", "")
            mbi_display = row["mbi"] or ""
            if current_mbi and current_mbi != row["mbi"]:
                mbi_display = f"{row['mbi']} → {current_mbi}"
            death_indicator = "†" if row.get("cms_death_dt") else ""
            hcmpi_display = row.get("hcmpi") or "⚠️ Not Mapped"
            awv_date = row.get("most_recent_awv_date")
            awv_date_str = str(awv_date) if awv_date else ""
            em_hc_flag = "✓" if row.get("flag_em_hcmg") in truthy_em_flags else ""
            rows.append(
                {
                    "HCMPI": hcmpi_display,
                    "MBI": mbi_display,
                    "Name": f"{row['patient_first_name']} {row['patient_last_name']} {death_indicator}",
                    "Provider": row["plurality_assigned_provider_name"],
                    "Office": row.get("office_name", ""),
                    "Market": row.get("office_market", ""),
                    "Location": city_state,
                    "Total Spend": (
                        f"${row['total_spend_ytd']:,.0f}"
                        if row["total_spend_ytd"]
                        else "$0"
                    ),
                    "ER (YTD)": row["er_admits_ytd"] or 0,
                    "ER (90d)": row["er_admits_90_day_prior"] or 0,
                    "IP (YTD)": row["any_inpatient_hospital_admits_ytd"] or 0,
                    "IP (90d)": row["any_inpatient_hospital_admits_90_day_prior"] or 0,
                    "Discussed": "✓" if row.get("already_discussed") else "",
                    "Hospice": "✓" if row["hospice_admission"] else "",
                    "E&M Visits": row["em_visits_ytd"] or 0,
                    "Last E&M": last_em_str,
                    "Last E&M Provider": row.get("aco_em_name") or "",
                    "E&M w/ HC": em_hc_flag,
                    "Last AWV": awv_date_str,
                    "AWV Claim ID": row.get("awv_claim_id", ""),
                }
            )
        return rows

    def format_provider_rows(self, summary_df: pl.DataFrame) -> list[dict]:
        """Row-dicts for the provider-summary table."""
        rows = []
        for row in summary_df.to_dicts():
            rows.append(
                {
                    "Provider": row["plurality_assigned_provider_name"],
                    "Patients": f"{row['patient_count']:,}",
                    "Total Spend": (
                        f"${row['total_spend']:,.0f}" if row["total_spend"] else "$0"
                    ),
                    "Avg Spend": (
                        f"${row['avg_spend_per_patient']:,.0f}"
                        if row["avg_spend_per_patient"]
                        else "$0"
                    ),
                    "ER Admits": (
                        f"{row['total_er_admits']:,}" if row["total_er_admits"] else "0"
                    ),
                    "IP Admits": (
                        f"{row['total_inpatient_admits']:,}"
                        if row["total_inpatient_admits"]
                        else "0"
                    ),
                    "Hospice": (
                        f"{row['hospice_admissions']:,}"
                        if row["hospice_admissions"]
                        else "0"
                    ),
                }
            )
        return rows

    def format_utilization_rows(self, trends_df: pl.DataFrame) -> list[dict]:
        """Row-dicts for the utilization-trends table with intensity glyphs."""

        def _intensity(pct: float) -> str:
            if pct > 50:
                return "🔴"
            if pct > 25:
                return "🟡"
            return "🟢"

        rows = []
        for row in trends_df.to_dicts():
            er_pct = row.get("er_recent_pct") or 0
            ip_pct = row.get("ip_recent_pct") or 0
            rows.append(
                {
                    "MBI": row["mbi"],
                    "Name": f"{row['patient_first_name']} {row['patient_last_name']}",
                    "Provider": row["plurality_assigned_provider_name"],
                    "ER YTD": row["er_admits_ytd"] or 0,
                    "ER 90d": row["er_admits_90_day_prior"] or 0,
                    "ER Recent %": f"{_intensity(er_pct)} {er_pct:.0f}%",
                    "IP YTD": row["any_inpatient_hospital_admits_ytd"] or 0,
                    "IP 90d": row["any_inpatient_hospital_admits_90_day_prior"] or 0,
                    "IP Recent %": f"{_intensity(ip_pct)} {ip_pct:.0f}%",
                    "Total Spend": (
                        f"${row['total_spend_ytd']:,.0f}"
                        if row["total_spend_ytd"]
                        else "$0"
                    ),
                }
            )
        return rows

    def format_spend_rows(self, items: list[dict]) -> list[dict]:
        """Format raw spend-breakdown items for display (currency + bar)."""
        out = []
        for item in items:
            pct = item["Percentage"]
            out.append(
                {
                    "Category": item["Category"],
                    "Total Spend": f"${item['Total Spend']:,.0f}",
                    "Percentage": f"{pct:.1f}%",
                    "Visual": "█" * int(pct / 2),
                }
            )
        return out

    def overview_metrics(self, df: pl.DataFrame) -> dict[str, Any]:
        """High-level dashboard counters for the overview cards."""
        if df.height == 0:
            return {
                "total_patients": 0,
                "total_spend": 0.0,
                "avg_spend": 0.0,
                "unique_providers": 0,
                "report_date": "Unknown",
                "total_with_hcmpi": 0,
                "hcmpi_pct": 0.0,
            }
        total_patients = df.height
        total_with_hcmpi = (
            df.filter(pl.col("hcmpi").is_not_null()).height
            if "hcmpi" in df.columns
            else 0
        )
        return {
            "total_patients": total_patients,
            "total_spend": df["total_spend_ytd"].sum() or 0.0,
            "avg_spend": df["total_spend_ytd"].mean() or 0.0,
            "unique_providers": df["plurality_assigned_provider_name"].n_unique(),
            "report_date": (
                df["file_date"].first() if "file_date" in df.columns else "Unknown"
            ),
            "total_with_hcmpi": total_with_hcmpi,
            "hcmpi_pct": (total_with_hcmpi / total_patients * 100),
        }

    def provider_dropdown_options(self, df: pl.DataFrame) -> list[str]:
        """Sorted unique provider names with ``"All Providers"`` first."""
        if df.height == 0:
            return ["All Providers"]
        names = [
            n
            for n in df["plurality_assigned_provider_name"].unique().to_list()
            if n is not None
        ]
        return ["All Providers"] + sorted(names)
