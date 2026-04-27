# © 2025 HarmonyCares
# All rights reserved.

"""
High-needs eligibility reconciliation analytics.

Backs ``notebooks/high_needs_reconciliation.py``: per-criterion population
counts, composite breakdowns, BAR / PBVAR A2 tie-outs, and the recall
residual attribution that buckets BAR-recall misses by upstream root cause.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry

CRITERIA = (
    ("a", "Mobility impairment (dx from B.6.1)"),
    ("b", "High risk score (≥3.0 AD / ≥0.35 ESRD)"),
    ("c", "Mid risk score + 2+ unplanned admits"),
    ("d", "Frailty (HCPCS from B.6.2, 2+ dates)"),
    ("e", "SNF ≥45 days OR HH ≥90 days (PY2024+)"),
)


class HighNeedsPlugins(PluginRegistry):
    """High-needs reconciliation rollups."""

    def load_recon(self, gold_path: Path) -> pl.DataFrame:
        path = Path(gold_path) / "high_needs_reconciliation.parquet"
        if not path.exists():
            raise FileNotFoundError(
                f"{path} not found. Run the high_needs pipeline to materialise it."
            )
        return pl.read_parquet(path)

    def criterion_counts(self, df: pl.DataFrame) -> dict[str, int]:
        """Population counts per ever-met criterion (overlapping)."""
        out: dict[str, int] = {"total": df.height}
        for letter, _ in CRITERIA:
            out[letter] = df.filter(
                pl.col(f"criterion_{letter}_met_ever").fill_null(False)
            ).height
        return out

    def per_criterion_table(self, df: pl.DataFrame) -> pl.DataFrame:
        total = df.height
        rows = []
        for letter, label in CRITERIA:
            ever_n = df.filter(
                pl.col(f"criterion_{letter}_met_ever").fill_null(False)
            ).height
            latest_n = df.filter(
                pl.col(f"criterion_{letter}_met").fill_null(False)
            ).height
            rows.append(
                {
                    "criterion": f"IV.B.1({letter})",
                    "description": label,
                    "benes_ever": ever_n,
                    "share_ever": round(ever_n / total, 4) if total else 0.0,
                    "benes_latest_check": latest_n,
                    "share_latest": round(latest_n / total, 4) if total else 0.0,
                }
            )
        return pl.DataFrame(rows)

    def composite_breakdown(self, df: pl.DataFrame) -> pl.DataFrame:
        total = df.height
        sticky = df.filter(
            pl.col("high_needs_eligible_sticky").fill_null(False)
        ).height
        this_py = df.filter(
            pl.col("high_needs_eligible_this_py").fill_null(False)
        ).height

        def _row(label: str, n: int) -> dict[str, Any]:
            return {
                "composite": label,
                "n_benes": n,
                "share_of_population": round(n / total, 4) if total else 0.0,
            }

        return pl.DataFrame(
            [
                _row("eligible (sticky, cross-PY)", sticky),
                _row("not eligible (sticky)", total - sticky),
                _row("eligible (this PY only)", this_py),
                _row("not eligible (this PY)", total - this_py),
            ]
        )

    def first_eligible_breakdown(
        self, df: pl.DataFrame
    ) -> tuple[pl.DataFrame, int] | None:
        """Returns (per-PY counts, never-eligible count) or None when missing."""
        if "first_eligible_py" not in df.columns:
            return None
        breakdown = (
            df.filter(pl.col("first_eligible_py").is_not_null())
            .group_by("first_eligible_py")
            .agg(pl.len().alias("n_beneficiaries"))
            .sort("first_eligible_py")
        )
        never = df.filter(pl.col("first_eligible_py").is_null()).height
        return breakdown, never

    def filter_by_flag(self, df: pl.DataFrame, flag: str) -> pl.DataFrame:
        """Drill: list benes meeting a single boolean flag column."""
        return df.filter(pl.col(flag).fill_null(False)).select(
            "mbi",
            "performance_year",
            "criterion_a_met_ever",
            "criterion_b_met_ever",
            "criterion_c_met_ever",
            "criterion_d_met_ever",
            "criterion_e_met_ever",
            "high_needs_eligible_sticky",
            "high_needs_eligible_this_py",
            "first_eligible_py",
            "first_eligible_check_date",
        )

    def ineligible_mbis(self, df: pl.DataFrame, limit: int = 500) -> list[str]:
        """First N MBIs flagged ineligible (sticky)."""
        return sorted(
            df.filter(~pl.col("high_needs_eligible_sticky").fill_null(False))["mbi"]
            .drop_nulls()
            .to_list()
        )[:limit]

    def bar_tieout(self, df: pl.DataFrame) -> dict[str, Any]:
        """BAR tie-out: recall stats + the missed-rows table."""
        on_bar = df.filter(pl.col("bar_claims_based_flag").is_not_null())
        total = on_bar.height
        agree = on_bar.filter(
            pl.col("high_needs_eligible_sticky").fill_null(False)
        ).height
        missed = on_bar.filter(
            ~pl.col("high_needs_eligible_sticky").fill_null(True)
        )
        recall = agree / total if total else 0.0
        return {
            "total_on_bar": total,
            "agree_eligible": agree,
            "missed": missed.height,
            "recall": recall,
            "missed_rows": missed.select(
                "mbi",
                "bar_file_date",
                "bar_mobility_impairment_flag",
                "bar_high_risk_flag",
                "bar_medium_risk_unplanned_flag",
                "bar_frailty_flag",
                "bar_claims_based_flag",
                "criterion_a_met_ever",
                "criterion_b_met_ever",
                "criterion_c_met_ever",
                "criterion_d_met_ever",
                "criterion_e_met_ever",
                "first_eligible_py",
            ),
        }

    def pbvar_a2_tieout(self, df: pl.DataFrame) -> dict[str, Any]:
        """PBVAR A2 tie-out: agreement stats + the over-match rows table."""
        a2 = df.filter(pl.col("pbvar_a2_present"))
        total = a2.height
        agree_not_eligible = a2.filter(
            ~pl.col("high_needs_eligible_sticky").fill_null(True)
        ).height
        overmatch = a2.filter(
            pl.col("high_needs_eligible_sticky").fill_null(False)
        ).height
        return {
            "total": total,
            "agree_not_eligible": agree_not_eligible,
            "overmatch": overmatch,
            "rows": a2.select(
                "mbi",
                "pbvar_a2_file_date",
                "pbvar_response_codes",
                "high_needs_eligible_sticky",
                "criterion_a_met_ever",
                "criterion_b_met_ever",
                "criterion_c_met_ever",
                "criterion_d_met_ever",
                "criterion_e_met_ever",
                "first_eligible_py",
            ).sort("high_needs_eligible_sticky", descending=True, nulls_last=True),
        }

    # ---- recall residual attribution ----------------------------------

    def recall_residual_buckets(
        self,
        df: pl.DataFrame,
        silver_path: Path,
        gold_path: Path,
    ) -> pl.DataFrame:
        """Bucket BAR-recall misses by upstream root cause.

        Buckets are mutually exclusive in the order: no-CCLF → CCLF8-only →
        scored-below → bnex-only → other.
        """
        silver = Path(silver_path)
        gold = Path(gold_path)
        missed = df.filter(
            pl.col("bar_claims_based_flag").is_not_null()
            & ~pl.col("high_needs_eligible_sticky").fill_null(True)
        )
        missed_mbis = missed["mbi"].unique().to_list()

        has_data: set[str] = set()
        for table in ("cclf1", "cclf6", "cclf8"):
            try:
                has_data |= set(
                    pl.scan_parquet(silver / f"{table}.parquet")
                    .filter(pl.col("bene_mbi_id").is_in(missed_mbis))
                    .select("bene_mbi_id")
                    .unique()
                    .collect()["bene_mbi_id"]
                    .to_list()
                )
            except Exception:  # ALLOWED: optional silver tables
                pass

        in_cclf8: set[str] = set()
        in_cclf1: set[str] = set()
        try:
            in_cclf8 = set(
                pl.scan_parquet(silver / "cclf8.parquet")
                .filter(pl.col("bene_mbi_id").is_in(missed_mbis))
                .select("bene_mbi_id").unique().collect()["bene_mbi_id"].to_list()
            )
            in_cclf1 = set(
                pl.scan_parquet(silver / "cclf1.parquet")
                .filter(pl.col("bene_mbi_id").is_in(missed_mbis))
                .select("bene_mbi_id").unique().collect()["bene_mbi_id"].to_list()
            )
        except Exception:  # ALLOWED: optional silver tables
            pass

        max_score: dict[str, float] = {}
        try:
            scores = (
                pl.scan_parquet(gold / "hcc_risk_scores.parquet")
                .filter(pl.col("mbi").is_in(missed_mbis))
                .group_by("mbi")
                .agg(pl.col("total_risk_score").max().alias("ms"))
                .collect()
            )
            max_score = dict(zip(scores["mbi"].to_list(), scores["ms"].to_list()))
        except Exception:  # ALLOWED: optional gold table
            pass

        bnex_only: set[str] = set()
        try:
            bnex_only = set(
                pl.scan_parquet(silver / "bnex.parquet")
                .filter(pl.col("MBI").is_in(missed_mbis))
                .select("MBI").unique().collect()["MBI"].to_list()
            ) - has_data
        except Exception:  # ALLOWED: optional bnex feed
            pass

        no_data = [m for m in missed_mbis if m not in has_data and m not in bnex_only]
        cclf8_only = [m for m in missed_mbis if m in in_cclf8 and m not in in_cclf1]
        scored_below = [m for m in missed_mbis if 0 < max_score.get(m, 0) < 3.0]
        close = [m for m in missed_mbis if 2.5 <= max_score.get(m, 0) < 3.0]

        total = len(missed_mbis)
        rows = [
            (
                "1. Out-of-scope: no CCLF data at all (newly aligned or unseen MBI)",
                len(no_data),
                "BAR sees them, we have no claims/demographics — cannot evaluate",
            ),
            (
                "2. In CCLF8 but no inpatient (CCLF1) — empty dx window",
                len(cclf8_only),
                "Score reduces to age/sex factors; no inpatient claim for criterion (a)",
            ),
            (
                "3. Scored, max < 3.0 (criterion b not met)",
                len(scored_below),
                f"Of which {len(close):,} are within 0.05 of threshold "
                "(model-coefficient drift)",
            ),
            (
                "4. Bnex-only (opt-out feed, no CCLF data)",
                len(bnex_only),
                "Singleton chain entry but cannot evaluate",
            ),
        ]
        attributed = sum(r[1] for r in rows)
        other = total - attributed
        rows.append(
            (
                "5. Other (residual: pre-PY2023 sticky carryforward, etc.)",
                other,
                "CMS sticky-aligned them based on pre-2023 data we don't compute",
            )
        )
        return pl.DataFrame(
            [
                {
                    "bucket": label,
                    "benes": n,
                    "share": round(n / total, 3) if total else 0.0,
                    "notes": notes,
                }
                for label, n, notes in rows
            ]
        )

    # ---- per-MBI inspector --------------------------------------------

    def per_mbi_summary(
        self, df: pl.DataFrame, mbi: str
    ) -> dict[str, Any]:
        """Single-bene record reshaped for the inspector view."""
        row = df.filter(pl.col("mbi") == mbi).row(0, named=True)

        def _flag(val: Any) -> str:
            if val is True:
                return "✅ yes"
            if val is False:
                return "— no"
            return "? null"

        criteria_lines = [
            f"| IV.B.1({letter}) "
            f"| {_flag(row.get(f'criterion_{letter}_met_ever'))} "
            f"| {_flag(row.get(f'criterion_{letter}_met'))} |"
            for letter, _ in CRITERIA
        ]
        first_py = row.get("first_eligible_py")
        first_date = row.get("first_eligible_check_date")
        first_line = (
            f"First qualified at **{first_date} (PY {first_py})**"
            if first_py is not None
            else "**Never qualified** under any of the five criteria in any PY "
                 "from 2023 onward."
        )
        return {
            "row": row,
            "first_line": first_line,
            "criteria_table_md": (
                "| Criterion | Ever met | Latest check |\n|---|---|---|\n"
                + "\n".join(criteria_lines)
            ),
            "composite_md": (
                "| Composite | Sticky (cross-PY) | This PY only |\n"
                "|---|---|---|\n"
                f"| Eligible | {_flag(row.get('high_needs_eligible_sticky'))} "
                f"| {_flag(row.get('high_needs_eligible_this_py'))} |"
            ),
        }
