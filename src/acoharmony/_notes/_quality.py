# © 2025 HarmonyCares
# All rights reserved.

"""
Quality-measure analytics (ACR / DAH / UAMCC).

QTLQR (quarterly_quality_report_claims_results) carries the
risk-adjusted scores and volumes per delivery; BLQQR (baseline
quarterly quality report) carries the underlying detail rows used to
roll up raw rates. Helpers here parse the filename-encoded delivery /
performance-year metadata once and produce the dashboard frames.
"""

from __future__ import annotations

from typing import Literal

import polars as pl

from ._base import PluginRegistry

KNOWN_MEASURES = ("ACR", "DAH", "UAMCC")
MEASURE_LABELS = {
    "ACR": "ACR — All-Cause Readmission (%)",
    "DAH": "DAH — Days at Home (days/yr)",
    "UAMCC": "UAMCC — Unplanned Admissions per 100 person-yrs",
}
MEASURE_UNITS = {"ACR": "%", "DAH": "days", "UAMCC": "per 100 PY"}

_PERIOD_RE = r"\.(Q\d)\.(PY\d{4})\."
BlqqrKind = Literal["acr", "dah"]


def _annotate_period(df: pl.DataFrame) -> pl.DataFrame:
    """Add quarter / perf_year columns parsed from source_filename."""
    return df.with_columns(
        pl.col("source_filename").str.extract(_PERIOD_RE, 1).alias("quarter"),
        pl.col("source_filename").str.extract(_PERIOD_RE, 2).alias("perf_year"),
    )


class QualityPlugins(PluginRegistry):
    """ACR / DAH / UAMCC analytics for the quality-catchup dashboard."""

    def qtlqr_measures(self, qtlqr_lf: pl.LazyFrame) -> pl.DataFrame:
        """Parse delivery_date / quarter / score / volume out of QTLQR rows."""
        return (
            qtlqr_lf.collect()
            .filter(pl.col("measure").is_in(list(KNOWN_MEASURES)))
            .with_columns(
                pl.col("source_filename")
                .str.extract(r"\.D(\d{6})\.", 1)
                .str.strptime(pl.Date, format="%y%m%d", strict=False)
                .alias("delivery_date"),
                pl.col("source_filename")
                .str.extract(r"QTLQR\.(Q\d)\.", 1)
                .alias("quarter"),
                pl.col("measure").replace(MEASURE_LABELS).alias("measure_label"),
                pl.col("measure_score").cast(pl.Float64, strict=False).alias("score"),
                pl.col("measure_volume").cast(pl.Float64, strict=False).alias("volume"),
            )
            .sort("delivery_date")
        )

    def blqqr_aggregate(
        self,
        blqqr_lf: pl.LazyFrame,
        kind: BlqqrKind,
    ) -> pl.DataFrame:
        """
        Roll BLQQR-{ACR|DAH} up to (perf_year, quarter) with raw rate / mean DAH.

        ACR: index_stays, benes, readmissions, raw_rate (= readmits/stays * 100).
        DAH: benes, raw_dah (mean(survival_days - observed_dah)).
        """
        df = _annotate_period(blqqr_lf.collect())
        if kind == "acr":
            return (
                df.group_by("quarter", "perf_year")
                .agg(
                    pl.len().alias("index_stays"),
                    pl.col("bene_id").n_unique().alias("benes"),
                    pl.col("radm30_flag")
                    .cast(pl.Int64, strict=False)
                    .sum()
                    .alias("readmissions"),
                )
                .with_columns(
                    (pl.col("readmissions") / pl.col("index_stays") * 100).alias("raw_rate"),
                    (pl.col("perf_year") + " " + pl.col("quarter")).alias("period"),
                )
                .sort("perf_year", "quarter")
            )
        # dah
        return (
            df.group_by("quarter", "perf_year")
            .agg(
                pl.col("bene_id").n_unique().alias("benes"),
                (
                    pl.col("survival_days").cast(pl.Float64)
                    - pl.col("observed_dah").cast(pl.Float64)
                )
                .mean()
                .alias("raw_dah"),
            )
            .with_columns(
                (pl.col("perf_year") + " " + pl.col("quarter")).alias("period")
            )
            .sort("perf_year", "quarter")
        )

    def blqqr_exclusions(self, excl_lf: pl.LazyFrame) -> pl.DataFrame:
        """BLQQR exclusions with quarter / perf_year parsed from filename."""
        return _annotate_period(excl_lf.collect())

    # ---- per-quarter CMS tie-outs --------------------------------------

    def acr_per_quarter(self, blqqr_acr_lf: pl.LazyFrame) -> pl.DataFrame:
        """Per-quarter ACR rate from BLQQR (our raw rate)."""
        df = _annotate_period(blqqr_acr_lf.collect())
        return (
            df.group_by("quarter", "perf_year")
            .agg(
                pl.len().alias("our_index_stays"),
                pl.col("bene_id").n_unique().alias("our_unique_benes"),
                pl.col("radm30_flag")
                .cast(pl.Int64, strict=False)
                .sum()
                .alias("our_readmissions"),
            )
            .with_columns(
                (pl.col("our_readmissions") / pl.col("our_index_stays") * 100)
                .round(2)
                .alias("our_raw_rate")
            )
            .sort("perf_year", "quarter")
        )

    def acr_exclusions_per_quarter(self, blqqr_excl_lf: pl.LazyFrame) -> pl.DataFrame:
        df = _annotate_period(blqqr_excl_lf.collect())
        return df.select(
            "quarter",
            "perf_year",
            pl.col("ct_benes_acr").cast(pl.Int64, strict=False).alias("cms_benes"),
        )

    def qtlqr_per_quarter(
        self,
        qtlqr_lf: pl.LazyFrame,
        measure: str,
    ) -> pl.DataFrame:
        """QTLQR rate + volume for a measure, parsed per quarter."""
        return (
            qtlqr_lf.collect()
            .filter(pl.col("measure") == measure)
            .with_columns(
                pl.col("source_filename")
                .str.extract(r"QTLQR\.(Q\d)\.", 1)
                .alias("quarter")
            )
            .select(
                "quarter",
                pl.col("measure_score")
                .cast(pl.Float64, strict=False)
                .alias("cms_score"),
                pl.col("measure_volume")
                .cast(pl.Float64, strict=False)
                .alias("cms_volume"),
            )
        )

    def acr_comparison(
        self,
        blqqr_acr_lf: pl.LazyFrame,
        blqqr_excl_lf: pl.LazyFrame,
        qtlqr_lf: pl.LazyFrame,
    ) -> pl.DataFrame:
        ours = self.acr_per_quarter(blqqr_acr_lf)
        excl = self.acr_exclusions_per_quarter(blqqr_excl_lf)
        cms = self.qtlqr_per_quarter(qtlqr_lf, "ACR").rename(
            {"cms_score": "cms_risk_adj_rate"}
        )
        return (
            ours.join(excl, on=["quarter", "perf_year"], how="left")
            .join(cms, on="quarter", how="left")
            .with_columns(
                (pl.col("our_unique_benes") == pl.col("cms_benes")).alias("bene_match"),
                (pl.col("our_raw_rate") - pl.col("cms_risk_adj_rate"))
                .round(2)
                .alias("rate_diff_pp"),
            )
        )

    def dah_per_quarter(self, blqqr_dah_lf: pl.LazyFrame) -> pl.DataFrame:
        df = _annotate_period(blqqr_dah_lf.collect())
        return (
            df.group_by("quarter", "perf_year")
            .agg(
                pl.col("bene_id").n_unique().alias("our_benes"),
                (
                    pl.col("survival_days").cast(pl.Float64)
                    - pl.col("observed_dah").cast(pl.Float64)
                )
                .mean()
                .round(1)
                .alias("our_raw_dah"),
            )
            .sort("perf_year", "quarter")
        )

    def dah_comparison(
        self, blqqr_dah_lf: pl.LazyFrame, qtlqr_lf: pl.LazyFrame
    ) -> pl.DataFrame:
        ours = self.dah_per_quarter(blqqr_dah_lf)
        cms = self.qtlqr_per_quarter(qtlqr_lf, "DAH").rename(
            {"cms_score": "cms_dah_score", "cms_volume": "cms_person_years"}
        )
        return ours.join(cms, on="quarter", how="left").with_columns(
            (pl.col("our_raw_dah") - pl.col("cms_dah_score")).round(1).alias("dah_diff")
        )

    def uamcc_per_quarter(self, blqqr_uamcc_lf: pl.LazyFrame) -> pl.DataFrame:
        df = _annotate_period(blqqr_uamcc_lf.collect())
        return (
            df.group_by("quarter", "perf_year")
            .agg(
                pl.col("bene_id").n_unique().alias("our_benes"),
                pl.col("count_unplanned_adm")
                .cast(pl.Int64, strict=False)
                .sum()
                .alias("our_unplanned"),
            )
            .sort("perf_year", "quarter")
        )

    def uamcc_comparison(
        self, blqqr_uamcc_lf: pl.LazyFrame, qtlqr_lf: pl.LazyFrame
    ) -> pl.DataFrame:
        ours = self.uamcc_per_quarter(blqqr_uamcc_lf)
        cms = self.qtlqr_per_quarter(qtlqr_lf, "UAMCC").rename(
            {"cms_score": "cms_uamcc_rate", "cms_volume": "cms_person_years"}
        )
        return ours.join(cms, on="quarter", how="left")

    # ---- value set inventory -------------------------------------------

    def value_set_inventory(self, silver_path) -> pl.DataFrame:
        """Inventory of every ``value_sets_*.parquet`` in silver, tagged by measure."""
        from pathlib import Path

        rows = []
        for path in sorted(Path(silver_path).glob("value_sets_*.parquet")):
            df = pl.read_parquet(path)
            name = path.name.lower()
            measure = (
                "UAMCC"
                if "uamcc" in name
                else "ACR"
                if "acr" in name
                else "HWR"
                if "hwr" in name
                else "Other"
            )
            rows.append(
                {
                    "measure": measure,
                    "file": path.name.replace("value_sets_", "").replace(".parquet", ""),
                    "rows": df.height,
                    "columns": df.width,
                }
            )
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows).sort("measure", "file")

    def exclusion_long(self, excl_df: pl.DataFrame) -> pl.DataFrame:
        """Long-form opt-out / prior-ineligible counts for ACR + DAH."""
        return pl.concat(
            [
                excl_df.select(
                    (pl.col("perf_year") + " " + pl.col("quarter")).alias("period"),
                    pl.col("ct_opting_out_acr")
                    .cast(pl.Float64, strict=False)
                    .alias("opted_out"),
                    pl.col("ct_elig_prior_acr")
                    .cast(pl.Float64, strict=False)
                    .alias("prior_inelig"),
                ).with_columns(pl.lit("ACR").alias("measure")),
                excl_df.select(
                    (pl.col("perf_year") + " " + pl.col("quarter")).alias("period"),
                    pl.col("ct_opting_out_dah")
                    .cast(pl.Float64, strict=False)
                    .alias("opted_out"),
                    pl.col("ct_elig_prior_dah")
                    .cast(pl.Float64, strict=False)
                    .alias("prior_inelig"),
                ).with_columns(pl.lit("DAH").alias("measure")),
            ]
        ).filter(pl.col("opted_out").is_not_null())
