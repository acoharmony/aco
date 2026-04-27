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
