"""
Alignment delivery expressions for point-in-time member-months reconciliation.

These expressions complement the existing ALR/BAR helpers in
``_aco_temporal_alr`` / ``_aco_temporal_bar`` (which handle ACO ID, program,
and ``file_date`` column parsing) with the two additional pieces we need
for reconciliation:

1. ``extract_performance_year_from_filename`` — the performance year is
   encoded in the BAR/ALR filename (e.g. ``ALGC24`` for BAR PY2024,
   ``QALR.2025Q1`` or ``AALR.Y2022`` for MSSP ALR) and is NOT available
   as a dedicated column in the silver tables.
2. ``bar_active_at_month_end_filter`` — a row-level filter that expresses
   CMS's "beneficiary is eligible for this month" rule using the
   ``start_date`` / ``end_date`` window.

ACO ID / program extraction is not re-implemented here; callers should use
``acoharmony._parsers._aco_id.extract_aco_id`` (or the wrapped column
expression in ``_aco_temporal_bar.build_bar_aco_id_expr``) directly.
Likewise, silver ``file_date`` string parsing uses the existing
``_aco_temporal_bar.build_bar_file_date_expr``.
"""

from __future__ import annotations

import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression


@register_expression(
    "alignment_delivery",
    schemas=["silver", "gold"],
    dataset_types=["alr", "bar", "reconciliation"],
    callable=False,
    description="Performance-year + active-at-month-end helpers for ALR/BAR",
)
class AlignmentDeliveryExpression:
    """Expression builders for ALR/BAR delivery metadata and eligibility."""

    @staticmethod
    @expression(
        name="alignment_extract_performance_year",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def extract_performance_year_from_filename() -> pl.Expr:
        """
        Extract the 4-digit performance year from a BAR/ALR ``source_filename``.

        Known encodings (probed in priority order):

          1. BAR (REACH):  ``P.<aco>.ALG[CR]<YY>.RP.D<YYMMDD>...``
             → 2000 + YY. Matches both claims-based (ALGC) and roster
             (ALGR) variants.
          2. MSSP quarterly ALR: ``P.<aco>.ACO.QALR.<YYYY>Q<N>...``
             → YYYY.
          3. MSSP annual ALR:    ``P.<aco>.ACO.AALR.Y<YYYY>...``
             → YYYY.

        Returns null for filenames that match none of the patterns (and
        for null inputs). Added as column ``performance_year`` (Int32).
        """
        fn = pl.col("source_filename").cast(pl.Utf8)

        # BAR: ALGC24 / ALGR25 → 2-digit year, add 2000
        bar_yy = fn.str.extract(r"\.ALG[CR](\d{2})\.", 1)
        bar_py = (bar_yy.cast(pl.Int32, strict=False) + 2000)

        # MSSP quarterly: QALR.2024Q1 → 4-digit year
        qalr_year = fn.str.extract(r"\.QALR\.(\d{4})Q\d", 1).cast(pl.Int32, strict=False)

        # MSSP annual: AALR.Y2022 → 4-digit year
        aalr_year = fn.str.extract(r"\.AALR\.Y(\d{4})", 1).cast(pl.Int32, strict=False)

        return (
            pl.coalesce([bar_py, qalr_year, aalr_year])
            .alias("performance_year")
        )

    @staticmethod
    @expression(
        name="alignment_bar_active_at_month_end",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def bar_active_at_month_end_filter(month_end) -> pl.Expr:
        """
        Row-level filter: bene is active on ``month_end``.

        Rule:
            active := start_date <= month_end
                  AND (end_date IS NULL OR end_date > month_end)

        End-date is treated as exclusive — a bene with ``end_date =
        2024-05-31`` churned out ON that day and is NOT eligible for May.
        This matches CMS's "coverage ends on the last day of month M"
        = "not eligible in month M" convention.

        Args:
            month_end: A ``datetime.date`` representing the last day of
                the month being tested.

        Returns:
            Polars filter expression suitable for ``frame.filter(...)``.
        """
        month_end_lit = pl.lit(month_end)
        return (pl.col("start_date") <= month_end_lit) & (
            pl.col("end_date").is_null() | (pl.col("end_date") > month_end_lit)
        )
