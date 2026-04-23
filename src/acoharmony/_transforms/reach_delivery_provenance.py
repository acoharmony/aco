# © 2025 HarmonyCares
# All rights reserved.

"""
REACH delivery provenance transform.

Cross-references the CMS ACO REACH Calendar (what was *scheduled*) with the
FourICLI state tracker (what actually *landed* in 4i) and emits one row per
(schema, period, performance year) pairing, annotated with signed day-diff
and a coarse status label.

The transform is read-only with respect to the 4i state file — it just joins
in-memory. The inputs it consumes:

    - ``silver/reach_calendar.parquet``            (scheduled events/reports)
    - ``logs/tracking/4icli_state.json``           (actual deliveries)

Output lands in ``gold/reach_delivery_provenance.parquet`` as a slowly-growing
table stamped with ``calendar_file_date`` so historical calendar revisions
remain auditable downstream.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from acoharmony._expressions._reach_calendar_delivery import (
    build_calendar_reports_lf,
    build_deliveries_lf,
    build_provenance_join,
)


def _resolve_state_file(storage) -> Path:
    """
    Locate the 4icli_state.json the tracker writes to. FourICLIConfig normally
    points at ``{logs}/tracking/4icli_state.json``; we keep that contract here
    so the transform can run without bootstrapping the 4icli config.
    """
    logs_path = Path(storage.get_path("logs"))
    return logs_path / "tracking" / "4icli_state.json"


def execute(executor) -> pl.LazyFrame:
    """
    Execute the delivery provenance join.

    Returns a LazyFrame suitable for ``sink_parquet`` by the pipeline runner.
    Empty or missing inputs degrade gracefully: if the state file is absent
    the calendar rows come through with ``actual_delivery_date`` null and a
    ``missing`` status, which is itself the signal the user wants.
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = Path(storage.get_path(MedallionLayer.SILVER))
    calendar_path = silver_path / "reach_calendar.parquet"
    state_file = _resolve_state_file(storage)

    calendar_lf = build_calendar_reports_lf(calendar_path, latest_only=True)

    if state_file.exists():
        deliveries_lf = build_deliveries_lf(state_file)
    else:
        deliveries_lf = pl.DataFrame(
            schema={
                "filename": pl.String,
                "schema_name": pl.String,
                "file_type_code": pl.Int64,
                "category": pl.String,
                "period": pl.String,
                "py": pl.Int64,
                "actual_delivery_date": pl.Date,
                "actual_delivery_source": pl.String,
                "remote_created_at": pl.Datetime,
                "downloaded_at": pl.Datetime,
                "filename_date": pl.Date,
            }
        ).lazy()

    return build_provenance_join(calendar_lf, deliveries_lf)
