# © 2025 HarmonyCares
# All rights reserved.

"""
SVA log data transforms.

Transforms parsed Mabel SFTP log data into analytics-ready views:

1. **Session summary** - One row per SFTP session with connection metadata,
   file counts, duration, and health flags.
2. **Upload detail** - One row per file upload with patient name extraction
   and SVA form classification.

These transforms consume the output of the ``mabel_log`` parser and apply
SVA log expressions for classification and metadata extraction.
"""

import polars as pl

from .._decor8 import transform_method
from .._expressions._sva_log import SvaLogExpression
from ._registry import register_pipeline


@register_pipeline(name="sva_log_sessions")
@transform_method(enable_composition=False, threshold=5.0)
def build_session_summary(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Aggregate parsed log events into one row per SFTP session.

    Produces columns:
        - session_id: Session counter
        - session_date: Header date string
        - server: SFTP server hostname
        - session_start: Earliest event timestamp
        - session_end: Latest event timestamp
        - session_duration_seconds: End - start in seconds
        - files_uploaded: Count of upload events
        - auth_succeeded: Whether authentication succeeded in this session
        - disconnected_cleanly: Whether session ended with clean disconnect
        - event_count: Total events in the session

    Args:
        lf: LazyFrame from mabel_log parser

    Returns:
        pl.LazyFrame: Session-level summary
    """
    return lf.group_by("session_id").agg(
        pl.col("session_date").first(),
        pl.col("server").first().alias("server"),
        pl.col("timestamp").min().alias("session_start"),
        pl.col("timestamp").max().alias("session_end"),
        (
            (pl.col("timestamp").max() - pl.col("timestamp").min())
            .dt.total_seconds()
            .alias("session_duration_seconds")
        ),
        SvaLogExpression.is_upload().sum().alias("files_uploaded"),
        SvaLogExpression.auth_succeeded().any().alias("auth_succeeded"),
        SvaLogExpression.disconnected_cleanly().any().alias("disconnected_cleanly"),
        pl.len().alias("event_count"),
    ).sort("session_id")


@register_pipeline(name="sva_log_uploads")
@transform_method(enable_composition=False, threshold=5.0)
def build_upload_detail(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Extract upload events with patient name and SVA form metadata.

    Filters to upload rows only, then enriches with:
        - patient_name: Extracted from filename
        - submission_date_str: Date portion from SVA filename
        - is_sva_form: Whether filename follows SVA naming convention

    Args:
        lf: LazyFrame from mabel_log parser

    Returns:
        pl.LazyFrame: One row per file upload with extracted metadata
    """
    return (
        lf.filter(SvaLogExpression.is_upload())
        .with_columns(
            SvaLogExpression.patient_name(),
            SvaLogExpression.is_sva_form().alias("is_sva_form"),
        )
        .select(
            "session_id",
            "timestamp",
            "server",
            "source_path",
            "destination_path",
            "filename",
            "patient_name",
            "submission_date",
            "is_sva_form",
        )
    )


@register_pipeline(name="sva_log_daily_summary")
@transform_method(enable_composition=False, threshold=5.0)
def build_daily_summary(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Aggregate upload activity by calendar date.

    Produces columns:
        - upload_date: Date of uploads
        - total_sessions: Number of SFTP sessions
        - total_uploads: Number of files uploaded
        - unique_patients: Distinct patient names uploaded

    Args:
        lf: LazyFrame from mabel_log parser

    Returns:
        pl.LazyFrame: One row per calendar date
    """
    uploads = lf.filter(SvaLogExpression.is_upload()).with_columns(
        SvaLogExpression.patient_name(),
        pl.col("timestamp").dt.date().alias("upload_date"),
    )

    return uploads.group_by("upload_date").agg(
        pl.col("session_id").n_unique().alias("total_sessions"),
        pl.len().alias("total_uploads"),
        pl.col("patient_name").drop_nulls().n_unique().alias("unique_patients"),
    ).sort("upload_date")
