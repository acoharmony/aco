# © 2025 HarmonyCares
# All rights reserved.

"""
Utility functions for CMS Public Use Files batch processing.

This module provides helpers for:
- Batch downloading files via _cite integration
- Progress tracking and reporting
- File validation and integrity checks
- Generating download manifests

Integrates with:
- _cite.transform_cite for downloads
- _cite.state for tracking
- StorageBackend for file management
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from .._cite.state import CiteStateTracker
from .._log import LogWriter
from .._store import StorageBackend

if TYPE_CHECKING:
    from .models import DownloadTask

logger = LogWriter("puf.utils")


def batch_download(
    tasks: list[DownloadTask],
    max_workers: int = 4,
    delay_between_downloads: float = 1.0,
    skip_existing: bool = True,
) -> dict[str, any]:
    """
    Batch download files from download tasks.

    Args:
        tasks: List of DownloadTask objects
        max_workers: Max concurrent downloads (currently sequential, future parallel)
        delay_between_downloads: Delay in seconds between downloads (rate limiting)
        skip_existing: Skip files already processed (checks state tracker)

    Returns:
        Dictionary with results:
        {
            "total": int,
            "downloaded": int,
            "skipped": int,
            "failed": int,
            "results": list of dicts with per-file results
        }
    """
    from .._transforms._cite import transform_cite

    state_tracker = CiteStateTracker()
    StorageBackend()

    results = {
        "total": len(tasks),
        "downloaded": 0,
        "skipped": 0,
        "failed": 0,
        "results": [],
    }

    logger.info(f"Starting batch download of {len(tasks)} files")

    for idx, task in enumerate(tasks, 1):
        file_url = str(task.file_metadata.url)
        file_key = task.file_metadata.key

        logger.info(f"[{idx}/{len(tasks)}] Processing: {file_key} ({task.year} {task.rule_type})")

        # Check if already processed
        if skip_existing and not task.force_refresh:
            # Generate same hash as _cite would
            import polars as pl

            from .._expressions import _cite_download

            url_df = pl.DataFrame({"url": [file_url]})
            url_df = url_df.with_columns(_cite_download.build_url_hash_expr())
            url_df = url_df.with_columns(_cite_download.build_content_extension_expr())

            url_hash = url_df["url_hash"][0]
            content_ext = url_df["content_extension"][0]

            if state_tracker.is_file_processed(f"{url_hash}.{content_ext}"):
                logger.info(f"  Skipping (already processed): {file_key}")
                results["skipped"] += 1
                results["results"].append(
                    {
                        "file_key": file_key,
                        "year": task.year,
                        "rule_type": str(task.rule_type),
                        "status": "skipped",
                        "url": file_url,
                    }
                )
                continue

        # Download via transform_cite
        try:
            logger.info(f"  Downloading: {file_url}")
            cite_kwargs = task.to_cite_kwargs()
            result_lf = transform_cite(**cite_kwargs)

            # Collect to verify success
            result_df = result_lf.collect()

            logger.info(f"  Downloaded successfully: {file_key}")
            results["downloaded"] += 1
            results["results"].append(
                {
                    "file_key": file_key,
                    "year": task.year,
                    "rule_type": str(task.rule_type),
                    "status": "success",
                    "url": file_url,
                    "file_hash": result_df["file_hash"][0],
                }
            )

        except Exception as e:
            logger.error(f"  Failed to download {file_key}: {e}")
            results["failed"] += 1
            results["results"].append(
                {
                    "file_key": file_key,
                    "year": task.year,
                    "rule_type": str(task.rule_type),
                    "status": "failed",
                    "url": file_url,
                    "error": str(e),
                }
            )

        # Rate limiting delay
        if idx < len(tasks):  # Don't delay after last download
            time.sleep(delay_between_downloads)

    logger.info(
        f"Batch download complete: {results['downloaded']} downloaded, "
        f"{results['skipped']} skipped, {results['failed']} failed"
    )

    return results


def generate_download_manifest(
    tasks: list[DownloadTask], output_path: Path | str | None = None
) -> pl.DataFrame:
    """
    Generate a manifest DataFrame from download tasks.

    Args:
        tasks: List of DownloadTask objects
        output_path: Optional path to save manifest as parquet

    Returns:
        DataFrame with manifest data
    """
    manifest_data = []

    for task in tasks:
        manifest_data.append(
            {
                "year": task.year,
                "rule_type": str(task.rule_type),
                "file_key": task.file_metadata.key,
                "url": str(task.file_metadata.url),
                "category": str(task.file_metadata.category),
                "format": str(task.file_metadata.format) if task.file_metadata.format else None,
                "schema_mapping": task.file_metadata.schema_mapping,
                "description": task.file_metadata.description,
                "priority": task.priority,
                "tags": ",".join(task.tags),
            }
        )

    df = pl.DataFrame(manifest_data)

    # Save if output path provided
    if output_path:
        output_path = Path(output_path)
        df.write_parquet(str(output_path), compression="zstd")
        logger.info(f"Manifest saved to {output_path}")

    return df


def check_download_status(tasks: list[DownloadTask]) -> dict[str, any]:
    """
    Check which files from task list are already downloaded.

    Args:
        tasks: List of DownloadTask objects

    Returns:
        Dictionary with status summary:
        {
            "total": int,
            "processed": int,
            "not_processed": int,
            "details": list of dicts
        }
    """
    from .._expressions import _cite_download

    state_tracker = CiteStateTracker()
    status = {"total": len(tasks), "processed": 0, "not_processed": 0, "details": []}

    for task in tasks:
        file_url = str(task.file_metadata.url)

        # Generate hash
        url_df = pl.DataFrame({"url": [file_url]})
        url_df = url_df.with_columns(_cite_download.build_url_hash_expr())
        url_df = url_df.with_columns(_cite_download.build_content_extension_expr())

        url_hash = url_df["url_hash"][0]
        content_ext = url_df["content_extension"][0]

        is_processed = state_tracker.is_file_processed(f"{url_hash}.{content_ext}")

        if is_processed:
            status["processed"] += 1
        else:
            status["not_processed"] += 1

        status["details"].append(
            {
                "file_key": task.file_metadata.key,
                "year": task.year,
                "rule_type": str(task.rule_type),
                "is_processed": is_processed,
                "url_hash": url_hash,
            }
        )

    return status


def get_corpus_files_for_year(year: str, rule_type: str | None = None) -> list[Path]:
    """
    Get list of corpus files for a specific year from citation storage.

    Args:
        year: Year string (e.g., "2024")
        rule_type: Optional rule type filter

    Returns:
        List of Path objects to corpus parquet files
    """
    storage = StorageBackend()
    state_tracker = CiteStateTracker()

    corpus_dir = Path(storage.get_path("cites/corpus"))
    if not corpus_dir.exists():
        return []

    # Get all state entries for the year
    state_df = state_tracker.get_state()

    if state_df is None or state_df.height == 0:
        return []

    # Filter by metadata tags
    filtered = state_df.filter(pl.col("metadata").str.contains(year))

    if rule_type:
        filtered = filtered.filter(pl.col("metadata").str.contains(rule_type))

    # Extract corpus paths
    corpus_files = []
    for row in filtered.iter_rows(named=True):
        corpus_path = row.get("corpus_path")
        if corpus_path and Path(corpus_path).exists():
            corpus_files.append(Path(corpus_path))

    return corpus_files


def validate_file_downloads(
    tasks: list[DownloadTask], check_file_size: bool = False
) -> dict[str, any]:
    """
    Validate that downloaded files exist and are valid.

    Args:
        tasks: List of DownloadTask objects to validate
        check_file_size: If True, check that file size > 0

    Returns:
        Validation results dictionary
    """
    storage = StorageBackend()
    state_tracker = CiteStateTracker()

    validation = {"total": len(tasks), "valid": 0, "invalid": 0, "missing": 0, "details": []}

    for task in tasks:
        from .._expressions import _cite_download

        file_url = str(task.file_metadata.url)

        # Generate hash
        url_df = pl.DataFrame({"url": [file_url]})
        url_df = url_df.with_columns(_cite_download.build_url_hash_expr())
        url_df = url_df.with_columns(_cite_download.build_content_extension_expr())

        url_hash = url_df["url_hash"][0]
        content_ext = url_df["content_extension"][0]

        # Check if processed
        if not state_tracker.is_file_processed(f"{url_hash}.{content_ext}"):
            validation["missing"] += 1
            validation["details"].append(
                {
                    "file_key": task.file_metadata.key,
                    "year": task.year,
                    "status": "missing",
                    "reason": "Not in state tracker",
                }
            )
            continue

        # Check corpus file exists
        corpus_path = Path(storage.get_path(f"cites/corpus/{url_hash}.parquet"))

        if not corpus_path.exists():
            validation["invalid"] += 1
            validation["details"].append(
                {
                    "file_key": task.file_metadata.key,
                    "year": task.year,
                    "status": "invalid",
                    "reason": "Corpus file missing",
                }
            )
            continue

        # Check file size if requested
        if check_file_size:
            file_size = corpus_path.stat().st_size
            if file_size == 0:
                validation["invalid"] += 1
                validation["details"].append(
                    {
                        "file_key": task.file_metadata.key,
                        "year": task.year,
                        "status": "invalid",
                        "reason": "Empty file",
                    }
                )
                continue

        validation["valid"] += 1
        validation["details"].append(
            {"file_key": task.file_metadata.key, "year": task.year, "status": "valid"}
        )

    return validation
