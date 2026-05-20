# © 2025 HarmonyCares
# All rights reserved.

"""
Batch citation transform implementation.

Provides batch processing capabilities for processing multiple citation URLs
or importing existing file collections.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl

from .._log import LogWriter
from ._cite import transform_cite

logger = LogWriter("transforms.cite_batch")


def transform_cite_batch(
    urls: list[str],
    force_refresh: bool = False,
    max_workers: int = 4,
) -> pl.LazyFrame:
    """
    Process multiple citation URLs in parallel.

    Args:
        urls: List of URLs to process
        force_refresh: If True, reprocess even if already cached
        max_workers: Maximum number of parallel workers

    Returns:
        pl.LazyFrame: Combined citation data from all URLs

    Note:
        Failed URLs are logged but don't stop processing
        Results from successful URLs are combined
        Writes to corpus happen sequentially at the end to avoid corruption
    """
    logger.info(f"Starting batch citation transform for {len(urls)} URLs")

    results = []
    failed_urls = []

    # Process URLs in parallel WITHOUT writing to corpus
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks with save_to_corpus=False
        future_to_url = {
            executor.submit(transform_cite, url, force_refresh, save_to_corpus=False): url
            for url in urls
        }

        # Collect results as they complete
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result_lf = future.result()
                results.append(result_lf)
                logger.info(f"Successfully processed: {url}")
            except Exception as e:
                logger.error(f"Failed to process {url}: {e}")
                failed_urls.append(url)

    if not results:
        logger.error("No URLs successfully processed")
        raise ValueError("No citations successfully processed")

    if failed_urls:
        logger.warning(f"Failed to process {len(failed_urls)} URLs: {failed_urls}")

    # Combine all results - use diagonal_relaxed to handle different schemas
    logger.info(f"Combining {len(results)} successful results")
    combined_lf = pl.concat(results, how="diagonal_relaxed")

    # Now write combined results to corpus in a single operation
    from pathlib import Path

    from .._store import StorageBackend

    storage = StorageBackend()
    corpus_dir = Path(storage.get_path("cites/corpus"))
    corpus_dir.mkdir(parents=True, exist_ok=True)
    master_corpus_path = corpus_dir / "corpus.parquet"

    # Collect the LazyFrame
    combined_df = combined_lf.collect()

    # Write to corpus
    if master_corpus_path.exists():
        existing_df = pl.read_parquet(master_corpus_path)
        final_df = pl.concat([existing_df, combined_df], how="diagonal_relaxed")
        final_df.write_parquet(str(master_corpus_path), compression="zstd")
        logger.info(f"Appended {len(combined_df)} citations to corpus ({len(final_df)} total)")
    else:
        combined_df.write_parquet(str(master_corpus_path), compression="zstd")
        logger.info(f"Created corpus with {len(combined_df)} citations")

    logger.info(
        f"Batch citation transform complete: {len(results)} succeeded, {len(failed_urls)} failed"
    )

    return combined_lf


def transform_cite_directory(
    directory: Path | str,
    pattern: str = "*",
    force_refresh: bool = False,
) -> pl.LazyFrame:
    """
    Process all files in a directory matching pattern.

    Useful for batch importing existing PDF/HTML collections.

    Args:
        directory: Directory containing files to process
        pattern: Glob pattern to match files (e.g., "*.pdf", "*.html")
        force_refresh: If True, reprocess even if already cached

    Returns:
        pl.LazyFrame: Combined citation data from all files

    Note:
        Files are processed as local files, not downloaded from URLs
        Uses file:// URLs internally
    """
    directory = Path(directory)

    if not directory.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")

    logger.info(f"Processing directory: {directory} with pattern: {pattern}")

    # Find all matching files
    files = list(directory.glob(pattern))

    if not files:
        logger.warning(f"No files found matching pattern: {pattern}")
        raise ValueError(f"No files found in {directory} matching {pattern}")

    logger.info(f"Found {len(files)} files to process")

    # Convert file paths to file:// URLs
    urls = [f"file://{file.absolute()}" for file in files]

    # Process using batch transform
    return transform_cite_batch(urls, force_refresh=force_refresh)
