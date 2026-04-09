# © 2025 HarmonyCares
# All rights reserved.

"""
Citation transform implementation.

Main transform that orchestrates the entire citation processing pipeline:
1. Download content from URL
2. Parse based on content type
3. Extract citations and metadata
4. Store in corpus
5. Track state

This transform is idempotent: processing the same URL multiple times
produces the same result.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import polars as pl
import requests

from .._cite.state import CiteStateTracker
from .._expressions import _cite_download, _cite_extraction, _cite_processing, _cite_storage
from .._log import LogWriter
from .._parsers import _html, _latex, _markdown, _pdf
from .._store import StorageBackend

logger = LogWriter("transforms.cite")


def _get_ca_bundle_path() -> str | bool:
    """
    Get the CA bundle path to use for SSL verification.

    Returns the system CA bundle path if available, otherwise returns True
    to use requests' default verification.

    Returns:
        str: Path to CA bundle file
        bool: True to use default verification
    """
    # Try common CA bundle locations in order
    ca_paths = [
        "/etc/ssl/certs/ca-certificates.crt",  # Debian/Ubuntu
        "/etc/pki/tls/certs/ca-bundle.crt",  # RHEL/CentOS
        "/etc/ssl/ca-bundle.pem",  # OpenSUSE
        "/etc/ssl/cert.pem",  # OpenBSD/Alpine
    ]

    for ca_path in ca_paths:
        if os.path.exists(ca_path):
            logger.debug(f"Using system CA bundle: {ca_path}")
            return ca_path

    # Fall back to default (certifi)
    logger.debug("Using default CA bundle (certifi)")
    return True


def _normalize_parser_columns(lf: pl.LazyFrame, content_type: str) -> pl.LazyFrame:
    """
    Normalize column names from different parsers to standard schema.

    Different parsers return different column names:
    - PDF: author, title, subject, keywords, creator, creation_date
    - HTML: citation_author, meta_author, citation_title, title, etc.
    - Markdown: author, title (from frontmatter), date
    - LaTeX: author, title, date, abstract

    This function maps parser-specific columns to standard names:
    - author: unified author column
    - title: unified title column
    - abstract: unified abstract/description column
    - date: unified date column
    - keywords: unified keywords column

    Args:
        lf: LazyFrame from parser
        content_type: Type of content (pdf, html, markdown, latex)

    Returns:
        LazyFrame with normalized column names

    """
    # Get existing schema
    schema = lf.collect_schema()

    # Build list of column expressions to add/rename
    exprs = []

    # Normalize author column
    if content_type == "html":
        # HTML: prefer citation_author, fallback to meta_author, then empty
        if "citation_author" in schema:
            exprs.append(
                pl.when(pl.col("citation_author").is_not_null() & (pl.col("citation_author") != ""))
                .then(pl.col("citation_author"))
                .when(pl.col("meta_author").is_not_null() & (pl.col("meta_author") != ""))
                .then(pl.col("meta_author"))
                .otherwise(pl.lit(""))
                .alias("author")
            )
        elif "meta_author" in schema:
            exprs.append(pl.col("meta_author").fill_null("").alias("author"))
    elif "author" not in schema:
        # If no author column exists, create empty one
        exprs.append(pl.lit("").alias("author"))

    # Normalize title column (most parsers already have "title")
    if content_type == "html" and "citation_title" in schema:
        # HTML: prefer citation_title over generic title
        exprs.append(
            pl.when(pl.col("citation_title").is_not_null() & (pl.col("citation_title") != ""))
            .then(pl.col("citation_title"))
            .when(pl.col("title").is_not_null() & (pl.col("title") != ""))
            .then(pl.col("title"))
            .otherwise(pl.lit(""))
            .alias("title")
        )
    elif "title" not in schema:
        # If no title column, create empty one
        exprs.append(pl.lit("").alias("title"))

    # Normalize abstract/description column
    if content_type == "html":
        if "meta_description" in schema:
            exprs.append(pl.col("meta_description").fill_null("").alias("abstract"))
        else:
            exprs.append(pl.lit("").alias("abstract"))
    elif content_type == "pdf":
        if "subject" in schema:
            exprs.append(pl.col("subject").fill_null("").alias("abstract"))
        else:
            exprs.append(pl.lit("").alias("abstract"))
    elif "abstract" not in schema:
        exprs.append(pl.lit("").alias("abstract"))

    # Normalize date column
    if content_type == "html" and "citation_date" in schema:
        exprs.append(pl.col("citation_date").fill_null("").alias("date"))
    elif content_type == "pdf" and "creation_date" in schema:
        exprs.append(pl.col("creation_date").fill_null("").alias("date"))
    elif "date" not in schema:
        exprs.append(pl.lit("").alias("date"))

    # Normalize keywords column
    if content_type == "html":
        if "meta_keywords" in schema:
            exprs.append(pl.col("meta_keywords").fill_null("").alias("keywords"))
        else:
            exprs.append(pl.lit("").alias("keywords"))
    elif "keywords" not in schema:
        exprs.append(pl.lit("").alias("keywords"))

    # Apply normalizations if we have any
    if exprs:
        lf = lf.with_columns(exprs)

    return lf


def transform_cite(
    url: str,
    force_refresh: bool = False,
    note: str = "",
    tags: list[str] | None = None,
    save_to_corpus: bool = True,
) -> pl.LazyFrame:
    """
    Transform citation from URL: download, parse, extract, store.

    This is the main citation processing pipeline that:
        1. Checks if URL already processed (unless force_refresh=True)
        2. Downloads content from URL
        3. Detects content type (PDF, HTML, Markdown, LaTeX)
        4. Parses with appropriate parser
        5. Extracts citation metadata and identifiers
        6. Stores raw and processed data
        7. Updates state tracker with optional note and tags

    Args:
        url: URL to fetch and process
        force_refresh: If True, reprocess even if already cached
        note: Optional note to attach to the citation
        tags: Optional list of tags to attach to the citation
        save_to_corpus: If False, skip writing to corpus (for batch processing)

    Returns:
        pl.LazyFrame: Processed citation data

    Note:
        Idempotent: Same URL produces same result
        State tracked in CiteStateTracker
    """
    logger.info(f"Starting citation transform for URL: {url}")

    # Default tags to empty list if None
    if tags is None:
        tags = []

    # Initialize storage and state tracker
    storage = StorageBackend()
    state_tracker = CiteStateTracker()

    # Step 1: URL Normalization
    url_df = pl.DataFrame({"url": [url]})
    url_df = url_df.with_columns(_cite_download.build_download_url_expr())
    url_df = url_df.with_columns(_cite_download.build_url_hash_expr())
    url_df = url_df.with_columns(_cite_download.build_content_type_detection_expr())
    url_df = url_df.with_columns(_cite_download.build_content_extension_expr())
    url_df = url_df.with_columns(_cite_download.build_url_domain_expr())

    normalized_url = url_df["normalized_url"][0]
    url_hash = url_df["url_hash"][0]
    content_type = url_df["content_type"][0]
    content_extension = url_df["content_extension"][0]
    url_domain = url_df["url_domain"][0]

    logger.info(
        "URL normalized",
        url=normalized_url,
        hash=url_hash,
        content_type=content_type,
        domain=url_domain,
    )

    # Step 2: Check if already processed
    if not force_refresh:
        # Check state tracker by URL hash
        if state_tracker.is_file_processed(f"{url_hash}.{content_extension}"):
            logger.info(f"URL already processed: {url_hash}")
            # Load from master corpus.parquet and filter by url_hash
            corpus_dir = Path(storage.get_path("cites/corpus"))
            master_corpus_path = corpus_dir / "corpus.parquet"
            if master_corpus_path.exists():
                return pl.scan_parquet(str(master_corpus_path)).filter(pl.col("url_hash") == url_hash)

    # Step 3: Check if raw file already exists (download cache)
    raw_dir = Path(storage.get_path("cites/raw")) / content_type
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path_obj = raw_dir / f"{url_hash}.{content_extension}"

    if raw_path_obj.exists() and not force_refresh:
        # File already downloaded - use cached version
        logger.info(f"Using cached file: {raw_path_obj} (size: {raw_path_obj.stat().st_size} bytes)")
        with open(raw_path_obj, "rb") as f:
            content = f.read()
    else:
        # Need to download
        if raw_path_obj.exists():
            logger.info(f"Force refresh enabled - re-downloading {normalized_url}")
        else:
            logger.info(f"Downloading content from {normalized_url}")

        try:
            # Handle file:// URLs differently
            if normalized_url.startswith("file://"):
                # Local file - read directly
                local_path = normalized_url.replace("file://", "")
                with open(local_path, "rb") as f:
                    content = f.read()
            else:
                # HTTP/HTTPS - download with browser-like headers
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "DNT": "1",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                }
                # Get CA bundle path (system or certifi)
                ca_bundle = _get_ca_bundle_path()
                response = requests.get(
                    normalized_url, headers=headers, timeout=30, allow_redirects=True, verify=ca_bundle
                )
                response.raise_for_status()
                content = response.content

                # Detect actual content type from headers if available
                if "content-type" in response.headers:
                    header_content_type = response.headers["content-type"].lower()
                    if "pdf" in header_content_type:
                        content_type = "pdf"
                        content_extension = "pdf"
                    elif "html" in header_content_type:
                        content_type = "html"
                        content_extension = "html"

        except (requests.RequestException, FileNotFoundError, OSError) as e:
            logger.error(f"Failed to download URL: {e}")
            raise

        # Step 4: Save raw content
        with open(raw_path_obj, "wb") as f:
            f.write(content)

        logger.info(f"Saved raw content to {raw_path_obj} (size: {len(content)} bytes)")

    # Step 5: Parse based on content type
    logger.info(f"Parsing as {content_type}")

    if content_type == "pdf":
        parsed_df = _pdf.parse_pdf(raw_path_obj)
    elif content_type == "html":
        parsed_df = _html.parse_html(raw_path_obj, source_url=normalized_url)
    elif content_type == "markdown":
        parsed_df = _markdown.parse_markdown(raw_path_obj)
    elif content_type == "latex":
        parsed_df = _latex.parse_latex(raw_path_obj)
    else:
        logger.warning(f"Unknown content type: {content_type}, treating as HTML")
        parsed_df = _html.parse_html(raw_path_obj, source_url=normalized_url)

    # Step 5.5: Normalize column names across different parsers
    logger.info("Normalizing column names")
    parsed_df = _normalize_parser_columns(parsed_df, content_type)

    # Setup corpus directory paths
    corpus_dir = Path(storage.get_path("cites/corpus"))
    corpus_dir.mkdir(parents=True, exist_ok=True)

    # Step 5.6: Check for domain-specific connectors (CMS, Federal Register, eCFR, arXiv, etc.)
    from .._cite.connectors import CMSConnector, ECFRConnector, FederalRegisterConnector

    if CMSConnector.can_handle(normalized_url):
        logger.info("CMS connector detected, routing to specialized handler")

        # Collect base citation
        base_citation_df = parsed_df.collect()

        # Process with CMS connector
        cms_citations = CMSConnector.process(
            normalized_url,
            raw_path_obj,
            base_citation_df,
        )

        if cms_citations:
            logger.info(f"CMS connector generated {len(cms_citations)} citations")

            # Add tags and notes to each citation DataFrame
            cms_citations_with_tags = []
            for citation_df in cms_citations:
                citation_with_tags = citation_df.with_columns(
                    [
                        pl.lit(tags if tags else [], dtype=pl.List(pl.String)).alias("tags"),
                        pl.lit(note if note else "").alias("note"),
                    ]
                )
                cms_citations_with_tags.append(citation_with_tags)

            # Append all citations to master corpus.parquet (if save_to_corpus enabled)
            if save_to_corpus:
                master_corpus_path = corpus_dir / "corpus.parquet"
                all_citations_df = pl.concat(cms_citations_with_tags, how="diagonal_relaxed")

                if master_corpus_path.exists():
                    existing_df = pl.read_parquet(master_corpus_path)
                    combined_df = pl.concat([existing_df, all_citations_df], how="diagonal_relaxed")
                    combined_df.write_parquet(str(master_corpus_path), compression="zstd")
                    logger.info(f"Appended {len(cms_citations)} CMS citations to master corpus ({len(combined_df)} total)")
                else:
                    all_citations_df.write_parquet(str(master_corpus_path), compression="zstd")
                    logger.info(f"Created master corpus with {len(cms_citations)} CMS citations")

            # Save individual JSON files for each citation
            for idx, citation_df in enumerate(cms_citations_with_tags):
                is_parent = citation_df["is_parent_citation"][0]
                citation_type = "parent" if is_parent else f"child_{idx}"

                # Generate unique file hash for each citation
                dedup_key = f"{url_hash}_{citation_type}"
                citation_file_hash = pl.Series([dedup_key]).hash(seed=0).cast(pl.Utf8).str.slice(0, 16)[0]

                # Save JSON
                citation_json_path = corpus_dir / f"{citation_file_hash}.json"
                citation_dict = citation_df.to_dicts()[0]
                for key, value in citation_dict.items():
                    if isinstance(value, pl.Series | list):
                        citation_dict[key] = str(value)

                with open(str(citation_json_path), "w") as f:
                    json.dump(citation_dict, f, indent=2, default=str)

            # Update state tracker for parent
            parent_citation = cms_citations_with_tags[0]
            state_tracker.mark_file_processed(
                file_path=raw_path_obj,
                source_type=content_type,
                corpus_path=corpus_dir / f"{url_hash}_parent.parquet",
                record_count=len(cms_citations),
                metadata={
                    "url": normalized_url,
                    "url_domain": url_domain,
                    "title": parent_citation["title"][0],
                    "citation_type": parent_citation["citation_type"][0],
                    "child_count": len(cms_citations) - 1,
                    "note": note,
                    "tags": tags,
                },
            )

            logger.info(f"CMS citation transform complete for {normalized_url}")
            # Return parent citation as LazyFrame
            return cms_citations_with_tags[0].lazy()

    # Check Federal Register connector
    if FederalRegisterConnector.can_handle(normalized_url):
        logger.info("Federal Register connector detected, routing to specialized handler")

        # Collect base citation
        base_citation_df = parsed_df.collect()

        # Process with Federal Register connector
        fr_citations = FederalRegisterConnector.process(
            normalized_url,
            raw_path_obj,
            base_citation_df,
        )

        if fr_citations:
            logger.info(f"Federal Register connector generated {len(fr_citations)} citations")

            # Add tags and notes to each citation DataFrame
            fr_citations_with_tags = []
            for citation_df in fr_citations:
                citation_with_tags = citation_df.with_columns(
                    [
                        pl.lit(tags if tags else [], dtype=pl.List(pl.String)).alias("tags"),
                        pl.lit(note if note else "").alias("note"),
                    ]
                )
                fr_citations_with_tags.append(citation_with_tags)

            # Append all citations to master corpus.parquet (if save_to_corpus enabled)
            if save_to_corpus:
                master_corpus_path = corpus_dir / "corpus.parquet"
                all_citations_df = pl.concat(fr_citations_with_tags, how="diagonal_relaxed")

                if master_corpus_path.exists():
                    existing_df = pl.read_parquet(master_corpus_path)
                    combined_df = pl.concat([existing_df, all_citations_df], how="diagonal_relaxed")
                    combined_df.write_parquet(str(master_corpus_path), compression="zstd")
                    logger.info(f"Appended {len(fr_citations)} FR citations to master corpus ({len(combined_df)} total)")
                else:
                    all_citations_df.write_parquet(str(master_corpus_path), compression="zstd")
                    logger.info(f"Created master corpus with {len(fr_citations)} FR citations")

            # Save individual JSON files for each citation
            for idx, citation_df in enumerate(fr_citations_with_tags):
                is_parent = citation_df["is_parent_citation"][0]
                citation_type = "parent" if is_parent else f"child_{idx}"

                # Generate unique file hash for each citation
                dedup_key = f"{url_hash}_{citation_type}"
                citation_file_hash = pl.Series([dedup_key]).hash(seed=0).cast(pl.Utf8).str.slice(0, 16)[0]

                # Save JSON
                citation_json_path = corpus_dir / f"{citation_file_hash}.json"
                citation_dict = citation_df.to_dicts()[0]
                for key, value in citation_dict.items():
                    if isinstance(value, pl.Series | list):
                        citation_dict[key] = str(value)

                with open(str(citation_json_path), "w") as f:
                    json.dump(citation_dict, f, indent=2, default=str)

            # Update state tracker for parent
            parent_citation = fr_citations_with_tags[0]
            state_tracker.mark_file_processed(
                file_path=raw_path_obj,
                source_type=content_type,
                corpus_path=corpus_dir / f"{url_hash}_parent.parquet",
                record_count=len(fr_citations),
                metadata={
                    "url": normalized_url,
                    "url_domain": url_domain,
                    "title": parent_citation["title"][0],
                    "citation_type": parent_citation["citation_type"][0],
                    "child_count": len(fr_citations) - 1,
                    "note": note,
                    "tags": tags,
                },
            )

            logger.info(f"Federal Register citation transform complete for {normalized_url}")
            # Return parent citation as LazyFrame
            return fr_citations_with_tags[0].lazy()

    # Check eCFR connector
    if ECFRConnector.can_handle(normalized_url):
        logger.info("eCFR connector detected, routing to specialized handler")

        # Collect base citation
        base_citation_df = parsed_df.collect()

        # Process with eCFR connector
        ecfr_citations = ECFRConnector.process(
            normalized_url,
            raw_path_obj,
            base_citation_df,
        )

        if ecfr_citations:
            logger.info(f"eCFR connector generated {len(ecfr_citations)} citations")

            # Add tags and notes to each citation DataFrame
            ecfr_citations_with_tags = []
            for citation_df in ecfr_citations:
                citation_with_tags = citation_df.with_columns(
                    [
                        pl.lit(tags if tags else [], dtype=pl.List(pl.String)).alias("tags"),
                        pl.lit(note if note else "").alias("note"),
                    ]
                )
                ecfr_citations_with_tags.append(citation_with_tags)

            # Append all citations to master corpus.parquet (if save_to_corpus enabled)
            if save_to_corpus:
                master_corpus_path = corpus_dir / "corpus.parquet"
                all_citations_df = pl.concat(ecfr_citations_with_tags, how="diagonal_relaxed")

                if master_corpus_path.exists():
                    existing_df = pl.read_parquet(master_corpus_path)
                    combined_df = pl.concat([existing_df, all_citations_df], how="diagonal_relaxed")
                    combined_df.write_parquet(str(master_corpus_path), compression="zstd")
                    logger.info(f"Appended {len(ecfr_citations)} eCFR citations to master corpus ({len(combined_df)} total)")
                else:
                    all_citations_df.write_parquet(str(master_corpus_path), compression="zstd")
                    logger.info(f"Created master corpus with {len(ecfr_citations)} eCFR citations")

            # Save individual JSON files for each citation
            for idx, citation_df in enumerate(ecfr_citations_with_tags):
                is_parent = citation_df["is_parent_citation"][0]
                citation_type = "parent" if is_parent else f"child_{idx}"

                # Generate unique file hash for each citation
                dedup_key = f"{url_hash}_{citation_type}"
                citation_file_hash = pl.Series([dedup_key]).hash(seed=0).cast(pl.Utf8).str.slice(0, 16)[0]

                # Save JSON
                citation_json_path = corpus_dir / f"{citation_file_hash}.json"
                citation_dict = citation_df.to_dicts()[0]
                for key, value in citation_dict.items():
                    if isinstance(value, pl.Series | list):
                        citation_dict[key] = str(value)

                with open(str(citation_json_path), "w") as f:
                    json.dump(citation_dict, f, indent=2, default=str)

            # Update state tracker for parent
            parent_citation = ecfr_citations_with_tags[0]
            state_tracker.mark_file_processed(
                file_path=raw_path_obj,
                source_type=content_type,
                corpus_path=corpus_dir / f"{url_hash}_parent.parquet",
                record_count=len(ecfr_citations),
                metadata={
                    "url": normalized_url,
                    "url_domain": url_domain,
                    "title": parent_citation["title"][0],
                    "citation_type": parent_citation["citation_type"][0],
                    "child_count": len(ecfr_citations) - 1,
                    "note": note,
                    "tags": tags,
                },
            )

            logger.info(f"eCFR citation transform complete for {normalized_url}")
            # Return parent citation as LazyFrame
            return ecfr_citations_with_tags[0].lazy()

    # Step 6: Apply extraction expressions
    logger.info("Applying extraction expressions")

    # Add URL metadata
    parsed_df = parsed_df.with_columns(
        [
            pl.lit(normalized_url).alias("source_url"),
            pl.lit(url_hash).alias("url_hash"),
            pl.lit(content_type).alias("content_type"),
            pl.lit(content_extension).alias("content_extension"),
            pl.lit(url_domain).alias("url_domain"),
        ]
    )

    # Extract identifiers
    parsed_df = parsed_df.with_columns(_cite_extraction.build_citation_identifier_exprs())
    parsed_df = parsed_df.with_columns(_cite_extraction.build_citation_metadata_exprs())
    parsed_df = parsed_df.with_columns(_cite_extraction.build_has_citation_expr())
    parsed_df = parsed_df.with_columns(_cite_extraction.build_reference_count_expr())

    # Step 7: Apply processing expressions
    logger.info("Applying processing expressions")
    parsed_df = parsed_df.with_columns(_cite_processing.build_title_normalization_expr())
    parsed_df = parsed_df.with_columns(_cite_processing.build_author_parsing_exprs())
    parsed_df = parsed_df.with_columns(_cite_processing.build_date_normalization_expr())
    parsed_df = parsed_df.with_columns(_cite_processing.build_deduplication_key_expr())
    parsed_df = parsed_df.with_columns(_cite_processing.build_citation_type_expr())
    parsed_df = parsed_df.with_columns(_cite_processing.build_completeness_score_expr())
    parsed_df = parsed_df.with_columns(_cite_processing.build_processing_metadata_exprs())

    # Step 8: Apply storage expressions
    logger.info("Applying storage expressions")
    parsed_df = parsed_df.with_columns(_cite_storage.build_storage_metadata_exprs())

    # Add tags and notes as columns
    parsed_df = parsed_df.with_columns(
        [
            pl.lit(tags if tags else [], dtype=pl.List(pl.String)).alias("tags"),
            pl.lit(note if note else "").alias("note"),
        ]
    )

    # Collect to eager DataFrame for saving
    result_df = parsed_df.collect()

    # Step 9: Save to corpus (both JSON and master Parquet)
    file_hash = result_df["file_hash"][0]

    # Construct corpus paths - use master corpus.parquet for all citations
    master_corpus_path = corpus_dir / "corpus.parquet"
    corpus_json_path = corpus_dir / f"{file_hash}.json"

    # Append to master corpus.parquet (if save_to_corpus enabled)
    if save_to_corpus:
        if master_corpus_path.exists():
            # Read existing, append, write
            existing_df = pl.read_parquet(master_corpus_path)
            combined_df = pl.concat([existing_df, result_df], how="diagonal_relaxed")
            combined_df.write_parquet(str(master_corpus_path), compression="zstd")
            logger.info(f"Appended to master corpus ({len(combined_df)} total citations)")
        else:
            # Create new master corpus
            result_df.write_parquet(str(master_corpus_path), compression="zstd")
            logger.info(f"Created master corpus at {master_corpus_path}")

    # Save JSON (for human inspection)
    result_dict = result_df.to_dicts()[0]
    # Convert non-serializable types
    for key, value in result_dict.items():
        if isinstance(value, pl.Series | list):
            result_dict[key] = str(value)

    with open(str(corpus_json_path), "w") as f:
        json.dump(result_dict, f, indent=2, default=str)
    logger.info(f"Saved corpus JSON to {corpus_json_path}")

    # Step 10: Update state tracker
    logger.info("Updating state tracker")
    state_tracker.mark_file_processed(
        file_path=raw_path_obj,
        source_type=content_type,
        corpus_path=Path(master_corpus_path),
        record_count=1,
        metadata={
            "url": normalized_url,
            "url_domain": url_domain,
            "title": result_df["normalized_title"][0],
            "citation_type": result_df["citation_type"][0],
            "doi": result_df["extracted_doi"][0],
            "note": note,
            "tags": tags,
        },
    )

    logger.info(f"Citation transform complete for {normalized_url}")

    # Return LazyFrame
    return result_df.lazy()
