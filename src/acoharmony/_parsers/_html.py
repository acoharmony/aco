# © 2025 HarmonyCares
# All rights reserved.

"""
HTML file parser implementation.

Provides HTML parsing for citation extraction, metadata harvesting, and web content
processing. This parser extracts text, metadata, links, and structured data from HTML
documents to support citation management and content analysis workflows.

What is HTML Parsing?

HTML (HyperText Markup Language) is the standard format for web content, including:

- **Research Articles**: Online journal articles, preprints, blog posts
- **Academic Pages**: Author profiles, research group pages, project documentation
- **Documentation**: Technical documentation, API references
- **Repositories**: Citation repositories, bibliographic databases

Key Features:

1. **Text Extraction**: Clean text with HTML tags removed
2. **Metadata Extraction**: Meta tags (title, description, keywords, author, citation_*)
3. **Link Extraction**: All hyperlinks with context
4. **Structured Data**: JSON-LD, microdata extraction
5. **Citation Detection**: Academic citation metadata from meta tags
6. **Reference Sections**: Identify bibliography and reference sections

Output Schema:

The parser returns a LazyFrame with the following structure:
    - filename: str - Original HTML filename
    - source_url: str - Source URL if available
    - html_content: str - Raw HTML content
    - text_content: str - Cleaned text (tags removed)
    - title: str - Page title
    - meta_description: str - Meta description
    - meta_keywords: str - Meta keywords
    - meta_author: str - Meta author
    - citation_title: str - Citation title from meta tags
    - citation_author: str - Citation author from meta tags
    - citation_date: str - Citation publication date
    - citation_doi: str - DOI from citation meta tags
    - citation_pmid: str - PubMed ID from meta tags
    - links: list - List of (href, text) tuples
    - has_references: bool - Whether page has reference section
    - structured_data: str - JSON-LD/microdata as JSON string
    - extraction_timestamp: datetime - When parsing occurred

Common Use Cases:

1. **Web Citation**: Extract citation metadata from online articles
2. **Content Archival**: Archive web content for offline analysis
3. **Link Analysis**: Build citation networks from hyperlink structure
4. **Metadata Harvesting**: Collect bibliographic data from academic websites
5. **Full-Text Indexing**: Index web content for search
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup

from .._log import LogWriter

logger = LogWriter("parsers.html")


def parse_html(
    file_path: Path | str,
    source_url: str | None = None,
) -> pl.LazyFrame:
    """
    Parse HTML file and extract text, metadata, links, and citations.

    Args:
        file_path: Path to HTML file or HTML content string
        source_url: Optional source URL for the HTML content

    Returns:
        pl.LazyFrame: Parsed HTML data

    Note:
        Handles various HTML encodings automatically
        Extracts academic citation metadata from meta tags
    """
    # Handle both file paths and content strings
    if isinstance(file_path, str) and not Path(file_path).exists():
        # Treat as HTML content string
        html_content = file_path
        filename = "inline_html"
        file_size = len(html_content)
    else:
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"HTML file not found: {file_path}")

        filename = file_path.name
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            html_content = f.read()
        file_size = file_path.stat().st_size

    logger.info(f"Parsing HTML: {filename}")

    try:
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(html_content, "lxml")

        # Extract title
        title = ""
        if soup.title:
            title = soup.title.string or ""

        # Extract meta tags
        meta_description = ""
        meta_keywords = ""
        meta_author = ""
        citation_title = ""
        citation_author = ""
        citation_date = ""
        citation_doi = ""
        citation_pmid = ""

        for meta in soup.find_all("meta"):
            name = meta.get("name", "").lower()
            property_attr = meta.get("property", "").lower()
            content = meta.get("content", "")

            # Standard meta tags
            if name == "description":
                meta_description = content
            elif name == "keywords":
                meta_keywords = content
            elif name == "author":
                meta_author = content

            # Citation meta tags (used by academic publishers)
            elif name == "citation_title":
                citation_title = content
            elif name in ["citation_author", "citation_authors"]:
                citation_author = content
            elif name in ["citation_publication_date", "citation_date"]:
                citation_date = content
            elif name == "citation_doi":
                citation_doi = content
            elif name == "citation_pmid":
                citation_pmid = content

            # Open Graph tags (fallback)
            elif property_attr == "og:title" and not title:
                title = content
            elif property_attr == "og:description" and not meta_description:
                meta_description = content

        # Extract clean text (remove scripts, styles)
        for script in soup(["script", "style"]):
            script.decompose()
        text_content = soup.get_text(separator=" ", strip=True)

        # Extract all links
        links = []
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            text = link.get_text(strip=True)
            if href:
                links.append({"href": href, "text": text})

        # Detect reference section
        has_references = False
        reference_keywords = [
            "references",
            "bibliography",
            "works cited",
            "citations",
        ]
        for keyword in reference_keywords:
            if re.search(
                rf"\b{keyword}\b",
                text_content.lower(),
            ):
                has_references = True
                break

        # Extract structured data (JSON-LD)
        structured_data = {}
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                structured_data = data
                break  # Use first JSON-LD found
            except json.JSONDecodeError:
                continue

        structured_data_str = json.dumps(structured_data) if structured_data else ""

        # Build record
        record = {
            "filename": filename,
            "source_url": source_url or "",
            "html_content": html_content,
            "text_content": text_content,
            "title": title,
            "meta_description": meta_description,
            "meta_keywords": meta_keywords,
            "meta_author": meta_author,
            "citation_title": citation_title,
            "citation_author": citation_author,
            "citation_date": citation_date,
            "citation_doi": citation_doi,
            "citation_pmid": citation_pmid,
            "links": links,
            "has_references": has_references,
            "structured_data": structured_data_str,
            "file_size_bytes": file_size,
            "extraction_timestamp": datetime.now(),
        }

        # Convert to LazyFrame
        df = pl.DataFrame([record])
        logger.info(f"Successfully parsed HTML: {len(links)} links found")
        return df.lazy()

    except Exception as e:
        logger.error(f"Failed to parse HTML {filename}: {e}")
        # Return empty LazyFrame with schema
        return pl.LazyFrame(
            schema={
                "filename": pl.Utf8,
                "source_url": pl.Utf8,
                "html_content": pl.Utf8,
                "text_content": pl.Utf8,
                "title": pl.Utf8,
                "meta_description": pl.Utf8,
                "meta_keywords": pl.Utf8,
                "meta_author": pl.Utf8,
                "citation_title": pl.Utf8,
                "citation_author": pl.Utf8,
                "citation_date": pl.Utf8,
                "citation_doi": pl.Utf8,
                "citation_pmid": pl.Utf8,
                "links": pl.List(pl.Struct([("href", pl.Utf8), ("text", pl.Utf8)])),
                "has_references": pl.Boolean,
                "structured_data": pl.Utf8,
                "file_size_bytes": pl.Int64,
                "extraction_timestamp": pl.Datetime,
            }
        )


def parse_html_batch(
    sources: list[Path | str],
    source_urls: list[str] | None = None,
) -> pl.LazyFrame:
    """
    Parse multiple HTML files and combine results.

    Args:
        sources: List of paths to HTML files or HTML content strings
        source_urls: Optional list of source URLs (must match length of sources)

    Returns:
        pl.LazyFrame: Combined parsed data from all HTML documents

    """
    if source_urls and len(sources) != len(source_urls):
        raise ValueError("sources and source_urls must have same length")

    dfs = []
    for i, source in enumerate(sources):
        try:
            url = source_urls[i] if source_urls else None
            df = parse_html(source, source_url=url)
            dfs.append(df)
        except Exception as e:
            logger.error(f"Failed to parse {source}: {e}")
            continue

    if not dfs:
        raise ValueError("No HTML documents successfully parsed")

    # Concatenate all LazyFrames
    return pl.concat(dfs, how="vertical_relaxed")
