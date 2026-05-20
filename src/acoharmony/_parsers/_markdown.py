# © 2025 HarmonyCares
# All rights reserved.

"""
Markdown file parser implementation.

Provides Markdown parsing for documentation, research notes, and citation extraction.
This parser extracts text, frontmatter metadata, links, citations, and structural
information from Markdown documents.

What is Markdown Parsing?

Markdown is a lightweight markup language used extensively in:

- **Documentation**: README files, technical documentation, wikis
- **Research Notes**: Lab notebooks, literature reviews, research logs
- **Academic Writing**: Preprints, drafts, collaborative writing (with Pandoc)
- **Citation Management**: Bibliographies with citation keys (Pandoc/Zotero style)
- **Blogging**: Academic blogs, science communication

Key Features:

1. **Frontmatter Extraction**: YAML/TOML metadata at document start
2. **Text Extraction**: Rendered HTML and plain text
3. **Link Parsing**: Markdown links [text](url) and reference-style [id]: url
4. **Citation Extraction**: Citation keys [@Smith2020], footnotes [^1]
5. **Header Parsing**: Document structure from headers
6. **Footnotes**: Extract footnotes and endnotes

Output Schema:

The parser returns a LazyFrame with the following structure:
    - filename: str - Original Markdown filename
    - source_path: str - Full path to Markdown file
    - markdown_content: str - Raw Markdown content
    - rendered_html: str - HTML rendered from Markdown
    - text_content: str - Plain text (no markup)
    - frontmatter: str - YAML/TOML frontmatter as JSON string
    - title: str - Title from frontmatter or first H1
    - author: str - Author from frontmatter
    - date: str - Date from frontmatter
    - tags: list - Tags from frontmatter
    - links: list - List of (text, url) tuples
    - citations: list - Citation keys found in text
    - footnotes: list - Footnotes (id, content)
    - headers: list - Headers (level, text)
    - extraction_timestamp: datetime - When parsing occurred

Common Use Cases:

1. **Documentation Indexing**: Index technical documentation for search
2. **Research Note Mining**: Extract citations from research notes
3. **Bibliography Building**: Collect citation keys for bibliography generation
4. **Content Migration**: Convert Markdown to other formats with metadata
5. **Link Analysis**: Build knowledge graphs from document links
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import frontmatter
import markdown
import polars as pl

from .._log import LogWriter

logger = LogWriter("parsers.markdown")


def parse_markdown(
    file_path: Path | str,
    extract_citations: bool = True,
) -> pl.LazyFrame:
    """
    Parse Markdown file and extract text, metadata, links, and citations.

    Args:
        file_path: Path to Markdown file
        extract_citations: Whether to extract citation keys

    Returns:
        pl.LazyFrame: Parsed Markdown data

    Note:
        Supports YAML and TOML frontmatter
        Detects Pandoc-style citations [@key]
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Markdown file not found: {file_path}")

    logger.info(f"Parsing Markdown: {file_path.name}")

    try:
        # Parse frontmatter and content
        with open(file_path, encoding="utf-8") as f:
            post = frontmatter.load(f)

        markdown_content = post.content
        frontmatter_dict = dict(post.metadata) if post.metadata else {}

        # Extract frontmatter fields
        title = frontmatter_dict.get("title", "")
        author = frontmatter_dict.get("author", "")
        date = frontmatter_dict.get("date", "")
        tags = frontmatter_dict.get("tags", [])

        # If no title in frontmatter, try to extract from first H1
        if not title:
            h1_match = re.search(r"^#\s+(.+)$", markdown_content, re.MULTILINE)
            if h1_match:
                title = h1_match.group(1).strip()

        # Render to HTML
        md = markdown.Markdown(
            extensions=[
                "extra",
                "meta",
                "footnotes",
                "tables",
                "toc",
            ]
        )
        rendered_html = md.convert(markdown_content)

        # Extract plain text (remove Markdown syntax)
        text_content = markdown_content
        # Remove code blocks
        text_content = re.sub(r"```.*?```", "", text_content, flags=re.DOTALL)
        # Remove inline code
        text_content = re.sub(r"`[^`]+`", "", text_content)
        # Remove links but keep text
        text_content = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", text_content)
        # Remove images
        text_content = re.sub(r"!\[([^\]]*)\]\([^\)]+\)", r"\1", text_content)
        # Remove bold/italic
        text_content = re.sub(r"[*_]{1,2}([^*_]+)[*_]{1,2}", r"\1", text_content)
        # Remove headers markers
        text_content = re.sub(r"^#{1,6}\s+", "", text_content, flags=re.MULTILINE)

        # Extract Markdown links [text](url)
        links = []
        for match in re.finditer(r"\[([^\]]+)\]\(([^\)]+)\)", markdown_content):
            text = match.group(1)
            url = match.group(2)
            links.append({"text": text, "url": url})

        # Extract reference-style links [id]: url
        for match in re.finditer(r"^\[([^\]]+)\]:\s+(.+)$", markdown_content, re.MULTILINE):
            ref_id = match.group(1)
            url = match.group(2)
            links.append({"text": ref_id, "url": url})

        # Extract citations
        citations = []
        if extract_citations:
            # Pandoc-style citations: [@Smith2020], [@Smith2020; @Jones2021]
            for match in re.finditer(r"\[@([^\]]+)\]", markdown_content):
                citation_text = match.group(1)
                # Split multiple citations
                for cite in citation_text.split(";"):
                    cite = cite.strip().lstrip("@")
                    if cite:
                        citations.append(cite)

            # Also check for footnote-style citations [^1]
            # These are handled by markdown.footnotes extension

        # Extract footnotes
        footnotes = []
        for match in re.finditer(
            r"^\[\^([^\]]+)\]:\s*(.+)$",
            markdown_content,
            re.MULTILINE,
        ):
            footnote_id = match.group(1)
            content = match.group(2)
            footnotes.append({"id": footnote_id, "content": content})

        # Extract headers
        headers = []
        for match in re.finditer(r"^(#{1,6})\s+(.+)$", markdown_content, re.MULTILINE):
            level = len(match.group(1))
            header_text = match.group(2).strip()
            headers.append({"level": level, "text": header_text})

        # Get file info
        file_size = file_path.stat().st_size
        extraction_time = datetime.now()

        # Build record
        record = {
            "filename": file_path.name,
            "source_path": str(file_path),
            "markdown_content": markdown_content,
            "rendered_html": rendered_html,
            "text_content": text_content,
            "frontmatter": json.dumps(frontmatter_dict, default=str) if frontmatter_dict else "",
            "title": title,
            "author": author,
            "date": str(date) if date else "",
            "tags": tags if isinstance(tags, list) else [],
            "links": links,
            "citations": citations,
            "footnotes": footnotes,
            "headers": headers,
            "file_size_bytes": file_size,
            "extraction_timestamp": extraction_time,
        }

        # Convert to LazyFrame
        df = pl.DataFrame([record])
        logger.info(f"Successfully parsed Markdown: {len(links)} links, {len(citations)} citations")
        return df.lazy()

    except Exception as e:
        logger.error(f"Failed to parse Markdown {file_path.name}: {e}")
        # Return empty LazyFrame with schema
        return pl.LazyFrame(
            schema={
                "filename": pl.Utf8,
                "source_path": pl.Utf8,
                "markdown_content": pl.Utf8,
                "rendered_html": pl.Utf8,
                "text_content": pl.Utf8,
                "frontmatter": pl.Utf8,
                "title": pl.Utf8,
                "author": pl.Utf8,
                "date": pl.Utf8,
                "tags": pl.List(pl.Utf8),
                "links": pl.List(pl.Struct({"text": pl.Utf8, "url": pl.Utf8})),
                "citations": pl.List(pl.Utf8),
                "footnotes": pl.List(pl.Struct({"id": pl.Utf8, "content": pl.Utf8})),
                "headers": pl.List(pl.Struct({"level": pl.Int64, "text": pl.Utf8})),
                "file_size_bytes": pl.Int64,
                "extraction_timestamp": pl.Datetime,
            }
        )


def parse_markdown_batch(
    file_paths: list[Path | str],
    extract_citations: bool = True,
) -> pl.LazyFrame:
    """
    Parse multiple Markdown files and combine results.

    Args:
        file_paths: List of paths to Markdown files
        extract_citations: Whether to extract citation keys

    Returns:
        pl.LazyFrame: Combined parsed data from all Markdown documents

    """
    dfs = []
    for file_path in file_paths:
        try:
            df = parse_markdown(file_path, extract_citations)
            dfs.append(df)
        except Exception as e:
            logger.error(f"Failed to parse {file_path}: {e}")
            continue

    if not dfs:
        raise ValueError("No Markdown documents successfully parsed")

    # Concatenate all LazyFrames
    return pl.concat(dfs, how="vertical_relaxed")
