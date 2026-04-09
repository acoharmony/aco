# © 2025 HarmonyCares
# All rights reserved.

"""
PDF file parser implementation.

Provides PDF parsing for citation extraction, document processing, and text mining.
This parser extracts text, metadata, and structural information from PDF documents
to support citation management and document analysis workflows.

What is PDF Parsing?

PDF (Portable Document Format) is ubiquitous in academic publishing, healthcare
documentation, and scientific research. This parser handles:

- **Research Papers**: Academic publications, preprints, conference papers
- **Clinical Guidelines**: Medical protocols, treatment guidelines
- **Reports**: Research reports, clinical trial results
- **Documentation**: Technical documentation, specifications

Key Features:

1. **Text Extraction**: Full text extraction with page awareness
2. **Metadata Extraction**: Title, author, subject, keywords, creation date
3. **Citation Detection**: Identifies DOI, PubMed ID, arXiv ID
4. **Page Information**: Page count, dimensions, content per page
5. **Error Handling**: Robust handling of encrypted, damaged, or malformed PDFs

Output Schema:

The parser returns a LazyFrame with the following structure:
    - filename: str - Original PDF filename
    - file_path: str - Full path to PDF file
    - text_content: str - Extracted text from all pages
    - page_count: int - Number of pages
    - title: str - Document title from metadata
    - author: str - Document author from metadata
    - subject: str - Document subject/abstract
    - keywords: str - Document keywords
    - creator: str - PDF creator software
    - creation_date: str - Document creation date
    - doi: str - DOI if detected in text
    - pubmed_id: str - PubMed ID if detected
    - arxiv_id: str - arXiv ID if detected
    - has_citations: bool - Whether document contains citations
    - file_size_bytes: int - File size
    - extraction_timestamp: datetime - When parsing occurred

Common Use Cases:

1. **Citation Extraction**: Extract citations from research papers for bibliography management
2. **Full-Text Search**: Index academic papers for search and retrieval
3. **Metadata Harvesting**: Build literature databases from PDF collections
4. **Text Mining**: Extract text for NLP analysis, topic modeling
5. **Document Classification**: Categorize papers by content and metadata
"""

import re
from datetime import datetime
from pathlib import Path

import polars as pl
from pypdf import PdfReader

from .._log import LogWriter

logger = LogWriter("parsers.pdf")


def parse_pdf(
    file_path: Path | str,
    extract_per_page: bool = False,
    detect_citations: bool = True,
) -> pl.LazyFrame:
    """
    Parse PDF file and extract text, metadata, and citations.

    Args:
        file_path: Path to PDF file
        extract_per_page: If True, return one row per page; if False, combine all pages
        detect_citations: Whether to detect citation identifiers (DOI, PubMed, arXiv)

    Returns:
        pl.LazyFrame: Parsed PDF data

    Note:
        Encrypted PDFs require password (not currently supported)
        Scanned PDFs without OCR will extract minimal text
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"PDF file not found: {file_path}")

    logger.info(f"Parsing PDF: {file_path.name}")

    try:
        reader = PdfReader(str(file_path))

        # Extract metadata
        metadata = reader.metadata or {}
        title = str(metadata.get("/Title", "")) if metadata.get("/Title") else ""
        author = str(metadata.get("/Author", "")) if metadata.get("/Author") else ""
        subject = str(metadata.get("/Subject", "")) if metadata.get("/Subject") else ""
        keywords = str(metadata.get("/Keywords", "")) if metadata.get("/Keywords") else ""
        creator = str(metadata.get("/Creator", "")) if metadata.get("/Creator") else ""
        creation_date = (
            str(metadata.get("/CreationDate", ""))
            if metadata.get("/CreationDate")
            else ""
        )

        # Extract text from all pages
        page_texts = []
        for page_num, page in enumerate(reader.pages, 1):
            try:
                text = page.extract_text() or ""
                page_texts.append(
                    {
                        "page_number": page_num,
                        "text": text,
                        "width": float(page.mediabox.width) if page.mediabox else 0.0,
                        "height": float(page.mediabox.height) if page.mediabox else 0.0,
                    }
                )
            except Exception as e:
                logger.warning(f"Failed to extract text from page {page_num}: {e}")
                page_texts.append(
                    {
                        "page_number": page_num,
                        "text": "",
                        "width": 0.0,
                        "height": 0.0,
                    }
                )

        # Combine all text for citation detection
        full_text = " ".join(p["text"] for p in page_texts)

        # Detect citation identifiers
        doi = ""
        pubmed_id = ""
        arxiv_id = ""
        has_citations = False

        if detect_citations and full_text:
            # DOI pattern: 10.xxxx/xxxxx
            doi_match = re.search(r"10\.\d{4,}/[^\s]+", full_text)
            if doi_match:
                doi = doi_match.group(0).rstrip(".,;")

            # PubMed ID pattern: PMID: 12345678
            pmid_match = re.search(r"PMID:?\s*(\d{7,})", full_text, re.IGNORECASE)
            if pmid_match:
                pubmed_id = pmid_match.group(1)

            # arXiv ID pattern: arXiv:1234.5678
            arxiv_match = re.search(r"arXiv:(\d{4}\.\d{4,5})", full_text, re.IGNORECASE)
            if arxiv_match:
                arxiv_id = arxiv_match.group(1)

            # Check for common citation markers
            citation_markers = [
                "references",
                "bibliography",
                "works cited",
                r"\[\d+\]",
                r"\(\d{4}\)",
            ]
            has_citations = any(
                re.search(marker, full_text, re.IGNORECASE) for marker in citation_markers
            )

        # Get file info
        file_size = file_path.stat().st_size
        extraction_time = datetime.now()

        # Build data structure
        if extract_per_page:
            # One row per page
            records = []
            for page_info in page_texts:
                records.append(
                    {
                        "filename": file_path.name,
                        "file_path": str(file_path),
                        "page_number": page_info["page_number"],
                        "text_content": page_info["text"],
                        "page_width": page_info["width"],
                        "page_height": page_info["height"],
                        "page_count": len(reader.pages),
                        "title": title,
                        "author": author,
                        "subject": subject,
                        "keywords": keywords,
                        "creator": creator,
                        "creation_date": creation_date,
                        "doi": doi,
                        "pubmed_id": pubmed_id,
                        "arxiv_id": arxiv_id,
                        "has_citations": has_citations,
                        "file_size_bytes": file_size,
                        "extraction_timestamp": extraction_time,
                    }
                )
        else:
            # One row for entire document
            records = [
                {
                    "filename": file_path.name,
                    "file_path": str(file_path),
                    "text_content": full_text,
                    "page_count": len(reader.pages),
                    "title": title,
                    "author": author,
                    "subject": subject,
                    "keywords": keywords,
                    "creator": creator,
                    "creation_date": creation_date,
                    "doi": doi,
                    "pubmed_id": pubmed_id,
                    "arxiv_id": arxiv_id,
                    "has_citations": has_citations,
                    "file_size_bytes": file_size,
                    "extraction_timestamp": extraction_time,
                }
            ]

        # Convert to LazyFrame
        df = pl.DataFrame(records)
        logger.info(f"Successfully parsed PDF: {len(reader.pages)} pages")
        return df.lazy()

    except Exception as e:
        logger.error(f"Failed to parse PDF {file_path.name}: {e}")
        # Return empty LazyFrame with schema
        return pl.LazyFrame(
            schema={
                "filename": pl.Utf8,
                "file_path": pl.Utf8,
                "text_content": pl.Utf8,
                "page_count": pl.Int64,
                "title": pl.Utf8,
                "author": pl.Utf8,
                "subject": pl.Utf8,
                "keywords": pl.Utf8,
                "creator": pl.Utf8,
                "creation_date": pl.Utf8,
                "doi": pl.Utf8,
                "pubmed_id": pl.Utf8,
                "arxiv_id": pl.Utf8,
                "has_citations": pl.Boolean,
                "file_size_bytes": pl.Int64,
                "extraction_timestamp": pl.Datetime,
            }
        )


def parse_pdf_batch(
    file_paths: list[Path] | list[str],
    extract_per_page: bool = False,
    detect_citations: bool = True,
) -> pl.LazyFrame:
    """
    Parse multiple PDF files and combine results.

    Args:
        file_paths: List of paths to PDF files
        extract_per_page: If True, return one row per page per file
        detect_citations: Whether to detect citation identifiers

    Returns:
        pl.LazyFrame: Combined parsed data from all PDFs

    """
    dfs = []
    for file_path in file_paths:
        try:
            df = parse_pdf(file_path, extract_per_page, detect_citations)
            dfs.append(df)
        except Exception as e:
            logger.error(f"Failed to parse {file_path}: {e}")
            continue

    if not dfs:
        raise ValueError("No PDFs successfully parsed")

    # Concatenate all LazyFrames
    return pl.concat(dfs, how="vertical_relaxed")
