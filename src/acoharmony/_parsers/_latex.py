# © 2025 HarmonyCares
# All rights reserved.

"""
LaTeX file parser implementation.

Provides LaTeX and BibTeX parsing for academic papers, bibliography extraction, and
citation analysis. This parser extracts text, metadata, citations, and bibliography
entries from LaTeX documents.

What is LaTeX Parsing?

LaTeX is the standard document preparation system for academic publishing:

- **Research Papers**: Journal articles, conference papers, dissertations
- **Academic Documents**: Technical reports, preprints, lecture notes
- **Bibliography Files**: .bib files with citation databases
- **Citation Analysis**: Extract citation networks from papers

Key Features:

1. **Metadata Extraction**: \\title, \\author, \\date from LaTeX
2. **Abstract Extraction**: Content from \\begin{abstract}...\\end{abstract}
3. **Citation Extraction**: \\cite{}, \\citep{}, \\citet{} references
4. **Bibliography Parsing**: Parse .bib files with bibtexparser
5. **Section Structure**: Extract document structure from sections
6. **Text Conversion**: Convert LaTeX to plain text

Output Schema:

The parser returns a LazyFrame with the following structure:
    - filename: str - Original LaTeX filename
    - source_path: str - Full path to LaTeX file
    - latex_content: str - Raw LaTeX content
    - text_content: str - Plain text conversion
    - title: str - Document title
    - author: str - Document author(s)
    - date: str - Document date
    - abstract: str - Abstract text
    - document_class: str - Document class (article, report, etc.)
    - citations: list - Citation keys from \\cite commands
    - bibliography_entries: list - Parsed bibliography entries
    - sections: list - Section structure (level, title)
    - extraction_timestamp: datetime - When parsing occurred

Common Use Cases:

1. **Citation Network Analysis**: Build citation graphs from papers
2. **Bibliography Management**: Extract and organize bibliography entries
3. **Full-Text Search**: Index academic papers for search
4. **Metadata Harvesting**: Build paper databases from LaTeX sources
5. **Text Mining**: Extract text for NLP analysis
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import bibtexparser
import polars as pl
from pylatexenc.latex2text import LatexNodes2Text

from .._log import LogWriter

logger = LogWriter("parsers.latex")


def parse_latex(
    file_path: Path | str,
    extract_biblio: bool = True,
) -> pl.LazyFrame:
    """
    Parse LaTeX file and extract text, metadata, citations, and bibliography.

    Args:
        file_path: Path to LaTeX (.tex) file
        extract_biblio: Whether to extract bibliography entries

    Returns:
        pl.LazyFrame: Parsed LaTeX data

    Note:
        Extracts citation keys but does not resolve to full citations
        For full bibliography, parse associated .bib file separately
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"LaTeX file not found: {file_path}")

    logger.info(f"Parsing LaTeX: {file_path.name}")

    try:
        # Read LaTeX content
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            latex_content = f.read()

        # Extract document class
        doc_class = ""
        doc_class_match = re.search(r"\\documentclass(?:\[.*?\])?\{([^}]+)\}", latex_content)
        if doc_class_match:
            doc_class = doc_class_match.group(1)

        # Extract title
        title = ""
        title_match = re.search(r"\\title\{([^}]+)\}", latex_content)
        if title_match:
            title = title_match.group(1)

        # Extract author
        author = ""
        author_match = re.search(r"\\author\{([^}]+)\}", latex_content)
        if author_match:
            author = author_match.group(1)

        # Extract date
        date = ""
        date_match = re.search(r"\\date\{([^}]+)\}", latex_content)
        if date_match:
            date = date_match.group(1)

        # Extract abstract
        abstract = ""
        abstract_match = re.search(
            r"\\begin\{abstract\}(.*?)\\end\{abstract\}",
            latex_content,
            re.DOTALL,
        )
        if abstract_match:
            abstract = abstract_match.group(1).strip()

        # Convert to plain text using pylatexenc
        try:
            latex_converter = LatexNodes2Text()
            text_content = latex_converter.latex_to_text(latex_content)
        except Exception as e:
            logger.warning(f"Failed to convert LaTeX to text: {e}")
            # Fallback: strip common LaTeX commands
            text_content = latex_content
            text_content = re.sub(r"\\[a-zA-Z]+(\[.*?\])?\{.*?\}", "", text_content)
            text_content = re.sub(r"\\[a-zA-Z]+", "", text_content)
            text_content = re.sub(r"[{}]", "", text_content)

        # Extract citations
        citations = []
        # Match various citation commands: \cite{}, \citep{}, \citet{}, etc.
        for match in re.finditer(r"\\cite[pt]?\{([^}]+)\}", latex_content):
            citation_text = match.group(1)
            # Split multiple citations
            for cite in citation_text.split(","):
                cite = cite.strip()
                if cite:
                    citations.append(cite)

        # Extract bibliography entries if requested
        bibliography_entries = []
        if extract_biblio:
            # Look for \bibitem entries (inline bibliography)
            for match in re.finditer(
                r"\\bibitem(?:\[.*?\])?\{([^}]+)\}(.*?)(?=\\bibitem|\\end\{thebibliography\}|$)",
                latex_content,
                re.DOTALL,
            ):
                key = match.group(1)
                entry_text = match.group(2).strip()

                # Try to parse basic info from entry text
                bibliography_entries.append(
                    {
                        "key": key,
                        "type": "misc",
                        "title": "",
                        "author": "",
                        "year": "",
                        "doi": "",
                        "url": "",
                        "raw_text": entry_text,
                    }
                )

            # Check for \bibliography{} command pointing to .bib file
            bib_match = re.search(r"\\bibliography\{([^}]+)\}", latex_content)
            if bib_match:
                bib_filename = bib_match.group(1)
                if not bib_filename.endswith(".bib"):
                    bib_filename += ".bib"

                # Try to find and parse .bib file
                bib_path = file_path.parent / bib_filename
                if bib_path.exists():
                    try:
                        bib_entries = parse_bibtex(bib_path)
                        # Merge with existing entries
                        if bib_entries:
                            bib_df = bib_entries.collect()
                            for row in bib_df.iter_rows(named=True):
                                bibliography_entries.extend(row.get("bibliography_entries", []))
                    except Exception as e:
                        logger.warning(f"Failed to parse .bib file {bib_path}: {e}")

        # Extract sections
        sections = []
        for match in re.finditer(
            r"\\(section|subsection|subsubsection)\{([^}]+)\}",
            latex_content,
        ):
            section_type = match.group(1)
            section_title = match.group(2)

            # Map section type to level
            level = 1
            if section_type == "subsection":
                level = 2
            elif section_type == "subsubsection":
                level = 3

            sections.append({"level": level, "title": section_title})

        # Get file info
        file_size = file_path.stat().st_size
        extraction_time = datetime.now()

        # Build record
        record = {
            "filename": file_path.name,
            "source_path": str(file_path),
            "latex_content": latex_content,
            "text_content": text_content,
            "title": title,
            "author": author,
            "date": date,
            "abstract": abstract,
            "document_class": doc_class,
            "citations": citations,
            "bibliography_entries": bibliography_entries,
            "sections": sections,
            "file_size_bytes": file_size,
            "extraction_timestamp": extraction_time,
        }

        # Convert to LazyFrame
        df = pl.DataFrame([record])
        logger.info(
            f"Successfully parsed LaTeX: {len(citations)} citations, "
            f"{len(bibliography_entries)} bib entries"
        )
        return df.lazy()

    except Exception as e:
        logger.error(f"Failed to parse LaTeX {file_path.name}: {e}")
        # Return empty DataFrame as LazyFrame with matching schema
        return pl.DataFrame(
            {
                "filename": [file_path.name],
                "source_path": [str(file_path)],
                "latex_content": [""],
                "text_content": [""],
                "title": [None],
                "author": [None],
                "date": [None],
                "abstract": [None],
                "document_class": [None],
                "citations": [[]],
                "bibliography_entries": [[]],
                "sections": [[]],
                "file_size_bytes": [0],
                "extraction_timestamp": [datetime.now()],
            }
        ).lazy()


def parse_bibtex(file_path: Path | str) -> pl.LazyFrame:
    """
    Parse BibTeX (.bib) file and extract bibliography entries.

    Args:
        file_path: Path to BibTeX file

    Returns:
        pl.LazyFrame: Parsed bibliography entries

    Note:
        Returns one row per .bib file with all entries as list column
        Each entry contains: key, type, title, author, year, doi, url
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"BibTeX file not found: {file_path}")

    logger.info(f"Parsing BibTeX: {file_path.name}")

    try:
        # Parse .bib file
        with open(file_path, encoding="utf-8") as f:
            bib_database = bibtexparser.load(f)

        # Extract entries
        bibliography_entries = []
        for entry in bib_database.entries:
            bibliography_entries.append(
                {
                    "key": entry.get("ID", ""),
                    "type": entry.get("ENTRYTYPE", ""),
                    "title": entry.get("title", ""),
                    "author": entry.get("author", ""),
                    "year": entry.get("year", ""),
                    "doi": entry.get("doi", ""),
                    "url": entry.get("url", ""),
                }
            )

        # Build record
        record = {
            "filename": file_path.name,
            "source_path": str(file_path),
            "entry_count": len(bibliography_entries),
            "bibliography_entries": bibliography_entries,
            "extraction_timestamp": datetime.now(),
        }

        # Convert to LazyFrame
        df = pl.DataFrame([record])
        logger.info(f"Successfully parsed BibTeX: {len(bibliography_entries)} entries")
        return df.lazy()

    except Exception as e:
        logger.error(f"Failed to parse BibTeX {file_path.name}: {e}")
        # Return empty DataFrame as LazyFrame with matching schema
        return pl.DataFrame(
            {
                "filename": [file_path.name],
                "source_path": [str(file_path)],
                "entry_count": [0],
                "bibliography_entries": [[]],
                "extraction_timestamp": [datetime.now()],
            }
        ).lazy()


def parse_latex_batch(
    file_paths: list[Path | str],
    extract_biblio: bool = True,
) -> pl.LazyFrame:
    """
    Parse multiple LaTeX files and combine results.

    Args:
        file_paths: List of paths to LaTeX files
        extract_biblio: Whether to extract bibliography entries

    Returns:
        pl.LazyFrame: Combined parsed data from all LaTeX documents

    """
    dfs = []
    for file_path in file_paths:
        try:
            df = parse_latex(file_path, extract_biblio)
            dfs.append(df)
        except Exception as e:
            logger.error(f"Failed to parse {file_path}: {e}")
            continue

    if not dfs:
        raise ValueError("No LaTeX documents successfully parsed")

    # Concatenate all LazyFrames
    return pl.concat(dfs, how="vertical_relaxed")
