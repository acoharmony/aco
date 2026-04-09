# © 2025 HarmonyCares
# All rights reserved.

"""
CMS connector for processing CMS.gov content.

Extensible architecture with multiple handlers:
- IOMHandler: Internet-Only Manuals with chapter downloads
- PFSHandler: Physician Fee Schedule regulation notices

Each handler:
- Detects applicable URL patterns
- Extracts domain-specific metadata
- Generates appropriate citations
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urljoin

import polars as pl
from bs4 import BeautifulSoup

from ..._log import LogWriter

if TYPE_CHECKING:
    pass

logger = LogWriter("connectors.cms")


class CMSHandler(ABC):
    """Abstract base class for CMS content handlers."""

    @staticmethod
    @abstractmethod
    def can_handle(url: str) -> bool:
        """Check if this handler can process the URL."""
        pass

    @staticmethod
    @abstractmethod
    def process(
        url: str,
        html_path: Path,
        base_citation: pl.DataFrame,
    ) -> list[pl.DataFrame]:
        """Process URL and generate citations."""
        pass


class IOMHandler(CMSHandler):
    """Handler for CMS Internet-Only Manuals (IOMs)."""

    @staticmethod
    def can_handle(url: str) -> bool:
        """
        Check if URL is a CMS IOM manual page.

        Args:
            url: URL to check

        Returns:
            True if URL is IOM manual page
        """
        return "cms.gov" in url.lower() and any(
            keyword in url.lower()
            for keyword in ["manual", "iom", "internet-only-manual", "cms018"]
        )

    @staticmethod
    def extract_publication_number(url: str, html_content: str) -> str:
        """
        Extract publication number from URL or page content.

        Looks for patterns like:
        - Pub 100-04
        - Publication 100-02
        - CMS Pub. 100-01

        Args:
            url: Source URL
            html_content: HTML content

        Returns:
            Publication number or empty string
        """
        # Try URL first (e.g., cms018912 -> Pub 100-04)
        url_match = re.search(r"cms(\d{6})", url.lower())
        if url_match:
            cms_code = url_match.group(1)
            # Map common CMS codes to pub numbers
            cms_to_pub = {
                "018912": "100-04",  # Medicare Claims Processing Manual
                "018913": "100-02",  # Medicare Benefit Policy Manual
                "018915": "100-01",  # Medicare General Information
                "019033": "100-08",  # Medicare Program Integrity Manual
            }
            if cms_code in cms_to_pub:
                return f"Pub {cms_to_pub[cms_code]}"

        # Try page content
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text()

        # Look for publication number patterns
        patterns = [
            r"Pub(?:lication)?\s+(\d{3}-\d{2})",
            r"CMS\s+Pub\.?\s+(\d{3}-\d{2})",
            r"Publication\s+Number:?\s+(\d{3}-\d{2})",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return f"Pub {match.group(1)}"

        return ""

    @staticmethod
    def extract_chapter_downloads(html_content: str, base_url: str) -> list[dict[str, str]]:
        """
        Extract all chapter download links from page.

        Args:
            html_content: HTML content
            base_url: Base URL for resolving relative links

        Returns:
            List of dicts with chapter info: {title, url, chapter_num}
        """
        soup = BeautifulSoup(html_content, "html.parser")
        downloads = []

        # Find all download links (PDFs, ZIPs)
        for link in soup.find_all("a", href=True):
            href = link["href"]

            # Skip non-download links
            if not any(ext in href.lower() for ext in [".pdf", ".zip", ".docx"]):
                continue

            # Get link text as title
            title = link.get_text(strip=True)
            if not title:
                # Try parent elements for context
                parent = link.find_parent(["li", "td", "div"])
                if parent:
                    title = parent.get_text(strip=True)[:200]

            # Extract chapter number if present
            chapter_match = re.search(r"Chapter\s+(\d+)", title, re.IGNORECASE)
            chapter_num = chapter_match.group(1) if chapter_match else ""

            # Resolve relative URLs
            full_url = urljoin(base_url, href)

            downloads.append(
                {
                    "title": title,
                    "url": full_url,
                    "chapter_num": chapter_num,
                }
            )

        logger.info(f"Found {len(downloads)} chapter downloads")
        return downloads

    @staticmethod
    def process(
        url: str,
        html_path: Path,
        base_citation: pl.DataFrame,
    ) -> list[pl.DataFrame]:
        """
        Process IOM manual page and generate citations for all chapters.

        Args:
            url: Source URL
            html_path: Path to downloaded HTML
            base_citation: Base citation DataFrame

        Returns:
            List of DataFrames (parent + all chapters)
        """
        logger.info(f"Processing IOM manual: {url}")

        # Read HTML content with error handling for non-UTF8 bytes
        with open(html_path, encoding="utf-8", errors="replace") as f:
            html_content = f.read()

        # Extract metadata
        pub_number = IOMHandler.extract_publication_number(url, html_content)
        chapters = IOMHandler.extract_chapter_downloads(html_content, url)

        citations = []

        # Update parent citation with CMS metadata
        parent_df = base_citation.clone()
        parent_df = parent_df.with_columns(
            [
                pl.lit("CMS").alias("author"),
                pl.lit("Centers for Medicare & Medicaid Services").alias("author_full"),
                pl.lit(pub_number).alias("publication_number"),
                pl.lit(len(chapters)).alias("child_count"),
                pl.lit("cms_iom").alias("citation_type"),
                pl.lit(True).alias("is_parent_citation"),
            ]
        )
        citations.append(parent_df)

        # Generate child citations for each chapter
        for idx, chapter in enumerate(chapters, 1):
            chapter_df = base_citation.clone()

            # Build chapter title
            chapter_title = chapter["title"]
            if pub_number and chapter["chapter_num"]:
                chapter_title = f"{pub_number} Chapter {chapter['chapter_num']}: {chapter_title}"
            elif pub_number:
                chapter_title = f"{pub_number} - {chapter_title}"

            chapter_df = chapter_df.with_columns(
                [
                    pl.lit("CMS").alias("author"),
                    pl.lit(chapter_title).alias("title"),
                    pl.lit(chapter_title).alias("normalized_title"),
                    pl.lit(chapter["url"]).alias("download_url"),
                    pl.lit(chapter["chapter_num"]).alias("chapter_number"),
                    pl.lit(pub_number).alias("publication_number"),
                    pl.lit(url).alias("parent_url"),
                    pl.lit("cms_iom_chapter").alias("citation_type"),
                    pl.lit(False).alias("is_parent_citation"),
                    pl.lit(idx).alias("child_sequence"),
                ]
            )
            citations.append(chapter_df)

        logger.info(
            f"Generated {len(citations)} citations: 1 parent + {len(chapters)} chapters"
        )
        return citations


class PFSHandler(CMSHandler):
    """Handler for CMS Physician Fee Schedule (PFS) regulation notices."""

    @staticmethod
    def can_handle(url: str) -> bool:
        """
        Check if URL is a PFS regulation notice.

        Args:
            url: URL to check

        Returns:
            True if URL is PFS notice page
        """
        return "cms.gov" in url.lower() and any(
            keyword in url.lower()
            for keyword in [
                "physician/federal-regulation-notices",
                "fee-schedules/physician",
                "cms-1",  # CMS regulation numbers
            ]
        )

    @staticmethod
    def extract_regulation_number(url: str, html_content: str) -> str:
        """
        Extract regulation number (e.g., CMS-1832-P).

        Args:
            url: Source URL
            html_content: HTML content

        Returns:
            Regulation number or empty string
        """
        # Try URL first
        url_match = re.search(r"(cms-\d{4}-[a-z])", url.lower())
        if url_match:
            return url_match.group(1).upper()

        # Try page content
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text()

        # Look for CMS regulation patterns
        patterns = [
            r"(CMS-\d{4}-[A-Z])",
            r"Regulation\s+Number:?\s+(CMS-\d{4}-[A-Z])",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).upper()

        return ""

    @staticmethod
    def extract_regulation_year(url: str, html_content: str) -> str:
        """
        Extract regulation year from URL or content.

        Args:
            url: Source URL
            html_content: HTML content

        Returns:
            Year as string or empty
        """
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text()

        # Look for year patterns
        year_match = re.search(r"(20\d{2})\s+(?:Physician|Fee Schedule)", text)
        if year_match:
            return year_match.group(1)

        # Fallback to URL
        url_year_match = re.search(r"/(20\d{2})/", url)
        if url_year_match:
            return url_year_match.group(1)

        return ""

    @staticmethod
    def extract_document_downloads(html_content: str, base_url: str) -> list[dict[str, str]]:
        """
        Extract all regulation document downloads.

        Args:
            html_content: HTML content
            base_url: Base URL for resolving relative links

        Returns:
            List of dicts with document info: {title, url, doc_type}
        """
        soup = BeautifulSoup(html_content, "html.parser")
        downloads = []

        # Find all download links
        for link in soup.find_all("a", href=True):
            href = link["href"]

            # Skip non-document links
            if not any(ext in href.lower() for ext in [".pdf", ".zip", ".docx", ".xlsx"]):
                continue

            # Get link text as title
            title = link.get_text(strip=True)
            if not title:
                parent = link.find_parent(["li", "td", "div"])
                if parent:
                    title = parent.get_text(strip=True)[:200]

            # Determine document type
            doc_type = "other"
            if "rule" in title.lower() or "regulation" in title.lower():
                doc_type = "final_rule"
            elif "proposed" in title.lower() or "nprm" in title.lower():
                doc_type = "proposed_rule"
            elif "fact sheet" in title.lower():
                doc_type = "fact_sheet"
            elif "display copy" in title.lower():
                doc_type = "display_copy"
            elif "comment" in title.lower() or "response" in title.lower():
                doc_type = "comments"

            # Resolve relative URLs
            full_url = urljoin(base_url, href)

            downloads.append(
                {
                    "title": title,
                    "url": full_url,
                    "doc_type": doc_type,
                }
            )

        logger.info(f"Found {len(downloads)} regulation documents")
        return downloads

    @staticmethod
    def process(
        url: str,
        html_path: Path,
        base_citation: pl.DataFrame,
    ) -> list[pl.DataFrame]:
        """
        Process PFS regulation page and generate citations.

        Args:
            url: Source URL
            html_path: Path to downloaded HTML
            base_citation: Base citation DataFrame

        Returns:
            List of DataFrames (parent + all documents)
        """
        logger.info(f"Processing PFS regulation: {url}")

        # Read HTML content with error handling for non-UTF8 bytes
        with open(html_path, encoding="utf-8", errors="replace") as f:
            html_content = f.read()

        # Extract metadata
        reg_number = PFSHandler.extract_regulation_number(url, html_content)
        reg_year = PFSHandler.extract_regulation_year(url, html_content)
        documents = PFSHandler.extract_document_downloads(html_content, url)

        citations = []

        # Update parent citation
        parent_df = base_citation.clone()
        parent_df = parent_df.with_columns(
            [
                pl.lit("CMS").alias("author"),
                pl.lit("Centers for Medicare & Medicaid Services").alias("author_full"),
                pl.lit(reg_number).alias("regulation_number"),
                pl.lit(reg_year).alias("regulation_year"),
                pl.lit(len(documents)).alias("child_count"),
                pl.lit("cms_pfs_regulation").alias("citation_type"),
                pl.lit(True).alias("is_parent_citation"),
            ]
        )
        citations.append(parent_df)

        # Generate child citations for each document
        for idx, doc in enumerate(documents, 1):
            doc_df = base_citation.clone()

            # Build document title
            doc_title = doc["title"]
            if reg_number:
                doc_title = f"{reg_number}: {doc_title}"

            doc_df = doc_df.with_columns(
                [
                    pl.lit("CMS").alias("author"),
                    pl.lit(doc_title).alias("title"),
                    pl.lit(doc_title).alias("normalized_title"),
                    pl.lit(doc["url"]).alias("download_url"),
                    pl.lit(doc["doc_type"]).alias("document_type"),
                    pl.lit(reg_number).alias("regulation_number"),
                    pl.lit(reg_year).alias("regulation_year"),
                    pl.lit(url).alias("parent_url"),
                    pl.lit("cms_pfs_document").alias("citation_type"),
                    pl.lit(False).alias("is_parent_citation"),
                    pl.lit(idx).alias("child_sequence"),
                ]
            )
            citations.append(doc_df)

        logger.info(
            f"Generated {len(citations)} citations: 1 parent + {len(documents)} documents"
        )
        return citations


class CMSConnector:
    """
    Main CMS connector that routes to appropriate handlers.

    Extensible architecture - add new handlers by:
    1. Subclass CMSHandler
    2. Implement can_handle() and process()
    3. Add to HANDLERS list
    """

    HANDLERS = [
        IOMHandler,
        PFSHandler,
    ]

    @staticmethod
    def can_handle(url: str) -> bool:
        """
        Check if any CMS handler can process this URL.

        Args:
            url: URL to check

        Returns:
            True if CMS connector can handle
        """
        return any(handler.can_handle(url) for handler in CMSConnector.HANDLERS)

    @staticmethod
    def process(
        url: str,
        html_path: Path,
        base_citation: pl.DataFrame,
    ) -> list[pl.DataFrame] | None:
        """
        Route to appropriate handler and process.

        Args:
            url: Source URL
            html_path: Path to downloaded HTML
            base_citation: Base citation DataFrame

        Returns:
            List of citation DataFrames or None if no handler found
        """
        for handler in CMSConnector.HANDLERS:
            if handler.can_handle(url):
                logger.info(f"Routing to {handler.__name__}")
                return handler.process(url, html_path, base_citation)

        logger.warning(f"No handler found for CMS URL: {url}")
        return None
