# © 2025 HarmonyCares
# All rights reserved.

"""
Citation data processing module for ACO Harmony.

Provides citation data management with state tracking, logging, and handler registry.
Uses pycite for citation parsing and processing.

Features:
    - State tracking for processed citation files
    - Storage paths for raw downloads and processed corpus
    - Integrated logging with custom LogWriter
    - Handler registry with decorators for syntactic sugar
    - Support for multiple citation sources (PubMed, Semantic Scholar, Crossref, etc.)

Storage Structure:
    cites/
    ├── raw/      # Raw citation downloads
    └── corpus/   # Processed citation corpus

State Tracking:
    State is persisted in logs/tracking/cite_state.json
    Daily logs written to logs/acoharmony_YYYYMMDD.jsonl

Registry Usage:
    @citation_parser(name="pubmed", source_type="pubmed")
    def parse_pubmed(file_path: Path) -> list[dict]:
        # Parse PubMed citations
        ...

    @citation_processor(name="deduplicate", processor_type="cleaning")
    def deduplicate(citations: list[dict]) -> list[dict]:
        # Remove duplicates
        ...
"""

from .._log import LogWriter, get_logger
from .._store import StorageBackend
from .decorators import (
    citation_enricher,
    citation_exporter,
    citation_parser,
    citation_processor,
    with_state_tracking,
)
from .registry import (
    CitationRegistry,
    get_handler,
    get_parser,
    list_enrichers,
    list_exporters,
    list_parsers,
    list_processors,
)
from .state import CitationFileState, CiteStateTracker

# Create module-level logger
logger = get_logger("cite")

# Create module-level log writer
log_writer = LogWriter(name="cite")


def get_storage_paths() -> dict[str, any]:
    """
    Get citation storage paths from active storage backend.

        Returns:
            Dictionary with paths for 'cites', 'raw', and 'corpus'
    """
    storage = StorageBackend()
    return {
        "cites": storage.get_path("cites"),
        "raw": storage.get_path("cites/raw"),
        "corpus": storage.get_path("cites/corpus"),
    }


def get_state_tracker(
    log_writer_instance: LogWriter | None = None,
) -> CiteStateTracker:
    """
    Get a citation state tracker instance.

        Args:
            log_writer_instance: Optional custom LogWriter. If None, uses module-level log_writer.

        Returns:
            CiteStateTracker instance configured with storage backend paths
    """
    return CiteStateTracker(log_writer=log_writer_instance or log_writer)


__all__ = [
    # State tracking
    "CitationFileState",
    "CiteStateTracker",
    "get_storage_paths",
    "get_state_tracker",
    # Registry
    "CitationRegistry",
    "get_handler",
    "get_parser",
    "list_parsers",
    "list_processors",
    "list_enrichers",
    "list_exporters",
    # Decorators
    "citation_parser",
    "citation_processor",
    "citation_enricher",
    "citation_exporter",
    "with_state_tracking",
    # Logging
    "logger",
    "log_writer",
]
