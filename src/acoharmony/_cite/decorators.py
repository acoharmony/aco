# © 2025 HarmonyCares
# All rights reserved.

"""
Decorator-based citation handler registration with syntactic sugar.

Provides decorators that register citation handlers with the CitationRegistry
and attach metadata dynamically.

"""

from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

from .._log import LogWriter
from .registry import CitationRegistry

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])

logger = LogWriter("cite.decorators")


def citation_parser(
    name: str,
    source_type: str,
    description: str = "",
    formats: list[str] | None = None,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> Callable[[F], F]:
    """
    Register a citation parser function.

        This decorator registers a function that parses citations from a specific source.
        The parser should accept a file path and return a list of citation dictionaries.

        Args:
            name: Unique parser name (e.g., "pubmed_parser", "crossref_parser")
            source_type: Citation source type (e.g., "pubmed", "crossref", "semantic_scholar")
            description: Human-readable description of what this parser does
            formats: List of supported file formats (e.g., ["xml", "json"])
            encoding: File encoding to use when parsing
            **kwargs: Additional parser-specific metadata

        Returns:
            Decorator function

    """

    def decorator(func: F) -> F:
        # Build metadata dictionary
        metadata = {
            "handler_type": "parser",
            "name": name,
            "source_type": source_type,
            "description": description,
            "formats": formats or [],
            "encoding": encoding,
        }
        metadata.update(kwargs)

        # Store metadata on function
        func._cite_handler_type = "parser"  # type: ignore
        func._cite_metadata = metadata  # type: ignore

        # Register with CitationRegistry
        CitationRegistry.register(
            handler_name=name,
            handler_func=func,
            handler_type="parser",
            metadata=metadata,
        )

        # Add convenience methods
        @wraps(func)
        def wrapper(*args, **wrapper_kwargs):
            """Wrapped parser with logging."""
            logger.debug(f"Calling parser '{name}' for source '{source_type}'")
            result = func(*args, **wrapper_kwargs)
            logger.debug(f"Parser '{name}' completed")
            return result

        # Attach metadata to wrapper
        wrapper.handler_name = name  # type: ignore
        wrapper.source_type = source_type  # type: ignore
        wrapper.handler_type = "parser"  # type: ignore
        wrapper.formats = formats or []  # type: ignore

        logger.info(
            f"Registered citation parser: {name}",
            source_type=source_type,
            formats=formats or [],
        )

        return wrapper  # type: ignore

    return decorator


def citation_processor(
    name: str,
    processor_type: str = "general",
    description: str = "",
    idempotent: bool = True,
    depends_on: list[str] | None = None,
    **kwargs: Any,
) -> Callable[[F], F]:
    """
    Register a citation processor function.

        This decorator registers a function that processes/transforms citation data.
        The processor should accept citations and return processed citations.

        Args:
            name: Unique processor name (e.g., "deduplication", "standardize_authors")
            processor_type: Type of processing (cleaning, deduplication, normalization, etc.)
            description: Human-readable description
            idempotent: Whether this processor is safe to run multiple times
            depends_on: List of other processors this depends on
            **kwargs: Additional processor-specific metadata

        Returns:
            Decorator function

    """

    def decorator(func: F) -> F:
        # Build metadata dictionary
        metadata = {
            "handler_type": "processor",
            "name": name,
            "processor_type": processor_type,
            "description": description,
            "idempotent": idempotent,
            "depends_on": depends_on or [],
        }
        metadata.update(kwargs)

        # Store metadata on function
        func._cite_handler_type = "processor"  # type: ignore
        func._cite_metadata = metadata  # type: ignore

        # Register with CitationRegistry
        CitationRegistry.register(
            handler_name=name,
            handler_func=func,
            handler_type="processor",
            metadata=metadata,
        )

        # Add convenience methods
        @wraps(func)
        def wrapper(*args, **wrapper_kwargs):
            """Wrapped processor with logging."""
            logger.debug(f"Calling processor '{name}' (type: {processor_type})")
            result = func(*args, **wrapper_kwargs)
            logger.debug(f"Processor '{name}' completed")
            return result

        # Attach metadata to wrapper
        wrapper.handler_name = name  # type: ignore
        wrapper.processor_type = processor_type  # type: ignore
        wrapper.handler_type = "processor"  # type: ignore
        wrapper.is_idempotent = idempotent  # type: ignore

        logger.info(
            f"Registered citation processor: {name}",
            processor_type=processor_type,
            idempotent=idempotent,
        )

        return wrapper  # type: ignore

    return decorator


def citation_enricher(
    name: str,
    enricher_type: str = "general",
    description: str = "",
    sources: list[str] | None = None,
    requires_api: bool = False,
    **kwargs: Any,
) -> Callable[[F], F]:
    """
    Register a citation enricher function.

        This decorator registers a function that enriches citations with additional metadata.
        The enricher should accept citations and return enriched citations.

        Args:
            name: Unique enricher name (e.g., "add_abstracts", "fetch_metrics")
            enricher_type: Type of enrichment (metadata, references, metrics, etc.)
            description: Human-readable description
            sources: List of external sources used (crossref, semantic_scholar, etc.)
            requires_api: Whether this enricher requires API access
            **kwargs: Additional enricher-specific metadata

        Returns:
            Decorator function

    """

    def decorator(func: F) -> F:
        # Build metadata dictionary
        metadata = {
            "handler_type": "enricher",
            "name": name,
            "enricher_type": enricher_type,
            "description": description,
            "sources": sources or [],
            "requires_api": requires_api,
        }
        metadata.update(kwargs)

        # Store metadata on function
        func._cite_handler_type = "enricher"  # type: ignore
        func._cite_metadata = metadata  # type: ignore

        # Register with CitationRegistry
        CitationRegistry.register(
            handler_name=name,
            handler_func=func,
            handler_type="enricher",
            metadata=metadata,
        )

        # Add convenience methods
        @wraps(func)
        def wrapper(*args, **wrapper_kwargs):
            """Wrapped enricher with logging."""
            logger.debug(f"Calling enricher '{name}' (type: {enricher_type})")
            result = func(*args, **wrapper_kwargs)
            logger.debug(f"Enricher '{name}' completed")
            return result

        # Attach metadata to wrapper
        wrapper.handler_name = name  # type: ignore
        wrapper.enricher_type = enricher_type  # type: ignore
        wrapper.handler_type = "enricher"  # type: ignore
        wrapper.requires_api = requires_api  # type: ignore

        logger.info(
            f"Registered citation enricher: {name}",
            enricher_type=enricher_type,
            sources=sources or [],
        )

        return wrapper  # type: ignore

    return decorator


def citation_exporter(
    name: str,
    format: str,
    description: str = "",
    extensions: list[str] | None = None,
    **kwargs: Any,
) -> Callable[[F], F]:
    """
    Register a citation exporter function.

        This decorator registers a function that exports citations to a specific format.
        The exporter should accept citations and output path, writing the formatted output.

        Args:
            name: Unique exporter name (e.g., "bibtex_exporter", "csv_exporter")
            format: Output format (bibtex, json, csv, ris, etc.)
            description: Human-readable description
            extensions: List of file extensions for this format (e.g., [".bib", ".bibtex"])
            **kwargs: Additional exporter-specific metadata

        Returns:
            Decorator function

    """

    def decorator(func: F) -> F:
        # Build metadata dictionary
        metadata = {
            "handler_type": "exporter",
            "name": name,
            "format": format,
            "description": description,
            "extensions": extensions or [],
        }
        metadata.update(kwargs)

        # Store metadata on function
        func._cite_handler_type = "exporter"  # type: ignore
        func._cite_metadata = metadata  # type: ignore

        # Register with CitationRegistry
        CitationRegistry.register(
            handler_name=name,
            handler_func=func,
            handler_type="exporter",
            metadata=metadata,
        )

        # Add convenience methods
        @wraps(func)
        def wrapper(*args, **wrapper_kwargs):
            """Wrapped exporter with logging."""
            logger.debug(f"Calling exporter '{name}' (format: {format})")
            result = func(*args, **wrapper_kwargs)
            logger.debug(f"Exporter '{name}' completed")
            return result

        # Attach metadata to wrapper
        wrapper.handler_name = name  # type: ignore
        wrapper.format = format  # type: ignore
        wrapper.handler_type = "exporter"  # type: ignore
        wrapper.extensions = extensions or []  # type: ignore

        logger.info(
            f"Registered citation exporter: {name}",
            format=format,
            extensions=extensions or [],
        )

        return wrapper  # type: ignore

    return decorator


def with_state_tracking(
    state_tracker: str | None = None,
) -> Callable[[F], F]:
    """
    Add state tracking to a citation handler.

        This decorator wraps a handler to automatically track processed files
        using the CiteStateTracker.

        Args:
            state_tracker: Optional name of state tracker instance to use

        Returns:
            Decorator function

    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **wrapper_kwargs):
            """Wrapped handler with state tracking."""
            # Get state tracker
            from . import get_state_tracker

            tracker = get_state_tracker()

            # Call original function
            result = func(*args, **wrapper_kwargs)

            # Track file if first arg is a Path
            if args and hasattr(args[0], "name"):
                from pathlib import Path

                if isinstance(args[0], Path):
                    source_type = getattr(func, "source_type", "unknown")
                    tracker.mark_file_processed(
                        file_path=args[0],
                        source_type=source_type,
                        record_count=len(result) if isinstance(result, list) else None,
                    )

            return result

        return wrapper  # type: ignore

    return decorator
