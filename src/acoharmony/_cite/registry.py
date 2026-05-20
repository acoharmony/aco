# © 2025 HarmonyCares
# All rights reserved.

"""
Central registry for citation handlers and processors.

This module maintains a global registry of all registered citation handlers:
- Parsers: Parse citations from different sources (PubMed, Crossref, etc.)
- Processors: Transform and clean citation data
- Enrichers: Add metadata and enrich citations
- Exporters: Export citations to different formats
"""

from collections.abc import Callable
from typing import Any


class CitationRegistry:
    """
    Global registry for citation handlers and their metadata.

        This registry connects handler functions with:
        - Handler metadata (name, source_type, description)
        - Parser configuration (format, encoding, etc.)
        - Processor configuration (transformation steps)
        - Enricher configuration (metadata sources)
        - Exporter configuration (output formats)

        The registry is populated automatically via decorators.
    """

    # Handler name -> handler function
    _handlers: dict[str, Callable] = {}

    # Handler name -> metadata
    _metadata: dict[str, dict[str, Any]] = {}

    # Source type -> list of parser handlers
    _parsers: dict[str, list[str]] = {}

    # Processor type -> list of processor handlers
    _processors: dict[str, list[str]] = {}

    # Enricher type -> list of enricher handlers
    _enrichers: dict[str, list[str]] = {}

    # Export format -> list of exporter handlers
    _exporters: dict[str, list[str]] = {}

    @classmethod
    def register(
        cls,
        handler_name: str,
        handler_func: Callable,
        handler_type: str,
        metadata: dict[str, Any],
    ) -> None:
        """
        Register a citation handler with the registry.

                Args:
                    handler_name: Unique handler name
                    handler_func: Handler function
                    handler_type: Type of handler (parser, processor, enricher, exporter)
                    metadata: Handler metadata (source_type, description, etc.)
        """
        cls._handlers[handler_name] = handler_func
        cls._metadata[handler_name] = metadata

        # Register by type-specific collections
        if handler_type == "parser":
            source_type = metadata.get("source_type", "unknown")
            if source_type not in cls._parsers:
                cls._parsers[source_type] = []
            cls._parsers[source_type].append(handler_name)

        elif handler_type == "processor":
            processor_type = metadata.get("processor_type", "general")
            if processor_type not in cls._processors:
                cls._processors[processor_type] = []
            cls._processors[processor_type].append(handler_name)

        elif handler_type == "enricher":
            enricher_type = metadata.get("enricher_type", "general")
            if enricher_type not in cls._enrichers:
                cls._enrichers[enricher_type] = []
            cls._enrichers[enricher_type].append(handler_name)

        elif handler_type == "exporter":
            export_format = metadata.get("format", "unknown")
            if export_format not in cls._exporters:
                cls._exporters[export_format] = []
            cls._exporters[export_format].append(handler_name)

    @classmethod
    def get_handler(cls, handler_name: str) -> Callable | None:
        """
        Get a handler function by name.

                Args:
                    handler_name: Handler name

                Returns:
                    Handler function or None if not found
        """
        return cls._handlers.get(handler_name)

    @classmethod
    def get_metadata(cls, handler_name: str) -> dict[str, Any]:
        """
        Get metadata for a handler.

                Args:
                    handler_name: Handler name

                Returns:
                    Metadata dictionary
        """
        return cls._metadata.get(handler_name, {})

    @classmethod
    def list_handlers(cls, handler_type: str | None = None) -> list[str]:
        """
        List all registered handlers, optionally filtered by type.

                Args:
                    handler_type: Optional handler type filter (parser, processor, enricher, exporter)

                Returns:
                    List of handler names
        """
        if handler_type is None:
            return list(cls._handlers.keys())

        # Filter by type
        handlers = []
        for name, meta in cls._metadata.items():
            if meta.get("handler_type") == handler_type:
                handlers.append(name)
        return handlers

    @classmethod
    def list_parsers(cls, source_type: str | None = None) -> list[str]:
        """
        List registered parsers, optionally filtered by source type.

                Args:
                    source_type: Optional source type filter (pubmed, crossref, semantic_scholar, etc.)

                Returns:
                    List of parser handler names
        """
        if source_type is None:
            # Return all parsers
            all_parsers = []
            for parsers in cls._parsers.values():
                all_parsers.extend(parsers)
            return all_parsers

        return cls._parsers.get(source_type, [])

    @classmethod
    def list_processors(cls, processor_type: str | None = None) -> list[str]:
        """
        List registered processors, optionally filtered by processor type.

                Args:
                    processor_type: Optional processor type filter (cleaning, deduplication, etc.)

                Returns:
                    List of processor handler names
        """
        if processor_type is None:
            all_processors = []
            for processors in cls._processors.values():
                all_processors.extend(processors)
            return all_processors

        return cls._processors.get(processor_type, [])

    @classmethod
    def list_enrichers(cls, enricher_type: str | None = None) -> list[str]:
        """
        List registered enrichers, optionally filtered by enricher type.

                Args:
                    enricher_type: Optional enricher type filter (metadata, references, etc.)

                Returns:
                    List of enricher handler names
        """
        if enricher_type is None:
            all_enrichers = []
            for enrichers in cls._enrichers.values():
                all_enrichers.extend(enrichers)
            return all_enrichers

        return cls._enrichers.get(enricher_type, [])

    @classmethod
    def list_exporters(cls, export_format: str | None = None) -> list[str]:
        """
        List registered exporters, optionally filtered by export format.

                Args:
                    export_format: Optional export format filter (bibtex, json, csv, etc.)

                Returns:
                    List of exporter handler names
        """
        if export_format is None:
            all_exporters = []
            for exporters in cls._exporters.values():
                all_exporters.extend(exporters)
            return all_exporters

        return cls._exporters.get(export_format, [])

    @classmethod
    def get_parser_for_source(cls, source_type: str) -> Callable | None:
        """
        Get the first available parser for a source type.

                Args:
                    source_type: Citation source type (pubmed, crossref, etc.)

                Returns:
                    Parser handler function or None
        """
        parsers = cls.list_parsers(source_type)
        if parsers:
            return cls.get_handler(parsers[0])
        return None

    @classmethod
    def clear(cls) -> None:
        """Clear all registered handlers (primarily for testing)."""
        cls._handlers.clear()
        cls._metadata.clear()
        cls._parsers.clear()
        cls._processors.clear()
        cls._enrichers.clear()
        cls._exporters.clear()


# Convenience functions for accessing the registry
def get_handler(handler_name: str) -> Callable | None:
    """Get handler function by name."""
    return CitationRegistry.get_handler(handler_name)


def get_parser(source_type: str) -> Callable | None:
    """Get parser for a citation source type."""
    return CitationRegistry.get_parser_for_source(source_type)


def list_parsers(source_type: str | None = None) -> list[str]:
    """List available citation parsers."""
    return CitationRegistry.list_parsers(source_type)


def list_processors(processor_type: str | None = None) -> list[str]:
    """List available citation processors."""
    return CitationRegistry.list_processors(processor_type)


def list_enrichers(enricher_type: str | None = None) -> list[str]:
    """List available citation enrichers."""
    return CitationRegistry.list_enrichers(enricher_type)


def list_exporters(export_format: str | None = None) -> list[str]:
    """List available citation exporters."""
    return CitationRegistry.list_exporters(export_format)
