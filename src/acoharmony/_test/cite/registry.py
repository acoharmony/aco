"""Tests for acoharmony._cite.registry module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._cite.registry is not None


# ===========================================================================
# 1. Registry tests
# ===========================================================================


class TestCitationRegistry:
    """Tests for CitationRegistry class and module-level convenience functions."""

    def setup_method(self):
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.clear()

    def teardown_method(self):
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.clear()

    # -- register & get_handler -------------------------------------------------

    @pytest.mark.unit
    def test_register_and_get_handler(self):
        from acoharmony._cite.registry import CitationRegistry

        def my_func():
            return "ok"

        CitationRegistry.register(
            "h1", my_func, "parser", {"handler_type": "parser", "source_type": "pubmed"}
        )
        assert CitationRegistry.get_handler("h1") is my_func

    @pytest.mark.unit
    def test_get_handler_missing_returns_none(self):
        from acoharmony._cite.registry import CitationRegistry

        assert CitationRegistry.get_handler("nonexistent") is None

    # -- get_metadata -----------------------------------------------------------

    @pytest.mark.unit
    def test_get_metadata(self):
        from acoharmony._cite.registry import CitationRegistry

        meta = {"handler_type": "parser", "source_type": "crossref", "extra": 42}
        CitationRegistry.register("m1", lambda: None, "parser", meta)
        assert CitationRegistry.get_metadata("m1") == meta

    @pytest.mark.unit
    def test_get_metadata_missing_returns_empty(self):
        from acoharmony._cite.registry import CitationRegistry

        assert CitationRegistry.get_metadata("nope") == {}

    # -- list_handlers ----------------------------------------------------------

    @pytest.mark.unit
    def test_list_handlers_all(self):
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.register(
            "a", lambda: None, "parser", {"handler_type": "parser", "source_type": "x"}
        )
        CitationRegistry.register(
            "b", lambda: None, "processor", {"handler_type": "processor", "processor_type": "y"}
        )
        names = CitationRegistry.list_handlers()
        assert "a" in names
        assert "b" in names

    @pytest.mark.unit
    def test_list_handlers_filtered(self):
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.register(
            "p1", lambda: None, "parser", {"handler_type": "parser", "source_type": "x"}
        )
        CitationRegistry.register(
            "e1", lambda: None, "exporter", {"handler_type": "exporter", "format": "csv"}
        )
        assert CitationRegistry.list_handlers("parser") == ["p1"]
        assert CitationRegistry.list_handlers("exporter") == ["e1"]
        assert CitationRegistry.list_handlers("enricher") == []

    # -- type-specific listing --------------------------------------------------

    @pytest.mark.unit
    def test_list_parsers_all_and_by_source(self):
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.register(
            "pa", lambda: None, "parser", {"handler_type": "parser", "source_type": "pubmed"}
        )
        CitationRegistry.register(
            "pb", lambda: None, "parser", {"handler_type": "parser", "source_type": "crossref"}
        )
        all_p = CitationRegistry.list_parsers()
        assert set(all_p) == {"pa", "pb"}
        assert CitationRegistry.list_parsers("pubmed") == ["pa"]
        assert CitationRegistry.list_parsers("unknown_source") == []

    @pytest.mark.unit
    def test_list_processors_all_and_by_type(self):
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.register(
            "pr1",
            lambda: None,
            "processor",
            {"handler_type": "processor", "processor_type": "cleaning"},
        )
        assert CitationRegistry.list_processors() == ["pr1"]
        assert CitationRegistry.list_processors("cleaning") == ["pr1"]
        assert CitationRegistry.list_processors("nope") == []

    @pytest.mark.unit
    def test_list_enrichers_all_and_by_type(self):
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.register(
            "en1",
            lambda: None,
            "enricher",
            {"handler_type": "enricher", "enricher_type": "metadata"},
        )
        assert CitationRegistry.list_enrichers() == ["en1"]
        assert CitationRegistry.list_enrichers("metadata") == ["en1"]
        assert CitationRegistry.list_enrichers("nope") == []

    @pytest.mark.unit
    def test_list_exporters_all_and_by_format(self):
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.register(
            "ex1", lambda: None, "exporter", {"handler_type": "exporter", "format": "bibtex"}
        )
        assert CitationRegistry.list_exporters() == ["ex1"]
        assert CitationRegistry.list_exporters("bibtex") == ["ex1"]
        assert CitationRegistry.list_exporters("csv") == []

    # -- get_parser_for_source --------------------------------------------------

    @pytest.mark.unit
    def test_get_parser_for_source_found(self):
        from acoharmony._cite.registry import CitationRegistry

        def fn():
            return "parsed"

        CitationRegistry.register(
            "ps", fn, "parser", {"handler_type": "parser", "source_type": "semantic_scholar"}
        )
        assert CitationRegistry.get_parser_for_source("semantic_scholar") is fn

    @pytest.mark.unit
    def test_get_parser_for_source_not_found(self):
        from acoharmony._cite.registry import CitationRegistry

        assert CitationRegistry.get_parser_for_source("nope") is None

    # -- clear ------------------------------------------------------------------

    @pytest.mark.unit
    def test_clear(self):
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.register(
            "z", lambda: None, "parser", {"handler_type": "parser", "source_type": "x"}
        )
        CitationRegistry.clear()
        assert CitationRegistry.list_handlers() == []
        assert CitationRegistry.list_parsers() == []
        assert CitationRegistry.list_processors() == []
        assert CitationRegistry.list_enrichers() == []
        assert CitationRegistry.list_exporters() == []

    # -- module-level convenience functions -------------------------------------

    @pytest.mark.unit
    def test_module_get_handler(self):
        from acoharmony._cite.registry import CitationRegistry, get_handler

        def fn():
            return None

        CitationRegistry.register(
            "mh", fn, "parser", {"handler_type": "parser", "source_type": "x"}
        )
        assert get_handler("mh") is fn

    @pytest.mark.unit
    def test_module_get_parser(self):
        from acoharmony._cite.registry import CitationRegistry, get_parser

        def fn():
            return None

        CitationRegistry.register(
            "mp", fn, "parser", {"handler_type": "parser", "source_type": "pm"}
        )
        assert get_parser("pm") is fn

    @pytest.mark.unit
    def test_module_list_parsers(self):
        from acoharmony._cite.registry import CitationRegistry, list_parsers

        CitationRegistry.register(
            "lp1", lambda: None, "parser", {"handler_type": "parser", "source_type": "x"}
        )
        assert "lp1" in list_parsers()
        assert "lp1" in list_parsers("x")

    @pytest.mark.unit
    def test_module_list_processors(self):
        from acoharmony._cite.registry import CitationRegistry, list_processors

        CitationRegistry.register(
            "lpr", lambda: None, "processor", {"handler_type": "processor", "processor_type": "g"}
        )
        assert "lpr" in list_processors()
        assert "lpr" in list_processors("g")

    @pytest.mark.unit
    def test_module_list_enrichers(self):
        from acoharmony._cite.registry import CitationRegistry, list_enrichers

        CitationRegistry.register(
            "le", lambda: None, "enricher", {"handler_type": "enricher", "enricher_type": "m"}
        )
        assert "le" in list_enrichers()
        assert "le" in list_enrichers("m")

    @pytest.mark.unit
    def test_module_list_exporters(self):
        from acoharmony._cite.registry import CitationRegistry, list_exporters

        CitationRegistry.register(
            "lex", lambda: None, "exporter", {"handler_type": "exporter", "format": "csv"}
        )
        assert "lex" in list_exporters()
        assert "lex" in list_exporters("csv")


class TestCitationRegistryExistingTypeKeys:
    """Cover branches 73->75, 79->81, 85->87, 89->-50, 91->93.

    These branches are taken when a type key already exists in the
    respective dict (parsers, processors, enrichers, exporters), so
    the `if ... not in ...` check is False and we skip dict creation.
    """

    def setup_method(self):
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.clear()

    def teardown_method(self):
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.clear()

    @pytest.mark.unit
    def test_register_second_parser_same_source_type(self):
        """Branch 73->75: _parsers already has the source_type key."""
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.register(
            "p1", lambda: None, "parser", {"source_type": "pubmed"}
        )
        CitationRegistry.register(
            "p2", lambda: None, "parser", {"source_type": "pubmed"}
        )
        parsers = CitationRegistry.list_parsers("pubmed")
        assert "p1" in parsers
        assert "p2" in parsers

    @pytest.mark.unit
    def test_register_second_processor_same_type(self):
        """Branch 79->81: _processors already has the processor_type key."""
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.register(
            "pr1", lambda: None, "processor", {"processor_type": "cleaning"}
        )
        CitationRegistry.register(
            "pr2", lambda: None, "processor", {"processor_type": "cleaning"}
        )
        procs = CitationRegistry.list_processors("cleaning")
        assert "pr1" in procs
        assert "pr2" in procs

    @pytest.mark.unit
    def test_register_second_enricher_same_type(self):
        """Branch 85->87: _enrichers already has the enricher_type key."""
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.register(
            "e1", lambda: None, "enricher", {"enricher_type": "metadata"}
        )
        CitationRegistry.register(
            "e2", lambda: None, "enricher", {"enricher_type": "metadata"}
        )
        enrichers = CitationRegistry.list_enrichers("metadata")
        assert "e1" in enrichers
        assert "e2" in enrichers

    @pytest.mark.unit
    def test_register_second_exporter_same_format(self):
        """Branch 91->93: _exporters already has the format key."""
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.register(
            "ex1", lambda: None, "exporter", {"format": "bibtex"}
        )
        CitationRegistry.register(
            "ex2", lambda: None, "exporter", {"format": "bibtex"}
        )
        exporters = CitationRegistry.list_exporters("bibtex")
        assert "ex1" in exporters
        assert "ex2" in exporters

    @pytest.mark.unit
    def test_register_unknown_handler_type(self):
        """Branch 89->-50: handler_type is not parser/processor/enricher/exporter."""
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.register(
            "u1", lambda: None, "unknown_type", {"some": "data"}
        )
        # Handler is registered but not in any type-specific collection
        assert CitationRegistry.get_handler("u1") is not None
        assert CitationRegistry.list_parsers() == []
        assert CitationRegistry.list_processors() == []
        assert CitationRegistry.list_enrichers() == []
        assert CitationRegistry.list_exporters() == []
