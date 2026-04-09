"""Tests for acoharmony._cite.decorators module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._cite.decorators is not None


# ===========================================================================
# 2. Decorator tests
# ===========================================================================


class TestDecorators:
    """Tests for all five decorator factories."""

    def setup_method(self):
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.clear()

    def teardown_method(self):
        from acoharmony._cite.registry import CitationRegistry

        CitationRegistry.clear()

    # -- citation_parser --------------------------------------------------------

    @pytest.mark.unit
    def test_citation_parser_registers_and_wraps(self):
        from acoharmony._cite.decorators import citation_parser
        from acoharmony._cite.registry import CitationRegistry

        @citation_parser(
            name="test_p",
            source_type="pubmed",
            description="desc",
            formats=["xml"],
            encoding="utf-8",
            extra_key="v",
        )
        def parse_pubmed(data):
            return ["result"]

        # Wrapper attributes
        assert parse_pubmed.handler_name == "test_p"
        assert parse_pubmed.source_type == "pubmed"
        assert parse_pubmed.handler_type == "parser"
        assert parse_pubmed.formats == ["xml"]

        # Registered in registry
        assert "test_p" in CitationRegistry.list_parsers("pubmed")
        meta = CitationRegistry.get_metadata("test_p")
        assert meta["description"] == "desc"
        assert meta["encoding"] == "utf-8"
        assert meta["extra_key"] == "v"

        # Callable
        assert parse_pubmed("x") == ["result"]

    @pytest.mark.unit
    def test_citation_parser_default_formats(self):
        from acoharmony._cite.decorators import citation_parser

        @citation_parser(name="dp", source_type="crossref")
        def fn(x):
            return x

        assert fn.formats == []

    # -- citation_processor -----------------------------------------------------

    @pytest.mark.unit
    def test_citation_processor_registers_and_wraps(self):
        from acoharmony._cite.decorators import citation_processor
        from acoharmony._cite.registry import CitationRegistry

        @citation_processor(
            name="test_proc",
            processor_type="cleaning",
            description="d",
            idempotent=False,
            depends_on=["a"],
        )
        def clean(data):
            return data

        assert clean.handler_name == "test_proc"
        assert clean.processor_type == "cleaning"
        assert clean.handler_type == "processor"
        assert clean.is_idempotent is False
        assert "test_proc" in CitationRegistry.list_processors("cleaning")
        meta = CitationRegistry.get_metadata("test_proc")
        assert meta["depends_on"] == ["a"]
        assert clean([1, 2]) == [1, 2]

    @pytest.mark.unit
    def test_citation_processor_defaults(self):
        from acoharmony._cite.decorators import citation_processor

        @citation_processor(name="dp2")
        def fn(x):
            return x

        assert fn.is_idempotent is True

    # -- citation_enricher ------------------------------------------------------

    @pytest.mark.unit
    def test_citation_enricher_registers_and_wraps(self):
        from acoharmony._cite.decorators import citation_enricher
        from acoharmony._cite.registry import CitationRegistry

        @citation_enricher(
            name="test_en",
            enricher_type="metadata",
            description="d",
            sources=["crossref"],
            requires_api=True,
            custom=1,
        )
        def enrich(data):
            return data + [1]

        assert enrich.handler_name == "test_en"
        assert enrich.enricher_type == "metadata"
        assert enrich.handler_type == "enricher"
        assert enrich.requires_api is True
        assert "test_en" in CitationRegistry.list_enrichers("metadata")
        meta = CitationRegistry.get_metadata("test_en")
        assert meta["sources"] == ["crossref"]
        assert meta["custom"] == 1
        assert enrich([]) == [1]

    @pytest.mark.unit
    def test_citation_enricher_defaults(self):
        from acoharmony._cite.decorators import citation_enricher

        @citation_enricher(name="de")
        def fn(x):
            return x

        assert fn.requires_api is False

    # -- citation_exporter ------------------------------------------------------

    @pytest.mark.unit
    def test_citation_exporter_registers_and_wraps(self):
        from acoharmony._cite.decorators import citation_exporter
        from acoharmony._cite.registry import CitationRegistry

        @citation_exporter(
            name="test_ex", format="bibtex", description="d", extensions=[".bib"], extra=99
        )
        def export(data, path):
            return "done"

        assert export.handler_name == "test_ex"
        assert export.format == "bibtex"
        assert export.handler_type == "exporter"
        assert export.extensions == [".bib"]
        assert "test_ex" in CitationRegistry.list_exporters("bibtex")
        meta = CitationRegistry.get_metadata("test_ex")
        assert meta["extra"] == 99
        assert export([], "/tmp") == "done"

    @pytest.mark.unit
    def test_citation_exporter_defaults(self):
        from acoharmony._cite.decorators import citation_exporter

        @citation_exporter(name="dex", format="csv")
        def fn(x, p):
            return x

        assert fn.extensions == []

    # -- with_state_tracking ----------------------------------------------------

    @pytest.mark.unit
    def test_with_state_tracking_calls_tracker_for_path_arg(self):
        from acoharmony._cite.decorators import with_state_tracking

        mock_tracker = MagicMock()
        with patch("acoharmony._cite.get_state_tracker", return_value=mock_tracker):

            @with_state_tracking()
            def handler(file_path):
                return ["a", "b"]

            result = handler(Path("/tmp/test.xml"))

        assert result == ["a", "b"]
        mock_tracker.mark_file_processed.assert_called_once()
        call_kwargs = mock_tracker.mark_file_processed.call_args
        assert call_kwargs.kwargs["file_path"] == Path("/tmp/test.xml")
        assert call_kwargs.kwargs["record_count"] == 2

    @pytest.mark.unit
    def test_with_state_tracking_skips_non_path_arg(self):
        from acoharmony._cite.decorators import with_state_tracking

        mock_tracker = MagicMock()
        with patch("acoharmony._cite.get_state_tracker", return_value=mock_tracker):

            @with_state_tracking()
            def handler(data):
                return data

            result = handler("just a string")

        assert result == "just a string"
        mock_tracker.mark_file_processed.assert_not_called()

    @pytest.mark.unit
    def test_with_state_tracking_no_args(self):
        from acoharmony._cite.decorators import with_state_tracking

        mock_tracker = MagicMock()
        with patch("acoharmony._cite.get_state_tracker", return_value=mock_tracker):

            @with_state_tracking()
            def handler():
                return 42

            assert handler() == 42
            mock_tracker.mark_file_processed.assert_not_called()

    @pytest.mark.unit
    def test_with_state_tracking_non_list_result(self):
        from acoharmony._cite.decorators import with_state_tracking

        mock_tracker = MagicMock()
        with patch("acoharmony._cite.get_state_tracker", return_value=mock_tracker):

            @with_state_tracking(state_tracker="mytracker")
            def handler(file_path):
                return {"key": "value"}  # not a list

            result = handler(Path("/tmp/test.xml"))

        assert result == {"key": "value"}
        call_kwargs = mock_tracker.mark_file_processed.call_args
        assert call_kwargs.kwargs["record_count"] is None

    @pytest.mark.unit
    def test_with_state_tracking_source_type_from_func(self):
        from acoharmony._cite.decorators import with_state_tracking

        mock_tracker = MagicMock()
        with patch("acoharmony._cite.get_state_tracker", return_value=mock_tracker):

            @with_state_tracking()
            def handler(file_path):
                return []

            handler.source_type = "pubmed"
            handler(Path("/tmp/test.xml"))

        call_kwargs = mock_tracker.mark_file_processed.call_args
        assert call_kwargs.kwargs["source_type"] == "unknown"  # wraps inner func, not wrapper

    @pytest.mark.unit
    def test_with_state_tracking_has_name_but_not_path(self):
        """Branch 363->371: first arg has .name attr but is NOT a Path instance.
        Should skip mark_file_processed and just return result."""
        from acoharmony._cite.decorators import with_state_tracking

        mock_tracker = MagicMock()
        with patch("acoharmony._cite.get_state_tracker", return_value=mock_tracker):

            @with_state_tracking()
            def handler(file_obj):
                return ["data"]

            # Create an object that has a .name attribute but is not a Path
            class FakeFile:
                name = "fake.xml"

            result = handler(FakeFile())

        assert result == ["data"]
        mock_tracker.mark_file_processed.assert_not_called()
