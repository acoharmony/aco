"""Tests for acoharmony._expressions.inspect module."""

import pytest

from acoharmony._expressions.inspect import (
    print_expression_metadata,
    print_expressions_for_schema,
)


class TestPrintExpressionMetadata:
    """Cover print_expression_metadata function."""

    @pytest.mark.unit
    def test_print_all_expressions(self, capsys):
        """Show all registered expressions."""
        print_expression_metadata()
        captured = capsys.readouterr()
        assert "Expression Type" in captured.out
        assert "Total expressions:" in captured.out

    @pytest.mark.unit
    def test_print_specific_expression(self, capsys):
        """Show specific expression metadata."""
        print_expression_metadata("sva_log")
        captured = capsys.readouterr()
        assert "sva_log" in captured.out

    @pytest.mark.unit
    def test_print_unknown_expression(self, capsys):
        """Unknown expression name prints not found."""
        print_expression_metadata("nonexistent_expr")
        captured = capsys.readouterr()
        assert "not found" in captured.out


class TestPrintExpressionsForSchema:
    """Cover print_expressions_for_schema function."""

    @pytest.mark.unit
    def test_print_silver_expressions(self, capsys):
        """List expressions for silver schema."""
        print_expressions_for_schema("silver")
        captured = capsys.readouterr()
        assert "silver" in captured.out
        assert "Total:" in captured.out

    @pytest.mark.unit
    def test_print_with_dataset_filter(self, capsys):
        """Filter by dataset type."""
        print_expressions_for_schema("silver", "claims")
        captured = capsys.readouterr()
        assert "silver" in captured.out

    @pytest.mark.unit
    def test_print_empty_schema(self, capsys):
        """Non-matching schema returns no results."""
        print_expressions_for_schema("nonexistent")
        captured = capsys.readouterr()
        assert "No applicable expressions found" in captured.out


class TestInspectMain:
    """Cover main() lines 95-113."""

    @pytest.mark.unit
    def test_main_no_args(self, capsys, monkeypatch):
        """main() with no args shows all expressions."""
        from acoharmony._expressions.inspect import main
        monkeypatch.setattr("sys.argv", ["inspect"])
        main()
        captured = capsys.readouterr()
        assert "Total expressions:" in captured.out

    @pytest.mark.unit
    def test_main_with_schema(self, capsys, monkeypatch):
        """main() with --schema shows filtered expressions."""
        from acoharmony._expressions.inspect import main
        monkeypatch.setattr("sys.argv", ["inspect", "--schema", "silver"])
        main()
        captured = capsys.readouterr()
        assert "silver" in captured.out

    @pytest.mark.unit
    def test_main_with_expression(self, capsys, monkeypatch):
        """main() with expression name shows specific expression."""
        from acoharmony._expressions.inspect import main
        monkeypatch.setattr("sys.argv", ["inspect", "sva_log"])
        main()
        captured = capsys.readouterr()
        assert "sva_log" in captured.out
