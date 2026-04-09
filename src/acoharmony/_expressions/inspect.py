#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""
Expression metadata inspector.

Utility to inspect registered expressions and their schema/dataset applicability.
"""

from ._registry import ExpressionRegistry


def print_expression_metadata(expression_type: str | None = None):
    """
    Print expression metadata in a formatted table.

        Args:
            expression_type: If provided, show only this expression. Otherwise show all.
    """
    if expression_type:
        # Show single expression
        metadata = ExpressionRegistry.get_metadata(expression_type)
        if not metadata:
            print(f"Expression '{expression_type}' not found")
            return

        print(f"\n{'=' * 80}")
        print(f"Expression: {expression_type}")
        print(f"{'=' * 80}")
        print(f"Class:        {metadata.get('class', 'N/A')}")
        print(f"Callable:     {metadata.get('callable', True)}")
        print(f"Schemas:      {', '.join(metadata.get('schemas', []))}")
        print(f"Dataset Types: {', '.join(metadata.get('dataset_types', [])) or 'All'}")
        print("\nDescription:")
        print(f"  {metadata.get('description', 'No description')}")
        print(f"{'=' * 80}\n")
    else:
        # Show all expressions
        expr_types = sorted(ExpressionRegistry.list_builders())

        print(f"\n{'=' * 100}")
        print(f"{'Expression Type':<30} {'Schemas':<20} {'Dataset Types':<25} {'Callable':<10}")
        print(f"{'=' * 100}")

        for expr_type in expr_types:
            metadata = ExpressionRegistry.get_metadata(expr_type)
            schemas = ", ".join(metadata.get("schemas", [])) if metadata else "N/A"
            dataset_types = ", ".join(metadata.get("dataset_types", [])) if metadata else ""
            dataset_types = dataset_types or "All"
            callable_str = "Yes" if metadata and metadata.get("callable", True) else "No"

            print(f"{expr_type:<30} {schemas:<20} {dataset_types:<25} {callable_str:<10}")

        print(f"{'=' * 100}\n")
        print(f"Total expressions: {len(expr_types)}\n")


def print_expressions_for_schema(schema: str, dataset_type: str | None = None):
    """
    Print all expressions applicable for a given schema.

        Args:
            schema: Schema name (bronze, silver, gold)
            dataset_type: Optional dataset type filter
    """
    applicable = ExpressionRegistry.list_for_schema(schema, dataset_type)

    filter_str = f" and dataset type '{dataset_type}'" if dataset_type else ""
    print(f"\n{'=' * 80}")
    print(f"Expressions applicable for schema '{schema}'{filter_str}")
    print(f"{'=' * 80}")

    if not applicable:
        print("No applicable expressions found\n")
        return

    print(f"\n{'Expression Type':<30} {'Callable':<10} {'Dataset Types':<30}")
    print(f"{'-' * 80}")

    for expr_type in sorted(applicable):
        metadata = ExpressionRegistry.get_metadata(expr_type)
        callable_str = "Yes" if metadata and metadata.get("callable", True) else "No"
        dataset_types = ", ".join(metadata.get("dataset_types", [])) if metadata else ""
        dataset_types = dataset_types or "All"

        print(f"{expr_type:<30} {callable_str:<10} {dataset_types:<30}")

    print(f"{'-' * 80}")
    print(f"Total: {len(applicable)} expressions\n")


def main():
    """CLI entry point for expression inspection."""
    import argparse

    parser = argparse.ArgumentParser(description="Inspect expression metadata and applicability")
    parser.add_argument(
        "expression", nargs="?", help="Expression type to inspect (default: show all)"
    )
    parser.add_argument(
        "--schema",
        choices=["bronze", "silver", "gold"],
        help="Show expressions for specific schema",
    )
    parser.add_argument("--dataset-type", help="Filter by dataset type (e.g., claims, eligibility)")

    args = parser.parse_args()

    if args.schema:
        print_expressions_for_schema(args.schema, args.dataset_type)
    else:
        print_expression_metadata(args.expression)


if __name__ == "__main__":
    main()
