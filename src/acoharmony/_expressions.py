# © 2025 HarmonyCares
# All rights reserved.

"""
Expression generation system for data transformations.

This module provides expression builders that return Polars expressions
(pl.Expr or list[pl.Expr]) for use in transforms.

All expressions follow the pattern of returning pl.Expr, NOT pl.LazyFrame.
Transforms handle LazyFrame orchestration.
"""

# Re-export key components from _expressions module
from ._expressions import (  # pragma: no cover
    ExpressionRegistry,
    register_expression,
)

__all__ = [  # pragma: no cover
    "ExpressionRegistry",
    "register_expression",
]
