# © 2025 HarmonyCares
# All rights reserved.

"""
Pipeline stage definitions for declarative pipeline composition.

Provides PipelineStage and BronzeStage classes that reference transform
modules directly for execution. Used by pipeline definitions in _pipes.
"""

from typing import Any


class PipelineStage:
    """Declarative pipeline stage definition with syntactic sugar."""

    def __init__(
        self, name: str, module: Any, group: str, order: int, depends_on: list[str] | None = None
    ):
        """
        Define a pipeline stage.

        Args:
            name: Output name for the transform
            module: Module containing execute() function
            group: Logical grouping (crosswalk, claims, supporting, enrollment)
            order: Execution order within pipeline
            depends_on: Optional list of stage names this depends on
        """
        self.name = name
        self.module = module
        self.group = group
        self.order = order
        self.depends_on = depends_on or []

    def __repr__(self) -> str:
        deps = f" → depends on {self.depends_on}" if self.depends_on else ""
        return f"Stage({self.order}: {self.name} [{self.group}]{deps})"


class BronzeStage:
    """Declarative bronze parsing."""

    def __init__(
        self,
        name: str,
        group: str,
        order: int,
        description: str = "",
        depends_on: list[str] | None = None,
        optional: bool = False,
    ):
        """
        Define a bronze parsing stage.

        Args:
            name: Schema name to parse (matches YAML file)
            group: Logical grouping (cclf_core, cclf_demographics, reports, etc.)
            order: Execution order within pipeline
            description: Human-readable description
            depends_on: Optional list of schemas this depends on (rarely used in bronze)
            optional: If True, skip if source files don't exist
        """
        self.name = name
        self.group = group
        self.order = order
        self.description = description
        self.depends_on = depends_on or []
        self.optional = optional

    def __repr__(self) -> str:
        opt = " [OPTIONAL]" if self.optional else ""
        deps = f" → depends on {self.depends_on}" if self.depends_on else ""
        return f"BronzeStage({self.order}: {self.name} [{self.group}]{opt}{deps})"
