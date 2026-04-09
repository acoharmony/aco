# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for preliminary_benchmark_report_unredacted schema.

Generated from: _schemas/preliminary_benchmark_report_unredacted.yml

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_four_icli,
    with_parser,
    with_storage,
    with_transform,
)


@register_schema(
    name="preliminary_benchmark_report_unredacted",
    version=2,
    tier="bronze",
    description="Preliminary Benchmark Report Unredacted",
    file_patterns={"reach": ["*PRBRU*"]},
)
@with_parser(type="excel", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*PRBRU*"]},
    silver={
        "output_name": "preliminary_benchmark_report_unredacted.parquet",
        "refresh_frequency": "monthly",
    },
)
@with_four_icli(
    category="Reports",
    file_type_code=219,
    file_pattern="REACH.D????.PRBRU.PY????.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class PreliminaryBenchmarkReportUnredacted:
    """
    Preliminary Benchmark Report Unredacted

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - PreliminaryBenchmarkReportUnredacted.schema_name() -> str
        - PreliminaryBenchmarkReportUnredacted.schema_metadata() -> dict
        - PreliminaryBenchmarkReportUnredacted.parser_config() -> dict
        - PreliminaryBenchmarkReportUnredacted.transform_config() -> dict
        - PreliminaryBenchmarkReportUnredacted.lineage_config() -> dict
    """

    pass  # No columns defined

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PreliminaryBenchmarkReportUnredacted":
        """Create instance from dictionary."""
        return cls(**data)
