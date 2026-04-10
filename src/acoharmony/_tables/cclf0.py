# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for cclf0 schema.

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_four_icli,
    with_parser,
    with_storage,
)


@register_schema(
    name="cclf0",
    version=2,
    tier="bronze",
    description="CCLF0 Summary Statistics File - Pipe-delimited file containing record counts for all CCLF files",
    file_patterns={
        "mssp": "P.A*.ACO.ZC0Y*.D*.T*",
        "reach": "P.D*.ACO.ZC0Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC0Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC0R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC0Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC0R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC0WY*.D*.T*",
    },
)
@with_parser(
    type="delimited", delimiter="|", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_storage(
    tier="bronze",
    file_patterns={
        "mssp": "P.A*.ACO.ZC0Y*.D*.T*",
        "reach": "P.D*.ACO.ZC0Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC0Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC0R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC0Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC0R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC0WY*.D*.T*",
    },
    medallion_layer="bronze",
    silver={
        "output_name": "cclf0.parquet",
        "refresh_frequency": "weekly",
        "last_updated_by": "scripts/process_raw_to_parquet.py",
    },
    gold={"output_name": None, "refresh_frequency": None, "last_updated_by": None},
)
@with_four_icli(
    category="Claim and Claim Line Feed (CCLF) Files",
    file_type_code=113,
    file_pattern="P.?????.ACO.ZC*??.D??????.T*.zip, P.?????.ACO.ZCWY??.S??????.E??????.D??????.T*.zip, P.?????.ACO.ZC*Y??.D??????.T*, P.?????.ACO.ZC*WY??.D??????.T*, P.?????.ACO.ZC*R??.D??????.T*",
    extract_zip=True,
    refresh_frequency="weekly",
    default_date_filter={"createdWithinLastWeek": True},
)
@dataclass
class Cclf0:
    """
    CCLF0 Summary Statistics File - Pipe-delimited file containing record counts for all CCLF files

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Cclf0.schema_name() -> str
        - Cclf0.schema_metadata() -> dict
        - Cclf0.parser_config() -> dict
        - Cclf0.transform_config() -> dict
        - Cclf0.lineage_config() -> dict
    """

    file_type: str = Field(description="Type of CCLF file (CCLF1, CCLF2, etc.)")
    file_description: str = Field(description="Description of the CCLF file type")
    record_count: int = Field(description="Total number of records in the file", ge=0)
    record_length: int = Field(description="Length of each record in the file")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Cclf0":
        """Create instance from dictionary."""
        return cls(**data)
