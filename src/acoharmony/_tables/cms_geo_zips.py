# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for cms_geo_zips schema.

Generated from: _schemas/cms_geo_zips.yml
"""

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
)


@register_schema(
    name="cms_geo_zips",
    version=2,
    tier="bronze",
    description="CMS Geographic ZIP Codes - Mapping of ZIP codes to Medicare localities and carriers",
    file_patterns={"quarterly": "*GEO_ZIP*.csv", "quarterly_xlsx": "*GEO_ZIP*.xlsx"},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_storage(
    tier="bronze",
    file_patterns={"quarterly": "*GEO_ZIP*.csv", "quarterly_xlsx": "*GEO_ZIP*.xlsx"},
    medallion_layer="bronze",
    silver={
        "output_name": "cms_geo_zips.parquet",
        "refresh_frequency": "quarterly",
        "last_updated_by": "aco transform cms_geo_zips",
    },
)
@dataclass
class CmsGeoZips:
    """
    CMS Geographic ZIP Codes - Mapping of ZIP codes to Medicare localities and carriers
    """

    geo_state_cd: str = Field(description="Geographic State Code")
    geo_zip_5: str = Field(description="5-Digit ZIP Code")
    year_quarter: str = Field(description="Year and Quarter in YYYYQ format (e.g., 2025Q1)")
    carrier: str | None = Field(default=None, description="Medicare Carrier Number")
    locality: str | None = Field(default=None, description="Medicare Locality Code")
    rural_indicator: str | None = Field(
        default=None, description="Rural Indicator (B=Both, R=Rural, blank=Urban)"
    )
    plus_four_flag: str | None = Field(
        default=None, description="Plus Four ZIP Extension Required Flag"
    )
    part_b_rx_indicator: str | None = Field(
        default=None, description="Part B Prescription Drug Indicator"
    )
