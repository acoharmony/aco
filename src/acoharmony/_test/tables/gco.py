"""Tests for the gift card order table schema."""

import dataclasses as dc
from typing import Any, cast

import polars as pl
import pytest

from acoharmony._notes._gift_card import GiftCardPlugins
from acoharmony._registry import get_full_table_config
from acoharmony._tables._gco import GCO_TEMPLATE_HEADERS, Gco


class TestGco:
    @pytest.mark.unit
    def test_template_headers_match_direct_delivery_output(self) -> None:
        mailing_list = pl.DataFrame(
            {
                "first_name": ["A"],
                "last_name": ["X"],
                "mbi": ["1AC2HJ3RT4Y"],
                "address_line_1": ["1 Main"],
                "address_line_2": [None],
                "city": ["Detroit"],
                "state": ["MI"],
                "zip": ["48226"],
            }
        )

        order = GiftCardPlugins().format_direct_delivery(mailing_list)

        assert tuple(order.columns) == GCO_TEMPLATE_HEADERS

    @pytest.mark.unit
    def test_schema_aliases_are_exact_template_headers(self) -> None:
        fields = dc.fields(Gco)
        aliases = tuple(cast(Any, field.default).alias for field in fields)

        assert aliases == GCO_TEMPLATE_HEADERS

    @pytest.mark.unit
    def test_registry_config_exposes_template_source_names(self) -> None:
        config = get_full_table_config("gco")
        source_names = tuple(col["source_name"] for col in config["columns"])

        assert source_names == GCO_TEMPLATE_HEADERS
        assert config["file_format"]["header_driven"] is True
        assert config["storage"]["silver"]["output_name"] == "gco.parquet"

    @pytest.mark.unit
    def test_can_instantiate_with_template_headers(self) -> None:
        row = Gco(
            **cast(
                Any,
                {
                    "First Name": "A",
                    "Last Name": "X",
                    "Company": None,
                    "Recipient ID (Optional)": "1AC2HJ3RT4Y",
                    "Street Address": "1 Main",
                    "Apt #, Floor, etc., (optional)": None,
                    "City": "Detroit",
                    "State/Province/Region": "MI",
                    "Postal Code": "48226",
                    "Country Code": "US",
                    "Shipping Method": "Standard",
                    "Card Name": "Visa",
                    "Available Denominations": None,
                    "Card Value": 50.0,
                    "To": "A X",
                    "From": "HarmonyCares",
                    "Message": "Thank you for completing your Annual Wellness Visit!",
                    "Card Carrier": None,
                },
            )
        )

        assert row.first_name == "A"
        assert row.card_value == 50.0
        assert row.from_name == "HarmonyCares"
