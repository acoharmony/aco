"""Gift card order table schema.

Defines the DirectDelivery gift card order template shape exactly as emitted by
``GiftCardPlugins.format_direct_delivery``.
"""

from typing import Any

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
)

GCO_TEMPLATE_HEADERS = (
    "First Name",
    "Last Name",
    "Company",
    "Recipient ID (Optional)",
    "Street Address",
    "Apt #, Floor, etc., (optional)",
    "City",
    "State/Province/Region",
    "Postal Code",
    "Country Code",
    "Shipping Method",
    "Card Name",
    "Available Denominations",
    "Card Value",
    "To",
    "From",
    "Message",
    "Card Carrier",
)

_MISSING: Any = object()


def _template_field(header: str, description: str, default: Any = _MISSING) -> Any:
    if default is _MISSING:
        return Field(
            alias=header,
            description=description,
            json_schema_extra={"source_name": header},
        )
    return Field(
        default=default,
        alias=header,
        description=description,
        json_schema_extra={"source_name": header},
    )


@register_schema(
    name="gco",
    version=1,
    tier="bronze",
    description="DirectDelivery gift card order template",
    file_patterns={
        "gift_card_order": [
            "DirectDeliveryOrder*.xlsx",
            "*Gift Card*Order*.xlsx",
            "*gift*card*order*.xlsx",
        ],
    },
)
@with_parser(
    type="excel",
    encoding="utf-8",
    has_header=True,
    header_driven=True,
    embedded_transforms=False,
    sheet_name=["Order Details", "Order Sheet"],
)
@with_storage(
    tier="bronze",
    file_patterns={
        "gift_card_order": [
            "DirectDeliveryOrder*.xlsx",
            "*Gift Card*Order*.xlsx",
            "*gift*card*order*.xlsx",
        ],
    },
    medallion_layer="bronze",
    silver={
        "output_name": "gco.parquet",
        "refresh_frequency": "ad hoc",
        "last_updated_by": "aco gift card order",
    },
)
@dataclass
class Gco:
    """DirectDelivery gift card order row."""

    first_name: str = _template_field("First Name", "Recipient first name")
    last_name: str = _template_field("Last Name", "Recipient last name")
    company: str | None = _template_field("Company", "Recipient company", default=None)
    recipient_id_optional: str | None = _template_field(
        "Recipient ID (Optional)",
        "Optional recipient identifier; ACO output uses MBI",
        default=None,
    )
    street_address: str = _template_field("Street Address", "Recipient street address line 1")
    apt_floor_etc_optional: str | None = _template_field(
        "Apt #, Floor, etc., (optional)",
        "Recipient street address line 2",
        default=None,
    )
    city: str = _template_field("City", "Recipient city")
    state_province_region: str = _template_field(
        "State/Province/Region",
        "Recipient state, province, or region",
    )
    postal_code: str = _template_field("Postal Code", "Recipient postal code")
    country_code: str = _template_field("Country Code", "Recipient country code")
    shipping_method: str = _template_field("Shipping Method", "Gift card shipping method")
    card_name: str = _template_field("Card Name", "Gift card product name")
    available_denominations: str | None = _template_field(
        "Available Denominations",
        "Available denomination options from the template",
        default=None,
    )
    card_value: float = _template_field("Card Value", "Gift card value")
    to: str = _template_field("To", "Card recipient display name")
    from_name: str = _template_field("From", "Card sender display name")
    message: str = _template_field("Message", "Gift card message")
    card_carrier: str | None = _template_field(
        "Card Carrier",
        "Optional card carrier value from the template",
        default=None,
    )
