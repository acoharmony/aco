# © 2025 HarmonyCares
# All rights reserved.

"""
HCPCS code filtering expressions for specific service types.

Implements reusable expressions for:
- Home visit and domiciliary care codes (99341-99350)
- Office visit codes (99202-99205, 99211-99215)
- Visit complexity add-on codes (G2211)
- Advanced primary care management codes (G0556-G0558)

These expressions support filtering claims for specific service categories
in gold layer pipelines.
"""

import polars as pl

from .._decor8 import expression
from ._registry import register_expression


@register_expression(
    "hcpcs_filter",
    schemas=["silver", "gold"],
    dataset_types=["claim", "physician"],
    callable=False,
    description="Filter claims by specific HCPCS codes for service categorization",
)
class HCPCSFilterExpression:
    """
    Expressions for filtering claims by HCPCS codes.

    Handles:
    - Home visit and domiciliary care codes
    - Office visit codes
    - Visit complexity add-on codes
    - Advanced primary care management codes
    """

    # Home visit and domiciliary care codes
    home_visit_codes = [
        "99341",  # Home or residence visit, new patient, 20 min
        "99342",  # Home or residence visit, new patient, 30 min
        "99344",  # Home or residence visit, new patient, 60 min
        "99345",  # Home or residence visit, new patient, 75 min
        "99347",  # Home or residence visit, established patient, 15 min
        "99348",  # Home or residence visit, established patient, 25 min
        "99349",  # Home or residence visit, established patient, 40 min
        "99350",  # Home or residence visit, established patient, 60 min
        "G2211",  # Visit complexity inherent to evaluation and management
        "G0556",  # Advanced primary care management (first 60 min)
        "G0557",  # Advanced primary care management (each add'l 30 min)
        "G0558",  # Advanced primary care management (single session)
    ]

    # Office visit codes
    office_visit_codes = [
        "99202",  # Office/outpatient visit, new patient, 15-29 min
        "99203",  # Office/outpatient visit, new patient, 30-44 min
        "99204",  # Office/outpatient visit, new patient, 45-59 min
        "99205",  # Office/outpatient visit, new patient, 60-74 min
        "99211",  # Office/outpatient visit, established patient, minimal
        "99212",  # Office/outpatient visit, established patient, 10-19 min
        "99213",  # Office/outpatient visit, established patient, 20-29 min
        "99214",  # Office/outpatient visit, established patient, 30-39 min
        "99215",  # Office/outpatient visit, established patient, 40-54 min
    ]

    # Wound care codes - all unique HCPCS from HDAI Excel file
    wound_care_codes = [
        "10060", "10061", "10140", "10160", "10180",
        "11000", "11004", "11005", "11042", "11043", "11044", "11045", "11046", "11047",
        "11055", "11056", "11102", "11104", "11105", "11106", "11720", "11721",
        "15271", "15272", "15273", "15274", "15275", "15276", "15277", "15278",
        "15734", "17250",
        "27590", "27592", "27880", "27882", "27886",
        "28124", "28160", "28800", "28805", "28810", "28820", "28825",
        "29445", "29580", "29581",
        "35566", "35666",
        "37184", "37185", "37186", "37220", "37221", "37222", "37223", "37224",
        "37225", "37226", "37227", "37228", "37229", "37230", "37231", "37232",
        "37233", "37235", "37238", "37239", "37252",
        "93970", "93971", "93985", "93986",
        "97597", "97598", "97605", "97606", "97607", "97608", "97610",
        "99183",
        "A2001", "A2002", "A2024", "A2025",
        "G0277",
        "Q4158", "Q4161", "Q4164", "Q4166", "Q4180", "Q4186", "Q4190", "Q4191",
        "Q4195", "Q4196", "Q4197", "Q4204", "Q4205", "Q4221", "Q4236", "Q4238",
        "Q4239", "Q4248", "Q4250", "Q4259", "Q4264", "Q4265", "Q4269", "Q4270",
        "Q4271", "Q4275", "Q4279", "Q4280", "Q4281", "Q4282", "Q4290", "Q4294",
        "Q4295", "Q4296", "Q4297", "Q4298", "Q4300", "Q4301", "Q4303", "Q4304",
        "Q4309", "Q4313", "Q4316", "Q4322", "Q4325", "Q4326", "Q4341", "Q4344",
        "Q4357", "Q4364",
    ]

    @staticmethod
    @expression(
        name="filter_home_visit_hcpcs",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def filter_home_visit_hcpcs() -> pl.Expr:
        """
        Filter for claims with home visit HCPCS codes.

        Includes:
        - Home/domiciliary visit codes (99341-99350)
        - Visit complexity add-on (G2211)
        - Advanced primary care management (G0556-G0558)

        Returns:
            Expression that filters for home visit HCPCS codes
        """
        return pl.col("hcpcs_code").is_in(HCPCSFilterExpression.home_visit_codes)

    @staticmethod
    @expression(
        name="filter_office_visit_hcpcs",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def filter_office_visit_hcpcs() -> pl.Expr:
        """
        Filter for claims with office visit HCPCS codes.

        Includes:
        - Office/outpatient visit codes for new patients (99202-99205)
        - Office/outpatient visit codes for established patients (99211-99215)

        Returns:
            Expression that filters for office visit HCPCS codes
        """
        return pl.col("hcpcs_code").is_in(HCPCSFilterExpression.office_visit_codes)

    # Skin substitute codes - comprehensive list
    skin_substitute_codes = [
        # Skin graft series (CPT codes)
        "15271", "15272", "15273", "15274", "15275", "15276", "15277", "15278",
        # Contractor-priced skin graft codes (C-codes)
        "C5271", "C5272", "C5273", "C5274", "C5275", "C5276", "C5277", "C5278",
        # Additional C-codes
        "C9358", "C9360", "C9363", "C9364",
        # A-codes for collagen and cellular products
        "A2001", "A2002", "A2004", "A2005", "A2006", "A2007", "A2008", "A2009",
        "A2010", "A2011", "A2012", "A2013", "A2014", "A2015", "A2016", "A2018",
        "A2019", "A2020", "A2021", "A2022", "A2023", "A2024", "A2025",
        # Q4xxx skin substitute codes
        "Q4101", "Q4102", "Q4103", "Q4104", "Q4105", "Q4106", "Q4107", "Q4108",
        "Q4110", "Q4111", "Q4112", "Q4113", "Q4114", "Q4115", "Q4116", "Q4117",
        "Q4118", "Q4121", "Q4122", "Q4123", "Q4124", "Q4125", "Q4126", "Q4127",
        "Q4128", "Q4130", "Q4132", "Q4133", "Q4134", "Q4135", "Q4136", "Q4137",
        "Q4138", "Q4139", "Q4140", "Q4141", "Q4142", "Q4143", "Q4145", "Q4146",
        "Q4147", "Q4148", "Q4149", "Q4150", "Q4151", "Q4152", "Q4153", "Q4154",
        "Q4155", "Q4156", "Q4157", "Q4158", "Q4159", "Q4160", "Q4161", "Q4162",
        "Q4163", "Q4164", "Q4165", "Q4166", "Q4167", "Q4168", "Q4169", "Q4170",
        "Q4171", "Q4173", "Q4174", "Q4175", "Q4176", "Q4177", "Q4178", "Q4179",
        "Q4180", "Q4181", "Q4182", "Q4183", "Q4184", "Q4185", "Q4186", "Q4187",
        "Q4188", "Q4189", "Q4190", "Q4191", "Q4192", "Q4193", "Q4194", "Q4195",
        "Q4196", "Q4197", "Q4198", "Q4199", "Q4200", "Q4201", "Q4202", "Q4203",
        "Q4204", "Q4205", "Q4206", "Q4208", "Q4209", "Q4211", "Q4212", "Q4213",
        "Q4214", "Q4215", "Q4216", "Q4217", "Q4218", "Q4219", "Q4220", "Q4221",
        "Q4222", "Q4225", "Q4226", "Q4227", "Q4229", "Q4230", "Q4231", "Q4232",
        "Q4233", "Q4234", "Q4235", "Q4236", "Q4237", "Q4238", "Q4239", "Q4240",
        "Q4241", "Q4242", "Q4245", "Q4246", "Q4247", "Q4248", "Q4249", "Q4250",
        "Q4251", "Q4252", "Q4253", "Q4254", "Q4255", "Q4256", "Q4257", "Q4258",
        "Q4259", "Q4260", "Q4261", "Q4262", "Q4263", "Q4264", "Q4265", "Q4266",
        "Q4267", "Q4268", "Q4269", "Q4270", "Q4271", "Q4272", "Q4273", "Q4274",
        "Q4275", "Q4276", "Q4278", "Q4279", "Q4280", "Q4281", "Q4282", "Q4283",
        "Q4284", "Q4285", "Q4286", "Q4287", "Q4288", "Q4289", "Q4290", "Q4291",
        "Q4292", "Q4293", "Q4294", "Q4295", "Q4296", "Q4297", "Q4298", "Q4299",
        "Q4300", "Q4301", "Q4302", "Q4303", "Q4304", "Q4305", "Q4306", "Q4307",
        "Q4308", "Q4309", "Q4310",
    ]

    @staticmethod
    @expression(
        name="wound_care",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def wound_care() -> pl.Expr:
        """
        Filter for wound care HCPCS codes.

        All unique HCPCS codes from HDAI Excel file A2671_D0259_WoundClaim_Ids_20251117.xlsx

        Includes:
        - Wound debridement (11042-11047, 97597-97598)
        - Vascular procedures (37xxx series)
        - Negative pressure wound therapy (97605-97608)
        - Hyperbaric oxygen therapy (99183, G0277)
        - Skin substitutes and grafts (15271-15278, Q4xxx)
        - Foot/lower extremity procedures

        Returns:
            Expression that filters for wound care HCPCS codes
        """
        return pl.col("hcpcs_code").is_in(HCPCSFilterExpression.wound_care_codes)

    @staticmethod
    @expression(
        name="skin_substitutes",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def skin_substitutes() -> pl.Expr:
        """
        Filter for skin substitute HCPCS codes.

        Comprehensive list including:
        - Skin graft series (15271-15278)
        - Contractor-priced codes (C5271-C5278, C9358, C9360, C9363, C9364)
        - Collagen and cellular products (A2xxx series)
        - Q4xxx cellular and tissue-based products (Q4101-Q4310)

        Returns:
            Expression that filters for skin substitute HCPCS codes
        """
        return pl.col("hcpcs_code").is_in(HCPCSFilterExpression.skin_substitute_codes)
