# © 2025 HarmonyCares
# All rights reserved.

"""
Model-aware parser utilities for automatic type coercion.

This module bridges the gap between raw file data (strings) and
Pydantic models (typed fields) by introspecting the model schema
and applying appropriate type conversions during parsing.
"""

import re
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any, get_args, get_origin

from pydantic.fields import FieldInfo


class ModelAwareCoercer:
    """
    Coerce raw parsed data to match Pydantic model field types.

        This class introspects a Pydantic model's field definitions and
        applies appropriate type conversions to raw string data from parsers.

        The goal is to prevent schema mismatches - parsers output what the
        model expects, no validation errors from type mismatches.
    """

    # Date format patterns for CCLF files (YYYYMMDD, CCYYMMDD)
    DATE_PATTERNS = [
        (r"^\d{8}$", "%Y%m%d"),  # YYYYMMDD or CCYYMMDD (same format)
        (r"^\d{4}-\d{2}-\d{2}$", "%Y-%m-%d"),  # YYYY-MM-DD
        (r"^\d{2}/\d{2}/\d{4}$", "%m/%d/%Y"),  # MM/DD/YYYY
    ]

    # Datetime patterns
    DATETIME_PATTERNS = [
        (r"^\d{14}$", "%Y%m%d%H%M%S"),  # YYYYMMDDhhmmss
        (r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", "%Y-%m-%d %H:%M:%S"),
    ]

    # Time patterns
    TIME_PATTERNS = [
        (r"^\d{6}$", "%H%M%S"),  # hhmmss
        (r"^\d{2}:\d{2}:\d{2}$", "%H:%M:%S"),  # hh:mm:ss
    ]

    @classmethod
    def get_field_info(cls, model_class: type) -> dict[str, FieldInfo]:
        """
        Extract field information from a Pydantic model.

                Args:
                    model_class: Pydantic dataclass model

                Returns:
                    Dictionary mapping field_name → FieldInfo
        """
        if hasattr(model_class, "__pydantic_fields__"):
            return model_class.__pydantic_fields__
        elif hasattr(model_class, "__dataclass_fields__"):
            # For pydantic dataclasses
            return {
                name: field.metadata.get("pydantic_field")
                for name, field in model_class.__dataclass_fields__.items()
                if "pydantic_field" in field.metadata
            }
        else:
            return {}

    @classmethod
    def coerce_value(cls, value: Any, field_name: str, field_info: FieldInfo) -> Any:
        """
        Coerce a raw value to match the field's expected type.

                Args:
                    value: Raw value (usually string from file)
                    field_name: Field name
                    field_info: Pydantic FieldInfo

                Returns:
                    Coerced value matching field type

                Raises:
                    ValueError: If coercion fails
        """
        # Handle None/empty strings
        if value is None or value == "":
            # Check if field is optional
            if cls._is_optional(field_info):
                return None
            # Required field with empty value - let Pydantic validate
            return value

        # Get the target type
        field_type = field_info.annotation

        # Handle Optional types
        if get_origin(field_type) is type(None) or get_origin(field_type) is type(None).__class__:
            # This is Optional[X], get the actual type
            args = get_args(field_type)
            if args:
                field_type = args[0]

        # String type - no coercion needed
        if field_type is str:
            return str(value)

        # Integer type
        if field_type is int:
            return cls._coerce_to_int(value, field_name)

        # Float type
        if field_type is float:
            return cls._coerce_to_float(value, field_name)

        # Decimal type
        if field_type is Decimal:
            return cls._coerce_to_decimal(value, field_name)

        # Date type
        if field_type is date:
            return cls._coerce_to_date(value, field_name)

        # Datetime type
        if field_type is datetime:
            return cls._coerce_to_datetime(value, field_name)

        # Time type
        if field_type is time:
            return cls._coerce_to_time(value, field_name)

        # Boolean type
        if field_type is bool:
            return cls._coerce_to_bool(value, field_name)

        # If we can't determine type, return as-is and let Pydantic validate
        return value

    @classmethod
    def _is_optional(cls, field_info: FieldInfo) -> bool:
        """Check if a field is Optional (allows None)."""
        # Check if annotation is Union with None
        origin = get_origin(field_info.annotation)
        if origin is type(None):
            return True

        args = get_args(field_info.annotation)
        return type(None) in args

    @classmethod
    def _coerce_to_int(cls, value: Any, field_name: str) -> int:
        """Coerce value to integer."""
        if isinstance(value, int):
            return value

        if isinstance(value, str):
            # Remove whitespace
            value = value.strip()
            if not value:
                raise ValueError(f"Empty string cannot be converted to int for {field_name}")

            # Remove commas (1,000 → 1000)
            value = value.replace(",", "")

            # Handle negative numbers
            try:
                return int(value)
            except ValueError as e:
                raise ValueError(f"Cannot convert '{value}' to int for field {field_name}") from e

        # Try direct conversion
        try:
            return int(value)
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Cannot convert {type(value).__name__} to int for field {field_name}"
            ) from e

    @classmethod
    def _coerce_to_float(cls, value: Any, field_name: str) -> float:
        """Coerce value to float."""
        if isinstance(value, float):
            return value

        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError(f"Empty string cannot be converted to float for {field_name}")

            # Remove commas
            value = value.replace(",", "")

            try:
                return float(value)
            except ValueError as e:
                raise ValueError(f"Cannot convert '{value}' to float for field {field_name}") from e

        try:
            return float(value)
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Cannot convert {type(value).__name__} to float for field {field_name}"
            ) from e

    @classmethod
    def _coerce_to_decimal(cls, value: Any, field_name: str) -> Decimal:
        """Coerce value to Decimal."""
        if isinstance(value, Decimal):
            return value

        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError(f"Empty string cannot be converted to Decimal for {field_name}")

            # Remove commas
            value = value.replace(",", "")

            try:
                return Decimal(value)
            except InvalidOperation as e:
                raise ValueError(
                    f"Cannot convert '{value}' to Decimal for field {field_name}"
                ) from e

        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError) as e:
            raise ValueError(
                f"Cannot convert {type(value).__name__} to Decimal for field {field_name}"
            ) from e

    @classmethod
    def _coerce_to_date(cls, value: Any, field_name: str) -> date:
        """Coerce value to date."""
        if isinstance(value, date):
            return value

        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError(f"Empty string cannot be converted to date for {field_name}")

            # Try each date pattern
            for pattern_regex, date_format in cls.DATE_PATTERNS:
                if re.match(pattern_regex, value):
                    try:
                        return datetime.strptime(value, date_format).date()
                    except ValueError:
                        continue

            # If no pattern matched, raise error
            raise ValueError(
                f"Cannot parse date '{value}' for field {field_name}. "
                f"Expected formats: YYYYMMDD, YYYY-MM-DD, MM/DD/YYYY"
            )

        raise ValueError(f"Cannot convert {type(value).__name__} to date for field {field_name}")

    @classmethod
    def _coerce_to_datetime(cls, value: Any, field_name: str) -> datetime:
        """Coerce value to datetime."""
        if isinstance(value, datetime):
            return value

        if isinstance(value, date):
            return datetime.combine(value, time.min)

        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError(f"Empty string cannot be converted to datetime for {field_name}")

            # Try each datetime pattern
            for pattern_regex, datetime_format in cls.DATETIME_PATTERNS:
                if re.match(pattern_regex, value):
                    try:
                        return datetime.strptime(value, datetime_format)
                    except ValueError:
                        continue

            # Also try date patterns (convert to datetime at midnight)
            for pattern_regex, date_format in cls.DATE_PATTERNS:
                if re.match(pattern_regex, value):
                    try:
                        return datetime.strptime(value, date_format)
                    except ValueError:
                        continue

            raise ValueError(
                f"Cannot parse datetime '{value}' for field {field_name}. "
                f"Expected formats: YYYYMMDDhhmmss, YYYY-MM-DD HH:MM:SS"
            )

        raise ValueError(
            f"Cannot convert {type(value).__name__} to datetime for field {field_name}"
        )

    @classmethod
    def _coerce_to_time(cls, value: Any, field_name: str) -> time:
        """Coerce value to time."""
        if isinstance(value, time):
            return value

        if isinstance(value, datetime):
            return value.time()

        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError(f"Empty string cannot be converted to time for {field_name}")

            # Try each time pattern
            for pattern_regex, time_format in cls.TIME_PATTERNS:
                if re.match(pattern_regex, value):
                    try:
                        return datetime.strptime(value, time_format).time()
                    except ValueError:
                        continue

            raise ValueError(
                f"Cannot parse time '{value}' for field {field_name}. "
                f"Expected formats: hhmmss, hh:mm:ss"
            )

        raise ValueError(f"Cannot convert {type(value).__name__} to time for field {field_name}")

    @classmethod
    def _coerce_to_bool(cls, value: Any, field_name: str) -> bool:
        """Coerce value to boolean."""
        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            value = value.strip().lower()
            if value in ("true", "t", "yes", "y", "1"):
                return True
            if value in ("false", "f", "no", "n", "0", ""):
                return False
            raise ValueError(
                f"Cannot convert '{value}' to bool for field {field_name}. "
                f"Expected: true/false, yes/no, 1/0"
            )

        if isinstance(value, int):
            return bool(value)

        raise ValueError(f"Cannot convert {type(value).__name__} to bool for field {field_name}")

    @classmethod
    def coerce_row(cls, row_data: dict[str, Any], model_class: type) -> dict[str, Any]:
        """
        Coerce an entire row of data to match model field types.

                This is the main entry point for parsers. Pass raw parsed data
                (all strings) and get back typed data ready for Pydantic validation.

                Args:
                    row_data: Raw row data (field_name → raw_value)
                    model_class: Pydantic model class

                Returns:
                    Coerced row data with proper types

        """
        field_info = cls.get_field_info(model_class)
        coerced = {}

        for field_name, value in row_data.items():
            if field_name in field_info:
                info = field_info[field_name]
                try:
                    coerced[field_name] = cls.coerce_value(value, field_name, info)
                except ValueError:
                    # Log warning but include raw value - let Pydantic validate
                    # This allows validation errors to be more informative
                    coerced[field_name] = value
            else:
                # Field not in model, include as-is
                coerced[field_name] = value

        return coerced
