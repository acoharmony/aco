from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import TYPE_CHECKING

import pytest

# © 2025 HarmonyCares
"""Tests for acoharmony/_exceptions/_validation.py."""



class TestValidation:
    """Test suite for _validation."""

    @pytest.mark.unit
    def test_validationerror_init(self) -> None:
        """Test ValidationError initialization."""
        exc = ValidationError("validation failed", auto_log=False, auto_trace=False)
        assert exc.message == "validation failed"
        assert exc.error_code == "VALIDATION_001"
        assert exc.category == "validation"
        assert isinstance(exc, ACOHarmonyException)

    @pytest.mark.unit
    def test_missingcolumnserror_init(self) -> None:
        """Test MissingColumnsError initialization."""
        exc = MissingColumnsError("cols missing", auto_log=False, auto_trace=False)
        assert exc.message == "cols missing"
        assert exc.error_code == "VALIDATION_002"
        assert isinstance(exc, ValidationError)

    @pytest.mark.unit
    def test_typevalidationerror_init(self) -> None:
        """Test TypeValidationError initialization."""
        exc = TypeValidationError("wrong type", auto_log=False, auto_trace=False)
        assert exc.message == "wrong type"
        assert exc.error_code == "VALIDATION_003"
        assert isinstance(exc, ValidationError)



if TYPE_CHECKING:
    pass


class TestValidationError:
    """Tests for ValidationError."""


    @pytest.mark.unit
    def test_validationerror_initialization(self) -> None:
        """ValidationError can be initialized."""
        exc = ValidationError("err", auto_log=False, auto_trace=False)
        assert exc.message == "err"
        assert isinstance(exc, ACOHarmonyException)

    @pytest.mark.unit
    def test_validationerror_basic_functionality(self) -> None:
        """ValidationError basic functionality works."""
        with pytest.raises(ValidationError):
            raise ValidationError("fail", auto_log=False, auto_trace=False)
        exc = ValidationError("v", auto_log=False, auto_trace=False)
        assert "VALIDATION_001" in repr(exc)

class TestMissingColumnsError:
    """Tests for MissingColumnsError."""


    @pytest.mark.unit
    def test_missingcolumnserror_initialization(self) -> None:
        """MissingColumnsError can be initialized."""
        exc = MissingColumnsError("missing", auto_log=False, auto_trace=False)
        assert exc.message == "missing"
        assert exc.error_code == "VALIDATION_002"

    @pytest.mark.unit
    def test_missingcolumnserror_basic_functionality(self) -> None:
        """MissingColumnsError basic functionality works."""
        exc = MissingColumnsError("m", auto_log=False, auto_trace=False)
        assert isinstance(exc, ValidationError)
        assert isinstance(exc, ACOHarmonyException)

class TestTypeValidationError:
    """Tests for TypeValidationError."""


    @pytest.mark.unit
    def test_typevalidationerror_initialization(self) -> None:
        """TypeValidationError can be initialized."""
        exc = TypeValidationError("type err", auto_log=False, auto_trace=False)
        assert exc.message == "type err"
        assert exc.error_code == "VALIDATION_003"

    @pytest.mark.unit
    def test_typevalidationerror_basic_functionality(self) -> None:
        """TypeValidationError basic functionality works."""
        exc = TypeValidationError("t", auto_log=False, auto_trace=False)
        assert isinstance(exc, ValidationError)

class TestEmptyDataError:
    """Tests for EmptyDataError."""


    @pytest.mark.unit
    def test_emptydataerror_initialization(self) -> None:
        """EmptyDataError can be initialized."""
        exc = EmptyDataError("empty", auto_log=False, auto_trace=False)
        assert exc.message == "empty"
        assert exc.error_code == "VALIDATION_004"

    @pytest.mark.unit
    def test_emptydataerror_basic_functionality(self) -> None:
        """EmptyDataError basic functionality works."""
        exc = EmptyDataError("e", auto_log=False, auto_trace=False)
        assert isinstance(exc, ValidationError)

class TestPathValidationError:
    """Tests for PathValidationError."""


    @pytest.mark.unit
    def test_pathvalidationerror_initialization(self) -> None:
        """PathValidationError can be initialized."""
        exc = PathValidationError("bad path", auto_log=False, auto_trace=False)
        assert exc.message == "bad path"
        assert exc.error_code == "VALIDATION_005"

    @pytest.mark.unit
    def test_pathvalidationerror_basic_functionality(self) -> None:
        """PathValidationError basic functionality works."""
        exc = PathValidationError("p", auto_log=False, auto_trace=False)
        assert isinstance(exc, ValidationError)

class TestFileFormatValidationError:
    """Tests for FileFormatValidationError."""


    @pytest.mark.unit
    def test_fileformatvalidationerror_initialization(self) -> None:
        """FileFormatValidationError can be initialized."""
        exc = FileFormatValidationError("bad fmt", auto_log=False, auto_trace=False)
        assert exc.message == "bad fmt"
        assert exc.error_code == "VALIDATION_006"

    @pytest.mark.unit
    def test_fileformatvalidationerror_basic_functionality(self) -> None:
        """FileFormatValidationError basic functionality works."""
        exc = FileFormatValidationError("f", auto_log=False, auto_trace=False)
        assert isinstance(exc, ValidationError)
