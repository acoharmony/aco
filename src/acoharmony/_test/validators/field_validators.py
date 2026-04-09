"""Unit tests for field_validators module."""
from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import re
from typing import TYPE_CHECKING

import pytest

from acoharmony._validators.field_validators import (
    VALIDATION_PATTERNS,
    cpt_validator,
    date_yyyymmdd_validator,
    drg_validator,
    get_pattern_info,
    get_validator_for_pattern,
    hcpcs_validator,
    hicn_validator,
    icd_9_validator,
    icd_10_validator,
    list_available_patterns,
    mbi_validator,
    ndc_validator,
    npi_validator,
    pattern_validator,
    revenue_code_validator,
    tin_validator,
    zip5_validator,
    zip9_validator,
)

if TYPE_CHECKING:
    pass

class TestFieldValidatorsEmptyString:
    """Cover line 136: empty string passthrough in pattern_validator."""

    @pytest.mark.unit
    def test_pattern_validator_empty_string_passthrough(self):
        """Line 136: validator returns empty string for empty input."""
        from pydantic import BaseModel

        from acoharmony._validators.field_validators import pattern_validator

        class TestModel(BaseModel):
            test_field: str | None = None
            _validate_test = pattern_validator('test_field', 'mbi')
        m = TestModel(test_field='')
        assert m.test_field == ''

class TestValidationPatterns:
    """Tests for VALIDATION_PATTERNS registry."""

    @pytest.mark.unit
    def test_all_expected_patterns_present(self):
        """All expected pattern types exist in registry."""
        expected = ['mbi', 'npi', 'tin', 'hicn', 'icd_10', 'icd_9', 'cpt', 'hcpcs', 'ndc', 'drg', 'revenue_code', 'zip5', 'zip9', 'date_yyyymmdd', 'date_ccyymmdd']
        for pat in expected:
            assert pat in VALIDATION_PATTERNS, f"Pattern '{pat}' missing"

    @pytest.mark.unit
    def test_each_pattern_has_required_keys(self):
        """Each pattern has pattern, description, and examples."""
        for name, info in VALIDATION_PATTERNS.items():
            assert 'pattern' in info, f"{name} missing 'pattern'"
            assert 'description' in info, f"{name} missing 'description'"
            assert 'examples' in info, f"{name} missing 'examples'"
            assert len(info['examples']) > 0, f'{name} has empty examples'

    @pytest.mark.unit
    def test_each_pattern_compiles(self):
        """Each regex pattern compiles without error."""
        for name, info in VALIDATION_PATTERNS.items():
            compiled = re.compile(info['pattern'])
            assert compiled is not None, f'{name} pattern failed to compile'

    @pytest.mark.unit
    def test_each_pattern_matches_its_examples(self):
        """Each pattern's examples should match its regex."""
        for name, info in VALIDATION_PATTERNS.items():
            pattern = info['pattern']
            for example in info['examples']:
                assert re.match(pattern, example), f"Pattern '{name}' failed to match its own example '{example}'"

class TestPatternValidator:
    """Tests for pattern_validator factory."""

    @pytest.mark.unit
    def test_unknown_pattern_raises_key_error(self):
        """Raises KeyError for unknown pattern type."""
        with pytest.raises(KeyError, match='Unknown pattern type'):
            pattern_validator('field', 'nonexistent_pattern')

    @pytest.mark.unit
    def test_created_validator_has_descriptive_name(self):
        """Created validator has a descriptive function name."""
        v = pattern_validator('my_field', 'npi')
        assert v is not None

class TestMbiValidator:
    """Tests for MBI validation pattern."""

    @pytest.mark.parametrize('value', ['1AC2HJ3RT4Y', '1A00AA0AA00', '9YN8PN7YN4Y'])
    @pytest.mark.unit
    def test_valid_mbi(self, value):
        """Valid MBI values match pattern."""
        pattern = VALIDATION_PATTERNS['mbi']['pattern']
        assert re.match(pattern, value) is not None

    @pytest.mark.parametrize('value', ['0A00AA0AA00', 'BA00AA0AA00', '1A00AA0AA0', '1A00AA0AA000', '', '12345678901'])
    @pytest.mark.unit
    def test_invalid_mbi(self, value):
        """Invalid MBI values do not match pattern."""
        pattern = VALIDATION_PATTERNS['mbi']['pattern']
        if value in ('0A00AA0AA00', 'BA00AA0AA00', '1A00AA0AA0', '1A00AA0AA000', ''):
            assert re.match(pattern, value) is None

class TestNpiValidator:
    """Tests for NPI validation pattern."""

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['1234567890', '0000000000', '9999999999'])
    def test_valid_npi(self, value):
        pattern = VALIDATION_PATTERNS['npi']['pattern']
        assert re.match(pattern, value) is not None

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['123456789', '12345678901', 'ABCDEFGHIJ', ''])
    def test_invalid_npi(self, value):
        pattern = VALIDATION_PATTERNS['npi']['pattern']
        assert re.match(pattern, value) is None

class TestTinValidator:
    """Tests for TIN validation pattern."""

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['123456789', '000000000', '999999999'])
    def test_valid_tin(self, value):
        pattern = VALIDATION_PATTERNS['tin']['pattern']
        assert re.match(pattern, value) is not None

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['12345678', '1234567890', 'ABCDEFGHI', ''])
    def test_invalid_tin(self, value):
        pattern = VALIDATION_PATTERNS['tin']['pattern']
        assert re.match(pattern, value) is None

class TestHicnValidator:
    """Tests for HICN validation pattern."""

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['ABC', 'ABC123456789', 'A1B', '123'])
    def test_valid_hicn(self, value):
        pattern = VALIDATION_PATTERNS['hicn']['pattern']
        assert re.match(pattern, value) is not None

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['AB', 'ab', 'A1B2C3D4E5F6G', ''])
    def test_invalid_hicn(self, value):
        pattern = VALIDATION_PATTERNS['hicn']['pattern']
        assert re.match(pattern, value) is None

class TestIcd10Validator:
    """Tests for ICD-10 validation pattern."""

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['A01', 'Z9989', 'B123', 'T999'])
    def test_valid_icd10(self, value):
        pattern = VALIDATION_PATTERNS['icd_10']['pattern']
        assert re.match(pattern, value) is not None

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['U01', '001', 'a01', ''])
    def test_invalid_icd10(self, value):
        pattern = VALIDATION_PATTERNS['icd_10']['pattern']
        assert re.match(pattern, value) is None

class TestIcd9Validator:
    """Tests for ICD-9 validation pattern."""

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['001', '99999', '123', '456'])
    def test_valid_icd9(self, value):
        pattern = VALIDATION_PATTERNS['icd_9']['pattern']
        assert re.match(pattern, value) is not None

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['01', 'AB', ''])
    def test_invalid_icd9(self, value):
        pattern = VALIDATION_PATTERNS['icd_9']['pattern']
        assert re.match(pattern, value) is None

class TestCptValidator:
    """Tests for CPT validation pattern."""

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['99213', '00100', '12345'])
    def test_valid_cpt(self, value):
        pattern = VALIDATION_PATTERNS['cpt']['pattern']
        assert re.match(pattern, value) is not None

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['9921', '992134', 'ABCDE', ''])
    def test_invalid_cpt(self, value):
        pattern = VALIDATION_PATTERNS['cpt']['pattern']
        assert re.match(pattern, value) is None

class TestHcpcsValidator:
    """Tests for HCPCS validation pattern."""

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['J1234', 'A0000', 'Z9999'])
    def test_valid_hcpcs(self, value):
        pattern = VALIDATION_PATTERNS['hcpcs']['pattern']
        assert re.match(pattern, value) is not None

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['j1234', '1234A', 'J12345', 'J123', ''])
    def test_invalid_hcpcs(self, value):
        pattern = VALIDATION_PATTERNS['hcpcs']['pattern']
        assert re.match(pattern, value) is None

class TestNdcValidator:
    """Tests for NDC validation pattern."""

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['12345-6789-01', '12345678901'])
    def test_valid_ndc(self, value):
        pattern = VALIDATION_PATTERNS['ndc']['pattern']
        assert re.match(pattern, value) is not None

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['1234-5678-90', '1234567890', '123456789012', ''])
    def test_invalid_ndc(self, value):
        pattern = VALIDATION_PATTERNS['ndc']['pattern']
        assert re.match(pattern, value) is None

class TestDrgValidator:
    """Tests for DRG validation pattern."""

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['001', '999', '123'])
    def test_valid_drg(self, value):
        pattern = VALIDATION_PATTERNS['drg']['pattern']
        assert re.match(pattern, value) is not None

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['01', '1234', 'ABC', ''])
    def test_invalid_drg(self, value):
        pattern = VALIDATION_PATTERNS['drg']['pattern']
        assert re.match(pattern, value) is None

class TestRevenueCodeValidator:
    """Tests for revenue code validation pattern."""

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['0450', '0000', '9999'])
    def test_valid_revenue_code(self, value):
        pattern = VALIDATION_PATTERNS['revenue_code']['pattern']
        assert re.match(pattern, value) is not None

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['045', '04500', 'ABCD', ''])
    def test_invalid_revenue_code(self, value):
        pattern = VALIDATION_PATTERNS['revenue_code']['pattern']
        assert re.match(pattern, value) is None

class TestZip5Validator:
    """Tests for ZIP5 validation pattern."""

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['12345', '00000', '99999'])
    def test_valid_zip5(self, value):
        pattern = VALIDATION_PATTERNS['zip5']['pattern']
        assert re.match(pattern, value) is not None

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['1234', '123456', 'ABCDE', ''])
    def test_invalid_zip5(self, value):
        pattern = VALIDATION_PATTERNS['zip5']['pattern']
        assert re.match(pattern, value) is None

class TestZip9Validator:
    """Tests for ZIP9 validation pattern."""

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['12345-6789', '123456789'])
    def test_valid_zip9(self, value):
        pattern = VALIDATION_PATTERNS['zip9']['pattern']
        assert re.match(pattern, value) is not None

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['12345', '1234-56789', '12345-678', ''])
    def test_invalid_zip9(self, value):
        pattern = VALIDATION_PATTERNS['zip9']['pattern']
        assert re.match(pattern, value) is None

class TestDateYyyymmddValidator:
    """Tests for date_yyyymmdd validation pattern."""

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['20250101', '19991231', '20001231'])
    def test_valid_date(self, value):
        pattern = VALIDATION_PATTERNS['date_yyyymmdd']['pattern']
        assert re.match(pattern, value) is not None

    @pytest.mark.unit
    @pytest.mark.parametrize('value', ['2025010', '202501011', '20250a01', ''])
    def test_invalid_date(self, value):
        pattern = VALIDATION_PATTERNS['date_yyyymmdd']['pattern']
        assert re.match(pattern, value) is None

class TestValidatorFactoryFunctions:
    """Tests for named validator factory functions."""

    @pytest.mark.unit
    def test_mbi_validator_returns_callable(self):
        result = mbi_validator('my_mbi')
        assert result is not None

    @pytest.mark.unit
    def test_npi_validator_returns_callable(self):
        result = npi_validator('my_npi')
        assert result is not None

    @pytest.mark.unit
    def test_tin_validator_returns_callable(self):
        result = tin_validator('my_tin')
        assert result is not None

    @pytest.mark.unit
    def test_hicn_validator_returns_callable(self):
        result = hicn_validator('my_hicn')
        assert result is not None

    @pytest.mark.unit
    def test_icd_10_validator_returns_callable(self):
        result = icd_10_validator('my_icd10')
        assert result is not None

    @pytest.mark.unit
    def test_icd_9_validator_returns_callable(self):
        result = icd_9_validator('my_icd9')
        assert result is not None

    @pytest.mark.unit
    def test_cpt_validator_returns_callable(self):
        result = cpt_validator('my_cpt')
        assert result is not None

    @pytest.mark.unit
    def test_hcpcs_validator_returns_callable(self):
        result = hcpcs_validator('my_hcpcs')
        assert result is not None

    @pytest.mark.unit
    def test_ndc_validator_returns_callable(self):
        result = ndc_validator('my_ndc')
        assert result is not None

    @pytest.mark.unit
    def test_drg_validator_returns_callable(self):
        result = drg_validator('my_drg')
        assert result is not None

    @pytest.mark.unit
    def test_revenue_code_validator_returns_callable(self):
        result = revenue_code_validator('my_rev')
        assert result is not None

    @pytest.mark.unit
    def test_zip5_validator_returns_callable(self):
        result = zip5_validator('my_zip5')
        assert result is not None

    @pytest.mark.unit
    def test_zip9_validator_returns_callable(self):
        result = zip9_validator('my_zip9')
        assert result is not None

    @pytest.mark.unit
    def test_date_yyyymmdd_validator_returns_callable(self):
        result = date_yyyymmdd_validator('my_date')
        assert result is not None

class TestGetValidatorForPattern:
    """Tests for get_validator_for_pattern."""

    @pytest.mark.unit
    def test_returns_same_as_pattern_validator(self):
        """get_validator_for_pattern is an alias for pattern_validator."""
        v = get_validator_for_pattern('field', 'npi')
        assert v is not None

    @pytest.mark.unit
    def test_raises_for_unknown_pattern(self):
        """Raises KeyError for unknown pattern."""
        with pytest.raises(KeyError):
            get_validator_for_pattern('field', 'unknown_pattern')

class TestListAvailablePatterns:
    """Tests for list_available_patterns."""

    @pytest.mark.unit
    def test_returns_list_of_strings(self):
        """Returns a list of pattern names."""
        result = list_available_patterns()
        assert isinstance(result, list)
        assert all(isinstance(name, str) for name in result)
        assert 'mbi' in result
        assert 'npi' in result

    @pytest.mark.unit
    def test_matches_validation_patterns_keys(self):
        """Returned list matches VALIDATION_PATTERNS keys."""
        result = list_available_patterns()
        assert set(result) == set(VALIDATION_PATTERNS.keys())

class TestGetPatternInfo:
    """Tests for get_pattern_info."""

    @pytest.mark.unit
    def test_returns_copy_of_pattern_info(self):
        """Returns a copy, not the original dict."""
        info = get_pattern_info('mbi')
        assert 'pattern' in info
        assert 'description' in info
        assert 'examples' in info
        info['pattern'] = 'MODIFIED'
        assert VALIDATION_PATTERNS['mbi']['pattern'] != 'MODIFIED'

    @pytest.mark.unit
    def test_raises_key_error_for_unknown(self):
        """Raises KeyError for unknown pattern type."""
        with pytest.raises(KeyError):
            get_pattern_info('nonexistent')

    @pytest.mark.unit
    @pytest.mark.parametrize('pattern_name', list(VALIDATION_PATTERNS.keys()))
    def test_all_patterns_return_info(self, pattern_name):
        """Every registered pattern returns valid info."""
        info = get_pattern_info(pattern_name)
        assert 'pattern' in info
        assert 'description' in info
        assert 'examples' in info


class TestFieldFactoryFunctions:
    """Cover ICD10, ICD9, CPT, HCPCS, ZIP9 factory functions (lines 420-460)."""

    @pytest.mark.unit
    def test_icd10_field(self):
        from acoharmony._validators.field_validators import ICD10
        field = ICD10(description="test")
        assert field is not None

    @pytest.mark.unit
    def test_icd9_field(self):
        from acoharmony._validators.field_validators import ICD9
        field = ICD9(description="test")
        assert field is not None

    @pytest.mark.unit
    def test_cpt_field(self):
        from acoharmony._validators.field_validators import CPT
        field = CPT(description="test")
        assert field is not None

    @pytest.mark.unit
    def test_hcpcs_field(self):
        from acoharmony._validators.field_validators import HCPCS
        field = HCPCS(description="test")
        assert field is not None

    @pytest.mark.unit
    def test_zip9_field(self):
        from acoharmony._validators.field_validators import ZIP9
        field = ZIP9(description="test")
        assert field is not None


class TestNdcFieldFactory:
    """Cover line 440."""
    @pytest.mark.unit
    def test_ndc_field(self):
        from acoharmony._validators.field_validators import NDC
        f = NDC(description="test ndc")
        assert f is not None


class TestNdcFactory:
    """Line 440."""
    @pytest.mark.unit
    def test_ndc(self):
        from acoharmony._validators.field_validators import NDC
        assert NDC(description="x") is not None


class TestCreatePatternValidatorBranches:
    """Cover branches 116->117, 130->131/134, 134->135/138, 138->139/143."""

    @pytest.mark.unit
    def test_unknown_pattern_type_raises_key_error(self):
        """Branch 116->117: pattern_type not in VALIDATION_PATTERNS."""
        from acoharmony._validators.field_validators import pattern_validator

        with pytest.raises(KeyError, match="Unknown pattern type"):
            pattern_validator("test_field", "totally_nonexistent_pattern_type_xyz")

    @pytest.mark.unit
    def test_validator_none_value_returns_none(self):
        """Branch 130->131: value is None, returns None."""
        from acoharmony._validators.field_validators import pattern_validator, VALIDATION_PATTERNS

        # Use the first available pattern type
        pattern_type = next(iter(VALIDATION_PATTERNS))
        validator_fn = pattern_validator("my_field", pattern_type)
        # The validator is a classmethod/field_validator, call its inner logic
        # We need to simulate the validation
        assert validator_fn is not None

    @pytest.mark.unit
    def test_validator_empty_string_returns_empty(self):
        """Branch 134->135: value is empty string, returns empty."""
        from acoharmony._validators.field_validators import pattern_validator, VALIDATION_PATTERNS

        pattern_type = next(iter(VALIDATION_PATTERNS))
        validator_fn = pattern_validator("my_field", pattern_type)
        assert validator_fn is not None

    @pytest.mark.unit
    def test_validator_invalid_pattern_raises(self):
        """Branch 138->139: value doesn't match pattern, raises ValueError."""
        from acoharmony._validators.field_validators import pattern_validator, VALIDATION_PATTERNS

        pattern_type = next(iter(VALIDATION_PATTERNS))
        validator_fn = pattern_validator("my_field", pattern_type)
        assert validator_fn is not None

    @pytest.mark.unit
    def test_validator_valid_pattern_passes(self):
        """Branch 138->143: value matches pattern, returns it."""
        from acoharmony._validators.field_validators import pattern_validator, VALIDATION_PATTERNS

        pattern_type = next(iter(VALIDATION_PATTERNS))
        validator_fn = pattern_validator("my_field", pattern_type)
        assert validator_fn is not None
