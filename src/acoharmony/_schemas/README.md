# _schemas

Schema definitions for healthcare data.

## Purpose

Define schemas for CCLF files, quality measures, and analytics tables. Validation. Type safety.

## Structure

YAML schema files defining:
- Column names and types
- Required fields
- Validation rules
- Transformations
- Business logic

## Schema Categories

### CCLF Schemas
- `cclf1.yml` - Part A Header claims
- `cclf5.yml` - Part B Physician claims
- `cclf6.yml` - Part B DME claims
- `cclf7.yml` - Part D Pharmacy claims
- `cclf8.yml` - Beneficiary Demographics
- `cclf9.yml` - Beneficiary Crosswalk (MBI/HICN)

### Intermediate Schemas
- `int_physician_claim_deduped.yml` - Deduped physician claims
- `int_institutional_claim_deduped.yml` - Deduped institutional
- `int_diagnosis_deduped.yml` - Deduped diagnoses
- `int_procedure_deduped.yml` - Deduped procedures

### Gold Schemas
- `medical_claim.yml` - Medical claim output
- `pharmacy_claim.yml` - Pharmacy claim output
- `eligibility.yml` - Member eligibility

## Usage

```python
from acoharmony._schemas import get_schema

schema = get_schema("cclf1")
# Returns schema definition with columns, types, validations
```

## Schema Structure

```yaml
name: cclf1
tier: bronze
columns:
  - name: cur_clm_uniq_id
    type: String
    required: true
  - name: clm_line_num
    type: String
    required: false
```

## Key Features

- **Type safety** - Define column types
- **Validation rules** - Required fields, constraints
- **Transform config** - Dedup, ADR, pivot logic
- **Documentation** - Schema serves as documentation
- **Versioning** - Track schema changes

## Integration

- `_validators/` - Schema validation
- `_transforms/` - Use schemas for transforms
- `_parsers/` - Parse files using schemas
