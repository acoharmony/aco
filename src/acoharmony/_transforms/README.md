# _transforms

Transform orchestration for LazyFrame pipelines.

## Purpose

Orchestrate LazyFrame operations. Combine expressions. Build pipelines. Register via decorators.

## Structure

- **96 modules** orchestrating pipelines
- **Decorator registration** via `@register_pipeline`
- **Quality measure framework** via `QualityMeasureBase`
- **Input/output tracking** for data lineage

## Core Components

### Registry (`_registry.py`)

Central registry for transform discovery.

```python
@register_pipeline(
    name="int_physician_claim_deduped",
    tier="silver",
    inputs=["bronze_physician_claims"],
    outputs=["silver_physician_deduped"]
)
def dedupe_physician_claims(df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.with_columns(CclfAdrExpression.negate_cancellations_header())
    df = df.unique(subset=["claim_id", "claim_line_num"], keep="last")
    return df
```

### Quality Measure Framework (`_quality_measure_base.py`)

Base class for standardized quality measures.

```python
@MeasureFactory.register
class DiabetesHbA1cControl(QualityMeasureBase):
    metadata = MeasureMetadata(
        measure_id="NQF_0059",
        measure_name="Diabetes HbA1c Poor Control",
        steward="NCQA",
        program="HEDIS"
    )

    def get_denominator(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return df.filter(pl.col("diabetes_dx"))

    def get_numerator(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return df.filter(pl.col("hba1c_value") > 9.0)
```

## Transform Categories

### Crosswalk & Standardization
- `_crosswalk.py` - Crosswalk operations
- `_standardization.py` - Data standardization
- `_enterprise_xwalk.py` - Enterprise crosswalk

### Quality Measures
- `_quality_cardiovascular.py` - Cardiovascular measures (22k lines)
- `_quality_diabetes.py` - Diabetes measures (21k lines)
- `_quality_medication_adherence.py` - Med adherence (29k lines)
- `_quality_preventive.py` - Preventive care (19k lines)

### Intermediate Claims
- `int_physician_claim_adr.py` - Physician ADR transform
- `int_physician_claim_deduped.py` - Physician deduplication
- `int_institutional_claim_adr.py` - Institutional ADR
- `int_institutional_claim_deduped.py` - Institutional dedup
- `int_dme_claim_adr.py` - DME ADR
- `int_dme_claim_deduped.py` - DME dedup
- `int_pharmacy_claim_adr.py` - Pharmacy ADR
- `int_pharmacy_claim_deduped.py` - Pharmacy dedup

### Intermediate Clinical
- `int_diagnosis_deduped.py` - Diagnosis deduplication
- `int_diagnosis_pivot.py` - Diagnosis pivot (long to wide)
- `int_procedure_deduped.py` - Procedure deduplication
- `int_procedure_pivot.py` - Procedure pivot
- `int_revenue_center_deduped.py` - Revenue center dedup

### Intermediate Demographics
- `int_beneficiary_demographics_deduped.py` - Demographics dedup
- `int_beneficiary_xref_deduped.py` - Crosswalk dedup
- `int_enrollment.py` - Enrollment processing

### Analytics
- `admissions_analysis.py` - Admissions analytics
- `readmissions_enhanced.py` - Readmission analytics
- `utilization.py` - Utilization analytics
- `financial_total_cost.py` - Financial analytics
- `hcc_gap_analysis.py` - HCC gap analysis
- `behavioral_health.py` - Behavioral health analytics

### ACO Alignment
- `_aco_alignment_temporal.py` - Temporal alignment tracking
- `_aco_alignment_provider.py` - Provider alignment
- `_aco_alignment_office.py` - Office alignment
- `_aco_alignment_demographics.py` - Demographics alignment
- `_aco_alignment_voluntary.py` - Voluntary alignment

### Citation Processing
- `_cite.py` - Citation processing (27k lines)
- `_cite_batch.py` - Citation batch processing

### Reference Tables
- `_reference.py` - Reference data transforms (23k lines)
- `_pfs_rates.py` - PFS rates (18k lines)

### Specialized Claims
- `_skin_substitute_claims.py` - Skin substitute claims
- `_wound_care_claims.py` - Wound care claims
- `home_visit_claims.py` - Home visit claims

### Member Analytics
- `beneficiary_metrics.py` - Beneficiary metrics
- `member_medical_claims_with_match.py` - Claims with matching

## Usage

```python
from acoharmony._transforms import TransformRegistry

# Get registered transform
transform = TransformRegistry.get("int_physician_claim_deduped")

# Apply transform
result = transform(bronze_df)
```

## Key Features

- **Pipeline orchestration** - Chain transforms
- **Data lineage** - Track inputs/outputs
- **Expression composition** - Combine expressions
- **Lazy evaluation** - Polars LazyFrame optimization
- **Quality framework** - Standardized measures

## Registration

Transforms register at import time via decorators:
- `@register_pipeline` - Standard transforms
- `@register_crosswalk` - Crosswalk operations
- `@MeasureFactory.register` - Quality measures
