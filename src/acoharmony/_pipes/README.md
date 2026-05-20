# _pipes

Pipeline orchestration and stage definitions.

## Purpose

Compose multi-stage transformation pipelines. Register via decorators. Execute via CLI.

## Core Components

### PipelineRegistry

Central registry for pipeline discovery.

```python
from acoharmony._pipes import register_pipeline, PipelineStage

@register_pipeline("medical_claim")
def medical_claim_pipeline():
    return [
        PipelineStage(name="bronze", transform="bronze_medical_claim"),
        PipelineStage(name="silver_adr", transform="int_physician_claim_adr"),
        PipelineStage(name="silver_dedup", transform="int_physician_claim_deduped"),
        PipelineStage(name="gold", transform="medical_claim")
    ]
```

### PipelineStage

Declarative stage definition.

```python
stage = PipelineStage(
    name="silver_dedup",
    transform="int_physician_claim_deduped",
    inputs=["bronze_physician_claims"],
    outputs=["silver_physician_deduped"]
)
```

### BronzeStage

Bronze parsing stage definition.

```python
bronze = BronzeStage(
    name="bronze_cclf1",
    source="cclf1",
    parser="cclf1_parser"
)
```

## Usage

### Define Pipeline

```python
@register_pipeline("eligibility")
def eligibility_pipeline():
    return [
        BronzeStage(name="bronze", source="cclf8", parser="cclf8_parser"),
        PipelineStage(name="silver", transform="int_beneficiary_demographics_deduped"),
        PipelineStage(name="gold", transform="eligibility")
    ]
```

### Execute Pipeline

```bash
uv run aco pipeline medical_claim
```

## Key Features

- **Declarative stages** - Define stages via classes
- **Transform references** - Reference transforms by name
- **Input/output tracking** - Data lineage
- **CLI integration** - Execute via `aco pipeline`
- **Decorator registration** - Auto-discovery at import

## Pipeline Types

### Bronze Pipelines
Parse raw files. Create bronze tables.

### Silver Pipelines
Standardize. Deduplicate. Apply business logic.

### Gold Pipelines
Analytics. Aggregations. Quality measures.

## Integration

- `_transforms/` - Transform execution
- `_registry/` - Transform discovery
- `CLI` - Pipeline execution
