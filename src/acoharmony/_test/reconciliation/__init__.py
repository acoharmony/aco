# © 2025 HarmonyCares
# All rights reserved.

"""
Reconciliation test suite.

Validates pipeline outputs against CMS-provided reference data:
- CCLF0 record counts vs parsed CCLF file row counts
- Quality measure denominators/numerators vs CMS quality reports
- Benchmark calculations vs BNMR/PLARU summary sheets
- Alignment counts vs BAR/ALR totals
- Financial settlement vs reconciliation reports

These tests require real data on disk at /opt/s3/data/workspace/.
Run with: pytest -m reconciliation
"""
