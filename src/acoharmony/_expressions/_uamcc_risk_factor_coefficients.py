# © 2025 HarmonyCares
# All rights reserved.

"""
UAMCC (Unplanned Admissions for Multiple Chronic Conditions) — Risk Factor
Coefficients for High-Needs Population ACOs.

These are the published fixed-effects coefficients for the hierarchical
negative binomial regression model that scores the UAMCC quality measure.
Published values lock in the per-risk-factor log-incidence-rate-ratio
contribution; running the model against our own beneficiary population
reproduces CMS's predicted-admission numerator for audit purposes.

Note that this is a *different* use of "unplanned admission" from the
one driving criterion IV.B.1(c) of the High-Needs eligibility rule.
That criterion operationalises "unplanned" via a much simpler filter —
per the Financial Operating Guide footnote 5 (PA/FOG
$BRONZE/REACHPY2024FinancialOperatGde.txt line 1193):

    "An unplanned hospital admission is defined as the claim for the
    inpatient stay being coded as non-elective, specifically based on
    the 'reason for admission' code (CLM_IP_ADMSN_TYPE_CD is not 3)."

So the criterion-(c) code path does not touch this module; this is
quality-measure scoring only.

Provenance
----------

Source: ``$BRONZE/PY2023-HighNeedsRiskFactorsReport.pdf``, Table 6
"UAMCC Coefficients, High Needs Population ACOs" — PDF pages 26–27
(doc pages 4-2..4-3).

Column layout matches the ACR tables:

    Factor | Prevalence (%) | Coefficient | Std. Err. | p-value

The reference age cell ("Age <70 (ref)") carries ``N/A`` for coefficient,
std-err, and p-value; all other factors have numeric coefficients. For
"ref" rows we store ``coefficient: None``.

Values were extracted by the same PDF parser used for the ACR tables —
see ``_quality_measure_risk_factor_coefficients.py`` docstring for the
full procedure.

Total rows: 54 (1 intercept + 5 age cells + 48 risk factors).

The ACR module's caveat applies equally here: the published coefficients
fluctuate across reporting periods and cannot be used to exactly
reproduce the CMS-published official score. Use for auditing, not to
replace the CMS calculation.
"""

from __future__ import annotations


UAMCC_COEFFICIENTS: list[dict] = [
    {"factor": "Intercept", "prevalence_pct": None, "coefficient": -1.0217, "std_err": 0.0077, "p_value": "<.0001"},
    {"factor": "Age <70 (ref)", "prevalence_pct": 11.7, "coefficient": None, "std_err": None, "p_value": "N/A"},
    {"factor": "Age 70-75", "prevalence_pct": 19.6, "coefficient": 0.0635, "std_err": 0.0052, "p_value": "<.0001"},
    {"factor": "Age 75-80", "prevalence_pct": 21.8, "coefficient": 0.1530, "std_err": 0.0051, "p_value": "<.0001"},
    {"factor": "Age 80-85", "prevalence_pct": 19.7, "coefficient": 0.2721, "std_err": 0.0053, "p_value": "<.0001"},
    {"factor": "Age >85", "prevalence_pct": 27.2, "coefficient": 0.4166, "std_err": 0.0054, "p_value": "<.0001"},
    {"factor": "Dialysis status", "prevalence_pct": 4.1, "coefficient": 0.2540, "std_err": 0.0063, "p_value": "<.0001"},
    {"factor": "Respiratory failure", "prevalence_pct": 24.2, "coefficient": 0.0028, "std_err": 0.0036, "p_value": "0.4430"},
    {"factor": "Liver disease", "prevalence_pct": 4.8, "coefficient": 0.1559, "std_err": 0.0060, "p_value": "<.0001"},
    {"factor": "Pneumonia", "prevalence_pct": 19.0, "coefficient": 0.1420, "std_err": 0.0036, "p_value": "<.0001"},
    {"factor": "Septicemia/shock", "prevalence_pct": 14.3, "coefficient": -0.0309, "std_err": 0.0040, "p_value": "<.0001"},
    {"factor": "Marked disability/frailty", "prevalence_pct": 26.1, "coefficient": 0.0651, "std_err": 0.0031, "p_value": "<.0001"},
    {"factor": "Hematological disease", "prevalence_pct": 21.8, "coefficient": -0.0127, "std_err": 0.0033, "p_value": "<.0001"},
    {"factor": "Advanced cancer", "prevalence_pct": 15.3, "coefficient": -0.0287, "std_err": 0.0039, "p_value": "<.0001"},
    {"factor": "Infectious and immune disorders", "prevalence_pct": 14.5, "coefficient": -0.0279, "std_err": 0.0040, "p_value": "<.0001"},
    {"factor": "Severe cognitive impairment", "prevalence_pct": 14.7, "coefficient": 0.1218, "std_err": 0.0039, "p_value": "<.0001"},
    {"factor": "Major organ transplant status", "prevalence_pct": 3.1, "coefficient": -0.3051, "std_err": 0.0097, "p_value": "<.0001"},
    {"factor": "Pulmonary heart disease", "prevalence_pct": 16.9, "coefficient": 0.0600, "std_err": 0.0035, "p_value": "<.0001"},
    {"factor": "Cardiomyopathy", "prevalence_pct": 12.6, "coefficient": 0.0185, "std_err": 0.0040, "p_value": "<.0001"},
    {"factor": "Gastrointestinal disease", "prevalence_pct": 26.5, "coefficient": 0.0290, "std_err": 0.0031, "p_value": "<.0001"},
    {"factor": "Iron deficiency anemia", "prevalence_pct": 55.2, "coefficient": 0.0544, "std_err": 0.0029, "p_value": "<.0001"},
    {"factor": "Ischemic heart disease, except acute myocardial infarction (AMI)", "prevalence_pct": 56.4, "coefficient": 0.0300, "std_err": 0.0029, "p_value": "<.0001"},
    {"factor": "Other lung disorders", "prevalence_pct": 38.4, "coefficient": -0.0146, "std_err": 0.0029, "p_value": "<.0001"},
    {"factor": "Vascular or circulatory disease", "prevalence_pct": 61.5, "coefficient": -0.0206, "std_err": 0.0028, "p_value": "<.0001"},
    {"factor": "Other significant endocrine disorders", "prevalence_pct": 9.9, "coefficient": 0.0140, "std_err": 0.0044, "p_value": "0.0010"},
    {"factor": "Other disabilities and paralysis", "prevalence_pct": 8.2, "coefficient": -0.1538, "std_err": 0.0053, "p_value": "<.0001"},
    {"factor": "Substance abuse", "prevalence_pct": 16.5, "coefficient": 0.0892, "std_err": 0.0036, "p_value": "<.0001"},
    {"factor": "Other neurologic disorders", "prevalence_pct": 41.7, "coefficient": 0.0185, "std_err": 0.0027, "p_value": "<.0001"},
    {"factor": "Specified arrhythmias and other heart disorders", "prevalence_pct": 36.3, "coefficient": -0.0123, "std_err": 0.0029, "p_value": "<.0001"},
    {"factor": "Hypertension", "prevalence_pct": 89.1, "coefficient": 0.0109, "std_err": 0.0045, "p_value": "0.0150"},
    {"factor": "Hip or vertebral fracture", "prevalence_pct": 9.7, "coefficient": -0.1255, "std_err": 0.0046, "p_value": "<.0001"},
    {"factor": "Lower-risk cardiovascular disease", "prevalence_pct": 37.1, "coefficient": 0.0412, "std_err": 0.0030, "p_value": "<.0001"},
    {"factor": "Cerebrovascular disease", "prevalence_pct": 5.0, "coefficient": 0.0272, "std_err": 0.0061, "p_value": "<.0001"},
    {"factor": "Morbid obesity", "prevalence_pct": 18.6, "coefficient": -0.0333, "std_err": 0.0036, "p_value": "<.0001"},
    {"factor": "Urinary disorders", "prevalence_pct": 31.2, "coefficient": -0.0011, "std_err": 0.0029, "p_value": "0.7000"},
    {"factor": "Psychiatric disorders other than depression", "prevalence_pct": 35.6, "coefficient": 0.0370, "std_err": 0.0030, "p_value": "<.0001"},
    {"factor": "AMI", "prevalence_pct": 2.6, "coefficient": -0.0611, "std_err": 0.0081, "p_value": "<.0001"},
    {"factor": "Alzheimer's disease and related disorders or senile dementia", "prevalence_pct": 32.5, "coefficient": 0.1203, "std_err": 0.0031, "p_value": "<.0001"},
    {"factor": "Atrial fibrillation", "prevalence_pct": 29.6, "coefficient": 0.0841, "std_err": 0.0030, "p_value": "<.0001"},
    {"factor": "Chronic kidney disease", "prevalence_pct": 67.2, "coefficient": 0.0897, "std_err": 0.0030, "p_value": "<.0001"},
    {"factor": "Chronic obstructive pulmonary disease/asthma", "prevalence_pct": 39.1, "coefficient": 0.0991, "std_err": 0.0029, "p_value": "<.0001"},
    {"factor": "Depression", "prevalence_pct": 44.1, "coefficient": 0.0386, "std_err": 0.0030, "p_value": "<.0001"},
    {"factor": "Heart failure", "prevalence_pct": 51.5, "coefficient": 0.1500, "std_err": 0.0030, "p_value": "<.0001"},
    {"factor": "Stroke or transient ischemic attack", "prevalence_pct": 17.2, "coefficient": -0.0512, "std_err": 0.0038, "p_value": "<.0001"},
    {"factor": "Diabetes", "prevalence_pct": 52.3, "coefficient": 0.0985, "std_err": 0.0028, "p_value": "<.0001"},
    {"factor": "Walking aids", "prevalence_pct": 6.7, "coefficient": -0.0958, "std_err": 0.0053, "p_value": "<.0001"},
    {"factor": "Wheelchair", "prevalence_pct": 8.0, "coefficient": 0.0640, "std_err": 0.0049, "p_value": "<.0001"},
    {"factor": "Hospital bed", "prevalence_pct": 3.6, "coefficient": -0.0237, "std_err": 0.0074, "p_value": "0.0010"},
    {"factor": "Lifts", "prevalence_pct": 0.8, "coefficient": 0.0498, "std_err": 0.0144, "p_value": "0.0010"},
    {"factor": "Oxygen", "prevalence_pct": 12.4, "coefficient": 0.1900, "std_err": 0.0041, "p_value": "<.0001"},
    {"factor": "Original reason for entitlement: disability insurance beneficiary", "prevalence_pct": 17.4, "coefficient": 0.0792, "std_err": 0.0037, "p_value": "<.0001"},
    {"factor": "Original reason for entitlement: End-stage renal disease", "prevalence_pct": 1.0, "coefficient": -0.0292, "std_err": 0.0162, "p_value": "0.0710"},
    {"factor": "Low Agency for Healthcare Research and Quality socioeconomic status index (<25th percentile)", "prevalence_pct": 13.3, "coefficient": 0.0359, "std_err": 0.0039, "p_value": "<.0001"},
    {"factor": "Density of physician specialists (<25th percentile)", "prevalence_pct": 2.6, "coefficient": -0.0305, "std_err": 0.0088, "p_value": "0.0010"},
]


EXPECTED_COUNT = 54
assert len(UAMCC_COEFFICIENTS) == EXPECTED_COUNT
