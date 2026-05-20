# © 2025 HarmonyCares
# All rights reserved.

"""
Quality Measure Risk Factor coefficients for High-Needs Population ACOs.

These coefficients are the empirical regression outputs CMS publishes
for the two claims-based, risk-adjusted quality measures that apply to
High-Needs Population ACOs in ACO REACH:

    1. All-Condition Readmission (ACR) — Tables 1 through 5, one per
       specialty cohort (Medicine, Surgery/Gynecology,
       Cardiorespiratory, Cardiovascular, Neurology).
    2. All-Cause Unplanned Admissions for Patients with Multiple
       Chronic Conditions (UAMCC) — Table 6.

Only the ACR coefficients (Tables 1–5) are transcribed below. UAMCC's
Table 6 lives in a sibling module; the two measures are scored
independently, and grouping them here would just make the provenance
note unwieldy.

Purpose of these values
-----------------------

The ACR measure computes, for each specialty cohort and REACH ACO, a
ratio of predicted to expected readmission rates. The predicted numerator
uses the REACH ACO's own beneficiary mix; the expected denominator uses
the overall national performance with the same case mix. Both the
numerator and denominator are produced by hierarchical logistic
regression models, one per cohort, and the coefficients below are the
fixed-effects intercepts and per-risk-factor log-odds contributions
published by CMS.

Purpose here (in a REACH ACO code base): run these models against our
own beneficiary population to reproduce what CMS publishes in the Annual
Quality Report, and flag discrepancies.

Provenance
----------

CMS does not distribute these coefficients in machine-readable form.
The authoritative published source is:

    ACO REACH Model: Quality Measure Risk Factors Report for PY 2023 —
    Quality Measures for High Needs Population ACOs (RTI International
    for CMS, October 2024, Revision: "Initial Posting" dated October 17,
    2024).

The binding bronze-layer source is:
    ``$BRONZE/PY2023-HighNeedsRiskFactorsReport.pdf`` (36 pages).

A text extraction of the same document
(``$BRONZE/PY2023-HighNeedsRiskFactorsReport.txt``, 3,965 lines) is
available as a convenience mirror but has noisier layout; values below
were extracted from the PDF.

ACR tables span doc-pages 3-1 through 3-14 (PDF pages 11 through 24):

    Table 1 — Medicine cohort:           doc p. 3-1 (PDF p. 11)
    Table 2 — Surgery/Gynecology:        doc p. 3-6 (PDF p. 16)
    Table 3 — Cardiorespiratory:         doc p. 3-9 (PDF p. 19)
    Table 4 — Cardiovascular:            doc p. 3-11 (PDF p. 21)
    Table 5 — Neurology:                 doc p. 3-12 (PDF p. 22)

Each table has the column layout:

    Factor | Prevalence (%) | Coefficient | Std. Err. | p-value

where "Coefficient" is the log-odds change contribution of that risk
factor to the readmission probability (ACR is a hierarchical logistic
regression model), "Prevalence" is the share of the calibration-sample
beneficiaries who had the factor, and "p-value" is either a decimal,
the string "<.0001", or "N/A" for reference factors whose coefficient
is fixed at 0.0000.

Values were extracted by a Python parser that:

    1. Concatenates PDF pages 11 through 24 via ``pypdf.extract_text``.
    2. Locates each "Table N. Model Coefficients for …" header.
    3. Walks the lines between consecutive table headers, skipping page
       chrome (headers/footers) and footnote lines.
    4. Recognises a data row as a line (or sequence of lines) whose
       last four tokens are floats/``N/A``/``<.0001``. Wrapped factor
       names (rows where the factor text flows onto continuation lines
       before the numeric tail) are reassembled into a single factor
       label.

Total rows parsed — all cohorts:

    Medicine:            121
    Surgery/Gynecology:  107
    Cardiorespiratory:    47
    Cardiovascular:       47
    Neurology:            44
    TOTAL:               366

These totals are locked as ``EXPECTED_COUNTS`` below; the companion
test module asserts them. The coefficients for the UAMCC model (Table 6
starting on PDF page 25) are in a sibling module.

Why a Python dict literal, not a CSV
------------------------------------

The source is a PDF; transcribing into a CSV adds a second format with
no added safety. The list-of-dict layout here carries the raw
``p_value`` as a string because CMS publishes three different forms
(decimal, ``<.0001``, ``N/A``), and preserving the exact string lets
consumers decide how to interpret each.

Note on caveats from the source document
-----------------------------------------

The report itself cautions (PDF page 3-0, source line 71):

    "Because the claims-based, risk-adjusted quality measures used on
    ACO REACH are estimated using hierarchical regression models and
    because the actual coefficients will fluctuate to some degree
    across reporting periods, ACOs will not be able to use these tables
    to recreate their official quality measure scores."

Consumers of this module must treat the scores as illustrative, not
authoritative. The official score comes from CMS's own production
calculation. We use these coefficients to reproduce a close
approximation for auditing our own population, not to replace the
CMS-published numbers.
"""

from __future__ import annotations


# =============================================================================
# Row record shape
# =============================================================================
# Every coefficient row is a dict with five keys:
#
#   factor          (str)          — risk-factor label as published
#   prevalence_pct  (float | None) — prevalence in calibration sample,
#                                     ``None`` for reference rows
#   coefficient     (float)        — log-odds contribution
#   std_err         (float | None) — standard error, ``None`` for reference rows
#   p_value         (str)          — raw p-value token: "<.0001", "N/A", or
#                                     a decimal as a string (e.g., "0.0123")
#
# ``p_value`` stays as a string so CMS's exact published text is
# preserved; numeric interpretation is left to consumers.


# =============================================================================
# Table 1 — Medicine cohort (121 rows)
# Source: PDF page 11 (doc page 3-1) through page 15 (doc page 3-5).
# =============================================================================

# =============================================================================
# Table 1 — Medicine cohort (121 rows)
# Source: PDF page 11 (doc page 3-1) through page 15 (doc page 3-5).
# =============================================================================

ACR_MEDICINE_COEFFICIENTS: list[dict] = [
    {"factor": 'Intercept', "prevalence_pct": None, "coefficient": -1.8162, "std_err": 0.0130, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 0001', "prevalence_pct": 1.2, "coefficient": -0.1431, "std_err": 0.0270, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 118', "prevalence_pct": 0.6, "coefficient": -0.1181, "std_err": 0.0372, "p_value": '0.0015'},
    {"factor": 'Diagnosis CCS 120', "prevalence_pct": 0.2, "coefficient": 0.0575, "std_err": 0.0562, "p_value": '0.3061'},
    {"factor": 'Diagnosis CCS 121', "prevalence_pct": 0.1, "coefficient": 0.0533, "std_err": 0.0691, "p_value": '0.4408'},
    {"factor": 'Diagnosis CCS 123', "prevalence_pct": 0.4, "coefficient": -0.2911, "std_err": 0.0455, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 126', "prevalence_pct": 0.1, "coefficient": -0.2651, "std_err": 0.0849, "p_value": '0.0018'},
    {"factor": 'Diagnosis CCS 134', "prevalence_pct": 0.1, "coefficient": -0.1052, "std_err": 0.0816, "p_value": '0.1973'},
    {"factor": 'Diagnosis CCS 135', "prevalence_pct": 1.3, "coefficient": -0.0098, "std_err": 0.0248, "p_value": '0.6940'},
    {"factor": 'Diagnosis CCS 137', "prevalence_pct": 0.1, "coefficient": -0.2838, "std_err": 0.1046, "p_value": '0.0067'},
    {"factor": 'Diagnosis CCS 138', "prevalence_pct": 0.6, "coefficient": -0.0603, "std_err": 0.0364, "p_value": '0.0979'},
    {"factor": 'Diagnosis CCS 139', "prevalence_pct": 0.1, "coefficient": -0.1734, "std_err": 0.0969, "p_value": '0.0735'},
    {"factor": 'Diagnosis CCS 140', "prevalence_pct": 0.5, "coefficient": 0.0455, "std_err": 0.0377, "p_value": '0.2274'},
    {"factor": 'Diagnosis CCS 141', "prevalence_pct": 0.7, "coefficient": 0.0201, "std_err": 0.0314, "p_value": '0.5207'},
    {"factor": 'Diagnosis CCS 142', "prevalence_pct": 0.1, "coefficient": 0.0641, "std_err": 0.1221, "p_value": '0.5999'},
    {"factor": 'Diagnosis CCS 143', "prevalence_pct": 0.2, "coefficient": -0.2999, "std_err": 0.0608, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 144', "prevalence_pct": 0.2, "coefficient": 0.0889, "std_err": 0.0531, "p_value": '0.0941'},
    {"factor": 'Diagnosis CCS 145', "prevalence_pct": 2.0, "coefficient": -0.1562, "std_err": 0.0221, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 146', "prevalence_pct": 1.5, "coefficient": 0.0156, "std_err": 0.0231, "p_value": '0.5000'},
    {"factor": 'Diagnosis CCS 147', "prevalence_pct": 0.2, "coefficient": 0.0021, "std_err": 0.0625, "p_value": '0.9729'},
    {"factor": 'Diagnosis CCS 148', "prevalence_pct": 0.1, "coefficient": 0.1109, "std_err": 0.0711, "p_value": '0.1189'},
    {"factor": 'Diagnosis CCS 149', "prevalence_pct": 0.9, "coefficient": 0.0983, "std_err": 0.0296, "p_value": '0.0009'},
    {"factor": 'Diagnosis CCS 151', "prevalence_pct": 0.6, "coefficient": 0.1417, "std_err": 0.0330, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 152', "prevalence_pct": 0.5, "coefficient": 0.0132, "std_err": 0.0370, "p_value": '0.7215'},
    {"factor": 'Diagnosis CCS 153', "prevalence_pct": 2.7, "coefficient": -0.0742, "std_err": 0.0180, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 154', "prevalence_pct": 0.7, "coefficient": -0.0570, "std_err": 0.0340, "p_value": '0.0940'},
    {"factor": 'Diagnosis CCS 155', "prevalence_pct": 1.3, "coefficient": 0.0032, "std_err": 0.0242, "p_value": '0.8966'},
    {"factor": 'Diagnosis CCS 157', "prevalence_pct": 5.6, "coefficient": -0.0456, "std_err": 0.0134, "p_value": '0.0007'},
    {"factor": 'Diagnosis CCS 159', "prevalence_pct": 6.3, "coefficient": -0.0764, "std_err": 0.0136, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 160', "prevalence_pct": 0.1, "coefficient": -0.0523, "std_err": 0.1061, "p_value": '0.6217'},
    {"factor": 'Diagnosis CCS 161', "prevalence_pct": 0.3, "coefficient": 0.0209, "std_err": 0.0531, "p_value": '0.6942'},
    {"factor": 'Diagnosis CCS 162', "prevalence_pct": 0.1, "coefficient": 0.1805, "std_err": 0.0951, "p_value": '0.0577'},
    {"factor": 'Diagnosis CCS 163', "prevalence_pct": 0.2, "coefficient": -0.0859, "std_err": 0.0628, "p_value": '0.1716'},
    {"factor": 'Diagnosis CCS 164', "prevalence_pct": 0.1, "coefficient": 0.0972, "std_err": 0.0974, "p_value": '0.3183'},
    {"factor": 'Diagnosis CCS 165', "prevalence_pct": 0.1, "coefficient": -0.2593, "std_err": 0.1046, "p_value": '0.0132'},
    {"factor": 'Diagnosis CCS 197', "prevalence_pct": 2.4, "coefficient": -0.1777, "std_err": 0.0200, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 199', "prevalence_pct": 0.2, "coefficient": -0.2106, "std_err": 0.0616, "p_value": '0.0006'},
    {"factor": 'Diagnosis CCS 2', "prevalence_pct": 21.1, "coefficient": -0.1975, "std_err": 0.0100, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 201', "prevalence_pct": 0.2, "coefficient": -0.1127, "std_err": 0.0568, "p_value": '0.0474'},
    {"factor": 'Diagnosis CCS 203', "prevalence_pct": 0.2, "coefficient": -0.3866, "std_err": 0.0800, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 204', "prevalence_pct": 0.2, "coefficient": -0.2544, "std_err": 0.0674, "p_value": '0.0002'},
    {"factor": 'Diagnosis CCS 205', "prevalence_pct": 0.8, "coefficient": -0.0705, "std_err": 0.0322, "p_value": '0.0287'},
    {"factor": 'Diagnosis CCS 207', "prevalence_pct": 0.5, "coefficient": -0.4059, "std_err": 0.0461, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 210', "prevalence_pct": 0.1, "coefficient": 0.1114, "std_err": 0.0962, "p_value": '0.2467'},
    {"factor": 'Diagnosis CCS 211', "prevalence_pct": 0.6, "coefficient": -0.2151, "std_err": 0.0381, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 226', "prevalence_pct": 0.3, "coefficient": -0.5710, "std_err": 0.0629, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 228', "prevalence_pct": 0.1, "coefficient": -0.4568, "std_err": 0.1021, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 229', "prevalence_pct": 0.4, "coefficient": -0.1987, "std_err": 0.0471, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 230', "prevalence_pct": 0.4, "coefficient": -0.3581, "std_err": 0.0517, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 231', "prevalence_pct": 2.6, "coefficient": -0.2997, "std_err": 0.0210, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 232', "prevalence_pct": 0.1, "coefficient": -0.3443, "std_err": 0.1024, "p_value": '0.0008'},
    {"factor": 'Diagnosis CCS 234', "prevalence_pct": 0.3, "coefficient": 0.0074, "std_err": 0.0485, "p_value": '0.8786'},
    {"factor": 'Diagnosis CCS 235', "prevalence_pct": 0.1, "coefficient": -0.4839, "std_err": 0.1071, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 236', "prevalence_pct": 0.1, "coefficient": -0.0927, "std_err": 0.1135, "p_value": '0.4137'},
    {"factor": 'Diagnosis CCS 237', "prevalence_pct": 5.5, "coefficient": -0.0303, "std_err": 0.0137, "p_value": '0.0277'},
    {"factor": 'Diagnosis CCS 238', "prevalence_pct": 2.5, "coefficient": -0.0931, "std_err": 0.0187, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 239', "prevalence_pct": 0.4, "coefficient": -0.1639, "std_err": 0.0454, "p_value": '0.0003'},
    {"factor": 'Diagnosis CCS 242', "prevalence_pct": 0.3, "coefficient": -0.1891, "std_err": 0.0512, "p_value": '0.0002'},
    {"factor": 'Diagnosis CCS 244', "prevalence_pct": 0.5, "coefficient": -0.1965, "std_err": 0.0420, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 245', "prevalence_pct": 0.6, "coefficient": -0.3827, "std_err": 0.0396, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 246', "prevalence_pct": 0.1, "coefficient": -0.1119, "std_err": 0.0789, "p_value": '0.1561'},
    {"factor": 'Diagnosis CCS 250', "prevalence_pct": 0.1, "coefficient": 0.0788, "std_err": 0.0679, "p_value": '0.2458'},
    {"factor": 'Diagnosis CCS 251', "prevalence_pct": 0.2, "coefficient": 0.0200, "std_err": 0.0692, "p_value": '0.7725'},
    {"factor": 'Diagnosis CCS 252', "prevalence_pct": 0.4, "coefficient": -0.1410, "std_err": 0.0463, "p_value": '0.0023'},
    {"factor": 'Diagnosis CCS 253', "prevalence_pct": 0.1, "coefficient": -0.0010, "std_err": 0.1037, "p_value": '0.9927'},
    {"factor": 'Diagnosis CCS 257', "prevalence_pct": 0.1, "coefficient": -1.3756, "std_err": 0.1344, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 259', "prevalence_pct": 0.2, "coefficient": -0.1498, "std_err": 0.0587, "p_value": '0.0107'},
    {"factor": 'Diagnosis CCS 3', "prevalence_pct": 0.2, "coefficient": -0.1232, "std_err": 0.0597, "p_value": '0.0390'},
    {"factor": 'Diagnosis CCS 4', "prevalence_pct": 0.1, "coefficient": 0.1679, "std_err": 0.0672, "p_value": '0.0125'},
    {"factor": 'Diagnosis CCS 47', "prevalence_pct": 0.1, "coefficient": -0.2232, "std_err": 0.0799, "p_value": '0.0052'},
    {"factor": 'Diagnosis CCS 48', "prevalence_pct": 0.1, "coefficient": -0.0042, "std_err": 0.0901, "p_value": '0.9630'},
    {"factor": 'Diagnosis CCS 50', "prevalence_pct": 2.1, "coefficient": -0.0578, "std_err": 0.0200, "p_value": '0.0039'},
    {"factor": 'Diagnosis CCS 51', "prevalence_pct": 0.8, "coefficient": 0.0402, "std_err": 0.0311, "p_value": '0.1969'},
    {"factor": 'Diagnosis CCS 52', "prevalence_pct": 0.1, "coefficient": -0.2171, "std_err": 0.0790, "p_value": '0.0060'},
    {"factor": 'Diagnosis CCS 54', "prevalence_pct": 0.1, "coefficient": -0.2854, "std_err": 0.0770, "p_value": '0.0002'},
    {"factor": 'Diagnosis CCS 55', "prevalence_pct": 3.0, "coefficient": -0.0257, "std_err": 0.0170, "p_value": '0.1306'},
    {"factor": 'Diagnosis CCS 58', "prevalence_pct": 0.7, "coefficient": -0.1219, "std_err": 0.0335, "p_value": '0.0003'},
    {"factor": 'Diagnosis CCS 59', "prevalence_pct": 1.3, "coefficient": 0.0716, "std_err": 0.0240, "p_value": '0.0029'},
    {"factor": 'Diagnosis CCS 6', "prevalence_pct": 0.1, "coefficient": 0.1528, "std_err": 0.0903, "p_value": '0.0907'},
    {"factor": 'Diagnosis CCS 60', "prevalence_pct": 0.7, "coefficient": 0.0066, "std_err": 0.0329, "p_value": '0.8408'},
    {"factor": 'Diagnosis CCS 62', "prevalence_pct": 0.6, "coefficient": -0.1245, "std_err": 0.0371, "p_value": '0.0008'},
    {"factor": 'Diagnosis CCS 63', "prevalence_pct": 0.3, "coefficient": 0.0285, "std_err": 0.0503, "p_value": '0.5708'},
    {"factor": 'Diagnosis CCS 653', "prevalence_pct": 0.6, "coefficient": -0.2626, "std_err": 0.0391, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 660', "prevalence_pct": 0.6, "coefficient": 0.0655, "std_err": 0.0356, "p_value": '0.0655'},
    {"factor": 'Diagnosis CCS 661', "prevalence_pct": 0.1, "coefficient": -0.0635, "std_err": 0.1023, "p_value": '0.5346'},
    {"factor": 'Diagnosis CCS 7', "prevalence_pct": 0.2, "coefficient": -0.1902, "std_err": 0.0620, "p_value": '0.0022'},
    {"factor": 'Diagnosis CCS 84', "prevalence_pct": 0.1, "coefficient": -0.4403, "std_err": 0.0982, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 93', "prevalence_pct": 0.3, "coefficient": -0.5442, "std_err": 0.0652, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 99', "prevalence_pct": 16.3, "coefficient": 0.0000, "std_err": None, "p_value": 'N/A'},
    {"factor": 'Years over 65 (continuous)2', "prevalence_pct": None, "coefficient": -0.0078, "std_err": 0.0004, "p_value": '<.0001'},
    {"factor": 'Metastatic cancer and acute leukemia', "prevalence_pct": 7.0, "coefficient": 0.0943, "std_err": 0.0118, "p_value": '<.0001'},
    {"factor": 'Severe cancer', "prevalence_pct": 9.9, "coefficient": 0.1132, "std_err": 0.0095, "p_value": '<.0001'},
    {"factor": 'Other cancers', "prevalence_pct": 13.0, "coefficient": 0.0603, "std_err": 0.0085, "p_value": '<.0001'},
    {"factor": 'Severe hematological disorders', "prevalence_pct": 1.9, "coefficient": 0.2125, "std_err": 0.0187, "p_value": '<.0001'},
    {"factor": 'Coagulation defects and other specified hematological disorders', "prevalence_pct": 19.9, "coefficient": 0.0540, "std_err": 0.0070, "p_value": '<.0001'},
    {"factor": 'Iron deficiency or other/unspecified anemias and blood disease', "prevalence_pct": 61.1, "coefficient": 0.1584, "std_err": 0.0064, "p_value": '<.0001'},
    {"factor": 'End-stage liver disease; cirrhosis of liver', "prevalence_pct": 5.8, "coefficient": 0.1860, "std_err": 0.0115, "p_value": '<.0001'},
    {"factor": 'Pancreatic disease; peptic ulcer, hemorrhage, other specified gastrointestinal disorders', "prevalence_pct": 19.7, "coefficient": 0.1224, "std_err": 0.0071, "p_value": '<.0001'},
    {"factor": 'Dialysis status', "prevalence_pct": 5.6, "coefficient": 0.2029, "std_err": 0.0114, "p_value": '<.0001'},
    {"factor": 'Renal failure', "prevalence_pct": 65.3, "coefficient": 0.1127, "std_err": 0.0065, "p_value": '<.0001'},
    {"factor": 'Transplants', "prevalence_pct": 2.9, "coefficient": 0.0434, "std_err": 0.0158, "p_value": '0.0060'},
    {"factor": 'Severe infection', "prevalence_pct": 2.0, "coefficient": 0.0835, "std_err": 0.0182, "p_value": '<.0001'},
    {"factor": 'Other infectious diseases and pneumonias', "prevalence_pct": 49.1, "coefficient": 0.0729, "std_err": 0.0062, "p_value": '<.0001'},
    {"factor": 'Septicemia, sepsis, systemic inflammatory response syndrome/shock', "prevalence_pct": 28.4, "coefficient": 0.0425, "std_err": 0.0069, "p_value": '<.0001'},
    {"factor": 'Congestive heart failure', "prevalence_pct": 53.8, "coefficient": 0.1060, "std_err": 0.0067, "p_value": '<.0001'},
    {"factor": 'Coronary atherosclerosis or angina, cerebrovascular disease 70.2 0.0625 0.0066 <.0001 Specified arrhythmias and other heart rhythm disorders', "prevalence_pct": 54.8, "coefficient": 0.0647, "std_err": 0.0061, "p_value": '<.0001'},
    {"factor": 'Cardiorespiratory failure and shock', "prevalence_pct": 40.8, "coefficient": 0.0325, "std_err": 0.0065, "p_value": '<.0001'},
    {"factor": 'Chronic obstructive pulmonary disease', "prevalence_pct": 29.9, "coefficient": 0.0984, "std_err": 0.0063, "p_value": '<.0001'},
    {"factor": 'Fibrosis of lung or other chronic lung disorders', "prevalence_pct": 5.1, "coefficient": 0.0784, "std_err": 0.0121, "p_value": '<.0001'},
    {"factor": 'Protein-calorie malnutrition', "prevalence_pct": 21.7, "coefficient": 0.1089, "std_err": 0.0068, "p_value": '<.0001'},
    {"factor": 'Other significant endocrine and metabolic disorders; disorders of fluid/electrolyte/acid-base balance', "prevalence_pct": 64.9, "coefficient": 0.1231, "std_err": 0.0064, "p_value": '<.0001'},
    {"factor": 'Rheumatoid arthritis and inflammatory connective tissue disease', "prevalence_pct": 8.5, "coefficient": 0.0466, "std_err": 0.0097, "p_value": '<.0001'},
    {"factor": 'Diabetes mellitus (DM) or DM complications', "prevalence_pct": 45.8, "coefficient": 0.0726, "std_err": 0.0058, "p_value": '<.0001'},
    {"factor": 'Decubitus ulcer or chronic skin ulcer', "prevalence_pct": 14.2, "coefficient": 0.1206, "std_err": 0.0080, "p_value": '<.0001'},
    {"factor": 'Hemiplegia, paraplegia, paralysis, functional disability', "prevalence_pct": 11.4, "coefficient": 0.0079, "std_err": 0.0088, "p_value": '0.3724'},
    {"factor": 'Seizure disorders and convulsions', "prevalence_pct": 6.3, "coefficient": 0.0641, "std_err": 0.0112, "p_value": '<.0001'},
    {"factor": 'Respirator dependence/tracheostomy status', "prevalence_pct": 0.8, "coefficient": 0.1722, "std_err": 0.0288, "p_value": '<.0001'},
    {"factor": 'Drug/alcohol psychosis or dependence', "prevalence_pct": 4.4, "coefficient": 0.1077, "std_err": 0.0131, "p_value": '<.0001'},
    {"factor": 'Psychiatric comorbidity', "prevalence_pct": 29.3, "coefficient": 0.0584, "std_err": 0.0061, "p_value": '<.0001'},
    {"factor": 'Hip fracture/dislocation', "prevalence_pct": 4.5, "coefficient": -0.1479, "std_err": 0.0139, "p_value": '<.0001'},
    {"factor": 'History of COVID-19', "prevalence_pct": 15.4, "coefficient": 0.0449, "std_err": 0.0075, "p_value": '<.0001'},
]


# =============================================================================
# Table 2 — Surgery/Gynecology cohort (107 rows)
# Source: PDF page 16 (doc page 3-6) through page 18 (doc page 3-8).
# =============================================================================

ACR_SURGERY_GYNECOLOGY_COEFFICIENTS: list[dict] = [
    {"factor": 'Intercept', "prevalence_pct": None, "coefficient": -1.7162, "std_err": 0.0443, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 0001', "prevalence_pct": 6.4, "coefficient": -0.1470, "std_err": 0.0455, "p_value": '0.0012'},
    {"factor": 'Diagnosis CCS 100', "prevalence_pct": 0.9, "coefficient": -0.1651, "std_err": 0.0659, "p_value": '0.0122'},
    {"factor": 'Diagnosis CCS 101', "prevalence_pct": 1.8, "coefficient": -0.2806, "std_err": 0.0569, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 106', "prevalence_pct": 0.2, "coefficient": 0.0008, "std_err": 0.1016, "p_value": '0.9936'},
    {"factor": 'Diagnosis CCS 109', "prevalence_pct": 2.2, "coefficient": -0.3304, "std_err": 0.0558, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 11', "prevalence_pct": 0.5, "coefficient": -0.3837, "std_err": 0.0907, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 110', "prevalence_pct": 1.8, "coefficient": -0.7193, "std_err": 0.0614, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 111', "prevalence_pct": 0.2, "coefficient": -0.7746, "std_err": 0.1445, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 114', "prevalence_pct": 1.1, "coefficient": -0.0740, "std_err": 0.0617, "p_value": '0.2304'},
    {"factor": 'Diagnosis CCS 118', "prevalence_pct": 0.2, "coefficient": -0.1590, "std_err": 0.1124, "p_value": '0.1573'},
    {"factor": 'Diagnosis CCS 122', "prevalence_pct": 0.2, "coefficient": -0.1567, "std_err": 0.1022, "p_value": '0.1252'},
    {"factor": 'Diagnosis CCS 13', "prevalence_pct": 0.2, "coefficient": -0.1422, "std_err": 0.1110, "p_value": '0.2003'},
    {"factor": 'Diagnosis CCS 130', "prevalence_pct": 0.3, "coefficient": -0.1176, "std_err": 0.0953, "p_value": '0.2172'},
    {"factor": 'Diagnosis CCS 133', "prevalence_pct": 0.2, "coefficient": -0.3996, "std_err": 0.1282, "p_value": '0.0018'},
    {"factor": 'Diagnosis CCS 138', "prevalence_pct": 0.1, "coefficient": -0.1794, "std_err": 0.1417, "p_value": '0.2055'},
    {"factor": 'Diagnosis CCS 14', "prevalence_pct": 1.3, "coefficient": -0.4670, "std_err": 0.0637, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 141', "prevalence_pct": 0.2, "coefficient": 0.0984, "std_err": 0.1032, "p_value": '0.3400'},
    {"factor": 'Diagnosis CCS 142', "prevalence_pct": 0.3, "coefficient": -0.3413, "std_err": 0.1047, "p_value": '0.0011'},
    {"factor": 'Diagnosis CCS 143', "prevalence_pct": 2.2, "coefficient": -0.4360, "std_err": 0.0552, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 145', "prevalence_pct": 1.9, "coefficient": -0.2469, "std_err": 0.0555, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 146', "prevalence_pct": 0.9, "coefficient": -0.2142, "std_err": 0.0685, "p_value": '0.0018'},
    {"factor": 'Diagnosis CCS 147', "prevalence_pct": 0.3, "coefficient": -0.5321, "std_err": 0.1042, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 149', "prevalence_pct": 1.9, "coefficient": -0.3296, "std_err": 0.0560, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 15', "prevalence_pct": 0.4, "coefficient": -0.0515, "std_err": 0.0909, "p_value": '0.5713'},
    {"factor": 'Diagnosis CCS 152', "prevalence_pct": 0.2, "coefficient": 0.1502, "std_err": 0.1026, "p_value": '0.1434'},
    {"factor": 'Diagnosis CCS 153', "prevalence_pct": 0.2, "coefficient": -0.0482, "std_err": 0.1009, "p_value": '0.6326'},
    {"factor": 'Diagnosis CCS 155', "prevalence_pct": 1.5, "coefficient": -0.4566, "std_err": 0.0608, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 157', "prevalence_pct": 0.4, "coefficient": -0.0390, "std_err": 0.0856, "p_value": '0.6488'},
    {"factor": 'Diagnosis CCS 159', "prevalence_pct": 0.8, "coefficient": 0.0377, "std_err": 0.0646, "p_value": '0.5594'},
    {"factor": 'Diagnosis CCS 160', "prevalence_pct": 0.3, "coefficient": -0.2652, "std_err": 0.1083, "p_value": '0.0144'},
    {"factor": 'Diagnosis CCS 161', "prevalence_pct": 0.4, "coefficient": -0.2929, "std_err": 0.0870, "p_value": '0.0008'},
    {"factor": 'Diagnosis CCS 162', "prevalence_pct": 0.3, "coefficient": 0.0470, "std_err": 0.0979, "p_value": '0.6311'},
    {"factor": 'Diagnosis CCS 163', "prevalence_pct": 0.2, "coefficient": -0.0193, "std_err": 0.1202, "p_value": '0.8722'},
    {"factor": 'Diagnosis CCS 164', "prevalence_pct": 0.3, "coefficient": -0.2599, "std_err": 0.0968, "p_value": '0.0072'},
    {"factor": 'Diagnosis CCS 17', "prevalence_pct": 0.4, "coefficient": -0.0919, "std_err": 0.0853, "p_value": '0.2812'},
    {"factor": 'Diagnosis CCS 170', "prevalence_pct": 0.0, "coefficient": -0.1725, "std_err": 0.2584, "p_value": '0.5043'},
    {"factor": 'Diagnosis CCS 18', "prevalence_pct": 0.3, "coefficient": -0.2463, "std_err": 0.1091, "p_value": '0.0240'},
    {"factor": 'Diagnosis CCS 19', "prevalence_pct": 1.1, "coefficient": -0.4830, "std_err": 0.0686, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 197', "prevalence_pct": 0.7, "coefficient": -0.4279, "std_err": 0.0733, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 199', "prevalence_pct": 0.5, "coefficient": -0.4840, "std_err": 0.0857, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 2', "prevalence_pct": 6.2, "coefficient": -0.2039, "std_err": 0.0460, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 201', "prevalence_pct": 1.0, "coefficient": -0.5103, "std_err": 0.0680, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 203', "prevalence_pct": 3.2, "coefficient": -0.6126, "std_err": 0.0534, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 205', "prevalence_pct": 3.8, "coefficient": -0.3702, "std_err": 0.0508, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 207', "prevalence_pct": 3.1, "coefficient": -0.4606, "std_err": 0.0522, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 209', "prevalence_pct": 0.5, "coefficient": -0.2800, "std_err": 0.0871, "p_value": '0.0013'},
    {"factor": 'Diagnosis CCS 211', "prevalence_pct": 0.3, "coefficient": -0.4965, "std_err": 0.1020, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 212', "prevalence_pct": 0.2, "coefficient": -0.6990, "std_err": 0.1490, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 213', "prevalence_pct": 0.1, "coefficient": -0.4536, "std_err": 0.1578, "p_value": '0.0040'},
    {"factor": 'Diagnosis CCS 225', "prevalence_pct": 0.2, "coefficient": -0.3305, "std_err": 0.1417, "p_value": '0.0197'},
    {"factor": 'Diagnosis CCS 226', "prevalence_pct": 13.5, "coefficient": -0.4909, "std_err": 0.0445, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 229', "prevalence_pct": 1.1, "coefficient": -0.5286, "std_err": 0.0689, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 230', "prevalence_pct": 3.1, "coefficient": -0.4716, "std_err": 0.0520, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 231', "prevalence_pct": 1.7, "coefficient": -0.3484, "std_err": 0.0581, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 233', "prevalence_pct": 0.5, "coefficient": -0.0735, "std_err": 0.0790, "p_value": '0.3522'},
    {"factor": 'Diagnosis CCS 234', "prevalence_pct": 0.2, "coefficient": -0.2920, "std_err": 0.1260, "p_value": '0.0204'},
    {"factor": 'Diagnosis CCS 237', "prevalence_pct": 7.9, "coefficient": -0.2401, "std_err": 0.0450, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 238', "prevalence_pct": 4.0, "coefficient": -0.2286, "std_err": 0.0480, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 248', "prevalence_pct": 0.5, "coefficient": -0.1072, "std_err": 0.0785, "p_value": '0.1718'},
    {"factor": 'Diagnosis CCS 25', "prevalence_pct": 0.2, "coefficient": -0.5484, "std_err": 0.1435, "p_value": '0.0001'},
    {"factor": 'Diagnosis CCS 257', "prevalence_pct": 0.2, "coefficient": -0.7945, "std_err": 0.1332, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 27', "prevalence_pct": 0.2, "coefficient": -0.7105, "std_err": 0.1504, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 29', "prevalence_pct": 0.1, "coefficient": -0.0975, "std_err": 0.1409, "p_value": '0.4889'},
    {"factor": 'Diagnosis CCS 32', "prevalence_pct": 0.7, "coefficient": 0.1711, "std_err": 0.0695, "p_value": '0.0138'},
    {"factor": 'Diagnosis CCS 33', "prevalence_pct": 0.4, "coefficient": -0.3482, "std_err": 0.0897, "p_value": '0.0001'},
    {"factor": 'Diagnosis CCS 35', "prevalence_pct": 0.3, "coefficient": -0.0893, "std_err": 0.1072, "p_value": '0.4045'},
    {"factor": 'Diagnosis CCS 42', "prevalence_pct": 1.5, "coefficient": -0.2953, "std_err": 0.0608, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 44', "prevalence_pct": 0.2, "coefficient": -0.1764, "std_err": 0.1296, "p_value": '0.1736'},
    {"factor": 'Diagnosis CCS 47', "prevalence_pct": 0.7, "coefficient": -0.0880, "std_err": 0.0715, "p_value": '0.2185'},
    {"factor": 'Diagnosis CCS 50', "prevalence_pct": 4.3, "coefficient": -0.2891, "std_err": 0.0482, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 58', "prevalence_pct": 0.2, "coefficient": -0.6196, "std_err": 0.1500, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 95', "prevalence_pct": 0.6, "coefficient": -0.2341, "std_err": 0.0773, "p_value": '0.0024'},
    {"factor": 'Diagnosis CCS 96', "prevalence_pct": 4.8, "coefficient": -0.3928, "std_err": 0.0474, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 99', "prevalence_pct": 1.2, "coefficient": 0.0000, "std_err": None, "p_value": 'N/A'},
    {"factor": 'Years over 65 (continuous)2', "prevalence_pct": None, "coefficient": 0.0021, "std_err": 0.0008, "p_value": '0.0054'},
    {"factor": 'Metastatic cancer and acute leukemia', "prevalence_pct": 7.2, "coefficient": 0.0709, "std_err": 0.0225, "p_value": '0.0016'},
    {"factor": 'Severe cancer', "prevalence_pct": 8.1, "coefficient": 0.0885, "std_err": 0.0193, "p_value": '<.0001'},
    {"factor": 'Other cancers', "prevalence_pct": 11.5, "coefficient": 0.0265, "std_err": 0.0169, "p_value": '0.1172'},
    {"factor": 'Severe hematological disorders', "prevalence_pct": 1.1, "coefficient": 0.1486, "std_err": 0.0447, "p_value": '0.0009'},
    {"factor": 'Coagulation defects and other specified hematological disorders', "prevalence_pct": 14.9, "coefficient": 0.0134, "std_err": 0.0145, "p_value": '0.3570'},
    {"factor": 'Iron deficiency or other/unspecified anemias and blood disease', "prevalence_pct": 49.2, "coefficient": 0.1195, "std_err": 0.0119, "p_value": '<.0001'},
    {"factor": 'End-stage liver disease; cirrhosis of liver', "prevalence_pct": 3.4, "coefficient": 0.2351, "std_err": 0.0261, "p_value": '<.0001'},
    {"factor": 'Pancreatic disease; peptic ulcer, hemorrhage, other specified gastrointestinal disorders', "prevalence_pct": 16.0, "coefficient": 0.0851, "std_err": 0.0146, "p_value": '<.0001'},
    {"factor": 'Dialysis status', "prevalence_pct": 4.5, "coefficient": 0.3438, "std_err": 0.0228, "p_value": '<.0001'},
    {"factor": 'Renal failure', "prevalence_pct": 48.9, "coefficient": 0.1033, "std_err": 0.0118, "p_value": '<.0001'},
    {"factor": 'Transplants', "prevalence_pct": 2.4, "coefficient": 0.0269, "std_err": 0.0325, "p_value": '0.4085'},
    {"factor": 'Severe infection', "prevalence_pct": 1.8, "coefficient": -0.0184, "std_err": 0.0373, "p_value": '0.6208'},
    {"factor": 'Other infectious diseases and pneumonias', "prevalence_pct": 29.5, "coefficient": 0.0226, "std_err": 0.0127, "p_value": '0.0761'},
    {"factor": 'Septicemia, sepsis, systemic inflammatory response syndrome/shock', "prevalence_pct": 16.9, "coefficient": 0.0011, "std_err": 0.0152, "p_value": '0.9413'},
    {"factor": 'Congestive heart failure', "prevalence_pct": 37.4, "coefficient": 0.1074, "std_err": 0.0123, "p_value": '<.0001'},
    {"factor": 'Coronary atherosclerosis or angina, cerebrovascular disease', "prevalence_pct": 61.8, "coefficient": 0.0751, "std_err": 0.0121, "p_value": '<.0001'},
    {"factor": 'Specified arrhythmias and other heart rhythm disorders', "prevalence_pct": 43.3, "coefficient": 0.0752, "std_err": 0.0114, "p_value": '<.0001'},
    {"factor": 'Cardiorespiratory failure and shock', "prevalence_pct": 22.7, "coefficient": 0.0446, "std_err": 0.0137, "p_value": '0.0011'},
    {"factor": 'Chronic obstructive pulmonary disease', "prevalence_pct": 23.2, "coefficient": 0.1289, "std_err": 0.0125, "p_value": '<.0001'},
    {"factor": 'Fibrosis of lung or other chronic lung disorders', "prevalence_pct": 3.5, "coefficient": 0.0766, "std_err": 0.0270, "p_value": '0.0045'},
    {"factor": 'Protein-calorie malnutrition', "prevalence_pct": 16.7, "coefficient": 0.1286, "std_err": 0.0140, "p_value": '<.0001'},
    {"factor": 'Other significant endocrine and metabolic disorders; disorders of fluid/electrolyte/acid- base balance', "prevalence_pct": 44.9, "coefficient": 0.0915, "std_err": 0.0118, "p_value": '<.0001'},
    {"factor": 'Rheumatoid arthritis and inflammatory connective tissue disease 8.5 0.0350 0.0185 0.0582 Diabetes mellitus (DM) or DM complications', "prevalence_pct": 39.8, "coefficient": 0.0798, "std_err": 0.0114, "p_value": '<.0001'},
    {"factor": 'Decubitus ulcer or chronic skin ulcer', "prevalence_pct": 13.8, "coefficient": 0.1138, "std_err": 0.0169, "p_value": '<.0001'},
    {"factor": 'Hemiplegia, paraplegia, paralysis, functional disability', "prevalence_pct": 12.6, "coefficient": -0.0384, "std_err": 0.0166, "p_value": '0.0203'},
    {"factor": 'Seizure disorders and convulsions', "prevalence_pct": 4.4, "coefficient": 0.0507, "std_err": 0.0246, "p_value": '0.0391'},
    {"factor": 'Respirator dependence/tracheostomy status', "prevalence_pct": 0.7, "coefficient": -0.0094, "std_err": 0.0565, "p_value": '0.8681'},
    {"factor": 'Drug/alcohol psychosis or dependence', "prevalence_pct": 3.6, "coefficient": -0.0095, "std_err": 0.0275, "p_value": '0.7297'},
    {"factor": 'Psychiatric comorbidity', "prevalence_pct": 25.7, "coefficient": 0.0446, "std_err": 0.0120, "p_value": '0.0002'},
    {"factor": 'Hip fracture/dislocation', "prevalence_pct": 7.0, "coefficient": -0.0765, "std_err": 0.0215, "p_value": '0.0004'},
    {"factor": 'History of COVID-19', "prevalence_pct": 11.7, "coefficient": 0.0043, "std_err": 0.0158, "p_value": '0.7857'},
]


# =============================================================================
# Table 3 — Cardiorespiratory cohort (47 rows)
# Source: PDF page 19 (doc page 3-9) through page 20 (doc page 3-10).
# =============================================================================

ACR_CARDIORESPIRATORY_COEFFICIENTS: list[dict] = [
    {"factor": 'Intercept', "prevalence_pct": None, "coefficient": -1.9505, "std_err": 0.0243, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 0001', "prevalence_pct": 0.0, "coefficient": -0.8462, "std_err": 0.4778, "p_value": '0.0766'},
    {"factor": 'Diagnosis CCS 103', "prevalence_pct": 6.3, "coefficient": -0.1513, "std_err": 0.0275, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 108', "prevalence_pct": 3.7, "coefficient": 0.1110, "std_err": 0.0313, "p_value": '0.0004'},
    {"factor": 'Diagnosis CCS 122', "prevalence_pct": 27.3, "coefficient": -0.0989, "std_err": 0.0175, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 125', "prevalence_pct": 1.2, "coefficient": -0.1502, "std_err": 0.0558, "p_value": '0.0071'},
    {"factor": 'Diagnosis CCS 127', "prevalence_pct": 12.4, "coefficient": 0.0558, "std_err": 0.0213, "p_value": '0.0087'},
    {"factor": 'Diagnosis CCS 128', "prevalence_pct": 1.0, "coefficient": 0.1180, "std_err": 0.0584, "p_value": '0.0432'},
    {"factor": 'Diagnosis CCS 129', "prevalence_pct": 9.1, "coefficient": -0.1339, "std_err": 0.0237, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 130', "prevalence_pct": 3.1, "coefficient": 0.1411, "std_err": 0.0333, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 131', "prevalence_pct": 14.6, "coefficient": 0.0328, "std_err": 0.0201, "p_value": '0.1029'},
    {"factor": 'Diagnosis CCS 132', "prevalence_pct": 0.6, "coefficient": -0.0559, "std_err": 0.0750, "p_value": '0.4557'},
    {"factor": 'Diagnosis CCS 133', "prevalence_pct": 2.7, "coefficient": -0.0028, "std_err": 0.0369, "p_value": '0.9392'},
    {"factor": 'Diagnosis CCS 999', "prevalence_pct": 18.2, "coefficient": 0.0000, "std_err": None, "p_value": 'N/A'},
    {"factor": 'Years over 65 (continuous)2', "prevalence_pct": None, "coefficient": -0.0084, "std_err": 0.0008, "p_value": '<.0001'},
    {"factor": 'Metastatic cancer and acute leukemia', "prevalence_pct": 6.4, "coefficient": 0.1814, "std_err": 0.0249, "p_value": '<.0001'},
    {"factor": 'Severe cancer', "prevalence_pct": 12.0, "coefficient": 0.1163, "std_err": 0.0181, "p_value": '<.0001'},
    {"factor": 'Other cancers', "prevalence_pct": 9.3, "coefficient": 0.0307, "std_err": 0.0199, "p_value": '0.1242'},
    {"factor": 'Severe hematological disorders', "prevalence_pct": 1.5, "coefficient": 0.1614, "std_err": 0.0442, "p_value": '0.0003'},
    {"factor": 'Coagulation defects and other specified hematological disorders', "prevalence_pct": 16.1, "coefficient": 0.0283, "std_err": 0.0154, "p_value": '0.0651'},
    {"factor": 'Iron deficiency or other/unspecified anemias and blood disease', "prevalence_pct": 52.3, "coefficient": 0.1502, "std_err": 0.0126, "p_value": '<.0001'},
    {"factor": 'End-stage liver disease; cirrhosis of liver', "prevalence_pct": 3.1, "coefficient": 0.1535, "std_err": 0.0301, "p_value": '<.0001'},
    {"factor": 'Pancreatic disease; peptic ulcer, hemorrhage, other specified gastrointestinal disorders', "prevalence_pct": 11.5, "coefficient": 0.1030, "std_err": 0.0172, "p_value": '<.0001'},
    {"factor": 'Dialysis status', "prevalence_pct": 3.9, "coefficient": 0.2567, "std_err": 0.0272, "p_value": '<.0001'},
    {"factor": 'Renal failure', "prevalence_pct": 52.7, "coefficient": 0.0847, "std_err": 0.0124, "p_value": '<.0001'},
    {"factor": 'Transplants', "prevalence_pct": 2.2, "coefficient": -0.0202, "std_err": 0.0378, "p_value": '0.5934'},
    {"factor": 'Severe infection', "prevalence_pct": 2.1, "coefficient": 0.0750, "std_err": 0.0369, "p_value": '0.0418'},
    {"factor": 'Other infectious diseases and pneumonias', "prevalence_pct": 49.1, "coefficient": 0.0701, "std_err": 0.0124, "p_value": '<.0001'},
    {"factor": 'Septicemia, sepsis, systemic inflammatory response syndrome/shock', "prevalence_pct": 17.9, "coefficient": 0.0768, "std_err": 0.0152, "p_value": '<.0001'},
    {"factor": 'Congestive heart failure', "prevalence_pct": 56.8, "coefficient": 0.1429, "std_err": 0.0130, "p_value": '<.0001'},
    {"factor": 'Coronary atherosclerosis or angina, cerebrovascular disease', "prevalence_pct": 67.3, "coefficient": 0.0483, "std_err": 0.0131, "p_value": '0.0002'},
    {"factor": 'Specified arrhythmias and other heart rhythm disorders', "prevalence_pct": 52.3, "coefficient": 0.1180, "std_err": 0.0122, "p_value": '<.0001'},
    {"factor": 'Cardiorespiratory failure and shock', "prevalence_pct": 71.0, "coefficient": 0.0956, "std_err": 0.0137, "p_value": '<.0001'},
    {"factor": 'Chronic obstructive pulmonary disease', "prevalence_pct": 49.9, "coefficient": 0.1477, "std_err": 0.0124, "p_value": '<.0001'},
    {"factor": 'Fibrosis of lung or other chronic lung disorders', "prevalence_pct": 11.3, "coefficient": 0.1089, "std_err": 0.0176, "p_value": '<.0001'},
    {"factor": 'Protein-calorie malnutrition', "prevalence_pct": 20.5, "coefficient": 0.0947, "std_err": 0.0142, "p_value": '<.0001'},
    {"factor": 'Other significant endocrine and metabolic disorders; disorders of fluid/electrolyte/acid- base balance', "prevalence_pct": 57.2, "coefficient": 0.1158, "std_err": 0.0125, "p_value": '<.0001'},
    {"factor": 'Rheumatoid arthritis and inflammatory connective tissue disease', "prevalence_pct": 8.8, "coefficient": 0.0316, "std_err": 0.0195, "p_value": '0.1063'},
    {"factor": 'Diabetes mellitus (DM) or DM complications', "prevalence_pct": 38.8, "coefficient": 0.0816, "std_err": 0.0120, "p_value": '<.0001'},
    {"factor": 'Decubitus ulcer or chronic skin ulcer', "prevalence_pct": 9.4, "coefficient": 0.1471, "std_err": 0.0188, "p_value": '<.0001'},
    {"factor": 'Hemiplegia, paraplegia, paralysis, functional disability', "prevalence_pct": 8.9, "coefficient": 0.0099, "std_err": 0.0200, "p_value": '0.6218'},
    {"factor": 'Seizure disorders and convulsions', "prevalence_pct": 5.7, "coefficient": 0.0528, "std_err": 0.0240, "p_value": '0.0276'},
    {"factor": 'Respirator dependence/tracheostomy status', "prevalence_pct": 1.0, "coefficient": 0.2082, "std_err": 0.0506, "p_value": '<.0001'},
    {"factor": 'Drug/alcohol psychosis or dependence', "prevalence_pct": 4.0, "coefficient": 0.1590, "std_err": 0.0270, "p_value": '<.0001'},
    {"factor": 'Psychiatric comorbidity', "prevalence_pct": 32.0, "coefficient": 0.0752, "std_err": 0.0123, "p_value": '<.0001'},
    {"factor": 'Hip fracture/dislocation', "prevalence_pct": 3.4, "coefficient": -0.1278, "std_err": 0.0318, "p_value": '<.0001'},
    {"factor": 'History of COVID-19', "prevalence_pct": 16.9, "coefficient": 0.0379, "std_err": 0.0148, "p_value": '0.0103'},
]


# =============================================================================
# Table 4 — Cardiovascular cohort (47 rows)
# Source: PDF page 21 (doc page 3-11).
# =============================================================================

ACR_CARDIOVASCULAR_COEFFICIENTS: list[dict] = [
    {"factor": 'Intercept', "prevalence_pct": None, "coefficient": -1.7262, "std_err": 0.0615, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 0001', "prevalence_pct": 0.5, "coefficient": -0.1976, "std_err": 0.1232, "p_value": '0.1087'},
    {"factor": 'Diagnosis CCS 100', "prevalence_pct": 19.1, "coefficient": -0.1453, "std_err": 0.0577, "p_value": '0.0118'},
    {"factor": 'Diagnosis CCS 101', "prevalence_pct": 8.0, "coefficient": -0.2957, "std_err": 0.0617, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 102', "prevalence_pct": 4.2, "coefficient": -0.5192, "std_err": 0.0681, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 104', "prevalence_pct": 0.6, "coefficient": -0.3554, "std_err": 0.1143, "p_value": '0.0019'},
    {"factor": 'Diagnosis CCS 105', "prevalence_pct": 4.8, "coefficient": -0.5230, "std_err": 0.0671, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 106', "prevalence_pct": 44.6, "coefficient": -0.3481, "std_err": 0.0563, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 107', "prevalence_pct": 0.7, "coefficient": -0.6081, "std_err": 0.1157, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 114', "prevalence_pct": 2.8, "coefficient": -0.2319, "std_err": 0.0711, "p_value": '0.0011'},
    {"factor": 'Diagnosis CCS 115', "prevalence_pct": 0.7, "coefficient": -0.4844, "std_err": 0.1111, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 117', "prevalence_pct": 10.1, "coefficient": -0.3667, "std_err": 0.0602, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 96', "prevalence_pct": 2.3, "coefficient": -0.3596, "std_err": 0.0748, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 97', "prevalence_pct": 1.6, "coefficient": 0.0000, "std_err": None, "p_value": 'N/A'},
    {"factor": 'Years over 65 (continuous)2', "prevalence_pct": None, "coefficient": 0.0054, "std_err": 0.0011, "p_value": '<.0001'},
    {"factor": 'Metastatic cancer and acute leukemia', "prevalence_pct": 3.9, "coefficient": 0.1296, "std_err": 0.0414, "p_value": '0.0018'},
    {"factor": 'Severe cancer', "prevalence_pct": 7.1, "coefficient": 0.1962, "std_err": 0.0297, "p_value": '<.0001'},
    {"factor": 'Other cancers', "prevalence_pct": 9.3, "coefficient": 0.0163, "std_err": 0.0277, "p_value": '0.5575'},
    {"factor": 'Severe hematological disorders', "prevalence_pct": 1.3, "coefficient": 0.0721, "std_err": 0.0646, "p_value": '0.2644'},
    {"factor": 'Coagulation defects and other specified hematological disorders', "prevalence_pct": 16.2, "coefficient": 0.0589, "std_err": 0.0211, "p_value": '0.0052'},
    {"factor": 'Iron deficiency or other/unspecified anemias and blood disease', "prevalence_pct": 49.7, "coefficient": 0.1521, "std_err": 0.0176, "p_value": '<.0001'},
    {"factor": 'End-stage liver disease; cirrhosis of liver', "prevalence_pct": 3.3, "coefficient": 0.0823, "std_err": 0.0419, "p_value": '0.0493'},
    {"factor": 'Pancreatic disease; peptic ulcer, hemorrhage, other specified gastrointestinal disorders', "prevalence_pct": 13.0, "coefficient": 0.0712, "std_err": 0.0232, "p_value": '0.0022'},
    {"factor": 'Dialysis status', "prevalence_pct": 5.4, "coefficient": 0.4063, "std_err": 0.0319, "p_value": '<.0001'},
    {"factor": 'Renal failure', "prevalence_pct": 58.0, "coefficient": 0.0447, "std_err": 0.0174, "p_value": '0.0103'},
    {"factor": 'Transplants', "prevalence_pct": 2.1, "coefficient": -0.1252, "std_err": 0.0556, "p_value": '0.0243'},
    {"factor": 'Severe infection', "prevalence_pct": 1.1, "coefficient": 0.1692, "std_err": 0.0663, "p_value": '0.0108'},
    {"factor": 'Other infectious diseases and pneumonias', "prevalence_pct": 28.1, "coefficient": 0.1199, "std_err": 0.0188, "p_value": '<.0001'},
    {"factor": 'Septicemia, sepsis, systemic inflammatory response syndrome/shock', "prevalence_pct": 12.4, "coefficient": -0.0172, "std_err": 0.0244, "p_value": '0.4807'},
    {"factor": 'Congestive heart failure', "prevalence_pct": 63.0, "coefficient": 0.1394, "std_err": 0.0179, "p_value": '<.0001'},
    {"factor": 'Coronary atherosclerosis or angina, cerebrovascular disease', "prevalence_pct": 79.8, "coefficient": 0.0298, "std_err": 0.0210, "p_value": '0.1568'},
    {"factor": 'Specified arrhythmias and other heart rhythm disorders', "prevalence_pct": 67.5, "coefficient": -0.0659, "std_err": 0.0176, "p_value": '0.0002'},
    {"factor": 'Cardiorespiratory failure and shock', "prevalence_pct": 31.6, "coefficient": 0.0915, "std_err": 0.0185, "p_value": '<.0001'},
    {"factor": 'Chronic obstructive pulmonary disease', "prevalence_pct": 28.9, "coefficient": 0.1175, "std_err": 0.0175, "p_value": '<.0001'},
    {"factor": 'Fibrosis of lung or other chronic lung disorders', "prevalence_pct": 4.9, "coefficient": 0.1008, "std_err": 0.0343, "p_value": '0.0033'},
    {"factor": 'Protein-calorie malnutrition', "prevalence_pct": 11.8, "coefficient": 0.0896, "std_err": 0.0238, "p_value": '0.0002'},
    {"factor": 'Other significant endocrine and metabolic disorders; disorders of fluid/electrolyte/acid- base balance', "prevalence_pct": 50.0, "coefficient": 0.1266, "std_err": 0.0173, "p_value": '<.0001'},
    {"factor": 'Rheumatoid arthritis and inflammatory connective tissue disease', "prevalence_pct": 8.3, "coefficient": -0.0070, "std_err": 0.0280, "p_value": '0.8015'},
    {"factor": 'Diabetes mellitus (DM) or DM complications', "prevalence_pct": 44.5, "coefficient": 0.0426, "std_err": 0.0164, "p_value": '0.0094'},
    {"factor": 'Decubitus ulcer or chronic skin ulcer', "prevalence_pct": 6.5, "coefficient": 0.1517, "std_err": 0.0300, "p_value": '<.0001'},
    {"factor": 'Hemiplegia, paraplegia, paralysis, functional disability', "prevalence_pct": 8.2, "coefficient": -0.0477, "std_err": 0.0286, "p_value": '0.0951'},
    {"factor": 'Seizure disorders and convulsions', "prevalence_pct": 4.6, "coefficient": 0.1074, "std_err": 0.0360, "p_value": '0.0028'},
    {"factor": 'Respirator dependence/tracheostomy status', "prevalence_pct": 0.3, "coefficient": 0.2272, "std_err": 0.1255, "p_value": '0.0703'},
    {"factor": 'Drug/alcohol psychosis or dependence', "prevalence_pct": 3.2, "coefficient": 0.1265, "std_err": 0.0421, "p_value": '0.0027'},
    {"factor": 'Psychiatric comorbidity', "prevalence_pct": 26.0, "coefficient": 0.1138, "std_err": 0.0177, "p_value": '<.0001'},
    {"factor": 'Hip fracture/dislocation', "prevalence_pct": 2.8, "coefficient": -0.2027, "std_err": 0.0486, "p_value": '<.0001'},
    {"factor": 'History of COVID-19', "prevalence_pct": 13.6, "coefficient": -0.0204, "std_err": 0.0225, "p_value": '0.3647'},
]


# =============================================================================
# Table 5 — Neurology cohort (44 rows)
# Source: PDF page 22 (doc page 3-12) through page 24 (doc page 3-14).
# =============================================================================

ACR_NEUROLOGY_COEFFICIENTS: list[dict] = [
    {"factor": 'Intercept', "prevalence_pct": None, "coefficient": -1.9696, "std_err": 0.0357, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 0001', "prevalence_pct": 1.9, "coefficient": -0.0187, "std_err": 0.0689, "p_value": '0.7856'},
    {"factor": 'Diagnosis CCS 109', "prevalence_pct": 43.6, "coefficient": -0.1290, "std_err": 0.0260, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 110', "prevalence_pct": 1.2, "coefficient": -0.3147, "std_err": 0.0934, "p_value": '0.0008'},
    {"factor": 'Diagnosis CCS 111', "prevalence_pct": 0.6, "coefficient": -0.3033, "std_err": 0.1219, "p_value": '0.0128'},
    {"factor": 'Diagnosis CCS 112', "prevalence_pct": 6.7, "coefficient": -0.1972, "std_err": 0.0423, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 113', "prevalence_pct": 2.3, "coefficient": -0.1437, "std_err": 0.0649, "p_value": '0.0267'},
    {"factor": 'Diagnosis CCS 233', "prevalence_pct": 9.0, "coefficient": 0.0357, "std_err": 0.0355, "p_value": '0.3145'},
    {"factor": 'Diagnosis CCS 79', "prevalence_pct": 2.5, "coefficient": -0.1512, "std_err": 0.0633, "p_value": '0.0169'},
    {"factor": 'Diagnosis CCS 81', "prevalence_pct": 1.7, "coefficient": -0.0773, "std_err": 0.0703, "p_value": '0.2711'},
    {"factor": 'Diagnosis CCS 83', "prevalence_pct": 10.4, "coefficient": -0.1728, "std_err": 0.0355, "p_value": '<.0001'},
    {"factor": 'Diagnosis CCS 95', "prevalence_pct": 20.1, "coefficient": 0.0000, "std_err": None, "p_value": 'N/A'},
    {"factor": 'Years over 65 (continuous)2', "prevalence_pct": None, "coefficient": -0.0030, "std_err": 0.0013, "p_value": '0.0188'},
    {"factor": 'Metastatic cancer and acute leukemia', "prevalence_pct": 6.5, "coefficient": 0.1654, "std_err": 0.0406, "p_value": '<.0001'},
    {"factor": 'Severe cancer', "prevalence_pct": 8.0, "coefficient": 0.1256, "std_err": 0.0347, "p_value": '0.0003'},
    {"factor": 'Other cancers', "prevalence_pct": 10.4, "coefficient": 0.0296, "std_err": 0.0312, "p_value": '0.3423'},
    {"factor": 'Severe hematological disorders', "prevalence_pct": 1.0, "coefficient": 0.1271, "std_err": 0.0822, "p_value": '0.1220'},
    {"factor": 'Coagulation defects and other specified hematological disorders', "prevalence_pct": 15.9, "coefficient": 0.0980, "std_err": 0.0248, "p_value": '<.0001'},
    {"factor": 'Iron deficiency or other/unspecified anemias and blood disease', "prevalence_pct": 42.6, "coefficient": 0.1307, "std_err": 0.0209, "p_value": '<.0001'},
    {"factor": 'End-stage liver disease; cirrhosis of liver', "prevalence_pct": 2.7, "coefficient": 0.2351, "std_err": 0.0501, "p_value": '<.0001'},
    {"factor": 'Pancreatic disease; peptic ulcer, hemorrhage, other specified gastrointestinal disorders', "prevalence_pct": 9.4, "coefficient": 0.1731, "std_err": 0.0298, "p_value": '<.0001'},
    {"factor": 'Dialysis status', "prevalence_pct": 3.4, "coefficient": 0.2961, "std_err": 0.0446, "p_value": '<.0001'},
    {"factor": 'Renal failure', "prevalence_pct": 46.5, "coefficient": 0.1220, "std_err": 0.0204, "p_value": '<.0001'},
    {"factor": 'Transplants', "prevalence_pct": 1.5, "coefficient": 0.2161, "std_err": 0.0661, "p_value": '0.0011'},
    {"factor": 'Severe infection', "prevalence_pct": 1.9, "coefficient": 0.1907, "std_err": 0.0607, "p_value": '0.0017'},
    {"factor": 'Other infectious diseases and pneumonias', "prevalence_pct": 29.0, "coefficient": 0.1129, "std_err": 0.0222, "p_value": '<.0001'},
    {"factor": 'Septicemia, sepsis, systemic inflammatory response syndrome/shock', "prevalence_pct": 12.1, "coefficient": 0.0113, "std_err": 0.0288, "p_value": '0.6938'},
    {"factor": 'Congestive heart failure', "prevalence_pct": 34.9, "coefficient": 0.1310, "std_err": 0.0214, "p_value": '<.0001'},
    {"factor": 'Coronary atherosclerosis or angina, cerebrovascular disease', "prevalence_pct": 70.6, "coefficient": 0.0503, "std_err": 0.0219, "p_value": '0.0215'},
    {"factor": 'Specified arrhythmias and other heart rhythm disorders', "prevalence_pct": 48.1, "coefficient": 0.0074, "std_err": 0.0198, "p_value": '0.7097'},
    {"factor": 'Cardiorespiratory failure and shock', "prevalence_pct": 22.5, "coefficient": 0.0321, "std_err": 0.0241, "p_value": '0.1831'},
    {"factor": 'Chronic obstructive pulmonary disease', "prevalence_pct": 19.9, "coefficient": 0.1042, "std_err": 0.0231, "p_value": '<.0001'},
    {"factor": 'Fibrosis of lung or other chronic lung disorders', "prevalence_pct": 3.0, "coefficient": -0.0643, "std_err": 0.0520, "p_value": '0.2166'},
    {"factor": 'Protein-calorie malnutrition', "prevalence_pct": 16.1, "coefficient": 0.0800, "std_err": 0.0250, "p_value": '0.0014'},
    {"factor": 'Other significant endocrine and metabolic disorders; disorders of fluid/electrolyte/acid- base balance', "prevalence_pct": 49.4, "coefficient": 0.0990, "std_err": 0.0206, "p_value": '<.0001'},
    {"factor": 'Rheumatoid arthritis and inflammatory connective tissue disease', "prevalence_pct": 6.6, "coefficient": 0.0643, "std_err": 0.0357, "p_value": '0.0713'},
    {"factor": 'Diabetes mellitus (DM) or DM complications 41.1 0.1168 0.0194 <.0001 Decubitus ulcer or chronic skin ulcer', "prevalence_pct": 7.2, "coefficient": 0.1096, "std_err": 0.0336, "p_value": '0.0011'},
    {"factor": 'Hemiplegia, paraplegia, paralysis, functional disability', "prevalence_pct": 37.2, "coefficient": -0.1347, "std_err": 0.0211, "p_value": '<.0001'},
    {"factor": 'Seizure disorders and convulsions', "prevalence_pct": 14.2, "coefficient": 0.0729, "std_err": 0.0272, "p_value": '0.0073'},
    {"factor": 'Respirator dependence/tracheostomy status', "prevalence_pct": 0.5, "coefficient": 0.2695, "std_err": 0.1131, "p_value": '0.0172'},
    {"factor": 'Drug/alcohol psychosis or dependence', "prevalence_pct": 3.6, "coefficient": 0.0372, "std_err": 0.0469, "p_value": '0.4278'},
    {"factor": 'Psychiatric comorbidity', "prevalence_pct": 27.7, "coefficient": 0.0289, "std_err": 0.0208, "p_value": '0.1650'},
    {"factor": 'Hip fracture/dislocation', "prevalence_pct": 3.7, "coefficient": -0.2055, "std_err": 0.0497, "p_value": '<.0001'},
    {"factor": 'History of COVID-19', "prevalence_pct": 11.9, "coefficient": 0.0274, "std_err": 0.0277, "p_value": '0.3215'},
]




# =============================================================================
# Invariants
# =============================================================================

EXPECTED_COUNTS: dict[str, int] = {
    "medicine": 121,
    "surgery_gynecology": 107,
    "cardiorespiratory": 47,
    "cardiovascular": 47,
    "neurology": 44,
}

TOTAL_COEFFICIENTS = sum(EXPECTED_COUNTS.values())
assert TOTAL_COEFFICIENTS == 366


# =============================================================================
# Convenience export
# =============================================================================

ACR_COEFFICIENTS_BY_COHORT: dict[str, list[dict]] = {
    "medicine": ACR_MEDICINE_COEFFICIENTS,
    "surgery_gynecology": ACR_SURGERY_GYNECOLOGY_COEFFICIENTS,
    "cardiorespiratory": ACR_CARDIORESPIRATORY_COEFFICIENTS,
    "cardiovascular": ACR_CARDIOVASCULAR_COEFFICIENTS,
    "neurology": ACR_NEUROLOGY_COEFFICIENTS,
}
