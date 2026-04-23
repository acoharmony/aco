# © 2025 HarmonyCares
# All rights reserved.

"""
CMMI-HCC Concurrent Risk Adjustment Model — Relative Factors (coefficients).

These coefficients are the empirical regression outputs CMMI publishes for
the concurrent risk-adjustment model applied to High-Needs Population ACO
beneficiaries in ACO REACH. The model is defined in PA Appendix B:

    "CMMI-HCC Concurrent Risk Adjustment Model" means a method for
    measuring the health risk of a population with a risk score to
    reflect the predicted expenditures of that population. The CMMI-HCC
    Concurrent Risk Adjustment Model has a concurrent model design,
    which means that risk scores are calculated using diagnoses recorded
    on claims with dates of service during the calendar year in which
    the risk scores are used for payment purposes. The CMMI-HCC
    Concurrent Risk Adjustment Model can be applied to Aged & Disabled
    (A&D) Beneficiaries; there is a non-End-Stage Renal Disease (ESRD)
    segment, but no ESRD segment.
        — $BRONZE/ACO_REACH_PY2026_AR_PA_2023_Starters_508.txt, line 4314

Provenance
----------

CMS does not distribute these coefficients in machine-readable form. The
authoritative published source is:

    ACO REACH and Kidney Care Choices Models PY2023 Risk Adjustment
    Rev. 1.1, Appendix B: Concurrent Risk Adjustment Relative Factors
    (Table B-1. CMMI-HCC Concurrent Risk Adjustment Model Relative
    Factors).

The bronze-layer PDF is the binding source:
    ``$BRONZE/PY2023 ACO REACH KCC Risk Adjustment.pdf`` (51 pages;
    Appendix B spans PDF pages 38–41, doc pages 37–40).

The text extraction at ``$BRONZE/PY2023 ACO REACH KCC Risk Adjustment.txt``
is a convenience mirror; it contains the same data but with noisier
layout. Values below were extracted programmatically from the PDF (not
the txt file) with a parser that:

    1. Concatenates PDF pages 38 through 41 (``pypdf.extract_text``).
    2. Drops page furniture (headers, page numbers, the NOTES block).
    3. Walks the remaining lines, recognising five segment headers —
       "Age/Sex Cells", "HCCs", "Post-Kidney Transplant Indicators",
       "Count of HCCs in the Model", "HCC Interactions with Age < 65" —
       and treating everything between them as either a single-line
       record ``CODE  DESCRIPTION  FLOAT`` or a wrapped record where the
       description flows onto one or more continuation lines before the
       terminating float.
    4. Emits ``(segment, code, description, factor)`` 4-tuples.

The parser's output totals match Appendix B's own footnote (PA-txt line
1737, PDF page 41):

    "There are 86 V24 CMS-HCCs, among which 85 CMS-HCCs are used in
    this risk adjustment model. (HCC134 Dialysis Status is excluded.)
    However, given the CMS-HCC groups, the effective number of CMS-HCCs
    is 82."

    24 age/sex + 85 HCC + 4 post-kidney + 11 payment-HCC-count + 4
    HCC-age<65 interactions = 128.

Scaling: the Appendix B notes (PDF page 40) state

    "Relative factors are calculated by dividing each coefficient
    estimate by average spending in our 2018 Medicare FFS concurrent
    modeling sample, $10,717.60. For presentation purposes, we round the
    relative factors to 4 decimal places."

So the values below are already normalised by average spending; a raw
risk score is the sum of applicable relative factors, and downstream
scoring applies the ACO-level normalisation factor and the Coding
Intensity Factor. That math lives in ``_hcc_cmmi_concurrent``, not here.

Why a Python dict literal, not a CSV
------------------------------------

The source is a PDF, not a machine table. Transcribing into a CSV adds
a second file format with no added safety; a dict literal gets the same
code-review treatment as the rest of the module and the provenance note
lives right here.

When CMMI publishes revised coefficients for a later PY (expected
annually per PA Section IV.B.2, PA-txt line 3791), add a new
``CMMI_CONCURRENT_<YEAR>_*`` block below with its own PDF cite.
"""

from __future__ import annotations


# =============================================================================
# CMMI-HCC Concurrent — 2023 calibration (PY2023 Risk Adjustment Rev. 1.1)
# Source: PY2023 ACO REACH KCC Risk Adjustment.pdf Appendix B Table B-1.
# =============================================================================

# -----------------------------------------------------------------------------
# Age/Sex cells — 24 cells.
# Keys match hccinfhir's age-sex cell convention: {F|M}{age_lo}_{age_hi|GT}.
# -----------------------------------------------------------------------------
CMMI_CONCURRENT_2023_AGE_SEX: dict[str, float] = {
    "F0_34":  0.1559,  # Age range 0-34, Female
    "F35_44": 0.1559,  # Age range 35-44, Female
    "F45_54": 0.1559,  # Age range 45-54, Female
    "F55_59": 0.1559,  # Age range 55-59, Female
    "F60_64": 0.1559,  # Age range 60-64, Female
    "F65_69": 0.1949,  # Age range 65-69, Female
    "F70_74": 0.1949,  # Age range 70-74, Female
    "F75_79": 0.1949,  # Age range 75-79, Female
    "F80_84": 0.1949,  # Age range 80-84, Female
    "F85_89": 0.1949,  # Age range 85-89, Female
    "F90_94": 0.2512,  # Age range 90-94, Female
    "F95_GT": 0.3532,  # Age range 95+, Female
    "M0_34":  0.0559,  # Age range 0-34, Male
    "M35_44": 0.0559,  # Age range 35-44, Male
    "M45_54": 0.0559,  # Age range 45-54, Male
    "M55_59": 0.0559,  # Age range 55-59, Male
    "M60_64": 0.0559,  # Age range 60-64, Male
    "M65_69": 0.1340,  # Age range 65-69, Male
    "M70_74": 0.1340,  # Age range 70-74, Male
    "M75_79": 0.1340,  # Age range 75-79, Male
    "M80_84": 0.1340,  # Age range 80-84, Male
    "M85_89": 0.1340,  # Age range 85-89, Male
    "M90_94": 0.1340,  # Age range 90-94, Male
    "M95_GT": 0.2279,  # Age range 95+, Male
}


# -----------------------------------------------------------------------------
# HCC coefficients — 85 HCCs (HCC134 Dialysis Status is deliberately absent;
# see the Modified Hierarchies note at PDF page 41).
#
# HCC groups (per the Appendix B notes, PDF page 41): HCCs that share a
# regression coefficient (e.g., 18 & 19 are both "Diabetes with/without
# Complications" and carry 0.0555 each) — CMS assigns the same relative
# factor to each member of the group. Those equal values below are not a
# parser artefact; they reflect the group structure.
# -----------------------------------------------------------------------------
CMMI_CONCURRENT_2023_HCC: dict[str, float] = {
    "1":   0.2847,  # HIV/AIDS
    "2":   1.1030,  # Septicemia, Sepsis, Systemic Inflammatory Response Syndrome/Shock
    "6":   0.9210,  # Opportunistic Infections
    "8":   2.7247,  # Metastatic Cancer and Acute Leukemia
    "9":   0.8743,  # Lung and Other Severe Cancers
    "10":  0.6678,  # Lymphoma and Other Cancers
    "11":  0.2083,  # Colorectal, Bladder, and Other Cancers
    "12":  0.2083,  # Breast, Prostate, and Other Cancers and Tumors
    "17":  0.4229,  # Diabetes with Acute Complications
    "18":  0.0555,  # Diabetes with Chronic Complications
    "19":  0.0555,  # Diabetes without Complication
    "21":  1.5099,  # Protein-Calorie Malnutrition
    "22":  0.1876,  # Morbid Obesity
    "23":  0.1428,  # Other Significant Endocrine and Metabolic Disorders
    "27":  0.5031,  # End-Stage Liver Disease
    "28":  0.0660,  # Cirrhosis of Liver
    "29":  0.0660,  # Chronic Hepatitis
    "33":  1.0700,  # Intestinal Obstruction/Perforation
    "34":  0.2739,  # Chronic Pancreatitis
    "35":  0.2258,  # Inflammatory Bowel Disease
    "39":  0.9684,  # Bone/Joint/Muscle Infections/Necrosis
    "40":  0.2462,  # Rheumatoid Arthritis and Inflammatory Connective Tissue Disease
    "46":  0.9257,  # Severe Hematological Disorders
    "47":  0.9672,  # Disorders of Immunity
    "48":  0.3814,  # Coagulation Defects and Other Specified Hematological Disorders
    "51":  0.3057,  # Dementia With Complications
    "52":  0.3057,  # Dementia Without Complication
    "54":  0.7220,  # Substance Use with Psychotic Complications
    "55":  0.2926,  # Substance Use Disorder, Moderate/Severe, or Substance Use with Complications
    "56":  0.2926,  # Substance Use Disorder, Mild, Except Alcohol and Cannabis
    "57":  0.5725,  # Schizophrenia
    "58":  0.5725,  # Reactive and Unspecified Psychosis
    "59":  0.1677,  # Major Depressive, Bipolar, and Paranoid Disorders
    "60":  0.1677,  # Personality Disorders
    "70":  0.7435,  # Quadriplegia
    "71":  0.7435,  # Paraplegia
    "72":  0.7435,  # Spinal Cord Disorders/Injuries
    "73":  0.8043,  # Amyotrophic Lateral Sclerosis and Other Motor Neuron Disease
    "74":  0.0000,  # Cerebral Palsy
    "75":  0.5403,  # Myasthenia Gravis/Myoneural Disorders and Guillain-Barre Syndrome/Inflammatory and Toxic Neuropathy
    "76":  0.1906,  # Muscular Dystrophy
    "77":  0.5095,  # Multiple Sclerosis
    "78":  0.2778,  # Parkinson's and Huntington's Diseases
    "79":  0.1260,  # Seizure Disorders and Convulsions
    "80":  1.5190,  # Coma, Brain Compression/Anoxic Damage
    "82":  4.4570,  # Respirator Dependence/Tracheostomy Status
    "83":  1.6367,  # Respiratory Arrest
    "84":  0.9949,  # Cardio-Respiratory Failure and Shock
    "85":  0.3126,  # Congestive Heart Failure
    "86":  0.9650,  # Acute Myocardial Infarction
    "87":  0.6713,  # Unstable Angina and Other Acute Ischemic Heart Disease
    "88":  0.1678,  # Angina Pectoris
    "96":  0.2539,  # Specified Heart Arrhythmias
    "99":  1.0540,  # Intracranial Hemorrhage
    "100": 0.2868,  # Ischemic or Unspecified Stroke
    "103": 0.7026,  # Hemiplegia/Hemiparesis
    "104": 0.4081,  # Monoplegia, Other Paralytic Syndromes
    "106": 1.5502,  # Atherosclerosis of the Extremities with Ulceration or Gangrene
    "107": 0.5992,  # Vascular Disease with Complications
    "108": 0.1732,  # Vascular Disease
    "110": 0.5460,  # Cystic Fibrosis
    "111": 0.0762,  # Chronic Obstructive Pulmonary Disease
    "112": 0.0762,  # Fibrosis of Lung and Other Chronic Lung Disorders
    "114": 1.0537,  # Aspiration and Specified Bacterial Pneumonias
    "115": 0.1374,  # Pneumococcal Pneumonia, Empyema, Lung Abscess
    "122": 0.0356,  # Proliferative Diabetic Retinopathy and Vitreous Hemorrhage
    "124": 0.3653,  # Exudative Macular Degeneration
    "135": 0.8558,  # Acute Renal Failure
    "136": 0.1387,  # Chronic Kidney Disease, Stage 5
    "137": 0.1387,  # Chronic Kidney Disease, Severe (Stage 4)
    "138": 0.0000,  # Chronic Kidney Disease, Moderate (Stage 3)
    "157": 1.8170,  # Pressure Ulcer of Skin with Necrosis Through to Muscle, Tendon, or Bone
    "158": 1.1260,  # Pressure Ulcer of Skin with Full Thickness Skin Loss
    "159": 0.6845,  # Pressure Ulcer of Skin with Partial Thickness Skin Loss
    "161": 0.1049,  # Chronic Ulcer of Skin, Except Pressure
    "162": 1.7078,  # Severe Skin Burn or Condition
    "166": 1.5190,  # Severe Head Injury
    "167": 0.3867,  # Major Head Injury
    "169": 0.5770,  # Vertebral Fractures without Spinal Cord Injury
    "170": 1.8075,  # Hip Fracture/Dislocation
    "173": 1.0607,  # Traumatic Amputations and Complications
    "176": 1.3937,  # Complications of Specified Implanted Device or Graft
    "186": 1.5373,  # Major Organ Transplant or Replacement Status
    "188": 0.7851,  # Artificial Openings for Feeding or Elimination
    "189": 0.1076,  # Amputation Status, Lower Limb/Amputation Complications
}


# -----------------------------------------------------------------------------
# Post-Kidney Transplant indicators — 4 indicators keyed on graft age and
# beneficiary age. A kidney-transplant beneficiary qualifies for one and
# only one of these (the rest are zero).
#
# Note: the published PDF's fourth row reads "Age >= 65 and months
# post-graft", missing the "10+". The surrounding rows (1.9729 for Age<65
# 4–9 months, 0.1835 for Age<65 10+ months, 2.3938 for Age>=65 4–9 months)
# make clear the fourth row is "Age >= 65 and 10+ months post-graft"; the
# abbreviation is a typo in the CMS document. The canonical label is
# restored here.
# -----------------------------------------------------------------------------
CMMI_CONCURRENT_2023_POST_KIDNEY_TRANSPLANT: dict[str, float] = {
    "Age <65 and 4-9 months post-graft":   1.9729,
    "Age <65 and 10+ months post-graft":   0.1835,
    "Age >=65 and 4-9 months post-graft":  2.3938,
    "Age >=65 and 10+ months post-graft":  0.2678,
}


# -----------------------------------------------------------------------------
# Count-of-HCCs interaction — an additional coefficient that depends on how
# many payment HCCs the beneficiary has. Counts < 5 contribute zero; for
# counts ≥ 15 use the ``>=15`` entry.
# -----------------------------------------------------------------------------
CMMI_CONCURRENT_2023_PAYMENT_HCC_COUNT: dict[str, float] = {
    "=5":   0.0433,
    "=6":   0.1425,
    "=7":   0.2854,
    "=8":   0.4763,
    "=9":   0.7227,
    "=10":  1.0152,
    "=11":  1.4179,
    "=12":  1.9065,
    "=13":  2.4376,
    "=14":  3.0497,
    ">=15": 5.2582,
}


# -----------------------------------------------------------------------------
# HCC × Age < 65 interactions — additional coefficients that apply when the
# beneficiary is under 65 AND has the listed HCC. Keys are HCC numbers.
# -----------------------------------------------------------------------------
CMMI_CONCURRENT_2023_HCC_AGE_LT_65_INTERACTION: dict[str, float] = {
    "46":  2.5608,  # Severe Hematological Disorders
    "110": 1.2052,  # Cystic Fibrosis
    "136": 0.4535,  # Chronic Kidney Disease, Stage 5 (mod hierarchy)
    "137": 0.4535,  # Chronic Kidney Disease, Severe (Stage 4) (mod hierarchy)
}


# =============================================================================
# Invariants — these counts must hold for any calibration year. The
# companion test module asserts them against the dicts above.
# =============================================================================

EXPECTED_COUNTS: dict[str, int] = {
    "age_sex": 24,
    "hcc": 85,                         # per Appendix B footnote, PDF page 41
    "post_kidney_transplant": 4,
    "payment_hcc_count": 11,
    "hcc_age_lt_65_interaction": 4,
}

TOTAL_COEFFICIENTS = (
    EXPECTED_COUNTS["age_sex"]
    + EXPECTED_COUNTS["hcc"]
    + EXPECTED_COUNTS["post_kidney_transplant"]
    + EXPECTED_COUNTS["payment_hcc_count"]
    + EXPECTED_COUNTS["hcc_age_lt_65_interaction"]
)
assert TOTAL_COEFFICIENTS == 128  # sanity: matches the Appendix B table length


# =============================================================================
# Convenience export: the full 2023 calibration as a single dict of dicts.
# Downstream drivers should reach for this map rather than importing the
# five dicts separately.
# =============================================================================

CMMI_CONCURRENT_2023: dict[str, dict[str, float]] = {
    "age_sex": CMMI_CONCURRENT_2023_AGE_SEX,
    "hcc": CMMI_CONCURRENT_2023_HCC,
    "post_kidney_transplant": CMMI_CONCURRENT_2023_POST_KIDNEY_TRANSPLANT,
    "payment_hcc_count": CMMI_CONCURRENT_2023_PAYMENT_HCC_COUNT,
    "hcc_age_lt_65_interaction": CMMI_CONCURRENT_2023_HCC_AGE_LT_65_INTERACTION,
}
