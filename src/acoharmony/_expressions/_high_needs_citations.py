# © 2025 HarmonyCares
# All rights reserved.

"""
Verbatim citations from the ACO REACH Participation Agreement (PY2026 AR).

Single source of truth for every quotation used in docstrings across the
high-needs and HCC risk-score modules. Keeping the text here — rather than
paraphrased inline in each module — means:

    - future code readers see the exact CMS language, not our gloss
    - line references in ``$BRONZE/ACO_REACH_PY2026_AR_PA_2023_Starters_508.txt``
      are audited once, here, and reused everywhere
    - when CMS revises the agreement, we re-quote in one place

Every quotation below names its Appendix and Section. Line numbers refer to
the text extraction at
``$BRONZE/ACO_REACH_PY2026_AR_PA_2023_Starters_508.txt`` as of the v0.0.14
bronze snapshot; treat them as documentation hints, not load-bearing.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Appendix A — Beneficiary Alignment, Section IV.B: Additional Eligibility
# Criteria for Alignment to a High Needs Population ACO
# ---------------------------------------------------------------------------

APPENDIX_A_SECTION_IV_B_1_LEAD = """\
If the ACO is a High Needs Population ACO, a Beneficiary must also meet one
or more of the following conditions when first aligned to the ACO for a
Performance Year, Base Year, reference year, or lookback period, as
applicable.
"""
# PA line 3762.

APPENDIX_A_SECTION_IV_B_1_A = """\
Have one or more developmental or inherited conditions or congenital
neurological anomalies that impair the Beneficiary's mobility or the
Beneficiary's neurological condition. Such conditions or anomalies could
include cerebral palsy, cystic fibrosis, muscular dystrophy, metabolic
disorders, or any other condition as specified by CMS. The codes that will
be considered for purposes of this Section IV.B.1(a) will be specified by
CMS prior to the start of the relevant Performance Year.
"""
# PA line 3763.

APPENDIX_A_SECTION_IV_B_1_B = """\
Have at least one significant chronic or other serious illness (defined as
having a risk score of 3.0 or greater for Aged & Disabled (A&D)
Beneficiaries or a risk score of 0.35 or greater for ESRD Beneficiaries).
"""
# PA lines 3765–3773.

APPENDIX_A_SECTION_IV_B_1_C = """\
Have a risk score between 2.0 and 3.0 for A&D Beneficiaries, or a risk
score between 0.24 and 0.35 for ESRD Beneficiaries, and two or more
unplanned hospital admissions in the previous 12 months as determined by
CMS based on criteria specified by CMS in advance of the relevant
Performance Year.
"""
# PA line 3779.

APPENDIX_A_SECTION_IV_B_1_D = """\
Exhibit signs of frailty, as evidenced by a claim submitted by a provider
or supplier for a hospital bed (e.g., specialized pressure-reducing
mattresses and some bed safety equipment), or transfer equipment (e.g.,
patient lift mechanisms, safety equipment, and standing systems) for use
in the home. The codes that will be considered for purposes of this
Section IV.B.1(d) will be specified by CMS prior to the start of the
relevant Performance Year.
"""
# PA line 3782.

APPENDIX_A_SECTION_IV_B_1_E = """\
For Performance Year 2024 and each subsequent Performance Year, have
qualified for and received skilled nursing and/or rehabilitation services
in a SNF for a minimum of 45 Days or qualified for and received home
health services for a minimum of 90 Days in the previous 12 months as
determined by CMS.
"""
# PA line 3786.

APPENDIX_A_SECTION_IV_B_2_RISK_MODEL_SELECTION = """\
For each Performance Year prior to Performance Year 2024, CMS determines
Beneficiary risk scores for the purposes of Section IV.B of this Appendix
for A&D Beneficiaries by using the risk score calculated under the
CMS-HCC Risk Adjustment Model or the CMMI-HCC Concurrent Risk Adjustment
Model (as defined in Appendix B of the Agreement), whichever risk score
is higher. Beginning Performance Year 2024, CMS determines Beneficiary
risk scores for the purposes of Section IV.B of this Appendix for A&D
Beneficiaries by using the risk score calculated under the 2020 CMS-HCC
Risk Adjustment Model (Version 24) or the 2024 CMS-HCC Risk Adjustment
Model (Version 28) or the CMMI-HCC Concurrent Risk Adjustment Model (as
defined in Appendix B of the Agreement), whichever risk score is higher.
CMS calculates Beneficiary risk scores for the purposes of Section IV.B
of this Appendix for ESRD Beneficiaries by using the CMS-HCC Risk
Adjustment Model.
"""
# PA line 3791.

APPENDIX_A_SECTION_IV_B_3_STICKY_ALIGNMENT = """\
Once a Beneficiary is aligned to a High-Needs Population ACO, the
Beneficiary will remain aligned to the ACO even if the Beneficiary
subsequently ceases to meet the criteria in Section IV.B.1 of this
Appendix.
"""
# PA line 3794.

APPENDIX_A_TABLE_B_FREQUENCY = """\
Table B. Frequency for Determining Whether a Beneficiary Meets Additional
Eligibility Criteria for Alignment to a High Needs Population ACO during
a Performance Year.

Rows (alignment track × prior-PY state):
    CA  prior to PY         — check Jan 1; re-check Apr 1, Jul 1, Oct 1
    VA  prior to PY         — check Jan 1; re-check Apr 1, Jul 1, Oct 1
    VA  for April 1 (P+)    — check Apr 1; re-check Jul 1, Oct 1
    VA  for July 1  (P+)    — check Jul 1; re-check Oct 1
    VA  for October 1 (P+)  — check Oct 1 only

    CA = Claims-Aligned, VA = Voluntarily Aligned, P+ = Prospective Plus.
"""
# PA lines 3819–3857.

APPENDIX_A_TABLE_C_LOOKBACK_ABCE = """\
Table C. Lookback Periods to Determine Whether a Beneficiary Meets
Additional Eligibility Criteria (a)-(c); (e) of Section IV.B.1 of
Appendix A for Alignment to a High Needs Population ACO during a
Performance Year.

Per check date, a 12-month window whose end is the last day of the
month three months before the check month:

    Jan 1 of PY → (PY-2)-Nov 1  through (PY-1)-Oct 31
    Apr 1 of PY → (PY-1)-Feb 1 through PY-Jan 31
    Jul 1 of PY → (PY-1)-May 1 through PY-Apr 30
    Oct 1 of PY → (PY-1)-Aug 1 through PY-Jul 31
"""
# PA lines 3857–3897.

APPENDIX_A_TABLE_D_LOOKBACK_D = """\
Table D. Lookback Periods to Determine Whether a Beneficiary Meets
Additional Eligibility Criteria (d) of Section IV.B.1 of Appendix A for
Alignment to a High Needs Population ACO during a Performance Year.

Per check date, a 60-month window whose end is the last day of the
month three months before the check month:

    Jan 1 of PY → (PY-6)-Nov 1  through (PY-1)-Oct 31
    Apr 1 of PY → (PY-5)-Feb 1 through PY-Jan 31
    Jul 1 of PY → (PY-5)-May 1 through PY-Apr 30
    Oct 1 of PY → (PY-5)-Aug 1 through PY-Jul 31
"""
# PA lines 3899–3938.

# ---------------------------------------------------------------------------
# Appendix B — ACO REACH Model Financial Methodology, risk model definitions
# ---------------------------------------------------------------------------

APPENDIX_B_CMMI_DEMOGRAPHIC_DEFINITION = """\
"CMMI Demographic Risk Adjustment Model" means the demographic risk score
model under which CMS determines a demographic risk score for a
Beneficiary based on a prediction of the Beneficiary's Medicare
expenditures using demographic variables that include age, sex, original
reason for entitlement code, and Medicaid dual status. From January 1,
2023, through January 20, 2025, this risk adjustment model used the term
"gender" instead of "sex."
"""
# PA line 4313.

APPENDIX_B_CMMI_HCC_CONCURRENT_DEFINITION = """\
"CMMI-HCC Concurrent Risk Adjustment Model" means a method for measuring
the health risk of a population with a risk score to reflect the
predicted expenditures of that population. The CMMI-HCC Concurrent Risk
Adjustment Model has a concurrent model design, which means that risk
scores are calculated using diagnoses recorded on claims with dates of
service during the calendar year in which the risk scores are used for
payment purposes. The CMMI-HCC Concurrent Risk Adjustment Model can be
applied to Aged & Disabled (A&D) Beneficiaries; there is a non-End-Stage
Renal Disease (ESRD) segment, but no ESRD segment.
"""
# PA line 4314.

APPENDIX_B_CMS_HCC_DEFINITION = """\
"CMS-HCC Risk Adjustment Model" means a method for measuring the health
risk of a population with a risk score to reflect the predicted
expenditures of that population. The CMS-HCC Risk Adjustment Model has a
prospective model design, which means that risk scores are calculated
using diagnoses recorded on claims with dates of service during the
calendar year prior to the calendar year in which the risk scores are
used for payment purposes. CMS-HCC Risk Adjustment Model can be applied
to A&D Beneficiaries (the non-ESRD segment) and the CMS-HCC ESRD Risk
Adjustment Model to ESRD Beneficiaries (the ESRD segment). For
Performance Year 2026, the risk score shall be calculated using a 100%
weight of the updated 2024 CMS-HCC Risk Adjustment Model (Version 28),
as described in the Announcement of Calendar Year (CY) 2026 Medicare
Advantage (MA) Capitation Rates and Part C and Part D Payment Policies.
"""
# PA line 4315.


# ---------------------------------------------------------------------------
# Convenience lookup. Docstrings import from here by name so the rendered
# module docs carry the exact CMS text without paraphrase.
# ---------------------------------------------------------------------------

CITATIONS: dict[str, str] = {
    "appendix_a_iv_b_1_lead": APPENDIX_A_SECTION_IV_B_1_LEAD,
    "appendix_a_iv_b_1_a": APPENDIX_A_SECTION_IV_B_1_A,
    "appendix_a_iv_b_1_b": APPENDIX_A_SECTION_IV_B_1_B,
    "appendix_a_iv_b_1_c": APPENDIX_A_SECTION_IV_B_1_C,
    "appendix_a_iv_b_1_d": APPENDIX_A_SECTION_IV_B_1_D,
    "appendix_a_iv_b_1_e": APPENDIX_A_SECTION_IV_B_1_E,
    "appendix_a_iv_b_2_risk_model_selection": APPENDIX_A_SECTION_IV_B_2_RISK_MODEL_SELECTION,
    "appendix_a_iv_b_3_sticky_alignment": APPENDIX_A_SECTION_IV_B_3_STICKY_ALIGNMENT,
    "appendix_a_table_b_frequency": APPENDIX_A_TABLE_B_FREQUENCY,
    "appendix_a_table_c_lookback_abce": APPENDIX_A_TABLE_C_LOOKBACK_ABCE,
    "appendix_a_table_d_lookback_d": APPENDIX_A_TABLE_D_LOOKBACK_D,
    "appendix_b_cmmi_demographic_definition": APPENDIX_B_CMMI_DEMOGRAPHIC_DEFINITION,
    "appendix_b_cmmi_hcc_concurrent_definition": APPENDIX_B_CMMI_HCC_CONCURRENT_DEFINITION,
    "appendix_b_cms_hcc_definition": APPENDIX_B_CMS_HCC_DEFINITION,
}


def cite(key: str) -> str:
    """
    Return the verbatim PA passage indexed by ``key``.

    Intended for use in docstrings: ``> quoted passage\\n``-style or as a
    literal include via f-string. The keys are the names of the module-
    level constants above, lower-cased with the leading ``APPENDIX_`` and
    ``SECTION_`` prefixes stripped — see the ``CITATIONS`` dict for the
    canonical list.
    """
    return CITATIONS[key]
