"""
Regenerate reconciliation test fixtures with obviously-synthetic values.

Every MBI starts with ``9`` (real CMS MBIs start with ``1``), so there is no
chance a random collision with a real beneficiary identifier is possible.
Names, addresses, dates are produced by Faker with a fixed seed for
reproducibility.

Re-run whenever test fixtures need refreshing::

    uv run python scripts/regenerate_reconciliation_fixtures.py
"""
from __future__ import annotations

import random
import string
from datetime import date, datetime
from pathlib import Path

import polars as pl
from faker import Faker

FIXTURE_DIR = Path(__file__).parent.parent / "src" / "acoharmony" / "_test" / "_fixtures" / "reconciliation"
SEED = 20260415

fake = Faker("en_US")
Faker.seed(SEED)
rng = random.Random(SEED)


def fake_mbi() -> str:
    """MBI-shaped identifier that CANNOT collide with a real CMS MBI.

    Real CMS MBIs start with '1' and use a restricted alphabet. We anchor the
    first position to '9' (disallowed in real MBIs per CMS rules) to guarantee
    the synthetic identifier is distinguishable from any real MBI.
    """
    alnum = string.ascii_uppercase + string.digits
    return "9" + "".join(rng.choices(alnum, k=10))


def fake_hic() -> str:
    """Old-style HIC number shape, anchored with 'Z' prefix."""
    return "Z" + "".join(rng.choices(string.digits, k=8)) + rng.choice(string.ascii_uppercase)


def fake_claim_uid() -> str:
    return "".join(rng.choices(string.digits, k=13))


def fake_npi() -> str:
    return "9" + "".join(rng.choices(string.digits, k=9))


def fake_oscar() -> str:
    return "".join(rng.choices(string.digits, k=6))


def fake_date(start: date, end: date) -> date:
    return fake.date_between(start_date=start, end_date=end)


def fake_amt(lo: float, hi: float) -> float:
    return round(rng.uniform(lo, hi), 2)


def mbi_pool(n: int) -> list[str]:
    return [fake_mbi() for _ in range(n)]


def build_alr(n: int = 300) -> pl.DataFrame:
    rng.seed(SEED + 1)
    Faker.seed(SEED + 1)
    mbis = mbi_pool(n)
    return pl.DataFrame({
        "bene_mbi": mbis,
        "bene_first_name": [fake.first_name() for _ in range(n)],
        "bene_last_name": [fake.last_name() for _ in range(n)],
        "bene_sex_cd": [rng.choice(["1", "2"]) for _ in range(n)],
        "bene_birth_dt": [fake_date(date(1930, 1, 1), date(1965, 12, 31)) for _ in range(n)],
        "death_date": [None] * n,
        "master_id": [str(1000000 + i) for i in range(n)],
        "b_em_line_cnt_t": [str(rng.randint(0, 50)) for _ in range(n)],
        "processed_at": ["2026-04-15T00:00:00"] * n,
        "source_file": ["alr"] * n,
        "source_filename": ["P.D0259.ACO.ALR.D260202.T100000001"] * n,
        "file_date": ["2026-02-02"] * n,
        "medallion_layer": ["bronze"] * n,
    })


def build_bar(n: int = 300) -> pl.DataFrame:
    rng.seed(SEED + 2)
    Faker.seed(SEED + 2)
    mbis = mbi_pool(n)
    return pl.DataFrame({
        "bene_mbi": mbis,
        "start_date": [date(2025, 1, 1)] * n,
        "end_date": [date(2026, 12, 31)] * n,
        "bene_first_name": [fake.first_name() for _ in range(n)],
        "bene_last_name": [fake.last_name() for _ in range(n)],
        "bene_address_line_1": [fake.street_address() for _ in range(n)],
        "bene_address_line_2": [None] * n,
        "bene_address_line_3": [None] * n,
        "bene_address_line_4": [None] * n,
        "bene_address_line_5": [None] * n,
        "bene_address_line_6": [None] * n,
        "bene_city": [fake.city() for _ in range(n)],
        "bene_state": [fake.state_abbr() for _ in range(n)],
        "bene_zip_5": [fake.postcode()[:5] for _ in range(n)],
        "bene_zip_4": ["".join(rng.choices(string.digits, k=4)) for _ in range(n)],
        "bene_county_ssa": ["".join(rng.choices(string.digits, k=3)) for _ in range(n)],
        "bene_county_fips": ["".join(rng.choices(string.digits, k=5)) for _ in range(n)],
        "bene_gender": [rng.choice(["M", "F"]) for _ in range(n)],
        "bene_race_ethnicity": [rng.choice(["1", "2", "3", "4", "5"]) for _ in range(n)],
        "bene_date_of_birth": [fake_date(date(1930, 1, 1), date(1965, 12, 31)) for _ in range(n)],
        "bene_age": [rng.randint(60, 95) for _ in range(n)],
        "bene_date_of_death": [None] * n,
        "bene_eligibility_year_1": ["2025"] * n,
        "bene_eligibility_year_2": ["2026"] * n,
        "bene_part_d_year_1": [rng.choice(["Y", "N"]) for _ in range(n)],
        "bene_part_d_year_2": [rng.choice(["Y", "N"]) for _ in range(n)],
        "newly_aligned_flag": [rng.choice(["Y", "N"]) for _ in range(n)],
        "prospective_plus_flag": [rng.choice(["Y", "N"]) for _ in range(n)],
        "claims_based_flag": [rng.choice(["Y", "N"]) for _ in range(n)],
        "voluntary_alignment_type": [rng.choice(["", "sva", "voluntary"]) for _ in range(n)],
        "mobility_impairment_flag": [rng.choice(["Y", "N"]) for _ in range(n)],
        "frailty_flag": [rng.choice(["Y", "N"]) for _ in range(n)],
        "medium_risk_unplanned_flag": [rng.choice(["Y", "N"]) for _ in range(n)],
        "high_risk_flag": [rng.choice(["Y", "N"]) for _ in range(n)],
        "processed_at": ["2026-04-15T00:00:00"] * n,
        "source_file": ["bar"] * n,
        "source_filename": ["P.D0259.ACO.BAR.D260202.T100000001"] * n,
        "file_date": ["2026-02-02"] * n,
        "medallion_layer": ["bronze"] * n,
    })


def _cclf_common_bene(n: int, mbi_pool_size: int, seed_offset: int) -> tuple[list[str], list[str]]:
    rng.seed(SEED + seed_offset)
    mbis_unique = mbi_pool(mbi_pool_size)
    hics_unique = [fake_hic() for _ in range(mbi_pool_size)]
    # Each row gets one mbi/hic, with some repetition to simulate multiple claims per bene
    mbis = [rng.choice(mbis_unique) for _ in range(n)]
    hics = [hics_unique[mbis_unique.index(m)] for m in mbis]
    return mbis, hics


def build_cclf1(n: int = 300) -> pl.DataFrame:
    Faker.seed(SEED + 10)
    mbis, hics = _cclf_common_bene(n, mbi_pool_size=60, seed_offset=10)
    return pl.DataFrame({
        "cur_clm_uniq_id": [fake_claim_uid() for _ in range(n)],
        "prvdr_oscar_num": [fake_oscar() for _ in range(n)],
        "bene_mbi_id": mbis,
        "bene_hic_num": [None] * n,
        "clm_type_cd": [rng.choice(["40", "50", "60", "10"]) for _ in range(n)],
        "clm_from_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_thru_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_bill_fac_type_cd": [rng.choice(["1", "2", "3", "8"]) for _ in range(n)],
        "clm_bill_clsfctn_cd": [rng.choice(["1", "2", "3"]) for _ in range(n)],
        "prncpl_dgns_cd": [fake.bothify(text="?##.#").upper() for _ in range(n)],
        "admtg_dgns_cd": [fake.bothify(text="?##.#").upper() for _ in range(n)],
        "clm_mdcr_npmt_rsn_cd": [None] * n,
        "clm_pmt_amt": [fake_amt(50.0, 15000.0) for _ in range(n)],
        "clm_nch_prmry_pyr_cd": [None] * n,
        "prvdr_fac_fips_st_cd": ["".join(rng.choices(string.digits, k=2)) for _ in range(n)],
        "bene_ptnt_stus_cd": [rng.choice(["01", "02", "03", "20"]) for _ in range(n)],
        "dgns_drg_cd": ["".join(rng.choices(string.digits, k=3)) for _ in range(n)],
        "clm_op_srvc_type_cd": [None] * n,
        "fac_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "oprtg_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "atndg_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "othr_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "clm_adjsmt_type_cd": [rng.choice(["0", "1", "2"]) for _ in range(n)],
        "clm_efctv_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_idr_ld_dt": [fake_date(date(2026, 2, 1), date(2026, 4, 1)) for _ in range(n)],
        "bene_eqtbl_bic_hicn_num": hics,
        "clm_admsn_type_cd": [rng.choice(["1", "2", "3"]) for _ in range(n)],
        "clm_admsn_src_cd": [rng.choice(["1", "2", "3"]) for _ in range(n)],
        "clm_bill_freq_cd": [rng.choice(["1", "2", "3"]) for _ in range(n)],
        "clm_query_cd": [rng.choice(["0", "1"]) for _ in range(n)],
        "dgns_prcdr_icd_ind": ["0"] * n,
        "clm_mdcr_instnl_tot_chrg_amt": [fake_amt(100.0, 50000.0) for _ in range(n)],
        "clm_mdcr_ip_pps_cptl_ime_amt": [fake_amt(0.0, 1000.0) for _ in range(n)],
        "clm_oprtnl_ime_amt": [fake_amt(0.0, 1000.0) for _ in range(n)],
        "clm_mdcr_ip_pps_dsprprtnt_amt": [fake_amt(0.0, 500.0) for _ in range(n)],
        "clm_hipps_uncompd_care_amt": [fake_amt(0.0, 500.0) for _ in range(n)],
        "clm_oprtnl_dsprprtnt_amt": [fake_amt(0.0, 500.0) for _ in range(n)],
        "clm_blg_prvdr_oscar_num": [fake_oscar() for _ in range(n)],
        "clm_blg_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "clm_oprtg_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "clm_atndg_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "clm_othr_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "clm_cntl_num": ["".join(rng.choices(string.digits, k=17)) for _ in range(n)],
        "clm_org_cntl_num": [None] * n,
        "clm_cntrctr_num": ["".join(rng.choices(string.digits, k=5)) for _ in range(n)],
        "processed_at": ["2026-04-15T00:00:00"] * n,
        "source_file": ["cclf1"] * n,
        "source_filename": ["P.D0259.ACO.ZC1Y26.D260202.T1000000"] * n,
        "file_date": ["2026-02-02"] * n,
        "medallion_layer": ["bronze"] * n,
    })


def build_cclf2(n: int = 300) -> pl.DataFrame:
    Faker.seed(SEED + 20)
    mbis, hics = _cclf_common_bene(n, mbi_pool_size=60, seed_offset=20)
    return pl.DataFrame({
        "cur_clm_uniq_id": [fake_claim_uid() for _ in range(n)],
        "clm_line_num": ["{:09d}".format(rng.randint(1, 100)) for _ in range(n)],
        "bene_mbi_id": mbis,
        "bene_hic_num": [None] * n,
        "clm_type_cd": [rng.choice(["40", "50", "60"]) for _ in range(n)],
        "clm_line_from_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_line_thru_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_line_prod_rev_ctr_cd": ["".join(rng.choices(string.digits, k=4)) for _ in range(n)],
        "clm_line_instnl_rev_ctr_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_line_hcpcs_cd": [fake.bothify(text="?####").upper() for _ in range(n)],
        "bene_eqtbl_bic_hicn_num": hics,
        "prvdr_oscar_num": [fake_oscar() for _ in range(n)],
        "clm_from_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_thru_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_line_srvc_unit_qty": [fake_amt(1.0, 100.0) for _ in range(n)],
        "clm_line_cvrd_pd_amt": [fake_amt(10.0, 5000.0) for _ in range(n)],
        "hcpcs_1_mdfr_cd": [None] * n,
        "hcpcs_2_mdfr_cd": [None] * n,
        "hcpcs_3_mdfr_cd": [None] * n,
        "hcpcs_4_mdfr_cd": [None] * n,
        "hcpcs_5_mdfr_cd": [None] * n,
        "clm_rev_apc_hipps_cd": [None] * n,
        "clm_fac_prvdr_oscar_num": [fake_oscar() for _ in range(n)],
        "processed_at": ["2026-04-15T00:00:00"] * n,
        "source_file": ["cclf2"] * n,
        "source_filename": ["P.D0259.ACO.ZC2Y26.D260202.T1000000"] * n,
        "file_date": ["2026-02-02"] * n,
        "medallion_layer": ["bronze"] * n,
    })


def build_cclf3(n: int = 300) -> pl.DataFrame:
    Faker.seed(SEED + 30)
    mbis, hics = _cclf_common_bene(n, mbi_pool_size=60, seed_offset=30)
    return pl.DataFrame({
        "cur_clm_uniq_id": [fake_claim_uid() for _ in range(n)],
        "bene_mbi_id": mbis,
        "bene_hic_num": [None] * n,
        "clm_type_cd": [rng.choice(["20", "60"]) for _ in range(n)],
        "clm_val_sqnc_num": [str(rng.randint(1, 10)) for _ in range(n)],
        "clm_prcdr_cd": [fake.bothify(text="#####").upper() for _ in range(n)],
        "clm_prcdr_prfrm_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "bene_eqtbl_bic_hicn_num": hics,
        "prvdr_oscar_num": [fake_oscar() for _ in range(n)],
        "clm_from_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_thru_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "dgns_prcdr_icd_ind": ["0"] * n,
        "clm_blg_prvdr_oscar_num": [fake_oscar() for _ in range(n)],
        "processed_at": ["2026-04-15T00:00:00"] * n,
        "source_file": ["cclf3"] * n,
        "source_filename": ["P.D0259.ACO.ZC3Y26.D260202.T1000000"] * n,
        "file_date": ["2026-02-02"] * n,
        "medallion_layer": ["bronze"] * n,
    })


def build_cclf5(n: int = 300) -> pl.DataFrame:
    Faker.seed(SEED + 50)
    mbis, hics = _cclf_common_bene(n, mbi_pool_size=60, seed_offset=50)
    return pl.DataFrame({
        "cur_clm_uniq_id": [fake_claim_uid() for _ in range(n)],
        "clm_line_num": ["{:09d}".format(rng.randint(1, 100)) for _ in range(n)],
        "bene_mbi_id": mbis,
        "bene_hic_num": [None] * n,
        "clm_type_cd": [rng.choice(["71", "72"]) for _ in range(n)],
        "clm_from_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_thru_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "rndrg_prvdr_type_cd": ["".join(rng.choices(string.digits, k=2)) for _ in range(n)],
        "rndrg_prvdr_fips_st_cd": ["".join(rng.choices(string.digits, k=2)) for _ in range(n)],
        "clm_prvdr_spclty_cd": ["".join(rng.choices(string.digits, k=2)) for _ in range(n)],
        "clm_fed_type_srvc_cd": [rng.choice(["1", "2", "3", "4"]) for _ in range(n)],
        "clm_pos_cd": [rng.choice(["11", "21", "22", "23"]) for _ in range(n)],
        "clm_line_from_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_line_thru_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_line_hcpcs_cd": [fake.bothify(text="?####").upper() for _ in range(n)],
        "clm_line_cvrd_pd_amt": [fake_amt(20.0, 2000.0) for _ in range(n)],
        "clm_line_prmry_pyr_cd": [None] * n,
        "clm_line_dgns_cd": [fake.bothify(text="?##.#").upper() for _ in range(n)],
        "clm_rndrg_prvdr_tax_num": ["".join(rng.choices(string.digits, k=9)) for _ in range(n)],
        "rndrg_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "clm_carr_pmt_dnl_cd": [rng.choice(["1", "2", "A", "B"]) for _ in range(n)],
        "clm_prcsg_ind_cd": [rng.choice(["A", "R"]) for _ in range(n)],
        "clm_adjsmt_type_cd": [rng.choice(["0", "1"]) for _ in range(n)],
        "clm_efctv_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_idr_ld_dt": [fake_date(date(2026, 2, 1), date(2026, 4, 1)) for _ in range(n)],
        "clm_cntl_num": ["".join(rng.choices(string.digits, k=17)) for _ in range(n)],
        "bene_eqtbl_bic_hicn_num": hics,
        "clm_line_alowd_chrg_amt": [fake_amt(20.0, 2000.0) for _ in range(n)],
        "clm_line_srvc_unit_qty": [fake_amt(1.0, 20.0) for _ in range(n)],
        "hcpcs_1_mdfr_cd": [None] * n,
        "hcpcs_2_mdfr_cd": [None] * n,
        "hcpcs_3_mdfr_cd": [None] * n,
        "hcpcs_4_mdfr_cd": [None] * n,
        "hcpcs_5_mdfr_cd": [None] * n,
        "clm_disp_cd": [rng.choice(["1", "2", "3"]) for _ in range(n)],
        "clm_dgns_1_cd": [fake.bothify(text="?##.#").upper() for _ in range(n)],
        "clm_dgns_2_cd": [None] * n,
        "clm_dgns_3_cd": [None] * n,
        "clm_dgns_4_cd": [None] * n,
        "clm_dgns_5_cd": [None] * n,
        "clm_dgns_6_cd": [None] * n,
        "clm_dgns_7_cd": [None] * n,
        "clm_dgns_8_cd": [None] * n,
        "dgns_prcdr_icd_ind": ["0"] * n,
        "clm_dgns_9_cd": [None] * n,
        "clm_dgns_10_cd": [None] * n,
        "clm_dgns_11_cd": [None] * n,
        "clm_dgns_12_cd": [None] * n,
        "hcpcs_betos_cd": [None] * n,
        "clm_rndrg_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "clm_rfrg_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "clm_cntrctor_num": ["".join(rng.choices(string.digits, k=5)) for _ in range(n)],
        "processed_at": ["2026-04-15T00:00:00"] * n,
        "source_file": ["cclf5"] * n,
        "source_filename": ["P.D0259.ACO.ZC5Y26.D260202.T1000000"] * n,
        "file_date": ["2026-02-02"] * n,
        "medallion_layer": ["bronze"] * n,
    })


def build_cclf6(n: int = 300) -> pl.DataFrame:
    Faker.seed(SEED + 60)
    mbis, hics = _cclf_common_bene(n, mbi_pool_size=60, seed_offset=60)
    return pl.DataFrame({
        "cur_clm_uniq_id": [fake_claim_uid() for _ in range(n)],
        "clm_line_num": ["{:09d}".format(rng.randint(1, 100)) for _ in range(n)],
        "bene_mbi_id": mbis,
        "bene_hic_num": [None] * n,
        "clm_type_cd": [rng.choice(["81", "82"]) for _ in range(n)],
        "clm_from_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_thru_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_fed_type_srvc_cd": [rng.choice(["1", "2", "3"]) for _ in range(n)],
        "clm_pos_cd": [rng.choice(["11", "21", "22"]) for _ in range(n)],
        "clm_line_from_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_line_thru_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_line_hcpcs_cd": [fake.bothify(text="?####").upper() for _ in range(n)],
        "clm_line_cvrd_pd_amt": [fake_amt(10.0, 500.0) for _ in range(n)],
        "clm_prmry_pyr_cd": [None] * n,
        "payto_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "ordrg_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "clm_carr_pmt_dnl_cd": [rng.choice(["1", "2", "A"]) for _ in range(n)],
        "clm_prcsg_ind_cd": ["A"] * n,
        "clm_adjsmt_type_cd": ["0"] * n,
        "clm_efctv_dt": [fake_date(date(2026, 1, 1), date(2026, 3, 31)) for _ in range(n)],
        "clm_idr_ld_dt": [fake_date(date(2026, 2, 1), date(2026, 4, 1)) for _ in range(n)],
        "clm_cntl_num": ["".join(rng.choices(string.digits, k=17)) for _ in range(n)],
        "bene_eqtbl_bic_hicn_num": hics,
        "clm_line_alowd_chrg_amt": [fake_amt(10.0, 500.0) for _ in range(n)],
        "clm_disp_cd": [rng.choice(["1", "2"]) for _ in range(n)],
        "clm_blg_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "clm_rfrg_prvdr_npi_num": [fake_npi() for _ in range(n)],
        "processed_at": ["2026-04-15T00:00:00"] * n,
        "source_file": ["cclf6"] * n,
        "source_filename": ["P.D0259.ACO.ZC6Y26.D260202.T1000000"] * n,
        "file_date": ["2026-02-02"] * n,
        "medallion_layer": ["bronze"] * n,
    })


def build_consolidated_alignment(n: int = 300) -> pl.DataFrame:
    """Consolidated alignment is wide (306 cols). Build col by col."""
    rng.seed(SEED + 100)
    Faker.seed(SEED + 100)
    mbis = mbi_pool(n)
    cols: dict[str, list] = {"current_mbi": mbis}

    # Year-month flag blocks (2022-12 through 2026-03)
    for year in range(2022, 2027):
        for m in range(1, 13):
            if year == 2022 and m < 12:
                continue
            if year == 2026 and m > 3:
                continue
            ym = f"ym_{year}{m:02d}"
            cols[f"{ym}_reach"] = [rng.random() < 0.3 for _ in range(n)]
            cols[f"{ym}_mssp"] = [rng.random() < 0.2 for _ in range(n)]
            cols[f"{ym}_ffs"] = [rng.random() < 0.5 for _ in range(n)]
            cols[f"{ym}_first_claim"] = [rng.random() < 0.1 for _ in range(n)]

    cols["months_in_reach"] = [rng.randint(0, 40) for _ in range(n)]
    cols["months_in_mssp"] = [rng.randint(0, 40) for _ in range(n)]
    cols["months_in_ffs"] = [rng.randint(0, 40) for _ in range(n)]
    cols["ever_reach"] = [rng.random() < 0.5 for _ in range(n)]
    cols["ever_mssp"] = [rng.random() < 0.5 for _ in range(n)]
    cols["ever_ffs"] = [rng.random() < 0.9 for _ in range(n)]
    cols["bene_mbi"] = mbis
    cols["current_program"] = [rng.choice(["reach", "mssp", "ffs"]) for _ in range(n)]
    cols["current_aco_id"] = ["A9999"] * n
    cols["continuous_enrollment"] = [rng.random() < 0.8 for _ in range(n)]
    cols["program_switches"] = [rng.randint(0, 3) for _ in range(n)]
    cols["enrollment_gaps"] = [rng.randint(0, 2) for _ in range(n)]
    cols["previous_mbi_count"] = [0] * n
    cols["first_reach_date"] = [fake_date(date(2022, 1, 1), date(2024, 1, 1)) for _ in range(n)]
    cols["last_reach_date"] = [fake_date(date(2024, 6, 1), date(2026, 3, 1)) for _ in range(n)]
    cols["first_mssp_date"] = [fake_date(date(2022, 1, 1), date(2024, 1, 1)) for _ in range(n)]
    cols["last_mssp_date"] = [fake_date(date(2024, 6, 1), date(2026, 3, 1)) for _ in range(n)]
    cols["birth_date"] = [fake_date(date(1930, 1, 1), date(1965, 12, 31)) for _ in range(n)]
    cols["death_date"] = [None] * n
    cols["sex"] = [rng.choice(["M", "F"]) for _ in range(n)]
    cols["race"] = [rng.choice(["1", "2", "3", "4"]) for _ in range(n)]
    cols["ethnicity"] = [None] * n
    cols["state"] = [fake.state_abbr() for _ in range(n)]
    cols["county"] = [fake.city() for _ in range(n)]
    cols["zip_code"] = [fake.postcode()[:5] for _ in range(n)]
    cols["has_ffs_service"] = [rng.random() < 0.8 for _ in range(n)]
    cols["ffs_claim_count"] = [rng.randint(0, 50) for _ in range(n)]
    cols["has_demographics"] = [True] * n
    cols["mbi_stability"] = ["stable"] * n
    cols["current_provider_tin"] = ["".join(rng.choices(string.digits, k=9)) for _ in range(n)]
    cols["ffs_first_date"] = [fake_date(date(2022, 1, 1), date(2024, 1, 1)) for _ in range(n)]
    cols["observable_start"] = [None] * n
    cols["observable_end"] = [None] * n
    cols["processed_at"] = [None] * n
    cols["has_voluntary_alignment"] = [rng.random() < 0.3 for _ in range(n)]
    cols["voluntary_alignment_type"] = [rng.choice(["", "sva", "voluntary"]) for _ in range(n)]
    cols["voluntary_alignment_date"] = [fake_date(date(2024, 1, 1), date(2026, 3, 1)) for _ in range(n)]
    cols["voluntary_provider_name"] = [fake.company() for _ in range(n)]
    cols["voluntary_provider_npi"] = [fake_npi() for _ in range(n)]
    cols["voluntary_provider_tin"] = ["".join(rng.choices(string.digits, k=9)) for _ in range(n)]
    cols["sva_provider_valid"] = [rng.random() < 0.9 for _ in range(n)]
    cols["first_valid_signature_date"] = [fake_date(date(2024, 1, 1), date(2026, 3, 1)) for _ in range(n)]
    cols["last_valid_signature_date"] = [fake_date(date(2024, 6, 1), date(2026, 3, 1)) for _ in range(n)]
    cols["first_sva_submission_date"] = [fake_date(date(2024, 1, 1), date(2026, 3, 1)) for _ in range(n)]
    cols["last_sva_submission_date"] = [fake_date(date(2024, 6, 1), date(2026, 3, 1)) for _ in range(n)]
    cols["latest_response_codes"] = ["Y"] * n
    cols["latest_response_detail"] = [""] * n
    cols["pbvar_report_date"] = [date(2026, 1, 15)] * n
    cols["has_email_opt_out"] = [rng.random() < 0.1 for _ in range(n)]
    cols["has_mail_opt_out"] = [rng.random() < 0.1 for _ in range(n)]
    cols["voluntary_ffs_date"] = [fake_date(date(2024, 1, 1), date(2026, 3, 1)) for _ in range(n)]
    cols["has_valid_voluntary_alignment"] = [rng.random() < 0.25 for _ in range(n)]
    cols["_voluntary_aligned"] = [rng.random() < 0.25 for _ in range(n)]
    cols["bene_first_name"] = [fake.first_name() for _ in range(n)]
    cols["bene_last_name"] = [fake.last_name() for _ in range(n)]
    cols["bene_middle_initial"] = [rng.choice(string.ascii_uppercase) for _ in range(n)]
    cols["bene_address_line_1"] = [fake.street_address() for _ in range(n)]
    cols["bene_city"] = [fake.city() for _ in range(n)]
    cols["bene_state"] = [fake.state_abbr() for _ in range(n)]
    cols["bene_zip"] = [fake.postcode()[:5] for _ in range(n)]
    cols["bene_county"] = [fake.city() for _ in range(n)]
    cols["bene_zip_5"] = [fake.postcode()[:5] for _ in range(n)]
    cols["_demographics_joined"] = [True] * n
    cols["office_name"] = [fake.company() for _ in range(n)]
    cols["market"] = [fake.state_abbr() for _ in range(n)]
    cols["office_location"] = [fake.city() for _ in range(n)]
    cols["_office_matched"] = [rng.random() < 0.9 for _ in range(n)]
    cols["mssp_tin"] = ["".join(rng.choices(string.digits, k=9)) for _ in range(n)]
    cols["mssp_npi"] = [fake_npi() for _ in range(n)]
    cols["mssp_provider_name"] = [fake.company() for _ in range(n)]
    cols["reach_attribution_type"] = [rng.choice(["claims", "voluntary", "none"]) for _ in range(n)]
    cols["reach_tin"] = ["".join(rng.choices(string.digits, k=9)) for _ in range(n)]
    cols["reach_npi"] = [fake_npi() for _ in range(n)]
    cols["reach_provider_name"] = [fake.company() for _ in range(n)]
    cols["aligned_provider_tin"] = ["".join(rng.choices(string.digits, k=9)) for _ in range(n)]
    cols["aligned_provider_npi"] = [fake_npi() for _ in range(n)]
    cols["aligned_provider_org"] = [fake.company() for _ in range(n)]
    cols["aligned_practitioner_name"] = [fake.name() for _ in range(n)]
    cols["latest_aco_id"] = ["A9999"] * n
    cols["_provider_attributed"] = [rng.random() < 0.8 for _ in range(n)]
    cols["consolidated_program"] = [rng.choice(["reach", "mssp", "ffs"]) for _ in range(n)]
    cols["total_aligned_months"] = [rng.randint(0, 40) for _ in range(n)]
    cols["primary_alignment_source"] = [rng.choice(["claims", "voluntary"]) for _ in range(n)]
    cols["is_currently_aligned"] = [rng.random() < 0.5 for _ in range(n)]
    cols["has_valid_historical_sva"] = [rng.random() < 0.3 for _ in range(n)]
    cols["has_program_transition"] = [rng.random() < 0.2 for _ in range(n)]
    cols["has_continuous_enrollment"] = [rng.random() < 0.8 for _ in range(n)]
    cols["bene_death_date"] = [None] * n
    cols["prvs_num"] = ["".join(rng.choices(string.digits, k=5)) for _ in range(n)]
    cols["mapping_type"] = [rng.choice(["direct", "derived"]) for _ in range(n)]
    cols["mssp_sva_recruitment_target"] = [rng.random() < 0.2 for _ in range(n)]
    cols["mssp_to_reach_status"] = [rng.choice(["", "pending", "transitioned"]) for _ in range(n)]
    cols["sva_submitted_after_pbvar"] = [rng.random() < 0.1 for _ in range(n)]
    cols["needs_sva_refresh_from_pbvar"] = [rng.random() < 0.1 for _ in range(n)]
    cols["sva_tin_match"] = [rng.random() < 0.9 for _ in range(n)]
    cols["sva_npi_match"] = [rng.random() < 0.9 for _ in range(n)]
    cols["previous_program"] = [rng.choice(["reach", "mssp", "ffs", ""]) for _ in range(n)]
    cols["program_transitions"] = [rng.randint(0, 3) for _ in range(n)]
    cols["_metrics_calculated"] = [True] * n
    cols["signature_expiry_date"] = [fake_date(date(2026, 1, 1), date(2027, 1, 1)) for _ in range(n)]
    cols["last_signature_expiry_date"] = [fake_date(date(2026, 1, 1), date(2027, 1, 1)) for _ in range(n)]
    cols["days_until_signature_expiry"] = [rng.randint(-30, 365) for _ in range(n)]
    cols["signature_valid_for_current_py"] = [rng.random() < 0.8 for _ in range(n)]
    cols["signature_valid_for_pys"] = [rng.choice(["2026", "2026,2027"]) for _ in range(n)]
    cols["sva_outreach_priority"] = [rng.choice(["high", "medium", "low"]) for _ in range(n)]
    cols["response_code_list"] = ["Y"] * n
    cols["latest_response_code"] = ["Y"] * n
    cols["has_acceptance"] = [rng.random() < 0.8 for _ in range(n)]
    cols["has_ineligible_alignment"] = [rng.random() < 0.05 for _ in range(n)]
    cols["validation_errors"] = [""] * n
    cols["has_validation_error"] = [False] * n
    cols["precedence_issues"] = [""] * n
    cols["has_precedence_issue"] = [False] * n
    cols["eligibility_issues"] = [""] * n
    cols["has_eligibility_issue"] = [False] * n
    cols["error_category"] = [""] * n
    cols["data_completeness"] = ["complete"] * n
    cols["lineage_transform"] = ["consolidated_alignment"] * n
    cols["lineage_processed_at"] = ["2026-04-15T00:00:00"] * n
    cols["data_start_date"] = [date(2022, 1, 1)] * n
    cols["data_end_date"] = [date(2026, 3, 31)] * n
    cols["source_tables"] = ["alr,bar,pbvar"] * n
    cols["last_updated"] = [datetime(2026, 4, 15, 0, 0, 0)] * n
    cols["has_opt_out"] = [rng.random() < 0.1 for _ in range(n)]
    cols["sva_action_needed"] = [rng.choice(["", "refresh", "new"]) for _ in range(n)]
    cols["outreach_priority"] = [rng.choice(["high", "medium", "low"]) for _ in range(n)]
    cols["_metadata_added"] = [True] * n
    cols["lost_2025_to_2026"] = [rng.random() < 0.1 for _ in range(n)]
    cols["newly_added_2025_to_2026"] = [rng.random() < 0.1 for _ in range(n)]
    cols["first_reach_month_2026"] = [rng.randint(1, 3) for _ in range(n)]
    cols["termed_bar_2025"] = [rng.random() < 0.1 for _ in range(n)]
    cols["expired_sva_2025"] = [rng.random() < 0.1 for _ in range(n)]
    cols["lost_provider_2025"] = [rng.random() < 0.05 for _ in range(n)]
    cols["moved_ma_2025"] = [rng.random() < 0.05 for _ in range(n)]
    cols["inactive_2025"] = [rng.random() < 0.05 for _ in range(n)]
    cols["unresolved_2025"] = [rng.random() < 0.05 for _ in range(n)]
    cols["newly_added_source_2025_to_2026"] = [rng.choice(["", "claims", "voluntary"]) for _ in range(n)]
    cols["days_since_last_activity"] = [rng.randint(0, 365) for _ in range(n)]
    cols["ma_enrollment_date"] = [None] * n
    cols["unresolved_reason"] = [""] * n
    cols["transition_category_2025"] = [rng.choice(["continued", "new", "lost"]) for _ in range(n)]
    cols["transition_analysis_current_year"] = [2026] * n
    cols["transition_analysis_previous_year"] = [2025] * n
    cols["transition_analysis_date"] = [datetime(2026, 4, 15, 0, 0, 0)] * n
    cols["_transitions_calculated"] = [True] * n

    # Explicit schema for Null-typed columns
    schema: dict[str, object] = {}
    for k, v in cols.items():
        if v and all(x is None for x in v):
            schema[k] = pl.Null
    return pl.DataFrame(cols, schema_overrides=schema)


def build_mexpr_data_claims(n: int = 500) -> pl.DataFrame:
    rng.seed(SEED + 200)
    Faker.seed(SEED + 200)
    return pl.DataFrame({
        "perf_yr": ["2026"] * n,
        "clndr_yr": [rng.choice(["2025", "2026"]) for _ in range(n)],
        "clndr_mnth": ["{:02d}".format(rng.randint(1, 12)) for _ in range(n)],
        "bnmrk": [rng.choice(["A", "B", "C"]) for _ in range(n)],
        "align_type": [rng.choice(["claims", "voluntary"]) for _ in range(n)],
        "bnmrk_type": ["benchmark"] * n,
        "aco_id": ["A9999"] * n,
        "clm_type_cd": [rng.choice(["40", "50", "60", "71", "72", "81", "82"]) for _ in range(n)],
        "clm_pmt_amt_agg": [fake_amt(1000.0, 500000.0) for _ in range(n)],
        "sqstr_amt_agg": [fake_amt(0.0, 10000.0) for _ in range(n)],
        "apa_rdctn_amt_agg": [fake_amt(0.0, 5000.0) for _ in range(n)],
        "pcc_rdctn_amt_agg": [fake_amt(0.0, 5000.0) for _ in range(n)],
        "tcc_rdctn_amt_agg": [fake_amt(0.0, 5000.0) for _ in range(n)],
        "apo_rdctn_amt_agg": [fake_amt(0.0, 5000.0) for _ in range(n)],
        "ucc_amt_agg": [fake_amt(0.0, 1000.0) for _ in range(n)],
        "op_dsh_amt_agg": [fake_amt(0.0, 1000.0) for _ in range(n)],
        "cp_dsh_amt_agg": [fake_amt(0.0, 1000.0) for _ in range(n)],
        "op_ime_amt_agg": [fake_amt(0.0, 1000.0) for _ in range(n)],
        "cp_ime_amt_agg": [fake_amt(0.0, 1000.0) for _ in range(n)],
        "dc_amt_agg_apa": [fake_amt(0.0, 500.0) for _ in range(n)],
        "total_exp_amt_agg": [fake_amt(1000.0, 500000.0) for _ in range(n)],
        "srvc_month": ["{:04d}-{:02d}".format(rng.choice([2025, 2026]), rng.randint(1, 12)) for _ in range(n)],
        "efctv_month": ["{:04d}-{:02d}".format(rng.choice([2025, 2026]), rng.randint(1, 12)) for _ in range(n)],
        "sheet_type": ["data_claims"] * n,
        "processed_at": ["2026-04-15T00:00:00"] * n,
        "source_file": ["mexpr"] * n,
        "source_filename": ["P.D0259.ACO.MEXPR.D260202.T100000001"] * n,
        "file_date": ["2026-02-02"] * n,
        "medallion_layer": ["bronze"] * n,
        "nonpbp_rdct_amt_agg": [fake_amt(0.0, 500.0) for _ in range(n)],
    })


def build_mexpr_data_enroll(n: int = 500) -> pl.DataFrame:
    rng.seed(SEED + 210)
    return pl.DataFrame({
        "perf_yr": ["2026"] * n,
        "clndr_yr": [rng.choice(["2025", "2026"]) for _ in range(n)],
        "clndr_mnth": ["{:02d}".format(rng.randint(1, 12)) for _ in range(n)],
        "bnmrk": [rng.choice(["A", "B", "C"]) for _ in range(n)],
        "align_type": [rng.choice(["claims", "voluntary"]) for _ in range(n)],
        "bnmrk_type": ["benchmark"] * n,
        "aco_id": ["A9999"] * n,
        "sheet_type": ["data_enroll"] * n,
        "bene_dcnt": [rng.randint(100, 5000) for _ in range(n)],
        "elig_mnths": [rng.randint(100, 60000) for _ in range(n)],
        "processed_at": ["2026-04-15T00:00:00"] * n,
        "source_file": ["mexpr"] * n,
        "source_filename": ["P.D0259.ACO.MEXPR.D260202.T100000001"] * n,
        "file_date": ["2026-02-02"] * n,
        "medallion_layer": ["bronze"] * n,
    })


def build_reach_bnmr_claims(n: int = 300) -> pl.DataFrame:
    rng.seed(SEED + 300)
    return pl.DataFrame({
        "sheet_type": ["claims"] * n,
        "performance_year": ["2026"] * n,
        "aco_id": ["A9999"] * n,
        "aco_type": ["REACH"] * n,
        "risk_arrangement": ["PROFESSIONAL"] * n,
        "payment_mechanism": ["PCC"] * n,
        "discount": [0.02] * n,
        "shared_savings_rate": [0.5] * n,
        "advanced_payment_option": ["NO"] * n,
        "stop_loss_elected": ["YES"] * n,
        "stop_loss_type": ["STANDARD"] * n,
        "quality_withhold": [0.02] * n,
        "quality_score": [fake_amt(0.7, 1.0) for _ in range(n)],
        "voluntary_aligned_benchmark": ["Y"] * n,
        "blend_percentage": [fake_amt(0.5, 0.9) for _ in range(n)],
        "blend_ceiling": [1.1] * n,
        "blend_floor": [0.9] * n,
        "ad_retrospective_trend": [fake_amt(0.01, 0.05) for _ in range(n)],
        "esrd_retrospective_trend": [fake_amt(0.01, 0.05) for _ in range(n)],
        "ad_completion_factor": [fake_amt(0.9, 1.0) for _ in range(n)],
        "esrd_completion_factor": [fake_amt(0.9, 1.0) for _ in range(n)],
        "stop_loss_payout_neutrality_factor": [1.0] * n,
        "perf_yr": ["2026"] * n,
        "clndr_yr": [rng.choice(["2025", "2026"]) for _ in range(n)],
        "clndr_mnth": ["{:02d}".format(rng.randint(1, 12)) for _ in range(n)],
        "bnmrk": [rng.choice(["A", "B", "C"]) for _ in range(n)],
        "align_type": [rng.choice(["claims", "voluntary"]) for _ in range(n)],
        "bnmrk_type": ["benchmark"] * n,
        "clm_type_cd": [rng.choice(["40", "50", "60", "71", "72", "81", "82"]) for _ in range(n)],
        "clm_pmt_amt_agg": [fake_amt(1000.0, 500000.0) for _ in range(n)],
        "sqstr_amt_agg": [fake_amt(0.0, 10000.0) for _ in range(n)],
        "apa_rdctn_amt_agg": [fake_amt(0.0, 5000.0) for _ in range(n)],
        "ucc_amt_agg": [fake_amt(0.0, 1000.0) for _ in range(n)],
        "op_dsh_amt_agg": [fake_amt(0.0, 1000.0) for _ in range(n)],
        "cp_dsh_amt_agg": [fake_amt(0.0, 1000.0) for _ in range(n)],
        "op_ime_amt_agg": [fake_amt(0.0, 1000.0) for _ in range(n)],
        "cp_ime_amt_agg": [fake_amt(0.0, 1000.0) for _ in range(n)],
        "aco_amt_agg_apa": [fake_amt(0.0, 500.0) for _ in range(n)],
        "srvc_month": ["{:04d}-{:02d}".format(rng.choice([2025, 2026]), rng.randint(1, 12)) for _ in range(n)],
        "efctv_month": ["{:04d}-{:02d}".format(rng.choice([2025, 2026]), rng.randint(1, 12)) for _ in range(n)],
        "apa_cd": [rng.choice(["A", "B"]) for _ in range(n)],
        "processed_at": ["2026-04-15T00:00:00"] * n,
        "source_file": ["reach_bnmr"] * n,
        "source_filename": ["P.D0259.ACO.BNMR.D260202.T100000001"] * n,
        "file_date": ["2026-02-02"] * n,
        "medallion_layer": ["bronze"] * n,
        "nonpbp_rdct_amt_agg": [fake_amt(0.0, 500.0) for _ in range(n)],
    })


BUILDERS = {
    "alr": build_alr,
    "bar": build_bar,
    "cclf1": build_cclf1,
    "cclf2": build_cclf2,
    "cclf3": build_cclf3,
    "cclf5": build_cclf5,
    "cclf6": build_cclf6,
    "consolidated_alignment": build_consolidated_alignment,
    "mexpr_data_claims": build_mexpr_data_claims,
    "mexpr_data_enroll": build_mexpr_data_enroll,
    "reach_bnmr_claims": build_reach_bnmr_claims,
}


def main() -> None:
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    for name, builder in BUILDERS.items():
        df = builder()
        out = FIXTURE_DIR / f"{name}.parquet"
        df.write_parquet(out)
        print(f"  wrote {out.relative_to(FIXTURE_DIR.parent.parent.parent.parent)}  ({df.height} x {df.width})")


if __name__ == "__main__":
    main()
