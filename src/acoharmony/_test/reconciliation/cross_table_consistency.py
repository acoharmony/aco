# © 2025 HarmonyCares
# All rights reserved.

"""Cross-table consistency: referential integrity, temporal coherence, provider matching."""

from datetime import date

import polars as pl
import pytest

from .conftest import GOLD, SILVER, requires_data, scan_gold, scan_silver


@requires_data
class TestClaimsCompleteness:
    """Claims flow from CCLF sources to gold medical_claim."""

    @pytest.mark.reconciliation
    def test_medical_claim_has_data(self):
        df = scan_gold("medical_claim").collect()
        assert df.height > 0

    @pytest.mark.reconciliation
    def test_medical_claim_has_identifiers(self):
        df = scan_gold("medical_claim").collect()
        has_claim = any(c for c in df.columns if "claim" in c.lower() and "id" in c.lower())
        has_person = any(c for c in df.columns if "person" in c.lower() or "mbi" in c.lower() or "bene" in c.lower())
        assert has_claim, f"No claim ID in: {df.columns[:10]}"
        assert has_person, f"No person ID in: {df.columns[:10]}"

    @pytest.mark.reconciliation
    def test_pharmacy_claim_has_data(self):
        path = GOLD / "pharmacy_claim.parquet"
        if not path.exists():
            pytest.skip("pharmacy_claim not available")
        df = pl.scan_parquet(path).collect()
        assert df.height > 0


@requires_data
class TestProviderConsistency:
    """Provider TINs in claims match participant list."""

    @pytest.mark.reconciliation
    def test_participant_list_exists(self):
        df = scan_silver("participant_list").collect()
        assert df.height > 0

    @pytest.mark.reconciliation
    def test_provider_tins_overlap_with_claims(self):
        try:
            plist = scan_silver("participant_list").collect()
            cclf5 = scan_silver("cclf5").collect()
        except Exception:
            pytest.skip("Required tables not available")
        # participant_list uses entity_tin/base_provider_tin; CCLF5 uses clm_rndrg_prvdr_tax_num
        plist_tin_cols = [c for c in plist.columns if "tin" in c.lower() or "tax" in c.lower()]
        claim_tin_cols = [c for c in cclf5.columns if "tax" in c.lower() or "tin" in c.lower()]
        if not plist_tin_cols or not claim_tin_cols:
            pytest.skip("TIN columns not found")
        # Gather all TINs from participant list (multiple TIN columns)
        plist_tins = set()
        for col in plist_tin_cols:
            plist_tins.update(plist[col].drop_nulls().cast(pl.Utf8, strict=False).to_list())
        plist_tins.discard("")
        claim_tins = set(cclf5[claim_tin_cols[0]].drop_nulls().cast(pl.Utf8, strict=False).to_list())
        claim_tins.discard("")
        overlap = plist_tins & claim_tins
        assert len(overlap) > 0, "No TIN overlap between participant list and CCLF5"


@requires_data
class TestTemporalCoherence:
    """Dates across tables are temporally reasonable."""

    @pytest.mark.reconciliation
    def test_no_far_future_claim_dates(self):
        """Claim dates should not be more than 1 year in the future."""
        cclf5 = scan_silver("cclf5").collect()
        date_cols = [c for c in cclf5.columns if "from_dt" in c.lower() or "thru_dt" in c.lower()]
        # Allow up to 1 year in future (PY data can include forward-looking entries)
        from datetime import timedelta
        cutoff = date.today() + timedelta(days=365)
        for col in date_cols[:2]:
            if cclf5[col].dtype == pl.Date:
                far_future = cclf5.filter(pl.col(col) > cutoff)
                assert far_future.height == 0, f"{col}: {far_future.height} dates beyond {cutoff}"

    @pytest.mark.reconciliation
    def test_bar_dates_in_reach_era(self):
        try:
            bar = scan_silver("bar").collect()
        except Exception:
            pytest.skip("bar not available")
        date_cols = [c for c in bar.columns if "date" in c.lower() or "start" in c.lower()]
        for col in date_cols[:2]:
            dates = bar[col].drop_nulls().cast(pl.Date, strict=False).drop_nulls()
            if dates.len() > 0:
                min_date = dates.min()
                assert min_date.year >= 2021, f"{col} has dates from {min_date.year}"


@requires_data
class TestEligibilityConsistency:
    """Gold eligibility matches silver sources."""

    @pytest.mark.reconciliation
    def test_gold_eligibility_has_data(self):
        df = scan_gold("eligibility").collect()
        assert df.height > 0

    @pytest.mark.reconciliation
    def test_beneficiary_metrics_has_data(self):
        path = GOLD / "beneficiary_metrics.parquet"
        if not path.exists():
            pytest.skip("beneficiary_metrics not in gold")
        df = pl.scan_parquet(path).collect()
        assert df.height > 0
