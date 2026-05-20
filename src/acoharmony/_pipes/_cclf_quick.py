# © 2025 HarmonyCares
# All rights reserved.

"""
CCLF Quick Pipeline using Polars Rust Plugin

High-performance Silver → Gold processing that stays lazy throughout
and produces three final parquets with no intermediate writes.

Outputs:
- medical_claim.parquet (institutional + professional + DME)
- pharmacy_claim.parquet (Part D events)
- eligibility.parquet (beneficiary demographics)
"""

import logging

from ..medallion import MedallionLayer

logger = logging.getLogger(__name__)


def run_cclf_quick(executor) -> None:
    """
    Run the quick CCLF pipeline using Rust plugin.

    This pipeline processes Silver → Gold with:
    - No intermediate parquet writes
    - Full lazy execution throughout
    - Streaming sink for memory efficiency
    """
    try:
        import acoharmony_polars as ap
    except ImportError as e:
        logger.error(
            "acoharmony-polars plugin not installed. "
            "Install with: cd acoharmony-polars && maturin develop --release"
        )
        raise ImportError(
            "acoharmony-polars plugin required for cclf_quick pipeline"
        ) from e

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)
    gold_path = storage.get_path(MedallionLayer.GOLD)

    # Ensure gold directory exists
    gold_path.mkdir(parents=True, exist_ok=True)

    # Configure streaming from profile
    polars_config = executor.profile_config.get("polars", {})
    chunk_size = polars_config.get("streaming_chunk_size", 50000)
    max_threads = polars_config.get("max_threads", 12)
    ap.configure_streaming(chunk_size, max_threads)

    logger.info(f"Running cclf_quick pipeline (chunk_size={chunk_size}, threads={max_threads})")

    # Process medical claims (institutional + professional + DME)
    logger.info("Processing medical_claim...")
    ap.process_medical_claim(
        str(silver_path),
        str(gold_path / "medical_claim.parquet"),
        chunk_size,
    )
    logger.info(f"  [OK] medical_claim → {gold_path / 'medical_claim.parquet'}")

    # Process pharmacy claims (Part D)
    logger.info("Processing pharmacy_claim...")
    ap.process_pharmacy_claim(
        str(silver_path),
        str(gold_path / "pharmacy_claim.parquet"),
        chunk_size,
    )
    logger.info(f"  [OK] pharmacy_claim → {gold_path / 'pharmacy_claim.parquet'}")

    # Process eligibility
    logger.info("Processing eligibility...")
    ap.process_eligibility(
        str(silver_path),
        str(gold_path / "eligibility.parquet"),
        chunk_size,
    )
    logger.info(f"  [OK] eligibility → {gold_path / 'eligibility.parquet'}")

    logger.info("cclf_quick pipeline completed successfully")
