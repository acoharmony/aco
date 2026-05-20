#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""
Generate all project documentation from code.

This tool runs all documentation generators to create comprehensive
documentation for pipelines, connectors, and all namespaced modules.

"""

from acoharmony._log import get_logger
from .connectors import generate_full_documentation as generate_connectors
from .modules import generate_module_docs
from .pipelines import generate_full_documentation as generate_pipelines

logger = get_logger("dev.generate_all_docs")


def generate_all_documentation():
    """
    Generate all project documentation.

    Runs documentation generators for:
    - Namespaced module API reference (AST-based, all _* packages)
    - Pipeline modules (expressions and transforms)
    - Citation connectors (CMS, arXiv, etc.)
    """
    logger.info("=" * 60)
    logger.info("Starting comprehensive documentation generation")
    logger.info("=" * 60)

    # Generate module API reference (AST-based, no imports needed)
    logger.info("\nGenerating Module API Reference...")
    try:
        generate_module_docs()
        logger.info("[SUCCESS] Module API reference complete")
    except Exception as e:
        logger.error(f"[FAILED] Module API reference failed: {e}")
        raise

    # Generate pipeline documentation
    logger.info("\nGenerating Pipeline Documentation...")
    try:
        generate_pipelines()
        logger.info("[SUCCESS] Pipeline documentation complete")
    except Exception as e:
        logger.error(f"[FAILED] Pipeline documentation failed: {e}")
        raise

    # Generate connector documentation
    logger.info("\nGenerating Connector Documentation...")
    try:
        generate_connectors()
        logger.info("[SUCCESS] Connector documentation complete")
    except Exception as e:
        logger.error(f"[FAILED] Connector documentation failed: {e}")
        raise

    logger.info("\n" + "=" * 60)
    logger.info("[SUCCESS] All documentation generated successfully!")
    logger.info("=" * 60)
    logger.info("\nGenerated documentation:")
    logger.info("  - docs/docs/modules/  (Module API reference)")
    logger.info("  - docs/pipelines/     (Pipeline architecture and modules)")
    logger.info("  - docs/citations/     (Citation connector modules)")
    logger.info("\nTo serve documentation:")
    logger.info("  cd docs && npm start")


if __name__ == "__main__":
    generate_all_documentation()
