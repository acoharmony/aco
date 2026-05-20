# © 2025 HarmonyCares
# All rights reserved.

"""
Voluntary Alignment expression for intermediate transformations.
"""



from acoharmony._log import get_logger

from ._registry import register_expression

logger = get_logger(__name__)


@register_expression(
    "voluntary_alignment",
    schemas=["gold"],
    dataset_types=["alignment"],
    callable=False,
    description="Consolidate voluntary alignment data (SVA, PBVAR, emails)",
)
class VoluntaryAlignmentExpression:
    """
    Expression for creating comprehensive voluntary alignment tracking.

        This expression consolidates all beneficiary touchpoints across email campaigns,
        mailings, SVA signatures, and PBVAR alignments into a unified view of the
        voluntary alignment journey.
    """
