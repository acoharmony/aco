# © 2025 HarmonyCares
# All rights reserved.

"""
ED (Emergency Department) Classification expression builder.

 expressions for classifying emergency department visits
using the NYU ED Algorithm (Johnston et al.). The algorithm assigns probability
scores to ED visits across multiple classification categories.

Classification Categories:
- Non-Emergent: Could have been treated in a non-ED setting
- Emergent, Primary Care Treatable: Emergent but treatable in primary care
- Emergent, ED Care Needed, Preventable: Needed ED but potentially preventable
- Emergent, ED Care Needed, Not Preventable: Appropriately used ED
- Injury: Injury-related visit
- Mental Health: Mental health or psychiatric condition
- Alcohol: Alcohol-related visit
- Drug: Drug-related visit (excluding alcohol)

The algorithm is widely used for analyzing ED utilization and identifying
potentially avoidable ED visits.

References:
- Johnston et al., NYU Center for Health and Public Service Research
- https://wagner.nyu.edu/faculty/billings/nyued-background

"""

from typing import Any

import polars as pl

from .._decor8 import explain, timeit, traced
from ._registry import register_expression


@register_expression(
    "ed_classification",
    schemas=["gold"],
    dataset_types=["claims"],
    description="ED visit classification using NYU ED Algorithm",
)
class EdClassificationExpression:
    """
    Generate expressions for classifying ED visits using NYU ED Algorithm.

        This expression builder creates Polars expressions that classify emergency
        department visits based on diagnosis codes using the Johnston et al.
        algorithm. Each visit is assigned probability scores across multiple
        categories indicating the appropriateness and nature of the ED visit.

        The algorithm considers:
        1. Primary diagnosis code (ICD-10-CM)
        2. Mapping to Clinical Classification Software (CCS) categories
        3. Probability distributions from historical ED utilization patterns

        Configuration Structure:
            ```yaml
            ed_classification:
              # Column names from claims data
              diagnosis_column: diagnosis_code_1  # Primary diagnosis
              claim_id_column: claim_id

              # Algorithm parameters
              use_ccs_mapping: true  # Whether to use CCS grouping
            ```

        Output Structure:
            The expression generates probability columns for each category:
            - non_emergent: Probability this was non-emergent
            - emergent_primary_care: Probability treatable in primary care
            - emergent_ed_preventable: Probability ED needed but preventable
            - emergent_ed_not_preventable: Probability appropriate ED use
            - injury: Probability injury-related
            - mental_health: Probability mental health related
            - alcohol: Probability alcohol-related
            - drug: Probability drug-related
            - unclassified: Probability not classifiable

            Plus a primary classification (highest probability):
            - ed_classification_primary: The category with highest probability
    """

    @staticmethod
    @traced()
    @explain(
        why="ED classification build failed",
        how="Check configuration and diagnosis column exists",
        causes=["Invalid config", "Missing diagnosis column"],
    )
    @timeit(log_level="debug")
    def classify_ed_visits(
        ed_visits: pl.LazyFrame,
        johnston_mapping: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Classify ED visits using Johnston algorithm probabilities.

                Args:
                    ed_visits: LazyFrame containing ED visits with diagnosis codes
                    johnston_mapping: LazyFrame with ICD-10 to probability mappings
                    config: Configuration dict with column names

                Returns:
                    LazyFrame with ED classification probability columns added
        """
        diag_col = config.get("diagnosis_column", "diagnosis_code_1")
        config.get("claim_id_column", "claim_id")
        classified = ed_visits.join(
            johnston_mapping.select(
                [
                    pl.col("icd10").alias("icd_10_cm"),
                    pl.col("edcnnpa").alias("emergent_ed_not_preventable"),
                    pl.col("edcnpa").alias("emergent_ed_preventable"),
                    pl.col("epct").alias("emergent_primary_care"),
                    pl.col("noner").alias("non_emergent"),
                    pl.col("injury"),
                    pl.col("psych").alias("mental_health"),
                    pl.col("alcohol"),
                    pl.col("drug"),
                ]
            ),
            left_on=diag_col,
            right_on="icd_10_cm",
            how="left",
        )

        probability_cols = [
            "emergent_ed_not_preventable",
            "emergent_ed_preventable",
            "emergent_primary_care",
            "non_emergent",
            "injury",
            "mental_health",
            "alcohol",
            "drug",
        ]

        classified = classified.with_columns(
            [pl.col(col).fill_null(0.0) for col in probability_cols]
        )

        classified = classified.with_columns(
            [
                (
                    pl.lit(1.0)
                    - pl.sum_horizontal(
                        [
                            pl.col("emergent_ed_not_preventable"),
                            pl.col("emergent_ed_preventable"),
                            pl.col("emergent_primary_care"),
                            pl.col("non_emergent"),
                        ]
                    )
                )
                .clip(0.0, 1.0)
                .alias("unclassified")
            ]
        )

        classified = classified.with_columns(
            [
                pl.when(
                    pl.col("non_emergent")
                    >= pl.max_horizontal(
                        [
                            pl.col("emergent_primary_care"),
                            pl.col("emergent_ed_preventable"),
                            pl.col("emergent_ed_not_preventable"),
                            pl.col("injury"),
                            pl.col("mental_health"),
                            pl.col("alcohol"),
                            pl.col("drug"),
                            pl.col("unclassified"),
                        ]
                    )
                )
                .then(pl.lit("Non-Emergent"))
                .when(
                    pl.col("emergent_primary_care")
                    >= pl.max_horizontal(
                        [
                            pl.col("emergent_ed_preventable"),
                            pl.col("emergent_ed_not_preventable"),
                            pl.col("injury"),
                            pl.col("mental_health"),
                            pl.col("alcohol"),
                            pl.col("drug"),
                            pl.col("unclassified"),
                        ]
                    )
                )
                .then(pl.lit("Emergent, Primary Care Treatable"))
                .when(
                    pl.col("emergent_ed_preventable")
                    >= pl.max_horizontal(
                        [
                            pl.col("emergent_ed_not_preventable"),
                            pl.col("injury"),
                            pl.col("mental_health"),
                            pl.col("alcohol"),
                            pl.col("drug"),
                            pl.col("unclassified"),
                        ]
                    )
                )
                .then(pl.lit("Emergent, ED Care Needed, Preventable"))
                .when(
                    pl.col("emergent_ed_not_preventable")
                    >= pl.max_horizontal(
                        [
                            pl.col("injury"),
                            pl.col("mental_health"),
                            pl.col("alcohol"),
                            pl.col("drug"),
                            pl.col("unclassified"),
                        ]
                    )
                )
                .then(pl.lit("Emergent, ED Care Needed, Not Preventable"))
                .when(
                    pl.col("injury")
                    >= pl.max_horizontal(
                        [
                            pl.col("mental_health"),
                            pl.col("alcohol"),
                            pl.col("drug"),
                            pl.col("unclassified"),
                        ]
                    )
                )
                .then(pl.lit("Injury"))
                .when(
                    pl.col("mental_health")
                    >= pl.max_horizontal(
                        [pl.col("alcohol"), pl.col("drug"), pl.col("unclassified")]
                    )
                )
                .then(pl.lit("Mental Health Related"))
                .when(
                    pl.col("alcohol") >= pl.max_horizontal([pl.col("drug"), pl.col("unclassified")])
                )
                .then(pl.lit("Alcohol Related"))
                .when(pl.col("drug") >= pl.col("unclassified"))
                .then(pl.lit("Drug Related"))
                .otherwise(pl.lit("Unclassified"))
                .alias("ed_classification_primary")
            ]
        )

        return classified

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def build_preventable_ed_flag_expr() -> pl.Expr:
        """
        Build expression for potentially preventable ED visit flag.

                An ED visit is considered potentially preventable if it falls into:
                - Non-emergent
                - Emergent, Primary Care Treatable
                - Emergent, ED Care Needed, Preventable

                Returns:
                    Expression that creates preventable_ed_flag column
        """
        return (
            (
                pl.col("non_emergent")
                + pl.col("emergent_primary_care")
                + pl.col("emergent_ed_preventable")
            )
            > 0.5  # If more than 50% probability of being preventable
        ).alias("preventable_ed_flag")
