"""Ecological Site Classification M3 Tree Suitability.

This dataset is derived from Forest Research's Ecolocial Site Classification (ESC)[^1]. It
provides a parcel level suitability score for different tree species under the Representative
Concentration Pathway (RCP) 4.5 climate scenario.

The ESC produced by Forest Research is an input into EVAST. The source data used here 
was received from EVAST and has been disaggregated to parcel level from the original 
ESC 1km grid resolution.

[^1] [Forest Research - Ecological Site Classification](https://www.forestresearch.gov.uk/tools-and-resources/fthr/ecological-site-classification)
"""
import re

import pandas as pd
from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pyspark.sql import functions as F

from elmo_geo.etl import Dataset, DerivedDataset, SourceDataset
from elmo_geo.utils.types import SparkDataFrame

esc_suitability_broadleaved_raw = SourceDataset(
    name="esc_suitability_broadleaved_raw",
    level0="bronze",
    level1="forest_research",
    restricted=False,
    is_geo=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/evast/M3_trees_rcp45/2024-07-04/EVAST_M3_native_broadleaved_rcp45.csv",
)
"""Raw version of the Ecological Site Classification (ESC) broadleaved tree suitability dataset, received from EVAST."""

esc_suitability_coniferous_raw = SourceDataset(
    name="esc_suitability_coniferous_raw",
    level0="bronze",
    level1="forest_research",
    restricted=False,
    is_geo=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/evast/M3_trees_rcp45/2024-07-04/EVAST_M3_productive_conifer_rcp45.csv",
)
"""Raw version of the Ecological Site Classification (ESC) coniferous tree suitability dataset, received from EVAST."""

esc_suitability_riparian_raw = SourceDataset(
    name="esc_suitability_riparian_raw",
    level0="bronze",
    level1="forest_research",
    restricted=False,
    is_geo=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/evast/M3_trees_rcp45/2024-07-04/EVAST_M3_riparian_rcp45.csv",
)
"""Raw version of the Ecological Site Classification (ESC) riparian tree suitability dataset, received from EVAST."""


class ESCTreeSuitabilityModel(DataFrameModel):
    """Model describing the derived ESC tree suitability dataset.

    Dataframe provides a suitability score and number of species count for each parcel, time period and woodland type
    (broadleaved, coniferous, riparian).

    There are four scenarios over which tree suitability is modelled, with each having slightly
    different tree suitability scores. Each scenario is defined by the time period the model runs.
    | Time Period 1 | Time Period 2 |
    | ------------- | ------------- |
    | 2021-2028     | 2021-2028     |
    | 2029-2036     | 2021-2036     |
    | 2037-2050     | 2021-2050     |
    | 2051-2100     | 2021-2100     |

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`
        nopeatArea: Non peat area of parcel
        period_T1_T2: Time period over which the ESC tree model is run
        woodland_type: The woodland type of the suitability score. Either 'broadleaved', 'coniferous', or 'riparian'
        suitability: The woodland suitability score, ranges from 0 (low suitability) to 1 (high suitability)
        n_species: The number of tree species averaged to give the suitability score
    """

    id_parcel: str = Field(coerce=True, nullable=False)
    nopeatArea: float = Field(coerce=True, nullable=False)
    period_T1_T2: Category = Field(
        coerce=True, nullable=False, isin=["2029_2036-2021_2036", "2037_2050-2021_2050", "2051_2100-2021_2100", "2021_2028-2021_2028"]
    )
    woodland_type: Category = Field(coerce=True, nullable=False, isin=["coniferous", "broadleaved", "riparian"])
    suitability: float = Field(coerce=True, nullable=False)
    n_species: int = Field(coerce=True, nullable=False)


def _convert_to_long_format(sdf: SparkDataFrame) -> SparkDataFrame:
    """Select the area, sutability, and yield_class variables for a single
    species at a time and concatenate into a long format dataframe. Facilitates
    more convenient calculation of average suitability across species.
    """
    vars_pattern = r"^(\w+)_(suitability|area|yield_class)$"
    vars_cols = {c for c in sdf.columns if re.match(vars_pattern, c) is not None}
    other_cols = {c for c in sdf.columns if c not in vars_cols}
    species = {re.match(vars_pattern, c).groups()[0] for c in vars_cols}

    sdf_long = None
    for s in species:
        sdf_sp = (
            sdf.withColumn("species", F.lit(s))
            .selectExpr(
                *other_cols, "species", *[f"{c} as {c[len(s)+1:]}" for c in vars_cols if c[: len(s) + 1] == f"{s}_"]
            )  # selects columns like BE_area as area
            .filter("area>0")  # exclude parcels which have no area for a species
        )

        if sdf_long is None:
            sdf_long = sdf_sp
        else:
            sdf_long = sdf_long.unionByName(sdf_sp, allowMissingColumns=True)
    return sdf_long


def _calculate_mean_suitability(sdf: SparkDataFrame) -> SparkDataFrame:
    """Calculate mean suitability over species within a parcel and model time frame.
    Calculates weighted mean based on area.
    """
    return sdf.groupBy(["RLR_RW_REFERENCE_PARCELS_DEC_21_LPIS_REF", "period_AA_T1", "period_T2", "woodland_type"]).agg(
        F.first("nopeatArea").alias("nopeatArea"),
        F.expr("SUM(area*suitability) / SUM(suitability) as suitability"),  # weighted mean by area
        F.expr("COUNT(distinct species) as n_species"),
    )


def _transform(
    esc_broadleaved: Dataset,
    esc_coniferous: Dataset,
    esc_riparian: Dataset,
) -> pd.DataFrame:
    """Produce single dataset indicating parcel level suitability for broadleaved, coniferous, and riparian
    woodland types for multiple modelled time periods. Returned dataset has a single row per parcel id."""

    sdf_all = None
    for dataset, woodland_type in zip(
        [esc_broadleaved, esc_coniferous, esc_riparian],
        ["broadleaved", "coniferous", "riparian"],
    ):
        drop_cols = []
        if woodland_type == "coniferous":
            # Coniferious dataset has incorrect values in the RC columns. Drop these
            # TODO: Update with corrected coniferous dataset
            drop_cols = ["RC_area", "RC_suitability", "RC_yield_class"]

        # Convert each dataset to long format
        sdf = dataset.sdf().drop(*drop_cols).transform(_convert_to_long_format).withColumn("woodland_type", F.lit(woodland_type))

        # Combine into single dataframe
        if sdf_all is None:
            sdf_all = sdf
        else:
            sdf_all = sdf_all.unionByName(sdf, allowMissingColumns=False)

    # Return woodland type suitablity
    return (
        sdf_all.transform(_calculate_mean_suitability)
        .withColumn("period_T1_T2", F.expr("CONCAT(period_AA_T1, '-', period_T2)"))
        .drop("period_AA_T1", "period_T2")
        .withColumnRenamed("RLR_RW_REFERENCE_PARCELS_DEC_21_LPIS_REF", "id_parcel")
        .toPandas()
    )


esc_tree_suitability = DerivedDataset(
    name="esc_tree_suitability",
    level0="silver",
    level1="forest_research",
    restricted=False,
    is_geo=False,
    func=_transform,
    dependencies=[esc_suitability_broadleaved_raw, esc_suitability_coniferous_raw, esc_suitability_riparian_raw],
    model=ESCTreeSuitabilityModel,
)
"""The Ecological Site Classification (ESC) tree suitability datasets aggregated to provide tree suitability scores 
for broadleaved, coniferous, and riparian trees over different modelled time periods for each parcel."""
