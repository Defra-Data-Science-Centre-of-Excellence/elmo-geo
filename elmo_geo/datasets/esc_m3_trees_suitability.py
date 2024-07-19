"""Ecological Site Classification M3 Tree Suitability.

This dataset is derived from Forest Research's Ecolocial Site Classification (ESC). It
provides a parcel level suitability score for different tree species under the RCP 4.5
climate scenario.

The ESC produced by Forest Research is an input into EVAST. The source data used here 
was received from EVAST and has been disaggregated to parcel level from the original 
ESC 1km grid resolution.

Background:
    - https://www.forestresearch.gov.uk/tools-and-resources/fthr/ecological-site-classification/#:~:text=ESC%20assesses%20the%20suitability%20of,communities%20defined%20in%20the%20NVC.
"""
import re
import pandas as pd
from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pyspark.sql import functions as F

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceDataset
from elmo_geo.utils.types import SparkDataFrame

esc_suitability_broadleaved_raw = SourceDataset(
    name="esc_suitability_broadleaved_raw",
    level0="bronze",
    level1="evast",
    restricted=False,
    is_geo=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/evast/M3_trees_rcp45/2024-07-04/EVAST_M3_native_broadleaved_rcp45.csv",
)
"""Definition for the raw sourced version of ESC broadleaved tree suitability dataset, recevied from EVAST."""

esc_suitability_coniferous_raw = SourceDataset(
    name="esc_suitability_coniferous_raw",
    level0="bronze",
    level1="evast",
    restricted=False,
    is_geo=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/evast/M3_trees_rcp45/2024-07-04/EVAST_M3_productive_conifer_rcp45.csv",
)
"""Definition for the raw sourced version of ESC coniferous tree suitability dataset, recevied from EVAST."""

esc_suitability_riparian_raw = SourceDataset(
    name="esc_suitability_riparian_raw",
    level0="bronze",
    level1="evast",
    restricted=False,
    is_geo=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/evast/M3_trees_rcp45/2024-07-04/EVAST_M3_riparian_rcp45.csv",
)
"""Definition for the raw sourced version of ESC riparian tree suitability dataset, recevied from EVAST."""

class ESCTreeSuitabilityModel(DataFrameModel):
    """Model describing the `esc_suitability_*` datasets.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        mean_species_suitability: The mean tree suitability score for that parcels, averaged over tree species.
        mean_species_suitability_quintile: The quintile of the mean tree suitability score.
    """
    id_parcel: str
    nopeatArea: float = Field(nullable=False)
        
    T1_2021_2028_T2_2021_2028_broadleaved_suitability: float = Field(ge=0, le=1, nullable=True)
    T1_2021_2028_T2_2021_2028_n_broadleaved_species: int
    T1_2029_2036_T2_2021_2036_broadleaved_suitability: float = Field(ge=0, le=1, nullable=True)
    T1_2029_2036_T2_2021_2036_n_broadleaved_species: int
    T1_2037_2050_T2_2021_2050_broadleaved_suitability: float = Field(ge=0, le=1, nullable=True)
    T1_2037_2050_T2_2021_2050_n_broadleaved_species: int
    T1_2051_2100_T2_2021_2100_broadleaved_suitability: float = Field(ge=0, le=1, nullable=True)
    T1_2051_2100_T2_2021_2100_n_broadleaved_species: int
    
    T1_2021_2028_T2_2021_2028_coniferous_suitability: float = Field(ge=0, le=1, nullable=True)
    T1_2021_2028_T2_2021_2028_n_coniferous_species: int
    T1_2029_2036_T2_2021_2036_coniferous_suitability: float = Field(ge=0, le=1, nullable=True)
    T1_2029_2036_T2_2021_2036_n_coniferous_species: int
    T1_2037_2050_T2_2021_2050_coniferous_suitability: float = Field(ge=0, le=1, nullable=True)
    T1_2037_2050_T2_2021_2050_n_coniferous_species: int
    T1_2051_2100_T2_2021_2100_coniferous_suitability: float = Field(ge=0, le=1, nullable=True)
    T1_2051_2100_T2_2021_2100_n_coniferous_species: int
    
    T1_2021_2028_T2_2021_2028_riparian_suitability: float = Field(ge=0, le=1, nullable=True)
    T1_2021_2028_T2_2021_2028_n_riparian_species: int
    T1_2029_2036_T2_2021_2036_riparian_suitability: float = Field(ge=0, le=1, nullable=True)
    T1_2029_2036_T2_2021_2036_n_riparian_species: int
    T1_2037_2050_T2_2021_2050_riparian_suitability: float = Field(ge=0, le=1, nullable=True)
    T1_2037_2050_T2_2021_2050_n_riparian_species: int
    T1_2051_2100_T2_2021_2100_riparian_suitability: float = Field(ge=0, le=1, nullable=True)
    T1_2051_2100_T2_2021_2100_n_riparian_species: int


def _convert_to_long_format(sdf: SparkDataFrame) -> SparkDataFrame:
    vars_pattern = r"^(\w+)_(suitability|area|yield_class)$"
    vars_cols = [c for c in sdf.columns if re.match(vars_pattern, c) is not None]
    other_cols = [c for c in sdf_c.columns if c not in vars_cols]
    species = list({re.match(species_pattern, c).groups()[0] for c in species_cols})

    sdf_long = None
    for s in species:
        sdf_sp = (sdf
            .withColumn("species", F.lit(s))
            .selectExpr(*other_cols,
                        "species",
                        *[f"{c} as {c[len(s)+1:]}" for c in vars_cols if c[:len(s)+1]==f"{s}_"]) # selects columns like BE_area as area
            .filter("area>0") # exclude parcels which have no area for a species
        )

        if sdf_long is None:
            sdf_long = sdf
        else:
            sdf_long = sdf_long.union(sdf)
    return sdf_long

def _calculate_mean_suitability(sdf: SparkDataFrame, woodland_type: str) -> SparkDataFrame:
    return (sdf_long.groupBy(["RLR_RW_REFERENCE_PARCELS_DEC_21_LPIS_REF", "period_AA_T1", "period_T2"])
 .agg(
    F.first("nopeatArea").alias("nopeatArea") ,
    F.expr(f"SUM(area*suitability) / SUM(suitability) as {woodland_type}_suitability"), # weighted mean by area
    F.expr("COUNT(distinct species) as n_species"),
 )
)

    
def _convert_to_wide_format(sdf: SparkDataFrame, woodland_type: str) -> SparkDataFrame:
    return (sdf
            .withColumn("T1_T2", F.expr("CONCAT('T1',period_AA_T1, '_T2_', period_T2 )"))
            .groupby("RLR_RW_REFERENCE_PARCELS_DEC_21_LPIS_REF", "nopeatArea")
            .pivot("T1_T2")
            .agg(
                F.first(f"{woodland_type}_suitability").alias("{woodland_type}_suitability"),
                F.first("n_species").alias("n_species"),
                )
            )

def _transform_single_woodland_type(sdf: Dataset, woodland_type:str) -> SparkDataFrame:
    return (sdf
            .pipe(_convert_to_long_format)
            .pipe(_calculate_mean_suitability, woodland_type)
            .pipe(_convert_to_wide_format, woodland_type)
            .withColumnsRenamed("id_parcel", "RLR_RW_REFERENCE_PARCELS_DEC_21_LPIS_REF")
    )


def _transform(esc_broadleaved: Dataset,
               esc_coniferous: Dataset,
               esc_riparian: Dataset) -> pd.DataFrame:
    """Calculate the average suitability for each parcel across all tree species for the
    broadleaved, coniferous, and riparian datasets."""

    
    # Coniferious dataset has incorrect values in the RC columns. Drop these
    # TODO: Update with corrected coniferous dataset
    sdf_coniferous = esc_coniferous.sdf().drop(['RC_area', 'RC_suitability', 'RC_yield_class'], axis=1)

    return (_transform_single_woodland_type(esc_broadleaved.sdf(), "boardleaved")
                   .join(
                       _transform_single_woodland_type(sdf_coniferous, "coniferous").drop("nopeatArea"),
                       on = ["id_parcel"]
                       )
                   .join(
                       _transform_single_woodland_type(esc_riparian.pdf(), "riparian").drop("nopeatArea"),
                       on = ["id_parcel"]
                       )
                   )

esc_tree_suitability = DerivedDataset(
    name="esc_tree_suitability",
    level0="silver",
    level1="evast",
    restricted=False,
    is_geo=False,
    func = _transform,
    dependencies = [esc_suitability_broadleaved_raw, esc_suitability_coniferous_raw, esc_suitability_riparian_raw],
    model = ESCTreeSuitabilityModel,
)
"""Definition for the ESC tree suitability dataset aggregated to provide a single broadleaved tree suitability score per parcel."""
