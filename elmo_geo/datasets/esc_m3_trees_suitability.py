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
    boardleaved_suitability: float = Field(ge=0, le=1, nullable=True)
    boardleaved_suitability_quintile: Category = Field(coerce=True, nullable=True)
    coniferous_suitability: float = Field(ge=0, le=1, nullable=True)
    coniferous_suitability_quintile: Category = Field(coerce=True, nullable=True)
    riparian_suitability: float = Field(ge=0, le=1, nullable=True)
    riparian_suitability_quintile: Category = Field(coerce=True, nullable=True)


def _transform(esc_broadleaved: Dataset,
               esc_coniferous: Dataset,
               esc_riparian: Dataset) -> pd.DataFrame:
    """Calculate the average suitability for each parcel across all tree species for the
    broadleaved, coniferous, and riparian datasets."""

    def average_suitability(pdf: pd.DataFrame, tree_group: str) -> pd.DataFrame:
        suitability_pattern = r".*_suitability$"
        suitability_cols = [c for c in pdf.columns if re.match(suitability_pattern, c) is not None]

        df_suitability = (pdf
                        .rename(columns={"RLR_RW_REFERENCE_PARCELS_DEC_21_LPIS_REF": "id_parcel"})
                        .set_index("id_parcel")[suitability_cols]
                        .mean(axis=1)
                        .rename(f"{tree_group}_suitability")
                        .reset_index()
        )
        df_suitability[f"{tree_group}_suitability_quintile"] = pd.qcut(df_suitability[f"{tree_group}_suitability"], 5, labels = range(1,6))
        return df_suitability
    
    # Coniferious dataset has incorrect values in the RC columns. Drop these
    # TODO: Update with corrected coniferous dataset
    df_coniferous = esc_coniferous.pdf().drop(['RC_area', 'RC_suitability', 'RC_yield_class'], axis=1)

    return (average_suitability(esc_broadleaved.pdf(), "boardleaved").set_index("id_parcel")
                      .join(
                          average_suitability(df_coniferous, "coniferous").set_index("id_parcel")
                      )
                      .join(
                          average_suitability(esc_riparian.pdf(), "riparian").set_index("id_parcel")
                      )
                      .reset_index()
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
