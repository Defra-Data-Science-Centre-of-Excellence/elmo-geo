"""Ecological Site Classification M3 woodland type suitability aggregated to RPA parcels.

This dataset is derived from Forest Research's Ecological Site Classification (ESC)[^1]. It
provides a parcel level suitability score for different tree species under the Representative
Concentration Pathway (RCP) 4.5 climate scenario. These scores are averaged to give a single
suitability score for each woodland type (broadleaved, riparian, coniferous).

The ESC produced by Forest Research is an input into EVAST. The source data used here 
was received from EVAST and has been aggregated to parcel level from the original 
ESC 1km grid resolution.

[^1] [Forest Research - Ecological Site Classification](https://www.forestresearch.gov.uk/tools-and-resources/fthr/ecological-site-classification)
"""

import pandas as pd
from pandera import DataFrameModel, Field
from pandera.dtypes import Category, Int8, Int16
from pyspark.sql import functions as F

from elmo_geo.etl import Dataset, DerivedDataset
from elmo_geo.utils.types import SparkDataFrame

from .fr_esc_m3_trees import esc_species_parcels


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
        id_parcel: Parcel ID
        nopeat_area_ha: Geographic area of parcel excluding intersecting peaty soils geometries, in hectares.
        woodland_type: Type of woodland modelled. Either
        rcp: Representing concetration pathway scenario (i.e cliamte change scenario)
        period_AA_T1: Time periods for annual average (AA) and T1 carbon values
        period_T2: period_T2: Time periods for T2 carbon values: 2021_2028, 2021_2036, 2021_2050, 2021_2100
        period_AA_T1_duration: Number of years in each time period (AA_T1)
        period_T2_duration: period_T2_duration: Number of years in each time period (T2): 8, 16, 30, 80
    """

    id_parcel: str = Field()
    nopeat_area: float = Field()
    woodland_type: Category = Field(
        isin=[
            "productive_conifer",
            "native_broadleaved",
            "riparian",
            "silvoarable",
            "wood_pasture",
        ],
        coerce=True,
    )
    rcp: Int8 = Field(isin=[26, 45, 60, 85], coerce=True)
    period_AA_T1: Category = Field(
        isin=[
            "2021_2028",
            "2029_2036",
            "2037_2050",
            "2051_2100",
        ],
        coerce=True,
    )
    period_T2: Category = Field(
        isin=[
            "2021_2050",
            "2021_2100",
            "2021_2036",
            "2021_2028",
        ],
        coerce=True,
    )
    period_AA_T1_duration: Int16 = Field(coerce=True)
    period_T2_duration: Int16 = Field(coerce=True)
    suitability: float = Field()
    n_species: Int8 = Field(coerce=True)


def _calculate_mean_suitability(sdf: SparkDataFrame) -> SparkDataFrame:
    """Calculate mean suitability over species within a parcel and model time frame.
    Calculates weighted mean based on area.
    """
    groupby_cols = [
        "id_parcel",
        "nopeat_area",
        "woodland_type",
        "rcp",
        "period_AA_T1",
        "period_T2",
        "period_AA_T1_duration",
        "period_T2_duration",
    ]

    return sdf.groupBy(*groupby_cols).agg(
        F.expr("COALESCE(SUM(area*suitability) / SUM(area), 0) as suitability"),  # weighted mean by area
        F.expr("COUNT(distinct species) as n_species"),
    )


def _transform(esc_species_parcels: Dataset) -> pd.DataFrame:
    """Produce single dataset indicating parcel level suitability for native broadleaved, productive conifer,
    riparian, woodland pasture, and silvoarable woodland types for multiple modelled time periods."""
    return esc_species_parcels.sdf().filter("species <> 'OPENSPACE'").transform(_calculate_mean_suitability).toPandas()


esc_woodland_suitability = DerivedDataset(
    name="esc_woodland_suitability",
    medallion="silver",
    source="forest_research",
    restricted=False,
    is_geo=False,
    func=_transform,
    dependencies=[esc_species_parcels],
    model=ESCTreeSuitabilityModel,
)
"""The Ecological Site Classification (ESC) tree suitability datasets aggregated to provide suitability scores 
for native broadleaved, productive conifer, riparian, woodland pasture, and silvoarable woodland types over different modelled
time periods for each parcel."""


def _filter_woodland_suitability_for_elmo(esc_woodland_suitability: DerivedDataset) -> pd.DataFrame:
    return (esc_woodland_suitability.sdf().filter("(rcp = 45) AND (period_AA_T1 = '2021_2028') AND (period_T2 = '2021_2028')")).toPandas()


esc_woodland_suitability_rcp45_2021_2028 = DerivedDataset(
    name="esc_woodland_suitability_rcp45_2021_2028",
    medallion="silver",
    source="forest_research",
    restricted=False,
    is_geo=False,
    func=_filter_woodland_suitability_for_elmo,
    dependencies=[esc_woodland_suitability],
    model=ESCTreeSuitabilityModel,
)
"""The Ecological Site Classification (ESC) tree suitability datasets aggregated to provide suitability scores 
for native broadleaved, productive conifer, riparian, woodland pasture, and silvoarable woodland types for the RCP 4.5
climate scenario and the 2021-2028 time period (forecast period closest to the present)."""
