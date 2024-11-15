"""ELMO's[^elmo] ingested Whole Farm Model (WFM).

[^elmo]: https://github.com/Defra-Data-Science-Centre-of-Excellence/elm_modelling_strategy/
"""
from pandera import DataFrameModel, Field
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from elmo_geo.etl import Dataset, DerivedDataset, SourceDataset

from .rpa_reference_parcels import reference_parcels

wfm_farms = SourceDataset(
    name="wfm_farms",
    level0="silver",
    level1="elmo",
    model=DataFrameModel,
    restricted=True,
    is_geo=False,
    source_path="/dbfs/FileStore/elmo_geo-uploads/wfm_farms_fe033c3b411c3a94de97ce4ac5573e0905bd78eb4c2dd0e88fb9d7b4529ee2a7.parquet",
)
"""WFM Farms created 2024-11-11.
Business level data from WFM.
This dataset is validated by ELMO.
"""

wfm_parcels = SourceDataset(
    name="wfm_parcels",
    level0="silver",
    level1="elmo",
    model=DataFrameModel,
    restricted=True,
    is_geo=False,
    source_path="/dbfs/FileStore/elmo_geo-uploads/wfm_parcels_fe033c3b411c3a94de97ce4ac5573e0905bd78eb4c2dd0e88fb9d7b4529ee2a7.parquet",
)
"""WFM Parcels created 2024-11-11.
Parcel level data from WFM.
This dataset is validated by ELMO.
"""


# WFM Info
class WfmInfo(DataFrameModel):
    """Model for a simplified WFM.

    Attributes:
        id_business: custid from WFM, approximates SBI/CPH/FRN and such a business/"farm".
        id_parcel: identifier for each parcel/field.
        ha_arable: land area used for arable produce, merely the sum of arable products, may be greater than geo-area.
        ha_grassland: land area used for grazing or fodder crops, which ignores grass purchases.
    """
    id_business: int = Field(nullable=True)
    id_parcels: str = Field()
    ha_arable: float = Field()
    ha_grassland: float = Field()
    x: float = Field()
    y: float = Field()


def _transform_wfm_info(reference_parcels: Dataset, wfm_parcels: Dataset, wfm_farms: Dataset) -> SparkDataFrame:
    """Create WFM business level Info from parcel geometries, WFM farms, and WFM fields."""
    arable = [
        "ha_winter_wheat",
        "ha_spring_wheat",
        "ha_winter_barley",
        "ha_spring_barley",
        "ha_winter_oats",
        "ha_spring_oats",
        "ha_oilseed_rape",
        "ha_peas",
        "ha_field_beans",
        "ha_grain_maize",
        "ha_potatoes",
        "ha_sugar_beet",
        "ha_other_crop",
        "ha_fodder_maize",
        "ha_temporary_pasture",
    ]

    grassland = [
        "ha_improved_grades_1_2",
        "ha_improved_grades_3_4_5",
        "ha_improved_disadvantaged",
        "ha_lowland_other",
        "ha_unimproved",
        "ha_unimproved_disadvantaged",
        "ha_disadvantaged",
        "ha_severely_disadvantaged",
        "ha_moorland",
        "ha_fenland",
    ]

    return (
        reference_parcels.sdf()
        .selectExpr(
            "id_parcel",
            "area_ha",
            "ST_X(ST_Centroid(geometry)) AS x",
            "ST_Y(ST_Centroid(geometry)) AS y",
        )
        .join(
            wfm_parcels.sdf().select("id_business", "id_parcel"),
            on="id_parcel",
        )
        .groupby("id_business")
        .agg(
            F.expr("CONCAT_WS(',', SORT_ARRAY(COLLECT_SET(id_parcel))) AS id_parcels"),
            F.expr("SUM(area_ha) AS area_ha"),
            F.expr("MEAN(x) AS x"),
            F.expr("MEAN(y) AS y"),
        )
        .join(
            (
                wfm_farms.sdf()
                .fillna(0)
                .selectExpr(
                    "id_business",
                    "CAST({} AS DOUBLE) AS ha_arable".format("+".join(arable)),
                    "CAST({} AS DOUBLE) AS ha_grassland".format("+".join(grassland)),
                )
            ),
            on="id_business",
        )
    )


wfm_info = DerivedDataset(
    name="wfm_info",
    level0="gold",
    level1="elmo",
    model=WfmInfo,
    restricted=True,
    is_geo=False,
    func=_transform_wfm_info,
    dependencies=[reference_parcels, wfm_parcels, wfm_farms],
)
"""A simplified WFM at business level.
This has often been used for reactive analysis, previously called "Business Info".
ha_arable and ha_grassland are used to define a coarser farm type "livestock vs arable".
The intention is that this is easier to digest than WFM in full.
"""
