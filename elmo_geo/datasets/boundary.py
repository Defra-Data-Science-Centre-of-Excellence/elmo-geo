"""Boundaries from RPA Parcels segmentised.

## Boundary Segments
Objective is to create a dataset of linear features which are likely to represent a realistic buffer strip.
Segmentation can be done at each node, it can also be done to split 2 points at a maximum length.  However, this can create very short segments
(especially along bends and corners), and such simplification is also utilised to merge shorter segments together.

| Parameter  | Value |     |
| ---------- | ----- | --- |
| Tolerance  |   10m | The tolerance used for simplification.  Lower means more segments.
| Max Length |   50m | The maximum length of each segment.  Lower means more segments.

## Adjacent Boundaries
Identifies the proportion of each parcel boundary segment within a buffer distance of a boundary from another parcel.
Used to identify if this parcel owner would be responsible for both sides of the boundary.

## Hedge Boundaries
This is used to identify if the boundary is a hedgerow, and suitable for maintaining hedgerow actions.
This is missing other sylvan features such as relict hedgerows and woodland.

## Water Boundaries
This is used to identify if a boundary is beside water, and suitable for riparian actions.
Not yet included is the separation between flowing, still, and seasonal water.

## Heritage Wall Boundaries
These boundaries would be suitable for actions like dry stone wall maintenance, or Cornish and Devon hedge maintenance.
OS field boundaries is not yet included in NGD.  Only OSM data is used.

## Merged Output Boundary Dataset
This is a combined dataset, useful for land change analysis.
This includes assumptions such as setting a strict feature distance.
"""
from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from elmo_geo.etl import SRID, Dataset, DerivedDataset
from elmo_geo.etl.transformations import sjoin_boundary_proportion
from elmo_geo.st.segmentise import segmentise_with_tolerance
from elmo_geo.st.udf import st_udf

from .fcp_sylvan import fcp_relict_hedge_raw
from .hedges import rpa_hedges_raw
from .osm import osm_tidy
from .rpa_reference_parcels import reference_parcels


# Boundary
def segmentise_boundary(dataset: Dataset) -> SparkDataFrame:
    """Segmentise boundary using"""
    return (
        dataset.sdf()
        .withColumn("geometry", F.expr("ST_Boundary(geometry)"))
        .withColumn("geometry", F.expr("EXPLODE(ST_Dump(geometry))"))
        .transform(st_udf, segmentise_with_tolerance)
        .withColumn("geometry", F.expr("EXPLODE(ST_Dump(geometry))"))
        .selectExpr(
            "monotonically_increasing_id() AS id_boundary",
            "id_parcel",
            "geometry",
            "ST_Length(geometry) AS m",
        )
    )


class BoundarySegments(DataFrameModel):
    """Model for segmentised boundaries of RPA Reference Parcels
    Attributes:
        id_boundary: unique identifier, created with monotonically_increasing_id
        id_parcel:
        geometry: LineString geometries in EPSG:27700.
        m: length in meters.
    """

    id_boundary: int = Field(unique=True)
    id_parcel: str = Field()
    geometry: Geometry(crs=SRID) = Field()
    m: float = Field()


boundary_segments = DerivedDataset(
    name="boundary_segments",
    level0="silver",
    level1="elmo_geo",
    model=BoundarySegments,
    restricted=False,
    func=segmentise_boundary,
    dependencies=[reference_parcels],
)


# Adjacency
class SjoinBoundaries(DataFrameModel):
    """Model for features joined to boundaries.
    Attributes:
        id_boundary: boundary id, unique id there are no grouping columns.
        id_parcel: parcel id in which that boundary came from.
        m: length of the boundary geometry.
        *columns: any columns from the features dataset grouped by.
        proportion_*m: the proportion of the boundary segment intersecting with the feature geometry buffered at "*"
    """

    id_boundary: int = Field()
    id_parcel: str = Field()
    m: float = Field()
    proportion_0m: float = Field(ge=0, le=1)
    proportion_2m: float = Field(ge=0, le=1)
    proportion_8m: float = Field(ge=0, le=1)
    proportion_12m: float = Field(ge=0, le=1)
    proportion_24m: float = Field(ge=0, le=1)


def fn_pre_adj(sdf: SparkDataFrame) -> SparkDataFrame:
    return sdf.selectExpr("id_parcel AS id_parcel_right", "geometry")


def fn_post_adj(sdf: SparkDataFrame) -> SparkDataFrame:
    return sdf.filter("id_parcel != id_parcel_right")


boundary_adjacencies = DerivedDataset(
    name="boundary_adjacencies",
    level0="silver",
    level1="elmo_geo",
    model=SjoinBoundaries,
    restricted=False,
    func=partial(sjoin_boundary_proportion, columns=["id_parcel_right"], fn_pre=fn_pre_adj, fn_post=fn_post_adj),
    dependencies=[reference_parcels, boundary_segments, boundary_segments],
    is_geo=False,
)
"""Proportion of parcel boundaries intersected by boundaries of other parcels at different buffer distances."""


# Hedge
boundary_hedgerows = DerivedDataset(
    level0="silver",
    level1="elmo_geo",
    name="boundary_hedgerows",
    model=SjoinBoundaries,
    restricted=False,
    func=sjoin_boundary_proportion,
    dependencies=[reference_parcels, boundary_segments, rpa_hedges_raw],
    is_geo=False,
)


# Water
def fn_pre_water(sdf: SparkDataFrame) -> SparkDataFrame:
    return sdf.filter("theme = 'Water' AND description NOT LIKE '%Catchment'")


# boundary_water = DerivedDataset(
#     level0="silver",
#     level1="elmo_geo",
#     name="boundary_water",
#     model=SjoinBoundaries,
#     restricted=True,
#     func=partial(sjoin_boundary_proportion, fn_pre=fn_pre_water),
#     dependencies=[reference_parcels, boundary_segments, os_ngd_raw],
#     is_geo=False,
# )


# Wall
def fn_pre_wall(sdf: SparkDataFrame) -> SparkDataFrame:
    return sdf.filter("tags LIKE \"{%'barrier': 'wall'%}\" OR tags LIKE \"{%'wall':%}\"")


boundary_walls = DerivedDataset(
    level0="silver",
    level1="elmo_geo",
    name="boundary_walls",
    model=SjoinBoundaries,
    restricted=False,
    func=partial(sjoin_boundary_proportion, fn_pre=fn_pre_wall),
    dependencies=[reference_parcels, boundary_segments, osm_tidy],
    is_geo=False,
)


def fn_pre_relict(sdf: SparkDataFrame) -> SparkDataFrame:
    return sdf.drop("id_parcel").filter("geometry IS NOT NULL")


boundary_relict = DerivedDataset(
    level0="silver",
    level1="fcp",
    name="boundary_relict",
    model=SjoinBoundaries,
    restricted=False,
    func=partial(sjoin_boundary_proportion, fn_pre=fn_pre_relict),
    dependencies=[reference_parcels, boundary_segments, fcp_relict_hedge_raw],
    is_geo=False,
)


def _aggregate_boundary(sdf_boundary: SparkDataFrame, type: str, width: int = 2, bufs: tuple[int] = (0, 2, 4, 8, 12, 24)) -> SparkDataFrame:
    """Aggregate boundary proportions to give length and area totals for parcels."""
    return sdf_boundary.groupby("id_parcel").agg()


def _combine_boundary_length_and_area_totals(
    boundary_hedgerows: DerivedDataset,
    boundary_walls: DerivedDataset,
    boundary_relict: DerivedDataset,
    width: int = 2,
    buffers: list[int] = [0, 2, 4, 8, 12, 24],
) -> SparkDataFrame:
    """Joined boundary datasets together into single wider dataset.

    Additionally calculate areas of feature overlap based on proportion of boundary and
    assumed distance from boundary that features are relevant within. For example,
    area within 2m of a hedgerow field boundary is assigned hedgerow.
    """

    sdf = (
        boundary_hedgerows.sdf().withColumn("type", F.lit("hedge"))
        .unionByName(boundary_walls.sdf().withColumn("type", F.lit("wall")))
        .unionByName(boundary_relict.sdf().withColumn("type", F.lit("relict_hedge")))
    )
    return (
        sdf.groupby("id_parcel", "type").agg(
            *[F.expr(f"SUM(proportion_{b}m * m) as m_{b}m") for b in buffers],
            *[F.expr(f"SUM(proportion_{b}m * m * {width} / 1000) as ha_{b}m") for b in buffers],
        )
        # .groupby("id_parcel").pivot("type")
    )


class BoundaryTotalsModel(DataFrameModel):
    """Model for boudnary length and area totals for parcels.
    Attributes:
        id_parcel: Parcel id in which that boundary came from.
        m_hedge_*m: The length of the boundary intersected by hedgerows buffered at "*"
        m_wall_*m: The length of the boundary intersected by walls buffered at "*"
        m_relict_*m: The length of the boundary intersected by relict hedgerows buffered at "*"
        ha_hedge_*m: The area of the parcel within 2m of a boundary intersected by hedgerows buffered at "*"
        ha_wall_*m: The area of the parcel within 2m of a boundary intersected by walls buffered at "*"
        ha_relict_*m: The area of the parcel within 2m of a boundary intersected by relict hedgerow buffered at "*"
    """

    id_parcel: str = Field()

    m_hedge_0m: float = Field()
    m_hedge_2m: float = Field()
    m_hedge_4m: float = Field()
    m_hedge_8m: float = Field()
    m_hedge_12m: float = Field()
    m_hedge_24m: float = Field()
    ha_hedge_0m: float = Field()
    ha_hedge_2m: float = Field()
    ha_hedge_4m: float = Field()
    ha_hedge_8m: float = Field()
    ha_hedge_12m: float = Field()
    ha_hedge_24m: float = Field()

    m_wall_0m: float = Field()
    m_wall_2m: float = Field()
    m_wall_4m: float = Field()
    m_wall_8m: float = Field()
    m_wall_12m: float = Field()
    m_wall_24m: float = Field()
    ha_wall_0m: float = Field()
    ha_wall_2m: float = Field()
    ha_wall_4m: float = Field()
    ha_wall_8m: float = Field()
    ha_wall_12m: float = Field()
    ha_wall_24m: float = Field()

    m_relict_0m: float = Field()
    m_relict_2m: float = Field()
    m_relict_4m: float = Field()
    m_relict_8m: float = Field()
    m_relict_12m: float = Field()
    m_relict_24m: float = Field()
    ha_relict_0m: float = Field()
    ha_relict_2m: float = Field()
    ha_relict_4m: float = Field()
    ha_relict_8m: float = Field()
    ha_relict_12m: float = Field()
    ha_relict_24m: float = Field()


boundary_parcel_totals = DerivedDataset(
    level0="gold",
    level1="fcp",
    name="boundary_parcel_totals",
    model=BoundaryTotalsModel,
    restricted=False,
    func=_combine_boundary_length_and_area_totals,
    dependencies=[boundary_hedgerows, boundary_walls, boundary_relict],
    is_geo=False,
)
