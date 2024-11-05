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
from functools import partial, reduce

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from elmo_geo.etl import SRID, Dataset, DerivedDataset
from elmo_geo.etl.transformations import sjoin_boundary_proportion
from elmo_geo.st.segmentise import segmentise_with_tolerance
from elmo_geo.st.udf import st_clean, st_udf

from .fcp_sylvan import fcp_relict_hedge_raw
from .hedges import rpa_hedges_raw
from .os import os_ngd_raw
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
    return sdf.filter("theme = 'Water' AND description NOT LIKE '%Catchment'").transform(st_clean, tollerance=2)


boundary_water_2m = DerivedDataset(
    level0="silver",
    level1="elmo_geo",
    name="boundary_water_2m",
    model=SjoinBoundaries,
    restricted=True,
    func=partial(sjoin_boundary_proportion, fn_pre=fn_pre_water),
    dependencies=[reference_parcels, boundary_segments, os_ngd_raw],
    is_geo=False,
)
"""Proportion of parcel boundary segmetns intersected by simplified waterbody geometries.

Simplified to 2m.
"""


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


# Relict
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


# Merger
def _transform_boundary_merger(
    boundary_adjacencies: Dataset,
    boundary_hedgerows: Dataset,
    boundary_relict: Dataset,
    boundary_walls: Dataset,
    boundary_water: Dataset,
) -> SparkDataFrame:
    """Joined boundary datasets together into single wider dataset.

    Set a threshold distance to based boundary proportion on. Default is 4m meaning that a feature must
    be within 4m of a aprcel boundary to be considered as intersecting that boundary.

    Then calculate the total length of parcel boundary intersected by each feature. Additionally estiamate the
    area of aprcel within different buffer distances from the feature boundary. This estimate double counts
    parcel corners where a feature is on adjacent boundary segments around a corner.
    """
    return (
        reduce(
            lambda x, y: x.join(y, on="id_boundary", how="outer"),
            (
                boundary_adjacencies.sdf().selectExpr("id_parcel", "id_boundary", "m", "CAST(0.5 < proportion_12m AS SMALLINT) AS bool_adjacency"),  # Assumption: 0.5<p12m
                boundary_hedgerows.sdf().selectExpr("id_boundary", "CAST(0.5 < proportion_12m AS SMALLINT) AS bool_hedgerow"),  # Assumption: 0.5<p12m
                boundary_relict.sdf().selectExpr("id_boundary", "CAST(0.5 < proportion_12m AS SMALLINT) AS bool_relict"),  # Assumption: 0.5<p12m
                boundary_walls.sdf().selectExpr("id_boundary", "CAST(0.5 < proportion_12m AS SMALLINT) AS bool_wall"),  # Assumption: 0.5<p12m
                boundary_water.sdf().selectExpr("id_boundary", "CAST(0.5 < proportion_12m AS SMALLINT) AS bool_water"),  # Assumption: 0.5<p12m
            ),
        )
        .withColumn("m_adj", F.expr("m * (2 - bool_adjacency) / 2 AS m_adj"))  # Buffer Strips are double sided, adjacency makes this single sided.
        .groupby("id_parcel")
        .agg(
            F.expr("SUM(m_adj * bool_hedgerow) AS m_hedgerow"),
            F.expr("SUM(m_adj * bool_relict) AS m_relict"),
            F.expr("SUM(m_adj * bool_wall) AS m_wall"),
            F.expr("SUM(m_adj * bool_water) AS m_water"),
        )
        .na.fill(0)
    )


class BoundaryMerger(DataFrameModel):
    """Model for boudnary length and area totals for parcels.
    Attributes:
        id_parcel: Parcel id in which that boundary came from.
        m_hedgerow: The length of the boundary intersected by hedgerows buffered by 4m
        m_relict: The length of the boundary intersected by relict hedgerows buffered by 4m
        m_wall: The length of the boundary intersected by walls buffered by 4m
        m_water: The length of the boundary intersected by waterbodies buffered by 4m
    """

    id_parcel: str = Field()
    m_hedgerow: float = Field()
    m_relict: float = Field()
    m_wall: float = Field()
    m_water: float = Field()


boundary_merger = DerivedDataset(
    level0="gold",
    level1="fcp",
    name="boundary_merger",
    model=BoundaryMerger,
    restricted=False,
    func=_transform_boundary_merger,
    dependencies=[boundary_adjacencies, boundary_hedgerows, boundary_relict, boundary_walls, boundary_water_2m],
    is_geo=False,
)
"""Total length of parcel boundaries intersected by hedgerows, walls, relict hedgerows and waterbodies at different buffer distances.

Also provide hectarage of parcel intersected by these features, given by the length of intersected boundary * buffer distances. This double
counts field corners where both edges of a field have a boundary features.
"""
