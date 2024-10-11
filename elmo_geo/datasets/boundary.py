"""Boundaries from RPA Parcels segmentised.

## Boundary Segments
Objective is to create a dataset of linear features which are likely to represent a realistic buffer strip.
Segmentation can be done at each node, it can also be done to split 2 points at a maximum length.  However, this can create very short segments
(especially along bends and corners), and such simplification is also utilised to merge shorter segments together.

| Parameter  | Value |     |
| ---------- | ----- | --- |
| Tolerance  |   10m | Collects nearby nodes to a single segment.  Higher means less segments.
| Max Length |   50m | Splits long lengths between 2 nodes.  Lower means more segments.

## Adjacent Boundaries
Identify the adjacency between boundary segments and another parcel.
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
from pyspark.sql import functions as F

from elmo_geo.etl import SRID, Dataset, DerivedDataset
from elmo_geo.etl.transformations import sjoin_boundary_proportion
from elmo_geo.io.file import auto_repartition
from elmo_geo.st.segmentise import segmentise_with_tolerance, st_udf
from elmo_geo.utils.types import SparkDataFrame

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
        .transform(lambda sdf: st_udf(sdf, segmentise_with_tolerance, "geometry"))
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
    restricted=True,
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
    return sdf.selectExpr("id_parcel AS id_parcel_right", "geometry").transform(auto_repartition)


def fn_post_adj(sdf: SparkDataFrame) -> SparkDataFrame:
    return sdf.filter("id_parcel != id_parcel_right")


boundary_adjacencies = DerivedDataset(
    name="boundary_adjacencies",
    level0="silver",
    level1="elmo_geo",
    model=SjoinBoundaries,
    restricted=True,
    func=partial(sjoin_boundary_proportion, columns=["id_parcel_right"], fn_pre=fn_pre_adj, fn_post=fn_post_adj),
    dependencies=[boundary_segments, boundary_segments],
    is_geo=False,
)


# Hedge
boundary_hedgerows = DerivedDataset(
    level0="silver",
    level1="elmo_geo",
    name="boundary_hedgerows",
    model=SjoinBoundaries,
    restricted=True,
    func=sjoin_boundary_proportion,
    dependencies=[boundary_segments, rpa_hedges_raw],
    is_geo=False,
)


# Water
def fn_pre_water(sdf: SparkDataFrame) -> SparkDataFrame:
    return sdf.filter("theme = 'Water' AND description NOT LIKE '%Catchment'").transform(auto_repartition)


boundary_water = DerivedDataset(
    level0="silver",
    level1="elmo_geo",
    name="boundary_water",
    model=SjoinBoundaries,
    restricted=True,
    func=partial(sjoin_boundary_proportion, fn_pre=fn_pre_water),
    dependencies=[boundary_segments, os_ngd_raw],
    is_geo=False,
)


# Wall
def fn_pre_wall(sdf: SparkDataFrame) -> SparkDataFrame:
    return sdf.filter("json_tags LIKE '%Wall'")


boundary_walls = DerivedDataset(
    level0="silver",
    level1="elmo_geo",
    name="boundary_walls",
    model=SjoinBoundaries,
    restricted=True,
    func=partial(sjoin_boundary_proportion, fn_pre=fn_pre_wall),
    dependencies=[boundary_segments, osm_tidy],
    is_geo=False,
)


# TODO: Merge
