"""Engalnd Tree map produced from tree dectation classification of LiDAR Vegetation Object Model data.

The outputs fromt he tree detection process are available as a source dataset. Derived datasets join these
tree detections to parcels and parcel boundaries.

A quality assurance review of the tree detection dataset is available in share point
([Evidence_QA_template Tree Detection](https://defra.sharepoint.com/:x:/r/teams/Team1645/Evidence%20and%20Analysis%20WS/4.9_Workstream_Areas_Modelling_Strategy/4.9.7%20Modelling%20Strategy%20Documentation/Sylvan_Features/Lidar%20Tree%20Detection/Evidence_QA_template%20Tree%20Detection.xlsm?d=w04f7b4159fbd4517b89dcc949b122cf0&csf=1&web=1&e=7jP6hB))

The [tree-detection](https://github.com/Defra-Data-Science-Centre-of-Excellence/tree-detection) GitHub repository
contains the code used to produce the tree detections.
"""
from functools import partial

import pyspark.sql.functions as F
from pandera import DataFrameModel, Field
from pandera.dtypes import Float64

from elmo_geo.etl import Dataset, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import pivot_wide_sdf, sjoin_boundaries, sjoin_boundary_count
from elmo_geo.io.file import auto_repartition
from elmo_geo.utils.types import SparkDataFrame

from .boundary import boundary_segments
from .rpa_reference_parcels import reference_parcels


# Tree Detctions Dataset - potentially temporay data used for a specific BTO request
class FCPTreeDetectionsRaw(DataFrameModel):
    """Model for raw tree detection data before parcel joins and counts.
    Attributes:
        top_x:easting spatial reference for point location of a tree
        top_y:northing spatial reference for point location of a tree
        top_height: height of identified tree (maybe meters, need to confirm)
        chm_path:source of lidar the tree detection is derived from
        msg: field for annotations
        top_point: point geometry
        crown_poly_raster: polygon geometry of the crown of the tree
        major_grid:possibly OS grid location i.e. SO

    """

    top_x: Float64 = Field()
    top_y: Float64 = Field()
    top_height: Float64 = Field()
    chm_path: str = Field()
    msg: str = Field()
    top_point: str = Field()
    crown_poly_raster: str = Field()
    major_grid: str = Field()


fcp_tree_detection_raw = SourceDataset(
    name="fcp_tree_detection_raw",
    level0="bronze",
    level1="fcp",
    model=FCPTreeDetectionsRaw,
    restricted=False,
    is_geo=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm/elmo/tree_features/tree_detections/tree_detections_202311231323.parquet/",
)
"""LiDAR derived tree map of England. Prodives estimated coordinate of peak of the tree crown and polygon of tree crown.
It was created by DSMT FCP with the [tree-detection](https://github.com/Defra-Data-Science-Centre-of-Excellence/tree-detection)
GitHub repository."""


class FCPTBoundaryTreeCounts(DataFrameModel):
    """Model for counts of trees intersecting parcel boudaries.
    Attributes:
        id_parcel: parcel id in which that boundary came from.
        id_boundary: boundary id, unique id for each parcel boudnary segment.
        m: length of the boundary geometry.
        count_*m: Number of trees intersectin the boundary segment buffered at "*"
    """

    id_boundary: int = Field()
    id_parcel: str = Field()
    m: float = Field()
    count_2m: float = Field(ge=0, le=1)
    count_8m: float = Field(ge=0, le=1)
    count_12m: float = Field(ge=0, le=1)
    count_24m: float = Field(ge=0, le=1)


def prep_tree_point(sdf):
    return sdf.selectExpr("ST_GeomFromWKT(top_point) AS geometry")


fcp_boundary_tree_count = DerivedDataset(
    medallion="silver",
    source="fcp",
    name="fcp_boundary_tree_count",
    model=FCPTBoundaryTreeCounts,
    restricted=False,
    func=partial(sjoin_boundary_count, fn_pre=prep_tree_point),
    dependencies=[reference_parcels, boundary_segments, fcp_tree_detection_raw],
    is_geo=False,
)
"""Counts fof trees intersecting parcel boundary segments."""


class FCPTInteriorTreeCounts(DataFrameModel):
    """Model for counts of trees intersecting parcel boudaries.
    Attributes:
        id_parcel: parcel id in which that boundary came from.
        m: length of the boundary geometry.
        count_*m: Number of trees intersectin the boundary segment buffered at "*"
    """

    id_parcel: str = Field()
    m: float = Field()
    count_0m: float = Field(ge=0, le=1)
    count_2m: float = Field(ge=0, le=1)
    count_8m: float = Field(ge=0, le=1)
    count_12m: float = Field(ge=0, le=1)
    count_24m: float = Field(ge=0, le=1)


def sjoin_interior_count(
    parcels: Dataset | SparkDataFrame,
    boundary_segments: Dataset | SparkDataFrame,
    features: Dataset | SparkDataFrame,
    buffers: list[float] = [0, 2, 4, 8, 12, 24],
    **kwargs,
):
    """Counts the number of trees in a parcel's interior.

    Parcel interios defined by the difference between buffered boundary segment geometries
    and the parcel geometry.
    """
    expr = "ST_Difference(ST_Union_Aggr(geometry), geometry_left)"
    expr = f"ST_Intersection({expr}, geometry_right) as interior_intersection"
    return (
        sjoin_boundaries(parcels, boundary_segments, features, distance=max(buffers), **kwargs)
        .withColumn("buffer", F.expr(f"EXPLODE(ARRAY{tuple(buffers)})"))
        .withColumn("geometry", F.expr("ST_Buffer(geometry, buffer)"))  # buffer segment geoms
        .transform(auto_repartition, multiplier=1.5)
        .groupby("id_parcel", "buffer", "geometry_left", "geometry_right")
        .agg(F.expr(expr))  # intersection between features and parcel interior
        .withColumn("interior_intersection", F.expr("EXPLODE(ST_Dump(interior_intersection))"))
        .filter("NOT ST_IsEmpty(interior_intersection)")
        .groupby("id_parcel", "buffer")
        .agg(F.expr("COUNT(interior_intersection) as count"))
        .transform(pivot_wide_sdf, name_col="buffer", value_col="count")
        .withColumnsRenamed({str(b): f"count_{b}m" for b in buffers})
    )


fcp_interior_tree_count = DerivedDataset(
    medallion="silver",
    source="fcp",
    name="fcp_interior_tree_count",
    model=FCPTInteriorTreeCounts,
    restricted=False,
    func=partial(sjoin_interior_count, fn_pre=prep_tree_point),
    dependencies=[reference_parcels, boundary_segments, fcp_tree_detection_raw],
    is_geo=False,
)
"""Counts of trees itersecting parcel interiors."""
