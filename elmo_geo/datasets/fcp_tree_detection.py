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
from pandera.dtypes import Float64, Int32
from pyspark.sql import Window

from elmo_geo.etl import Dataset, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import (
    pivot_wide_sdf,
    sjoin_parcels,
)
from elmo_geo.io.file import auto_repartition
from elmo_geo.utils.types import SparkDataFrame

from .boundary import boundary_segments
from .rpa_reference_parcels import reference_parcels


# Tree Detections Dataset - potentially temporary data used for a specific BTO request
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
    medallion="bronze",
    source="fcp",
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
    count_2m: Int32 = Field()
    count_4m: Int32 = Field()
    count_8m: Int32 = Field()
    count_12m: Int32 = Field()
    count_24m: Int32 = Field()


def prep_tree_point(sdf):
    return sdf.selectExpr("ST_GeomFromWKT(top_point) AS geometry")


def sjoin_boundary_count(
    parcels: Dataset | SparkDataFrame,
    boundary_segments: Dataset | SparkDataFrame,
    features: Dataset | SparkDataFrame,
    buffers: list[int] = [0, 2, 4, 8, 12, 24],
    **kwargs,
):
    """Count the number of feature geometries intersecting parcel boundary segments."""
    sdf_segments = boundary_segments if isinstance(boundary_segments, SparkDataFrame) else boundary_segments.sdf()

    # window used to avoid double counting geomerites within a parcel
    # eg where a feature intersects multiple buffered parcel boundary segments.
    window = Window.partitionBy("id_parcel", "buffer", "geometry_right").orderBy("distance")
    return (
        sjoin_parcels(parcels, features, distance=max(buffers), **kwargs)
        .join(sdf_segments, on="id_parcel")
        .withColumn("buffer", F.expr(f"EXPLODE(ARRAY{tuple(buffers)})"))
        .transform(auto_repartition, count_ratio=1e-5, cols=["id_parcel", "buffer"])
        .withColumn("geometry_buffer", F.expr("ST_Buffer(geometry, buffer)"))
        .withColumn("geometry_right", F.expr("EXPLODE(ST_Dump(geometry_right))"))
        .filter("ST_Intersects(geometry_buffer, geometry_right)")
        .withColumn("distance", F.expr("ST_Distance(geometry_right, geometry)"))
        .withColumn("rank", F.row_number().over(window))
        .groupby("id_parcel", "id_boundary", "buffer")
        .agg(
            F.expr("FIRST(m) as m"),
            F.expr("CAST(SUM(CASE WHEN rank=1 THEN 1 ELSE 0 END) as Int) as count"),
        )
        .transform(pivot_wide_sdf, name_col="buffer", value_col="count")
        .withColumnsRenamed({str(b): f"count_{b}m" for b in buffers})
        .fillna(0)
    )


fcp_boundary_tree_count = DerivedDataset(
    name="fcp_boundary_tree_count",
    medallion="silver",
    source="fcp",
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
        count_*m: Number of trees intersectin the boundary segment buffered at "*"
    """

    id_parcel: str = Field()
    count_0m: Int32 = Field()
    count_2m: Int32 = Field()
    count_4m: Int32 = Field()
    count_8m: Int32 = Field()
    count_12m: Int32 = Field()
    count_24m: Int32 = Field()


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
    sdf_segments = boundary_segments if isinstance(boundary_segments, SparkDataFrame) else boundary_segments.sdf()

    return (
        sjoin_parcels(parcels, features, distance=max(buffers), **kwargs)
        .join(sdf_segments, on="id_parcel")
        .withColumn("buffer", F.expr(f"EXPLODE(ARRAY{tuple(buffers)})"))
        .transform(auto_repartition, count_ratio=1e-5, cols=["id_parcel", "buffer"])
        .withColumn("geometry", F.expr("ST_Buffer(geometry, buffer)"))  # buffer segment geoms
        .groupby("id_parcel", "buffer")
        .agg(
            F.expr("FIRST(geometry_right) as geometry_tree"),
            F.expr("ST_Difference(FIRST(geometry_left), ST_Union_Aggr(geometry)) as geometry_parcel_interior"),
        )
        .withColumn("geometry_tree", F.expr("EXPLODE(ST_Dump(geometry_tree))"))
        .filter("ST_Intersects(geometry_parcel_interior, geometry_tree)")
        .groupby("id_parcel", "buffer")
        .agg(F.expr("CAST(COUNT(geometry_tree) as Int) as count"))
        .transform(pivot_wide_sdf, name_col="buffer", value_col="count")
        .withColumnsRenamed({str(b): f"count_{b}m" for b in buffers})
        .fillna(0)
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
