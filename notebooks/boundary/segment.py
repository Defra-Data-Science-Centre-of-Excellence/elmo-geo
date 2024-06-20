# Databricks notebook source
# MAGIC %md
# MAGIC # Segment Parcel Boundaries
# MAGIC Objective is to create a dataset of linear features which are likely to represent a realistic buffer strip.
# MAGIC
# MAGIC Segmentation can be done at each node, it can also be done to split 2 points at a maximum length.  However, this can create very short segments
# MAGIC (especially along bends and corners), and such simplification is also utilised to merge shorter segments together.
# MAGIC
# MAGIC | Parameter | Value | |
# MAGIC |---|---|---|
# MAGIC | Tolerance | 10m | Collects nearby nodes to a single segment.  Higher means less segments. |
# MAGIC | Max Length | 50m | Splits long lengths between 2 nodes.  Lower means more segments. |
# MAGIC
# MAGIC ### Data
# MAGIC - Input: rpa-parcel
# MAGIC - Output: elmo_geo-boundary_segment

# COMMAND ----------

from datetime import datetime

import geopandas as gpd
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T
from shapely import LineString, MultiLineString, MultiPoint, Point, segmentize

from elmo_geo import register
from elmo_geo.datasets.catalogue import add_to_catalogue, find_datasets
from elmo_geo.io import to_gdf, to_parquet
from elmo_geo.plot.base_map import plot_gdf
from elmo_geo.utils.misc import load_sdf
from elmo_geo.utils.types import SparkDataFrame

register()


parcel = find_datasets("rpa-parcel-adas")[-1]

date = datetime.today().strftime("%Y_%m_%d")
path = "/".join(parcel["silver"].split("/")[:-1])
name = f"elmo_geo-boundary_segment-{date}"

boundary_segment = {"name": name, "tasks": {}, "silver": f"{path}/{name}.parquet"}

boundary_segment

# COMMAND ----------


def linear_to_multiline(geometry):
    if geometry.geom_type == "LineString":
        geometry = MultiLineString([geometry])
    return geometry


def linear_to_coords(geometry):
    geometry = linear_to_multiline(geometry)
    for geom in geometry.geoms:
        yield from geom.coords


def linear_to_multipoint(geometry):
    return MultiPoint(list(linear_to_coords(geometry)))


def closest_point_index(point: Point, points: MultiPoint) -> int:
    """Gets the index of the closest Point of a MultiPoint from a Point.
    Without constructing an RTree.
    """
    distances = [point.distance(p) for p in points.geoms]
    return distances.index(min(distances))


def segmentize_with_tolerance(geometry: LineString, tolerance: float = 10, length: float = 50) -> MultiLineString:
    """
    Segments a LineString into smaller segments based on tolerance and length limit.
    Uses segment on each vertex methodology, but adds a tolerance to create longer segments.
    Args:
        geometry (LineString): The input LineString geometry.
        tolerance (float): The tolerance used for simplification.
        length (float): The maximum length of each segment.
    Returns:
        MultiLineString: The segmented LineString.
    """
    original = linear_to_multipoint(segmentize(geometry, length))
    simplified = linear_to_multipoint(segmentize(geometry.simplify(tolerance), length))
    indices = sorted(list(set(closest_point_index(point, original) for point in simplified.geoms)))
    slices = indices[:-1], [*indices[1:-1], len(original.geoms) + 1]
    return MultiLineString([original.geoms[i : j + 1].geoms for i, j in zip(*slices)])


def st_udf(sdf: SparkDataFrame, fn: callable, geometry_column: str = "geometry"):
    """
    Applies a shapely geometry function to a SparkDataFrame.

    # Example using shapely.segmentize
    ```py
    (sdf
        .withColumn("geometry", F.expr("ST_Boundary(geometry)"))
        .transform(st_udf, lambda g: shapely.segmentize(g, 100))
    )
    ```
    """
    return (
        sdf.withColumn(geometry_column, F.expr(f"ST_AsBinary({geometry_column})"))
        .withColumn(geometry_column, F.pandas_udf(lambda col: gpd.GeoSeries.from_wkb(col).apply(fn).to_wkb(), T.BinaryType())(geometry_column))
        .withColumn(geometry_column, F.expr(f"ST_GeomFromWKB({geometry_column})"))
    )


# COMMAND ----------

tile = "NY97"
tile2 = "NY9271"

gdf = to_gdf(pd.read_parquet(parcel["silver"] + "/sindex=" + tile)).query(f"id_parcel.str.startswith('{tile2}')")[["id_parcel", "geometry"]]

gdf["segments"] = gdf.geometry.boundary.apply(segmentize_with_tolerance, tolerance=10, length=50)
segments_breaks = gdf["segments"].explode(index_parts=True).apply(lambda g: linear_to_multipoint(g).geoms[0])

ax = plot_gdf(gdf, color="darkgoldenrod", alpha=0.3, edgecolor="k")
gdf["segments"].plot(ax=ax)
segments_breaks.plot(ax=ax, color="k", markersize=6)

# COMMAND ----------


sdf = (
    load_sdf(parcel["silver"])
    # Groupby and collect polygon features to recreate the unchipped Parcel
    .withColumn("geometry", F.expr("ST_CollectionExtract(geometry, 3)"))
    .groupby("id_parcel")
    .agg(F.expr("ST_Union_Aggr(geometry) AS geometry"))
    # Segment boundary
    .withColumn("geometry", F.expr("ST_Boundary(geometry)"))
    .transform(lambda sdf: st_udf(sdf, segmentize_with_tolerance, "geometry"))
    .withColumn("geometry", F.expr("EXPLODE(ST_Dump(geometry))"))
)


sdf.display()
sdf.count()

# COMMAND ----------

to_parquet(sdf, boundary_segment["silver"], sindex_column=None)

add_to_catalogue([boundary_segment])
