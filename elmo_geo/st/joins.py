"""Module for joining large geospatial datasets using spark"""

from typing import List, Optional

from pyspark.sql.functions import col, expr, round
from sedona.core.spatialOperator import JoinQuery
from sedona.utils.adapter import Adapter

from elmo_geo.utils.types import SparkDataFrame, SparkSession


def _get_column_names(df_left: SparkDataFrame, df_right: SparkDataFrame) -> List[str]:
    """Generate column names for the DataFrame resulting from a join
    Parameters:
        df_parcels: The parcels dataset
        df_features: The features dataset
    Returns:
        A list of column names
    Raises:
        KeyError: When either DataFrame is missing a column called `geometry`
    """
    for df in [df_left, df_right]:
        if "geometry" not in (cols := [f.name for f in df.schema]):
            raise KeyError(
                "Could not find `geometry` column in DataFrame. "
                f"Found the following columns: {cols}"
            )
    cols_parcels = ["geom_left"] + [f.name for f in df_left.schema if f.name != "geometry"]
    cols_features = ["geom_right"] + [f.name for f in df_right.schema if f.name != "geometry"]
    return cols_parcels + cols_features


def spatial_join(
    df_left: SparkDataFrame,
    df_right: SparkDataFrame,
    spark: SparkSession,
    partitioning: Optional[str] = None,
    num_partitions: int = 200,
    partition_right: bool = False,
    indexType: Optional[str] = None,
    useIndex: bool = True,
    considerBoundaryIntersection: bool = True,
    calculate_proportion: bool = True,
) -> SparkDataFrame:
    DeprecationWarning(
        '"elmo_geo.st.joins.spatial_join" can be replaced with "elmo_geo.st.sjoin", for RDDs see "elmo_geo.st.join.sjoin_rdd".'  # noqa:E501
    )
    """Spatially join two spark DataFrames of polygons using Sedona.
    Uses Sedona's python RDD api to perform partitioning and indexing.
    `df_left` and `df_right` must use the same CRS. `simplify` units will depend on the CRS.
    Parameters:
        df_left: The left DataFrame to join, must have `geometry` column
        df_right: The left DataFrame to join, must have `geometry` column
        spark: The current spark session
        partitioning: The partitioning scheme to use. Options: {'KDBTREE', 'QUADTREE'},
            defaults to 'KDBTREE'
        num_partitions: The number of partitions to use, defaults to 200
        partition_right: Whether to partition on the right geometries rather than the left (default)
        indexType: The index type to use. Options: {'RTREE', 'QUADTREE'}, defaults to 'RTREE'
        useIndex: Whether to use an index for the join
        considerBoundaryIntersection: Whether to include all intersections (ST_Intersects) (default)
            or just left geometries contained within the right (ST_Contains)
        calculate_proportion: Whether to calculate the proportion of the left geometry that
            intersects with the right (slower)
    Returns:
        A DataFrame of the resulting join
    Note:
        Troubleshooting...
        - Ensure geometries are valid (ST_MakeValid)
        - Ensure polygons are not more complex than they need to be (ST_SimplifyPreserveTopology)
        - Remove z dimension (ST_Force_2D)
        - Break up very complex geometries (ST_SubdivideExplode)
        - Adjust the `num_partitions` to suit the datasets
        - Ensure there is a `geometry` column in both dataframes
    """

    # convert to RDD
    rdd_left = Adapter.toSpatialRdd(df_left, "geometry")
    rdd_right = Adapter.toSpatialRdd(df_right, "geometry")
    rdd_left.analyze()
    rdd_right.analyze()

    # partition
    if partitioning is None:
        partitioning = "KDBTREE"

    if partition_right:
        rdd_right.spatialPartitioning(partitioning=partitioning, num_partitions=num_partitions)
        rdd_left.spatialPartitioning(rdd_right.getPartitioner())
    else:
        rdd_left.spatialPartitioning(partitioning=partitioning, num_partitions=num_partitions)
        rdd_right.spatialPartitioning(rdd_left.getPartitioner())

    # # index
    if indexType is None:
        indexType = "RTREE"
    if useIndex:
        rdd_left.buildIndex(indexType=indexType, buildIndexOnSpatialPartitionedRDD=True)

    # join
    result_pair_rdd = JoinQuery.SpatialJoinQueryFlat(
        rdd_right,
        rdd_left,
        useIndex=useIndex,
        considerBoundaryIntersection=considerBoundaryIntersection,
    )
    df = Adapter.toDf(spatialPairRDD=result_pair_rdd, sparkSession=spark).toDF(
        *_get_column_names(df_left, df_right)
    )

    if calculate_proportion:
        # calculate the proportion of the left geometry which intersects with the right
        df = (
            df.withColumn("geom_intersection", expr("ST_Intersection(geom_left, geom_right)"))
            .withColumn("area_left", expr("ST_Area(geom_left)"))
            .withColumn("area_intersection", expr("ST_Area(geom_intersection)"))
            .withColumn("proportion", col("area_intersection") / col("area_left"))
            .drop("area_left", "area_intersection", "geom_left", "geom_right", "geom_intersection")
        )
        # group up the result and sum the proportions in case multiple polygons with
        # the same attributes intersect with the parcel
        df = (
            df.groupBy(*[col for col in df.columns if col != "proportion"])
            .sum("proportion")
            .withColumn("proportion", round("sum(proportion)", 6))
            .drop("sum(proportion)")
        )
    return df
