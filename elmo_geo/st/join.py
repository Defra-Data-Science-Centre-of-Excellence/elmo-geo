from pyspark.sql import functions as F
from sedona.core.spatialOperator import JoinQuery
from sedona.utils.adapter import Adapter

from elmo_geo.st.geometry import load_missing

# from elmo_geo import LOG
from elmo_geo.utils.dbr import spark
from elmo_geo.utils.types import SparkDataFrame


def sjoin_rdd(
    sdf_left: SparkDataFrame,
    sdf_right: SparkDataFrame,
    partitioning: str = "KDBTREE",
    num_partitions: int = 200,
    partition_right: bool = False,
    index_type: str = "RTREE",
    use_index: bool = True,
    consider_boundary_itersection: bool = True,
) -> SparkDataFrame:
    """Spatial Join using RDD"""
    # RDD
    rdd_left = Adapter.toSpatialRdd(sdf_left, "geometry")
    rdd_right = Adapter.toSpatialRdd(sdf_right, "geometry")
    rdd_left.analyze()
    rdd_right.analyze()
    # Build Tree
    if partition_right:
        rdd_right.spatialPartitioning(partitioning=partitioning, num_partitions=num_partitions)
        rdd_left.spatialPartitioning(rdd_right.getPartitioner())
    else:
        rdd_left.spatialPartitioning(partitioning=partitioning, num_partitions=num_partitions)
        rdd_right.spatialPartitioning(rdd_left.getPartitioner())
    if use_index:
        rdd_left.buildIndex(indexType=index_type, buildIndexOnSpatialPartitionedRDD=True)
    # Join
    rdd = JoinQuery.SpatialJoinQueryFlat(
        rdd_right,
        rdd_left,
        useIndex=use_index,
        considerBoundaryIntersection=consider_boundary_itersection,
    )
    return Adapter.toDf(spatialPairRDD=rdd, sparkSession=spark)


def sjoin_sql(
    sdf_left: SparkDataFrame,
    sdf_right: SparkDataFrame,
    distance: float = 0,
) -> SparkDataFrame:
    """Spatial Join, only returning keys
    Only suitable for minimal SparkDataFrames
    left.select('id_left', 'geometry')
    right.select('id_right', 'geometry')
    """
    # Distance Join
    if distance > 0:
        sdf_left = sdf_left.withColumn(
            "geometry",
            F.expr(f"ST_MakeValid(ST_Buffer(ST_MakeValid(ST_Buffer(geometry, 0.001)), {distance-0.001}))"),
        )
    # Add to SQL
    sdf_left.createOrReplaceTempView("left")
    sdf_right.createOrReplaceTempView("right")
    # Join
    sdf = spark.sql(
        """
        SELECT id_left, id_right
        FROM left JOIN right
        ON ST_Intersects(left.geometry, right.geometry)
    """,
    )  # nothing else does a quadtree, consider a manual partition rect tree?
    # Remove from SQL
    spark.sql("DROP TABLE left")
    spark.sql("DROP TABLE right")
    return sdf


def sjoin_partenv(
    sdf_left: SparkDataFrame,
    sdf_right: SparkDataFrame,
    distance: float = 0,
    lsuffix: str = "_left",
    rsuffix: str = "_right",
):
    """Partition-Envelope Spatial Join
    This does an envelope join, before doing the spatial join
    This should avoid Sedona's quadtree.
    """
    method = f"{distance} > ST_Distance" if distance else "ST_Intersects"
    sdf_left.withColumn("_pl", F.spark_partition_id()).createOrReplaceTempView("left")
    sdf_right.withColumn("_pr", F.spark_partition_id()).createOrReplaceTempView("right")
    sdf = spark.sql(
        f"""
        SELECT left.* EXCEPT (_pl), right.* EXCEPT (_pr)
        FROM (
            SELECT l._pl, r._pr
            FROM (
                SELECT _pl, ST_Envelope_Aggr(geometry{lsuffix}) AS bbox
                FROM left
                GROUP BY _pl
            ) AS l
            JOIN (
                SELECT _pr, ST_Envelope_Aggr(geometry{rsuffix}) AS bbox
                FROM right
                GROUP BY _pr
            ) AS r
            ON {method}(l.bbox, r.bbox)
        )
        JOIN left USING (_pl)
        JOIN right USING (_pr)
        WHERE {method}(left.geometry{lsuffix}, right.geometry{rsuffix})
    """,
    )
    spark.sql("DROP TABLE left")
    spark.sql("DROP TABLE right")
    return sdf


def sjoin(
    sdf_left: SparkDataFrame,
    sdf_right: SparkDataFrame,
    how: str = "inner",
    lsuffix: str = "_left",
    rsuffix: str = "_right",
    sjoin_fn: callable = sjoin_sql,
    **sjoin_kwargs,
) -> SparkDataFrame:
    """Spatial Join with how
    how: {inner, full, left, right}
    sjoin: {elmo_geo.st.join.sjoin_sql, elmo_geo.st.join.sjoin_rdd}
    """
    # ID
    sdf_left = sdf_left.withColumn("id_left", F.monotonically_increasing_id())
    sdf_right = sdf_right.withColumn("id_right", F.monotonically_increasing_id())
    # How
    how_outer = ["outer", "outer_left", "outer_right", "anti", "anti_left", "anti_right"]
    if how in how_outer:
        raise NotImplementedError(how_outer)
    how_left = "full" if how in ["left", "full"] else "inner"
    how_right = "full" if how in ["right", "full"] else "inner"
    # Spatial Join
    sdf = sjoin_fn(
        sdf_left.select("id_left", "geometry"),
        sdf_right.select("id_right", "geometry"),
        **sjoin_kwargs,
    )
    # Rename
    for col in sdf_left.columns:
        if col in sdf_right.columns:
            sdf_left = sdf_left.withColumnRenamed(col, col + lsuffix)
            sdf_right = sdf_right.withColumnRenamed(col, col + rsuffix)
    geometry_left, geometry_right = f"geometry{lsuffix}", f"geometry{rsuffix}"
    # Regular Join
    return (
        sdf.join(sdf_left, how=how_left, on="id_left")
        .join(sdf_right, how=how_right, on="id_right")
        .withColumn(geometry_left, load_missing(geometry_left))
        .withColumn(geometry_right, load_missing(geometry_right))
        .drop("id_left", "id_right")
    )


def overlap(
    sdf_left: SparkDataFrame,
    sdf_right: SparkDataFrame,
    lsuffix: str = "_left",
    rsuffix: str = "_right",
    **kwargs,
):
    geometry_left, geometry_right = "geometry" + lsuffix, "geometry" + rsuffix
    return (
        sjoin(sdf_left, sdf_right, lsuffix=lsuffix, rsuffix=lsuffix, **kwargs)
        # TODO: groupby
        .withColumn("geometry", F.expr(f"ST_Intersection({geometry_left}, {geometry_right})"))
        .withColumn("proportion", F.expr(f"ST_Area(geometry) / ST_Area({geometry_left})"))
        .drop(geometry_left, geometry_right)
    )
