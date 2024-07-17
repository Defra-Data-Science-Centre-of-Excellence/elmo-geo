from pyspark.sql import functions as F
from sedona.core.spatialOperator import JoinQuery
from sedona.utils.adapter import Adapter

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
    on: str = "geometry",
    lsuffix: str = "_left",
    rsuffix: str = "_right",
    distance: float = 0,
    knn: int = 0,
) -> SparkDataFrame:
    """Spatial Join using SQL"""
    if how != "inner":
        raise NotImplementedError("sjoin: inner spatial join only")
    if on != "geometry":
        raise NotImplementedError("sjoin: geometry_column must be named geometry")
    # Add to SQL, without name conflicts
    columns_overlap = set(sdf_left.columns).intersection(sdf_right.columns)
    sdf_left.withColumnsRenamed({col: col + lsuffix for col in columns_overlap}).createOrReplaceTempView("left")
    sdf_right.withColumnsRenamed({col: col + rsuffix for col in columns_overlap}).createOrReplaceTempView("right")
    # spatial join
    if distance == 0:
        sdf = spark.sql(
            f"""
            SELECT left.*, right.*
            FROM left JOIN right
            ON ST_Intersects(left.geometry{lsuffix}, right.geometry{rsuffix})
        """
        )
    else:
        sdf = spark.sql(
            f"""
            SELECT left.*, right.*
            FROM left JOIN right
            ON ST_Distance(left.geometry{lsuffix}, right.geometry{rsuffix}) <= {distance}
        """
        )
    if 0 < knn:
        raise NotImplementedError("sjoin: nearest neighbour not supported")
    # Remove from SQL
    spark.sql("DROP TABLE left")
    spark.sql("DROP TABLE right")
    return sdf


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


def knn(
    sdf_left,
    sdf_right,
    id_left: str,
    id_right: str,
    k: int = 1,
    distance_threshold: int = 5_000,
):
    """K-nearest neighbours within distance threshold.

    Parameters:
        sdf_left: Spark data frame with geometry field.
        sdf_right: Spark data frame with geometry field.
        id_left: Field in left dataframe to group entries by when finding nearest neighbour.
        id_left: Field in right dataframe to group entries by when finding nearest neighbour.
        k: Number of neighbours to return.
        distance_threshol: Maximum distance in meters of neighbours.

    Returns:
        SparkDataFrame
    """
    sdf_left.createOrReplaceTempView("left")
    sdf_right.createOrReplaceTempView("right")

    sdf = spark.sql(
        f"""
        SELECT {id_left}, {id_right}, distance, rank
        FROM (
            SELECT {id_left}, {id_right}, distance,
                ROW_NUMBER() OVER(PARTITION BY {id_left}, {id_right} ORDER BY distance ASC) AS rank
            FROM (
                SELECT left.{id_left}, right.{id_right},
                    ST_Distance(left.geometry, right.geometry) AS distance
                FROM left JOIN right
                ON ST_Distance(left.geometry, right.geometry) < {distance_threshold}
            )
        )
        WHERE rank <= {k}
        """
    )
    spark.sql("DROP TABLE left")
    spark.sql("DROP TABLE right")
    return sdf
