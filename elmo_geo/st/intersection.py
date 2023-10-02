"""Functions for performing joins on vector intersections - depreciated in favour of
joins.spatial_join()"""

import pyspark.sql.functions as F

from elmo_geo import LOG
from elmo_geo.utils.types import SparkSession


def intersect_parcels(
    spark: SparkSession,
    path_read: str,
    path_write: str,
    path_parcels: str,
    threshold: float = 1e-6,
    n_parcel_partitions=10000,
):
    DeprecationWarning('"elmo_geo.st.intersection.intersect_parcels" is partially replaced by "elmo_geo.st.join.overlap"')
    """Intersect a geospatial feature dataset with the parcels dataset and return inner join
    with geometry info removed. The output is persisted to parquet.

    Note:
        This approach is good for polygons as it removes noise/insignificant intersections using
        the threshold, but it means it won't work for point/line features as they have no area!

    Parameters:
        spark: The spark session object
        dataset: The name of the dataset - this must have been pre-processed into a partitioned
            parquet file here: "/mnt/lab/unrestricted/elmo/{dataset}/polygons.parquet"
        threshold: Matches below this threshold parcel area proportion will be ignored
        path_read: Path to read from - must be a pre-processed parquet file with geometry column
        path_write: Path to write the resulting parquet file to
    """

    if threshold > 1 or threshold < 0:
        raise ValueError("threshold must be between 0 and 1")

    # read the datasets in and process the geometries
    LOG.info(f"Reading in the parcels from {path_parcels}")
    df_parcels = (
        spark.read.parquet(path_parcels)  # .limit(1000)
        # .withColumn("geometry", F.hex(F.col("geometry")))
        .withColumn("parcel_geom", F.expr("ST_GeomFromWKB(hex(geometry))")).repartition(
            n_parcel_partitions
        )
    )
    LOG.info(f"Reading in the dataset from {path_read}")
    df_feature = (
        spark.read.parquet(path_read)
        # .withColumn("geometry", F.hex(F.col("geometry")))
        .withColumn("feature_geom", F.expr("ST_GeomFromWKB(hex(geometry))"))
    )

    feature_cols = [col for col in df_feature.columns if col not in ["geometry", "feature_geom"]]

    # Join the parcels dataset to the feature where the two geometries intersect
    LOG.info("Joining the two datasets where the geometries intersect")
    df_parcels.createOrReplaceTempView("parcels")
    df_feature.createOrReplaceTempView("feature")
    result = spark.sql(
        """
        SELECT p.parcel_geom, p.id as id_parcel, f.feature_geom, {cols}
        FROM parcels p, feature f
        WHERE ST_Intersects(p.parcel_geom, f.feature_geom)
    """.format(
            cols=", ".join(f"f.{col}" for col in feature_cols)
        )
    )

    # result = result.localCheckpoint()

    result = (
        result
        # Calculate the intersection geometry
        .withColumn("intersection_geom", F.expr("ST_Intersection(parcel_geom, feature_geom)"))
        # Calculate the area of the parcel and the intersection
        .withColumn("parcel_area", F.expr("ST_Area(parcel_geom)"))
        .withColumn("intersection_area", F.expr("ST_Area(intersection_geom)"))
        # calculate the % of the parcel that intersects with the feature
        .withColumn("proportion", F.col("intersection_area") / F.col("parcel_area"))
        # generate parition col from first 4 chars of BNG
        .withColumn("partition", F.substring("id_parcel", pos=1, len=4))
        # group up the result and sum the proportions incase multiple polygons intersect
        # with the parcel
        .groupBy("id_parcel", "partition", *feature_cols)
        .sum("proportion")
        # rename - as groupby unhelpfully renames the col...
        .withColumnRenamed("sum(proportion)", "proportion")
        # if it is below the threshold, then drop it
        .filter(F.col("proportion") > threshold)
        # persist to parquet
        .write.partitionBy("partition")
        .format("parquet")
        .save(path_write, mode="overwrite")
    )
    LOG.info(f"Saved result to {path_write}")
