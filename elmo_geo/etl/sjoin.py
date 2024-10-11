import geopandas as gpd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from elmo_geo.utils.dbr import spark


@F.pandas_udf("binary")
def udf_bbox(col):
    return gpd.GeoSeries.from_wkb(col).bbox


def sjoin_bbox(
    sdf_left: SparkDataFrame,
    sdf_right: SparkDataFrame,
    lsuffix: str = "_left",
    rsuffix: str = "_right",
    distance: float = 0,
    **kwargs,
) -> SparkDataFrame:
    """TODO"""
    # bbox
    if "bbox" not in sdf_left.columns:
        sdf_left = sdf_left.withColumn("bbox", udf_bbox("geometry"))
    if "bbox" not in sdf_right.columns:
        sdf_right = sdf_right.withColumn("bbox", udf_bbox("geometry"))
    # rename
    columns_overlap = set(sdf_left.columns).intersection(sdf_right.columns)
    sdf_left.withColumnsRenamed({col: col + lsuffix for col in columns_overlap}).createOrReplaceTempView("left")
    sdf_right.withColumnsRenamed({col: col + rsuffix for col in columns_overlap}).createOrReplaceTempView("right")
    # sjoin bbox
    sdf_left.createOrReplaceTempView("l")
    sdf_right.createOrReplaceTempView("r")
    sdf = spark.sql(
        """
        SELECT *
        FROM l JOIN r
        ON LEAST(bbox{0}[2], bbox{1}[2]) - GREATEST(bbox{0}[0], bbox{1}[0]) < {2}
        AND LEAST(bbox{0}[3], bbox{1}[3]) - GREATEST(bbox{0}[1], bbox{1}[1]) < {2}
    """.format(lsuffix, rsuffix, distance)
    )
    spark.sql("DROP TABLE left")
    spark.sql("DROP TABLE right")
    return sdf
