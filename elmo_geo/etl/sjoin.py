import geopandas as gpd
from pyspark.sql import DataFrame as SparkDataFrame


def sjoin(
    sdf_left: SparkDataFrame,
    sdf_right: SparkDataFrame,
    lsuffix: str = "_left",
    rsuffix: str = "_right",
    distance: float = 0,
) -> SparkDataFrame:
    """TODO
    """
    # rename
    columns_overlap = set(sdf_left.columns).intersection(sdf_right.columns)
    sdf_left.withColumnsRenamed({col: col + lsuffix for col in columns_overlap}).createOrReplaceTempView("left")
    sdf_right.withColumnsRenamed({col: col + rsuffix for col in columns_overlap}).createOrReplaceTempView("right")
    # sjoin bbox
    sdf_left.createOrReplaceTempView("l")
    sdf_right.createOrReplaceTempView("r")
    sdf = spark.sql("""
        SELECT *
        FROM l JOIN r
        ON LEAST(bbox{0}[2], bbox{1}[2]) - GREATEST(bbox{0}[0], bbox{1}[0]) < distance
        AND LEAST(bbox{0}[3], bbox{1}[3]) - GREATEST(bbox{0}[1], bbox{1}[1]) < distance
    """.format(lsuffix, rsuffix))
    spark.sql("DROP TABLE left")
    spark.sql("DROP TABLE right")
    # sjoin intersects
    def _intersects(iterator):
        for pdf in iterator:
            geometry_left = gpd.GeoSeries.from_wkb(pdf[f"geometry{lsuffix}"])
            geometry_right = gpd.GeoSeries.from_wkb(pdf[f"geometry{rsuffix}"])
            intersects = geometry_left.distance(geometry_right) < distance if distance else geometry_left.intersects(geometry_right)
            return pdf.iloc[intersects]
    sdf = sdf.mapInPandas(_intersects, sdf.schema)
    return sdf