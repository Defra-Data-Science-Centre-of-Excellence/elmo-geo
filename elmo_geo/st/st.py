from elmo_geo.utils.types import SparkDataFrame


def sjoin(
    sdf_left: SparkDataFrame,
    sdf_right: SparkDataFrame,
    lsuffix: str = "_left",
    rsuffix: str = "_right",
    distance: float = 0,
    spark=None,
) -> SparkDataFrame:
    DeprecationWarning('"elmo_geo.st.st.sjoin" is replaced by "elmo_geo.st.sjoin", which adds "how".')
    # Rename
    for col in sdf_left.columns:
        if col in sdf_right.columns:
            sdf_left = sdf_left.withColumnRenamed(col, col + lsuffix)
            sdf_right = sdf_right.withColumnRenamed(col, col + rsuffix)
    geometry_left = f"left.geometry{lsuffix}"
    geometry_right = f"right.geometry{rsuffix}"
    # Add to SQL
    sdf_left.createOrReplaceTempView("left")
    sdf_right.createOrReplaceTempView("right")
    # Join
    if distance == 0:
        # Intersects Join
        sdf = spark.sql(
            f"""
      SELECT left.*, right.*
      FROM left, right
      WHERE ST_Intersects({geometry_left}, {geometry_right})
    """
        )
    elif distance > 0:
        # Intersects+Distance Join
        sdf = spark.sql(
            f"""
      SELECT left.*, right.*
      FROM left, right
      WHERE ST_Intersects(ST_MakeValid(ST_Buffer({geometry_left}, {distance})), {geometry_right})
    """
        )
    elif False:
        # Distance Join
        sdf = spark.sql(
            f"""
      SELECT left.*, right.*
      FROM left, right
      WHERE ST_Distance({geometry_left}, {geometry_right}) < {distance}
    """
        )
    else:
        raise TypeError(f"distance should be positive real: {distance}")
    # Remove from SQL
    spark.sql("DROP TABLE left")
    spark.sql("DROP TABLE right")
    return sdf
