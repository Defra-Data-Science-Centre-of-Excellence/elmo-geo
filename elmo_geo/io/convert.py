from pyspark.sql import functions as F

from elmo_geo.utils.dbr import spark
from elmo_geo.utils.types import (
    BaseGeometry,
    DataFrame,
    GeoDataFrame,
    Geometry,
    GeoSeries,
    PandasDataFrame,
    SedonaType,
    SparkDataFrame,
    Union,
)


def to_gdf(
    x: Union[DataFrame, Geometry],
    column: str = "geometry",
    crs: Union[int, str] = 27700,
) -> GeoDataFrame:
    """Convert anything-ish to GeoDataFrame"""
    if isinstance(x, GeoDataFrame):
        gdf = x.set_geometry(column)
    elif isinstance(x, SparkDataFrame):
        for c in x.schema:
            if isinstance(c.dataType, SedonaType):
                x = x.withColumn(c.name, F.expr(f"ST_AsBinary({c.name})"))
        gdf = to_gdf(x.toPandas(), column, crs)
    elif isinstance(x, PandasDataFrame):
        gdf = GeoDataFrame(x, geometry=GeoSeries.from_wkb(x[column]))
    elif isinstance(x, GeoSeries):
        gdf = x.to_frame(name=column)
    elif isinstance(x, BaseGeometry):
        gdf = GeoSeries(x).to_frame(name=column)
    else:
        raise TypeError(f"Unknown type: {type(x)}")
    return gdf.set_crs(crs)


def to_sdf(
    x: Union[DataFrame, Geometry],
    column: str = "geometry",
    crs: Union[int, str] = 27700,
) -> SparkDataFrame:
    """Convert anything-ish to SparkDataFrame"""
    if isinstance(x, SparkDataFrame):
        sdf = x
    elif isinstance(x, Geometry):
        # GeoDataFrames and base geometries
        sdf = to_sdf(to_gdf(x, column, crs).to_wkb(), column, crs)
    elif isinstance(x, PandasDataFrame):
        sdf = spark.createDataFrame(x).withColumn(column, F.expr(f"ST_GeomFromWKB({column})"))
    else:
        raise TypeError(f"Unknown type: {type(x)}")
    return sdf
