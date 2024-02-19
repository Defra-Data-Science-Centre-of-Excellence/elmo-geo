from pyspark.sql import functions as F

from elmo_geo.io.geometry import load_geometry
from elmo_geo.utils.dbr import spark
from elmo_geo.utils.types import (
    BaseGeometry,
    GeoDataFrame,
    Geometry,
    GeoSeries,
    PandasDataFrame,
    SedonaType,
    SparkDataFrame,
    Union,
)


def to_gdf(
    x: Union[SparkDataFrame, Geometry],
    column: str = "geometry",
    crs: Union[int, str] = 27700,
) -> GeoDataFrame:
    """Convert anything-ish to GeoDataFrame"""
    if isinstance(x, GeoDataFrame):
        gdf = x
    elif isinstance(x, SparkDataFrame):
        # TODO: withColumns
        for column in x.columns:
            if isinstance(x.schema[column].dataType, SedonaType):
                x = x.withColumn(column, F.expr(f"ST_AsBinary({column})"))
        gdf = to_gdf(x.toPandas(), column, crs)
    elif isinstance(x, PandasDataFrame):
        gdf = GeoDataFrame(
            x,
            geometry=GeoSeries.from_wkb(x[column], crs=crs),
            crs=crs,
        ).drop(columns=[column])
    elif isinstance(x, GeoSeries):
        gdf = x.to_GeoDataFrame(crs=crs)
    elif isinstance(x, BaseGeometry):
        gdf = GeoSeries(x).to_GeoDataFrame(crs=crs)
    else:
        raise TypeError(f"Unknown type: {type(x)}")
    return gdf


def to_sdf(
    x: Union[SparkDataFrame, Geometry],
    column: str = "geometry",
    crs: Union[int, str] = 27700,
) -> SparkDataFrame:
    """Convert anything-ish to SparkDataFrame"""
    if isinstance(x, SparkDataFrame):
        sdf = x
    elif isinstance(x, PandasDataFrame):
        sdf = spark.createDataFrame(x).withColumn(column, load_geometry(column))
    else:
        sdf = to_sdf(to_gdf(x, column, crs), column, crs)
    return sdf
