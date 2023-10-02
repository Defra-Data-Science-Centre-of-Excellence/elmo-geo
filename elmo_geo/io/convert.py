from elmo_geo.utils.types import (
    BaseGeometry,
    GeoDataFrame,
    Geometry,
    GeoSeries,
    PandasDataFrame,
    SedonaType,
    SparkDataFrame,
    SparkSession,
    Union,
)
from pyspark.sql import functions as F


def GeoDataFrame_to_PandasDataFrame(df: GeoDataFrame) -> PandasDataFrame:
    return df.to_wkb()


def PandasDataFrame_to_SparkDataFrame(
    df: PandasDataFrame, spark: SparkSession = None
) -> SparkDataFrame:
    return spark.createDataFrame(df)


def GeoDataFrame_to_SparkDataFrame(gdf: GeoDataFrame) -> SparkDataFrame:
    return gdf.pipe(GeoDataFrame_to_PandasDataFrame).pipe(PandasDataFrame_to_SparkDataFrame)


def SparkDataFrame_to_PandasDataFrame(df: SparkDataFrame) -> PandasDataFrame:
    for column in df.columns:
        if isinstance(df.schema[column].dataType, SedonaType):
            df = df.withColumn(column, F.expr(f"ST_AsBinary({column})"))
    return df.toPandas()


def PandasDataFrame_to_GeoDataFrame(
    pdf: PandasDataFrame, column: str, crs: Union[int, str]
) -> GeoDataFrame:
    return GeoDataFrame(pdf, geometry=GeoSeries.from_wkb(pdf[column], crs=crs), crs=crs).drop(
        columns=[column]
    )


def SparkDataFrame_to_GeoDataFrame(
    df: SparkDataFrame, column: str, crs: Union[int, str]
) -> GeoDataFrame:
    return SparkDataFrame_to_PandasDataFrame(df).pipe(
        PandasDataFrame_to_GeoDataFrame, column=column, crs=crs
    )


def to_gdf(
    x: Union[SparkDataFrame, Geometry], column: str = "geometry", crs: Union[int, str] = 27700
) -> GeoDataFrame:
    """Convert anything-ish to GeoDataFrame"""
    if isinstance(x, GeoDataFrame):
        gdf = x
    elif isinstance(x, SparkDataFrame):
        gdf = SparkDataFrame_to_GeoDataFrame(x, column=column, crs=crs)
    elif isinstance(x, PandasDataFrame):
        gdf = PandasDataFrame_to_GeoDataFrame(x, column=column, crs=crs)
    elif isinstance(x, GeoSeries):
        gdf = x.to_GeoDataFrame(crs=crs)
    elif isinstance(x, BaseGeometry):
        gdf = GeoSeries(x).to_GeoDataFrame(crs=crs)
    else:
        raise TypeError(f"Unknown type: {type(x)}")
    return gdf


def to_sdf(x: Union[SparkDataFrame, Geometry], crs: Union[int, str] = None) -> SparkDataFrame:
    """Convert anything-ish to SparkDataFrame"""
    if isinstance(x, SparkDataFrame):
        sdf = x
    elif isinstance(x, PandasDataFrame):
        sdf = PandasDataFrame_to_SparkDataFrame(x)
    else:
        sdf = GeoDataFrame_to_SparkDataFrame(to_gdf(x, crs))
    return sdf
