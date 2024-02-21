import geopandas as gpd
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T
from shapely.ops import transform

from elmo_geo.utils.types import SparkDataFrame


@F.pandas_udf(T.BinaryType())
def remove_z(geoms: pd.Series) -> pd.Series:
    """Remove the z coordinate from WKB geometries"""
    return gpd.GeoSeries.from_wkb(geoms).map(lambda g: transform(lambda x, y, z=None: (x, y), g)).to_wkb()


@F.pandas_udf(T.BooleanType())
def has_z(geoms: pd.Series) -> pd.Series:
    """Identify whether WKB geometries have a z coordinate"""
    return gpd.GeoSeries.from_wkb(geoms).has_z


@F.pandas_udf(T.BooleanType())
def is_valid(geoms: pd.Series) -> pd.Series:
    """Identify whether WKB geometries are valid"""
    return gpd.GeoSeries.from_wkb(geoms).is_valid


@F.udf(T.ArrayType(T.BinaryType()))
def st_dump_to_list(col):
    gs = gpd.GeoSeries.from_wkb([col])
    return gs.explode().wkb.tolist()


def st_explode(sdf: SparkDataFrame) -> SparkDataFrame:
    return (
        sdf.withColumn("geometry", F.expr("ST_AsBinary(geometry)"))
        .withColumn("geometry", st_dump_to_list("geometry"))
        .withColumn("geometry", F.explode("geometry"))
        .withColumn("geometry", F.expr("ST_GeomFromWKB(geometry)"))
    )


def st_union(sdf: SparkDataFrame, keys: list[str] | str = ["id_parcel"], col: str = "geometry") -> SparkDataFrame:
    if isinstance(keys, str):
        keys = [keys]

    def _fn(pdf):
        gdf = gpd.GeoDataFrame(pdf, geometry=gpd.GeoSeries.from_wkb(pdf[col]))
        return gdf.dissolve(by=keys).reset_index().to_wkb()

    _sdf = sdf.select(*keys, col).withColumn(col, F.expr(f"ST_AsBinary({col})"))
    return _sdf.groupby(keys).applyInPandas(_fn, _sdf.schema).withColumn(col, F.expr(f"ST_GeomFromWKB({col})"))
