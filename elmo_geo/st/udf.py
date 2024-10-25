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


def st_clean(sdf: SparkDataFrame, column: str = "geometry") -> SparkDataFrame:
    """Uses mapInPandas to clean a spark geometry field to 1m precision using GeoPandas functions."""

    def _clean(iterator):
        for pdf in iterator:
            pdf[column] = gpd.GeoSeries.from_wkb(pdf[column]).force_2d().simplify(1).set_precision(1).remove_repeated_points(1).make_valid().to_wkb()
            yield pdf

    return (
        sdf.withColumn(column, F.expr(f"ST_AsBinary({column})"))
        .transform(lambda sdf: sdf.mapInPandas(_clean, sdf.schema))
        .withColumn(column, F.expr(f"ST_GeomFromWKB({column})"))
    )


def st_explode(sdf: SparkDataFrame) -> SparkDataFrame:
    return (
        sdf.withColumn("geometry", F.expr("ST_AsBinary(geometry)"))
        .withColumn("geometry", st_dump_to_list("geometry"))
        .withColumn("geometry", F.explode("geometry"))
        .withColumn("geometry", F.expr("ST_GeomFromWKB(geometry)"))
    )


def st_union(sdf: SparkDataFrame, keys: list[str] | str = ["id_parcel"], col: str = "geometry") -> SparkDataFrame:
    """Group geometries of different types, using geopandas

    Example
    ```py
    sf = (
        'dbfs:/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_reference_parcels/'
        'format_GEOPARQUET_rpa_reference_parcels/LATEST_rpa_reference_parcels/reference_parcels.parquet'
    )
    sdf = (spark.read.parquet(sf)
        .limit(1000)
        .selectExpr(
            'CONCAT(SHEET_ID, PARCEL_ID) AS id_parcel',
            'ST_GeomFromWKB(GEOM) AS geometry',
        )
        .transform(st_union, ['id_parcel'], 'geometry')
    )
    sdf.display()
    ```
    """
    if isinstance(keys, str):
        keys = [keys]

    def _fn(pdf):
        gdf = gpd.GeoDataFrame(pdf, geometry=gpd.GeoSeries.from_wkb(pdf[col]))
        return gdf.dissolve(by=keys).reset_index().to_wkb()

    _sdf = sdf.select(*keys, col).withColumn(col, F.expr(f"ST_AsBinary({col})"))
    return _sdf.groupby(keys).applyInPandas(_fn, _sdf.schema).withColumn(col, F.expr(f"ST_GeomFromWKB({col})"))
