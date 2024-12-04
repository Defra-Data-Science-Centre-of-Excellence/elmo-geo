from functools import partial

import geopandas as gpd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from_wkb = partial(gpd.GeoSeries.from_wkb, crs=27700)


def st_udf(sdf: SparkDataFrame, fn: callable, geometry_column: str = "geometry", return_type: str = T.BinaryType(), geometry_not_geoseries: bool = True):
    """Applies a shapely geometry function to a SparkDataFrame.
    # Example using shapely.segmentize
    ```py
    (sdf
        .withColumn("geometry", F.expr("ST_Boundary(geometry)"))
        .transform(st_udf, lambda g: shapely.segmentize(g, 100))
    )
    ```
    """

    @F.pandas_udf(return_type)
    def _udf(col):
        if geometry_not_geoseries:
            return from_wkb(col).apply(fn).to_wkb()
        else:
            return from_wkb(col).pipe(fn).to_wkb()

    return (
        sdf.withColumn(geometry_column, F.expr(f"ST_AsBinary({geometry_column})"))
        .withColumn(geometry_column, _udf(geometry_column))
        .withColumn(geometry_column, F.expr(f"ST_GeomFromWKB({geometry_column})"))
    )


def clean_geometries(gs: gpd.GeoSeries, tolerance=1) -> gpd.GeoSeries:
    return gs.force_2d().simplify(tolerance).make_valid().set_precision(1).remove_repeated_points(1).make_valid()


def st_clean(sdf: SparkDataFrame, column: str = "geometry", tolerance=1) -> SparkDataFrame:
    """Clean a spark geometry field to chosen precision using GeoPandas functions.

    Default precision is 1m.
    """
    return sdf.transform(st_udf, partial(clean_geometries, tolerance=tolerance), geometry_not_geoseries=False)


@F.udf(T.ArrayType(T.BinaryType()))
def dump_to_list(col):
    gs = from_wkb([col], crs=27700)
    return gs.explode().wkb.tolist()


def st_explode(sdf: SparkDataFrame) -> SparkDataFrame:
    return (
        sdf.withColumn("geometry", F.expr("ST_AsBinary(geometry)"))
        .withColumn("geometry", dump_to_list("geometry"))
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
        gdf = gpd.GeoDataFrame(pdf, geometry=from_wkb(pdf[col]))
        return gdf.dissolve(by=keys).reset_index().to_wkb()

    _sdf = sdf.select(*keys, col).withColumn(col, F.expr(f"ST_AsBinary({col})"))
    return _sdf.groupby(keys).applyInPandas(_fn, _sdf.schema).withColumn(col, F.expr(f"ST_GeomFromWKB({col})"))
