import geopandas as gpd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def st_udf(
    sdf: SparkDataFrame,
    fn: callable,
    geometry_column: str = "geometry",
    return_type: str = T.BinaryType(),
):
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
    def _pudf(col):
        return gpd.GeoSeries.from_wkb(col).apply(fn).to_wkb()

    return (
        sdf.withColumn(geometry_column, F.expr(f"ST_AsBinary({geometry_column})"))
        .withColumn(geometry_column, _pudf(geometry_column))
        .withColumn(geometry_column, F.expr(f"ST_GeomFromWKB({geometry_column})"))
    )


def explode_geometries(gs: gpd.GeoSeries) -> gpd.GeoSeries:
    return gs.explode().wkb.tolist()


def clean_geometries(gs: gpd.GeoSeries) -> gpd.GeoSeries:
    return gs.force_2d().simplify(1).set_precision(1).remove_repeated_points(1).make_valid().to_wkb()


def st_clean(sdf: SparkDataFrame, column: str = "geometry") -> SparkDataFrame:
    """Clean a spark geometry field to 1m precision using GeoPandas functions."""
    return sdf.transform(st_udf, clean_geometries)


def st_explode(sdf: SparkDataFrame) -> SparkDataFrame:
    return (
        sdf.withColumn("geometry", F.expr("ST_AsBinary(geometry)"))
        .withColumn("geometry", explode_geometries("geometry"))
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
