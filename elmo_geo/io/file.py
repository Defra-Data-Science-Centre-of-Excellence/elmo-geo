import geopandas as gpd
import pyarrow.parquet as pq
from geopandas.io.arrow import SUPPORTED_VERSIONS, _geopandas_to_arrow
from pyspark.sql import functions as F

from elmo_geo.st.index import centroid_index, sindex
from elmo_geo.utils.misc import dbfs, info_sdf, sh_run
from elmo_geo.utils.types import SparkDataFrame


def convert_file(f_in: str, f_out: str):
    sh_run(["./elmo_geo/io/ogr2gpq.sh", f_in, f_out], shell=False)


def repartitonBy(sdf: SparkDataFrame, by: str) -> SparkDataFrame:
    n = sdf.select(by).distinct().count()
    return sdf.repartition(n, by)


def to_parquet(sdf: SparkDataFrame, f: str, geometry_column: str = "geometry", sindex_column: str | None = "sindex", **kwargs):
    """SparkDataFrame to Parquet with WKB encoding by without metadata, partitioned by "sindex"
    This assumes a indexing method has been used externally.
    """
    _sdf = sdf
    if sindex_column is not None:
        _sdf = _sdf.transform(repartitonBy, sindex_column)
    if geometry_column is not None:
        _sdf = _sdf.withColumn(geometry_column, F.expr(f"ST_AsBinary({geometry_column})"))
    _sdf.write.parquet(dbfs(f, True), partitionBy=sindex_column, **kwargs)
    info_sdf(sdf, f)
    return sdf


def to_geoparquet_partitioned(sdf: SparkDataFrame, f: str, **kwargs):
    """SparkDataFrame to GeoParquet, partitioned by BNG index"""
    sdf = sdf.transform(sindex).transform(repartitonBy, "sindex")
    sdf.write.format("geoparquet").save(dbfs(f, True), partitionBy="sindex", **kwargs)
    info_sdf(sdf, f)
    return sdf


def gpd_to_partitioned_parquet(
    gdf: gpd.GeoDataFrame,
    path: str,
    index: bool | None = None,
    geometry_encoding: str = "wkb",
    write_covering_bbox: bool = False,
    compression: str = "snappy",
    partition_cols: list[str] | None = None,
    **kwargs,
) -> None:
    """`geopandas.GeoDataFrame` to partitioned parquet.

    Note:
        We want to use the experimental geoparquet 1.1 here which saves as geoarrow
        instead of WKB and adds a bounding box column for predicate pushdown.
        See geopandas docs for more info, but have left the default of WKB for now
        to avoit potential incompatibility.
        https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_parquet.html#geopandas.GeoDataFrame.to_parquet
    """
    schema_version = SUPPORTED_VERSIONS[-1]
    table = _geopandas_to_arrow(gdf, index=index, schema_version=schema_version)
    pq.write_to_dataset(
        table,
        path,
        compression=compression,
        partition_cols=partition_cols,
        **kwargs,
    )


def to_parquet_partitioned(sdf: SparkDataFrame, f: str, **kwargs):
    """SparkDataFrame to Parquet, partitioned by BNG index"""
    sdf = sdf.transform(sindex).transform(repartitonBy, "sindex")
    sdf.withColumn("geometry", F.expr("ST_AsBinary(geometry)")).write.parquet(dbfs(f, True), partitionBy="sindex", **kwargs)
    info_sdf(sdf, f)
    return sdf


def to_geoparquet_sorted(sdf: SparkDataFrame, f: str, **kwargs):
    """SparkDataFrame to GeoParquet, sorted by BNG index"""
    sdf = sdf.transform(centroid_index, resolution="1km").sort("sindex")
    sdf.write.format("geoparquet").save(dbfs(f, True), **kwargs)
    info_sdf(sdf, f)
    return sdf


def to_geoparquet_zsorted(sdf: SparkDataFrame, f: str, **kwargs):
    """SparkDataFrame to GeoParquet, sorted by geohash index"""
    sdf = (
        sdf.transform(sindex, resolution="1km", index_fn="chipped_index")
        .withColumn("geohash", F.expr('ST_GeoHash(ST_FlipCoordinates(ST_Transform(geometry, "EPSG:27700", "EPSG:4326")))'))
        .sort("geohash")
    )
    sdf.write.format("geoparquet").save(dbfs(f, True), **kwargs)
    info_sdf(sdf, f)
    return sdf


to_gpq = to_geoparquet_partitioned
