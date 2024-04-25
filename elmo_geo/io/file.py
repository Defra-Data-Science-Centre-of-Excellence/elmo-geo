import os

from pyspark.sql import functions as F

from elmo_geo import LOG
from elmo_geo.st.index import centroid_index, chipped_index, sindex
from elmo_geo.utils.misc import dbfs, sh_run, info_sdf
from elmo_geo.utils.types import SparkDataFrame



def convert_file(f_in: str, f_out: str):
    sh_run(["./elmo_geo/io/ogr2gpq.sh", f_in, f_out], shell=False)


def repartitonBy(sdf: SparkDataFrame, by: str) -> SparkDataFrame:
    n = sdf.select(by).distinct().count()
    return sdf.repartition(n, by)


def to_pq(sdf: SparkDataFrame, f: str, **kwargs):
    """SparkDataFrame to Parquet with WKB encoding by without metadata, partitioned by "sindex"
    This assumes a indexing method has been used externally.
    """
    sdf = sdf.transform(repartitonBy, "sindex").withColumn("geometry", F.expr("ST_AsBinary(geometry)"))
    sdf.write.parquet.save(dbfs(f, True), partitionBy="sindex", **kwargs)
    info_sdf(sdf, f)
    return sdf

def to_gpq_partitioned(sdf: SparkDataFrame, f: str, **kwargs):
    """SparkDataFrame to GeoParquet, partitioned by BNG index"""
    sdf = sdf.withColumn("geometry", st_simplify()).transform(sindex).transform(repartitonBy, "sindex")
    sdf.write.format("geoparquet").save(dbfs(f, True), partitionBy="sindex", **kwargs)
    info_sdf(sdf, f)
    return sdf



def to_pq_partitioned(sdf: SparkDataFrame, f: str, **kwargs):
    """SparkDataFrame to Parquet, partitioned by BNG index"""
    sdf = sdf.transform(sindex).transform(repartitonBy, "sindex").withColumn("geometry", F.expr("ST_AsBinary(geometry)"))
    sdf.write.parquet.save(dbfs(f, True), partitionBy="sindex", **kwargs)
    info_sdf(sdf, f)
    return sdf


def to_gpq_sorted(sdf: SparkDataFrame, f: str, **kwargs):
    """SparkDataFrame to GeoParquet, sorted by BNG index"""
    (sdf.transform(centroid_index, resolution="1km").sort("sindex").write.format("geoparquet").save(dbfs(f, True), **kwargs))
    info_sdf(sdf, f)
    return sdf


def to_gpq_zsorted(sdf: SparkDataFrame, f: str, **kwargs):
    """SparkDataFrame to GeoParquet, sorted by geohash index"""
    (
        sdf.transform(sindex, resolution="1km", index_join=chipped_index)
        .withColumn(
            "geohash",
            F.expr('ST_GeoHash(ST_FlipCoordinates(ST_Transform(geometry, "EPSG:27700", "EPSG:4326")))'),
        )
        .sort("geohash")
        .write.format("geoparquet")
        .save(dbfs(f, True), **kwargs)
    )
    info_sdf(sdf, f)
    return sdf


to_gpq = to_gpq_partitioned
