from pyspark.sql import functions as F

from elmo_geo.st.index import centroid_index, chipped_index, sindex
from elmo_geo.utils.misc import sh_run
from elmo_geo.utils.types import SparkDataFrame


def convert_file(f_in: str, f_out: str):
    sh_run(["./elmo_geo/io/ogr2gpq.sh", f_in, f_out], shell=False)


def to_gpq_partitioned(sdf: SparkDataFrame, sf_out: str, **kwargs):
    """SparkDataFrame to GeoParquet, partitioned by BNG index"""
    (sdf
        .transform(sindex)
        .write.format("geoparquet")
        .save(sf_out, partitionBy="sindex", **kwargs)
    )


def to_gpq_sorted(sdf: SparkDataFrame, sf_out: str, **kwargs):
    """SparkDataFrame to GeoParquet, sorted by BNG index"""
    (sdf
        .transform(centroid_index, resolution="1km")
        .sort("sindex")
        .write.format("geoparquet")
        .save(sf_out, **kwargs)
    )


def to_gpq_zsorted(sdf: SparkDataFrame, sf_out: str, **kwargs):
    """SparkDataFrame to GeoParquet, sorted by geohash index"""
    (sdf
        .transform(sindex, resolution="1km", index_join=chipped_index)
        .withColumn("geohash", F.expr('ST_GeoHash(ST_FlipCoordinates(ST_Transform(geometry, "EPSG:27700", "EPSG:4326")))'))
        .sort("geohash")
        .write.format("geoparquet")
        .save(sf_out, **kwargs)
    )


to_gpq = to_gpq_partitioned
