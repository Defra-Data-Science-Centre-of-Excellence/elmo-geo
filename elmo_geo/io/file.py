from pyspark.sql import functions as F

from elmo_geo import LOG
from elmo_geo.st.index import chipped_index, index, index_centroid
from elmo_geo.utils.misc import run
from elmo_geo.utils.types import SparkDataFrame


def convert_file(f_in: str, f_out: str, layer: str):
    out = run("/databricks/minconda/bin/ogr2ogr -progress -t_srs EPSG:27700 {f_out} {f_in} {layer}")
    LOG.info(out)


def to_gpq_partitioned(sdf: SparkDataFrame, sf_out: str):
    """SparkDataFrame to GeoParquet, partitioned by BNG index"""
    (sdf.transform(index_centroid).write.format("geoparquet").save(sf_out, partitionBy="sindex"))


def to_gpq_sorted(sdf: SparkDataFrame, sf_out: str):
    """SparkDataFrame to GeoParquet, sorted by BNG index"""
    (
        sdf.transform(index_centroid, resolution="1km")
        .sort("sindex")
        .write.format("geoparquet")
        .save(sf_out)
    )


def to_gpq_zsorted(sdf: SparkDataFrame, sf_out: str):
    """SparkDataFrame to GeoParquet, sorted by geohash index"""
    (
        sdf.transform(index, resolution="1km", index_join=chipped_index)
        .withColumn(
            "geohash",
            F.expr(
                'ST_GeoHash(ST_FlipCoordinates(ST_Transform(geometry, "EPSG:27700", "EPSG:4326")))'
            ),
        )
        .sort("geohash")
        .write.format("geoparquet")
        .save(sf_out)
    )
