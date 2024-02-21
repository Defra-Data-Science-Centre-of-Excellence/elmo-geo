import os

from pyspark.sql import functions as F

from elmo_geo import LOG
from elmo_geo.st.index import centroid_index, chipped_index, sindex
from elmo_geo.utils.misc import dbfs, sh_run
from elmo_geo.utils.types import SparkDataFrame


def st_simplify(col: str = "geometry", precision: int = 1) -> F.expr:
    """Simplifies geometries
    Ensuring they are valid for other processes
    And to avoid non-noded intersection errors

    precision=1 for BNG (EPSG:27700) is 100mm
    """
    null = 'ST_GeomFromText("Point EMPTY")'
    expr = f"ST_MakeValid(COALESCE({col}, {null}))"
    expr = f"ST_MakeValid(ST_PrecisionReduce(ST_MakeValid(ST_SimplifyPreserveTopology({expr}, {10**-precision})), {precision}))"
    expr = f"{expr} AS {col}"
    return F.expr(expr)


def count_files(folder):
    file_count = 0
    for root, dirs, files in os.walk(folder):
        file_count += len(files)
    return file_count


def convert_file(f_in: str, f_out: str):
    sh_run(["./elmo_geo/io/ogr2gpq.sh", f_in, f_out], shell=False)


def repartitonBy(sdf: SparkDataFrame, by: str) -> SparkDataFrame:
    n = sdf.select(by).distinct().count()
    return sdf.repartition(n, by)


def to_gpq_partitioned(sdf: SparkDataFrame, sf: str, **kwargs):
    """SparkDataFrame to GeoParquet, partitioned by BNG index"""
    sdf = sdf.withColumn("geometry", st_simplify()).transform(sindex).transform(repartitonBy, "sindex")
    sdf.write.format("geoparquet").save(sf, partitionBy="sindex", **kwargs)
    LOG.info(
        f"""
        Wrote GeoParquet: {sf}
        Count: {sdf.count()}
        sindexes: {sdf.select("sindex").distinct().count()}
        Partitions: {sdf.rdd.getNumPartitions()}
        Files: {count_files(dbfs(sf, False))}
    """,
    )
    return sdf


def to_gpq_sorted(sdf: SparkDataFrame, sf: str, **kwargs):
    """SparkDataFrame to GeoParquet, sorted by BNG index"""
    (sdf.transform(centroid_index, resolution="1km").sort("sindex").write.format("geoparquet").save(sf, **kwargs))


def to_gpq_zsorted(sdf: SparkDataFrame, sf: str, **kwargs):
    """SparkDataFrame to GeoParquet, sorted by geohash index"""
    (
        sdf.transform(sindex, resolution="1km", index_join=chipped_index)
        .withColumn(
            "geohash",
            F.expr('ST_GeoHash(ST_FlipCoordinates(ST_Transform(geometry, "EPSG:27700", "EPSG:4326")))'),
        )
        .sort("geohash")
        .write.format("geoparquet")
        .save(sf, **kwargs)
    )


to_gpq = to_gpq_partitioned
