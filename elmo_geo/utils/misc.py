import subprocess

from pyspark.sql import functions as F

from elmo_geo.utils.log import LOG
from elmo_geo.utils.types import SparkDataFrame


def dbfs(f: str, spark_api: bool):
    """Confirm the filepath to spark_api or file_api"""
    if f.startswith("dbfs:/"):
        if spark_api:
            return f
        else:
            return f.replace("dbfs:/", "/dbfs/")
    elif f.startswith("/dbfs/"):
        if spark_api:
            return f.replace("/dbfs/", "dbfs:/")
        else:
            return f
    else:
        raise Exception(f'file must start with "dbfs:/" or "/dbfs/": {f}')


def sh_run(exc: str, **kwargs):
    _kwargs = dict(shell=True, capture_output=True, text=True)
    _kwargs.update(kwargs)
    out = subprocess.run(exc, **_kwargs)
    LOG.info(out.__repr__())
    return out


def info_sdf(sdf: SparkDataFrame, col: str = "geometry") -> SparkDataFrame:
    """Get Info about SedonaDataFrame
    Logs the number of partitions, geometry types, number of features, and average number of coordinates.

    Tips:
    *gtypes,*
    In general you want 1 geometry dimension: Point+MultiPoint, LineString+LinearRing+MultiLineString, or Polygon+MultiPolygon.
    groupby `ST_GeometryType` and save as layers to handle separately.
    `F.expr('ST_Dump(geometry)')` for exploding those multis
    *coords,*
    It's best if your geometries fit into a single buffer, that's 256 coords.
    `ST_SubDivideExplode` exists for this purpose

    Example
    ```py
    info_sdf(sdf_water)
    >>> INFO:elmo_geo.utils.log:partitions:  169
    >>>                    gtype    count  mean_coords
    >>> 0        ST_MultiPolygon    73914         94.9
    >>> 1          ST_LineString  1049103         14.3
    >>> 2  ST_GeometryCollection  1779376         94.2
    >>> 3          ST_MultiPoint    21342          2.1
    >>> 4     ST_MultiLineString  1593084         57.0
    >>> 5               ST_Point   189777          1.0
    >>> 6             ST_Polygon   737962         44.8
    ```
    """
    n = sdf.rdd.getNumPartitions()
    df = (
        sdf.selectExpr(
            f"ST_GeometryType({col}) AS gtype",
            f"ST_NPoints({col}) AS n",
        )
        .groupby("gtype")
        .agg(
            F.count("gtype").alias("count"),
            F.round(F.mean("n"), 1).alias("mean_coords"),
        )
        .toPandas()
    )
    LOG.info(f"partitions:  {n}\n{df}")
    return df


def total_bounds(sdf: SparkDataFrame, col: str = "geometry"):
    sdf = sdf.groupby().agg(
        F.expr(f"MIN(ST_XMin({col})) AS xmin"),
        F.expr(f"MIN(ST_YMin({col})) AS ymin"),
        F.expr(f"MAX(ST_XMax({col})) AS xmax"),
        F.expr(f"MAX(ST_YMax({col})) AS ymax"),
    )
    LOG.info(f"total_bounds:  {sdf.head().asDict()}")
    return sdf


def cache(sdf: SparkDataFrame) -> SparkDataFrame:
    """sdf.transform(cache) to fully run your dataframe"""
    sdf.write.format("noop").mode("overwrite").save()
    return sdf


def isolate_error(
    sdf: SparkDataFrame,
    fn: callable,
    keys: list[str] | str = ["sindex", "id_parcel"],
) -> list:
    if isinstance(keys, str):
        keys = [keys]
    key = keys[0]
    for k in sdf.select(key).distinct().toPandas()[key].tolist():
        sdf2 = sdf.filter(F.col(key).isin())
        try:
            sdf2.transform(fn).transform(cache)
        except Exception:
            result = [k]
            if len(keys) > 1:
                result.extend(isolate_error(sdf2, fn, keys[1:]))
            yield result
