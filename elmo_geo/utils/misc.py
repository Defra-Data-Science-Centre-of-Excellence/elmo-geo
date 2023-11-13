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


def sh_run(exc: str):
    out = subprocess.run(exc, shell=True, capture_output=True, text=True)
    LOG.info(out.__repr__())
    return out


def gtypes(sdf: SparkDataFrame, col: str = "geometry"):
    sdf = sdf.select(F.expr(f"ST_GeometryType({col}) AS gtype")).groupby("gtype").count()
    LOG.info(f"gtypes:  {sdf.rdd.collectAsMap()}")
    return sdf


def total_bounds(sdf: SparkDataFrame, col: str = "geometry"):
    sdf = sdf.groupby().agg(
        F.expr(f"MIN(ST_XMin({col})) AS xmin"),
        F.expr(f"MIN(ST_YMin({col})) AS ymin"),
        F.expr(f"MAX(ST_XMax({col})) AS xmax"),
        F.expr(f"MAX(ST_YMax({col})) AS ymax"),
    )
    LOG.info(f"total_bounds:  {sdf.head().asDict()}")
    return sdf
