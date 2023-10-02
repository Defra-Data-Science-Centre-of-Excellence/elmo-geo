import subprocess

from pyspark.sql import functions as F

from elmo_geo.utils.dbr import display
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


def run(exc: str):
    out = subprocess.run(exc, shell=True, check=True)
    LOG.info(exc, out)


def gtypes(sdf: SparkDataFrame, col: str = "geometry"):
    display(sdf.select(F.expr("ST_GeometryType({col}) AS gtype")).groupby("gtype").count())
