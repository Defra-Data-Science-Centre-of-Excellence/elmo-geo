import os

import mosaic as mos

from elmo_geo.utils.dbr import RemoteDbUtils, dbutils, spark
from elmo_geo.utils.log import LOG
from elmo_geo.utils.types import SparkSession

try:
    from sedona.spark import SedonaContext

    register_sedona = SedonaContext.create
except Exception:
    from sedona.register import SedonaRegistrator

    register_sedona = SedonaRegistrator.registerAll


def register_dir(dir: str):
    """Move up the current working directory to <dir>.
    This is useful for notebooks in subfolders, as they _sometimes_ can't read local modules.
    """
    cwd = os.getcwd()
    nwd = cwd.split(dir)[0] + dir
    if cwd != nwd:
        os.chdir(nwd)
        LOG.info(f"Changed Directory: {cwd} => {nwd}")


def register_mosaic(spark: SparkSession, dbutils: RemoteDbUtils):
    mos.enable_mosaic(spark, dbutils)
    mos.enable_gdal(spark)
    LOG.info("Registered:  Mosaic")


def register(spark: SparkSession = spark, dbutils: RemoteDbUtils = dbutils, dir: str = "/elmo-geo"):
    register_dir(dir)
    # register_mosaic(spark, dbutils)
    register_sedona(spark)
    LOG.info("Registered: Sedona")
    return True
