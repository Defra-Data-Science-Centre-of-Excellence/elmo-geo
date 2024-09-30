import os

from elmo_geo.utils.dbr import spark
from elmo_geo.utils.log import LOG
from elmo_geo.utils.types import SparkSession

try:
    from sedona.spark import SedonaContext

    register_sedona = SedonaContext.create
except Exception:
    from sedona.register import SedonaRegistrator

    register_sedona = SedonaRegistrator.registerAll


def register_dir(path: str):
    """Move up the current working directory to <dir>.
    This is useful for notebooks in subfolders, as they _sometimes_ can't read local modules.
    """
    cwd = os.getcwd()
    nwd = cwd.split(path)[0] + path
    if cwd != nwd:
        os.chdir(nwd)
        LOG.info(f"Changed Directory: {cwd} => {nwd}")


def register_no_coalesce(no_coalesce: bool):
    if no_coalesce:
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
        LOG.info("spark.sql.adaptive.coalescePartitions.enabled = false")


def register(spark: SparkSession = spark, dir: str = "/elmo-geo", no_coalesce: bool = False):
    register_dir(dir)
    register_no_coalesce(no_coalesce)
    register_sedona(spark)
    LOG.info("Registered: Sedona")
    return True
