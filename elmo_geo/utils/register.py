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


def register(spark: SparkSession = spark, dir: str = "/elmo-geo"):
    register_dir(dir)
    register_sedona(spark)
    LOG.info("Registered: Sedona")
    return True
