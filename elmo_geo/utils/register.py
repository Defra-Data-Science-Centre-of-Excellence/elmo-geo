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

def set_spark_config(advisory_size: str | None = "32mb"):
    """Set configuration settings for partitioning and coalescing.

    Parameters:
        advisory_size: The advisory maximum size of partitions in bytes.
    """
    if advisory_size is not None:
        spark.conf.set("spark.sql.files.maxPartitionBytes", advisory_size)
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", advisory_size)
        LOG.info(f"Spark config advisory partition size:= set to {advisory_size}")

def register(
    spark: SparkSession = spark,
    dir: str = "/elmo-geo",
    advisory_size: str = "32mb",
):
    register_dir(dir)
    set_spark_config(advisory_size)
    register_sedona(spark)
    LOG.info("Registered: Sedona")
    return True
