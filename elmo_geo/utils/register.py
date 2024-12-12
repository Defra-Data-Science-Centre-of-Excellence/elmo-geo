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


def register_adaptive_partitions(adaptive_partitions: bool, shuffle_partitions: int, default_parallelism: int):
    if adaptive_partitions:
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "32mb")  # default 64mb
        spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", default_parallelism)
        # spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "true")
        # spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "10mb")
        LOG.info("spark.sql.adaptive.coalescePartitions.enabled = true")
    else:
        # Without adaptive partitioning the default partitions values are increased.
        # Advised # partitions is 3x number of cores, but with complex geometries
        # it can be better to use higher than typical number of partitions.
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
        spark.conf.set("spark.sql.suffle.partitions", shuffle_partitions)
        spark.conf.set("spark.default.parallelism", default_parallelism)
        LOG.info("spark.sql.adaptive.coalescePartitions.enabled = false")


def register(
    spark: SparkSession = spark,
    dir: str = "/elmo-geo",
    adaptive_partitions: bool = False,
    shuffle_partitions: int = 600,
    default_parallelism: int = 600,
):
    register_dir(dir)
    register_adaptive_partitions(adaptive_partitions, shuffle_partitions, default_parallelism)
    register_sedona(spark)
    LOG.info("Registered: Sedona")
    return True
