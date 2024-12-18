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


def register_adaptive_partitions(adaptive_partitions: bool, default_parallelism: int, advisory_size: str):
    """Set configuration settings for partitioning and coalescing.

    Parameters:
        adaptive_partitions: Whether to adaptively partition and coalesce.
        default_parallelism: Default number of partitions to use in joins, aggregations and other operations.
        advisory_size: The advisory maximum size of partitions in bytes.
    """
    # Regardless of adaptive behaviour will likely want to set the default partition number above the default of 200
    spark.conf.set("spark.default.parallelism", default_parallelism)
    spark.conf.set("spark.sql.shuffle.partitions", default_parallelism)
    spark.conf.set("spark.sql.files.maxPartitionBytes", advisory_size)

    if adaptive_partitions:
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", default_parallelism)
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", advisory_size)
        spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "500kb")
    else:
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    LOG.info(f"Adaptive partitioning: {adaptive_partitions}. default partitions: {default_parallelism}, advisory partition size: {advisory_size}")


def register(
    spark: SparkSession = spark,
    dir: str = "/elmo-geo",
    adaptive_partitions: bool = True,
    default_parallelism: int = 600,
    advisory_size: str = "32mb",
):
    register_dir(dir)
    register_adaptive_partitions(
        adaptive_partitions,
        default_parallelism,
        advisory_size,
    )
    register_sedona(spark)
    LOG.info("Registered: Sedona")
    return True
