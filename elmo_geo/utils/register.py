from elmo_geo.utils.log import LOG
from elmo_geo.utils.dbr import spark
from elmo_geo.utils.types import SparkSession

try:
    from sedona.spark import SedonaContext

    register_sedona = SedonaContext.create
except Exception:
    from sedona.register import SedonaRegistrator

    register_sedona = SedonaRegistrator.registerAll


def register(spark: SparkSession = spark):
    register_sedona(spark)
    LOG.info("Registered: Sedona")