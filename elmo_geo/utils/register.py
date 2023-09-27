from elmo_geo import LOG
from elmo_geo.utils.dbr import spark
from elmo_geo.utils.types import SparkSession

import sedona
if sedona.version < '1.4.1':
  from sedona.register import SedonaRegistrator
  register_sedona = SedonaRegistrator.registerAll
else:
  from sedona.spark import SedonaContext
  register_sedona = SedonaContext.create


def register(spark:SparkSession=spark):
  register_sedona(spark)
  LOG.info(f'Registered: Sedona {sedona.version}')
