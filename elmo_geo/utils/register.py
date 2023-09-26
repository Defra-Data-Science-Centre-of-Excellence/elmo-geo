from elmo_geo.utils.log import LOG
from elmo_geo.utils.dbr import spark



def register_sedona():
  import sedona
  if sedona.version < '1.4.1':
    import sedona.register
    sedona.register.SedonaRegistrator.registerAll(spark)
  else:
    import sedona.spark
    sedona.spark.SedonaContext.create(spark)
  LOG.info(f'Registered: Sedona {sedona.version}')


def register():
  register_sedona()
