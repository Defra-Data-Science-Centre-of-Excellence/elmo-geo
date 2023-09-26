from elm_se.log import LOG
from elm_se.types import SparkSession
import sedona
import pkgutil
import inspect
from functools import partial


def partial_functions_with_kwarg(key, value):
  module = inspect.getmodule(inspect.stack()[1][0])
  for loader, name, is_pkg in pkgutil.walk_packages(module.__path__):
    submodule = loader.find_module(name).load_module(name)
    for subname in dir(submodule):
      obj = getattr(submodule, subname)
      if callable(obj) and hasattr(obj, '__code__') and hasattr(obj.__code__, 'co_varnames') and key in obj.__code__.co_varnames:
        setattr(submodule, subname, partial(obj, **{key: value}))
        LOG.info(f'Registered: {module.__name__}.{submodule.__name__}.{subname}: "{key}"')

def register_sedona(spark):
  import sedona
  if sedona.version < '1.4.1':
    import sedona.register
    sedona.register.SedonaRegistrator.registerAll(spark)
  else:
    import sedona.spark
    sedona.spark.SedonaContext.create(spark)
  LOG.info(f'Registered: Sedona {sedona.version}')

def register(spark:SparkSession=None):
  if spark is None:
    from databricks.sdk.runtime import spark
  register_sedona(spark)
  partial_functions_with_kwarg('spark', spark)
